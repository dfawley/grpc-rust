/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

//! Client-side health checking (gRFC A17), as a subchannel [`Producer`].
//!
//! The [`HealthProducer`] publishes a [`HealthState`] that mirrors the
//! subchannel's connectivity state, adjusted by the results of the
//! `grpc.health.v1.Health/Watch` stream when a health check service name is
//! configured.  A policy can use it alone for picking (gRFC A61).
//!
//! grpc doesn't depend on protobuf, so the Watch call itself is made by a
//! [`HealthWatchClient`] installed with [`set_health_watch_client`], which the
//! `grpc-health` crate implements.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::StatusCodeError;
use crate::StatusError;
use crate::async_trait;
use crate::client::ConnectivityState;
use crate::client::boxed::ArcInvoker;
use crate::client::load_balancing::producer::Producer;
use crate::client::load_balancing::producer::ProducerOptions;
use crate::client::load_balancing::producer::ProducerWatch;
use crate::client::load_balancing::producer::Publisher;
use crate::client::load_balancing::producer::WatchId;
use crate::client::load_balancing::subchannel::SubchannelState;
use crate::client::name_resolution::backoff::DEFAULT_EXPONENTIAL_CONFIG;
use crate::client::name_resolution::backoff::ExponentialBackoff;
use crate::rt::BoxedTaskHandle;
use crate::rt::GrpcRuntime;

/// The health of a subchannel, delivered to watchers of [`HealthProducer`].
///
/// This is a connectivity state, like the subchannel's own.  It can be
/// CONNECTING or TRANSIENT_FAILURE while the subchannel is READY.
#[derive(Clone, Debug)]
pub struct HealthState(pub SubchannelState);

/// The subchannel attribute that intercepts health watches.  See
/// [`ProducerWatch`].
pub type HealthWatch = ProducerWatch<HealthProducer>;

/// The serving status reported by a health check response.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServingStatus {
    /// `UNKNOWN`.
    Unknown,
    /// `SERVING`.
    Serving,
    /// `NOT_SERVING`.
    NotServing,
    /// `SERVICE_UNKNOWN`.
    ServiceUnknown,
}

/// Makes the `grpc.health.v1.Health/Watch` call for the [`HealthProducer`].
///
/// Implemented by the `grpc-health` crate, which installs it with
/// [`set_health_watch_client`].
#[async_trait]
pub trait HealthWatchClient: Send + Sync + 'static {
    /// Runs one Watch call for `service` on `subchannel`, and reports the
    /// status in each response to `reporter`.  Returns the call's final
    /// status: `Ok` if it ended with OK, or the error it ended with.
    ///
    /// The returned future is dropped to cancel the call.
    async fn watch(
        &self,
        subchannel: ArcInvoker,
        service: String,
        reporter: HealthReporter,
    ) -> Result<(), StatusError>;
}

static HEALTH_WATCH_CLIENT: RwLock<Option<Arc<dyn HealthWatchClient>>> = RwLock::new(None);

/// Installs the client used to make health check calls, replacing any
/// installed before.  Without one, health checking is disabled even when a
/// service name is configured.
pub fn set_health_watch_client(client: Arc<dyn HealthWatchClient>) {
    *HEALTH_WATCH_CLIENT.write().unwrap() = Some(client);
}

fn health_watch_client() -> Option<Arc<dyn HealthWatchClient>> {
    HEALTH_WATCH_CLIENT.read().unwrap().clone()
}

/// Receives the serving status from each response of one health check call.
#[derive(Clone)]
pub struct HealthReporter {
    inner: Weak<Inner>,
    /// Identifies the stream this reporter belongs to.  Reports for any other
    /// stream are ignored.
    epoch: u64,
    received: Arc<AtomicBool>,
}

impl HealthReporter {
    /// Reports the serving status from a response.
    pub fn report(&self, status: ServingStatus) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut shared = inner.shared.lock().unwrap();
        if shared.epoch != self.epoch {
            return;
        }
        self.received.store(true, Ordering::Release);
        let state = if status == ServingStatus::Serving {
            SubchannelState::ready()
        } else {
            SubchannelState::transient_failure(format!(
                "{}: backend reported itself unhealthy ({status:?})",
                inner.address
            ))
        };
        shared.set_health(state);
    }
}

impl Debug for HealthReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthReporter")
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

/// Publishes the health of a subchannel.  See the [module docs](self).
///
/// Every watcher receives the same state, starting with the current state
/// when it is added.
///
/// | Situation | Sent to watchers | Stream |
/// | --- | --- | --- |
/// | Subchannel not READY | that state | stopped |
/// | READY, no service name or no [`HealthWatchClient`] | READY | none |
/// | READY, before the first response | CONNECTING | started |
/// | A response arrives | READY if SERVING, otherwise TRANSIENT_FAILURE | continues |
/// | `UNIMPLEMENTED` | READY | not retried until the subchannel next becomes READY |
/// | Any other end of the stream | TRANSIENT_FAILURE, then CONNECTING when the next attempt starts | retried: immediately if the stream received a response, otherwise with backoff |
/// | Service name changes | as for a new stream | restarted |
pub struct HealthProducer {
    inner: Arc<Inner>,
    name_task: BoxedTaskHandle,
}

struct Inner {
    invoker: ArcInvoker,
    runtime: GrpcRuntime,
    address: String,
    /// Overrides the installed client, for tests.
    client_override: Option<Arc<dyn HealthWatchClient>>,
    shared: Mutex<Shared>,
}

struct Shared {
    watchers: HashMap<WatchId, Publisher<HealthProducer>>,
    /// The subchannel's state.
    raw: SubchannelState,
    /// The health state last sent to watchers.
    health: SubchannelState,
    service_name: Option<String>,
    /// Set when the server returned UNIMPLEMENTED; cleared when the subchannel
    /// leaves READY.
    unimplemented: bool,
    stream: Option<BoxedTaskHandle>,
    /// Incremented whenever the stream is started or stopped.  A stream task
    /// or reporter acts only while the epoch is the one it was started with,
    /// so nothing is published from a stream after it is stopped.
    epoch: u64,
}

fn same_state(a: &SubchannelState, b: &SubchannelState) -> bool {
    a.connectivity_state == b.connectivity_state
        && a.last_connection_error == b.last_connection_error
}

static WARNED_NO_CLIENT: AtomicBool = AtomicBool::new(false);

impl Shared {
    /// Sends `state` to every watcher if it differs from the last one sent.
    fn set_health(&mut self, state: SubchannelState) {
        if same_state(&self.health, &state) {
            return;
        }
        self.health = state;
        for publisher in self.watchers.values() {
            publisher.send(HealthState(self.health.clone()));
        }
    }

    fn stop_stream(&mut self) {
        self.epoch += 1;
        if let Some(stream) = self.stream.take() {
            stream.abort();
        }
    }
}

impl Inner {
    fn client(&self) -> Option<Arc<dyn HealthWatchClient>> {
        self.client_override.clone().or_else(health_watch_client)
    }

    /// Brings the stream and the published health in line with the current
    /// state, service name and client.
    fn reconcile(self: &Arc<Self>, shared: &mut Shared) {
        if shared.raw.connectivity_state != ConnectivityState::Ready {
            shared.stop_stream();
            shared.set_health(shared.raw.clone());
            return;
        }
        if shared.stream.is_some() {
            return;
        }
        let Some(service_name) = shared.service_name.clone() else {
            shared.set_health(SubchannelState::ready());
            return;
        };
        if shared.unimplemented {
            shared.set_health(SubchannelState::ready());
            return;
        }
        let Some(client) = self.client() else {
            if !WARNED_NO_CLIENT.swap(true, Ordering::Relaxed) {
                eprintln!(
                    "health check service name {service_name:?} is configured, but no \
                     HealthWatchClient is installed; health checking is disabled"
                );
            }
            shared.set_health(SubchannelState::ready());
            return;
        };

        shared.stop_stream();
        let epoch = shared.epoch;
        shared.stream = Some(self.runtime.spawn(Box::pin(run_stream(
            self.clone(),
            epoch,
            service_name,
            client,
        ))));
        shared.set_health(SubchannelState::connecting());
    }
}

/// Runs the health check stream, and retries it, until it is stopped.
async fn run_stream(
    inner: Arc<Inner>,
    epoch: u64,
    service_name: String,
    client: Arc<dyn HealthWatchClient>,
) {
    let mut backoff = ExponentialBackoff::new(DEFAULT_EXPONENTIAL_CONFIG.clone())
        .expect("default backoff config is valid");
    loop {
        let received = Arc::new(AtomicBool::new(false));
        let reporter = HealthReporter {
            inner: Arc::downgrade(&inner),
            epoch,
            received: received.clone(),
        };
        let result = client
            .watch(inner.invoker.clone(), service_name.clone(), reporter)
            .await;

        let retry_immediately = {
            let mut shared = inner.shared.lock().unwrap();
            if shared.epoch != epoch {
                return;
            }
            if let Err(err) = &result
                && err.code() == StatusCodeError::Unimplemented
            {
                eprintln!(
                    "{}: health checking Watch method returned UNIMPLEMENTED; disabling \
                     health checks but assuming the server is healthy",
                    inner.address
                );
                shared.unimplemented = true;
                shared.stream = None;
                shared.set_health(SubchannelState::ready());
                return;
            }
            // Any other end of the stream is a failure (gRFC A17), even after
            // it received responses.
            //
            // TODO: if the stream failed because its connection was lost, the
            // subchannel will leave READY shortly, but we may see the failure
            // first: hyper fails streams before it reports the connection
            // closed.  Then watchers briefly see TRANSIENT_FAILURE (and
            // CONNECTING, as the retry starts on the dying connection) before
            // the subchannel's new state.  The spec says a connection loss
            // cancels the stream instead.  Fixing this needs stream failures
            // caused by connection loss to be held back until the subchannel
            // has processed the disconnection.
            let message = match &result {
                Ok(()) => "health check stream ended".to_string(),
                Err(err) => format!("health check stream failed: {err}"),
            };
            shared.set_health(SubchannelState::transient_failure(format!(
                "{}: {message}",
                inner.address
            )));
            // After a stream that received a response, the next attempt starts
            // immediately, and later attempts back off again.
            let received = received.load(Ordering::Acquire);
            if received {
                backoff.reset();
                shared.set_health(SubchannelState::connecting());
            }
            received
        };

        if retry_immediately {
            continue;
        }

        inner.runtime.sleep(backoff.backoff_duration()).await;

        let mut shared = inner.shared.lock().unwrap();
        if shared.epoch != epoch {
            return;
        }
        // The next attempt starts.
        shared.set_health(SubchannelState::connecting());
    }
}

impl HealthProducer {
    fn new_with_client(
        options: ProducerOptions,
        client_override: Option<Arc<dyn HealthWatchClient>>,
    ) -> Self {
        let mut names = options.health_service_name;
        let service_name = names.borrow_and_update().clone();
        let inner = Arc::new(Inner {
            invoker: options.subchannel,
            runtime: options.runtime.clone(),
            address: options.address.to_string(),
            client_override,
            shared: Mutex::new(Shared {
                watchers: HashMap::new(),
                raw: options.state.clone(),
                health: options.state,
                service_name,
                unimplemented: false,
                stream: None,
                epoch: 0,
            }),
        });
        inner.reconcile(&mut inner.shared.lock().unwrap());

        // Follow changes to the channel's service name.
        let weak = Arc::downgrade(&inner);
        let name_task = options.runtime.spawn(Box::pin(async move {
            while names.changed().await.is_ok() {
                let service_name = names.borrow_and_update().clone();
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let mut shared = inner.shared.lock().unwrap();
                if shared.service_name == service_name {
                    continue;
                }
                shared.service_name = service_name;
                shared.unimplemented = false;
                shared.stop_stream();
                inner.reconcile(&mut shared);
            }
        }));

        Self { inner, name_task }
    }
}

impl Producer for HealthProducer {
    type Data = HealthState;
    type Params = ();

    fn new(options: ProducerOptions) -> Self {
        Self::new_with_client(options, None)
    }

    fn subchannel_state(&mut self, state: &SubchannelState) {
        let mut shared = self.inner.shared.lock().unwrap();
        shared.raw = state.clone();
        if state.connectivity_state != ConnectivityState::Ready {
            // Health checking may be implemented on the next connection.
            shared.unimplemented = false;
        }
        // TODO: this is sent to watchers directly, but SubchannelSharing
        // delivers the subchannel's own SubchannelUpdate with a second trip
        // through the work queue.  So watchers may see the health state for
        // a change before the SubchannelUpdate for it.
        self.inner.reconcile(&mut shared);
    }

    fn add_watch(&mut self, id: WatchId, _params: (), publisher: Publisher<Self>) {
        let mut shared = self.inner.shared.lock().unwrap();
        publisher.send(HealthState(shared.health.clone()));
        shared.watchers.insert(id, publisher);
    }

    fn remove_watch(&mut self, id: WatchId) {
        self.inner.shared.lock().unwrap().watchers.remove(&id);
    }
}

impl Drop for HealthProducer {
    fn drop(&mut self) {
        self.name_task.abort();
        self.inner.shared.lock().unwrap().stop_stream();
    }
}

impl Debug for HealthProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthProducer").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use tokio::sync::mpsc as tokio_mpsc;
    use tokio::sync::oneshot;
    use tokio::sync::watch;

    use super::*;
    use crate::client::CallOptions;
    use crate::client::Invoke;
    use crate::client::RequestHeaders;
    use crate::client::boxed::BoxRecvStream;
    use crate::client::boxed::BoxSendStream;
    use crate::client::load_balancing::WorkData;
    use crate::client::load_balancing::WorkScheduler;
    use crate::client::load_balancing::producer::WatchUpdate;
    use crate::client::load_balancing::subchannel::Subchannel;
    use crate::client::load_balancing::test_utils::TestSubchannel;
    use crate::client::load_balancing::test_utils::TestWorkScheduler;
    use crate::core::Address;

    struct NopInvoker;
    impl Invoke for NopInvoker {
        type SendStream = BoxSendStream;
        type RecvStream = BoxRecvStream;
        async fn invoke(
            &self,
            _headers: RequestHeaders,
            _options: CallOptions,
        ) -> (BoxSendStream, BoxRecvStream) {
            panic!("the fake client doesn't start RPCs");
        }
    }

    // One Watch call made by the fake client.
    struct Call {
        service: String,
        reporter: HealthReporter,
        finish: oneshot::Sender<Result<(), StatusError>>,
    }

    struct FakeClient {
        calls: tokio_mpsc::UnboundedSender<Call>,
    }

    #[async_trait]
    impl HealthWatchClient for FakeClient {
        async fn watch(
            &self,
            _subchannel: ArcInvoker,
            service: String,
            reporter: HealthReporter,
        ) -> Result<(), StatusError> {
            let (finish, done) = oneshot::channel();
            let _ = self.calls.send(Call {
                service,
                reporter,
                finish,
            });
            // If the producer cancels the call, this future is dropped.
            done.await.unwrap_or(Ok(()))
        }
    }

    struct Env {
        producer: HealthProducer,
        names: watch::Sender<Option<String>>,
        calls: tokio_mpsc::UnboundedReceiver<Call>,
        rx_work: mpsc::Receiver<Option<WorkData>>,
        // The subchannel updates are tagged with; kept alive for delivery.
        _subchannel: Arc<dyn Subchannel>,
    }

    impl Env {
        fn new(state: SubchannelState, service_name: Option<&str>) -> Self {
            let (names, names_rx) = watch::channel(service_name.map(str::to_string));
            let (tx_calls, calls) = tokio_mpsc::unbounded_channel();
            let options = ProducerOptions {
                subchannel: ArcInvoker::new(NopInvoker),
                state,
                address: Address::default(),
                runtime: crate::rt::default_runtime(),
                health_service_name: names_rx,
            };
            let mut producer = HealthProducer::new_with_client(
                options,
                Some(Arc::new(FakeClient { calls: tx_calls })),
            );
            let subchannel: Arc<dyn Subchannel> =
                Arc::new(TestSubchannel::new(Address::default(), mpsc::channel().0));
            let (tx_work, rx_work) = mpsc::channel();
            let ws: Arc<dyn WorkScheduler> = Arc::new(TestWorkScheduler { tx_work });
            producer.add_watch(
                crate::client::load_balancing::producer::test_utils::watch_id(0),
                (),
                crate::client::load_balancing::producer::test_utils::new_publisher(&subchannel, ws),
            );
            Self {
                producer,
                names,
                calls,
                rx_work,
                _subchannel: subchannel,
            }
        }

        // Returns the next health state sent to the watcher.
        fn next_state(&self) -> SubchannelState {
            let data = self
                .rx_work
                .try_recv()
                .expect("expected a health update")
                .expect("expected data");
            WatchUpdate::<HealthProducer>::from_work_data(data)
                .expect("expected a health WatchUpdate")
                .data
                .0
        }

        fn expect_state(&self, want: ConnectivityState) {
            let got = self.next_state();
            assert_eq!(got.connectivity_state, want, "got {got:?}");
        }

        fn expect_no_state(&self) {
            if let Ok(data) = self.rx_work.try_recv() {
                panic!("unexpected update: {data:?}");
            }
        }

        async fn next_call(&mut self) -> Call {
            crate::rt::default_runtime()
                .timeout(Duration::from_secs(1), self.calls.recv())
                .await
                .expect("timed out waiting for a health check call")
                .expect("client dropped")
        }

        async fn expect_no_call(&mut self) {
            if let Ok(Some(call)) = crate::rt::default_runtime()
                .timeout(Duration::from_millis(10), self.calls.recv())
                .await
            {
                panic!("unexpected call for {}", call.service);
            }
        }
    }

    // Lets spawned tasks run.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn mirrors_state_without_service_name() {
        let mut env = Env::new(SubchannelState::idle(), None);
        env.expect_state(ConnectivityState::Idle);
        env.producer
            .subchannel_state(&SubchannelState::connecting());
        env.expect_state(ConnectivityState::Connecting);
        env.producer.subchannel_state(&SubchannelState::ready());
        env.expect_state(ConnectivityState::Ready);
        env.producer
            .subchannel_state(&SubchannelState::transient_failure("err"));
        let state = env.next_state();
        assert_eq!(
            state.connectivity_state,
            ConnectivityState::TransientFailure
        );
        assert_eq!(state.last_connection_error.as_deref(), Some("err"));
        env.expect_no_call().await;
    }

    #[tokio::test]
    async fn reports_serving_status() {
        let mut env = Env::new(SubchannelState::connecting(), Some("svc"));
        env.expect_state(ConnectivityState::Connecting);
        env.producer.subchannel_state(&SubchannelState::ready());
        // READY, before the first response: CONNECTING, which was already the
        // last state sent, so nothing new is sent.
        env.expect_no_state();
        let call = env.next_call().await;
        assert_eq!(call.service, "svc");

        call.reporter.report(ServingStatus::Serving);
        env.expect_state(ConnectivityState::Ready);
        call.reporter.report(ServingStatus::NotServing);
        env.expect_state(ConnectivityState::TransientFailure);
        call.reporter.report(ServingStatus::Serving);
        env.expect_state(ConnectivityState::Ready);

        // Leaving READY stops the stream and reports the subchannel's state.
        env.producer.subchannel_state(&SubchannelState::idle());
        env.expect_state(ConnectivityState::Idle);
        settle().await;
        assert!(call.finish.is_closed(), "the call should be cancelled");
        // Reports from the stopped stream are ignored.
        call.reporter.report(ServingStatus::Serving);
        env.expect_no_state();
    }

    #[tokio::test]
    async fn new_watcher_gets_current_state() {
        let mut env = Env::new(SubchannelState::ready(), Some("svc"));
        env.expect_state(ConnectivityState::Connecting);
        let call = env.next_call().await;
        call.reporter.report(ServingStatus::Serving);
        env.expect_state(ConnectivityState::Ready);

        let subchannel: Arc<dyn Subchannel> =
            Arc::new(TestSubchannel::new(Address::default(), mpsc::channel().0));
        let (tx_work, rx_work) = mpsc::channel();
        env.producer.add_watch(
            crate::client::load_balancing::producer::test_utils::watch_id(1),
            (),
            crate::client::load_balancing::producer::test_utils::new_publisher(
                &subchannel,
                Arc::new(TestWorkScheduler { tx_work }),
            ),
        );
        let data = rx_work.try_recv().unwrap().unwrap();
        let update = WatchUpdate::<HealthProducer>::from_work_data(data).unwrap();
        assert!(Arc::ptr_eq(&update.subchannel, &subchannel));
        assert_eq!(update.data.0.connectivity_state, ConnectivityState::Ready);
    }

    #[tokio::test]
    async fn unimplemented_disables_until_next_ready() {
        let mut env = Env::new(SubchannelState::ready(), Some("svc"));
        env.expect_state(ConnectivityState::Connecting);
        let call = env.next_call().await;
        call.finish
            .send(Err(StatusError::new(StatusCodeError::Unimplemented, "")))
            .unwrap();
        settle().await;
        env.expect_state(ConnectivityState::Ready);
        env.expect_no_call().await;

        // Health checking is tried again on the next connection.
        env.producer.subchannel_state(&SubchannelState::idle());
        env.expect_state(ConnectivityState::Idle);
        env.producer.subchannel_state(&SubchannelState::ready());
        env.expect_state(ConnectivityState::Connecting);
        env.next_call().await;
    }

    #[tokio::test(start_paused = true)]
    async fn retries_with_backoff_or_immediately() {
        let mut env = Env::new(SubchannelState::ready(), Some("svc"));
        env.expect_state(ConnectivityState::Connecting);

        // A failure before any response: TRANSIENT_FAILURE, then a retry after
        // backoff, which goes back to CONNECTING.
        let call = env.next_call().await;
        call.finish
            .send(Err(StatusError::new(StatusCodeError::Unavailable, "oops")))
            .unwrap();
        settle().await;
        env.expect_state(ConnectivityState::TransientFailure);
        env.expect_no_call().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        let call = env.next_call().await;
        env.expect_state(ConnectivityState::Connecting);

        // A failure after a response: TRANSIENT_FAILURE, then an immediate
        // retry, which goes back to CONNECTING.
        call.reporter.report(ServingStatus::Serving);
        env.expect_state(ConnectivityState::Ready);
        call.finish.send(Ok(())).unwrap();
        let call = env.next_call().await;
        env.expect_state(ConnectivityState::TransientFailure);
        env.expect_state(ConnectivityState::Connecting);
        env.expect_no_state();

        // The attempt after that backs off again if it gets no response.
        call.finish
            .send(Err(StatusError::new(StatusCodeError::Unavailable, "oops")))
            .unwrap();
        settle().await;
        env.expect_state(ConnectivityState::TransientFailure);
        env.expect_no_call().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        env.next_call().await;
        env.expect_state(ConnectivityState::Connecting);
    }

    #[tokio::test]
    async fn service_name_changes_restart_stream() {
        let mut env = Env::new(SubchannelState::ready(), Some("a"));
        env.expect_state(ConnectivityState::Connecting);
        let call = env.next_call().await;
        assert_eq!(call.service, "a");
        call.reporter.report(ServingStatus::Serving);
        env.expect_state(ConnectivityState::Ready);

        env.names.send(Some("b".to_string())).unwrap();
        let call_b = env.next_call().await;
        assert_eq!(call_b.service, "b");
        env.expect_state(ConnectivityState::Connecting);
        settle().await;
        assert!(call.finish.is_closed(), "the old call should be cancelled");

        // Removing the name stops health checking.
        env.names.send(None).unwrap();
        settle().await;
        env.expect_state(ConnectivityState::Ready);
        assert!(call_b.finish.is_closed());
        env.expect_no_call().await;
    }

    #[tokio::test]
    async fn drop_cancels_stream() {
        let mut env = Env::new(SubchannelState::ready(), Some("svc"));
        let call = env.next_call().await;
        drop(env.producer);
        settle().await;
        assert!(call.finish.is_closed());
    }
}
