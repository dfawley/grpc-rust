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

//! Subchannel producers.
//!
//! A [`Producer`] runs on a subchannel and publishes data to LB policies that
//! watch it, e.g. the results of a health-checking or ORCA stream.  A policy
//! watches a producer type with [`SubchannelExt::watch`]; the subchannel
//! creates the producer on the first watch and drops it when the last watch
//! ends.  Producers live as long as their subchannel, across any number of
//! READY / not-READY cycles, and receive its state changes.
//!
//! A parent policy can intercept its children's watches for a producer type by
//! returning a [`ProducerWatch`] attribute from the subchannels it wraps.

use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::watch;

use crate::client::boxed::ArcInvoker;
use crate::client::load_balancing::WorkData;
use crate::client::load_balancing::WorkScheduler;
use crate::client::load_balancing::subchannel::Subchannel;
use crate::client::load_balancing::subchannel::SubchannelState;
use crate::core::Address;
use crate::rt::GrpcRuntime;

/// A type that runs on a subchannel and publishes data to the watches on it.
///
/// The subchannel creates the producer with [`Producer::new`] when it gets
/// its first watch for the type, and drops it when the last watch ends or the
/// subchannel is dropped.  The producer must stop any work it started in its
/// `Drop` implementation.
///
/// All methods are called synchronously while the subchannel holds a lock, so
/// they must not block or call back into the subchannel.  Producers do their
/// asynchronous work in tasks spawned on [`ProducerOptions::runtime`].
///
/// Fan-out is the producer's job: it keeps the [`Publisher`] from each
/// `add_watch` and decides what to send to each one.  It should send its
/// current value, if it has one, to each new watch.
pub trait Producer: Send + Sized + 'static {
    /// The data delivered to watchers in [`WatchUpdate::data`].
    type Data: Send + Debug + 'static;
    /// The parameters supplied with each watch.
    type Params: Send + 'static;

    /// Creates the producer.  [`Producer::add_watch`] is called for the first
    /// watch immediately afterwards.
    fn new(options: ProducerOptions) -> Self;

    /// Called, in order, on every state change of the subchannel.
    fn subchannel_state(&mut self, state: &SubchannelState);

    /// Called when a watch is added.  `publisher` delivers data to this watch
    /// until [`Producer::remove_watch`] is called with the same `id`.
    fn add_watch(&mut self, id: WatchId, params: Self::Params, publisher: Publisher<Self>);

    /// Called when a watch ends.  This is called for every watch, including
    /// the last, before the producer is dropped.
    fn remove_watch(&mut self, id: WatchId);
}

/// What a [`Producer`] is given when it is created.
#[non_exhaustive]
pub struct ProducerOptions {
    /// Sends each RPC on a READY connection of the subchannel, with the same
    /// call credentials checks as the subchannel's own RPCs.
    ///
    /// This is valid for the producer's whole lifetime.  An RPC started while
    /// the subchannel is not READY, or after it is dropped, fails immediately
    /// with UNAVAILABLE.  A stream fails when its connection is lost.
    pub subchannel: ArcInvoker,
    /// The subchannel's state when the producer is created.
    pub state: SubchannelState,
    /// The subchannel's address.
    pub address: Address,
    /// The runtime for spawning tasks and timers.
    pub runtime: GrpcRuntime,
    /// The channel's configured health check service name.  Only grpc's own
    /// health producer reads it.
    pub(crate) health_service_name: watch::Receiver<Option<String>>,
}

impl Debug for ProducerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerOptions")
            .field("state", &self.state)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// Identifies one watch on a producer.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WatchId(u64);

/// Delivers data from a producer to one watch.
///
/// Each [`Publisher::send`] schedules a [`WatchUpdate`] on the watch's work
/// scheduler, tagged with the subchannel the watch was created on.  `send`
/// does nothing once the watch has ended or the policy has dropped that
/// subchannel.
pub struct Publisher<P: Producer> {
    inner: Arc<PublisherInner>,
    _producer: std::marker::PhantomData<fn() -> P>,
}

struct PublisherInner {
    /// The subchannel the policy called `watch` on, held weakly so delivery
    /// stops once the policy releases it.
    subchannel: Weak<dyn Subchannel>,
    work_scheduler: Arc<dyn WorkScheduler>,
    closed: AtomicBool,
}

impl<P: Producer> Publisher<P> {
    fn new(subchannel: Weak<dyn Subchannel>, work_scheduler: Arc<dyn WorkScheduler>) -> Self {
        Self {
            inner: Arc::new(PublisherInner {
                subchannel,
                work_scheduler,
                closed: AtomicBool::new(false),
            }),
            _producer: std::marker::PhantomData,
        }
    }

    /// Schedules a [`WatchUpdate`] carrying `data` on this watch's work
    /// scheduler.  Does nothing if the watch has ended.
    pub fn send(&self, data: P::Data) {
        if self.is_closed() {
            return;
        }
        let Some(subchannel) = self.inner.subchannel.upgrade() else {
            return;
        };
        self.inner
            .work_scheduler
            .schedule_work(Some(WorkData::new(WatchUpdate::<P> { subchannel, data })));
    }

    /// Returns true once the watch has ended, after which [`Publisher::send`]
    /// does nothing.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
}

impl<P: Producer> Clone for Publisher<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _producer: std::marker::PhantomData,
        }
    }
}

impl<P: Producer> Debug for Publisher<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("producer", &std::any::type_name::<P>())
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Data from a producer, delivered to a watching policy's `work` method as
/// [`WorkData`].
pub struct WatchUpdate<P: Producer> {
    /// The subchannel the policy called [`SubchannelExt::watch`] on.
    pub subchannel: Arc<dyn Subchannel>,
    /// The producer's data.
    pub data: P::Data,
}

impl<P: Producer> WatchUpdate<P> {
    /// Returns the `WatchUpdate<P>` held by `data`, or `data` unchanged if it
    /// holds something else.
    pub fn from_work_data(data: WorkData) -> Result<Self, WorkData> {
        data.downcast::<Self>()
    }
}

impl<P: Producer> Debug for WatchUpdate<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchUpdate")
            .field("producer", &std::any::type_name::<P>())
            .field("subchannel", &self.subchannel)
            .field("data", &self.data)
            .finish()
    }
}

/// Keeps a watch alive.  Dropping it ends the watch.
///
/// Some data may still be delivered after the handle is dropped, if it was
/// scheduled before; policies ignore data they no longer need.
pub struct WatchHandle {
    on_drop: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl WatchHandle {
    /// Creates a handle that calls `on_drop` when it is dropped.  For use by
    /// [`WatchInterceptor`]s.
    pub fn new(on_drop: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            on_drop: Some(Box::new(on_drop)),
        }
    }

    fn noop() -> Self {
        Self { on_drop: None }
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            on_drop();
        }
    }
}

impl Debug for WatchHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchHandle").finish_non_exhaustive()
    }
}

/// Intercepts watches for producer `P` on the subchannels a parent policy
/// wraps.  See [`ProducerWatch`].
pub trait WatchInterceptor<P: Producer>: Send + Sync + 'static {
    /// Handles a watch for `P`.  `publisher` delivers data to the watcher.
    /// The returned handle is dropped when the watch ends.
    fn watch(&self, params: P::Params, publisher: Publisher<P>) -> WatchHandle;
}

/// The subchannel attribute that intercepts watches for producer `P`.
///
/// A wrapping subchannel returns this from
/// [`get_attribute_dyn`](Subchannel::get_attribute_dyn) for
/// `TypeId::of::<ProducerWatch<P>>()`.  [`SubchannelExt::watch`] calls the
/// first interceptor it finds, searching from the subchannel it is called on;
/// wrappers forward attribute lookups by default.  Watches for producer types
/// a wrapper doesn't intercept pass through it.
///
/// To watch the real data below itself, an interceptor calls
/// [`SubchannelExt::watch`] on the subchannel it wraps.
//
// TODO: an interceptor that receives data through its own watch and passes it
// on with `Publisher::send` makes it take a second trip through the work
// queue, so the child can see it after a later SubchannelUpdate.  Consider
// letting interceptors deliver to their children synchronously instead.
pub struct ProducerWatch<P: Producer>(Arc<dyn WatchInterceptor<P>>);

impl<P: Producer> ProducerWatch<P> {
    /// Creates the attribute for `interceptor`.
    pub fn new(interceptor: Arc<dyn WatchInterceptor<P>>) -> Self {
        Self(interceptor)
    }
}

impl<P: Producer> Debug for ProducerWatch<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ProducerWatch")
            .field(&std::any::type_name::<P>())
            .finish()
    }
}

/// Adds [`watch`](SubchannelExt::watch) to subchannels.
pub trait SubchannelExt {
    /// Watches producer `P` on this subchannel, and returns a handle that
    /// keeps the watch alive.
    ///
    /// The producer's data is delivered to `work_scheduler` as
    /// [`WatchUpdate<P>`] work, tagged with this subchannel.  The watch lasts
    /// until the handle is dropped, across any number of state changes, so a
    /// policy watches once, not on each READY.
    ///
    /// A watch on a subchannel that is not hosted by a grpc channel and is not
    /// intercepted delivers nothing, and an error is logged.
    fn watch<P: Producer>(
        &self,
        params: P::Params,
        work_scheduler: Arc<dyn WorkScheduler>,
    ) -> WatchHandle;
}

impl SubchannelExt for Arc<dyn Subchannel> {
    fn watch<P: Producer>(
        &self,
        params: P::Params,
        work_scheduler: Arc<dyn WorkScheduler>,
    ) -> WatchHandle {
        let publisher = Publisher::<P>::new(Arc::downgrade(self), work_scheduler);
        let publisher_inner = publisher.inner.clone();

        let inner_handle = if let Some(interceptor) = self
            .get_attribute::<ProducerWatch<P>>()
            .map(|w| w.0.clone())
        {
            interceptor.watch(params, publisher)
        } else if let Some(producers) = self.get_attribute::<ProducerSet>().cloned() {
            producers.watch::<P>(params, publisher)
        } else {
            eprintln!(
                "watch for producer {} on subchannel {} which has no producers; nothing will be delivered",
                std::any::type_name::<P>(),
                self.address()
            );
            WatchHandle::noop()
        };

        // Close the publisher before ending the watch, so nothing is sent on
        // it afterwards even if the interceptor or producer keeps a clone.
        WatchHandle::new(move || {
            publisher_inner.closed.store(true, Ordering::Release);
            drop(inner_handle);
        })
    }
}

/// The producers of one subchannel, keyed by producer type.
///
/// Returned by the hosting subchannel as an attribute, so that
/// [`SubchannelExt::watch`] can find it through any wrappers.
#[derive(Clone)]
pub(crate) struct ProducerSet {
    inner: Arc<Mutex<ProducerSetInner>>,
}

struct ProducerSetInner {
    /// A copy of the subchannel's current state, so that watching never needs
    /// the subchannel's own lock.
    state: SubchannelState,
    producers: HashMap<TypeId, Box<dyn ErasedProducer>>,
    next_id: u64,
    invoker: ArcInvoker,
    address: Address,
    runtime: GrpcRuntime,
    health_service_name: watch::Receiver<Option<String>>,
}

/// A producer and the watches on it, with the producer type erased.
trait ErasedProducer: Send {
    fn subchannel_state(&mut self, state: &SubchannelState);
    /// Removes a watch.  Returns true if no watches remain.
    fn remove_watch(&mut self, id: WatchId) -> bool;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

struct ProducerEntry<P: Producer> {
    producer: P,
    watches: HashSet<WatchId>,
}

impl<P: Producer> ErasedProducer for ProducerEntry<P> {
    fn subchannel_state(&mut self, state: &SubchannelState) {
        self.producer.subchannel_state(state);
    }

    fn remove_watch(&mut self, id: WatchId) -> bool {
        if self.watches.remove(&id) {
            self.producer.remove_watch(id);
        }
        self.watches.is_empty()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ProducerSet {
    pub(crate) fn new(
        state: SubchannelState,
        invoker: ArcInvoker,
        address: Address,
        runtime: GrpcRuntime,
        health_service_name: watch::Receiver<Option<String>>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProducerSetInner {
                state,
                producers: HashMap::new(),
                next_id: 0,
                invoker,
                address,
                runtime,
                health_service_name,
            })),
        }
    }

    /// Records the subchannel's new state and passes it to every producer.
    pub(crate) fn subchannel_state(&self, state: &SubchannelState) {
        let mut inner = self.inner.lock().unwrap();
        inner.state = state.clone();
        for producer in inner.producers.values_mut() {
            producer.subchannel_state(state);
        }
    }

    /// Adds a watch for `P`, creating the producer if needed.
    pub(crate) fn watch<P: Producer>(
        &self,
        params: P::Params,
        publisher: Publisher<P>,
    ) -> WatchHandle {
        let type_id = TypeId::of::<P>();
        let id = {
            let mut guard = self.inner.lock().unwrap();
            let inner = &mut *guard;
            let id = WatchId(inner.next_id);
            inner.next_id += 1;
            let entry = inner.producers.entry(type_id).or_insert_with(|| {
                Box::new(ProducerEntry {
                    producer: P::new(ProducerOptions {
                        subchannel: inner.invoker.clone(),
                        state: inner.state.clone(),
                        address: inner.address.clone(),
                        runtime: inner.runtime.clone(),
                        health_service_name: inner.health_service_name.clone(),
                    }),
                    watches: HashSet::new(),
                })
            });
            let entry = entry
                .as_any_mut()
                .downcast_mut::<ProducerEntry<P>>()
                .expect("producer map is keyed by producer type");
            entry.watches.insert(id);
            entry.producer.add_watch(id, params, publisher);
            id
        };

        // Hold the set weakly, so that a watch doesn't keep the subchannel's
        // producers (or the subchannel) alive.
        let weak = Arc::downgrade(&self.inner);
        WatchHandle::new(move || {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let removed = {
                let mut inner = inner.lock().unwrap();
                let empty = inner
                    .producers
                    .get_mut(&type_id)
                    .is_some_and(|entry| entry.remove_watch(id));
                if empty {
                    inner.producers.remove(&type_id)
                } else {
                    None
                }
            };
            // Drop the producer outside the lock, in case its Drop does
            // anything that needs it.
            drop(removed);
        })
    }
}

impl Debug for ProducerSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerSet").finish_non_exhaustive()
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;

    /// Returns a WatchId for tests that call a producer directly.
    pub(crate) fn watch_id(id: u64) -> WatchId {
        WatchId(id)
    }

    /// Creates a Publisher tagged with `subchannel`, for tests that call a
    /// producer directly.
    pub(crate) fn new_publisher<P: Producer>(
        subchannel: &Arc<dyn Subchannel>,
        work_scheduler: Arc<dyn WorkScheduler>,
    ) -> Publisher<P> {
        Publisher::new(Arc::downgrade(subchannel), work_scheduler)
    }

    /// Creates a ProducerSet for tests.
    pub(crate) fn new_producer_set(
        state: SubchannelState,
        invoker: ArcInvoker,
        health_service_name: watch::Receiver<Option<String>>,
    ) -> ProducerSet {
        ProducerSet::new(
            state,
            invoker,
            Address::default(),
            crate::rt::default_runtime(),
            health_service_name,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::hash::Hash;
    use std::sync::mpsc;

    use super::*;
    use crate::client::CallOptions;
    use crate::client::Invoke;
    use crate::client::RequestHeaders;
    use crate::client::boxed::BoxRecvStream;
    use crate::client::boxed::BoxSendStream;
    use crate::client::load_balancing::subchannel::ForwardingSubchannel;
    use crate::client::load_balancing::test_utils::TestWorkScheduler;

    // An invoker for tests that are not expected to start RPCs.
    struct PanicInvoker;
    impl Invoke for PanicInvoker {
        type SendStream = BoxSendStream;
        type RecvStream = BoxRecvStream;
        async fn invoke(
            &self,
            _headers: RequestHeaders,
            _options: CallOptions,
        ) -> (BoxSendStream, BoxRecvStream) {
            panic!("unexpected RPC");
        }
    }

    // A subchannel that hosts a ProducerSet, like InternalSubchannel.
    struct HostSubchannel {
        producers: ProducerSet,
    }

    impl HostSubchannel {
        fn new_arc() -> (Arc<dyn Subchannel>, ProducerSet) {
            let (_tx, rx) = watch::channel(None);
            let producers = test_utils::new_producer_set(
                SubchannelState::idle(),
                ArcInvoker::new(PanicInvoker),
                rx,
            );
            (
                Arc::new(HostSubchannel {
                    producers: producers.clone(),
                }),
                producers,
            )
        }
    }

    impl ForwardingSubchannel for HostSubchannel {
        fn delegate(&self) -> &Arc<dyn Subchannel> {
            panic!("no delegate")
        }
        fn address(&self) -> Address {
            Address::default()
        }
        fn get_attribute_dyn(&self, id: TypeId) -> Option<&dyn Any> {
            (id == TypeId::of::<ProducerSet>()).then_some(&self.producers as &dyn Any)
        }
        fn connect(&self) {}
    }

    impl PartialEq for HostSubchannel {
        fn eq(&self, other: &Self) -> bool {
            std::ptr::eq(self, other)
        }
    }
    impl Eq for HostSubchannel {}
    impl Hash for HostSubchannel {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            (self as *const Self).hash(state);
        }
    }

    // A wrapper that may intercept watches for CountingProducer.
    struct Wrapper {
        delegate: Arc<dyn Subchannel>,
        intercept: Option<ProducerWatch<CountingProducer>>,
    }

    impl ForwardingSubchannel for Wrapper {
        fn delegate(&self) -> &Arc<dyn Subchannel> {
            &self.delegate
        }
        fn get_attribute_dyn(&self, id: TypeId) -> Option<&dyn Any> {
            if id == TypeId::of::<ProducerWatch<CountingProducer>>()
                && let Some(intercept) = &self.intercept
            {
                return Some(intercept);
            }
            self.delegate.get_attribute_dyn(id)
        }
    }

    impl PartialEq for Wrapper {
        fn eq(&self, other: &Self) -> bool {
            std::ptr::eq(self, other)
        }
    }
    impl Eq for Wrapper {}
    impl Hash for Wrapper {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            (self as *const Self).hash(state);
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Event {
        New(u32),
        State(crate::client::ConnectivityState),
        Add(WatchId, u32),
        Remove(WatchId),
        Dropped,
    }

    static EVENTS: Mutex<Option<mpsc::Sender<Event>>> = Mutex::new(None);

    fn record(event: Event) {
        if let Some(tx) = EVENTS.lock().unwrap().as_ref() {
            let _ = tx.send(event);
        }
    }

    // A producer that records its callbacks, and sends each watch its params
    // plus the number of watches.
    struct CountingProducer {
        publishers: HashMap<WatchId, Publisher<CountingProducer>>,
    }

    impl Producer for CountingProducer {
        type Data = u32;
        type Params = u32;

        fn new(_options: ProducerOptions) -> Self {
            record(Event::New(0));
            Self {
                publishers: HashMap::new(),
            }
        }

        fn subchannel_state(&mut self, state: &SubchannelState) {
            record(Event::State(state.connectivity_state));
            for p in self.publishers.values() {
                p.send(100);
            }
        }

        fn add_watch(&mut self, id: WatchId, params: u32, publisher: Publisher<Self>) {
            record(Event::Add(id, params));
            publisher.send(params);
            self.publishers.insert(id, publisher);
        }

        fn remove_watch(&mut self, id: WatchId) {
            record(Event::Remove(id));
            self.publishers.remove(&id);
        }
    }

    impl Drop for CountingProducer {
        fn drop(&mut self) {
            record(Event::Dropped);
        }
    }

    // Tests share the EVENTS global, so they hold this lock.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn setup() -> (std::sync::MutexGuard<'static, ()>, mpsc::Receiver<Event>) {
        let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (tx, rx) = mpsc::channel();
        *EVENTS.lock().unwrap() = Some(tx);
        (guard, rx)
    }

    fn work_scheduler() -> (Arc<dyn WorkScheduler>, mpsc::Receiver<Option<WorkData>>) {
        let (tx_work, rx_work) = mpsc::channel();
        (Arc::new(TestWorkScheduler { tx_work }), rx_work)
    }

    fn recv_update(rx: &mpsc::Receiver<Option<WorkData>>) -> WatchUpdate<CountingProducer> {
        let data = rx
            .try_recv()
            .expect("expected work")
            .expect("expected data");
        WatchUpdate::<CountingProducer>::from_work_data(data).expect("expected a WatchUpdate")
    }

    #[test]
    fn producer_lifecycle() {
        let (_guard, events) = setup();
        let (sc, producers) = HostSubchannel::new_arc();
        let (ws1, rx1) = work_scheduler();
        let (ws2, rx2) = work_scheduler();

        let h1 = sc.watch::<CountingProducer>(1, ws1);
        assert_eq!(events.try_recv().unwrap(), Event::New(0));
        let Event::Add(id1, 1) = events.try_recv().unwrap() else {
            panic!("expected Add");
        };
        let u = recv_update(&rx1);
        assert!(Arc::ptr_eq(&u.subchannel, &sc));
        assert_eq!(u.data, 1);

        // A second watch shares the producer.
        let h2 = sc.watch::<CountingProducer>(2, ws2);
        let Event::Add(id2, 2) = events.try_recv().unwrap() else {
            panic!("expected Add");
        };
        assert_ne!(id1, id2);
        assert_eq!(recv_update(&rx2).data, 2);

        // State changes reach the producer.
        producers.subchannel_state(&SubchannelState::connecting());
        assert_eq!(
            events.try_recv().unwrap(),
            Event::State(crate::client::ConnectivityState::Connecting)
        );
        assert_eq!(recv_update(&rx1).data, 100);
        assert_eq!(recv_update(&rx2).data, 100);

        // Ending one watch removes it; the producer stays.
        drop(h1);
        assert_eq!(events.try_recv().unwrap(), Event::Remove(id1));
        assert!(events.try_recv().is_err());

        // Ending the last watch removes it and drops the producer.
        drop(h2);
        assert_eq!(events.try_recv().unwrap(), Event::Remove(id2));
        assert_eq!(events.try_recv().unwrap(), Event::Dropped);

        // A new watch creates a new producer that starts from the current
        // state.
        let (ws3, _rx3) = work_scheduler();
        let _h3 = sc.watch::<CountingProducer>(3, ws3);
        assert_eq!(events.try_recv().unwrap(), Event::New(0));
    }

    #[test]
    fn publisher_closed_after_handle_dropped() {
        let (_guard, _events) = setup();
        let (sc, producers) = HostSubchannel::new_arc();
        let (ws, rx) = work_scheduler();

        // Keep a second watch so the producer survives.
        let (ws_other, _rx_other) = work_scheduler();
        let _other = sc.watch::<CountingProducer>(0, ws_other);

        let h = sc.watch::<CountingProducer>(1, ws);
        assert_eq!(recv_update(&rx).data, 1);
        drop(h);
        producers.subchannel_state(&SubchannelState::ready());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn delivery_stops_when_subchannel_dropped() {
        let (_guard, _events) = setup();
        let (sc, producers) = HostSubchannel::new_arc();
        let wrapper: Arc<dyn Subchannel> = Arc::new(Wrapper {
            delegate: sc.clone(),
            intercept: None,
        });
        let (ws, rx) = work_scheduler();
        let _h = wrapper.watch::<CountingProducer>(1, ws);
        // Updates are tagged with the wrapper the watch was called on.
        assert!(Arc::ptr_eq(&recv_update(&rx).subchannel, &wrapper));

        drop(wrapper);
        producers.subchannel_state(&SubchannelState::ready());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn handle_does_not_keep_producers_alive() {
        let (_guard, events) = setup();
        let (sc, producers) = HostSubchannel::new_arc();
        let (ws, rx) = work_scheduler();
        let h = sc.watch::<CountingProducer>(1, ws);
        assert_eq!(events.try_recv().unwrap(), Event::New(0));
        assert!(matches!(events.try_recv().unwrap(), Event::Add(..)));
        // Release the subchannel reference held by the queued update.
        recv_update(&rx);

        // Dropping the subchannel (and so its ProducerSet) drops the producer
        // even though the handle is still alive.
        drop(sc);
        drop(producers);
        assert_eq!(events.try_recv().unwrap(), Event::Dropped);
        drop(h);
        assert!(events.try_recv().is_err());
    }

    struct Interceptor {
        delegate: Arc<dyn Subchannel>,
        seen: Mutex<Vec<(u32, Publisher<CountingProducer>)>>,
    }

    impl WatchInterceptor<CountingProducer> for Interceptor {
        fn watch(&self, params: u32, publisher: Publisher<CountingProducer>) -> WatchHandle {
            publisher.send(params + 1000);
            self.seen.lock().unwrap().push((params, publisher));
            WatchHandle::new(|| record(Event::Remove(WatchId(u64::MAX))))
        }
    }

    #[test]
    fn interceptor_handles_watch() {
        let (_guard, events) = setup();
        let (sc, _producers) = HostSubchannel::new_arc();
        let interceptor = Arc::new(Interceptor {
            delegate: sc.clone(),
            seen: Mutex::new(Vec::new()),
        });
        let wrapper: Arc<dyn Subchannel> = Arc::new(Wrapper {
            delegate: sc.clone(),
            intercept: Some(ProducerWatch::new(interceptor.clone())),
        });

        let (ws, rx) = work_scheduler();
        let h = wrapper.watch::<CountingProducer>(5, ws);
        // The interceptor handled it; no producer was created.
        assert!(events.try_recv().is_err());
        let u = recv_update(&rx);
        assert!(Arc::ptr_eq(&u.subchannel, &wrapper));
        assert_eq!(u.data, 1005);

        // The interceptor can watch the real producer below itself.
        let (ws_real, rx_real) = work_scheduler();
        let _real = interceptor.delegate.watch::<CountingProducer>(7, ws_real);
        assert_eq!(events.try_recv().unwrap(), Event::New(0));
        assert!(matches!(events.try_recv().unwrap(), Event::Add(_, 7)));
        assert_eq!(recv_update(&rx_real).data, 7);

        // Dropping the handle closes the publisher, then ends the watch.
        let publisher = interceptor.seen.lock().unwrap()[0].1.clone();
        drop(h);
        assert!(publisher.is_closed());
        assert_eq!(events.try_recv().unwrap(), Event::Remove(WatchId(u64::MAX)));
        publisher.send(1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn watch_without_host_delivers_nothing() {
        let (_guard, _events) = setup();
        let sc: Arc<dyn Subchannel> = Arc::new(
            crate::client::load_balancing::test_utils::TestSubchannel::new(
                Address::default(),
                mpsc::channel().0,
            ),
        );
        let (ws, rx) = work_scheduler();
        let _h = sc.watch::<CountingProducer>(1, ws);
        assert!(rx.try_recv().is_err());
    }
}
