/*
 *
 * Copyright 2025 gRPC authors.
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

use std::error::Error;
use std::future::Future;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Instant;

use bytes::Buf;
use bytes::BufMut as _;
use bytes::Bytes;
use http::Request as HttpRequest;
use http::Response as HttpResponse;
use http::Uri;
use http::uri::PathAndQuery;
use hyper::client::conn::http2::Builder;
use hyper::client::conn::http2::SendRequest;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::CancellationHandle;
use tonic::Code;
use tonic::Request as TonicRequest;
use tonic::Status as TonicStatus;
use tonic::Streaming;
use tonic::body::Body;
use tonic::client::Grpc;
use tonic::client::GrpcService;
use tonic::codec::Codec;
use tonic::codec::Decoder;
use tonic::codec::EncodeBuf;
use tonic::codec::Encoder;
use tonic::metadata::MetadataMap as TonicMeta;
use tower::ServiceBuilder;
use tower::buffer::Buffer;
use tower::buffer::future::ResponseFuture as BufferResponseFuture;
use tower::limit::ConcurrencyLimitLayer;
use tower::limit::RateLimitLayer;
use tower::util::BoxService;
use tower_service::Service as TowerService;

use crate::StatusCodeError;
use crate::StatusError;
use crate::attributes::Attributes;
use crate::byte_str::ByteStr;
use crate::client::CallOptions;
use crate::client::Invoke;
use crate::client::RecvStream;
use crate::client::RequestHeaders;
use crate::client::ResponseHeaders;
use crate::client::ResponseStreamItem;
use crate::client::SendOptions;
use crate::client::SendStream;
use crate::client::Trailers;
use crate::client::name_resolution::TCP_IP_NETWORK_TYPE;
use crate::client::name_resolution::UNIX_NETWORK_TYPE;
use crate::client::transport::SecurityOpts;
use crate::client::transport::Transport;
use crate::client::transport::TransportOptions;
use crate::client::transport::registry::GLOBAL_TRANSPORT_REGISTRY;
use crate::core::Address;
use crate::core::ConnectionInfo;
use crate::core::RecvMessage;
use crate::core::SendMessage;
use crate::core::StreamEnded;
use crate::private;
use crate::rt::BoxedTaskHandle;
use crate::rt::GrpcRuntime;
use crate::rt::Sleep;
use crate::rt::TcpOptions;
use crate::rt::UnixSocketOptions;
use crate::rt::hyper_wrapper::HyperCompatExec;
use crate::rt::hyper_wrapper::HyperCompatTimer;
use crate::rt::hyper_wrapper::HyperStream;

#[cfg(test)]
mod test;

const DEFAULT_BUFFER_SIZE: usize = 1024;

type BoxError = Box<dyn Error + Send + Sync>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, TonicStatus>> + Send>>;
type BoxBuf = Box<dyn Buf + Send + Sync>;

pub(crate) fn reg() {
    GLOBAL_TRANSPORT_REGISTRY.add_transport(
        TCP_IP_NETWORK_TYPE,
        TransportBuilder {
            network_type: NetworkType::Tcp,
        },
    );
    GLOBAL_TRANSPORT_REGISTRY.add_transport(
        UNIX_NETWORK_TYPE,
        TransportBuilder {
            network_type: NetworkType::Unix,
        },
    );
}

#[derive(Debug, Copy, Clone)]
enum NetworkType {
    Tcp,
    Unix,
}

struct TransportBuilder {
    network_type: NetworkType,
}

struct TonicTransport {
    grpc: Grpc<TonicService>,
    task_handle: BoxedTaskHandle,
    runtime: GrpcRuntime,
    connection_info: ConnectionInfo,
}

impl Drop for TonicTransport {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl Invoke for TonicTransport {
    type SendStream = TonicSendStream;
    type RecvStream = TonicRecvStream;

    async fn invoke(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
    ) -> (Self::SendStream, Self::RecvStream) {
        let (req_tx, req_rx) = mpsc::channel(1);
        let request_stream = ReceiverStream::new(req_rx);
        let mut request = TonicRequest::new(Box::pin(request_stream));
        let (method, metadata) = headers.into_parts();
        *request.metadata_mut() = metadata.into();

        let cancel_tx = request.cancellation_handle();

        let Ok(path) = PathAndQuery::from_maybe_shared(method) else {
            return self
                .local_err_streams(StatusError::new(StatusCodeError::Internal, "invalid path"));
        };

        // If a deadline is set, start a timer that will fire when it is
        // reached.  The timer bounds both the wait for the transport to become
        // ready and the lifetime of the stream itself.
        let deadline = options.deadline();
        let mut deadline_timer = None;
        if let Some(deadline) = deadline {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return self.local_err_streams(deadline_exceeded_error());
            }
            deadline_timer = Some(self.runtime.sleep(timeout));
        }

        let mut grpc = self.grpc.clone();
        let ready = match deadline_timer.as_mut() {
            Some(timer) => race_deadline(timer, grpc.ready()).await,
            None => Some(grpc.ready().await),
        };
        match ready {
            None => return self.local_err_streams(deadline_exceeded_error()),
            Some(Err(e)) => {
                return self.local_err_streams(StatusError::new(
                    StatusCodeError::Unavailable,
                    format!("Service was not ready: {e}"),
                ));
            }
            Some(Ok(())) => {}
        }

        // Propagate the remaining time to the server via the `grpc-timeout`
        // header.  This is computed after waiting for readiness so that it
        // reflects the time actually remaining when the request is sent.
        if let Some(deadline) = deadline {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return self.local_err_streams(deadline_exceeded_error());
            }
            request.set_timeout(timeout);
        }

        // If there is a deadline, the spawned task below also enforces it.
        // The task notifies the TonicRecvStream through `deadline_rx` when the
        // deadline fires.
        let mut deadline_rx = None;
        let enforcer = deadline_timer.map(|timer| {
            let (fired_tx, fired_rx) = oneshot::channel();
            deadline_rx = Some(fired_rx);
            let body_slot = ResponseBodySlot::default();
            // TonicService::call picks this up and routes the response body
            // through it, so the enforcer can tear the body down.
            request.extensions_mut().insert(body_slot.clone());
            DeadlineEnforcer {
                timer,
                fired_tx,
                cancel: cancel_tx.clone(),
                body_slot,
            }
        });

        // Note that Tonic's streaming call blocks until the server's headers
        // are received.  The client needs a SendStream to provide the request
        // message(s), which the server may be awaiting before sending its
        // headers.  So, we spawn a task for this period of time, and then we
        // send the response (headers, stream) to the TonicRecvStream when it is
        // available.
        //
        // If the call has a deadline, the same task then stays alive until the
        // deadline is reached or the call ends, whichever happens first.  That
        // way the stream is cancelled as soon as the deadline passes, even if
        // the application is not inside `recv` at the time.
        let (resp_tx, resp_rx) = oneshot::channel();
        self.runtime.spawn(Box::pin(async move {
            // Start the RPC future and poll it directly or inside the enforcer
            // as needed.
            let response = grpc.streaming(request, path, BufCodec {});
            match enforcer {
                None => {
                    let _ = resp_tx.send(response.await);
                }
                Some(enforcer) => enforcer.run(response, resp_tx).await,
            }
        }));

        (
            TonicSendStream { sender: Ok(req_tx) },
            TonicRecvStream {
                state: StreamState::AwaitingHeaders(resp_rx),
                cancel_tx: Some(cancel_tx),
                connection_info: Some(self.connection_info.clone()),
                deadline_rx,
            },
        )
    }
}

type ResponseResult = Result<tonic::Response<Streaming<Bytes>>, TonicStatus>;

/// Enforces a call's deadline from the call's background task.
///
/// When the deadline fires, the enforcer notifies the [`TonicRecvStream`] and
/// then resets the HTTP/2 stream.  It always resets it, whatever the call
/// state:
///
/// * Before response headers, it drops the in-flight response future.  Hyper
///   resets the stream when that happens.
/// * While the request body is still open, it cancels the request through
///   tonic's cancellation token, which also resets the stream.
/// * After response headers, it drops the response body via the
///   [`ResponseBodySlot`].  Once the request body has finished, that releases
///   the last handle to the h2 stream, and h2 resets the stream.
struct DeadlineEnforcer {
    timer: Pin<Box<dyn Sleep>>,
    /// Signals the `TonicRecvStream` that the deadline fired.  The
    /// `TonicRecvStream` drops the receiver when the call ends, which tells
    /// the enforcer to stop.
    fired_tx: oneshot::Sender<()>,
    cancel: CancellationHandle,
    body_slot: ResponseBodySlot,
}

impl DeadlineEnforcer {
    async fn run(
        mut self,
        response: impl Future<Output = ResponseResult>,
        resp_tx: oneshot::Sender<ResponseResult>,
    ) {
        // Drive `response` until it completes or the `TonicRecvStream` is
        // dropped.  `race_deadline` consumes this future, so if the deadline
        // fires first, `response` is dropped before `expire` runs.
        let response_or_closed = async {
            let mut response = pin!(response);
            let mut closed = pin!(self.fired_tx.closed());
            poll_fn(|cx| {
                if let Poll::Ready(res) = response.as_mut().poll(cx) {
                    return Poll::Ready(Some(res));
                }
                closed.as_mut().poll(cx).map(|()| None)
            })
            .await
        };
        match race_deadline(&mut self.timer, response_or_closed).await {
            None => return self.expire(),
            Some(None) => return,
            Some(Some(response)) => {
                let is_ok = response.is_ok();
                if resp_tx.send(response).is_err() || !is_ok {
                    // The TonicRecvStream is gone or the call has already
                    // ended (Trailers-Only response).
                    return;
                }
            }
        }
        // Wait for the deadline, or for the TonicRecvStream to finish the call
        // (it drops the receiver).
        if race_deadline(&mut self.timer, self.fired_tx.closed())
            .await
            .is_none()
        {
            self.expire();
        }
    }

    fn expire(self) {
        // Notify the TonicRecvStream before tearing anything down.  That way,
        // if it sees an error caused by the teardown, it can tell the error
        // came from the deadline.
        let _ = self.fired_tx.send(());
        self.cancel.cancel();
        self.body_slot.kill();
    }
}

/// A call's response body, held in shared ownership so that
/// [`DeadlineEnforcer`] can drop it (and so reset the stream) without waiting
/// for the application to call `recv`.
#[derive(Clone, Default)]
struct ResponseBodySlot(Arc<Mutex<BodySlotState>>);

#[derive(Default)]
enum BodySlotState {
    /// The response has not arrived yet.
    #[default]
    Empty,
    Attached(Body),
    /// The deadline fired; any body has been dropped.
    Killed,
}

impl ResponseBodySlot {
    fn lock(&self) -> MutexGuard<'_, BodySlotState> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores `body` in the slot and returns a body that reads from it.
    fn attach(&self, body: Body) -> Body {
        {
            let mut state = self.lock();
            if !matches!(*state, BodySlotState::Killed) {
                *state = BodySlotState::Attached(body);
            }
        }
        Body::new(SlotBody(self.clone()))
    }

    /// Drops the body, if any, and fails any future reads.
    fn kill(&self) {
        let old = std::mem::replace(&mut *self.lock(), BodySlotState::Killed);
        // Drop the body (which may send RST_STREAM) outside the lock.
        drop(old);
    }
}

/// The response body handed to tonic.  It reads from a [`ResponseBodySlot`].
struct SlotBody(ResponseBodySlot);

impl http_body::Body for SlotBody {
    type Data = Bytes;
    type Error = TonicStatus;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match &mut *self.0.lock() {
            BodySlotState::Attached(body) => Pin::new(body).poll_frame(cx),
            BodySlotState::Empty | BodySlotState::Killed => Poll::Ready(Some(Err(
                TonicStatus::deadline_exceeded(deadline_exceeded_error().message()),
            ))),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &*self.0.lock() {
            BodySlotState::Attached(body) => body.is_end_stream(),
            // Not at end-of-stream: the next poll_frame reports the error.
            BodySlotState::Empty | BodySlotState::Killed => false,
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match &*self.0.lock() {
            BodySlotState::Attached(body) => body.size_hint(),
            BodySlotState::Empty | BodySlotState::Killed => http_body::SizeHint::default(),
        }
    }
}

impl TonicTransport {
    /// Creates a send/recv stream pair representing locally-produced errors.
    fn local_err_streams(&self, status: StatusError) -> (TonicSendStream, TonicRecvStream) {
        (
            TonicSendStream { sender: Err(()) },
            TonicRecvStream {
                state: StreamState::LocalError(status),
                cancel_tx: None,
                connection_info: Some(self.connection_info.clone()),
                deadline_rx: None,
            },
        )
    }
}

/// Returns the status used when an RPC's deadline is exceeded.
fn deadline_exceeded_error() -> StatusError {
    StatusError::new(StatusCodeError::DeadlineExceeded, "Deadline exceeded")
}

/// Drives `fut` to completion unless `timer` fires first.
///
/// Returns `None` if the timer fired before `fut` completed.  `fut` is always
/// polled before `timer`, so a result that is already available wins over an
/// expired timer.
async fn race_deadline<F: Future>(timer: &mut Pin<Box<dyn Sleep>>, fut: F) -> Option<F::Output> {
    let mut fut = pin!(fut);
    poll_fn(|cx| {
        if let Poll::Ready(output) = fut.as_mut().poll(cx) {
            return Poll::Ready(Some(output));
        }
        timer.as_mut().poll(cx).map(|()| None)
    })
    .await
}

struct TonicSendStream {
    sender: Result<mpsc::Sender<BoxBuf>, ()>,
}

impl SendStream for TonicSendStream {
    async fn send(
        &mut self,
        msg: &dyn SendMessage,
        options: SendOptions,
    ) -> Result<(), StreamEnded> {
        if let Ok(tx) = &self.sender
            && let Ok(buf) = msg.encode()
            && tx.send(buf).await.is_ok()
        {
            if options.final_msg {
                self.sender = Err(());
            }
            return Ok(());
        }
        Err(StreamEnded)
    }
}

struct TonicRecvStream {
    state: StreamState,
    cancel_tx: Option<CancellationHandle>,
    connection_info: Option<ConnectionInfo>,
    /// Resolves when the call's [`DeadlineEnforcer`] fires.  Present only
    /// while the call is live and has a deadline.  Dropping it tells the
    /// enforcer to stop.
    deadline_rx: Option<oneshot::Receiver<()>>,
}

impl TonicRecvStream {
    // Converts from a tonic status to a trailers stream item.
    fn trailers_from_tonic_status(
        &mut self,
        status: &TonicStatus,
        mut md: TonicMeta,
    ) -> ResponseStreamItem {
        if !status.details().is_empty() {
            md.insert_bin(
                "grpc-status-details-bin",
                tonic::metadata::MetadataValue::from_bytes(status.details()),
            );
        }
        let status_res = match status.code() {
            Code::Ok => Ok(()),
            code => Err(StatusError::new(
                StatusCodeError::from(code as i32),
                status.message(),
            )),
        };
        self.trailers_from_grpc_result(status_res, Some(&md))
    }

    // Builds a trailers stream item with a status.
    fn trailers_from_grpc_result(
        &mut self,
        status: crate::Result<()>,
        md: Option<&TonicMeta>,
    ) -> ResponseStreamItem {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            cancel_tx.cancel();
        }
        let trailers = if let Some(md) = md {
            match md.try_into() {
                Err(e) => Trailers::new(Err(StatusError::new(
                    StatusCodeError::Internal,
                    format!("failed to parse metadata: {e}"),
                ))),
                Ok(metadata) => Trailers::new(status).with_metadata(metadata),
            }
        } else {
            Trailers::new(status)
        };
        ResponseStreamItem::Trailers(trailers.with_connection_info(self.connection_info.take()))
    }
}

enum StreamState {
    LocalError(StatusError),
    AwaitingHeaders(oneshot::Receiver<Result<tonic::Response<Streaming<Bytes>>, TonicStatus>>),
    Streaming(Streaming<Bytes>),
    Closed,
}

impl RecvStream for TonicRecvStream {
    async fn recv(&mut self, msg: &mut dyn RecvMessage) -> ResponseStreamItem {
        let Some(mut deadline_rx) = self.deadline_rx.take() else {
            return self.recv_inner(msg).await;
        };
        let item = {
            let mut recv = pin!(self.recv_inner(msg));
            let mut deadline_live = true;
            poll_fn(|cx| {
                if deadline_live {
                    match Pin::new(&mut deadline_rx).poll(cx) {
                        Poll::Ready(Ok(())) => return Poll::Ready(None),
                        // The enforcer is gone without firing (e.g. the call
                        // ended), so the deadline can no longer fire.
                        Poll::Ready(Err(_)) => deadline_live = false,
                        Poll::Pending => {}
                    }
                }
                if let Poll::Ready(item) = recv.as_mut().poll(cx) {
                    return Poll::Ready(Some(item));
                }
                Poll::Pending
            })
            .await
        };

        match item {
            // The enforcer signals before it tears the stream down, so if
            // `recv` failed because the enforcer fired between polling
            // `deadline_rx` and `recv`, the signal is already visible here.
            // Report the deadline in that case while preserving any
            // `connection_info` already taken by `recv_inner`.
            Some(ResponseStreamItem::Trailers(trailers))
                if trailers.status().is_err() && deadline_rx.try_recv().is_ok() =>
            {
                ResponseStreamItem::Trailers(
                    Trailers::new(Err(deadline_exceeded_error()))
                        .with_connection_info(trailers.connection_info().clone()),
                )
            }
            Some(item) => {
                // Keep watching the deadline only while the stream is live.
                if !matches!(
                    item,
                    ResponseStreamItem::Trailers(_) | ResponseStreamItem::StreamClosed
                ) {
                    self.deadline_rx = Some(deadline_rx);
                }
                item
            }
            None => {
                // The enforcer has already reset the stream; drop whatever
                // state remains.
                self.state = StreamState::Closed;
                self.trailers_from_grpc_result(Err(deadline_exceeded_error()), None)
            }
        }
    }
}

impl TonicRecvStream {
    async fn recv_inner(&mut self, msg: &mut dyn RecvMessage) -> ResponseStreamItem {
        // Take the current state, leaving `Closed` in its place temporarily
        let state = std::mem::replace(&mut self.state, StreamState::Closed);

        match state {
            // Closed is terminal.
            StreamState::Closed => ResponseStreamItem::StreamClosed,
            // Stay closed after sending trailers (do not set self.state).
            StreamState::LocalError(error) => self.trailers_from_grpc_result(Err(error), None),
            StreamState::AwaitingHeaders(rx) => match rx.await {
                Ok(Ok(response)) => {
                    let (metadata, stream, _extensions) = response.into_parts();
                    // Tonic decodes base64-encoded binary headers lazily. It
                    // does not fail the RPC upon receiving invalid base64 data;
                    // the error only surfaces when the application attempts to
                    // read the metadata.
                    // In contrast, standard gRPC implementations eagerly decode
                    // these headers and immediately fail the RPC with an
                    // Internal status.
                    match (&metadata).try_into() {
                        Ok(md) => {
                            // Start streaming and return the headers.
                            self.state = StreamState::Streaming(stream);
                            let Some(connection_info) = self.connection_info.take() else {
                                return self.trailers_from_grpc_result(
                                    Err(StatusError::new(
                                        StatusCodeError::Internal,
                                        "required connection info missing",
                                    )),
                                    None,
                                );
                            };
                            let headers = ResponseHeaders::new(connection_info).with_metadata(md);
                            ResponseStreamItem::Headers(headers)
                        }
                        Err(e) => self.trailers_from_grpc_result(
                            Err(StatusError::new(
                                StatusCodeError::Internal,
                                format!("error decoding response: {e}"),
                            )),
                            None,
                        ),
                    }
                }
                Err(_) => {
                    // Stay closed after sending trailers (do not set self.state).
                    self.trailers_from_grpc_result(
                        Err(StatusError::new(StatusCodeError::Unknown, "Task cancelled")),
                        None,
                    )
                }
                Ok(Err(mut status)) => {
                    // In a Trailers-only response, the tonic status contains
                    // the metadata.
                    // Stay closed after sending trailers (do not set self.state).
                    let md = std::mem::take(status.metadata_mut());
                    self.trailers_from_tonic_status(&status, md)
                }
            },
            StreamState::Streaming(mut stream) => match stream.message().await {
                Ok(Some(mut buf)) => match msg.decode(&mut buf) {
                    Ok(()) => {
                        // More messages may remain in the stream; set receiver
                        // again.
                        self.state = StreamState::Streaming(stream);
                        ResponseStreamItem::Message
                    }
                    Err(e) => self.trailers_from_grpc_result(
                        Err(StatusError::new(
                            StatusCodeError::Internal,
                            format!("error decoding response: {e}"),
                        )),
                        None,
                    ),
                },
                Err(status) => {
                    // Stay closed after sending trailers (do not set self.state).
                    let trailers = stream.trailers().await;
                    let md = trailers.unwrap_or_default().unwrap_or_default();
                    self.trailers_from_tonic_status(&status, md)
                }
                Ok(None) => {
                    // Stay closed after sending trailers (do not set self.state).
                    let trailers = stream.trailers().await;
                    let md = trailers.unwrap_or_default().unwrap_or_default();
                    self.trailers_from_grpc_result(Ok(()), Some(&md))
                }
            },
        }
    }
}

impl Drop for TonicRecvStream {
    fn drop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            cancel_tx.cancel();
        }
    }
}

impl Transport for TransportBuilder {
    type Service = TonicTransport;

    async fn connect(
        &self,
        address: &Address,
        runtime: GrpcRuntime,
        security_info: &SecurityOpts,
        opts: &TransportOptions,
    ) -> Result<
        (
            Self::Service,
            ConnectionInfo,
            oneshot::Receiver<Result<(), String>>,
        ),
        String,
    > {
        let runtime = runtime.clone();
        let mut settings = Builder::<HyperCompatExec>::new(HyperCompatExec {
            inner: runtime.clone(),
        })
        .timer(HyperCompatTimer {
            inner: runtime.clone(),
        })
        .initial_stream_window_size(opts.init_stream_window_size)
        .initial_connection_window_size(opts.init_connection_window_size)
        .adaptive_window(opts.http2_adaptive_window)
        .keep_alive_interval(opts.http2_keep_alive_interval)
        .clone();

        if let Some(val) = opts.http2_keep_alive_timeout {
            settings.keep_alive_timeout(val);
        }

        if let Some(val) = opts.http2_keep_alive_while_idle {
            settings.keep_alive_while_idle(val);
        }

        if let Some(val) = opts.http2_max_header_list_size {
            settings.max_header_list_size(val);
        }

        let transport_fut = match self.network_type {
            NetworkType::Tcp => {
                let addr: SocketAddr =
                    SocketAddr::from_str(&address.address).map_err(|err| err.to_string())?;
                runtime.tcp_stream(
                    addr,
                    TcpOptions {
                        enable_nodelay: opts.tcp_nodelay,
                        keepalive: opts.tcp_keepalive,
                    },
                )
            }
            NetworkType::Unix => runtime.unix_stream(
                PathBuf::from(&*address.address),
                UnixSocketOptions::default(),
            ),
        };
        let transport = transport_fut.await?;
        let credentials = &security_info.credentials;
        let handshake_ouput = credentials
            .connect(
                &security_info.authority,
                transport,
                &security_info.handshake_info,
                &runtime,
                private::Internal,
            )
            .await?;

        let local_address = Address {
            network_type: handshake_ouput.endpoint.get_network_type(),
            address: ByteStr::from(handshake_ouput.endpoint.get_local_address().to_string()),
            attributes: Attributes::new(),
        };
        let remote_address = Address {
            network_type: handshake_ouput.endpoint.get_network_type(),
            address: ByteStr::from(handshake_ouput.endpoint.get_peer_address().to_string()),
            attributes: Attributes::new(),
        };

        let transport = HyperStream::new(handshake_ouput.endpoint);

        let (sender, connection) = settings
            .handshake(transport)
            .await
            .map_err(|err| err.to_string())?;
        let (tx, rx) = oneshot::channel();

        let task_handle = runtime.spawn(Box::pin(async move {
            if let Err(err) = connection.await {
                let _ = tx.send(Err(err.to_string()));
            } else {
                let _ = tx.send(Ok(()));
            }
        }));
        let sender = SendRequestWrapper::from(sender);

        let service = ServiceBuilder::new()
            .option_layer(opts.concurrency_limit.map(ConcurrencyLimitLayer::new))
            .option_layer(opts.rate_limit.map(|(l, d)| RateLimitLayer::new(l, d)))
            .map_err(Into::<BoxError>::into)
            .service(sender);

        let service = BoxService::new(service);
        let (service, worker) = Buffer::pair(service, DEFAULT_BUFFER_SIZE);
        runtime.spawn(Box::pin(worker));
        let authority = &security_info.authority.host_port_string();
        let uri = Uri::from_maybe_shared(format!("http://{}", authority))
            .map_err(|e| format!("failed to create URL with authority {}: {}", authority, e))?;
        let grpc = Grpc::with_origin(TonicService { inner: service }, uri);

        let connection_info = ConnectionInfo::new(
            local_address,
            remote_address,
            handshake_ouput.security_info.clone(),
        );

        let service = TonicTransport {
            grpc,
            task_handle,
            runtime,
            connection_info: connection_info.clone(),
        };
        Ok((service, connection_info, rx))
    }
}

struct SendRequestWrapper {
    inner: SendRequest<Body>,
}

impl From<SendRequest<Body>> for SendRequestWrapper {
    fn from(inner: SendRequest<Body>) -> Self {
        Self { inner }
    }
}

impl TowerService<HttpRequest<Body>> for SendRequestWrapper {
    type Response = HttpResponse<Body>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let fut = self.inner.send_request(req);
        Box::pin(async move { fut.await.map_err(Into::into).map(|res| res.map(Body::new)) })
    }
}

#[derive(Clone)]
struct TonicService {
    inner: Buffer<http::Request<Body>, BoxFuture<'static, Result<http::Response<Body>, BoxError>>>,
}

impl GrpcService<Body> for TonicService {
    type ResponseBody = Body;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let body_slot = request.extensions_mut().remove::<ResponseBodySlot>();
        ResponseFuture {
            inner: tower::Service::call(&mut self.inner, request),
            body_slot,
        }
    }
}

/// A future that resolves to an HTTP response.
///
/// This is returned by the `Service::call` on [`Channel`].
pub(crate) struct ResponseFuture {
    inner: BufferResponseFuture<BoxFuture<'static, Result<HttpResponse<Body>, BoxError>>>,
    /// If set, the response body is routed through this slot so the call's
    /// [`DeadlineEnforcer`] can drop it.
    body_slot: Option<ResponseBodySlot>,
}

impl Future for ResponseFuture {
    type Output = Result<http::Response<Body>, BoxError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let response = ready!(Pin::new(&mut self.inner).poll(cx))?;
        Poll::Ready(Ok(match self.body_slot.take() {
            Some(slot) => response.map(|body| slot.attach(body)),
            None => response,
        }))
    }
}

pub(crate) struct BufCodec {}

impl Codec for BufCodec {
    type Encode = BoxBuf;
    type Decode = Bytes;
    type Encoder = BufEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        BufEncoder {}
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesDecoder {}
    }
}

pub struct BufEncoder {}

impl Encoder for BufEncoder {
    type Item = BoxBuf;
    type Error = TonicStatus;

    fn encode(&mut self, mut item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put(&mut *item);
        Ok(())
    }
}

#[derive(Debug)]
pub struct BytesDecoder {}

impl Decoder for BytesDecoder {
    type Item = Bytes;
    type Error = TonicStatus;

    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}
