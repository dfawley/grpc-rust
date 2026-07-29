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

use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Buf;
use bytes::Bytes;
use grpc::StatusCodeError as GrpcStatusCodeError;
use grpc::StatusError as GrpcStatusError;
use grpc::client::CallOptions;
use grpc::client::Invoke;
use grpc::client::RecvStream;
use grpc::client::RequestHeaders;
use grpc::client::ResponseStreamItem;
use grpc::client::SendOptions;
use grpc::client::SendStream;
use grpc::client::Trailers;
use grpc::client::stream_util::RecvStreamValidator;
use grpc::core::RecvMessage;
use grpc::core::SendMessage;
use grpc::metadata::AsciiMetadataKey as GrpcAsciiMetadataKey;
use grpc::metadata::BinaryMetadataKey as GrpcBinaryMetadataKey;
use grpc::metadata::KeyAndValueRef as GrpcKeyAndValueRef;
use grpc::metadata::MetadataMap as GrpcMetadataMap;
use grpc::metadata::MetadataValue as GrpcMetadataValue;
use prost::Message;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tonic::Code;
use tonic::Extensions;
use tonic::Request as TonicRequest;
use tonic::Status;
use tonic::metadata::AsciiMetadataKey as TonicAsciiMetadataKey;
use tonic::metadata::BinaryMetadataKey as TonicBinaryMetadataKey;
use tonic::metadata::KeyAndValueRef as TonicKeyAndValueRef;
use tonic::metadata::MetadataMap as TonicMetadataMap;
use tonic::metadata::MetadataValue as TonicMetadataValue;
use tonic::service::Interceptor;

/// Adapter for encoding Prost messages into gRPC `SendMessage`.
#[doc(hidden)]
pub struct ProstSendMessage<'a, M>(pub &'a M);

impl<'a, M: Message> SendMessage for ProstSendMessage<'a, M> {
    fn encode(&self) -> Result<Box<dyn Buf + Send + Sync>, String> {
        Ok(Box::new(Bytes::from(self.0.encode_to_vec())))
    }
}

/// Adapter for decoding gRPC `RecvMessage` into Prost messages.
#[doc(hidden)]
pub struct ProstRecvMessage<'a, M>(pub &'a mut M);

impl<'a, M: Message + Default> RecvMessage for ProstRecvMessage<'a, M> {
    fn decode(&mut self, data: &mut dyn Buf) -> Result<(), String> {
        *self.0 = M::decode(data).map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// Converts `grpc::metadata::MetadataMap` into `tonic::metadata::MetadataMap`.
fn convert_metadata(grpc_md: &GrpcMetadataMap) -> TonicMetadataMap {
    let mut map = TonicMetadataMap::new();
    merge_grpc_metadata(&mut map, grpc_md);
    map
}

/// Merges metadata entries directly from `GrpcMetadataMap` into `TonicMetadataMap`.
#[doc(hidden)]
pub fn merge_grpc_metadata(target: &mut TonicMetadataMap, source: &GrpcMetadataMap) {
    for entry in source.iter() {
        match entry {
            GrpcKeyAndValueRef::Ascii(k, v) => {
                if let Ok(val) = v.to_str().parse()
                    && let Ok(key) = TonicAsciiMetadataKey::from_bytes(k.as_str().as_bytes())
                {
                    target.append(key, val);
                }
            }
            GrpcKeyAndValueRef::Binary(k, v) => {
                let val = TonicMetadataValue::from_bytes(v.as_bytes());
                if let Ok(key) = TonicBinaryMetadataKey::from_bytes(k.as_str().as_bytes()) {
                    target.append_bin(key, val);
                }
            }
        }
    }
}

/// Constructs a `tonic::Status` from a `grpc::StatusError` and metadata map.
#[doc(hidden)]
pub fn status_from_grpc_error(err: &GrpcStatusError, metadata: TonicMetadataMap) -> Status {
    let code = Code::from(err.code() as i32);
    let mut status = Status::new(code, err.message());
    *status.metadata_mut() = metadata;
    status
}

/// Converts `tonic::metadata::MetadataMap` into `grpc::metadata::MetadataMap` and extracts timeout.
#[doc(hidden)]
pub fn convert_tonic_metadata_to_grpc(
    tonic_md: &TonicMetadataMap,
) -> (GrpcMetadataMap, Option<Instant>) {
    let mut grpc_md = GrpcMetadataMap::new();
    let mut deadline = None;

    for item in tonic_md.iter() {
        match item {
            TonicKeyAndValueRef::Ascii(k, v) => {
                if k.as_str() == "grpc-timeout"
                    && let Ok(s) = v.to_str()
                    && !s.is_empty()
                {
                    let (val_str, unit) = s.split_at(s.len() - 1);
                    if let Ok(val) = val_str.parse::<u64>() {
                        let duration = match unit {
                            "H" => val.checked_mul(3600).map(Duration::from_secs),
                            "M" => val.checked_mul(60).map(Duration::from_secs),
                            "S" => Some(Duration::from_secs(val)),
                            "m" => Some(Duration::from_millis(val)),
                            "u" => Some(Duration::from_micros(val)),
                            "n" => Some(Duration::from_nanos(val)),
                            _ => None,
                        };
                        if let Some(d) = duration {
                            deadline = Instant::now().checked_add(d);
                        }
                    }
                    // Do not add grpc-timeout to the metadata; it is returned
                    // separately instead.
                    continue;
                }

                if let Ok(key) = GrpcAsciiMetadataKey::from_bytes(k.as_str().as_bytes())
                    && let Ok(s) = v.to_str()
                    && let Ok(val) = s.parse()
                {
                    grpc_md.append(key, val);
                }
            }
            TonicKeyAndValueRef::Binary(k, v) => {
                if let Ok(key) = GrpcBinaryMetadataKey::from_bytes(k.as_str().as_bytes())
                    && let Ok(bytes) = v.to_bytes()
                {
                    grpc_md.append_bin(key, GrpcMetadataValue::from_bytes(&bytes));
                }
            }
        }
    }

    (grpc_md, deadline)
}

/// RAII guard that aborts a spawned Tokio task when dropped.
#[doc(hidden)]
#[derive(Debug)]
pub struct AbortOnDrop(pub JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A gRPC response stream adapter.
pub struct GrpcResponseStream<Res, Rx> {
    rx: mpsc::Receiver<Result<Res, Status>>,
    trailers: Arc<Mutex<Option<TonicMetadataMap>>>,
    _drop_guard: AbortOnDrop,
    _send_guard: Option<AbortOnDrop>,
    _phantom: PhantomData<Rx>,
}

impl<Res, Rx> fmt::Debug for GrpcResponseStream<Res, Rx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcResponseStream").finish_non_exhaustive()
    }
}

impl<Res, Rx> GrpcResponseStream<Res, Rx>
where
    Rx: RecvStream + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    /// Creates a new `GrpcResponseStream`.
    pub fn new(
        mut stream: RecvStreamValidator<Rx>,
    ) -> (Self, oneshot::Receiver<Result<TonicMetadataMap, Status>>) {
        let (msg_tx, msg_rx) = mpsc::channel(1);
        let (headers_tx, headers_rx) = oneshot::channel();
        let trailers_arc = Arc::new(Mutex::new(None));
        let trailers_clone = trailers_arc.clone();

        let mut headers_tx_opt = Some(headers_tx);

        let task = tokio::spawn(async move {
            let mut response_md = TonicMetadataMap::new();

            loop {
                let mut res = Res::default();
                let mut recv_adapter = ProstRecvMessage(&mut res);
                let item = stream.recv(&mut recv_adapter).await;

                match item {
                    ResponseStreamItem::Headers(h) => {
                        merge_grpc_metadata(&mut response_md, h.metadata());
                        if let Some(tx) = headers_tx_opt.take() {
                            let _ = tx.send(Ok(std::mem::take(&mut response_md)));
                        }
                    }
                    ResponseStreamItem::Message => {
                        if let Some(tx) = headers_tx_opt.take() {
                            let _ = tx.send(Ok(std::mem::take(&mut response_md)));
                        }
                        if msg_tx.send(Ok(res)).await.is_err() {
                            break;
                        }
                    }
                    ResponseStreamItem::Trailers(t) => {
                        let mut md = TonicMetadataMap::new();
                        merge_grpc_metadata(&mut md, t.metadata());
                        *trailers_clone.lock().unwrap() = Some(md.clone());

                        if let Some(tx) = headers_tx_opt.take() {
                            match t.into_status() {
                                Ok(()) => {
                                    let _ = tx.send(Ok(md));
                                }
                                Err(err) => {
                                    let _ = tx.send(Err(status_from_grpc_error(&err, md)));
                                }
                            }
                        } else if let Err(err) = t.into_status() {
                            let _ = msg_tx.send(Err(status_from_grpc_error(&err, md))).await;
                        }
                        break;
                    }
                    ResponseStreamItem::StreamClosed => {
                        if let Some(tx) = headers_tx_opt.take() {
                            let _ = tx.send(Ok(std::mem::take(&mut response_md)));
                        }
                        break;
                    }
                }
            }
        });

        (
            Self {
                rx: msg_rx,
                trailers: trailers_arc,
                _drop_guard: AbortOnDrop(task),
                _send_guard: None,
                _phantom: PhantomData,
            },
            headers_rx,
        )
    }

    /// Attaches an [`AbortOnDrop`] guard to tie a background send task's lifecycle to this stream.
    pub fn with_send_guard(mut self, guard: AbortOnDrop) -> Self {
        self._send_guard = Some(guard);
        self
    }

    /// Reads trailing metadata if present.
    pub async fn trailers(&mut self) -> Result<Option<TonicMetadataMap>, Status> {
        while self.message().await?.is_some() {}
        Ok(self.trailers.lock().unwrap().clone())
    }

    /// Reads the next message from the response stream.
    pub async fn message(&mut self) -> Result<Option<Res>, Status> {
        match self.rx.recv().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

impl<Res, Rx> Unpin for GrpcResponseStream<Res, Rx> {}

impl<Res, Rx> Stream for GrpcResponseStream<Res, Rx>
where
    Rx: Send + 'static,
    Res: Send + 'static,
{
    type Item = Result<Res, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// A channel wrapped with a `tonic::service::Interceptor`.
#[derive(Debug, Clone)]
pub struct InterceptedChannel<C, F> {
    channel: C,
    interceptor: F,
}

impl<C, F> InterceptedChannel<C, F> {
    /// Creates a new `InterceptedChannel`.
    pub fn new(channel: C, interceptor: F) -> Self {
        Self {
            channel,
            interceptor,
        }
    }
}

/// RecvStream wrapper for `InterceptedChannel`.
pub enum InterceptedRecvStream<R> {
    Inner(R),
    Failed(Option<Trailers>),
}

impl<R: RecvStream> RecvStream for InterceptedRecvStream<R> {
    async fn recv(&mut self, msg: &mut dyn RecvMessage) -> ResponseStreamItem {
        match self {
            Self::Inner(r) => r.recv(msg).await,
            Self::Failed(trailers) => match trailers.take() {
                Some(t) => ResponseStreamItem::Trailers(t),
                None => ResponseStreamItem::StreamClosed,
            },
        }
    }
}

/// SendStream wrapper for `InterceptedChannel`.
pub enum InterceptedSendStream<S> {
    Inner(S),
    Failed,
}

impl<S: SendStream> SendStream for InterceptedSendStream<S> {
    async fn send(&mut self, msg: &dyn SendMessage, options: SendOptions) -> Result<(), ()> {
        match self {
            Self::Inner(s) => s.send(msg, options).await,
            Self::Failed => Err(()),
        }
    }
}

impl<C, F> Invoke for InterceptedChannel<C, F>
where
    C: Invoke + Send + Sync,
    F: Interceptor + Clone + Send + Sync + 'static,
{
    type SendStream = InterceptedSendStream<C::SendStream>;
    type RecvStream = InterceptedRecvStream<C::RecvStream>;

    async fn invoke(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
    ) -> (Self::SendStream, Self::RecvStream) {
        let tonic_md = convert_metadata(headers.metadata());
        let req = TonicRequest::from_parts(tonic_md, Extensions::new(), ());
        let mut interceptor = self.interceptor.clone();
        match interceptor.call(req) {
            Ok(intercepted_req) => {
                let (md, _ext, ()) = intercepted_req.into_parts();
                let (grpc_md, deadline) = convert_tonic_metadata_to_grpc(&md);
                let new_headers = RequestHeaders::new()
                    .with_method_name(headers.method_name())
                    .with_metadata(grpc_md);
                let mut options = options;
                if let Some(d) = deadline {
                    options.set_deadline(d);
                }
                let (tx, rx) = self.channel.invoke(new_headers, options).await;
                (
                    InterceptedSendStream::Inner(tx),
                    InterceptedRecvStream::Inner(rx),
                )
            }
            Err(status) => {
                let code = GrpcStatusCodeError::try_from(status.code() as i32)
                    .unwrap_or(GrpcStatusCodeError::Unknown);
                let (grpc_md, _) = convert_tonic_metadata_to_grpc(status.metadata());
                let status_err = GrpcStatusError::new(code, status.message());
                let trailers = Trailers::new(Err(status_err)).with_metadata(grpc_md);
                (
                    InterceptedSendStream::Failed,
                    InterceptedRecvStream::Failed(Some(trailers)),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Waker;
    use std::time::Duration;

    use grpc::StatusCodeError;
    use grpc::StatusError;
    use grpc::client::Invoke;
    use grpc::client::ResponseHeaders;
    use grpc::client::SendOptions;
    use grpc::client::SendStream;
    use grpc::client::Trailers;
    use tokio::sync::mpsc;
    use tokio_stream::Stream;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Code;
    use tonic::Extensions;
    use tonic::Request;
    use tonic::Response;
    use tonic::Status;
    use tonic::metadata::MetadataValue;

    use super::*;

    #[test]
    fn test_metadata_conversion_roundtrip() {
        let mut tonic_md = TonicMetadataMap::new();
        tonic_md.insert("x-custom-header", MetadataValue::from_static("test-val"));
        tonic_md.insert("grpc-timeout", MetadataValue::from_static("100m"));

        let (grpc_md, deadline) = convert_tonic_metadata_to_grpc(&tonic_md);
        assert!(deadline.is_some());
        assert_eq!(
            grpc_md
                .get(GrpcAsciiMetadataKey::from_bytes(b"x-custom-header").unwrap())
                .map(|v| v.to_str()),
            Some("test-val")
        );

        let roundtrip = convert_metadata(&grpc_md);
        assert_eq!(
            roundtrip
                .get("x-custom-header")
                .map(|v| v.to_str().unwrap()),
            Some("test-val")
        );
    }

    #[test]
    fn test_convert_metadata_preserves_multi_valued_keys() {
        let mut tonic_md = TonicMetadataMap::new();
        tonic_md.append("x-custom-header", MetadataValue::from_static("val1"));
        tonic_md.append("x-custom-header", MetadataValue::from_static("val2"));

        let (grpc_md, _) = convert_tonic_metadata_to_grpc(&tonic_md);
        let key = GrpcAsciiMetadataKey::from_bytes(b"x-custom-header").unwrap();
        let values: Vec<_> = grpc_md.get_all(&key).iter().map(|v| v.to_str()).collect();
        assert_eq!(
            values,
            vec!["val1", "val2"],
            "convert_tonic_metadata_to_grpc should preserve all values for multi-valued headers"
        );

        let mut grpc_md2 = GrpcMetadataMap::new();
        grpc_md2.append(key.clone(), "val1".parse().unwrap());
        grpc_md2.append(key, "val2".parse().unwrap());

        let tonic_md2 = convert_metadata(&grpc_md2);
        let tonic_values: Vec<_> = tonic_md2
            .get_all("x-custom-header")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(
            tonic_values,
            vec!["val1", "val2"],
            "convert_metadata should preserve all values for multi-valued headers"
        );
    }

    #[test]
    fn test_convert_timeout_metadata_handles_large_duration_without_overflow() {
        let mut tonic_md = TonicMetadataMap::new();
        // "18446744073709551615H" (u64::MAX with unit 'H') causes val * 3600 to overflow u64 if unchecked.
        tonic_md.insert(
            "grpc-timeout",
            MetadataValue::from_static("18446744073709551615H"),
        );

        let _ = convert_tonic_metadata_to_grpc(&tonic_md);
    }

    #[test]
    fn test_convert_timeout_metadata_removes_grpc_timeout_header() {
        let mut tonic_md = TonicMetadataMap::new();
        tonic_md.insert("grpc-timeout", MetadataValue::from_static("100m"));

        let (grpc_md, deadline) = convert_tonic_metadata_to_grpc(&tonic_md);
        assert!(deadline.is_some());

        let key = GrpcAsciiMetadataKey::from_bytes(b"grpc-timeout").unwrap();
        assert!(
            grpc_md.get(&key).is_none(),
            "grpc-timeout header should not be present in grpc_md once converted to deadline"
        );
    }

    struct MockRecvStream {
        items: Vec<ResponseStreamItem>,
    }

    impl RecvStream for MockRecvStream {
        async fn recv(&mut self, _msg: &mut dyn RecvMessage) -> ResponseStreamItem {
            if self.items.is_empty() {
                ResponseStreamItem::StreamClosed
            } else {
                self.items.remove(0)
            }
        }
    }

    #[tokio::test]
    async fn test_stream_poll_next_retains_trailers_metadata() {
        let mut map = GrpcMetadataMap::new();
        map.insert("x-trailer-key", "trailer-val".parse().unwrap());
        let trailers = Trailers::new(Ok(())).with_metadata(map);

        let mock_rx = MockRecvStream {
            items: vec![
                ResponseStreamItem::Headers(ResponseHeaders::new()),
                ResponseStreamItem::Trailers(trailers),
            ],
        };
        let validator = RecvStreamValidator::new(mock_rx, false);
        let (mut response_stream, _) = GrpcResponseStream::<(), MockRecvStream>::new(validator);

        // Consume the stream via StreamExt::next
        let item = response_stream.next().await;
        assert!(item.is_none());

        // Fetch trailers - should contain x-trailer-key
        let trailers = response_stream.trailers().await.unwrap();
        assert!(
            trailers.is_some(),
            "trailers() should return Some metadata when stream completed with trailers"
        );
        let trailers_map = trailers.unwrap();
        assert_eq!(
            trailers_map
                .get("x-trailer-key")
                .map(|v| v.to_str().unwrap()),
            Some("trailer-val")
        );
    }

    #[tokio::test]
    async fn test_stream_message_allows_call_after_poll() {
        let mock_rx = MockRecvStream {
            items: vec![
                ResponseStreamItem::Headers(ResponseHeaders::new()),
                ResponseStreamItem::Message,
                ResponseStreamItem::Trailers(Trailers::new(Ok(()))),
            ],
        };
        let validator = RecvStreamValidator::new(mock_rx, false);
        let (mut response_stream, _) = GrpcResponseStream::<(), MockRecvStream>::new(validator);

        // Poll stream once.
        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);
        let _ = Pin::new(&mut response_stream).poll_next(&mut cx);

        // Call message() on the stream after stream polling - should not panic
        let res = response_stream.message().await;
        assert!(res.is_ok());
    }

    struct MockSendStream;

    impl SendStream for MockSendStream {
        async fn send(&mut self, _msg: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
            Ok(())
        }
    }

    struct UnaryMockInvoker;

    impl Invoke for UnaryMockInvoker {
        type SendStream = MockSendStream;
        type RecvStream = MockRecvStream;

        async fn invoke(
            &self,
            _headers: RequestHeaders,
            _options: CallOptions,
        ) -> (Self::SendStream, Self::RecvStream) {
            let mut map = GrpcMetadataMap::new();
            map.insert("x-resp-header", "resp-val".parse().unwrap());
            let trailers = Trailers::new(Ok(())).with_metadata(map);

            (
                MockSendStream,
                MockRecvStream {
                    items: vec![
                        ResponseStreamItem::Headers(ResponseHeaders::new()),
                        ResponseStreamItem::Message,
                        ResponseStreamItem::Trailers(trailers),
                    ],
                },
            )
        }
    }

    #[tokio::test]
    async fn test_unary_response_populates_metadata_from_trailers() {
        let invoker = UnaryMockInvoker;
        let headers = RequestHeaders::new().with_method_name("/test.Service/TestMethod");
        let options = CallOptions::default();

        let (_tx, rx) = invoker.invoke(headers, options).await;
        let mut rx = RecvStreamValidator::new(rx, true);

        let mut res_msg = ();
        let mut recv_adapter = ProstRecvMessage(&mut res_msg);

        let mut response_md = TonicMetadataMap::new();

        // Simulates the exact loop generated by generate_unary in client_grpc.rs
        let response: Response<()> = loop {
            let item = rx.recv(&mut recv_adapter).await;
            match item {
                ResponseStreamItem::Message => {}
                ResponseStreamItem::Trailers(t) => {
                    merge_grpc_metadata(&mut response_md, t.metadata());
                    break match t.into_status() {
                        Ok(()) => Ok::<_, Status>(Response::from_parts(
                            response_md,
                            res_msg,
                            Extensions::new(),
                        )),
                        Err(err) => Err(status_from_grpc_error(&err, response_md)),
                    }
                    .unwrap();
                }
                ResponseStreamItem::Headers(h) => {
                    merge_grpc_metadata(&mut response_md, h.metadata());
                }
                ResponseStreamItem::StreamClosed => panic!("closed prematurely"),
            }
        };

        assert_eq!(
            response
                .metadata()
                .get("x-resp-header")
                .map(|v| v.to_str().unwrap()),
            Some("resp-val"),
            "tonic::Response metadata should contain headers/trailers received from server"
        );
    }

    #[tokio::test]
    async fn test_intercepted_channel_aborts_invocation_on_interceptor_error() {
        struct PassthroughInvoker;

        impl Invoke for PassthroughInvoker {
            type SendStream = MockSendStream;
            type RecvStream = MockRecvStream;

            async fn invoke(
                &self,
                _headers: RequestHeaders,
                _options: CallOptions,
            ) -> (Self::SendStream, Self::RecvStream) {
                (
                    MockSendStream,
                    MockRecvStream {
                        items: vec![ResponseStreamItem::Trailers(Trailers::new(Ok(())))],
                    },
                )
            }
        }

        let channel = InterceptedChannel::new(PassthroughInvoker, |_: Request<()>| {
            Err(Status::unauthenticated("unauthorized"))
        });

        let (_tx, rx) = channel
            .invoke(RequestHeaders::default(), CallOptions::default())
            .await;
        let mut rx = RecvStreamValidator::new(rx, false);
        let item = rx.recv(&mut ProstRecvMessage(&mut ())).await;

        match item {
            ResponseStreamItem::Trailers(t) => {
                let status = t.into_status();
                assert!(
                    status.is_err(),
                    "InterceptedChannel should return error status when interceptor fails"
                );
                assert_eq!(status.unwrap_err().code(), StatusCodeError::Unauthenticated);
            }
            _ => panic!("expected error trailers from failed interceptor"),
        }
    }

    #[tokio::test]
    async fn test_stream_message_attaches_metadata_to_status_on_error() {
        let mut map = GrpcMetadataMap::new();
        map.insert("x-error-detail", "err-val".parse().unwrap());
        let trailers = Trailers::new(Err(StatusError::new(
            StatusCodeError::InvalidArgument,
            "invalid arg",
        )))
        .with_metadata(map);

        let mock_rx = MockRecvStream {
            items: vec![
                ResponseStreamItem::Headers(ResponseHeaders::new()),
                ResponseStreamItem::Trailers(trailers),
            ],
        };
        let validator = RecvStreamValidator::new(mock_rx, false);
        let (mut response_stream, _) = GrpcResponseStream::<(), MockRecvStream>::new(validator);

        let err = response_stream.message().await.unwrap_err();
        assert_eq!(
            err.metadata()
                .get("x-error-detail")
                .map(|v| v.to_str().unwrap()),
            Some("err-val"),
            "status returned by message() should retain response metadata on error"
        );
    }

    #[test]
    fn test_convert_timeout_metadata_handles_large_seconds_duration_without_panic() {
        let mut tonic_md = TonicMetadataMap::new();
        tonic_md.insert(
            "grpc-timeout",
            MetadataValue::from_static("18446744073709551615S"),
        );

        let (_, deadline) = convert_tonic_metadata_to_grpc(&tonic_md);
        let _ = deadline;
    }

    #[tokio::test]
    async fn test_response_stream_trailers_retains_metadata_on_error() {
        let mut map = GrpcMetadataMap::new();
        map.insert("x-error-trailer", "trailer-err-val".parse().unwrap());
        let trailers = Trailers::new(Err(StatusError::new(
            StatusCodeError::NotFound,
            "not found",
        )))
        .with_metadata(map);

        let mock_rx = MockRecvStream {
            items: vec![
                ResponseStreamItem::Headers(ResponseHeaders::new()),
                ResponseStreamItem::Trailers(trailers),
            ],
        };
        let validator = RecvStreamValidator::new(mock_rx, false);
        let (mut response_stream, _headers_rx) =
            GrpcResponseStream::<(), MockRecvStream>::new(validator);

        let _err = response_stream.message().await.unwrap_err();
        let trailers_res = response_stream.trailers().await;
        assert!(
            trailers_res.is_ok(),
            "trailers() should return Ok(Some(map)) containing metadata even when status is error"
        );
        let trailers_map = trailers_res
            .unwrap()
            .expect("trailers map should be present");
        assert_eq!(
            trailers_map
                .get("x-error-trailer")
                .map(|v| v.to_str().unwrap()),
            Some("trailer-err-val")
        );
    }

    #[tokio::test]
    async fn test_intercepted_channel_preserves_interceptor_error_metadata() {
        struct PassthroughInvoker;

        impl Invoke for PassthroughInvoker {
            type SendStream = MockSendStream;
            type RecvStream = MockRecvStream;

            async fn invoke(
                &self,
                _headers: RequestHeaders,
                _options: CallOptions,
            ) -> (Self::SendStream, Self::RecvStream) {
                (MockSendStream, MockRecvStream { items: vec![] })
            }
        }

        let channel = InterceptedChannel::new(PassthroughInvoker, |_: Request<()>| {
            let mut status = Status::unauthenticated("unauthorized");
            status.metadata_mut().insert(
                "x-interceptor-error",
                MetadataValue::from_static("auth-failed"),
            );
            Err(status)
        });

        let (_tx, rx) = channel
            .invoke(RequestHeaders::default(), CallOptions::default())
            .await;
        let mut rx = RecvStreamValidator::new(rx, false);
        let item = rx.recv(&mut ProstRecvMessage(&mut ())).await;

        match item {
            ResponseStreamItem::Trailers(t) => {
                let metadata = convert_metadata(t.metadata());
                assert_eq!(
                    metadata
                        .get("x-interceptor-error")
                        .map(|v| v.to_str().unwrap()),
                    Some("auth-failed"),
                    "metadata attached to interceptor status error should be preserved in trailers"
                );
            }
            _ => panic!("expected error trailers from failed interceptor"),
        }
    }

    #[tokio::test]
    async fn test_stream_retains_initial_headers_metadata() {
        let mut headers_map = GrpcMetadataMap::new();
        headers_map.insert("x-initial-header", "header-val".parse().unwrap());
        let headers = ResponseHeaders::new().with_metadata(headers_map);

        let mock_rx = MockRecvStream {
            items: vec![
                ResponseStreamItem::Headers(headers),
                ResponseStreamItem::Trailers(Trailers::new(Ok(()))),
            ],
        };
        let validator = RecvStreamValidator::new(mock_rx, false);
        let (mut response_stream, headers_rx) =
            GrpcResponseStream::<(), MockRecvStream>::new(validator);

        // Consume the stream via StreamExt::next
        let item = response_stream.next().await;
        assert!(item.is_none());

        // Fetch initial headers - should contain x-initial-header
        let initial_md = headers_rx.await.unwrap().unwrap();
        assert_eq!(
            initial_md
                .get("x-initial-header")
                .map(|v| v.to_str().unwrap()),
            Some("header-val"),
            "initial response headers metadata should be retained and accessible"
        );
    }

    #[tokio::test]
    async fn test_response_stream_trailers_waits_for_stream_completion_after_message() {
        let mut initial_md = GrpcMetadataMap::new();
        initial_md.insert("x-initial", "header-val".parse().unwrap());
        let initial_headers = ResponseHeaders::new().with_metadata(initial_md);

        let mut trailer_md = GrpcMetadataMap::new();
        trailer_md.insert("x-trailer", "trailer-val".parse().unwrap());
        let trailers = Trailers::new(Ok(())).with_metadata(trailer_md);

        let mock_rx = MockRecvStream {
            items: vec![
                ResponseStreamItem::Headers(initial_headers),
                ResponseStreamItem::Message,
                ResponseStreamItem::Trailers(trailers),
            ],
        };
        let validator = RecvStreamValidator::new(mock_rx, false);
        let (mut response_stream, _) = GrpcResponseStream::<(), MockRecvStream>::new(validator);

        // 1. Consume the message.
        let msg = response_stream.message().await.unwrap();
        assert!(msg.is_some());

        // 2. Consume the end of the stream so trailers are received.
        let end = response_stream.message().await;
        assert!(end.unwrap().is_none());

        let trailers_map = response_stream
            .trailers()
            .await
            .unwrap()
            .expect("trailers should be present");

        assert_eq!(
            trailers_map.get("x-trailer").map(|v| v.to_str().unwrap()),
            Some("trailer-val"),
            "trailers() should wait for stream completion and return trailer metadata sent at end of stream"
        );
    }

    struct PendingRecvStream {
        yielded: bool,
        items: Vec<ResponseStreamItem>,
    }

    impl RecvStream for PendingRecvStream {
        async fn recv(&mut self, _msg: &mut dyn RecvMessage) -> ResponseStreamItem {
            if !self.yielded {
                self.yielded = true;
                tokio::task::yield_now().await;
            }
            if self.items.is_empty() {
                ResponseStreamItem::StreamClosed
            } else {
                self.items.remove(0)
            }
        }
    }

    #[tokio::test]
    async fn test_response_stream_trailers_discards_remaining_messages() {
        let mock_rx = PendingRecvStream {
            yielded: false,
            items: vec![
                ResponseStreamItem::Headers(ResponseHeaders::new()),
                ResponseStreamItem::Message,
                ResponseStreamItem::Trailers(Trailers::new(Ok(()))),
            ],
        };
        let validator = RecvStreamValidator::new(mock_rx, false);
        let (mut response_stream, _) = GrpcResponseStream::<(), PendingRecvStream>::new(validator);

        // Poll stream once. Because PendingRecvStream yields on first call, poll_next will create fut and return Pending.
        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);
        let poll_res = Pin::new(&mut response_stream).poll_next(&mut cx);
        assert!(
            poll_res.is_pending(),
            "poll_next should return Pending on yielding stream"
        );

        // Call trailers(). This should drain the remaining messages.
        let _trailers = response_stream.trailers().await.unwrap();

        // The message fetched during poll_next must be discarded.
        let msg = response_stream.message().await.unwrap();
        assert!(
            msg.is_none(),
            "trailers() should drain and discard all remaining messages"
        );
    }

    #[tokio::test]
    async fn test_response_stream_drop_cancels_background_send_task() {
        let (request_tx, request_rx) = mpsc::channel::<()>(10);
        let mut req_stream = Box::pin(ReceiverStream::new(request_rx));

        let mock_rx = MockRecvStream { items: vec![] };
        let validator = RecvStreamValidator::new(mock_rx, false);

        let (task_done_tx, mut task_done_rx) = mpsc::channel::<()>(1);

        let handle = tokio::spawn(async move {
            while let Some(_msg) = req_stream.next().await {
                // Simulates sending request messages
            }
            let _ = task_done_tx.send(()).await;
        });

        let (response_stream, _) = GrpcResponseStream::<(), MockRecvStream>::new(validator);
        let response_stream = response_stream.with_send_guard(AbortOnDrop(handle));

        // Send one message to the request stream
        request_tx.send(()).await.unwrap();

        // Drop the response stream (simulating client dropping the response/call handle)
        drop(response_stream);

        // Dropping response_stream should cause the background send task to terminate.
        let terminated =
            tokio::time::timeout(Duration::from_millis(100), task_done_rx.recv()).await;

        assert!(
            terminated.is_ok(),
            "dropping response stream should cancel/terminate the background request pump task"
        );
    }

    #[tokio::test]
    async fn test_client_streaming_receives_early_error_while_request_stream_pending() {
        struct ImmediateErrorInvoker;

        impl Invoke for ImmediateErrorInvoker {
            type SendStream = MockSendStream;
            type RecvStream = MockRecvStream;

            async fn invoke(
                &self,
                _headers: RequestHeaders,
                _options: CallOptions,
            ) -> (Self::SendStream, Self::RecvStream) {
                let trailers = Trailers::new(Err(StatusError::new(
                    StatusCodeError::Unauthenticated,
                    "unauthorized",
                )));
                (
                    MockSendStream,
                    MockRecvStream {
                        items: vec![ResponseStreamItem::Trailers(trailers)],
                    },
                )
            }
        }

        let (req_tx, req_rx) = mpsc::channel::<()>(10);
        let mut req_stream = ReceiverStream::new(req_rx);

        // Send 1 item so the stream yields once, then stays pending (open channel, no more items)
        req_tx.send(()).await.unwrap();

        let invoker = ImmediateErrorInvoker;
        let headers = RequestHeaders::new().with_method_name("/test.Service/ClientStreamMethod");
        let options = CallOptions::default();

        let (mut tx, rx) = invoker.invoke(headers, options).await;
        let mut rx = RecvStreamValidator::new(rx, true);

        // Tests the fixed client-streaming code pattern generated by client_grpc.rs:
        let client_call_fut = async {
            let send_task = tokio::spawn(async move {
                while let Some(msg) = req_stream.next().await {
                    let send_msg = ProstSendMessage(&msg);
                    if tx.send(&send_msg, SendOptions::default()).await.is_err() {
                        break;
                    }
                }
                drop(tx);
            });
            let _send_guard = AbortOnDrop(send_task);

            let mut res_msg = ();
            let mut recv_adapter = ProstRecvMessage(&mut res_msg);
            loop {
                let item = rx.recv(&mut recv_adapter).await;
                match item {
                    ResponseStreamItem::Message => {}
                    ResponseStreamItem::Trailers(t) => {
                        return t.into_status().map_err(|err| {
                            Status::new(Code::from(err.code() as i32), err.message())
                        });
                    }
                    ResponseStreamItem::StreamClosed => {
                        return Err(Status::internal("closed"));
                    }
                    _ => {}
                }
            }
        };

        // Because request sending runs in background task, the client call receives the error immediately
        let result = tokio::time::timeout(Duration::from_millis(100), client_call_fut).await;

        assert!(
            result.is_ok(),
            "Client streaming call should complete without timing out when server sends early error"
        );
        let status = result.unwrap();
        assert_eq!(
            status.unwrap_err().code(),
            Code::Unauthenticated,
            "Client streaming call should receive Unauthenticated status from early error trailers"
        );
    }

    #[tokio::test]
    async fn test_unary_interceptor_error_preserves_status_and_metadata() {
        struct PassthroughInvoker;

        impl Invoke for PassthroughInvoker {
            type SendStream = MockSendStream;
            type RecvStream = MockRecvStream;

            async fn invoke(
                &self,
                _headers: RequestHeaders,
                _options: CallOptions,
            ) -> (Self::SendStream, Self::RecvStream) {
                (MockSendStream, MockRecvStream { items: vec![] })
            }
        }

        let channel = InterceptedChannel::new(PassthroughInvoker, |_: Request<()>| {
            let mut status = Status::unauthenticated("unauthorized");
            status.metadata_mut().insert(
                "x-interceptor-error",
                MetadataValue::from_static("auth-failed"),
            );
            Err(status)
        });

        let (mut tx, rx) = channel
            .invoke(RequestHeaders::default(), CallOptions::default())
            .await;
        let mut rx = RecvStreamValidator::new(rx, true);

        // Emulates simplified unary call handling pattern:
        let send_msg = ProstSendMessage(&());
        let mut response_md = TonicMetadataMap::new();
        let mut res_msg = ();
        let mut recv_adapter = ProstRecvMessage(&mut res_msg);

        let _ = tx
            .send(&send_msg, SendOptions::new().with_final_msg(true))
            .await;

        let status: Result<(), Status> = loop {
            let item = rx.recv(&mut recv_adapter).await;
            match item {
                ResponseStreamItem::Message => {}
                ResponseStreamItem::Trailers(t) => {
                    merge_grpc_metadata(&mut response_md, t.metadata());
                    break match t.into_status() {
                        Ok(()) => Ok(()),
                        Err(err) => Err(status_from_grpc_error(&err, response_md)),
                    };
                }
                ResponseStreamItem::Headers(h) => {
                    merge_grpc_metadata(&mut response_md, h.metadata());
                }
                ResponseStreamItem::StreamClosed => {
                    break Err(Status::internal("stream closed prematurely"));
                }
            }
        };
        let status = status.unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(
            status
                .metadata()
                .get("x-interceptor-error")
                .map(|v| v.to_str().unwrap()),
            Some("auth-failed"),
            "unary call must preserve interceptor error status and metadata"
        );
    }

    #[tokio::test]
    async fn test_unary_early_server_error_preserves_status_and_metadata() {
        struct FailingSendStream;

        impl SendStream for FailingSendStream {
            async fn send(
                &mut self,
                _msg: &dyn SendMessage,
                _options: SendOptions,
            ) -> Result<(), ()> {
                Err(())
            }
        }

        struct EarlyErrorInvoker;

        impl Invoke for EarlyErrorInvoker {
            type SendStream = FailingSendStream;
            type RecvStream = MockRecvStream;

            async fn invoke(
                &self,
                _headers: RequestHeaders,
                _options: CallOptions,
            ) -> (Self::SendStream, Self::RecvStream) {
                let mut map = GrpcMetadataMap::new();
                map.insert("x-server-error", "early-err-val".parse().unwrap());
                let trailers = Trailers::new(Err(StatusError::new(
                    StatusCodeError::Unauthenticated,
                    "invalid token",
                )))
                .with_metadata(map);

                (
                    FailingSendStream,
                    MockRecvStream {
                        items: vec![ResponseStreamItem::Trailers(trailers)],
                    },
                )
            }
        }

        let invoker = EarlyErrorInvoker;
        let (mut tx, rx) = invoker
            .invoke(RequestHeaders::default(), CallOptions::default())
            .await;
        let mut rx = RecvStreamValidator::new(rx, true);

        let mut response_md = TonicMetadataMap::new();
        let mut res_msg = ();
        let mut recv_adapter = ProstRecvMessage(&mut res_msg);

        let _ = tx
            .send(
                &ProstSendMessage(&()),
                SendOptions::new().with_final_msg(true),
            )
            .await;

        let status: Result<(), Status> = loop {
            let item = rx.recv(&mut recv_adapter).await;
            match item {
                ResponseStreamItem::Message => {}
                ResponseStreamItem::Trailers(t) => {
                    merge_grpc_metadata(&mut response_md, t.metadata());
                    break match t.into_status() {
                        Ok(()) => Ok(()),
                        Err(err) => Err(status_from_grpc_error(&err, response_md)),
                    };
                }
                ResponseStreamItem::Headers(h) => {
                    merge_grpc_metadata(&mut response_md, h.metadata());
                }
                ResponseStreamItem::StreamClosed => {
                    break Err(Status::internal("stream closed prematurely"));
                }
            }
        };
        let status = status.unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(
            status
                .metadata()
                .get("x-server-error")
                .map(|v| v.to_str().unwrap()),
            Some("early-err-val"),
            "unary call must preserve server early-error status and trailer metadata"
        );
    }

    #[test]
    fn test_convert_tonic_metadata_to_grpc_binary_valid_base64_raw_bytes() {
        let mut tonic_md = TonicMetadataMap::new();
        let raw_bytes = b"ABCD"; // Raw binary bytes that happen to be valid base64 ASCII
        tonic_md.insert_bin("custom-bin", MetadataValue::from_bytes(raw_bytes));

        let (grpc_md, _) = convert_tonic_metadata_to_grpc(&tonic_md);
        let key = GrpcBinaryMetadataKey::from_bytes(b"custom-bin").unwrap();
        let val = grpc_md
            .get_bin(&key)
            .expect("custom-bin key should be present in grpc_md");
        assert_eq!(
            val.as_bytes(),
            raw_bytes,
            "binary metadata containing raw bytes that form valid base64 must not be speculatively decoded"
        );
    }
}
