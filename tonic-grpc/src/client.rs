/*
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
 */

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures::future::BoxFuture;
use http_body_util::BodyExt as _;
use tower_service::Service;

use grpc::client::{CallOptions, Invoke, RecvStream, ResponseStreamItem, SendOptions, SendStream};
use grpc::core::RequestHeaders;
use grpc::metadata::MetadataMap;

use crate::framing::{FrameDeframer, RawBytesRecvMessage, RawBytesSendMessage};

/// A Tower [`Service`] adapter wrapping a [`grpc::client::Invoke`] implementation.
///
/// Implements `Service<http::Request<ReqBody>>` so that it satisfies Tonic's
/// `tonic::client::GrpcService<ReqBody>` blanket implementation.
pub struct InvokeService<I> {
    invoker: Arc<I>,
}

impl<I> InvokeService<I> {
    /// Creates a new [`InvokeService`] wrapping the provided invoker.
    pub fn new(invoker: I) -> Self {
        Self {
            invoker: Arc::new(invoker),
        }
    }
}

impl<I> Clone for InvokeService<I> {
    fn clone(&self) -> Self {
        Self {
            invoker: self.invoker.clone(),
        }
    }
}

impl<I> fmt::Debug for InvokeService<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvokeService").finish()
    }
}

type RecvFuture<R> =
    Pin<Box<dyn Future<Output = (R, ResponseStreamItem, Option<Bytes>)> + Send + 'static>>;

enum ResponseState<R> {
    Idle(Option<R>),
    Future(RecvFuture<R>),
    Done,
}

/// The HTTP response body returned by [`InvokeService`].
pub struct InvokeResponseBody<R> {
    state: ResponseState<R>,
}

impl<R> fmt::Debug for InvokeResponseBody<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvokeResponseBody").finish()
    }
}

impl<R: RecvStream + Unpin + Send + 'static> http_body::Body for InvokeResponseBody<R> {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        loop {
            match self.state {
                ResponseState::Done => return Poll::Ready(None),
                ResponseState::Idle(ref mut recv_stream_opt) => {
                    let mut recv_stream = match recv_stream_opt.take() {
                        Some(s) => s,
                        None => {
                            self.state = ResponseState::Done;
                            return Poll::Ready(None);
                        }
                    };

                    let fut = Box::pin(async move {
                        let mut raw_msg = RawBytesRecvMessage::default();
                        let item = recv_stream.recv(&mut raw_msg).await;
                        (recv_stream, item, raw_msg.bytes)
                    });
                    self.state = ResponseState::Future(fut);
                }
                ResponseState::Future(ref mut fut) => {
                    let (recv_stream, item, msg_bytes) = match fut.as_mut().poll(cx) {
                        Poll::Ready(res) => res,
                        Poll::Pending => return Poll::Pending,
                    };

                    match item {
                        ResponseStreamItem::Message => {
                            let payload = msg_bytes.unwrap_or_default();
                            let mut framed = BytesMut::with_capacity(5 + payload.len());
                            framed.put_u8(0); // 0x00 = uncompressed
                            framed.put_u32(payload.len() as u32);
                            framed.extend_from_slice(&payload);

                            self.state = ResponseState::Idle(Some(recv_stream));
                            return Poll::Ready(Some(Ok(http_body::Frame::data(
                                framed.freeze(),
                            ))));
                        }
                        ResponseStreamItem::Trailers(trailers) => {
                            self.state = ResponseState::Done;
                            let headers: tonic::metadata::MetadataMap =
                                trailers.metadata().clone().into();
                            if let Err(err) = trailers.into_status() {
                                let (code, msg) = err.into_parts();
                                return Poll::Ready(Some(Err(tonic::Status::new(
                                    tonic::Code::from(code as i32),
                                    msg,
                                ))));
                            }
                            return Poll::Ready(Some(Ok(http_body::Frame::trailers(
                                headers.into_headers(),
                            ))));
                        }
                        ResponseStreamItem::StreamClosed => {
                            self.state = ResponseState::Done;
                            return Poll::Ready(None);
                        }
                        ResponseStreamItem::Headers(_) => {
                            self.state = ResponseState::Idle(Some(recv_stream));
                        }
                    }
                }
            }
        }
    }
}

impl<I, ReqBody> Service<http::Request<ReqBody>> for InvokeService<I>
where
    I: Invoke + Send + Sync + 'static,
    ReqBody: http_body::Body<Data = Bytes> + Unpin + Send + 'static,
    ReqBody::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send + 'static,
{
    type Response = http::Response<InvokeResponseBody<I::RecvStream>>;
    type Error = tonic::Status;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let invoker = self.invoker.clone();

        Box::pin(async move {
            let (parts, body) = req.into_parts();

            let path = parts.uri.path().to_string();
            let tonic_meta = tonic::metadata::MetadataMap::from_headers(parts.headers);
            let metadata = MetadataMap::try_from(&tonic_meta).unwrap_or_default();
            let headers = RequestHeaders::new()
                .with_method_name(path)
                .with_metadata(metadata);

            let options = CallOptions::new();

            let (mut send_stream, mut recv_stream) = invoker.invoke(headers, options).await;

            tokio::spawn(async move {
                let mut body = body;
                let mut deframer = FrameDeframer::new();

                while let Ok(Some(frame)) = body.frame().await.transpose() {
                    if let Ok(data) = frame.into_data() {
                        deframer.push(data);
                        loop {
                            match deframer.pop_frame() {
                                Ok(Some(payload)) => {
                                    let msg = RawBytesSendMessage(payload);
                                    if send_stream.send(&msg, SendOptions::new()).await.is_err() {
                                        return;
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => return, // Compressed incoming request unsupported
                            }
                        }
                    }
                }

                while let Ok(Some(payload)) = deframer.pop_frame() {
                    let msg = RawBytesSendMessage(payload);
                    let _ = send_stream.send(&msg, SendOptions::new()).await;
                }
            });

            let mut raw_msg = RawBytesRecvMessage::default();
            match recv_stream.recv(&mut raw_msg).await {
                ResponseStreamItem::Headers(hdrs) => {
                    let mut resp = http::Response::builder()
                        .status(200)
                        .body(InvokeResponseBody {
                            state: ResponseState::Idle(Some(recv_stream)),
                        })
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;

                    let metadata: tonic::metadata::MetadataMap = hdrs.metadata().clone().into();
                    *resp.headers_mut() = metadata.into_headers();
                    Ok(resp)
                }
                ResponseStreamItem::Trailers(trailers) => {
                    if let Err(err) = trailers.into_status() {
                        let (code, msg) = err.into_parts();
                        Err(tonic::Status::new(tonic::Code::from(code as i32), msg))
                    } else {
                        Err(tonic::Status::internal(
                            "Received trailers-only response without error status",
                        ))
                    }
                }
                ResponseStreamItem::StreamClosed => {
                    Err(tonic::Status::internal("Stream closed before response headers received"))
                }
                ResponseStreamItem::Message => {
                    Err(tonic::Status::internal("Received response message before headers"))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grpc::client::SendStream;
    use grpc::core::{ResponseHeaders, Trailers};
    use http_body_util::Full;
    use tonic::client::GrpcService;

    struct TestSendStream;
    impl SendStream for TestSendStream {
        async fn send(
            &mut self,
            _msg: &dyn grpc::core::SendMessage,
            _options: SendOptions,
        ) -> Result<(), ()> {
            Ok(())
        }
    }

    struct TestRecvStream {
        yielded_headers: bool,
        yielded_msg: bool,
        yielded_trailers: bool,
    }

    impl RecvStream for TestRecvStream {
        async fn recv(&mut self, msg: &mut dyn grpc::core::RecvMessage) -> ResponseStreamItem {
            if !self.yielded_headers {
                self.yielded_headers = true;
                return ResponseStreamItem::Headers(ResponseHeaders::new());
            }
            if !self.yielded_msg {
                self.yielded_msg = true;
                let data = Bytes::from_static(b"response payload");
                let mut buf = std::io::Cursor::new(data);
                let _ = msg.decode(&mut buf);
                return ResponseStreamItem::Message;
            }
            if !self.yielded_trailers {
                self.yielded_trailers = true;
                return ResponseStreamItem::Trailers(Trailers::new(Ok(())));
            }
            ResponseStreamItem::StreamClosed
        }
    }

    struct TestInvoker;
    impl Invoke for TestInvoker {
        type SendStream = TestSendStream;
        type RecvStream = TestRecvStream;

        async fn invoke(
            &self,
            _headers: RequestHeaders,
            _options: CallOptions,
        ) -> (Self::SendStream, Self::RecvStream) {
            (
                TestSendStream,
                TestRecvStream {
                    yielded_headers: false,
                    yielded_msg: false,
                    yielded_trailers: false,
                },
            )
        }
    }

    #[tokio::test]
    async fn test_invoke_service_call() {
        let invoker = TestInvoker;
        let mut service = InvokeService::new(invoker);

        fn assert_grpc_service<T: GrpcService<Full<Bytes>>>(_svc: &T) {}
        assert_grpc_service(&service);

        let mut req_body = BytesMut::new();
        req_body.extend_from_slice(&[0x00]);
        req_body.extend_from_slice(&5u32.to_be_bytes());
        req_body.extend_from_slice(b"hello");

        let request = http::Request::builder()
            .uri("/test.Service/TestMethod")
            .body(Full::new(req_body.freeze()))
            .unwrap();

        let response = tower_service::Service::call(&mut service, request).await.unwrap();
        assert_eq!(response.status(), 200);

        let mut resp_body = response.into_body();
        let frame1 = resp_body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(&frame1[..5], &[0x00, 0, 0, 0, 16]);
        assert_eq!(&frame1[5..], b"response payload");
    }
}

