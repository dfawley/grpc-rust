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

use std::collections::HashSet;

use proc_macro2::TokenStream;

use super::Attributes;
use super::Method;
use super::Service;
use crate::format_method_name;
use crate::format_method_path;
use crate::format_service_name;
use crate::generate_deprecated;
use crate::generate_doc_comments;
use crate::naive_snake_case;

pub(crate) fn generate_internal<T: Service>(
    service: &T,
    emit_package: bool,
    proto_path: &str,
    compile_well_known_types: bool,
    _build_transport: bool,
    attributes: &Attributes,
    disable_comments: &HashSet<String>,
) -> TokenStream {
    let service_ident = quote::format_ident!("{}Client", service.name());
    let client_mod = quote::format_ident!("{}_client", naive_snake_case(service.name()));
    let methods = generate_methods(
        service,
        emit_package,
        proto_path,
        compile_well_known_types,
        disable_comments,
    );

    let package = if emit_package { service.package() } else { "" };
    let service_name = format_service_name(service, emit_package);

    let service_doc = if disable_comments.contains(&service_name) {
        TokenStream::new()
    } else {
        generate_doc_comments(service.comment())
    };

    let mod_attributes = attributes.for_mod(package);
    let struct_attributes = attributes.for_struct(&service_name);

    quote::quote! {
        /// Generated client implementations using the `grpc` crate.
        #(#mod_attributes)*
        pub mod #client_mod {
            #![allow(
                unused_variables,
                dead_code,
                missing_docs,
                clippy::wildcard_imports,
                clippy::let_unit_value,
            )]
            use grpc::client::*;
            use grpc::client::stream_util::RecvStreamValidator;
            use tonic_grpc::client::*;

            #service_doc
            #(#struct_attributes)*
            #[derive(Debug, Clone)]
            pub struct #service_ident<T> {
                inner: T,
            }

            impl<T> #service_ident<T>
            where
                T: grpc::client::Invoke,
            {
                pub fn new(inner: T) -> Self {
                    Self { inner }
                }

                pub fn with_interceptor<F>(inner: T, interceptor: F) -> #service_ident<InterceptedChannel<T, F>>
                where
                    F: tonic::service::Interceptor + Clone + Send + Sync + 'static,
                {
                    #service_ident::new(InterceptedChannel::new(inner, interceptor))
                }

                pub fn inner(&self) -> &T {
                    &self.inner
                }

                pub fn inner_mut(&mut self) -> &mut T {
                    &mut self.inner
                }

                pub fn into_inner(self) -> T {
                    self.inner
                }

                #methods
            }

            pub use tonic_grpc::client::{
                GrpcResponseStream, InterceptedChannel,
            };
        }
    }
}

fn generate_methods<T: Service>(
    service: &T,
    emit_package: bool,
    proto_path: &str,
    compile_well_known_types: bool,
    disable_comments: &HashSet<String>,
) -> TokenStream {
    let mut stream = TokenStream::new();

    for method in service.methods() {
        if !disable_comments.contains(&format_method_name(service, method, emit_package)) {
            stream.extend(generate_doc_comments(method.comment()));
        }
        if method.deprecated() {
            stream.extend(generate_deprecated());
        }

        let ident = quote::format_ident!("{}", method.name());
        let (request, response) = method.request_response_name(proto_path, compile_well_known_types);
        let path = format_method_path(service, method, emit_package);

        let generated_method = match (method.client_streaming(), method.server_streaming()) {
            (false, false) => generate_unary(&ident, &request, &response, &path),
            (false, true) => generate_server_streaming(&ident, &request, &response, &path),
            (true, false) => generate_client_streaming(&ident, &request, &response, &path),
            (true, true) => generate_streaming(&ident, &request, &response, &path),
        };

        stream.extend(generated_method);
    }

    stream
}

fn generate_unary_response_recv_loop(response: &TokenStream) -> TokenStream {
    quote::quote! {
        let mut res_msg = #response::default();
        let mut recv_adapter = ProstRecvMessage(&mut res_msg);
        let mut response_md = tonic::metadata::MetadataMap::new();

        loop {
            let item = rx.recv(&mut recv_adapter).await;
            match item {
                ResponseStreamItem::Message => {},
                ResponseStreamItem::Trailers(t) => {
                    merge_grpc_metadata(&mut response_md, t.metadata());
                    return match t.into_status() {
                        Ok(()) => Ok(tonic::Response::from_parts(response_md, res_msg, tonic::Extensions::new())),
                        Err(err) => Err(status_from_grpc_error(&err, response_md)),
                    };
                }
                ResponseStreamItem::Headers(h) => {
                    merge_grpc_metadata(&mut response_md, h.metadata());
                }
                ResponseStreamItem::StreamClosed => {
                    return Err(tonic::Status::internal("stream closed prematurely"));
                }
            }
        }
    }
}

fn generate_request_setup(
    path: &str,
    is_streaming_req: bool,
) -> TokenStream {
    if is_streaming_req {
        quote::quote! {
            let req = request.into_streaming_request();
            let (grpc_md, deadline) = convert_tonic_metadata_to_grpc(req.metadata());
            let mut req_stream = Box::pin(req.into_inner());
            let headers = RequestHeaders::new().with_method_name(#path).with_metadata(grpc_md);
            let mut options = CallOptions::default();
            if let Some(d) = deadline {
                options.set_deadline(d);
            }
        }
    } else {
        quote::quote! {
            let req = request.into_request();
            let (grpc_md, deadline) = convert_tonic_metadata_to_grpc(req.metadata());
            let req_msg = req.into_inner();
            let headers = RequestHeaders::new().with_method_name(#path).with_metadata(grpc_md);
            let mut options = CallOptions::default();
            if let Some(d) = deadline {
                options.set_deadline(d);
            }
        }
    }
}

fn generate_send_task() -> TokenStream {
    quote::quote! {
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            while let Some(msg) = req_stream.next().await {
                let send_msg = ProstSendMessage(&msg);
                if tx.send(&send_msg, SendOptions::default()).await.is_err() {
                    break;
                }
            }
            drop(tx);
        })
    }
}

fn generate_unary(
    ident: &proc_macro2::Ident,
    request: &TokenStream,
    response: &TokenStream,
    path: &str,
) -> TokenStream {
    let request_setup = generate_request_setup(path, false);
    let response_recv_loop = generate_unary_response_recv_loop(response);

    quote::quote! {
        pub async fn #ident(
            &mut self,
            request: impl tonic::IntoRequest<#request>,
        ) -> std::result::Result<tonic::Response<#response>, tonic::Status> {
            #request_setup

            let (mut tx, rx) = self.inner.invoke(headers, options).await;
            let mut rx = RecvStreamValidator::new(rx, true);

            let send_msg = ProstSendMessage(&req_msg);
            let _ = tx.send(&send_msg, SendOptions::new().with_final_msg(true)).await;

            #response_recv_loop
        }
    }
}

fn generate_server_streaming(
    ident: &proc_macro2::Ident,
    request: &TokenStream,
    response: &TokenStream,
    path: &str,
) -> TokenStream {
    let request_setup = generate_request_setup(path, false);

    quote::quote! {
        pub async fn #ident(
            &mut self,
            request: impl tonic::IntoRequest<#request>,
        ) -> std::result::Result<tonic::Response<GrpcResponseStream<#response, T::RecvStream>>, tonic::Status> {
            #request_setup

            let (mut tx, rx) = self.inner.invoke(headers, options).await;
            let rx = RecvStreamValidator::new(rx, false);

            let send_msg = ProstSendMessage(&req_msg);
            let _ = tx.send(&send_msg, SendOptions::new().with_final_msg(true)).await;

            let (stream, headers_rx) = GrpcResponseStream::new(rx);
            let response_md = headers_rx
                .await
                .map_err(|_| tonic::Status::internal("stream closed before headers"))??;

            Ok(tonic::Response::from_parts(
                response_md,
                stream,
                tonic::Extensions::new(),
            ))
        }
    }
}

fn generate_client_streaming(
    ident: &proc_macro2::Ident,
    request: &TokenStream,
    response: &TokenStream,
    path: &str,
) -> TokenStream {
    let request_setup = generate_request_setup(path, true);
    let response_recv_loop = generate_unary_response_recv_loop(response);
    let send_task = generate_send_task();

    quote::quote! {
        pub async fn #ident(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = #request>,
        ) -> std::result::Result<tonic::Response<#response>, tonic::Status> {
            #request_setup

            let (mut tx, rx) = self.inner.invoke(headers, options).await;
            let mut rx = RecvStreamValidator::new(rx, true);

            let _send_guard = AbortOnDrop(#send_task);

            #response_recv_loop
        }
    }
}

fn generate_streaming(
    ident: &proc_macro2::Ident,
    request: &TokenStream,
    response: &TokenStream,
    path: &str,
) -> TokenStream {
    let request_setup = generate_request_setup(path, true);
    let send_task = generate_send_task();

    quote::quote! {
        pub async fn #ident(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = #request>,
        ) -> std::result::Result<tonic::Response<GrpcResponseStream<#response, T::RecvStream>>, tonic::Status> {
            #request_setup

            let (mut tx, rx) = self.inner.invoke(headers, options).await;
            let rx = RecvStreamValidator::new(rx, false);

            let (stream, headers_rx) = GrpcResponseStream::new(rx);
            let stream = stream.with_send_guard(AbortOnDrop(#send_task));
            let response_md = headers_rx
                .await
                .map_err(|_| tonic::Status::internal("stream closed before headers"))??;

            Ok(tonic::Response::from_parts(
                response_md,
                stream,
                tonic::Extensions::new(),
            ))
        }
    }
}
