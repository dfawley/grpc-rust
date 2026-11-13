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

//! [Tokio](https://tokio.rs) runtime support for [gRPC-Rust](grpc).
//!
//! The `grpc` crate is runtime-agnostic: channels and servers perform all
//! I/O, timers, and task spawning through a pluggable runtime. This crate
//! provides that runtime for Tokio.
//!
//! Use [`channel_builder`], [`server_builder`], and [`tcp_listener`] to
//! create Tokio-backed channels and servers:
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use grpc::credentials::LocalChannelCredentials;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let channel = grpc_tokio::channel_builder(
//!     "dns:///localhost:50051",
//!     Arc::new(LocalChannelCredentials::new()),
//! )
//! .build();
//! # }
//! ```
//!
//! For other APIs that take a runtime, use [`runtime()`].
//!
//! All operations must be performed from within the context of a Tokio
//! runtime with the IO and time drivers enabled.
//!
//! # Feature Flags
//!
//! * `dns` (enabled by default): Uses [hickory-resolver] for DNS resolution,
//!   which supports TXT record lookups (used for service configs) and custom
//!   DNS authorities (e.g. `dns://8.8.8.8/foo.googleapis.com`). Without it,
//!   only host name lookups via the system resolver are supported.
//!
//! [hickory-resolver]: https://docs.rs/hickory-resolver

use std::net::SocketAddr;
use std::sync::Arc;

use grpc::__unstable::rt::GrpcRuntime;
use grpc::client::Channel;
use grpc::client::ChannelBuilder;
use grpc::credentials::ChannelCredentials;
use grpc::credentials::ServerCredentials;
use grpc::server::Http2Config;
use grpc::server::Listener;
use grpc::server::Server;
use grpc::server::builder::ServerBuilder;

#[cfg(feature = "dns")]
mod hickory_resolver;
mod runtime;
#[cfg(unix)]
mod unix;

pub use runtime::TokioRuntime;

/// Returns a [`GrpcRuntime`] backed by Tokio.
pub fn runtime() -> GrpcRuntime {
    GrpcRuntime::new(TokioRuntime::new())
}

/// Creates a new channel builder for the given target and credentials that
/// uses the Tokio runtime.
///
/// See [`Channel::builder`] for details on the arguments.
pub fn channel_builder(
    target: impl Into<String>,
    credentials: Arc<dyn ChannelCredentials>,
) -> ChannelBuilder {
    Channel::builder(target, credentials, runtime())
}

/// Creates a new server builder that uses the Tokio runtime.
///
/// See [`Server::builder`] for details.
pub fn server_builder() -> ServerBuilder {
    Server::builder(runtime())
}

/// Binds a TCP listener to `addr` using the Tokio runtime, with the given
/// server credentials and HTTP/2 configuration.
///
/// See [`Server::tcp_listener`] for details.
pub async fn tcp_listener(
    addr: SocketAddr,
    creds: Arc<dyn ServerCredentials>,
    config: Http2Config,
) -> Result<impl Listener, String> {
    Server::tcp_listener(addr, creds, config, runtime()).await
}
