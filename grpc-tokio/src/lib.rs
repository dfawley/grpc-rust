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
//! The simplest way to use it is to import the extension traits from the
//! [`prelude`], which add Tokio-backed constructors to the `grpc` types:
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use grpc::client::Channel;
//! use grpc::credentials::LocalChannelCredentials;
//! use grpc_tokio::prelude::*;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let channel = Channel::builder(
//!     "dns:///localhost:50051",
//!     Arc::new(LocalChannelCredentials::new()),
//! )
//! .build();
//! # }
//! ```
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

use std::future::Future;
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
use grpc::server::interceptor::Intercept;

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

/// Extension traits that add Tokio-backed constructors to `grpc` types.
///
/// Import everything with `use grpc_tokio::prelude::*;`.
pub mod prelude {
    pub use crate::ChannelExt;
    pub use crate::ServerBuilderExt;
    pub use crate::ServerExt;
}

/// Adds Tokio-backed constructors to [`Channel`].
pub trait ChannelExt: private::Sealed {
    /// Creates a new channel builder for the given target and credentials
    /// that uses the Tokio runtime.
    ///
    /// See [`Channel::builder_with_runtime`] for details on the arguments.
    fn builder(
        target: impl Into<String>,
        credentials: Arc<dyn ChannelCredentials>,
    ) -> ChannelBuilder;
}

impl ChannelExt for Channel {
    fn builder(
        target: impl Into<String>,
        credentials: Arc<dyn ChannelCredentials>,
    ) -> ChannelBuilder {
        Channel::builder_with_runtime(target, credentials, runtime())
    }
}

/// Adds Tokio-backed constructors to [`ServerBuilder`].
pub trait ServerBuilderExt: private::Sealed {
    /// Builds the [`Server`] using the Tokio runtime.
    fn build(self) -> Server;
}

impl<I: Intercept + Clone + Send + Sync + 'static> ServerBuilderExt for ServerBuilder<I> {
    fn build(self) -> Server {
        self.build_with_runtime(runtime())
    }
}

/// Adds Tokio-backed constructors to [`Server`].
pub trait ServerExt: private::Sealed {
    /// Binds a TCP listener to `addr` using the Tokio runtime, with the given
    /// server credentials and HTTP/2 configuration.
    fn tcp_listener(
        addr: SocketAddr,
        creds: Arc<dyn ServerCredentials>,
        config: Http2Config,
    ) -> impl Future<Output = Result<impl Listener, String>> + Send;
}

impl ServerExt for Server {
    fn tcp_listener(
        addr: SocketAddr,
        creds: Arc<dyn ServerCredentials>,
        config: Http2Config,
    ) -> impl Future<Output = Result<impl Listener, String>> + Send {
        Server::tcp_listener_with_runtime(addr, creds, config, runtime())
    }
}

mod private {
    pub trait Sealed {}

    impl Sealed for grpc::client::Channel {}
    impl<I> Sealed for grpc::server::builder::ServerBuilder<I> {}
    impl Sealed for grpc::server::Server {}
}
