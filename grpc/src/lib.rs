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

//! The official Rust implementation of [gRPC], a high performance, open source,
//! universal RPC framework
//!
//! This version is in progress and not recommended for any production use.  All
//! APIs are unstable.  Proceed at your own risk.
//!
//! [gRPC]: https://grpc.io
#![allow(dead_code, unused_variables)]

pub mod client;
pub mod core;
pub mod credentials;
pub mod inmemory;
pub mod server;

mod macros;
mod status;

pub use client::load_balancing::health_watcher::HealthSender;
pub use client::load_balancing::health_watcher::HealthState;
pub use client::load_balancing::health_watcher::HealthStreamRunner;
pub use client::load_balancing::health_watcher::register_health_stream_runner;
pub use status::ServerStatus;
pub use status::Status;
pub use status::StatusCode;

#[cfg(feature = "experimental_name_resolution")]
pub mod attributes;
#[cfg(not(feature = "experimental_name_resolution"))]
mod attributes;
mod byte_str;
#[cfg(feature = "experimental_runtime")]
pub mod rt;
#[cfg(not(feature = "experimental_runtime"))]
mod rt;
mod send_future;

mod private {
    /// A zero-sized type used to seal methods on a public trait.
    ///
    /// Because this type is private to this crate, it cannot be constructed or
    /// named by external crates. As a result, any method requiring an `Internal`
    /// argument becomes uncallable from outside the crate.
    pub struct Internal;
}

#[cfg(test)]
mod echo_pb {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/generated/grpc_examples_echo.rs"
    ));
}
