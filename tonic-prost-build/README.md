# tonic-prost-build

Prost build integration for [tonic] gRPC framework.

## Overview

This crate provides code generation for gRPC services using protobuf definitions via the [prost] ecosystem. It bridges [prost-build] with [tonic]'s generic code generation infrastructure.

## Usage

Add to your `build.rs`:

```rust
fn main() {
    tonic_prost_build::configure()
        .compile_protos(&["proto/service.proto"], &["proto"])
        .unwrap();
}
```

### Compiling for the `grpc` Crate Transport

`tonic-prost-build` supports generating client stubs built on top of the `grpc`
crate instead of Tonic's internal transport using `compile_protos_grpc()` or by
adding `.target_grpc()` to the `tonic_prost_build::Builder` created by
`tonic_prost_build::configure()`:

```rust
fn main() {
    // Simple compilation for the `grpc` crate target
    tonic_prost_build::compile_protos_grpc("proto/service.proto").unwrap();
}
```

Or with builder options:

```rust
fn main() {
    tonic_prost_build::configure()
        .target_grpc()
        .build_server(false)
        .compile_protos(&["proto/service.proto"], &["proto"])
        .unwrap();
}
```

## Migrating from Tonic to `grpc`

For detailed instructions on migrating channel construction, build scripts, and Tower middleware from Tonic to the `grpc` crate, see [tonic-migration.md](../tonic-migration.md).

> **Note**: For new projects, the preferred method of using gRPC in Rust is native `grpc` code generation using `grpc-protobuf-build`. For setup instructions, see the [gRPC Rust Documentation](https://grpc.io/docs/languages/rust/).

## Features

- `transport`: Enables transport layer code generation
- `cleanup-markdown`: Enables markdown cleanup in generated documentation

[tonic]: https://github.com/hyperium/tonic
[prost]: https://github.com/tokio-rs/prost
[prost-build]: https://github.com/tokio-rs/prost