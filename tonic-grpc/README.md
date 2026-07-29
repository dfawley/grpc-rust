# tonic-grpc

Bridge crate connecting `tonic` types and the `grpc` transport engine.

## Overview

This crate provides runtime support and adapters for running `tonic` service definitions and client stubs directly over `grpc::client::Channel` transports, including:

- `InterceptedChannel`: Adapter wrapping `grpc::client::Channel` with `tonic::service::Interceptor`.
- `GrpcResponseStream`: Adapter converting `grpc::client::RecvStream` into `tokio_stream::Stream`.
- Metadata & timeout translation helpers between `tonic::metadata` and `grpc::metadata`.
