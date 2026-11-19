# gRPC-Rust

The official Rust implementation of [gRPC], a high performance, open source,
universal RPC framework.

> NOTE: This version is a preview and not recommended for any production
> use.  All APIs are unstable.  Proceed at your own risk.

## Documentation, Examples, and Getting Started

Please see [our website] for everything you should need to get started using
gRPC!

[our website]: https://grpc.io/docs/languages/rust

### Rust Version

`grpc`'s MSRV is `1.88`.

### Project Layout

- `grpc` (this crate): The core gRPC implementation.
- [`grpc-tokio`]: Tokio runtime support.
- [`protoc-gen-grpc-rust`]: The protobuf code generator, used with [`protoc`].
- [`grpc-protobuf-build`]: Build integration for grpc protobuf code generation.
- [`grpc-protobuf`]: Implementation of protobuf-over-grpc used by the generated code.

## License

This project is licensed under the [MIT license] and/or the
[`Apache license 2.0`].

### Contributing

If you'd like to contribute, please see our project's
[`CONTRIBUTING.md`] for some guidelines.

[MIT license]: https://github.com/grpc/grpc-rust/blob/master/LICENSE
[`Apache license 2.0`]: https://www.apache.org/licenses/LICENSE-2.0.txt
[`CONTRIBUTING.md`]: https://github.com/grpc/grpc-rust/blob/master/CONTRIBUTING.md
[gRPC]: https://grpc.io
[`protoc`]: https://protobuf.dev/installation/
[`protoc-gen-grpc-rust`]: https://docs.rs/protoc-gen-grpc-rust
[`grpc-protobuf-build`]: https://docs.rs/grpc-protobuf-build
[`grpc-tokio`]: https://docs.rs/grpc-tokio
[`grpc-protobuf`]: https://docs.rs/grpc-protobuf
