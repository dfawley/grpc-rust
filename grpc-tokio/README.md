# grpc-tokio

[Tokio](https://tokio.rs) runtime support for [`grpc`](../grpc).

The `grpc` crate is runtime-agnostic. This crate provides the runtime used for
I/O, timers, DNS resolution, and task spawning when running on Tokio, along
with extension traits that add Tokio-backed constructors to `grpc` types:

```rust,ignore
use grpc_tokio::prelude::*;

let channel = grpc::client::Channel::builder(target, credentials).build();
let server = grpc::server::Server::builder().add_service(service).build();
let listener = grpc::server::Server::tcp_listener(addr, creds, config).await?;
```

> NOTE: This version is a preview and not recommended for any production
> use.  All APIs are unstable.  Proceed at your own risk.

## Feature Flags

- `dns` (default): Use [`hickory-resolver`](https://docs.rs/hickory-resolver)
  for DNS resolution, enabling TXT lookups (for service configs) and custom DNS
  authorities.

## License

This project is licensed under the [MIT license](../LICENSE) and/or the [`Apache
license 2.0`](https://www.apache.org/licenses/LICENSE-2.0.txt).
