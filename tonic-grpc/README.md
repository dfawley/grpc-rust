# `tonic-grpc`

`tonic-grpc` is an adapter crate that bridges the core [`grpc`](https://crates.io/crates/grpc) engine crate with [`tonic`](https://crates.io/crates/tonic)'s client stubs.

It exposes `InvokeService<I>`, a Tower [`Service`](https://docs.rs/tower-service) wrapper over any [`grpc::client::Invoke`](file:///usr/local/google/home/dfawley/grpc-rust/tonic-adapter/grpc/src/client/mod.rs#L146) implementation (such as [`grpc::client::Channel`](file:///usr/local/google/home/dfawley/grpc-rust/tonic-adapter/grpc/src/client/channel.rs#L91)). Because Tonic provides a blanket implementation of [`tonic::client::GrpcService`](file:///usr/local/google/home/dfawley/grpc-rust/tonic-adapter/tonic/src/client/service.rs#L37) for Tower services, `InvokeService` can be passed directly to generated Tonic client stubs.

---

## Migration Guide for Tonic Users

### 1. Traditional Tonic Client Setup

Previously, Tonic clients constructed an `http`/`hyper`-based transport channel:

```rust,ignore
use tonic::transport::Channel;
use helloworld::greeter_client::GreeterClient;
use helloworld::HelloRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Standard Tonic channel (Tokio / Hyper based)
    let channel = Channel::from_static("http://[::1]:50051").connect().await?;
    let mut client = GreeterClient::new(channel);

    let response = client
        .say_hello(HelloRequest { name: "Tonic".into() })
        .await?;
    println!("Response: {:?}", response);

    Ok(())
}
```

---

### 2. Migrating to `grpc` Channel + `tonic-grpc` Adapter

To replace Tonic's default transport with the `grpc` core channel and transports while keeping generated Tonic client stubs, wrap the `grpc::client::Channel` with `tonic_grpc::InvokeService`.

#### Option A: Plaintext / Local Credentials

Use `LocalChannelCredentials` for local or unencrypted connections:

```rust,ignore
use std::sync::Arc;
use grpc::client::Channel;
use grpc::credentials::LocalChannelCredentials;
use tonic_grpc::InvokeService;
use helloworld::greeter_client::GreeterClient;
use helloworld::HelloRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create local (plaintext) channel credentials
    let creds = LocalChannelCredentials::new_arc();

    // 2. Construct the core grpc Channel
    let channel = Channel::builder("dns:///localhost:50051", creds).build();

    // 3. Wrap the channel with the tonic-grpc adapter
    let service = InvokeService::new(channel);

    // 4. Pass the adapter service to your generated Tonic client
    let mut client = GreeterClient::new(service);

    let response = client
        .say_hello(HelloRequest { name: "World".into() })
        .await?;
    println!("Response: {:?}", response);

    Ok(())
}
```

#### Option B: TLS Credentials

Use `RustlsChannelCredentials` for encrypted TLS connections:

```rust,ignore
use std::sync::Arc;
use grpc::client::Channel;
use grpc::credentials::rustls::client::{ClientTlsConfig, RustlsChannelCredentials};
use tonic_grpc::InvokeService;
use helloworld::greeter_client::GreeterClient;
use helloworld::HelloRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure Client TLS settings
    let tls_config = ClientTlsConfig::new();

    // 2. Build TLS channel credentials
    let creds = Arc::new(
        RustlsChannelCredentials::new(tls_config)
            .map_err(|e| format!("Failed to create TLS creds: {e}"))?
    );

    // 3. Construct the core grpc Channel with TLS target URI
    let channel = Channel::builder("dns:///example.com:443", creds).build();

    // 4. Wrap the channel with the tonic-grpc adapter
    let service = InvokeService::new(channel);

    // 5. Instantiate generated Tonic client with the adapter
    let mut client = GreeterClient::new(service);

    let response = client
        .say_hello(HelloRequest { name: "TLS User".into() })
        .await?;
    println!("Response: {:?}", response);

    Ok(())
}
```

---

## Important Usage Notes & Limitations

- **Incoming Request Compression**: Compressed incoming request frames (e.g. `client.send_compressed(...)`) are **not supported**. Attempting to send compressed request data will return an `UNIMPLEMENTED` status error.
- **Zero-Copy Payload Handling**: Payload bytes are extracted and framed using `bytes::Bytes` slicing, avoiding re-serialization or unnecessary heap allocations.
- **Tower Compatibility**: `InvokeService` implements `Clone`, `Send`, and `Sync`, making it fully compatible with Tower middleware layers.
