# Migrating from `tonic` to `grpc`

This document describes how to migrate an existing Tonic application to use the
`grpc` crate as its transport layer while preserving existing message types,
traits, and client call-site signatures.

> [!NOTE]
> The migration methods described below allow for a fast and easy conversion.
> For new projects and complete migrations, it is highly advised to use the
> newer style code generation based on google-protobuf. For detailed
> instructions, please see the [gRPC Rust
> Documentation](https://grpc.io/docs/languages/rust/).

## 1. Migrate Code Generation

> [!IMPORTANT]
> Client code generated below relies on `tonic-grpc`. Ensure `tonic-grpc`
> (and `grpc`) are added to your crate's dependencies.

### tonic-prost-build

#### Simple Compilation

Top-level `compile` calls can be updated to use their `_grpc` variants instead:

```rust
// Before (Tonic transport)
tonic_prost_build::compile_protos("proto/service.proto")?;

// After (grpc crate transport)
tonic_prost_build::compile_protos_grpc("proto/service.proto")?;
```

#### Advanced Builder Configuration

When using the `Builder` for more complex configurations, add `.target_grpc()`
before the compile call:

```rust
// Before
tonic_prost_build::configure()
    .build_server(false)
    .compile_protos(&["proto/service.proto"], &["proto"])?;

// After
tonic_prost_build::configure()
    .target_grpc()
    .build_server(false)
    .compile_protos(&["proto/service.proto"], &["proto"])?;
```

### tonic-build

When using `tonic_build::CodeGenBuilder` directly (e.g., when implementing
custom code generation with the `Service` trait), call `.target_grpc()` on
`CodeGenBuilder` before generating client code:

```rust
// Before (Tonic transport)
let mut builder = tonic_build::CodeGenBuilder::new();
let client_code = builder.generate_client(&service, "super");

// After (grpc crate transport)
let mut builder = tonic_build::CodeGenBuilder::new();
builder.target_grpc();
let client_code = builder.generate_client(&service, "super");
```

## 2. Migrate Channel Construction

The generated client struct `MyServiceClient<T>` now expects a channel
implementing `grpc::client::Invoke` (such as `grpc::client::Channel`) instead of
`tonic::transport::Channel`.

### Local / Plaintext Channels

```rust
// Before (Tonic)
let channel = tonic::transport::Endpoint::from_static("http://localhost:50051")
    .connect()
    .await?;
let mut client = MyServiceClient::new(channel);

// After (grpc crate)
let channel = grpc::client::Channel::builder(
    "dns:///localhost:50051",
    grpc::credentials::LocalChannelCredentials::new_arc(),
).build();
let mut client = MyServiceClient::new(channel);
```

### TLS Channel

```rust
// Before (Tonic)
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

let pem = std::fs::read_to_string("ca.pem")?;
let ca = Certificate::from_pem(pem);
let tls = ClientTlsConfig::new().ca_certificate(ca).domain_name("example.com");
let channel = Endpoint::from_static("https://localhost:50051")
    .tls_config(tls)?
    .connect()
    .await?;

// After (grpc crate)
use grpc::credentials::rustls::{ClientTlsConfig, RootCertificates, RustlsChannelCredentials, StaticProvider};

let pem = std::fs::read_to_string("ca.pem")?;
let root_certs = RootCertificates::from_pem(pem);
let creds = RustlsChannelCredentials::new(
    ClientTlsConfig::new().with_root_certificates_provider(StaticProvider::new(root_certs)),
)?;

let channel = grpc::client::Channel::builder("dns:///localhost:50051", Arc::new(creds))
    .authority("example.com")
    .build();
```

## 3. Migrate Tonic Interceptors

Tonic interceptors used for request metadata manipulation or authentication
(such as injecting auth headers or checking metadata) are fully supported with
`grpc::client::Channel`.

### Client-Level Interceptors

You can attach interceptors to a specific service client using `with_interceptor`:

```rust
fn auth_interceptor(mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    req.metadata_mut().insert(
        "authorization",
        "Bearer my-auth-token".parse().unwrap(),
    );
    Ok(req)
}

let channel = grpc::client::Channel::builder("dns:///localhost:50051", creds).build();
let mut client = MyServiceClient::with_interceptor(channel, auth_interceptor);
```

### Channel-Level Interceptors (For All Services)

To apply interceptors to an entire `grpc` channel so that it automatically runs
for all service clients created from that channel, wrap the channel with
`tonic_grpc::InterceptedChannel`:

```rust
use tonic_grpc::InterceptedChannel;

let raw_channel = grpc::client::Channel::builder("dns:///localhost:50051", creds).build();
let channel = InterceptedChannel::new(raw_channel, auth_interceptor);

// All service clients using `channel` will run `auth_interceptor` on every request:
let mut client1 = Service1Client::new(channel.clone());
let mut client2 = Service2Client::new(channel);
```

### Tower Middleware Limitations & Migration

> [!WARNING]
> **Tower layers using `tonic::body::Body` or `http::Request<Body>` are NOT
> supported**.
>
> Tonic Tower middleware operates on HTTP/2 body streams
> (`http::Request<tonic::body::Body>`).  Because `grpc::client::Channel`
> encapsulates HTTP/2 operations internally as an implementation detail, it does
> **not** expose HTTP/2 body streams.
>
> Existing Tower HTTP middleware (such as `tower-http` layers, HTTP
> tracing/logging, or custom `tower::Service<http::Request<Body>>`
> implementations) must be migrated manually:
>
> 1. **If accessing request metadata only**:
>    - Rewrite as a `grpc::client::Intercept` interceptor, OR
>    - Rewrite as a `tonic::service::Interceptor`.
>
> 2. **For advanced transport/stream interception**:
>    - Must be rewritten as a `grpc::client::Intercept` interceptor.
