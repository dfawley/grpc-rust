# `grpc-health`

Client-side health checking ([gRFC A17]) for the [`grpc` crate](../grpc).

> NOTE: This version is a preview and not recommended for any production
> use.  All APIs are unstable.  Proceed at your own risk.

Call `grpc_health::init()` once, before creating channels.  Channels whose
service config sets `healthCheckConfig.serviceName` then run the
`grpc.health.v1.Health/Watch` call on each subchannel that an LB policy
watches for health.

The generated code in `generated/` is checked in.  To regenerate it, build
with `GRPC_RUST_REGENERATE_PROTO=1` (see `build.rs`).

[gRFC A17]: https://github.com/grpc/proposal/blob/master/A17-client-side-health-checking.md
