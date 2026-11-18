/*
 *
 * Copyright 2026 gRPC authors.
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

//! Client-side health checking ([gRFC A17]) for the [`grpc`] crate.
//!
//! grpc's health producer runs the `grpc.health.v1.Health/Watch` call on each
//! subchannel when a health check service name is configured
//! (`healthCheckConfig.serviceName` in the service config).  grpc doesn't
//! depend on protobuf, so the call itself is made by this crate.  Call
//! [`init`] once, before creating channels, to enable it.
//!
//! > NOTE: This version is a preview and not recommended for any production
//! > use.  All APIs are unstable.  Proceed at your own risk.
//!
//! [gRFC A17]: https://github.com/grpc/proposal/blob/master/A17-client-side-health-checking.md

use std::sync::Arc;

use grpc::__internal::client::load_balancing::health::HealthReporter;
use grpc::__internal::client::load_balancing::health::HealthWatchClient;
use grpc::__internal::client::load_balancing::health::ServingStatus;
use grpc::__internal::client::load_balancing::health::set_health_watch_client;
use grpc::__unstable::client::boxed::ArcInvoker;
use grpc::client::Invoke;

#[allow(unused, missing_docs, unreachable_pub, clippy::all)]
mod generated {
    grpc::include_generated_proto!("generated/grpc/health/v1", "health");
}

use generated::HealthCheckRequest;
use generated::health_check_response::ServingStatus as ProtoServingStatus;
use generated::health_client::HealthClient;

/// Enables client-side health checking in grpc channels.
pub fn init() {
    set_health_watch_client(Arc::new(WatchClient));
}

/// Makes Watch calls for grpc's health producer.
struct WatchClient;

#[grpc::async_trait]
impl HealthWatchClient for WatchClient {
    async fn watch(
        &self,
        subchannel: ArcInvoker,
        service: String,
        reporter: HealthReporter,
    ) -> Result<(), grpc::StatusError> {
        watch(subchannel, service, |status| reporter.report(status)).await
    }
}

/// Runs one Watch call for `service` on `channel`, calling `report` with the
/// status in each response, and returns the call's final status.
async fn watch(
    channel: impl Invoke,
    service: String,
    mut report: impl FnMut(ServingStatus) + Send,
) -> Result<(), grpc::StatusError> {
    let client = HealthClient::new(channel);
    let mut request = HealthCheckRequest::new();
    request.set_service(service);
    let mut responses = client.watch(request).await;
    while let Some(response) = responses.recv().await {
        report(serving_status(response.status()));
    }
    responses.status().await.map_err(|err| {
        grpc::StatusError::new(
            grpc::StatusCodeError::from(err.code() as i32),
            err.message(),
        )
    })
}

fn serving_status(status: ProtoServingStatus) -> ServingStatus {
    match status {
        ProtoServingStatus::Serving => ServingStatus::Serving,
        ProtoServingStatus::NotServing => ServingStatus::NotServing,
        ProtoServingStatus::ServiceUnknown => ServingStatus::ServiceUnknown,
        _ => ServingStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Mutex;

    use grpc::__unstable::rt::address::TcpAddress;
    use grpc::client::Channel;
    use grpc::credentials::LocalChannelCredentials;
    use grpc::credentials::LocalServerCredentials;
    use grpc::server::Http2Config;
    use grpc::server::Listener;
    use grpc::server::Server;
    use grpc_protobuf::ServerStatus;
    use grpc_protobuf::ServerStatusError;
    use grpc_protobuf::StatusCodeError;
    use grpc_protobuf::server::GrpcStreamingResponse;
    use protobuf::proto;

    use super::*;
    use crate::generated::HealthCheckRequestView;
    use crate::generated::HealthCheckResponse;
    use crate::generated::health_server::Health;
    use crate::generated::health_server::HealthServer;

    // Sends each status in turn, then ends the call with `end`.
    struct FakeHealth {
        statuses: Vec<ProtoServingStatus>,
        end: Option<StatusCodeError>,
        services: Mutex<Vec<String>>,
    }

    #[grpc::async_trait]
    impl Health for FakeHealth {
        async fn watch(
            &self,
            request: HealthCheckRequestView<'_>,
            mut responses: GrpcStreamingResponse<'_, HealthCheckResponse>,
        ) -> ServerStatus {
            self.services
                .lock()
                .unwrap()
                .push(request.service().to_string());
            for status in &self.statuses {
                let status = *status;
                if responses
                    .send(proto!(HealthCheckResponse { status }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            match self.end {
                None => Ok(()),
                Some(code) => Err(ServerStatusError::new(code, "ended")),
            }
        }
    }

    async fn start_server(service: FakeHealth) -> (Arc<FakeHealth>, String) {
        let service = Arc::new(service);
        let server = Server::builder()
            .add_service(HealthServer::from_arc(service.clone()))
            .build();
        let listener = Server::tcp_listener(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(LocalServerCredentials::new()),
            Http2Config::default(),
        )
        .await
        .unwrap();
        let addr = listener.local_addr();
        let addr = (&*addr as &dyn Any)
            .downcast_ref::<TcpAddress>()
            .expect("tcp listener")
            .0;
        tokio::spawn(async move { server.serve(listener).await });
        (service, format!("dns:///{addr}"))
    }

    #[tokio::test]
    async fn reports_statuses_and_final_status() {
        let (service, target) = start_server(FakeHealth {
            statuses: vec![
                ProtoServingStatus::Serving,
                ProtoServingStatus::NotServing,
                ProtoServingStatus::ServiceUnknown,
                ProtoServingStatus::Unknown,
            ],
            end: Some(StatusCodeError::Unavailable),
            services: Mutex::new(Vec::new()),
        })
        .await;
        let channel = Channel::builder(&target, Arc::new(LocalChannelCredentials::new())).build();

        let mut reported = Vec::new();
        let result = watch(channel, "svc".to_string(), |s| reported.push(s)).await;

        assert_eq!(
            reported,
            vec![
                ServingStatus::Serving,
                ServingStatus::NotServing,
                ServingStatus::ServiceUnknown,
                ServingStatus::Unknown,
            ]
        );
        let err = result.unwrap_err();
        assert_eq!(err.code(), grpc::StatusCodeError::Unavailable);
        assert_eq!(*service.services.lock().unwrap(), vec!["svc".to_string()]);
    }

    #[tokio::test]
    async fn unimplemented() {
        // The default Health implementation returns UNIMPLEMENTED.
        struct NoWatch;
        #[grpc::async_trait]
        impl Health for NoWatch {}

        let server = Server::builder()
            .add_service(HealthServer::new(NoWatch))
            .build();
        let listener = Server::tcp_listener(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(LocalServerCredentials::new()),
            Http2Config::default(),
        )
        .await
        .unwrap();
        let addr = listener.local_addr();
        let addr = (&*addr as &dyn Any).downcast_ref::<TcpAddress>().unwrap().0;
        tokio::spawn(async move { server.serve(listener).await });
        let channel = Channel::builder(
            format!("dns:///{addr}"),
            Arc::new(LocalChannelCredentials::new()),
        )
        .build();

        let result = watch(channel, String::new(), |_| panic!("no responses")).await;
        assert_eq!(
            result.unwrap_err().code(),
            grpc::StatusCodeError::Unimplemented
        );
    }
}
