/// Generated client implementations.
pub mod health_client {
    #![allow(
        unused_imports,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
    )]
    use grpc::client::*;
    use grpc_protobuf::*;
    use grpc_protobuf::client::*;

    #[derive(Debug, Clone)]
    pub struct HealthClient<T> {
        channel: T,
    }

    impl<T> HealthClient<T>
    where
        T: grpc::client::Invoke,
    {
        pub fn new(channel: T) -> Self {
            Self { channel }
        }

        /// If the requested service is unknown, the call will fail with status
        /// NOT\_FOUND.
        pub fn check<ReqMsgView>(
            &self,
            request: ReqMsgView,
        ) -> UnaryCallBuilder<'_, &T, ReqMsgView, super::HealthCheckResponse>
        where
          ReqMsgView: protobuf::AsView<Proxied = super::HealthCheckRequest> + Send + Sync {
          UnaryCallBuilder::new(&self.channel, "/grpc.health.v1.Health/Check", request)
        }

        /// Performs a watch for the serving status of the requested service.
        /// The server will immediately send back a message indicating the current
        /// serving status.  It will then subsequently send a new message whenever
        /// the service's serving status changes.
        ///
        /// If the requested service is unknown when the call is received, the
        /// server will send a message setting the serving status to
        /// SERVICE\_UNKNOWN but will \*not\* terminate the call.  If at some
        /// future point, the serving status of the service becomes known, the
        /// server will send a new message with the service's serving status.
        ///
        /// If the call terminates with status UNIMPLEMENTED, then clients
        /// should assume this method is not supported and should not retry the
        /// call.  If the call terminates with any other status (including OK),
        /// clients should retry the call with appropriate exponential backoff.
        pub fn watch<ReqMsgView>(
            &self,
            request: ReqMsgView,
        ) -> ServerStreamingCallBuilder<'_, &T, ReqMsgView, super::HealthCheckResponse>
        where
          ReqMsgView: protobuf::AsView<Proxied = super::HealthCheckRequest> + Send + Sync {
          ServerStreamingCallBuilder::new(&self.channel, "/grpc.health.v1.Health/Watch", request)
        }
    }
}

/// Generated server implementations.
pub mod health_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
    )]

    mod method_wrappers {
        pub(super) struct Check<T> { pub(super) service: std::sync::Arc<T> }
        impl<T: super::Health> grpc_protobuf::server::UnaryMethod for Check<T> {
            type Request = super::super::HealthCheckRequest;
            type Response = super::super::HealthCheckResponse;

            async fn call(
                &self,
                request: <Self::Request as protobuf::Proxied>::View<'_>,
                response: <Self::Response as protobuf::MutProxied>::Mut<'_>,
            ) -> grpc_protobuf::ServerStatus {
                self.service.check(request, response).await
            }
        }

        pub(super) struct Watch<T> { pub(super) service: std::sync::Arc<T> }
        impl<T: super::Health> grpc_protobuf::server::ServerStreamingMethod for Watch<T> {
            type Request = super::super::HealthCheckRequest;
            type Response = super::super::HealthCheckResponse;

            async fn call(
                &self,
                request: <Self::Request as protobuf::Proxied>::View<'_>,
                responses: grpc_protobuf::server::GrpcStreamingResponse<'_, super::super::HealthCheckResponse>,
            ) -> grpc_protobuf::ServerStatus {
                self.service.watch(request, responses).await
            }
        }
    }

    /// Generated trait containing gRPC methods that should be implemented for use with HealthServer.

    #[grpc::async_trait]
    pub trait Health : std::marker::Send + std::marker::Sync + 'static {
        /// If the requested service is unknown, the call will fail with status
/// NOT\_FOUND.

        async fn check(
            &self, 
            request: super::HealthCheckRequestView<'_>,
            response: super::HealthCheckResponseMut<'_>,
        ) -> grpc_protobuf::ServerStatus {
            Err(grpc_protobuf::ServerStatusError::new(grpc_protobuf::StatusCodeError::Unimplemented, "Not yet implemented"))
        }

        /// Performs a watch for the serving status of the requested service.
/// The server will immediately send back a message indicating the current
/// serving status.  It will then subsequently send a new message whenever
/// the service's serving status changes.
///
/// If the requested service is unknown when the call is received, the
/// server will send a message setting the serving status to
/// SERVICE\_UNKNOWN but will \*not\* terminate the call.  If at some
/// future point, the serving status of the service becomes known, the
/// server will send a new message with the service's serving status.
///
/// If the call terminates with status UNIMPLEMENTED, then clients
/// should assume this method is not supported and should not retry the
/// call.  If the call terminates with any other status (including OK),
/// clients should retry the call with appropriate exponential backoff.

        async fn watch(
            &self, 
            request: super::HealthCheckRequestView<'_>,
            responses: grpc_protobuf::server::GrpcStreamingResponse<'_, super::HealthCheckResponse>,
        ) -> grpc_protobuf::ServerStatus {
            Err(grpc_protobuf::ServerStatusError::new(grpc_protobuf::StatusCodeError::Unimplemented, "Not yet implemented"))
        }
    }

    #[derive(Debug)]
    pub struct HealthServer<T> {
        inner: std::sync::Arc<T>,
    }

    impl<T> HealthServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(std::sync::Arc::new(inner))
        }

        pub fn from_arc(inner: std::sync::Arc<T>) -> Self {
            Self {
                inner,
            }
        }
    }

    impl<T: Health> grpc::server::service::Service for HealthServer<T> {
        fn descriptor(&self) -> grpc::server::descriptor::ServiceDescriptor {
            grpc::server::descriptor::ServiceDescriptor::new(
                "grpc.health.v1.Health",
                vec![
    grpc::server::descriptor::MethodDescriptor::new("/grpc.health.v1.Health/Check"),
    grpc::server::descriptor::MethodDescriptor::new("/grpc.health.v1.Health/Watch"),
    ]
    )
    }

    fn register_methods(self) -> std::vec::Vec<(std::string::String, std::sync::Arc<dyn grpc::server::DynHandle>)> {
    vec![
    (
        "/grpc.health.v1.Health/Check".to_string(),
        std::sync::Arc::new(grpc_protobuf::server::UnaryAdapter::new(method_wrappers::Check {
            service: self.inner.clone(),
        })),
    ),
    (
        "/grpc.health.v1.Health/Watch".to_string(),
        std::sync::Arc::new(grpc_protobuf::server::ServerStreamingAdapter::new(method_wrappers::Watch {
            service: self.inner.clone(),
        })),
    ),
    ]
    }
    }
}
