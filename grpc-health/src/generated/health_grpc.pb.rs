/// Generated client implementations.
pub mod health_client {
    use grpc::client::*;
    use grpc_protobuf::*;
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
            ReqMsgView: protobuf::AsView<Proxied = super::HealthCheckRequest> + Send
                + Sync,
        {
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
            ReqMsgView: protobuf::AsView<Proxied = super::HealthCheckRequest> + Send
                + Sync,
        {
            ServerStreamingCallBuilder::new(
                &self.channel,
                "/grpc.health.v1.Health/Watch",
                request,
            )
        }
    }
}
