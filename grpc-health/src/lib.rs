use grpc::HealthSender;
use grpc::HealthStreamRunner;
use grpc::client::Invoke;

#[allow(unused)]
mod generated {
    pub mod health {
        include!("generated/generated.rs");
        include!("generated/health_grpc.pb.rs");
    }
}

fn start() {
    grpc::register_health_stream_runner(StreamRunner {});
}

struct StreamRunner {}

impl HealthStreamRunner for StreamRunner {
    async fn run(invoker: impl Invoke, updates: HealthSender) {
        todo!()
    }
}
