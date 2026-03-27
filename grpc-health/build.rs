fn main() {
    let proto = "../tonic-health/proto/health.proto";

    eprintln!("{}", grpc_protobuf_build::protoc());
    let path = std::env::var("PATH").unwrap_or_default();
    unsafe {
        std::env::set_var("PATH", format!("{}:{}", path, grpc_protobuf_build::bin()));
    }

    grpc_protobuf_build::CodeGen::new()
        .include("../tonic-health/proto")
        .inputs(["health.proto"])
        .output_dir("src/generated")
        .client_only()
        .compile()
        .unwrap();

    // prevent needing to rebuild if files (or deps) haven't changed
    println!("cargo:rerun-if-changed={proto}");
}
