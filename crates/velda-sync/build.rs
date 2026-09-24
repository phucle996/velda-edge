fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/sync/v1/sync.proto");
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["../../proto/sync/v1/sync.proto"], &["../../proto"])?;
    Ok(())
}
