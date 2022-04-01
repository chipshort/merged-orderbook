fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/service.proto")?;
    // tonic_build::configure()
    //     .build_client(false)
    //     .compile(&["proto/service.proto"], &["proto"])?;
    Ok(())
}