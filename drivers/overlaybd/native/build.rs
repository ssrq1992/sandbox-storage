fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = "../../../api/overlaybd/v1/backend.proto";
    println!("cargo:rerun-if-changed={source}");
    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .build_server(true)
        .build_client(true)
        .compile_protos(&[source], &["../../../api"])?;
    Ok(())
}
