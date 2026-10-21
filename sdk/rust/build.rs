fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_build::configure().build_server(false)
        .compile_protos(&["../../api/ssf/v1/node.proto"], &["../../api"])?;
    println!("cargo:rerun-if-changed=../../api/ssf/v1/node.proto");
    Ok(())
}
