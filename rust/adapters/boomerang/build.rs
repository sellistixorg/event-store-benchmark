fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Vendored client contract: protos/sellistix/slingshot/v1/event_store.proto
    // (package sellistix.slingshot.v1). SLINGSHOT_PROTO_DIR, when set, replaces
    // that proto root so a Slingshot checkout's protos/ directory can be used
    // instead of the copy in this crate.
    println!("cargo:rerun-if-env-changed=SLINGSHOT_PROTO_DIR");

    let proto_root = std::env::var("SLINGSHOT_PROTO_DIR")
        .ok()
        .filter(|dir| !dir.is_empty())
        .unwrap_or_else(|| "protos".to_string());
    let proto_file = format!("{proto_root}/sellistix/slingshot/v1/event_store.proto");
    println!("cargo:rerun-if-changed={proto_file}");

    // <root> is the include path. google.protobuf.Timestamp is a well-known
    // type; the generated field uses prost-types instead of a second copy.
    tonic_prost_build::configure()
        // Server stubs let unit tests host an in-process EventStoreService.
        .build_server(true)
        .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp")
        .compile_protos(&[&proto_file], &[&proto_root])?;
    Ok(())
}
