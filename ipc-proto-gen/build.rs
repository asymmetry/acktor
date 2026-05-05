// The protobuf compiler is required to build this crate.

use std::fs;
use std::path::Path;

fn main() {
    let proto_dir = Path::new("src").join("proto");

    let proto_files = [
        "control_message.proto",
        "message.proto",
        "node_message.proto",
        "utils.proto",
    ];

    for file in proto_files.iter() {
        println!("cargo:rerun-if-changed={}", proto_dir.join(file).display());
    }

    let mut config = prost_build::Config::new();
    config.bytes(&["."]);
    config
        .compile_protos(
            &proto_files
                .iter()
                .map(|file| proto_dir.join(file))
                .collect::<Vec<_>>(),
            &[&proto_dir],
        )
        .unwrap();

    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let crate_dir = Path::new(&crate_dir);

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_dir = Path::new(&out_dir);

    // copy the generated files to the source directory

    let rust_dir = Path::new("rust");

    if !rust_dir.exists() {
        fs::create_dir(&rust_dir).unwrap();
    }

    let acktor_ipc_proto_dir = crate_dir
        .join("..")
        .join("acktor-ipc-proto")
        .join("src")
        .join("proto");

    for file in proto_files.iter() {
        let rust_file = Path::new(file).with_extension("rs");

        fs::copy(out_dir.join(&rust_file), rust_dir.join(&rust_file)).unwrap();
        fs::copy(
            out_dir.join(&rust_file),
            acktor_ipc_proto_dir.join(&rust_file),
        )
        .unwrap();
    }
}
