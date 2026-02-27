use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/streaming-data-types/*");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new(
            "src/streaming-data-types/schemas/ev44_events.fbs",
        )],
        out_dir: Path::new("src/flatbuffers_generated/"),
        ..Default::default()
    })
    .expect("cannot find flatc compiler");
}
