use std::path::Path;

fn main() {
    // Keep cfg validation explicit so unknown cfgs fail fast.
    println!("cargo:rustc-check-cfg=cfg(flapjack_dashboard_dist)");
    println!("cargo:rerun-if-changed=../dashboard/dist");
    println!("cargo:rerun-if-changed=../dashboard/dist/index.html");

    if Path::new("../dashboard/dist").is_dir() {
        println!("cargo:rustc-cfg=flapjack_dashboard_dist");
    }
}
