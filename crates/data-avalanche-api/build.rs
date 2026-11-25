//! Embeds the console's app, `ui/dist` as `npm run build` in `ui/` leaves it, into the binary
//! (Admin_Console §10). Without it the console says its app isn't built, so Rust-only work needs no
//! Node; `DATA_AVALANCHE_UI=required` (CI, the image) refuses to build without it.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

fn main() {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets it"));
    let ui = manifest.join("../../ui");
    let dist = ui.join("dist");
    println!("cargo::rerun-if-env-changed=DATA_AVALANCHE_UI");
    let mut files = Vec::new();
    if dist.join("index.html").is_file() {
        println!("cargo::rerun-if-changed={}", dist.display());
        walk(&dist, &dist, &mut files);
    } else {
        // A path that doesn't exist reruns the script at every build: `ui/` holds `dist` once
        // it is built.
        println!("cargo::rerun-if-changed={}", ui.display());
    }
    if files.is_empty() && std::env::var("DATA_AVALANCHE_UI").as_deref() == Ok("required") {
        panic!(
            "DATA_AVALANCHE_UI=required, but {} holds no index.html: run `npm ci && npm run build` in ui/",
            dist.display()
        );
    }
    files.sort();
    let mut out = String::from("&[\n");
    for (path, file) in &files {
        writeln!(
            out,
            "    AppFile {{ path: {path:?}, body: include_bytes!({:?}) }},",
            file.display()
        )
        .expect("a String takes it");
    }
    out.push(']');
    let target = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets it")).join("app.rs");
    std::fs::write(target, out).expect("OUT_DIR is writable");
}

/// Every file under `dir`, as its path from `root` with `/` between names, and where it is.
fn walk(root: &Path, dir: &Path, files: &mut Vec<(String, PathBuf)>) {
    for entry in std::fs::read_dir(dir).expect("ui/dist is readable") {
        let path = entry.expect("ui/dist is readable").path();
        if path.is_dir() {
            walk(root, &path, files);
        } else {
            let name = path
                .strip_prefix(root)
                .expect("under ui/dist")
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            files.push((name, path.canonicalize().expect("a file found exists")));
        }
    }
}
