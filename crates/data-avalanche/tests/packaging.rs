//! The image, the Helm chart and the release against the workspace (roadmap §4 M11a): the
//! Dockerfile's toolchain, the chart's versions, its example config and its defaults, what the
//! release publishes. `helm lint`, `helm template` and kubeconform check the chart itself in CI,
//! and the E2E suite's k3s scenario runs it.

use std::path::PathBuf;

/// A file of the repository, by its path from the root.
fn repo(file: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The value of the first line `<prefix><value>` of `text`, quotes taken off.
fn value(text: &str, prefix: &str) -> String {
    text.lines()
        .find_map(|l| l.strip_prefix(prefix))
        .unwrap_or_else(|| panic!("no {prefix:?}"))
        .trim_matches('"')
        .to_owned()
}

#[test]
fn the_image_builds_with_the_workspace_s_toolchain() {
    let channel = value(&repo("rust-toolchain.toml"), "channel = ");
    let dockerfile = repo("deploy/docker/Dockerfile");
    assert!(
        dockerfile.contains(&format!("FROM rust:{channel}-trixie AS build\n")),
        "the build stage isn't rust:{channel}"
    );
    assert!(dockerfile.contains("FROM gcr.io/distroless/cc-debian13:nonroot\n"));
}
