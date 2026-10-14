//! The binary's command line, hermetic: what it refuses before touching storage.

use std::process::Command;

fn run(args: &[&str]) -> (Option<i32>, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_data-avalanche"))
        .args(args)
        .output()
        .expect("the binary runs");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn a_missing_config_file_exits_2_naming_it() {
    let (code, err) = run(&["run", "--config", "/nonexistent/da.yaml"]);
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("/nonexistent/da.yaml"), "{err}");
}

#[test]
fn an_invalid_config_exits_2_with_its_rule() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.yaml");
    std::fs::write(
        &path,
        "config_version: 1\nglobals: { state: { bucket: s }, cluster: { lease_duration: 3s, lease_renewal_interval: 2s } }\npipelines: {}\n",
    )
    .unwrap();
    let (code, err) = run(&["run", "--config", path.to_str().unwrap()]);
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("error V-"), "{err}");
}

#[test]
fn a_log_level_that_isnt_a_filter_exits_2() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ok.yaml");
    std::fs::write(
        &path,
        "config_version: 1\nglobals: { state: { bucket: s } }\npipelines: {}\n",
    )
    .unwrap();
    let (code, err) = run(&[
        "run",
        "--config",
        path.to_str().unwrap(),
        "--log-level",
        "info,[",
    ]);
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("--log-level"), "{err}");
}
