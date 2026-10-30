//! The image, the Helm chart and the release against the workspace (roadmap §4 M11a): the
//! Dockerfile's toolchain, the chart's versions, its example config and its defaults, what the
//! release publishes. `helm lint`, `helm template` and kubeconform check the chart itself in CI,
//! and the E2E suite's k3s scenario runs it.

use std::collections::BTreeMap;
use std::path::PathBuf;

use data_avalanche_config::{FingerprintKey, load_str, validate_str};

/// A file of the repository, by its path from the root.
fn repo(file: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn chart(file: &str) -> String {
    repo(&format!("deploy/helm/data-avalanche/{file}"))
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

#[test]
fn the_image_s_build_context_holds_every_workspace_member() {
    // `.dockerignore` lets in only what it names, and cargo reads every member's manifest even
    // to build one package: a member left out fails the image's build.
    let members = value(&repo("Cargo.toml"), "members = ");
    let admitted: Vec<String> = repo(".dockerignore").lines().map(str::to_owned).collect();
    for member in members.trim_matches(['[', ']']).split(',') {
        let member = member.trim().trim_matches('"');
        let dir = member.split('/').next().unwrap();
        assert!(
            admitted.contains(&format!("!{dir}/")),
            "the member {member} isn't in the image's build context (.dockerignore)"
        );
    }
}

#[test]
fn the_chart_s_versions_are_the_workspace_s() {
    // The release refuses a tag unless all three agree (.github/workflows/release.yml).
    let workspace = value(&repo("Cargo.toml"), "version = ");
    let chart = chart("Chart.yaml");
    assert_eq!(value(&chart, "version: "), workspace);
    assert_eq!(value(&chart, "appVersion: "), workspace);
    assert_eq!(workspace, env!("CARGO_PKG_VERSION"));
}

/// The value of a top-level key written as a block: the lines under it, two spaces taken off.
fn block(values: &str, key: &str) -> String {
    let start = format!("{key}:");
    let mut lines = values.lines().skip_while(|l| *l != start).skip(1);
    let mut text = String::new();
    for line in lines.by_ref() {
        if !line.is_empty() && !line.starts_with(' ') {
            break;
        }
        text.push_str(line.strip_prefix("  ").unwrap_or(line));
        text.push('\n');
    }
    assert!(!text.trim().is_empty(), "no {key} block");
    text
}

/// What the chart puts in every pod's environment, and the secrets the examples' configs name:
/// a password, and the API's certificate and key as PEM text, made for the test.
fn pod_env() -> BTreeMap<String, String> {
    let tls = rcgen::generate_simple_self_signed(vec!["data-avalanche".into()]).unwrap();
    [
        ("POD_NAME", "data-avalanche-0".to_owned()),
        ("POD_NAMESPACE", "cdc".to_owned()),
        ("POD_IP", "10.0.0.5".to_owned()),
        (
            "DATA_AVALANCHE_DISCOVERY_DNS_NAME",
            "data-avalanche-headless.cdc.svc.cluster.local".to_owned(),
        ),
        ("ORDERS_DB_PASSWORD", "secret".to_owned()),
        ("API_TLS_CERT", tls.cert.pem()),
        ("API_TLS_KEY", tls.signing_key.serialize_pem()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

#[test]
fn the_examples_configs_validate_with_the_pod_s_environment() {
    for example in ["ci/example-values.yaml", "ci/tls-values.yaml"] {
        let config = block(&chart(example), "config");
        let report = validate_str(&config, &pod_env());
        assert!(report.is_ok(), "{example}: {:#?}\n{config}", report.errors);
    }
}

#[test]
fn the_chart_sets_the_environment_its_values_document() {
    let statefulset = chart("templates/statefulset.yaml");
    let values = chart("values.yaml");
    for name in [
        "POD_NAME",
        "POD_NAMESPACE",
        "POD_IP",
        "DATA_AVALANCHE_DISCOVERY_DNS_NAME",
    ] {
        assert!(statefulset.contains(&format!("- name: {name}\n")), "{name}");
        assert!(values.contains(name), "values.yaml doesn't document {name}");
    }
}

#[test]
fn a_deleted_pod_has_longer_than_the_drain_to_hand_off() {
    let config = block(&chart("ci/example-values.yaml"), "config");
    let loaded = load_str(&config, &pod_env(), &FingerprintKey::from_bytes([0; 32])).unwrap();
    let drain = loaded.config.globals.cluster.drain_timeout;
    let grace: u64 = value(&chart("values.yaml"), "terminationGracePeriodSeconds: ")
        .parse()
        .unwrap();
    assert!(grace > drain.as_secs(), "{grace} s, the drain {drain:?}");
}

#[test]
fn a_rollout_waits_for_the_pipelines_to_come_back() {
    // A new pod gets its pipelines back once it has been a member for rebalance_stabilization;
    // the next pod rolls only after that (M11b).
    let config = block(&chart("ci/example-values.yaml"), "config");
    let loaded = load_str(&config, &pod_env(), &FingerprintKey::from_bytes([0; 32])).unwrap();
    let settle = loaded.config.globals.cluster.rebalance_stabilization;
    let ready: u64 = value(&chart("values.yaml"), "minReadySeconds: ")
        .parse()
        .unwrap();
    assert!(
        ready > settle.as_secs(),
        "{ready} s, the stabilization {settle:?}"
    );
    assert!(
        chart("templates/statefulset.yaml")
            .contains("minReadySeconds: {{ .Values.minReadySeconds }}")
    );
}

#[test]
fn a_changed_config_is_said_to_go_through_the_reload() {
    // A starting node adopts the cluster's config generation, not its file (config §12): the
    // chart restarts nothing on a changed ConfigMap, and tells the operator so.
    for text in [chart("templates/NOTES.txt"), chart("README.md")] {
        assert!(text.contains("POST /v1/config/reload"), "{text}");
    }
    assert!(!chart("templates/statefulset.yaml").contains("checksum/"));
}

#[test]
fn the_release_publishes_the_image_the_chart_pulls() {
    let release = repo(".github/workflows/release.yml");
    let pulled = value(&chart("values.yaml"), "  repository: ");
    let owner = pulled
        .strip_prefix("ghcr.io/")
        .and_then(|r| r.strip_suffix("/data-avalanche"))
        .unwrap_or_else(|| panic!("{pulled}"));
    assert_eq!(owner, "alex-karpenko");
    assert!(release.contains("IMAGE: ghcr.io/${{ github.repository_owner }}/data-avalanche\n"));
    assert!(release.contains("CHARTS: oci://ghcr.io/${{ github.repository_owner }}/charts\n"));
    // Each platform on a runner of its own architecture, as the weekly arm64 build.
    for (platform, runner) in [
        ("linux/amd64", "ubuntu-24.04"),
        ("linux/arm64", "ubuntu-24.04-arm"),
    ] {
        assert!(
            release.contains(&format!(
                "- platform: {platform}\n            runner: {runner}\n"
            )),
            "{platform}"
        );
    }
}
