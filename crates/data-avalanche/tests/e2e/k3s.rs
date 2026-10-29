//! The Helm chart on Kubernetes (roadmap §4 M11a; the 3-node run moved from M8, Q-M8-4): three
//! pods of the chart's StatefulSet find each other through the headless Service, run one
//! pipeline end to end, and a deleted pod hands it to the others. The cluster is test-infra's
//! `k3s` service, beside PostgreSQL 18 and RustFS on the compose network; the pods reach those by
//! the chart's `hostAliases`, since k3s's DNS doesn't forward to Docker's. Needs
//! `DATA_AVALANCHE_K3S=1` and `test-infra/k3s.sh` (k3s, the image imported).

use std::process::Command;
use std::time::Duration;

use data_avalanche_testkit::{require_it, require_k3s};

use crate::harness::{E2e, Options, eventually, get_json};

const RELEASE: &str = "smoke";
/// The chart's API Service, as the node's port 30080 maps it on the host (compose.yaml).
const API: u16 = 58080;

fn repo(path: &str) -> String {
    format!("{}/../../{path}", env!("CARGO_MANIFEST_DIR"))
}

/// Runs `program` against k3s with `args`; its stdout, or a panic with its stderr.
fn run(program: &str, args: &[&str]) -> String {
    let out = Command::new(program)
        .args(args)
        .env("KUBECONFIG", repo("test-infra/.k3s/kubeconfig.yaml"))
        .output()
        .unwrap_or_else(|e| panic!("{program}: {e}"));
    assert!(
        out.status.success(),
        "{program} {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A compose service's address on the compose network.
fn address(service: &str) -> String {
    let ip = run(
        "docker",
        &[
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
            &format!("data-avalanche-test-{service}-1"),
        ],
    );
    ip.split_whitespace()
        .next()
        .unwrap_or_else(|| panic!("{service} has no address"))
        .to_owned()
}

/// A pod's UID, `None` while it doesn't exist.
fn uid(pod: &str) -> Option<String> {
    let out = Command::new("kubectl")
        .args(["get", "pod", pod, "-o", "jsonpath={.metadata.uid}"])
        .env("KUBECONFIG", repo("test-infra/.k3s/kubeconfig.yaml"))
        .output()
        .ok()?;
    let uid = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (out.status.success() && !uid.is_empty()).then_some(uid)
}

/// Uninstalls the release when the scenario ends, failed or not.
struct Release;

impl Drop for Release {
    fn drop(&mut self) {
        let _ = Command::new("helm")
            .args(["uninstall", RELEASE, "--wait"])
            .env("KUBECONFIG", repo("test-infra/.k3s/kubeconfig.yaml"))
            .output();
    }
}

/// The scenario's config as the pods run it: the source by its service name, the API on every
/// address, the peers from the headless Service, each pod named by itself.
fn in_cluster(config: &str) -> String {
    let mut out = String::new();
    for line in config.lines() {
        let trimmed = line.trim_start();
        let indent = &line[..line.len() - trimmed.len()];
        let line = if trimmed.starts_with("api: ") {
            format!("{indent}api: {{ address: \"0.0.0.0:8080\" }}")
        } else if trimmed == "cluster:" {
            format!(
                "{line}\n{indent}  discovery: dns\n{indent}  discovery_dns_name: ${{DATA_AVALANCHE_DISCOVERY_DNS_NAME}}"
            )
        } else if trimmed.starts_with("memberlist: ") {
            format!(
                "{indent}memberlist: {{ node_name: \"${{POD_NAME}}\", advertise_addr: \"${{POD_IP}}\" }}"
            )
        } else if trimmed == "host: 127.0.0.1" {
            format!("{indent}host: postgres-18")
        } else if trimmed == "port: 55418" {
            format!("{indent}port: 5432")
        } else {
            line.to_owned()
        };
        out.push_str(&line);
        out.push('\n');
    }
    for moved in [
        "0.0.0.0:8080",
        "discovery: dns",
        "${POD_NAME}",
        "host: postgres-18",
        "port: 5432",
    ] {
        assert!(out.contains(moved), "{moved} isn't in:\n{out}");
    }
    out
}

/// The values the scenario adds: where postgres-18 and rustfs are, RustFS as the S3 endpoint.
fn addresses() -> String {
    format!(
        "hostAliases:
  - {{ ip: \"{pg}\", hostnames: [postgres-18] }}
  - {{ ip: \"{rustfs}\", hostnames: [rustfs] }}
env:
  - {{ name: AWS_ACCESS_KEY_ID, value: avalanche }}
  - {{ name: AWS_SECRET_ACCESS_KEY, value: avalanche-secret }}
  - {{ name: AWS_REGION, value: us-east-1 }}
  - {{ name: AWS_ENDPOINT_URL, value: \"http://rustfs:9000\" }}
  - {{ name: AWS_ALLOW_HTTP, value: \"true\" }}
  - {{ name: RUST_BACKTRACE, value: \"1\" }}
",
        pg = address("postgres-18"),
        rustfs = address("rustfs"),
    )
}

/// The members `GET /v1/cluster/status` names, through the API Service.
async fn members() -> Vec<String> {
    let Some((200, v)) = get_json(API, "/v1/cluster/status").await else {
        return vec![];
    };
    v["members"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["node"].as_str().map(str::to_owned))
        .collect()
}

#[tokio::test]
async fn three_pods_run_a_pipeline_and_a_deleted_pod_hands_it_off() {
    require_it!();
    require_k3s!();
    crate::postgres_only!();
    let e = E2e::new(
        "k3s",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items VALUES (1, 'a'), (2, 'b');",
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.yaml");
    std::fs::write(&config, in_cluster(&e.config())).unwrap();
    let more = dir.path().join("addresses.yaml");
    std::fs::write(&more, addresses()).unwrap();

    let release = Release;
    run(
        "helm",
        &[
            "upgrade",
            "--install",
            RELEASE,
            &repo("deploy/helm/data-avalanche"),
            "-f",
            &repo("test-infra/k3s/values.yaml"),
            "-f",
            &more.display().to_string(),
            "--set-file",
            &format!("config={}", config.display()),
            "--wait",
            "--timeout",
            "5m",
        ],
    );
    let pods = [0, 1, 2].map(|i| format!("{RELEASE}-data-avalanche-{i}"));
    eventually("three members", Duration::from_secs(120), async || {
        let mut m = members().await;
        m.sort();
        m == pods
    })
    .await;

    e.converged("items").await;
    let lease = e.lease().await.expect("a lease").doc;
    let holder = lease.holder_node_id;
    assert!(pods.contains(&holder), "{holder}");

    // A deleted pod leaves: it hands the pipeline off before it stops (the chart's grace is
    // longer than the drain), and the StatefulSet starts it again under its name.
    let before = uid(&holder).expect("the holder's pod");
    run("kubectl", &["delete", "pod", &holder, "--wait=false"]);
    eventually(
        "another pod holds it",
        Duration::from_secs(120),
        async || {
            e.lease().await.is_some_and(|l| {
                !l.doc.released
                    && l.doc.holder_node_id != holder
                    && l.doc.owner_epoch > lease.owner_epoch
            })
        },
    )
    .await;
    let taker = e.lease().await.unwrap().doc;
    eprintln!(
        "{holder} (epoch {}) handed off to {} (epoch {})",
        lease.owner_epoch, taker.holder_node_id, taker.owner_epoch
    );
    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1")
        .await;
    e.converged("items").await;
    assert_eq!(e.ids("items").await, [1, 2, 3]);
    eventually(
        "the deleted pod back, a member again",
        Duration::from_secs(180),
        async || uid(&holder).is_some_and(|u| u != before) && members().await.contains(&holder),
    )
    .await;
    run(
        "kubectl",
        &[
            "wait",
            "--for=condition=Ready",
            "pod",
            &holder,
            "--timeout=120s",
        ],
    );
    drop(release);
    e.cleanup().await;
}
