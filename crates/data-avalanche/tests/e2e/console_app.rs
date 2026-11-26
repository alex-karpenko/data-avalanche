//! The console's app in a browser (Admin_Console §10; M15, M16a, M16b): Playwright's flows
//! (`ui/e2e`) against a node under each `globals.api.auth` mode, the console switched off, the
//! pipelines' pages and their actions, the Cluster pages and a node's actions, and the page
//! budgets (§8) with 50 pipelines on three nodes. Needs
//! `DATA_AVALANCHE_BROWSER=1`, the app built into the binary (`npm ci && npm run build` in `ui/`,
//! then the binary) and Playwright's Chromium (`npx playwright install chromium` in `ui/`);
//! without the variable each skips itself.

use std::path::PathBuf;
use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::console_sessions::session_key;
use crate::harness::{Doc, E2e, Options, Proc, eventually};
use crate::idp::{Idp, Provider};
use crate::nodes::{Cluster, ITEMS, MORE};

/// The pipelines of the budget's cluster (Admin_Console §8).
const PIPELINES: usize = 50;

/// `ops:s3cret`, the `basic` user of the flows, as an `Authorization` header.
const OPS: (&str, &str) = ("Authorization", "Basic b3BzOnMzY3JldA==");

/// Returns early unless the browser's flows are asked for.
macro_rules! require_browser {
    () => {
        if std::env::var("DATA_AVALANCHE_BROWSER").as_deref() != Ok("1") {
            eprintln!("skipped: set DATA_AVALANCHE_BROWSER=1 to run the console in a browser");
            return;
        }
    };
}

/// Runs `spec`'s flows (under `ui/e2e`) against the node at `port`, with `env`.
async fn playwright(spec: &str, port: u16, env: &[(&str, &str)]) {
    let ui = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../ui");
    let status = tokio::process::Command::new("npx")
        .args(["playwright", "test", spec])
        .current_dir(&ui)
        .env("DA_CONSOLE_URL", format!("http://127.0.0.1:{port}"))
        .envs(env.iter().copied())
        .status()
        .await
        .expect("npx runs");
    assert!(status.success(), "{spec}: the browser's flows failed");
}

/// `globals.console`: on, with a session key made at run time and `more`.
fn console(more: &str) -> String {
    format!(
        r#"console: {{ enabled: true, session_key: "{}"{more} }}"#,
        session_key()
    )
}

/// A node running `test`'s pipeline, its rows landed, with `globals.api` entries `api` and the
/// `globals` line given.
async fn node(test: &str, api: &str, globals: &str) -> (E2e, Proc) {
    let e = E2e::new(
        test,
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            api,
            globals,
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.insert_series("items", 5, Doc::Row).await;
    e.converged("items").await;
    (e, p)
}

/// `basic`: the sign-in form and back to the page asked for; the Overview live as a pause
/// happens, the anti-forgery token required; the sign-out; a session that no longer opens.
#[tokio::test]
async fn basic_sign_in_and_the_live_overview_in_a_browser() {
    require_it!();
    require_browser!();
    let (e, p) = node(
        "console_app_basic",
        ", auth: basic, user: ops, password: s3cret",
        &console(""),
    )
    .await;
    let env = [
        ("DA_USER", "ops"),
        ("DA_PASSWORD", "s3cret"),
        ("DA_PIPELINE", e.test.as_str()),
    ];
    playwright("basic.spec.ts", p.port(), &env).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `oauth2`: the provider's sign-in and back; the sign-out taken on to the provider.
#[tokio::test]
async fn oauth2_sign_in_in_a_browser() {
    require_it!();
    require_browser!();
    let provider = Provider::start(&["operator"]).await;
    let (e, p) = node(
        "console_app_oauth2",
        &provider.api(),
        &console(r#", public_url: "http://127.0.0.1:${E2E_API_PORT}""#),
    )
    .await;
    playwright(
        "oauth2.spec.ts",
        p.port(),
        &[("DA_PIPELINE", e.test.as_str())],
    )
    .await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `jwt`: the proxy's token on every request, a viewer's showing the pipelines with nothing to act
/// on; without one, "No valid token"; one whose roles map to none, "Forbidden".
#[tokio::test]
async fn jwt_tokens_in_a_browser() {
    require_it!();
    require_browser!();
    let idp = Idp::start().await;
    let (e, p) = node(
        "console_app_jwt",
        &idp.api(""),
        "console: { enabled: true }",
    )
    .await;
    let (viewer, unmapped) = (idp.token(&["viewer"]), idp.token(&["nobody"]));
    let env = [
        ("DA_VIEWER_TOKEN", viewer.as_str()),
        ("DA_UNMAPPED_TOKEN", unmapped.as_str()),
        ("DA_PIPELINE", e.test.as_str()),
    ];
    playwright("jwt.spec.ts", p.port(), &env).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `none`: everyone is admin, with the banner; the console off serves nothing under `/ui/`.
#[tokio::test]
async fn no_authentication_and_the_console_off_in_a_browser() {
    require_it!();
    require_browser!();
    let (e, p) = node("console_app_none", "", "console: { enabled: true }").await;
    playwright("none.spec.ts", p.port(), &[]).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
    let (e, p) = node("console_app_off", "", "").await;
    playwright("off.spec.ts", p.port(), &[]).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// The pipelines' pages under `basic` (mockups 05, 06): the list's filters in the address, kept by
/// Back; a row opened from the keyboard; a pipeline's tables, source and lag chart; a pause and a
/// resume through their dialogs, recorded with the note; an action on a selection.
#[tokio::test]
async fn the_pipelines_pages_and_their_actions_in_a_browser() {
    require_it!();
    require_browser!();
    let (e, p) = node(
        "console_app_pipelines",
        ", auth: basic, user: ops, password: s3cret",
        &console(""),
    )
    .await;
    let env = [
        ("DA_USER", "ops"),
        ("DA_PASSWORD", "s3cret"),
        ("DA_PIPELINE", e.test.as_str()),
    ];
    playwright("pipelines.spec.ts", p.port(), &env).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// The Cluster pages under `basic` on three nodes (mockups 03, 04, 11): the members and leases, the
/// console's status, a node's page; n3 asked to leave through its dialog, recorded with its note
/// and still running out of the cluster; n2 shut down once its name is typed, and gone.
#[tokio::test]
async fn the_cluster_pages_and_a_node_s_actions_in_a_browser() {
    require_it!();
    require_browser!();
    let mut c = Cluster::with(
        "console_app_cluster",
        ITEMS,
        MORE,
        Options {
            tables: ", snapshots: { mode: never }",
            cluster: ", rebalance_stabilization: 3s",
            api: ", auth: basic, user: ops, password: s3cret",
            globals: &console(""),
            ..Options::default()
        },
    )
    .await;
    c.start_all().await;
    c.settled().await;
    let env = [("DA_USER", "ops"), ("DA_PASSWORD", "s3cret")];
    playwright("cluster.spec.ts", c.proc("n1").port(), &env).await;

    let i = c.index("n2");
    let mut n2 = c.nodes[i].proc.take().expect("n2 ran");
    assert!(n2.exit().await.success(), "{}", n2.log());
    let n1 = c.proc("n1");
    for (kind, node, note) in [
        ("action.node.leave", "n3", Some("node pool upgrade")),
        ("action.node.shutdown", "n2", None),
    ] {
        eventually(
            &format!("{kind} is recorded"),
            Duration::from_secs(30),
            async || {
                let path = format!("/v1/events?kind={kind}");
                let (_, _, body) = n1.exchange_ok("GET", &path, &[OPS], "").await;
                let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                v["events"].as_array().is_some_and(|e| {
                    e.iter()
                        .any(|e| e["node"] == node && e["note"].as_str() == note)
                })
            },
        )
        .await;
    }
    // n3 left the ring but keeps running, out of the cluster, until it is stopped.
    assert_eq!(c.proc("n3").answers("/health/live").await, Some(200));
    c.stop().await;
}

/// Admin_Console §8: the Overview drawn under 1 s, and moving between pages under 300 ms, with 50
/// pipelines on three nodes.
#[tokio::test]
async fn the_pages_are_drawn_within_their_budgets() {
    require_it!();
    require_browser!();
    let more: String = (1..PIPELINES)
        .map(|i| {
            format!(
                "  p{i:02}:\n    id: p{i:02}\n@SOURCE(_p{i:02})@\n    tables: {{ include: ['@INCLUDE@'], snapshots: {{ mode: never }} }}\n    sink: {{ type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_p{i:02}, delta: {{}}, flush: {{ seconds: 1 }} }}\n"
            )
        })
        .collect();
    let mut c = Cluster::with(
        "console_app_budget",
        ITEMS,
        &more,
        Options {
            tables: ", snapshots: { mode: never }",
            cluster: ", rebalance_stabilization: 3s",
            globals: "console: { enabled: true }",
            ..Options::default()
        },
    )
    .await;
    for i in 1..PIPELINES {
        c.e.drop_source(&format!("_p{i:02}")).await;
    }
    for i in 0..c.nodes.len() {
        c.start(i).await;
    }
    // Every pipeline runs on its ring owner: a pipeline still being handed over has no tables to
    // draw until its new worker runs, which would time the hand-off, not the page.
    eventually(
        "every pipeline runs on its ring owner",
        Duration::from_secs(180),
        async || {
            let (_, v) = c.proc("n1").call("GET", "/v1/cluster/status").await;
            v["pipelines"].as_array().is_some_and(|p| {
                p.len() == PIPELINES
                    && p.iter()
                        .all(|l| l["holder"].is_string() && l["holder"] == l["ring_owner"])
            })
        },
    )
    .await;
    let n = PIPELINES.to_string();
    playwright(
        "budget.spec.ts",
        c.proc("n1").port(),
        &[("DA_PIPELINES", &n)],
    )
    .await;
    c.stop().await;
}
