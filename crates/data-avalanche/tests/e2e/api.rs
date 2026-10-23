//! The management API with its credentials (M9a): TLS on the listener, bearer tokens from an
//! issuer's JWKS, the roles they carry, and a `role_mapping` a reload swaps in.

use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, Proc, eventually};
use crate::idp::{Idp, tls_files};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

/// The status code of a request with `token`.
async fn code(p: &Proc, token: Option<&str>, method: &str, path: &str) -> u16 {
    p.send(token, method, path, "")
        .await
        .unwrap_or_else(|| panic!("{method} {path} answers"))
        .0
}

/// Over TLS, a request without a token is 401 with the bearer challenge, a viewer reads and
/// may not pause, an operator pauses; a reload that maps `ops` to the operator role takes
/// effect on the running node without a restart.
#[tokio::test]
async fn the_api_takes_tls_and_tokens_and_a_reload_of_its_roles() {
    require_it!();
    let idp = Idp::start().await;
    let dir = tempfile::tempdir().expect("a temp dir");
    let (tls, cert) = tls_files(dir.path());
    let api = format!("{}{tls}", idp.api(""));
    let mut e = E2e::new(
        "api_tls",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            api: &api,
            ..Options::default()
        },
    )
    .await;
    e.trust(cert);
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    let id = e.test.clone();
    let path = format!("/v1/pipelines/{id}");
    let pause = format!("{path}/pause");

    let (status, head, _) = p.send(None, "GET", &path, "").await.expect("answers");
    assert_eq!(status, 401);
    assert!(
        head.to_ascii_lowercase()
            .contains("www-authenticate: bearer realm=\"data-avalanche\""),
        "{head}"
    );
    assert_eq!(code(&p, Some("not.a.token"), "GET", &path).await, 401);
    let viewer = idp.token(&["viewer"]);
    let operator = idp.token(&["operator"]);
    assert_eq!(code(&p, Some(&viewer), "GET", &path).await, 200);
    assert_eq!(code(&p, Some(&viewer), "POST", &pause).await, 403);
    assert_eq!(code(&p, Some(&operator), "POST", &pause).await, 202);
    p.authorize(&viewer);
    p.shows(&id, "paused").await;
    assert_eq!(
        code(&p, Some(&operator), "POST", &format!("{path}/resume")).await,
        202
    );
    p.shows(&id, "running").await;

    // `role_mapping` follows a reload (R): `ops` is the operator role, and the three names
    // themselves no longer count.
    let mapped = format!("{}{tls}", idp.api(", role_mapping: { operator: [ops] }"));
    e.write_config(&e.config().replace(&api, &mapped));
    p.authorize(&operator);
    let (status, v) = p.reload().await;
    assert_eq!(status, 202, "{v}");
    let ops = idp.token(&["ops"]);
    eventually(
        "the reload's role mapping applies",
        Duration::from_secs(30),
        async || code(&p, Some(&viewer), "GET", &path).await == 403,
    )
    .await;
    assert_eq!(code(&p, Some(&operator), "POST", &pause).await, 403);
    assert_eq!(code(&p, Some(&ops), "POST", &pause).await, 202);
    p.authorize(&ops);
    p.shows(&id, "paused").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
