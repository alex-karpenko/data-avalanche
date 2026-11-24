//! The console's sign-in and sessions (Admin_Console §9; M14) on three nodes: a session started
//! on one node is every node's, for its pages, the API, a node's detail asked through another,
//! and a live stream that follows the other nodes with it; a request that changes something sends
//! the session's anti-forgery token, or is refused. With `basic` (a form, its attempts counted)
//! and `oauth2` (a provider's sign-in, the sign-out taken on to it).

use std::io::Read;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use data_avalanche_testkit::require_it;
use serde_json::{Value, json};

use crate::harness::{Doc, Options, eventually};
use crate::idp::{Provider, query_value};
use crate::nodes::{Cluster, ITEMS};

/// A session key made at run time: 48 random bytes.
fn session_key() -> String {
    let mut b = [0u8; 48];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .expect("the system's random source");
    URL_SAFE_NO_PAD.encode(b)
}

/// `n1`, `n2` and `n3` running the test's pipeline, its rows landed, with `globals.api` entries
/// `api` and the console on, `console` adding to it.
async fn cluster(test: &str, api: &str, console: &str) -> Cluster {
    let globals = format!(
        r#"console: {{ enabled: true, session_key: "{}"{console} }}"#,
        session_key()
    );
    let mut c = Cluster::with(
        test,
        ITEMS,
        "",
        Options {
            tables: ", snapshots: { mode: never }",
            cluster: ", rebalance_stabilization: 3s",
            api,
            globals: &globals,
            ..Options::default()
        },
    )
    .await;
    c.start_all().await;
    c.e.insert_series("items", 5, Doc::Row).await;
    c.landed().await;
    c
}

/// The values of header `name` in `head`.
fn headers_of<'h>(head: &'h str, name: &str) -> Vec<&'h str> {
    head.lines()
        .filter_map(|l| l.split_once(": "))
        .filter(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
        .collect()
}

/// The `Cookie` a browser sends back after `head`'s `Set-Cookie`s.
fn kept(head: &str) -> String {
    headers_of(head, "set-cookie")
        .into_iter()
        .filter(|c| !c.contains("Max-Age=0"))
        .map(|c| c.split(';').next().unwrap())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Every node takes the session `cookie` as `name`, with `role`: its pages and session, a
/// pause only with its anti-forgery token (recorded under its name), a node's detail through
/// another node, and a stream that follows the other two with it.
async fn every_node_takes(c: &Cluster, cookie: &str, name: &str, role: &str) {
    let test = c.e.test.clone();
    let with = [("Cookie", cookie)];
    let mut csrf = None;
    for n in ["n1", "n2", "n3"] {
        let p = c.proc(n);
        let (code, _, page) = p.exchange_ok("GET", "/ui/", &with, "").await;
        assert_eq!(code, 200, "{n}: {page}");
        assert!(
            page.contains(&format!(
                "You are <b>{name}</b>, signed in as <b>{role}</b>."
            )),
            "{n}: {page}"
        );
        let (code, _, text) = p.exchange_ok("GET", "/ui/session", &with, "").await;
        assert_eq!(code, 200, "{n}: {text}");
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            (&v["name"], &v["role"]),
            (&json!(name), &json!(role)),
            "{n}"
        );
        let token = v["csrf"].as_str().unwrap().to_owned();
        assert_eq!(
            csrf.get_or_insert(token.clone()),
            &token,
            "{n}: one session"
        );
    }
    let csrf = csrf.unwrap();

    // A pause on n3: refused without the anti-forgery token, taken with it, recorded under the
    // session's name.
    let pause = format!("/v1/pipelines/{test}/pause");
    let n3 = c.proc("n3");
    let (code, _, text) = n3.exchange_ok("POST", &pause, &with, "").await;
    assert_eq!(code, 403, "{text}");
    assert!(text.contains("anti-forgery token"), "{text}");
    let both = [("Cookie", cookie), ("X-CSRF-Token", csrf.as_str())];
    let (code, _, text) = n3.exchange_ok("POST", &pause, &both, "").await;
    assert_eq!(code, 202, "{text}");
    let query = format!("/v1/events?kind=action.pipeline.pause&object={test}");
    let n2 = c.proc("n2");
    eventually("the pause's event", Duration::from_secs(30), async || {
        let Some((200, _, text)) = n2.exchange("GET", &query, &with, "").await else {
            return false;
        };
        let v: Value = serde_json::from_str(&text).unwrap();
        v["events"]
            .as_array()
            .is_some_and(|e| e.iter().any(|e| e["actor"]["name"] == name))
    })
    .await;
    let resume = format!("/v1/pipelines/{test}/resume");
    assert_eq!(n3.exchange_ok("POST", &resume, &both, "").await.0, 202);

    // Another node's detail, through n1: n3 opens the session itself.
    let n1 = c.proc("n1");
    let (code, _, text) = n1.exchange_ok("GET", "/v1/nodes/n3", &with, "").await;
    assert_eq!(code, 200, "{text}");

    // A stream on n1 follows n2 and n3 with the session.
    let mut s = n1
        .stream_with("/v1/stream?kind=node", &with)
        .await
        .expect("the stream opens");
    let (kind, hello) = s.next().await.expect("hello");
    assert_eq!(
        (kind.as_str(), &hello["follows"]),
        ("hello", &json!(["n2", "n3"]))
    );
    let followed = tokio::time::timeout(Duration::from_secs(30), async {
        let mut followed = std::collections::BTreeSet::new();
        while followed.len() < 2 {
            let (kind, v) = s.next().await.expect("the stream goes on");
            if kind == "peer" {
                assert_eq!(v["state"], "following", "{v}");
                followed.insert(v["peer"].as_str().unwrap().to_owned());
            }
        }
        followed
    })
    .await
    .expect("both peers followed within 30 s");
    assert_eq!(followed, ["n2".to_owned(), "n3".to_owned()].into());
}

#[tokio::test]
async fn a_basic_session_started_on_one_node_is_every_node_s() {
    require_it!();
    let c = cluster(
        "console_basic",
        ", auth: basic, user: ops, password: s3cret",
        "",
    )
    .await;
    let n1 = c.proc("n1");
    let (code, head, _) = n1.exchange_ok("GET", "/ui/", &[], "").await;
    assert_eq!(
        (code, headers_of(&head, "location")),
        (303, vec!["/ui/sign-in?next=%2Fui%2F"])
    );
    let form = [("Content-Type", "application/x-www-form-urlencoded")];
    let wrong = "user=ops&password=nope&next=%2Fui%2F";
    let (code, _, page) = n1.exchange_ok("POST", "/ui/sign-in", &form, wrong).await;
    assert_eq!(code, 401);
    assert!(
        page.contains("4 attempts left before a 1 min wait."),
        "{page}"
    );
    let right = "user=ops&password=s3cret&next=%2Fui%2F";
    let (code, head, _) = n1.exchange_ok("POST", "/ui/sign-in", &form, right).await;
    assert_eq!((code, headers_of(&head, "location")), (303, vec!["/ui/"]));
    let cookie = kept(&head);
    every_node_takes(&c, &cookie, "ops", "admin").await;

    // Signed out on n2: the session's cookies removed.
    let out = [
        ("Cookie", cookie.as_str()),
        ("Sec-Fetch-Site", "same-origin"),
    ];
    let (code, head, _) = c
        .proc("n2")
        .exchange_ok("POST", "/ui/sign-out", &out, "")
        .await;
    assert_eq!(
        (code, headers_of(&head, "location")),
        (303, vec!["/ui/sign-in?signed_out"])
    );
    assert!(
        headers_of(&head, "set-cookie")
            .iter()
            .any(|c| c.starts_with("__Host-da_session=;") && c.contains("Max-Age=0"))
    );
    c.stop().await;
}

#[tokio::test]
async fn an_oauth2_session_started_through_the_provider_is_every_node_s() {
    require_it!();
    let provider = Provider::start(&["operator"]).await;
    let c = cluster(
        "console_oauth2",
        &provider.api(),
        r#", public_url: "http://127.0.0.1:${E2E_API_PORT}""#,
    )
    .await;
    let n1 = c.proc("n1");
    let (code, head, page) = n1.exchange_ok("GET", "/ui/sign-in", &[], "").await;
    assert_eq!(code, 200, "{head}");
    let host = provider
        .issuer
        .trim_start_matches("http://")
        .split(':')
        .next()
        .unwrap();
    assert!(page.contains(&format!("Continue with {host}")), "{page}");
    let (code, head2, _) = n1
        .exchange_ok("GET", "/ui/oauth2/start?next=%2Fui%2F", &[], "")
        .await;
    assert_eq!(code, 303, "{head2}");
    let back = provider.approve(headers_of(&head2, "location")[0]);
    let path = back
        .strip_prefix(&format!("http://127.0.0.1:{}", n1.port()))
        .expect("back at n1, its public_url")
        .to_owned();
    let left_with = kept(&head2);
    let (code, head, page) = n1
        .exchange_ok("GET", &path, &[("Cookie", left_with.as_str())], "")
        .await;
    assert_eq!(code, 200, "{page}");
    let cookie = kept(&head);
    every_node_takes(&c, &cookie, "alice.smith", "operator").await;

    // Signed out on n2: on to the provider, with the ID token, and back to n2's sign-in.
    let n2 = c.proc("n2");
    let out = [
        ("Cookie", cookie.as_str()),
        ("Sec-Fetch-Site", "same-origin"),
    ];
    let (code, head, _) = n2.exchange_ok("POST", "/ui/sign-out", &out, "").await;
    assert_eq!(code, 303);
    let to = headers_of(&head, "location")[0];
    assert!(
        to.starts_with(&format!("{}/end-session?", provider.issuer)),
        "{to}"
    );
    assert_eq!(query_value(to, "client_id").as_deref(), Some("cdc-console"));
    assert_eq!(
        query_value(to, "post_logout_redirect_uri"),
        Some(format!(
            "http://127.0.0.1:{}/ui/sign-in?signed_out",
            n2.port()
        ))
    );
    assert!(query_value(to, "id_token_hint").is_some(), "{to}");
    c.stop().await;
}
