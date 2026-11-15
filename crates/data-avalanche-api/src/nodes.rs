//! `/v1/nodes/{node}` (Admin_Console §4, §5, §10; D-M13b.5–6): a node's detail, and the actions
//! only that node takes, asked of any node. The detail of this node is answered here; another
//! node's is fetched from it. An action goes to the node's own route, this node's included, so
//! the node that acts verifies the caller and records it.

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use data_avalanche_cluster::{Phase, Ring};
use serde_json::{Value, json};

use crate::peers::{ACTION_LIMIT, Asked, DETAIL_LIMIT, FORWARDED_BY, STATUS_LIMIT};
use crate::{Api, Node, body, starting};

/// The first 16 hex digits of a `blake3:<hex>` hash, as a node gossips its file's.
pub(crate) fn short(hash: &str) -> &str {
    let hex = hash.strip_prefix("blake3:").unwrap_or(hash);
    hex.get(..16).unwrap_or(hex)
}

fn phase(p: Phase) -> &'static str {
    match p {
        Phase::Joining => "joining",
        Phase::Active => "active",
        Phase::Draining => "draining",
        Phase::Leaving => "leaving",
    }
}

/// This node's detail (D-M13b.6).
async fn detail(node: &Node) -> Value {
    let members = node.members.borrow().clone();
    let me = members.nodes.get(&members.me);
    let generation = node.coordinator.generation().await;
    let file_hash = me.map(|m| m.meta.file_hash.as_str()).unwrap_or_default();
    let config = node.coordinator.config().await;
    let ring = Ring::new(
        members
            .nodes
            .iter()
            .filter(|(_, m)| m.meta.phase == Phase::Active)
            .map(|(name, _)| name.as_str()),
        config.globals.cluster.tokens,
    );
    let mut ids: Vec<&str> = config.pipelines.values().map(|p| p.id.as_str()).collect();
    ids.sort_unstable();
    let mut held = Some(vec![]);
    for id in ids {
        match node.leases.observe(id).await {
            Ok(Some(l)) if !l.doc.released && l.doc.holder_node_id == members.me => {
                if let Some(h) = held.as_mut() {
                    h.push(json!({
                        "id": id,
                        "owner_epoch": l.doc.owner_epoch,
                        "ring_owner": ring.owner(id),
                    }));
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("a node's detail, the lease of {id}: {e}");
                held = None;
                break;
            }
        }
    }
    let p = *node.process.borrow();
    let g = node.gossip.borrow().clone();
    let peers: Vec<Value> = members
        .nodes
        .iter()
        .filter(|(name, _)| **name != members.me)
        .map(|(name, m)| {
            json!({
                "node": name,
                "state": if m.suspect { "suspect" } else { "alive" },
                "phase": phase(m.meta.phase),
            })
        })
        .collect();
    let started = me.and_then(|m| {
        i64::try_from(m.incarnation)
            .ok()
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    });
    json!({
        "node": members.me,
        "api_url": me.map(|m| m.meta.api_url.as_str()),
        "gossip_addr": me.map(|m| m.addr.to_string()),
        "build": me.map(|m| m.meta.build.as_str()),
        "incarnation": me.map(|m| m.incarnation),
        "started_at": started,
        "phase": me.map(|m| phase(m.meta.phase)),
        "config_generation": generation.generation,
        "file": {
            "hash": (!file_hash.is_empty()).then_some(file_hash),
            "generation_hash": short(&generation.hash),
            "matches": (!file_hash.is_empty()).then(|| file_hash == short(&generation.hash)),
        },
        "restart_pending": node.coordinator.restart_pending().await,
        "process": {
            "memory_bytes": p.reading.memory_bytes,
            "memory_limit_bytes": p.reading.memory_limit_bytes,
            "cpu_seconds": p.reading.cpu_seconds,
            "cpu_cores": p.cpu_cores,
            "cpu_limit_cores": p.reading.cpu_limit_cores,
        },
        "gossip": {
            "peers": peers,
            "last_probe": g.last_probe.map(|(n, at)| json!({ "node": n, "at": at })),
            "probe_rtt_ms": g.probe_rtt.map(|d| d.as_secs_f64() * 1000.0),
            "suspicions": {
                "raised": g.raised.iter().map(|(n, at)| json!({ "node": n, "at": at })).collect::<Vec<_>>(),
                "received": g.received,
            },
        },
        "pipelines": held,
    })
}

/// Sends the caller's request on to `node`'s `path`: `404` for a name no member has, `503` for
/// a member that gossips no API address, and never a request that was forwarded already.
async fn forward(
    node: &Node,
    headers: &HeaderMap,
    to: &str,
    path: &str,
    asked: Asked,
    limit: std::time::Duration,
) -> Response {
    if headers.contains_key(FORWARDED_BY) {
        return body(
            StatusCode::MISDIRECTED_REQUEST,
            &json!({ "error": format!("{to} isn't this node, and a forwarded request isn't forwarded again") }),
        );
    }
    let member = node.members.borrow().nodes.get(to).cloned();
    match member {
        None => body(
            StatusCode::NOT_FOUND,
            &json!({ "error": format!("no member {to:?} in this node's view of the cluster") }),
        ),
        Some(m) if m.meta.api_url.is_empty() => body(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({ "error": format!("{to} gossips no API address") }),
        ),
        Some(m) => {
            node.peers
                .send(to, &m.meta.api_url, path, asked, limit)
                .await
        }
    }
}

/// `GET /v1/nodes/{node}`.
pub(crate) async fn get(
    State(api): State<Api>,
    Path(to): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    if to == node.members.borrow().me {
        return body(StatusCode::OK, &detail(node).await);
    }
    let asked = Asked::from(Method::GET, &headers, Bytes::new());
    let path = format!("/v1/nodes/{to}");
    forward(node, &headers, &to, &path, asked, DETAIL_LIMIT).await
}

/// A node action, sent to the node's own `path`, this node's included.
async fn act(
    api: &Api,
    to: &str,
    headers: &HeaderMap,
    method: Method,
    path: &str,
    bytes: Bytes,
    limit: std::time::Duration,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let asked = Asked::from(method, headers, bytes);
    forward(node, headers, to, path, asked, limit).await
}

/// `POST /v1/nodes/{node}/leave` → the node's `POST /v1/cluster/leave`.
pub(crate) async fn leave(
    State(api): State<Api>,
    Path(to): Path<String>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    act(
        &api,
        &to,
        &headers,
        Method::POST,
        "/v1/cluster/leave",
        bytes,
        ACTION_LIMIT,
    )
    .await
}

/// `POST /v1/nodes/{node}/shutdown` → the node's `POST /v1/admin/shutdown`.
pub(crate) async fn shutdown(
    State(api): State<Api>,
    Path(to): Path<String>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    act(
        &api,
        &to,
        &headers,
        Method::POST,
        "/v1/admin/shutdown",
        bytes,
        ACTION_LIMIT,
    )
    .await
}

/// `POST /v1/nodes/{node}/config/validate` → the node's `POST /v1/config/validate`: its own
/// file when the body is empty.
pub(crate) async fn validate(
    State(api): State<Api>,
    Path(to): Path<String>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    act(
        &api,
        &to,
        &headers,
        Method::POST,
        "/v1/config/validate",
        bytes,
        ACTION_LIMIT,
    )
    .await
}

/// `POST /v1/nodes/{node}/config/reload` → the node's `POST /v1/config/reload`, with the query:
/// the node publishes its own file.
pub(crate) async fn reload(
    State(api): State<Api>,
    Path(to): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    let path = match query {
        Some(q) => format!("/v1/config/reload?{q}"),
        None => "/v1/config/reload".to_owned(),
    };
    act(
        &api,
        &to,
        &headers,
        Method::POST,
        &path,
        bytes,
        ACTION_LIMIT,
    )
    .await
}

/// `GET /v1/nodes/{node}/config/status` → the node's `GET /v1/config/status`: its generation
/// and its own restart-pending fields.
pub(crate) async fn config_status(
    State(api): State<Api>,
    Path(to): Path<String>,
    headers: HeaderMap,
) -> Response {
    act(
        &api,
        &to,
        &headers,
        Method::GET,
        "/v1/config/status",
        Bytes::new(),
        STATUS_LIMIT,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use axum::Router;
    use axum::http::Uri;
    use data_avalanche_cluster::{Member, Members, NodeMeta, file_hash};
    use data_avalanche_config::{FingerprintKey, Origin, Pem, Secret, TlsIdentity};
    use data_avalanche_state::{EventDoc, EventQuery, Events};
    use tempfile::TempDir;
    use tokio::sync::watch;

    use super::*;
    use crate::peers::{ANSWERED_BY, Peers, TARGET};
    use crate::test_support::{
        CONFIG, Issuer, Lone, USERNAME_CLAIM, api, lone, open, request, serve,
    };
    use crate::{Auth, Stop};

    /// What a fake node saw of one request.
    #[derive(Debug, Clone)]
    struct Seen {
        method: Method,
        uri: String,
        authorization: Option<String>,
        target: Option<String>,
        forwarded_by: Option<String>,
        body: String,
    }

    type Log = Arc<Mutex<Vec<Seen>>>;

    /// A fake node: answers every request `202 {"node": "node-b"}` after `delay`, keeping what it
    /// saw.
    async fn fake(delay: Duration) -> (String, Log) {
        let log: Log = Arc::default();
        let kept = Arc::clone(&log);
        let app = Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, bytes: Bytes| {
                let kept = Arc::clone(&kept);
                async move {
                    let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
                    kept.lock().unwrap().push(Seen {
                        method,
                        uri: uri.to_string(),
                        authorization: h("authorization"),
                        target: h(TARGET),
                        forwarded_by: h(FORWARDED_BY),
                        body: String::from_utf8(bytes.to_vec()).unwrap(),
                    });
                    tokio::time::sleep(delay).await;
                    body(StatusCode::ACCEPTED, &json!({ "node": "node-b" }))
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, log)
    }

    fn member(api_url: &str, phase: Phase) -> Member {
        Member {
            addr: "127.0.0.1:7946".parse().unwrap(),
            incarnation: 1_731_600_000_000,
            meta: NodeMeta {
                phase,
                config_generation: 1,
                build: "1.1.0".into(),
                api_url: api_url.into(),
                file_hash: file_hash(CONFIG),
            },
            suspect: false,
        }
    }

    /// node-a's API under `auth`; its view holds itself at its own address and node-b at
    /// `b_url`.
    async fn served(b_url: &str, auth: Arc<Auth>) -> (SocketAddr, Lone, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let mut lone = lone(file).await;
        let (view, members) = watch::channel(Members {
            me: "node-a".into(),
            nodes: Default::default(),
        });
        lone.node.members = members;
        let a = api(auth);
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;
        view.send_replace(Members {
            me: "node-a".into(),
            nodes: [
                (
                    "node-a".to_owned(),
                    member(&format!("http://{addr}"), Phase::Active),
                ),
                ("node-b".to_owned(), member(b_url, Phase::Active)),
            ]
            .into(),
        });
        std::mem::forget(view);
        (addr, lone, dir)
    }

    fn json(text: &str) -> Value {
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    async fn recorded(events: &Events) -> Vec<EventDoc> {
        let page = events.list(&EventQuery::default()).await.unwrap();
        page.events.into_iter().map(|(_, e)| e).collect()
    }

    #[tokio::test]
    async fn this_node_s_detail_is_answered_here() {
        let (b, log) = fake(Duration::ZERO).await;
        let (addr, _lone, _dir) = served(&b, open()).await;
        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-a", &[], "").await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        assert_eq!(v["node"], "node-a");
        assert_eq!(v["api_url"], format!("http://{addr}"));
        assert_eq!(v["build"], "1.1.0");
        assert_eq!(v["started_at"], "2024-11-14T16:00:00Z");
        assert_eq!(v["config_generation"], 1);
        assert_eq!(v["file"]["hash"], file_hash(CONFIG));
        assert_eq!(v["file"]["matches"], true);
        assert_eq!(
            v["gossip"]["peers"],
            json!([{ "node": "node-b", "state": "alive", "phase": "active" }])
        );
        assert_eq!(v["pipelines"], json!([]));
        assert!(
            v["process"].is_object() && v["restart_pending"].is_array(),
            "{v}"
        );
        assert!(log.lock().unwrap().is_empty(), "nothing forwarded");
    }

    #[tokio::test]
    async fn another_node_s_detail_is_fetched_with_the_callers_own_credentials() {
        let (b, log) = fake(Duration::ZERO).await;
        let (addr, _lone, _dir) = served(&b, open()).await;
        let auth = [("Authorization", "Bearer the-callers-token")];
        let (code, head, text) = request(addr, "GET", "/v1/nodes/node-b", &auth, "").await;
        assert_eq!((code, json(&text)), (202, json!({ "node": "node-b" })));
        assert!(
            head.to_ascii_lowercase()
                .contains(&format!("{ANSWERED_BY}: node-b")),
            "{head}"
        );
        let seen = log.lock().unwrap()[0].clone();
        assert_eq!(
            (seen.method, seen.uri.as_str()),
            (Method::GET, "/v1/nodes/node-b")
        );
        assert_eq!(
            seen.authorization.as_deref(),
            Some("Bearer the-callers-token")
        );
        assert_eq!(seen.target.as_deref(), Some("node-b"));
        assert_eq!(seen.forwarded_by.as_deref(), Some("node-a"));
    }

    #[tokio::test]
    async fn a_peer_that_doesnt_answer_is_named_within_the_limit() {
        let (addr, _lone, _dir) = served("http://127.0.0.1:1", open()).await;
        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-b", &[], "").await;
        assert_eq!(code, 502, "{text}");
        let v = json(&text);
        assert_eq!(v["node"], "node-b");
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .starts_with("node-b didn't answer"),
            "{v}"
        );
        let (code, _, _) = request(addr, "GET", "/v1/nodes/node-z", &[], "").await;
        assert_eq!(code, 404, "no such member");
    }

    #[tokio::test]
    async fn a_slow_peer_is_cut_off_at_the_limit() {
        let (b, _log) = fake(Duration::from_secs(30)).await;
        let (addr, _lone, _dir) = served(&b, open()).await;
        let t = Instant::now();
        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-b", &[], "").await;
        assert_eq!(code, 504, "{text}");
        assert!(
            t.elapsed() >= DETAIL_LIMIT && t.elapsed() < DETAIL_LIMIT * 2,
            "{:?}",
            t.elapsed()
        );
    }

    #[tokio::test]
    async fn a_request_forwarded_to_the_wrong_node_is_refused() {
        let (b, log) = fake(Duration::ZERO).await;
        let (addr, lone, _dir) = served(&b, open()).await;
        let to_b = [(TARGET, "node-b")];
        let (code, _, text) = request(addr, "POST", "/v1/cluster/leave", &to_b, "").await;
        assert_eq!(code, 421, "{text}");
        assert_eq!(*lone.stops.borrow(), Stop::Run, "nothing acted on");
        let again = [(FORWARDED_BY, "node-c")];
        let (code, _, _) = request(addr, "GET", "/v1/nodes/node-b", &again, "").await;
        assert_eq!(code, 421, "never forwarded twice");
        assert!(log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_leave_goes_to_the_node_named_which_records_it() {
        let (b, log) = fake(Duration::ZERO).await;
        let (addr, lone, _dir) = served(&b, open()).await;
        let note = r#"{"note": "node pool upgrade"}"#;
        let (code, _, _) = request(addr, "POST", "/v1/nodes/node-b/leave", &[], note).await;
        assert_eq!(code, 202);
        let seen = log.lock().unwrap()[0].clone();
        assert_eq!(
            (seen.method, seen.uri.as_str()),
            (Method::POST, "/v1/cluster/leave")
        );
        assert_eq!(seen.body, note);
        assert_eq!(*lone.stops.borrow(), Stop::Run, "node-a stays");

        // Through itself: its own route acts and records, once.
        let (code, _, text) = request(addr, "POST", "/v1/nodes/node-a/leave", &[], note).await;
        assert_eq!(
            (code, json(&text)),
            (202, json!({ "leaving": true, "node": "node-a" }))
        );
        assert_eq!(*lone.stops.borrow(), Stop::Leave);
        assert!(lone.node.recorder.flush(Duration::from_secs(5)).await);
        let got = recorded(&lone.events).await;
        let kinds: Vec<&str> = got.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(kinds, ["action.node.leave"], "{got:?}");
        assert_eq!(got[0].note.as_deref(), Some("node pool upgrade"));
    }

    #[tokio::test]
    async fn the_config_routes_go_to_the_node_s_own() {
        let (b, log) = fake(Duration::ZERO).await;
        let (addr, _lone, _dir) = served(&b, open()).await;
        for (method, path, to) in [
            (
                "POST",
                "/v1/nodes/node-b/config/validate",
                "/v1/config/validate",
            ),
            (
                "POST",
                "/v1/nodes/node-b/config/reload?restart=accept",
                "/v1/config/reload?restart=accept",
            ),
            ("GET", "/v1/nodes/node-b/config/status", "/v1/config/status"),
            ("POST", "/v1/nodes/node-b/shutdown", "/v1/admin/shutdown"),
        ] {
            let (code, _, _) = request(addr, method, path, &[], "").await;
            assert_eq!(code, 202, "{path}");
            assert_eq!(log.lock().unwrap().last().unwrap().uri, to);
        }
    }

    #[tokio::test]
    async fn a_viewer_cant_leave_another_node() {
        let idp = Issuer::start().await;
        let (b, log) = fake(Duration::ZERO).await;
        let (addr, lone, _dir) =
            served(&b, Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
        let token = format!("Bearer {}", idp.token(json!(["viewer"])));
        let auth = [("Authorization", token.as_str())];
        let (code, _, _) = request(addr, "POST", "/v1/nodes/node-b/leave", &auth, "").await;
        assert_eq!(code, 403);
        assert!(log.lock().unwrap().is_empty(), "refused before it was sent");
        assert!(lone.node.recorder.flush(Duration::from_secs(5)).await);
        let got = recorded(&lone.events).await;
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].kind, "action.denied");
        assert_eq!(
            got[0].text,
            "leave the cluster: node-b refused: needs admin"
        );
    }

    /// The cluster's nodes share a CA: a peer trusts every certificate of its own `api.tls`.
    #[tokio::test]
    async fn peers_trust_this_node_s_own_certificate() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let key = FingerprintKey::random();
        let pem = |text: String| Pem {
            origin: Origin::Inline,
            text: Secret::new(text, &key),
        };
        let tls = TlsIdentity {
            cert: pem(cert.cert.pem()),
            key: pem(cert.signing_key.serialize_pem()),
        };
        let bound = crate::bind(
            &data_avalanche_config::HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            Some(&tls),
        )
        .await
        .unwrap();
        let port = bound.local_addr().unwrap().port();
        let app = Router::new().fallback(|| async { body(StatusCode::OK, &json!({})) });
        tokio::spawn(bound.serve(app, std::future::pending()));
        let url = format!("https://localhost:{port}");
        let asked = || Asked::from(Method::GET, &HeaderMap::new(), Bytes::new());
        let ours = Peers::new("node-a", Some(&tls)).unwrap();
        let r = ours
            .send("node-b", &url, "/v1/x", asked(), DETAIL_LIMIT)
            .await;
        assert_eq!(r.status(), 200);
        let strangers = Peers::new("node-a", None).unwrap();
        let r = strangers
            .send("node-b", &url, "/v1/x", asked(), DETAIL_LIMIT)
            .await;
        assert_eq!(r.status(), 502, "a certificate nobody vouches for");
    }
}
