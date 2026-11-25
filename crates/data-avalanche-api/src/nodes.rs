//! `/v1/nodes/{node}` (Admin_Console §4, §5, §10; D-M13b.5–6): a node's detail, and the actions
//! only that node takes, asked of any node. The detail of this node is answered here; another
//! node's is fetched from it. An action goes to the node's own route, this node's included, so
//! the node that acts verifies the caller and records it.

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use data_avalanche_cluster::{GossipFigures, Members, Phase, Ring};
use data_avalanche_telemetry::ProcessFigures;
use serde_json::{Value, json};

use crate::peers::{ACTION_LIMIT, Asked, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, STATUS_LIMIT};
use crate::{Api, Node, body, starting};

/// The first 16 hex digits of a `blake3:<hex>` fingerprint, as a node gossips its file's.
pub(crate) fn short(hash: &str) -> &str {
    let hex = hash.strip_prefix("blake3:").unwrap_or(hash);
    hex.get(..16).unwrap_or(hex)
}

pub(crate) fn phase(p: Phase) -> &'static str {
    match p {
        Phase::Joining => "joining",
        Phase::Active => "active",
        Phase::Draining => "draining",
        Phase::Leaving => "leaving",
    }
}

/// The process's figures, as a node's detail and its live `node` message show them.
pub(crate) fn process_json(p: &ProcessFigures) -> Value {
    json!({
        "memory_bytes": p.reading.memory_bytes,
        "memory_limit_bytes": p.reading.memory_limit_bytes,
        "cpu_seconds": p.reading.cpu_seconds,
        "cpu_cores": p.cpu_cores,
        "cpu_limit_cores": p.reading.cpu_limit_cores,
    })
}

/// What the gossip saw of the node's peers and its probes, as of `now`: a suspicion a day old
/// goes though no newer one came to prune it.
pub(crate) fn gossip_json(members: &Members, mut g: GossipFigures, now: DateTime<Utc>) -> Value {
    g.forget_before(now);
    let (raised_more, received_more) = (g.raised_more(), g.received_more());
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
    json!({
        "peers": peers,
        "last_probe": g.last_probe.map(|(n, at)| json!({ "node": n, "at": at })),
        "probe_rtt_ms": g.probe_rtt.map(|d| d.as_secs_f64() * 1000.0),
        "suspicions": {
            "raised": g.raised.iter().map(|(n, at)| json!({ "node": n, "at": at })).collect::<Vec<_>>(),
            "received": g.received,
            "raised_more": raised_more,
            "received_more": received_more,
        },
    })
}

/// How many leases a node's detail reads at once.
const LEASE_READS: usize = 16;

/// This node's detail (D-M13b.6).
async fn detail(node: &Node) -> Value {
    let members = node.members.borrow().clone();
    let me = members.nodes.get(&members.me);
    let generation = node.coordinator.generation().await;
    let file_hash = me.map(|m| m.meta.file_hash.as_str()).unwrap_or_default();
    // Keyed, as the gossiped file's is: never the text's plain hash (Admin_Console §4 Config).
    let generation_hash = match node
        .coordinator
        .generation_fingerprint(generation.generation, &generation.hash)
        .await
    {
        Ok(fp) => Some(short(&fp).to_owned()),
        Err(e) => {
            tracing::debug!("a node's detail, its generation's text: {e}");
            None
        }
    };
    let config = node.coordinator.config().await;
    let ring = Ring::new(
        members
            .nodes
            .iter()
            .filter(|(_, m)| m.meta.phase == Phase::Active)
            .map(|(name, _)| name.as_str()),
        config.globals.cluster.tokens,
    );
    let mut ids: Vec<String> = config.pipelines.values().map(|p| p.id.clone()).collect();
    ids.sort_unstable();
    // Read LEASE_READS at a time: one after another, a few hundred pipelines on S3 would take
    // longer than the detail's limit when asked through another node.
    let mut held = Some(vec![]);
    'chunks: for chunk in ids.chunks(LEASE_READS) {
        let mut reads = tokio::task::JoinSet::new();
        for (i, id) in chunk.iter().enumerate() {
            let (leases, id) = (node.leases.clone(), id.clone());
            reads.spawn(async move { (i, leases.observe(&id).await) });
        }
        let mut read = reads.join_all().await;
        read.sort_unstable_by_key(|(i, _)| *i);
        for (id, (_, lease)) in chunk.iter().zip(read) {
            match lease {
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
                    break 'chunks;
                }
            }
        }
    }
    let p = *node.process.borrow();
    let g = node.gossip.borrow().clone();
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
            "generation_hash": generation_hash,
            "matches": generation_hash
                .as_deref()
                .filter(|_| !file_hash.is_empty())
                .map(|g| file_hash == g),
        },
        "restart_pending": node.coordinator.restart_pending().await,
        "process": process_json(&p),
        "gossip": gossip_json(&members, g, Utc::now()),
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

/// `GET /v1/config/nodes` (D-M13b.12): the generation the cluster runs, and for each member the
/// one it runs, whether its own file is that generation's text (`same`, `differs`, or `unknown`
/// when it gossips no hash), and its fields waiting for a restart, from its own
/// `/v1/config/status` asked with the caller's credentials (`null`, and `error`, when it doesn't
/// answer within the limit).
pub(crate) async fn files(State(api): State<Api>, headers: HeaderMap) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let unreadable = |e: data_avalanche_state::StateError| {
        body(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({ "error": format!("the config generation can't be read: {e}") }),
        )
    };
    // The text's fingerprint under the cluster's key, as each node gossips its file's.
    let (current, keyed) = match node.coordinator.generations().current().await {
        Ok(Some(c)) => {
            let keyed = node.coordinator.fingerprint(&c.text);
            (c.pointer.doc, keyed)
        }
        Ok(None) => {
            let g = node.coordinator.generation().await;
            match node
                .coordinator
                .generation_fingerprint(g.generation, &g.hash)
                .await
            {
                Ok(keyed) => (g, keyed),
                Err(e) => return unreadable(e),
            }
        }
        Err(e) => return unreadable(e),
    };
    let members = node.members.borrow().clone();
    let mut asks = tokio::task::JoinSet::new();
    for (name, m) in &members.nodes {
        if *name == members.me || m.meta.api_url.is_empty() {
            continue;
        }
        let (peers, name, url) = (node.peers.clone(), name.clone(), m.meta.api_url.clone());
        let asked = Asked::from(Method::GET, &headers, Bytes::new());
        asks.spawn(async move {
            let answer = peers
                .send(&name, &url, "/v1/config/status", asked, STATUS_LIMIT)
                .await;
            let code = answer.status();
            let v: Value = axum::body::to_bytes(answer.into_body(), MAX_ANSWER)
                .await
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or(Value::Null);
            let pending = if code == StatusCode::OK {
                Ok(v["restart_pending"].clone())
            } else {
                Err(v["error"]
                    .as_str()
                    .map_or_else(|| format!("{name} answered {code}"), str::to_owned))
            };
            (name, pending)
        });
    }
    let mut pending: std::collections::BTreeMap<String, Result<Value, String>> =
        asks.join_all().await.into_iter().collect();
    pending.insert(
        members.me.clone(),
        Ok(json!(node.coordinator.restart_pending().await)),
    );
    let nodes: Vec<Value> = members
        .nodes
        .iter()
        .map(|(name, m)| {
            let file = match m.meta.file_hash.as_str() {
                "" => "unknown",
                h if h == short(&keyed) => "same",
                _ => "differs",
            };
            let mut v = json!({
                "node": name,
                "generation": m.meta.config_generation,
                "file": file,
            });
            match pending.remove(name) {
                Some(Ok(p)) => v["restart_pending"] = p,
                Some(Err(e)) => {
                    v["restart_pending"] = Value::Null;
                    v["error"] = json!(e);
                }
                None => {
                    v["restart_pending"] = Value::Null;
                    v["error"] = json!(format!("{name} gossips no API address"));
                }
            }
            v
        })
        .collect();
    body(
        StatusCode::OK,
        &json!({ "generation": current.generation, "hash": keyed, "nodes": nodes }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use axum::Router;
    use axum::http::Uri;
    use data_avalanche_cluster::file_hash;
    use data_avalanche_config::{
        FingerprintKey, Origin, Pem, Secret, TlsIdentity, text_fingerprint,
    };
    use data_avalanche_state::{EventDoc, EventQuery, Events, config_hash};
    use tokio::sync::watch;

    use super::*;
    use crate::peers::{ANSWERED_BY, Peers, TARGET};
    use crate::test_support::{
        CONFIG, Issuer, Log, Seen, api, console, fake, lone_with, member, open, request, serve,
        served,
    };
    use crate::{Auth, Stop};

    fn json(text: &str) -> Value {
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    async fn recorded(events: &Events) -> Vec<EventDoc> {
        let page = events.list(&EventQuery::default()).await.unwrap();
        page.events.into_iter().map(|(_, e)| e).collect()
    }

    /// Each member's generation, file and pending fields: node-b's from its own status, asked
    /// with the caller's credentials; node-c gossips no address, and a file of its own.
    #[tokio::test]
    async fn each_node_s_file_and_pending_fields_are_listed() {
        let log: Log = Arc::default();
        let kept = Arc::clone(&log);
        let app = Router::new().fallback(move |uri: Uri, headers: HeaderMap| {
            let kept = Arc::clone(&kept);
            async move {
                kept.lock().unwrap().push(Seen {
                    method: Method::GET,
                    uri: uri.to_string(),
                    authorization: headers
                        .get("authorization")
                        .map(|v| v.to_str().unwrap().to_owned()),
                    cookie: None,
                    csrf: None,
                    target: None,
                    forwarded_by: None,
                    body: String::new(),
                });
                body(
                    StatusCode::OK,
                    &json!({ "restart_pending": ["globals.api.address"] }),
                )
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // node-a's view, with node-c beside node-b, served again.
        let (_, lone, _dir) = served(&b, open()).await;
        let mut c = member("", Phase::Active);
        c.meta.file_hash = file_hash(&FingerprintKey::from_bytes([1; 32]), "another file");
        let mut view = lone.node.members.borrow().clone();
        view.nodes.insert("node-c".into(), c);
        let (tx, rx) = watch::channel(view);
        let mut node = lone.node.clone();
        node.members = rx;
        let a = api(open());
        a.node.set(node).unwrap();
        let addr = serve(a).await;
        std::mem::forget(tx);

        let auth = [("Authorization", "Bearer the-callers-token")];
        let (code, _, text) = request(addr, "GET", "/v1/config/nodes", &auth, "").await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        assert_eq!(v["generation"], 1);
        assert_eq!(
            v["nodes"],
            json!([
                { "node": "node-a", "generation": 1, "file": "same", "restart_pending": [] },
                { "node": "node-b", "generation": 1, "file": "same", "restart_pending": ["globals.api.address"] },
                {
                    "node": "node-c", "generation": 1, "file": "differs", "restart_pending": null,
                    "error": "node-c gossips no API address",
                },
            ])
        );
        let seen = log.lock().unwrap()[0].clone();
        assert_eq!(
            (seen.uri.as_str(), seen.authorization.as_deref()),
            ("/v1/config/status", Some("Bearer the-callers-token"))
        );
    }

    /// The masked text of a generation keeps every byte but its secrets. No hash the API returns
    /// of that text, nor of a node's file, is its plain BLAKE3: a viewer who puts a guess back for
    /// a mask, in any quoting, can't confirm it offline. Each is the text's fingerprint under the
    /// cluster's key, comparable across nodes.
    #[tokio::test]
    async fn no_hash_the_api_returns_confirms_a_guessed_secret() {
        let (b, _log) = fake(Duration::ZERO).await;
        let (addr, _lone, _dir) = served(&b, open()).await;
        let get = |path: &'static str| async move {
            let (code, _, text) = request(addr, "GET", path, &[], "").await;
            assert_eq!(code, 200, "{path}: {text}");
            json(&text)
        };
        let one = get("/v1/config/generations/1").await;
        let masked = one["text"].as_str().unwrap();
        let from = masked
            .find(&format!("\"{}", data_avalanche_config::MASK))
            .unwrap();
        let to = from + 1 + masked[from + 1..].find('"').unwrap() + 1;
        let guesses: Vec<String> = ["pw", "'pw'", "\"pw\""]
            .iter()
            .map(|g| format!("{}{g}{}", &masked[..from], &masked[to..]))
            .collect();
        assert!(
            guesses.iter().any(|g| g == CONFIG),
            "the right guess is the text"
        );
        let plain: Vec<String> = guesses.iter().map(|g| config_hash(g)).collect();

        let keyed = text_fingerprint(&FingerprintKey::from_bytes([1; 32]), CONFIG);
        let list = get("/v1/config/generations").await;
        let files = get("/v1/config/nodes").await;
        let detail = get("/v1/nodes/node-a").await;
        let status = get("/v1/cluster/status").await;
        let full = [
            &list["generations"][0]["hash"],
            &one["hash"],
            &files["hash"],
        ];
        let short_ones = [
            &detail["file"]["hash"],
            &detail["file"]["generation_hash"],
            &status["members"][0]["file_hash"],
            &status["members"][1]["file_hash"],
        ];
        for got in full.iter().chain(&short_ones) {
            let got = got.as_str().unwrap_or_else(|| panic!("a hash: {got}"));
            for p in &plain {
                assert!(!p.contains(short(got)), "{got} confirms a guess");
            }
        }
        for got in full {
            assert_eq!(got, &json!(keyed));
        }
        for got in short_ones {
            assert_eq!(got, &json!(short(&keyed)));
        }
        assert_eq!(detail["file"]["matches"], true);
        assert_eq!(files["nodes"][0]["file"], "same");
    }

    /// A node's detail lists every lease it holds, however many there are (read
    /// [`LEASE_READS`] at a time), in id order, and none another node holds.
    #[tokio::test]
    async fn the_detail_lists_every_lease_held_here() {
        let orders = &CONFIG[CONFIG.find("  orders:").unwrap()..];
        let mut text = CONFIG.to_owned();
        let ids: Vec<String> = (0..2 * LEASE_READS + 3)
            .map(|i| format!("p{i:02}"))
            .collect();
        for id in &ids {
            text.push_str(
                &orders
                    .replace("orders", id)
                    .replace("prefix: shop", &format!("prefix: shop-{id}")),
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, &text).unwrap();
        let lone = lone_with(file, &text, Arc::new(data_avalanche_config::ProcessEnv)).await;
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;
        let holder = |node: &str| data_avalanche_state::Holder {
            node_id: node.into(),
            incarnation: 1,
        };
        let mut want: Vec<String> = vec!["orders".into()];
        want.extend(ids.iter().filter(|id| *id != "p05").cloned());
        for id in want.iter().chain([&"p05".to_owned()]) {
            let by = if id == "p05" { "node-b" } else { "node-a" };
            lone.leases
                .acquire(id, &holder(by), Duration::from_secs(30), chrono::Utc::now())
                .await
                .unwrap();
        }
        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-a", &[], "").await;
        assert_eq!(code, 200, "{text}");
        let got: Vec<String> = json(&text)["pipelines"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap().to_owned())
            .collect();
        want.sort();
        assert_eq!(got, want);
    }

    /// A peer that answers otherwise than `200`, or not within the limit, has no pending
    /// fields and is named with why; the others are listed all the same.
    #[tokio::test]
    async fn a_peer_s_status_that_fails_is_named_in_the_nodes_files() {
        for (delay, why) in [
            (Duration::ZERO, "node-b answered 202 Accepted"),
            (Duration::from_secs(30), "node-b didn't answer within 2s"),
        ] {
            let (b, _log) = fake(delay).await;
            let (addr, _lone, _dir) = served(&b, open()).await;
            let t = Instant::now();
            let (code, _, text) = request(addr, "GET", "/v1/config/nodes", &[], "").await;
            assert_eq!(code, 200, "{text}");
            assert!(t.elapsed() < STATUS_LIMIT * 2, "{:?}", t.elapsed());
            let v = json(&text);
            assert_eq!(v["nodes"][0]["restart_pending"], json!([]), "{v}");
            assert_eq!(
                (&v["nodes"][1]["restart_pending"], &v["nodes"][1]["error"]),
                (&Value::Null, &json!(why)),
                "{v}"
            );
        }
    }

    /// The gossip's figures are read as of the request: a suspicion older than a day isn't
    /// shown, though no newer one has come to prune it.
    #[tokio::test]
    async fn the_detail_shows_only_a_day_s_suspicions() {
        let (b, _log) = fake(Duration::ZERO).await;
        let (_, lone, _dir) = served(&b, open()).await;
        let now = chrono::Utc::now();
        let old = now - chrono::Duration::hours(25);
        let mut figures = data_avalanche_cluster::GossipFigures::default();
        figures.raised = vec![("node-b".into(), old), ("node-b".into(), now)];
        figures.received = vec![old];
        let (tx, rx) = watch::channel(figures);
        let mut node = lone.node.clone();
        node.gossip = rx;
        let a = api(open());
        a.node.set(node).unwrap();
        let addr = serve(a).await;
        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-a", &[], "").await;
        assert_eq!(code, 200, "{text}");
        let s = &json(&text)["gossip"]["suspicions"];
        assert_eq!(s["raised"].as_array().map(Vec::len), Some(1), "{s}");
        assert_eq!(s["received"], json!([]), "{s}");
        assert_eq!(
            (&s["raised_more"], &s["received_more"]),
            (&json!(0), &json!(0))
        );
        drop(tx);
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
        assert_eq!(
            v["file"]["hash"],
            file_hash(&FingerprintKey::from_bytes([1; 32]), CONFIG)
        );
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

    /// M13f's carry-in: a peer opens the console's session with the key every node has, so a
    /// node passes the session on as it passes a token, with its anti-forgery token.
    #[tokio::test]
    async fn a_node_action_is_forwarded_with_the_callers_session() {
        let (b, log) = fake(Duration::ZERO).await;
        let auth = Auth::new(
            &crate::test_support::basic_api("s3cret"),
            &crate::test_support::sessions(Duration::from_secs(3600)),
        );
        let (addr, _lone, _dir) = served(&b, Arc::new(auth)).await;
        let session = crate::test_support::basic_session("s3cret", Duration::from_secs(60));
        let cookie = format!(
            "theme=dark; {}",
            crate::test_support::session_cookie(&session)
        );
        let sent = [
            ("Cookie", cookie.as_str()),
            (crate::CSRF_HEADER, session.csrf.as_str()),
        ];
        let (code, _, text) = request(addr, "POST", "/v1/nodes/node-b/leave", &sent, "").await;
        assert_eq!(code, 202, "{text}");
        let seen = log.lock().unwrap()[0].clone();
        assert_eq!(seen.authorization, None);
        assert_eq!(
            seen.cookie.as_deref(),
            cookie.strip_prefix("theme=dark; "),
            "the session's cookies alone"
        );
        assert_eq!(seen.csrf.as_deref(), Some(session.csrf.as_str()));
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
            served(&b, Arc::new(Auth::new(&idp.api("roles"), &console()))).await;
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

    /// A self-signed identity for `localhost`.
    fn identity() -> TlsIdentity {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let key = FingerprintKey::random();
        let pem = |text: String| Pem {
            origin: Origin::Inline,
            text: Secret::new(text, &key),
        };
        TlsIdentity {
            cert: pem(cert.cert.pem()),
            key: pem(cert.signing_key.serialize_pem()),
        }
    }

    fn asked_by(authorization: &str) -> Asked {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", authorization.parse().unwrap());
        Asked::from(Method::GET, &headers, Bytes::new())
    }

    /// A node with TLS sends a caller's credentials over HTTPS only: a peer that gossips an
    /// `http://` address (one not yet restarted with TLS) is a `502`, and gets nothing.
    #[tokio::test]
    async fn a_node_with_tls_never_forwards_over_plain_http() {
        let (b, log) = fake(Duration::ZERO).await;
        let peers = Peers::new("node-a", Some(&identity())).unwrap();
        let r = peers
            .send("node-b", &b, "/v1/x", asked_by("Bearer t"), DETAIL_LIMIT)
            .await;
        assert_eq!(r.status(), 502);
        assert!(log.lock().unwrap().is_empty(), "nothing sent in clear");
    }

    /// A forwarded `429` keeps its `Retry-After`, a `401` its `WWW-Authenticate`.
    #[tokio::test]
    async fn a_forwarded_answer_keeps_its_retry_and_auth_headers() {
        let app = Router::new()
            .route(
                "/busy",
                axum::routing::get(|| async {
                    let mut r = body(StatusCode::TOO_MANY_REQUESTS, &json!({}));
                    r.headers_mut().insert("retry-after", "7".parse().unwrap());
                    r
                }),
            )
            .route(
                "/who",
                axum::routing::get(|| async {
                    let mut r = body(StatusCode::UNAUTHORIZED, &json!({}));
                    r.headers_mut()
                        .insert("www-authenticate", "Bearer realm=\"x\"".parse().unwrap());
                    r
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let peers = Peers::new("node-a", None).unwrap();
        let r = peers
            .send("node-b", &url, "/busy", asked_by("Bearer t"), DETAIL_LIMIT)
            .await;
        assert_eq!(r.status(), 429);
        assert_eq!(r.headers()["retry-after"], "7");
        assert_eq!(r.headers()["content-type"], "application/json");
        let r = peers
            .send("node-b", &url, "/who", asked_by("Bearer t"), DETAIL_LIMIT)
            .await;
        assert_eq!(r.status(), 401);
        assert_eq!(r.headers()["www-authenticate"], "Bearer realm=\"x\"");
    }

    /// An answer without a length is read against the cap a chunk at a time: one past it is a
    /// `502` as soon as it is, not once it ends.
    #[tokio::test]
    async fn an_answer_over_the_cap_is_cut_off_as_it_is_read() {
        use futures_util::StreamExt;
        let app = Router::new().fallback(|| async {
            let chunks = futures_util::stream::iter(
                (0..16).map(|_| Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; 1 << 20]))),
            )
            .chain(futures_util::stream::pending());
            axum::body::Body::from_stream(chunks)
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let t = Instant::now();
        let r = Peers::new("node-a", None)
            .unwrap()
            .send("node-b", &url, "/v1/x", asked_by("Bearer t"), DETAIL_LIMIT)
            .await;
        assert_eq!(r.status(), 502);
        assert!(t.elapsed() < DETAIL_LIMIT, "{:?}", t.elapsed());
    }

    /// A request to forward never prints the caller's credentials.
    #[test]
    fn an_asked_request_s_debug_hides_its_credentials() {
        let shown = format!("{:?}", asked_by("Bearer the-callers-token"));
        assert!(!shown.contains("the-callers-token"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    /// A member that gossips no API address is a `503` naming it; a request forwarded to this
    /// node for its own detail is answered here.
    #[tokio::test]
    async fn a_member_without_an_api_address_is_503_and_this_node_answers_its_own() {
        let (addr, _lone, _dir) = served("", open()).await;
        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-b", &[], "").await;
        assert_eq!(code, 503, "{text}");
        assert_eq!(json(&text)["error"], "node-b gossips no API address");
        let forwarded = [(FORWARDED_BY, "node-c"), (TARGET, "node-a")];
        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-a", &forwarded, "").await;
        assert_eq!(
            (code, &json(&text)["node"]),
            (200, &json!("node-a")),
            "{text}"
        );
    }

    /// The cluster's nodes share a CA: a peer trusts every certificate of its own `api.tls`.
    #[tokio::test]
    async fn peers_trust_this_node_s_own_certificate() {
        let tls = identity();
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
