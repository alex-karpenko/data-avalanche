//! `GET /v1/stream` (M13f, Admin_Console §8): the cluster's live changes as server-sent events.
//! A stream starts with `hello`, then sends each change as it happens, without replay: a page
//! reads the current state, then applies what follows. A stream that fell too far behind is sent
//! `reset`, and reads again.
//!
//! The node a page asks merges into its stream each other member's, followed with the caller's
//! own credentials (`peer` says when one is followed and when it can't be, `members` when the
//! members change); a stream it is asked for by another node is its own alone. Before it follows
//! a peer again, it checks the caller's credentials itself: once they no longer verify, the stream
//! is sent `reset` (`credentials`) and ends, and the page opens it again with fresh ones.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use std::collections::BTreeMap;

use axum::Extension;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use data_avalanche_cluster::{Live, MAX_STREAMS, Members, Opened};
use data_avalanche_config::{ResolvedConfig, Role};
use serde_json::{Value, json};
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::Instant;

use crate::auth::{Denied, Presented, Principal};
use crate::follow::FOLLOW_SILENCE;
use crate::nodes::phase;
use crate::peers::FORWARDED_BY;
use crate::{Api, Auth, Failure, Node, starting};

/// What `GET /v1/stream` takes, each once.
pub const PARAMETERS: &[(&str, &str)] = &[
    (
        "pipeline",
        "a pipeline's id, or several comma-separated: only the messages about them; by default every message",
    ),
    (
        "kind",
        "the kinds sent, comma-separated, of `pipeline`, `figures`, `node`, `samples`, `event`; by default every kind",
    ),
];

/// The kinds of change a stream can be asked for; `hello`, `reset`, `members` and `peer` are
/// always sent.
pub const KINDS: &[&str] = &["pipeline", "figures", "node", "samples", "event"];

/// How often a stream with nothing to send sends a comment, under the server's idle timeout.
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// How long a peer's stream that ended or failed waits before it is followed again: each wait in
/// turn, the last one from then on, until it is followed.
pub const FOLLOW_BACKOFF: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

/// How long a client refused for [`MAX_STREAMS`] waits before it tries again, in seconds.
const RETRY_AFTER: &str = "5";

/// The messages a stream's response holds before the node waits on its client.
const BUFFER: usize = 64;

/// Which messages a stream is sent.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Filter {
    pipelines: Option<BTreeSet<String>>,
    kinds: Option<BTreeSet<&'static str>>,
}

impl Filter {
    /// Whether `live` is sent: of a kind asked, and, when pipelines are asked, about one of them.
    pub(crate) fn passes(&self, live: &Live) -> bool {
        self.kinds.as_ref().is_none_or(|k| k.contains(live.kind))
            && self
                .pipelines
                .as_ref()
                .is_none_or(|p| live.pipeline.as_ref().is_some_and(|id| p.contains(id)))
    }
}

fn bad(error: impl Into<String>) -> Failure {
    Failure::new(StatusCode::BAD_REQUEST, error)
}

/// The filter of `params`, each once, against `config`. A stream another node asked for
/// (`config` none) was checked by that node, which may run a newer generation: a pipeline or a
/// kind this node doesn't know is taken (it passes nothing until this node runs it), and a
/// parameter it doesn't know is skipped.
fn filter(params: &[(String, String)], config: Option<&ResolvedConfig>) -> Result<Filter, Failure> {
    let mut f = Filter::default();
    let mut given = BTreeSet::new();
    for (name, value) in params {
        if !given.insert(name.as_str()) {
            return Err(bad(format!("{name} is given twice")));
        }
        let list: Vec<&str> = value
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        match name.as_str() {
            "pipeline" => {
                if list.is_empty() {
                    return Err(bad("pipeline names a pipeline's id"));
                }
                if let Some(c) = config
                    && let Some(id) = list
                        .iter()
                        .find(|id| !c.pipelines.values().any(|p| p.id == **id))
                {
                    return Err(Failure::new(
                        StatusCode::NOT_FOUND,
                        format!("no pipeline {id:?} in the config generation this node runs"),
                    ));
                }
                f.pipelines = Some(list.iter().map(|s| (*s).to_owned()).collect());
            }
            "kind" => {
                if list.is_empty() {
                    return Err(bad("kind names a kind"));
                }
                let mut kinds = BTreeSet::new();
                for k in list {
                    let Some(k) = KINDS.iter().find(|n| **n == k) else {
                        if config.is_none() {
                            continue;
                        }
                        return Err(bad(format!(
                            "unknown kind {k:?}: a stream sends {}",
                            KINDS.join(", ")
                        )));
                    };
                    kinds.insert(*k);
                }
                f.kinds = Some(kinds);
            }
            _ if config.is_none() => {}
            _ => {
                return Err(bad(format!(
                    "unknown parameter {name:?}: the stream takes {}",
                    PARAMETERS
                        .iter()
                        .map(|(n, _)| *n)
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
    }
    Ok(f)
}

/// A message as the stream sends it.
fn event(kind: &str, data: &Value) -> Event {
    Event::default().event(kind).data(data.to_string())
}

/// An open stream checks its caller again this often, and at its credentials' expiry when that
/// comes first: once they no longer verify (a session or a token ended, a password changed, a
/// role taken away), it is sent `reset` (`credentials`) and ends, and the page opens it again
/// with fresh credentials.
pub const CREDENTIALS_EVERY: Duration = Duration::from_secs(60);

/// `GET /v1/stream`.
pub(crate) async fn open(
    State(api): State<Api>,
    Extension(caller): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let raw = raw.unwrap_or_default();
    let params: Vec<(String, String)> = url::form_urlencoded::parse(raw.as_bytes())
        .into_owned()
        .collect();
    let forwarded = headers.contains_key(FORWARDED_BY);
    // Asked by another node, which checked the query: no config read.
    let config = if forwarded {
        None
    } else {
        Some(node.coordinator.config().await)
    };
    let filter = match filter(&params, config.as_ref()) {
        Ok(f) => f,
        Err(f) => return f.into(),
    };
    let Some(opened) = node.feed.open() else {
        let mut r: Response = Failure::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{MAX_STREAMS} streams are open on this node, as many as it takes"),
        )
        .into();
        r.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static(RETRY_AFTER));
        return r;
    };
    let me = node.members.borrow().me.clone();
    // Asked by another node: this node's own messages alone, never followed further.
    let follow = (!forwarded).then(|| {
        Arc::new(Follow {
            node: node.clone(),
            auth: Arc::clone(&api.auth),
            path: if raw.is_empty() {
                "/v1/stream".to_owned()
            } else {
                format!("/v1/stream?{raw}")
            },
            presented: Presented::of(&headers),
            expires: caller.expires,
        })
    });
    let (tx, rx) = mpsc::channel(BUFFER);
    tokio::spawn(pump(me, filter, opened, tx, follow));
    let events = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|e| (Ok::<_, Infallible>(e), rx))
    });
    let mut r = Sse::new(events)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response();
    // A proxy in front of the node passes each message on as it comes.
    r.headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    r
}

/// Sends `e` unless the client went or the node stops first; whether the stream goes on.
async fn deliver(tx: &mpsc::Sender<Event>, opened: &mut Opened, e: Event) -> bool {
    tokio::select! {
        sent = tx.send(e) => sent.is_ok(),
        _ = opened.closed.wait_for(|c| *c) => false,
    }
}

/// What a stream follows its peers with: the node's members and client, the API's own check of
/// a caller, the stream's own path and query, the credentials the caller presented (never
/// logged), and when they expire.
struct Follow {
    node: Node,
    auth: Arc<Auth>,
    path: String,
    presented: Presented,
    expires: Option<DateTime<Utc>>,
}

impl Follow {
    /// Whether the caller isn't refused here, as the route's own check says: a token expired, a
    /// password changed or a role taken away since the stream opened is. A verifier that can't
    /// check now (its settings refused, the issuer's keys unreachable: a 503) refuses no caller:
    /// the peer is followed, and answers for itself.
    async fn still_verified(&self) -> bool {
        match self.auth.verify(&self.presented.headers()).await {
            Ok(p) => p.role >= Role::Viewer,
            Err(Denied::Unavailable(_)) => true,
            Err(Denied::Unauthenticated { .. } | Denied::Forbidden { .. }) => false,
        }
    }

    /// When the caller is checked again: [`CREDENTIALS_EVERY`] from now, or a second after its
    /// credentials expire when that comes first.
    fn next_check(&self) -> Instant {
        let every = Instant::now() + CREDENTIALS_EVERY;
        self.expires
            .and_then(|at| (at - Utc::now()).to_std().ok())
            .map_or(every, |left| {
                every.min(Instant::now() + left + Duration::from_secs(1))
            })
    }
}

/// Resolves at `when`; never without one.
async fn at(when: Option<Instant>) {
    match when {
        Some(when) => tokio::time::sleep_until(when).await,
        None => std::future::pending().await,
    }
}

/// Each other member with an API address, by name.
fn peers_of(view: &Members) -> BTreeMap<String, String> {
    view.nodes
        .iter()
        .filter(|(name, m)| **name != view.me && !m.meta.api_url.is_empty())
        .map(|(name, m)| (name.clone(), m.meta.api_url.clone()))
        .collect()
}

/// The members as `members` lists them.
fn listed(view: &Members) -> Value {
    view.nodes
        .iter()
        .map(|(name, m)| json!({ "node": name, "phase": phase(m.meta.phase) }))
        .collect()
}

/// The peers a stream follows, each by its own task, all ended with the stream; `refused` is
/// told when the caller's credentials no longer verify.
#[derive(Default)]
struct Following {
    tasks: JoinSet<()>,
    nodes: BTreeMap<String, (String, AbortHandle)>,
    refused: Arc<Notify>,
}

impl Following {
    /// Follows each peer of `view` not yet followed at its address, and stops following the
    /// others.
    fn reconcile(&mut self, f: &Arc<Follow>, view: &Members, tx: &mpsc::Sender<Event>) {
        let want = peers_of(view);
        self.nodes.retain(|name, (url, task)| {
            let keep = want.get(name) == Some(url);
            if !keep {
                task.abort();
            }
            keep
        });
        for (name, url) in want {
            if !self.nodes.contains_key(&name) {
                let task = self.tasks.spawn(follow_peer(
                    Arc::clone(f),
                    view.me.clone(),
                    name.clone(),
                    url.clone(),
                    tx.clone(),
                    Arc::clone(&self.refused),
                ));
                self.nodes.insert(name, (url, task));
            }
        }
        while self.tasks.try_join_next().is_some() {}
    }
}

/// Passes `peer`'s messages on, its `hello` as `peer` following; when its stream can't be
/// opened, ends or fails, says so as `peer` unreachable and follows it again after
/// [`FOLLOW_BACKOFF`]. Ends once the stream's client goes, or, telling `refused`, when the
/// caller's credentials no longer verify here before it is followed again.
async fn follow_peer(
    f: Arc<Follow>,
    me: String,
    peer: String,
    url: String,
    tx: mpsc::Sender<Event>,
    refused: Arc<Notify>,
) {
    let mut failed = 0;
    let mut again = false;
    loop {
        if std::mem::replace(&mut again, true) && !f.still_verified().await {
            refused.notify_one();
            return;
        }
        let error = match f
            .node
            .peers
            .follow(&peer, &url, &f.path, &f.presented, FOLLOW_SILENCE)
            .await
        {
            Err(e) => e,
            Ok(mut followed) => loop {
                let e = match followed.next().await {
                    Ok(Some(f)) if f.kind == "hello" => {
                        failed = 0;
                        event(
                            "peer",
                            &json!({ "node": me, "peer": peer, "state": "following" }),
                        )
                    }
                    Ok(Some(f)) => Event::default().event(f.kind).data(f.data),
                    Ok(None) => break "its stream ended".to_owned(),
                    Err(e) => break e,
                };
                if tx.send(e).await.is_err() {
                    return;
                }
            },
        };
        let unreachable =
            json!({ "node": me, "peer": peer, "state": "unreachable", "error": error });
        if tx.send(event("peer", &unreachable)).await.is_err() {
            return;
        }
        let wait = FOLLOW_BACKOFF[failed.min(FOLLOW_BACKOFF.len() - 1)];
        failed += 1;
        tokio::select! {
            () = tx.closed() => return,
            () = tokio::time::sleep(wait) => {}
        }
    }
}

/// The next change of the members, when the stream follows them.
async fn changed(members: &mut Option<watch::Receiver<Members>>) -> Option<()> {
    match members {
        Some(m) => m.changed().await.ok(),
        None => std::future::pending().await,
    }
}

/// What a stream does next.
enum Next {
    /// The members changed.
    Members,
    /// This node's next message, or how many it missed.
    Live(Result<Arc<Live>, broadcast::error::RecvError>),
    /// The caller's credentials no longer verify.
    Refused,
    /// Time to check the caller again.
    Check,
}

/// Sends `hello`, then this node's messages that pass `filter` and, with `follow`, each peer's,
/// until the client goes or the node stops; or, once the caller's credentials no longer verify
/// (checked every [`CREDENTIALS_EVERY`], at their expiry, and before a peer is followed again),
/// `reset` (`credentials`), and ends.
async fn pump(
    me: String,
    filter: Filter,
    mut opened: Opened,
    tx: mpsc::Sender<Event>,
    follow: Option<Arc<Follow>>,
) {
    let mut members = follow.as_ref().map(|f| f.node.members.clone());
    let view = members.as_mut().map(|m| m.borrow_and_update().clone());
    let follows: Vec<String> = view
        .as_ref()
        .map(|v| peers_of(v).into_keys().collect())
        .unwrap_or_default();
    let hello = json!({ "node": me, "at": Utc::now(), "follows": follows });
    if !deliver(&tx, &mut opened, event("hello", &hello)).await {
        return;
    }
    let mut following = Following::default();
    let refused = Arc::clone(&following.refused);
    let mut shown = view.as_ref().map(listed);
    let mut check = follow.as_ref().map(|f| f.next_check());
    if let (Some(f), Some(view)) = (&follow, &view) {
        following.reconcile(f, view, &tx);
    }
    loop {
        let next = tokio::select! {
            () = tx.closed() => return,
            _ = opened.closed.wait_for(|c| *c) => return,
            () = refused.notified() => Next::Refused,
            () = at(check) => Next::Check,
            Some(()) = changed(&mut members) => Next::Members,
            r = opened.messages.recv() => Next::Live(r),
        };
        let next = match next {
            Next::Check => match &follow {
                Some(f) if !f.still_verified().await => Next::Refused,
                Some(f) => {
                    check = Some(f.next_check());
                    continue;
                }
                None => continue,
            },
            other => other,
        };
        let next = match next {
            Next::Check => continue,
            Next::Live(r) => r,
            Next::Refused => {
                // Its peers' messages end first, so `reset` is the stream's last.
                following.tasks.shutdown().await;
                let e = event("reset", &json!({ "node": me, "reason": "credentials" }));
                deliver(&tx, &mut opened, e).await;
                return;
            }
            Next::Members => {
                let (Some(f), Some(m)) = (&follow, &mut members) else {
                    continue;
                };
                let view = m.borrow_and_update().clone();
                following.reconcile(f, &view, &tx);
                let now = listed(&view);
                if shown.as_ref() == Some(&now) {
                    continue;
                }
                shown = Some(now.clone());
                let e = event("members", &json!({ "node": me, "members": now }));
                if !deliver(&tx, &mut opened, e).await {
                    return;
                }
                continue;
            }
        };
        let e = match next {
            Ok(live) if filter.passes(&live) => event(live.kind, &live.data),
            Ok(_) => continue,
            Err(broadcast::error::RecvError::Lagged(missed)) => event(
                "reset",
                &json!({ "node": me, "reason": "lagged", "missed": missed }),
            ),
            Err(broadcast::error::RecvError::Closed) => return,
        };
        if !deliver(&tx, &mut opened, e).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use data_avalanche_cluster::FEED_CAPACITY;

    use super::*;
    use crate::test_support::{
        Stream, basic_api, basic_session, served_with, session_cookie, sessions, stream,
    };

    fn live(kind: &'static str, pipeline: Option<&str>, n: u64) -> Live {
        Live {
            kind,
            pipeline: pipeline.map(str::to_owned),
            data: json!({ "node": "node-a", "n": n }),
        }
    }

    /// node-a, with node-b a member without an API address: its streams are node-a's alone.
    async fn node_a() -> (SocketAddr, crate::test_support::Lone, tempfile::TempDir) {
        served_with("", crate::test_support::open(), |_| {}).await
    }

    /// A stream's `hello`, its node now following none.
    async fn hello(s: &mut Stream) {
        let (kind, data) = s.next().await;
        assert_eq!(kind, "hello");
        assert_eq!(data["node"], "node-a");
    }

    #[tokio::test]
    async fn a_stream_says_hello_then_sends_each_change_as_it_happens() {
        let (addr, lone, _dir) = node_a().await;
        let mut s = stream(addr, "/v1/stream", &[]).await;
        assert_eq!(s.code, 200);
        assert!(s.head.contains("text/event-stream"), "{}", s.head);
        assert!(s.head.contains("x-accel-buffering: no"), "{}", s.head);
        assert!(s.head.contains("cache-control: no-cache"), "{}", s.head);
        hello(&mut s).await;
        lone.node.feed.publish(live("figures", Some("orders"), 1));
        lone.node.feed.publish(live("node", None, 2));
        assert_eq!(
            s.next().await,
            ("figures".into(), live("figures", None, 1).data)
        );
        assert_eq!(s.next().await, ("node".into(), live("node", None, 2).data));
    }

    /// Served whatever the console's switch says, as the events and the history are.
    #[tokio::test]
    async fn a_stream_is_served_with_the_console_off() {
        use crate::test_support::{CONFIG, api, lone_with, serve};
        let text = CONFIG.replace(
            "pipelines:\n",
            "  console: { enabled: false }\npipelines:\n",
        );
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, &text).unwrap();
        let lone = lone_with(
            file,
            &text,
            std::sync::Arc::new(data_avalanche_config::ProcessEnv),
        )
        .await;
        assert!(!lone.node.coordinator.config().await.globals.console.enabled);
        let a = api(crate::test_support::open());
        a.node.set(lone.node.clone()).unwrap();
        let mut s = stream(serve(a).await, "/v1/stream", &[]).await;
        assert_eq!(s.code, 200);
        hello(&mut s).await;
    }

    #[tokio::test]
    async fn a_stream_asked_for_a_pipeline_and_kinds_gets_only_those() {
        let (addr, lone, _dir) = node_a().await;
        let mut s = stream(addr, "/v1/stream?pipeline=orders&kind=event,figures", &[]).await;
        hello(&mut s).await;
        let feed = &lone.node.feed;
        feed.publish(live("node", None, 1));
        feed.publish(live("event", None, 2));
        feed.publish(live("pipeline", Some("orders"), 3));
        feed.publish(live("figures", Some("other"), 4));
        feed.publish(live("event", Some("orders"), 5));
        assert_eq!(s.next().await.1["n"], 5);
    }

    #[tokio::test]
    async fn a_parameter_that_isnt_one_is_refused() {
        let (addr, _lone, _dir) = node_a().await;
        for (path, code, error) in [
            (
                "/v1/stream?kind=nodes",
                400,
                "unknown kind \"nodes\": a stream sends pipeline, figures, node, samples, event",
            ),
            (
                "/v1/stream?pipeline=",
                400,
                "pipeline names a pipeline's id",
            ),
            (
                "/v1/stream?kind=node&kind=event",
                400,
                "kind is given twice",
            ),
            (
                "/v1/stream?since=1",
                400,
                "unknown parameter \"since\": the stream takes pipeline, kind",
            ),
            (
                "/v1/stream?pipeline=orders,nope",
                404,
                "no pipeline \"nope\" in the config generation this node runs",
            ),
        ] {
            let (c, v) = crate::test_support::json_of(addr, "GET", path).await;
            assert_eq!((c, v["error"].as_str().unwrap()), (code, error), "{path}");
        }
    }

    /// Another node, on a newer generation, already checked what it asks: a pipeline or a kind
    /// this one doesn't know is accepted.
    #[tokio::test]
    async fn a_stream_another_node_asked_for_accepts_what_this_one_doesnt_know() {
        let (addr, lone, _dir) = node_a().await;
        let path = "/v1/stream?pipeline=nope&kind=node,future&since=1";
        let mut s = stream(addr, path, &[(FORWARDED_BY, "node-c")]).await;
        assert_eq!(s.code, 200);
        hello(&mut s).await;
        let feed = &lone.node.feed;
        feed.publish(live("figures", Some("nope"), 1));
        feed.publish(live("node", Some("orders"), 2));
        feed.publish(live("node", None, 3));
        feed.publish(live("node", Some("nope"), 4));
        assert_eq!(s.next().await.1["n"], 4, "only node, about nope");
        let twice = [(FORWARDED_BY, "node-c")];
        let (code, _, body) = crate::test_support::request(
            addr,
            "GET",
            "/v1/stream?kind=node&kind=event",
            &twice,
            "",
        )
        .await;
        assert_eq!(code, 400, "{body}");
        assert!(body.contains("kind is given twice"), "{body}");
        // Asked directly, the same is refused.
        for (path, code) in [
            ("/v1/stream?pipeline=nope&kind=node,future", 404),
            ("/v1/stream?kind=node,future", 400),
            ("/v1/stream?since=1", 400),
        ] {
            let (c, _) = crate::test_support::json_of(addr, "GET", path).await;
            assert_eq!(c, code, "{path}");
        }
    }

    #[tokio::test]
    async fn a_stream_far_behind_is_told_to_read_again() {
        let (addr, lone, _dir) = node_a().await;
        let mut s = stream(addr, "/v1/stream", &[]).await;
        hello(&mut s).await;
        // Published at once, before the stream reads one: it falls 10 behind the feed's capacity.
        for n in 0..(FEED_CAPACITY as u64 + 10) {
            lone.node.feed.publish(live("node", None, n));
        }
        let (kind, data) = s.next().await;
        assert_eq!(kind, "reset");
        assert_eq!(
            data,
            json!({ "node": "node-a", "reason": "lagged", "missed": 10 })
        );
        assert_eq!(
            s.next().await.1["n"],
            10,
            "then what it still holds, in order"
        );
    }

    #[tokio::test]
    async fn at_most_max_streams_are_open_on_a_node() {
        let (addr, lone, _dir) = node_a().await;
        let held: Vec<_> = (0..MAX_STREAMS).map(|_| lone.node.feed.open()).collect();
        let (code, head, body) = crate::test_support::get(addr, "/v1/stream").await;
        assert_eq!(code, 503);
        assert!(head.to_lowercase().contains("retry-after: 5"), "{head}");
        assert!(body.contains("64 streams are open on this node"), "{body}");
        drop(held);
        let mut s = stream(addr, "/v1/stream", &[]).await;
        hello(&mut s).await;
    }

    /// A node that stops ends its streams: an open one doesn't hold the server's stop.
    #[tokio::test]
    async fn a_closed_feed_ends_every_stream() {
        let (addr, lone, _dir) = node_a().await;
        let mut s = stream(addr, "/v1/stream", &[]).await;
        hello(&mut s).await;
        lone.node.feed.close();
        assert!(s.ended().await, "the response ends");
    }

    /// A stream whose client went gives its place back.
    #[tokio::test]
    async fn a_client_that_goes_gives_its_place_back() {
        let (addr, lone, _dir) = node_a().await;
        let mut s = stream(addr, "/v1/stream", &[]).await;
        hello(&mut s).await;
        assert_eq!(lone.node.feed.followers(), 1);
        drop(s);
        let within = tokio::time::Instant::now() + Duration::from_secs(3);
        while lone.node.feed.followers() > 0 {
            assert!(tokio::time::Instant::now() < within, "still open");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A fake node-b whose stream says `hello`, then sends what the test sends it; what each
    /// request carried is kept.
    struct NodeB {
        url: String,
        send: broadcast::Sender<(String, Value)>,
        seen: std::sync::Arc<std::sync::Mutex<Vec<(String, HeaderMap)>>>,
    }

    async fn node_b() -> NodeB {
        use axum::http::Uri;
        use futures_util::StreamExt;
        let send = broadcast::Sender::<(String, Value)>::new(16);
        let seen = std::sync::Arc::<std::sync::Mutex<Vec<(String, HeaderMap)>>>::default();
        let (sender, kept) = (send.clone(), std::sync::Arc::clone(&seen));
        let app = axum::Router::new().route(
            "/v1/stream",
            axum::routing::get(move |uri: Uri, headers: HeaderMap| {
                kept.lock().unwrap().push((uri.to_string(), headers));
                let rx = sender.subscribe();
                async move {
                    let hello = event("hello", &json!({ "node": "node-b" }));
                    let rest = futures_util::stream::unfold(rx, |mut rx| async move {
                        let (kind, data) = rx.recv().await.ok()?;
                        Some((event(&kind, &data), rx))
                    });
                    Sse::new(
                        futures_util::stream::iter([hello])
                            .chain(rest)
                            .map(Ok::<_, Infallible>),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        NodeB { url, send, seen }
    }

    #[tokio::test]
    async fn a_stream_merges_each_other_node_s_followed_with_the_callers_credentials() {
        let b = node_b().await;
        let (addr, lone, _dir) = served_with(&b.url, crate::test_support::open(), |_| {}).await;
        let mut s = stream(
            addr,
            "/v1/stream?kind=figures,node",
            &[("authorization", "Basic dmlld2VyOnZpZXdlcg==")],
        )
        .await;
        let (kind, hello) = s.next().await;
        assert_eq!(
            (kind.as_str(), &hello["follows"]),
            ("hello", &json!(["node-b"]))
        );
        assert_eq!(
            s.next().await,
            (
                "peer".into(),
                json!({ "node": "node-a", "peer": "node-b", "state": "following" })
            )
        );
        b.send
            .send((
                "figures".into(),
                json!({ "node": "node-b", "id": "orders" }),
            ))
            .unwrap();
        assert_eq!(
            s.next().await,
            (
                "figures".into(),
                json!({ "node": "node-b", "id": "orders" })
            ),
            "as node-b sent it"
        );
        lone.node.feed.publish(live("node", None, 1));
        assert_eq!(
            s.next().await.1,
            live("node", None, 1).data,
            "and node-a's own"
        );
        let (uri, h) = b.seen.lock().unwrap()[0].clone();
        assert_eq!(
            uri, "/v1/stream?kind=figures,node",
            "asked as the caller asked"
        );
        assert_eq!(h[crate::peers::TARGET], "node-b");
        assert_eq!(h[FORWARDED_BY], "node-a");
        assert_eq!(h[header::AUTHORIZATION], "Basic dmlld2VyOnZpZXdlcg==");
    }

    #[tokio::test]
    async fn a_peer_that_cant_be_followed_is_named_and_followed_again() {
        let (b_url, log) = crate::test_support::fake_answering(
            Duration::ZERO,
            StatusCode::NOT_FOUND,
            json!({ "error": "no pipeline \"orders\" in the config generation this node runs" }),
        )
        .await;
        let (addr, _lone, _dir) = served_with(&b_url, crate::test_support::open(), |_| {}).await;
        let mut s = stream(addr, "/v1/stream", &[]).await;
        s.next().await;
        let unreachable = json!({
            "node": "node-a",
            "peer": "node-b",
            "state": "unreachable",
            "error": "it answered 404: no pipeline \"orders\" in the config generation this node runs",
        });
        assert_eq!(s.next().await, ("peer".into(), unreachable.clone()));
        // After the first wait, FOLLOW_BACKOFF[0].
        assert_eq!(s.next().await, ("peer".into(), unreachable));
        assert_eq!(log.lock().unwrap().len(), 2);
    }

    /// `ops:s3cret`, as a Basic `Authorization`.
    const OPS: &str = "Basic b3BzOnMzY3JldA==";

    #[tokio::test]
    async fn a_stream_whose_credentials_no_longer_verify_ends_before_a_peer_is_followed_again() {
        let (b_url, log) = crate::test_support::fake_answering(
            Duration::ZERO,
            StatusCode::NOT_FOUND,
            json!({ "error": "no pipeline \"orders\"" }),
        )
        .await;
        let auth = std::sync::Arc::new(crate::Auth::new(
            &basic_api("s3cret"),
            &crate::test_support::console(),
        ));
        let (addr, _lone, _dir) = served_with(&b_url, std::sync::Arc::clone(&auth), |_| {}).await;
        let mut s = stream(addr, "/v1/stream", &[("authorization", OPS)]).await;
        assert_eq!(s.code, 200);
        s.next().await;
        assert_eq!(s.next().await.1["state"], "unreachable");
        // The password changes before node-b is followed again (FOLLOW_BACKOFF[0]).
        auth.set(&basic_api("rotated"), &crate::test_support::console());
        assert_eq!(
            s.next().await,
            (
                "reset".into(),
                json!({ "node": "node-a", "reason": "credentials" })
            )
        );
        assert!(s.ended().await, "the stream ends: the page opens it again");
        assert_eq!(log.lock().unwrap().len(), 1, "node-b never asked again");
    }

    /// A verifier that can't check now (the settings refused after a reload) isn't the caller
    /// failing: the peer is followed again, and its own answer is what the page sees.
    #[tokio::test]
    async fn a_stream_whose_verifier_is_unavailable_goes_on_following() {
        let (b_url, log) = crate::test_support::fake_answering(
            Duration::ZERO,
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "the API's auth settings are inconsistent" }),
        )
        .await;
        let auth = std::sync::Arc::new(crate::Auth::new(
            &basic_api("s3cret"),
            &crate::test_support::console(),
        ));
        let (addr, lone, _dir) = served_with(&b_url, std::sync::Arc::clone(&auth), |_| {}).await;
        let mut s = stream(addr, "/v1/stream", &[("authorization", OPS)]).await;
        s.next().await;
        let unreachable = json!({
            "node": "node-a",
            "peer": "node-b",
            "state": "unreachable",
            "error": "it answered 503: the API's auth settings are inconsistent",
        });
        assert_eq!(s.next().await, ("peer".into(), unreachable.clone()));
        // `auth: basic` without its credentials: every caller is answered 503, none refused.
        let mut refused = basic_api("s3cret");
        refused.credentials = None;
        auth.set(&refused, &crate::test_support::console());
        assert_eq!(
            s.next().await,
            ("peer".into(), unreachable),
            "followed again, no reset"
        );
        assert_eq!(log.lock().unwrap().len(), 2);
        lone.node.feed.publish(live("node", None, 1));
        assert_eq!(s.next().await.1["n"], 1, "the stream goes on");
    }

    #[tokio::test]
    async fn a_peer_refusing_credentials_this_node_takes_is_unreachable_and_the_stream_goes_on() {
        let (b_url, log) = crate::test_support::fake_answering(
            Duration::ZERO,
            StatusCode::UNAUTHORIZED,
            json!({ "error": "wrong user or password" }),
        )
        .await;
        let auth = std::sync::Arc::new(crate::Auth::new(
            &basic_api("s3cret"),
            &crate::test_support::console(),
        ));
        let (addr, lone, _dir) = served_with(&b_url, auth, |_| {}).await;
        let mut s = stream(addr, "/v1/stream", &[("authorization", OPS)]).await;
        s.next().await;
        let unreachable = json!({
            "node": "node-a",
            "peer": "node-b",
            "state": "unreachable",
            "error": "it answered 401: wrong user or password",
        });
        assert_eq!(s.next().await, ("peer".into(), unreachable.clone()));
        assert_eq!(s.next().await, ("peer".into(), unreachable), "asked again");
        assert_eq!(log.lock().unwrap().len(), 2);
        lone.node.feed.publish(live("node", None, 1));
        assert_eq!(s.next().await.1["n"], 1, "the stream goes on");
    }

    /// M13f's carry-in: a stream ends with its caller's session, and a token's stream with its
    /// token, at their end; the page opens it again with fresh credentials.
    #[tokio::test]
    async fn a_stream_ends_when_its_session_does() {
        let hour = Duration::from_secs(3600);
        let auth = std::sync::Arc::new(crate::Auth::new(&basic_api("s3cret"), &sessions(hour)));
        let (addr, lone, _dir) = served_with("", auth, |_| {}).await;
        // It ends within three seconds.
        let session = basic_session("s3cret", hour - Duration::from_secs(2));
        let cookie = session_cookie(&session);
        let opened = std::time::Instant::now();
        let mut s = stream(addr, "/v1/stream", &[("cookie", cookie.as_str())]).await;
        assert_eq!(s.code, 200);
        hello(&mut s).await;
        lone.node.feed.publish(live("node", None, 1));
        assert_eq!(
            s.next().await.1["n"],
            1,
            "it streams while the session lasts"
        );
        assert_eq!(
            s.next().await,
            (
                "reset".into(),
                json!({ "node": "node-a", "reason": "credentials" })
            )
        );
        assert!(s.ended().await, "the stream ends: the page opens it again");
        let took = opened.elapsed();
        assert!(
            took >= Duration::from_secs(1) && took < Duration::from_secs(8),
            "{took:?}"
        );
    }

    #[tokio::test]
    async fn a_stream_another_node_asked_for_follows_none() {
        let b = node_b().await;
        let (addr, lone, _dir) = served_with(&b.url, crate::test_support::open(), |_| {}).await;
        let mut s = stream(addr, "/v1/stream", &[(FORWARDED_BY, "node-c")]).await;
        let (_, hello) = s.next().await;
        assert_eq!(hello["follows"], json!([]));
        lone.node.feed.publish(live("node", None, 1));
        assert_eq!(s.next().await.1["n"], 1);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(b.seen.lock().unwrap().is_empty(), "node-b never asked");
    }

    #[tokio::test]
    async fn a_member_at_a_new_address_is_followed_there() {
        use data_avalanche_cluster::Phase;

        use crate::test_support::member;
        let (old, new) = (node_b().await, node_b().await);
        let at = |url: &str| Members {
            me: "node-a".into(),
            nodes: [
                ("node-a".to_owned(), member("", Phase::Active)),
                ("node-b".to_owned(), member(url, Phase::Active)),
            ]
            .into(),
        };
        let (view, members) = watch::channel(at(&old.url));
        let (addr, _lone, _dir) = served_with("", crate::test_support::open(), |n| {
            n.members = members;
        })
        .await;
        let mut s = stream(addr, "/v1/stream", &[]).await;
        s.next().await;
        assert_eq!(s.next().await.1["state"], "following");
        view.send_replace(at(&new.url));
        assert_eq!(
            s.next().await,
            (
                "peer".into(),
                json!({ "node": "node-a", "peer": "node-b", "state": "following" })
            ),
            "followed again, at its new address"
        );
        assert_eq!(new.seen.lock().unwrap().len(), 1);
        let within = tokio::time::Instant::now() + Duration::from_secs(3);
        while old.send.receiver_count() > 0 {
            assert!(
                tokio::time::Instant::now() < within,
                "the old address still followed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn a_member_that_leaves_is_no_longer_followed_and_the_members_are_sent() {
        use data_avalanche_cluster::Phase;

        use crate::test_support::member;
        let b = node_b().await;
        let both = Members {
            me: "node-a".into(),
            nodes: [
                ("node-a".to_owned(), member("", Phase::Active)),
                ("node-b".to_owned(), member(&b.url, Phase::Active)),
            ]
            .into(),
        };
        let (view, members) = watch::channel(both.clone());
        let (addr, _lone, _dir) = served_with("", crate::test_support::open(), |n| {
            n.members = members;
        })
        .await;
        let mut s = stream(addr, "/v1/stream", &[]).await;
        s.next().await;
        assert_eq!(s.next().await.1["state"], "following");
        let mut alone = both.clone();
        alone.nodes.remove("node-b");
        view.send_replace(alone);
        assert_eq!(
            s.next().await,
            (
                "members".into(),
                json!({ "node": "node-a", "members": [{ "node": "node-a", "phase": "active" }] })
            )
        );
        let within = tokio::time::Instant::now() + Duration::from_secs(3);
        while b.send.receiver_count() > 0 {
            assert!(
                tokio::time::Instant::now() < within,
                "node-b still followed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
