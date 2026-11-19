//! `GET /v1/stream` (M13f, Admin_Console §8): the cluster's live changes as server-sent events.
//! A stream starts with `hello`, then sends each change as it happens, without replay: a page
//! reads the current state, then applies what follows. A stream that fell too far behind is sent
//! `reset`, and reads again.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{RawQuery, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use data_avalanche_cluster::{Live, MAX_STREAMS, Opened};
use data_avalanche_config::ResolvedConfig;
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};

use crate::{Api, Failure, starting};

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

/// The filter of `params`, each once, against `config`.
fn filter(params: &[(String, String)], config: &ResolvedConfig) -> Result<Filter, Failure> {
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
                for id in &list {
                    if !config.pipelines.values().any(|p| p.id == *id) {
                        return Err(Failure::new(
                            StatusCode::NOT_FOUND,
                            format!("no pipeline {id:?} in the config generation this node runs"),
                        ));
                    }
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
                        return Err(bad(format!(
                            "unknown kind {k:?}: a stream sends {}",
                            KINDS.join(", ")
                        )));
                    };
                    kinds.insert(*k);
                }
                f.kinds = Some(kinds);
            }
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

/// `GET /v1/stream`.
pub(crate) async fn open(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let params: Vec<(String, String)> =
        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let config = node.coordinator.config().await;
    let filter = match filter(&params, &config) {
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
    let (tx, rx) = mpsc::channel(BUFFER);
    tokio::spawn(pump(me, filter, opened, tx));
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

/// Sends `hello`, then this node's messages that pass `filter`, until the client goes or the
/// node stops.
async fn pump(me: String, filter: Filter, mut opened: Opened, tx: mpsc::Sender<Event>) {
    let hello = json!({ "node": me, "at": Utc::now(), "follows": [] });
    if !deliver(&tx, &mut opened, event("hello", &hello)).await {
        return;
    }
    loop {
        let next = tokio::select! {
            () = tx.closed() => return,
            _ = opened.closed.wait_for(|c| *c) => return,
            r = opened.messages.recv() => r,
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
    use crate::test_support::{Stream, served_with, stream};

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
        hello(&mut s).await;
        lone.node.feed.publish(live("figures", Some("orders"), 1));
        lone.node.feed.publish(live("node", None, 2));
        assert_eq!(
            s.next().await,
            ("figures".into(), live("figures", None, 1).data)
        );
        assert_eq!(s.next().await, ("node".into(), live("node", None, 2).data));
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
}
