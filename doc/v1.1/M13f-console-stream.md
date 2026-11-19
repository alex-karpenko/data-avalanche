---
updated: 2026-11-19
domain: Architecture
tags: [v1.1, implementation-plan, m13f, console, live-stream, sse, api, cluster, state]
---

# M13f — The Console's Live Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The sixth and last server milestone of the 1.1.0 admin console (Admin_Console §14). It
builds the push the console's pages live on (Admin_Console §8, "Live by push"):
- each node publishes its own changes as they happen, from its memory: its pipelines' states and
  live figures, its own figures, each chart sample it takes, each event it writes;
- `GET /v1/stream` sends them as server-sent events, and the node a page asks follows every other
  member's stream with the caller's credentials and merges them into one;
- the site and the E2E suite follow.

**Architecture:**

- **The feed** (Task 1, `data-avalanche-cluster`, `data-avalanche-state`): `Feed`, a node's live
  messages (`Live`) on a broadcast channel, which builds none while no stream is open. The events'
  recorder publishes each event once written, the history's sampler each sample once taken.
- **The publisher** (Task 2, `data-avalanche-api`, the binary): every second while a stream is
  open, each pipeline's state when it changed; every 5 s its live figures and the node's own.
- **`GET /v1/stream`, this node's own** (Task 3, `data-avalanche-api`, the binary): the SSE
  route, its filters and limits; a node that stops ends its streams before its server's stop.
- **Following the others** (Task 4, `data-avalanche-api`): the node asked follows each other
  member's stream (`Peers::follow`), passes its messages on, and says when a member is followed,
  unreachable, or gone.
- **The site** (Task 5) and **the E2E suite** (Task 6). On three nodes, a stream opened on a node
  that doesn't run the pipeline shows the holder's state, figures and an action's event; the holder
  killed, it names it unreachable and shows the next holder; an open stream doesn't hold the
  nodes' stop. The scenario runs on PostgreSQL and on all three MySQL targets.

**Tech Stack:** Rust 1.100.0 (edition 2024). No new crate, and no new feature of one: axum 0.8's
`response::sse` and reqwest's `Response::chunk` serve and follow the streams.
`data-avalanche-cluster` gains `serde_json`, already in the workspace.

**Spec:**
- `doc/research/Admin_Console.md`: §4 (what each page shows), §8 (freshness: live by push, age
  visible, degraded not blank, replication first), §10 (forwarding with the caller's own
  credentials), §12 (a live stream (SSE)), §13 (the SSE message shapes are the builder's), §14.
  Task 5 adds §8.1, the stream's shapes.
- The approved mockups `doc/v1.1/mockups/` (every page's "updated … ago", the "node-c unreachable"
  states of `11-degraded.html`).
- `doc/specs/System-Overview.md` §Web console, §Management API endpoints. Amended by S-M13f.1,
  below.
- `doc/v1.1/M13a-console-events.md` (the events' recorder), `M13b-console-nodes.md` (forwarding:
  `TARGET`, `FORWARDED_BY`, the caller's credentials; process and gossip figures),
  `M13d-console-pipelines.md` (the live figures), `M13e-console-history.md` (the sampler;
  D-M13e.1: a page draws its chart's tail from the stream's samples).

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`, with the sources and the sinks beside the pipeline. `testkit` is only
  ever a dev-dependency.
- **Served whatever the console's switch says** (Admin_Console §3), as `GET /v1/events` and
  `GET /v1/history` are: `GET /v1/stream` answers with `globals.console.enabled: false`.
- **Replication first** (Admin_Console §8): publishing never waits on a stream. A message goes on a
  broadcast channel that drops what a slow stream missed; the publisher reads the controllers'
  statuses and never storage; nothing is built while no stream is open.
- **No node-to-node trust** (Admin_Console §10, D-M13b.4): a followed stream carries the caller's
  own credentials and nothing of the node's, so each node verifies the caller itself.
- **No replay** (Oleksii): a stream sends what happens from its opening on; a page reads the
  current state first.
- **A stream never holds a node's stop**: the server's graceful stop waits for open requests, so
  the node ends its streams first.
- **Time in tests:** the publisher's tests pass `now` (no clock); the stream's tests run in real
  time on loopback ports, a fake peer serving its own stream.
- **Test gates:** plain `cargo test` stays hermetic. The E2E suite needs `DATA_AVALANCHE_IT=1` and
  the `e2e-hooks` feature, and runs with `--test-threads=1`, on PostgreSQL and on
  `DATA_AVALANCHE_E2E_SOURCE=mysql` (CI's e2e-mysql runs `mysql-84`, `mysql-84-nogtid` and
  `mariadb-118`).
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types.
- **Code blocks:**
  - They are the verified content, already `rustfmt`-formatted.
  - Every block is a `git apply`-ready diff, one per file.
  - A step's diffs apply in the order given, after the earlier steps'.
  - The OpenAPI document, the site's API reference and `Cargo.lock` are regenerated, not written
    (Tasks 1, 3).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone running 1.1.0 in production. Each is pinned by a
test in the task after the arrow.

1. **A page that stops reading** (a tab in the background, a laptop asleep). Its stream falls
   behind; the node must not hold its messages or slow, and the page must learn it missed some →
   Task 1 (`a_stream_far_behind_learns_it_missed_some`), Task 3
   (`a_stream_far_behind_is_told_to_read_again`).
2. **A node killed or cut off while pages are open.** The page shows it unreachable, not blank,
   and the stream follows it again when it is back → Task 4
   (`a_peer_that_cant_be_followed_is_named_and_followed_again`), Task 6 (the holder killed).
3. **A node stopping with streams open.** The server's graceful stop would wait for them forever
   → Task 3 (`a_closed_feed_ends_every_stream`), Task 6 (the nodes stop with a stream open).
4. **Many pages, or a reconnect storm after a deploy.** Each stream holds a connection and a task:
   64 a node, `503` with `Retry-After` past that, and a closed tab gives its place back → Task 3
   (`at_most_max_streams_are_open_on_a_node`, `a_client_that_goes_gives_its_place_back`).
5. **A peer that goes silent or sends what isn't a message** (a frozen process, a half-open
   connection, a proxy's page). The stream drops it after 45 s and reads no message over 1 MiB →
   Task 4 (`a_followed_stream_carries_the_callers_credentials_and_is_dropped_once_silent`,
   `a_message_over_the_cap_is_refused_as_it_is_read`).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-19):
- the stream sends **values as they change**, not "something changed, read again";
- a pipeline's and a node's **figures every 5 s**;
- **no replay**: a page reads the current state, then applies what follows.

The rest are Claude's calls under Admin_Console §13.

- **D-M13f.1 What this milestone serves, and what waits.**
  - Served here: every live value of the mockups' pages, pushed (Admin_Console §8). A page reads
    the current state from the existing routes, then applies the stream.
  - The chart's tail (D-M13e.1): the `samples` messages, each sample as taken, so a chart is never
    the stored history's minute behind.
  - M14: sessions. A stream's caller is verified when it opens; it outlives a token that expires
    after, until the session work says how a page's stream ends with its session.
  - M13e parked nothing for the stream.
- **D-M13f.2 `GET /v1/stream`** (viewer). Server-sent events (`text/event-stream`), each message's
  name its kind and its data one JSON object that names the node that sent it (`node`).
  - `pipeline` (an id, several comma-separated): only the messages about them. `404` for a pipeline
    not in the generation the node runs.
  - `kind` (comma-separated, of `pipeline`, `figures`, `node`, `samples`, `event`): only those.
    `400` for a kind that isn't one, an unknown parameter, or one given twice.
  - `hello`, `members`, `peer` and `reset` are always sent.
  - Not recorded (a viewer's `GET`); not cached (`Cache-Control: no-cache`, axum's); a proxy is
    asked not to buffer (`X-Accel-Buffering: no`).
- **D-M13f.3 The messages.**

  | Kind | From | When | Data |
  |---|---|---|---|
  | `hello` | the node asked | first | `node`, `at`, `follows` (the other members it follows) |
  | `pipeline` | the node running it | its state, error or lease term changed (looked at every `STATE_EVERY`, 1 s); `standby` once when the node stops running it; every state again within a second of a new stream | `node`, `pipeline`, `at`, `actual`, `error`, `owner_epoch` |
  | `figures` | the node running it | every `FIGURES_EVERY` (5 s) | `GET /v1/pipelines/{id}/live`'s answer, without the halts' start times (storage's) |
  | `node` | each node | every `FIGURES_EVERY` | `node`, `at`, `phase`, `process`, `gossip` (as `GET /v1/nodes/{node}` shows them), `pipelines` (the ids it runs) |
  | `samples` | each node | each sample the history takes (M13e) | `node`, `scope` (`pipeline`, `node`), `pipeline`, `at` (unix seconds), `series` (`{name: value}`), `tables` (`{table: {name: value}}`) |
  | `event` | the node that wrote it | once written | the event as `GET /v1/events` lists it (`EventDoc::to_json`, with its `key`) |
  | `members` | the node asked | the members or their phases changed | `node`, `members` (`[{node, phase}]`) |
  | `peer` | the node asked | a member's stream followed, or not | `node`, `peer`, `state` (`following`, `unreachable`), `error` |
  | `reset` | any node | the stream missed messages: more than `FEED_CAPACITY` (1024) behind | `node`, `reason` (`lagged`), `missed` |

- **D-M13f.4 Publishing** (`Feed`, `Live`, in `data-avalanche-cluster`; `publish`, in
  `data-avalanche-api`).
  - `Feed` is a broadcast channel of `Arc<Live>`, its capacity `FEED_CAPACITY`. `publish` sends
    only while a stream is open (`followed`); a stream that falls behind gets `Lagged` and is sent
    `reset`.
  - The recorder publishes an event after its write succeeds (an event dropped after its retries is
    never sent); the sampler publishes each sample before it adds it.
  - The publisher (`publish(Node)`, a task on every node) ticks every `STATE_EVERY`; every fifth
    tick sends the figures. It reads the controllers' statuses, the members, the process and
    gossip figures, and the config the coordinator holds: never storage. When the count of open
    streams grows, it forgets what it sent, so the new stream learns every state within a second.
- **D-M13f.5 Following** (`Peers::follow`, `Followed`, `Frames`, in `data-avalanche-api`).
  - The node asked follows each other member whose gossip carries an API address: a `GET` of the
    same path and query, with `TARGET` (that member), `FORWARDED_BY` (this node) and the caller's
    `Authorization`. A stream that carries `FORWARDED_BY` sends its node's own messages only and
    follows none.
  - Answered within `DETAIL_LIMIT` (3 s); a refusal (`401`, a `404` from a node on another
    generation) is `unreachable` with its code and error. No total timeout: a followed stream silent
    for `FOLLOW_SILENCE` (45 s, three keep-alives) is dropped.
  - Messages are read a chunk at a time, each at most `MAX_FRAME` (1 MiB); a line ends at CR, LF or
    both. They are passed on as the member sent them; its `hello` becomes `peer` `following`.
  - A stream that can't be opened, ends or fails is `unreachable` and followed again after
    `FOLLOW_BACKOFF` (1, 2, 5, then every 10 s), while the member is one. A member that leaves is
    no longer followed; a member at a new address is followed there.
  - Each followed stream is a task of the stream's, ended with it.
- **D-M13f.6 Limits.**
  - `MAX_STREAMS` (64) a node, the streams it serves its peers included: `503` with
    `Retry-After: 5` past that. A stream's place is given back when its client goes.
  - A stream's response holds 64 messages before the node waits on its client; while it waits, the
    feed moves on and the stream lags (`reset`). A followed member's messages wait on the client
    too, so a slow page lags at the member, which says so.
  - A comment every `KEEP_ALIVE` (15 s), under the server's idle timeout (60 s) and a proxy's.
- **D-M13f.7 Stopping.** The binary aborts the publisher and closes the feed (`Feed::close`)
  before it stops its server, which ends every stream; the history's last samples, published
  after, reach none.
- **D-M13f.8 Rollback.** Nothing is stored: no release note.
- **D-M13f.9 The E2E scenario** reads the stream over HTTP/1.0 (its body unchunked) from a node
  that doesn't hold the test's pipeline, about 40 s on each source.

### Spec patch S-M13f.1 (needs approval)

`doc/specs/System-Overview.md` §Management API endpoints, a row after **Chart history**:

> | Live stream | `GET /v1/stream` | viewer | the cluster's changes as server-sent events, from any node, which follows every other member's stream with the caller's own credentials and merges them: `hello` first (no replay: read the current state, then apply what follows), then each pipeline's state as it changes (`pipeline`), its live figures every 5 s (`figures`), each node's own figures every 5 s (`node`), each chart sample (`samples`), each event written (`event`), the members (`members`) and each member followed or unreachable (`peer`); `reset` when a stream fell more than 1024 messages behind. Filters `pipeline` (several) and `kind`. At most 64 streams a node (`503` with `Retry-After`); a comment every 15 s |

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-cluster/src/live.rs` (new), `lib.rs`, `Cargo.toml` | `Feed`, `Live`, `Opened`; `FEED_CAPACITY`, `MAX_STREAMS` (Tasks 1–3) |
| `crates/data-avalanche-cluster/src/{events,history}.rs` | The recorder publishes each event once written; the sampler each sample once taken (Task 1) |
| `crates/data-avalanche-state/src/events.rs`, `crates/data-avalanche-api/src/events.rs` | `EventDoc::to_json`, shared by `GET /v1/events` and the `event` message (Task 1) |
| `crates/data-avalanche-api/src/publish.rs` (new), `nodes.rs`, `live.rs` | The publisher; the process and gossip JSON a node's detail and the `node` message share (Task 2) |
| `crates/data-avalanche-api/src/stream.rs` (new), `lib.rs`, `openapi.rs`, `openapi.json` | `GET /v1/stream`: its filters, limits and merge (Tasks 3, 4) |
| `crates/data-avalanche-api/src/follow.rs` (new), `peers.rs` | `Peers::follow`, a followed stream read message by message (Task 4) |
| `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche-cluster/src/{coordinator,test_support}.rs` | `Node::feed`; a stream read as a page reads it; the callers of `Recorder::start` (Tasks 1–3) |
| `crates/data-avalanche/src/run.rs` | One feed for the recorder, the sampler and the routes; the publisher; the feed closed before the server stops (Tasks 1–3) |
| `xtask/src/api.rs`, `docs/src/content/docs/reference/api.md`, `docs/public/openapi.json` | The API page's `stream` section (Task 3) |
| `docs/src/content/docs/deployment/observability.md`, `doc/research/Admin_Console.md` | The page and §8.1 (Task 5) |
| `crates/data-avalanche/tests/e2e/{console_stream,harness,main}.rs` | The scenario on three nodes; `Proc::stream` (Task 6) |

The plan's commit also carries S-M13f.1 (once approved) and `doc/v1.1/README.md`: M13e merged
(#31), and this plan's row.

---

### Task 1: The feed: a node's live messages (`data-avalanche-cluster`, `data-avalanche-state`)

**Files:**
- Regenerate: `Cargo.lock`
- Modify: `crates/data-avalanche-api/src/events.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche-cluster/Cargo.toml`
- Modify: `crates/data-avalanche-cluster/src/coordinator.rs`
- Modify: `crates/data-avalanche-cluster/src/events.rs`
- Modify: `crates/data-avalanche-cluster/src/history.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Create: `crates/data-avalanche-cluster/src/live.rs`
- Modify: `crates/data-avalanche-cluster/src/test_support.rs`
- Modify: `crates/data-avalanche-state/src/events.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `EventDoc` (M13a); `Recorder` (M13a); `Sampler`, `sample_history` (M13e).
- Produces (cluster crate root):
  - `Live { kind: &'static str, pipeline: Option<String>, data: Value }`, `Live::event(key, &EventDoc)`;
  - `Feed::new()`, `publish(&self, Live)` (only while followed), `followed(&self) -> bool`,
    `subscribe(&self) -> broadcast::Receiver<Arc<Live>>`, `close(&self)`, `closed(&self) -> watch::Receiver<bool>`;
  - `FEED_CAPACITY` (1024);
  - `Recorder::start(events, node, metrics, feed: &Feed)`; `sample_history(sampler, statuses, process, metrics, feed: Feed, stop)`;
  - `Sampler::sample(...) -> Vec<Live>` (each scope's `samples` message).
- State crate: `EventDoc::to_json(&self, key) -> Value` (the API's listing uses it).
- The binary creates one `Feed` and passes it to the recorder and the sampler.

- [ ] **Step 1: Write the failing tests**

The tests: `a_message_reaches_every_stream_open_and_waits_for_none`, `a_stream_far_behind_learns_it_missed_some`, `a_closed_feed_says_so_to_every_stream`, `an_event_is_sent_as_the_events_list_it` (live); `a_written_event_reaches_the_live_streams_with_its_key` (events); `each_sample_is_sent_as_taken` (history); `an_event_s_json_names_its_key` (state). The callers of `Recorder::start` in tests pass a `Feed`. The new module holds only its tests for now.

`crates/data-avalanche-api/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/events.rs b/crates/data-avalanche-api/src/events.rs
index 6cb5db1..03aa506 100644
--- a/crates/data-avalanche-api/src/events.rs
+++ b/crates/data-avalanche-api/src/events.rs
@@ -157,6 +157,9 @@ mod tests {
     use chrono::TimeZone;
     use data_avalanche_state::{Actor, EventSeverity};
 
+    use data_avalanche_state::EventDoc;
+    use serde_json::Value;
+
     use super::*;
     use crate::test_support::{CONFIG, api, json_of, lone, open, serve};
 
````

`crates/data-avalanche-cluster/src/coordinator.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 2b44d6e..86cc747 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -1102,7 +1102,13 @@ mod tests {
             advertiser: gossip.advertiser(),
             metrics: metrics.clone(),
             globals: Arc::new(move |g| seen.lock().unwrap().push(g.clone())),
-            recorder: Recorder::start(Arc::clone(&events), "n1", Metrics::new()).0,
+            recorder: Recorder::start(
+                Arc::clone(&events),
+                "n1",
+                Metrics::new(),
+                &crate::Feed::new(),
+            )
+            .0,
         };
         let (members_tx, members_rx) =
             watch::channel(members("n1", &[("n1", Phase::Active, false)]));
````

`crates/data-avalanche-cluster/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index c0d54ef..94abbe5 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -529,7 +529,8 @@ mod tests {
     async fn a_flush_waits_until_the_queue_is_written() {
         // The first event's write fails twice: 200 ms, then 1 s, before it lands.
         let events = store(2);
-        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", Metrics::new());
+        let (r, _task) =
+            Recorder::start(Arc::clone(&events), "node-a", Metrics::new(), &Feed::new());
         assert!(r.flush(Duration::ZERO).await, "nothing queued");
         for text in ["one", "two", "three"] {
             r.record(r.event("member.joined", EventSeverity::Info, text));
@@ -545,7 +546,8 @@ mod tests {
     #[tokio::test(start_paused = true)]
     async fn a_flush_gives_up_at_its_bound() {
         let events = failing(0, true);
-        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", Metrics::new());
+        let (r, _task) =
+            Recorder::start(Arc::clone(&events), "node-a", Metrics::new(), &Feed::new());
         r.record(r.event("member.joined", EventSeverity::Info, "never"));
         let started = tokio::time::Instant::now();
         assert!(!r.flush(Duration::from_secs(5)).await);
@@ -569,7 +571,8 @@ mod tests {
     async fn a_failing_store_is_retried_then_the_event_lands() {
         let events = store(2);
         let metrics = Metrics::new();
-        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
+        let (r, _task) =
+            Recorder::start(Arc::clone(&events), "node-a", metrics.clone(), &Feed::new());
         r.record(r.event("member.joined", EventSeverity::Info, "node-c joined"));
         assert_eq!(written(&events, 1).await, ["node-c joined"]);
         assert!(
@@ -583,7 +586,8 @@ mod tests {
     async fn a_store_that_keeps_failing_drops_the_event_and_counts_it() {
         let events = store(WRITE_RETRIES.len() + 1);
         let metrics = Metrics::new();
-        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
+        let (r, _task) =
+            Recorder::start(Arc::clone(&events), "node-a", metrics.clone(), &Feed::new());
         r.record(r.event("member.joined", EventSeverity::Info, "lost"));
         r.record(r.event("member.joined", EventSeverity::Info, "kept"));
         assert_eq!(written(&events, 1).await, ["kept"]);
@@ -594,6 +598,30 @@ mod tests {
         );
     }
 
+    /// A stream sees each event once it is written, with the key it was written at; one the
+    /// store keeps refusing never reaches it.
+    #[tokio::test(start_paused = true)]
+    async fn a_written_event_reaches_the_live_streams_with_its_key() {
+        let events = store(WRITE_RETRIES.len() + 1);
+        let feed = Feed::new();
+        let mut stream = feed.subscribe();
+        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", Metrics::new(), &feed);
+        r.record(r.event("member.joined", EventSeverity::Info, "lost"));
+        r.record(r.of_pipeline(
+            "orders",
+            "action.pipeline.pause",
+            EventSeverity::Info,
+            "kept",
+        ));
+        let live = stream.recv().await.unwrap();
+        assert_eq!(live.kind, "event");
+        assert_eq!(live.pipeline.as_deref(), Some("orders"));
+        assert_eq!(live.data["text"], "kept");
+        let (key, _) = &events.list(&EventQuery::default()).await.unwrap().events[0];
+        assert_eq!(live.data["key"], key.as_str());
+        assert!(stream.try_recv().is_err(), "the dropped event isn't sent");
+    }
+
     fn status(actual: ActualState, error: Option<&str>, degraded: Option<&str>) -> StatusDoc {
         StatusDoc {
             holder_node_id: "node-a".into(),
@@ -826,7 +854,8 @@ mod tests {
     async fn a_new_node_records_nothing_until_its_view_settles() {
         use NodePhase::{Active, Joining};
         let events = store(0);
-        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", Metrics::new());
+        let (r, _task) =
+            Recorder::start(Arc::clone(&events), "node-a", Metrics::new(), &Feed::new());
         let (view, rx) = watch::channel(members("a", &[("a", 1, false, Joining)]));
         let _recording = tokio::spawn(record_members(r.clone(), rx));
         tokio::task::yield_now().await;
````

`crates/data-avalanche-cluster/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/history.rs b/crates/data-avalanche-cluster/src/history.rs
index cd1b762..4b3b35f 100644
--- a/crates/data-avalanche-cluster/src/history.rs
+++ b/crates/data-avalanche-cluster/src/history.rs
@@ -522,6 +522,33 @@ mod tests {
         assert_eq!(n[0].series["lease_renew_seconds"].v, [0.041]);
     }
 
+    /// Each sample is a live message too: a pipeline's series and tables, and the node's.
+    #[test]
+    fn each_sample_is_sent_as_taken() {
+        let (mut s, _, _) = sampler();
+        let now = at(9, 0, 15);
+        let sent = s.sample(
+            now,
+            &statuses(&[("orders", running(4, now))]),
+            &figures(1 << 30, 0.5),
+            None,
+            true,
+        );
+        assert_eq!(sent.len(), 2, "{sent:?}");
+        let p = &sent[0];
+        assert_eq!((p.kind, p.pipeline.as_deref()), ("samples", Some("orders")));
+        assert_eq!(p.data["node"], "node-a");
+        assert_eq!(p.data["scope"], "pipeline");
+        assert_eq!(p.data["at"], now.timestamp());
+        assert_eq!(p.data["series"]["lag_seconds"], 30.0);
+        assert_eq!(p.data["tables"]["public.orders"]["files_rewritten"], 4.0);
+        let n = &sent[1];
+        assert_eq!((n.kind, n.pipeline.as_deref()), ("samples", None));
+        assert_eq!(n.data["scope"], "node");
+        assert_eq!(n.data["series"]["memory_bytes"], (1u64 << 30) as f64);
+        assert!(n.data["series"].get("lease_renew_seconds").is_none());
+    }
+
     #[tokio::test]
     async fn a_chunk_is_written_each_minute_while_it_changes() {
         let (mut s, charts, _) = sampler();
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 4e6109b..e9b5a79 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -16,6 +16,7 @@ pub mod gossip;
 pub mod history;
 mod jobs;
 pub mod lease;
+pub mod live;
 pub mod node;
 pub mod placement;
 pub mod reconciler;
````

`crates/data-avalanche-cluster/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/live.rs b/crates/data-avalanche-cluster/src/live.rs
new file mode 100644
index 0000000..1f2e9db
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/live.rs
@@ -0,0 +1,79 @@
+//! The node's live messages (Admin_Console §8, M13f): what `GET /v1/stream` sends. The node
+//! publishes each as it happens (an event once written, a sample once taken, a pipeline's state
+//! and figures, its own figures) to every stream open on it, and builds none while no stream is
+//! open. A stream that falls more than [`FEED_CAPACITY`] messages behind is told to read again.
+
+#[cfg(test)]
+mod tests {
+    use chrono::Utc;
+    use data_avalanche_state::EventSeverity;
+
+    use super::*;
+
+    fn live(n: u64) -> Live {
+        Live {
+            kind: "node",
+            pipeline: None,
+            data: Value::from(n),
+        }
+    }
+
+    #[tokio::test]
+    async fn a_message_reaches_every_stream_open_and_waits_for_none() {
+        let feed = Feed::new();
+        assert!(!feed.followed());
+        feed.publish(live(1));
+        let (mut a, mut b) = (feed.subscribe(), feed.subscribe());
+        assert!(feed.followed());
+        feed.publish(live(2));
+        assert_eq!(
+            *a.recv().await.unwrap(),
+            live(2),
+            "not the one sent before it opened"
+        );
+        assert_eq!(*b.recv().await.unwrap(), live(2));
+        drop((a, b));
+        assert!(!feed.followed());
+        feed.publish(live(3));
+    }
+
+    #[tokio::test]
+    async fn a_stream_far_behind_learns_it_missed_some() {
+        let feed = Feed::new();
+        let mut slow = feed.subscribe();
+        for n in 0..=FEED_CAPACITY as u64 {
+            feed.publish(live(n));
+        }
+        assert!(matches!(
+            slow.recv().await,
+            Err(broadcast::error::RecvError::Lagged(1))
+        ));
+        assert_eq!(*slow.recv().await.unwrap(), live(1));
+    }
+
+    #[test]
+    fn a_closed_feed_says_so_to_every_stream() {
+        let feed = Feed::new();
+        let open = feed.closed();
+        assert!(!*open.borrow());
+        feed.clone().close();
+        assert!(*open.borrow());
+        assert!(*feed.closed().borrow());
+    }
+
+    #[test]
+    fn an_event_is_sent_as_the_events_list_it() {
+        let mut e = EventDoc::new(
+            Utc::now(),
+            "node-a",
+            "action.pipeline.pause",
+            EventSeverity::Info,
+            "paused",
+        );
+        e.pipeline = Some("orders".into());
+        let l = Live::event("events/k.json", &e);
+        assert_eq!(l.kind, "event");
+        assert_eq!(l.pipeline.as_deref(), Some("orders"));
+        assert_eq!(l.data, e.to_json("events/k.json"));
+    }
+}
````

`crates/data-avalanche-state/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/events.rs b/crates/data-avalanche-state/src/events.rs
index 91c0d83..61cdf84 100644
--- a/crates/data-avalanche-state/src/events.rs
+++ b/crates/data-avalanche-state/src/events.rs
@@ -729,4 +729,18 @@ mod tests {
             "none one at a time"
         );
     }
+
+    #[test]
+    fn an_event_s_json_names_its_key() {
+        let mut e = event(at(18, 9, 30), "action.pipeline.pause", "node-a");
+        e.pipeline = Some("orders".into());
+        let v = e.to_json("events/2026-11-18/093000.000000Z-000001-node-a.json");
+        assert_eq!(
+            v["key"],
+            "events/2026-11-18/093000.000000Z-000001-node-a.json"
+        );
+        assert_eq!(v["kind"], "action.pipeline.pause");
+        assert_eq!(v["node"], "node-a");
+        assert_eq!(v["pipeline"], "orders");
+    }
 }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-cluster --lib -- live:: events:: history:: && cargo test -p data-avalanche-state --lib events::`
Expected: the cluster's tests don't compile: they find no `Feed`, `Live` or `FEED_CAPACITY` (nor the imports the code above them brings), and `Recorder::start` takes 3 arguments, not 4. Run on its own, the state crate's test finds no `EventDoc::to_json`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/events.rs b/crates/data-avalanche-api/src/events.rs
index 03aa506..eadffcd 100644
--- a/crates/data-avalanche-api/src/events.rs
+++ b/crates/data-avalanche-api/src/events.rs
@@ -5,8 +5,8 @@ use axum::extract::{RawQuery, State};
 use axum::http::StatusCode;
 use axum::response::Response;
 use chrono::{DateTime, Utc};
-use data_avalanche_state::{EventDoc, EventQuery, MAX_EVENTS, StateError};
-use serde_json::{Value, json};
+use data_avalanche_state::{EventQuery, MAX_EVENTS, StateError};
+use serde_json::json;
 
 use crate::{Api, Failure, body, starting};
 
@@ -114,12 +114,6 @@ fn query(params: &[(String, String)]) -> Result<EventQuery, Failure> {
     Ok(q)
 }
 
-pub(crate) fn event_json(key: &str, e: &EventDoc) -> Value {
-    let mut v = serde_json::to_value(e).expect("an event serializes");
-    v["key"] = json!(key);
-    v
-}
-
 pub(crate) async fn list(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
     let Some(node) = api.node.get() else {
         return starting();
@@ -136,7 +130,7 @@ pub(crate) async fn list(State(api): State<Api>, RawQuery(raw): RawQuery) -> Res
         Ok(page) => body(
             StatusCode::OK,
             &json!({
-                "events": page.events.iter().map(|(k, e)| event_json(k, e)).collect::<Vec<_>>(),
+                "events": page.events.iter().map(|(k, e)| e.to_json(k)).collect::<Vec<_>>(),
                 "next": page.next,
             }),
         ),
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 2d177f0..e9ce619 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -238,7 +238,12 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
     );
     let metrics = Metrics::new();
     let events = Arc::new(Events::new(docs.clone()));
-    let (recorder, _) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
+    let (recorder, _) = Recorder::start(
+        Arc::clone(&events),
+        "node-a",
+        metrics.clone(),
+        &data_avalanche_cluster::Feed::new(),
+    );
     let c = &config.globals.cluster;
     let removals = Removals::new(docs.clone());
     let deps = ControllerDeps {
````

`crates/data-avalanche-cluster/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
index ba15f12..fc4f290 100644
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -26,6 +26,7 @@ getrandom.workspace = true
 if-addrs.workspace = true
 rand.workspace = true
 serde.workspace = true
+serde_json.workspace = true
 thiserror.workspace = true
 tokio = { workspace = true, features = ["fs", "net", "sync"] }
 tracing.workspace = true
````

`crates/data-avalanche-cluster/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index 94abbe5..ff8fd0f 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -2,7 +2,8 @@
 //! task per node writes them to the cluster's timeline in `globals.state`, retrying a failing
 //! store a few times; an event it can't write is counted (`data_avalanche_events_dropped_total`)
 //! and logged, never allowed to slow or fail what it records. A clean stop waits a bounded time
-//! for the queue to be written ([`Recorder::flush`]). Another task expires the days past
+//! for the queue to be written ([`Recorder::flush`]). Each event written goes to the node's live
+//! streams too ([`Feed`], M13f). Another task expires the days past
 //! `globals.console.events.retention`.
 
 use std::collections::BTreeMap;
@@ -18,6 +19,7 @@ use tokio::sync::{mpsc, watch};
 use tokio::task::JoinHandle;
 
 use crate::gossip::{Members, Phase as NodePhase};
+use crate::live::{Feed, Live};
 
 /// Events waiting to be written; past this, new ones are dropped and counted.
 pub const EVENT_QUEUE: usize = 1024;
@@ -44,11 +46,22 @@ pub struct Recorder {
 }
 
 impl Recorder {
-    /// The recorder of `node`, and its writer task.
-    pub fn start(events: Arc<Events>, node: &str, metrics: Metrics) -> (Self, JoinHandle<()>) {
+    /// The recorder of `node`, and its writer task, which sends each event it writes to `feed`.
+    pub fn start(
+        events: Arc<Events>,
+        node: &str,
+        metrics: Metrics,
+        feed: &Feed,
+    ) -> (Self, JoinHandle<()>) {
         let (tx, rx) = mpsc::channel(EVENT_QUEUE);
         let pending = Arc::new(watch::Sender::new(0));
-        let task = tokio::spawn(write(events, rx, Arc::clone(&pending), metrics.clone()));
+        let task = tokio::spawn(write(
+            events,
+            rx,
+            Arc::clone(&pending),
+            metrics.clone(),
+            feed.clone(),
+        ));
         (
             Self {
                 node: node.into(),
@@ -120,19 +133,22 @@ impl Recorder {
     }
 }
 
-/// Writes each queued event, then marks it done in `pending` (written or dropped).
+/// Writes each queued event, sends it to `feed` once written, then marks it done in `pending`
+/// (written or dropped).
 async fn write(
     events: Arc<Events>,
     mut rx: mpsc::Receiver<EventDoc>,
     pending: Arc<watch::Sender<usize>>,
     metrics: Metrics,
+    feed: Feed,
 ) {
     while let Some(event) = rx.recv().await {
         let mut waits = WRITE_RETRIES.iter();
         loop {
             match events.record(&event).await {
-                Ok(_) => {
+                Ok(key) => {
                     metrics.event_written();
+                    feed.publish(Live::event(&key, &event));
                     break;
                 }
                 Err(e) => match waits.next() {
````

`crates/data-avalanche-cluster/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/history.rs b/crates/data-avalanche-cluster/src/history.rs
index 4b3b35f..1c874c2 100644
--- a/crates/data-avalanche-cluster/src/history.rs
+++ b/crates/data-avalanche-cluster/src/history.rs
@@ -4,7 +4,8 @@
 //! current hour's chunks. A chunk is written when it gets its first sample or a new series, then
 //! every [`WRITE_EVERY`] while it changes, and once more when its hour or its lease term ends. A
 //! write that fails is tried again at the next sample; the samples wait in memory, never holding
-//! up a pipeline. Another task expires the days past `globals.console.history.retention`.
+//! up a pipeline. Each sample also goes to the node's live streams as it is taken (M13f). Another
+//! task expires the days past `globals.console.history.retention`.
 
 use std::collections::{BTreeMap, BTreeSet};
 use std::sync::Arc;
@@ -18,10 +19,12 @@ use data_avalanche_state::{
 };
 use data_avalanche_telemetry::{Metrics, ProcessFigures};
 use futures_util::{StreamExt, stream};
+use serde_json::{Map, Value, json};
 use tokio::sync::{oneshot, watch};
 use tokio::time::MissedTickBehavior;
 
 use crate::controller::{Actual, ControllerStatus};
+use crate::live::{Feed, Live};
 
 /// Between two writes of a chunk that keeps changing.
 pub const WRITE_EVERY: Duration = Duration::from_secs(60);
@@ -88,7 +91,7 @@ impl Sampler {
     /// Samples at `now`: each pipeline in `statuses` this node runs (its tables too when
     /// `tables`), and the node, from `process` and the slowest lease renewal since the last
     /// sample. A chunk of a scope this sample doesn't reach (a lease gone) or of another hour or
-    /// term ends.
+    /// term ends. Returns each scope's sample as a live message (`samples`, M13f): a chart's tail.
     pub fn sample(
         &mut self,
         now: DateTime<Utc>,
@@ -96,7 +99,8 @@ impl Sampler {
         process: &ProcessFigures,
         slowest_renewal: Option<Duration>,
         tables: bool,
-    ) {
+    ) -> Vec<Live> {
+        let mut sent = Vec::new();
         let hour = hour_of(now);
         let t = u16::try_from((now - hour).num_seconds()).unwrap_or(u16::MAX);
         let mut current = Vec::new();
@@ -115,6 +119,7 @@ impl Sampler {
                 .map(|o| o.chunk.tables.keys().cloned().collect())
                 .unwrap_or_default();
             let values = self.pipeline(id, &writer, s, now, tables, &known);
+            sent.extend(self.sampled(&scope, now, &values));
             current.push(self.add(scope, hour, &writer, t, values));
         }
         let r = &process.reading;
@@ -134,12 +139,53 @@ impl Sampler {
         );
         let writer = format!("i{}", self.incarnation);
         let scope = ChartScope::Node(self.node.clone());
+        sent.extend(self.sampled(&scope, now, &values));
         current.push(self.add(scope, hour, &writer, t, values));
         for (key, open) in &mut self.open {
             open.ended = !current.contains(key);
         }
         self.copied
             .retain(|id, _| statuses.get(id).is_some_and(|s| s.worker.is_some()));
+        sent
+    }
+
+    /// A scope's sample at `now` as a live message: its series, and its tables' (none when it
+    /// has no value).
+    fn sampled(
+        &self,
+        scope: &ChartScope,
+        now: DateTime<Utc>,
+        (own, tables): &Values,
+    ) -> Option<Live> {
+        if own.is_empty() && tables.iter().all(|(_, v)| v.is_empty()) {
+            return None;
+        }
+        let values = |list: &[(&'static str, f64)]| -> Map<String, Value> {
+            list.iter()
+                .map(|(n, v)| ((*n).to_owned(), json!(v)))
+                .collect()
+        };
+        let tables: Map<String, Value> = tables
+            .iter()
+            .filter(|(_, v)| !v.is_empty())
+            .map(|(t, v)| (t.clone(), Value::Object(values(v))))
+            .collect();
+        let (kind, pipeline) = match scope {
+            ChartScope::Pipeline(id) => ("pipeline", Some(id.clone())),
+            ChartScope::Node(_) => ("node", None),
+        };
+        Some(Live {
+            kind: "samples",
+            pipeline: pipeline.clone(),
+            data: json!({
+                "node": self.node,
+                "scope": kind,
+                "pipeline": pipeline,
+                "at": now.timestamp(),
+                "series": values(own),
+                "tables": tables,
+            }),
+        })
     }
 
     /// A pipeline's values at `now`, from its controller's status.
@@ -321,7 +367,8 @@ impl Sampler {
 }
 
 /// Samples every [`SAMPLE_EVERY`] (the tables every [`TABLE_SAMPLE_EVERY`]) the controllers
-/// `statuses` lists and the process's `figures`, and writes what is due, until `stop` fires (or
+/// `statuses` lists and the process's `figures`, sends each sample to `feed`, and writes what is
+/// due, until `stop` fires (or
 /// its sender is dropped): then it takes a last sample and writes every chunk it holds, ended
 /// ([`Sampler::finish`]), and returns.
 pub async fn sample_history(
@@ -329,6 +376,7 @@ pub async fn sample_history(
     statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
     process: watch::Receiver<ProcessFigures>,
     metrics: Metrics,
+    feed: Feed,
     mut stop: oneshot::Receiver<()>,
 ) {
     let per_table = (TABLE_SAMPLE_EVERY.as_secs() / SAMPLE_EVERY.as_secs()).max(1);
@@ -343,13 +391,15 @@ pub async fn sample_history(
             .map(|(id, s)| (id.clone(), s.borrow().clone()))
             .collect();
         let figures = *process.borrow();
-        sampler.sample(
+        for live in sampler.sample(
             now,
             &current,
             &figures,
             metrics.take_slowest_renewal(),
             tables,
-        );
+        ) {
+            feed.publish(live);
+        }
         now
     };
     loop {
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index e9b5a79..cbdac92 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -50,6 +50,7 @@ pub use history::{
     HISTORY_PRUNE_EVERY, Sampler, UNWRITTEN_KEPT, WRITE_EVERY, expire_history, sample_history,
 };
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
+pub use live::{FEED_CAPACITY, Feed, Live};
 pub use node::{JOIN_WAIT, join, leave};
 pub use placement::Placement;
 pub use reconciler::{
````

`crates/data-avalanche-cluster/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/live.rs b/crates/data-avalanche-cluster/src/live.rs
index 1f2e9db..c88e525 100644
--- a/crates/data-avalanche-cluster/src/live.rs
+++ b/crates/data-avalanche-cluster/src/live.rs
@@ -3,6 +3,85 @@
 //! and figures, its own figures) to every stream open on it, and builds none while no stream is
 //! open. A stream that falls more than [`FEED_CAPACITY`] messages behind is told to read again.
 
+use std::sync::Arc;
+
+use data_avalanche_state::EventDoc;
+use serde_json::Value;
+use tokio::sync::{broadcast, watch};
+
+/// The messages a stream may fall behind by before it misses some (and is told so).
+pub const FEED_CAPACITY: usize = 1024;
+
+/// One live message: its kind (the stream's event name), the pipeline it is about, and its data,
+/// which names the node that sent it.
+#[derive(Debug, Clone, PartialEq)]
+pub struct Live {
+    pub kind: &'static str,
+    pub pipeline: Option<String>,
+    pub data: Value,
+}
+
+impl Live {
+    /// The event `e` this node wrote at `key`, as `GET /v1/events` lists it.
+    pub fn event(key: &str, e: &EventDoc) -> Self {
+        Self {
+            kind: "event",
+            pipeline: e.pipeline.clone(),
+            data: e.to_json(key),
+        }
+    }
+}
+
+/// The node's live messages, sent to every stream open on it; cheap to clone.
+#[derive(Debug, Clone)]
+pub struct Feed {
+    tx: broadcast::Sender<Arc<Live>>,
+    closed: Arc<watch::Sender<bool>>,
+}
+
+impl Default for Feed {
+    fn default() -> Self {
+        Self::new()
+    }
+}
+
+impl Feed {
+    pub fn new() -> Self {
+        Self {
+            tx: broadcast::Sender::new(FEED_CAPACITY),
+            closed: Arc::new(watch::Sender::new(false)),
+        }
+    }
+
+    /// Sends `live` to every stream open now: to none, and dropped, when none is.
+    pub fn publish(&self, live: Live) {
+        if self.followed() {
+            let _ = self.tx.send(Arc::new(live));
+        }
+    }
+
+    /// Whether a stream is open on this node: what builds a message only to publish it asks
+    /// first.
+    pub fn followed(&self) -> bool {
+        self.tx.receiver_count() > 0
+    }
+
+    /// A new stream's messages, from now on.
+    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Live>> {
+        self.tx.subscribe()
+    }
+
+    /// Ends every stream: the node stops, and an open stream must not hold the server's stop.
+    pub fn close(&self) {
+        self.closed.send_replace(true);
+    }
+
+    /// Whether the feed is closed, and its change.
+    pub fn closed(&self) -> watch::Receiver<bool> {
+        self.closed.subscribe()
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use chrono::Utc;
````

`crates/data-avalanche-cluster/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index a262574..ad4d3c5 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -525,7 +525,13 @@ pub(crate) fn recorded_node(
         ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap(),
     ))));
     let mut deps = deps(name, store, factory);
-    deps.recorder = crate::Recorder::start(Arc::clone(&events), name, Metrics::new()).0;
+    deps.recorder = crate::Recorder::start(
+        Arc::clone(&events),
+        name,
+        Metrics::new(),
+        &crate::Feed::new(),
+    )
+    .0;
     (node_of(deps, docs), events)
 }
 
````

`crates/data-avalanche-state/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/events.rs b/crates/data-avalanche-state/src/events.rs
index 61cdf84..17aadfb 100644
--- a/crates/data-avalanche-state/src/events.rs
+++ b/crates/data-avalanche-state/src/events.rs
@@ -105,6 +105,14 @@ impl EventDoc {
             detail: BTreeMap::new(),
         }
     }
+
+    /// The event as `GET /v1/events` lists it and a live stream sends it: its fields, with its
+    /// `key`.
+    pub fn to_json(&self, key: &str) -> serde_json::Value {
+        let mut v = serde_json::to_value(self).expect("an event serializes");
+        v["key"] = serde_json::Value::from(key);
+        v
+    }
 }
 
 /// Which events [`Events::list`] selects, and how many: every filter set must match, and an
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index a7bacb1..ff93442 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -12,7 +12,7 @@ use data_avalanche_api::{
 };
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
-    DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
+    DocsFn, Feed, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
     ReconcilerSettings, Recorder, Sampler, Seeds, Udp, advertise, api_url, discover, expire_events,
     expire_history, file_hash, interfaces, join as join_gossip, leave, node_identity,
     record_members, removed_pipelines, sample_history, watch_file,
@@ -181,8 +181,10 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
 
     // The cluster's events (Admin_Console §6): recorded from here, expired on every node.
     let events = Arc::new(Events::new(joined.globals.clone()));
+    // The node's live messages (M13f): what its open streams send.
+    let feed = Feed::new();
     let (recorder, _writer) =
-        Recorder::start(Arc::clone(&events), &holder.node_id, metrics.clone());
+        Recorder::start(Arc::clone(&events), &holder.node_id, metrics.clone(), &feed);
     let (retention, kept) = watch::channel(config.globals.console.events.retention);
     let _expiry = tokio::spawn(expire_events(Arc::clone(&events), kept));
     // The chart history (Admin_Console §7): sampled once the controllers run, expired on every
@@ -282,6 +284,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         reconciler.watch_statuses(),
         process.clone(),
         metrics.clone(),
+        feed.clone(),
         history_stop,
     ));
     // Each newer config generation, this node's reloads included (Clustering §9).
````

Then regenerate `Cargo.lock`: `cargo build` updates `Cargo.lock` (the cluster crate's `serde_json`).

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-cluster --lib -- live:: events:: history:: && cargo test -p data-avalanche-state --lib events::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/data-avalanche-api/src/events.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche-cluster/Cargo.toml crates/data-avalanche-cluster/src/coordinator.rs crates/data-avalanche-cluster/src/events.rs crates/data-avalanche-cluster/src/history.rs crates/data-avalanche-cluster/src/lib.rs crates/data-avalanche-cluster/src/live.rs crates/data-avalanche-cluster/src/test_support.rs crates/data-avalanche-state/src/events.rs crates/data-avalanche/src/run.rs
git commit -m "feat(cluster): the node's live feed — each event once written and each sample once taken, while a stream is open"
```

---

### Task 2: The publisher: states on change, figures every 5 s (`data-avalanche-api`, `data-avalanche`)

**Files:**
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/live.rs`
- Modify: `crates/data-avalanche-api/src/nodes.rs`
- Create: `crates/data-avalanche-api/src/publish.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche-cluster/src/live.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: Task 1's `Feed`, `Live`; the API's `live::answer`, `live::actual_of` (now `pub(crate)`); `ControllerStatus`,
  `Actual` (M6b); the node's members, process and gossip figures (M13b).
- Produces:
  - `Feed::followers(&self) -> usize` (cluster);
  - `Node::feed: Feed`; `publish(node: Node)` (a task until aborted), `STATE_EVERY` (1 s), `FIGURES_EVERY` (5 s) at the
    API crate root; `Publisher::due(&mut self, &Node, now, followers, figures: bool) -> Vec<Live>` (crate-private);
  - `nodes::process_json`, `nodes::gossip_json`, `nodes::phase` (`pub(crate)`), which a node's detail and the `node`
    message share.
- The binary sets `Node::feed` and spawns `publish` once the node runs its generation.

- [ ] **Step 1: Write the failing tests**

The tests: `a_pipeline_s_state_is_sent_when_it_changes`, `a_stream_that_opens_gets_every_state_again`, `figures_are_the_live_answer_and_the_node_s_own`, `the_node_publishes_only_while_followed` (publish); the feed's test counts its followers. The new module holds only its tests for now.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 6dbb935..3b2f664 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -22,6 +22,7 @@ mod openapi;
 mod peers;
 mod pipelines;
 mod prometheus;
+mod publish;
 mod server;
 mod snapshots;
 mod tables;
````

`crates/data-avalanche-api/src/publish.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/publish.rs b/crates/data-avalanche-api/src/publish.rs
new file mode 100644
index 0000000..337a96d
--- /dev/null
+++ b/crates/data-avalanche-api/src/publish.rs
@@ -0,0 +1,155 @@
+//! What a node sends its live streams of its own accord (M13f, D-M13f.3), from its memory only,
+//! never from storage, and only while a stream is open on it:
+//! - `pipeline`: a pipeline it runs, when its state, error or term changes (looked at every
+//!   [`STATE_EVERY`]), and once more as `standby` when it stops running it;
+//! - `figures`: each pipeline it runs, every [`FIGURES_EVERY`], as `GET /v1/pipelines/{id}/live`
+//!   answers (the halts' times left out: they are in storage);
+//! - `node`: its own process and gossip figures, every [`FIGURES_EVERY`].
+//!
+//! When a stream opens, every state is sent again at the next look: a stream starts without
+//! replay, so a page learns each pipeline's state within a second of opening it.
+
+#[cfg(test)]
+mod tests {
+    use data_avalanche_cluster::Feed;
+    use data_avalanche_pipeline::WorkerStatus;
+    use tokio::sync::watch;
+
+    use super::*;
+    use crate::test_support::lone;
+
+    type Statuses = watch::Sender<BTreeMap<String, watch::Receiver<ControllerStatus>>>;
+
+    fn status(actual: Actual, epoch: u64) -> ControllerStatus {
+        ControllerStatus {
+            actual: Some(actual),
+            owner_epoch: Some(epoch),
+            worker: Some(WorkerStatus::default()),
+            ..ControllerStatus::default()
+        }
+    }
+
+    /// node-a running `orders` as `actual`, its statuses' sender kept to change them.
+    async fn node(actual: Actual) -> (Node, Statuses, tempfile::TempDir) {
+        let dir = tempfile::tempdir().unwrap();
+        let mut lone = lone(dir.path().join("config.yaml")).await;
+        let (tx, rx) = watch::channel(BTreeMap::from([(
+            "orders".to_owned(),
+            watch::channel(status(actual, 4)).1,
+        )]));
+        lone.node.statuses = rx;
+        (lone.node, tx, dir)
+    }
+
+    fn set(tx: &Statuses, s: ControllerStatus) {
+        tx.send_replace(BTreeMap::from([("orders".to_owned(), watch::channel(s).1)]));
+    }
+
+    fn kinds(sent: &[Live]) -> Vec<&'static str> {
+        sent.iter().map(|l| l.kind).collect()
+    }
+
+    #[tokio::test]
+    async fn a_pipeline_s_state_is_sent_when_it_changes() {
+        let (node, tx, _dir) = node(Actual::Running).await;
+        let mut p = Publisher::default();
+        let now = Utc::now();
+        let sent = p.due(&node, now, 1, false).await;
+        assert_eq!(kinds(&sent), ["pipeline"]);
+        let d = &sent[0].data;
+        assert_eq!(sent[0].pipeline.as_deref(), Some("orders"));
+        assert_eq!(
+            (
+                &d["node"],
+                &d["pipeline"],
+                &d["actual"],
+                &d["error"],
+                &d["owner_epoch"]
+            ),
+            (
+                &json!("node-a"),
+                &json!("orders"),
+                &json!("running"),
+                &Value::Null,
+                &json!(4)
+            )
+        );
+        assert!(p.due(&node, now, 1, false).await.is_empty(), "unchanged");
+        set(
+            &tx,
+            status(
+                Actual::Retrying {
+                    error: "the source is down".into(),
+                },
+                4,
+            ),
+        );
+        let sent = p.due(&node, now, 1, false).await;
+        assert_eq!(sent[0].data["actual"], "retrying");
+        assert_eq!(sent[0].data["error"], "the source is down");
+        // The lease gone: once `standby`, then nothing.
+        set(&tx, status(Actual::Standby { holder: None }, 4));
+        let sent = p.due(&node, now, 1, false).await;
+        assert_eq!(kinds(&sent), ["pipeline"]);
+        assert_eq!(sent[0].data["actual"], "standby");
+        assert_eq!(sent[0].data["owner_epoch"], Value::Null);
+        assert!(p.due(&node, now, 1, false).await.is_empty());
+    }
+
+    #[tokio::test]
+    async fn a_stream_that_opens_gets_every_state_again() {
+        let (node, _tx, _dir) = node(Actual::Paused).await;
+        let mut p = Publisher::default();
+        let now = Utc::now();
+        assert_eq!(p.due(&node, now, 1, false).await.len(), 1);
+        assert!(p.due(&node, now, 1, false).await.is_empty());
+        let again = p.due(&node, now, 2, false).await;
+        assert_eq!(again[0].data["actual"], "paused");
+        assert!(p.due(&node, now, 1, false).await.is_empty(), "one closed");
+    }
+
+    #[tokio::test]
+    async fn figures_are_the_live_answer_and_the_node_s_own() {
+        let (node, _tx, _dir) = node(Actual::Running).await;
+        let mut p = Publisher::default();
+        let now = Utc::now();
+        let sent = p.due(&node, now, 1, true).await;
+        assert_eq!(kinds(&sent), ["pipeline", "figures", "node"]);
+        let f = &sent[1].data;
+        assert_eq!(
+            (&f["id"], &f["node"], &f["actual"]),
+            (&json!("orders"), &json!("node-a"), &json!("running"))
+        );
+        assert_eq!(f["worker"]["phase"], "starting");
+        let n = &sent[2].data;
+        assert_eq!(n["node"], "node-a");
+        assert_eq!(n["pipelines"], json!(["orders"]));
+        assert!(n["process"].get("memory_bytes").is_some(), "{n}");
+        assert!(n["gossip"]["peers"].is_array(), "{n}");
+        let again = p.due(&node, now, 1, true).await;
+        assert_eq!(
+            kinds(&again),
+            ["figures", "node"],
+            "figures every time, the state once"
+        );
+    }
+
+    /// The loop sends nothing while no stream follows, and the state once one does.
+    #[tokio::test]
+    async fn the_node_publishes_only_while_followed() {
+        let (mut node, _tx, _dir) = node(Actual::Running).await;
+        node.feed = Feed::new();
+        let task = tokio::spawn(publish(node.clone()));
+        tokio::time::sleep(STATE_EVERY * 2).await;
+        let mut stream = node.feed.subscribe();
+        let first = tokio::time::timeout(STATE_EVERY * 3, stream.recv())
+            .await
+            .expect("within a look")
+            .unwrap();
+        assert!(
+            ["pipeline", "figures", "node"].contains(&first.kind),
+            "{first:?}"
+        );
+        task.abort();
+    }
+}
````

`crates/data-avalanche-cluster/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/live.rs b/crates/data-avalanche-cluster/src/live.rs
index c88e525..8900773 100644
--- a/crates/data-avalanche-cluster/src/live.rs
+++ b/crates/data-avalanche-cluster/src/live.rs
@@ -104,6 +104,7 @@ mod tests {
         feed.publish(live(1));
         let (mut a, mut b) = (feed.subscribe(), feed.subscribe());
         assert!(feed.followed());
+        assert_eq!(feed.followers(), 2);
         feed.publish(live(2));
         assert_eq!(
             *a.recv().await.unwrap(),
@@ -113,6 +114,7 @@ mod tests {
         assert_eq!(*b.recv().await.unwrap(), live(2));
         drop((a, b));
         assert!(!feed.followed());
+        assert_eq!(feed.followers(), 0);
         feed.publish(live(3));
     }
 
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- publish:: nodes:: && cargo test -p data-avalanche-cluster --lib live::`
Expected: the API's tests don't compile: the new module's find no `Publisher`, `publish` or `STATE_EVERY`, nor the imports the code above them brings (`json!`, `BTreeMap`, `ControllerStatus`, `Actual`, `Live`, `Node`, `Utc`, `Value`).

- [ ] **Step 3: Implement**

`nodes.rs`'s detail moves its process and gossip JSON into `process_json` and `gossip_json`, unchanged, for the `node` message to share.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 3b2f664..96d7b1f 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -41,7 +41,8 @@ use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
 use axum::routing::{MethodRouter, get, post};
 use data_avalanche_cluster::{
-    ConfigCoordinator, ControllerStatus, DocsFn, GossipFigures, Members, Recorder, WorkerFactory,
+    ConfigCoordinator, ControllerStatus, DocsFn, Feed, GossipFigures, Members, Recorder,
+    WorkerFactory,
 };
 use data_avalanche_config::Role;
 use data_avalanche_state::{Charts, Events, Leases};
@@ -64,6 +65,7 @@ pub use prometheus::{
     MAX_PROMETHEUS_ANSWER, MIN_RATE_WINDOW, Outcome, PrometheusClient, PrometheusError,
     PrometheusReader, promql,
 };
+pub use publish::{FIGURES_EVERY, STATE_EVERY, publish};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 pub use snapshots::{EndedJobs, READ_AT_ONCE};
 pub use tables::{FACTS_FOR, FACTS_WITHIN, Lake};
@@ -121,6 +123,8 @@ pub struct Node {
     pub charts: Arc<Charts>,
     /// Reads a pipeline's series older than the history kept from `globals.console.prometheus`.
     pub prometheus: Arc<PrometheusReader>,
+    /// The node's live messages, sent to each `GET /v1/stream` open on it (M13f).
+    pub feed: Feed,
 }
 
 impl std::fmt::Debug for Node {
````

`crates/data-avalanche-api/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/live.rs b/crates/data-avalanche-api/src/live.rs
index 39140b9..76c89d8 100644
--- a/crates/data-avalanche-api/src/live.rs
+++ b/crates/data-avalanche-api/src/live.rs
@@ -37,7 +37,7 @@ fn phase_name(p: &Phase) -> &'static str {
 }
 
 /// The controller's state, and its error.
-fn actual_of(a: &Actual) -> (&'static str, Option<&str>) {
+pub(crate) fn actual_of(a: &Actual) -> (&'static str, Option<&str>) {
     match a {
         Actual::Standby { .. } => ("standby", None),
         Actual::Starting => ("starting", None),
@@ -201,7 +201,7 @@ fn tables(
 }
 
 /// The answer of the node that runs the pipeline.
-fn answer(
+pub(crate) fn answer(
     me: &str,
     p: &Pipeline,
     s: &ControllerStatus,
````

`crates/data-avalanche-api/src/nodes.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/nodes.rs b/crates/data-avalanche-api/src/nodes.rs
index 65dec61..e868aa4 100644
--- a/crates/data-avalanche-api/src/nodes.rs
+++ b/crates/data-avalanche-api/src/nodes.rs
@@ -8,7 +8,8 @@ use axum::extract::{Path, RawQuery, State};
 use axum::http::{HeaderMap, Method, StatusCode};
 use axum::response::Response;
 use chrono::{DateTime, Utc};
-use data_avalanche_cluster::{Phase, Ring};
+use data_avalanche_cluster::{GossipFigures, Members, Phase, Ring};
+use data_avalanche_telemetry::ProcessFigures;
 use serde_json::{Value, json};
 
 use crate::peers::{ACTION_LIMIT, Asked, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, STATUS_LIMIT};
@@ -20,7 +21,7 @@ pub(crate) fn short(hash: &str) -> &str {
     hex.get(..16).unwrap_or(hex)
 }
 
-fn phase(p: Phase) -> &'static str {
+pub(crate) fn phase(p: Phase) -> &'static str {
     match p {
         Phase::Joining => "joining",
         Phase::Active => "active",
@@ -29,6 +30,47 @@ fn phase(p: Phase) -> &'static str {
     }
 }
 
+/// The process's figures, as a node's detail and its live `node` message show them.
+pub(crate) fn process_json(p: &ProcessFigures) -> Value {
+    json!({
+        "memory_bytes": p.reading.memory_bytes,
+        "memory_limit_bytes": p.reading.memory_limit_bytes,
+        "cpu_seconds": p.reading.cpu_seconds,
+        "cpu_cores": p.cpu_cores,
+        "cpu_limit_cores": p.reading.cpu_limit_cores,
+    })
+}
+
+/// What the gossip saw of the node's peers and its probes, as of `now`: a suspicion a day old
+/// goes though no newer one came to prune it.
+pub(crate) fn gossip_json(members: &Members, mut g: GossipFigures, now: DateTime<Utc>) -> Value {
+    g.forget_before(now);
+    let (raised_more, received_more) = (g.raised_more(), g.received_more());
+    let peers: Vec<Value> = members
+        .nodes
+        .iter()
+        .filter(|(name, _)| **name != members.me)
+        .map(|(name, m)| {
+            json!({
+                "node": name,
+                "state": if m.suspect { "suspect" } else { "alive" },
+                "phase": phase(m.meta.phase),
+            })
+        })
+        .collect();
+    json!({
+        "peers": peers,
+        "last_probe": g.last_probe.map(|(n, at)| json!({ "node": n, "at": at })),
+        "probe_rtt_ms": g.probe_rtt.map(|d| d.as_secs_f64() * 1000.0),
+        "suspicions": {
+            "raised": g.raised.iter().map(|(n, at)| json!({ "node": n, "at": at })).collect::<Vec<_>>(),
+            "received": g.received,
+            "raised_more": raised_more,
+            "received_more": received_more,
+        },
+    })
+}
+
 /// How many leases a node's detail reads at once.
 const LEASE_READS: usize = 16;
 
@@ -93,22 +135,7 @@ async fn detail(node: &Node) -> Value {
         }
     }
     let p = *node.process.borrow();
-    // As of now: a suspicion a day old goes though no newer one came to prune it.
-    let mut g = node.gossip.borrow().clone();
-    g.forget_before(Utc::now());
-    let (raised_more, received_more) = (g.raised_more(), g.received_more());
-    let peers: Vec<Value> = members
-        .nodes
-        .iter()
-        .filter(|(name, _)| **name != members.me)
-        .map(|(name, m)| {
-            json!({
-                "node": name,
-                "state": if m.suspect { "suspect" } else { "alive" },
-                "phase": phase(m.meta.phase),
-            })
-        })
-        .collect();
+    let g = node.gossip.borrow().clone();
     let started = me.and_then(|m| {
         i64::try_from(m.incarnation)
             .ok()
@@ -132,24 +159,8 @@ async fn detail(node: &Node) -> Value {
                 .map(|g| file_hash == g),
         },
         "restart_pending": node.coordinator.restart_pending().await,
-        "process": {
-            "memory_bytes": p.reading.memory_bytes,
-            "memory_limit_bytes": p.reading.memory_limit_bytes,
-            "cpu_seconds": p.reading.cpu_seconds,
-            "cpu_cores": p.cpu_cores,
-            "cpu_limit_cores": p.reading.cpu_limit_cores,
-        },
-        "gossip": {
-            "peers": peers,
-            "last_probe": g.last_probe.map(|(n, at)| json!({ "node": n, "at": at })),
-            "probe_rtt_ms": g.probe_rtt.map(|d| d.as_secs_f64() * 1000.0),
-            "suspicions": {
-                "raised": g.raised.iter().map(|(n, at)| json!({ "node": n, "at": at })).collect::<Vec<_>>(),
-                "received": g.received,
-                "raised_more": raised_more,
-                "received_more": received_more,
-            },
-        },
+        "process": process_json(&p),
+        "gossip": gossip_json(&members, g, Utc::now()),
         "pipelines": held,
     })
 }
````

`crates/data-avalanche-api/src/publish.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/publish.rs b/crates/data-avalanche-api/src/publish.rs
index 337a96d..2da1b7f 100644
--- a/crates/data-avalanche-api/src/publish.rs
+++ b/crates/data-avalanche-api/src/publish.rs
@@ -9,6 +9,148 @@
 //! When a stream opens, every state is sent again at the next look: a stream starts without
 //! replay, so a page learns each pipeline's state within a second of opening it.
 
+use std::collections::{BTreeMap, BTreeSet};
+use std::time::Duration;
+
+use chrono::{DateTime, Utc};
+use data_avalanche_cluster::{Actual, ControllerStatus, Live};
+use serde_json::{Value, json};
+use tokio::time::MissedTickBehavior;
+
+use crate::Node;
+use crate::live::{actual_of, answer};
+use crate::nodes::{gossip_json, phase, process_json};
+
+/// How often a pipeline's state is looked at.
+pub const STATE_EVERY: Duration = Duration::from_secs(1);
+/// How often a pipeline's and the node's figures are sent (Oleksii: 5 s).
+pub const FIGURES_EVERY: Duration = Duration::from_secs(5);
+
+/// What the node sent last of each pipeline it runs, and how many streams followed it then.
+#[derive(Debug, Default)]
+pub(crate) struct Publisher {
+    sent: BTreeMap<String, Value>,
+    followers: usize,
+}
+
+impl Publisher {
+    /// The messages due at `now`, with `followers` streams open: each pipeline whose state
+    /// changed since the last look (every one when a stream opened since), a `standby` for each
+    /// that stopped running here, and with `figures` each pipeline's live figures and the node's.
+    pub(crate) async fn due(
+        &mut self,
+        node: &Node,
+        now: DateTime<Utc>,
+        followers: usize,
+        figures: bool,
+    ) -> Vec<Live> {
+        if followers > self.followers {
+            self.sent.clear();
+        }
+        self.followers = followers;
+        let me = node.members.borrow().me.clone();
+        let running: BTreeMap<String, ControllerStatus> = node
+            .statuses
+            .borrow()
+            .iter()
+            .map(|(id, s)| (id.clone(), s.borrow().clone()))
+            .filter(|(_, s)| {
+                s.actual
+                    .as_ref()
+                    .is_some_and(|a| !matches!(a, Actual::Standby { .. }))
+            })
+            .collect();
+        let mut out = Vec::new();
+        for (id, s) in &running {
+            let (actual, error) = s.actual.as_ref().map_or(("starting", None), actual_of);
+            let state = json!({ "actual": actual, "error": error, "owner_epoch": s.owner_epoch });
+            if self.sent.get(id) != Some(&state) {
+                out.push(state_of(&me, id, now, &state));
+                self.sent.insert(id.clone(), state);
+            }
+        }
+        let gone: Vec<String> = self
+            .sent
+            .keys()
+            .filter(|id| !running.contains_key(*id))
+            .cloned()
+            .collect();
+        for id in gone {
+            self.sent.remove(&id);
+            let state = json!({ "actual": "standby", "error": null, "owner_epoch": null });
+            out.push(state_of(&me, &id, now, &state));
+        }
+        if figures {
+            let config = node.coordinator.config().await;
+            for (id, s) in &running {
+                if let Some(p) = config.pipelines.values().find(|p| p.id == *id) {
+                    out.push(Live {
+                        kind: "figures",
+                        pipeline: Some(id.clone()),
+                        data: answer(&me, p, s, None, now),
+                    });
+                }
+            }
+            out.push(node_of(node, &me, running.keys().cloned().collect(), now));
+        }
+        out
+    }
+}
+
+/// A pipeline's state message.
+fn state_of(me: &str, id: &str, now: DateTime<Utc>, state: &Value) -> Live {
+    let mut data = json!({ "node": me, "pipeline": id, "at": now });
+    for (k, v) in state.as_object().into_iter().flatten() {
+        data[k] = v.clone();
+    }
+    Live {
+        kind: "pipeline",
+        pipeline: Some(id.to_owned()),
+        data,
+    }
+}
+
+/// The node's own figures: its phase, process and gossip, and the pipelines it runs.
+fn node_of(node: &Node, me: &str, running: BTreeSet<String>, now: DateTime<Utc>) -> Live {
+    let members = node.members.borrow().clone();
+    let process = *node.process.borrow();
+    let gossip = node.gossip.borrow().clone();
+    Live {
+        kind: "node",
+        pipeline: None,
+        data: json!({
+            "node": me,
+            "at": now,
+            "phase": members.nodes.get(me).map(|m| phase(m.meta.phase)),
+            "process": process_json(&process),
+            "gossip": gossip_json(&members, gossip, now),
+            "pipelines": running,
+        }),
+    }
+}
+
+/// Sends what is due every [`STATE_EVERY`] while a stream follows the node, until aborted.
+pub async fn publish(node: Node) {
+    let per_figures = (FIGURES_EVERY.as_secs() / STATE_EVERY.as_secs()).max(1);
+    let mut every = tokio::time::interval(STATE_EVERY);
+    every.set_missed_tick_behavior(MissedTickBehavior::Delay);
+    let mut publisher = Publisher::default();
+    let mut ticks: u64 = 0;
+    loop {
+        every.tick().await;
+        let followers = node.feed.followers();
+        if followers == 0 {
+            publisher = Publisher::default();
+            continue;
+        }
+        let figures = ticks.is_multiple_of(per_figures);
+        for live in publisher.due(&node, Utc::now(), followers, figures).await {
+            node.feed.publish(live);
+        }
+        ticks += 1;
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use data_avalanche_cluster::Feed;
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index e9ce619..eb0e743 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -238,12 +238,8 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
     );
     let metrics = Metrics::new();
     let events = Arc::new(Events::new(docs.clone()));
-    let (recorder, _) = Recorder::start(
-        Arc::clone(&events),
-        "node-a",
-        metrics.clone(),
-        &data_avalanche_cluster::Feed::new(),
-    );
+    let feed = data_avalanche_cluster::Feed::new();
+    let (recorder, _) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone(), &feed);
     let c = &config.globals.cluster;
     let removals = Removals::new(docs.clone());
     let deps = ControllerDeps {
@@ -322,6 +318,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             lake: Arc::default(),
             charts: Arc::new(data_avalanche_state::Charts::new(docs.clone())),
             prometheus: Arc::default(),
+            feed,
         },
         events,
         leases,
````

`crates/data-avalanche-cluster/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/live.rs b/crates/data-avalanche-cluster/src/live.rs
index 8900773..c3b65c3 100644
--- a/crates/data-avalanche-cluster/src/live.rs
+++ b/crates/data-avalanche-cluster/src/live.rs
@@ -66,6 +66,11 @@ impl Feed {
         self.tx.receiver_count() > 0
     }
 
+    /// How many streams are open on this node: a new one is sent every pipeline's state again.
+    pub fn followers(&self) -> usize {
+        self.tx.receiver_count()
+    }
+
     /// A new stream's messages, from now on.
     pub fn subscribe(&self) -> broadcast::Receiver<Arc<Live>> {
         self.tx.subscribe()
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index ff93442..dd19e37 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -8,7 +8,7 @@ use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
 use data_avalanche_api::{
-    Api, Auth, Control, Node, Peers, RateLimit, ServeError, Stop, bind, router,
+    Api, Auth, Control, Node, Peers, RateLimit, ServeError, Stop, bind, publish, router,
 };
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
@@ -339,7 +339,10 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         lake: Arc::default(),
         charts,
         prometheus: Arc::default(),
+        feed: feed.clone(),
     });
+    // What the node sends its live streams of its own accord: its pipelines' states and figures.
+    let publisher = node.get().cloned().map(|n| tokio::spawn(publish(n)));
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
     let mut stops = Stops::new(Box::pin(shutdown_signal()), asked);
@@ -366,6 +369,9 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         tracing::info!("left the cluster; the process stays up until it is stopped");
     }
     tracing::info!("shutting down");
+    if let Some(p) = publisher {
+        p.abort();
+    }
     let _ = stop_http.send(());
     let _ = server.await;
     // The leave's last events (each lease released) are still queued, and the history's last
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- publish:: nodes:: && cargo test -p data-avalanche-cluster --lib live::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/live.rs crates/data-avalanche-api/src/nodes.rs crates/data-avalanche-api/src/publish.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche-cluster/src/live.rs crates/data-avalanche/src/run.rs
git commit -m "feat(api): the live publisher — each pipeline's state as it changes, its figures and the node's every 5 s"
```

---

### Task 3: `GET /v1/stream`: this node's own (`data-avalanche-api`, `data-avalanche`)

**Files:**
- Regenerate: `crates/data-avalanche-api/openapi.json`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/openapi.rs`
- Create: `crates/data-avalanche-api/src/stream.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-cluster/src/live.rs`
- Modify: `crates/data-avalanche/src/run.rs`
- Regenerate: `docs/public/openapi.json`
- Regenerate: `docs/src/content/docs/reference/api.md`
- Modify: `xtask/src/api.rs`

**Interfaces:**
- Consumes: Task 1's `Feed`; Task 2's `Node::feed`.
- Produces:
  - `Feed::open(&self) -> Option<Opened>`, `Opened { messages, closed, .. }` (its place given back on drop),
    `MAX_STREAMS` (64) (cluster);
  - the route `GET /v1/stream` (viewer), `stream::open`; `stream::PARAMETERS`, `KINDS`, `KEEP_ALIVE` (15 s);
    `Filter::passes(&self, &Live) -> bool` (crate-private);
  - the OpenAPI document's `text/event-stream` answer and the site's API reference's `stream` section
    (`xtask/src/api.rs`), generated with the route so `cargo xtask docs --check` holds at every commit;
  - `test_support::{Stream, stream}`: a stream read as a page reads it.
- The binary closes the feed before it stops its server.

- [ ] **Step 1: Write the failing tests**

The tests: `a_stream_says_hello_then_sends_each_change_as_it_happens`, `a_stream_asked_for_a_pipeline_and_kinds_gets_only_those`, `a_parameter_that_isnt_one_is_refused`, `a_stream_far_behind_is_told_to_read_again`, `at_most_max_streams_are_open_on_a_node`, `a_closed_feed_ends_every_stream`, `a_client_that_goes_gives_its_place_back` (stream); `at_most_max_streams_are_open_at_once` (live); the route table. The new module holds only its tests for now.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 96d7b1f..03fc788 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -25,6 +25,7 @@ mod prometheus;
 mod publish;
 mod server;
 mod snapshots;
+mod stream;
 mod tables;
 #[cfg(test)]
 mod test_support;
@@ -1018,6 +1019,7 @@ mod tests {
                 ("GET", "/v1/nodes/{node}/config/status", Viewer),
                 ("GET", "/v1/events", Viewer),
                 ("GET", "/v1/history", Viewer),
+                ("GET", "/v1/stream", Viewer),
                 ("GET", "/v1/openapi.json", Viewer),
             ]
         );
````

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
new file mode 100644
index 0000000..dc62354
--- /dev/null
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -0,0 +1,160 @@
+//! `GET /v1/stream` (M13f, Admin_Console §8): the cluster's live changes as server-sent events.
+//! A stream starts with `hello`, then sends each change as it happens, without replay: a page
+//! reads the current state, then applies what follows. A stream that fell too far behind is sent
+//! `reset`, and reads again.
+
+#[cfg(test)]
+mod tests {
+    use std::net::SocketAddr;
+
+    use data_avalanche_cluster::FEED_CAPACITY;
+
+    use super::*;
+    use crate::test_support::{Stream, served_with, stream};
+
+    fn live(kind: &'static str, pipeline: Option<&str>, n: u64) -> Live {
+        Live {
+            kind,
+            pipeline: pipeline.map(str::to_owned),
+            data: json!({ "node": "node-a", "n": n }),
+        }
+    }
+
+    /// node-a, with node-b a member without an API address: its streams are node-a's alone.
+    async fn node_a() -> (SocketAddr, crate::test_support::Lone, tempfile::TempDir) {
+        served_with("", crate::test_support::open(), |_| {}).await
+    }
+
+    /// A stream's `hello`, its node now following none.
+    async fn hello(s: &mut Stream) {
+        let (kind, data) = s.next().await;
+        assert_eq!(kind, "hello");
+        assert_eq!(data["node"], "node-a");
+    }
+
+    #[tokio::test]
+    async fn a_stream_says_hello_then_sends_each_change_as_it_happens() {
+        let (addr, lone, _dir) = node_a().await;
+        let mut s = stream(addr, "/v1/stream", &[]).await;
+        assert_eq!(s.code, 200);
+        assert!(s.head.contains("text/event-stream"), "{}", s.head);
+        assert!(s.head.contains("x-accel-buffering: no"), "{}", s.head);
+        hello(&mut s).await;
+        lone.node.feed.publish(live("figures", Some("orders"), 1));
+        lone.node.feed.publish(live("node", None, 2));
+        assert_eq!(
+            s.next().await,
+            ("figures".into(), live("figures", None, 1).data)
+        );
+        assert_eq!(s.next().await, ("node".into(), live("node", None, 2).data));
+    }
+
+    #[tokio::test]
+    async fn a_stream_asked_for_a_pipeline_and_kinds_gets_only_those() {
+        let (addr, lone, _dir) = node_a().await;
+        let mut s = stream(addr, "/v1/stream?pipeline=orders&kind=event,figures", &[]).await;
+        hello(&mut s).await;
+        let feed = &lone.node.feed;
+        feed.publish(live("node", None, 1));
+        feed.publish(live("event", None, 2));
+        feed.publish(live("pipeline", Some("orders"), 3));
+        feed.publish(live("figures", Some("other"), 4));
+        feed.publish(live("event", Some("orders"), 5));
+        assert_eq!(s.next().await.1["n"], 5);
+    }
+
+    #[tokio::test]
+    async fn a_parameter_that_isnt_one_is_refused() {
+        let (addr, _lone, _dir) = node_a().await;
+        for (path, code, error) in [
+            (
+                "/v1/stream?kind=nodes",
+                400,
+                "unknown kind \"nodes\": a stream sends pipeline, figures, node, samples, event",
+            ),
+            (
+                "/v1/stream?pipeline=",
+                400,
+                "pipeline names a pipeline's id",
+            ),
+            (
+                "/v1/stream?kind=node&kind=event",
+                400,
+                "kind is given twice",
+            ),
+            (
+                "/v1/stream?since=1",
+                400,
+                "unknown parameter \"since\": the stream takes pipeline, kind",
+            ),
+            (
+                "/v1/stream?pipeline=orders,nope",
+                404,
+                "no pipeline \"nope\" in the config generation this node runs",
+            ),
+        ] {
+            let (c, v) = crate::test_support::json_of(addr, "GET", path).await;
+            assert_eq!((c, v["error"].as_str().unwrap()), (code, error), "{path}");
+        }
+    }
+
+    #[tokio::test]
+    async fn a_stream_far_behind_is_told_to_read_again() {
+        let (addr, lone, _dir) = node_a().await;
+        let mut s = stream(addr, "/v1/stream", &[]).await;
+        hello(&mut s).await;
+        // Published at once, before the stream reads one: it falls 10 behind the feed's capacity.
+        for n in 0..(FEED_CAPACITY as u64 + 10) {
+            lone.node.feed.publish(live("node", None, n));
+        }
+        let (kind, data) = s.next().await;
+        assert_eq!(kind, "reset");
+        assert_eq!(
+            data,
+            json!({ "node": "node-a", "reason": "lagged", "missed": 10 })
+        );
+        assert_eq!(
+            s.next().await.1["n"],
+            10,
+            "then what it still holds, in order"
+        );
+    }
+
+    #[tokio::test]
+    async fn at_most_max_streams_are_open_on_a_node() {
+        let (addr, lone, _dir) = node_a().await;
+        let held: Vec<_> = (0..MAX_STREAMS).map(|_| lone.node.feed.open()).collect();
+        let (code, head, body) = crate::test_support::get(addr, "/v1/stream").await;
+        assert_eq!(code, 503);
+        assert!(head.to_lowercase().contains("retry-after: 5"), "{head}");
+        assert!(body.contains("64 streams are open on this node"), "{body}");
+        drop(held);
+        let mut s = stream(addr, "/v1/stream", &[]).await;
+        hello(&mut s).await;
+    }
+
+    /// A node that stops ends its streams: an open one doesn't hold the server's stop.
+    #[tokio::test]
+    async fn a_closed_feed_ends_every_stream() {
+        let (addr, lone, _dir) = node_a().await;
+        let mut s = stream(addr, "/v1/stream", &[]).await;
+        hello(&mut s).await;
+        lone.node.feed.close();
+        assert!(s.ended().await, "the response ends");
+    }
+
+    /// A stream whose client went gives its place back.
+    #[tokio::test]
+    async fn a_client_that_goes_gives_its_place_back() {
+        let (addr, lone, _dir) = node_a().await;
+        let mut s = stream(addr, "/v1/stream", &[]).await;
+        hello(&mut s).await;
+        assert_eq!(lone.node.feed.followers(), 1);
+        drop(s);
+        let within = tokio::time::Instant::now() + Duration::from_secs(3);
+        while lone.node.feed.followers() > 0 {
+            assert!(tokio::time::Instant::now() < within, "still open");
+            tokio::time::sleep(Duration::from_millis(20)).await;
+        }
+    }
+}
````

`crates/data-avalanche-cluster/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/live.rs b/crates/data-avalanche-cluster/src/live.rs
index c3b65c3..bc6b24f 100644
--- a/crates/data-avalanche-cluster/src/live.rs
+++ b/crates/data-avalanche-cluster/src/live.rs
@@ -137,6 +137,16 @@ mod tests {
         assert_eq!(*slow.recv().await.unwrap(), live(1));
     }
 
+    #[test]
+    fn at_most_max_streams_are_open_at_once() {
+        let feed = Feed::new();
+        let open: Vec<Opened> = (0..MAX_STREAMS).map(|_| feed.open().unwrap()).collect();
+        assert_eq!(feed.followers(), MAX_STREAMS);
+        assert!(feed.open().is_none());
+        drop(open);
+        assert!(feed.open().is_some(), "their places given back");
+    }
+
     #[test]
     fn a_closed_feed_says_so_to_every_stream() {
         let feed = Feed::new();
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- stream:: tests::the_routes && cargo test -p data-avalanche-cluster --lib live::`
Expected: the API's tests don't compile: `crate::test_support::{Stream, stream}` are unresolved, the new module finds no `Live`, `MAX_STREAMS`, `Duration` or `json!`, and `Feed` has no `open`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 03fc788..da796ce 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -69,6 +69,7 @@ pub use prometheus::{
 pub use publish::{FIGURES_EVERY, STATE_EVERY, publish};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 pub use snapshots::{EndedJobs, READ_AT_ONCE};
+pub use stream::{KEEP_ALIVE, KINDS};
 pub use tables::{FACTS_FOR, FACTS_WITHIN, Lake};
 
 use crate::auth::Denied;
@@ -821,6 +822,27 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(history::read),
         ),
+        (
+            r(
+                "GET",
+                "/v1/stream",
+                Viewer,
+                "The cluster's live changes as server-sent events, from any node: each pipeline's state and figures, each node's own, the samples taken and the events written, as they happen",
+                &[
+                    (
+                        200,
+                        "`text/event-stream`: `hello` first, then each change as it happens, without replay: read the current state, then apply what follows",
+                    ),
+                    (400, "a parameter that isn't one, or isn't in its form"),
+                    (404, "no such pipeline in the generation this node runs"),
+                    (
+                        503,
+                        "as many streams are open on this node as it takes; `Retry-After` says when to try again",
+                    ),
+                ],
+            ),
+            get(stream::open),
+        ),
         (
             r(
                 "GET",
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index a8c67e7..53c09df 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -136,6 +136,18 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
             );
         }
     }
+    if r.path == "/v1/stream" {
+        for (name, description) in crate::stream::PARAMETERS {
+            op = op.parameter(
+                ParameterBuilder::new()
+                    .name(*name)
+                    .parameter_in(ParameterIn::Query)
+                    .required(Required::False)
+                    .description(Some(*description))
+                    .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
+            );
+        }
+    }
     if r.path == "/v1/events" {
         for (name, description) in crate::events::PARAMETERS {
             let schema = if *name == "limit" {
@@ -191,11 +203,17 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
     }
     answers.sort_unstable();
     for (code, description) in answers {
+        // The stream's messages are server-sent events; its refusals JSON as every route's.
+        let content = if r.path == "/v1/stream" && code == 200 {
+            "text/event-stream"
+        } else {
+            "application/json"
+        };
         op = op.response(
             code.to_string(),
             ResponseBuilder::new()
                 .description(description)
-                .content("application/json", ContentBuilder::new().build()),
+                .content(content, ContentBuilder::new().build()),
         );
     }
     op.build()
````

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
index dc62354..9efb1f2 100644
--- a/crates/data-avalanche-api/src/stream.rs
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -3,6 +3,208 @@
 //! reads the current state, then applies what follows. A stream that fell too far behind is sent
 //! `reset`, and reads again.
 
+use std::collections::BTreeSet;
+use std::convert::Infallible;
+use std::time::Duration;
+
+use axum::extract::{RawQuery, State};
+use axum::http::{HeaderValue, StatusCode, header};
+use axum::response::sse::{Event, KeepAlive, Sse};
+use axum::response::{IntoResponse, Response};
+use chrono::Utc;
+use data_avalanche_cluster::{Live, MAX_STREAMS, Opened};
+use data_avalanche_config::ResolvedConfig;
+use serde_json::{Value, json};
+use tokio::sync::{broadcast, mpsc};
+
+use crate::{Api, Failure, starting};
+
+/// What `GET /v1/stream` takes, each once.
+pub const PARAMETERS: &[(&str, &str)] = &[
+    (
+        "pipeline",
+        "a pipeline's id, or several comma-separated: only the messages about them; by default every message",
+    ),
+    (
+        "kind",
+        "the kinds sent, comma-separated, of `pipeline`, `figures`, `node`, `samples`, `event`; by default every kind",
+    ),
+];
+
+/// The kinds of change a stream can be asked for; `hello`, `reset`, `members` and `peer` are
+/// always sent.
+pub const KINDS: &[&str] = &["pipeline", "figures", "node", "samples", "event"];
+
+/// How often a stream with nothing to send sends a comment, under the server's idle timeout.
+pub const KEEP_ALIVE: Duration = Duration::from_secs(15);
+
+/// How long a client refused for [`MAX_STREAMS`] waits before it tries again, in seconds.
+const RETRY_AFTER: &str = "5";
+
+/// The messages a stream's response holds before the node waits on its client.
+const BUFFER: usize = 64;
+
+/// Which messages a stream is sent.
+#[derive(Debug, Clone, Default, PartialEq)]
+pub(crate) struct Filter {
+    pipelines: Option<BTreeSet<String>>,
+    kinds: Option<BTreeSet<&'static str>>,
+}
+
+impl Filter {
+    /// Whether `live` is sent: of a kind asked, and, when pipelines are asked, about one of them.
+    pub(crate) fn passes(&self, live: &Live) -> bool {
+        self.kinds.as_ref().is_none_or(|k| k.contains(live.kind))
+            && self
+                .pipelines
+                .as_ref()
+                .is_none_or(|p| live.pipeline.as_ref().is_some_and(|id| p.contains(id)))
+    }
+}
+
+fn bad(error: impl Into<String>) -> Failure {
+    Failure::new(StatusCode::BAD_REQUEST, error)
+}
+
+/// The filter of `params`, each once, against `config`.
+fn filter(params: &[(String, String)], config: &ResolvedConfig) -> Result<Filter, Failure> {
+    let mut f = Filter::default();
+    let mut given = BTreeSet::new();
+    for (name, value) in params {
+        if !given.insert(name.as_str()) {
+            return Err(bad(format!("{name} is given twice")));
+        }
+        let list: Vec<&str> = value
+            .split(',')
+            .map(str::trim)
+            .filter(|s| !s.is_empty())
+            .collect();
+        match name.as_str() {
+            "pipeline" => {
+                if list.is_empty() {
+                    return Err(bad("pipeline names a pipeline's id"));
+                }
+                for id in &list {
+                    if !config.pipelines.values().any(|p| p.id == *id) {
+                        return Err(Failure::new(
+                            StatusCode::NOT_FOUND,
+                            format!("no pipeline {id:?} in the config generation this node runs"),
+                        ));
+                    }
+                }
+                f.pipelines = Some(list.iter().map(|s| (*s).to_owned()).collect());
+            }
+            "kind" => {
+                if list.is_empty() {
+                    return Err(bad("kind names a kind"));
+                }
+                let mut kinds = BTreeSet::new();
+                for k in list {
+                    let Some(k) = KINDS.iter().find(|n| **n == k) else {
+                        return Err(bad(format!(
+                            "unknown kind {k:?}: a stream sends {}",
+                            KINDS.join(", ")
+                        )));
+                    };
+                    kinds.insert(*k);
+                }
+                f.kinds = Some(kinds);
+            }
+            _ => {
+                return Err(bad(format!(
+                    "unknown parameter {name:?}: the stream takes {}",
+                    PARAMETERS
+                        .iter()
+                        .map(|(n, _)| *n)
+                        .collect::<Vec<_>>()
+                        .join(", ")
+                )));
+            }
+        }
+    }
+    Ok(f)
+}
+
+/// A message as the stream sends it.
+fn event(kind: &str, data: &Value) -> Event {
+    Event::default().event(kind).data(data.to_string())
+}
+
+/// `GET /v1/stream`.
+pub(crate) async fn open(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let params: Vec<(String, String)> =
+        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
+            .into_owned()
+            .collect();
+    let config = node.coordinator.config().await;
+    let filter = match filter(&params, &config) {
+        Ok(f) => f,
+        Err(f) => return f.into(),
+    };
+    let Some(opened) = node.feed.open() else {
+        let mut r: Response = Failure::new(
+            StatusCode::SERVICE_UNAVAILABLE,
+            format!("{MAX_STREAMS} streams are open on this node, as many as it takes"),
+        )
+        .into();
+        r.headers_mut()
+            .insert(header::RETRY_AFTER, HeaderValue::from_static(RETRY_AFTER));
+        return r;
+    };
+    let me = node.members.borrow().me.clone();
+    let (tx, rx) = mpsc::channel(BUFFER);
+    tokio::spawn(pump(me, filter, opened, tx));
+    let events = futures_util::stream::unfold(rx, |mut rx| async move {
+        rx.recv().await.map(|e| (Ok::<_, Infallible>(e), rx))
+    });
+    let mut r = Sse::new(events)
+        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
+        .into_response();
+    // A proxy in front of the node passes each message on as it comes.
+    r.headers_mut()
+        .insert("x-accel-buffering", HeaderValue::from_static("no"));
+    r
+}
+
+/// Sends `e` unless the client went or the node stops first; whether the stream goes on.
+async fn deliver(tx: &mpsc::Sender<Event>, opened: &mut Opened, e: Event) -> bool {
+    tokio::select! {
+        sent = tx.send(e) => sent.is_ok(),
+        _ = opened.closed.wait_for(|c| *c) => false,
+    }
+}
+
+/// Sends `hello`, then this node's messages that pass `filter`, until the client goes or the
+/// node stops.
+async fn pump(me: String, filter: Filter, mut opened: Opened, tx: mpsc::Sender<Event>) {
+    let hello = json!({ "node": me, "at": Utc::now(), "follows": [] });
+    if !deliver(&tx, &mut opened, event("hello", &hello)).await {
+        return;
+    }
+    loop {
+        let next = tokio::select! {
+            () = tx.closed() => return,
+            _ = opened.closed.wait_for(|c| *c) => return,
+            r = opened.messages.recv() => r,
+        };
+        let e = match next {
+            Ok(live) if filter.passes(&live) => event(live.kind, &live.data),
+            Ok(_) => continue,
+            Err(broadcast::error::RecvError::Lagged(missed)) => event(
+                "reset",
+                &json!({ "node": me, "reason": "lagged", "missed": missed }),
+            ),
+            Err(broadcast::error::RecvError::Closed) => return,
+        };
+        if !deliver(&tx, &mut opened, e).await {
+            return;
+        }
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::SocketAddr;
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index eb0e743..3d7e37e 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -588,3 +588,74 @@ pub(crate) async fn served_with(
     std::mem::forget(view);
     (addr, lone, dir)
 }
+
+/// A `GET /v1/stream` read as a page reads it: its code and head, then one message at a time.
+pub(crate) struct Stream {
+    pub code: u16,
+    /// The response's headers, `name: value` a line, names in lower case.
+    pub head: String,
+    response: reqwest::Response,
+    buffer: String,
+}
+
+/// Opens a stream at `path` with `headers`.
+pub(crate) async fn stream(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Stream {
+    let mut asked = reqwest::Client::new().get(format!("http://{addr}{path}"));
+    for (k, v) in headers {
+        asked = asked.header(*k, *v);
+    }
+    let response = asked.send().await.unwrap();
+    let head = response
+        .headers()
+        .iter()
+        .map(|(k, v)| format!("{k}: {}\n", v.to_str().unwrap_or("")))
+        .collect();
+    Stream {
+        code: response.status().as_u16(),
+        head,
+        response,
+        buffer: String::new(),
+    }
+}
+
+impl Stream {
+    /// The next message's kind and data, keep-alive comments skipped; within 5 s.
+    pub async fn next(&mut self) -> (String, Value) {
+        tokio::time::timeout(Duration::from_secs(5), async {
+            loop {
+                while let Some(end) = self.buffer.find("\n\n") {
+                    let frame: String = self.buffer.drain(..end + 2).collect();
+                    let (mut kind, mut data) = (None, String::new());
+                    for line in frame.lines() {
+                        if let Some(k) = line.strip_prefix("event:") {
+                            kind = Some(k.trim().to_owned());
+                        } else if let Some(d) = line.strip_prefix("data:") {
+                            data.push_str(d.trim_start());
+                        }
+                    }
+                    if let Some(kind) = kind {
+                        return (kind, serde_json::from_str(&data).unwrap());
+                    }
+                }
+                let chunk = self
+                    .response
+                    .chunk()
+                    .await
+                    .unwrap()
+                    .expect("the stream ended");
+                self.buffer.push_str(std::str::from_utf8(&chunk).unwrap());
+            }
+        })
+        .await
+        .expect("a message within 5 s")
+    }
+
+    /// Whether the response ends within 3 s, what it still sends read past.
+    pub async fn ended(&mut self) -> bool {
+        tokio::time::timeout(Duration::from_secs(3), async {
+            while self.response.chunk().await.ok().flatten().is_some() {}
+        })
+        .await
+        .is_ok()
+    }
+}
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index cbdac92..f87d932 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -50,7 +50,7 @@ pub use history::{
     HISTORY_PRUNE_EVERY, Sampler, UNWRITTEN_KEPT, WRITE_EVERY, expire_history, sample_history,
 };
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
-pub use live::{FEED_CAPACITY, Feed, Live};
+pub use live::{FEED_CAPACITY, Feed, Live, MAX_STREAMS, Opened};
 pub use node::{JOIN_WAIT, join, leave};
 pub use placement::Placement;
 pub use reconciler::{
````

`crates/data-avalanche-cluster/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/live.rs b/crates/data-avalanche-cluster/src/live.rs
index bc6b24f..b0d0f94 100644
--- a/crates/data-avalanche-cluster/src/live.rs
+++ b/crates/data-avalanche-cluster/src/live.rs
@@ -7,10 +7,12 @@ use std::sync::Arc;
 
 use data_avalanche_state::EventDoc;
 use serde_json::Value;
-use tokio::sync::{broadcast, watch};
+use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, watch};
 
 /// The messages a stream may fall behind by before it misses some (and is told so).
 pub const FEED_CAPACITY: usize = 1024;
+/// The streams open on a node at once, its peers' included: each holds a connection.
+pub const MAX_STREAMS: usize = 64;
 
 /// One live message: its kind (the stream's event name), the pipeline it is about, and its data,
 /// which names the node that sent it.
@@ -37,6 +39,16 @@ impl Live {
 pub struct Feed {
     tx: broadcast::Sender<Arc<Live>>,
     closed: Arc<watch::Sender<bool>>,
+    streams: Arc<Semaphore>,
+}
+
+/// A stream open on the node: its messages from now on, the feed's close, and its place among
+/// the [`MAX_STREAMS`], given back when it is dropped.
+#[derive(Debug)]
+pub struct Opened {
+    pub messages: broadcast::Receiver<Arc<Live>>,
+    pub closed: watch::Receiver<bool>,
+    _place: OwnedSemaphorePermit,
 }
 
 impl Default for Feed {
@@ -50,9 +62,20 @@ impl Feed {
         Self {
             tx: broadcast::Sender::new(FEED_CAPACITY),
             closed: Arc::new(watch::Sender::new(false)),
+            streams: Arc::new(Semaphore::new(MAX_STREAMS)),
         }
     }
 
+    /// A new stream, or none while [`MAX_STREAMS`] are open.
+    pub fn open(&self) -> Option<Opened> {
+        let place = Arc::clone(&self.streams).try_acquire_owned().ok()?;
+        Some(Opened {
+            messages: self.tx.subscribe(),
+            closed: self.closed.subscribe(),
+            _place: place,
+        })
+    }
+
     /// Sends `live` to every stream open now: to none, and dropped, when none is.
     pub fn publish(&self, live: Live) {
         if self.followed() {
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index dd19e37..ad68551 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -372,6 +372,8 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     if let Some(p) = publisher {
         p.abort();
     }
+    // An open stream would hold the server's stop, which waits for the open requests.
+    feed.close();
     let _ = stop_http.send(());
     let _ = server.await;
     // The leave's last events (each lease released) are still queued, and the history's last
````

`xtask/src/api.rs`:

````diff
diff --git a/xtask/src/api.rs b/xtask/src/api.rs
index 0e5be66..e3ac885 100644
--- a/xtask/src/api.rs
+++ b/xtask/src/api.rs
@@ -36,6 +36,10 @@ const TAGS: &[(&str, &str)] = &[
         "history",
         "The series the nodes sampled for charts, kept for `globals.console.history.retention`; a pipeline's older range from `globals.console.prometheus` when it is set.",
     ),
+    (
+        "stream",
+        "The cluster's live changes as server-sent events, from any node, without replay: read the current state, then apply what follows.",
+    ),
     ("openapi.json", "The document this page is rendered from."),
 ];
 
````

Then regenerate `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`: `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi::` writes the document (check its diff is the new path, its two parameters and its `text/event-stream` answer), then `cargo xtask docs` writes the site's reference and its copy of the document.

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- stream:: tests::the_routes && cargo test -p data-avalanche-cluster --lib live::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/openapi.rs crates/data-avalanche-api/src/stream.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche-cluster/src/lib.rs crates/data-avalanche-cluster/src/live.rs crates/data-avalanche/src/run.rs docs/public/openapi.json docs/src/content/docs/reference/api.md xtask/src/api.rs
git commit -m "feat(api): GET /v1/stream — the node's live messages as server-sent events, filtered, bounded, ended at the node's stop"
```

---

### Task 4: Following the other nodes (`data-avalanche-api`)

**Files:**
- Create: `crates/data-avalanche-api/src/follow.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/peers.rs`
- Modify: `crates/data-avalanche-api/src/stream.rs`

**Interfaces:**
- Consumes: Task 3's route and `pump`; `Peers` (M13b: its `me` and `http` now `pub(crate)`), `peers::cause` (now
  `pub(crate)`), `TARGET`, `FORWARDED_BY`, `DETAIL_LIMIT`; `nodes::phase`.
- Produces:
  - `Peers::follow(&self, node, api_url, path_and_query, authorization, silence) -> Result<Followed, String>`;
    `Followed::next(&mut self) -> Result<Option<Frame>, String>`; `Frames::{push, next}`, `Frame { kind, data }`
    (crate-private);
  - `MAX_FRAME` (1 MiB), `FOLLOW_SILENCE` (45 s), `FOLLOW_BACKOFF` (1, 2, 5, 10 s) at the crate root;
  - the `members` and `peer` messages; a stream asked with `FORWARDED_BY` follows none.

- [ ] **Step 1: Write the failing tests**

The tests: `a_message_is_read_once_whole_whatever_its_chunks`, `a_message_over_the_cap_is_refused_as_it_is_read`, `a_followed_stream_carries_the_callers_credentials_and_is_dropped_once_silent`, `a_refusal_names_its_code_and_its_reason` (follow); `a_stream_merges_each_other_node_s_followed_with_the_callers_credentials`, `a_peer_that_cant_be_followed_is_named_and_followed_again`, `a_stream_another_node_asked_for_follows_none`, `a_member_that_leaves_is_no_longer_followed_and_the_members_are_sent` (stream, against a fake node-b serving its own stream). The new module holds only its tests for now.

`crates/data-avalanche-api/src/follow.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/follow.rs b/crates/data-avalanche-api/src/follow.rs
new file mode 100644
index 0000000..52bfc29
--- /dev/null
+++ b/crates/data-avalanche-api/src/follow.rs
@@ -0,0 +1,126 @@
+//! Another node's live stream, followed on a caller's behalf (M13f, D-M13f.5): the request
+//! carries the caller's own credentials, names its target, and says which node forwarded it, so
+//! the node it reaches sends its own messages alone and follows none. Its messages are read one
+//! at a time, each at most [`MAX_FRAME`] bytes.
+
+#[cfg(test)]
+mod tests {
+    use axum::Router;
+    use axum::http::HeaderMap;
+    use axum::response::sse::{Event, Sse};
+    use axum::routing::get;
+    use futures_util::stream;
+
+    use super::*;
+
+    fn frame(kind: &str, data: &str) -> Frame {
+        Frame {
+            kind: kind.into(),
+            data: data.into(),
+        }
+    }
+
+    #[test]
+    fn a_message_is_read_once_whole_whatever_its_chunks() {
+        let mut f = Frames::default();
+        f.push(b": keep-alive\n\nevent: fig");
+        assert_eq!(f.next(), Ok(None));
+        f.push(b"ures\ndata: {\"n\":1}\n");
+        assert_eq!(f.next(), Ok(None));
+        f.push(b"\nevent: node\r\ndata: a\r\ndata: b\r\n\nid: 4\ndata: x\n\n");
+        assert_eq!(f.next(), Ok(Some(frame("figures", "{\"n\":1}"))));
+        assert_eq!(f.next(), Ok(Some(frame("node", "a\nb"))));
+        assert_eq!(f.next(), Ok(None), "a message without a kind is skipped");
+    }
+
+    #[test]
+    fn a_message_over_the_cap_is_refused_as_it_is_read() {
+        let mut f = Frames::default();
+        f.push(&vec![b'x'; MAX_FRAME + 1]);
+        assert_eq!(
+            f.next(),
+            Err(format!("a message over {MAX_FRAME} bytes")),
+            "before its end comes"
+        );
+        let mut f = Frames::default();
+        f.push(b"event: x\ndata: \xff\n\n");
+        assert_eq!(f.next(), Err("a message that isn't UTF-8".into()));
+    }
+
+    /// A node whose stream sends `hello` and then nothing, keeping what each request carried.
+    async fn quiet_node() -> (String, std::sync::Arc<std::sync::Mutex<Vec<HeaderMap>>>) {
+        let seen = std::sync::Arc::<std::sync::Mutex<Vec<HeaderMap>>>::default();
+        let kept = std::sync::Arc::clone(&seen);
+        let app = Router::new().route(
+            "/v1/stream",
+            get(move |headers: HeaderMap| {
+                kept.lock().unwrap().push(headers);
+                async move {
+                    let hello = Event::default().event("hello").data("{}");
+                    let events = stream::iter([Ok::<_, std::convert::Infallible>(hello)])
+                        .chain(stream::pending());
+                    Sse::new(events)
+                }
+            }),
+        );
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let url = format!("http://{}", listener.local_addr().unwrap());
+        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
+        (url, seen)
+    }
+
+    use futures_util::StreamExt;
+
+    #[tokio::test]
+    async fn a_followed_stream_carries_the_callers_credentials_and_is_dropped_once_silent() {
+        let (url, seen) = quiet_node().await;
+        let peers = Peers::new("node-a", None).unwrap();
+        let silence = Duration::from_millis(300);
+        let mut f = peers
+            .follow(
+                "node-b",
+                &url,
+                "/v1/stream?kind=node",
+                Some(HeaderValue::from_static("Basic dmlld2VyOnZpZXdlcg==")),
+                silence,
+            )
+            .await
+            .unwrap();
+        assert_eq!(f.next().await, Ok(Some(frame("hello", "{}"))));
+        assert_eq!(
+            f.next().await,
+            Err(format!("it sent nothing for {silence:?}"))
+        );
+        let h = &seen.lock().unwrap()[0];
+        assert_eq!(h[TARGET], "node-b");
+        assert_eq!(h[FORWARDED_BY], "node-a");
+        assert_eq!(h[header::AUTHORIZATION], "Basic dmlld2VyOnZpZXdlcg==");
+    }
+
+    #[tokio::test]
+    async fn a_refusal_names_its_code_and_its_reason() {
+        let (url, _log) = crate::test_support::fake_answering(
+            Duration::ZERO,
+            reqwest::StatusCode::NOT_FOUND,
+            serde_json::json!({ "error": "no pipeline \"x\"" }),
+        )
+        .await;
+        let peers = Peers::new("node-a", None).unwrap();
+        let err = peers
+            .follow("node-b", &url, "/v1/stream", None, FOLLOW_SILENCE)
+            .await
+            .unwrap_err();
+        assert_eq!(err, "it answered 404: no pipeline \"x\"");
+        let err = peers
+            .follow(
+                "node-b",
+                "http://127.0.0.1:9",
+                "/v1/stream",
+                None,
+                FOLLOW_SILENCE,
+            )
+            .await
+            .unwrap_err();
+        assert!(!err.is_empty(), "the connection's own cause");
+    }
+}
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index da796ce..79f6402 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -13,6 +13,7 @@ mod auth;
 mod cluster;
 mod config;
 mod events;
+mod follow;
 mod generations;
 mod history;
 mod limit;
````

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
index 9efb1f2..03ad9f1 100644
--- a/crates/data-avalanche-api/src/stream.rs
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -359,4 +359,173 @@ mod tests {
             tokio::time::sleep(Duration::from_millis(20)).await;
         }
     }
+
+    /// A fake node-b whose stream says `hello`, then sends what the test sends it; what each
+    /// request carried is kept.
+    struct NodeB {
+        url: String,
+        send: broadcast::Sender<(String, Value)>,
+        seen: std::sync::Arc<std::sync::Mutex<Vec<(String, HeaderMap)>>>,
+    }
+
+    async fn node_b() -> NodeB {
+        use axum::http::Uri;
+        use futures_util::StreamExt;
+        let send = broadcast::Sender::<(String, Value)>::new(16);
+        let seen = std::sync::Arc::<std::sync::Mutex<Vec<(String, HeaderMap)>>>::default();
+        let (sender, kept) = (send.clone(), std::sync::Arc::clone(&seen));
+        let app = axum::Router::new().route(
+            "/v1/stream",
+            axum::routing::get(move |uri: Uri, headers: HeaderMap| {
+                kept.lock().unwrap().push((uri.to_string(), headers));
+                let rx = sender.subscribe();
+                async move {
+                    let hello = event("hello", &json!({ "node": "node-b" }));
+                    let rest = futures_util::stream::unfold(rx, |mut rx| async move {
+                        let (kind, data) = rx.recv().await.ok()?;
+                        Some((event(&kind, &data), rx))
+                    });
+                    Sse::new(
+                        futures_util::stream::iter([hello])
+                            .chain(rest)
+                            .map(Ok::<_, Infallible>),
+                    )
+                }
+            }),
+        );
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let url = format!("http://{}", listener.local_addr().unwrap());
+        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
+        NodeB { url, send, seen }
+    }
+
+    #[tokio::test]
+    async fn a_stream_merges_each_other_node_s_followed_with_the_callers_credentials() {
+        let b = node_b().await;
+        let (addr, lone, _dir) = served_with(&b.url, crate::test_support::open(), |_| {}).await;
+        let mut s = stream(
+            addr,
+            "/v1/stream?kind=figures,node",
+            &[("authorization", "Basic dmlld2VyOnZpZXdlcg==")],
+        )
+        .await;
+        let (kind, hello) = s.next().await;
+        assert_eq!(
+            (kind.as_str(), &hello["follows"]),
+            ("hello", &json!(["node-b"]))
+        );
+        assert_eq!(
+            s.next().await,
+            (
+                "peer".into(),
+                json!({ "node": "node-a", "peer": "node-b", "state": "following" })
+            )
+        );
+        b.send
+            .send((
+                "figures".into(),
+                json!({ "node": "node-b", "id": "orders" }),
+            ))
+            .unwrap();
+        assert_eq!(
+            s.next().await,
+            (
+                "figures".into(),
+                json!({ "node": "node-b", "id": "orders" })
+            ),
+            "as node-b sent it"
+        );
+        lone.node.feed.publish(live("node", None, 1));
+        assert_eq!(
+            s.next().await.1,
+            live("node", None, 1).data,
+            "and node-a's own"
+        );
+        let (uri, h) = b.seen.lock().unwrap()[0].clone();
+        assert_eq!(
+            uri, "/v1/stream?kind=figures,node",
+            "asked as the caller asked"
+        );
+        assert_eq!(h[crate::peers::TARGET], "node-b");
+        assert_eq!(h[FORWARDED_BY], "node-a");
+        assert_eq!(h[header::AUTHORIZATION], "Basic dmlld2VyOnZpZXdlcg==");
+    }
+
+    #[tokio::test]
+    async fn a_peer_that_cant_be_followed_is_named_and_followed_again() {
+        let (b_url, log) = crate::test_support::fake_answering(
+            Duration::ZERO,
+            StatusCode::NOT_FOUND,
+            json!({ "error": "no pipeline \"orders\" in the config generation this node runs" }),
+        )
+        .await;
+        let (addr, _lone, _dir) = served_with(&b_url, crate::test_support::open(), |_| {}).await;
+        let mut s = stream(addr, "/v1/stream", &[]).await;
+        s.next().await;
+        let unreachable = json!({
+            "node": "node-a",
+            "peer": "node-b",
+            "state": "unreachable",
+            "error": "it answered 404: no pipeline \"orders\" in the config generation this node runs",
+        });
+        assert_eq!(s.next().await, ("peer".into(), unreachable.clone()));
+        // After the first wait, FOLLOW_BACKOFF[0].
+        assert_eq!(s.next().await, ("peer".into(), unreachable));
+        assert_eq!(log.lock().unwrap().len(), 2);
+    }
+
+    #[tokio::test]
+    async fn a_stream_another_node_asked_for_follows_none() {
+        let b = node_b().await;
+        let (addr, lone, _dir) = served_with(&b.url, crate::test_support::open(), |_| {}).await;
+        let mut s = stream(addr, "/v1/stream", &[(FORWARDED_BY, "node-c")]).await;
+        let (_, hello) = s.next().await;
+        assert_eq!(hello["follows"], json!([]));
+        lone.node.feed.publish(live("node", None, 1));
+        assert_eq!(s.next().await.1["n"], 1);
+        tokio::time::sleep(Duration::from_millis(200)).await;
+        assert!(b.seen.lock().unwrap().is_empty(), "node-b never asked");
+    }
+
+    #[tokio::test]
+    async fn a_member_that_leaves_is_no_longer_followed_and_the_members_are_sent() {
+        use data_avalanche_cluster::Phase;
+
+        use crate::test_support::member;
+        let b = node_b().await;
+        let both = Members {
+            me: "node-a".into(),
+            nodes: [
+                ("node-a".to_owned(), member("", Phase::Active)),
+                ("node-b".to_owned(), member(&b.url, Phase::Active)),
+            ]
+            .into(),
+        };
+        let (view, members) = watch::channel(both.clone());
+        let (addr, _lone, _dir) = served_with("", crate::test_support::open(), |n| {
+            n.members = members;
+        })
+        .await;
+        let mut s = stream(addr, "/v1/stream", &[]).await;
+        s.next().await;
+        assert_eq!(s.next().await.1["state"], "following");
+        let mut alone = both.clone();
+        alone.nodes.remove("node-b");
+        view.send_replace(alone);
+        assert_eq!(
+            s.next().await,
+            (
+                "members".into(),
+                json!({ "node": "node-a", "members": [{ "node": "node-a", "phase": "active" }] })
+            )
+        );
+        let within = tokio::time::Instant::now() + Duration::from_secs(3);
+        while b.send.receiver_count() > 0 {
+            assert!(
+                tokio::time::Instant::now() < within,
+                "node-b still followed"
+            );
+            tokio::time::sleep(Duration::from_millis(20)).await;
+        }
+    }
 }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- follow:: stream::`
Expected: the API's tests don't compile: the new module's find no `Frames`, `Frame`, `MAX_FRAME`, `FOLLOW_SILENCE`, `Peers`, `TARGET` or `FORWARDED_BY` (nor the other imports the code above them brings), and the stream's find no `Members`, `HeaderMap`, `watch` or `Duration` in scope.

- [ ] **Step 3: Implement**

`pump` takes the stream's `Follow` (the node, the path and query, the caller's `Authorization`) when the request carries no `FORWARDED_BY`: it sends `hello` with the members it will follow, then follows them (`Following::reconcile`, again on each change of the members, with a `members` message when the names or phases changed).

`crates/data-avalanche-api/src/follow.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/follow.rs b/crates/data-avalanche-api/src/follow.rs
index 52bfc29..50a533e 100644
--- a/crates/data-avalanche-api/src/follow.rs
+++ b/crates/data-avalanche-api/src/follow.rs
@@ -3,6 +3,143 @@
 //! the node it reaches sends its own messages alone and follows none. Its messages are read one
 //! at a time, each at most [`MAX_FRAME`] bytes.
 
+use std::time::Duration;
+
+use axum::http::{HeaderValue, header};
+
+use crate::peers::{DETAIL_LIMIT, FORWARDED_BY, Peers, TARGET};
+
+/// The largest message read from a followed stream.
+pub const MAX_FRAME: usize = 1 << 20;
+/// How long a followed stream may send nothing, its keep-alive included, before it is dropped.
+pub const FOLLOW_SILENCE: Duration = Duration::from_secs(45);
+
+/// One message: its kind (`event:`) and its data, its `data:` lines joined.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct Frame {
+    pub kind: String,
+    pub data: String,
+}
+
+/// The messages of a stream's bytes, as they come.
+#[derive(Debug, Default)]
+pub(crate) struct Frames {
+    buffer: Vec<u8>,
+}
+
+impl Frames {
+    /// Adds `bytes` read.
+    pub(crate) fn push(&mut self, bytes: &[u8]) {
+        self.buffer.extend_from_slice(bytes);
+    }
+
+    /// The next whole message, comments and messages without a kind skipped; an error when the
+    /// one being read is over [`MAX_FRAME`] or isn't UTF-8.
+    pub(crate) fn next(&mut self) -> Result<Option<Frame>, String> {
+        loop {
+            let Some(end) = self.buffer.windows(2).position(|w| w == b"\n\n") else {
+                if self.buffer.len() > MAX_FRAME {
+                    return Err(format!("a message over {MAX_FRAME} bytes"));
+                }
+                return Ok(None);
+            };
+            if end > MAX_FRAME {
+                return Err(format!("a message over {MAX_FRAME} bytes"));
+            }
+            let raw: Vec<u8> = self.buffer.drain(..end + 2).collect();
+            let text = std::str::from_utf8(&raw).map_err(|_| "a message that isn't UTF-8")?;
+            let (mut kind, mut data) = (None, Vec::new());
+            // A line ends at a CR, an LF or both: none is left in a kind or a datum.
+            for line in text.split(['\r', '\n']) {
+                if let Some(k) = line.strip_prefix("event:") {
+                    kind = Some(k.trim().to_owned());
+                } else if let Some(d) = line.strip_prefix("data:") {
+                    data.push(d.strip_prefix(' ').unwrap_or(d));
+                }
+            }
+            if let Some(kind) = kind.filter(|k| !k.is_empty()) {
+                return Ok(Some(Frame {
+                    kind,
+                    data: data.join("\n"),
+                }));
+            }
+        }
+    }
+}
+
+/// A node's stream, followed.
+#[derive(Debug)]
+pub(crate) struct Followed {
+    answer: reqwest::Response,
+    frames: Frames,
+    silence: Duration,
+}
+
+impl Followed {
+    /// The next message; `Ok(None)` when the stream ended, an error when it failed, sent a
+    /// message over [`MAX_FRAME`], or sent nothing for its silence.
+    pub(crate) async fn next(&mut self) -> Result<Option<Frame>, String> {
+        loop {
+            if let Some(f) = self.frames.next()? {
+                return Ok(Some(f));
+            }
+            match tokio::time::timeout(self.silence, self.answer.chunk()).await {
+                Err(_) => return Err(format!("it sent nothing for {:?}", self.silence)),
+                Ok(Err(e)) => return Err(crate::peers::cause(&e)),
+                Ok(Ok(None)) => return Ok(None),
+                Ok(Ok(Some(bytes))) => self.frames.push(&bytes),
+            }
+        }
+    }
+}
+
+impl Peers {
+    /// Opens node `node`'s stream at `api_url` + `path_and_query` with the caller's
+    /// `authorization`, answered within [`DETAIL_LIMIT`]; dropped once it sends nothing for
+    /// `silence`. An answer other than `200` is an error naming its code and its own.
+    pub(crate) async fn follow(
+        &self,
+        node: &str,
+        api_url: &str,
+        path_and_query: &str,
+        authorization: Option<HeaderValue>,
+        silence: Duration,
+    ) -> Result<Followed, String> {
+        let mut req = self
+            .http
+            .get(format!("{api_url}{path_and_query}"))
+            .header(TARGET, node)
+            .header(FORWARDED_BY, &self.me)
+            .header(header::ACCEPT, "text/event-stream");
+        if let Some(a) = authorization {
+            req = req.header(header::AUTHORIZATION, a);
+        }
+        let answer = match tokio::time::timeout(DETAIL_LIMIT, req.send()).await {
+            Err(_) => return Err(format!("it didn't answer within {DETAIL_LIMIT:?}")),
+            Ok(Err(e)) => return Err(crate::peers::cause(&e)),
+            Ok(Ok(a)) => a,
+        };
+        let code = answer.status();
+        if code != reqwest::StatusCode::OK {
+            let said = tokio::time::timeout(DETAIL_LIMIT, answer.bytes())
+                .await
+                .ok()
+                .and_then(Result::ok)
+                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
+                .and_then(|v| v["error"].as_str().map(str::to_owned));
+            return Err(match said {
+                Some(e) => format!("it answered {}: {e}", code.as_u16()),
+                None => format!("it answered {}", code.as_u16()),
+            });
+        }
+        Ok(Followed {
+            answer,
+            frames: Frames::default(),
+            silence,
+        })
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use axum::Router;
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 79f6402..89c654d 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -55,6 +55,7 @@ use tokio::sync::watch;
 pub use actions::{DENIALS_BURST, MAX_ERROR, MAX_NOTE, MAX_TARGET, Recorded, note_of};
 pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER};
 pub use cluster::{Control, Stop};
+pub use follow::{FOLLOW_SILENCE, MAX_FRAME};
 pub use history::{MAX_POINTS, MAX_SPAN};
 pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
 pub use live::FRESH_WITHIN;
@@ -70,7 +71,7 @@ pub use prometheus::{
 pub use publish::{FIGURES_EVERY, STATE_EVERY, publish};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 pub use snapshots::{EndedJobs, READ_AT_ONCE};
-pub use stream::{KEEP_ALIVE, KINDS};
+pub use stream::{FOLLOW_BACKOFF, KEEP_ALIVE, KINDS};
 pub use tables::{FACTS_FOR, FACTS_WITHIN, Lake};
 
 use crate::auth::Denied;
````

`crates/data-avalanche-api/src/peers.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/peers.rs b/crates/data-avalanche-api/src/peers.rs
index c06083c..e5ce7c2 100644
--- a/crates/data-avalanche-api/src/peers.rs
+++ b/crates/data-avalanche-api/src/peers.rs
@@ -34,8 +34,8 @@ pub const MAX_ANSWER: usize = 8 << 20;
 /// The client every forwarded request goes through.
 #[derive(Debug, Clone)]
 pub struct Peers {
-    me: String,
-    http: reqwest::Client,
+    pub(crate) me: String,
+    pub(crate) http: reqwest::Client,
 }
 
 /// Why the peers' client can't be built.
@@ -223,7 +223,7 @@ impl Peers {
 }
 
 /// The innermost cause: reqwest's own message says only "error sending request".
-fn cause(e: &(dyn std::error::Error + 'static)) -> String {
+pub(crate) fn cause(e: &(dyn std::error::Error + 'static)) -> String {
     let mut inner = e;
     while let Some(next) = inner.source() {
         inner = next;
````

`crates/data-avalanche-api/src/stream.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/stream.rs b/crates/data-avalanche-api/src/stream.rs
index 03ad9f1..e098873 100644
--- a/crates/data-avalanche-api/src/stream.rs
+++ b/crates/data-avalanche-api/src/stream.rs
@@ -2,22 +2,32 @@
 //! A stream starts with `hello`, then sends each change as it happens, without replay: a page
 //! reads the current state, then applies what follows. A stream that fell too far behind is sent
 //! `reset`, and reads again.
+//!
+//! The node a page asks merges into its stream each other member's, followed with the caller's
+//! own credentials (`peer` says when one is followed and when it can't be, `members` when the
+//! members change); a stream it is asked for by another node is its own alone.
 
 use std::collections::BTreeSet;
 use std::convert::Infallible;
 use std::time::Duration;
 
+use std::collections::BTreeMap;
+
 use axum::extract::{RawQuery, State};
-use axum::http::{HeaderValue, StatusCode, header};
+use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
 use axum::response::sse::{Event, KeepAlive, Sse};
 use axum::response::{IntoResponse, Response};
 use chrono::Utc;
-use data_avalanche_cluster::{Live, MAX_STREAMS, Opened};
+use data_avalanche_cluster::{Live, MAX_STREAMS, Members, Opened};
 use data_avalanche_config::ResolvedConfig;
 use serde_json::{Value, json};
-use tokio::sync::{broadcast, mpsc};
+use tokio::sync::{broadcast, mpsc, watch};
+use tokio::task::{AbortHandle, JoinSet};
 
-use crate::{Api, Failure, starting};
+use crate::follow::FOLLOW_SILENCE;
+use crate::nodes::phase;
+use crate::peers::FORWARDED_BY;
+use crate::{Api, Failure, Node, starting};
 
 /// What `GET /v1/stream` takes, each once.
 pub const PARAMETERS: &[(&str, &str)] = &[
@@ -38,6 +48,15 @@ pub const KINDS: &[&str] = &["pipeline", "figures", "node", "samples", "event"];
 /// How often a stream with nothing to send sends a comment, under the server's idle timeout.
 pub const KEEP_ALIVE: Duration = Duration::from_secs(15);
 
+/// How long a peer's stream that ended or failed waits before it is followed again: each wait in
+/// turn, the last one from then on, until it is followed.
+pub const FOLLOW_BACKOFF: [Duration; 4] = [
+    Duration::from_secs(1),
+    Duration::from_secs(2),
+    Duration::from_secs(5),
+    Duration::from_secs(10),
+];
+
 /// How long a client refused for [`MAX_STREAMS`] waits before it tries again, in seconds.
 const RETRY_AFTER: &str = "5";
 
@@ -131,14 +150,18 @@ fn event(kind: &str, data: &Value) -> Event {
 }
 
 /// `GET /v1/stream`.
-pub(crate) async fn open(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+pub(crate) async fn open(
+    State(api): State<Api>,
+    headers: HeaderMap,
+    RawQuery(raw): RawQuery,
+) -> Response {
     let Some(node) = api.node.get() else {
         return starting();
     };
-    let params: Vec<(String, String)> =
-        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
-            .into_owned()
-            .collect();
+    let raw = raw.unwrap_or_default();
+    let params: Vec<(String, String)> = url::form_urlencoded::parse(raw.as_bytes())
+        .into_owned()
+        .collect();
     let config = node.coordinator.config().await;
     let filter = match filter(&params, &config) {
         Ok(f) => f,
@@ -155,8 +178,18 @@ pub(crate) async fn open(State(api): State<Api>, RawQuery(raw): RawQuery) -> Res
         return r;
     };
     let me = node.members.borrow().me.clone();
+    // Asked by another node: this node's own messages alone, never followed further.
+    let follow = (!headers.contains_key(FORWARDED_BY)).then(|| Follow {
+        node: node.clone(),
+        path: if raw.is_empty() {
+            "/v1/stream".to_owned()
+        } else {
+            format!("/v1/stream?{raw}")
+        },
+        authorization: headers.get(header::AUTHORIZATION).cloned(),
+    });
     let (tx, rx) = mpsc::channel(BUFFER);
-    tokio::spawn(pump(me, filter, opened, tx));
+    tokio::spawn(pump(me, filter, opened, tx, follow));
     let events = futures_util::stream::unfold(rx, |mut rx| async move {
         rx.recv().await.map(|e| (Ok::<_, Infallible>(e), rx))
     });
@@ -177,18 +210,174 @@ async fn deliver(tx: &mpsc::Sender<Event>, opened: &mut Opened, e: Event) -> boo
     }
 }
 
-/// Sends `hello`, then this node's messages that pass `filter`, until the client goes or the
-/// node stops.
-async fn pump(me: String, filter: Filter, mut opened: Opened, tx: mpsc::Sender<Event>) {
-    let hello = json!({ "node": me, "at": Utc::now(), "follows": [] });
+/// What a stream follows its peers with: the node's members and client, the stream's own path
+/// and query, and the caller's credentials.
+struct Follow {
+    node: Node,
+    path: String,
+    authorization: Option<HeaderValue>,
+}
+
+/// Each other member with an API address, by name.
+fn peers_of(view: &Members) -> BTreeMap<String, String> {
+    view.nodes
+        .iter()
+        .filter(|(name, m)| **name != view.me && !m.meta.api_url.is_empty())
+        .map(|(name, m)| (name.clone(), m.meta.api_url.clone()))
+        .collect()
+}
+
+/// The members as `members` lists them.
+fn listed(view: &Members) -> Value {
+    view.nodes
+        .iter()
+        .map(|(name, m)| json!({ "node": name, "phase": phase(m.meta.phase) }))
+        .collect()
+}
+
+/// The peers a stream follows, each by its own task, all ended with the stream.
+#[derive(Default)]
+struct Following {
+    tasks: JoinSet<()>,
+    nodes: BTreeMap<String, (String, AbortHandle)>,
+}
+
+impl Following {
+    /// Follows each peer of `view` not yet followed at its address, and stops following the
+    /// others.
+    fn reconcile(&mut self, f: &Follow, view: &Members, tx: &mpsc::Sender<Event>) {
+        let want = peers_of(view);
+        self.nodes.retain(|name, (url, task)| {
+            let keep = want.get(name) == Some(url);
+            if !keep {
+                task.abort();
+            }
+            keep
+        });
+        for (name, url) in want {
+            if !self.nodes.contains_key(&name) {
+                let task = self.tasks.spawn(follow_peer(
+                    f.node.peers.clone(),
+                    view.me.clone(),
+                    name.clone(),
+                    url.clone(),
+                    f.path.clone(),
+                    f.authorization.clone(),
+                    tx.clone(),
+                ));
+                self.nodes.insert(name, (url, task));
+            }
+        }
+        while self.tasks.try_join_next().is_some() {}
+    }
+}
+
+/// Passes `peer`'s messages on, its `hello` as `peer` following; when its stream can't be
+/// opened, ends or fails, says so as `peer` unreachable and follows it again after
+/// [`FOLLOW_BACKOFF`]. Ends once the stream's client goes.
+async fn follow_peer(
+    peers: crate::Peers,
+    me: String,
+    peer: String,
+    url: String,
+    path: String,
+    authorization: Option<HeaderValue>,
+    tx: mpsc::Sender<Event>,
+) {
+    let mut failed = 0;
+    loop {
+        let error = match peers
+            .follow(&peer, &url, &path, authorization.clone(), FOLLOW_SILENCE)
+            .await
+        {
+            Err(e) => e,
+            Ok(mut followed) => loop {
+                let e = match followed.next().await {
+                    Ok(Some(f)) if f.kind == "hello" => {
+                        failed = 0;
+                        event(
+                            "peer",
+                            &json!({ "node": me, "peer": peer, "state": "following" }),
+                        )
+                    }
+                    Ok(Some(f)) => Event::default().event(f.kind).data(f.data),
+                    Ok(None) => break "its stream ended".to_owned(),
+                    Err(e) => break e,
+                };
+                if tx.send(e).await.is_err() {
+                    return;
+                }
+            },
+        };
+        let unreachable =
+            json!({ "node": me, "peer": peer, "state": "unreachable", "error": error });
+        if tx.send(event("peer", &unreachable)).await.is_err() {
+            return;
+        }
+        let wait = FOLLOW_BACKOFF[failed.min(FOLLOW_BACKOFF.len() - 1)];
+        failed += 1;
+        tokio::select! {
+            () = tx.closed() => return,
+            () = tokio::time::sleep(wait) => {}
+        }
+    }
+}
+
+/// The next change of the members, when the stream follows them.
+async fn changed(members: &mut Option<watch::Receiver<Members>>) -> Option<()> {
+    match members {
+        Some(m) => m.changed().await.ok(),
+        None => std::future::pending().await,
+    }
+}
+
+/// Sends `hello`, then this node's messages that pass `filter` and, with `follow`, each peer's,
+/// until the client goes or the node stops.
+async fn pump(
+    me: String,
+    filter: Filter,
+    mut opened: Opened,
+    tx: mpsc::Sender<Event>,
+    follow: Option<Follow>,
+) {
+    let mut members = follow.as_ref().map(|f| f.node.members.clone());
+    let view = members.as_mut().map(|m| m.borrow_and_update().clone());
+    let follows: Vec<String> = view
+        .as_ref()
+        .map(|v| peers_of(v).into_keys().collect())
+        .unwrap_or_default();
+    let hello = json!({ "node": me, "at": Utc::now(), "follows": follows });
     if !deliver(&tx, &mut opened, event("hello", &hello)).await {
         return;
     }
+    let mut following = Following::default();
+    let mut shown = view.as_ref().map(listed);
+    if let (Some(f), Some(view)) = (&follow, &view) {
+        following.reconcile(f, view, &tx);
+    }
     loop {
         let next = tokio::select! {
             () = tx.closed() => return,
             _ = opened.closed.wait_for(|c| *c) => return,
-            r = opened.messages.recv() => r,
+            Some(()) = changed(&mut members) => None,
+            r = opened.messages.recv() => Some(r),
+        };
+        let Some(next) = next else {
+            let (Some(f), Some(m)) = (&follow, &mut members) else {
+                continue;
+            };
+            let view = m.borrow_and_update().clone();
+            following.reconcile(f, &view, &tx);
+            let now = listed(&view);
+            if shown.as_ref() == Some(&now) {
+                continue;
+            }
+            shown = Some(now.clone());
+            let e = event("members", &json!({ "node": me, "members": now }));
+            if !deliver(&tx, &mut opened, e).await {
+                return;
+            }
+            continue;
         };
         let e = match next {
             Ok(live) if filter.passes(&live) => event(live.kind, &live.data),
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- follow:: stream::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/follow.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/peers.rs crates/data-avalanche-api/src/stream.rs
git commit -m "feat(api): the live stream merges every other node's, followed with the caller's own credentials"
```

---

### Task 5: The site and the research doc

**Files:**
- Modify: `doc/research/Admin_Console.md`
- Modify: `docs/src/content/docs/deployment/observability.md`

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: the observability page's "The live stream"; `Admin_Console.md` §8.1.

- [ ] **Step 1: Write the pages**

The observability page says what the stream sends, from which node and when, its filters and limits, and what a proxy in front of the nodes needs; the research doc records Oleksii's three choices and the shapes.

`doc/research/Admin_Console.md`:

````diff
diff --git a/doc/research/Admin_Console.md b/doc/research/Admin_Console.md
index 265a52e..e9b142e 100644
--- a/doc/research/Admin_Console.md
+++ b/doc/research/Admin_Console.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-14
+updated: 2026-11-19
 domain: Architecture
 tags: [admin-console, web-ui, management-api, auth, sessions, audit, metrics-history, prometheus, v1.1]
 ---
@@ -180,6 +180,33 @@ expire.
 - **Replication first.** The console's work never slows the pipelines: lower priority, its own
   request limits.
 
+### 8.1 The live stream (M13f)
+
+`GET /v1/stream` (viewer) is the push above. Oleksii decided three things on 2026-11-19: it sends
+**values as they change** (not "something changed, read again"), the **figures every 5 s**, and
+**no replay** (a page reads the current state, then applies what follows). The rest is Claude's
+call.
+
+- **Each node publishes its own, from memory.** A pipeline's `pipeline` state (looked at every
+  second, sent on a change of state, error or lease term; `standby` once when the node stops
+  running it), its `figures` (the live answer, the halts' start times left out: they are in
+  storage), the node's own `node` figures, each chart `samples` as taken, each `event` once
+  written. Nothing is built while no stream is open on the node; a stream that opens gets every
+  state again within a second.
+- **The node asked merges the others.** It follows each other member with an API address, with
+  the caller's own credentials and the target and forwarded-by headers of §10's forwarding, and
+  passes their messages on as sent; a stream asked for by another node is that node's own alone.
+  `peer` says when a member is followed (`following`) and when it can't be (`unreachable`, with
+  why), retried after 1, 2, 5, then every 10 s; `members` says when the members or their phases
+  change. Every message's data names the node that sent it.
+- **Bounded.** 64 streams a node, its peers' included (`503` with `Retry-After` past that); a
+  stream more than 1024 messages behind is sent `reset` and reads again; a followed stream silent
+  for 45 s (three keep-alives) is dropped and followed again; a followed message over 1 MiB ends
+  that follow. A slow page holds only its own stream.
+- **It never holds a node's stop.** A node ends its streams before its server's graceful stop.
+- **Filters:** `pipeline` (ids) and `kind`; the control messages (`hello`, `members`, `peer`,
+  `reset`) are always sent. The filters go to the followed nodes with the request.
+
 ## 9. Sign-in and sessions
 
 One sign-in page; what follows depends on `globals.api.auth`:
@@ -293,9 +320,9 @@ the runbooks pointing at the console where it does the job.
 
 The attention thresholds; the event and history layouts in object storage; sampling resolution;
 the exact paths of the new `/v1/` endpoints (they go into the spec's endpoint table with the first
-server plan) and of the routes under `/ui/`; the SSE message shapes; how the milestones
-split this (likely: server foundations — events, attribution, history, new endpoints; sign-in and
-sessions; the console shell and Overview; the detail pages and actions).
+server plan) and of the routes under `/ui/`; the SSE message shapes (settled in §8.1); how the
+milestones split this (likely: server foundations — events, attribution, history, new endpoints;
+sign-in and sessions; the console shell and Overview; the detail pages and actions).
 
 ## 14. Delivery
 
````

`docs/src/content/docs/deployment/observability.md`:

````diff
diff --git a/docs/src/content/docs/deployment/observability.md b/docs/src/content/docs/deployment/observability.md
index a3e25e5..f7df251 100644
--- a/docs/src/content/docs/deployment/observability.md
+++ b/docs/src/content/docs/deployment/observability.md
@@ -114,6 +114,43 @@ what failed); or `not_in_prometheus` for a table's or a node's series, whose met
 label. A series without `older` asked nothing older than the history kept. Without the setting,
 the history kept is the whole answer, and nothing is marked.
 
+## The live stream
+
+`GET /v1/stream` sends the cluster's changes as they happen, as server-sent events, from any
+node: the node asked follows every other member's stream with the caller's own credentials and
+merges them into one. It sends no replay: read the current state (`GET /v1/pipelines`,
+`/v1/cluster/status`, `/v1/history`), then apply what follows.
+
+```sh
+curl -N -u alice:… 'https://avalanche.example.com:8080/v1/stream?pipeline=orders-pg'
+```
+
+Each message's data is a JSON object; `node` names the node that sent it.
+
+| Message | When | Data |
+|---|---|---|
+| `hello` | first | `at`, and `follows`: the other members followed |
+| `pipeline` | a pipeline's state, error or lease term changes on the node that runs it (looked at every second); `standby` once when it stops running it | `pipeline`, `actual`, `error`, `owner_epoch` |
+| `figures` | every 5 s, for each pipeline a node runs | what `GET /v1/pipelines/{id}/live` answers, without the halts' start times |
+| `node` | every 5 s, from each node | `phase`, `process`, `gossip`, and the `pipelines` it runs |
+| `samples` | each chart sample, as it is taken | `scope` (`pipeline` or `node`), `pipeline`, `at` (unix seconds), `series`, and a pipeline's `tables` |
+| `event` | each event, once written | the event as `GET /v1/events` lists it |
+| `members` | the members or their phases change | `members`: each one's `node` and `phase` |
+| `peer` | another node's stream is followed, or can't be (it is followed again after 1, 2, 5, then every 10 s) | `peer`, `state` (`following` or `unreachable`), `error` |
+| `reset` | the stream fell more than 1024 messages behind and missed some | `reason`, `missed`: read the current state again |
+
+`pipeline` takes a pipeline's id, or several comma-separated, and sends only the messages about
+them. `kind` takes the kinds wanted, comma-separated, of `pipeline`, `figures`, `node`,
+`samples` and `event`. `hello`, `members`, `peer` and `reset` are always sent. A pipeline the
+generation doesn't have is `404`; a kind that isn't one, `400`.
+
+A node holds at most 64 streams at once, those it serves its peers included; past that it answers
+`503` with `Retry-After`. A stream with nothing to send sends a comment every 15 s, so a proxy in
+front of the nodes must wait longer than that before it closes one, and must not buffer the
+answer (the node sends `X-Accel-Buffering: no` for nginx). A node builds no message while no stream
+is open on it, and ends its streams when it stops. The caller's credentials are checked when the
+stream opens.
+
 ## The log
 
 To stderr, as `globals.log.format`: `json` (one object a line), `compact` or `pretty`; at
````

- [ ] **Step 2: Build the site**

Run: `cd docs && npm ci && npm run build`
Expected: the build ends with "All internal links are valid" and 31 pages.

- [ ] **Step 3: Run the gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 4: Commit**

```bash
git add doc/research/Admin_Console.md docs/src/content/docs/deployment/observability.md
git commit -m "docs: the live stream — its messages, filters and limits; Admin_Console §8.1"
```

---

### Task 6: The scenario on three nodes (`data-avalanche` E2E)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/console_stream.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the E2E harness's `Cluster` (M8); Tasks 1–4 through the binary.
- Produces: `Proc::stream(&self, path) -> Option<Sse>`, `Sse::{next, until}` (harness);
  `console_stream::the_live_stream_merges_every_node_s_and_follows_a_failover`.

- [ ] **Step 1: Write the scenario**

Through a node that doesn't hold the test's pipeline: `hello` naming the two others, the holder's state and figures, a pause's event and the holder's `paused` then `running`; the holder killed: `peer` unreachable, the next holder's state and the members without it, in any order; then the nodes stop with the stream open, and it ends.

`crates/data-avalanche/tests/e2e/console_stream.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_stream.rs b/crates/data-avalanche/tests/e2e/console_stream.rs
new file mode 100644
index 0000000..fadfaf8
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/console_stream.rs
@@ -0,0 +1,120 @@
+//! The live stream (Admin_Console §8.1; M13f) on three nodes: a stream opened on a node that
+//! doesn't run the test's pipeline follows the two others, so it shows the holder's state and
+//! figures as they change, and an action's event; the holder killed, it names it unreachable,
+//! then shows the next holder's state and the members without it; an open stream doesn't hold
+//! the nodes' stop.
+
+use std::time::{Duration, Instant};
+
+use data_avalanche_testkit::require_it;
+use serde_json::json;
+
+use crate::harness::Doc;
+use crate::nodes::Cluster;
+
+#[tokio::test]
+async fn the_live_stream_merges_every_node_s_and_follows_a_failover() {
+    require_it!();
+    let mut c = Cluster::new("console_stream").await;
+    c.start_all().await;
+    let test = c.e.test.clone();
+    c.e.insert_series("items", 5, Doc::Row).await;
+    c.landed().await;
+    let holder = c.holders().await[&test].0.clone();
+    let other = ["n1", "n2", "n3"]
+        .into_iter()
+        .find(|n| *n != holder)
+        .expect("another node")
+        .to_owned();
+    let mut s = c
+        .proc(&other)
+        .stream(&format!("/v1/stream?pipeline={test}"))
+        .await
+        .expect("the stream opens");
+    let (kind, hello) = s.next().await.expect("hello");
+    assert_eq!(kind, "hello");
+    assert_eq!(hello["node"], other.as_str(), "{hello}");
+    let peers: Vec<&str> = ["n1", "n2", "n3"]
+        .into_iter()
+        .filter(|n| *n != other)
+        .collect();
+    assert_eq!(hello["follows"], json!(peers), "{hello}");
+    let minute = Duration::from_secs(60);
+
+    // The holder's state and figures, through the node asked.
+    let is_holder = |v: &serde_json::Value| v["node"] == holder.as_str();
+    let state = s
+        .until("the holder's state", "pipeline", minute, |v| is_holder(v))
+        .await;
+    assert_eq!(
+        (&state["pipeline"], &state["actual"]),
+        (&json!(test), &json!("running"))
+    );
+    let figures = s
+        .until("the holder's figures", "figures", minute, |v| is_holder(v))
+        .await;
+    assert_eq!(
+        (&figures["id"], &figures["actual"]),
+        (&json!(test), &json!("running"))
+    );
+
+    // A pause: its event, then the holder's new state; and back.
+    let o = c.proc(&other);
+    assert_eq!(o.ask(&test, "pause").await.0, 202);
+    s.until("the pause's event", "event", minute, |v| {
+        v["kind"] == "action.pipeline.pause" && v["pipeline"] == test.as_str()
+    })
+    .await;
+    s.until("the holder paused", "pipeline", minute, |v| {
+        is_holder(v) && v["actual"] == "paused"
+    })
+    .await;
+    assert_eq!(o.ask(&test, "resume").await.0, 202);
+    s.until("the holder running again", "pipeline", minute, |v| {
+        is_holder(v) && v["actual"] == "running"
+    })
+    .await;
+
+    // The holder killed: named unreachable; the next holder's state; the members without it, in
+    // whatever order they come.
+    let i = c.index(&holder);
+    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
+    let (mut unreachable, mut next, mut members) = (false, None, false);
+    let mut read = Vec::new();
+    let until = Instant::now() + Duration::from_secs(120);
+    while !(unreachable && next.is_some() && members) {
+        let left = until.saturating_duration_since(Instant::now());
+        let Ok(Some((kind, v))) = tokio::time::timeout(left, s.next()).await else {
+            panic!(
+                "after the kill: unreachable {unreachable}, next {next:?}, members {members}; read:\n{}",
+                read.join("\n")
+            );
+        };
+        match kind.as_str() {
+            "peer" if v["peer"] == holder.as_str() && v["state"] == "unreachable" => {
+                unreachable = true;
+            }
+            "pipeline" if !is_holder(&v) && v["actual"] == "running" => next = Some(v.clone()),
+            "members"
+                if v["members"]
+                    .as_array()
+                    .is_some_and(|m| m.iter().all(|m| m["node"] != holder.as_str())) =>
+            {
+                members = true;
+            }
+            _ => {}
+        }
+        read.push(format!("{kind} {v}"));
+    }
+    let next = next.unwrap();
+    assert_eq!(next["pipeline"], test.as_str(), "{next}");
+
+    // The nodes stop with the stream still open on one of them.
+    c.stop().await;
+    // What it sent before the stop read past, the stream ends.
+    let ended = tokio::time::timeout(Duration::from_secs(10), async {
+        while s.next().await.is_some() {}
+    })
+    .await;
+    assert!(ended.is_ok(), "the stream ended");
+}
````

`crates/data-avalanche/tests/e2e/harness.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 775e1ab..e06c823 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -977,7 +977,111 @@ async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, request: &[u8]) -
     Some(out)
 }
 
+/// A `GET /v1/stream` open on a node, read one message at a time.
+pub struct Sse {
+    read: Box<dyn AsyncRead + Send + Unpin>,
+    buffer: String,
+}
+
+impl Sse {
+    /// The next message's kind and data, keep-alive comments skipped; `None` once the stream
+    /// ended.
+    pub async fn next(&mut self) -> Option<(String, serde_json::Value)> {
+        loop {
+            while let Some(end) = self.buffer.find("\n\n") {
+                let frame: String = self.buffer.drain(..end + 2).collect();
+                let kind = frame.lines().find_map(|l| l.strip_prefix("event:"));
+                let data = frame.lines().find_map(|l| l.strip_prefix("data:"));
+                if let (Some(kind), Some(data)) = (kind, data) {
+                    let data = serde_json::from_str(data.trim()).expect("a message's data is JSON");
+                    return Some((kind.trim().to_owned(), data));
+                }
+            }
+            let mut chunk = [0u8; 8192];
+            let n = self.read.read(&mut chunk).await.ok()?;
+            if n == 0 {
+                return None;
+            }
+            self.buffer
+                .push_str(std::str::from_utf8(&chunk[..n]).expect("UTF-8"));
+        }
+    }
+
+    /// Reads on until a message of `kind` whose data `is` what is waited for, within `within`:
+    /// its data. Panics past it, naming `what` and the messages read meanwhile.
+    pub async fn until(
+        &mut self,
+        what: &str,
+        kind: &str,
+        within: Duration,
+        is: impl Fn(&serde_json::Value) -> bool,
+    ) -> serde_json::Value {
+        let mut read = Vec::new();
+        let found = tokio::time::timeout(within, async {
+            while let Some((k, v)) = self.next().await {
+                if k == kind && is(&v) {
+                    return Some(v);
+                }
+                read.push(format!("{k} {v}"));
+            }
+            None
+        })
+        .await;
+        match found {
+            Ok(Some(v)) => v,
+            _ => panic!(
+                "{what}: no such {kind} within {within:?}; read meanwhile:\n{}",
+                read.join("\n")
+            ),
+        }
+    }
+}
+
 impl Proc {
+    /// Opens `GET path` as a stream, with the token of [`Self::authorize`], over HTTP/1.0: its
+    /// answer's body is the stream's bytes as sent. `None` when nothing answers or the answer
+    /// isn't `200`.
+    pub async fn stream(&self, path: &str) -> Option<Sse> {
+        let auth = self
+            .token
+            .as_deref()
+            .map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
+        let request = format!("GET {path} HTTP/1.0\r\nHost: t\r\n{auth}\r\n");
+        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
+            .await
+            .ok()?;
+        let mut read: Box<dyn AsyncRead + Send + Unpin> = match &self.client.tls {
+            None => {
+                let mut tcp = tcp;
+                tcp.write_all(request.as_bytes()).await.ok()?;
+                Box::new(tcp)
+            }
+            Some(tls) => {
+                let name = ServerName::IpAddress(std::net::Ipv4Addr::LOCALHOST.into());
+                let mut s = tls.connect(name, tcp).await.ok()?;
+                s.write_all(request.as_bytes()).await.ok()?;
+                Box::new(s)
+            }
+        };
+        // The head, a byte at a time: what follows it is the stream's.
+        let mut head = Vec::new();
+        while !head.ends_with(b"\r\n\r\n") {
+            let mut b = [0u8; 1];
+            if read.read(&mut b).await.ok()? == 0 {
+                return None;
+            }
+            head.push(b[0]);
+        }
+        let head = String::from_utf8(head).ok()?;
+        if head.get(9..12) != Some("200") {
+            return None;
+        }
+        Some(Sse {
+            read,
+            buffer: String::new(),
+        })
+    }
+
     pub fn log(&self) -> String {
         std::fs::read_to_string(&self.log).unwrap_or_default()
     }
````

`crates/data-avalanche/tests/e2e/main.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 34dd668..8a11e5b 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -11,6 +11,7 @@ mod cluster;
 mod console_history;
 mod console_nodes;
 mod console_pipelines;
+mod console_stream;
 mod crashes;
 mod events;
 mod failover;
````

- [ ] **Step 2: Run it**

Run, with the emulators up (`test-infra/up.sh`):

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e console_stream -- --test-threads=1
for m in mysql-84 mysql-84-nogtid mariadb-118; do
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=$m \
    cargo test -p data-avalanche --features e2e-hooks --test e2e console_stream -- --test-threads=1
done
```

Expected: `1 passed` on each, in about 40 s. The scenario passes at once: Tasks 1–4 built what it checks.

- [ ] **Step 3: Run the gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/data-avalanche/tests/e2e/console_stream.rs crates/data-avalanche/tests/e2e/harness.rs crates/data-avalanche/tests/e2e/main.rs
git commit -m "test(e2e): the live stream through any node, across a failover and the nodes' stop"
```

---

## Exit

- Every task committed, one commit each, after the plan's and the spec patch's commits; the gates
  pass on the last one (`cargo fmt --all --check`, both clippy runs, `cargo test --workspace`).
- With the emulators up: the whole E2E suite on PostgreSQL 18 (`DATA_AVALANCHE_IT=1
  DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e --
  --test-threads=1`), and Task 6's scenario on `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
- `cargo xtask docs --check` and `cd docs && npm run build` (its link check) pass.
- At the plan's verification (the code above, on `c464e14`, each task's red and green commits
  apart): every Step 2 failure as stated; each task's gates (both clippy runs, `cargo xtask docs
  --check`) and its tests pass at its commit; `cargo test --workspace` 1776 passed at Task 6;
  Task 6's scenario passed on PostgreSQL 18 (40 s), `mysql-84` (37 s), `mysql-84-nogtid` (38 s)
  and `mariadb-118` (40 s); the whole E2E suite passed on PostgreSQL 18 (57 tests, 833 s); the
  site built (31 pages, link check clean).
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, the PR.
