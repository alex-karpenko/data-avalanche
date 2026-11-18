//! `GET /v1/pipelines/{id}/live` (Admin_Console §4, D-M13d.6): what the pipeline's worker shows
//! now, from the memory of the node that runs it. Any node answers: one that doesn't run the
//! worker asks the lease holder, with the caller's own credentials.

use std::collections::BTreeSet;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use data_avalanche_cluster::{Actual, ControllerStatus};
use data_avalanche_config::Pipeline;
use data_avalanche_core::TableRef;
use data_avalanche_pipeline::{Phase, WorkerStatus};
use data_avalanche_state::{HaltsDoc, PipelineDocs};
use serde_json::{Value, json};

use crate::peers::{Asked, DETAIL_LIMIT, FORWARDED_BY};
use crate::pipelines::{find, unavailable};
use crate::snapshots::table_name;
use crate::{Api, Node, body, starting};

/// A table is stale when what lake readers see of it is older than this while its pipeline runs:
/// the replication-lag target (System-Overview Scope, p99 ≤ 60 s).
pub const FRESH_WITHIN: Duration = Duration::from_secs(60);

fn phase_name(p: &Phase) -> &'static str {
    match p {
        Phase::Starting => "starting",
        Phase::Streaming => "streaming",
        Phase::Reconnecting { .. } => "reconnecting",
        Phase::Draining => "draining",
        Phase::Paused => "paused",
    }
}

/// The controller's state, and its error.
fn actual_of(a: &Actual) -> (&'static str, Option<&str>) {
    match a {
        Actual::Standby { .. } => ("standby", None),
        Actual::Starting => ("starting", None),
        Actual::Running => ("running", None),
        Actual::Paused => ("paused", None),
        Actual::Stopped => ("stopped", None),
        Actual::Dumped => ("dumped", None),
        Actual::Retrying { error } => ("retrying", Some(error)),
        Actual::Error { error, .. } => ("error", Some(error)),
    }
}

/// The pipeline's controller on this node, when it runs the pipeline: it holds the lease.
fn running_here(node: &Node, id: &str) -> Option<ControllerStatus> {
    let status = node.statuses.borrow().get(id)?.borrow().clone();
    match status.actual {
        None | Some(Actual::Standby { .. }) => None,
        Some(_) => Some(status),
    }
}

/// What lake readers see of the pipeline's tables as of now: every change read before the oldest
/// one not yet durable (`replication_lag_seconds`, System-Overview), all of them when none waits.
fn fresh_as_of(w: &WorkerStatus, now: DateTime<Utc>) -> DateTime<Utc> {
    w.unsynced_since_micros
        .and_then(DateTime::from_timestamp_micros)
        .map_or(now, |at| at.min(now))
}

/// Each table the worker tracks, halts, copies or has committed to: its state, its figures, and
/// how fresh it is. A halted table is as fresh as its halt (`halts`' times; none when unread). A
/// streaming table is stale when that is older than [`FRESH_WITHIN`] while the worker streams.
fn tables(
    p: &Pipeline,
    s: &ControllerStatus,
    w: &WorkerStatus,
    halts: Option<&HaltsDoc>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let running = matches!(s.actual, Some(Actual::Running)) && w.phase == Phase::Streaming;
    let fresh = fresh_as_of(w, now);
    let window = chrono::Duration::from_std(FRESH_WITHIN).expect("a minute");
    let all: BTreeSet<&TableRef> = w
        .tracked
        .iter()
        .chain(w.halted.keys())
        .chain(w.snapshots.keys())
        .chain(w.tables.keys())
        .collect();
    all.into_iter()
        .map(|t| {
            let halt = w.halted.get(t);
            let since = halts.and_then(|h| h.since(t));
            let snapshot = w.snapshots.get(t);
            let state = match (halt, snapshot) {
                (Some(_), _) => "halted",
                (None, Some(_)) => "snapshot",
                (None, None) => "streaming",
            };
            let fresh = if halt.is_some() { since } else { Some(fresh) };
            let figures = w.tables.get(t);
            json!({
                "table": table_name(p, t),
                "state": state,
                "halt": halt.map(|r| json!({
                    "code": r.code(),
                    "detail": r.to_string(),
                    "since": since,
                })),
                "snapshot": snapshot.map(|s| json!({
                    "rows_done": s.rows_done,
                    "waiting": s.waiting,
                })),
                "changes": figures.map_or(0, |f| f.changes),
                "changes_per_minute": figures.map_or(0, |f| f.changes_per_minute(now)),
                "last_commit": figures.and_then(|f| f.last_commit),
                "version": figures.and_then(|f| f.version),
                "files_rewritten": figures.and_then(|f| f.cost).map(|c| c.files_rewritten),
                "files": figures.and_then(|f| f.cost).and_then(|c| c.files),
                "soft_dropped": w.soft_dropped.get(t).copied().unwrap_or(0),
                "fresh_as_of": fresh,
                // A copy's readers see what its job says (`lake`), not the stream's freshness.
                "stale": state == "streaming" && running && fresh.is_some_and(|f| now - f > window),
            })
        })
        .collect()
}

/// The answer of the node that runs the pipeline.
fn answer(
    me: &str,
    p: &Pipeline,
    s: &ControllerStatus,
    halts: Option<&HaltsDoc>,
    now: DateTime<Utc>,
) -> Value {
    let (actual, error) = s.actual.as_ref().map_or(("starting", None), actual_of);
    let w = s.worker.as_ref();
    let worker = w.map(|w| {
        let fresh = fresh_as_of(w, now);
        json!({
            "phase": phase_name(&w.phase),
            "reconnecting": match &w.phase {
                Phase::Reconnecting { error } => Some(error.as_str()),
                _ => None,
            },
            "read": w.read.as_ref().map(ToString::to_string),
            "checkpoint": w.checkpoint.as_ref().map(ToString::to_string),
            "lag_seconds": (now - fresh).as_seconds_f64(),
            "fresh_as_of": fresh,
            "degraded": w.degraded,
            "reconnects": w.reconnects,
            "rows_read": {
                "insert": w.rows_read.insert,
                "update": w.rows_read.update,
                "delete": w.rows_read.delete,
                "snapshot": w.rows_read.snapshot,
            },
            "buffer_bytes": w.buffer_bytes,
            "warnings": w.warnings,
        })
    });
    let source = w.map(|w| {
        json!({
            "detail": w.source,
            "read_at": w.source_read_at,
            "retained_bytes": w.retained_bytes,
            "behind_seconds": w.source_behind.map(|d| d.as_secs_f64()),
        })
    });
    json!({
        "id": p.id,
        "node": me,
        "as_of": now,
        "actual": actual,
        "error": error,
        "owner_epoch": s.owner_epoch,
        "worker": worker,
        "source": source,
        "tables": w.map(|w| tables(p, s, w, halts, now)).unwrap_or_default(),
    })
}

/// This node's answer when it runs the pipeline. A failed read of the halts leaves the halts'
/// times out, not the answer.
async fn here(node: &Node, p: &Pipeline, docs: &PipelineDocs) -> Option<Value> {
    let s = running_here(node, &p.id)?;
    let halts = match docs.halts().await {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::debug!("{}'s live figures, its halts: {e}", p.id);
            None
        }
    };
    let me = node.members.borrow().me.clone();
    Some(answer(&me, p, &s, halts.as_ref(), Utc::now()))
}

/// The lease holder's answer to the caller's request, sent on: `409` when no node holds the
/// lease, `503` naming a holder this node can't ask, and never a request that was forwarded
/// already (`421`: the lease moved).
async fn from_holder(node: &Node, id: &str, headers: &HeaderMap) -> Response {
    let me = node.members.borrow().me.clone();
    if headers.contains_key(FORWARDED_BY) {
        return body(
            StatusCode::MISDIRECTED_REQUEST,
            &json!({ "error": format!("{me} doesn't run {id}'s worker: its lease moved") }),
        );
    }
    let lease = match node.leases.observe(id).await {
        Ok(l) => l.map(|v| v.doc).filter(|l| !l.released),
        Err(e) => return unavailable(&e),
    };
    let Some(lease) = lease else {
        return body(
            StatusCode::CONFLICT,
            &json!({ "error": format!("no node holds {id}'s lease: no worker runs it") }),
        );
    };
    let holder = lease.holder_node_id;
    let unreachable = |why: String| {
        body(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({ "error": why, "node": holder }),
        )
    };
    if holder == me {
        return unreachable(format!(
            "{me} holds {id}'s lease and doesn't run its worker yet"
        ));
    }
    let member = node.members.borrow().nodes.get(&holder).cloned();
    match member {
        None => unreachable(format!(
            "{holder} holds {id}'s lease and isn't a member this node sees"
        )),
        Some(m) if m.meta.api_url.is_empty() => unreachable(format!(
            "{holder} holds {id}'s lease and gossips no API address"
        )),
        Some(m) => {
            let asked = Asked::from(Method::GET, headers, Bytes::new());
            let path = format!("/v1/pipelines/{id}/live");
            node.peers
                .send(&holder, &m.meta.api_url, &path, asked, DETAIL_LIMIT)
                .await
        }
    }
}

/// `GET /v1/pipelines/{id}/live`.
pub(crate) async fn get(
    State(api): State<Api>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs) = match find(node, &id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    match here(node, &p, &docs).await {
        Some(v) => body(StatusCode::OK, &v),
        None => from_holder(node, &id, &headers).await,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{
        CommitCost, HaltReason, PgDetail, SourceDetail, SourcePosition, TableRef,
    };
    use data_avalanche_pipeline::{SnapshotProgress, TableCommit, WorkerStatus};
    use data_avalanche_state::{Holder, TableHalt};
    use tokio::sync::watch;

    use super::*;
    use crate::peers::{ANSWERED_BY, TARGET};
    use crate::test_support::{fake_answering, open, request, served, served_with};

    fn t(name: &str) -> TableRef {
        TableRef::new("public", name)
    }

    /// A worker that streams `orders`, committed 5 changes to it, has `items` halted and copies
    /// `audit`; its oldest change waiting is `lag` old.
    fn worker(lag: chrono::Duration) -> WorkerStatus {
        let now = Utc::now();
        let mut w = WorkerStatus {
            phase: Phase::Streaming,
            unsynced_since_micros: Some((now - lag).timestamp_micros()),
            tracked: [t("orders"), t("items")].into(),
            halted: [(t("items"), HaltReason::TableDropped)].into(),
            snapshots: [(
                t("audit"),
                SnapshotProgress {
                    rows_done: 40,
                    waiting: false,
                },
            )]
            .into(),
            source: Some(SourceDetail::Postgres(PgDetail {
                head: SourcePosition::Pg(Lsn(0x30)),
                slot: None,
                max_slot_wal_keep_size: None,
                publication: None,
            })),
            source_read_at: Some(now),
            retained_bytes: Some(4096),
            ..WorkerStatus::default()
        };
        w.committed(&TableCommit {
            table: t("orders"),
            rows: 5,
            snapshot: false,
            version: 18,
            cost: CommitCost {
                files_rewritten: 6,
                files: Some(820),
            },
            at: now,
        });
        w
    }

    fn statuses(
        actual: Actual,
        worker: Option<WorkerStatus>,
    ) -> watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>> {
        let status = ControllerStatus {
            actual: Some(actual),
            owner_epoch: Some(7),
            lease: None,
            worker,
        };
        watch::channel(BTreeMap::from([(
            "orders".to_owned(),
            watch::channel(status).1,
        )]))
        .1
    }

    fn json(text: &str) -> Value {
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    fn by_table<'v>(v: &'v Value, name: &str) -> &'v Value {
        v["tables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["table"] == name)
            .unwrap_or_else(|| panic!("{name} in {v}"))
    }

    /// The node that runs the worker answers from its memory: each table's state, figures and
    /// freshness; a halted table is as fresh as its halt, and isn't stale.
    #[tokio::test]
    async fn the_node_that_runs_the_worker_answers_from_its_memory() {
        let lagging = statuses(Actual::Running, Some(worker(chrono::Duration::seconds(90))));
        let (addr, lone, _dir) =
            served_with("http://127.0.0.1:1", open(), |n| n.statuses = lagging).await;
        let p = crate::test_support::config().pipelines["orders"].clone();
        let docs = (lone.node.docs)(&p).unwrap();
        docs.record_halt(&TableHalt {
            table: t("items"),
            reason: HaltReason::TableDropped,
            at: SourcePosition::Pg(Lsn(0x20)),
            copy: false,
        })
        .await
        .unwrap();
        let since = docs.halts().await.unwrap().since(&t("items")).unwrap();
        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        assert_eq!(
            (&v["node"], &v["actual"], &v["owner_epoch"]),
            (&json!("node-a"), &json!("running"), &json!(7))
        );
        assert_eq!(v["worker"]["phase"], "streaming");
        assert!(v["worker"]["lag_seconds"].as_f64().unwrap() >= 90.0, "{v}");
        assert_eq!(v["source"]["detail"]["engine"], "postgres");
        assert_eq!(v["source"]["retained_bytes"], 4096);
        let orders = by_table(&v, "public.orders");
        assert_eq!(orders["state"], "streaming");
        assert_eq!(
            (&orders["changes"], &orders["changes_per_minute"]),
            (&json!(5), &json!(5))
        );
        assert_eq!(
            (
                &orders["version"],
                &orders["files_rewritten"],
                &orders["files"]
            ),
            (&json!(18), &json!(6), &json!(820))
        );
        assert_eq!(orders["stale"], true, "90 s behind, past {FRESH_WITHIN:?}");
        let items = by_table(&v, "public.items");
        assert_eq!(
            (&items["state"], &items["halt"]["code"]),
            (&json!("halted"), &json!("TABLE_DROPPED"))
        );
        assert_eq!(items["fresh_as_of"], json!(since));
        assert_eq!(items["halt"]["since"], json!(since));
        assert_eq!(items["stale"], false);
        let audit = by_table(&v, "public.audit");
        assert_eq!(
            (&audit["state"], &audit["snapshot"]["rows_done"]),
            (&json!("snapshot"), &json!(40))
        );
        assert_eq!(
            audit["stale"], false,
            "a copy's readers see what its job says"
        );
    }

    /// A paused pipeline is behind by design: nothing is stale.
    #[tokio::test]
    async fn nothing_is_stale_while_paused() {
        let mut w = worker(chrono::Duration::seconds(300));
        w.phase = Phase::Paused;
        let paused = statuses(Actual::Paused, Some(w));
        let (addr, _lone, _dir) =
            served_with("http://127.0.0.1:1", open(), |n| n.statuses = paused).await;
        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
        assert_eq!(code, 200, "{text}");
        assert_eq!(by_table(&json(&text), "public.orders")["stale"], false);
    }

    async fn hold(lone: &crate::test_support::Lone, holder: &str) {
        let holder = Holder {
            node_id: holder.into(),
            incarnation: 1_731_600_000_000,
        };
        lone.leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap();
    }

    /// Another node asks the lease holder with the caller's credentials, and passes its answer
    /// on; a forwarded request isn't forwarded again.
    #[tokio::test]
    async fn another_node_asks_the_lease_holder() {
        let (b, log) =
            fake_answering(Duration::ZERO, StatusCode::OK, json!({ "id": "orders" })).await;
        let (addr, lone, _dir) = served(&b, open()).await;
        hold(&lone, "node-b").await;
        let auth = [("Authorization", "Bearer the-callers-token")];
        let (code, head, text) = request(addr, "GET", "/v1/pipelines/orders/live", &auth, "").await;
        assert_eq!((code, json(&text)), (200, json!({ "id": "orders" })));
        assert!(
            head.to_ascii_lowercase()
                .contains(&format!("{ANSWERED_BY}: node-b")),
            "{head}"
        );
        let seen = log.lock().unwrap()[0].clone();
        assert_eq!(seen.uri, "/v1/pipelines/orders/live");
        assert_eq!(
            seen.authorization.as_deref(),
            Some("Bearer the-callers-token")
        );
        assert_eq!(seen.target.as_deref(), Some("node-b"));
        let again = [(FORWARDED_BY, "node-c"), (TARGET, "node-a")];
        let (code, _, _) = request(addr, "GET", "/v1/pipelines/orders/live", &again, "").await;
        assert_eq!(code, 421, "the lease moved");
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    /// No holder: 409. A holder that isn't a member, or this node holding the lease without a
    /// worker yet: 503, naming it.
    #[tokio::test]
    async fn without_a_holder_to_ask_the_answer_says_why() {
        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
        assert_eq!(code, 409, "{text}");
        hold(&lone, "node-z").await;
        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
        assert_eq!((code, &json(&text)["node"]), (503, &json!("node-z")));
        let (code, _, _) = request(addr, "GET", "/v1/pipelines/nope/live", &[], "").await;
        assert_eq!(code, 404);
    }
}
