//! `/v1/pipelines` (Pipeline_Lifecycle §3, §6): the status any node reads from the pipeline's
//! state documents and its lease, and the lifecycle actions. Pause, stop and resume set the
//! persisted desired state; restart and flush are requests the owner takes at its next poll.
//! Both answer 202 once recorded: the owner acts within a `lease_renewal_interval`.

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::Utc;
use data_avalanche_config::Pipeline;
use data_avalanche_core::SourcePosition;
use data_avalanche_state::{
    Action, ActualState, DesiredState, LeaseDoc, PipelineDocs, Request, StateError, StatusDoc,
    Transition,
};
use serde_json::{Value, json};

use crate::{Api, Failure, Node, Principal, body, starting};

/// What a status read failed on: 503, the store is the answer.
fn unavailable(e: &StateError) -> Response {
    body(
        StatusCode::SERVICE_UNAVAILABLE,
        &json!({ "error": format!("the pipeline's state: {e}") }),
    )
}

/// The pipeline with this id in the generation the node runs, and its state documents.
async fn find(node: &Node, id: &str) -> Result<(Pipeline, PipelineDocs), Failure> {
    let config = node.coordinator.config().await;
    let Some(p) = config.pipelines.values().find(|p| p.id == id).cloned() else {
        return Err(Failure::new(
            StatusCode::NOT_FOUND,
            format!("no pipeline {id:?} in the config generation this node runs"),
        ));
    };
    let docs = (node.docs)(&p).map_err(|e| {
        Failure::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the pipeline's state location: {e}"),
        )
    })?;
    Ok((p, docs))
}

fn desired_name(d: DesiredState) -> &'static str {
    match d {
        DesiredState::Running => "running",
        DesiredState::Paused => "paused",
        DesiredState::Stopped => "stopped",
    }
}

fn actual_name(a: ActualState) -> &'static str {
    match a {
        ActualState::Starting => "starting",
        ActualState::Running => "running",
        ActualState::Paused => "paused",
        ActualState::Stopped => "stopped",
        ActualState::Dumped => "dumped",
        ActualState::Retrying => "retrying",
        ActualState::Error => "error",
    }
}

/// The actual state: `unowned` when no node holds the lease, the holder's `status.json` when
/// it wrote one in this lease term, else `starting`.
fn actual<'s>(
    lease: Option<&LeaseDoc>,
    status: Option<&'s StatusDoc>,
) -> (&'static str, Option<&'s StatusDoc>) {
    let Some(lease) = lease.filter(|l| !l.released) else {
        return ("unowned", None);
    };
    match status
        .filter(|s| s.owner_epoch == lease.owner_epoch && s.holder_node_id == lease.holder_node_id)
    {
        Some(s) => (actual_name(s.actual), Some(s)),
        None => ("starting", None),
    }
}

/// The documents a status is read from.
struct Read {
    desired: DesiredState,
    lease: Option<LeaseDoc>,
    status: Option<StatusDoc>,
}

async fn read(node: &Node, p: &Pipeline, docs: &PipelineDocs) -> Result<Read, StateError> {
    let (state, lease, status) =
        tokio::join!(docs.state(), node.leases.observe(&p.id), docs.status());
    Ok(Read {
        desired: state?.map_or(DesiredState::Running, |v| v.doc.desired_state),
        lease: lease?.map(|v| v.doc),
        status: status?.map(|v| v.doc),
    })
}

fn summary(p: &Pipeline, r: &Read) -> Value {
    let (actual, _) = actual(r.lease.as_ref(), r.status.as_ref());
    json!({
        "id": p.id,
        "name": p.name,
        "desired_state": desired_name(r.desired),
        "actual_state": actual,
        "holder": r.lease.as_ref().filter(|l| !l.released).map(|l| l.holder_node_id.clone()),
    })
}

/// Every pipeline of the generation the node runs, by id.
pub(crate) async fn list(State(api): State<Api>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let config = node.coordinator.config().await;
    let mut pipelines: Vec<&Pipeline> = config.pipelines.values().collect();
    pipelines.sort_by(|a, b| a.id.cmp(&b.id));
    let mut out = vec![];
    for p in pipelines {
        let docs = match (node.docs)(p) {
            Ok(docs) => docs,
            Err(e) => {
                return body(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({ "error": format!("{}: the state location: {e}", p.id) }),
                );
            }
        };
        match read(node, p, &docs).await {
            Ok(r) => out.push(summary(p, &r)),
            Err(e) => return unavailable(&e),
        }
    }
    body(StatusCode::OK, &json!({ "pipelines": out }))
}

/// One pipeline: its desired and actual states, the holder of its lease, its position, and
/// its halted tables with their reasons (System-Overview's endpoint table).
pub(crate) async fn get(State(api): State<Api>, Path(id): Path<String>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs) = match find(node, &id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    let read_all = async {
        let r = read(node, &p, &docs).await?;
        let (slot, position, halts) = tokio::join!(docs.slot(), docs.position(), docs.halts());
        let position = match (slot?, position?) {
            (_, Some(v)) => Some(v.doc.position),
            (Some(v), None) if !v.doc.is_intent() => {
                Some(SourcePosition::Pg(v.doc.confirmed_flush_lsn))
            }
            _ => None,
        };
        Ok::<_, StateError>((r, position, halts?))
    };
    let (r, position, halts) = match read_all.await {
        Ok(all) => all,
        Err(e) => return unavailable(&e),
    };
    let (actual, status) = actual(r.lease.as_ref(), r.status.as_ref());
    let mut v = summary(&p, &r);
    v["source"] = json!({
        "type": p.source.kind.as_str(),
        "host": p.source.host,
        "port": p.source.port,
        "db": p.source.db,
    });
    v["sink"] =
        json!({ "type": p.sink.kind.as_str(), "bucket": p.sink.bucket, "prefix": p.sink.prefix });
    v["actual_state"] = json!(actual);
    v["error"] = json!(status.and_then(|s| s.error.clone()));
    v["code"] = json!(status.and_then(|s| s.code.clone()));
    v["degraded"] = json!(status.and_then(|s| s.degraded.clone()));
    v["status_written_at"] = json!(status.map(|s| s.written_at));
    v["holder"] = match &r.lease {
        Some(l) => json!({
            "node": l.holder_node_id,
            "owner_epoch": l.owner_epoch,
            "renewed_at": l.renewed_at,
            "released": l.released,
        }),
        None => Value::Null,
    };
    v["position"] = json!(position.map(|p| p.to_string()));
    v["halted_tables"] = json!(
        halts
            .tables
            .iter()
            .map(|h| json!({
                "table": h.table.to_string(),
                "code": h.reason.code(),
                "detail": h.reason.to_string(),
                "at": h.at.to_string(),
            }))
            .collect::<Vec<_>>()
    );
    body(StatusCode::OK, &v)
}

/// Pause, stop or resume: 202 when the desired state changed, 200 when it was that already,
/// 409 for a pause of a stopped pipeline.
async fn transition(api: &Api, id: &str, to: DesiredState, by: &Principal) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs) = match find(node, id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    let opened = docs.open(&p.id, p.sink.kind.as_str()).await;
    let done = match opened {
        Ok(_) => docs.transition(to).await,
        Err(e) => Err(e),
    };
    let to_name = desired_name(to);
    match done {
        Ok(Transition::Changed { from }) => {
            tracing::info!(pipeline = %p.name, by = %by.name, from = desired_name(from), "desired state set to {to_name} through the API");
            body(
                StatusCode::ACCEPTED,
                &json!({ "desired_state": to_name, "previous": desired_name(from) }),
            )
        }
        Ok(Transition::Unchanged) => body(
            StatusCode::OK,
            &json!({ "desired_state": to_name, "unchanged": true }),
        ),
        Ok(Transition::Refused { current }) => body(
            StatusCode::CONFLICT,
            &json!({
                "error": format!("a {} pipeline can't be set {to_name}; resume it first", desired_name(current)),
                "desired_state": desired_name(current),
            }),
        ),
        Err(e @ (StateError::PipelineIdMismatch { .. } | StateError::SinkTypeMismatch { .. })) => {
            body(StatusCode::CONFLICT, &json!({ "error": e.to_string() }))
        }
        Err(e) => unavailable(&e),
    }
}

pub(crate) async fn pause(
    State(api): State<Api>,
    Path(id): Path<String>,
    Extension(by): Extension<Principal>,
) -> Response {
    transition(&api, &id, DesiredState::Paused, &by).await
}

pub(crate) async fn stop(
    State(api): State<Api>,
    Path(id): Path<String>,
    Extension(by): Extension<Principal>,
) -> Response {
    transition(&api, &id, DesiredState::Stopped, &by).await
}

pub(crate) async fn resume(
    State(api): State<Api>,
    Path(id): Path<String>,
    Extension(by): Extension<Principal>,
) -> Response {
    transition(&api, &id, DesiredState::Running, &by).await
}

/// Restart or flush, asked of the lease holder. Refused (409) with no holder, and when the
/// desired state rules it out: a restart of a paused or stopped pipeline, a flush of a stopped
/// one. A paused pipeline is flushed already: 200.
async fn ask(api: &Api, id: &str, action: Action, by: &Principal) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs) = match find(node, id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    let r = match read(node, &p, &docs).await {
        Ok(r) => r,
        Err(e) => return unavailable(&e),
    };
    let what = match action {
        Action::Restart => "restart",
        Action::Flush => "flush",
    };
    let conflict = |why: String| {
        body(
            StatusCode::CONFLICT,
            &json!({ "error": why, "desired_state": desired_name(r.desired) }),
        )
    };
    match (action, r.desired) {
        (_, DesiredState::Stopped) | (Action::Restart, DesiredState::Paused) => {
            return conflict(format!(
                "a {} pipeline can't {what}; resume it first",
                desired_name(r.desired)
            ));
        }
        (Action::Flush, DesiredState::Paused) => {
            return body(
                StatusCode::OK,
                &json!({ "desired_state": "paused", "unchanged": true }),
            );
        }
        (_, DesiredState::Running) => {}
    }
    if r.lease.as_ref().is_none_or(|l| l.released) {
        return conflict(format!(
            "no node holds the pipeline's lease, so none can {what} it"
        ));
    }
    let request = Request {
        id: uuid::Uuid::new_v4().to_string(),
        at: Utc::now(),
    };
    match docs.request(action, request.clone()).await {
        Ok(()) => {
            tracing::info!(pipeline = %p.name, by = %by.name, request = %request.id, "{what} asked through the API");
            body(StatusCode::ACCEPTED, &json!({ "request": request.id }))
        }
        Err(e) => unavailable(&e),
    }
}

pub(crate) async fn restart(
    State(api): State<Api>,
    Path(id): Path<String>,
    Extension(by): Extension<Principal>,
) -> Response {
    ask(&api, &id, Action::Restart, &by).await
}

pub(crate) async fn flush(
    State(api): State<Api>,
    Path(id): Path<String>,
    Extension(by): Extension<Principal>,
) -> Response {
    ask(&api, &id, Action::Flush, &by).await
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use data_avalanche_core::{HaltReason, TableRef};
    use data_avalanche_state::{Acquired, Holder, SlotDoc, TableHalt};
    use tempfile::TempDir;

    use super::*;
    use crate::test_support::{CONFIG, Lone, api, json_of, lone, open, serve};

    /// A lone node's API: the pipeline `orders` of [`CONFIG`], its state in memory.
    async fn served() -> (SocketAddr, Lone, PipelineDocs, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let config = lone.node.coordinator.config().await;
        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        (serve(a).await, lone, docs, dir)
    }

    async fn held_by(lone: &Lone, node: &str) -> data_avalanche_state::Versioned<LeaseDoc> {
        let holder = Holder {
            node_id: node.into(),
            incarnation: 1,
        };
        match lone
            .leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap()
        {
            Acquired::Mine(l) => l,
            Acquired::HeldBy(l) => panic!("held by {l:?}"),
        }
    }

    fn written(node: &str, epoch: u64, actual: ActualState) -> StatusDoc {
        StatusDoc {
            holder_node_id: node.into(),
            owner_epoch: epoch,
            actual,
            error: None,
            code: None,
            degraded: None,
            written_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn a_pipeline_s_status_is_read_from_its_state_and_its_lease() {
        let (addr, lone, docs, _dir) = served().await;
        let (code, v) = json_of(addr, "GET", "/v1/pipelines/nope").await;
        assert_eq!(code, 404, "{v}");

        let (code, v) = json_of(addr, "GET", "/v1/pipelines/orders").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["desired_state"], "running", "never started: it runs");
        assert_eq!(v["actual_state"], "unowned");
        assert_eq!(v["holder"], Value::Null);
        assert_eq!(v["position"], Value::Null);
        assert_eq!(v["halted_tables"], json!([]));
        assert_eq!(
            v["source"],
            json!({ "type": "postgres", "host": "db.example.com", "port": 5432, "db": "shop" })
        );
        assert_eq!(
            v["sink"],
            json!({ "type": "delta", "bucket": "lake", "prefix": "shop" })
        );

        let lease = held_by(&lone, "node-b").await;
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(v["actual_state"], "starting", "held, nothing reported yet");
        assert_eq!(v["holder"]["node"], "node-b");
        assert_eq!(v["holder"]["owner_epoch"], 1);

        docs.save_status(&written("node-b", 1, ActualState::Running))
            .await
            .unwrap();
        let lsn = "0/16B3748".parse().unwrap();
        docs.save_slot(&SlotDoc {
            slot_name: "orders_slot".into(),
            confirmed_flush_lsn: lsn,
        })
        .await
        .unwrap();
        docs.record_halt(&TableHalt {
            table: TableRef::new("public", "orders"),
            reason: HaltReason::TableDropped,
            at: SourcePosition::Pg(lsn),
        })
        .await
        .unwrap();
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(v["actual_state"], "running");
        assert_eq!(v["position"], "pg:0/16B3748");
        assert_eq!(v["halted_tables"][0]["table"], "public.orders");
        assert_eq!(v["halted_tables"][0]["code"], "TABLE_DROPPED");
        assert_eq!(v["halted_tables"][0]["at"], "pg:0/16B3748");
        assert!(
            v["halted_tables"][0]["detail"]
                .as_str()
                .unwrap()
                .contains("dropped"),
            "{v}"
        );

        let failed = StatusDoc {
            error: Some("no privilege".into()),
            code: Some("BINLOG_SOURCE_CHANGED".into()),
            ..written("node-b", 1, ActualState::Error)
        };
        docs.save_status(&failed).await.unwrap();
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(
            (&v["actual_state"], &v["error"], &v["code"]),
            (
                &json!("error"),
                &json!("no privilege"),
                &json!("BINLOG_SOURCE_CHANGED")
            )
        );

        // Another node's status, or the holder's own of an earlier term, is not this term's.
        for (node, epoch) in [("node-a", 1), ("node-b", 0)] {
            docs.save_status(&written(node, epoch, ActualState::Stopped))
                .await
                .unwrap();
            let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
            assert_eq!(v["actual_state"], "starting", "{node} at epoch {epoch}");
        }

        lone.leases.release(&lease, Utc::now()).await.unwrap();
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(v["actual_state"], "unowned");
        assert_eq!(v["holder"]["released"], true);
    }

    #[tokio::test]
    async fn the_list_has_every_pipeline_s_summary() {
        let (addr, lone, docs, _dir) = served().await;
        held_by(&lone, "node-b").await;
        docs.save_status(&written("node-b", 1, ActualState::Paused))
            .await
            .unwrap();
        let (code, v) = json_of(addr, "GET", "/v1/pipelines").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(
            v,
            json!({ "pipelines": [{
                "id": "orders", "name": "orders", "desired_state": "running",
                "actual_state": "paused", "holder": "node-b",
            }] })
        );
    }

    #[tokio::test]
    async fn pause_stop_and_resume_move_the_desired_state() {
        let (addr, _lone, docs, _dir) = served().await;
        let post = |action: &str| {
            let path = format!("/v1/pipelines/orders/{action}");
            async move { json_of(addr, "POST", &path).await }
        };
        assert_eq!(
            post("pause").await,
            (
                202,
                json!({ "desired_state": "paused", "previous": "running" })
            )
        );
        assert_eq!(
            post("pause").await,
            (200, json!({ "desired_state": "paused", "unchanged": true }))
        );
        assert_eq!(post("stop").await.0, 202);
        let (code, v) = post("pause").await;
        assert_eq!(code, 409, "{v}");
        assert_eq!(v["desired_state"], "stopped");
        assert_eq!(
            post("resume").await,
            (
                202,
                json!({ "desired_state": "running", "previous": "stopped" })
            )
        );
        let state = docs.state().await.unwrap().unwrap().doc;
        assert_eq!(state.desired_state, DesiredState::Running);
        assert_eq!(state.sink_type, "delta", "opened for the pipeline");
        assert_eq!(
            json_of(addr, "POST", "/v1/pipelines/nope/pause").await.0,
            404
        );
    }

    #[tokio::test]
    async fn restart_and_flush_are_asked_of_the_lease_holder() {
        let (addr, lone, docs, _dir) = served().await;
        let post = |action: &str| {
            let path = format!("/v1/pipelines/orders/{action}");
            async move { json_of(addr, "POST", &path).await }
        };
        let (code, v) = post("restart").await;
        assert_eq!(code, 409, "{v}");
        assert!(v["error"].as_str().unwrap().contains("lease"), "{v}");

        held_by(&lone, "node-b").await;
        let (code, v) = post("restart").await;
        assert_eq!(code, 202, "{v}");
        let asked = docs.requests().await.unwrap();
        assert_eq!(
            Some(&v["request"]),
            asked.restart.map(|r| json!(r.id)).as_ref()
        );
        let (code, v) = post("flush").await;
        assert_eq!(code, 202, "{v}");
        let flushed = docs.requests().await.unwrap().flush.unwrap().id;
        assert_eq!(json!(flushed), v["request"]);

        assert_eq!(post("pause").await.0, 202);
        let (code, v) = post("restart").await;
        assert_eq!((code, &v["desired_state"]), (409, &json!("paused")), "{v}");
        assert_eq!(
            post("flush").await,
            (200, json!({ "desired_state": "paused", "unchanged": true }))
        );
        assert_eq!(
            docs.requests().await.unwrap().flush.unwrap().id,
            flushed,
            "a paused pipeline's flush asks nothing"
        );
        assert_eq!(post("stop").await.0, 202);
        assert_eq!(post("flush").await.0, 409);
        assert_eq!(post("restart").await.0, 409);
    }
}
