//! The on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). A job is asked of a running
//! pipeline whose lease a node holds, as a restart is (Q-M9b-4): its document is written
//! `queued`, then a restart under the job's id, which the owner takes at its next poll. The new
//! worker's start takes the job's tables and the owner follows them to their end. The job's id
//! is `<pipeline id>~<suffix>` (Q-M9-2), so any node reads a job by its id from the pipeline's
//! state.

use std::collections::BTreeSet;

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::Utc;
use data_avalanche_config::{Pipeline, SourceType, key_table};
use data_avalanche_core::TableRef;
use data_avalanche_state::{
    Action, DesiredState, JobStatus, JobTableStatus, Request, SnapshotJobDoc,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::pipelines::{desired_name, find, read, unavailable};
use crate::{Api, Failure, Principal, body, starting};

/// Between a job's pipeline id and its suffix: a pipeline id has none (V-055).
const SEPARATOR: char = '~';

/// A new job id: the pipeline's, then a UUID v7, time-ordered.
fn new_job_id(pipeline_id: &str) -> String {
    format!("{pipeline_id}{SEPARATOR}{}", uuid::Uuid::now_v7().simple())
}

/// The pipeline id a job id names.
fn job_pipeline(job_id: &str) -> Option<&str> {
    match job_id.split_once(SEPARATOR) {
        Some((p, suffix)) if !p.is_empty() && !suffix.is_empty() => Some(p),
        _ => None,
    }
}

/// A table as the request and the answers name it: the source's per-table config key.
fn table_name(p: &Pipeline, t: &TableRef) -> String {
    match p.source.kind {
        SourceType::Postgres => t.to_string(),
        SourceType::Mysql => t.name.clone(),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Asked {
    tables: Vec<String>,
}

/// The body's tables, each once, in its order: named as the source's per-table config keys,
/// no globs.
fn tables(p: &Pipeline, text: &str) -> Result<Vec<TableRef>, String> {
    let asked: Asked = serde_json::from_str(text)
        .map_err(|e| format!("the body is {{\"tables\": [\"<table>\", ...]}}: {e}"))?;
    if asked.tables.is_empty() {
        return Err("the body names no table".into());
    }
    let mut seen = BTreeSet::new();
    let mut out = vec![];
    for name in &asked.tables {
        if name.contains('*') {
            return Err(format!(
                "{name:?}: a table is named exactly, without a glob"
            ));
        }
        let table = match p.source.kind {
            SourceType::Mysql if name.contains('.') => None,
            _ => key_table(p, name),
        }
        .filter(|t| !t.schema.is_empty() && !t.name.is_empty())
        .ok_or_else(|| match p.source.kind {
            SourceType::Postgres => format!("{name:?}: a PostgreSQL table is schema.table"),
            SourceType::Mysql => format!(
                "{name:?}: a MySQL table is named without its database ({})",
                p.source.db
            ),
        })?;
        if seen.insert(table.clone()) {
            out.push(table);
        }
    }
    Ok(out)
}

fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
    let status = |s: JobStatus| match s {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
    };
    let table_status = |s: JobTableStatus| match s {
        JobTableStatus::Queued => "queued",
        JobTableStatus::Running => "running",
        JobTableStatus::Completed => "completed",
        JobTableStatus::Failed => "failed",
    };
    json!({
        "job_id": j.job_id,
        "pipeline": p.id,
        "status": status(j.status),
        "tables": j.tables.iter().map(|t| json!({
            "table": table_name(p, &t.table),
            "status": table_status(t.status),
            "rows_done": t.rows_done,
            "error": t.error,
        })).collect::<Vec<_>>(),
        "requested_at": j.requested_at,
        "requested_by": j.requested_by,
        "finished_at": j.finished_at,
        "error": j.error,
    })
}

/// `POST /v1/pipelines/{id}/snapshots`: 202 with the job, once its document and its restart
/// are written.
pub(crate) async fn ask(
    State(api): State<Api>,
    Path(id): Path<String>,
    Extension(by): Extension<Principal>,
    text: String,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs) = match find(node, &id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    let tables = match tables(&p, &text) {
        Ok(tables) => tables,
        Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
    };
    let r = match read(node, &p, &docs).await {
        Ok(r) => r,
        Err(e) => return unavailable(&e),
    };
    let conflict = |why: String| {
        body(
            StatusCode::CONFLICT,
            &json!({ "error": why, "desired_state": desired_name(r.desired) }),
        )
    };
    if r.desired != DesiredState::Running {
        return conflict(format!(
            "a {} pipeline takes no snapshot job; resume it first",
            desired_name(r.desired)
        ));
    }
    if r.lease.as_ref().is_none_or(|l| l.released) {
        return conflict("no node holds the pipeline's lease, so none can run a job".into());
    }
    let unfinished = match docs.unfinished_snapshot_jobs().await {
        Ok(jobs) => jobs,
        Err(e) => return unavailable(&e),
    };
    for j in &unfinished {
        if let Some(t) = j.tables.iter().find(|t| {
            matches!(t.status, JobTableStatus::Queued | JobTableStatus::Running)
                && tables.contains(&t.table)
        }) {
            return conflict(format!(
                "job {} snapshots {} already; ask again once it ended",
                j.job_id,
                table_name(&p, &t.table)
            ));
        }
    }
    let job = SnapshotJobDoc::new(new_job_id(&p.id), tables, by.name.clone(), Utc::now());
    if let Err(e) = docs.create_snapshot_job(&job).await {
        return unavailable(&e);
    }
    let restart = Request {
        id: job.job_id.clone(),
        at: job.requested_at,
    };
    if let Err(e) = docs.request(Action::Restart, restart).await {
        return body(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({
                "error": format!("the job is recorded, but its restart isn't ({e}): it runs at the pipeline's next start"),
                "job_id": job.job_id,
            }),
        );
    }
    tracing::info!(pipeline = %p.name, by = %by.name, job = %job.job_id, tables = job.tables.len(), "snapshot job asked through the API");
    let v = job_json(&p, &job);
    body(
        StatusCode::ACCEPTED,
        &json!({
            "job_id": v["job_id"],
            "status": v["status"],
            "tables": v["tables"].as_array().into_iter().flatten().map(|t| t["table"].clone()).collect::<Vec<_>>(),
        }),
    )
}

/// `GET /v1/pipelines/{id}/snapshots`: every job of the pipeline, oldest request first.
pub(crate) async fn list(State(api): State<Api>, Path(id): Path<String>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs) = match find(node, &id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    match docs.snapshot_jobs().await {
        Ok(jobs) => body(
            StatusCode::OK,
            &json!({ "jobs": jobs.iter().map(|v| job_json(&p, &v.doc)).collect::<Vec<_>>() }),
        ),
        Err(e) => unavailable(&e),
    }
}

/// `GET /v1/snapshots/{job_id}`: the job, read from the state of the pipeline its id names.
pub(crate) async fn get(State(api): State<Api>, Path(job_id): Path<String>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let missing = || Failure::new(StatusCode::NOT_FOUND, format!("no snapshot job {job_id:?}"));
    let Some(pipeline) = job_pipeline(&job_id) else {
        return missing().into();
    };
    let (p, docs) = match find(node, pipeline).await {
        Ok(found) => found,
        Err(_) => return missing().into(),
    };
    match docs.snapshot_job(&job_id).await {
        Ok(Some(v)) => body(StatusCode::OK, &job_json(&p, &v.doc)),
        Ok(None) => missing().into(),
        Err(e) => unavailable(&e),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use chrono::Utc;
    use data_avalanche_core::TableRef;
    use data_avalanche_state::{
        Acquired, DesiredState, Holder, JobStatus, JobTableStatus, PipelineDocs, SnapshotJobDoc,
    };
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use crate::test_support::{CONFIG, Lone, api, json_of, lone, open, request, serve};

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

    async fn held(lone: &Lone) {
        let holder = Holder {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        let got = lone
            .leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap();
        assert!(matches!(got, Acquired::Mine(_)));
    }

    async fn ask(addr: SocketAddr, body: &str) -> (u16, Value) {
        let (code, _, text) = request(
            addr,
            "POST",
            "/v1/pipelines/orders/snapshots",
            &[("Content-Type", "application/json")],
            body,
        )
        .await;
        (
            code,
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}")),
        )
    }

    /// A job is recorded queued, then asked of the lease holder as a restart under the job's
    /// id; its id names the pipeline, so any node reads it.
    #[tokio::test]
    async fn a_job_is_recorded_then_asked_of_the_holder() {
        let (addr, lone, docs, _dir) = served().await;
        let tables = r#"{"tables": ["public.orders", "public.items", "public.orders"]}"#;
        let (code, v) = ask(addr, tables).await;
        assert_eq!(code, 409, "no holder: {v}");
        assert!(v["error"].as_str().unwrap().contains("lease"), "{v}");
        assert!(docs.snapshot_jobs().await.unwrap().is_empty());

        held(&lone).await;
        let (code, v) = ask(addr, tables).await;
        assert_eq!(code, 202, "{v}");
        let id = v["job_id"].as_str().unwrap().to_owned();
        let (pipeline, suffix) = id.split_once('~').unwrap();
        assert_eq!(pipeline, "orders");
        assert_eq!(suffix.len(), 32, "{id}");
        assert_eq!(v["status"], "queued");
        assert_eq!(
            v["tables"],
            json!(["public.orders", "public.items"]),
            "once each"
        );

        let job = docs.snapshot_job(&id).await.unwrap().unwrap().doc;
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.requested_by, "anonymous");
        assert_eq!(
            job.tables
                .iter()
                .map(|t| t.table.clone())
                .collect::<Vec<_>>(),
            [
                TableRef::new("public", "orders"),
                TableRef::new("public", "items")
            ]
        );
        assert_eq!(docs.requests().await.unwrap().restart.unwrap().id, id);

        let (code, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
        assert_eq!(code, 200, "{one}");
        assert_eq!(one["job_id"], id.as_str());
        assert_eq!(one["pipeline"], "orders");
        assert_eq!(one["status"], "queued");
        assert_eq!(
            one["tables"][0],
            json!({ "table": "public.orders", "status": "queued", "rows_done": 0, "error": null })
        );
        let (code, all) = json_of(addr, "GET", "/v1/pipelines/orders/snapshots").await;
        assert_eq!(code, 200, "{all}");
        assert_eq!(all["jobs"], json!([one]));
    }

    /// A job is asked of a running pipeline only (Q-M9b-4), as a restart is.
    #[tokio::test]
    async fn a_job_of_a_paused_or_stopped_pipeline_is_refused() {
        let (addr, lone, docs, _dir) = served().await;
        held(&lone).await;
        docs.open("orders", "delta").await.unwrap();
        for state in [DesiredState::Paused, DesiredState::Stopped] {
            docs.set_desired_state(state).await.unwrap();
            let (code, v) = ask(addr, r#"{"tables": ["public.orders"]}"#).await;
            assert_eq!(code, 409, "{state:?}: {v}");
        }
        assert!(docs.snapshot_jobs().await.unwrap().is_empty());
    }

    /// A table an unfinished job holds is refused, naming the job; once that job ended, it can
    /// be asked again.
    #[tokio::test]
    async fn a_table_an_unfinished_job_holds_is_refused() {
        let (addr, lone, docs, _dir) = served().await;
        held(&lone).await;
        let (_, first) = ask(addr, r#"{"tables": ["public.orders"]}"#).await;
        let first = first["job_id"].as_str().unwrap().to_owned();
        let (code, v) = ask(addr, r#"{"tables": ["public.items", "public.orders"]}"#).await;
        assert_eq!(code, 409, "{v}");
        let error = v["error"].as_str().unwrap();
        assert!(
            error.contains("public.orders") && error.contains(&first),
            "{v}"
        );
        docs.update_snapshot_job(&first, |j| {
            j.tables[0].status = JobTableStatus::Completed;
            j.settle(Utc::now());
        })
        .await
        .unwrap();
        let (code, v) = ask(addr, r#"{"tables": ["public.items", "public.orders"]}"#).await;
        assert_eq!(code, 202, "{v}");
    }

    /// The body names tables, each exactly (System-Overview: "tables list, no globs"), as the
    /// source's per-table config keys do: PostgreSQL `schema.table`.
    #[tokio::test]
    async fn a_request_names_its_tables_exactly() {
        let (addr, lone, docs, _dir) = served().await;
        held(&lone).await;
        for body in [
            "",
            "not json",
            r#"{"tables": "public.orders"}"#,
            r#"{"tables": []}"#,
            r#"{"tables": ["public.*"]}"#,
            r#"{"tables": ["orders"]}"#,
            r#"{"tables": ["public.orders"], "truncate": true}"#,
        ] {
            let (code, v) = ask(addr, body).await;
            assert_eq!(code, 422, "{body:?}: {v}");
            assert!(v["error"].as_str().is_some(), "{v}");
        }
        assert!(docs.snapshot_jobs().await.unwrap().is_empty());
    }

    /// A job id that names no pipeline of the generation, or no job of it, is 404; so is the
    /// list of a pipeline that isn't there.
    #[tokio::test]
    async fn a_job_is_found_by_its_id() {
        let (addr, _lone, docs, _dir) = served().await;
        for id in ["nope", "other~0190", "orders~0190", "orders~"] {
            let (code, v) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
            assert_eq!(code, 404, "{id}: {v}");
        }
        let mut job = SnapshotJobDoc::new(
            "orders~0190".into(),
            vec![TableRef::new("public", "orders")],
            "ops".into(),
            Utc::now(),
        );
        job.tables[0].status = JobTableStatus::Failed;
        job.tables[0].error = Some("no such table".into());
        job.settle(Utc::now());
        docs.create_snapshot_job(&job).await.unwrap();
        let (code, v) = json_of(addr, "GET", "/v1/snapshots/orders~0190").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["status"], "failed");
        assert_eq!(
            v["error"],
            "1 of 1 tables failed: public.orders (no such table)"
        );
        assert!(v["finished_at"].is_string(), "{v}");
        assert_eq!(
            json_of(addr, "GET", "/v1/pipelines/nope/snapshots").await.0,
            404
        );
    }
}
