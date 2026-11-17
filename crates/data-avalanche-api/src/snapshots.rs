//! The on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). A job is asked of a running
//! pipeline whose lease a node holds, as a restart is (Q-M9b-4): its document is written
//! `queued`, then a restart under the job's id, which the owner takes at its next poll. The new
//! worker's start takes the job's tables and the owner follows them to their end. The job's id
//! is `<pipeline id>~<suffix>` (Q-M9-2), so any node reads a job by its id from the pipeline's
//! state.

use std::collections::BTreeSet;

use axum::Extension;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use data_avalanche_config::{Pipeline, SourceType, key_table};
use data_avalanche_core::TableRef;
use data_avalanche_state::{
    Action, DesiredState, JobCancel, JobStatus, JobTable, JobTableStatus, PipelineDocs, Request,
    SnapshotJobDoc, StateError,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::pipelines::{desired_name, find, read, unavailable};
use crate::{Api, Failure, Node, Principal, body, starting};

/// Between a job's pipeline id and its suffix: a pipeline id has none (V-055).
const SEPARATOR: char = '~';

/// A new job id: the pipeline's, then a UUID v7, time-ordered.
fn new_job_id(pipeline_id: &str) -> String {
    format!("{pipeline_id}{SEPARATOR}{}", uuid::Uuid::now_v7().simple())
}

/// The pipeline id a job id names.
pub(crate) fn job_pipeline(job_id: &str) -> Option<&str> {
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
    /// Empty each table's sink table before its copy (DA-SNAP-15).
    #[serde(default)]
    truncate: bool,
    /// Why, kept with the job and its events (checked by the recording, Admin_Console §5).
    #[serde(default)]
    note: Option<String>,
}

/// The body's tables, each once, in its order: named as the source's per-table config keys,
/// no globs; and whether the job empties them first.
fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool, Option<String>), String> {
    let asked: Asked = serde_json::from_str(text).map_err(|e| {
        format!("the body is {{\"tables\": [\"<table>\", ...], \"truncate\": false}}: {e}")
    })?;
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
    // The recording checked it already (a 422 before this route runs).
    let note = asked
        .note
        .map(|n| n.trim().to_owned())
        .filter(|n| !n.is_empty());
    Ok((out, asked.truncate, note))
}

/// A job's status as the API names it: `cancelling` from the cancel until a start ends it.
fn status_name(j: &SnapshotJobDoc) -> &'static str {
    match j.status {
        _ if j.cancel.is_some() && !j.is_finished() => "cancelling",
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
}

/// What lake readers see of a job's table (Admin_Console §3): `complete` once it is copied;
/// while a truncating job's copy of it began and hasn't completed, `empty` before its first
/// rows and `partial` after; else `as_before`, the rows it held before the job (a copy that
/// doesn't truncate writes over them).
fn lake(truncate: bool, t: &JobTable) -> &'static str {
    let began = t.started_at.is_some()
        || matches!(
            t.status,
            JobTableStatus::Running | JobTableStatus::Completed
        );
    match t.status {
        JobTableStatus::Completed => "complete",
        _ if !truncate || !began => "as_before",
        _ if t.rows_done == 0 => "empty",
        _ => "partial",
    }
}

/// The least a job runs before its rate is shown: a shorter run's rate says little.
const RATE_AFTER: chrono::TimeDelta = chrono::TimeDelta::seconds(60);

/// A running job's copy rate, rows a second, since its first table began, and the seconds left
/// at that rate from the source's estimates of its unended tables. No rate before
/// [`RATE_AFTER`] or once it ended; no ETA without a rate or an estimate of every unended table.
fn pace(j: &SnapshotJobDoc, now: DateTime<Utc>) -> (Option<u64>, Option<u64>) {
    if j.is_finished() {
        return (None, None);
    }
    let Some(first) = j.tables.iter().filter_map(|t| t.started_at).min() else {
        return (None, None);
    };
    let ran = now - first;
    if ran < RATE_AFTER {
        return (None, None);
    }
    let done: u64 = j.tables.iter().map(|t| t.rows_done).sum();
    let rate = done * 1000
        / u64::try_from(ran.num_milliseconds())
            .unwrap_or(u64::MAX)
            .max(1);
    if rate == 0 {
        return (Some(0), None);
    }
    let left: Option<u64> = j
        .tables
        .iter()
        .filter(|t| !t.status.is_ended())
        .map(|t| t.estimate.map(|e| e.saturating_sub(t.rows_done)))
        .sum();
    (Some(rate), left.map(|l| l.div_ceil(rate)))
}

fn job_json(p: &Pipeline, j: &SnapshotJobDoc, now: DateTime<Utc>) -> Value {
    let table_status = |s: JobTableStatus| match s {
        JobTableStatus::Queued => "queued",
        JobTableStatus::Running => "running",
        JobTableStatus::Completed => "completed",
        JobTableStatus::Failed => "failed",
        JobTableStatus::Cancelled => "cancelled",
    };
    let (rate, eta) = pace(j, now);
    json!({
        "job_id": j.job_id,
        "pipeline": p.id,
        "status": status_name(j),
        "tables": j.tables.iter().map(|t| json!({
            "table": table_name(p, &t.table),
            "status": table_status(t.status),
            "rows_done": t.rows_done,
            "estimate": t.estimate,
            "started_at": t.started_at,
            "ended_at": t.ended_at,
            "from_halt": t.from_halt,
            "lake": lake(j.truncate, t),
            "error": t.error,
        })).collect::<Vec<_>>(),
        "rows_done": j.tables.iter().map(|t| t.rows_done).sum::<u64>(),
        // The job's total only when every table has an estimate: a short total reads as the job's.
        "estimate": (!j.tables.is_empty())
            .then(|| j.tables.iter().map(|t| t.estimate).sum::<Option<u64>>())
            .flatten(),
        "rate": rate,
        "eta_seconds": eta,
        "requested_at": j.requested_at,
        "requested_by": j.requested_by,
        "requested_role": j.requested_role,
        "note": j.note,
        "retry_of": j.retry_of,
        "cancel": j.cancel.as_ref().map(|c| json!({
            "by": c.by,
            "role": c.role,
            "at": c.at,
            "note": c.note,
        })),
        "finished_at": j.finished_at,
        "error": j.error,
        "truncate": j.truncate,
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
    let (tables, truncate, note) = match tables(&p, &text) {
        Ok(asked) => asked,
        Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
    };
    let start = Start {
        tables,
        truncate,
        note,
        retry_of: None,
    };
    start_job(node, &p, &docs, &by, start).await
}

/// A job to start: its tables, whether it empties them first, why, and the job it retries.
struct Start {
    tables: Vec<TableRef>,
    truncate: bool,
    note: Option<String>,
    retry_of: Option<String>,
}

/// Records job `start` of pipeline `p`, asked by `by`, then its restart: 202 with the job. 409
/// when the pipeline doesn't run, no node holds its lease, or an unfinished job holds a table.
async fn start_job(
    node: &Node,
    p: &Pipeline,
    docs: &PipelineDocs,
    by: &Principal,
    start: Start,
) -> Response {
    let Start {
        tables,
        truncate,
        note,
        retry_of,
    } = start;
    let r = match read(node, p, docs).await {
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
    // Two asks of one table at once can both pass this check, and take no lock for it: both
    // jobs run, the start reads the table once, and both complete (D-M9b.4).
    for j in &unfinished {
        if let Some(t) = j.tables.iter().find(|t| {
            matches!(t.status, JobTableStatus::Queued | JobTableStatus::Running)
                && tables.contains(&t.table)
        }) {
            let queued = if j.status == JobStatus::Queued {
                // Its restart may not have been written (DA-SNAP-21): ask it again, under the
                // job's id, so the owner takes it at its next poll.
                match ask_restart_again(docs, j).await {
                    Ok(()) => " (it is queued: its restart is asked again, and it runs at the pipeline's next start)".to_owned(),
                    Err(e) => format!(" (it is queued, and its restart can't be asked again ({e}): it runs at the pipeline's next start)"),
                }
            } else {
                String::new()
            };
            return conflict(format!(
                "job {} snapshots {} already; ask again once it ended{queued}",
                j.job_id,
                table_name(p, &t.table)
            ));
        }
    }
    let mut job = SnapshotJobDoc::new(new_job_id(&p.id), tables, by.name.clone(), Utc::now());
    job.truncate = truncate;
    job.note = note;
    job.requested_role = Some(by.role.to_string());
    job.retry_of = retry_of;
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
    tracing::info!(pipeline = %p.name, by = %by.name, job = %job.job_id, tables = job.tables.len(), truncate, retry_of = ?job.retry_of, "snapshot job asked through the API");
    let v = job_json(p, &job, Utc::now());
    let mut answer = json!({
        "job_id": v["job_id"],
        "status": v["status"],
        "tables": v["tables"].as_array().into_iter().flatten().map(|t| t["table"].clone()).collect::<Vec<_>>(),
        "truncate": truncate,
    });
    if let Some(of) = &job.retry_of {
        answer["retry_of"] = json!(of);
    }
    body(StatusCode::ACCEPTED, &answer)
}

/// The job `job_id` names, with its pipeline and that pipeline's state: 404 when the id names
/// no pipeline of the generation, or no job of it; 503 when the state can't be read.
async fn job_of(
    node: &Node,
    job_id: &str,
) -> Result<(Pipeline, PipelineDocs, SnapshotJobDoc), Failure> {
    let missing = || Failure::new(StatusCode::NOT_FOUND, format!("no snapshot job {job_id:?}"));
    let pipeline = job_pipeline(job_id).ok_or_else(missing)?;
    let (p, docs) = find(node, pipeline).await.map_err(|f| match f.code {
        StatusCode::NOT_FOUND => missing(),
        _ => f,
    })?;
    match docs.snapshot_job(job_id).await {
        Ok(Some(v)) => Ok((p, docs, v.doc)),
        Ok(None) => Err(missing()),
        Err(e) => Err(Failure::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("the pipeline's state: {e}"),
        )),
    }
}

/// A body of a note alone, or none.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelBody {
    /// Checked by the recording (a 422 before this route runs).
    #[serde(default)]
    note: Option<String>,
}

/// A JSON body as `T`, an empty one as `T`'s default: 422 with `form` when it isn't one.
fn parsed<T: serde::de::DeserializeOwned>(text: &str, form: &str) -> Result<T, Failure> {
    let text = if text.trim().is_empty() { "{}" } else { text };
    serde_json::from_str(text).map_err(|e| {
        Failure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("the body is {form}: {e}"),
        )
    })
}

fn trimmed(note: Option<String>) -> Option<String> {
    note.map(|n| n.trim().to_owned()).filter(|n| !n.is_empty())
}

/// `POST /v1/snapshots/{job_id}/cancel` (D-M13c.2): records who cancelled the job and why, then
/// asks the pipeline's restart, whose start ends the job's unended tables; a copy of a table the
/// job brought back from a halt halts it again, `COPY_CANCELLED`. A pipeline stopped, or whose
/// lease no node holds, ends them at its next start. 202 `cancelling`; 200 when it was
/// cancelled already; 409 once it ended.
pub(crate) async fn cancel(
    State(api): State<Api>,
    Path(job_id): Path<String>,
    Extension(by): Extension<Principal>,
    text: String,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs, job) = match job_of(node, &job_id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    let note = match parsed::<CancelBody>(&text, "{\"note\": \"...\"} or empty") {
        Ok(n) => trimmed(n.note),
        Err(f) => return f.into(),
    };
    let ended = |j: &SnapshotJobDoc| {
        body(
            StatusCode::CONFLICT,
            &json!({
                "error": format!("job {job_id} ended ({}) already: there is nothing to cancel", status_name(j)),
                "job_id": job_id,
                "status": status_name(j),
            }),
        )
    };
    if job.is_finished() {
        return ended(&job);
    }
    if job.cancel.is_some() {
        return body(
            StatusCode::OK,
            &json!({ "job_id": job_id, "status": "cancelling", "unchanged": true }),
        );
    }
    let cancel = JobCancel {
        by: by.name.clone(),
        role: by.role.to_string(),
        at: Utc::now(),
        note,
    };
    let updated = docs
        .update_snapshot_job(&job_id, |j| {
            if !j.is_finished() && j.cancel.is_none() {
                j.cancel = Some(cancel.clone());
            }
        })
        .await;
    match updated {
        Ok(Some(j)) if j.is_finished() => return ended(&j),
        Ok(Some(_)) => {}
        Ok(None) => {
            return Failure::new(StatusCode::NOT_FOUND, format!("no snapshot job {job_id:?}"))
                .into();
        }
        Err(e) => return unavailable(&e),
    }
    // A worker runs while the pipeline runs or is paused: its restart takes the cancel now.
    let r = match read(node, &p, &docs).await {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::warn!(pipeline = %p.name, job = %job_id, "a cancel's restart unasked, the pipeline's state unread: {e}");
            None
        }
    };
    let runs = r.as_ref().is_some_and(|r| {
        r.desired != DesiredState::Stopped && r.lease.as_ref().is_some_and(|l| !l.released)
    });
    let restart = Request {
        id: format!("{job_id}/cancel"),
        at: cancel.at,
    };
    let asked = runs
        && {
            match docs.request(Action::Restart, restart).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(pipeline = %p.name, job = %job_id, "a cancel's restart unasked: {e}");
                    false
                }
            }
        };
    tracing::info!(pipeline = %p.name, by = %by.name, job = %job_id, restart = asked, "snapshot job cancelled through the API");
    let detail = if asked {
        "the owner restarts the pipeline's worker, whose start stops the job's copies"
    } else {
        "no worker runs the pipeline now: its next start stops the job's copies"
    };
    body(
        StatusCode::ACCEPTED,
        &json!({ "job_id": job_id, "status": "cancelling", "restart": asked, "detail": detail }),
    )
}

/// Which of an ended job's tables a retry copies again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Which {
    All,
    /// Those that didn't complete.
    #[default]
    NotCopied,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retried {
    #[serde(default)]
    tables: Which,
    /// The old job's when absent.
    #[serde(default)]
    truncate: Option<bool>,
    #[serde(default)]
    note: Option<String>,
}

/// `POST /v1/snapshots/{job_id}/retry`: a new job of an ended one's tables (`all`, or
/// `not_copied`, the default), truncating as it did unless the body says, recording the job it
/// retries; asked as `POST /v1/pipelines/{id}/snapshots` is. 409 for a job that hasn't ended,
/// or with no table left to copy.
pub(crate) async fn retry(
    State(api): State<Api>,
    Path(job_id): Path<String>,
    Extension(by): Extension<Principal>,
    text: String,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs, job) = match job_of(node, &job_id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    let form =
        "{\"tables\": \"all\" | \"not_copied\", \"truncate\": true | false, \"note\": \"...\"}";
    let asked = match parsed::<Retried>(&text, form) {
        Ok(a) => a,
        Err(f) => return f.into(),
    };
    let conflict = |why: String| {
        body(
            StatusCode::CONFLICT,
            &json!({ "error": why, "job_id": job_id, "status": status_name(&job) }),
        )
    };
    if !job.is_finished() {
        return conflict(format!(
            "job {job_id} is {}: retry it once it ended",
            status_name(&job)
        ));
    }
    let tables: Vec<TableRef> = job
        .tables
        .iter()
        .filter(|t| asked.tables == Which::All || t.status != JobTableStatus::Completed)
        .map(|t| t.table.clone())
        .collect();
    if tables.is_empty() {
        return conflict(format!(
            "every table of job {job_id} was copied; retry it with \"tables\": \"all\" to copy them again"
        ));
    }
    let start = Start {
        tables,
        truncate: asked.truncate.unwrap_or(job.truncate),
        note: trimmed(asked.note),
        retry_of: Some(job_id.clone()),
    };
    start_job(node, &p, &docs, &by, start).await
}

/// Writes queued job `j`'s restart again, unless it is the restart recorded already.
async fn ask_restart_again(docs: &PipelineDocs, j: &SnapshotJobDoc) -> Result<(), StateError> {
    let restart = Request {
        id: j.job_id.clone(),
        at: j.requested_at,
    };
    if docs.requests().await?.restart.as_ref() == Some(&restart) {
        return Ok(());
    }
    docs.request(Action::Restart, restart).await
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
    let now = Utc::now();
    match docs.snapshot_jobs().await {
        Ok(jobs) => body(
            StatusCode::OK,
            &json!({ "jobs": jobs.iter().map(|v| job_json(&p, &v.doc, now)).collect::<Vec<_>>() }),
        ),
        Err(e) => unavailable(&e),
    }
}

/// `GET /v1/snapshots/{job_id}`: the job, read from the state of the pipeline its id names.
pub(crate) async fn get(State(api): State<Api>, Path(job_id): Path<String>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    match job_of(node, &job_id).await {
        Ok((p, _, job)) => body(StatusCode::OK, &job_json(&p, &job, Utc::now())),
        Err(f) => f.into(),
    }
}

/// How many pipelines' jobs `GET /v1/snapshots` reads at once.
pub const READ_AT_ONCE: usize = 16;

/// The statuses a job is listed by.
const STATUSES: [&str; 6] = [
    "queued",
    "running",
    "cancelling",
    "completed",
    "failed",
    "cancelled",
];

/// `GET /v1/snapshots`'s query.
pub(crate) const PARAMETERS: &[(&str, &str)] = &[
    (
        "status",
        "queued, running, cancelling, completed, failed or cancelled; several comma-separated",
    ),
    ("pipeline", "a pipeline's id"),
    ("by", "who asked for it"),
];

/// The ended snapshot jobs `GET /v1/snapshots` read, by pipeline and key: an ended job doesn't
/// change, so it is read once. A job a later listing lacks (pruned) goes.
#[derive(Debug, Default)]
pub struct EndedJobs(
    std::sync::Mutex<std::collections::BTreeMap<(String, String), SnapshotJobDoc>>,
);

impl EndedJobs {
    fn known(&self, pipeline: &str, key: &str) -> Option<SnapshotJobDoc> {
        let kept = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        kept.get(&(pipeline.to_owned(), key.to_owned())).cloned()
    }

    /// Keeps pipeline `pipeline`'s ended jobs of `listed`, and only those.
    fn keep(&self, pipeline: &str, listed: &[(String, SnapshotJobDoc)]) {
        let mut kept = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        kept.retain(|(p, _), _| p != pipeline);
        for (key, job) in listed.iter().filter(|(_, j)| j.is_finished()) {
            kept.insert((pipeline.to_owned(), key.clone()), job.clone());
        }
    }
}

/// `GET /v1/snapshots`'s filter: the statuses, the pipeline, who asked.
#[derive(Debug, Default)]
struct Filter {
    statuses: Vec<String>,
    pipeline: Option<String>,
    by: Option<String>,
}

fn filter(params: &[(String, String)]) -> Result<Filter, Failure> {
    let bad = |why: String| Failure::new(StatusCode::BAD_REQUEST, why);
    let mut f = Filter::default();
    let mut seen = Vec::new();
    for (name, value) in params {
        if seen.contains(name) {
            return Err(bad(format!("{name} is given twice")));
        }
        seen.push(name.clone());
        let value = value.trim().to_owned();
        match name.as_str() {
            "status" => {
                for s in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    if !STATUSES.contains(&s) {
                        return Err(bad(format!(
                            "status {s:?} isn't one of {}",
                            STATUSES.join(", ")
                        )));
                    }
                    f.statuses.push(s.to_owned());
                }
            }
            "pipeline" => f.pipeline = Some(value),
            "by" => f.by = Some(value),
            other => {
                return Err(bad(format!(
                    "unknown parameter {other:?}: the jobs take status, pipeline, by"
                )));
            }
        }
    }
    Ok(f)
}

/// Pipeline `p`'s jobs, each with its key, the ended ones [`EndedJobs`] keeps not read again.
async fn jobs_of(node: &Node, p: &Pipeline) -> Result<Vec<(String, SnapshotJobDoc)>, String> {
    let docs = (node.docs)(p).map_err(|e| format!("the state location: {e}"))?;
    let jobs = docs
        .snapshot_jobs_reusing(|key| node.ended_jobs.known(&p.id, key))
        .await
        .map_err(|e| e.to_string())?;
    node.ended_jobs.keep(&p.id, &jobs);
    Ok(jobs)
}

/// `GET /v1/snapshots`: the jobs of every pipeline of the generation, newest request first,
/// filtered; the pipelines read [`READ_AT_ONCE`] at a time. A pipeline whose jobs can't be read
/// is named under `unreadable`, and the others are listed.
pub(crate) async fn all(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
    use futures_util::StreamExt;
    let Some(node) = api.node.get() else {
        return starting();
    };
    let params: Vec<(String, String)> =
        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let f = match filter(&params) {
        Ok(f) => f,
        Err(f) => return f.into(),
    };
    let config = node.coordinator.config().await;
    let mut pipelines: Vec<&Pipeline> = config
        .pipelines
        .values()
        .filter(|p| f.pipeline.as_ref().is_none_or(|id| &p.id == id))
        .collect();
    if let Some(id) = &f.pipeline
        && pipelines.is_empty()
    {
        return Failure::new(
            StatusCode::NOT_FOUND,
            format!("no pipeline {id:?} in the config generation this node runs"),
        )
        .into();
    }
    pipelines.sort_by(|a, b| a.id.cmp(&b.id));
    // Owned: a stream over references doesn't make a `Send` future (rust-lang/rust#64552).
    let owned: Vec<Pipeline> = pipelines.iter().map(|p| (*p).clone()).collect();
    let read = futures_util::stream::iter(owned)
        .map(|p| async move { jobs_of(node, &p).await })
        .buffered(READ_AT_ONCE)
        .collect::<Vec<_>>()
        .await;
    let now = Utc::now();
    let mut jobs = vec![];
    let mut unreadable = vec![];
    for (p, got) in pipelines.into_iter().zip(read) {
        match got {
            Ok(listed) => jobs.extend(listed.into_iter().map(|(_, j)| (p, j))),
            Err(error) => unreadable.push(json!({ "pipeline": p.id, "error": error })),
        }
    }
    jobs.retain(|(_, j)| {
        (f.statuses.is_empty() || f.statuses.iter().any(|s| s == status_name(j)))
            && f.by.as_ref().is_none_or(|by| &j.requested_by == by)
    });
    jobs.sort_by(|(_, a), (_, b)| (b.requested_at, &b.job_id).cmp(&(a.requested_at, &a.job_id)));
    body(
        StatusCode::OK,
        &json!({
            "jobs": jobs.iter().map(|(p, j)| job_json(p, j, now)).collect::<Vec<_>>(),
            "unreadable": unreadable,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use chrono::Utc;
    use data_avalanche_core::TableRef;
    use data_avalanche_state::{
        Acquired, DesiredState, Holder, JobCancel, JobStatus, JobTable, JobTableStatus,
        PipelineDocs, SnapshotJobDoc,
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
        assert!(!job.truncate, "a job keeps the rows unless asked");
        assert_eq!(v["truncate"], false);

        let (code, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
        assert_eq!(code, 200, "{one}");
        assert_eq!(one["job_id"], id.as_str());
        assert_eq!(one["pipeline"], "orders");
        assert_eq!(one["status"], "queued");
        assert_eq!(
            one["tables"][0],
            json!({
                "table": "public.orders",
                "status": "queued",
                "rows_done": 0,
                "estimate": null,
                "started_at": null,
                "ended_at": null,
                "from_halt": null,
                "lake": "as_before",
                "error": null,
            })
        );
        assert_eq!(one["requested_role"], "admin", "auth none: an admin");
        for unset in [
            "rate",
            "eta_seconds",
            "estimate",
            "retry_of",
            "cancel",
            "note",
        ] {
            assert_eq!(one[unset], Value::Null, "{unset}: {one}");
        }
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
        assert!(error.contains("its restart is asked again"), "queued: {v}");
        // DA-SNAP-21: a job whose restart was lost gets it again when its table is asked.
        let other = data_avalanche_state::Request {
            id: "someone-else".into(),
            at: Utc::now(),
        };
        docs.request(data_avalanche_state::Action::Restart, other)
            .await
            .unwrap();
        let (code, _) = ask(addr, r#"{"tables": ["public.orders"]}"#).await;
        assert_eq!(code, 409);
        assert_eq!(docs.requests().await.unwrap().restart.unwrap().id, first);
        docs.update_snapshot_job(&first, |j| {
            j.tables[0].status = JobTableStatus::Running;
            j.settle(Utc::now());
        })
        .await
        .unwrap();
        let (code, v) = ask(addr, r#"{"tables": ["public.orders"]}"#).await;
        assert_eq!(code, 409, "{v}");
        assert_eq!(
            v["error"],
            format!("job {first} snapshots public.orders already; ask again once it ended")
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

    /// DA-SNAP-15: `truncate: true` is recorded with the job and shown with it.
    #[tokio::test]
    async fn a_job_asked_with_truncate_records_it() {
        let (addr, lone, docs, _dir) = served().await;
        held(&lone).await;
        let (code, v) = ask(addr, r#"{"tables": ["public.orders"], "truncate": true}"#).await;
        assert_eq!(code, 202, "{v}");
        assert_eq!(v["truncate"], true);
        let id = v["job_id"].as_str().unwrap();
        assert!(docs.snapshot_job(id).await.unwrap().unwrap().doc.truncate);
        let (_, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
        assert_eq!(one["truncate"], true, "{one}");
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
            r#"{"tables": ["public.orders"], "truncate": "yes"}"#,
            r#"{"tables": ["public.orders"], "drop": true}"#,
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

    /// A job whose pipeline's state can't be read is a failure of the node, not a job missing.
    #[tokio::test]
    async fn a_job_whose_state_can_t_be_read_isn_t_a_404() {
        let (_, lone, _docs, _dir) = served().await;
        let mut node = lone.node.clone();
        node.docs = std::sync::Arc::new(|_| Err("no bucket".into()));
        let a = api(open());
        a.node.set(node).unwrap();
        let addr = serve(a).await;
        let (code, v) = json_of(addr, "GET", "/v1/snapshots/orders~0190").await;
        assert_eq!(code, 500, "{v}");
        assert!(v["error"].as_str().unwrap().contains("no bucket"), "{v}");
    }

    async fn post(addr: SocketAddr, path: &str, body: &str) -> (u16, Value) {
        let (code, _, text) = request(addr, "POST", path, &[], body).await;
        let v = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
        };
        (code, v)
    }

    /// A job of `tables`, recorded as the API would, `status` each.
    async fn recorded(docs: &PipelineDocs, id: &str, tables: &[(&str, JobTableStatus)]) {
        let mut job = SnapshotJobDoc::new(
            id.into(),
            tables
                .iter()
                .map(|(n, _)| TableRef::new("public", *n))
                .collect(),
            "ops".into(),
            Utc::now(),
        );
        for (t, (_, s)) in job.tables.iter_mut().zip(tables) {
            t.status = *s;
        }
        job.settle(Utc::now());
        docs.create_snapshot_job(&job).await.unwrap();
    }

    /// D-M13c.2: a cancel records who cancelled the job and why, then asks the holder's
    /// restart, whose start ends it; the job reads `cancelling` until then. A second cancel
    /// changes nothing; one of a job that ended is refused.
    #[tokio::test]
    async fn a_job_is_cancelled_through_a_restart() {
        let (addr, lone, docs, _dir) = served().await;
        held(&lone).await;
        recorded(&docs, "orders~01", &[("orders", JobTableStatus::Running)]).await;
        let (code, v) = post(
            addr,
            "/v1/snapshots/orders~01/cancel",
            r#"{"note": " wrong table "}"#,
        )
        .await;
        assert_eq!(code, 202, "{v}");
        assert_eq!(v["status"], "cancelling");
        assert_eq!(v["restart"], true);
        assert_eq!(
            docs.requests().await.unwrap().restart.unwrap().id,
            "orders~01/cancel"
        );
        let job = docs.snapshot_job("orders~01").await.unwrap().unwrap().doc;
        let cancel = job.cancel.unwrap();
        assert_eq!(
            (cancel.by.as_str(), cancel.role.as_str()),
            ("anonymous", "admin")
        );
        assert_eq!(cancel.note.as_deref(), Some("wrong table"));
        assert_eq!(job.status, JobStatus::Running, "the start ends it");
        let (_, one) = json_of(addr, "GET", "/v1/snapshots/orders~01").await;
        assert_eq!(one["status"], "cancelling", "{one}");
        assert_eq!(one["cancel"]["by"], "anonymous");
        assert_eq!(one["cancel"]["note"], "wrong table");

        let (code, v) = post(addr, "/v1/snapshots/orders~01/cancel", "").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["unchanged"], true);

        docs.update_snapshot_job("orders~01", |j| {
            j.tables[0].end(JobTableStatus::Cancelled, None, Utc::now());
            j.settle(Utc::now());
        })
        .await
        .unwrap();
        let (_, one) = json_of(addr, "GET", "/v1/snapshots/orders~01").await;
        assert_eq!(one["status"], "cancelled", "{one}");
        recorded(&docs, "orders~02", &[("orders", JobTableStatus::Completed)]).await;
        let (code, v) = post(addr, "/v1/snapshots/orders~02/cancel", "").await;
        assert_eq!(code, 409, "{v}");
        assert!(
            v["error"].as_str().unwrap().contains("ended (completed)"),
            "{v}"
        );
    }

    /// D-M13c.2: no worker runs a stopped pipeline, nor one whose lease no node holds: the
    /// cancel is recorded, and its next start takes it. A paused pipeline's worker restarts.
    #[tokio::test]
    async fn a_cancel_restarts_only_a_worker_that_runs() {
        let (addr, lone, docs, _dir) = served().await;
        recorded(&docs, "orders~01", &[("orders", JobTableStatus::Running)]).await;
        let (code, v) = post(addr, "/v1/snapshots/orders~01/cancel", "").await;
        assert_eq!(code, 202, "{v}");
        assert_eq!(v["restart"], false, "no holder: {v}");
        assert!(docs.requests().await.unwrap().restart.is_none());
        assert!(
            docs.snapshot_job("orders~01")
                .await
                .unwrap()
                .unwrap()
                .doc
                .cancel
                .is_some()
        );

        held(&lone).await;
        docs.open("orders", "delta").await.unwrap();
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        recorded(&docs, "orders~02", &[("items", JobTableStatus::Queued)]).await;
        let (code, v) = post(addr, "/v1/snapshots/orders~02/cancel", "").await;
        assert_eq!((code, &v["restart"]), (202, &json!(false)), "stopped: {v}");
        assert!(docs.requests().await.unwrap().restart.is_none());

        docs.set_desired_state(DesiredState::Paused).await.unwrap();
        recorded(&docs, "orders~03", &[("customers", JobTableStatus::Queued)]).await;
        let (code, v) = post(addr, "/v1/snapshots/orders~03/cancel", "").await;
        assert_eq!((code, &v["restart"]), (202, &json!(true)), "paused: {v}");
    }

    /// A retry starts a new job of an ended one's tables, those not copied unless the body
    /// asks for all, truncating as the old job did unless it says, and records the job it
    /// retries. A job that hasn't ended, or left nothing to copy, is refused.
    #[tokio::test]
    async fn an_ended_job_is_retried_as_a_new_job() {
        let (addr, lone, docs, _dir) = served().await;
        held(&lone).await;
        recorded(
            &docs,
            "orders~01",
            &[
                ("orders", JobTableStatus::Completed),
                ("items", JobTableStatus::Failed),
            ],
        )
        .await;
        docs.update_snapshot_job("orders~01", |j| j.truncate = true)
            .await
            .unwrap();
        let (code, v) = post(addr, "/v1/snapshots/orders~01/retry", "").await;
        assert_eq!(code, 202, "{v}");
        assert_eq!(v["retry_of"], "orders~01");
        assert_eq!(v["tables"], json!(["public.items"]), "not copied: {v}");
        assert_eq!(v["truncate"], true, "as the old job");
        let id = v["job_id"].as_str().unwrap().to_owned();
        let job = docs.snapshot_job(&id).await.unwrap().unwrap().doc;
        assert_eq!(job.retry_of.as_deref(), Some("orders~01"));
        assert_eq!(job.requested_role.as_deref(), Some("admin"));
        assert_eq!(docs.requests().await.unwrap().restart.unwrap().id, id);
        let (_, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
        assert_eq!(one["retry_of"], "orders~01", "{one}");

        // The new job holds public.items until it ends.
        let (code, v) = post(
            addr,
            "/v1/snapshots/orders~01/retry",
            r#"{"tables": "all"}"#,
        )
        .await;
        assert_eq!(code, 409, "{v}");
        docs.update_snapshot_job(&id, |j| {
            j.tables[0].end(JobTableStatus::Completed, None, Utc::now());
            j.settle(Utc::now());
        })
        .await
        .unwrap();
        let (code, v) = post(
            addr,
            "/v1/snapshots/orders~01/retry",
            r#"{"tables": "all", "truncate": false, "note": "again"}"#,
        )
        .await;
        assert_eq!(code, 202, "{v}");
        assert_eq!(v["tables"], json!(["public.orders", "public.items"]));
        assert_eq!(v["truncate"], false);
        let again = v["job_id"].as_str().unwrap();
        let job = docs.snapshot_job(again).await.unwrap().unwrap().doc;
        assert_eq!(job.note.as_deref(), Some("again"));

        let (code, v) = post(addr, &format!("/v1/snapshots/{again}/retry"), "").await;
        assert_eq!(code, 409, "queued: {v}");
        assert!(
            v["error"].as_str().unwrap().contains("once it ended"),
            "{v}"
        );
        let (code, v) = post(addr, &format!("/v1/snapshots/{id}/retry"), "").await;
        assert_eq!(code, 409, "{v}");
        assert!(v["error"].as_str().unwrap().contains("was copied"), "{v}");
        for bad in [r#"{"tables": "some"}"#, r#"{"drop": true}"#, r#""x""#] {
            let (code, v) = post(addr, "/v1/snapshots/orders~01/retry", bad).await;
            assert_eq!(code, 422, "{bad}: {v}");
        }
        let (code, _) = post(addr, "/v1/snapshots/orders~09/retry", "").await;
        assert_eq!(code, 404);
        let (code, _) = post(addr, "/v1/snapshots/orders~09/cancel", "").await;
        assert_eq!(code, 404);
    }

    fn table(status: JobTableStatus, rows_done: u64, started: bool) -> JobTable {
        let mut t = JobTable::queued(TableRef::new("public", "a"));
        t.status = status;
        t.rows_done = rows_done;
        t.started_at = started.then(Utc::now);
        t
    }

    #[test]
    fn lake_says_what_lake_readers_see() {
        use JobTableStatus::{Cancelled, Completed, Failed, Queued, Running};
        for truncate in [false, true] {
            assert_eq!(
                super::lake(truncate, &table(Completed, 5, true)),
                "complete"
            );
            assert_eq!(super::lake(truncate, &table(Queued, 0, false)), "as_before");
            assert_eq!(
                super::lake(truncate, &table(Cancelled, 0, false)),
                "as_before",
                "cancelled before it began"
            );
        }
        assert_eq!(super::lake(false, &table(Running, 5, true)), "as_before");
        assert_eq!(super::lake(true, &table(Running, 0, true)), "empty");
        assert_eq!(super::lake(true, &table(Running, 5, true)), "partial");
        assert_eq!(super::lake(true, &table(Cancelled, 5, true)), "partial");
        assert_eq!(super::lake(true, &table(Failed, 0, true)), "empty");
        assert_eq!(
            super::lake(true, &table(Running, 5, false)),
            "partial",
            "a job a 1.0 node started records no start"
        );
    }

    #[test]
    fn a_job_s_pace_is_its_rate_and_the_time_its_estimates_leave() {
        let t0 = Utc::now();
        let mut job = SnapshotJobDoc::new(
            "orders~01".into(),
            vec![
                TableRef::new("public", "a"),
                TableRef::new("public", "b"),
                TableRef::new("public", "c"),
            ],
            "ops".into(),
            t0,
        );
        for (t, (status, rows, estimate)) in job.tables.iter_mut().zip([
            (JobTableStatus::Completed, 600, 600),
            (JobTableStatus::Running, 400, 1000),
            (JobTableStatus::Running, 0, 500),
        ]) {
            t.status = status;
            t.rows_done = rows;
            t.estimate = Some(estimate);
            t.started_at = Some(t0);
        }
        job.settle(t0);
        let at = |s: i64| t0 + chrono::TimeDelta::seconds(s);
        assert_eq!(super::pace(&job, at(59)), (None, None), "under a minute");
        // 1000 rows in 100 s; (1000 - 400) + 500 left.
        assert_eq!(super::pace(&job, at(100)), (Some(10), Some(110)));
        job.tables[2].estimate = None;
        assert_eq!(
            super::pace(&job, at(100)),
            (Some(10), None),
            "an unended table without an estimate"
        );
        job.tables[1].end(JobTableStatus::Cancelled, None, at(100));
        job.tables[2].end(JobTableStatus::Cancelled, None, at(100));
        job.settle(at(100));
        assert_eq!(super::pace(&job, at(200)), (None, None), "ended");
    }

    /// A job's estimate is the sum of its tables' estimates when every table has one, else none:
    /// a total that leaves a table out would read short. Each table's estimate is shown as stored.
    #[tokio::test]
    async fn a_job_s_estimate_is_none_unless_every_table_has_one() {
        let (addr, _lone, docs, _dir) = served().await;
        recorded(
            &docs,
            "orders~01",
            &[
                ("orders", JobTableStatus::Queued),
                ("items", JobTableStatus::Queued),
            ],
        )
        .await;
        docs.update_snapshot_job("orders~01", |j| {
            j.tables[0].estimate = Some(600);
            j.tables[1].estimate = Some(400);
        })
        .await
        .unwrap();
        let (_, one) = json_of(addr, "GET", "/v1/snapshots/orders~01").await;
        assert_eq!(one["estimate"], 1000, "{one}");
        assert_eq!(one["tables"][0]["estimate"], 600, "{one}");
        assert_eq!(one["tables"][1]["estimate"], 400, "{one}");

        docs.update_snapshot_job("orders~01", |j| j.tables[1].estimate = None)
            .await
            .unwrap();
        let (_, one) = json_of(addr, "GET", "/v1/snapshots/orders~01").await;
        assert_eq!(one["estimate"], Value::Null, "one table has none: {one}");
        assert_eq!(one["tables"][0]["estimate"], 600, "{one}");
        assert_eq!(one["tables"][1]["estimate"], Value::Null, "{one}");
    }

    /// [`CONFIG`] and a second pipeline, `billing`, its state elsewhere.
    async fn served_two() -> (SocketAddr, Lone, PipelineDocs, PipelineDocs, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        let two = format!(
            "{CONFIG}  billing:
    id: billing
    source:
      host: db.example.com
      db: billing
      user: cdc
      password: pw
      postgres: {{ publication: billing_pub, slot: billing_slot }}
    tables:
      include: [public.invoices]
    sink:
      type: delta
      bucket: lake
      prefix: bill
"
        );
        std::fs::write(&file, &two).unwrap();
        let env = std::sync::Arc::new(data_avalanche_config::ProcessEnv);
        let lone = crate::test_support::lone_with(file, &two, env).await;
        let config = lone.node.coordinator.config().await;
        let orders = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
        let billing = (lone.node.docs)(&config.pipelines["billing"]).unwrap();
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        (serve(a).await, lone, orders, billing, dir)
    }

    async fn listed(addr: SocketAddr, query: &str) -> (u16, Value) {
        json_of(addr, "GET", &format!("/v1/snapshots{query}")).await
    }

    fn ids(v: &Value) -> Vec<String> {
        v["jobs"]
            .as_array()
            .unwrap_or_else(|| panic!("{v}"))
            .iter()
            .map(|j| {
                format!(
                    "{} {}",
                    j["job_id"].as_str().unwrap(),
                    j["status"].as_str().unwrap()
                )
            })
            .collect()
    }

    /// Every pipeline's jobs, newest request first, filtered by status (several at once),
    /// pipeline and who asked.
    #[tokio::test]
    async fn the_jobs_of_every_pipeline_are_listed_and_filtered() {
        let (addr, _lone, orders, billing, _dir) = served_two().await;
        recorded(
            &orders,
            "orders~01",
            &[("orders", JobTableStatus::Completed)],
        )
        .await;
        recorded(
            &billing,
            "billing~02",
            &[("invoices", JobTableStatus::Running)],
        )
        .await;
        recorded(&orders, "orders~03", &[("orders", JobTableStatus::Queued)]).await;
        orders
            .update_snapshot_job("orders~03", |j| {
                j.cancel = Some(JobCancel {
                    by: "ops".into(),
                    role: "operator".into(),
                    at: Utc::now(),
                    note: None,
                });
            })
            .await
            .unwrap();
        let (code, v) = listed(addr, "").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(
            ids(&v),
            [
                "orders~03 cancelling",
                "billing~02 running",
                "orders~01 completed"
            ]
        );
        assert_eq!(v["jobs"][1]["pipeline"], "billing");
        assert_eq!(v["unreadable"], json!([]));
        let (_, v) = listed(addr, "?status=running,cancelling").await;
        assert_eq!(ids(&v), ["orders~03 cancelling", "billing~02 running"]);
        let (_, v) = listed(addr, "?pipeline=billing").await;
        assert_eq!(ids(&v), ["billing~02 running"]);
        let (_, v) = listed(addr, "?by=ops&status=completed").await;
        assert_eq!(ids(&v), ["orders~01 completed"]);
        let (_, v) = listed(addr, "?by=someone").await;
        assert!(ids(&v).is_empty());
        assert_eq!(listed(addr, "?pipeline=nope").await.0, 404);
        for bad in ["?status=done", "?colour=red", "?by=a&by=b"] {
            let (code, v) = listed(addr, bad).await;
            assert_eq!(code, 400, "{bad}: {v}");
        }
    }

    /// An ended job is read once: it doesn't change. A running one is read each time, and an
    /// ended one pruned goes.
    #[tokio::test]
    async fn an_ended_job_is_read_once_and_goes_once_pruned() {
        let (addr, _lone, orders, billing, _dir) = served_two().await;
        recorded(
            &orders,
            "orders~01",
            &[("orders", JobTableStatus::Completed)],
        )
        .await;
        recorded(
            &billing,
            "billing~02",
            &[("invoices", JobTableStatus::Running)],
        )
        .await;
        assert_eq!(listed(addr, "").await.1["jobs"][1]["requested_by"], "ops");
        for (docs, id) in [(&orders, "orders~01"), (&billing, "billing~02")] {
            docs.update_snapshot_job(id, |j| j.requested_by = "changed".into())
                .await
                .unwrap();
        }
        let (_, v) = listed(addr, "").await;
        assert_eq!(v["jobs"][0]["requested_by"], "changed", "running: {v}");
        assert_eq!(v["jobs"][1]["requested_by"], "ops", "ended, kept: {v}");
        assert_eq!(orders.prune_snapshot_jobs(0).await.unwrap(), 1);
        let (_, v) = listed(addr, "").await;
        assert_eq!(ids(&v), ["billing~02 running"]);
    }

    /// A pipeline whose jobs can't be read is named; the others' are listed.
    #[tokio::test]
    async fn a_pipeline_whose_jobs_can_t_be_read_is_named() {
        let (_, lone, orders, _billing, _dir) = served_two().await;
        recorded(
            &orders,
            "orders~01",
            &[("orders", JobTableStatus::Completed)],
        )
        .await;
        let mut node = lone.node.clone();
        let docs = std::sync::Arc::clone(&node.docs);
        node.docs = std::sync::Arc::new(move |p| match p.id.as_str() {
            "billing" => Err("no bucket".into()),
            _ => docs(p),
        });
        let a = api(open());
        a.node.set(node).unwrap();
        let addr = serve(a).await;
        let (code, v) = listed(addr, "").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(ids(&v), ["orders~01 completed"]);
        assert_eq!(v["unreadable"][0]["pipeline"], "billing");
        assert!(
            v["unreadable"][0]["error"]
                .as_str()
                .unwrap()
                .contains("no bucket"),
            "{v}"
        );
    }
}
