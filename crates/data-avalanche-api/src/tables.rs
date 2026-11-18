//! The tables (Admin_Console §4 Tables, D-M13d.5, D-M13d.7). Every pipeline's tables, each as
//! its lease holder's worker shows it, or from storage when the holder can't be asked. A table's
//! page: what its pipeline's worker shows of it, its halt with the time it began, the Delta
//! table's facts read on request, its columns at the source against Delta's, and the snapshot
//! jobs that copied it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_schema::{Schema, SchemaRef};
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use data_avalanche_cluster::WorkerFactory;
use data_avalanche_config::{Pipeline, SourceType};
use data_avalanche_core::{
    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, LOGICAL_TAG, TableFacts, TableRef, TableSchema,
};
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use serde_json::{Value, json};

use crate::live::live;
use crate::pipelines::{find, unavailable};
use crate::snapshots::{
    READ_AT_ONCE, jobs_of, lake, named_table, status_name, table_name, table_status,
};
use crate::{Api, Failure, Node, body, starting};

/// How long a table's Delta facts and columns, once read, answer for it: the table's page reads
/// them on request, and a reload of the page within this reads nothing again.
pub const FACTS_FOR: Duration = Duration::from_secs(30);

/// How long the table's page waits for its Delta read: past it the page answers without the
/// facts (`delta_error`), and the read goes on, kept for the next request once it ends.
pub const FACTS_WITHIN: Duration = Duration::from_secs(10);

/// What the sink holds of a table, read at `at`.
#[derive(Debug, Clone)]
struct Read {
    facts: Option<TableFacts>,
    schema: Option<SchemaRef>,
    at: DateTime<Utc>,
    /// The config generation it was read under: a reload may move the table.
    generation: u64,
    kept: Instant,
}

/// A read in flight, shared by every request that finds nothing kept.
type Reading = Shared<BoxFuture<'static, Result<Read, String>>>;

/// A read's key: the pipeline, the table, and the config generation it is read under.
type Key = (String, TableRef, u64);

/// The tables' Delta facts and columns this node read, each kept [`FACTS_FOR`], and the reads in
/// flight: one per table and generation, whoever waits for it.
#[derive(Debug)]
pub struct Lake {
    read: Mutex<HashMap<(String, TableRef), Read>>,
    reading: Mutex<HashMap<Key, Reading>>,
    /// How long a request waits for a read: [`FACTS_WITHIN`].
    within: Duration,
}

impl Default for Lake {
    fn default() -> Self {
        Self::with_bound(FACTS_WITHIN)
    }
}

impl Lake {
    /// A lake whose requests wait `within` for a read.
    #[must_use]
    pub fn with_bound(within: Duration) -> Self {
        Self {
            read: Mutex::default(),
            reading: Mutex::default(),
            within,
        }
    }

    fn kept(&self, id: &str, table: &TableRef, generation: u64) -> Option<Read> {
        let read = self.read.lock().expect("the lake's reads");
        read.get(&(id.to_owned(), table.clone()))
            .filter(|r| r.generation == generation && r.kept.elapsed() < FACTS_FOR)
            .cloned()
    }

    fn keep(&self, id: &str, table: &TableRef, r: Read) {
        let mut read = self.read.lock().expect("the lake's reads");
        read.retain(|_, r| r.kept.elapsed() < FACTS_FOR);
        read.insert((id.to_owned(), table.clone()), r);
    }

    /// The sink's facts and columns of `table`: kept ones while fresh, else the read in flight,
    /// else a new one through a sink built for the pipeline now. A request waits for it
    /// [`FACTS_WITHIN`]; the read runs on its own task, so neither the bound nor a dropped request
    /// ends it, and it is kept when it ends. A failed read isn't kept.
    async fn read(
        self: &Arc<Self>,
        factory: &Arc<dyn WorkerFactory>,
        p: &Pipeline,
        table: &TableRef,
        generation: u64,
    ) -> Result<Read, String> {
        if let Some(r) = self.kept(&p.id, table, generation) {
            return Ok(r);
        }
        let reading = self.reading(factory, p, table, generation);
        tokio::time::timeout(self.within, reading)
            .await
            .unwrap_or_else(|_| Err(format!("not read within {} s", self.within.as_secs_f64())))
    }

    /// The read of `table` in flight, started when none is.
    fn reading(
        self: &Arc<Self>,
        factory: &Arc<dyn WorkerFactory>,
        p: &Pipeline,
        table: &TableRef,
        generation: u64,
    ) -> Reading {
        let key = (p.id.clone(), table.clone(), generation);
        let mut reading = self.reading.lock().expect("the lake's reads in flight");
        if let Some(r) = reading.get(&key) {
            return r.clone();
        }
        let (lake, factory, p, table) = (
            Arc::clone(self),
            Arc::clone(factory),
            p.clone(),
            table.clone(),
        );
        let ended = key.clone();
        let task = tokio::spawn(async move {
            let r = read_sink(&*factory, &p, &table, generation).await;
            // Kept before the read leaves the flight: a request in between finds one or the
            // other, and never reads again.
            if let Ok(r) = &r {
                lake.keep(&p.id, &table, r.clone());
            }
            lake.reading
                .lock()
                .expect("the lake's reads in flight")
                .remove(&ended);
            r
        });
        let shared = async move {
            task.await
                .unwrap_or_else(|e| Err(format!("the read ended: {e}")))
        }
        .boxed()
        .shared();
        reading.insert(key, shared.clone());
        shared
    }
}

/// `table`'s facts and columns through a sink built for `p` now. The columns come from the
/// table's state: the facts don't carry them, and a [`data_avalanche_core::Sink`] that read both
/// from one snapshot would need a method of its own.
async fn read_sink(
    factory: &dyn WorkerFactory,
    p: &Pipeline,
    table: &TableRef,
    generation: u64,
) -> Result<Read, String> {
    let sink = factory.build(p)?.sink;
    let at = Utc::now();
    let (facts, state) = tokio::join!(sink.facts(table), sink.table_state(table));
    Ok(Read {
        facts: facts.map_err(|e| e.to_string())?,
        schema: state.map_err(|e| e.to_string())?.schema,
        at,
        generation,
        kept: Instant::now(),
    })
}

/// The type Delta's column has, as the sink records it: its logical tag, else its Arrow type.
fn delta_type(f: &arrow_schema::Field) -> String {
    f.metadata()
        .get(LOGICAL_TAG)
        .cloned()
        .unwrap_or_else(|| f.data_type().to_string())
}

/// What the table's page knows of Delta's columns.
#[derive(Debug, Clone, Copy)]
enum DeltaColumns<'a> {
    /// The read failed or ran past its bound.
    Unread,
    /// The read found no Delta table.
    Absent,
    Read(&'a Schema),
}

/// The table's columns at the source (the pipeline's cached schema) against Delta's: the source's
/// in order, then the ones only Delta has (`soft_dropped`: kept, written NULL), then the lineage
/// columns. A source column Delta lacks, or Delta has no table yet, is `not_in_delta`; with
/// Delta unread, `unknown`. MySQL compares names without case (DA-SRC-25).
fn columns(source: Option<&TableSchema>, delta: DeltaColumns<'_>, fold: bool) -> Vec<Value> {
    let same = |a: &str, b: &str| {
        if fold {
            a.eq_ignore_ascii_case(b)
        } else {
            a == b
        }
    };
    let fields: Vec<&arrow_schema::Field> = match delta {
        DeltaColumns::Read(s) => s.fields().iter().map(AsRef::as_ref).collect(),
        DeltaColumns::Unread | DeltaColumns::Absent => vec![],
    };
    let field = |name: &str| fields.iter().find(|f| same(f.name(), name)).copied();
    let mut out = vec![];
    for c in source.map(TableSchema::columns).unwrap_or_default() {
        let f = field(&c.name);
        let state = match (delta, f) {
            (DeltaColumns::Unread, _) => "unknown",
            (_, Some(_)) => "kept",
            (_, None) => "not_in_delta",
        };
        out.push(json!({
            "name": c.name,
            "source": c.source_type,
            "delta": f.map(delta_type),
            "key": source.is_some_and(|s| s.identifier().iter().any(|k| k == &c.name)),
            "state": state,
        }));
    }
    let lineage = |n: &str| LINEAGE_COLUMNS.contains(&n);
    for f in &fields {
        let in_source = source.is_some_and(|s| s.columns().iter().any(|c| same(&c.name, f.name())));
        if in_source || lineage(f.name()) || f.name().starts_with(INTERNAL_COLUMN_PREFIX) {
            continue;
        }
        out.push(json!({
            "name": f.name(),
            "source": null,
            "delta": delta_type(f),
            "key": false,
            "state": if source.is_some() { "soft_dropped" } else { "kept" },
        }));
    }
    for f in fields.iter().filter(|f| lineage(f.name())) {
        out.push(json!({
            "name": f.name(),
            "source": null,
            "delta": delta_type(f),
            "key": false,
            "state": "lineage",
        }));
    }
    out
}

/// `GET /v1/pipelines/{id}/tables/{table}`: `404` for a table the pipeline has no trace of.
pub(crate) async fn get(
    State(api): State<Api>,
    Path((id, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (p, docs) = match find(node, &id).await {
        Ok(found) => found,
        Err(f) => return f.into(),
    };
    let table = match named_table(&p, &name) {
        Ok(t) => t,
        Err(e) => return body(StatusCode::BAD_REQUEST, &json!({ "error": e })),
    };
    let name = table_name(&p, &table);
    let generation = node.coordinator.generation().await.generation;
    let (live, halts, cached, jobs, lake_read) = tokio::join!(
        live(node, &p, &docs, &headers),
        docs.halts(),
        docs.table_schema(&table),
        jobs_of(node, &p),
        node.lake.read(&node.factory, &p, &table, generation),
    );
    let (halts, cached) = match (halts, cached) {
        (Ok(h), Ok(c)) => (h, c.map(|v| v.doc)),
        (Err(e), _) | (_, Err(e)) => return unavailable(&e),
    };
    let jobs = match jobs {
        Ok(jobs) => jobs,
        Err(e) => {
            return body(
                StatusCode::SERVICE_UNAVAILABLE,
                &json!({ "error": format!("the pipeline's snapshot jobs: {e}") }),
            );
        }
    };
    let (holder, live_table, live_error) = match live {
        Ok(v) => {
            let t = v["tables"]
                .as_array()
                .and_then(|ts| ts.iter().find(|t| t["table"] == name.as_str()))
                .cloned();
            (v["node"].clone(), t, Value::Null)
        }
        Err(why) => (Value::Null, None, why),
    };
    let halt = halts.tables.iter().find(|h| h.table == table);
    let (delta, delta_error) = match &lake_read {
        Ok(r) => (
            r.facts.as_ref().map(|f| {
                json!({
                    "location": f.location,
                    "version": f.version,
                    "files": f.files,
                    "bytes": f.bytes,
                    "rows": f.rows,
                    "read_at": r.at,
                })
            }),
            None,
        ),
        Err(e) => (None, Some(e.clone())),
    };
    let sink_schema = lake_read.as_ref().ok().map(|r| r.schema.clone());
    let copied: Vec<Value> = jobs
        .iter()
        .rev()
        .filter_map(|(_, j)| {
            let t = j.tables.iter().find(|t| t.table == table)?;
            Some(json!({
                "job_id": j.job_id,
                "status": status_name(j),
                "requested_at": j.requested_at,
                "requested_by": j.requested_by,
                "note": j.note,
                "truncate": j.truncate,
                "table": {
                    "status": table_status(t.status),
                    "rows_done": t.rows_done,
                    "started_at": t.started_at,
                    "ended_at": t.ended_at,
                    "lake": lake(j.truncate, t),
                },
            }))
        })
        .collect();
    let known = live_table.is_some()
        || halt.is_some()
        || cached.is_some()
        || lake_read.as_ref().is_ok_and(|r| r.facts.is_some())
        || !copied.is_empty();
    if !known {
        return body(
            StatusCode::NOT_FOUND,
            &json!({ "error": format!("{} has no table {name}", p.id) }),
        );
    }
    let fold = p.source.kind == SourceType::Mysql;
    let delta_columns = match &sink_schema {
        None => DeltaColumns::Unread,
        Some(None) => DeltaColumns::Absent,
        Some(Some(s)) => DeltaColumns::Read(s),
    };
    body(
        StatusCode::OK,
        &json!({
            "pipeline": p.id,
            "table": name,
            "as_of": Utc::now(),
            "node": holder,
            "live": live_table,
            "live_error": live_error,
            "halt": halt.map(|h| json!({
                "code": h.reason.code(),
                "detail": h.reason.to_string(),
                "at": h.at.to_string(),
                "since": halts.since(&table),
                "copy": h.copy,
            })),
            "delta": delta,
            "delta_error": delta_error,
            "columns": columns(cached.as_ref(), delta_columns, fold),
            "jobs": copied,
        }),
    )
}

/// The states `GET /v1/tables` filters by, worst first: the order it lists them in. `costly` is a
/// streaming table whose last MERGE rewrote more than half its files (D-MS.15); `copied`, a
/// `dump_*` table that isn't streamed.
const STATES: [&str; 6] = [
    "halted",
    "stale",
    "snapshot",
    "costly",
    "streaming",
    "copied",
];

/// `GET /v1/tables`'s filter: the states, the pipeline, and text the table's name holds.
#[derive(Debug, Default)]
struct Filter {
    states: Vec<String>,
    pipeline: Option<String>,
    q: Option<String>,
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
            "state" => {
                for s in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    if !STATES.contains(&s) {
                        return Err(bad(format!(
                            "state {s:?} isn't one of {}",
                            STATES.join(", ")
                        )));
                    }
                    f.states.push(s.to_owned());
                }
            }
            "pipeline" => f.pipeline = Some(value),
            "q" => f.q = Some(value.to_lowercase()).filter(|q| !q.is_empty()),
            other => {
                return Err(bad(format!(
                    "unknown parameter {other:?}: the tables take state, pipeline, q"
                )));
            }
        }
    }
    Ok(f)
}

/// The state a table is listed under: its own, `stale` for a stale one, `costly` for a streaming
/// one whose last MERGE rewrote more than half its files. A `copied` one is neither.
fn listed_state(t: &Value) -> &'static str {
    let rewritten = t["files_rewritten"].as_u64();
    let files = t["files"].as_u64();
    match t["state"].as_str() {
        Some("halted") => "halted",
        _ if t["stale"] == true => "stale",
        Some("snapshot") => "snapshot",
        Some("copied") => "copied",
        _ if rewritten
            .zip(files)
            .is_some_and(|(r, f)| f > 0 && r * 2 > f) =>
        {
            "costly"
        }
        _ => "streaming",
    }
}

/// A pipeline's tables when its holder can't be asked: its halts, from storage.
async fn stored(node: &Node, p: &Pipeline) -> Result<Vec<Value>, String> {
    let docs = (node.docs)(p).map_err(|e| format!("the state location: {e}"))?;
    let halts = docs.halts().await.map_err(|e| e.to_string())?;
    Ok(crate::live::stored(p, &halts, Utc::now()))
}

/// One pipeline's tables, each with its pipeline and node, and why the holder couldn't be asked:
/// the pipeline, the answer's `code`, the `error` (with why its halts couldn't be read either,
/// when they couldn't) and the holder's `node`, null when none is known.
async fn of_pipeline(
    node: &Node,
    p: &Pipeline,
    headers: &HeaderMap,
) -> (Vec<Value>, Option<Value>) {
    let docs = match (node.docs)(p) {
        Ok(docs) => docs,
        Err(e) => {
            let why = json!({
                "pipeline": p.id,
                "code": StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                "error": format!("the state location: {e}"),
                "node": null,
            });
            return (vec![], Some(why));
        }
    };
    let (rows, node_name, why) = match live(node, p, &docs, headers).await {
        Ok(v) => {
            let rows = v["tables"].as_array().cloned().unwrap_or_default();
            (rows, v["node"].clone(), None)
        }
        Err(why) => match stored(node, p).await {
            Ok(rows) => (rows, Value::Null, Some(why)),
            Err(e) => {
                let error = match why["error"].as_str() {
                    Some(holder) => format!("{holder}; its halts: {e}"),
                    None => format!("its halts: {e}"),
                };
                let why = json!({ "code": why["code"], "error": error, "node": why["node"] });
                (vec![], Value::Null, Some(why))
            }
        },
    };
    let rows = rows
        .into_iter()
        .map(|mut t| {
            t["pipeline"] = json!(p.id);
            t["node"] = node_name.clone();
            t["live"] = json!(why.is_none());
            t["listed_as"] = json!(listed_state(&t));
            t
        })
        .collect();
    let why = why.map(|mut w| {
        w["pipeline"] = json!(p.id);
        w
    });
    (rows, why)
}

/// `GET /v1/tables`: every pipeline's tables, worst first, filtered; each pipeline's holder asked
/// [`READ_AT_ONCE`] at a time, in any order, so a slow one holds up no other. A pipeline whose holder can't be asked lists its halted tables
/// from storage, and is named under `unreachable` with why. `counts` are by listed state, of the
/// tables the pipeline and `q` filters keep.
pub(crate) async fn all(
    State(api): State<Api>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Response {
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
    let mut pipelines: Vec<Pipeline> = config
        .pipelines
        .values()
        .filter(|p| f.pipeline.as_ref().is_none_or(|id| &p.id == id))
        .cloned()
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
    let headers = &headers;
    let read = futures_util::stream::iter(pipelines)
        .map(|p| async move { of_pipeline(node, &p, headers).await })
        .buffer_unordered(READ_AT_ONCE)
        .collect::<Vec<_>>()
        .await;
    let mut tables = vec![];
    let mut unreachable = vec![];
    for (rows, why) in read {
        tables.extend(rows.into_iter().filter(|t| {
            f.q.as_ref().is_none_or(|q| {
                t["table"]
                    .as_str()
                    .is_some_and(|n| n.to_lowercase().contains(q.as_str()))
            })
        }));
        unreachable.extend(why);
    }
    // Read in any order: listed by pipeline.
    unreachable.sort_by(|a: &Value, b: &Value| a["pipeline"].as_str().cmp(&b["pipeline"].as_str()));
    let mut counts = serde_json::Map::new();
    counts.insert("all".into(), json!(tables.len()));
    for state in STATES {
        let n = tables.iter().filter(|t| t["listed_as"] == state).count();
        counts.insert(state.into(), json!(n));
    }
    tables.retain(|t| f.states.is_empty() || f.states.iter().any(|s| t["listed_as"] == s.as_str()));
    let rank = |t: &Value| {
        STATES
            .iter()
            .position(|s| t["listed_as"] == *s)
            .unwrap_or(STATES.len())
    };
    // Total: a pipeline lists each of its tables once.
    tables.sort_by(|a, b| {
        (rank(a), a["pipeline"].as_str(), a["table"].as_str()).cmp(&(
            rank(b),
            b["pipeline"].as_str(),
            b["table"].as_str(),
        ))
    });
    body(
        StatusCode::OK,
        &json!({
            "as_of": Utc::now(),
            "counts": counts,
            "tables": tables,
            "unreachable": unreachable,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use data_avalanche_cluster::{Actual, ControllerStatus, Parts, WorkerFactory};
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{
        ColumnSchema, ColumnValueType as V, CommitCost, CommitMarker, CommitOutcome, EvolveOutcome,
        HaltReason, SchemaDelta, Sink, SinkCapabilities, SinkError, SinkKind, SinkTableState,
        SourcePosition, TableChangeSet, TableSpec,
    };
    use data_avalanche_pipeline::{Phase, SnapshotProgress, TableCommit, WorkerStatus};
    use data_avalanche_state::{Holder, SnapshotJobDoc, TableHalt};
    use data_avalanche_testkit::{FakeSource, MemorySink, SinkOp};
    use tokio::sync::watch;

    use super::*;
    use crate::test_support::{config, open, request, served, served_with};

    fn t(name: &str) -> TableRef {
        TableRef::new("public", name)
    }

    fn schema(table: &str, with_legacy: bool) -> TableSchema {
        let mut columns = vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("note", V::Text, "varchar(200)"),
        ];
        if with_legacy {
            columns.push(ColumnSchema::new("legacy", V::Bool, "boolean"));
        }
        TableSchema::new(t(table), columns, vec!["id".into()]).unwrap()
    }

    fn json(text: &str) -> Value {
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    /// The table's page: the worker's figures of it, its halt with when it began, the Delta
    /// facts, the columns (one soft-dropped), and the jobs that copied it, newest first.
    #[tokio::test]
    async fn a_table_s_page_brings_what_is_known_of_it() {
        let worker = WorkerStatus {
            phase: Phase::Streaming,
            tracked: [t("orders")].into(),
            halted: [(t("orders"), HaltReason::TableDropped)].into(),
            ..WorkerStatus::default()
        };
        let status = ControllerStatus {
            actual: Some(Actual::Running),
            owner_epoch: Some(4),
            lease: None,
            worker: Some(worker),
            ..ControllerStatus::default()
        };
        let statuses = watch::channel(BTreeMap::from([(
            "orders".to_owned(),
            watch::channel(status).1,
        )]))
        .1;
        let (addr, lone, _dir) =
            served_with("http://127.0.0.1:1", open(), |n| n.statuses = statuses).await;
        let p = config().pipelines["orders"].clone();
        let docs = (lone.node.docs)(&p).unwrap();
        // Delta has the column the source dropped since.
        lone.sink
            .ensure_table(&TableSpec::new(&schema("orders", true), vec![]).unwrap())
            .await
            .unwrap();
        docs.save_table_schema(&schema("orders", false))
            .await
            .unwrap();
        docs.record_halt(&TableHalt {
            table: t("orders"),
            reason: HaltReason::TableDropped,
            at: SourcePosition::Pg(Lsn(0x20)),
            copy: false,
        })
        .await
        .unwrap();
        let now = Utc::now();
        for (i, tables) in [vec![t("orders")], vec![t("items")], vec![t("orders")]]
            .into_iter()
            .enumerate()
        {
            let at = now + chrono::Duration::seconds(i as i64);
            let job = SnapshotJobDoc::new(format!("orders~000{i}"), tables, "ops".into(), at);
            docs.create_snapshot_job(&job).await.unwrap();
        }
        let (code, _, text) = request(
            addr,
            "GET",
            "/v1/pipelines/orders/tables/public.orders",
            &[],
            "",
        )
        .await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        assert_eq!(
            (&v["node"], &v["table"]),
            (&json!("node-a"), &json!("public.orders"))
        );
        assert_eq!(v["live"]["state"], "halted");
        assert_eq!(v["halt"]["code"], "TABLE_DROPPED");
        assert!(v["halt"]["since"].is_string(), "{v}");
        assert_eq!(
            (&v["delta"]["version"], &v["delta"]["rows"]),
            (&json!(0), &json!(0))
        );
        let cols: Vec<(&str, &str)> = v["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["name"].as_str().unwrap(), c["state"].as_str().unwrap()))
            .collect();
        assert_eq!(
            &cols[..3],
            [("id", "kept"), ("note", "kept"), ("legacy", "soft_dropped")]
        );
        assert!(cols[3..].iter().all(|(_, s)| *s == "lineage"), "{cols:?}");
        assert_eq!(v["columns"][0]["key"], true);
        assert_eq!(v["columns"][1]["source"], "varchar(200)");
        let jobs: Vec<&str> = v["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| j["job_id"].as_str().unwrap())
            .collect();
        assert_eq!(jobs, ["orders~0002", "orders~0000"]);
    }

    /// The facts are kept a while: a commit after the first read shows only once they expire.
    #[test]
    fn facts_are_kept_for_their_while() {
        let lake = Lake::default();
        let read = |kept| Read {
            facts: None,
            schema: None,
            at: Utc::now(),
            generation: 1,
            kept,
        };
        lake.keep("orders", &t("a"), read(Instant::now()));
        assert!(lake.kept("orders", &t("a"), 1).is_some());
        assert!(
            lake.kept("orders", &t("a"), 2).is_none(),
            "another generation"
        );
        let old = Instant::now().checked_sub(FACTS_FOR).unwrap();
        lake.keep("orders", &t("b"), read(old));
        assert!(lake.kept("orders", &t("b"), 1).is_none(), "expired");
    }

    /// A sink whose table reads wait until `gate` opens; its factory counts the sinks it builds.
    #[derive(Debug)]
    struct Gated {
        sink: MemorySink,
        gate: watch::Receiver<bool>,
        builds: Arc<AtomicUsize>,
    }

    impl Gated {
        async fn open(&self) {
            let mut gate = self.gate.clone();
            let _ = gate.wait_for(|open| *open).await;
        }
    }

    impl WorkerFactory for Gated {
        fn build(&self, _: &Pipeline) -> Result<Parts, String> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            Ok(Parts {
                source: Arc::new(FakeSource::new(vec![])),
                sink: Arc::new(Gated {
                    sink: self.sink.clone(),
                    gate: self.gate.clone(),
                    builds: Arc::clone(&self.builds),
                }),
                hooks: Arc::new(data_avalanche_pipeline::NoHooks),
            })
        }
    }

    #[async_trait::async_trait]
    impl Sink for Gated {
        fn kind(&self) -> SinkKind {
            self.sink.kind()
        }
        fn capabilities(&self) -> &SinkCapabilities {
            self.sink.capabilities()
        }
        async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError> {
            self.open().await;
            self.sink.table_state(table).await
        }
        async fn facts(&self, table: &TableRef) -> Result<Option<TableFacts>, SinkError> {
            self.open().await;
            self.sink.facts(table).await
        }
        async fn held_tables(&self) -> Result<Vec<TableRef>, SinkError> {
            self.sink.held_tables().await
        }
        async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError> {
            self.sink.ensure_table(spec).await
        }
        async fn evolve(
            &self,
            table: &TableRef,
            delta: &SchemaDelta,
            owner_epoch: u64,
        ) -> Result<EvolveOutcome, SinkError> {
            self.sink.evolve(table, delta, owner_epoch).await
        }
        async fn commit(
            &self,
            cs: TableChangeSet,
            marker: CommitMarker,
        ) -> Result<CommitOutcome, SinkError> {
            self.sink.commit(cs, marker).await
        }
    }

    /// A node whose sink reads wait for the returned gate, its lake reads bounded by `bound`;
    /// `public.orders` is cached and in the sink. The counter is the sinks built.
    async fn gated(
        bound: Duration,
    ) -> (
        std::net::SocketAddr,
        crate::test_support::Lone,
        tempfile::TempDir,
        watch::Sender<bool>,
        Arc<AtomicUsize>,
    ) {
        let (gate, opened) = watch::channel(false);
        let builds = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&builds);
        let (addr, lone, dir) = served_with("http://127.0.0.1:1", open(), |n| {
            n.factory = Arc::new(Gated {
                sink: MemorySink::new(),
                gate: opened,
                builds: counted,
            });
            n.lake = Arc::new(Lake::with_bound(bound));
        })
        .await;
        (addr, lone, dir, gate, builds)
    }

    async fn page(addr: std::net::SocketAddr, table: &str) -> (u16, Value) {
        let path = format!("/v1/pipelines/orders/tables/{table}");
        let (code, _, text) = request(addr, "GET", &path, &[], "").await;
        (code, json(&text))
    }

    fn states(v: &Value) -> Vec<&str> {
        v["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["state"].as_str().unwrap())
            .collect()
    }

    /// A lake read past its bound: the page answers without it, its columns unknown. The read goes
    /// on and, once it ends, answers the next request from what it kept.
    #[tokio::test]
    async fn a_slow_lake_read_is_bounded_and_ends_in_the_background() {
        let (addr, lone, _dir, gate, builds) = gated(Duration::from_millis(100)).await;
        let docs = (lone.node.docs)(&config().pipelines["orders"]).unwrap();
        docs.save_table_schema(&schema("orders", false))
            .await
            .unwrap();
        let (code, v) = page(addr, "public.orders").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(
            (&v["delta"], &v["delta_error"]),
            (&Value::Null, &json!("not read within 0.1 s"))
        );
        assert_eq!(states(&v), ["unknown", "unknown"]);
        gate.send_replace(true);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (_, v) = page(addr, "public.orders").await;
            if v["delta_error"].is_null() {
                break;
            }
            assert!(Instant::now() < deadline, "the read never ended: {v}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "one read, ended in the background"
        );
    }

    /// Two requests that find nothing kept share one read.
    #[tokio::test]
    async fn concurrent_misses_share_one_read() {
        let (addr, lone, _dir, gate, builds) = gated(FACTS_WITHIN).await;
        let docs = (lone.node.docs)(&config().pipelines["orders"]).unwrap();
        docs.save_table_schema(&schema("orders", false))
            .await
            .unwrap();
        let ((a, va), (b, vb), ()) = tokio::join!(
            page(addr, "public.orders"),
            page(addr, "public.orders"),
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                gate.send_replace(true);
            }
        );
        assert_eq!((a, b), (200, 200), "{va} {vb}");
        assert_eq!(
            (&va["delta_error"], &vb["delta_error"]),
            (&Value::Null, &Value::Null)
        );
        assert_eq!(builds.load(Ordering::SeqCst), 1, "one sink built for both");
    }

    /// The columns' state when Delta's schema isn't known: a read that found no Delta table says
    /// the source's columns aren't in Delta; a failed read says nothing is known of them.
    #[tokio::test]
    async fn columns_say_what_is_unknown() {
        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
        let docs = (lone.node.docs)(&config().pipelines["orders"]).unwrap();
        docs.save_table_schema(&schema("orders", false))
            .await
            .unwrap();
        docs.save_table_schema(&schema("items", false))
            .await
            .unwrap();
        let (code, v) = page(addr, "public.orders").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(
            (&v["delta"], &v["delta_error"]),
            (&Value::Null, &Value::Null)
        );
        assert_eq!(states(&v), ["not_in_delta", "not_in_delta"]);
        lone.sink
            .fail(SinkOp::TableState, None, 10, || SinkError::Transient {
                reason: "the store is down".into(),
            });
        let (code, v) = page(addr, "public.items").await;
        assert_eq!(code, 200, "{v}");
        assert!(v["delta_error"].is_string(), "{v}");
        assert_eq!(states(&v), ["unknown", "unknown"]);
    }

    /// A table the pipeline has no trace of is 404; a name not in the source's form is 400. With
    /// no node running the pipeline, the page says why it has no live figures.
    #[tokio::test]
    async fn an_unknown_table_and_a_bad_name_are_refused() {
        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
        let (code, _, text) = request(
            addr,
            "GET",
            "/v1/pipelines/orders/tables/public.nope",
            &[],
            "",
        )
        .await;
        assert_eq!(code, 404, "{text}");
        let (code, _, _) =
            request(addr, "GET", "/v1/pipelines/orders/tables/orders", &[], "").await;
        assert_eq!(code, 400, "a PostgreSQL table is schema.table");
        lone.sink
            .ensure_table(&TableSpec::new(&schema("orders", false), vec![]).unwrap())
            .await
            .unwrap();
        let (code, _, text) = request(
            addr,
            "GET",
            "/v1/pipelines/orders/tables/public.orders",
            &[],
            "",
        )
        .await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        assert_eq!(
            (&v["live"], &v["live_error"]["code"]),
            (&Value::Null, &json!(409))
        );
    }

    fn commit(table: &str, rewritten: u64, files: u64) -> TableCommit {
        TableCommit {
            table: t(table),
            rows: 1,
            snapshot: false,
            version: 3,
            cost: CommitCost {
                files_rewritten: rewritten,
                files: Some(files),
            },
            at: Utc::now(),
        }
    }

    /// A worker 90 s behind: `items` halted, `audit` copying, `events` rewriting most of its files
    /// (stale too: so `quiet`, caught up, is the one streaming table), `orders` stale; `dumped` a
    /// dump that ended, its copy rewriting every file.
    fn statuses(
        behind: i64,
    ) -> watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>> {
        let mut w = WorkerStatus {
            phase: Phase::Streaming,
            unsynced_since_micros: Some(
                (Utc::now() - chrono::Duration::seconds(behind)).timestamp_micros(),
            ),
            tracked: [t("orders"), t("items"), t("events"), t("quiet")].into(),
            copied: [t("dumped")].into(),
            halted: [(t("items"), HaltReason::TableDropped)].into(),
            snapshots: [(
                t("audit"),
                SnapshotProgress {
                    rows_done: 1,
                    waiting: false,
                },
            )]
            .into(),
            ..WorkerStatus::default()
        };
        w.committed(&commit("events", 8, 10));
        w.committed(&commit("quiet", 1, 10));
        w.committed(&commit("dumped", 10, 10));
        let status = ControllerStatus {
            actual: Some(Actual::Running),
            owner_epoch: Some(4),
            lease: None,
            pending_since_micros: w.unsynced_since_micros,
            caught_up_at: None,
            worker: Some(w),
        };
        watch::channel(BTreeMap::from([(
            "orders".to_owned(),
            watch::channel(status).1,
        )]))
        .1
    }

    fn names(v: &Value) -> Vec<(String, String)> {
        v["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                (
                    t["table"].as_str().unwrap().to_owned(),
                    t["listed_as"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    }

    /// Every table, worst first, with counts by listed state; the filters keep their tables.
    #[tokio::test]
    async fn every_table_is_listed_worst_first() {
        let behind = statuses(0);
        let (addr, _lone, _dir) =
            served_with("http://127.0.0.1:1", open(), |n| n.statuses = behind).await;
        let (code, _, text) = request(addr, "GET", "/v1/tables", &[], "").await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        let pairs = |p: &[(&str, &str)]| -> Vec<(String, String)> {
            p.iter()
                .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
                .collect()
        };
        assert_eq!(
            names(&v),
            pairs(&[
                ("public.items", "halted"),
                ("public.audit", "snapshot"),
                ("public.events", "costly"),
                ("public.orders", "streaming"),
                ("public.quiet", "streaming"),
                ("public.dumped", "copied"),
            ])
        );
        assert_eq!(v["tables"][0]["pipeline"], "orders");
        assert_eq!(v["tables"][0]["node"], "node-a");
        assert_eq!(
            v["counts"],
            json!({
                "all": 6, "halted": 1, "stale": 0, "snapshot": 1, "costly": 1, "streaming": 2,
                "copied": 1
            })
        );
        assert_eq!(v["unreachable"], json!([]));
        let (_, _, text) =
            request(addr, "GET", "/v1/tables?state=halted,costly&q=EVE", &[], "").await;
        assert_eq!(names(&json(&text)), pairs(&[("public.events", "costly")]));
        let (code, _, _) = request(addr, "GET", "/v1/tables?state=gone", &[], "").await;
        assert_eq!(code, 400);
        let (code, _, _) = request(addr, "GET", "/v1/tables?pipeline=nope", &[], "").await;
        assert_eq!(code, 404);
    }

    /// Past the lag target while streaming, a table is stale, listed after the halted ones.
    #[tokio::test]
    async fn a_table_behind_the_target_is_stale() {
        let behind = statuses(90);
        let (addr, _lone, _dir) =
            served_with("http://127.0.0.1:1", open(), |n| n.statuses = behind).await;
        let (_, _, text) = request(addr, "GET", "/v1/tables?state=stale", &[], "").await;
        let v = json(&text);
        let stale: Vec<String> = names(&v).into_iter().map(|(n, _)| n).collect();
        assert_eq!(stale, ["public.events", "public.orders", "public.quiet"]);
        assert_eq!(v["counts"]["stale"], 3);
    }

    /// Every `unreachable` entry says the pipeline, the answer's code, why and the node: a state
    /// location that can't be built is `503` with no node; a holder that can't be asked and
    /// halts that can't be read keep the holder's code and node, with both errors.
    #[tokio::test]
    async fn every_unreachable_pipeline_says_why_and_where() {
        let (addr, _lone, _dir) = served_with("http://127.0.0.1:1", open(), |n| {
            n.docs = Arc::new(|_| Err("no such bucket".into()));
        })
        .await;
        let (code, _, text) = request(addr, "GET", "/v1/tables", &[], "").await;
        assert_eq!(code, 200, "{text}");
        assert_eq!(
            json(&text)["unreachable"],
            json!([{
                "pipeline": "orders",
                "code": 503,
                "error": "the state location: no such bucket",
                "node": null,
            }])
        );
        // The state location builds once, for the holder's question; then it fails.
        let calls = Arc::new(AtomicUsize::new(0));
        let (addr, lone, _dir) = served_with("http://127.0.0.1:1", open(), |n| {
            let real = Arc::clone(&n.docs);
            n.docs = Arc::new(move |p| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    real(p)
                } else {
                    Err("down".into())
                }
            });
        })
        .await;
        let holder = Holder {
            node_id: "node-z".into(),
            incarnation: 1,
        };
        lone.leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap();
        let (_, _, text) = request(addr, "GET", "/v1/tables", &[], "").await;
        assert_eq!(
            json(&text)["unreachable"],
            json!([{
                "pipeline": "orders",
                "code": 503,
                "error": "node-z holds orders's lease and isn't a member this node sees; its halts: the state location: down",
                "node": "node-z",
            }])
        );
    }

    /// A holder without a worker (a stopped pipeline) answers with its halts from storage: they
    /// are listed live, from it.
    #[tokio::test]
    async fn a_holder_without_a_worker_lists_its_halts() {
        let status = ControllerStatus {
            actual: Some(Actual::Stopped),
            owner_epoch: Some(4),
            lease: None,
            worker: None,
            ..ControllerStatus::default()
        };
        let stopped = watch::channel(BTreeMap::from([(
            "orders".to_owned(),
            watch::channel(status).1,
        )]))
        .1;
        let (addr, lone, _dir) =
            served_with("http://127.0.0.1:1", open(), |n| n.statuses = stopped).await;
        let docs = (lone.node.docs)(&config().pipelines["orders"]).unwrap();
        docs.record_halt(&TableHalt {
            table: t("items"),
            reason: HaltReason::TableDropped,
            at: SourcePosition::Pg(Lsn(0x20)),
            copy: false,
        })
        .await
        .unwrap();
        let (code, _, text) = request(addr, "GET", "/v1/tables", &[], "").await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        assert_eq!(
            names(&v),
            [("public.items".to_owned(), "halted".to_owned())]
        );
        assert_eq!(
            (&v["tables"][0]["live"], &v["tables"][0]["node"]),
            (&json!(true), &json!("node-a"))
        );
        assert_eq!(v["unreachable"], json!([]));
    }

    /// A holder that can't be asked: the pipeline's halted tables come from storage, and the
    /// pipeline is named under `unreachable` with why.
    #[tokio::test]
    async fn a_pipeline_whose_holder_can_t_be_asked_lists_its_halts() {
        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
        let docs = (lone.node.docs)(&config().pipelines["orders"]).unwrap();
        docs.record_halt(&TableHalt {
            table: t("items"),
            reason: HaltReason::TableDropped,
            at: SourcePosition::Pg(Lsn(0x20)),
            copy: false,
        })
        .await
        .unwrap();
        let holder = Holder {
            node_id: "node-z".into(),
            incarnation: 1,
        };
        lone.leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap();
        let (code, _, text) = request(addr, "GET", "/v1/tables", &[], "").await;
        assert_eq!(code, 200, "{text}");
        let v = json(&text);
        assert_eq!(
            names(&v),
            [("public.items".to_owned(), "halted".to_owned())]
        );
        assert_eq!(v["tables"][0]["live"], false);
        assert!(v["tables"][0]["fresh_as_of"].is_string(), "{v}");
        assert_eq!(
            (
                &v["unreachable"][0]["pipeline"],
                &v["unreachable"][0]["code"],
                &v["unreachable"][0]["node"]
            ),
            (&json!("orders"), &json!(503), &json!("node-z"))
        );
    }
}
