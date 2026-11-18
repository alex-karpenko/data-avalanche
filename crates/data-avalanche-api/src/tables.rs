//! The tables (Admin_Console §4 Tables, D-M13d.5, D-M13d.7). Every pipeline's tables, each as
//! its lease holder's worker shows it, or from storage when the holder can't be asked. A table's
//! page: what its pipeline's worker shows of it, its halt with the time it began, the Delta
//! table's facts read on request, its columns at the source against Delta's, and the snapshot
//! jobs that copied it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_schema::{Schema, SchemaRef};
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use data_avalanche_config::{Pipeline, SourceType};
use data_avalanche_core::{
    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, LOGICAL_TAG, TableFacts, TableRef, TableSchema,
};
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

/// The tables' Delta facts and columns this node read, each kept [`FACTS_FOR`].
#[derive(Debug, Default)]
pub struct Lake {
    read: Mutex<HashMap<(String, TableRef), Read>>,
}

impl Lake {
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

    /// The sink's facts and columns of `table`: kept ones while fresh, else read through a sink
    /// built for the pipeline now. A failed read isn't kept.
    async fn read(
        &self,
        node: &Node,
        p: &Pipeline,
        table: &TableRef,
        generation: u64,
    ) -> Result<Read, String> {
        if let Some(r) = self.kept(&p.id, table, generation) {
            return Ok(r);
        }
        let sink = node.factory.build(p)?.sink;
        let at = Utc::now();
        let (facts, state) = tokio::join!(sink.facts(table), sink.table_state(table));
        let r = Read {
            facts: facts.map_err(|e| e.to_string())?,
            schema: state.map_err(|e| e.to_string())?.schema,
            at,
            generation,
            kept: Instant::now(),
        };
        self.keep(&p.id, table, r.clone());
        Ok(r)
    }
}

/// The type Delta's column has, as the sink records it: its logical tag, else its Arrow type.
fn delta_type(f: &arrow_schema::Field) -> String {
    f.metadata()
        .get(LOGICAL_TAG)
        .cloned()
        .unwrap_or_else(|| f.data_type().to_string())
}

/// The table's columns at the source (the pipeline's cached schema) against Delta's: the source's
/// in order, then the ones only Delta has (`soft_dropped`: kept, written NULL), then the lineage
/// columns. MySQL compares names without case (DA-SRC-25).
fn columns(source: Option<&TableSchema>, delta: Option<&Schema>, fold: bool) -> Vec<Value> {
    let same = |a: &str, b: &str| {
        if fold {
            a.eq_ignore_ascii_case(b)
        } else {
            a == b
        }
    };
    let fields: Vec<&arrow_schema::Field> = delta
        .map(|s| s.fields().iter().map(AsRef::as_ref).collect())
        .unwrap_or_default();
    let field = |name: &str| fields.iter().find(|f| same(f.name(), name)).copied();
    let mut out = vec![];
    for c in source.map(TableSchema::columns).unwrap_or_default() {
        let f = field(&c.name);
        out.push(json!({
            "name": c.name,
            "source": c.source_type,
            "delta": f.map(delta_type),
            "key": source.is_some_and(|s| s.identifier().iter().any(|k| k == &c.name)),
            "state": if f.is_some() || delta.is_none() { "kept" } else { "not_in_delta" },
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
        node.lake.read(node, &p, &table, generation),
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
    let sink_schema = lake_read.as_ref().ok().and_then(|r| r.schema.clone());
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
            "columns": columns(cached.as_ref(), sink_schema.as_deref(), fold),
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

/// One pipeline's tables, each with its pipeline and node, and why the holder couldn't be asked.
async fn of_pipeline(
    node: &Node,
    p: &Pipeline,
    headers: &HeaderMap,
) -> (Vec<Value>, Option<Value>) {
    let docs = match (node.docs)(p) {
        Ok(docs) => docs,
        Err(e) => {
            let why = json!({ "pipeline": p.id, "error": format!("the state location: {e}") });
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
            Err(e) => (
                vec![],
                Value::Null,
                Some(json!({ "code": why["code"], "error": e })),
            ),
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
/// [`READ_AT_ONCE`] at a time. A pipeline whose holder can't be asked lists its halted tables
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
        .buffered(READ_AT_ONCE)
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

    use data_avalanche_cluster::{Actual, ControllerStatus};
    use data_avalanche_core::CommitCost;
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{
        ColumnSchema, ColumnValueType as V, HaltReason, Sink, SourcePosition, TableSpec,
    };
    use data_avalanche_pipeline::{Phase, SnapshotProgress, TableCommit, WorkerStatus};
    use data_avalanche_state::{Holder, SnapshotJobDoc, TableHalt};
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

    /// A holder without a worker (a stopped pipeline) answers with its halts from storage: they
    /// are listed live, from it.
    #[tokio::test]
    async fn a_holder_without_a_worker_lists_its_halts() {
        let status = ControllerStatus {
            actual: Some(Actual::Stopped),
            owner_epoch: Some(4),
            lease: None,
            worker: None,
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
