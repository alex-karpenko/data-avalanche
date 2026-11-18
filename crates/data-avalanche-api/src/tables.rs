//! A table's page (Admin_Console §4 Tables, D-M13d.5): what its pipeline's worker shows of it,
//! its halt with the time it began, the Delta table's facts read on request, its columns at the
//! source against Delta's, and the snapshot jobs that copied it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_schema::{Schema, SchemaRef};
use axum::extract::{Path, State};
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
use crate::snapshots::{jobs_of, lake, named_table, status_name, table_name, table_status};
use crate::{Api, Node, body, starting};

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use data_avalanche_cluster::{Actual, ControllerStatus};
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{
        ColumnSchema, ColumnValueType as V, HaltReason, Sink, SourcePosition, TableSpec,
    };
    use data_avalanche_pipeline::{Phase, WorkerStatus};
    use data_avalanche_state::{SnapshotJobDoc, TableHalt};
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
}
