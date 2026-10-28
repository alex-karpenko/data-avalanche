//! M10's exit (roadmap M10): a running pipeline exports the mandatory catalogue with its labels
//! (System-Overview §Observability catalogue), and its flushes, commits and snapshot chunks
//! reach an OTLP/HTTP collector as traces (config §6.6): Jaeger, from `test-infra/compose.yaml`.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use data_avalanche_testkit::require_it;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::{E2e, Options, eventually};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
                     INSERT INTO items VALUES (1, 'a'), (2, 'b');";

const CHANGES: &str = "INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1; DELETE FROM items WHERE id = 2";

/// A heartbeat every 2 s: the retained log, and with it the source's lag series, is read at
/// each.
fn options() -> Options<'static> {
    Options {
        tables: ", snapshots: { mode: initial }",
        postgres: ", heartbeat: 2s",
        mysql: ", heartbeat: 2s",
        ..Options::default()
    }
}

/// The series of the catalogue a pipeline shows once its snapshot is done and a change of each
/// kind went through, with the labels beyond the pipeline's three; a histogram by its `_count`.
/// Not here: the commit and renewal failures, which only a failure makes, and the source's lag,
/// which depends on the source.
const LIVE: &[(&str, &str)] = &[
    ("data_avalanche_replication_lag_seconds", ""),
    ("data_avalanche_rows_written_total", ""),
    ("data_avalanche_flush_duration_seconds_count", ""),
    ("data_avalanche_flush_rows_count", ""),
    ("data_avalanche_transaction_buffer_bytes", ""),
    ("data_avalanche_source_reconnects_total", ""),
    ("data_avalanche_snapshot_rows_done", ""),
    ("data_avalanche_snapshot_state", r#",state="running""#),
    ("data_avalanche_snapshot_state", r#",state="waiting""#),
    ("data_avalanche_snapshot_chunk_duration_seconds_count", ""),
    ("data_avalanche_reconcile_log_entries", ""),
    ("data_avalanche_reconcile_log_keys", ""),
    (
        "data_avalanche_sink_commit_duration_seconds_count",
        r#",sink_type="delta""#,
    ),
    ("data_avalanche_sink_merge_files_rewritten_total", ""),
    ("data_avalanche_sink_merge_rows_copied_total", ""),
    ("data_avalanche_sink_soft_dropped_columns", ""),
    ("data_avalanche_lease_state", r#",state="held""#),
    ("data_avalanche_lease_renew_latency_seconds_count", ""),
    ("data_avalanche_pipeline_status", r#",status="running""#),
    ("data_avalanche_halted_tables", ""),
];

/// The value of the sample `name{<labels><extra>}`, or of the node's `name` when `labels` is
/// empty.
fn sample(text: &str, name: &str, labels: &str, extra: &str) -> Option<f64> {
    let head = if labels.is_empty() {
        format!("{name} ")
    } else {
        format!("{name}{{{labels}{extra}}} ")
    };
    text.lines()
        .find_map(|l| l.strip_prefix(&head))
        .map(|v| v.parse().expect("a sample's value"))
}

/// The source's lag: the WAL a PostgreSQL slot retains, the age of the last transaction a MySQL
/// stream read (D-M10.4); and the other one, which the pipeline doesn't have.
fn lags(e: &E2e) -> (&'static str, &'static str) {
    if e.source.is_postgres() {
        (
            "data_avalanche_wal_lag_bytes",
            "data_avalanche_binlog_lag_seconds",
        )
    } else {
        (
            "data_avalanche_binlog_lag_seconds",
            "data_avalanche_wal_lag_bytes",
        )
    }
}

#[tokio::test]
async fn a_running_pipeline_exports_the_catalogue() {
    require_it!();
    let e = E2e::new("observed", ITEMS, options()).await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    e.exec(CHANGES).await;
    e.converged("items").await;
    let labels = e.labels();
    let (lag, other) = lags(&e);
    eventually("the source's lag", Duration::from_secs(60), async || {
        sample(&p.metrics().await, lag, &labels, "").is_some()
    })
    .await;
    let text = p.metrics().await;
    for (name, extra) in LIVE {
        assert!(
            sample(&text, name, &labels, extra).is_some(),
            "{name}{{{labels}{extra}}} isn't scraped:\n{text}"
        );
    }
    for node in [
        "data_avalanche_cluster_members",
        "data_avalanche_owned_pipelines",
        "data_avalanche_ring_generation",
    ] {
        assert!(sample(&text, node, "", "").is_some(), "{node}:\n{text}");
    }
    assert!(!text.contains(&format!("{other}{{")), "{text}");
    let read = |op: &str| {
        sample(
            &text,
            "data_avalanche_rows_read_total",
            &labels,
            &format!(r#",op="{op}""#),
        )
    };
    assert_eq!(
        ["snapshot", "insert", "update", "delete"].map(read),
        [Some(2.0), Some(1.0), Some(1.0), Some(1.0)],
        "{text}"
    );
    assert_eq!(
        sample(&text, "data_avalanche_rows_written_total", &labels, ""),
        Some(5.0),
        "the two rows of the snapshot and the three changed"
    );
    let lagged = sample(&text, "data_avalanche_replication_lag_seconds", &labels, "");
    assert!(lagged.is_some_and(|s| s < 60.0), "{text}");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

const JAEGER_OTLP: &str = "http://127.0.0.1:54318";
const JAEGER_QUERY: u16 = 56686;

/// A span as Jaeger's query API returns it (OTLP JSON), its attributes and its resource's as
/// text.
#[derive(Debug)]
struct Span {
    trace: String,
    id: String,
    parent: String,
    name: String,
    attributes: BTreeMap<String, String>,
    resource: BTreeMap<String, String>,
}

fn attributes(v: &Value) -> BTreeMap<String, String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|kv| {
            let value = kv["value"].as_object()?.values().next()?;
            let text = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            Some((kv["key"].as_str()?.to_owned(), text))
        })
        .collect()
}

/// `GET path` of Jaeger's query API: the whole answer. HTTP/1.0, so the body runs to the close,
/// never chunked.
async fn get(path: &str) -> Option<String> {
    let mut tcp = tokio::net::TcpStream::connect(("127.0.0.1", JAEGER_QUERY))
        .await
        .ok()?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: jaeger\r\n\r\n");
    tcp.write_all(request.as_bytes()).await.ok()?;
    let mut answer = String::new();
    tcp.read_to_string(&mut answer).await.ok()?;
    Some(answer)
}

/// The spans of the traces of `operation` that started since `since` (`GET /api/v3/traces`);
/// none when Jaeger has none, or doesn't answer within 5 s.
async fn traced(operation: &str, since: DateTime<Utc>) -> Vec<Span> {
    let at = |t: DateTime<Utc>| t.to_rfc3339_opts(SecondsFormat::Secs, true);
    let path = format!(
        "/api/v3/traces?query.service_name=data-avalanche&query.operation_name={operation}&query.start_time_min={}&query.start_time_max={}&query.num_traces=100",
        at(since - chrono::Duration::seconds(5)),
        at(Utc::now() + chrono::Duration::seconds(5)),
    );
    let Ok(Some(answer)) = tokio::time::timeout(Duration::from_secs(5), get(&path)).await else {
        return Vec::new();
    };
    let Some((_, body)) = answer.split_once("\r\n\r\n") else {
        return Vec::new();
    };
    let Ok(v) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    let mut spans = Vec::new();
    for rs in v["result"]["resourceSpans"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let resource = attributes(&rs["resource"]["attributes"]);
        for ss in rs["scopeSpans"].as_array().into_iter().flatten() {
            for s in ss["spans"].as_array().into_iter().flatten() {
                spans.push(Span {
                    trace: s["traceId"].as_str().unwrap_or_default().to_owned(),
                    id: s["spanId"].as_str().unwrap_or_default().to_owned(),
                    parent: s["parentSpanId"].as_str().unwrap_or_default().to_owned(),
                    name: s["name"].as_str().unwrap_or_default().to_owned(),
                    attributes: attributes(&s["attributes"]),
                    resource: resource.clone(),
                });
            }
        }
    }
    spans
}

/// The pipeline's root spans named `name`, each with its children.
fn roots<'a>(spans: &'a [Span], name: &str, pipeline: &str) -> Vec<(&'a Span, Vec<&'a Span>)> {
    spans
        .iter()
        .filter(|s| {
            s.name == name
                && s.parent.is_empty()
                && s.attributes.get("pipeline").map(String::as_str) == Some(pipeline)
        })
        .map(|root| {
            let children = spans
                .iter()
                .filter(|s| s.trace == root.trace && s.parent == root.id)
                .collect();
            (root, children)
        })
        .collect()
}

/// Config §6.6: with an endpoint and every trace sampled, a flush is a trace whose root carries
/// the pipeline and whose children are its tables' commits; a snapshot chunk is a trace of its
/// own; the node is the resource's `service.instance.id`.
#[tokio::test]
async fn the_flushes_commits_and_chunks_are_traced_to_the_collector() {
    require_it!();
    let tracing = format!("tracing: {{ otlp_endpoint: \"{JAEGER_OTLP}\", sample_ratio: 1.0 }}");
    let e = E2e::new(
        "traced",
        ITEMS,
        Options {
            globals: &tracing,
            ..options()
        },
    )
    .await;
    let since = Utc::now();
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    e.exec(CHANGES).await;
    e.converged("items").await;
    let committed = |spans: &[Span]| {
        roots(spans, "pipeline.flush", &e.test)
            .iter()
            .any(|(_, commits)| {
                commits.iter().any(|c| {
                    c.name == "sink.commit"
                        && c.attributes.get("outcome").map(String::as_str) == Some("committed")
                        && c.attributes
                            .get("table")
                            .is_some_and(|t| t.ends_with("items"))
                })
            })
    };
    eventually(
        "a traced flush and its commit",
        Duration::from_secs(60),
        async || committed(&traced("pipeline.flush", since).await),
    )
    .await;
    let flushes = traced("pipeline.flush", since).await;
    let (flush, _) = roots(&flushes, "pipeline.flush", &e.test)[0];
    assert_eq!(
        flush.resource.get("service.name").map(String::as_str),
        Some("data-avalanche")
    );
    assert_eq!(
        flush
            .resource
            .get("service.instance.id")
            .map(String::as_str),
        Some("n1")
    );
    eventually(
        "a traced snapshot chunk",
        Duration::from_secs(60),
        async || {
            let chunks = traced("snapshot.chunk", since).await;
            roots(&chunks, "snapshot.chunk", &e.test)
                .iter()
                .any(|(c, _)| {
                    c.attributes
                        .get("table")
                        .is_some_and(|t| t.ends_with("items"))
                })
        },
    )
    .await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
