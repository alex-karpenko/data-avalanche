//! The mandatory catalogue (System-Overview §Observability catalogue): once a pipeline's handle
//! has set each of its series, every mandatory sample is in `/metrics` with exactly its labels.

use std::collections::BTreeSet;
use std::time::Duration;

use data_avalanche_telemetry::{
    LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, RowsRead, SnapshotState,
    Totals,
};

const PIPELINE: &[&str] = &["pipeline", "source_host", "source_db"];

/// Every mandatory sample, and the labels it carries beyond the pipeline's three (`None`: a node
/// gauge, no labels at all). A histogram is named by its `_count` sample.
const CATALOGUE: &[(&str, Option<&[&str]>)] = &[
    // Replication
    ("data_avalanche_replication_lag_seconds", Some(&[])),
    ("data_avalanche_rows_read_total", Some(&["op"])),
    ("data_avalanche_rows_written_total", Some(&[])),
    ("data_avalanche_flush_duration_seconds_count", Some(&[])),
    ("data_avalanche_flush_rows_count", Some(&[])),
    ("data_avalanche_transaction_buffer_bytes", Some(&[])),
    // Source health
    ("data_avalanche_wal_lag_bytes", Some(&[])),
    ("data_avalanche_binlog_lag_seconds", Some(&[])),
    ("data_avalanche_source_reconnects_total", Some(&[])),
    // Snapshot
    ("data_avalanche_snapshot_rows_done", Some(&[])),
    ("data_avalanche_snapshot_state", Some(&["state"])),
    (
        "data_avalanche_snapshot_chunk_duration_seconds_count",
        Some(&[]),
    ),
    ("data_avalanche_reconcile_log_entries", Some(&[])),
    ("data_avalanche_reconcile_log_keys", Some(&[])),
    // Sink
    (
        "data_avalanche_sink_commit_duration_seconds_count",
        Some(&["sink_type"]),
    ),
    (
        "data_avalanche_sink_commit_failures_total",
        Some(&["sink_type"]),
    ),
    ("data_avalanche_sink_merge_files_rewritten_total", Some(&[])),
    ("data_avalanche_sink_merge_rows_copied_total", Some(&[])),
    ("data_avalanche_sink_soft_dropped_columns", Some(&[])),
    // Clustering
    ("data_avalanche_lease_state", Some(&["state"])),
    (
        "data_avalanche_lease_renew_failures_total",
        Some(&["class"]),
    ),
    (
        "data_avalanche_lease_renew_latency_seconds_count",
        Some(&[]),
    ),
    ("data_avalanche_cluster_members", None),
    ("data_avalanche_owned_pipelines", None),
    ("data_avalanche_ring_generation", None),
    // Status
    ("data_avalanche_pipeline_status", Some(&["status"])),
    ("data_avalanche_halted_tables", Some(&[])),
];

/// The per-table series `sink.metrics.per_table` adds `table` to, with their other labels.
const PER_TABLE: &[(&str, &[&str])] = &[
    ("data_avalanche_snapshot_rows_done", &["table"]),
    ("data_avalanche_snapshot_state", &["table", "state"]),
];

fn labels() -> PipelineLabels {
    PipelineLabels {
        pipeline: "orders".into(),
        source_host: "db1".into(),
        source_db: "shop".into(),
        sink_type: "delta".into(),
    }
}

/// Each sample name in the text, with the label names of each of its lines.
fn samples(text: &str) -> Vec<(String, BTreeSet<String>)> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .map(|l| {
            let name_end = l.find(['{', ' ']).unwrap_or(l.len());
            let names = match l[name_end..].strip_prefix('{') {
                Some(rest) => rest[..rest.find('}').expect("a closed label set")]
                    .split(',')
                    .map(|kv| kv.split('=').next().unwrap().to_owned())
                    .collect(),
                None => BTreeSet::new(),
            };
            (l[..name_end].to_owned(), names)
        })
        .collect()
}

/// Sets every series a pipeline and the node have.
fn everything(per_table: bool) -> Metrics {
    let m = Metrics::new();
    let p = m.pipeline(labels(), per_table);
    p.status(PipelineStatus::Running);
    p.lease(LeaseState::Held);
    p.renew_failed(RenewFailure::Transient);
    p.renewed(Duration::from_millis(5));
    p.wal_lag_bytes(Some(1));
    p.binlog_lag(Some(Duration::from_secs(1)));
    p.snapshot_rows_done(&[("public.a".to_owned(), 5)]);
    p.snapshot_state(&[("public.a".to_owned(), SnapshotState::Running)]);
    p.halted_tables(0);
    p.reconcile_log(0, 0);
    p.replication_lag(Duration::ZERO);
    p.transaction_buffer_bytes(0);
    p.soft_dropped_columns(0);
    p.flushed(1, Duration::from_millis(1));
    p.committed(1, Duration::from_millis(1));
    p.commit_failed();
    p.chunk_read(Duration::from_millis(1));
    p.totals(Totals {
        source_reconnects: 1,
        sink_merge_files_rewritten: 1,
        sink_merge_rows_copied: 1,
        rows_read: RowsRead {
            insert: 1,
            ..RowsRead::default()
        },
    });
    m.owned_by(1);
    m.set_cluster_members(1);
    m.set_ring_generation(1);
    m
}

#[test]
fn every_mandatory_series_is_exported_with_its_labels() {
    let found = samples(&everything(false).encode());
    for (name, extra) in CATALOGUE {
        let want: BTreeSet<String> = match extra {
            Some(extra) => PIPELINE
                .iter()
                .chain(*extra)
                .map(|s| (*s).to_owned())
                .collect(),
            None => BTreeSet::new(),
        };
        let lines: Vec<_> = found.iter().filter(|(n, _)| n == name).collect();
        assert!(!lines.is_empty(), "{name} isn't exported");
        for (_, labels) in lines {
            assert_eq!(labels, &want, "{name}");
        }
    }
}

#[test]
fn per_table_adds_the_table_label_to_the_snapshot_series_only() {
    let found = samples(&everything(true).encode());
    for (name, extra) in PER_TABLE {
        let want: BTreeSet<String> = PIPELINE
            .iter()
            .chain(*extra)
            .map(|s| (*s).to_owned())
            .collect();
        let lines: Vec<_> = found.iter().filter(|(n, _)| n == name).collect();
        assert!(!lines.is_empty(), "{name} isn't exported");
        for (_, labels) in lines {
            assert_eq!(labels, &want, "{name}");
        }
    }
    let tabled: BTreeSet<&str> = found
        .iter()
        .filter(|(_, l)| l.contains("table"))
        .map(|(n, _)| n.as_str())
        .collect();
    assert_eq!(
        tabled,
        PER_TABLE.iter().map(|(n, _)| *n).collect::<BTreeSet<_>>(),
        "no other series takes the table label"
    );
}

/// Each series the text names (`data_avalanche_…`), with the labels its selector matches on.
fn selectors(text: &str) -> Vec<(String, Vec<String>)> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("data_avalanche_") {
        rest = &rest[at..];
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        let name = rest[..end].to_owned();
        rest = &rest[end..];
        let mut labels = Vec::new();
        if let Some(selector) = rest.strip_prefix('{') {
            let close = selector.find('}').expect("a closed selector");
            labels = selector[..close]
                .split(',')
                .filter_map(|m| m.split_once('=').map(|(l, _)| l.trim().to_owned()))
                .collect();
        }
        found.push((name, labels));
    }
    found
}

/// The example alert rules (`deploy/prometheus/alerts.yaml`, roadmap M10) name only series of
/// the catalogue, and match only on the labels those carry; promtool runs them in CI.
#[test]
fn the_alert_rules_name_only_catalogued_series_and_labels() {
    let rules = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/prometheus/alerts.yaml"
    ));
    let named = selectors(rules);
    assert_eq!(named.len(), 5, "{named:?}");
    for (name, labels) in named {
        let (_, extra) = CATALOGUE
            .iter()
            .find(|(n, _)| *n == name)
            .unwrap_or_else(|| panic!("{name} isn't in the catalogue"));
        let carried: Vec<&str> = PIPELINE
            .iter()
            .chain(extra.unwrap_or_default())
            .copied()
            .collect();
        for label in labels {
            assert!(carried.contains(&label.as_str()), "{name} has no {label}");
        }
    }
}
