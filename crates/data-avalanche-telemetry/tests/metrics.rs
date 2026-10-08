use std::time::Duration;

use data_avalanche_telemetry::{
    LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, Totals,
};

const L: &str = r#"pipeline="orders",source_host="db1",source_db="shop""#;

fn labels() -> PipelineLabels {
    PipelineLabels {
        pipeline: "orders".into(),
        source_host: "db1".into(),
        source_db: "shop".into(),
    }
}

/// The text's sample lines that start with `name`, sorted.
fn samples(text: &str, name: &str) -> Vec<String> {
    let mut lines: Vec<String> = text
        .lines()
        .filter(|l| !l.starts_with('#') && l.starts_with(name))
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

#[test]
fn the_status_and_the_lease_are_one_hot() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.status(PipelineStatus::Running);
    p.status(PipelineStatus::Paused);
    p.lease(LeaseState::Degraded);
    let text = m.encode();
    assert_eq!(
        samples(&text, "data_avalanche_pipeline_status"),
        [
            format!(r#"data_avalanche_pipeline_status{{{L},status="error"}} 0"#),
            format!(r#"data_avalanche_pipeline_status{{{L},status="paused"}} 1"#),
            format!(r#"data_avalanche_pipeline_status{{{L},status="running"}} 0"#),
            format!(r#"data_avalanche_pipeline_status{{{L},status="stopped"}} 0"#),
        ]
    );
    assert_eq!(
        samples(&text, "data_avalanche_lease_state"),
        [
            format!(r#"data_avalanche_lease_state{{{L},state="degraded"}} 1"#),
            format!(r#"data_avalanche_lease_state{{{L},state="held"}} 0"#),
            format!(r#"data_avalanche_lease_state{{{L},state="lost"}} 0"#),
        ]
    );
    assert!(text.ends_with("# EOF\n"), "{text}");
}

#[test]
fn renewals_count_their_failures_and_time_their_successes() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.renew_failed(RenewFailure::Transient);
    p.renew_failed(RenewFailure::Transient);
    p.renewed(Duration::from_millis(20));
    let text = m.encode();
    assert_eq!(
        samples(&text, "data_avalanche_lease_renew_failures_total"),
        [format!(
            r#"data_avalanche_lease_renew_failures_total{{{L},class="transient"}} 2"#
        )]
    );
    assert!(
        text.contains(&format!(
            "data_avalanche_lease_renew_latency_seconds_count{{{L}}} 1"
        )),
        "{text}"
    );
}

#[test]
fn counters_keep_counting_across_workers() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    let totals = |n| Totals {
        source_reconnects: n,
        sink_merge_files_rewritten: 10 * n,
        sink_merge_rows_copied: 100 * n,
    };
    p.totals(totals(2));
    p.totals(totals(3));
    p.worker_ended();
    p.totals(totals(1));
    let text = m.encode();
    assert_eq!(
        samples(&text, "data_avalanche_source_reconnects_total"),
        [format!("data_avalanche_source_reconnects_total{{{L}}} 4")]
    );
    assert_eq!(
        samples(&text, "data_avalanche_sink_merge_files_rewritten_total"),
        [format!(
            "data_avalanche_sink_merge_files_rewritten_total{{{L}}} 40"
        )]
    );
    assert_eq!(
        samples(&text, "data_avalanche_sink_merge_rows_copied_total"),
        [format!(
            "data_avalanche_sink_merge_rows_copied_total{{{L}}} 400"
        )]
    );
}

#[test]
fn snapshot_rows_are_summed_unless_per_table() {
    let m = Metrics::new();
    let rows = [("public.a".to_owned(), 5), ("public.b".to_owned(), 7)];
    m.pipeline(labels(), false).snapshot_rows_done(&rows);
    assert_eq!(
        samples(&m.encode(), "data_avalanche_snapshot_rows_done"),
        [format!("data_avalanche_snapshot_rows_done{{{L}}} 12")]
    );

    let m = Metrics::new();
    let p = m.pipeline(labels(), true);
    p.snapshot_rows_done(&rows);
    p.snapshot_rows_done(&rows[1..]);
    assert_eq!(
        samples(&m.encode(), "data_avalanche_snapshot_rows_done"),
        [format!(
            r#"data_avalanche_snapshot_rows_done{{{L},table="public.b"}} 7"#
        )],
        "a finished snapshot loses its series"
    );
}

#[test]
fn a_pipeline_the_node_no_longer_owns_loses_its_series() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), true);
    p.status(PipelineStatus::Running);
    p.lease(LeaseState::Held);
    p.renew_failed(RenewFailure::Conflict);
    p.renewed(Duration::from_millis(5));
    p.wal_lag_bytes(Some(4096));
    p.snapshot_rows_done(&[("public.a".to_owned(), 5)]);
    p.halted_tables(1);
    p.reconcile_log(3, 9);
    p.totals(Totals {
        source_reconnects: 1,
        ..Totals::default()
    });
    m.owned_by(1);
    m.set_cluster_members(1);
    p.remove();
    m.owned_by(-1);
    let text = m.encode();
    assert!(!text.contains("orders"), "{text}");
    assert!(text.contains("data_avalanche_owned_pipelines 0"), "{text}");
    assert!(text.contains("data_avalanche_cluster_members 1"), "{text}");
}

#[test]
fn the_gauges_read_what_was_set_last() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.wal_lag_bytes(Some(4096));
    p.halted_tables(2);
    p.reconcile_log(3, 9);
    let text = m.encode();
    for line in [
        format!("data_avalanche_wal_lag_bytes{{{L}}} 4096"),
        format!("data_avalanche_halted_tables{{{L}}} 2"),
        format!("data_avalanche_reconcile_log_entries{{{L}}} 3"),
        format!("data_avalanche_reconcile_log_keys{{{L}}} 9"),
    ] {
        assert!(text.lines().any(|l| l == line), "{line}\n{text}");
    }
    p.wal_lag_bytes(None);
    assert!(!m.encode().contains("data_avalanche_wal_lag_bytes{"));
}
