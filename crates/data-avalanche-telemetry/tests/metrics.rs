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
fn a_pipeline_the_node_no_longer_owns_loses_its_series_but_its_renew_failures() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), true);
    p.status(PipelineStatus::Running);
    p.lease(LeaseState::Held);
    p.renew_failed(RenewFailure::Conflict);
    p.renew_failed(RenewFailure::Lost);
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
    m.set_ring_generation(3);
    p.remove();
    m.owned_by(-1);
    let text = m.encode();
    // A counter stays: the loss that sent the node to standby is still scraped after it.
    let failures = "data_avalanche_lease_renew_failures_total";
    assert_eq!(
        samples(&text, failures),
        [
            format!(r#"{failures}{{{L},class="conflict"}} 1"#),
            format!(r#"{failures}{{{L},class="lost"}} 1"#),
        ]
    );
    assert!(
        !text
            .lines()
            .any(|l| l.contains("orders") && !l.starts_with(failures)),
        "{text}"
    );
    assert!(text.contains("data_avalanche_owned_pipelines 0"), "{text}");
    assert!(text.contains("data_avalanche_cluster_members 1"), "{text}");
    assert!(text.contains("data_avalanche_ring_generation 3"), "{text}");
    // Owned again: the count goes on from where it was.
    p.renew_failed(RenewFailure::Lost);
    assert_eq!(
        samples(&m.encode(), &format!("{failures}{{{L},class=\"lost\"")),
        [format!(r#"{failures}{{{L},class="lost"}} 2"#)]
    );
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

/// A renamed pipeline (config §12.3) loses every series under its old name, the renew failures
/// too; what is set next carries the new name. The same labels again change nothing.
#[test]
fn a_relabelled_pipeline_drops_its_old_series() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.status(PipelineStatus::Running);
    p.renew_failed(RenewFailure::Lost);
    p.relabel(labels(), false);
    assert_eq!(
        samples(&m.encode(), "data_avalanche_lease_renew_failures_total"),
        [format!(
            r#"data_avalanche_lease_renew_failures_total{{{L},class="lost"}} 1"#
        )],
        "the same labels: nothing changed"
    );
    let renamed = PipelineLabels {
        pipeline: "renamed".into(),
        ..labels()
    };
    p.relabel(renamed, true);
    p.status(PipelineStatus::Running);
    p.snapshot_rows_done(&[("public.a".to_owned(), 5)]);
    let text = m.encode();
    assert!(!text.contains(r#"pipeline="orders""#), "{text}");
    let new = r#"pipeline="renamed",source_host="db1",source_db="shop""#;
    assert!(
        text.lines()
            .any(|l| l == format!(r#"data_avalanche_pipeline_status{{{new},status="running"}} 1"#)),
        "{text}"
    );
    assert!(
        text.lines()
            .any(|l| l
                == format!(r#"data_avalanche_snapshot_rows_done{{{new},table="public.a"}} 5"#)),
        "per_table follows: {text}"
    );
}

/// A pipeline removed from the config loses every series, the renew failures too.
#[test]
fn a_forgotten_pipeline_keeps_no_series() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.status(PipelineStatus::Running);
    p.renew_failed(RenewFailure::Lost);
    p.forget();
    let text = m.encode();
    assert!(!text.contains("orders"), "{text}");
}

/// The config generation this node applies (Clustering §9): a node behind the cluster shows it.
#[test]
fn the_node_reports_its_config_generation() {
    let m = Metrics::new();
    m.set_config_generation(3);
    let text = m.encode();
    assert!(
        text.lines()
            .any(|l| l == "data_avalanche_config_generation 3"),
        "{text}"
    );
}
