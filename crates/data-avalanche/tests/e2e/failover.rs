//! A PostgreSQL failover (DA-SRC-20) on test-infra's `replica` pair, named by
//! `DATA_AVALANCHE_PG_REPLICA=<primary port>:<replica port>`: the pipeline reads the primary, the
//! replica is cut off and promoted behind it, and the pipeline is moved to it. The promoted
//! server's LSNs repeat ones the lake has marked, so the start halts with `WAL_SOURCE_CHANGED`;
//! under `source_change_action: re_snapshot` it re-snapshots from the new server and streams on
//! past the old markers.

use std::time::Duration;

use data_avalanche_core::SourcePosition;
use data_avalanche_core::position::Lsn;
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};
use crate::source::{Db, Source};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

/// The primary's and the replica's ports, or `None` (the scenario skipped) without the variable.
fn pair() -> Option<(u16, u16)> {
    let v = std::env::var("DATA_AVALANCHE_PG_REPLICA").ok()?;
    let (primary, replica) = v
        .split_once(':')
        .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)))
        .unwrap_or_else(|| panic!("DATA_AVALANCHE_PG_REPLICA={v}: <primary port>:<replica port>"));
    Some((primary, replica))
}

/// Whether the log has the line of a pipeline in `ERROR` with `code` (Q-M7b-4).
fn halted_with(log: &str, code: &str) -> bool {
    log.lines()
        .any(|l| l.contains("pipeline in ERROR") && l.contains(&format!("code=\"{code}\"")))
}

/// The keys of `items` on `db`, in order.
async fn ids(db: &Db, schema: &str) -> Vec<i64> {
    db.text(&format!("SELECT id FROM {schema}.items ORDER BY id"))
        .await
        .expect("the ids")
        .into_iter()
        .map(|r| r[0].as_deref().expect("an id").parse().expect("a number"))
        .collect()
}

/// The lake holds `want`, within a minute.
async fn holds(e: &E2e, want: &[i64]) {
    let what = format!("the lake holds {want:?}");
    eventually(&what, Duration::from_secs(60), async || {
        e.ids("items").await == want
    })
    .await;
}

#[tokio::test]
async fn a_promoted_replica_halts_the_pipeline_then_re_snapshots_on_request() {
    require_it!();
    crate::postgres_only!();
    let Some((primary, replica)) = pair() else {
        eprintln!("skipped: DATA_AVALANCHE_PG_REPLICA unset (test-infra's replica profile)");
        return;
    };
    let standby = Db::connect(Source::Postgres { port: replica }).await;
    if standby
        .text("SELECT pg_is_in_recovery()")
        .await
        .expect("recovery")[0][0]
        .as_deref()
        != Some("t")
    {
        panic!(
            "postgres-replica isn't a standby (an earlier run promoted it): recreate the pair with \
             docker compose -f test-infra/compose.yaml --profile replica up -d --wait \
             --force-recreate postgres-primary postgres-replica"
        );
    }
    let e = E2e::on(
        Source::Postgres { port: primary },
        "pg_failover",
        &format!("{ITEMS} INSERT INTO items SELECT g, 'p' FROM generate_series(1, 5) g;"),
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let s = e.schema.clone();
    let mut p = e.start(&[]);
    p.ready().await;
    holds(&e, &[1, 2, 3, 4, 5]).await;
    e.streaming().await;
    eventually(
        "the replica has 1..5",
        Duration::from_secs(30),
        async || ids(&standby, &s).await.len() == 5,
    )
    .await;

    // The replica stops receiving WAL. The primary goes on, a WAL segment further (the lake's
    // markers well past anything the promoted replica writes next).
    for sql in [
        "ALTER SYSTEM SET primary_conninfo = ''",
        "SELECT pg_reload_conf()",
    ] {
        standby.exec(sql).await.expect(sql);
    }
    eventually(
        "the replica's WAL receiver stops",
        Duration::from_secs(30),
        async || !standby.any("SELECT 1 FROM pg_stat_wal_receiver").await,
    )
    .await;
    e.exec("INSERT INTO items SELECT g, 'p' FROM generate_series(6, 9) g")
        .await;
    e.db.exec("SELECT pg_switch_wal()")
        .await
        .expect("a WAL switch");
    e.exec("INSERT INTO items VALUES (10, 'p')").await;
    holds(&e, &(1..=10).collect::<Vec<_>>()).await;
    let marker = e.marker("items").await;

    // The failover: the replica, promoted, takes writes on a timeline of its own.
    standby
        .exec("SELECT pg_promote()")
        .await
        .expect("the promotion");
    standby
        .exec(&format!(
            "INSERT INTO {s}.items VALUES (11, 'r'), (12, 'r')"
        ))
        .await
        .expect("writes on the promoted replica");
    let lsn = standby
        .text("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")
        .await
        .expect("the LSN")[0][0]
        .as_deref()
        .expect("an LSN")
        .parse::<u64>()
        .expect("a number");
    let SourcePosition::Pg(Lsn(marked)) = marker else {
        panic!("a PostgreSQL marker: {marker}")
    };
    assert!(
        lsn < marked,
        "the promoted replica's LSN {lsn} is below the lake's marker {marked}: the guard would skip its changes"
    );

    // The pipeline moved to the promoted replica (a source change restarts it): its start
    // finds another server than the checkpoint's and halts.
    let first = e.config();
    let moved = first.replacen(
        &format!("      port: {primary}\n"),
        &format!("      port: {replica}\n"),
        1,
    );
    assert_ne!(moved, first);
    e.write_config(&moved);
    assert_eq!(p.reload().await.0, 202);
    p.metric(&e.status("error")).await;
    let log = p.log();
    assert!(halted_with(&log, "WAL_SOURCE_CHANGED"), "{log}");
    assert_eq!(e.ids("items").await, (1..=10).collect::<Vec<_>>());

    // Asked to re-snapshot: the table is copied again from the promoted replica, and a change
    // written there after it, at an LSN below the old marker, streams in.
    let snap = moved.replacen(
        &format!("slot: {s}"),
        &format!("slot: {s}, source_change_action: re_snapshot"),
        1,
    );
    assert_ne!(snap, moved);
    e.write_config(&snap);
    assert_eq!(p.reload().await.0, 202);
    assert_eq!(p.ask(&e.test, "restart").await.0, 202);
    p.metric(&e.status("running")).await;
    // Rows 6–10 are ghosts: the promoted server never had them, and a snapshot adds and
    // updates rows but deletes none.
    holds(&e, &(1..=12).collect::<Vec<_>>()).await;
    standby
        .exec(&format!("INSERT INTO {s}.items VALUES (13, 'r')"))
        .await
        .expect("a write after the recovery");
    holds(&e, &(1..=13).collect::<Vec<_>>()).await;
    let log = p.log();
    assert!(
        log.contains("source_change_action is re_snapshot, so the pipeline re-snapshots"),
        "{log}"
    );
    assert!(p.terminate().await.success());
    standby
        .exec(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
              WHERE slot_name = '{s}' AND NOT active;
             DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;"
        ))
        .await
        .expect("the promoted replica's copy dropped");
    e.cleanup().await;
}
