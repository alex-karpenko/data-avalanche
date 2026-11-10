//! A PostgreSQL failover (DA-SRC-20) on test-infra's `replica` pair, named by
//! `DATA_AVALANCHE_PG_REPLICA=<primary port>:<replica port>`: the pipeline reads the primary, the
//! replica is cut off and promoted behind it, and the pipeline is moved to it. The promoted
//! server's LSNs repeat ones the lake has marked, so the start halts with `WAL_SOURCE_CHANGED`;
//! under `source_change_action: re_snapshot` it re-snapshots from the new server and streams on
//! past the old markers.
//!
//! The slot-sync pair (`DATA_AVALANCHE_PG_SYNC_REPLICA=<primary port>:<replica port>`, DA-SRC-33):
//! the pipeline's slot is a failover slot the standby synchronizes, so a promotion it followed
//! continues the stream on the new timeline, without a halt or a re-snapshot.

use std::time::Duration;

use data_avalanche_core::SourcePosition;
use data_avalanche_core::position::Lsn;
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};
use crate::source::{Db, Source};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

/// The primary's and the replica's ports `var` names, or `None` (the scenario skipped) without it.
fn pair(var: &str) -> Option<(u16, u16)> {
    let v = std::env::var(var).ok()?;
    let (primary, replica) = v
        .split_once(':')
        .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)))
        .unwrap_or_else(|| panic!("{var}={v}: <primary port>:<replica port>"));
    Some((primary, replica))
}

/// The standby on `port`, which must not have been promoted by an earlier run.
async fn standby(port: u16, services: &str) -> Db {
    let standby = Db::connect(Source::Postgres { port }).await;
    if standby
        .text("SELECT pg_is_in_recovery()")
        .await
        .expect("recovery")[0][0]
        .as_deref()
        != Some("t")
    {
        panic!(
            "the replica isn't a standby (an earlier run promoted it): recreate the pair with \
             docker compose -f test-infra/compose.yaml --profile replica up -d --wait \
             --force-recreate {services}"
        );
    }
    standby
}

/// Whether the log has the line of a pipeline in `ERROR` with `code` (Q-M7b-4).
fn halted_with(log: &str, code: &str) -> bool {
    log.lines()
        .any(|l| l.contains("pipeline in ERROR") && l.contains(&format!("code=\"{code}\"")))
}

/// The promoted server's current WAL position.
async fn wal_lsn(db: &Db) -> u64 {
    db.text("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")
        .await
        .expect("the LSN")[0][0]
        .as_deref()
        .expect("an LSN")
        .parse::<u64>()
        .expect("a number")
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
    let Some((primary, replica)) = pair("DATA_AVALANCHE_PG_REPLICA") else {
        eprintln!("skipped: DATA_AVALANCHE_PG_REPLICA unset (test-infra's replica profile)");
        return;
    };
    let standby = standby(replica, "postgres-primary postgres-replica").await;
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
    let first_doc = e.completed("items", None).await;
    e.streaming().await;
    eventually(
        "the replica has 1..5",
        Duration::from_secs(30),
        async || {
            standby
                .text(&format!("SELECT id FROM {s}.items"))
                .await
                .map(|r| r.len() == 5)
                .unwrap_or(false)
        },
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
    let promoted = standby
        .text("SELECT pg_promote()")
        .await
        .expect("the promotion");
    assert_eq!(
        promoted[0][0].as_deref(),
        Some("t"),
        "pg_promote() didn't finish promoting the replica"
    );
    standby
        .exec(&format!(
            "INSERT INTO {s}.items VALUES (11, 'r'), (12, 'r')"
        ))
        .await
        .expect("writes on the promoted replica");
    let lsn = wal_lsn(&standby).await;
    let SourcePosition::Pg(Lsn(marked)) = marker else {
        panic!("a PostgreSQL marker: {marker}")
    };
    assert!(
        lsn < marked,
        "the promoted replica's LSN {lsn} isn't below the lake's marker {marked}: the scenario proves nothing"
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
    // The re-snapshot is over (its last chunk read too), so row 13 below can only come by stream.
    e.completed("items", Some(&first_doc)).await;
    // Every table's marker is reset to the new server's position, below the old marker.
    let reset = e.marker("items").await;
    let SourcePosition::Pg(Lsn(reset)) = reset else {
        panic!("a PostgreSQL marker: {reset}")
    };
    assert!(
        reset < marked,
        "the marker {reset} wasn't reset below the old marker {marked}"
    );
    // Rows 6–10 are ghosts: the promoted server never had them, and a snapshot adds and
    // updates rows but deletes none.
    holds(&e, &(1..=12).collect::<Vec<_>>()).await;
    standby
        .exec(&format!("INSERT INTO {s}.items VALUES (13, 'r')"))
        .await
        .expect("a write after the recovery");
    holds(&e, &(1..=13).collect::<Vec<_>>()).await;
    // The stream delivered a change at an LSN below the old marker.
    let streamed = wal_lsn(&standby).await;
    assert!(
        streamed < marked,
        "row 13 at {streamed}, old marker {marked}"
    );
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

/// DA-SRC-33 (Q-M12d-5): the pipeline makes its slot a failover slot (PostgreSQL 17+), and the
/// standby synchronizes it. Stopped once the slot holds the lake's markers, the pipeline is moved
/// to the promoted standby: its start finds the slot there, synchronized past every marker, and
/// the stream continues on the new timeline, under `source_change_action`'s default (`error`),
/// without a re-snapshot.
#[tokio::test]
async fn a_promotion_the_failover_slot_followed_continues_the_stream() {
    require_it!();
    crate::postgres_only!();
    let Some((primary, replica)) = pair("DATA_AVALANCHE_PG_SYNC_REPLICA") else {
        eprintln!("skipped: DATA_AVALANCHE_PG_SYNC_REPLICA unset (test-infra's replica profile)");
        return;
    };
    let standby = standby(replica, "postgres-sync-primary postgres-sync-replica").await;
    let e = E2e::on(
        Source::Postgres { port: primary },
        "pg_failover_slot",
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
    let first_doc = e.completed("items", None).await;
    e.streaming().await;
    e.exec("INSERT INTO items SELECT g, 'p' FROM generate_series(6, 8) g")
        .await;
    holds(&e, &(1..=8).collect::<Vec<_>>()).await;
    // The standby's first copy of the slot waits until the slot's catalog_xmin passes the
    // standby's own: the streaming pipeline moves it, decoding the standby snapshots logged on
    // the primary (measured on PostgreSQL 18: a slot no one reads is never synchronized).
    eventually(
        "the standby synchronizes the slot",
        Duration::from_secs(180),
        async || {
            let _ = e.db.exec("SELECT pg_log_standby_snapshot()").await;
            standby
                .any(&format!(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{s}' AND synced"
                ))
                .await
        },
    )
    .await;
    let marker = e.marker("items").await;
    eventually(
        "the slot confirms the marker",
        Duration::from_secs(60),
        async || e.acknowledges(&marker).await,
    )
    .await;
    assert!(p.terminate().await.success());
    assert_eq!(
        e.db.text(&format!(
            "SELECT failover FROM pg_replication_slots WHERE slot_name = '{s}'"
        ))
        .await
        .expect("the slot")[0][0]
            .as_deref(),
        Some("t"),
        "a managed slot is a failover slot on PostgreSQL 17+"
    );

    // The standby's copy follows the stopped pipeline's slot up to its checkpoint.
    let checkpoint = e
        .docs()
        .slot()
        .await
        .expect("the slot document reads")
        .expect("a checkpoint")
        .doc
        .confirmed_flush_lsn;
    eventually(
        "the standby's copy of the slot reaches the checkpoint",
        Duration::from_secs(180),
        async || {
            standby
                .any(&format!(
                    "SELECT 1 FROM pg_replication_slots
                      WHERE slot_name = '{s}' AND synced AND failover
                        AND confirmed_flush_lsn >= '{checkpoint}'::pg_lsn
                        AND confirmed_flush_lsn >= '{}'::pg_lsn",
                    marker.untagged()
                ))
                .await
        },
    )
    .await;

    // The failover: the standby, promoted, takes writes on a timeline of its own.
    let promoted = standby
        .text("SELECT pg_promote()")
        .await
        .expect("the promotion");
    assert_eq!(
        promoted[0][0].as_deref(),
        Some("t"),
        "pg_promote() didn't finish"
    );
    standby
        .exec(&format!("INSERT INTO {s}.items VALUES (9, 'r')"))
        .await
        .expect("a write on the promoted standby");

    // The pipeline, moved to it, streams on: no halt, no re-snapshot. The node runs the
    // cluster's config generation, the old server's, until the move is reloaded.
    let mut p = e.start(&[]);
    p.ready().await;
    let first = e.config();
    let moved = first.replacen(
        &format!("      port: {primary}\n"),
        &format!("      port: {replica}\n"),
        1,
    );
    assert_ne!(moved, first);
    e.write_config(&moved);
    assert_eq!(p.reload().await.0, 202);
    p.metric(&e.status("running")).await;
    holds(&e, &(1..=9).collect::<Vec<_>>()).await;
    standby
        .exec(&format!("INSERT INTO {s}.items VALUES (10, 'r')"))
        .await
        .expect("a second write");
    holds(&e, &(1..=10).collect::<Vec<_>>()).await;
    assert_eq!(
        e.snapshot("items").await.and_then(|d| d.completed_at),
        first_doc.completed_at,
        "no re-snapshot"
    );
    let log = p.log();
    assert!(
        log.contains("so the stream continues on the new timeline"),
        "{log}"
    );
    assert!(!log.contains("WAL_SOURCE_CHANGED"), "{log}");
    assert!(p.terminate().await.success());
    standby
        .exec(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
              WHERE slot_name = '{s}' AND NOT active;
             DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;"
        ))
        .await
        .expect("the promoted standby's copy dropped");
    e.cleanup().await;
}
