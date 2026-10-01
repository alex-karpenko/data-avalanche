//! A slot PostgreSQL invalidated (`wal_status = 'lost'`) is recreated under `managed` (D-M5.13).
//! Its own test binary, which cargo never runs beside the `pg` tests: it lowers the server-wide
//! `max_slot_wal_keep_size`, which would invalidate their slots too.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "pg/common.rs"]
mod common;

use data_avalanche_core::{SnapshotReason, StartPlan};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, fresh, pipeline};

#[tokio::test]
async fn an_invalidated_slot_is_recreated() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "slot_invalidated",
        "CREATE TABLE t (id serial PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let tables = [n.table("t")];
    let p = c.prepare(None, &tables).await.unwrap();
    let StartPlan::Snapshot { from, .. } = p.plan else {
        panic!("a first start snapshots")
    };

    // WAL past the slot, beyond a 1 MB allowance, then a checkpoint removes it.
    // ALTER SYSTEM can't run in the implicit transaction of a multi-statement batch.
    for sql in [
        "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'",
        "SELECT pg_reload_conf()",
    ] {
        a.batch_execute(sql).await.unwrap();
    }
    for _ in 0..3 {
        a.batch_execute(&format!(
            "INSERT INTO {}.t DEFAULT VALUES; SELECT pg_switch_wal()",
            n.schema
        ))
        .await
        .unwrap();
    }
    let lost = a.batch_execute("CHECKPOINT").await.map(|_| ());
    let status: Option<String> = a
        .query_one(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
            &[&n.slot],
        )
        .await
        .unwrap()
        .get(0);
    for sql in [
        "ALTER SYSTEM RESET max_slot_wal_keep_size",
        "SELECT pg_reload_conf()",
    ] {
        a.batch_execute(sql).await.unwrap();
    }
    lost.unwrap();
    assert_eq!(status.as_deref(), Some("lost"));

    let p = c.prepare(Some(&from), &tables).await.unwrap();
    let StartPlan::Snapshot { reason, .. } = p.plan else {
        panic!("a lost slot snapshots: {:?}", p.plan)
    };
    assert_eq!(reason, SnapshotReason::PositionLost);
    let status: Option<String> = a
        .query_one(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
            &[&n.slot],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(status.as_deref(), Some("lost"), "a new slot");
    cleanup(&a, &n).await;
}
