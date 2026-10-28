//! The slot state machine cell by cell (`Postgres_Slot…` §1–§3, the M5 exit) and publication
//! reconciliation (§4) against a live server.

use std::time::Duration;

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    Checkpoint, SnapshotReason, SourceConnector, SourceError, SourcePosition, StartPlan, TableRef,
};
use data_avalanche_testkit::require_it;
use tokio_postgres::Client;

use crate::common::{Names, admin, cleanup, connector, first_stream, fresh, pipeline};

const DDL: &str = "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);
                   CREATE TABLE c (id int PRIMARY KEY);";

async fn confirmed(a: &Client, n: &Names) -> Option<Lsn> {
    a.query_opt(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        &[&n.slot],
    )
    .await
    .unwrap()
    .map(|r| r.get::<_, String>(0).parse().unwrap())
}

async fn published(a: &Client, n: &Names) -> Vec<String> {
    a.query(
        "SELECT tablename::text FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1",
        &[&n.publication],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| r.get(0))
    .collect()
}

fn from(plan: &StartPlan, expect: SnapshotReason) -> SourcePosition {
    match plan {
        StartPlan::Snapshot { from, reason } if *reason == expect => from.clone(),
        other => panic!("a {expect:?} snapshot expected, got {other:?}"),
    }
}

fn permanent(e: SourceError, needle: &str) {
    match e {
        SourceError::Permanent(m) => assert!(m.contains(needle), "{m:?} should mention {needle:?}"),
        other => panic!("permanent expected, got {other:?}"),
    }
}

#[tokio::test]
async fn first_start_then_resume_then_a_lost_slot() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_cells", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let tables = [n.table("a"), n.table("b")];

    // state absent × slot absent: create, snapshot from the slot's consistent point.
    let p = c.prepare(None, &tables).await.unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    assert!(p.warnings.is_empty());
    assert_eq!(
        confirmed(&a, &n).await.map(SourcePosition::Pg),
        Some(start.clone())
    );
    assert_eq!(published(&a, &n).await, ["a", "b"]);

    // state present × slot present: resume.
    let p = c
        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
        .await
        .unwrap();
    assert_eq!(p.plan, StartPlan::Resume);

    // state present × slot absent: recreate, snapshot (D-15.3).
    a.execute("SELECT pg_drop_replication_slot($1)", &[&n.slot])
        .await
        .unwrap();
    let p = c
        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
        .await
        .unwrap();
    let again = from(&p.plan, SnapshotReason::PositionLost);
    assert!(confirmed(&a, &n).await.is_some(), "recreated");

    // A slot confirmed past the checkpoint lost what lies between: recreate, snapshot.
    a.execute(&format!("INSERT INTO {}.a VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    a.execute(
        "SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())",
        &[&n.slot],
    )
    .await
    .unwrap();
    let p = c
        .prepare(Some(&Checkpoint::from(again.clone())), &tables)
        .await
        .unwrap();
    from(&p.plan, SnapshotReason::PositionLost);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn an_orphan_halts_by_default_and_is_adopted_on_request() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_orphan", DDL).await;
    a.execute(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        &[&n.slot],
    )
    .await
    .unwrap();
    let tables = [n.table("a")];
    let halt = connector(&pipeline(&n, &["*"], "", ""));
    permanent(halt.prepare(None, &tables).await.unwrap_err(), "orphan");

    a.execute(&format!("INSERT INTO {}.a VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let before = confirmed(&a, &n).await.unwrap();
    let adopt = connector(&pipeline(
        &n,
        &["*"],
        "",
        ", slot_on_orphan: adopt_resnapshot",
    ));
    let p = adopt.prepare(None, &tables).await.unwrap();
    let SourcePosition::Pg(at) = from(&p.plan, SnapshotReason::AdoptedOrphan) else {
        panic!("a PostgreSQL position")
    };
    assert!(at > before, "advanced past the orphan's old position");
    assert_eq!(confirmed(&a, &n).await, Some(at));
    cleanup(&a, &n).await;
}

/// A reload's `slot_on_orphan` reaches the running connector: its next prepare adopts the slot
/// the last one halted on.
#[tokio::test]
async fn a_reloaded_orphan_policy_takes_the_next_prepare() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_orphan_reload", DDL).await;
    a.execute(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        &[&n.slot],
    )
    .await
    .unwrap();
    let tables = [n.table("a")];
    let c = connector(&pipeline(&n, &["*"], "", ""));
    permanent(c.prepare(None, &tables).await.unwrap_err(), "orphan");
    let reloaded = pipeline(&n, &["*"], "", ", slot_on_orphan: adopt_resnapshot");
    c.reconfigure(&reloaded);
    let p = c.prepare(None, &tables).await.unwrap();
    from(&p.plan, SnapshotReason::AdoptedOrphan);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn require_existing_never_creates() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_existing", DDL).await;
    let postgres = ", slot_management: require_existing, auto_cleanup: false";
    let c = connector(&pipeline(&n, &["*"], "", postgres));
    let tables = [n.table("a"), n.table("b")];
    permanent(
        c.prepare(None, &tables).await.unwrap_err(),
        &format!("publication {}", n.publication),
    );

    a.batch_execute(&format!(
        "CREATE PUBLICATION {} FOR TABLE {}.a",
        n.publication, n.schema
    ))
    .await
    .unwrap();
    permanent(c.prepare(None, &tables).await.unwrap_err(), "doesn't exist");
    let start: String = a
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&n.slot],
        )
        .await
        .unwrap()
        .get(0);
    let start = SourcePosition::Pg(start.parse().unwrap());
    let p = c
        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
        .await
        .unwrap();
    assert_eq!(p.plan, StartPlan::Resume);
    assert_eq!(
        p.warnings,
        [format!(
            "publication {} doesn't publish {}.b: its changes won't arrive",
            n.publication, n.schema
        )],
        "the DBA's publication is validated, not altered"
    );
    assert_eq!(published(&a, &n).await, ["a"]);

    // Lost slot under require_existing: refused, not recreated.
    a.execute("SELECT pg_drop_replication_slot($1)", &[&n.slot])
        .await
        .unwrap();
    permanent(
        c.prepare(Some(&Checkpoint::from(start.clone())), &tables)
            .await
            .unwrap_err(),
        "require_existing",
    );
    permanent(c.reset_position().await.unwrap_err(), "require_existing");
    c.teardown().await.unwrap();
    assert_eq!(
        published(&a, &n).await,
        ["a"],
        "teardown leaves what it didn't create"
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn the_publication_follows_the_table_set() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_publication", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let p = c
        .prepare(None, &[n.table("a"), n.table("b")])
        .await
        .unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    assert!(p.added.is_empty(), "a first start snapshots every table");

    // A table joins and one leaves: resume; the joining table is reported for its snapshot.
    let p = c
        .prepare(
            Some(&Checkpoint::from(start.clone())),
            &[n.table("b"), n.table("c")],
        )
        .await
        .unwrap();
    assert_eq!(p.plan, StartPlan::Resume);
    assert_eq!(p.added, [n.table("c")]);
    assert_eq!(published(&a, &n).await, ["b", "c"]);
    let empty: [TableRef; 0] = [];
    c.prepare(Some(&Checkpoint::from(start.clone())), &empty)
        .await
        .unwrap();
    assert!(published(&a, &n).await.is_empty());
    cleanup(&a, &n).await;
}

/// Final review I2: a publication change that waits on a table's lock is cancelled by the server,
/// not left to commit after the client gave up. The retry adds the table again and reports it for
/// its snapshot; without the server's timeout the abandoned change committed once the lock was
/// free, and the retry reported nothing.
#[tokio::test]
async fn a_publication_change_blocked_by_a_lock_is_retried_whole() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_lock", DDL).await;
    let mut p = pipeline(&n, &["*"], "", "");
    p.source.timeouts.command = Duration::from_secs(1);
    let c = connector(&p);
    let first = c.prepare(None, &[n.table("a")]).await.unwrap();
    let start = from(&first.plan, SnapshotReason::FirstStart);
    let holder = admin().await;
    holder
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {}.b IN ACCESS EXCLUSIVE MODE",
            n.schema
        ))
        .await
        .unwrap();
    let err = c
        .prepare(
            Some(&Checkpoint::from(start.clone())),
            &[n.table("a"), n.table("b")],
        )
        .await
        .unwrap_err();
    assert!(err.is_transient(), "{err}");
    holder.batch_execute("COMMIT").await.unwrap();
    // Time for a change the server didn't cancel to commit.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let p = c
        .prepare(
            Some(&Checkpoint::from(start.clone())),
            &[n.table("a"), n.table("b")],
        )
        .await
        .unwrap();
    assert_eq!(p.plan, StartPlan::Resume);
    assert_eq!(p.added, [n.table("b")]);
    cleanup(&a, &n).await;
}

/// pgoutput reads the publication as of each change's WAL position. A publication repaired at
/// start covers only what follows, so resuming from the checkpoint would miss what lies between:
/// the slot is recreated and every table snapshotted instead.
#[tokio::test]
async fn a_repaired_publication_re_snapshots() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_repair", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let tables = [n.table("a")];
    let p = c.prepare(None, &tables).await.unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    let ops = || async {
        a.query_one(
            "SELECT pubinsert AND pubupdate AND pubdelete AND pubtruncate FROM pg_publication WHERE pubname = $1",
            &[&n.publication],
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    };

    // Fewer operations: a delete made now would never be published.
    a.batch_execute(&format!(
        "ALTER PUBLICATION {} SET (publish = 'insert'); DELETE FROM {}.a",
        n.publication, n.schema
    ))
    .await
    .unwrap();
    let p = c
        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
        .await
        .unwrap();
    let again = from(&p.plan, SnapshotReason::PositionLost);
    assert!(ops().await, "every operation published again");
    assert!(
        p.warnings.iter().any(|w| w.contains("re-snapshots")),
        "{:?}",
        p.warnings
    );
    assert!(p.added.is_empty());
    let SourcePosition::Pg(at) = &again else {
        unreachable!()
    };
    assert_eq!(confirmed(&a, &n).await, Some(*at), "the slot was recreated");

    // A publication dropped behind the pipeline's back: created again, the same way.
    a.batch_execute(&format!("DROP PUBLICATION {}", n.publication))
        .await
        .unwrap();
    let p = c
        .prepare(Some(&Checkpoint::from(again.clone())), &tables)
        .await
        .unwrap();
    from(&p.plan, SnapshotReason::PositionLost);
    assert_eq!(published(&a, &n).await, ["a"]);

    // Nothing to repair: resume.
    let SourcePosition::Pg(now) = from(&p.plan, SnapshotReason::PositionLost) else {
        unreachable!()
    };
    let p = c
        .prepare(Some(&Checkpoint::from(SourcePosition::Pg(now))), &tables)
        .await
        .unwrap();
    assert_eq!(p.plan, StartPlan::Resume);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_slot_of_another_plugin_is_refused() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_plugin", DDL).await;
    let at: String = a
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&n.slot],
        )
        .await
        .unwrap()
        .get(0);
    let at = SourcePosition::Pg(at.parse().unwrap());
    let c = connector(&pipeline(&n, &["*"], "", ""));
    permanent(
        c.prepare(Some(&Checkpoint::from(at.clone())), &[n.table("a")])
            .await
            .unwrap_err(),
        "uses the test_decoding plugin, not pgoutput",
    );
    let adopt = connector(&pipeline(
        &n,
        &["*"],
        "",
        ", slot_on_orphan: adopt_resnapshot",
    ));
    permanent(
        adopt.prepare(None, &[n.table("a")]).await.unwrap_err(),
        "test_decoding",
    );
    assert!(confirmed(&a, &n).await.is_some(), "a slot not ours is kept");
    assert!(
        a.query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&n.publication]
        )
        .await
        .unwrap()
        .is_none(),
        "a refusal changes nothing, not even the publication"
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_root_publication_of_tracked_partitions_is_refused() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "slot_via_root",
        "CREATE TABLE a (id int PRIMARY KEY);
         CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
         CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
    )
    .await;
    a.batch_execute(&format!(
        "CREATE PUBLICATION {} FOR TABLE {s}.a, {s}.events WITH (publish_via_partition_root = true)",
        n.publication,
        s = n.schema
    ))
    .await
    .unwrap();
    let start: String = a
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&n.slot],
        )
        .await
        .unwrap()
        .get(0);
    let start = SourcePosition::Pg(start.parse().unwrap());
    let postgres = ", slot_management: require_existing, auto_cleanup: false";
    let c = connector(&pipeline(&n, &["*"], "", postgres));
    let e = c
        .prepare(
            Some(&Checkpoint::from(start.clone())),
            &[n.table("a"), n.table("events_2024")],
        )
        .await
        .unwrap_err();
    permanent(
        e,
        &format!(
            "the partitions {}.events_2024 would arrive under their parent's name",
            n.schema
        ),
    );
    let p = c
        .prepare(Some(&Checkpoint::from(start.clone())), &[n.table("a")])
        .await
        .unwrap();
    assert_eq!(
        p.plan,
        StartPlan::Resume,
        "no partition tracked: the flag is harmless"
    );

    // Under `managed`, a FOR ALL TABLES publication isn't changed either.
    a.batch_execute(&format!(
        "DROP PUBLICATION {p}; CREATE PUBLICATION {p} FOR ALL TABLES WITH (publish_via_partition_root = true)",
        p = n.publication
    ))
    .await
    .unwrap();
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let e = c
        .prepare(
            Some(&Checkpoint::from(start.clone())),
            &[n.table("events_2024")],
        )
        .await
        .unwrap_err();
    permanent(e, "publish_via_partition_root = false");
    cleanup(&a, &n).await;
}

/// The heartbeat's read says whether a stream reads the slot: the stream watchdog's evidence
/// that a silent stream is still there (DA-SRC-16).
#[tokio::test]
async fn the_retained_log_says_whether_a_stream_reads_the_slot() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_streamed", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let streamed = async |want: bool| {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let log = c.retained().await.unwrap().expect("the slot");
            if log.streamed == want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "streamed stayed {}",
                log.streamed
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let (stream, _, _) = first_stream(&c).await;
    streamed(true).await;
    drop(stream);
    streamed(false).await;
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn reset_teardown_and_retained_wal() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_reset", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    assert_eq!(c.retained().await.unwrap(), None, "no slot yet");
    let p = c.prepare(None, &[n.table("a")]).await.unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    a.execute(
        &format!(
            "INSERT INTO {}.a SELECT g FROM generate_series(1, 1000) g",
            n.schema
        ),
        &[],
    )
    .await
    .unwrap();
    let retained = c.retained().await.unwrap().unwrap();
    assert!(
        retained.bytes.is_some_and(|b| b > 0),
        "WAL written since the slot's position"
    );
    assert!(!retained.streamed, "no stream reads the slot");
    assert_eq!(retained.behind, None, "the binlog lag is MySQL's");

    let SourcePosition::Pg(fresh_at) = c.reset_position().await.unwrap() else {
        panic!("a PostgreSQL position")
    };
    let SourcePosition::Pg(old) = start else {
        unreachable!()
    };
    assert!(fresh_at > old, "a new consistent point");

    let kept = connector(&pipeline(&n, &["*"], "", ", auto_cleanup: false"));
    kept.teardown().await.unwrap();
    assert!(
        confirmed(&a, &n).await.is_some(),
        "auto_cleanup: false keeps the slot"
    );
    c.teardown().await.unwrap();
    assert!(
        confirmed(&a, &n).await.is_none(),
        "auto_cleanup drops the slot"
    );
    assert!(
        a.query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&n.publication]
        )
        .await
        .unwrap()
        .is_none(),
        "and the publication"
    );
    cleanup(&a, &n).await;
}
