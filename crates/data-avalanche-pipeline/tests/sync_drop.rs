//! A column dropped between a chunk read's plan and its query (Task 16 review F, concern 4), on a
//! live server. The read plan checks the cached columns in the catalog, then the chunk's SELECT
//! takes its lock. A DROP COLUMN that commits between the two makes the SELECT fail with
//! SQLSTATE 42703, which must be transient, as the plan's own check is: the worker rescans and
//! reads again instead of ending. The window is made wide: the DROP waits for a synchronous
//! standby that never answers, holding its lock while still invisible, so the next read plans
//! under the old columns and its SELECT waits for the lock. Cancelling that wait makes the DROP
//! visible. Its own test binary, which cargo never runs beside the `pg` tests: it sets the
//! server-wide `synchronous_standby_names`, which stalls every synchronous commit on the server.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "pg/common.rs"]
mod common;

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use data_avalanche_pipeline::Phase;
use data_avalanche_testkit::require_it;
use futures_util::FutureExt;
use tokio_postgres::NoTls;

use crate::common::{Lake, admin, cleanup, converge, dsn, fresh, pipeline, start};

#[tokio::test]
async fn a_column_dropped_under_a_chunk_query_retries_the_read() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "sync_drop",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text, extra text);
         INSERT INTO t SELECT i, 'v' || i, 'x' || i FROM generate_series(1, 2000) i;",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let p = pipeline(&n, &["t"], "mode: initial, chunk_size: 1");
    let run = start(&p, &lake, None);
    run.wait("the snapshot has emitted a row", |st| {
        st.snapshots.get(&t).is_some_and(|s| s.rows_done >= 1)
    })
    .await;

    // The guarded section below is caught on an error and on a panic, then the setting is reset,
    // then the outcome is raised: the setting is restored whatever happens. The first reset undoes
    // an earlier run that died before its own. ALTER SYSTEM can't run in a multi-statement batch,
    // hence one statement per call. Clearing the setting also releases a DROP still waiting.
    let standby = |names: Option<&'static str>| {
        let a = &a;
        async move {
            let set = match names {
                None => "ALTER SYSTEM RESET synchronous_standby_names".to_string(),
                Some(n) => format!("ALTER SYSTEM SET synchronous_standby_names = '{n}'"),
            };
            a.batch_execute(&set).await?;
            a.batch_execute("SELECT pg_reload_conf()").await
        }
    };
    standby(None).await.unwrap();
    let window = AssertUnwindSafe(async {
        standby(Some("phantom_standby")).await?;
        let (d, conn) = tokio_postgres::connect(&dsn(), NoTls).await?;
        tokio::spawn(conn);
        let drop = format!("SET synchronous_commit = on; ALTER TABLE {t} DROP COLUMN extra");
        let dropping = tokio::spawn(async move { d.batch_execute(&drop).await });
        // The DROP holds its lock while it waits for the standby, and the next chunk's SELECT,
        // planned under the old columns, waits for the lock.
        let mut waiting = false;
        for _ in 0..200 {
            let (sync, lock): (i64, i64) = {
                let r = a
                    .query_one(
                        "SELECT count(*) FILTER (WHERE wait_event = 'SyncRep'),
                                count(*) FILTER (WHERE wait_event_type = 'Lock'
                                                   AND query LIKE '%AS src%')
                           FROM pg_stat_activity",
                        &[],
                    )
                    .await?;
                (r.get(0), r.get(1))
            };
            if sync > 0 && lock > 0 {
                waiting = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if !waiting {
            return Err(
                "no DROP waiting for the standby with a chunk read waiting for its lock".into(),
            );
        }
        let rows_done = run.handle.status.borrow().snapshots[&t].rows_done;
        assert!(
            rows_done < 2000,
            "the snapshot is past the window: {rows_done}"
        );
        // The DROP becomes visible; the waiting SELECT meets a column that is gone.
        a.batch_execute(
            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
        )
        .await?;
        dropping.await??;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    standby(None).await.unwrap();
    match window {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(r) => r.unwrap(),
    }

    // The worker reads on: the snapshot completes under the new schema, and nothing halts.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if run.task.is_finished() {
            panic!("the worker ended: {:?}", run.end().await);
        }
        let st = run.handle.status.borrow().clone();
        if st.phase == Phase::Streaming && st.snapshots.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the snapshot is done: timed out: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    converge(&a, &lake, &t).await;
    let st = run.handle.status.borrow().clone();
    assert!(st.halted.is_empty(), "{st:?}");
    // The failed reads warn once, then the sink's soft drop (final review M2).
    let failed = format!("table {t}: a snapshot chunk read failed (");
    let dropped = format!("table {t}: column \"extra\" soft-dropped: kept, written NULL");
    assert!(
        matches!(&st.warnings[..], [f, d] if f.starts_with(&failed) && f.contains("extra") && *d == dropped),
        "{st:?}"
    );
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
