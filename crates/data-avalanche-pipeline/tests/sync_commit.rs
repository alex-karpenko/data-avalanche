//! DA-SNAP-9 through the worker, on a live server: a synchronous commit waiting for its standby
//! is streamed and applied while the chunk that holds its row is read without it. The chunk's
//! stale row must not undo the change. Its own test binary, which cargo never runs beside the
//! `pg` tests: it sets the server-wide `synchronous_standby_names` to a standby that never
//! answers, which stalls every synchronous commit on the server.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "pg/common.rs"]
mod common;

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use data_avalanche_testkit::require_it;
use futures_util::FutureExt;
use tokio_postgres::NoTls;

use crate::common::{Lake, admin, cleanup, converge, dsn, fresh, pipeline_with, start};

#[tokio::test]
async fn a_stalled_synchronous_commit_survives_the_chunk_that_missed_it() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "sync_commit",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'old' FROM generate_series(1, 5000) i;",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    // The sink holds the first chunk's commit, and the buffer holds one chunk: the next chunk
    // waits for room, and the snapshot reads no further until the release. It is far from the
    // stalled row when the stream applies the commit after the release.
    lake.sink.hold_commits();
    let p = pipeline_with(
        &n,
        &["t"],
        "mode: initial, chunk_size: 10",
        ", buffer_capacity_rows: 10",
        "",
    );
    let run = start(&p, &lake, None);
    run.wait("the snapshot starts", |st| !st.snapshots.is_empty())
        .await;

    // The guarded section below is caught on an error and on a panic, then the setting is reset,
    // then the outcome is raised: the setting is restored whatever happens. The first reset undoes
    // an earlier run that died before its own. ALTER SYSTEM can't run in a multi-statement batch,
    // hence one statement per call. Clearing the setting also releases a writer still waiting.
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
    let run_stalled = AssertUnwindSafe(async {
        standby(Some("phantom_standby")).await?;
        let (w, conn) = tokio_postgres::connect(&dsn(), NoTls).await?;
        tokio::spawn(conn);
        let update =
            format!("SET synchronous_commit = on; UPDATE {t} SET v = 'new' WHERE id = 5000");
        let writer = tokio::spawn(async move { w.batch_execute(&update).await });
        let mut waiting = false;
        for _ in 0..100 {
            let n: i64 = a
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
                    &[],
                )
                .await?
                .get(0);
            if n > 0 {
                waiting = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !waiting {
            return Err("no writer waiting for the standby".into());
        }
        let rows_done = run.handle.status.borrow().snapshots[&t].rows_done;
        assert!(
            rows_done < 4990,
            "the snapshot read past the stalled row: {rows_done}"
        );
        // The held commit stalls the reads: the snapshot reads nothing more meanwhile.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let later = run.handle.status.borrow().snapshots[&t].rows_done;
        assert_eq!(
            later, rows_done,
            "the snapshot read on while the commit was held"
        );
        lake.sink.release_commits();
        // The snapshot finishes while the commit is still invisible to every read.
        run.wait("the snapshot is done", |st| st.snapshots.is_empty())
            .await;
        let rows = lake.rows(&t);
        a.batch_execute(
            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
        )
        .await?;
        writer.await??;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(rows)
    })
    .catch_unwind()
    .await;
    standby(None).await.unwrap();
    let rows = match run_stalled {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(r) => r.unwrap(),
    };

    assert_eq!(rows.len(), 5000);
    assert_eq!(
        rows[4999],
        (5000, Some("new".into())),
        "the stream's change stays"
    );
    converge(&a, &lake, &t).await;
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
