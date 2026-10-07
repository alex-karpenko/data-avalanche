use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use data_avalanche_pipeline::{HookAction, HookPoint, Hooks, Phase, WorkerError};
use data_avalanche_testkit::require_it;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline, start};

/// Counts the chunks emitted, and crashes at the `crash_at`th sink commit (0: never).
#[derive(Debug, Default)]
struct Probe {
    chunks: AtomicUsize,
    commits: AtomicUsize,
    crash_at: usize,
}

impl Hooks for Probe {
    fn at(&self, point: &HookPoint) -> HookAction {
        match point {
            HookPoint::ChunkEmitted { .. } => {
                self.chunks.fetch_add(1, Ordering::SeqCst);
            }
            HookPoint::SinkCommitted { .. } => {
                let n = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
                if n == self.crash_at {
                    return HookAction::Crash;
                }
            }
            _ => {}
        }
        HookAction::Continue
    }
}

#[tokio::test]
async fn a_restart_resumes_where_the_last_run_stopped() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "restart",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a');",
    )
    .await;
    let t = n.table("t");
    let p = pipeline(&n, &["t"], "mode: initial");
    let lake = Lake::new();
    let run = start(&p, &lake, None);
    run.snapshotted().await;
    a.batch_execute(&format!("INSERT INTO {t} VALUES (2, 'b')"))
        .await
        .unwrap();
    converge(&a, &lake, &t).await;
    run.drain().await.unwrap();

    a.batch_execute(&format!(
        "UPDATE {t} SET v = 'A' WHERE id = 1; INSERT INTO {t} VALUES (3, 'c');"
    ))
    .await
    .unwrap();
    let probe = Arc::new(Probe::default());
    let run = start(&p, &lake, Some(probe.clone()));
    run.wait("streaming", |st| matches!(st.phase, Phase::Streaming))
        .await;
    assert!(
        run.handle.status.borrow().snapshots.is_empty(),
        "no second snapshot"
    );
    converge(&a, &lake, &t).await;
    run.drain().await.unwrap();
    assert_eq!(probe.chunks.load(Ordering::SeqCst), 0, "no second snapshot");
    cleanup(&a, &n).await;
}

/// The third chunk's commit crashes the worker before its cursor is saved; the next run reads on
/// from the second chunk's cursor.
#[tokio::test]
async fn a_crash_mid_snapshot_resumes_from_the_saved_cursor() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "crash_snapshot",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'v' || i FROM generate_series(1, 1000) i;",
    )
    .await;
    let t = n.table("t");
    let p = pipeline(&n, &["t"], "mode: initial, chunk_size: 100");
    let lake = Lake::new();
    let crash = Arc::new(Probe {
        crash_at: 3,
        ..Probe::default()
    });
    let run = start(&p, &lake, Some(crash));
    let end = run.end().await;
    assert!(matches!(end, Err(WorkerError::Crashed(_))), "{end:?}");

    let probe = Arc::new(Probe::default());
    let run = start(&p, &lake, Some(probe.clone()));
    converge(&a, &lake, &t).await;
    let chunks = probe.chunks.load(Ordering::SeqCst);
    assert!(
        chunks <= 9,
        "{chunks} chunks: at least two of ten were already done"
    );
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
