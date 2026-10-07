use std::time::{Duration, Instant};

use data_avalanche_pipeline::Exit;
use data_avalanche_testkit::require_it;
use tokio_postgres::NoTls;

use crate::common::{
    Lake, admin, cleanup, converge, dsn, fresh, pipeline, pipeline_with, source_rows, start,
};

/// Writers change the table while it is being snapshotted: updates, deletes and inserts in many
/// small transactions, over keys chunks have and haven't reached yet. A chunk's read races the
/// stream: a change it didn't see may already be applied when it is emitted (DA-SNAP-9). The
/// writers start once the snapshot runs and write until it is done; one of them must start
/// before the last full chunk is emitted.
///
/// Chunk reads share the catalog connection with the queries the session awaits inline, such as
/// the heartbeat's: a read nobody polled would stall such a query until the command timeout
/// (Task 13 review I3). With the timeout at 60 s, the run must end well under it.
#[tokio::test]
async fn a_snapshot_under_concurrent_writes_ends_equal_to_the_source() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "busy_snapshot",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'old' FROM generate_series(1, 40000) i;",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let p = pipeline_with(
        &n,
        &["t"],
        "mode: initial, chunk_size: 2000",
        "\n      timeouts: { command: 60s }",
        "",
        "",
    );
    // Connected before the snapshot starts, so the writers start together.
    let mut clients = vec![];
    for _ in 0..4 {
        let (c, conn) = tokio_postgres::connect(&dsn(), NoTls).await.unwrap();
        tokio::spawn(conn);
        clients.push(c);
    }
    let begun = Instant::now();
    let run = start(&p, &lake, None);
    run.wait("the snapshot starts", |st| st.snapshots.contains_key(&t))
        .await;
    let mut writers = vec![];
    for (w, c) in (0..4u64).zip(clients) {
        let t = t.clone();
        let status = run.handle.status.clone();
        writers.push(tokio::spawn(async move {
            // A fixed pseudo-random walk over the keys per writer, so a failure replays.
            let mut x: u64 = 0x2545_f491_4f6c_dd1d ^ (w + 1);
            // The snapshot's progress at this writer's first write.
            let mut first = None;
            for i in 0..10_000 {
                let Some(progress) = status.borrow().snapshots.get(&t).copied() else {
                    break;
                };
                first.get_or_insert(progress.rows_done);
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let id = (x % 42_000) as i64 + 1;
                let sql = match i % 3 {
                    0 => format!("UPDATE {t} SET v = 'new {w}.{i}' WHERE id = {id}"),
                    1 => format!("DELETE FROM {t} WHERE id = {id}"),
                    _ => format!(
                        "INSERT INTO {t} VALUES ({id}, 'ins {w}.{i}') \
                         ON CONFLICT (id) DO UPDATE SET v = 'ups {w}.{i}'"
                    ),
                };
                c.batch_execute(&sql).await.unwrap();
            }
            first
        }));
    }
    let mut firsts = vec![];
    for w in writers {
        firsts.push(w.await.unwrap());
    }
    assert!(
        firsts.iter().flatten().any(|&rows_done| rows_done < 38_000),
        "no write began before the last full chunk was emitted: {firsts:?}"
    );
    converge(&a, &lake, &t).await;
    let took = begun.elapsed();
    assert!(
        took < Duration::from_secs(20),
        "took {took:?}: a chunk read stalled behind an inline query"
    );
    assert_eq!(run.drain().await.unwrap(), Exit::Drained);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_dump_initial_table_ends_the_worker_once_dumped() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "dump",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'v' || i FROM generate_series(1, 250) i;",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(
        &pipeline(&n, &["t"], "mode: dump_initial, chunk_size: 100"),
        &lake,
        None,
    );
    assert_eq!(run.end().await.unwrap(), Exit::Dumped);
    assert_eq!(lake.rows(&t), source_rows(&a, &t).await);
    cleanup(&a, &n).await;
}
