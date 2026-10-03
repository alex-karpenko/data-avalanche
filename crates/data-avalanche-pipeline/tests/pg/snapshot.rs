use data_avalanche_pipeline::Exit;
use data_avalanche_testkit::require_it;
use tokio_postgres::NoTls;

use crate::common::{Lake, admin, cleanup, converge, dsn, fresh, pipeline, source_rows, start};

/// Writers change the table while it is being snapshotted: updates, deletes and inserts in many
/// small transactions, over keys chunks have and haven't reached yet. A chunk's read races the
/// stream: a change it didn't see may already be applied when it is emitted (DA-SNAP-9).
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
    let run = start(
        &pipeline(&n, &["t"], "mode: initial, chunk_size: 2000"),
        &lake,
        None,
    );
    let mut writers = vec![];
    for w in 0..4u64 {
        let (c, conn) = tokio_postgres::connect(&dsn(), NoTls).await.unwrap();
        tokio::spawn(conn);
        let t = t.clone();
        writers.push(tokio::spawn(async move {
            // A fixed pseudo-random walk over the keys per writer, so a failure replays.
            let mut x: u64 = 0x2545_f491_4f6c_dd1d ^ (w + 1);
            for i in 0..400 {
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
        }));
    }
    for w in writers {
        w.await.unwrap();
    }
    converge(&a, &lake, &t).await;
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
