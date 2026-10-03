//! DA-SNAP-9 on a live server: a synchronous commit waiting for its standby is already in the
//! stream while a chunk's snapshot doesn't see it. Its own test binary, which cargo never runs
//! beside the `pg` tests: it sets the server-wide `synchronous_standby_names` to a standby that
//! never answers, which stalls every synchronous commit on the server.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "pg/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, ChunkVisibility, ColumnValue as V, StreamItem};
use data_avalanche_testkit::require_it;
use tokio_postgres::NoTls;

use crate::common::{admin, cleanup, connector, first_stream, fresh, pipeline, port};

type Fallible<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn a_stalled_synchronous_commit_is_streamed_before_a_chunk_sees_it() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "sync_commit",
        "CREATE TABLE t (id int PRIMARY KEY, v int NOT NULL);
         INSERT INTO t VALUES (1, 0);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let (mut stream, tables, _) = first_stream(&c).await;
    let t = tables[0].clone();
    let oid = t.source_id().unwrap();

    // Every step is fallible, not panicking, so the setting is restored whatever happens; the
    // first reset undoes an earlier run that died before its own. ALTER SYSTEM can't run in a
    // multi-statement batch, hence one statement per call. Clearing the setting also releases a
    // writer still waiting.
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
    let run: Fallible<_> = async {
        standby(Some("phantom_standby")).await?;
        let dsn = format!(
            "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
            port()
        );
        let (w, conn) = tokio_postgres::connect(&dsn, NoTls).await?;
        tokio::spawn(conn);
        let update = format!(
            "SET synchronous_commit = on; UPDATE {}.t SET v = 1 WHERE id = 1",
            n.schema
        );
        let writer = tokio::spawn(async move { w.batch_execute(&update).await });
        // The writer's commit is in the WAL and waits for the standby.
        let mut xid = None;
        for _ in 0..100 {
            let row = a
                .query_opt(
                    "SELECT backend_xid::text FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
                    &[],
                )
                .await?;
            if let Some(r) = row {
                xid = r.get::<_, Option<String>>(0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let xid: u64 = xid.ok_or("no writer waiting for the standby")?.parse()?;
        // The stream delivers its commit already.
        let mut committed = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while committed.is_none() {
            match tokio::time::timeout_at(deadline, stream.next()).await?? {
                StreamItem::Commit(b) if b.xid == xid => committed = Some(b),
                _ => {}
            }
        }
        let chunk = c.read_chunk(&t, None, 10).await?;
        let in_flight = c.in_flight().await?.ok_or("in_flight is None")?;
        a.batch_execute(
            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
        )
        .await?;
        writer.await??;
        let after = c.read_chunk(&t, None, 10).await?;
        Ok((xid, chunk, in_flight, after))
    }
    .await;
    standby(None).await.unwrap();
    let (xid, chunk, in_flight, after) = run.unwrap();

    let Some(ChunkVisibility::Xids(s)) = &chunk.visibility else {
        panic!("{chunk:?}")
    };
    assert!(!s.sees(xid), "{s:?} doesn't see the stalled {xid}");
    assert_eq!(chunk.rows[0].get("v"), Some(&V::Int32(0)), "the old row");
    let stalled = in_flight.iter().find(|x| x.xid == xid);
    assert!(
        stalled.is_some_and(|x| x.tables.contains(&oid)),
        "{in_flight:?} lists {xid} writing {oid}"
    );
    let Some(ChunkVisibility::Xids(s)) = &after.visibility else {
        panic!("{after:?}")
    };
    assert!(s.sees(xid));
    assert_eq!(after.rows[0].get("v"), Some(&V::Int32(1)));
    stream.close().await.unwrap();
    cleanup(&a, &n).await;
}
