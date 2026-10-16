//! `binlog_row_metadata` other than FULL (config §11.2): preflight refuses the pipeline, and a
//! stream whose table maps lose their column names (the setting turned back while it runs) halts
//! the tracked table. Its own test binary, which cargo never runs beside the `mysql` tests: the
//! setting is global, and would take the names from their table maps too.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "mysql/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, HaltReason, SourceError, StreamItem};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{admin_in, cleanup, connector, fresh, pipeline, target};

/// Sets `binlog_row_metadata` back to FULL when dropped, a panic included: on a thread of its
/// own, since a drop can't await on the test's runtime.
struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        let restored = std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime")
                .block_on(async {
                    admin_in("avalanche")
                        .await
                        .query_drop("SET GLOBAL binlog_row_metadata = FULL")
                        .await
                })
        })
        .join();
        if !matches!(restored, Ok(Ok(()))) && !std::thread::panicking() {
            panic!("restore binlog_row_metadata = FULL: {restored:?}");
        }
    }
}

#[tokio::test]
async fn row_metadata_other_than_full_refuses_and_halts() {
    require_it!();
    let (n, mut a) = fresh("row_metadata", "CREATE TABLE t (id INT PRIMARY KEY, v INT)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    c.preflight().await.expect("preflight under FULL");
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.position_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    if let Err(e) = a
        .query_drop("SET GLOBAL binlog_row_metadata = MINIMAL")
        .await
    {
        eprintln!(
            "skipped on {}: SET GLOBAL binlog_row_metadata: {e}",
            target().name
        );
        return;
    }
    let restore = Restore;
    match c.preflight().await {
        Err(SourceError::Permanent(m)) => assert!(
            m.starts_with(
                "binlog_row_metadata is MINIMAL; the source needs binlog_row_metadata = FULL"
            ),
            "{m}"
        ),
        other => panic!("refused expected, got {other:?}"),
    }
    // The open stream: the next row change's table map names no columns.
    admin_in(&n.db)
        .await
        .query_drop("INSERT INTO t VALUES (1, 1)")
        .await
        .unwrap();
    let halt = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match s.next().await.expect("the stream") {
                StreamItem::TableHalt { table, reason, .. } => return (table, reason),
                StreamItem::Change(e) => panic!("a change decoded without names: {e:?}"),
                _ => {}
            }
        }
    })
    .await
    .expect("a halt within 20 s");
    assert_eq!(halt.0.name, "t");
    assert!(
        matches!(&halt.1, HaltReason::SchemaMismatch { detail, .. }
            if detail.contains("the binlog's table map names no columns (binlog_row_metadata isn't FULL)")),
        "{:?}",
        halt.1
    );
    drop(restore);
    Box::new(s).close().await.expect("close");
    cleanup(&n).await;
}
