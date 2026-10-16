//! MariaDB's compressed binlog events (`log_bin_compress`, Q-M7a-5) decode as the plain ones do.
//! Its own test binary, which cargo never runs beside the `mysql` tests: the setting is global,
//! and would compress their events too. A MySQL target returns at once.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "mysql/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, ColumnValue, Op, StreamItem};
use data_avalanche_source_mysql::Flavour;
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{admin_in, cleanup, connector, fresh, pipeline, target};

#[tokio::test]
async fn compressed_events_decode() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        return;
    }
    let (n, mut a) = fresh(
        "mariadb_compress",
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.position_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    let long = "x".repeat(300);
    // Compress every event of 10 bytes or more (the least the server takes), from a session opened
    // after the change. Every step until the reset is fallible, so the setting is restored
    // whatever happens; the server's defaults also undo an earlier run that died before its own.
    let wrote = async {
        a.query_drop("SET GLOBAL log_bin_compress = ON, GLOBAL log_bin_compress_min_len = 10")
            .await?;
        let mut w = admin_in(&n.db).await;
        w.query_drop(format!(
            "INSERT INTO t VALUES (1, '{long}'), (2, 'short');
             UPDATE t SET v = 'changed' WHERE id = 1;
             DELETE FROM t WHERE id = 2;
             ALTER TABLE t ADD COLUMN w INT"
        ))
        .await?;
        let file = c.binary_logs().await.unwrap().pop().unwrap().name;
        let kinds: Vec<String> = w
            .query_map(
                format!("SHOW BINLOG EVENTS IN '{file}'"),
                |r: mysql_async::Row| r.get::<String, _>(2).unwrap_or_default(),
            )
            .await?;
        Ok::<_, mysql_async::Error>(kinds)
    }
    .await;
    a.query_drop(
        "SET GLOBAL log_bin_compress = DEFAULT, GLOBAL log_bin_compress_min_len = DEFAULT",
    )
    .await
    .expect("restore log_bin_compress");
    let kinds = wrote.expect("the compressed writes");
    // The server did compress them.
    for kind in [
        "Query_compressed",
        "Write_rows_compressed_v1",
        "Update_rows_compressed_v1",
        "Delete_rows_compressed_v1",
    ] {
        assert!(kinds.iter().any(|k| k == kind), "{kind} in {kinds:?}");
    }
    let mut changes = vec![];
    let mut signalled = false;
    while !signalled {
        let item = tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
            .expect("the stream");
        match item {
            StreamItem::Change(e) => {
                let image = e.after.as_ref().or(e.before.as_ref()).unwrap();
                changes.push((e.op, image.get("id").cloned(), image.get("v").cloned()));
            }
            StreamItem::SchemaSignal { table, .. } => {
                assert_eq!(table.name, "t");
                signalled = true;
            }
            _ => {}
        }
    }
    let text = |v: &str| Some(ColumnValue::Text(v.into()));
    assert_eq!(
        changes,
        [
            (Op::Insert, Some(ColumnValue::Int32(1)), text(&long)),
            (Op::Insert, Some(ColumnValue::Int32(2)), text("short")),
            (Op::Update, Some(ColumnValue::Int32(1)), text("changed")),
            (Op::Delete, Some(ColumnValue::Int32(2)), text("short")),
        ]
    );
    Box::new(s).close().await.expect("close");
    cleanup(&n).await;
}
