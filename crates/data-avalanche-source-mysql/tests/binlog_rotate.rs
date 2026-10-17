//! In file/position mode a rotate between transactions moves the position to the start of the
//! next file, so a checkpoint taken while idle doesn't point into a file the server may expire.
//! Its own test binary, which cargo never runs beside the `mysql` tests: with no other writes,
//! only the rotate can have moved the position the heartbeat's keep-alive carries.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "mysql/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, SourcePosition, StreamItem};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

#[tokio::test]
async fn a_rotate_moves_a_file_position_to_the_next_file() {
    require_it!();
    if target().replication != "binlog" {
        eprintln!("skipped: file/position mode only");
        return;
    }
    let (n, mut a) = fresh("rotate", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.checkpoint_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.unwrap();
    a.query_drop("FLUSH BINARY LOGS").await.unwrap();
    let newest = c.binary_logs().await.unwrap().pop().unwrap().name;
    // A 1 s heartbeat; the start of a binlog is right after its 4-byte magic number.
    let p = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match s.next().await.unwrap() {
                StreamItem::KeepAlive(SourcePosition::BinlogFile(f)) if f.file() == newest => {
                    return f;
                }
                StreamItem::Change(e) => panic!("an untracked change: {e:?}"),
                _ => {}
            }
        }
    })
    .await
    .expect("a keep-alive in the new binlog within 5 s");
    assert_eq!(p.pos(), 4, "{p:?}");
    Box::new(s).close().await.unwrap();
    cleanup(&n).await;
}
