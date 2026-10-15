//! A checkpoint the server purged after it was taken follows `binlog_purge_action` (G-26), at
//! `prepare` and, past `prepare`, on the server's own refusal (error 1236). Its own test binary,
//! which cargo never runs beside the `mysql` tests: `PURGE BINARY LOGS` removes the binlogs their
//! streams read.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "mysql/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, Checkpoint, SourceConnector, SourceError, StartPlan};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline};

/// The error `result` ends in: the open's, else the stream's first.
async fn ended(result: Result<Box<dyn ChangeStream>, SourceError>) -> SourceError {
    let mut s = match result {
        Err(e) => return e,
        Ok(s) => s,
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Err(e) = s.next().await {
                return e;
            }
        }
    })
    .await
    .expect("the stream ends")
}

#[tokio::test]
async fn a_purged_checkpoint_follows_the_purge_action() {
    require_it!();
    let (n, mut a) = fresh("binlog_purge", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let halts = connector(&pipeline(&n, &["t"], "", ""));
    let recovers = connector(&pipeline(
        &n,
        &["t"],
        "",
        ", binlog_purge_action: re_snapshot",
    ));
    let tables = halts.resolve_tables().await.unwrap().tables;
    let cp = Checkpoint {
        position: halts.position_now().await.unwrap(),
        server: Some(halts.server_identity().await.unwrap()),
    };
    let resume = SourceConnector::prepare(&halts, Some(&cp), &[])
        .await
        .unwrap();
    assert_eq!(resume.plan, StartPlan::Resume);
    // A transaction after the checkpoint, in a binlog the server then purges.
    a.query_drop("INSERT INTO t VALUES (1); FLUSH BINARY LOGS")
        .await
        .unwrap();
    let newest = halts.binary_logs().await.unwrap().pop().unwrap().name;
    // MariaDB keeps the file it just closed until its binlog checkpoint moves on.
    let mut left = vec![];
    for _ in 0..50 {
        a.query_drop(format!("PURGE BINARY LOGS TO '{newest}'"))
            .await
            .unwrap();
        left = halts.binary_logs().await.unwrap();
        if left.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(left.len(), 1, "the purge left {left:?}");
    for (c, re_snapshot) in [(&halts, false), (&recovers, true)] {
        let e = SourceConnector::prepare(c, Some(&cp), &[])
            .await
            .unwrap_err();
        check(re_snapshot, "prepare", e);
        // The server's refusal, for a stream opened without the check.
        let e = ended(SourceConnector::open_stream(c, &cp.position, &tables).await).await;
        check(re_snapshot, "the dump", e);
    }
    cleanup(&n).await;
}

/// `re_snapshot` keeps the position unreachable; `error` halts the pipeline.
fn check(re_snapshot: bool, at: &str, e: SourceError) {
    match (re_snapshot, &e) {
        (true, SourceError::PositionUnreachable { .. }) => {}
        (false, SourceError::Permanent(m)) if m.contains("binlog_purge_action is error") => {}
        _ => panic!("{at}, re_snapshot {re_snapshot}: {e:?}"),
    }
}
