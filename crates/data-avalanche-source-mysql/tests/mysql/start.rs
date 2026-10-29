//! A start's stream position against the target server: the first start, a resume, a checkpoint
//! in another dialect, from another server or purged, and the heartbeat's watch on the dump.

use std::time::Duration;

use data_avalanche_core::{
    CdcEvent, ChangeStream, Checkpoint, ColumnValue, ServerIdentity, SnapshotReason,
    SourceConnector, SourceError, SourcePosition, StartPlan, StreamItem,
};
use data_avalanche_source_mysql::{Flavour, Mode, MySqlConnector, MySqlStream};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, pipeline_as, target, user};

const RE_SNAPSHOT: &str = ", binlog_purge_action: re_snapshot";

/// Another server of the target's flavour.
fn elsewhere() -> ServerIdentity {
    match target().flavour {
        Flavour::Mysql => ServerIdentity::Mysql {
            server_uuid: "00000000-0000-0000-0000-000000000001".into(),
        },
        Flavour::Mariadb => ServerIdentity::Mariadb {
            server_id: u32::MAX,
        },
    }
}

/// A checkpoint `c` can't read: a GTID set where it reads files, a file where it reads GTIDs.
async fn foreign(c: &MySqlConnector) -> SourcePosition {
    let tagged = match c.mode().await.unwrap() {
        Mode::File => "mysql-gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
        _ => "binlog:binlog.000001:4",
    };
    SourcePosition::parse_tagged(tagged).unwrap()
}

/// `prepare` for `checkpoint` through the trait.
async fn prepare(c: &MySqlConnector, checkpoint: &Checkpoint) -> Result<StartPlan, SourceError> {
    SourceConnector::prepare(c, Some(checkpoint), &[])
        .await
        .map(|p| p.plan)
}

#[tokio::test]
async fn a_first_start_snapshots_and_records_the_server_then_resumes() {
    require_it!();
    let (n, _a) = fresh("start_first", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let identity = c.server_identity().await.unwrap();
    let first = SourceConnector::prepare(&c, None, &[])
        .await
        .expect("prepare");
    let StartPlan::Snapshot { from, reason } = first.plan else {
        panic!("a snapshot, got {:?}", first.plan)
    };
    assert_eq!(reason, SnapshotReason::FirstStart);
    assert_eq!(from.dialect(), c.mode().await.unwrap().dialect());
    assert_eq!(first.server.as_ref(), Some(&identity));
    let again = SourceConnector::prepare(
        &c,
        Some(&Checkpoint {
            position: from.clone(),
            server: first.server,
        }),
        &[],
    )
    .await
    .expect("resume");
    assert_eq!(again.plan, StartPlan::Resume);
    assert!(again.added.is_empty() && again.warnings.is_empty());
    assert_eq!(again.server, Some(identity.clone()));
    // A lost position is replaced by the write position now.
    let reset = c.reset_position().await.unwrap();
    assert!(
        reset.position.covers(&from).unwrap(),
        "{} covers {from}",
        reset.position
    );
    assert_eq!(reset.server, Some(identity));
    cleanup(&n).await;
}

#[tokio::test]
async fn a_checkpoint_in_another_dialect_follows_the_purge_action() {
    require_it!();
    let (n, _a) = fresh("start_dialect", "").await;
    let halts = connector(&pipeline(&n, &["t"], "", ""));
    let cp = Checkpoint {
        position: foreign(&halts).await,
        server: Some(halts.server_identity().await.unwrap()),
    };
    match prepare(&halts, &cp).await {
        Err(SourceError::Permanent(m)) => {
            assert!(
                m.contains("now reads") && m.contains("binlog_purge_action is error"),
                "{m}"
            )
        }
        other => panic!("a halt, got {other:?}"),
    }
    let recovers = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
    re_snapshots(&recovers, &cp).await;
    cleanup(&n).await;
}

/// Under `re_snapshot`, `prepare` plans a snapshot from the server's position now, with a warning
/// naming the lost checkpoint.
async fn re_snapshots(c: &MySqlConnector, cp: &Checkpoint) {
    let p = SourceConnector::prepare(c, Some(cp), &[])
        .await
        .expect("a re-snapshot");
    let StartPlan::Snapshot { from, reason } = p.plan else {
        panic!("a snapshot, got {:?}", p.plan)
    };
    assert_eq!(reason, SnapshotReason::PositionLost);
    assert_eq!(from.dialect(), c.mode().await.unwrap().dialect());
    assert!(
        p.warnings.len() == 1
            && p.warnings[0].contains(&cp.position.to_tagged())
            && p.warnings[0].contains("re_snapshot"),
        "{:?}",
        p.warnings
    );
    assert_eq!(p.server, Some(c.server_identity().await.unwrap()));
}

/// File/position mode halts on a checkpoint from another server; GTIDs carry across servers.
#[tokio::test]
async fn a_checkpoint_from_another_server() {
    require_it!();
    let (n, _a) = fresh("start_server", "").await;
    let c = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
    let position = c.position_now().await.unwrap();
    for server in [Some(elsewhere()), None] {
        let cp = Checkpoint {
            position: position.clone(),
            server,
        };
        match (c.mode().await.unwrap(), prepare(&c, &cp).await) {
            (Mode::File, Err(SourceError::Permanent(m))) => {
                assert!(m.starts_with("BINLOG_SOURCE_CHANGED: "), "{m}")
            }
            (Mode::MysqlGtid | Mode::MariadbGtid, Ok(StartPlan::Resume)) => {}
            other => panic!("{cp:?}: {other:?}"),
        }
    }
    cleanup(&n).await;
}

/// A binlog file the server doesn't hold (GTID purges need a purge on the server:
/// `tests/mysql_global.rs`).
#[tokio::test]
async fn a_purged_file_follows_the_purge_action() {
    require_it!();
    let (n, _a) = fresh("start_purged", "").await;
    let halts = connector(&pipeline(&n, &["t"], "", ""));
    let SourcePosition::BinlogFile(now) = halts.position_now().await.unwrap() else {
        return;
    };
    let cp = Checkpoint {
        position: SourcePosition::parse_tagged(&format!("binlog:{}.000000:4", now.base())).unwrap(),
        server: Some(halts.server_identity().await.unwrap()),
    };
    match prepare(&halts, &cp).await {
        Err(SourceError::Permanent(m)) => assert!(
            m.contains("is no longer on the server") && m.contains("binlog_purge_action is error"),
            "{m}"
        ),
        other => panic!("a halt, got {other:?}"),
    }
    let recovers = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
    re_snapshots(&recovers, &cp).await;
    cleanup(&n).await;
}

/// Polls `retained` until it reads `streamed`.
async fn watch(c: &MySqlConnector, streamed: bool) {
    for _ in 0..50 {
        let r = c.retained().await.expect("retained");
        if r.is_some_and(|r| r.streamed == streamed && r.bytes.is_none()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("retained never read streamed: {streamed}");
}

/// The watch finds the dump by its connection id, as a user without `PROCESS`; a dump the server
/// ended reads as not streamed before the stream hears of it (DA-SRC-16).
#[tokio::test]
async fn the_watch_finds_the_dump_by_its_connection() {
    require_it!();
    let (n, mut a) = fresh("start_watch", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let name = "it_start_watch";
    user(
        &mut a,
        name,
        "SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.*",
        &[],
    )
    .await;
    let c = connector(&pipeline_as(
        &n,
        name,
        "      ssl_mode: disable",
        "",
        &["t"],
        "",
    ));
    watch(&c, false).await;
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.checkpoint_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    watch(&c, true).await;
    let id = c.dump_id().expect("the dump's id");
    a.query_drop(format!("KILL {id}")).await.unwrap();
    watch(&c, false).await;
    // What the server sent before the kill comes first.
    let e = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Err(e) = s.next().await {
                return e;
            }
        }
    })
    .await
    .expect("the stream ends");
    assert!(e.is_transient(), "{e}");
    let _ = Box::new(s).close().await;
    assert_eq!(c.dump_id(), None);
    a.query_drop(format!("DROP USER '{name}'@'%'"))
        .await
        .unwrap();
    cleanup(&n).await;
}

const FILE: &str = ", replication_type: binlog";

/// The heartbeat's lag: behind the server's write position, the age of the last transaction the
/// stream read; none without a stream. A MySQL source keeps no log for the pipeline: no bytes.
/// Other tests write the same server meanwhile, so the stream is never sure to be at its head:
/// the zero at the head is the unit test's.
#[tokio::test]
async fn the_binlog_lag_is_the_age_of_the_last_commit_read() {
    require_it!();
    let (n, mut a) = fresh("start_lag", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let lag = async |c: &MySqlConnector| c.retained().await.expect("retained").expect("a log");
    assert_eq!(lag(&c).await.behind, None, "no stream");
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.checkpoint_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    a.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    to_commit(&mut s).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    a.query_drop("INSERT INTO t VALUES (2)").await.unwrap();
    let r = lag(&c).await;
    assert!(
        r.behind.is_some_and(|b| b >= Duration::from_secs(1)),
        "the commit read 2 s ago is the lag: {r:?}"
    );
    assert_eq!(r.bytes, None);
    to_commit(&mut s).await;
    let r = lag(&c).await;
    assert!(
        r.behind.is_some_and(|b| b < Duration::from_secs(2)),
        "the commit just read is the lag: {r:?}"
    );
    let _ = Box::new(s).close().await;
    assert_eq!(lag(&c).await.behind, None, "closed");
    cleanup(&n).await;
}

/// A stream that read to a heartbeat is 0 behind; once the server writes on and the stream
/// doesn't read it, the lag counts from that heartbeat: the idle flag doesn't hide the stall.
#[tokio::test]
async fn an_idle_stream_that_stops_reading_lags_once_the_server_writes_on() {
    require_it!();
    let (n, mut a) = fresh("start_stall", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.checkpoint_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    a.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    to_commit(&mut s).await;
    // The 1 s heartbeat of an idle dump comes as a keep-alive; so does the first commit of
    // another test's database, which isn't a heartbeat and may leave the stream short of the
    // head. Check at each keep-alive until one finds the stream caught up.
    let lag = async |c: &MySqlConnector| c.retained().await.expect("retained").expect("a log");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            while !matches!(s.next().await.unwrap(), StreamItem::KeepAlive(_)) {}
            if lag(&c).await.behind == Some(Duration::ZERO) {
                return;
            }
        }
    })
    .await
    .expect("a check at a keep-alive reads 0 behind within 10 s at a 1 s heartbeat");
    a.query_drop("INSERT INTO t VALUES (2)").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let r = lag(&c).await;
    assert!(
        r.behind.is_some_and(|b| b > Duration::ZERO),
        "the server wrote on, the stream didn't read it: {r:?}"
    );
    let _ = Box::new(s).close().await;
    cleanup(&n).await;
}

/// The changes up to the stream's next commit, and the commit's position, within 20 s.
async fn to_commit(s: &mut MySqlStream) -> (Vec<CdcEvent>, SourcePosition) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut changes = vec![];
        loop {
            match s.next().await.expect("the stream reads") {
                StreamItem::Change(e) => changes.push(e),
                StreamItem::Commit(b) => return (changes, b.commit_pos),
                _ => {}
            }
        }
    })
    .await
    .expect("a commit within 20 s")
}

/// File/position mode resumes after its checkpoint: a restart's `prepare` resumes, and the
/// stream opened at the checkpoint reads only what came after it.
#[tokio::test]
async fn a_file_position_checkpoint_resumes_after_itself() {
    require_it!();
    let (n, mut a) = fresh("start_file_resume", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", FILE));
    assert_eq!(c.mode().await.unwrap(), Mode::File);
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.checkpoint_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    a.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    let (_, end) = to_commit(&mut s).await;
    Box::new(s).close().await.expect("close");
    a.query_drop("INSERT INTO t VALUES (2)").await.unwrap();
    let checkpoint = Checkpoint {
        position: end,
        server: from.server.clone(),
    };
    assert!(matches!(checkpoint.position, SourcePosition::BinlogFile(_)));
    assert_eq!(prepare(&c, &checkpoint).await.unwrap(), StartPlan::Resume);
    let mut s = c.open_stream(&checkpoint, &tables).await.expect("resumed");
    let (changes, _) = to_commit(&mut s).await;
    let ids: Vec<_> = changes
        .iter()
        .map(|e| e.after.as_ref().and_then(|r| r.get("id").cloned()))
        .collect();
    assert_eq!(
        ids,
        [Some(ColumnValue::Int32(2))],
        "the row after the checkpoint, once"
    );
    Box::new(s).close().await.expect("close");
    cleanup(&n).await;
}

/// In file/position mode the dump checks the server on its own connection: a checkpoint
/// recorded on another server (a failover behind the same address), or with none, halts the
/// pipeline before a dump starts. A GTID position isn't checked: GTIDs carry across servers.
#[tokio::test]
async fn a_file_position_stream_opens_only_on_its_server() {
    require_it!();
    let (n, _a) = fresh("start_file_server", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", FILE));
    let tables = c.resolve_tables().await.unwrap().tables;
    let now = c.position_now().await.unwrap();
    for server in [Some(elsewhere()), None] {
        let cp = Checkpoint {
            position: now.clone(),
            server,
        };
        match c.open_stream(&cp, &tables).await.err() {
            Some(SourceError::Permanent(m)) => {
                assert!(m.starts_with("BINLOG_SOURCE_CHANGED: "), "{m}")
            }
            other => panic!("{cp:?}: {other:?}"),
        }
    }
    assert_eq!(c.dump_id(), None, "no dump started");
    if target().replication == "gtid" {
        let gtid = connector(&pipeline(&n, &["t"], "", ""));
        let cp = Checkpoint {
            position: gtid.position_now().await.unwrap(),
            server: Some(elsewhere()),
        };
        let s = gtid.open_stream(&cp, &tables).await.expect("a GTID stream");
        Box::new(s).close().await.expect("close");
    }
    cleanup(&n).await;
}
