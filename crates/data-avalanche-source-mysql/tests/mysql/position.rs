//! Positions on the target server (S-MY10): the write position in the pipeline's dialect, the
//! server's identity, the binlogs it holds, and the purge check on a live position.

use data_avalanche_core::{ServerIdentity, SourcePosition};
use data_avalanche_source_mysql::{Flavour, Retention, purged};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

#[tokio::test]
async fn the_write_position_moves_past_a_write_in_the_pipelines_dialect() {
    require_it!();
    let (n, mut a) = fresh("position_now", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let before = c.position_now().await.expect("position");
    let dialect = match (target().replication, target().flavour) {
        ("binlog", _) => "binlog",
        (_, Flavour::Mysql) => "mysql-gtid",
        (_, Flavour::Mariadb) => "mariadb-gtid",
    };
    assert_eq!(before.dialect(), dialect);
    a.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    let after = c.position_now().await.expect("position");
    assert!(after.covers(&before).unwrap(), "{after} covers {before}");
    assert!(
        !before.covers(&after).unwrap(),
        "{before} is before {after}"
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn the_identity_names_the_server() {
    require_it!();
    let (n, mut a) = fresh("position_identity", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let id = c.server_identity().await.expect("identity");
    match target().flavour {
        Flavour::Mysql => {
            let uuid: String = a
                .query_first("SELECT @@server_uuid")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(id, ServerIdentity::Mysql { server_uuid: uuid });
        }
        Flavour::Mariadb => {
            let server_id: u32 = a.query_first("SELECT @@server_id").await.unwrap().unwrap();
            assert_eq!(id, ServerIdentity::Mariadb { server_id });
        }
    }
    cleanup(&n).await;
}

#[tokio::test]
async fn a_live_position_is_reachable_and_a_file_never_written_is_purged() {
    require_it!();
    let (n, _a) = fresh("position_purged", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let now = c.position_now().await.unwrap();
    let retention = c.retention(&now).await.expect("retention");
    assert_eq!(purged(&now, &retention), None, "{retention:?}");
    // The binlogs: the current file is listed with its bytes.
    let logs = c.binary_logs().await.expect("binary logs");
    assert!(
        !logs.is_empty() && logs.iter().all(|l| l.size > 0),
        "{logs:?}"
    );
    if let SourcePosition::BinlogFile(at) = &now {
        assert!(logs.iter().any(|l| l.name == at.file()), "{at} in {logs:?}");
        // Index 0 is never written: the series starts at 000001.
        let gone = SourcePosition::parse_tagged(&format!("binlog:{}.000000:4", at.base())).unwrap();
        let r = c.retention(&gone).await.unwrap();
        assert!(matches!(r, Retention::Files(_)));
        let why = purged(&gone, &r).expect("purged");
        assert!(why.contains("is no longer on the server"), "{why}");
    }
    cleanup(&n).await;
}
