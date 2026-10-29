//! The MySQL and MariaDB scenarios (Q-M7b-1): a corrupted compressed transaction and a failover
//! behind the pipeline's address each halt the pipeline with their code; a checkpoint the server
//! purged follows `binlog_purge_action`.

use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};
use crate::proxy::Proxy;
use crate::source::{Db, Source, target};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";
const COLS: &[&str] = &["id", "doc"];

/// Whether the log has the line of a pipeline in `ERROR` with `code` (Q-M7b-4); the log isn't
/// in colour, stderr being no terminal (DA-OBS-6).
fn halted_with(log: &str, code: &str) -> bool {
    log.lines()
        .any(|l| l.contains("pipeline in ERROR") && l.contains(&format!("code=\"{code}\"")))
}

/// A compressed transaction (MySQL's `binlog_transaction_compression`) whose payload the wire
/// corrupted halts the pipeline with `BINLOG_PAYLOAD_INVALID`: status `error`, the code in the
/// log, and nothing of the transaction in the lake. A restart without the fault reads the
/// transaction again, whole.
#[tokio::test]
async fn a_corrupted_compressed_transaction_halts_the_pipeline() {
    require_it!();
    let t = crate::mysql_only!();
    if t.mariadb {
        eprintln!("skipped: MySQL's compressed transactions");
        return;
    }
    let proxy = Proxy::start(t.port).await;
    // The stream finds the payload's events lost at the next event or heartbeat, and nothing is
    // written after the corrupted transaction: 1 s, not the default minute (the wait's length).
    let e = E2e::new(
        "my_payload",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            mysql: ", heartbeat: 1s",
            port: Some(proxy.port()),
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'plain')").await;
    e.mirrored("items", COLS).await;

    proxy.corrupt_next_payload();
    e.exec(
        "SET SESSION binlog_transaction_compression = ON;
         INSERT INTO items VALUES (2, REPEAT('x', 200)), (3, REPEAT('y', 200));
         SET SESSION binlog_transaction_compression = OFF",
    )
    .await;
    p.metric(&e.status("error")).await;
    assert!(proxy.corrupted(), "the proxy corrupted the payload");
    let log = p.log();
    assert!(halted_with(&log, "BINLOG_PAYLOAD_INVALID"), "{log}");
    assert_eq!(
        e.ids("items").await,
        [1],
        "nothing of the transaction lands"
    );
    assert!(p.terminate().await.success());

    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("items", COLS).await;
    assert_eq!(e.ids("items").await, [1, 2, 3]);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Purges every binlog but the newest, after closing the current one: the transactions in them
/// are gone from the server.
async fn purge(db: &Db) {
    db.exec("FLUSH BINARY LOGS").await.expect("flushed");
    let logs = db.text("SHOW BINARY LOGS").await.expect("the binlogs");
    let newest = logs.last().and_then(|r| r[0].clone()).expect("a binlog");
    // MariaDB keeps the file it just closed until its binlog checkpoint moves on.
    eventually("the purge", Duration::from_secs(10), async || {
        db.exec(&format!("PURGE BINARY LOGS TO '{newest}'"))
            .await
            .expect("purged");
        db.text("SHOW BINARY LOGS")
            .await
            .expect("the binlogs")
            .len()
            == 1
    })
    .await;
}

/// A world whose checkpoint the server purged while the pipeline was down: a first run
/// snapshots `items`, then a row is inserted and its binlog purged. Returns the first snapshot.
async fn purged(e: &E2e) -> data_avalanche_state::TableSnapshotDoc {
    let mut p = e.start(&[]);
    p.ready().await;
    let first = e.completed("items", None).await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'B' WHERE id = 2")
        .await;
    purge(&e.db).await;
    first
}

/// Under `binlog_purge_action: re_snapshot` the start re-snapshots from the server's position now,
/// with a warning, and the lake gets the rows the purged binlog held; then it streams on.
#[tokio::test]
async fn a_purged_checkpoint_re_snapshots_at_the_start() {
    require_it!();
    crate::mysql_only!();
    let e = E2e::new(
        "my_purge_snap",
        &format!("{ITEMS} INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
        Options {
            tables: ", snapshots: { mode: initial }",
            mysql: ", binlog_purge_action: re_snapshot",
            ..Options::default()
        },
    )
    .await;
    let first = purged(&e).await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", Some(&first)).await;
    e.mirrored("items", COLS).await;
    let log = p.log();
    assert!(
        log.contains("binlog_purge_action is re_snapshot, so the pipeline re-snapshots"),
        "{log}"
    );
    p.metric(&e.status("running")).await;
    e.exec("INSERT INTO items VALUES (4, 'd')").await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Under `binlog_purge_action: error` (the default) the same start halts the pipeline: status
/// `error`, the log saying that the purge action halts it and the halt carrying no code
/// (D-M7b.16), the lake as the first run left it.
#[tokio::test]
async fn a_purged_checkpoint_halts_under_error() {
    require_it!();
    crate::mysql_only!();
    let e = E2e::new(
        "my_purge_error",
        &format!("{ITEMS} INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    purged(&e).await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("error")).await;
    let log = p.log();
    assert!(
        log.contains("binlog_purge_action is error, so the pipeline halts"),
        "{log}"
    );
    let halt = log.lines().find(|l| l.contains("pipeline in ERROR"));
    assert!(halt.is_some_and(|l| !l.contains("code=")), "{log}");
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A failover behind the pipeline's address in file/position mode: the reconnect reaches another
/// server, whose binlog files the checkpoint means nothing in. The dump's open checks the server
/// on its own connection and halts the pipeline with `BINLOG_SOURCE_CHANGED`. Back on its server,
/// a restart streams on from the checkpoint. MySQL only: MariaDB tells servers apart by
/// `server_id`, which every test-infra server shares.
#[tokio::test]
async fn a_failover_in_file_position_mode_halts_the_pipeline() {
    require_it!();
    let t = crate::mysql_only!();
    if t.name != "mysql-84-nogtid" {
        eprintln!("skipped: file/position mode on MySQL (mysql-84-nogtid)");
        return;
    }
    // The server behind the address after the failover: the test's database, table and user
    // are there too, as on a replica.
    let other = Source::Mysql(target("mysql-84"));
    let replica = Db::connect(other).await;
    let s = "e2e_my_failover";
    replica
        .exec(&format!(
            "DROP DATABASE IF EXISTS {s}; CREATE DATABASE {s}; USE {s}; {ITEMS}
             DROP USER IF EXISTS '{s}'@'%';
             CREATE USER '{s}'@'%' IDENTIFIED BY 'avalanche';
             GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '{s}'@'%';"
        ))
        .await
        .expect("the replica's copy");
    let proxy = Proxy::start(t.port).await;
    let e = E2e::new(
        "my_failover",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            port: Some(proxy.port()),
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.mirrored("items", COLS).await;

    proxy.switch(target("mysql-84").port);
    p.metric(&e.status("error")).await;
    let log = p.log();
    assert!(halted_with(&log, "BINLOG_SOURCE_CHANGED"), "{log}");
    assert!(p.terminate().await.success());

    proxy.switch(t.port);
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    replica
        .exec(&format!(
            "DROP DATABASE IF EXISTS {s}; DROP USER IF EXISTS '{s}'@'%';"
        ))
        .await
        .expect("the replica's copy dropped");
    e.cleanup().await;
}

const TYPES_DDL: &str = "CREATE TABLE types (
        id bigint PRIMARY KEY,
        b tinyint(1), ti tinyint, tu tinyint unsigned, si smallint, su smallint unsigned,
        mi mediumint, iu int unsigned, i int, bi bigint, bu bigint unsigned,
        f float, d double, n decimal(12,3),
        ch char(5), vc varchar(10), tx text, bn binary(4), vb varbinary(8), bl blob,
        dt date, tm time(6), dtt datetime(6), ts timestamp(6) NULL, y year,
        j json, e enum('sad','ok','happy'), st set('a','b','c'), bt bit(10)
    )";

/// The non-key columns' SQL literals, per row.
const TYPES_A: &str = "1, -128, 255, -32768, 65535, -8388608, 4294967295, 2147483647,
    9223372036854775807, 18446744073709551615, 1.5, 0.1, 123456789.123,
    'ab', 'hello', 'it''s', X'0102', X'DEADBEEF', X'00FF',
    '2024-02-29', '-838:59:59.000000', '2024-02-29 12:34:56.789012',
    '2024-02-29 12:34:56.789012', 2155, '{\"a\": 1}', 'happy', 'a,c', b'1000000001'";
const TYPES_B: &str = "0, 127, 0, 32767, 0, 8388607, 0, -2147483648,
    -9223372036854775808, 0, -1.5, -0.001, -0.001,
    'abcde', '', '', X'', X'', X'',
    '1970-01-01', '00:00:00', '1000-01-01 00:00:00', '1970-01-01 00:00:01', 1901, '[]', 'sad',
    '', b'0'";

/// Per column, the Delta column's type as DuckDB reads it, and DuckDB's text of [`TYPES_A`] and
/// [`TYPES_B`] (`Type_Mapping.md`, MySQL and Hop 3b).
const TYPES_EXPECTED: &[[&str; 4]] = &[
    ["b", "BOOLEAN", "true", "false"],
    ["ti", "SMALLINT", "-128", "127"],
    ["tu", "SMALLINT", "255", "0"],
    ["si", "SMALLINT", "-32768", "32767"],
    ["su", "INTEGER", "65535", "0"],
    ["mi", "INTEGER", "-8388608", "8388607"],
    ["iu", "BIGINT", "4294967295", "0"],
    ["i", "INTEGER", "2147483647", "-2147483648"],
    [
        "bi",
        "BIGINT",
        "9223372036854775807",
        "-9223372036854775808",
    ],
    ["bu", "DECIMAL(20,0)", "18446744073709551615", "0"],
    ["f", "FLOAT", "1.5", "-1.5"],
    ["d", "DOUBLE", "0.1", "-0.001"],
    ["n", "DECIMAL(12,3)", "123456789.123", "-0.001"],
    ["ch", "VARCHAR", "ab", "abcde"],
    ["vc", "VARCHAR", "hello", ""],
    ["tx", "VARCHAR", "it's", ""],
    ["bn", "BLOB", "\\x01\\x02\\x00\\x00", "\\x00\\x00\\x00\\x00"],
    ["vb", "BLOB", "\\xDE\\xAD\\xBE\\xEF", ""],
    ["bl", "BLOB", "\\x00\\xFF", ""],
    ["dt", "DATE", "2024-02-29", "1970-01-01"],
    ["tm", "VARCHAR", "-838:59:59.000000", "00:00:00.000000"],
    [
        "dtt",
        "TIMESTAMP",
        "2024-02-29 12:34:56.789012",
        "1000-01-01 00:00:00",
    ],
    [
        "ts",
        "TIMESTAMP WITH TIME ZONE",
        "2024-02-29 12:34:56.789012+00",
        "1970-01-01 00:00:01+00",
    ],
    ["y", "INTEGER", "2155", "1901"],
    ["j", "VARCHAR", "{\"a\":1}", "[]"],
    ["e", "VARCHAR", "happy", "sad"],
    ["st", "VARCHAR[]", "[a, c]", "[]"],
    ["bt", "BLOB", "\\x02\\x01", "\\x00\\x00"],
];

/// Each MySQL type family through a snapshot and through the stream into Delta, read back by
/// DuckDB's `delta_scan` (`Type_Mapping.md`, MySQL and Hop 3b); the session's zone is UTC, so a
/// `TIMESTAMP` literal is its UTC value. MariaDB's `JSON` is `LONGTEXT`: its text stays as
/// written.
#[tokio::test]
async fn every_mysql_type_family_reads_back_in_duckdb() {
    require_it!();
    let t = crate::mysql_only!();
    let e = E2e::new(
        "my_types",
        &format!(
            "SET time_zone = '+00:00'; {TYPES_DDL};
             INSERT INTO types VALUES (1, {TYPES_A}), (2, {TYPES_B});"
        ),
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("types", None).await;
    e.exec(&format!(
        "SET time_zone = '+00:00'; INSERT INTO types VALUES (3, {TYPES_A});
         INSERT INTO types (id) VALUES (4)"
    ))
    .await;
    e.converged("types").await;
    assert!(p.terminate().await.success());

    let (columns, types, rows) = crate::types::duckdb_read(&e.table_url("types"));
    assert_eq!(rows.len(), 4);
    let source: Vec<usize> = (0..columns.len())
        .filter(|&i| columns[i] != "id" && !columns[i].starts_with('_'))
        .collect();
    let named = |row: usize| -> Vec<(&str, Option<&str>)> {
        source
            .iter()
            .map(|&i| (columns[i].as_str(), rows[row][i].as_deref()))
            .collect()
    };
    let want = |i: usize| -> Vec<(&str, Option<&str>)> {
        TYPES_EXPECTED
            .iter()
            .map(|c| match (c[0], i, t.mariadb) {
                ("j", 2, true) => ("j", Some("{\"a\": 1}")),
                _ => (c[0], Some(c[i])),
            })
            .collect()
    };
    let typed: Vec<(&str, &str)> = source
        .iter()
        .map(|&i| (columns[i].as_str(), types[i].as_str()))
        .collect();
    let want_types: Vec<(&str, &str)> = TYPES_EXPECTED.iter().map(|c| (c[0], c[1])).collect();
    assert_eq!(typed, want_types, "the Delta column types");
    assert_eq!(named(0), want(2), "row 1, from the snapshot");
    assert_eq!(named(1), want(3), "row 2, from the snapshot");
    assert_eq!(
        named(2),
        named(0),
        "the stream reads what the snapshot reads"
    );
    assert!(
        named(3).iter().all(|(_, v)| v.is_none()),
        "the all-NULL row"
    );
    e.cleanup().await;
}
