//! Snapshot chunks against the target server (Snapshot_Resumability §1.1, §2; DA-SNAP-10): every
//! key type walks in chunks without a skip or a repeat, in the server's order; the locking read
//! waits for a writer of its rows; a table gone or changed under the snapshot halts or retries.

use std::time::Duration;

use data_avalanche_core::{ColumnValue, HaltReason, KeysetCursor, Row, SourceError, TableSchema};
use data_avalanche_source_mysql::{Flavour, MySqlConnector};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

/// Every row of `t`, in chunks of `limit`; the cursors along the way.
async fn walk(c: &MySqlConnector, t: &TableSchema, limit: usize) -> (Vec<Row>, Vec<KeysetCursor>) {
    let (mut rows, mut cursors) = (vec![], vec![]);
    let mut after: Option<KeysetCursor> = None;
    loop {
        let chunk = c
            .read_chunk(t, after.as_ref(), limit)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", t.table()));
        assert!(chunk.visibility.is_none());
        assert!(chunk.rows.len() <= limit);
        rows.extend(chunk.rows);
        match chunk.next {
            None => return (rows, cursors),
            Some(next) => {
                cursors.push(next.clone());
                after = Some(next);
            }
        }
    }
}

/// Key types and values chosen where a cursor parameter could go wrong: past `i64`, digits a
/// double loses, case and accents under the column's collation, latin1 above ASCII, trailing zero
/// bytes, the clock ranges' ends, negative times, ENUM (upper-case labels too) and SET in member
/// order, BIT as a number.
const KEYS: &[(&str, &str, &str)] = &[
    (
        "bu",
        "BIGINT UNSIGNED",
        "0, 1, 9223372036854775808, 18446744073709551615",
    ),
    ("i", "INT", "-5, 0, 3, 2147483647"),
    (
        "dec",
        "DECIMAL(30,10)",
        "12345678901234567890.0000000001, 12345678901234567890.0000000002, \
         12345678901234567890.0000000003, -1",
    ),
    ("ci", "VARCHAR(10)", "'a', 'B', 'c', 'ö', 'Z', 'aa'"),
    (
        "l1",
        "VARCHAR(10) CHARACTER SET latin1",
        "'a', 'B', 'é', 'ÿ', '€', 'z'",
    ),
    (
        "bin",
        "VARCHAR(10) COLLATE utf8mb4_bin",
        "'a', 'B', 'b', 'A'",
    ),
    ("vb", "VARBINARY(8)", "x'00', x'0000', x'FF', x'61', x''"),
    ("bn", "BINARY(3)", "x'00', x'0001', x'FF', x'000001'"),
    (
        "dt",
        "DATETIME(6)",
        "'1000-01-01 00:00:00', '2024-02-29 12:00:00.000001', '2024-02-29 12:00:00', \
         '9999-12-31 23:59:59.999999'",
    ),
    (
        "ts",
        "TIMESTAMP(3)",
        "'1970-01-01 00:00:01', '2038-01-19 03:14:07.999', '2000-01-01 00:00:00.5'",
    ),
    ("d", "DATE", "'1000-01-01', '1969-12-31', '9999-12-31'"),
    (
        "tm",
        "TIME(6)",
        "'-838:59:59', '-00:00:00.000001', '00:00:00', '838:59:59', '-01:00:00'",
    ),
    ("y", "YEAR", "1901, 2155, '0000', 1999"),
    ("e", "ENUM('z','a','m')", "'z', 'a', 'm'"),
    ("eu", "ENUM('Zed','a','M')", "'Zed', 'a', 'M'"),
    ("st", "SET('x','y','z')", "'', 'x', 'y,z', 'x,z', 'z'"),
    ("bt", "BIT(10)", "0, 1, 512, 1023, 2"),
    ("f", "FLOAT", "-1.5, 0.1, 3.4e38, 0"),
    ("db", "DOUBLE", "0.1, 0.2, 1e300, -0.30000000000000004"),
];

/// MariaDB's own types.
const MARIADB_KEYS: &[(&str, &str, &str)] = &[
    (
        "uu",
        "UUID",
        "'00000000-0000-1000-8000-000000000002', '00000001-0000-1000-8000-000000000001', \
         'ffffffff-ffff-4fff-bfff-ffffffffffff', '00000000-0000-0000-0000-000000000000'",
    ),
    (
        "i4",
        "INET4",
        "'0.0.0.0', '10.0.0.1', '255.255.255.255', '9.255.0.1'",
    ),
    (
        "i6",
        "INET6",
        "'::', '::1', 'fe80::1', '2001:db8::ff00:42:8329', '::ffff:1.2.3.4'",
    ),
];

#[tokio::test]
async fn every_key_type_walks_in_the_servers_order() {
    require_it!();
    let mut keys = KEYS.to_vec();
    if target().flavour == Flavour::Mariadb {
        keys.extend_from_slice(MARIADB_KEYS);
    }
    let mut ddl = String::new();
    for (name, ty, values) in &keys {
        let rows: Vec<String> = values
            .split(", ")
            .enumerate()
            .map(|(i, v)| format!("({v}, {i})"))
            .collect();
        ddl += &format!(
            "CREATE TABLE k_{name} (k {ty} NOT NULL PRIMARY KEY, n INT);
             INSERT INTO k_{name} VALUES {};",
            rows.join(", ")
        );
    }
    // A composite key, the second column breaking ties.
    ddl += "CREATE TABLE k_pair (a INT, b VARCHAR(5), n INT, PRIMARY KEY (a, b));
            INSERT INTO k_pair VALUES (1, 'b', 0), (1, 'a', 1), (2, 'a', 2), (0, 'z', 3),
                                      (1, 'C', 4);";
    let (n, _a) = fresh("snapshot_keys", &ddl).await;
    // Float and SET keys are identifiers only as text.
    let p = pipeline(
        &n,
        &["'k_*'"],
        "      column_type_overrides: { k_f.k: string, k_db.k: string, k_st.k: string }",
        "",
    );
    let c = connector(&p);
    let r = c.resolve_tables().await.expect("resolve");
    assert!(r.halted.is_empty(), "{:?}", r.halted);
    assert_eq!(r.tables.len(), keys.len() + 1);
    for t in &r.tables {
        let (whole, _) = walk(&c, t, 1000).await;
        let (chunked, cursors) = walk(&c, t, 2).await;
        assert_eq!(
            chunked,
            whole,
            "{}: chunks of 2 read every row once, in the server's order",
            t.table()
        );
        assert_eq!(
            whole.len(),
            match t.table().name.as_str() {
                "k_pair" => 5,
                name => keys
                    .iter()
                    .find(|k| format!("k_{}", k.0) == name)
                    .unwrap()
                    .2
                    .split(", ")
                    .count(),
            },
            "{}",
            t.table()
        );
        let text_key = ["k_ci", "k_l1", "k_bin", "k_pair"].contains(&t.table().name.as_str());
        assert_eq!(
            cursors[0].collation.is_some(),
            text_key,
            "{}: {:?}",
            t.table(),
            cursors[0].collation
        );
    }
    cleanup(&n).await;
}

#[tokio::test]
async fn a_locking_read_waits_for_a_writer_of_its_rows() {
    require_it!();
    let (n, mut a) = fresh(
        "snapshot_lock",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT);
         INSERT INTO t VALUES (1, 1), (2, 1);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let t = c.resolve_tables().await.unwrap().tables.remove(0);
    a.query_drop("START TRANSACTION; UPDATE t SET v = 2 WHERE id = 1")
        .await
        .unwrap();
    let read = tokio::spawn({
        let t = t.clone();
        async move { c.read_chunk(&t, None, 10).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!read.is_finished(), "the read waits for the open writer");
    a.query_drop("COMMIT").await.unwrap();
    let chunk = read.await.unwrap().expect("the chunk");
    assert_eq!(
        chunk.rows[0].get("v"),
        Some(&ColumnValue::Int32(2)),
        "the committed value"
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn a_table_changed_under_the_snapshot_halts_or_waits() {
    require_it!();
    let (n, mut a) = fresh(
        "snapshot_changed",
        "CREATE TABLE gone (id INT PRIMARY KEY);
         CREATE TABLE typed (id INT PRIMARY KEY, v INT);
         CREATE TABLE named (k VARCHAR(5) PRIMARY KEY);
         INSERT INTO named VALUES ('a'), ('b');
         CREATE TABLE keyed (k INT NOT NULL, UNIQUE (k));
         CREATE TABLE flag (k TINYINT(1) PRIMARY KEY);
         INSERT INTO flag VALUES (0), (1), (2);",
    )
    .await;
    let c = connector(&pipeline(
        &n,
        &["gone", "typed", "named", "keyed", "flag"],
        "      primary_key: { keyed: [k] }",
        "",
    ));
    let r = c.resolve_tables().await.unwrap();
    let schema = |name: &str| r.tables.iter().find(|s| s.table().name == name).unwrap();
    let halted = |e: SourceError| match e {
        SourceError::TableHalt { reason, .. } => reason,
        other => panic!("not a table halt: {other}"),
    };
    let first = c.read_chunk(schema("named"), None, 1).await.unwrap();
    a.query_drop(
        "DROP TABLE gone;
         ALTER TABLE typed MODIFY v VARCHAR(5);
         ALTER TABLE named MODIFY k VARCHAR(5) COLLATE utf8mb4_bin;
         ALTER TABLE keyed MODIFY k INT NULL;",
    )
    .await
    .unwrap();
    assert_eq!(
        halted(c.read_chunk(schema("gone"), None, 10).await.unwrap_err()),
        HaltReason::TableDropped
    );
    assert!(matches!(
        c.read_chunk(schema("typed"), None, 10).await.unwrap_err(),
        SourceError::Transient(m) if m.contains("column v is now")
    ));
    assert!(matches!(
        halted(c.read_chunk(schema("named"), first.next.as_ref(), 10).await.unwrap_err()),
        HaltReason::SchemaMismatch { detail, .. } if detail.contains("re-snapshot")
    ));
    assert!(matches!(
        halted(c.read_chunk(schema("keyed"), None, 10).await.unwrap_err()),
        HaltReason::IdentifierNotUsable { reason, .. } if reason.contains("nullable")
    ));
    assert!(matches!(
        halted(c.read_chunk(schema("flag"), None, 10).await.unwrap_err()),
        HaltReason::SchemaMismatch { detail, .. } if detail.contains("TINYINT(1) key value 2")
    ));
    cleanup(&n).await;
}
