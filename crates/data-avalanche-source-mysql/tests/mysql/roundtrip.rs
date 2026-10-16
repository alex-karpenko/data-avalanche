//! Every type, both ways (`Type_Mapping.md`): the binlog's row images decode to the values a
//! chunk reads, at each type's extremes, NULL and ordinary values, through an insert and an
//! update.

use std::time::Duration;

use data_avalanche_core::{ChangeStream, ColumnValue, Op, Row, StreamItem};
use data_avalanche_source_mysql::{Flavour, version_of};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

/// A column, its type, and its value as SQL at the low end, the high end, and an ordinary one.
type Column = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

const COLUMNS: &[Column] = &[
    ("b", "TINYINT(1)", "0", "1", "1"),
    ("ti", "TINYINT", "-128", "127", "-5"),
    ("tiu", "TINYINT UNSIGNED", "0", "255", "200"),
    ("si", "SMALLINT", "-32768", "32767", "-300"),
    ("siu", "SMALLINT UNSIGNED", "0", "65535", "40000"),
    ("mi", "MEDIUMINT", "-8388608", "8388607", "-70000"),
    ("miu", "MEDIUMINT UNSIGNED", "0", "16777215", "9000000"),
    ("i", "INT", "-2147483648", "2147483647", "-7"),
    ("iu", "INT UNSIGNED", "0", "4294967295", "3000000000"),
    (
        "bi",
        "BIGINT",
        "-9223372036854775808",
        "9223372036854775807",
        "-1",
    ),
    (
        "biu",
        "BIGINT UNSIGNED",
        "0",
        "18446744073709551615",
        "10000000000000000000",
    ),
    ("f", "FLOAT", "-3.4e38", "3.4e38", "1.5"),
    (
        "d",
        "DOUBLE",
        "-1.7976931348623157e308",
        "1.7976931348623157e308",
        "0.1",
    ),
    ("dec0", "DECIMAL(10,0)", "-9999999999", "9999999999", "42"),
    (
        "dec2",
        "DECIMAL(12,2)",
        "-9999999999.99",
        "9999999999.99",
        "-0.01",
    ),
    (
        "dec38",
        "DECIMAL(38,10)",
        "-9999999999999999999999999999.9999999999",
        "9999999999999999999999999999.9999999999",
        "0.0000000001",
    ),
    (
        "dec65",
        "DECIMAL(65,30)",
        "-99999999999999999999999999999999999.999999999999999999999999999999",
        "99999999999999999999999999999999999.999999999999999999999999999999",
        "1.5",
    ),
    ("c", "CHAR(4)", "''", "'abcd'", "'ab'"),
    ("cl", "CHAR(4) CHARACTER SET latin1", "''", "'Ææ¿ÿ'", "'€'"),
    ("vc", "VARCHAR(40)", "''", "'😀 ünïcødé 中文'", "'x'"),
    (
        "vcl",
        "VARCHAR(40) CHARACTER SET latin1",
        "''",
        "'Ææ¿ÿ€‰'",
        "'é'",
    ),
    ("tt", "TINYTEXT", "''", "REPEAT('ü', 127)", "'t'"),
    ("tx", "TEXT", "''", "REPEAT('ü', 30000)", "'t'"),
    ("mt", "MEDIUMTEXT", "''", "REPEAT('ü', 100000)", "'t'"),
    ("lt", "LONGTEXT", "''", "REPEAT('😀', 100000)", "'t'"),
    ("bn", "BINARY(4)", "''", "x'01020304'", "x'ff'"),
    ("vb", "VARBINARY(8)", "''", "x'00ff00ff00ff00ff'", "x'00'"),
    ("tb", "TINYBLOB", "''", "REPEAT(x'ff', 255)", "x'01'"),
    ("bl", "BLOB", "''", "REPEAT(x'00ff', 30000)", "x'01'"),
    ("mb", "MEDIUMBLOB", "''", "REPEAT(x'00ff', 100000)", "x'01'"),
    ("lb", "LONGBLOB", "''", "REPEAT(x'abcd', 100000)", "x'01'"),
    ("dt", "DATE", "'1000-01-01'", "'9999-12-31'", "'2026-10-14'"),
    ("tm0", "TIME", "'-838:59:59'", "'838:59:59'", "'12:34:56'"),
    (
        "tm6",
        "TIME(6)",
        "'-838:59:58.999999'",
        "'838:59:58.999999'",
        "'-00:00:00.000001'",
    ),
    (
        "dtm0",
        "DATETIME",
        "'1000-01-01 00:00:00'",
        "'9999-12-31 23:59:59'",
        "'2026-10-14 12:00:00'",
    ),
    (
        "dtm3",
        "DATETIME(3)",
        "'1000-01-01 00:00:00.000'",
        "'9999-12-31 23:59:59.999'",
        "'2026-10-14 12:00:00.5'",
    ),
    (
        "dtm6",
        "DATETIME(6)",
        "'1000-01-01 00:00:00.000000'",
        "'9999-12-31 23:59:59.999999'",
        "'2026-10-14 12:00:00.000001'",
    ),
    (
        "ts0",
        "TIMESTAMP NULL",
        "'1970-01-01 00:00:01'",
        "'2038-01-19 03:14:07'",
        "'2026-10-14 12:00:00'",
    ),
    (
        "ts6",
        "TIMESTAMP(6) NULL",
        "'1970-01-01 00:00:01.000000'",
        "'2038-01-19 03:14:07.999999'",
        "'2026-10-14 12:00:00.123456'",
    ),
    ("yr", "YEAR", "1901", "2155", "2026"),
    ("en", "ENUM('a','b','ç')", "'a'", "'ç'", "'b'"),
    ("st", "SET('x','y','z')", "''", "'x,y,z'", "'y'"),
    // Labels keep their case through the binlog's index and bitmap.
    ("enc", "ENUM('Active','b','Zz')", "'Active'", "'Zz'", "'b'"),
    ("stc", "SET('X','y')", "'X'", "'X,y'", "'y'"),
    ("bt1", "BIT(1)", "b'0'", "b'1'", "b'1'"),
    ("bt64", "BIT(64)", "0", "18446744073709551615", "5"),
    (
        "js",
        "JSON",
        "'null'",
        r#"'{"a": [1, 2.5, "x", null, true, false], "b": {"c": "ü😀"}, "big": 18446744073709551615, "neg": -9223372036854775808, "f": 1.5e300, "e": "", "o": {}}'"#,
        "'[]'",
    ),
    (
        "g",
        "GEOMETRY",
        "ST_GeomFromText('POINT(0 0)')",
        "ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 0))')",
        "ST_GeomFromText('LINESTRING(1 1,2 2)')",
    ),
    (
        "pt",
        "POINT",
        "ST_GeomFromText('POINT(-1.5 -2.5)')",
        "ST_GeomFromText('POINT(10 20)', 4326)",
        "ST_GeomFromText('POINT(1 2)')",
    ),
];

/// MariaDB's own types; `VECTOR` from 11.7. Its text form refuses magnitudes past about 1e19 and
/// its binary form past about the same (measured on 11.8): the ends are float32 bytes, ±1e16 with
/// the smallest normal and denormal floats.
const MARIADB: &[Column] = &[
    (
        "u",
        "UUID",
        "'00000000-0000-0000-0000-000000000000'",
        "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
        "'123e4567-e89b-12d3-a456-426614174000'",
    ),
    (
        "i4",
        "INET4",
        "'0.0.0.0'",
        "'255.255.255.255'",
        "'192.168.0.1'",
    ),
    (
        "i6",
        "INET6",
        "'::'",
        "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'",
        "'2001:db8::1'",
    ),
];
const VECTOR: Column = (
    "vec",
    "VECTOR(3)",
    "x'ca1b0eda0000000001000080'",
    "x'ca1b0e5a0000800001000000'",
    "VEC_FromText('[0.5,0.25,0.125]')",
);

/// The values of `columns` at `pick` (1 low, 2 high, 3 ordinary).
fn values(columns: &[Column], pick: usize) -> Vec<&'static str> {
    columns
        .iter()
        .map(|c| match pick {
            1 => c.2,
            2 => c.3,
            _ => c.4,
        })
        .collect()
}

/// The columns where `a` and `b` differ.
fn differ(a: &Row, b: &Row) -> Vec<String> {
    let names: Vec<&String> = a.0.iter().map(|(n, _)| n).collect();
    let mut out = vec![];
    if names != b.0.iter().map(|(n, _)| n).collect::<Vec<_>>() {
        out.push(format!("columns {names:?} vs {:?}", b.0));
    }
    for ((n, x), (_, y)) in a.0.iter().zip(&b.0) {
        if x != y {
            let show = |v: &ColumnValue| {
                let s = format!("{v:?}");
                s.chars().take(200).collect::<String>()
            };
            out.push(format!("{n}: {} vs {}", show(x), show(y)));
        }
    }
    out
}

#[tokio::test]
async fn every_type_reads_the_same_from_the_stream_and_a_chunk() {
    require_it!();
    let mut columns = COLUMNS.to_vec();
    if target().flavour == Flavour::Mariadb {
        columns.extend_from_slice(MARIADB);
    }
    let (n, mut a) = fresh("roundtrip", "").await;
    let version: String = a.query_first("SELECT VERSION()").await.unwrap().unwrap();
    if target().flavour == Flavour::Mariadb && version_of(&version) >= Some((11, 7)) {
        columns.push(VECTOR);
    }
    let ddl: Vec<String> = columns.iter().map(|c| format!("{} {}", c.0, c.1)).collect();
    a.query_drop(format!(
        "CREATE TABLE t (id INT PRIMARY KEY, {}); SET time_zone = '+00:00'",
        ddl.join(", ")
    ))
    .await
    .unwrap();
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let resolved = c.resolve_tables().await.unwrap();
    assert!(resolved.halted.is_empty(), "{:?}", resolved.halted);
    let tables = resolved.tables;
    let from = c.position_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");

    let names: Vec<&str> = columns.iter().map(|c| c.0).collect();
    let row = |id: u32, v: Vec<&str>| format!("({id}, {})", v.join(", "));
    let nulls = vec!["NULL"; columns.len()];
    a.query_drop(format!(
        "INSERT INTO t (id, {}) VALUES {}, {}, {}, {}",
        names.join(", "),
        row(1, values(&columns, 1)),
        row(2, values(&columns, 2)),
        row(3, nulls),
        row(4, values(&columns, 3)),
    ))
    .await
    .unwrap();
    // Row 4, every column changed: to the low ends.
    let set: Vec<String> = columns
        .iter()
        .map(|c| format!("{} = {}", c.0, c.2))
        .collect();
    a.query_drop(format!("UPDATE t SET {} WHERE id = 4", set.join(", ")))
        .await
        .unwrap();

    let (mut inserted, mut updated) = (vec![], None);
    let mut commits = 0;
    while commits < 2 {
        let item = tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
            .expect("the stream");
        match item {
            StreamItem::Change(e) if e.op == Op::Insert => inserted.push(e.after.unwrap()),
            StreamItem::Change(e) if e.op == Op::Update => {
                updated = Some((e.before.unwrap(), e.after.unwrap()))
            }
            StreamItem::Commit(_) => commits += 1,
            StreamItem::TableHalt { reason, .. } => panic!("halted: {reason:?}"),
            _ => {}
        }
    }
    Box::new(s).close().await.expect("close");
    let chunk = c.read_chunk(&tables[0], None, 10).await.expect("chunk");
    assert_eq!(chunk.rows.len(), 4);
    let (before, after) = updated.expect("the update");
    let pairs = [
        ("insert 1", &inserted[0], &chunk.rows[0]),
        ("insert 2", &inserted[1], &chunk.rows[1]),
        ("insert 3", &inserted[2], &chunk.rows[2]),
        ("update before", &before, &inserted[3]),
        ("update after", &after, &chunk.rows[3]),
    ];
    let mut failures = vec![];
    for (what, stream, read) in pairs {
        for d in differ(stream, read) {
            failures.push(format!("{what}: {d}"));
        }
    }
    assert!(
        failures.is_empty(),
        "stream vs chunk:\n{}",
        failures.join("\n")
    );
    // A few values pinned, so both paths can't be wrong alike.
    let high = &chunk.rows[1];
    assert_eq!(high.get("biu"), Some(&ColumnValue::UInt64(u64::MAX)));
    assert_eq!(high.get("b"), Some(&ColumnValue::Bool(true)));
    assert_eq!(high.get("bt64"), Some(&ColumnValue::Bytes(vec![0xff; 8])));
    assert_eq!(chunk.rows[0].get("bi"), Some(&ColumnValue::Int64(i64::MIN)));
    assert_eq!(
        chunk.rows[0].get("enc"),
        Some(&ColumnValue::Text("Active".into()))
    );
    assert!(
        chunk.rows[2]
            .0
            .iter()
            .skip(1)
            .all(|(_, v)| *v == ColumnValue::Null)
    );
    cleanup(&n).await;
}
