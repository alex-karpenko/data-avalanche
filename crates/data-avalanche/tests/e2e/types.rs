//! Scenario 6, types: each PostgreSQL type family through a snapshot and through the stream into
//! Delta, read back by an independent engine, DuckDB's `delta_scan` (`Type_Mapping.md` Hop 3b).

use data_avalanche_testkit::endpoints::{RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL};
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const DDL: &str = "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE types (
        id bigint PRIMARY KEY,
        b bool, i2 int2, i4 int4, i8 int8, o oid, f4 float4, f8 float8,
        n numeric(12,3), n38 numeric(38,0), nb numeric, m money,
        bp char(5), vc varchar(10), tx text, by bytea,
        d date, tm time, tz timetz, ts timestamp, tt timestamptz, iv interval,
        u uuid, j json, jb jsonb, ip inet, r int4range,
        ai int4[], at text[], e mood
    );";

/// The non-key columns' SQL literals, per row.
const ROW_A: &str = "true, -32768, 2147483647, 9223372036854775807, 4294967295, 1.5, 0.1,
    123456789.123, 99999999999999999999999999999999999999, 12345.6789, 12.34,
    'ab', 'hello', 'it''s', '\\xdeadbeef',
    '2024-02-29', '12:34:56.789012', '12:00:00+05:30', '2024-02-29 12:34:56.789012',
    '2024-02-29 12:34:56.789012+00', '1 year 2 mons 3 days 04:05:06',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"a\": 1}', '{\"b\": [1, 2]}', '192.168.0.1/24',
    '[1,10)', '{1,2,NULL}', '{\"x\",\"y z\"}', 'happy'";
const ROW_B: &str = "false, 32767, -2147483648, -9223372036854775808, 0, '-Infinity', 'NaN',
    -0.001, -99999999999999999999999999999999999999, 0, -1.00,
    'abcde', '', '', '\\x',
    '1970-01-01', '00:00:00', '23:59:59-08', '1969-12-31 23:59:59.999999',
    '1970-01-01 00:00:00+00', '-1 days',
    '00000000-0000-0000-0000-000000000000', 'null', '[]', '::1',
    'empty', '{}', '{}', 'sad'";

/// Per column, the Delta column's type as DuckDB reads it, and DuckDB's text of [`ROW_A`] and
/// [`ROW_B`] (`Type_Mapping.md` Hop 3b: `numeric` without a precision, `time`, `timetz`,
/// `interval`, `uuid`, JSON, ranges, `inet` and enums as strings, `char(n)` without its padding,
/// `timestamp` as `timestamp_ntz`, `timestamptz` as `timestamp`, compact JSON).
const EXPECTED: &[[&str; 4]] = &[
    ["b", "BOOLEAN", "true", "false"],
    ["i2", "SMALLINT", "-32768", "32767"],
    ["i4", "INTEGER", "2147483647", "-2147483648"],
    [
        "i8",
        "BIGINT",
        "9223372036854775807",
        "-9223372036854775808",
    ],
    ["o", "BIGINT", "4294967295", "0"],
    ["f4", "FLOAT", "1.5", "-inf"],
    ["f8", "DOUBLE", "0.1", "nan"],
    ["n", "DECIMAL(12,3)", "123456789.123", "-0.001"],
    [
        "n38",
        "DECIMAL(38,0)",
        "99999999999999999999999999999999999999",
        "-99999999999999999999999999999999999999",
    ],
    ["nb", "VARCHAR", "12345.6789", "0"],
    ["m", "DECIMAL(19,2)", "12.34", "-1.00"],
    ["bp", "VARCHAR", "ab", "abcde"],
    ["vc", "VARCHAR", "hello", ""],
    ["tx", "VARCHAR", "it's", ""],
    ["by", "BLOB", "\\xDE\\xAD\\xBE\\xEF", ""],
    ["d", "DATE", "2024-02-29", "1970-01-01"],
    ["tm", "VARCHAR", "12:34:56.789012", "00:00:00.000000"],
    ["tz", "VARCHAR", "12:00:00+05:30", "23:59:59-08"],
    [
        "ts",
        "TIMESTAMP",
        "2024-02-29 12:34:56.789012",
        "1969-12-31 23:59:59.999999",
    ],
    [
        "tt",
        "TIMESTAMP WITH TIME ZONE",
        "2024-02-29 12:34:56.789012+00",
        "1970-01-01 00:00:00+00",
    ],
    ["iv", "VARCHAR", "P1Y2M3DT4H5M6S", "P-1D"],
    [
        "u",
        "VARCHAR",
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "00000000-0000-0000-0000-000000000000",
    ],
    ["j", "VARCHAR", "{\"a\":1}", "null"],
    ["jb", "VARCHAR", "{\"b\":[1,2]}", "[]"],
    ["ip", "VARCHAR", "192.168.0.1/24", "::1"],
    ["r", "VARCHAR", "[1,10)", "empty"],
    ["ai", "INTEGER[]", "[1, 2, NULL]", "[]"],
    ["at", "VARCHAR[]", "[x, y z]", "[]"],
    ["e", "VARCHAR", "happy", "sad"],
];

/// DuckDB's `delta_scan` of a lake table: the column names, their DuckDB types and every cell as
/// text, rows in key order. Needs `uv` (the CI integration job installs it).
fn duckdb_read(url: &str) -> (Vec<String>, Vec<String>, Vec<Vec<Option<String>>>) {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/e2e/duckdb_read.py");
    let out = std::process::Command::new("uv")
        .args([
            "run",
            "--no-project",
            "--with",
            "duckdb==1.5.6",
            "python",
            script,
            url,
        ])
        .env(
            "DUCKDB_S3_ENDPOINT",
            RUSTFS_URL.trim_start_matches("http://"),
        )
        .env("DUCKDB_S3_KEY_ID", RUSTFS_ACCESS_KEY)
        .env("DUCKDB_S3_SECRET", RUSTFS_SECRET_KEY)
        .output()
        .expect("uv is installed (https://docs.astral.sh/uv/)");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).expect("JSON");
    (
        serde_json::from_value(v["columns"].clone()).expect("columns"),
        serde_json::from_value(v["types"].clone()).expect("types"),
        serde_json::from_value(v["rows"].clone()).expect("rows"),
    )
}

#[tokio::test]
async fn every_type_family_reads_back_in_duckdb() {
    require_it!();
    crate::postgres_only!();
    let e = E2e::new(
        "types",
        &format!("{DDL} INSERT INTO types VALUES (1, {ROW_A}), (2, {ROW_B});"),
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
        "INSERT INTO types VALUES (3, {ROW_A}); INSERT INTO types (id) VALUES (4)"
    ))
    .await;
    e.converged("types").await;
    assert!(p.terminate().await.success());

    let (columns, types, rows) = duckdb_read(&e.table_url("types"));
    assert_eq!(rows.len(), 4);
    let source: Vec<usize> = (0..columns.len())
        .filter(|&i| columns[i] != "id" && !columns[i].starts_with('_'))
        .collect();
    let cells = |row: usize| -> Vec<Option<&str>> {
        source.iter().map(|&i| rows[row][i].as_deref()).collect()
    };
    let named = |row: usize| -> Vec<(&str, Option<&str>)> {
        source
            .iter()
            .map(|&i| (columns[i].as_str(), rows[row][i].as_deref()))
            .collect()
    };
    let want = |i: usize| -> Vec<(&str, Option<&str>)> {
        EXPECTED.iter().map(|c| (c[0], Some(c[i]))).collect()
    };
    let typed: Vec<(&str, &str)> = source
        .iter()
        .map(|&i| (columns[i].as_str(), types[i].as_str()))
        .collect();
    let want_types: Vec<(&str, &str)> = EXPECTED.iter().map(|c| (c[0], c[1])).collect();
    assert_eq!(typed, want_types, "the Delta column types");
    assert_eq!(named(0), want(2), "row 1, from the snapshot");
    assert_eq!(named(1), want(3), "row 2, from the snapshot");
    assert_eq!(
        cells(2),
        cells(0),
        "the stream reads what the snapshot reads"
    );
    assert!(cells(3).iter().all(Option::is_none), "the all-NULL row");
    e.cleanup().await;
}
