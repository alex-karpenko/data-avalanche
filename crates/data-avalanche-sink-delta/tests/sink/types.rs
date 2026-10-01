//! Probe P10 against the crate: every Hop 2 type round-trips through the sink's MERGE (insert, then
//! a full re-image) and reads back as the reference sink holds it, for every `timestamp_ntz_as`.
//! Dates and timestamps outside the Delta range read back clamped (Q-M4-2). With
//! `DATA_AVALANCHE_IT`, DuckDB's `delta_scan` reads the table too (R-12).

use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, TimestampMicrosecondType};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::DataType;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ColumnSchema, ColumnValue as C, ColumnValueType as V, CommitMarker, Decimal, Op, Row,
    Sink, SourcePosition, SourceSeq, TableChangeSet, TableRef, TableSchema, TableSpec,
    TransactionBoundary,
};
use data_avalanche_pipeline::{Accumulator, FlushPolicy, Lineage};
use data_avalanche_sink_delta::{
    DELTA_MAX_DATE, DELTA_MAX_MICROS, DELTA_MIN_DATE, DELTA_MIN_MICROS, DeltaLocation, DeltaSink,
    DeltaSinkConfig, NtzAs, TableDefaults,
};
use data_avalanche_testkit::{MemorySink, require_it};

fn table() -> TableRef {
    TableRef::new("public", "types")
}

fn schema() -> TableSchema {
    let cols = vec![
        ColumnSchema::new("id", V::Int64, "bigint").not_null(),
        ColumnSchema::new("flag", V::Bool, "boolean"),
        ColumnSchema::new("small", V::Int16, "smallint"),
        ColumnSchema::new("int", V::Int32, "integer"),
        ColumnSchema::new("u64", V::UInt64, "bigint unsigned"),
        ColumnSchema::new("f64", V::Float64, "double precision"),
        ColumnSchema::new(
            "dec",
            V::Numeric {
                precision: 38,
                scale: 10,
            },
            "numeric(38,10)",
        ),
        ColumnSchema::new("s", V::Text, "text"),
        ColumnSchema::new("js", V::Json, "jsonb"),
        ColumnSchema::new("bin", V::Bytes, "bytea"),
        ColumnSchema::new("d", V::Date, "date"),
        ColumnSchema::new("t", V::Time, "time"),
        ColumnSchema::new("tz", V::Timestamp { with_tz: true }, "timestamptz"),
        ColumnSchema::new("ntz", V::Timestamp { with_tz: false }, "timestamp"),
        ColumnSchema::new("u", V::Uuid, "uuid"),
        ColumnSchema::new("tags", V::Array(Box::new(V::Text)), "text[]"),
        ColumnSchema::new("ids", V::Array(Box::new(V::Uuid)), "uuid[]"),
    ];
    TableSchema::new(table(), cols, vec!["id".into()]).unwrap()
}

fn uuid(b: u8) -> C {
    C::Uuid(uuid::Uuid::from_bytes([b; 16]))
}

/// Row 1: ordinary values; row 2: NULL everywhere it may be; row 3: the extremes, including
/// PostgreSQL `±infinity` clamped to the Arrow range (Q-M5-1).
fn images(variant: i64) -> Vec<Row> {
    let text = |s: &str| C::Text(format!("{s}{variant}"));
    let row = |cells: Vec<C>| {
        Row(schema()
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .zip(cells)
            .collect())
    };
    let nulls = |id| {
        let mut cells = vec![C::Int64(id)];
        cells.extend((1..schema().columns().len()).map(|_| C::Null));
        row(cells)
    };
    vec![
        row(vec![
            C::Int64(1),
            C::Bool(true),
            C::Int16(-7),
            C::Int32(42 + variant as i32),
            C::UInt64(18_446_744_073_709_551_615 - variant as u64),
            C::Float64(0.5),
            C::Numeric(Decimal::parse("-1234567890123456789012345678.0123456789").unwrap()),
            text("héllo ✓ "),
            C::Json(serde_json::json!({"a": [1, null], "v": variant})),
            C::Bytes(vec![0, 255, variant as u8]),
            C::Date(20_722),
            C::Time(45_296_789_012 + variant),
            C::Timestamp {
                micros: 1_790_000_000_123_456 + variant,
                with_tz: true,
            },
            C::Timestamp {
                micros: -1,
                with_tz: false,
            },
            uuid(0xab),
            C::Array {
                element_type: V::Text,
                values: vec![text("t"), C::Null],
            },
            C::Array {
                element_type: V::Uuid,
                values: vec![uuid(1), uuid(2)],
            },
        ]),
        nulls(2),
        row(vec![
            C::Int64(3),
            C::Bool(false),
            C::Int16(i16::MIN),
            C::Int32(i32::MAX),
            C::UInt64(0),
            C::Float64(f64::MIN_POSITIVE),
            C::Numeric(Decimal::parse("9999999999999999999999999999.9999999999").unwrap()),
            C::Text(String::new()),
            C::Json(serde_json::json!([])),
            C::Bytes(vec![]),
            C::Date(i32::MIN),
            C::Time(-3_020_399_000_000),
            C::Timestamp {
                micros: i64::MAX,
                with_tz: true,
            },
            C::Timestamp {
                micros: i64::MIN,
                with_tz: false,
            },
            uuid(0),
            C::Array {
                element_type: V::Text,
                values: vec![],
            },
            C::Array {
                element_type: V::Uuid,
                values: vec![C::Null],
            },
        ]),
    ]
}

/// The change set inserting (variant 0) or re-imaging (variant 7) every row, from the accumulator.
fn change_set(variant: i64) -> TableChangeSet {
    let lineage = Lineage {
        source_db: "db".into(),
        pipeline: "p".into(),
    };
    let never = FlushPolicy {
        seconds: None,
        rows: None,
        bytes: None,
    };
    let start = SourcePosition::Pg(Lsn(variant as u64 * 100));
    let pos = SourcePosition::Pg(Lsn(variant as u64 * 100 + 50));
    let mut acc = Accumulator::new(lineage, never, start);
    acc.add_table(schema(), &[]).unwrap();
    let now = Instant::now();
    acc.begin(1).unwrap();
    for (i, after) in images(variant).into_iter().enumerate() {
        let op = if variant == 0 { Op::Insert } else { Op::Update };
        let e = CdcEvent {
            op,
            table: table(),
            before: None,
            after: Some(after),
            source_pos: pos.clone(),
            change_seq: SourceSeq::Pg(Lsn(variant as u64 * 100 + i as u64 + 1)),
            xid: 1,
            commit_ts_micros: 0,
        };
        acc.apply(&e, now).unwrap();
    }
    acc.commit(
        &TransactionBoundary {
            xid: 1,
            commit_pos: pos,
            commit_ts_micros: 0,
        },
        now,
    )
    .unwrap();
    acc.flush().change_sets.pop().unwrap()
}

/// The reference content with the date and timestamp cells Delta stores as such clamped to the
/// Delta range.
fn clamped(expected: RecordBatch, ntz: NtzAs) -> RecordBatch {
    let columns: Vec<ArrayRef> = expected
        .schema()
        .fields()
        .iter()
        .zip(expected.columns())
        .map(|(f, c)| -> ArrayRef {
            match (f.name().as_str(), f.data_type()) {
                ("d", _) => std::sync::Arc::new(
                    c.as_primitive::<Date32Type>()
                        .unary::<_, Date32Type>(|d| d.clamp(DELTA_MIN_DATE, DELTA_MAX_DATE)),
                ),
                ("ntz", _) if ntz == NtzAs::String => c.clone(),
                (_, DataType::Timestamp(_, tz)) => {
                    let p: arrow_array::TimestampMicrosecondArray = c
                        .as_primitive::<TimestampMicrosecondType>()
                        .unary(|t| t.clamp(DELTA_MIN_MICROS, DELTA_MAX_MICROS));
                    std::sync::Arc::new(p.with_timezone_opt(tz.clone()))
                }
                _ => c.clone(),
            }
        })
        .collect();
    RecordBatch::try_new(expected.schema(), columns).unwrap()
}

async fn round_trip(location: DeltaLocation, ntz: NtzAs) -> DeltaSink {
    let sink = DeltaSink::new(DeltaSinkConfig {
        pipeline_id: "p".into(),
        location,
        table: TableDefaults {
            timestamp_ntz_as: ntz,
            ..TableDefaults::default()
        },
        merge_memory_limit: None,
    });
    let reference = MemorySink::new();
    let spec = TableSpec::new(&schema(), vec![]).unwrap();
    sink.ensure_table(&spec).await.unwrap();
    reference.ensure_table(&spec).await.unwrap();
    for variant in [0, 7] {
        let cs = change_set(variant);
        let marker = CommitMarker {
            owner_epoch: 1,
            committed_position: cs.position.end.clone(),
            pipeline_id: "p".into(),
        };
        sink.commit(cs.clone(), marker.clone()).await.unwrap();
        reference.commit(cs, marker).await.unwrap();
    }
    let got = sink.content(&table()).await.unwrap().unwrap();
    assert_eq!(
        got,
        clamped(reference.content(&table()).unwrap(), ntz),
        "{ntz:?}"
    );
    sink
}

#[tokio::test]
async fn every_type_round_trips_under_every_timestamp_ntz_option() {
    for ntz in [NtzAs::TimestampNtz, NtzAs::Timestamp, NtzAs::String] {
        let dir = tempfile::tempdir().unwrap();
        round_trip(
            DeltaLocation::Local {
                root: dir.path().to_path_buf(),
            },
            ntz,
        )
        .await;
    }
}

/// DuckDB's `delta_scan` on a table root: every cell as text, rows in key order. Needs `uv`
/// (the CI integration job installs it).
fn duckdb_read(root: &std::path::Path) -> Vec<Vec<Option<String>>> {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/duckdb_read.py");
    let out = std::process::Command::new("uv")
        .args([
            "run",
            "--no-project",
            "--with",
            "duckdb==1.5.6",
            "python",
            script,
        ])
        .arg(root)
        .output()
        .expect("uv is installed (https://docs.astral.sh/uv/)");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_str(stdout.trim()).unwrap()
}

/// R-12: an independent reader (DuckDB, delta-kernel based) reads the table, clamped bounds
/// included, and a table under an escaped root (Q-M4-5). DuckDB can't open a path with non-ASCII
/// text (Q-M4-6), so the escaped name here is ASCII.
#[tokio::test]
async fn duckdb_reads_the_table() {
    require_it!();
    let dir = tempfile::tempdir().unwrap();
    let location = DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    };
    round_trip(location.clone(), NtzAs::TimestampNtz).await;
    let rows = duckdb_read(&dir.path().join("public/types"));
    let names: Vec<String> = schema().columns().iter().map(|c| c.name.clone()).collect();
    let cell = |row: usize, name: &str| {
        let i = names.iter().position(|n| n == name).unwrap();
        rows[row][i].as_deref()
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(
        cell(0, "u64"),
        Some("18446744073709551608"),
        "the exact u64 text read back from decimal(20,0)"
    );
    assert_eq!(cell(0, "u"), Some("abababab-abab-abab-abab-abababababab"));
    assert_eq!(cell(0, "t"), Some("12:34:56.789019"), "time as text");
    assert_eq!(cell(0, "ntz"), Some("1969-12-31 23:59:59.999999"));
    assert_eq!(cell(1, "s"), None, "the all-NULL row");
    assert_eq!(
        cell(2, "d"),
        Some("0001-01-01"),
        "clamped to the Delta minimum"
    );
    assert_eq!(cell(2, "ntz"), Some("0001-01-01 00:00:00"));
    assert_eq!(
        cell(2, "dec"),
        Some("9999999999999999999999999999.9999999999")
    );
    let ascii = TableRef::new("sales 50%", "a/b~c");
    let odd = crate::scenarios::escaped_names(location, ascii).await;
    let root = data_avalanche_storage::table_root("", "", &odd.schema, &odd.name).root;
    let rows = duckdb_read(&dir.path().join(root));
    assert_eq!((rows.len(), rows[0][1].as_deref()), (1, Some("a")));
}
