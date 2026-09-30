//! P10 — Hop-3b type round-trip (research `Multi_Sink_Architecture_and_Delta_Lake.md` §6.5,
//! `Type_Mapping.md` "Hop 3b"). Writes one Delta table covering every Hop-3b row through BOTH
//! write paths the sink uses (append for the first load, MERGE for changes), converting the
//! Delta-less logical types exactly as the Delta sink would (uuid → canonical string,
//! time → `HH:MM:SS.ffffff`), then verifies the read-back through delta-rs.
//!
//! The table is left on disk when `P10_OUT=<dir>` is set, for the independent cross-read by
//! DuckDB's delta extension (delta-kernel based): `scripts/p10_duckdb_crossread.py`.
//!
//!   P10_OUT=/tmp/p10 cargo test --release --features probe --test probe_types -- --nocapture

#![cfg(feature = "probe")]

use std::sync::Arc;

use deltalake::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Int16Array, Int64Array, Int8Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema, TimeUnit};
use deltalake::datafusion::prelude::{col, SessionContext};
use deltalake::kernel::{ArrayType, DataType, StructField};
use deltalake::DeltaTable;
use url::Url;

// ---------------------------------------------------------------- Hop-3b conversions (sink side)

/// uuid (Arrow FixedSizeBinary(16)) → canonical lowercase 36-char string.
fn uuid_to_string(b: &[u8]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// time (Arrow Time64(µs) since midnight) → `HH:MM:SS.ffffff` (D-MS.8).
fn time_us_to_string(us: i64) -> String {
    let (s, f) = (us / 1_000_000, us % 1_000_000);
    format!("{:02}:{:02}:{:02}.{:06}", s / 3600, (s / 60) % 60, s % 60, f)
}

// ---------------------------------------------------------------- fixture

const U64_MAX: i128 = u64::MAX as i128; // BIGINT UNSIGNED max → decimal(20,0)

/// The "accumulator" batch as Hop 2 produces it (uuid still FixedSizeBinary, time still Time64).
fn accumulator_batch(ids: &[i64], variant: u8) -> RecordBatch {
    let n = ids.len();
    let uuids: Vec<[u8; 16]> = (0..n)
        .map(|i| {
            let mut u = [0u8; 16];
            for (k, b) in u.iter_mut().enumerate() {
                *b = (k as u8) * 17 + i as u8 + variant;
            }
            u
        })
        .collect();
    let times = [0i64, 86_399_999_999, 45_296_789_012]; // 00:00, 23:59:59.999999, 12:34:56.789012
    let ts = [1_790_000_000_123_456i64, -1, 0]; // 2026-..., 1969-12-31T23:59:59.999999, epoch
    let mut tags = ListBuilder::new(StringBuilder::new());
    for i in 0..n {
        if i == 2 {
            tags.append_null();
        } else {
            tags.values().append_value(format!("t{i}"));
            tags.values().append_value("x");
            tags.append(true);
        }
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("flag", ArrowType::Boolean, true),
        Field::new("tiny", ArrowType::Int8, true),
        Field::new("small", ArrowType::Int16, true),
        Field::new("u64", ArrowType::Decimal128(20, 0), true),
        Field::new("dec", ArrowType::Decimal128(38, 10), true),
        Field::new("s", ArrowType::Utf8, true),
        Field::new("js", ArrowType::Utf8, true),
        Field::new("bin", ArrowType::Binary, true),
        Field::new("d", ArrowType::Date32, true),
        Field::new("uuid", ArrowType::FixedSizeBinary(16), true),
        Field::new("t", ArrowType::Time64(TimeUnit::Microsecond), true),
        Field::new("ts_tz", ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("ts_ntz", ArrowType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("tags", ArrowType::List(Arc::new(Field::new("item", ArrowType::Utf8, true))), true),
    ]));
    let v = variant as i64;
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.to_vec())),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(Int8Array::from(vec![Some(-128i8), Some(127), None])),
        Arc::new(Int16Array::from(vec![Some(-32768i16), Some(32767), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(0i128), Some(U64_MAX - v as i128), None])
                .with_precision_and_scale(20, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![
                Some(-12_345_678_901_234_567_890_123_456_789_012_345_678i128 + v as i128),
                Some(1i128),
                None,
            ])
            .with_precision_and_scale(38, 10)
            .unwrap(),
        ),
        Arc::new(StringArray::from(vec![Some(format!("héllo ✓ {v}")), Some(String::new()), None])),
        Arc::new(StringArray::from(vec![Some(r#"{"a":1,"b":[true,null]}"#), Some("[]"), None])),
        Arc::new(BinaryArray::from(vec![Some(&[0u8, 255, 1][..]), Some(&[][..]), None])),
        Arc::new(Date32Array::from(vec![Some(20_722), Some(-1), None])), // 2026-09-26, 1969-12-31
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                uuids.iter().enumerate().map(|(i, u)| (i != 2).then_some(u.to_vec())),
                16,
            )
            .unwrap(),
        ),
        Arc::new(Time64MicrosecondArray::from(vec![Some(times[0]), Some(times[1]), Some(times[2] + v)])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(ts[0] + v), Some(ts[1]), Some(ts[2])]).with_timezone("UTC")),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(ts[0] + v), Some(ts[1]), Some(ts[2])])),
        Arc::new(tags.finish()),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

/// Hop 3b: accumulator batch → Delta-physical batch (uuid/time converted to strings).
fn to_delta_physical(b: &RecordBatch) -> RecordBatch {
    let mut fields = vec![];
    let mut cols: Vec<ArrayRef> = vec![];
    for (f, c) in b.schema().fields().iter().zip(b.columns()) {
        match f.data_type() {
            ArrowType::FixedSizeBinary(16) => {
                let a = c.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                let v: StringArray = (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| uuid_to_string(a.value(i))))
                    .collect();
                fields.push(Field::new(f.name(), ArrowType::Utf8, true));
                cols.push(Arc::new(v));
            }
            ArrowType::Time64(TimeUnit::Microsecond) => {
                let a = c.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
                let v: StringArray = (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| time_us_to_string(a.value(i))))
                    .collect();
                fields.push(Field::new(f.name(), ArrowType::Utf8, true));
                cols.push(Arc::new(v));
            }
            _ => {
                fields.push(f.as_ref().clone());
                cols.push(c.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), cols).unwrap()
}

fn delta_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DataType::LONG, false),
        StructField::new("flag", DataType::BOOLEAN, true),
        StructField::new("tiny", DataType::BYTE, true),
        StructField::new("small", DataType::SHORT, true),
        StructField::new("u64", DataType::decimal(20, 0).unwrap(), true),
        StructField::new("dec", DataType::decimal(38, 10).unwrap(), true),
        StructField::new("s", DataType::STRING, true),
        StructField::new("js", DataType::STRING, true),
        StructField::new("bin", DataType::BINARY, true),
        StructField::new("d", DataType::DATE, true),
        StructField::new("uuid", DataType::STRING, true),
        StructField::new("t", DataType::STRING, true),
        StructField::new("ts_tz", DataType::TIMESTAMP, true),
        StructField::new("ts_ntz", DataType::TIMESTAMP_NTZ, true),
        StructField::new("tags", DataType::from(ArrayType::new(DataType::STRING, true)), true),
    ]
}

/// Read the table back; each column rendered to Utf8 (so comparison is type-exact but
/// format-agnostic) plus the returned Arrow types.
async fn read_back(table: &DeltaTable) -> (Vec<(String, ArrowType)>, Vec<Vec<Option<String>>>) {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    ctx.register_table("t", table.table_provider().await.unwrap()).unwrap();
    let batches = ctx
        .sql("SELECT * FROM t ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let schema = batches[0].schema();
    let types = schema.fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect();
    let mut cols = vec![vec![]; schema.fields().len()];
    for b in &batches {
        for (ci, c) in b.columns().iter().enumerate() {
            cols[ci].extend(render(c));
        }
    }
    (types, cols)
}

fn render(c: &ArrayRef) -> Vec<Option<String>> {
    // Lists: render element-wise; everything else: arrow cast to Utf8.
    let c = match c.data_type() {
        ArrowType::Utf8View | ArrowType::LargeUtf8 => cast(c, &ArrowType::Utf8).unwrap(),
        _ => c.clone(),
    };
    if let ArrowType::List(_) | ArrowType::LargeList(_) | ArrowType::ListView(_) = c.data_type() {
        let l = cast(&c, &ArrowType::List(Arc::new(Field::new("item", ArrowType::Utf8, true)))).unwrap();
        let l = l.as_any().downcast_ref::<deltalake::arrow::array::ListArray>().unwrap();
        return (0..l.len())
            .map(|i| {
                (!l.is_null(i)).then(|| {
                    let v = l.value(i);
                    let v = v.as_any().downcast_ref::<StringArray>().unwrap();
                    format!("{:?}", (0..v.len()).map(|j| v.value(j).to_string()).collect::<Vec<_>>())
                })
            })
            .collect();
    }
    if let ArrowType::Binary | ArrowType::BinaryView | ArrowType::LargeBinary = c.data_type() {
        // hex, so non-UTF-8 bytes are compared exactly (a Utf8 cast would null them out)
        let b = cast(&c, &ArrowType::Binary).unwrap();
        let b = b.as_any().downcast_ref::<BinaryArray>().unwrap();
        return (0..b.len())
            .map(|i| (!b.is_null(i)).then(|| b.value(i).iter().map(|x| format!("{x:02x}")).collect()))
            .collect();
    }
    let s = cast(&c, &ArrowType::Utf8).unwrap();
    let s = s.as_any().downcast_ref::<StringArray>().unwrap();
    (0..s.len()).map(|i| (!s.is_null(i)).then(|| s.value(i).to_string())).collect()
}

// ---------------------------------------------------------------- probe

#[tokio::test]
async fn p10_type_round_trip_append_and_merge() {
    let (_keep, dir) = match std::env::var("P10_OUT") {
        Ok(p) => {
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            (None, std::path::PathBuf::from(p))
        }
        Err(_) => {
            let t = tempfile::tempdir().unwrap();
            let p = t.path().to_path_buf();
            (Some(t), p)
        }
    };
    let url = Url::from_directory_path(&dir).unwrap();

    // 1. create with the Hop-3b Delta schema
    let t = DeltaTable::try_from_url(url.clone())
        .await
        .unwrap()
        .create()
        .with_columns(delta_columns())
        .await
        .unwrap();
    let proto = t.snapshot().unwrap().protocol().clone();
    println!("P10 protocol: {proto:?}");

    // 2. initial load via append (ids 1..3, variant 0)
    let load = to_delta_physical(&accumulator_batch(&[1, 2, 3], 0));
    let t = t.write(vec![load.clone()]).await.unwrap();

    // 3. change via the sink's MERGE path: rows 1..3 re-imaged with variant 7 (every column updated)
    let upd = to_delta_physical(&accumulator_batch(&[1, 2, 3], 7));
    let ctx = SessionContext::new();
    let df = ctx.read_batch(upd.clone()).unwrap();
    let names: Vec<String> = upd.schema().fields().iter().map(|f| f.name().clone()).collect();
    let (t, m) = t
        .merge(df, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|mut u| {
            for n in names.iter().filter(|n| *n != "id") {
                u = u.update(n.as_str(), col(format!("source.{n}")));
            }
            u
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(m.num_target_rows_updated, 3);

    // 4. read back from a fresh handle
    drop(t);
    let t = deltalake::open_table(url.clone()).await.unwrap();
    let delta_schema: Vec<String> = t
        .snapshot()
        .unwrap()
        .schema()
        .fields()
        .map(|f| format!("{}: {}", f.name(), f.data_type()))
        .collect();
    println!("P10 Delta schema: {delta_schema:?}");
    let (types, got) = read_back(&t).await;
    println!("P10 read-back Arrow types: {types:?}");

    // types: tz-ness must be preserved exactly; uuid/time are strings in Delta
    let ty = |n: &str| types.iter().find(|(k, _)| k == n).unwrap().1.clone();
    assert!(matches!(ty("ts_tz"), ArrowType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC"));
    assert_eq!(ty("ts_ntz"), ArrowType::Timestamp(TimeUnit::Microsecond, None));
    assert_eq!(ty("u64"), ArrowType::Decimal128(20, 0));
    assert_eq!(ty("dec"), ArrowType::Decimal128(38, 10));
    assert!(matches!(ty("uuid"), ArrowType::Utf8 | ArrowType::Utf8View | ArrowType::LargeUtf8));
    assert!(matches!(ty("t"), ArrowType::Utf8 | ArrowType::Utf8View | ArrowType::LargeUtf8));

    // values: every column equals the MERGE source image, row by row
    let schema = upd.schema();
    for (ci, f) in schema.fields().iter().enumerate() {
        let want = render(upd.column(ci));
        let gi = types.iter().position(|(k, _)| k == f.name()).unwrap();
        assert_eq!(got[gi], want, "column {} differs", f.name());
    }
    // spot-check the conversions and extremes
    let at = |n: &str, row: usize| got[types.iter().position(|(k, _)| k == n).unwrap()][row].clone();
    assert_eq!(at("t", 1).as_deref(), Some("23:59:59.999999"));
    assert_eq!(at("u64", 1).as_deref(), Some("18446744073709551608")); // u64::MAX - 7
    assert_eq!(at("uuid", 0).as_deref(), Some("0718293a-4b5c-6d7e-8fa0-b1c2d3e4f506"));
    assert_eq!(at("bin", 0).as_deref(), Some("00ff01"));
    println!("P10 row 1 (id=2): t={:?} u64={:?} ts_tz={:?} ts_ntz={:?} d={:?}",
        at("t", 1), at("u64", 1), at("ts_tz", 1), at("ts_ntz", 1), at("d", 1));
    if std::env::var("P10_OUT").is_ok() {
        println!("P10 table kept at {}", dir.display());
    }
}

/// P10b — the protocol impact is caused by `timestamp_ntz` alone (gap DA-MS-8): the same column
/// set minus `ts_ntz` stays at the minimal protocol.
#[tokio::test]
async fn p10b_protocol_without_timestamp_ntz() {
    let dir = tempfile::tempdir().unwrap();
    let url = Url::from_directory_path(dir.path()).unwrap();
    let cols: Vec<StructField> = delta_columns().into_iter().filter(|f| f.name() != "ts_ntz").collect();
    let t = DeltaTable::try_from_url(url).await.unwrap().create().with_columns(cols).await.unwrap();
    let p = t.snapshot().unwrap().protocol().clone();
    println!("P10b protocol without timestamp_ntz: {p:?}");
    assert!(p.min_reader_version() < 3, "reader version raised without timestamp_ntz: {p:?}");
}
