//! A composite key with a `date` part, end to end (DA-SRC-18): the MERGE matches on every key
//! column, so an upsert updates only its own `(region, day)` and a delete removes only its own.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Date32Type;
use arrow_array::{Array, ArrayRef, Date32Array, RecordBatch, StringArray};
use data_avalanche_core::{
    ColumnSchema, ColumnValueType, PositionRange, Sink, TableChangeSet, TableRef, TableSchema,
    TableSpec, delete_schema, upsert_schema,
};
use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};

use crate::common::{commit, lsn, sink};

fn orders() -> TableRef {
    TableRef::new("public", "orders")
}

fn schema() -> TableSchema {
    TableSchema::new(
        orders(),
        vec![
            ColumnSchema::new("region", ColumnValueType::Text, "text").not_null(),
            ColumnSchema::new("day", ColumnValueType::Date, "date").not_null(),
            ColumnSchema::new("doc", ColumnValueType::Text, "text"),
        ],
        vec!["region".into(), "day".into()],
    )
    .unwrap()
}

type Row<'a> = (&'a str, i32, &'a str);

/// Upsert rows of `orders`, lineage filled in.
fn upserts(rows: &[Row]) -> RecordBatch {
    let n = rows.len();
    let text = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.1))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        text("U"),
        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![0; n]).with_timezone("UTC")),
        text("0/1"),
        text("00000000/00000001"),
        text("h/db"),
        text("p"),
    ];
    RecordBatch::try_new(upsert_schema(&schema()), columns).unwrap()
}

fn deletes(keys: &[(&str, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        delete_schema(&schema(), &[]).unwrap(),
        vec![
            Arc::new(StringArray::from_iter_values(keys.iter().map(|k| k.0))),
            Arc::new(Date32Array::from_iter_values(keys.iter().map(|k| k.1))),
        ],
    )
    .unwrap()
}

fn cs(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
    TableChangeSet {
        table: orders(),
        schema: upsert_schema(&schema()),
        identifier: vec!["region".into(), "day".into()],
        truncate: false,
        upserts: up,
        deletes: del,
        position: PositionRange {
            start: lsn(0),
            end: lsn(end),
            last_change: lsn(end),
        },
        split: None,
        snapshot: false,
        reposition: false,
    }
}

/// `(region, day, doc)` of every row, sorted.
async fn rows(s: &data_avalanche_sink_delta::DeltaSink) -> Vec<(String, i32, String)> {
    let b = s.content(&orders()).await.unwrap().unwrap();
    let (r, d, doc) = (
        b.column(0).as_string::<i32>(),
        b.column(1).as_primitive::<Date32Type>(),
        b.column(2).as_string::<i32>(),
    );
    let mut out: Vec<_> = (0..b.num_rows())
        .map(|i| {
            assert!(doc.is_valid(i));
            (r.value(i).to_owned(), d.value(i), doc.value(i).to_owned())
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn a_composite_date_key_matches_on_every_part() {
    let dir = tempfile::tempdir().unwrap();
    let s = sink(
        DeltaLocation::Local {
            root: dir.path().to_path_buf(),
        },
        TableDefaults::default(),
    );
    s.ensure_table(&TableSpec::new(&schema(), vec![]).unwrap())
        .await
        .unwrap();
    // 2026-10-30 is day 20_756 of the epoch.
    let first = upserts(&[
        ("eu", 20_756, "a"),
        ("eu", 20_757, "b"),
        ("us", 20_756, "c"),
    ]);
    commit(&s, cs(10, vec![first], vec![]), 1).await;
    // Each key shares one part with another row: only the full match counts.
    let up = upserts(&[("eu", 20_757, "B"), ("us", 20_757, "d")]);
    commit(&s, cs(20, vec![up], vec![deletes(&[("us", 20_756)])]), 1).await;
    let owned = |r: &[Row]| -> Vec<(String, i32, String)> {
        r.iter()
            .map(|(a, b, c)| ((*a).to_owned(), *b, (*c).to_owned()))
            .collect()
    };
    assert_eq!(
        rows(&s).await,
        owned(&[
            ("eu", 20_756, "a"),
            ("eu", 20_757, "B"),
            ("us", 20_757, "d"),
        ])
    );
    // The key survives the round trip as a Delta `date`, not a string.
    let b = s.content(&orders()).await.unwrap().unwrap();
    assert_eq!(
        b.schema().field(1).data_type(),
        &arrow_schema::DataType::Date32
    );
}
