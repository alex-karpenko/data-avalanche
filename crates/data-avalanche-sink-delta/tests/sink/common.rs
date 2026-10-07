//! Fixtures: the table `public.items (id bigint key, doc text)` and change sets of it.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    ColumnSchema, ColumnValueType, CommitMarker, CommitOutcome, PositionRange, Sink,
    SourcePosition, TableChangeSet, TableRef, TableSchema, TableSpec, delete_schema,
    unchanged_mask_field, upsert_schema,
};
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig, TableDefaults};

pub fn items() -> TableRef {
    TableRef::new("public", "items")
}

pub fn schema() -> TableSchema {
    TableSchema::new(
        items(),
        vec![
            ColumnSchema::new("id", ColumnValueType::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", ColumnValueType::Text, "text"),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

pub fn spec() -> TableSpec {
    TableSpec::new(&schema(), vec![]).unwrap()
}

pub fn sink(location: DeltaLocation, defaults: TableDefaults) -> DeltaSink {
    DeltaSink::new(DeltaSinkConfig {
        pipeline_id: "p".into(),
        location,
        table: defaults,
        merge_memory_limit: None,
    })
}

pub type Row<'a> = (i64, Option<&'a str>);

/// Upsert rows of `items` (lineage filled in); `masked` marks `doc` unchanged per row.
pub fn upserts(rows: &[Row], masked: Option<&[bool]>) -> RecordBatch {
    let full = upsert_schema(&schema());
    let n = rows.len();
    let text = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
    let mut fields = full.fields().to_vec();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
        text("U"),
        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![0; n]).with_timezone("UTC")),
        text("0/1"),
        text("00000000/00000001"),
        text("h/db"),
        text("p"),
    ];
    if let Some(m) = masked {
        fields.push(Arc::new(unchanged_mask_field("doc")));
        columns.push(Arc::new(BooleanArray::from(m.to_vec())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub fn deletes(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        delete_schema(&schema(), &[]).unwrap(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap()
}

/// A change set of `items` ending at LSN `end`.
pub fn cs(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
    TableChangeSet {
        table: items(),
        schema: upsert_schema(&schema()),
        identifier: vec!["id".into()],
        truncate: false,
        upserts: up,
        deletes: del,
        position: PositionRange {
            start: SourcePosition::Pg(Lsn(0)),
            end: SourcePosition::Pg(Lsn(end)),
        },
        split: None,
        snapshot: false,
    }
}

/// Commits `c` at `epoch` with a marker at its end, as the pipeline does.
pub async fn commit(sink: &dyn Sink, c: TableChangeSet, epoch: u64) -> CommitOutcome {
    let marker = CommitMarker {
        owner_epoch: epoch,
        committed_position: c.position.end.clone(),
        pipeline_id: "p".into(),
    };
    sink.commit(c, marker).await.unwrap()
}

/// `(id, doc)` of every row, in key order.
pub async fn rows(sink: &DeltaSink) -> Vec<(i64, Option<String>)> {
    let b = sink.content(&items()).await.unwrap().unwrap();
    let ids = b.column(0).as_primitive::<Int64Type>();
    let docs = b.column(1).as_string::<i32>();
    (0..b.num_rows())
        .map(|i| {
            (
                ids.value(i),
                docs.is_valid(i).then(|| docs.value(i).to_string()),
            )
        })
        .collect()
}

pub fn owned(rows: &[Row]) -> Vec<(i64, Option<String>)> {
    rows.iter()
        .map(|(i, d)| (*i, d.map(String::from)))
        .collect()
}

pub fn lsn(n: u64) -> SourcePosition {
    SourcePosition::Pg(Lsn(n))
}
