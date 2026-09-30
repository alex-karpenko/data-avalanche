//! Fixtures for the crate's unit tests: a local table of a small schema.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{Field, Schema, SchemaRef};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    ColumnSchema, ColumnValueType as V, CommitMarker, PositionRange, SourcePosition,
    TableChangeSet, TableRef, TableSchema, TableSpec, delete_schema, unchanged_mask_field,
    upsert_schema,
};

use crate::DeltaLocation;
use crate::table::TableHandle;

/// `public.orders (id bigint key, doc text, ref uuid, at timestamp)`.
pub(crate) fn orders() -> TableSchema {
    TableSchema::new(
        TableRef::new("public", "orders"),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", V::Text, "text"),
            ColumnSchema::new("ref", V::Uuid, "uuid"),
            ColumnSchema::new("at", V::Timestamp { with_tz: false }, "timestamp"),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

pub(crate) fn spec(schema: &TableSchema) -> TableSpec {
    TableSpec::new(schema, vec![]).unwrap()
}

pub(crate) fn local(dir: &tempfile::TempDir) -> DeltaLocation {
    DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    }
}

pub(crate) async fn handle(dir: &tempfile::TempDir, table: &TableRef) -> TableHandle {
    TableHandle::open(local(dir).table_url(table), HashMap::new())
        .await
        .unwrap()
}

/// One `orders` row: `ref` is 16 bytes of `id`, `at` is `id` ms, `doc` as given.
pub(crate) type Row<'a> = (i64, Option<&'a str>);

/// Upsert rows of `schema` (a prefix of `orders`' upsert schema: all of it, or without `doc`),
/// with an unchanged mask on `doc` when `mask` is given.
pub(crate) fn upserts(schema: &SchemaRef, rows: &[Row], mask: Option<&[bool]>) -> RecordBatch {
    let n = rows.len();
    let mut fields: Vec<Arc<Field>> = schema.fields().to_vec();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for f in schema.fields() {
        let s = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
        columns.push(match f.name().as_str() {
            "id" => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            "doc" => Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
            "ref" => Arc::new(
                FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| [r.0 as u8; 16])).unwrap(),
            ),
            "at" => Arc::new(TimestampMicrosecondArray::from_iter_values(
                rows.iter().map(|r| r.0 * 1000),
            )),
            "_ts" => Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|_| 0))
                    .with_timezone("UTC"),
            ),
            "_op" => s("I"),
            "_lsn" => s("0/1"),
            "_seq" => s("00000000/00000001"),
            "_source_db" => s("h/db"),
            "_pipeline" => s("p"),
            other => panic!("no fixture for {other}"),
        });
    }
    if let Some(m) = mask {
        fields.push(Arc::new(unchanged_mask_field("doc")));
        columns.push(Arc::new(BooleanArray::from(m.to_vec())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub(crate) fn deletes(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        delete_schema(&orders(), &[]).unwrap(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap()
}

/// A change set of `orders` ending at LSN `end`.
pub(crate) fn change_set(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
    TableChangeSet {
        table: orders().table().clone(),
        schema: upsert_schema(&orders()),
        identifier: vec!["id".into()],
        truncate: false,
        upserts: up,
        deletes: del,
        position: PositionRange {
            start: SourcePosition::Pg(Lsn(0)),
            end: SourcePosition::Pg(Lsn(end)),
        },
        split: None,
    }
}

pub(crate) fn marker(epoch: u64, cs: &TableChangeSet) -> CommitMarker {
    CommitMarker {
        owner_epoch: epoch,
        committed_position: cs.position.end.clone(),
        pipeline_id: "p".into(),
    }
}
