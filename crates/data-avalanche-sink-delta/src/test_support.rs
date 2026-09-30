//! Fixtures for the crate's unit tests: a local table of a small schema.

use std::collections::HashMap;

use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableRef, TableSchema, TableSpec};

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
