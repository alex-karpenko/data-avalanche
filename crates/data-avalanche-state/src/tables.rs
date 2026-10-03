//! Per-table documents beside the snapshot state (Data-Flow §State Layout): the schema cache,
//! `schemas/<schema>.<table>.json`, one per tracked table, and the halt registry, `halts.json`.
//!
//! The schema cache holds each table's schema as of the pipeline's checkpoint, with the source's
//! identity of the table (`TableSchema::source_id`): a restart opens the stream with it, and a
//! table dropped and created again while the pipeline was down is told apart by that identity
//! (Q-M5-20). The pipeline writes it after the checkpoint that covers a schema change.
//!
//! A tracked table's halt is recorded before the checkpoint passes it: the halted table's changes
//! after it are dropped, so resuming it from a later checkpoint would skip them. A recorded halt
//! keeps the table halted at every start until a re-snapshot clears it.

use data_avalanche_core::{HaltReason, SourcePosition, TableRef, TableSchema};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{Document, StateError, Versioned};
use crate::pipeline::PipelineDocs;

pub const SCHEMAS_PREFIX: &str = "schemas";
pub const HALTS_KEY: &str = "halts.json";

impl Document for TableSchema {
    const KIND: &'static str = "table schema";
    const SCHEMA_VERSION: u64 = 1;
}

/// `<prefix>/<schema>.<table>.json`. Each name is escaped as a key segment and its dots become
/// `%2E`, so the `.` between them is unambiguous (`a.b` + `c` ≠ `a` + `b.c`).
pub(crate) fn table_key(prefix: &str, table: &TableRef) -> String {
    let part = |s: &str| escape_segment(s).replace('.', "%2E");
    format!(
        "{prefix}/{}.{}.json",
        part(&table.schema),
        part(&table.name)
    )
}

/// `schemas/<schema>.<table>.json`.
pub fn table_schema_key(table: &TableRef) -> String {
    table_key(SCHEMAS_PREFIX, table)
}

/// One halted table: why, and how far its sink content is complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableHalt {
    pub table: TableRef,
    pub reason: HaltReason,
    /// The sink holds the table's changes through this position; later ones were dropped.
    pub at: SourcePosition,
}

/// `halts.json`: the tracked tables that halted, in table order.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HaltsDoc {
    pub tables: Vec<TableHalt>,
}

impl Document for HaltsDoc {
    const KIND: &'static str = "halt registry";
    const SCHEMA_VERSION: u64 = 1;
}

impl PipelineDocs {
    /// # Errors
    /// A storage or decode failure; `Corrupt` when the stored schema names another table.
    pub async fn table_schema(
        &self,
        table: &TableRef,
    ) -> Result<Option<Versioned<TableSchema>>, StateError> {
        let key = table_schema_key(table);
        let found = self.docs.load::<TableSchema>(&key).await?;
        if let Some(v) = &found
            && v.doc.table() != table
        {
            return Err(StateError::Corrupt {
                key,
                kind: TableSchema::KIND,
                reason: format!("it is for table {}, not {table}", v.doc.table()),
            });
        }
        Ok(found)
    }

    /// Every cached schema, in key order.
    ///
    /// # Errors
    /// A storage or decode failure (one bad document fails the listing).
    pub async fn table_schemas(&self) -> Result<Vec<TableSchema>, StateError> {
        let mut out = vec![];
        for key in self.docs.store().list(SCHEMAS_PREFIX).await? {
            if let Some(v) = self.docs.load::<TableSchema>(&key).await? {
                if table_schema_key(v.doc.table()) != key {
                    return Err(StateError::Corrupt {
                        key,
                        kind: TableSchema::KIND,
                        reason: format!("it is for table {}", v.doc.table()),
                    });
                }
                out.push(v.doc);
            }
        }
        Ok(out)
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_table_schema(
        &self,
        schema: &TableSchema,
    ) -> Result<Versioned<TableSchema>, StateError> {
        self.docs
            .modify(
                &table_schema_key(schema.table()),
                |_: Option<TableSchema>| Ok(schema.clone()),
            )
            .await
    }

    /// The halt registry; empty when no table ever halted.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn halts(&self) -> Result<HaltsDoc, StateError> {
        Ok(self
            .docs
            .load::<HaltsDoc>(HALTS_KEY)
            .await?
            .map(|v| v.doc)
            .unwrap_or_default())
    }

    /// Records a table's halt, replacing an earlier one of the same table.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn record_halt(&self, halt: &TableHalt) -> Result<Versioned<HaltsDoc>, StateError> {
        self.docs
            .modify(HALTS_KEY, |cur: Option<HaltsDoc>| {
                let mut doc = cur.unwrap_or_default();
                doc.tables.retain(|h| h.table != halt.table);
                doc.tables.push(halt.clone());
                doc.tables.sort_by(|a, b| a.table.cmp(&b.table));
                Ok(doc)
            })
            .await
    }

    /// Clears a table's halt (the re-snapshot that resumes it).
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn clear_halt(&self, table: &TableRef) -> Result<Versioned<HaltsDoc>, StateError> {
        self.docs
            .modify(HALTS_KEY, |cur: Option<HaltsDoc>| {
                let mut doc = cur.unwrap_or_default();
                doc.tables.retain(|h| &h.table != table);
                Ok(doc)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V};
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::doc::DocStore;

    fn pipeline() -> PipelineDocs {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn schema(table: TableRef) -> TableSchema {
        TableSchema::new(
            table,
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
        .with_source_id(16384)
    }

    fn json<T: Document>(doc: &T) -> String {
        String::from_utf8(crate::doc::encode(doc).to_vec()).unwrap()
    }

    #[test]
    fn schema_keys_escape_like_snapshot_keys() {
        let key = |s: &str, n: &str| table_schema_key(&TableRef::new(s, n));
        assert_eq!(key("public", "orders"), "schemas/public.orders.json");
        assert_eq!(key("a.b", "c"), "schemas/a%2Eb.c.json");
        assert_eq!(key("a", "b.c"), "schemas/a.b%2Ec.json");
    }

    #[test]
    fn schema_json_shape() {
        let s = schema(TableRef::new("public", "orders"));
        assert_eq!(
            json(&s),
            r#"{
  "version": 1,
  "table": {
    "schema": "public",
    "name": "orders"
  },
  "columns": [
    {
      "name": "id",
      "value_type": "int64",
      "logical": "int64",
      "nullable": false,
      "source_type": "bigint",
      "source_ordinal": null
    },
    {
      "name": "note",
      "value_type": "text",
      "logical": "string",
      "nullable": true,
      "source_type": "text",
      "source_ordinal": null
    }
  ],
  "identifier": [
    "id"
  ],
  "source_id": 16384
}"#
        );
    }

    #[tokio::test]
    async fn schemas_round_trip_and_list() {
        let p = pipeline();
        let (orders, items) = (
            TableRef::new("public", "orders"),
            TableRef::new("public", "items"),
        );
        assert!(p.table_schema(&orders).await.unwrap().is_none());
        assert!(p.table_schemas().await.unwrap().is_empty());
        p.save_table_schema(&schema(orders.clone())).await.unwrap();
        p.save_table_schema(&schema(items.clone())).await.unwrap();
        let later = schema(orders.clone()).with_source_id(16999);
        p.save_table_schema(&later).await.unwrap();
        assert_eq!(p.table_schema(&orders).await.unwrap().unwrap().doc, later);
        assert_eq!(
            p.table_schemas().await.unwrap(),
            vec![schema(items), later],
            "in key order"
        );
    }

    #[tokio::test]
    async fn a_schema_under_another_tables_key_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let body = crate::doc::encode(&schema(TableRef::new("public", "invoices")));
        p.docs
            .store()
            .put(&table_schema_key(&orders), body)
            .await
            .unwrap();
        let e = p.table_schema(&orders).await.unwrap_err();
        assert!(
            e.to_string()
                .contains("it is for table public.invoices, not public.orders"),
            "{e}"
        );
        assert!(matches!(
            p.table_schemas().await.unwrap_err(),
            StateError::Corrupt { .. }
        ));
    }

    #[tokio::test]
    async fn a_stored_schema_that_isnt_replicable_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let text =
            json(&schema(orders.clone())).replace("\"nullable\": false", "\"nullable\": true");
        p.docs
            .store()
            .put(&table_schema_key(&orders), Bytes::from(text))
            .await
            .unwrap();
        let e = p.table_schema(&orders).await.unwrap_err();
        assert!(e.to_string().contains("can't be a key"), "{e}");
    }

    fn halt(name: &str, reason: HaltReason) -> TableHalt {
        TableHalt {
            table: TableRef::new("public", name),
            reason,
            at: "pg:0/16B3748".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn halts_are_recorded_replaced_and_cleared() {
        let p = pipeline();
        assert_eq!(p.halts().await.unwrap(), HaltsDoc::default());
        let dropped = halt("orders", HaltReason::TableDropped);
        p.record_halt(&dropped).await.unwrap();
        let overflow = halt(
            "items",
            HaltReason::NumericOverflow {
                column: "price".into(),
            },
        );
        p.record_halt(&overflow).await.unwrap();
        let again = halt(
            "orders",
            HaltReason::UnsafeDdl {
                detail: "narrowed".into(),
            },
        );
        p.record_halt(&again).await.unwrap();
        assert_eq!(
            p.halts().await.unwrap().tables,
            vec![overflow.clone(), again],
            "one per table, in table order"
        );
        p.clear_halt(&TableRef::new("public", "orders"))
            .await
            .unwrap();
        assert_eq!(p.halts().await.unwrap().tables, vec![overflow]);
    }

    #[test]
    fn halts_json_shape() {
        let doc = HaltsDoc {
            tables: vec![halt(
                "orders",
                HaltReason::NumericOverflow {
                    column: "price".into(),
                },
            )],
        };
        assert_eq!(
            json(&doc),
            r#"{
  "version": 1,
  "tables": [
    {
      "table": {
        "schema": "public",
        "name": "orders"
      },
      "reason": {
        "code": "NUMERIC_OVERFLOW",
        "column": "price"
      },
      "at": "pg:0/16B3748"
    }
  ]
}"#
        );
    }
}
