//! Hop 2 of the type mapping (`Type_Mapping.md`): the canonical Arrow schema of a table, with the
//! `data-avalanche.logical` field tag where the Arrow type alone doesn't say what the column is.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::{ColumnSchema, ColumnValueType, HaltReason, LINEAGE_COLUMNS, LogicalType, TableSchema};

/// Field-metadata key of the logical-type tag (D-IMPL-7).
pub const LOGICAL_TAG: &str = "data-avalanche.logical";

/// Upsert batches carry one boolean column named `<prefix><column>` for each column that is still
/// [`crate::ColumnValue::Unchanged`] in some row at flush (D-IMPL-3); `true` means "keep the
/// target's value". The prefix is reserved (config §5), so it can't collide with a source column.
pub const UNCHANGED_MASK_PREFIX: &str = "__data_avalanche_unchanged:";

/// Name of the element field inside an Arrow `List` (Delta's and Parquet's convention).
pub const LIST_ELEMENT: &str = "element";

/// The Arrow type a logical type is accumulated as (Multi_Sink §6.5).
pub fn arrow_type(logical: &LogicalType) -> DataType {
    use LogicalType as L;
    match logical {
        L::Bool => DataType::Boolean,
        L::Int16 => DataType::Int16,
        L::Int32 => DataType::Int32,
        L::Int64 => DataType::Int64,
        L::Float32 => DataType::Float32,
        L::Float64 => DataType::Float64,
        L::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        L::String | L::Json => DataType::Utf8,
        L::Binary => DataType::Binary,
        L::Date => DataType::Date32,
        L::Time => DataType::Time64(TimeUnit::Microsecond),
        L::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        L::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
        L::Uuid => DataType::FixedSizeBinary(16),
        L::List(e) => DataType::List(Arc::new(Field::new(LIST_ELEMENT, arrow_type(e), true))),
    }
}

/// The `data-avalanche.logical` tag Hop 2 attaches (`Type_Mapping.md` §Logical type tags), or
/// `None` when the Arrow type says it all.
pub fn logical_tag(column: &ColumnSchema) -> Option<&'static str> {
    match (&column.value_type, &column.logical) {
        (ColumnValueType::UInt64, LogicalType::Decimal { .. }) => Some("uint64"),
        (_, LogicalType::Uuid) => Some("uuid"),
        (_, LogicalType::Time) => Some("time"),
        (_, LogicalType::Json) => Some("json"),
        (_, LogicalType::TimestampTz) => Some("timestamp_tz"),
        (_, LogicalType::TimestampNtz) => Some("timestamp_ntz"),
        _ => None,
    }
}

/// The Arrow field of a source column. Only identifier columns are non-nullable in the target;
/// every other column is nullable, whatever the source says (S-O).
pub fn column_field(schema: &TableSchema, column: &ColumnSchema) -> Field {
    let field = Field::new(
        &column.name,
        arrow_type(&column.logical),
        !schema.is_identifier(&column.name),
    );
    match logical_tag(column) {
        Some(tag) => field.with_metadata(HashMap::from([(LOGICAL_TAG.into(), tag.into())])),
        None => field,
    }
}

/// The lineage fields appended to every upsert row (Data-Flow §Data Lineage), in
/// [`LINEAGE_COLUMNS`] order: `_op`, `_ts` (UTC instant), `_lsn`, `_seq`, `_source_db`,
/// `_pipeline`. None is nullable.
pub fn lineage_fields() -> Vec<Field> {
    LINEAGE_COLUMNS
        .iter()
        .map(|name| {
            let t = if *name == "_ts" {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            } else {
                DataType::Utf8
            };
            Field::new(*name, t, false)
        })
        .collect()
}

/// The canonical Arrow schema of a table's rows: the source columns in source order, then the
/// lineage columns. This is `TableChangeSet::schema` and the schema a sink creates the table with.
pub fn upsert_schema(schema: &TableSchema) -> SchemaRef {
    let mut fields: Vec<Field> = schema
        .columns()
        .iter()
        .map(|c| column_field(schema, c))
        .collect();
    fields.extend(lineage_fields());
    Arc::new(Schema::new(fields))
}

/// The schema of delete rows: the identifier columns, then `extra` columns (the before-image
/// partition columns a sink routes deletes by), each once. Only identifier columns are required.
pub fn delete_schema(schema: &TableSchema, extra: &[String]) -> Result<SchemaRef, HaltReason> {
    let mut fields = Vec::new();
    for name in schema.identifier().iter().chain(extra) {
        let column = schema
            .column(name)
            .ok_or_else(|| HaltReason::SchemaMismatch {
                column: name.clone(),
                detail: "a delete column that isn't in the table".into(),
            })?;
        if !fields.iter().any(|f: &Field| f.name() == name) {
            fields.push(column_field(schema, column));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// The field of the unchanged mask for `column` (see [`UNCHANGED_MASK_PREFIX`]).
pub fn unchanged_mask_field(column: &str) -> Field {
    Field::new(
        format!("{UNCHANGED_MASK_PREFIX}{column}"),
        DataType::Boolean,
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TableRef;
    use ColumnValueType as V;

    fn table() -> TableSchema {
        let mut uuid = ColumnSchema::new("ref", V::Uuid, "uuid");
        uuid.nullable = false;
        TableSchema::new(
            TableRef::new("public", "orders"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("total", V::UInt64, "bigint unsigned").not_null(),
                uuid,
                ColumnSchema::new("tags", V::Array(Box::new(V::Text)), "text[]"),
                ColumnSchema::new("at", V::Timestamp { with_tz: false }, "timestamp"),
                ColumnSchema::new("region", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    #[test]
    fn logical_types_map_to_their_arrow_types() {
        use LogicalType as L;
        let cases = [
            (L::Bool, DataType::Boolean),
            (L::Int16, DataType::Int16),
            (L::Int32, DataType::Int32),
            (L::Int64, DataType::Int64),
            (L::Float32, DataType::Float32),
            (L::Float64, DataType::Float64),
            (
                L::Decimal {
                    precision: 38,
                    scale: 10,
                },
                DataType::Decimal128(38, 10),
            ),
            (L::String, DataType::Utf8),
            (L::Json, DataType::Utf8),
            (L::Binary, DataType::Binary),
            (L::Date, DataType::Date32),
            (L::Time, DataType::Time64(TimeUnit::Microsecond)),
            (
                L::TimestampTz,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                L::TimestampNtz,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (L::Uuid, DataType::FixedSizeBinary(16)),
        ];
        for (logical, arrow) in cases {
            assert_eq!(arrow_type(&logical), arrow, "{logical}");
        }
        let list = arrow_type(&L::List(Box::new(L::List(Box::new(L::Int32)))));
        let DataType::List(outer) = list else {
            panic!("{list:?}")
        };
        assert_eq!(outer.name(), LIST_ELEMENT);
        assert!(outer.is_nullable());
        assert!(
            matches!(outer.data_type(), DataType::List(inner) if inner.data_type() == &DataType::Int32)
        );
    }

    #[test]
    fn upsert_schema_is_columns_then_lineage_and_only_keys_are_required() {
        let s = upsert_schema(&table());
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "id",
                "total",
                "ref",
                "tags",
                "at",
                "region",
                "_op",
                "_ts",
                "_lsn",
                "_seq",
                "_source_db",
                "_pipeline"
            ]
        );
        let nullable = |n: &str| s.field_with_name(n).unwrap().is_nullable();
        assert!(!nullable("id"));
        assert!(
            nullable("total") && nullable("ref"),
            "NOT NULL at the source, nullable in the target (S-O)"
        );
        assert!(!nullable("_op") && !nullable("_seq"));
        assert_eq!(
            s.field_with_name("_ts").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn tags_mark_what_arrow_alone_doesnt_say() {
        let s = upsert_schema(&table());
        let tag = |n: &str| {
            s.field_with_name(n)
                .unwrap()
                .metadata()
                .get(LOGICAL_TAG)
                .cloned()
        };
        assert_eq!(tag("total").as_deref(), Some("uint64"));
        assert_eq!(
            s.field_with_name("total").unwrap().data_type(),
            &DataType::Decimal128(20, 0)
        );
        assert_eq!(tag("ref").as_deref(), Some("uuid"));
        assert_eq!(tag("at").as_deref(), Some("timestamp_ntz"));
        assert_eq!(tag("id"), None);
        assert_eq!(tag("tags"), None);
        assert_eq!(tag("_ts"), None);
    }

    #[test]
    fn delete_schema_is_identifier_then_extra_columns() {
        let s = delete_schema(&table(), &["region".into(), "id".into()]).unwrap();
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            ["id", "region"],
            "an identifier listed again isn't repeated"
        );
        assert!(!s.field(0).is_nullable() && s.field(1).is_nullable());
        let err = delete_schema(&table(), &["nope".into()]).unwrap_err();
        assert_eq!(err.code(), "SCHEMA_MISMATCH");
    }

    #[test]
    fn unchanged_masks_use_the_reserved_prefix() {
        let f = unchanged_mask_field("doc");
        assert_eq!(f.name(), "__data_avalanche_unchanged:doc");
        assert!(crate::is_reserved_column(f.name()));
        assert_eq!(f.data_type(), &DataType::Boolean);
    }
}
