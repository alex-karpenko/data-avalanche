//! Hop 3b for schemas (`Type_Mapping.md` §Hop 3b): a Hop 2 Arrow field ↔ a Delta column.
//!
//! Delta has no `uuid` or `time`, and `timestamp_ntz` may be stored three ways, so a Delta column
//! alone doesn't say which Hop 2 type it holds. Every column this sink creates carries its logical
//! type in the column metadata (`data-avalanche.type`) and the Hop 2 tag (`data-avalanche.logical`)
//! when there is one; reading them back gives the Hop 2 field exactly.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use data_avalanche_core::{LIST_ELEMENT, LOGICAL_TAG, LogicalType, arrow_type};
use deltalake::kernel::{
    ArrayType, DataType as DeltaType, MetadataValue, PrimitiveType, StructField, StructType,
};

use crate::NtzAs;

/// Column-metadata key holding the column's logical type (`LogicalType`'s text form).
pub const TYPE_KEY: &str = "data-avalanche.type";

/// The logical type a Hop 2 field holds (the inverse of `arrow_type`, with the tag telling `json`
/// from `string`).
pub fn logical_of(field: &Field) -> Result<LogicalType, String> {
    let tag = field.metadata().get(LOGICAL_TAG).map(String::as_str);
    Ok(match field.data_type() {
        DataType::Boolean => LogicalType::Bool,
        DataType::Int16 => LogicalType::Int16,
        DataType::Int32 => LogicalType::Int32,
        DataType::Int64 => LogicalType::Int64,
        DataType::Float32 => LogicalType::Float32,
        DataType::Float64 => LogicalType::Float64,
        DataType::Decimal128(precision, scale) => LogicalType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 if tag == Some("json") => LogicalType::Json,
        DataType::Utf8 => LogicalType::String,
        DataType::Binary => LogicalType::Binary,
        DataType::Date32 => LogicalType::Date,
        DataType::Time64(TimeUnit::Microsecond) => LogicalType::Time,
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            LogicalType::TimestampTz
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => LogicalType::TimestampNtz,
        DataType::FixedSizeBinary(16) => LogicalType::Uuid,
        DataType::List(element) => LogicalType::List(Box::new(logical_of(element)?)),
        other => {
            return Err(format!(
                "column {:?}: {other} isn't a Hop 2 type",
                field.name()
            ));
        }
    })
}

/// The Delta type a logical type is stored as (`Type_Mapping.md` §Hop 3b).
pub fn delta_type(logical: &LogicalType, ntz: NtzAs) -> Result<DeltaType, String> {
    use LogicalType as L;
    Ok(match logical {
        L::Bool => DeltaType::BOOLEAN,
        L::Int16 => DeltaType::SHORT,
        L::Int32 => DeltaType::INTEGER,
        L::Int64 => DeltaType::LONG,
        L::Float32 => DeltaType::FLOAT,
        L::Float64 => DeltaType::DOUBLE,
        L::Decimal { precision, scale } => {
            let scale = u8::try_from(*scale)
                .map_err(|_| format!("{logical}: Delta decimals have no negative scale"))?;
            DeltaType::decimal(*precision, scale).map_err(|e| format!("{logical}: {e}"))?
        }
        L::String | L::Json | L::Uuid | L::Time => DeltaType::STRING,
        L::Binary => DeltaType::BINARY,
        L::Date => DeltaType::DATE,
        L::TimestampTz => DeltaType::TIMESTAMP,
        L::TimestampNtz => match ntz {
            NtzAs::TimestampNtz => DeltaType::TIMESTAMP_NTZ,
            NtzAs::Timestamp => DeltaType::TIMESTAMP,
            NtzAs::String => DeltaType::STRING,
        },
        L::List(element) => ArrayType::new(delta_type(element, ntz)?, true).into(),
    })
}

/// The Delta column for a Hop 2 field: same name and nullability, the Hop 3b type, and the
/// metadata that makes it reversible.
pub fn delta_field(field: &Field, ntz: NtzAs) -> Result<StructField, String> {
    let logical = logical_of(field)?;
    let delta = delta_type(&logical, ntz).map_err(|e| format!("column {:?}: {e}", field.name()))?;
    let mut metadata = vec![(TYPE_KEY.to_string(), logical.to_string())];
    if let Some(tag) = field.metadata().get(LOGICAL_TAG) {
        metadata.push((LOGICAL_TAG.to_string(), tag.clone()));
    }
    Ok(StructField::new(field.name(), delta, field.is_nullable()).with_metadata(metadata))
}

/// The Arrow type delta-rs reads and writes a Delta type as (the "physical" type of Hop 3b).
pub fn physical_type(delta: &DeltaType) -> Result<DataType, String> {
    Ok(match delta {
        DeltaType::Primitive(p) => match p {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Byte => DataType::Int8,
            PrimitiveType::Short => DataType::Int16,
            PrimitiveType::Integer => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal(d) => DataType::Decimal128(d.precision(), d.scale() as i8),
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Binary => DataType::Binary,
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Timestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            PrimitiveType::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
            other => return Err(format!("Delta type {other} isn't one this sink writes")),
        },
        DeltaType::Array(a) => DataType::List(Arc::new(Field::new(
            LIST_ELEMENT,
            physical_type(a.element_type())?,
            a.contains_null(),
        ))),
        other => return Err(format!("Delta type {other} isn't one this sink writes")),
    })
}

fn metadata_text(field: &StructField, key: &str) -> Option<String> {
    match field.metadata().get(key) {
        Some(MetadataValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The logical type a Delta column holds: from its metadata when this sink created it, else read
/// off its Delta type (a column someone else added).
pub fn column_logical(field: &StructField) -> Result<LogicalType, String> {
    match metadata_text(field, TYPE_KEY) {
        Some(text) => text
            .parse()
            .map_err(|e| format!("column {:?}: {e}", field.name())),
        None => {
            let physical = physical_type(field.data_type())
                .map_err(|e| format!("column {:?}: {e}", field.name()))?;
            logical_of(&Field::new(field.name(), physical, true))
        }
    }
}

/// The Hop 2 field of a Delta column (the inverse of [`delta_field`]).
pub fn hop2_field(field: &StructField) -> Result<Field, String> {
    let logical = column_logical(field)?;
    let hop2 = Field::new(field.name(), arrow_type(&logical), field.is_nullable());
    Ok(match metadata_text(field, LOGICAL_TAG) {
        Some(tag) => hop2.with_metadata(HashMap::from([(LOGICAL_TAG.to_string(), tag)])),
        None => hop2,
    })
}

/// The Hop 2 schema of a Delta table, in its column order.
pub fn hop2_schema(schema: &StructType) -> Result<SchemaRef, String> {
    let fields = schema
        .fields()
        .map(hop2_field)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{
        ColumnSchema, ColumnValueType as V, TableRef, TableSchema, upsert_schema,
    };

    use super::*;

    /// One column of every Hop 2 type, the identifier NOT NULL.
    fn every_type() -> TableSchema {
        let cols = vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("flag", V::Bool, "boolean"),
            ColumnSchema::new("small", V::Int16, "smallint"),
            ColumnSchema::new("int", V::Int32, "integer"),
            ColumnSchema::new("u64", V::UInt64, "bigint unsigned"),
            ColumnSchema::new("f32", V::Float32, "real"),
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
            ColumnSchema::new("tags", V::Array(Box::new(V::Uuid)), "uuid[]"),
            ColumnSchema::new(
                "grid",
                V::Array(Box::new(V::Array(Box::new(V::Time)))),
                "time[][]",
            ),
        ];
        TableSchema::new(TableRef::new("public", "t"), cols, vec!["id".into()]).unwrap()
    }

    #[test]
    fn hop3b_maps_every_logical_type() {
        let s = upsert_schema(&every_type());
        let ty = |name: &str, ntz| {
            delta_field(s.field_with_name(name).unwrap(), ntz)
                .unwrap()
                .data_type()
                .clone()
        };
        let n = NtzAs::TimestampNtz;
        assert_eq!(ty("flag", n), DeltaType::BOOLEAN);
        assert_eq!(ty("small", n), DeltaType::SHORT);
        assert_eq!(ty("int", n), DeltaType::INTEGER);
        assert_eq!(ty("id", n), DeltaType::LONG);
        assert_eq!(ty("u64", n), DeltaType::decimal(20, 0).unwrap());
        assert_eq!(ty("f32", n), DeltaType::FLOAT);
        assert_eq!(ty("f64", n), DeltaType::DOUBLE);
        assert_eq!(ty("dec", n), DeltaType::decimal(38, 10).unwrap());
        for s in ["s", "js", "u", "t", "_op", "_lsn", "_seq"] {
            assert_eq!(ty(s, n), DeltaType::STRING, "{s}");
        }
        assert_eq!(ty("bin", n), DeltaType::BINARY);
        assert_eq!(ty("d", n), DeltaType::DATE);
        assert_eq!(ty("tz", n), DeltaType::TIMESTAMP);
        assert_eq!(ty("_ts", n), DeltaType::TIMESTAMP);
        assert_eq!(ty("ntz", NtzAs::TimestampNtz), DeltaType::TIMESTAMP_NTZ);
        assert_eq!(ty("ntz", NtzAs::Timestamp), DeltaType::TIMESTAMP);
        assert_eq!(ty("ntz", NtzAs::String), DeltaType::STRING);
        assert_eq!(
            ty("tags", n),
            ArrayType::new(DeltaType::STRING, true).into()
        );
        assert_eq!(
            ty("grid", n),
            ArrayType::new(
                DeltaType::from(ArrayType::new(DeltaType::STRING, true)),
                true
            )
            .into()
        );
    }

    #[test]
    fn every_hop2_field_round_trips_through_its_delta_column() {
        let s = upsert_schema(&every_type());
        for ntz in [NtzAs::TimestampNtz, NtzAs::Timestamp, NtzAs::String] {
            let delta =
                StructType::try_new(s.fields().iter().map(|f| delta_field(f, ntz).unwrap()))
                    .unwrap();
            assert_eq!(hop2_schema(&delta).unwrap(), s, "{ntz:?}");
        }
    }

    #[test]
    fn the_column_keeps_nullability_and_the_tag() {
        let s = upsert_schema(&every_type());
        let id = delta_field(s.field_with_name("id").unwrap(), NtzAs::default()).unwrap();
        assert!(!id.is_nullable());
        let u64 = delta_field(s.field_with_name("u64").unwrap(), NtzAs::default()).unwrap();
        assert!(u64.is_nullable());
        assert_eq!(metadata_text(&u64, LOGICAL_TAG).as_deref(), Some("uint64"));
        assert_eq!(
            metadata_text(&u64, TYPE_KEY).as_deref(),
            Some("decimal(20,0)")
        );
    }

    #[test]
    fn a_column_without_metadata_reads_by_its_delta_type() {
        let foreign = StructType::try_new([
            StructField::new("n", DeltaType::LONG, true),
            StructField::new("ts", DeltaType::TIMESTAMP_NTZ, true),
            StructField::new("s", ArrayType::new(DeltaType::STRING, false), true),
        ])
        .unwrap();
        let s = hop2_schema(&foreign).unwrap();
        assert_eq!(s.field(0).data_type(), &DataType::Int64);
        assert_eq!(
            s.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            s.field(2).data_type(),
            &DataType::List(Arc::new(Field::new(LIST_ELEMENT, DataType::Utf8, true))),
            "the Hop 2 list element is always nullable"
        );
        let map = StructField::new(
            "m",
            deltalake::kernel::MapType::new(DeltaType::STRING, DeltaType::STRING, true),
            true,
        );
        assert!(
            hop2_field(&map)
                .unwrap_err()
                .contains("isn't one this sink writes")
        );
    }

    #[test]
    fn a_negative_decimal_scale_is_refused() {
        let f = Field::new("n", DataType::Decimal128(5, -2), true);
        let err = delta_field(&f, NtzAs::default()).unwrap_err();
        assert!(err.contains("negative scale"), "{err}");
        let f = Field::new("x", DataType::Int8, true);
        assert!(logical_of(&f).unwrap_err().contains("isn't a Hop 2 type"));
    }
}
