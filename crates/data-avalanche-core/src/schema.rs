use thiserror::Error;

use crate::{ColumnValueType, LogicalType, TableRef, first_reserved_collision};

/// One source column (`Schema_Change…` §7): what the decoder produces (`value_type`), what the
/// target stores (`logical`, Hop 2), and the source's own description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub value_type: ColumnValueType,
    pub logical: LogicalType,
    /// The source column's nullability. The target ignores it: every non-identifier target column
    /// is nullable (S-O).
    pub nullable: bool,
    /// The source type as the catalog prints it (`varchar(100)`, `bigint unsigned`), for messages.
    pub source_type: String,
    /// PostgreSQL `attnum`, the identity that survives a rename (`Schema_Change…` §6).
    pub source_ordinal: Option<i32>,
}

impl ColumnSchema {
    /// A column whose `logical` type is the canonical one for `value_type`
    /// ([`canonical_logical`]), nullable, with no ordinal.
    pub fn new(name: &str, value_type: ColumnValueType, source_type: &str) -> Self {
        Self {
            name: name.to_owned(),
            logical: canonical_logical(&value_type),
            value_type,
            nullable: true,
            source_type: source_type.to_owned(),
            source_ordinal: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
}

/// The logical type a decoded value type maps to when no override applies (`Type_Mapping.md`):
/// `UInt64` → `decimal(20,0)`, `Numeric(P,S)` → `decimal(P,S)`, `Text` → `string`, and so on.
pub fn canonical_logical(t: &ColumnValueType) -> LogicalType {
    use ColumnValueType as V;
    match t {
        V::Bool => LogicalType::Bool,
        V::Int16 => LogicalType::Int16,
        V::Int32 => LogicalType::Int32,
        V::Int64 => LogicalType::Int64,
        V::UInt64 => LogicalType::Decimal {
            precision: 20,
            scale: 0,
        },
        V::Float32 => LogicalType::Float32,
        V::Float64 => LogicalType::Float64,
        V::Numeric { precision, scale } => LogicalType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        V::Text => LogicalType::String,
        V::Bytes => LogicalType::Binary,
        V::Timestamp { with_tz: true } => LogicalType::TimestampTz,
        V::Timestamp { with_tz: false } => LogicalType::TimestampNtz,
        V::Date => LogicalType::Date,
        V::Time => LogicalType::Time,
        V::Uuid => LogicalType::Uuid,
        V::Json => LogicalType::Json,
        V::Array(e) => LogicalType::List(Box::new(canonical_logical(e))),
    }
}

/// True when Hop 2 can store values of `value_type` in a column of `logical` type without a
/// conversion other than a decimal rescale. Overrides that change the representation (`uuid` →
/// `string`) are applied by the decoder (Hop 1), which then reports the new `value_type`.
pub fn hop2_compatible(value_type: &ColumnValueType, logical: &LogicalType) -> bool {
    use ColumnValueType as V;
    match (value_type, logical) {
        (V::UInt64 | V::Numeric { .. }, LogicalType::Decimal { .. }) => true,
        (V::Array(e), LogicalType::List(l)) => hop2_compatible(e, l),
        (V::Array(_), _) | (_, LogicalType::List(_)) => false,
        (v, l) => canonical_logical(v) == *l,
    }
}

/// Why a table stops replicating while the other tables of its pipeline continue (`Schema_Change…`
/// §7.2, config §5, `Type_Mapping.md`). [`HaltReason::code`] is the stable identifier for status,
/// logs and metrics.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HaltReason {
    #[error(
        "column {column:?} collides with a reserved lineage or internal column name; rename it at the source"
    )]
    ReservedColumnCollision { column: String },
    #[error(
        "the table was dropped at the source; the target is kept, and a re-created table needs an operator re-snapshot"
    )]
    TableDropped,
    #[error("unsafe schema change: {detail}")]
    UnsafeDdl { detail: String },
    #[error("the table has no primary key and no tables.primary_key override")]
    NoIdentifier,
    #[error("identifier column {column:?} can't be a key: {reason}")]
    IdentifierNotUsable { column: String, reason: String },
    #[error("identifier columns {missing:?} are not in the table's REPLICA IDENTITY")]
    IdentifierNotInReplicaIdentity { missing: Vec<String> },
    #[error("the sink can't apply this schema change: {detail}")]
    EvolutionUnsupported { detail: String },
    #[error("the schema changed and schema_evolution.enabled is false")]
    EvolutionDisabled,
    #[error("a value in column {column:?} doesn't fit its decimal type")]
    NumericOverflow { column: String },
    #[error(
        "column {column:?} holds the invalid date {raw:?}; set tables.column_type_overrides: {{{column}: string}} or fix the source rows"
    )]
    InvalidDateValue { column: String, raw: String },
    #[error("column {column:?}: {detail}")]
    SchemaMismatch { column: String, detail: String },
    #[error("identifier column {column:?} is NULL")]
    NullIdentifier { column: String },
    #[error("the source can't replicate this table: {reason}")]
    NotReplicable { reason: String },
}

impl HaltReason {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ReservedColumnCollision { .. } => "RESERVED_COLUMN_COLLISION",
            Self::TableDropped => "TABLE_DROPPED",
            Self::UnsafeDdl { .. } => "UNSAFE_DDL",
            Self::NoIdentifier => "NO_IDENTIFIER",
            Self::IdentifierNotUsable { .. } => "IDENTIFIER_NOT_USABLE",
            Self::IdentifierNotInReplicaIdentity { .. } => "IDENTIFIER_NOT_IN_REPLICA_IDENTITY",
            Self::EvolutionUnsupported { .. } => "EVOLUTION_UNSUPPORTED",
            Self::EvolutionDisabled => "EVOLUTION_DISABLED",
            Self::NumericOverflow { .. } => "NUMERIC_OVERFLOW",
            Self::InvalidDateValue { .. } => "INVALID_DATE_VALUE",
            Self::SchemaMismatch { .. } => "SCHEMA_MISMATCH",
            Self::NullIdentifier { .. } => "NULL_IDENTIFIER",
            Self::NotReplicable { .. } => "TABLE_NOT_REPLICABLE",
        }
    }
}

/// A source table's schema as of one stream position: its columns in source order and the
/// identifier (inferred PK or `tables.primary_key`) that keys upserts and deletes (G-13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    table: TableRef,
    columns: Vec<ColumnSchema>,
    identifier: Vec<String>,
    source_id: Option<u64>,
}

impl TableSchema {
    /// Checks the table can be replicated, in this order: no reserved column name (config §5), a
    /// non-empty identifier (Ghost_Row §3), identifier columns listed once that exist and are NOT
    /// NULL, non-floating and primitive (Ghost_Row §1), unique column names, and every column's
    /// logical type reachable from its value type ([`hop2_compatible`]).
    pub fn new(
        table: TableRef,
        columns: Vec<ColumnSchema>,
        identifier: Vec<String>,
    ) -> Result<Self, HaltReason> {
        if let Some(c) = first_reserved_collision(columns.iter().map(|c| c.name.as_str())) {
            return Err(HaltReason::ReservedColumnCollision {
                column: c.to_owned(),
            });
        }
        if identifier.is_empty() {
            return Err(HaltReason::NoIdentifier);
        }
        for (i, id) in identifier.iter().enumerate() {
            let unusable = |reason: &str| HaltReason::IdentifierNotUsable {
                column: id.clone(),
                reason: reason.to_owned(),
            };
            if identifier[..i].contains(id) {
                return Err(unusable("listed twice"));
            }
            let c = columns
                .iter()
                .find(|c| &c.name == id)
                .ok_or_else(|| unusable("no such column"))?;
            if c.nullable {
                return Err(unusable("it is nullable"));
            }
            match c.value_type {
                ColumnValueType::Float32 | ColumnValueType::Float64 => {
                    return Err(unusable("floating-point equality is unsafe"));
                }
                ColumnValueType::Array(_) | ColumnValueType::Json => {
                    return Err(unusable("it isn't a primitive type"));
                }
                _ => {}
            }
        }
        for (i, c) in columns.iter().enumerate() {
            if columns[..i].iter().any(|p| p.name == c.name) {
                return Err(HaltReason::SchemaMismatch {
                    column: c.name.clone(),
                    detail: "the name appears twice".into(),
                });
            }
            if !hop2_compatible(&c.value_type, &c.logical) {
                return Err(HaltReason::SchemaMismatch {
                    column: c.name.clone(),
                    detail: format!(
                        "values of type {:?} can't be stored as {}",
                        c.value_type, c.logical
                    ),
                });
            }
        }
        Ok(Self {
            table,
            columns,
            identifier,
            source_id: None,
        })
    }

    /// The source's own identity of the table (PostgreSQL: the relation OID), which outlives a
    /// rename and changes when a table is dropped and another takes its name: the table this
    /// schema describes is gone when the name now has another identity (`Schema_Change…` §2.1).
    pub fn with_source_id(mut self, id: u64) -> Self {
        self.source_id = Some(id);
        self
    }

    /// See [`TableSchema::with_source_id`]; `None` for a source without one.
    pub fn source_id(&self) -> Option<u64> {
        self.source_id
    }

    pub fn table(&self) -> &TableRef {
        &self.table
    }

    pub fn columns(&self) -> &[ColumnSchema] {
        &self.columns
    }

    pub fn identifier(&self) -> &[String] {
        &self.identifier
    }

    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn is_identifier(&self, name: &str) -> bool {
        self.identifier.iter().any(|i| i == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ColumnValueType as V;

    fn t() -> TableRef {
        TableRef::new("public", "orders")
    }

    fn cols() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new(
                "amount",
                V::Numeric {
                    precision: 10,
                    scale: 2,
                },
                "numeric(10,2)",
            ),
            ColumnSchema::new("note", V::Text, "text"),
        ]
    }

    fn halt(columns: Vec<ColumnSchema>, identifier: &[&str]) -> HaltReason {
        TableSchema::new(
            t(),
            columns,
            identifier.iter().map(|s| s.to_string()).collect(),
        )
        .unwrap_err()
    }

    #[test]
    fn a_keyed_table_builds() {
        let s = TableSchema::new(t(), cols(), vec!["id".into()]).unwrap();
        assert_eq!(s.index_of("note"), Some(2));
        assert!(s.is_identifier("id") && !s.is_identifier("note"));
        assert_eq!(
            s.column("amount").unwrap().logical,
            LogicalType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn a_reserved_column_name_halts_first() {
        let mut c = cols();
        c.push(ColumnSchema::new("_SEQ", V::Text, "text"));
        let h = halt(c, &[]);
        assert_eq!(h.code(), "RESERVED_COLUMN_COLLISION");
        assert_eq!(
            h,
            HaltReason::ReservedColumnCollision {
                column: "_SEQ".into()
            }
        );
    }

    #[test]
    fn identifier_rules() {
        assert_eq!(halt(cols(), &[]), HaltReason::NoIdentifier);
        let reason = |columns, id| match halt(columns, &[id]) {
            HaltReason::IdentifierNotUsable { reason, .. } => reason,
            other => panic!("{other:?}"),
        };
        assert_eq!(reason(cols(), "missing"), "no such column");
        assert_eq!(reason(cols(), "note"), "it is nullable");
        let mut c = cols();
        c.push(ColumnSchema::new("score", V::Float64, "double precision").not_null());
        c.push(ColumnSchema::new("doc", V::Json, "jsonb").not_null());
        assert_eq!(
            reason(c.clone(), "score"),
            "floating-point equality is unsafe"
        );
        assert_eq!(reason(c, "doc"), "it isn't a primitive type");
        assert_eq!(
            halt(cols(), &["id", "id"]),
            HaltReason::IdentifierNotUsable {
                column: "id".into(),
                reason: "listed twice".into()
            }
        );
    }

    #[test]
    fn text_uuid_unsigned_and_decimal_identifiers_are_keys() {
        let columns = vec![
            ColumnSchema::new("code", V::Text, "text").not_null(),
            ColumnSchema::new("ref", V::Uuid, "uuid").not_null(),
            ColumnSchema::new("seq", V::UInt64, "bigint unsigned").not_null(),
            ColumnSchema::new(
                "part",
                V::Numeric {
                    precision: 10,
                    scale: 0,
                },
                "numeric(10,0)",
            )
            .not_null(),
        ];
        let id: Vec<String> = ["code", "ref", "seq", "part"].map(String::from).into();
        let s = TableSchema::new(t(), columns, id.clone()).unwrap();
        assert_eq!(s.identifier(), id);
    }

    #[test]
    fn duplicate_names_and_unreachable_logical_types_are_mismatches() {
        let mut c = cols();
        c.push(ColumnSchema::new("note", V::Text, "text"));
        assert_eq!(halt(c, &["id"]).code(), "SCHEMA_MISMATCH");
        let mut c = cols();
        c[2].logical = LogicalType::Uuid;
        assert!(
            halt(c, &["id"])
                .to_string()
                .contains("can't be stored as uuid")
        );
    }

    #[test]
    fn canonical_and_compatible_logical_types() {
        assert_eq!(
            canonical_logical(&V::UInt64),
            LogicalType::Decimal {
                precision: 20,
                scale: 0
            }
        );
        assert_eq!(
            canonical_logical(&V::Timestamp { with_tz: false }),
            LogicalType::TimestampNtz
        );
        assert_eq!(
            canonical_logical(&V::Array(Box::new(V::Text))),
            LogicalType::List(Box::new(LogicalType::String))
        );
        // A decimal override may change precision and scale; values are rescaled at append.
        assert!(hop2_compatible(
            &V::Numeric {
                precision: 10,
                scale: 2
            },
            &LogicalType::Decimal {
                precision: 38,
                scale: 10
            }
        ));
        assert!(!hop2_compatible(&V::Uuid, &LogicalType::String));
        assert!(!hop2_compatible(
            &V::Array(Box::new(V::Int32)),
            &LogicalType::List(Box::new(LogicalType::Int64))
        ));
    }

    #[test]
    fn every_halt_reason_has_a_distinct_code() {
        let all = [
            HaltReason::ReservedColumnCollision { column: "c".into() },
            HaltReason::TableDropped,
            HaltReason::UnsafeDdl { detail: "d".into() },
            HaltReason::NoIdentifier,
            HaltReason::IdentifierNotUsable {
                column: "c".into(),
                reason: "r".into(),
            },
            HaltReason::IdentifierNotInReplicaIdentity {
                missing: vec!["c".into()],
            },
            HaltReason::EvolutionUnsupported { detail: "d".into() },
            HaltReason::EvolutionDisabled,
            HaltReason::NumericOverflow { column: "c".into() },
            HaltReason::InvalidDateValue {
                column: "c".into(),
                raw: "2024-00-15".into(),
            },
            HaltReason::SchemaMismatch {
                column: "c".into(),
                detail: "d".into(),
            },
            HaltReason::NullIdentifier { column: "c".into() },
            HaltReason::NotReplicable { reason: "r".into() },
        ];
        let codes: std::collections::BTreeSet<_> = all.iter().map(HaltReason::code).collect();
        assert_eq!(codes.len(), all.len());
        assert!(
            all[9]
                .to_string()
                .contains("tables.column_type_overrides: {c: string}")
        );
    }

    #[test]
    fn a_source_id_is_kept_and_tells_tables_apart() {
        let s = TableSchema::new(t(), cols(), vec!["id".into()]).unwrap();
        assert_eq!(s.source_id(), None);
        let a = s.clone().with_source_id(7);
        assert_eq!(a.source_id(), Some(7));
        assert_ne!(a, s.with_source_id(8), "another table under the name");
    }
}
