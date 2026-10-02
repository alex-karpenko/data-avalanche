use crate::{ColumnSchema, HaltReason, LogicalType, TableRef, TableSchema, is_reserved_column};

/// One normalized source schema change (`Schema_Change…` §3: channels B + C produce them, in
/// stream order, scoped to the change).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChange {
    AddColumn(ColumnSchema),
    DropColumn(String),
    /// `proven`: PG `attnum` or a MySQL `RENAME`/`CHANGE COLUMN` keyword shows it is a rename,
    /// not a drop + add (§6).
    RenameColumn {
        from: String,
        to: String,
        proven: bool,
    },
    /// `narrowed`: the source says the new type holds fewer values even when both map to the same
    /// logical type (`varchar(100)` → `varchar(10)`). Only the source can tell.
    ChangeType {
        column: String,
        from: ColumnSchema,
        to: ColumnSchema,
        narrowed: bool,
    },
    SetNotNull(String),
    DropNotNull(String),
    /// The identifier (the primary key, or the `tables.primary_key` columns) changed. A replica
    /// identity that no longer covers it halts the table too; another replica identity change
    /// is ignorable (`Schema_Change…` §7.2, Q-M5-17).
    IdentifierChanged,
    TableDropped,
    /// MySQL `ENUM`/`SET` gained values at the end (decoded strings keep their meaning).
    EnumValuesAppended(String),
    /// MySQL `ENUM`/`SET` values were reordered or removed (decoded strings change meaning).
    EnumValuesChanged(String),
    /// A change with no effect on the target: index, comment, storage parameters,
    /// `AUTO_INCREMENT`, a charset change with the same logical type.
    NoTargetEffect(String),
    /// DDL that couldn't be parsed or classified (the DDL text or a description).
    Unclassifiable(String),
}

/// The changes to one table at one stream position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDelta {
    pub table: TableRef,
    pub changes: Vec<SchemaChange>,
}

/// §7.2: `Safe` is applied through the sink's `evolve` (which may still answer `Degraded` or
/// `Unsupported`), `Unsafe` halts the table, `Ignorable` needs no target action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaClass {
    Safe,
    Unsafe(HaltReason),
    Ignorable,
}

/// The §7.2 DDL classification table (the G-19 contract) as code. `schema` is the table's schema
/// before the delta; it tells which columns are identifier columns.
pub fn classify(schema: &TableSchema, delta: &SchemaDelta) -> Vec<(SchemaChange, DeltaClass)> {
    delta
        .changes
        .iter()
        .map(|c| (c.clone(), classify_one(schema, c)))
        .collect()
}

fn unsafe_ddl(detail: String) -> DeltaClass {
    DeltaClass::Unsafe(HaltReason::UnsafeDdl { detail })
}

fn classify_one(schema: &TableSchema, change: &SchemaChange) -> DeltaClass {
    use SchemaChange as C;
    match change {
        C::AddColumn(c) if is_reserved_column(&c.name) => {
            DeltaClass::Unsafe(HaltReason::ReservedColumnCollision {
                column: c.name.clone(),
            })
        }
        C::AddColumn(_) => DeltaClass::Safe,
        C::DropColumn(c) if schema.is_identifier(c) => unsafe_ddl(format!(
            "dropping identifier column {c:?} breaks keying; re-snapshot"
        )),
        C::DropColumn(_) => DeltaClass::Safe,
        C::RenameColumn { to, .. } if is_reserved_column(to) => {
            DeltaClass::Unsafe(HaltReason::ReservedColumnCollision { column: to.clone() })
        }
        C::RenameColumn {
            from,
            proven: false,
            ..
        } if schema.is_identifier(from) => unsafe_ddl(format!(
            "identifier column {from:?} was renamed, and the rename can't be proven (it reads as a drop); re-snapshot"
        )),
        C::RenameColumn { .. } => DeltaClass::Safe,
        C::ChangeType {
            column,
            from,
            to,
            narrowed,
        } => classify_type(column, &from.logical, &to.logical, *narrowed),
        // S-O: every non-identifier target column is nullable, so source nullability never
        // reaches the target.
        C::SetNotNull(_) | C::DropNotNull(_) => DeltaClass::Ignorable,
        C::IdentifierChanged => unsafe_ddl(
            "the identifier (primary key or tables.primary_key) changed; re-snapshot".into(),
        ),
        C::TableDropped => DeltaClass::Unsafe(HaltReason::TableDropped),
        C::EnumValuesAppended(_) | C::NoTargetEffect(_) => DeltaClass::Ignorable,
        C::EnumValuesChanged(c) => unsafe_ddl(format!(
            "ENUM/SET values of column {c:?} were reordered or removed, which changes what stored values mean"
        )),
        C::Unclassifiable(what) => unsafe_ddl(format!("unclassifiable schema change: {what}")),
    }
}

fn classify_type(column: &str, from: &LogicalType, to: &LogicalType, narrowed: bool) -> DeltaClass {
    use LogicalType as L;
    if from == to {
        return if narrowed {
            unsafe_ddl(format!("column {column:?} was narrowed"))
        } else {
            DeltaClass::Ignorable
        };
    }
    let widening = matches!(
        (from, to),
        (L::Int16, L::Int32 | L::Int64) | (L::Int32, L::Int64) | (L::Float32, L::Float64)
    ) || matches!(
        (from, to),
        (L::Decimal { precision: p, scale: s }, L::Decimal { precision: p2, scale: s2 })
            if s == s2 && p2 > p
    );
    if widening && !narrowed {
        DeltaClass::Safe
    } else {
        unsafe_ddl(format!(
            "column {column:?} changed type from {from} to {to}, which isn't a supported widening"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnValueType as V;

    fn schema() -> TableSchema {
        TableSchema::new(
            TableRef::new("public", "orders"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("qty", V::Int32, "integer"),
                ColumnSchema::new("note", V::Text, "varchar(100)"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn class(change: SchemaChange) -> DeltaClass {
        let delta = SchemaDelta {
            table: TableRef::new("public", "orders"),
            changes: vec![change],
        };
        let mut out = classify(&schema(), &delta);
        assert_eq!(out.len(), 1);
        out.pop().unwrap().1
    }

    fn unsafe_code(change: SchemaChange) -> &'static str {
        match class(change) {
            DeltaClass::Unsafe(h) => h.code(),
            other => panic!("expected Unsafe, got {other:?}"),
        }
    }

    fn col(name: &str, t: V, source: &str) -> ColumnSchema {
        ColumnSchema::new(name, t, source)
    }

    fn retype(column: &str, from: V, to: V, narrowed: bool) -> SchemaChange {
        SchemaChange::ChangeType {
            column: column.into(),
            from: col(column, from, "before"),
            to: col(column, to, "after"),
            narrowed,
        }
    }

    #[test]
    fn row_add_column_any_nullability_is_safe() {
        assert_eq!(
            class(SchemaChange::AddColumn(col("c", V::Text, "text"))),
            DeltaClass::Safe
        );
        let not_null = col("c", V::Text, "text").not_null();
        assert_eq!(class(SchemaChange::AddColumn(not_null)), DeltaClass::Safe);
    }

    #[test]
    fn row_drop_non_identifier_column_is_safe() {
        assert_eq!(
            class(SchemaChange::DropColumn("note".into())),
            DeltaClass::Safe
        );
    }

    #[test]
    fn row_drop_identifier_column_is_unsafe() {
        assert_eq!(
            unsafe_code(SchemaChange::DropColumn("id".into())),
            "UNSAFE_DDL"
        );
    }

    #[test]
    fn row_rename_proven_is_safe() {
        let r = SchemaChange::RenameColumn {
            from: "note".into(),
            to: "memo".into(),
            proven: true,
        };
        assert_eq!(class(r), DeltaClass::Safe);
        let id = SchemaChange::RenameColumn {
            from: "id".into(),
            to: "order_id".into(),
            proven: true,
        };
        assert_eq!(class(id), DeltaClass::Safe, "a proven rename keeps the key");
    }

    #[test]
    fn row_rename_not_provable_is_safe_degraded_except_for_the_identifier() {
        let r = SchemaChange::RenameColumn {
            from: "note".into(),
            to: "memo".into(),
            proven: false,
        };
        assert_eq!(class(r), DeltaClass::Safe);
        let id = SchemaChange::RenameColumn {
            from: "id".into(),
            to: "order_id".into(),
            proven: false,
        };
        assert_eq!(unsafe_code(id), "UNSAFE_DDL", "reads as dropping the key");
    }

    #[test]
    fn row_widen_within_the_iceberg_set_is_safe() {
        for (from, to) in [
            (V::Int16, V::Int32),
            (V::Int16, V::Int64),
            (V::Int32, V::Int64),
            (V::Float32, V::Float64),
            (
                V::Numeric {
                    precision: 10,
                    scale: 2,
                },
                V::Numeric {
                    precision: 12,
                    scale: 2,
                },
            ),
        ] {
            assert_eq!(class(retype("qty", from, to, false)), DeltaClass::Safe);
        }
    }

    #[test]
    fn row_widen_outside_the_set_is_unsafe() {
        let to_decimal = V::Numeric {
            precision: 20,
            scale: 0,
        };
        assert_eq!(
            unsafe_code(retype("qty", V::Int32, to_decimal, false)),
            "UNSAFE_DDL"
        );
        let rescale = retype(
            "qty",
            V::Numeric {
                precision: 10,
                scale: 2,
            },
            V::Numeric {
                precision: 12,
                scale: 4,
            },
            false,
        );
        assert_eq!(unsafe_code(rescale), "UNSAFE_DDL");
    }

    #[test]
    fn row_narrow_is_unsafe() {
        assert_eq!(
            unsafe_code(retype("qty", V::Int64, V::Int32, false)),
            "UNSAFE_DDL"
        );
        let shrink = retype(
            "qty",
            V::Numeric {
                precision: 12,
                scale: 2,
            },
            V::Numeric {
                precision: 10,
                scale: 2,
            },
            false,
        );
        assert_eq!(unsafe_code(shrink), "UNSAFE_DDL");
        // varchar(100) -> varchar(10): same logical type, the source reports the narrowing.
        assert_eq!(
            unsafe_code(retype("note", V::Text, V::Text, true)),
            "UNSAFE_DDL"
        );
        // varchar(10) -> varchar(100): nothing changes in the target.
        assert_eq!(
            class(retype("note", V::Text, V::Text, false)),
            DeltaClass::Ignorable
        );
    }

    #[test]
    fn row_cross_family_is_unsafe() {
        assert_eq!(
            unsafe_code(retype("qty", V::Int32, V::Text, false)),
            "UNSAFE_DDL"
        );
        let ts = V::Timestamp { with_tz: false };
        assert_eq!(unsafe_code(retype("qty", ts, V::Date, false)), "UNSAFE_DDL");
    }

    #[test]
    fn row_nullability_changes_are_ignorable_under_s_o() {
        assert_eq!(
            class(SchemaChange::SetNotNull("note".into())),
            DeltaClass::Ignorable
        );
        assert_eq!(
            class(SchemaChange::DropNotNull("note".into())),
            DeltaClass::Ignorable
        );
    }

    #[test]
    fn row_identifier_change_is_unsafe() {
        assert_eq!(unsafe_code(SchemaChange::IdentifierChanged), "UNSAFE_DDL");
    }

    #[test]
    fn row_reserved_lineage_name_is_unsafe() {
        for name in ["_op", "_TS", "_seq", "__data_avalanche_del"] {
            let add = SchemaChange::AddColumn(col(name, V::Text, "text"));
            assert_eq!(unsafe_code(add), "RESERVED_COLUMN_COLLISION");
            let rename = SchemaChange::RenameColumn {
                from: "note".into(),
                to: name.into(),
                proven: true,
            };
            assert_eq!(unsafe_code(rename), "RESERVED_COLUMN_COLLISION");
        }
    }

    #[test]
    fn row_drop_table_is_unsafe_table_dropped() {
        assert_eq!(unsafe_code(SchemaChange::TableDropped), "TABLE_DROPPED");
    }

    #[test]
    fn row_enum_append_is_ignorable_and_reorder_is_unsafe() {
        assert_eq!(
            class(SchemaChange::EnumValuesAppended("status".into())),
            DeltaClass::Ignorable
        );
        assert_eq!(
            unsafe_code(SchemaChange::EnumValuesChanged("status".into())),
            "UNSAFE_DDL"
        );
    }

    #[test]
    fn row_index_comment_storage_changes_are_ignorable() {
        let c = SchemaChange::NoTargetEffect("CREATE INDEX orders_note ON orders(note)".into());
        assert_eq!(class(c), DeltaClass::Ignorable);
    }

    #[test]
    fn row_unclassifiable_ddl_is_unsafe() {
        let c = SchemaChange::Unclassifiable("ALTER TABLE orders ???".into());
        match class(c) {
            DeltaClass::Unsafe(h) => assert!(h.to_string().contains("ALTER TABLE orders ???")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_delta_classifies_each_change_in_order() {
        let delta = SchemaDelta {
            table: TableRef::new("public", "orders"),
            changes: vec![
                SchemaChange::AddColumn(col("c", V::Text, "text")),
                SchemaChange::DropNotNull("note".into()),
                SchemaChange::TableDropped,
            ],
        };
        let classes: Vec<_> = classify(&schema(), &delta)
            .into_iter()
            .map(|(_, c)| c)
            .collect();
        assert_eq!(
            classes,
            [
                DeltaClass::Safe,
                DeltaClass::Ignorable,
                DeltaClass::Unsafe(HaltReason::TableDropped)
            ]
        );
    }
}
