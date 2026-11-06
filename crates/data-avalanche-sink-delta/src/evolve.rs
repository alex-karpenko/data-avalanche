//! Schema evolution on Delta (Schema_Change §7.1.2, §7.2 Delta column; D-MS.9). delta-rs 1.0 adds
//! columns and nothing else, so:
//! - ADD COLUMN → `Applied` (nullable, Hop 3b type);
//! - DROP COLUMN → `Degraded`: a soft-drop, the column stays and later sets write it NULL;
//! - RENAME → `Degraded`: the new name is added with the old column's type, the old one stays;
//! - NOT NULL changes, appended enum values, changes with no target effect → `Applied`, nothing
//!   to do (target columns are nullable, S-O);
//! - everything else (a type change, an identifier change, a dropped table …) → `Unsupported`,
//!   and so is a column added or renamed to a name that differs from one of the table's by case
//!   alone: delta-rs can't hold both, and would refuse the commit at every retry (DA-SINK-12).
//!
//! Every step is idempotent: a replayed delta finds its columns and adds nothing (§8.3).

use data_avalanche_core::{
    EvolveOutcome, SchemaChange, SchemaDelta, SinkError, added_column_field, differ_by_case_alone,
};
use deltalake::DeltaTable;
use deltalake::kernel::StructField;
use deltalake::kernel::transaction::CommitProperties;

use crate::NtzAs;
use crate::error::{permanent, sink_error};
use crate::types::{delta_field, hop2_field};

/// Applies `delta` to `table`, committing with `props` (the caller's fence). The table is
/// returned as it is after the change (unchanged when nothing was added).
pub async fn evolve(
    table: DeltaTable,
    delta: &SchemaDelta,
    ntz: NtzAs,
    props: CommitProperties,
) -> Result<(DeltaTable, EvolveOutcome), SinkError> {
    let schema = table.snapshot().map_err(sink_error)?.schema();
    let mut added: Vec<StructField> = Vec::new();
    let mut warnings = Vec::new();
    let has = |name: &str, added: &[StructField]| {
        schema
            .field(name)
            .cloned()
            .or_else(|| added.iter().find(|f| f.name() == name).cloned())
    };
    // A name of the table's, or one this delta adds, that `name` matches by case alone.
    let clash = |name: &str, added: &[StructField]| {
        schema
            .fields()
            .chain(added.iter())
            .find(|f| differ_by_case_alone(f.name(), name))
            .map(|f| f.name().clone())
    };
    let clashes = |name: &str, held: String| EvolveOutcome::Unsupported {
        reason: format!(
            "column {name:?} differs from the table's column {held:?} by case alone, and a \
             Delta table can't hold both"
        ),
    };
    for change in &delta.changes {
        match change {
            SchemaChange::AddColumn(column) => {
                if let Some(held) = clash(&column.name, &added) {
                    return Ok((table, clashes(&column.name, held)));
                }
                let field = added_column_field(column);
                match has(&column.name, &added) {
                    Some(existing) => {
                        let held = hop2_field(&existing).map_err(permanent)?;
                        if held.data_type() != field.data_type() {
                            return Ok((
                                table,
                                EvolveOutcome::Unsupported {
                                    reason: format!(
                                        "column {:?} exists as {}, the source added it as {}",
                                        column.name,
                                        held.data_type(),
                                        field.data_type()
                                    ),
                                },
                            ));
                        }
                    }
                    None => added.push(delta_field(&field, ntz).map_err(permanent)?),
                }
            }
            SchemaChange::DropColumn(name) => {
                warnings.push(format!("column {name:?} soft-dropped: kept, written NULL"));
            }
            SchemaChange::RenameColumn { from, to, .. } => {
                if let Some(held) = clash(to, &added) {
                    return Ok((table, clashes(to, held)));
                }
                let old = has(from, &added).ok_or_else(|| {
                    permanent(format!("renamed column {from:?} isn't in the table"))
                })?;
                if has(to, &added).is_none() {
                    added.push(
                        StructField::new(to, old.data_type().clone(), true)
                            .with_metadata(old.metadata().clone()),
                    );
                }
                warnings.push(format!(
                    "column {from:?} renamed to {to:?}: added {to:?}, kept {from:?} (history stays under the old name)"
                ));
            }
            SchemaChange::SetNotNull(_)
            | SchemaChange::DropNotNull(_)
            | SchemaChange::EnumValuesAppended(_)
            | SchemaChange::NoTargetEffect(_) => {}
            other => {
                return Ok((
                    table,
                    EvolveOutcome::Unsupported {
                        reason: format!("{other:?} isn't supported by the Delta sink"),
                    },
                ));
            }
        }
    }
    let table = if added.is_empty() {
        table
    } else {
        table
            .add_columns()
            .with_fields(added)
            .with_commit_properties(props)
            .await
            .map_err(sink_error)?
    };
    let outcome = if warnings.is_empty() {
        EvolveOutcome::Applied
    } else {
        EvolveOutcome::Degraded {
            warning: warnings.join("; "),
        }
    };
    Ok((table, outcome))
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableRef};

    use super::*;
    use crate::TableDefaults;
    use crate::table::TableHandle;
    use crate::test_support::{handle, orders, spec};

    async fn created(dir: &tempfile::TempDir) -> TableHandle {
        let mut h = handle(dir, &TableRef::new("public", "orders")).await;
        h.create(&spec(&orders()), &TableDefaults::default())
            .await
            .unwrap();
        h
    }

    fn delta(changes: Vec<SchemaChange>) -> SchemaDelta {
        SchemaDelta {
            table: TableRef::new("public", "orders"),
            changes,
        }
    }

    async fn apply(h: &mut TableHandle, changes: Vec<SchemaChange>) -> EvolveOutcome {
        let (table, outcome) = evolve(
            h.table().clone(),
            &delta(changes),
            NtzAs::default(),
            CommitProperties::default(),
        )
        .await
        .unwrap();
        h.set_table(table);
        outcome
    }

    #[tokio::test]
    async fn add_column_is_applied_nullable_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let add = || SchemaChange::AddColumn(ColumnSchema::new("tag", V::Uuid, "uuid").not_null());
        assert_eq!(apply(&mut h, vec![add()]).await, EvolveOutcome::Applied);
        let version = h.version();
        assert_eq!(
            apply(&mut h, vec![add()]).await,
            EvolveOutcome::Applied,
            "replay"
        );
        assert_eq!(h.version(), version, "a replay adds nothing");
        let s = h.schema().unwrap();
        let tag = s.field_with_name("tag").unwrap();
        assert!(tag.is_nullable(), "added columns are nullable (S-O)");
        assert_eq!(
            tag,
            &added_column_field(&ColumnSchema::new("tag", V::Uuid, "uuid"))
        );
    }

    #[tokio::test]
    async fn drop_and_rename_are_degraded_and_keep_the_old_column() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let out = apply(&mut h, vec![SchemaChange::DropColumn("doc".into())]).await;
        assert!(
            matches!(out, EvolveOutcome::Degraded { ref warning } if warning.contains("soft-dropped")),
            "{out:?}"
        );
        assert!(h.schema().unwrap().field_with_name("doc").is_ok());
        let rename = SchemaChange::RenameColumn {
            from: "ref".into(),
            to: "reference".into(),
            proven: true,
        };
        let out = apply(&mut h, vec![rename.clone()]).await;
        assert!(matches!(out, EvolveOutcome::Degraded { .. }), "{out:?}");
        let s = h.schema().unwrap();
        let old = s.field_with_name("ref").unwrap();
        let new = s.field_with_name("reference").unwrap();
        assert_eq!(new.data_type(), old.data_type());
        assert_eq!(new.metadata(), old.metadata(), "the uuid tag moves along");
        let version = h.version();
        apply(&mut h, vec![rename]).await;
        assert_eq!(h.version(), version, "a replayed rename adds nothing");
    }

    /// DA-SINK-12: delta-rs refuses a column beside one that differs by case alone, Unicode case
    /// too, so the evolver says so instead of retrying a commit that fails every time.
    #[tokio::test]
    async fn a_column_that_differs_from_one_by_case_alone_is_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let add = |name: &str| SchemaChange::AddColumn(ColumnSchema::new(name, V::Text, "text"));
        assert_eq!(
            apply(&mut h, vec![add("ärger")]).await,
            EvolveOutcome::Applied
        );
        let version = h.version();
        for change in [
            add("DOC"),
            add("ÄRGER"),
            SchemaChange::RenameColumn {
                from: "ref".into(),
                to: "Doc".into(),
                proven: true,
            },
            // Within one delta: the second spelling clashes with the first.
            add("Tag"),
        ] {
            let changes = match &change {
                SchemaChange::AddColumn(c) if c.name == "Tag" => vec![add("tag"), change],
                _ => vec![change],
            };
            let out = apply(&mut h, changes).await;
            assert!(
                matches!(&out, EvolveOutcome::Unsupported { reason } if reason.contains("by case alone")),
                "{out:?}"
            );
        }
        assert_eq!(h.version(), version, "nothing was written");
        // A rename of a column's case alone clashes with the column it renames, which the sink
        // keeps (a rename adds the new name and keeps the old one).
        let out = apply(
            &mut h,
            vec![SchemaChange::RenameColumn {
                from: "doc".into(),
                to: "DOC".into(),
                proven: true,
            }],
        )
        .await;
        assert!(matches!(out, EvolveOutcome::Unsupported { .. }), "{out:?}");
    }

    /// What the rule above follows, measured on the delta-rs this workspace links: its schema
    /// check folds names with Unicode lower-casing, not ASCII's.
    #[tokio::test]
    async fn delta_rs_refuses_a_column_beside_one_that_differs_by_unicode_case() {
        let dir = tempfile::tempdir().unwrap();
        let h = created(&dir).await;
        let field = |n: &str| StructField::new(n, deltalake::kernel::DataType::STRING, true);
        let table = h
            .table()
            .clone()
            .add_columns()
            .with_fields([field("ärger")])
            .await
            .unwrap();
        let refused = table.add_columns().with_fields([field("ÄRGER")]).await;
        assert!(
            refused.is_err(),
            "delta-rs took a column differing by Unicode case"
        );
    }

    #[tokio::test]
    async fn type_changes_and_structural_changes_are_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let widen = SchemaChange::ChangeType {
            column: "id".into(),
            from: ColumnSchema::new("id", V::Int32, "integer"),
            to: ColumnSchema::new("id", V::Int64, "bigint"),
            narrowed: false,
        };
        for change in [
            widen,
            SchemaChange::IdentifierChanged,
            SchemaChange::TableDropped,
        ] {
            let out = apply(&mut h, vec![change]).await;
            assert!(matches!(out, EvolveOutcome::Unsupported { .. }), "{out:?}");
        }
        let clash = SchemaChange::AddColumn(ColumnSchema::new("doc", V::Int64, "bigint"));
        assert!(matches!(
            apply(&mut h, vec![clash]).await,
            EvolveOutcome::Unsupported { .. }
        ));
        for nothing in [
            SchemaChange::SetNotNull("doc".into()),
            SchemaChange::DropNotNull("doc".into()),
            SchemaChange::EnumValuesAppended("doc".into()),
            SchemaChange::NoTargetEffect("index".into()),
        ] {
            assert_eq!(apply(&mut h, vec![nothing]).await, EvolveOutcome::Applied);
        }
        assert_eq!(h.version(), 0, "nothing was written");
    }
}
