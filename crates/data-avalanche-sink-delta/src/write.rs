//! The writes of one commit (Data-Flow §Delta Lake realization): the MERGE of a change set, the
//! DELETE of a truncate, and the marker-only commit that moves the position when nothing changed.

use std::ops::Not;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, new_null_array};
use arrow_schema::{DataType, Field, Schema};
use data_avalanche_core::{SinkError, TableChangeSet, UNCHANGED_MASK_PREFIX};
use deltalake::DeltaTable;
use deltalake::datafusion::common::{Column, ScalarValue};
use deltalake::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use deltalake::datafusion::logical_expr::{Expr, when};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaSessionContext;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{DataType as DeltaType, StructType};
use deltalake::operations::merge::MergeMetrics;
use deltalake::protocol::{DeltaOperation, OutputMode};

use crate::convert::to_delta;
use crate::error::permanent;
use crate::types::{column_logical, logical_of, physical_type};

/// The boolean MERGE-source column that marks a delete row.
pub const DELETE_FLAG: &str = "__data_avalanche_del";

fn source(name: &str) -> Expr {
    Expr::Column(Column::new(Some("source"), name))
}

fn target(name: &str) -> Expr {
    Expr::Column(Column::new(Some("target"), name))
}

fn mask_name(column: &str) -> String {
    format!("{UNCHANGED_MASK_PREFIX}{column}")
}

/// Checks the change set against the table: every column of the set is a column of the table
/// holding the same Hop 2 type (the set may lack columns: soft-dropped, or added after it). Returns
/// each set column's Delta type.
pub fn check_columns(cs: &TableChangeSet, table: &StructType) -> Result<Vec<DeltaType>, SinkError> {
    cs.schema
        .fields()
        .iter()
        .map(|f| {
            let column = table.field(f.name()).ok_or_else(|| {
                permanent(format!(
                    "column {:?} isn't in table {} (evolve first)",
                    f.name(),
                    cs.table
                ))
            })?;
            let logical = logical_of(f).map_err(permanent)?;
            let held = column_logical(column).map_err(permanent)?;
            if data_avalanche_core::arrow_type(&held) != *f.data_type() {
                return Err(permanent(format!(
                    "column {:?} of {} holds {held}, the change set has {logical}",
                    f.name(),
                    cs.table
                )));
            }
            Ok(column.data_type().clone())
        })
        .collect()
}

/// The MERGE source: the upserts, then the deletes, in the table's physical types, each row with
/// [`DELETE_FLAG`] and the upserts' unchanged masks (`false` on delete rows). Every field is
/// nullable; a delete row carries only its identifier.
pub fn merge_source(
    cs: &TableChangeSet,
    types: &[DeltaType],
) -> Result<Vec<RecordBatch>, SinkError> {
    let masks: Vec<String> = cs
        .upserts
        .first()
        .map(|b| {
            b.schema().fields()[cs.schema.fields().len()..]
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let mut fields = Vec::new();
    for (f, t) in cs.schema.fields().iter().zip(types) {
        fields.push(Field::new(
            f.name(),
            physical_type(t).map_err(permanent)?,
            true,
        ));
    }
    fields.extend(
        masks
            .iter()
            .map(|m| Field::new(m, DataType::Boolean, false)),
    );
    fields.push(Field::new(DELETE_FLAG, DataType::Boolean, false));
    let schema = Arc::new(Schema::new(fields));
    let mut batches = Vec::new();
    for b in &cs.upserts {
        let mut columns = Vec::new();
        for (i, (f, t)) in cs.schema.fields().iter().zip(types).enumerate() {
            let logical = logical_of(f).map_err(permanent)?;
            columns.push(to_delta(b.column(i), &logical, t).map_err(permanent)?);
        }
        for m in &masks {
            columns.push(b.column_by_name(m).expect("validated masks").clone());
        }
        columns.push(Arc::new(BooleanArray::from(vec![false; b.num_rows()])));
        batches.push(
            RecordBatch::try_new(schema.clone(), columns).map_err(|e| permanent(e.to_string()))?,
        );
    }
    for b in &cs.deletes {
        let rows = b.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (f, t) in cs.schema.fields().iter().zip(types) {
            let physical = physical_type(t).map_err(permanent)?;
            columns.push(match cs.identifier.iter().position(|k| k == f.name()) {
                Some(k) => {
                    let logical = logical_of(f).map_err(permanent)?;
                    to_delta(b.column(k), &logical, t).map_err(permanent)?
                }
                None => new_null_array(&physical, rows),
            });
        }
        for _ in &masks {
            columns.push(Arc::new(BooleanArray::from(vec![false; rows])));
        }
        columns.push(Arc::new(BooleanArray::from(vec![true; rows])));
        batches.push(
            RecordBatch::try_new(schema.clone(), columns).map_err(|e| permanent(e.to_string()))?,
        );
    }
    Ok(batches)
}

/// delta-rs's own DataFusion session (its planner and tuned settings, the session the probes ran
/// on), with a memory ceiling when one is set (R-6).
pub fn session(memory_limit: Option<usize>) -> Result<SessionContext, SinkError> {
    let Some(limit) = memory_limit else {
        return Ok(DeltaSessionContext::new().into_inner());
    };
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(limit, 1.0)
        .build_arc()
        .map_err(|e| permanent(e.to_string()))?;
    Ok(DeltaSessionContext::with_runtime_env(runtime).into_inner())
}

/// A deterministic failure building the MERGE: `InvalidData`, which `sink_error` classes
/// `Permanent`, so the pipeline halts the table instead of retrying it.
fn invalid(e: impl std::fmt::Display) -> deltalake::DeltaTableError {
    deltalake::DeltaTableError::InvalidData {
        message: e.to_string(),
    }
}

/// One MERGE of the change set's rows (Data-Flow §Delta Lake realization):
/// - matched on the identifier only (a row that moved partitions is still matched);
/// - `WHEN MATCHED AND del THEN DELETE`;
/// - `WHEN MATCHED AND NOT del THEN UPDATE`: a masked column keeps the target's value
///   (`CASE WHEN mask THEN target ELSE source`, D-IMPL-3), a table column the set lacks is NULL;
/// - `WHEN NOT MATCHED AND NOT del THEN INSERT` every table column, NULL where the set has none.
///
/// A MERGE that changes no row writes no commit; the caller checks the version.
pub async fn merge(
    table: DeltaTable,
    cs: &TableChangeSet,
    types: &[DeltaType],
    props: CommitProperties,
    memory_limit: Option<usize>,
) -> Result<(DeltaTable, MergeMetrics), deltalake::DeltaTableError> {
    let batches = merge_source(cs, types).map_err(invalid)?;
    let ctx = session(memory_limit).map_err(invalid)?;
    let df = ctx.read_batches(batches)?;
    let columns: Vec<(String, DeltaType)> = table
        .snapshot()?
        .schema()
        .fields()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    let in_set = |name: &str| cs.schema.field_with_name(name).is_ok();
    let masked = |name: &str| {
        cs.upserts
            .first()
            .is_some_and(|b| b.column_by_name(&mask_name(name)).is_some())
    };
    let null = |t: &DeltaType| -> Result<Expr, deltalake::DeltaTableError> {
        let physical = physical_type(t).map_err(invalid)?;
        Ok(Expr::Literal(ScalarValue::try_from(&physical)?, None))
    };
    let mut on: Option<Expr> = None;
    for k in &cs.identifier {
        let eq = target(k).eq(source(k));
        on = Some(match on {
            Some(prev) => prev.and(eq),
            None => eq,
        });
    }
    let on = on.expect("a change set has an identifier");
    let mut updates = Vec::new();
    let mut inserts = Vec::new();
    for (name, t) in &columns {
        let value = if !in_set(name) {
            null(t)?
        } else {
            source(name)
        };
        if !cs.identifier.contains(name) {
            let update = if in_set(name) && masked(name) {
                when(source(&mask_name(name)), target(name)).otherwise(source(name))?
            } else {
                value.clone()
            };
            updates.push((Column::new_unqualified(name), update));
        }
        inserts.push((Column::new_unqualified(name), value));
    }
    let delete = source(DELETE_FLAG);
    let state = Arc::new(ctx.state());
    table
        .merge(df, on)
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(state)
        .with_commit_properties(props)
        .when_matched_delete(|d| d.predicate(delete.clone()))?
        .when_matched_update(|mut u| {
            u = u.predicate(delete.clone().not());
            for (c, e) in updates {
                u = u.update(c, e);
            }
            u
        })?
        .when_not_matched_insert(|mut i| {
            i = i.predicate(delete.clone().not());
            for (c, e) in inserts {
                i = i.set(c, e);
            }
            i
        })?
        .await
}

/// Removes every row (a TRUNCATE): metadata-only, one commit.
pub async fn truncate(
    table: DeltaTable,
    props: CommitProperties,
) -> Result<DeltaTable, deltalake::DeltaTableError> {
    let (table, _) = table.delete().with_commit_properties(props).await?;
    Ok(table)
}

/// A commit with no data: just the `txn` and the `commitInfo` in `props`. It moves the table's
/// marker when the change set changed no row.
pub async fn marker_only(
    table: &DeltaTable,
    props: CommitProperties,
    app_id: &str,
    epoch: i64,
) -> Result<u64, deltalake::DeltaTableError> {
    let operation = DeltaOperation::StreamingUpdate {
        output_mode: OutputMode::Append,
        query_id: app_id.to_string(),
        epoch_id: epoch,
    };
    let done = CommitBuilder::from(props)
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .await?;
    Ok(done.version())
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{SourcePosition, TableRef, upsert_schema};
    use deltalake::kernel::Transaction;

    use super::*;
    use crate::TableDefaults;
    use crate::marker::{app_id, commit_metadata};
    use crate::table::TableHandle;
    use crate::test_support::{Row, change_set, deletes, handle, orders, spec, upserts};

    fn props(epoch: u64, lsn: u64) -> CommitProperties {
        CommitProperties::default()
            .with_application_transaction(Transaction::new(app_id("p"), epoch as i64))
            .with_metadata(commit_metadata(
                "p",
                epoch,
                Some(&SourcePosition::Pg(Lsn(lsn))),
            ))
    }

    async fn created(dir: &tempfile::TempDir) -> TableHandle {
        let mut h = handle(dir, &TableRef::new("public", "orders")).await;
        h.create(&spec(&orders()), &TableDefaults::default())
            .await
            .unwrap();
        h
    }

    /// Commits `cs` by MERGE, returning the metrics.
    async fn merged(h: &mut TableHandle, cs: &TableChangeSet, lsn: u64) -> MergeMetrics {
        let snapshot = h.table().snapshot().unwrap();
        let types = check_columns(cs, snapshot.schema().as_ref()).unwrap();
        let (table, metrics) = merge(h.table().clone(), cs, &types, props(1, lsn), None)
            .await
            .unwrap();
        h.set_table(table);
        metrics
    }

    async fn rows(h: &TableHandle) -> Vec<(i64, Option<String>)> {
        let c = h.content().await.unwrap();
        let ids = c.column(0).as_primitive::<Int64Type>();
        let docs = c.column(1).as_string::<i32>();
        (0..c.num_rows())
            .map(|i| {
                (
                    ids.value(i),
                    docs.is_valid(i).then(|| docs.value(i).to_string()),
                )
            })
            .collect()
    }

    fn owned(rows: &[Row]) -> Vec<(i64, Option<String>)> {
        rows.iter()
            .map(|(i, d)| (*i, d.map(String::from)))
            .collect()
    }

    fn full() -> arrow_schema::SchemaRef {
        upsert_schema(&orders())
    }

    #[tokio::test]
    async fn updates_deletes_and_inserts_land_in_one_version() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let seed = change_set(
            10,
            vec![upserts(
                &full(),
                &[(1, Some("a")), (2, Some("b")), (3, Some("c"))],
                None,
            )],
            vec![],
        );
        merged(&mut h, &seed, 10).await;
        let before = h.version();
        let cs = change_set(
            20,
            vec![upserts(&full(), &[(1, Some("A")), (4, Some("d"))], None)],
            vec![deletes(&[2])],
        );
        let m = merged(&mut h, &cs, 20).await;
        assert_eq!(h.version(), before + 1, "one new version");
        assert_eq!(
            (
                m.num_target_rows_updated,
                m.num_target_rows_deleted,
                m.num_target_rows_inserted
            ),
            (1, 1, 1)
        );
        assert_eq!(
            rows(&h).await,
            owned(&[(1, Some("A")), (3, Some("c")), (4, Some("d"))])
        );
        // Replaying the same set gives the same table (probe P2).
        merged(&mut h, &cs, 20).await;
        assert_eq!(
            rows(&h).await,
            owned(&[(1, Some("A")), (3, Some("c")), (4, Some("d"))])
        );
    }

    #[tokio::test]
    async fn a_masked_cell_keeps_the_stored_value_and_a_new_row_gets_null() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(
                10,
                vec![upserts(&full(), &[(1, Some("big"))], None)],
                vec![],
            ),
            10,
        )
        .await;
        let masked = upserts(
            &full(),
            &[(1, None), (2, None), (3, Some("x"))],
            Some(&[true, true, false]),
        );
        merged(&mut h, &change_set(20, vec![masked], vec![]), 20).await;
        assert_eq!(
            rows(&h).await,
            owned(&[(1, Some("big")), (2, None), (3, Some("x"))])
        );
    }

    #[tokio::test]
    async fn a_table_column_the_set_lacks_is_written_null() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(10, vec![upserts(&full(), &[(1, Some("a"))], None)], vec![]),
            10,
        )
        .await;
        // A set without `doc` (a soft-dropped column): the update and the insert write NULL.
        let fields: Vec<_> = full()
            .fields()
            .iter()
            .filter(|f| f.name() != "doc")
            .cloned()
            .collect();
        let narrow = Arc::new(Schema::new(fields));
        let mut cs = change_set(
            20,
            vec![upserts(&narrow, &[(1, None), (2, None)], None)],
            vec![],
        );
        cs.schema = narrow;
        merged(&mut h, &cs, 20).await;
        assert_eq!(rows(&h).await, owned(&[(1, None), (2, None)]));
    }

    #[tokio::test]
    async fn a_set_column_the_table_lacks_or_types_differently_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let h = created(&dir).await;
        let snapshot = h.table().snapshot().unwrap();
        let mut cs = change_set(10, vec![], vec![]);
        let mut fields = cs.schema.fields().to_vec();
        fields.push(Arc::new(Field::new("extra", DataType::Int32, true)));
        cs.schema = Arc::new(Schema::new(fields));
        let err = check_columns(&cs, snapshot.schema().as_ref()).unwrap_err();
        assert!(err.to_string().contains("evolve first"), "{err}");
        let mut cs = change_set(10, vec![], vec![]);
        let mut fields = cs.schema.fields().to_vec();
        fields[1] = Arc::new(Field::new("doc", DataType::Int32, true));
        cs.schema = Arc::new(Schema::new(fields));
        let err = check_columns(&cs, snapshot.schema().as_ref()).unwrap_err();
        assert!(err.to_string().contains("holds string"), "{err}");
    }

    #[tokio::test]
    async fn a_merge_that_changes_nothing_commits_nothing_so_the_marker_goes_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(10, vec![upserts(&full(), &[(1, Some("a"))], None)], vec![]),
            10,
        )
        .await;
        let before = h.version();
        merged(&mut h, &change_set(20, vec![], vec![deletes(&[42])]), 20).await;
        assert_eq!(
            h.version(),
            before,
            "deleting a missing key writes no version"
        );
        let v = marker_only(h.table(), props(2, 20), &app_id("p"), 2)
            .await
            .unwrap();
        assert_eq!(v, before + 1);
        assert!(h.refresh().await.unwrap());
        assert_eq!(h.fence_epoch("p").await.unwrap(), Some(2));
        assert_eq!(
            h.stored_position("p").await.unwrap(),
            Some(SourcePosition::Pg(Lsn(20)))
        );
    }

    #[tokio::test]
    async fn truncate_removes_every_row_without_rewriting() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(
                10,
                vec![upserts(&full(), &[(1, Some("a")), (2, Some("b"))], None)],
                vec![],
            ),
            10,
        )
        .await;
        let files_before = h.table().snapshot().unwrap().log_data().num_files();
        assert!(files_before > 0);
        let table = truncate(h.table().clone(), props(1, 20)).await.unwrap();
        h.set_table(table);
        assert_eq!(rows(&h).await, vec![]);
        let commit = h
            .table()
            .log_store()
            .read_commit_entry(h.version())
            .await
            .unwrap()
            .unwrap();
        let text = String::from_utf8(commit.to_vec()).unwrap();
        assert!(text.contains(r#""remove""#), "{text}");
        assert!(!text.contains(r#""add""#), "metadata-only: {text}");
        merged(
            &mut h,
            &change_set(30, vec![upserts(&full(), &[(3, Some("c"))], None)], vec![]),
            30,
        )
        .await;
        assert_eq!(rows(&h).await, owned(&[(3, Some("c"))]));
    }

    #[tokio::test]
    async fn a_row_that_moves_partition_is_matched_by_key_not_duplicated() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = handle(&dir, &TableRef::new("public", "orders")).await;
        let mut s = spec(&orders());
        s.partition_by = vec![data_avalanche_core::PartitionField {
            column: "doc".into(),
            transform: data_avalanche_core::PartitionTransform::Identity,
        }];
        h.create(&s, &TableDefaults::default()).await.unwrap();
        let seed = vec![upserts(&full(), &[(1, Some("a")), (2, Some("a"))], None)];
        merged(&mut h, &change_set(10, seed, vec![]), 10).await;
        // Row 1 moves from partition `a` to `b`; row 2 is deleted by its key alone.
        let cs = change_set(
            20,
            vec![upserts(&full(), &[(1, Some("b"))], None)],
            vec![deletes(&[2])],
        );
        let m = merged(&mut h, &cs, 20).await;
        assert_eq!(
            (m.num_target_rows_updated, m.num_target_rows_deleted),
            (1, 1)
        );
        assert_eq!(rows(&h).await, owned(&[(1, Some("b"))]));
    }

    #[tokio::test]
    async fn a_merge_over_the_memory_limit_fails_instead_of_growing() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let many: Vec<Row> = (0..20_000)
            .map(|i| (i, Some("xxxxxxxxxxxxxxxxxxxxxxxx")))
            .collect();
        let cs = change_set(10, vec![upserts(&full(), &many, None)], vec![]);
        merged(&mut h, &cs, 10).await;
        let snapshot = h.table().snapshot().unwrap();
        let types = check_columns(&cs, snapshot.schema().as_ref()).unwrap();
        let err = merge(h.table().clone(), &cs, &types, props(1, 20), Some(64 << 10))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Resources exhausted"), "{err}");
        merge(h.table().clone(), &cs, &types, props(1, 20), Some(1 << 30))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_change_set_the_merge_cant_convert_is_permanent() {
        let dir = tempfile::tempdir().unwrap();
        let h = created(&dir).await;
        let cs = change_set(10, vec![upserts(&full(), &[(1, Some("a"))], None)], vec![]);
        // Delta types that don't fit the set's columns: the source can't be built.
        let wrong = vec![
            DeltaType::Primitive(deltalake::kernel::PrimitiveType::Integer);
            cs.schema.fields().len()
        ];
        let err = merge(h.table().clone(), &cs, &wrong, props(1, 10), None)
            .await
            .unwrap_err();
        let err = crate::error::sink_error(err);
        assert!(matches!(err, SinkError::Permanent { .. }), "{err}");
    }
}
