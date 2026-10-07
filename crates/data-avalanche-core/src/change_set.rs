use std::collections::HashSet;

use arrow_array::{Array, RecordBatch};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, SchemaRef};
use thiserror::Error;

use crate::{SourcePosition, TableRef, UNCHANGED_MASK_PREFIX};

/// The stream range a flush covers: changes after `start` (the previous checkpoint) up to and
/// including `end`. `end` is what the sink records as the table's `committed_position` and what
/// the position guard compares (Delivery §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionRange {
    pub start: SourcePosition,
    pub end: SourcePosition,
}

/// Marks a slice of an oversized transaction flushed before the source commit (split commits,
/// Large_Transaction §1). A slice's range ends at `txn_start`: the table's marker stays pinned there
/// until the source commit, so a crash replays the whole transaction and the slices re-apply as
/// idempotent upserts. What the transaction writes after its last slice goes out with the next
/// ordinary flush, whose range ends at the commit. The sink's position guard treats slices specially
/// ([`crate::CommitMarker::covers`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitInfo {
    pub txn_start: SourcePosition,
    /// 1 for the first slice of the transaction.
    pub slice: u32,
}

/// One table's collapsed changes for one flush: the sink-neutral boundary type (Data-Flow Phase 3,
/// Multi_Sink §6.2).
///
/// Invariants ([`TableChangeSet::validate`]):
/// - `identifier` names at least one column (a table without a key halts before it gets here,
///   Ghost_Row_Dedup §3);
/// - every upsert batch has `schema`'s fields, then zero or more unchanged-mask columns
///   ([`UNCHANGED_MASK_PREFIX`]), the same for every batch;
/// - every delete batch starts with the identifier columns;
/// - a key appears at most once across `upserts` and `deletes` (a PK-changing UPDATE is two keys);
/// - identifier values are never NULL;
/// - the range doesn't run backwards, and a slice's range ends at its `txn_start`;
/// - a snapshot set only upserts: no deletes, no truncate, not a slice.
#[derive(Debug, Clone)]
pub struct TableChangeSet {
    pub table: TableRef,
    /// The canonical upsert schema ([`crate::upsert_schema`]).
    pub schema: SchemaRef,
    pub identifier: Vec<String>,
    /// Clear the table before applying `upserts` and `deletes` (TRUNCATE barrier).
    pub truncate: bool,
    pub upserts: Vec<RecordBatch>,
    pub deletes: Vec<RecordBatch>,
    pub position: PositionRange,
    pub split: Option<SplitInfo>,
    /// Snapshot chunk rows, read from the source now rather than replayed from the stream: the
    /// position guard never skips them ([`crate::CommitMarker::covers`]). A chunk is emitted at
    /// the stream's frontier, which the table's marker may already cover (Q-M6a-6).
    pub snapshot: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid change set for {table}: {detail}")]
pub struct ChangeSetError {
    pub table: TableRef,
    pub detail: String,
}

impl TableChangeSet {
    pub fn upsert_rows(&self) -> usize {
        self.upserts.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn delete_rows(&self) -> usize {
        self.deletes.iter().map(RecordBatch::num_rows).sum()
    }

    /// No rows and no truncate: committing it only moves the table's marker.
    pub fn is_empty(&self) -> bool {
        !self.truncate && self.upsert_rows() == 0 && self.delete_rows() == 0
    }

    /// Checks the invariants listed on the type.
    pub fn validate(&self) -> Result<(), ChangeSetError> {
        let err = |detail: String| ChangeSetError {
            table: self.table.clone(),
            detail,
        };
        if self.identifier.is_empty() {
            return Err(err("a change set with no identifier columns".into()));
        }
        let canonical = self.schema.fields();
        let mut upsert_extra: Option<Vec<String>> = None;
        for b in &self.upserts {
            let fields = b.schema().fields().clone();
            if fields.len() < canonical.len() || fields[..canonical.len()] != canonical[..] {
                return Err(err(
                    "an upsert batch doesn't start with the table's schema".into()
                ));
            }
            let extra: Vec<String> = fields[canonical.len()..]
                .iter()
                .map(|f| f.name().clone())
                .collect();
            for f in &fields[canonical.len()..] {
                let column = f.name().strip_prefix(UNCHANGED_MASK_PREFIX);
                let known = column.is_some_and(|c| {
                    canonical.iter().any(|k| k.name() == c)
                        && !self.identifier.iter().any(|i| i == c)
                });
                if !known || f.data_type() != &DataType::Boolean || f.is_nullable() {
                    return Err(err(format!(
                        "upsert batch column {:?} is neither in the schema nor an unchanged mask of a non-identifier column",
                        f.name()
                    )));
                }
            }
            match &upsert_extra {
                Some(prev) if prev != &extra => {
                    return Err(err("upsert batches carry different unchanged masks".into()));
                }
                _ => upsert_extra = Some(extra),
            }
        }
        for b in &self.deletes {
            let schema = b.schema();
            let names: Vec<&str> = schema
                .fields()
                .iter()
                .take(self.identifier.len())
                .map(|f| f.name().as_str())
                .collect();
            if names != self.identifier {
                return Err(err(
                    "a delete batch doesn't start with the identifier columns".into(),
                ));
            }
        }
        let (start, end) = (&self.position.start, &self.position.end);
        match end.covers(start) {
            Ok(true) => {}
            Ok(false) => {
                return Err(err(format!(
                    "position range ends at {end} before it starts at {start}"
                )));
            }
            Err(e) => {
                return Err(err(format!(
                    "position range from {start} to {end} doesn't compare: {e}"
                )));
            }
        }
        if self.snapshot && (self.truncate || self.split.is_some() || self.delete_rows() > 0) {
            return Err(err(
                "a snapshot set carries upserts only: no deletes, truncate or slice".into(),
            ));
        }
        if let Some(split) = &self.split
            && self.position.end != split.txn_start
        {
            return Err(err(format!(
                "a slice ends at {} instead of its transaction start {}",
                self.position.end, split.txn_start
            )));
        }
        self.check_keys().map_err(err)
    }

    fn check_keys(&self) -> Result<(), String> {
        let key_types: Vec<SortField> = self
            .identifier
            .iter()
            .map(|name| {
                self.schema
                    .field_with_name(name)
                    .map(|f| SortField::new(f.data_type().clone()))
                    .map_err(|_| format!("identifier column {name:?} isn't in the schema"))
            })
            .collect::<Result<_, _>>()?;
        let converter = RowConverter::new(key_types).map_err(|e| e.to_string())?;
        let mut seen = HashSet::new();
        for (kind, batches) in [("upserts", &self.upserts), ("deletes", &self.deletes)] {
            for b in batches {
                let mut columns = Vec::with_capacity(self.identifier.len());
                for name in &self.identifier {
                    let c = b
                        .column_by_name(name)
                        .ok_or_else(|| format!("{kind} batch lacks identifier column {name:?}"))?;
                    if c.null_count() > 0 {
                        return Err(format!(
                            "{kind} batch has a NULL in identifier column {name:?}"
                        ));
                    }
                    columns.push(c.clone());
                }
                let rows = converter
                    .convert_columns(&columns)
                    .map_err(|e| e.to_string())?;
                for row in rows.iter() {
                    if !seen.insert(row.owned()) {
                        return Err(format!("a key appears more than once (in {kind})"));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::position::Lsn;
    use crate::{SourcePosition, TableRef};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("doc", DataType::Utf8, true),
        ]))
    }

    fn upserts(ids: &[i64], mask: Option<&[bool]>) -> RecordBatch {
        let mut fields = schema().fields().to_vec();
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![Some("x"); ids.len()])),
        ];
        if let Some(m) = mask {
            fields.push(Arc::new(crate::unchanged_mask_field("doc")));
            cols.push(Arc::new(BooleanArray::from(m.to_vec())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn deletes(ids: &[Option<i64>]) -> RecordBatch {
        let s = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        RecordBatch::try_new(s, vec![Arc::new(Int64Array::from(ids.to_vec()))]).unwrap()
    }

    fn cs(up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
        TableChangeSet {
            table: TableRef::new("public", "t"),
            schema: schema(),
            identifier: vec!["id".into()],
            truncate: false,
            upserts: up,
            deletes: del,
            position: PositionRange {
                start: SourcePosition::Pg(Lsn(10)),
                end: SourcePosition::Pg(Lsn(20)),
            },
            split: None,
            snapshot: false,
        }
    }

    fn detail(c: TableChangeSet) -> String {
        c.validate().unwrap_err().detail
    }

    #[test]
    fn a_well_formed_set_validates() {
        let c = cs(
            vec![
                upserts(&[1, 2], Some(&[false, true])),
                upserts(&[3], Some(&[false])),
            ],
            vec![deletes(&[Some(4)])],
        );
        c.validate().unwrap();
        assert_eq!((c.upsert_rows(), c.delete_rows()), (3, 1));
        assert!(!c.is_empty());
        assert!(cs(vec![], vec![]).is_empty());
    }

    #[test]
    fn a_key_appears_at_most_once() {
        let d = detail(cs(vec![upserts(&[1], None)], vec![deletes(&[Some(1)])]));
        assert!(d.contains("more than once"), "{d}");
        let d = detail(cs(vec![upserts(&[1], None), upserts(&[1], None)], vec![]));
        assert!(d.contains("more than once"), "{d}");
    }

    #[test]
    fn identifier_values_are_never_null() {
        let d = detail(cs(vec![], vec![deletes(&[None])]));
        assert!(d.contains("NULL in identifier column \"id\""), "{d}");
    }

    #[test]
    fn a_change_set_has_an_identifier() {
        let mut c = cs(vec![upserts(&[1], None)], vec![]);
        c.identifier.clear();
        assert!(detail(c).contains("no identifier columns"));
    }

    #[test]
    fn batch_schemas_are_checked() {
        let other = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let wrong =
            RecordBatch::try_new(other.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(
            detail(cs(vec![wrong.clone()], vec![]))
                .contains("doesn't start with the table's schema")
        );
        assert!(detail(cs(vec![], vec![wrong])).contains("doesn't start with the identifier"));
        let mixed = cs(
            vec![upserts(&[1], Some(&[true])), upserts(&[2], None)],
            vec![],
        );
        assert!(detail(mixed).contains("different unchanged masks"));
    }

    #[test]
    fn a_slice_ends_at_its_transaction_start() {
        let mut c = cs(vec![upserts(&[1], None)], vec![]);
        c.split = Some(SplitInfo {
            txn_start: SourcePosition::Pg(Lsn(20)),
            slice: 1,
        });
        c.validate().unwrap();
        c.position.end = SourcePosition::Pg(Lsn(30));
        assert!(detail(c).contains("instead of its transaction start"));
    }

    #[test]
    fn a_snapshot_set_only_upserts() {
        let mut c = cs(vec![upserts(&[1], None)], vec![]);
        c.snapshot = true;
        c.validate().unwrap();
        let refused = |f: fn(&mut TableChangeSet)| {
            let mut c = cs(vec![upserts(&[1], None)], vec![]);
            c.snapshot = true;
            f(&mut c);
            detail(c)
        };
        for d in [
            refused(|c| c.deletes = vec![deletes(&[Some(2)])]),
            refused(|c| c.truncate = true),
            refused(|c| {
                c.split = Some(SplitInfo {
                    txn_start: SourcePosition::Pg(Lsn(20)),
                    slice: 1,
                });
            }),
        ] {
            assert!(d.contains("upserts only"), "{d}");
        }
    }

    #[test]
    fn the_range_must_not_run_backwards() {
        let mut c = cs(vec![], vec![]);
        c.position.end = SourcePosition::Pg(Lsn(5));
        assert!(detail(c).contains("before it starts"));
    }

    #[test]
    fn a_range_across_dialects_names_both() {
        let mut c = cs(vec![], vec![]);
        c.position.end = SourcePosition::parse_tagged("binlog:mysql-bin.000001:4").unwrap();
        let d = detail(c);
        assert!(!d.contains("before it starts"), "{d}");
        assert!(
            d.contains("cannot combine a binlog position with a pg position"),
            "{d}"
        );
    }
}
