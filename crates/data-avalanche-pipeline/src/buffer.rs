//! One table's buffer: change events collapsed by identifier on arrival (Ghost_Row §4), in two
//! layers — the transaction still open at the source, and the transactions already committed.

use std::cmp::Ordering;
use std::collections::HashMap;

use data_avalanche_core::{
    CdcEvent, ColumnValue, ColumnValueType, HaltReason, LogicalType, Op, Row, SourcePosition,
    SourceSeq, TableSchema,
};

use crate::AccumulatorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Upsert,
    Delete,
}

/// The latest image of one key.
#[derive(Debug, Clone, PartialEq)]
pub struct CollapsedRow {
    pub kind: RowKind,
    /// One value per table column, in schema order. An upsert may hold
    /// [`ColumnValue::Unchanged`] (the target keeps its value); a delete holds the identifier and the
    /// buffer's extra delete columns, NULL elsewhere.
    pub values: Vec<ColumnValue>,
    pub op: Op,
    pub commit_ts_micros: i64,
    /// Commit position of the event's transaction (the `_lsn` lineage column).
    pub position: SourcePosition,
    pub seq: SourceSeq,
}

/// A key: the identifier values, compared exactly. Identifier columns are never float, array or
/// JSON ([`TableSchema::new`]), and decimals are rescaled to the column's scale on the way in, so
/// equal values have one representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key(Vec<KeyPart>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyPart {
    Bool(bool),
    Int(i128),
    Text(String),
    Bytes(Vec<u8>),
}

fn key_part(v: &ColumnValue) -> Option<KeyPart> {
    use ColumnValue as V;
    Some(match v {
        V::Bool(b) => KeyPart::Bool(*b),
        V::Int16(i) => KeyPart::Int(i128::from(*i)),
        V::Int32(i) => KeyPart::Int(i128::from(*i)),
        V::Int64(i) => KeyPart::Int(i128::from(*i)),
        V::UInt64(i) => KeyPart::Int(i128::from(*i)),
        V::Numeric(d) => KeyPart::Int(d.unscaled()),
        V::Timestamp { micros, .. } => KeyPart::Int(i128::from(*micros)),
        V::Date(d) => KeyPart::Int(i128::from(*d)),
        V::Time(t) => KeyPart::Int(i128::from(*t)),
        V::Text(s) => KeyPart::Text(s.clone()),
        V::Bytes(b) => KeyPart::Bytes(b.clone()),
        V::Uuid(u) => KeyPart::Bytes(u.as_bytes().to_vec()),
        V::Null | V::Unchanged | V::Float32(_) | V::Float64(_) | V::Json(_) | V::Array { .. } => {
            return None;
        }
    })
}

/// Rough in-memory size of a row, for the flush `bytes` trigger and the transaction buffer ceiling.
fn row_bytes(values: &[ColumnValue]) -> usize {
    fn value_bytes(v: &ColumnValue) -> usize {
        let payload = match v {
            ColumnValue::Text(s) => s.len(),
            ColumnValue::Bytes(b) => b.len(),
            ColumnValue::Json(j) => j.to_string().len(),
            ColumnValue::Array { values, .. } => values.iter().map(value_bytes).sum(),
            _ => 0,
        };
        size_of::<ColumnValue>() + payload
    }
    values.iter().map(value_bytes).sum()
}

/// Conform a source value to its column: the type must match (a mismatch means the buffer's schema
/// is stale), and decimals are rescaled to the column's Arrow scale (Hop 2).
fn conform_value(
    v: ColumnValue,
    t: &ColumnValueType,
    logical: &LogicalType,
) -> Result<ColumnValue, Misfit> {
    match (v, t, logical) {
        (
            ColumnValue::Numeric(d),
            ColumnValueType::Numeric { .. },
            LogicalType::Decimal { precision, scale },
        ) => match d.rescale(*scale) {
            Ok(r) if r.precision() <= *precision => Ok(ColumnValue::Numeric(r)),
            _ => Err(Misfit::Overflow),
        },
        (
            ColumnValue::Array {
                element_type,
                values,
            },
            ColumnValueType::Array(et),
            LogicalType::List(el),
        ) if element_type == **et => {
            let values = values
                .into_iter()
                .map(|e| conform_value(e, et, el))
                .collect::<Result<_, _>>()?;
            Ok(ColumnValue::Array {
                element_type,
                values,
            })
        }
        (v, t, _) if v.fits(t) => Ok(v),
        (v, t, _) => Err(Misfit::Type(format!(
            "a value {v:?} doesn't fit the column type {t:?}"
        ))),
    }
}

/// Why a value doesn't conform to its column.
enum Misfit {
    /// A decimal with more digits than the column holds.
    Overflow,
    Type(String),
}

/// Which columns an image must carry.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Image {
    /// An after-image: every column.
    After,
    /// A before-image: PostgreSQL's default replica identity sends only the key, so a missing
    /// column is NULL.
    Before,
}

/// One table's buffer. The accumulator applies events to the open layer, moves it to the
/// committed layer at the source commit, and takes the committed layer at a flush.
#[derive(Debug)]
pub struct TableBuffer {
    schema: TableSchema,
    identifier: Vec<usize>,
    /// Column indices a delete carries: the identifier, then the extra delete columns.
    delete_columns: Vec<usize>,
    open: HashMap<Key, CollapsedRow>,
    committed: HashMap<Key, CollapsedRow>,
    open_bytes: usize,
    committed_bytes: usize,
    open_events: u64,
    committed_events: u64,
}

impl TableBuffer {
    /// `delete_extra` names the columns a delete carries besides the identifier (the partition
    /// columns a sink prunes by).
    pub fn new(schema: TableSchema, delete_extra: &[String]) -> Result<Self, HaltReason> {
        let index = |name: &String| {
            schema
                .index_of(name)
                .ok_or_else(|| HaltReason::SchemaMismatch {
                    column: name.clone(),
                    detail: "a delete column that isn't in the table".into(),
                })
        };
        let identifier = schema
            .identifier()
            .iter()
            .map(index)
            .collect::<Result<Vec<_>, _>>()?;
        let mut delete_columns = identifier.clone();
        for name in delete_extra {
            let i = index(name)?;
            if !delete_columns.contains(&i) {
                delete_columns.push(i);
            }
        }
        Ok(Self {
            schema,
            identifier,
            delete_columns,
            open: HashMap::new(),
            committed: HashMap::new(),
            open_bytes: 0,
            committed_bytes: 0,
            open_events: 0,
            committed_events: 0,
        })
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Column indices a delete row carries, identifier first.
    pub fn delete_columns(&self) -> &[usize] {
        &self.delete_columns
    }

    pub fn open_bytes(&self) -> usize {
        self.open_bytes
    }

    pub fn committed_bytes(&self) -> usize {
        self.committed_bytes
    }

    /// Events applied since the last flush, not collapsed rows (the flush `rows` trigger counts
    /// events).
    pub fn committed_events(&self) -> u64 {
        self.committed_events
    }

    pub fn open_events(&self) -> u64 {
        self.open_events
    }

    pub fn is_empty(&self) -> bool {
        self.open.is_empty() && self.committed.is_empty()
    }

    fn halt(&self, reason: HaltReason) -> AccumulatorError {
        AccumulatorError::Halt {
            table: self.schema.table().clone(),
            reason,
        }
    }

    fn conform(&self, row: &Row, image: Image) -> Result<Vec<ColumnValue>, AccumulatorError> {
        let columns = self.schema.columns();
        let mut out: Vec<Option<ColumnValue>> = vec![None; columns.len()];
        for (name, value) in &row.0 {
            let Some(i) = self.schema.index_of(name) else {
                return Err(self.halt(HaltReason::SchemaMismatch {
                    column: name.clone(),
                    detail: "a column the table's schema doesn't have".into(),
                }));
            };
            let c = &columns[i];
            let value = match (image, value) {
                (Image::Before, ColumnValue::Unchanged) => ColumnValue::Null,
                _ => value.clone(),
            };
            out[i] = Some(
                conform_value(value, &c.value_type, &c.logical).map_err(|e| {
                    let column = c.name.clone();
                    self.halt(match e {
                        Misfit::Overflow => HaltReason::NumericOverflow { column },
                        Misfit::Type(detail) => HaltReason::SchemaMismatch { column, detail },
                    })
                })?,
            );
        }
        out.into_iter()
            .zip(columns)
            .map(|(v, c)| match (v, image) {
                (Some(v), _) => Ok(v),
                (None, Image::Before) => Ok(ColumnValue::Null),
                (None, Image::After) => Err(self.halt(HaltReason::SchemaMismatch {
                    column: c.name.clone(),
                    detail: "a column missing from the row image".into(),
                })),
            })
            .collect()
    }

    fn key(&self, values: &[ColumnValue]) -> Result<Key, AccumulatorError> {
        self.identifier
            .iter()
            .map(|&i| {
                key_part(&values[i]).ok_or_else(|| {
                    self.halt(HaltReason::NullIdentifier {
                        column: self.schema.columns()[i].name.clone(),
                    })
                })
            })
            .collect::<Result<_, _>>()
            .map(Key)
    }

    fn row(&self, kind: RowKind, values: Vec<ColumnValue>, e: &CdcEvent) -> CollapsedRow {
        let values = match kind {
            RowKind::Upsert => values,
            RowKind::Delete => values
                .into_iter()
                .enumerate()
                .map(|(i, v)| {
                    if self.delete_columns.contains(&i) {
                        v
                    } else {
                        ColumnValue::Null
                    }
                })
                .collect(),
        };
        CollapsedRow {
            kind,
            values,
            op: e.op,
            commit_ts_micros: e.commit_ts_micros,
            position: e.source_pos.clone(),
            seq: e.change_seq,
        }
    }

    /// The newest image of `key` in either layer.
    fn latest(&self, key: &Key) -> Option<&CollapsedRow> {
        self.open.get(key).or_else(|| self.committed.get(key))
    }

    /// Applies one event to the open layer.
    pub fn apply(&mut self, event: &CdcEvent) -> Result<(), AccumulatorError> {
        event.validate()?;
        match event.op {
            Op::Delete => {
                let before =
                    self.conform(event.before.as_ref().expect("validated"), Image::Before)?;
                let key = self.key(&before)?;
                let row = self.row(RowKind::Delete, before, event);
                self.put_open(key, row);
            }
            Op::Insert | Op::Update | Op::Snapshot => {
                let mut after =
                    self.conform(event.after.as_ref().expect("validated"), Image::After)?;
                let key = self.key(&after)?;
                // An UPDATE whose before-image has another key moved the row: delete the old key.
                // A before-image without its key halts: the row may have moved, and guessing it
                // didn't would leave the old key behind.
                let moved = match (&event.before, event.op) {
                    (Some(before), Op::Update) => {
                        let before = self.conform(before, Image::Before)?;
                        let old = self.key(&before)?;
                        (old != key).then_some((old, before))
                    }
                    _ => None,
                };
                // Unchanged-TOAST values belong to the row's previous image, under its old key when
                // the key moved (D-IMPL-3).
                if after.iter().any(|v| matches!(v, ColumnValue::Unchanged)) {
                    let source = moved.as_ref().map_or(&key, |(old, _)| old);
                    if let Some(prev) = self.latest(source).filter(|p| p.kind == RowKind::Upsert) {
                        for (v, p) in after.iter_mut().zip(&prev.values) {
                            if matches!(v, ColumnValue::Unchanged) {
                                *v = p.clone();
                            }
                        }
                    }
                }
                if let Some((old, before)) = moved {
                    let row = self.row(RowKind::Delete, before, event);
                    self.put_open(old, row);
                }
                let row = self.row(RowKind::Upsert, after, event);
                self.put_open(key, row);
            }
        }
        self.open_events += 1;
        Ok(())
    }

    fn put_open(&mut self, key: Key, row: CollapsedRow) {
        supersede(&mut self.open, &mut self.open_bytes, key, row);
    }

    /// The source committed the open transaction: its rows join the committed layer.
    pub fn commit_txn(&mut self) {
        for (key, row) in std::mem::take(&mut self.open) {
            supersede(&mut self.committed, &mut self.committed_bytes, key, row);
        }
        self.committed_events += self.open_events;
        self.open_bytes = 0;
        self.open_events = 0;
    }

    /// Takes the committed layer in `_seq` order; a key change's delete (same `_seq` as its upsert)
    /// comes first.
    pub fn take_committed(&mut self) -> Vec<CollapsedRow> {
        let mut rows: Vec<CollapsedRow> =
            std::mem::take(&mut self.committed).into_values().collect();
        rows.sort_by(|a, b| {
            let seq = a.seq.partial_cmp(&b.seq).unwrap_or(Ordering::Equal);
            seq.then_with(|| (a.kind == RowKind::Upsert).cmp(&(b.kind == RowKind::Upsert)))
        });
        self.committed_bytes = 0;
        self.committed_events = 0;
        rows
    }
}

/// Keep-last by `_seq`: `row` replaces the key's image unless the stored one is newer (a replayed
/// or out-of-order event never rolls a key back).
fn supersede(
    layer: &mut HashMap<Key, CollapsedRow>,
    bytes: &mut usize,
    key: Key,
    row: CollapsedRow,
) {
    if let Some(stored) = layer.get(&key) {
        if stored.seq.partial_cmp(&row.seq) == Some(Ordering::Greater) {
            return;
        }
        *bytes -= row_bytes(&stored.values);
    }
    *bytes += row_bytes(&row.values);
    layer.insert(key, row);
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, Decimal, TableRef};

    use super::*;

    fn schema() -> TableSchema {
        TableSchema::new(
            TableRef::new("public", "t"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("region", V::Text, "text"),
                ColumnSchema::new(
                    "price",
                    V::Numeric {
                        precision: 6,
                        scale: 2,
                    },
                    "numeric(6,2)",
                ),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn buffer() -> TableBuffer {
        TableBuffer::new(schema(), &["region".into()]).unwrap()
    }

    fn row(id: i64, doc: ColumnValue) -> Row {
        Row(vec![
            ("id".into(), ColumnValue::Int64(id)),
            ("doc".into(), doc),
            ("region".into(), ColumnValue::Text("eu".into())),
            ("price".into(), ColumnValue::Null),
        ])
    }

    fn text(s: &str) -> ColumnValue {
        ColumnValue::Text(s.into())
    }

    fn event(op: Op, seq: u64, before: Option<Row>, after: Option<Row>) -> CdcEvent {
        CdcEvent {
            op,
            table: TableRef::new("public", "t"),
            before,
            after,
            source_pos: SourcePosition::Pg(Lsn(1000)),
            change_seq: SourceSeq::Pg(Lsn(seq)),
            xid: 7,
            commit_ts_micros: 42,
        }
    }

    fn insert(seq: u64, id: i64, doc: &str) -> CdcEvent {
        event(Op::Insert, seq, None, Some(row(id, text(doc))))
    }

    fn update(seq: u64, id: i64, doc: ColumnValue) -> CdcEvent {
        event(Op::Update, seq, None, Some(row(id, doc)))
    }

    fn delete(seq: u64, id: i64) -> CdcEvent {
        let key_only = Row(vec![("id".into(), ColumnValue::Int64(id))]);
        event(Op::Delete, seq, Some(key_only), None)
    }

    fn flush(b: &mut TableBuffer) -> Vec<CollapsedRow> {
        b.commit_txn();
        b.take_committed()
    }

    fn halt_code(r: Result<(), AccumulatorError>) -> &'static str {
        match r {
            Err(AccumulatorError::Halt { reason, .. }) => reason.code(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_key_collapses_to_its_last_image() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "a")).unwrap();
        b.apply(&update(2, 1, text("b"))).unwrap();
        b.apply(&update(3, 1, text("c"))).unwrap();
        let rows = flush(&mut b);
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].kind, rows[0].op), (RowKind::Upsert, Op::Update));
        assert_eq!(rows[0].values[1], text("c"));
        assert_eq!(rows[0].seq, SourceSeq::Pg(Lsn(3)));
    }

    #[test]
    fn a_delete_is_the_last_image_and_carries_only_its_columns() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "a")).unwrap();
        let full = event(Op::Delete, 2, Some(row(1, text("a"))), None);
        b.apply(&full).unwrap();
        let rows = flush(&mut b);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, RowKind::Delete);
        assert_eq!(
            rows[0].values,
            [
                ColumnValue::Int64(1),
                ColumnValue::Null,
                text("eu"),
                ColumnValue::Null
            ],
            "identifier and the extra delete column (region); the rest NULL"
        );
        b.apply(&delete(3, 2)).unwrap();
        assert_eq!(
            flush(&mut b)[0].values[2],
            ColumnValue::Null,
            "key-only before-image"
        );
    }

    #[test]
    fn a_key_change_is_a_delete_of_the_old_key_and_an_upsert_of_the_new() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "big")).unwrap();
        let moved = event(
            Op::Update,
            2,
            Some(row(1, text("big"))),
            Some(row(2, ColumnValue::Unchanged)),
        );
        b.apply(&moved).unwrap();
        let rows = flush(&mut b);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[0].kind, &rows[0].values[0]),
            (RowKind::Delete, &ColumnValue::Int64(1))
        );
        assert_eq!(
            (rows[1].kind, &rows[1].values[0]),
            (RowKind::Upsert, &ColumnValue::Int64(2))
        );
        assert_eq!(
            rows[1].values[1],
            text("big"),
            "unchanged TOAST filled from the old key"
        );
    }

    #[test]
    fn a_before_image_with_the_same_key_is_not_a_key_change() {
        let mut b = buffer();
        let full = event(
            Op::Update,
            1,
            Some(row(1, text("a"))),
            Some(row(1, text("b"))),
        );
        b.apply(&full).unwrap();
        let rows = flush(&mut b);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, RowKind::Upsert);
    }

    #[test]
    fn unchanged_toast_is_filled_from_an_earlier_image_across_transactions() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "big")).unwrap();
        b.commit_txn();
        b.apply(&update(2, 1, ColumnValue::Unchanged)).unwrap();
        assert_eq!(flush(&mut b)[0].values[1], text("big"));

        b.apply(&update(3, 1, ColumnValue::Unchanged)).unwrap();
        assert_eq!(
            flush(&mut b)[0].values[1],
            ColumnValue::Unchanged,
            "no earlier image in the buffer: the target keeps its value"
        );
    }

    #[test]
    fn an_older_event_never_rolls_a_key_back() {
        let mut b = buffer();
        b.apply(&update(5, 1, text("new"))).unwrap();
        b.apply(&update(4, 1, text("old"))).unwrap();
        b.commit_txn();
        b.apply(&update(3, 1, text("older"))).unwrap();
        assert_eq!(flush(&mut b)[0].values[1], text("new"));
    }

    #[test]
    fn the_open_transaction_stays_out_of_a_flush() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "a")).unwrap();
        b.commit_txn();
        b.apply(&insert(2, 2, "b")).unwrap();
        assert_eq!((b.committed_events(), b.open_events()), (1, 1));
        assert!(b.open_bytes() > 0 && b.committed_bytes() > 0);
        let rows = b.take_committed();
        assert_eq!(rows.len(), 1);
        assert_eq!(b.committed_bytes(), 0);
        assert!(!b.is_empty(), "the open row is still buffered");
        assert_eq!(flush(&mut b)[0].values[0], ColumnValue::Int64(2));
        assert!(b.is_empty());
    }

    #[test]
    fn bytes_follow_replacement_not_arrival() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "a")).unwrap();
        let one = b.open_bytes();
        b.apply(&update(2, 1, text("b"))).unwrap();
        assert_eq!(b.open_bytes(), one);
        assert_eq!(b.open_events(), 2);
    }

    #[test]
    fn decimals_are_rescaled_to_the_column_and_overflow_halts() {
        let mut b = buffer();
        let mut r = row(1, text("a"));
        r.0[3].1 = ColumnValue::Numeric(Decimal::parse("1.5").unwrap());
        b.apply(&event(Op::Insert, 1, None, Some(r.clone())))
            .unwrap();
        assert_eq!(
            flush(&mut b)[0].values[3],
            ColumnValue::Numeric(Decimal::new(150, 2))
        );

        r.0[3].1 = ColumnValue::Numeric(Decimal::parse("12345.5").unwrap());
        assert_eq!(
            halt_code(b.apply(&event(Op::Insert, 2, None, Some(r.clone())))),
            "NUMERIC_OVERFLOW"
        );
        r.0[3].1 = ColumnValue::Numeric(Decimal::parse("1.555").unwrap());
        assert_eq!(
            halt_code(b.apply(&event(Op::Insert, 3, None, Some(r)))),
            "NUMERIC_OVERFLOW"
        );
    }

    #[test]
    fn a_row_that_doesnt_match_the_schema_halts() {
        let mut b = buffer();
        let mut extra = row(1, text("a"));
        extra.0.push(("new_col".into(), ColumnValue::Int32(1)));
        assert_eq!(
            halt_code(b.apply(&event(Op::Insert, 1, None, Some(extra)))),
            "SCHEMA_MISMATCH"
        );
        let mut missing = row(1, text("a"));
        missing.0.pop();
        assert_eq!(
            halt_code(b.apply(&event(Op::Insert, 1, None, Some(missing)))),
            "SCHEMA_MISMATCH"
        );
        assert_eq!(
            halt_code(b.apply(&update(1, 1, ColumnValue::Int32(3)))),
            "SCHEMA_MISMATCH"
        );
    }

    #[test]
    fn a_null_or_unsent_identifier_halts() {
        let mut b = buffer();
        let mut r = row(1, text("a"));
        r.0[0].1 = ColumnValue::Null;
        assert_eq!(
            halt_code(b.apply(&event(Op::Insert, 1, None, Some(r)))),
            "NULL_IDENTIFIER"
        );
        let no_key = event(
            Op::Delete,
            2,
            Some(Row(vec![("doc".into(), text("a"))])),
            None,
        );
        assert_eq!(halt_code(b.apply(&no_key)), "NULL_IDENTIFIER");
    }

    #[test]
    fn an_update_whose_before_image_lacks_the_key_halts() {
        let mut b = buffer();
        let no_key = event(
            Op::Update,
            1,
            Some(Row(vec![("doc".into(), text("a"))])),
            Some(row(1, text("b"))),
        );
        assert_eq!(halt_code(b.apply(&no_key)), "NULL_IDENTIFIER");
        assert!(b.is_empty(), "a halted event leaves the buffer as it was");
    }

    #[test]
    fn a_malformed_event_is_an_event_error() {
        let mut b = buffer();
        let bad = event(Op::Insert, 1, None, None);
        assert!(matches!(b.apply(&bad), Err(AccumulatorError::Event(_))));
    }

    #[test]
    fn delete_columns_must_exist() {
        let err = TableBuffer::new(schema(), &["nope".into()]).unwrap_err();
        assert_eq!(err.code(), "SCHEMA_MISMATCH");
        assert_eq!(buffer().delete_columns(), [0, 2]);
    }
}
