//! One table's buffer: change events collapsed by identifier on arrival (Ghost_Row §4), in two
//! layers — the transaction still open at the source, and the transactions already committed.

use std::cmp::Ordering;
use std::collections::HashMap;

use data_avalanche_core::{
    CdcEvent, ColumnValue, ColumnValueType, Decimal, HaltReason, LogicalType, Op, PositionRange,
    Row, SourcePosition, SourceSeq, SplitInfo, TableChangeSet, TableSchema, upsert_schema,
};

use crate::AccumulatorError;
use crate::batch::{self, Lineage, MAX_BATCH_BYTES};

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
pub(crate) struct Key(Vec<KeyPart>);

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

/// The key of `row` in `schema`, compared as the buffer compares keys; `None` when an identifier
/// value is missing, NULL or doesn't fit its column.
pub(crate) fn identity(schema: &TableSchema, row: &Row) -> Option<Key> {
    schema
        .identifier()
        .iter()
        .map(|name| {
            let c = schema.column(name)?;
            let v = conform_value(row.get(name)?.clone(), &c.value_type, &c.logical).ok()?;
            key_part(&v)
        })
        .collect::<Option<_>>()
        .map(Key)
}

/// Rough in-memory size of a row, for the flush `bytes` trigger and the transaction buffer ceiling.
pub(crate) fn row_bytes(values: &[ColumnValue]) -> usize {
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
/// is stale), and decimals are rescaled to the column's Arrow scale (Hop 2). A `bigint unsigned`
/// becomes a decimal of scale 0 first, so it is rescaled and checked the same way; the Numeric arm
/// takes a `UInt64` column too, for a buffered value a schema change conforms again.
fn conform_value(
    v: ColumnValue,
    t: &ColumnValueType,
    logical: &LogicalType,
) -> Result<ColumnValue, Misfit> {
    match (v, t, logical) {
        (ColumnValue::UInt64(u), ColumnValueType::UInt64, LogicalType::Decimal { .. }) => {
            conform_value(
                ColumnValue::Numeric(Decimal::new(i128::from(u), 0)),
                t,
                logical,
            )
        }
        (
            ColumnValue::Numeric(d),
            ColumnValueType::Numeric { .. } | ColumnValueType::UInt64,
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
    /// A TRUNCATE in the open transaction / in the committed layer.
    open_truncate: bool,
    committed_truncate: bool,
}

/// The identifier's column indices, and the columns a delete carries (identifier first).
fn layout(
    schema: &TableSchema,
    delete_extra: &[String],
) -> Result<(Vec<usize>, Vec<usize>), HaltReason> {
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
    Ok((identifier, delete_columns))
}

/// The widenings the classifier lets through as Safe (`Schema_Change…` §7.2); decimals widen by
/// precision alone, which [`conform_value`] handles.
fn widen(v: ColumnValue, to: &ColumnValueType) -> ColumnValue {
    use ColumnValue as V;
    use ColumnValueType as T;
    match (v, to) {
        (V::Int16(i), T::Int32) => V::Int32(i.into()),
        (V::Int16(i), T::Int64) => V::Int64(i.into()),
        (V::Int32(i), T::Int64) => V::Int64(i.into()),
        (V::Float32(f), T::Float64) => V::Float64(f.into()),
        (v, _) => v,
    }
}

impl TableBuffer {
    /// `delete_extra` names the columns a delete carries besides the identifier (the partition
    /// columns a sink prunes by).
    pub fn new(schema: TableSchema, delete_extra: &[String]) -> Result<Self, HaltReason> {
        let (identifier, delete_columns) = layout(&schema, delete_extra)?;
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
            open_truncate: false,
            committed_truncate: false,
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
                let before = match (&event.before, event.op) {
                    (Some(raw), Op::Update) => Some((raw, self.conform(raw, Image::Before)?)),
                    _ => None,
                };
                let moved = match &before {
                    Some((_, before)) => Some(self.key(before)?).filter(|old| *old != key),
                    None => None,
                };
                // Unchanged-TOAST values belong to the row's previous image, under its old key when
                // the key moved (D-IMPL-3): the buffered image first, then the before-image where
                // the source sent the column (REPLICA IDENTITY FULL; a sent NULL is a value).
                if after.iter().any(|v| matches!(v, ColumnValue::Unchanged)) {
                    let source = moved.as_ref().unwrap_or(&key);
                    if let Some(prev) = self.latest(source).filter(|p| p.kind == RowKind::Upsert) {
                        for (v, p) in after.iter_mut().zip(&prev.values) {
                            if matches!(v, ColumnValue::Unchanged) {
                                *v = p.clone();
                            }
                        }
                    }
                    if let Some((raw, before)) = &before {
                        let columns = self.schema.columns();
                        for ((v, b), c) in after.iter_mut().zip(before).zip(columns) {
                            let sent = raw
                                .get(&c.name)
                                .is_some_and(|r| !matches!(r, ColumnValue::Unchanged));
                            if sent && matches!(v, ColumnValue::Unchanged) {
                                *v = b.clone();
                            }
                        }
                    }
                    // A value still unchanged stays masked: the sink keeps the target's value, or
                    // writes NULL for a moved key the target doesn't have yet (Data-Flow TOAST
                    // step 4; its metric comes with a later milestone).
                }
                if let (Some(old), Some((_, before))) = (moved, before) {
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

    fn flush_required(&self) -> Result<(), AccumulatorError> {
        if self.committed_events > 0 {
            return Err(AccumulatorError::FlushRequired {
                table: self.schema.table().clone(),
            });
        }
        Ok(())
    }

    /// A TRUNCATE in the open transaction: the barrier (Data-Flow §TRUNCATE barrier). The committed
    /// layer must have been flushed ([`AccumulatorError::FlushRequired`] otherwise, nothing
    /// changed); the open transaction's earlier rows are dropped, and the next change set clears
    /// the table before applying the rows that follow.
    pub fn truncate(&mut self) -> Result<(), AccumulatorError> {
        self.flush_required()?;
        self.open.clear();
        self.open_bytes = 0;
        self.open_truncate = true;
        self.open_events += 1;
        Ok(())
    }

    /// A schema change (`Schema_Change…` §8: seal → flush → apply). The committed layer must have
    /// been flushed ([`AccumulatorError::FlushRequired`] otherwise); the open transaction's rows
    /// are carried over by column name: a new column is NULL, a dropped one goes, a widened one is
    /// widened. The identifier can't change (the classifier halts that).
    pub fn set_schema(
        &mut self,
        schema: TableSchema,
        delete_extra: &[String],
    ) -> Result<(), AccumulatorError> {
        self.flush_required()?;
        if schema.identifier() != self.schema.identifier() {
            return Err(self.halt(HaltReason::UnsafeDdl {
                detail: "the identifier changed".into(),
            }));
        }
        let (identifier, delete_columns) =
            layout(&schema, delete_extra).map_err(|r| self.halt(r))?;
        let mut open = HashMap::with_capacity(self.open.len());
        let mut open_bytes = 0;
        for row in self.open.values() {
            let mut values = Vec::with_capacity(schema.columns().len());
            for (j, c) in schema.columns().iter().enumerate() {
                let v = match self.schema.index_of(&c.name) {
                    Some(i) if row.kind == RowKind::Upsert || delete_columns.contains(&j) => {
                        widen(row.values[i].clone(), &c.value_type)
                    }
                    _ => ColumnValue::Null,
                };
                let v = conform_value(v, &c.value_type, &c.logical).map_err(|_| {
                    self.halt(HaltReason::SchemaMismatch {
                        column: c.name.clone(),
                        detail: "a buffered value doesn't fit the new column type".into(),
                    })
                })?;
                values.push(v);
            }
            let key = Key(identifier
                .iter()
                .map(|&i| key_part(&values[i]).expect("identifier values stay non-null"))
                .collect());
            open_bytes += row_bytes(&values);
            open.insert(
                key,
                CollapsedRow {
                    values,
                    ..row.clone()
                },
            );
        }
        self.schema = schema;
        self.identifier = identifier;
        self.delete_columns = delete_columns;
        self.open = open;
        self.open_bytes = open_bytes;
        Ok(())
    }

    /// The source committed the open transaction: its rows join the committed layer.
    pub fn commit_txn(&mut self) {
        if std::mem::take(&mut self.open_truncate) {
            self.committed_truncate = true;
        }
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

    /// Takes the committed layer as a change set for the sink.
    pub fn flush(
        &mut self,
        lineage: &Lineage,
        position: PositionRange,
        split: Option<SplitInfo>,
    ) -> TableChangeSet {
        let rows = self.take_committed();
        let (upserts, deletes) = batch::build(
            &self.schema,
            &self.delete_columns,
            rows,
            lineage,
            MAX_BATCH_BYTES,
        );
        TableChangeSet {
            table: self.schema.table().clone(),
            schema: upsert_schema(&self.schema),
            identifier: self.schema.identifier().to_vec(),
            truncate: std::mem::take(&mut self.committed_truncate),
            upserts,
            deletes,
            position,
            split,
            snapshot: false,
        }
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
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableRef};

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
    fn a_key_move_fills_unchanged_toast_from_the_before_image_the_source_sent() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "big")).unwrap();
        flush(&mut b);
        let full = event(
            Op::Update,
            2,
            Some(row(1, text("big"))),
            Some(row(2, ColumnValue::Unchanged)),
        );
        b.apply(&full).unwrap();
        let rows = flush(&mut b);
        assert_eq!(rows[1].kind, RowKind::Upsert);
        assert_eq!(
            rows[1].values[1],
            text("big"),
            "the old image was flushed: filled from the REPLICA IDENTITY FULL before-image"
        );

        let key_only = event(
            Op::Update,
            3,
            Some(Row(vec![("id".into(), ColumnValue::Int64(2))])),
            Some(row(3, ColumnValue::Unchanged)),
        );
        b.apply(&key_only).unwrap();
        assert_eq!(
            flush(&mut b)[1].values[1],
            ColumnValue::Unchanged,
            "a key-only before-image didn't send doc"
        );
    }

    #[test]
    fn a_full_before_image_fills_unchanged_toast_with_nothing_buffered() {
        let mut b = buffer();
        let full = event(
            Op::Update,
            1,
            Some(row(1, text("big"))),
            Some(row(1, ColumnValue::Unchanged)),
        );
        b.apply(&full).unwrap();
        assert_eq!(flush(&mut b)[0].values[1], text("big"));

        let mut sent_null = row(1, ColumnValue::Null);
        sent_null.0[2].1 = ColumnValue::Unchanged;
        let mut after = row(1, ColumnValue::Unchanged);
        after.0[2].1 = ColumnValue::Unchanged;
        b.apply(&event(Op::Update, 2, Some(sent_null), Some(after)))
            .unwrap();
        let values = &flush(&mut b)[0].values;
        assert_eq!(values[1], ColumnValue::Null, "a sent NULL is a value");
        assert_eq!(
            values[2],
            ColumnValue::Unchanged,
            "unchanged in the before-image too: not sent"
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

    /// A table `(id bigint key, n bigint unsigned)` whose `n` is stored as `logical`.
    fn unsigned(logical: LogicalType) -> TableSchema {
        let mut n = ColumnSchema::new("n", V::UInt64, "bigint unsigned");
        n.logical = logical;
        TableSchema::new(
            TableRef::new("public", "t"),
            vec![ColumnSchema::new("id", V::Int64, "bigint").not_null(), n],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn unsigned_row(seq: u64, n: u64) -> CdcEvent {
        let r = Row(vec![
            ("id".into(), ColumnValue::Int64(seq as i64)),
            ("n".into(), ColumnValue::UInt64(n)),
        ]);
        event(Op::Insert, seq, None, Some(r))
    }

    fn decimal(precision: u8, scale: i8) -> LogicalType {
        LogicalType::Decimal { precision, scale }
    }

    #[test]
    fn an_unsigned_value_is_rescaled_to_its_decimal_column() {
        let mut b = TableBuffer::new(unsigned(decimal(25, 2)), &[]).unwrap();
        b.apply(&unsigned_row(1, 12345)).unwrap();
        assert_eq!(
            flush(&mut b)[0].values[1],
            ColumnValue::Numeric(Decimal::new(1_234_500, 2)),
            "12345, not 123.45"
        );

        let mut b = TableBuffer::new(unsigned(decimal(10, 0)), &[]).unwrap();
        assert_eq!(
            halt_code(b.apply(&unsigned_row(1, u64::MAX))),
            "NUMERIC_OVERFLOW"
        );

        let canonical = TableSchema::new(
            TableRef::new("public", "t"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("n", V::UInt64, "bigint unsigned"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        assert_eq!(canonical.columns()[1].logical, decimal(20, 0));
        let mut b = TableBuffer::new(canonical, &[]).unwrap();
        b.apply(&unsigned_row(1, u64::MAX)).unwrap();
        assert_eq!(
            flush(&mut b)[0].values[1],
            ColumnValue::Numeric(Decimal::new(i128::from(u64::MAX), 0))
        );
    }

    #[test]
    fn a_rescaled_unsigned_value_survives_a_schema_change() {
        let mut b = TableBuffer::new(unsigned(decimal(25, 2)), &[]).unwrap();
        b.apply(&unsigned_row(1, 12345)).unwrap();
        b.set_schema(unsigned(decimal(30, 4)), &[]).unwrap();
        b.apply(&unsigned_row(2, 7)).unwrap();
        let rows = flush(&mut b);
        assert_eq!(
            rows[0].values[1],
            ColumnValue::Numeric(Decimal::new(123_450_000, 4))
        );
        assert_eq!(
            rows[1].values[1],
            ColumnValue::Numeric(Decimal::new(70_000, 4)),
            "every value of the column at one scale"
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
    fn a_flush_is_a_valid_change_set_of_the_committed_layer() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "a")).unwrap();
        b.apply(&insert(2, 2, "b")).unwrap();
        b.apply(&delete(3, 2)).unwrap();
        b.commit_txn();
        b.apply(&insert(4, 3, "open")).unwrap();
        let cs = b.flush(&lineage(), range(), None);
        cs.validate().unwrap();
        assert_eq!((cs.upsert_rows(), cs.delete_rows()), (1, 1));
        assert_eq!(b.open_events(), 1, "the open transaction stays buffered");
    }

    fn lineage() -> Lineage {
        Lineage {
            source_db: "db".into(),
            pipeline: "p".into(),
        }
    }

    fn range() -> PositionRange {
        PositionRange {
            start: SourcePosition::Pg(Lsn(0)),
            end: SourcePosition::Pg(Lsn(1000)),
        }
    }

    fn is_flush_required(r: Result<(), AccumulatorError>) -> bool {
        matches!(r, Err(AccumulatorError::FlushRequired { .. }))
    }

    #[test]
    fn a_truncate_drops_earlier_open_rows_and_marks_the_next_change_set() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "before")).unwrap();
        b.truncate().unwrap();
        b.apply(&insert(2, 2, "after")).unwrap();
        b.commit_txn();
        let cs = b.flush(&lineage(), range(), None);
        assert!(cs.truncate);
        assert_eq!(cs.upsert_rows(), 1);
        b.apply(&insert(3, 3, "later")).unwrap();
        b.commit_txn();
        assert!(!b.flush(&lineage(), range(), None).truncate, "only once");
    }

    #[test]
    fn a_truncate_alone_is_a_change_set() {
        let mut b = buffer();
        b.truncate().unwrap();
        b.commit_txn();
        assert_eq!(b.committed_events(), 1);
        let cs = b.flush(&lineage(), range(), None);
        assert!(cs.truncate && cs.upsert_rows() == 0 && !cs.is_empty());
    }

    #[test]
    fn barriers_need_the_committed_layer_flushed() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "a")).unwrap();
        b.commit_txn();
        b.apply(&insert(2, 2, "open")).unwrap();
        assert!(is_flush_required(b.truncate()));
        assert!(is_flush_required(b.set_schema(schema(), &[])));
        assert_eq!(b.open_events(), 1, "nothing changed");
        b.take_committed();
        b.truncate().unwrap();
    }

    #[test]
    fn a_schema_change_carries_open_rows_over_by_name() {
        let mut b = buffer();
        b.apply(&insert(1, 1, "a")).unwrap();
        b.apply(&delete(2, 2)).unwrap();
        let wider = TableSchema::new(
            TableRef::new("public", "t"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("added", V::Int32, "integer"),
                ColumnSchema::new("region", V::Text, "text"),
                ColumnSchema::new(
                    "price",
                    V::Numeric {
                        precision: 10,
                        scale: 2,
                    },
                    "numeric(10,2)",
                ),
                ColumnSchema::new("doc", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        b.set_schema(wider, &["region".into()]).unwrap();
        b.apply(&event(
            Op::Insert,
            3,
            None,
            Some(Row(vec![
                ("id".into(), ColumnValue::Int64(3)),
                ("added".into(), ColumnValue::Int32(7)),
                ("region".into(), text("us")),
                ("price".into(), ColumnValue::Null),
                ("doc".into(), text("c")),
            ])),
        ))
        .unwrap();
        b.commit_txn();
        let rows = b.take_committed();
        assert_eq!(
            rows[0].values,
            [
                ColumnValue::Int64(1),
                ColumnValue::Null,
                text("eu"),
                ColumnValue::Null,
                text("a")
            ]
        );
        assert_eq!(rows[1].kind, RowKind::Delete);
        assert_eq!(rows[2].values[1], ColumnValue::Int32(7));
    }

    #[test]
    fn a_schema_change_widens_buffered_values_and_halts_on_the_rest() {
        let narrow = |t: V| {
            TableSchema::new(
                TableRef::new("public", "t"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("n", t, "n"),
                ],
                vec!["id".into()],
            )
            .unwrap()
        };
        let mut b = TableBuffer::new(narrow(V::Int32), &[]).unwrap();
        let r = |n| Row(vec![("id".into(), ColumnValue::Int64(1)), ("n".into(), n)]);
        b.apply(&event(Op::Insert, 1, None, Some(r(ColumnValue::Int32(5)))))
            .unwrap();
        b.set_schema(narrow(V::Int64), &[]).unwrap();
        assert!(b.open_bytes() > 0);
        b.commit_txn();
        assert_eq!(b.take_committed()[0].values[1], ColumnValue::Int64(5));

        b.apply(&event(Op::Insert, 2, None, Some(r(ColumnValue::Int64(5)))))
            .unwrap();
        assert_eq!(
            halt_code(b.set_schema(narrow(V::Text), &[])),
            "SCHEMA_MISMATCH"
        );
        assert_eq!(
            b.schema().columns()[1].value_type,
            V::Int64,
            "nothing changed"
        );
        let other_key = TableSchema::new(
            TableRef::new("public", "t"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("n", V::Int64, "n").not_null(),
            ],
            vec!["n".into()],
        )
        .unwrap();
        assert_eq!(halt_code(b.set_schema(other_key, &[])), "UNSAFE_DDL");
    }

    #[test]
    fn delete_columns_must_exist() {
        let err = TableBuffer::new(schema(), &["nope".into()]).unwrap_err();
        assert_eq!(err.code(), "SCHEMA_MISMATCH");
        assert_eq!(buffer().delete_columns(), [0, 2]);
    }

    #[test]
    fn a_rows_identity_is_its_key_as_the_buffer_compares_it() {
        let s = TableSchema::new(
            TableRef::new("public", "n"),
            vec![
                ColumnSchema::new(
                    "k",
                    V::Numeric {
                        precision: 6,
                        scale: 2,
                    },
                    "numeric(6,2)",
                )
                .not_null(),
                ColumnSchema::new("v", V::Text, "text"),
            ],
            vec!["k".into()],
        )
        .unwrap();
        let key = |d| Row(vec![("k".into(), ColumnValue::Numeric(d))]);
        assert_eq!(
            identity(&s, &key(Decimal::new(15, 1))),
            identity(&s, &key(Decimal::new(150, 2))),
            "1.5 and 1.50 are one key"
        );
        assert!(identity(&s, &key(Decimal::new(15, 1))).is_some());
        assert_eq!(identity(&s, &Row(vec![("v".into(), text("x"))])), None);
        assert_eq!(
            identity(&s, &Row(vec![("k".into(), ColumnValue::Null)])),
            None
        );
    }
}
