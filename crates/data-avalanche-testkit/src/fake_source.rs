//! A scripted source (roadmap M6a): an in-memory database with a write-ahead log, a replication
//! slot and MVCC visibility, behind the `SourceConnector` contract, so pipeline tests run the
//! whole worker without PostgreSQL. Positions are PostgreSQL LSNs: like pgoutput, a change
//! carries its commit record's start and the `Commit` the record's end, 8 bytes on, which is what
//! a stream acknowledges and resumes from.
//!
//! - Tests commit transactions ([`FakeTxn`]): the rows change and the log grows; an open stream
//!   delivers them. [`FakeSource::commit_stalled`] commits one that the stream delivers but a
//!   chunk doesn't see until [`FakeSource::release`], like a synchronous commit waiting for its
//!   standby (DA-SNAP-9); [`FakeSource::open_writer`] is a transaction still running.
//! - The slot follows `Postgres_Slot…` §1 in short: a first start creates it, a checkpoint the slot
//!   is past (or a lost slot) recreates it, and `acknowledge` moves it.
//! - [`FakeSource::alter`] changes a table's schema: the stream signals it before the table's next
//!   change, `augment` diffs by column name and `rescan` reports it. Like PostgreSQL's, `augment`
//!   keeps to the columns a `Columns` hint names, so a catalog ahead of the hint adds nothing.
//!   [`FakeSource::after_next_rescan`] acts between a rescan and the position read after it.
//! - [`FakeSource::fail`] makes the next call of one method fail; [`FakeSource::hold_reads`]
//!   holds chunk reads after they took their snapshot, so a test can commit while one is out.
//!
//! Keys are integer or text identifier columns. The fake trusts its tests: a change of a key a
//! stalled transaction holds, or a key that isn't an integer or text, panics.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ChangeStream, Chunk, ChunkVisibility, ColumnSchema, ColumnValue, HaltReason,
    InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables, Row,
    SchemaChange, SchemaDelta, SchemaHint, SnapshotReason, SourceConnector, SourceError,
    SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef, TableSchema, TransactionBoundary,
    XidSnapshot, identifier_values,
};
use tokio::sync::Notify;

/// The method a [`FakeSource::fail`] fault fires in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Preflight,
    ResolveTables,
    Prepare,
    ResetPosition,
    RetainedBytes,
    CurrentPosition,
    InFlight,
    ReadChunk,
    FetchRows,
    OpenStream,
    /// A stream's `next`, after the items it has already queued.
    Next,
    Augment,
    Rescan,
}

/// One change of a [`FakeTxn`].
#[derive(Debug, Clone)]
enum FakeOp {
    Insert(TableRef, Row),
    /// The after-image; a [`ColumnValue::Unchanged`] keeps the stored value (unchanged TOAST).
    Update(TableRef, Row),
    /// A key change: the old key, then the new row.
    Move(TableRef, Row, Row),
    Delete(TableRef, Row),
    Truncate(Vec<TableRef>),
}

/// A source transaction, built change by change.
#[derive(Debug, Clone, Default)]
pub struct FakeTxn {
    ops: Vec<FakeOp>,
}

impl FakeTxn {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(mut self, table: &TableRef, row: Row) -> Self {
        self.ops.push(FakeOp::Insert(table.clone(), row));
        self
    }

    /// Updates the row with `row`'s key; a [`ColumnValue::Unchanged`] in it keeps the stored
    /// value and is streamed as unchanged. Streamed without a before-image, as PostgreSQL sends it
    /// under REPLICA IDENTITY DEFAULT.
    pub fn update(mut self, table: &TableRef, row: Row) -> Self {
        self.ops.push(FakeOp::Update(table.clone(), row));
        self
    }

    /// Moves the row with key `old` to `row`'s key (a PK-changing UPDATE), streamed with `old` as
    /// its before-image.
    pub fn update_key(mut self, table: &TableRef, old: Row, row: Row) -> Self {
        self.ops.push(FakeOp::Move(table.clone(), old, row));
        self
    }

    /// Deletes the row with key `key` (its identifier columns).
    pub fn delete(mut self, table: &TableRef, key: Row) -> Self {
        self.ops.push(FakeOp::Delete(table.clone(), key));
        self
    }

    pub fn truncate(mut self, tables: &[TableRef]) -> Self {
        self.ops.push(FakeOp::Truncate(tables.to_vec()));
        self
    }
}

/// A key in identifier order, for sorting chunks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum KeyPart {
    Int(i64),
    Text(String),
}

type Key = Vec<KeyPart>;

fn key_of(schema: &TableSchema, row: &Row) -> Key {
    let key = identifier_values(schema, row)
        .unwrap_or_else(|| panic!("{}: a row without its key: {row:?}", schema.table()));
    key.0
        .iter()
        .map(|(_, v)| match v {
            ColumnValue::Int16(i) => KeyPart::Int(i64::from(*i)),
            ColumnValue::Int32(i) => KeyPart::Int(i64::from(*i)),
            ColumnValue::Int64(i) => KeyPart::Int(*i),
            ColumnValue::Text(s) => KeyPart::Text(s.clone()),
            other => panic!("the fake source keys on integers and text, not {other:?}"),
        })
        .collect()
}

/// One logged change: what the stream sends, and the table's columns when it was written.
#[derive(Debug, Clone)]
enum WalChange {
    Change {
        event: CdcEvent,
        columns: Vec<ColumnSchema>,
    },
    Truncate(Vec<TableRef>, SourceSeq),
}

#[derive(Debug, Clone)]
enum WalEntry {
    Txn {
        xid: u64,
        ts: i64,
        changes: Vec<WalChange>,
        commit: Lsn,
    },
    KeepAlive(Lsn),
}

impl WalEntry {
    fn end(&self) -> Lsn {
        match self {
            Self::Txn { commit, .. } | Self::KeepAlive(commit) => *commit,
        }
    }
}

/// A test's action, run once inside a source call.
struct Action(Box<dyn FnOnce(&FakeSource) + Send>);

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Action")
    }
}

#[derive(Debug)]
struct Db {
    catalog: BTreeMap<TableRef, TableSchema>,
    next_source_id: u64,
    rows: BTreeMap<TableRef, BTreeMap<Key, Row>>,
    /// Stalled transactions: the rows they changed, as they were before (`None`: absent).
    stalled: BTreeMap<u64, Vec<(TableRef, Key, Option<Row>)>>,
    /// Transactions still running, with the tables they write.
    open: BTreeMap<u64, Vec<TableRef>>,
    wal: Vec<WalEntry>,
    wal_end: Lsn,
    next_xid: u64,
    next_ts: i64,
    slot: Option<Lsn>,
    slot_lost: bool,
    added: Vec<TableRef>,
    retained: Option<Option<u64>>,
    faults: Vec<(Method, SourceError)>,
    hold_reads: bool,
    after_rescan: Option<Action>,
    acknowledged: Vec<SourcePosition>,
    opened: Vec<SourcePosition>,
    reads: BTreeMap<TableRef, usize>,
    rescans: usize,
    fetches: BTreeMap<TableRef, usize>,
    prepares: Vec<Option<SourcePosition>>,
}

/// The scripted source. Cloning shares the database.
#[derive(Debug, Clone)]
pub struct FakeSource {
    db: Arc<Mutex<Db>>,
    /// Wakes open streams when the log grows, and held reads.
    wake: Arc<Notify>,
}

fn lsn(p: &SourcePosition) -> Lsn {
    match p {
        SourcePosition::Pg(l) => *l,
        other => panic!("the fake source speaks PostgreSQL positions, not {other}"),
    }
}

impl FakeSource {
    /// A database holding `tables`, empty, with its log at `0/100`.
    pub fn new(tables: Vec<TableSchema>) -> Self {
        let mut db = Db {
            catalog: BTreeMap::new(),
            next_source_id: 1000,
            rows: BTreeMap::new(),
            stalled: BTreeMap::new(),
            open: BTreeMap::new(),
            wal: vec![],
            wal_end: Lsn(0x100),
            next_xid: 700,
            next_ts: 1_700_000_000_000_000,
            slot: None,
            slot_lost: false,
            added: vec![],
            retained: None,
            faults: vec![],
            hold_reads: false,
            after_rescan: None,
            acknowledged: vec![],
            opened: vec![],
            reads: BTreeMap::new(),
            rescans: 0,
            fetches: BTreeMap::new(),
            prepares: vec![],
        };
        for t in tables {
            db.create(t);
        }
        Self {
            db: Arc::new(Mutex::new(db)),
            wake: Arc::new(Notify::new()),
        }
    }

    fn db(&self) -> MutexGuard<'_, Db> {
        self.db
            .lock()
            .expect("no test panics while holding the lock")
    }

    /// Rows present before the pipeline starts: in the table, not in the log.
    pub fn load(&self, table: &TableRef, rows: Vec<Row>) {
        let mut db = self.db();
        let schema = db.schema(table);
        for row in rows {
            let key = key_of(&schema, &row);
            db.rows.get_mut(table).expect("a table").insert(key, row);
        }
    }

    /// The schema the catalog has for `table` now, with its source id.
    pub fn schema(&self, table: &TableRef) -> TableSchema {
        self.db().schema(table)
    }

    /// Commits `txn`: the rows change and the stream delivers it. Returns its xid.
    pub fn commit(&self, txn: FakeTxn) -> u64 {
        let xid = self.db().commit(None, txn, false);
        self.wake.notify_waiters();
        xid
    }

    /// Commits `txn` into the log, but chunks don't see it until [`Self::release`]: a synchronous
    /// commit waiting for its standby.
    pub fn commit_stalled(&self, txn: FakeTxn) -> u64 {
        let xid = self.db().commit(None, txn, true);
        self.wake.notify_waiters();
        xid
    }

    /// The stalled transaction becomes visible.
    pub fn release(&self, xid: u64) {
        self.db()
            .stalled
            .remove(&xid)
            .expect("a stalled transaction");
    }

    /// A transaction that is running and writes `tables`: in flight, invisible, not in the log.
    pub fn open_writer(&self, tables: &[TableRef]) -> u64 {
        let mut db = self.db();
        let xid = db.next_xid;
        db.next_xid += 1;
        db.open.insert(xid, tables.to_vec());
        xid
    }

    /// The running transaction `xid` commits `txn`.
    pub fn commit_writer(&self, xid: u64, txn: FakeTxn) {
        let mut db = self.db();
        db.open.remove(&xid).expect("an open writer");
        db.commit(Some(xid), txn, false);
        drop(db);
        self.wake.notify_waiters();
    }

    /// The running transaction `xid` rolls back.
    pub fn abort_writer(&self, xid: u64) {
        self.db().open.remove(&xid).expect("an open writer");
    }

    /// A keep-alive at the log's end.
    pub fn keepalive(&self) {
        let mut db = self.db();
        db.wal_end = Lsn(db.wal_end.0 + 0x10);
        let at = db.wal_end;
        db.wal.push(WalEntry::KeepAlive(at));
        drop(db);
        self.wake.notify_waiters();
    }

    /// Changes `table`'s schema in the catalog. Stored rows lose dropped columns and read NULL in
    /// added ones. Keeps the source id.
    pub fn alter(&self, table: &TableRef, schema: TableSchema) {
        let mut db = self.db();
        let id = db.schema(table).source_id().expect("an id");
        let schema = schema.with_source_id(id);
        for row in db.rows.get_mut(table).expect("a table").values_mut() {
            *row = Row(schema
                .columns()
                .iter()
                .map(|c| {
                    let v = row.get(&c.name).cloned().unwrap_or(ColumnValue::Null);
                    (c.name.clone(), v)
                })
                .collect());
        }
        db.catalog.insert(table.clone(), schema);
    }

    /// Drops `table` from the catalog (it streams nothing more).
    pub fn drop_table(&self, table: &TableRef) {
        let mut db = self.db();
        db.catalog.remove(table);
        db.rows.remove(table);
    }

    /// Creates `schema`'s table (again): a new source id.
    pub fn create_table(&self, schema: TableSchema) {
        self.db().create(schema);
    }

    /// The source loses the slot's position: the next stream fails with `PositionUnreachable`,
    /// and the next `prepare` recreates the slot.
    pub fn lose_slot(&self) {
        self.db().slot_lost = true;
        self.wake.notify_waiters();
    }

    /// The tables the next resuming `prepare` reports in `Prepared::added`.
    pub fn report_added(&self, tables: &[TableRef]) {
        self.db().added = tables.to_vec();
    }

    /// What `retained_bytes` reports from now on (by default, the log from the slot to its end).
    pub fn set_retained(&self, bytes: Option<u64>) {
        self.db().retained = Some(bytes);
    }

    /// The next call of `method` fails with `error`.
    pub fn fail(&self, method: Method, error: SourceError) {
        self.db().faults.push((method, error));
        self.wake.notify_waiters();
    }

    /// Chunk reads wait, after taking their snapshot, until [`Self::release_reads`].
    pub fn hold_reads(&self) {
        self.db().hold_reads = true;
    }

    /// Runs `f` once, inside the next `rescan`, after it has read the catalog: a change made
    /// between a rescan and the position read after it.
    pub fn after_next_rescan(&self, f: impl FnOnce(&FakeSource) + Send + 'static) {
        self.db().after_rescan = Some(Action(Box::new(f)));
    }

    pub fn release_reads(&self) {
        self.db().hold_reads = false;
        self.wake.notify_waiters();
    }

    /// Every position acknowledged so far, in order.
    pub fn acknowledged(&self) -> Vec<SourcePosition> {
        self.db().acknowledged.clone()
    }

    /// Where each stream was opened, in order.
    pub fn opened(&self) -> Vec<SourcePosition> {
        self.db().opened.clone()
    }

    /// The checkpoint each `prepare` was called with.
    pub fn prepares(&self) -> Vec<Option<SourcePosition>> {
        self.db().prepares.clone()
    }

    /// Chunk reads of `table` so far.
    pub fn reads(&self, table: &TableRef) -> usize {
        self.db().reads.get(table).copied().unwrap_or(0)
    }

    /// `rescan` calls so far, failed ones included.
    pub fn rescans(&self) -> usize {
        self.db().rescans
    }

    /// `fetch_rows` calls for `table` so far, failed ones included.
    pub fn fetches(&self, table: &TableRef) -> usize {
        self.db().fetches.get(table).copied().unwrap_or(0)
    }

    /// The slot's confirmed position; `None` without a slot.
    pub fn slot(&self) -> Option<SourcePosition> {
        self.db().slot.map(SourcePosition::Pg)
    }

    /// The log's end.
    pub fn wal_end(&self) -> SourcePosition {
        SourcePosition::Pg(self.db().wal_end)
    }

    /// `table`'s visible rows in key order.
    pub fn rows(&self, table: &TableRef) -> Vec<Row> {
        self.db().visible(table).into_values().collect()
    }

    fn take_fault(&self, method: Method) -> Result<(), SourceError> {
        let mut db = self.db();
        match db.faults.iter().position(|(m, _)| *m == method) {
            Some(i) => Err(db.faults.remove(i).1),
            None => Ok(()),
        }
    }
}

impl Db {
    fn create(&mut self, schema: TableSchema) {
        let schema = schema.with_source_id(self.next_source_id);
        self.next_source_id += 1;
        self.rows.insert(schema.table().clone(), BTreeMap::new());
        self.catalog.insert(schema.table().clone(), schema);
    }

    fn schema(&self, table: &TableRef) -> TableSchema {
        self.catalog
            .get(table)
            .unwrap_or_else(|| panic!("no table {table}"))
            .clone()
    }

    fn held(&self, table: &TableRef, key: &Key) -> bool {
        self.stalled
            .values()
            .any(|c| c.iter().any(|(t, k, _)| t == table && k == key))
    }

    fn commit(&mut self, xid: Option<u64>, txn: FakeTxn, stalled: bool) -> u64 {
        let xid = xid.unwrap_or_else(|| {
            self.next_xid += 1;
            self.next_xid - 1
        });
        let ts = self.next_ts;
        self.next_ts += 1_000;
        let mut undo = vec![];
        let mut changes = vec![];
        let mut next = self.wal_end.0;
        let mut seq = || {
            next += 0x10;
            SourceSeq::Pg(Lsn(next))
        };
        for op in txn.ops {
            let mut write =
                |db: &mut Self, table: &TableRef, key: Key, row: Option<Row>| -> Option<Row> {
                    assert!(
                        !db.held(table, &key),
                        "{table}: a stalled transaction holds {key:?}; PostgreSQL would block"
                    );
                    let rows = db.rows.get_mut(table).expect("a table");
                    let old = match row {
                        Some(r) => rows.insert(key.clone(), r),
                        None => rows.remove(&key),
                    };
                    undo.push((table.clone(), key, old.clone()));
                    old
                };
            match op {
                FakeOp::Insert(t, row) => {
                    let schema = self.schema(&t);
                    write(self, &t, key_of(&schema, &row), Some(row.clone()));
                    changes.push(self.change(&t, Op::Insert, None, Some(row), seq()));
                }
                FakeOp::Update(t, row) => {
                    let schema = self.schema(&t);
                    let key = key_of(&schema, &row);
                    let stored = self.rows[&t].get(&key).cloned();
                    let merged = Row(row
                        .0
                        .iter()
                        .map(|(n, v)| match v {
                            ColumnValue::Unchanged => (
                                n.clone(),
                                stored
                                    .as_ref()
                                    .and_then(|s| s.get(n).cloned())
                                    .unwrap_or(ColumnValue::Null),
                            ),
                            v => (n.clone(), v.clone()),
                        })
                        .collect());
                    write(self, &t, key, Some(merged));
                    changes.push(self.change(&t, Op::Update, None, Some(row), seq()));
                }
                FakeOp::Move(t, old, row) => {
                    let schema = self.schema(&t);
                    write(self, &t, key_of(&schema, &old), None);
                    write(self, &t, key_of(&schema, &row), Some(row.clone()));
                    changes.push(self.change(&t, Op::Update, Some(old), Some(row), seq()));
                }
                FakeOp::Delete(t, key) => {
                    let schema = self.schema(&t);
                    write(self, &t, key_of(&schema, &key), None);
                    changes.push(self.change(&t, Op::Delete, Some(key), None, seq()));
                }
                FakeOp::Truncate(tables) => {
                    for t in &tables {
                        let keys: Vec<Key> = self.rows[t].keys().cloned().collect();
                        for k in keys {
                            write(self, t, k, None);
                        }
                    }
                    changes.push(WalChange::Truncate(tables, seq()));
                }
            }
        }
        let commit_lsn = Lsn(next + 0x10);
        let commit = Lsn(next + 0x18);
        self.wal_end = commit;
        for c in &mut changes {
            if let WalChange::Change { event, .. } = c {
                event.xid = xid;
                event.commit_ts_micros = ts;
                event.source_pos = SourcePosition::Pg(commit_lsn);
            }
        }
        self.wal.push(WalEntry::Txn {
            xid,
            ts,
            changes,
            commit,
        });
        if stalled {
            self.stalled.insert(xid, undo);
        }
        xid
    }

    fn change(
        &self,
        table: &TableRef,
        op: Op,
        before: Option<Row>,
        after: Option<Row>,
        seq: SourceSeq,
    ) -> WalChange {
        WalChange::Change {
            event: CdcEvent {
                op,
                table: table.clone(),
                before,
                after,
                source_pos: SourcePosition::Pg(Lsn(0)),
                change_seq: seq,
                xid: 0,
                commit_ts_micros: 0,
            },
            columns: self.schema(table).columns().to_vec(),
        }
    }

    /// The rows a new snapshot sees: stalled transactions' changes undone.
    fn visible(&self, table: &TableRef) -> BTreeMap<Key, Row> {
        let mut rows = self.rows.get(table).cloned().unwrap_or_default();
        for changes in self.stalled.values() {
            for (t, key, old) in changes.iter().rev() {
                if t == table {
                    match old {
                        Some(r) => rows.insert(key.clone(), r.clone()),
                        None => rows.remove(key),
                    };
                }
            }
        }
        rows
    }

    fn snapshot(&self) -> XidSnapshot {
        let xip: Vec<u64> = self
            .stalled
            .keys()
            .chain(self.open.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        XidSnapshot {
            xmin: xip.first().copied().unwrap_or(self.next_xid),
            xmax: self.next_xid,
            xip,
        }
    }
}

/// What `augment` and `rescan` compare: columns by name, with their types.
fn shape(columns: &[ColumnSchema]) -> Vec<(String, data_avalanche_core::ColumnValueType)> {
    columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type.clone()))
        .collect()
}

/// The columns the stream carries (`hint`), in its order, as PostgreSQL's `scope` reads them: the
/// stream's types, and the nullability and ordinal of the catalog's column of that name (or the
/// cached one, for a column the catalog dropped since). A column known to neither, or an
/// identifier column the stream doesn't carry, is a guess the table halts on.
fn scoped(
    cached: &TableSchema,
    hint: &[ColumnSchema],
    now: &TableSchema,
) -> Result<TableSchema, SourceError> {
    let table = now.table().clone();
    let mut columns = Vec::with_capacity(hint.len());
    for h in hint {
        let Some(known) = now.column(&h.name).or_else(|| cached.column(&h.name)) else {
            return Err(SourceError::TableHalt {
                table,
                reason: HaltReason::SchemaMismatch {
                    column: h.name.clone(),
                    detail: "the stream carries a column the catalog doesn't have; re-snapshot"
                        .into(),
                },
            });
        };
        columns.push(ColumnSchema {
            nullable: known.nullable,
            source_ordinal: known.source_ordinal,
            ..h.clone()
        });
    }
    let halt = |reason| SourceError::TableHalt {
        table: table.clone(),
        reason,
    };
    if let Some(k) = now
        .identifier()
        .iter()
        .find(|k| !columns.iter().any(|c| &c.name == *k))
    {
        return Err(halt(HaltReason::SchemaMismatch {
            column: k.clone(),
            detail: "the identifier column isn't in the stream's columns".into(),
        }));
    }
    let scoped =
        TableSchema::new(table.clone(), columns, now.identifier().to_vec()).map_err(halt)?;
    Ok(match now.source_id() {
        Some(id) => scoped.with_source_id(id),
        None => scoped,
    })
}

fn diff(cached: &TableSchema, now: &TableSchema) -> Vec<SchemaChange> {
    let mut changes = vec![];
    for c in cached.columns() {
        let Some(n) = now.column(&c.name) else {
            changes.push(SchemaChange::DropColumn(c.name.clone()));
            continue;
        };
        if n.value_type != c.value_type {
            changes.push(SchemaChange::ChangeType {
                column: c.name.clone(),
                from: c.clone(),
                to: n.clone(),
                narrowed: false,
            });
        }
        match (c.nullable, n.nullable) {
            (true, false) => changes.push(SchemaChange::SetNotNull(c.name.clone())),
            (false, true) => changes.push(SchemaChange::DropNotNull(c.name.clone())),
            _ => {}
        }
    }
    for c in now.columns() {
        if cached.column(&c.name).is_none() {
            changes.push(SchemaChange::AddColumn(c.clone()));
        }
    }
    if cached.identifier() != now.identifier() {
        changes.push(SchemaChange::IdentifierChanged);
    }
    changes
}

#[async_trait]
impl SourceConnector for FakeSource {
    async fn preflight(&self) -> Result<Preflight, SourceError> {
        self.take_fault(Method::Preflight)?;
        Ok(Preflight {
            server_version: "fake".into(),
            warnings: vec![],
        })
    }

    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        self.take_fault(Method::ResolveTables)?;
        Ok(ResolvedTables {
            tables: self.db().catalog.values().cloned().collect(),
            ..ResolvedTables::default()
        })
    }

    async fn prepare(
        &self,
        checkpoint: Option<&SourcePosition>,
        _tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        self.take_fault(Method::Prepare)?;
        let mut db = self.db();
        db.prepares.push(checkpoint.cloned());
        let checkpoint = checkpoint.map(lsn);
        let reason = match (checkpoint, db.slot) {
            (None, None) => Some(SnapshotReason::FirstStart),
            (None, Some(_)) => {
                return Err(SourceError::Permanent(
                    "replication slot exists but the pipeline has no state (an orphan)".into(),
                ));
            }
            (Some(_), None) => Some(SnapshotReason::PositionLost),
            (Some(_), Some(_)) if db.slot_lost => Some(SnapshotReason::PositionLost),
            (Some(at), Some(s)) if s > at => Some(SnapshotReason::PositionLost),
            (Some(_), Some(_)) => None,
        };
        Ok(match reason {
            None => Prepared {
                plan: StartPlan::Resume,
                added: std::mem::take(&mut db.added),
                warnings: vec![],
            },
            Some(reason) => {
                db.slot = Some(db.wal_end);
                db.slot_lost = false;
                Prepared {
                    plan: StartPlan::Snapshot {
                        from: SourcePosition::Pg(db.wal_end),
                        reason,
                    },
                    added: vec![],
                    warnings: vec![],
                }
            }
        })
    }

    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
        self.take_fault(Method::ResetPosition)?;
        let mut db = self.db();
        db.slot = Some(db.wal_end);
        db.slot_lost = false;
        Ok(SourcePosition::Pg(db.wal_end))
    }

    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
        self.take_fault(Method::RetainedBytes)?;
        let db = self.db();
        Ok(match db.retained {
            Some(r) => r,
            None => db.slot.map(|s| db.wal_end.0.saturating_sub(s.0)),
        })
    }

    async fn current_position(&self) -> Result<SourcePosition, SourceError> {
        self.take_fault(Method::CurrentPosition)?;
        Ok(SourcePosition::Pg(self.db().wal_end))
    }

    async fn in_flight(
        &self,
    ) -> Result<Option<Vec<data_avalanche_core::InFlightTxn>>, SourceError> {
        self.take_fault(Method::InFlight)?;
        let db = self.db();
        let id = |t: &TableRef| db.catalog.get(t).and_then(TableSchema::source_id);
        let mut out: Vec<InFlightTxn> = db
            .stalled
            .iter()
            .map(|(xid, changes)| InFlightTxn {
                xid: *xid,
                tables: changes
                    .iter()
                    .filter_map(|(t, _, _)| id(t))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            })
            .collect();
        out.extend(db.open.iter().map(|(xid, tables)| InFlightTxn {
            xid: *xid,
            tables: tables.iter().filter_map(id).collect(),
        }));
        Ok(Some(out))
    }

    async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        self.take_fault(Method::ReadChunk)?;
        let (rows, visibility) = {
            let mut db = self.db();
            *db.reads.entry(table.table().clone()).or_default() += 1;
            let Some(now) = db.catalog.get(table.table()) else {
                return Err(SourceError::TableHalt {
                    table: table.table().clone(),
                    reason: HaltReason::TableDropped,
                });
            };
            if shape(now.columns()) != shape(table.columns()) {
                return Err(SourceError::Transient(format!(
                    "{} changed under the snapshot",
                    table.table()
                )));
            }
            let from = after.map(|c| key_of(table, &c.key));
            let rows: Vec<Row> = db
                .visible(table.table())
                .into_iter()
                .filter(|(k, _)| from.as_ref().is_none_or(|f| k > f))
                .take(limit)
                .map(|(_, r)| r)
                .collect();
            (rows, db.snapshot())
        };
        loop {
            let held = self.wake.notified();
            if !self.db().hold_reads {
                break;
            }
            held.await;
        }
        let next = rows.last().map(|r| KeysetCursor {
            key: identifier_values(table, r).expect("a key"),
            collation: None,
        });
        Ok(Chunk {
            rows,
            next,
            visibility: Some(ChunkVisibility::Xids(visibility)),
        })
    }

    async fn fetch_rows(&self, table: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError> {
        *self.db().fetches.entry(table.table().clone()).or_default() += 1;
        self.take_fault(Method::FetchRows)?;
        let visible = self.db().visible(table.table());
        Ok(keys
            .iter()
            .filter_map(|k| visible.get(&key_of(table, k)).cloned())
            .collect())
    }

    async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError> {
        self.take_fault(Method::OpenStream)?;
        self.db().opened.push(from.clone());
        Ok(Box::new(FakeStream {
            source: self.clone(),
            after: lsn(from),
            next_entry: 0,
            pending: VecDeque::new(),
            known: tables
                .iter()
                .map(|t| (t.table().clone(), shape(t.columns())))
                .collect(),
        }))
    }

    async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        self.take_fault(Method::Augment)?;
        let table = cached.table().clone();
        Ok(match self.db().catalog.get(&table) {
            None => (
                SchemaDelta {
                    table,
                    changes: vec![SchemaChange::TableDropped],
                },
                None,
            ),
            Some(now) => {
                let now = match hint {
                    SchemaHint::Columns(columns) => scoped(cached, columns, now)?,
                    SchemaHint::Ddl(_) => now.clone(),
                };
                (
                    SchemaDelta {
                        table,
                        changes: diff(cached, &now),
                    },
                    Some(now),
                )
            }
        })
    }

    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        self.db().rescans += 1;
        self.take_fault(Method::Rescan)?;
        let mut db = self.db();
        let mut report = RescanReport::default();
        for k in known {
            match db.catalog.get(k.table()) {
                Some(now) if now.source_id() != k.source_id() && k.source_id().is_some() => {
                    report.dropped.push(k.table().clone())
                }
                None => report.dropped.push(k.table().clone()),
                Some(now) if shape(now.columns()) != shape(k.columns()) => {
                    report.changed.push(now.clone())
                }
                Some(_) => {}
            }
        }
        let after = db.after_rescan.take();
        drop(db);
        if let Some(Action(f)) = after {
            f(self);
        }
        Ok(report)
    }

    async fn teardown(&self) -> Result<(), SourceError> {
        Ok(())
    }
}

/// The stream of one `open_stream`: the log after its start, then whatever is committed later.
struct FakeStream {
    source: FakeSource,
    after: Lsn,
    next_entry: usize,
    pending: VecDeque<StreamItem>,
    known: BTreeMap<TableRef, Vec<(String, data_avalanche_core::ColumnValueType)>>,
}

impl FakeStream {
    /// Queues the next log entry past the start; false when there is none yet.
    fn fill(&mut self, db: &Db) -> bool {
        while let Some(entry) = db.wal.get(self.next_entry) {
            self.next_entry += 1;
            if entry.end() <= self.after {
                continue;
            }
            match entry {
                WalEntry::KeepAlive(at) => {
                    self.pending
                        .push_back(StreamItem::KeepAlive(SourcePosition::Pg(*at)));
                }
                WalEntry::Txn {
                    xid,
                    ts,
                    changes,
                    commit,
                } => {
                    self.pending.push_back(StreamItem::Begin {
                        xid: *xid,
                        commit_ts_micros: *ts,
                    });
                    for c in changes {
                        match c {
                            WalChange::Change { event, columns } => {
                                let Some(known) = self.known.get_mut(&event.table) else {
                                    continue;
                                };
                                let now = shape(columns);
                                if *known != now {
                                    *known = now;
                                    self.pending.push_back(StreamItem::SchemaSignal {
                                        table: event.table.clone(),
                                        seq: event.change_seq,
                                        hint: SchemaHint::Columns(columns.clone()),
                                    });
                                }
                                self.pending.push_back(StreamItem::Change(event.clone()));
                            }
                            WalChange::Truncate(tables, seq) => {
                                let tables: Vec<TableRef> = tables
                                    .iter()
                                    .filter(|t| self.known.contains_key(t))
                                    .cloned()
                                    .collect();
                                if !tables.is_empty() {
                                    self.pending
                                        .push_back(StreamItem::Truncate { tables, seq: *seq });
                                }
                            }
                        }
                    }
                    self.pending
                        .push_back(StreamItem::Commit(TransactionBoundary {
                            xid: *xid,
                            commit_pos: SourcePosition::Pg(*commit),
                            commit_ts_micros: *ts,
                        }));
                }
            }
            return true;
        }
        false
    }
}

#[async_trait]
impl ChangeStream for FakeStream {
    async fn next(&mut self) -> Result<StreamItem, SourceError> {
        let source = self.source.clone();
        loop {
            let wake = source.wake.notified();
            {
                let mut db = source.db();
                if db.slot_lost {
                    return Err(SourceError::PositionUnreachable {
                        position: self.after.to_string(),
                        detail: "the fake slot lost its position".into(),
                    });
                }
                if let Some(item) = self.pending.pop_front() {
                    return Ok(item);
                }
                if let Some(i) = db.faults.iter().position(|(m, _)| *m == Method::Next) {
                    return Err(db.faults.remove(i).1);
                }
                if self.fill(&db) {
                    continue;
                }
            }
            wake.await;
        }
    }

    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError> {
        let mut db = self.source.db();
        let at = lsn(durable);
        db.slot = Some(db.slot.map_or(at, |s| s.max(at)));
        db.acknowledged.push(durable.clone());
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), SourceError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnValue as Val, ColumnValueType as V};

    use super::*;

    fn t() -> TableRef {
        TableRef::new("public", "t")
    }

    fn schema() -> TableSchema {
        TableSchema::new(
            t(),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("v", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn row(id: i64, v: &str) -> Row {
        Row(vec![
            ("id".into(), Val::Int64(id)),
            ("v".into(), Val::Text(v.into())),
        ])
    }

    fn key(id: i64) -> Row {
        Row(vec![("id".into(), Val::Int64(id))])
    }

    async fn items(s: &mut Box<dyn ChangeStream>, n: usize) -> Vec<StreamItem> {
        let mut out = vec![];
        for _ in 0..n {
            out.push(s.next().await.unwrap());
        }
        out
    }

    /// A `Columns` hint behind the catalog, as PostgreSQL's `scope` reads it: the stream's types
    /// and columns, the catalog's nullability; a column known to neither halts the table.
    #[tokio::test]
    async fn augment_reads_a_columns_hint_as_postgresql_does() {
        let src = FakeSource::new(vec![schema()]);
        let cached = src.schema(&t());
        let ahead = TableSchema::new(
            t(),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("v", V::Text, "text").not_null(),
                ColumnSchema::new("w", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        src.alter(&t(), ahead);
        let hint = vec![
            ColumnSchema::new("id", V::Int64, "bigint"),
            ColumnSchema::new("v", V::Text, "character varying(9)"),
        ];
        let (delta, now) = src
            .augment(&cached, &SchemaHint::Columns(hint))
            .await
            .unwrap();
        let now = now.unwrap();
        let got: Vec<(&str, &str, bool)> = now
            .columns()
            .iter()
            .map(|c| (c.name.as_str(), c.source_type.as_str(), c.nullable))
            .collect();
        assert_eq!(
            got,
            [
                ("id", "bigint", false),
                ("v", "character varying(9)", false)
            ],
            "not `w`, the stream's type, the catalog's nullability"
        );
        assert_eq!(now.source_id(), cached.source_id());
        assert_eq!(delta.changes, [SchemaChange::SetNotNull("v".into())]);
        let unknown = vec![
            ColumnSchema::new("id", V::Int64, "bigint"),
            ColumnSchema::new("q", V::Text, "text"),
        ];
        match src.augment(&cached, &SchemaHint::Columns(unknown)).await {
            Err(SourceError::TableHalt { reason, .. }) => {
                assert_eq!(reason.code(), "SCHEMA_MISMATCH")
            }
            other => panic!("{other:?}"),
        }
        let keyless = vec![ColumnSchema::new("v", V::Text, "text")];
        match src.augment(&cached, &SchemaHint::Columns(keyless)).await {
            Err(SourceError::TableHalt {
                reason: HaltReason::SchemaMismatch { column, .. },
                ..
            }) => assert_eq!(column, "id", "the table halts, not the worker"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_first_start_snapshots_then_streams_from_the_slot() {
        let src = FakeSource::new(vec![schema()]);
        src.load(&t(), vec![row(1, "a")]);
        let p = src.prepare(None, &[t()]).await.unwrap();
        let StartPlan::Snapshot { from, reason } = p.plan else {
            panic!("{p:?}")
        };
        assert_eq!(reason, SnapshotReason::FirstStart);
        assert_eq!(src.slot(), Some(from.clone()));
        let tables = src.resolve_tables().await.unwrap().tables;
        assert_eq!(tables[0].source_id(), Some(1000));
        let mut s = src.open_stream(&from, &tables).await.unwrap();
        src.commit(FakeTxn::new().insert(&t(), row(2, "b")));
        let got = items(&mut s, 3).await;
        assert!(matches!(got[0], StreamItem::Begin { xid: 700, .. }));
        let StreamItem::Change(e) = &got[1] else {
            panic!("{got:?}")
        };
        assert_eq!(
            (e.op, e.after.clone(), e.xid),
            (Op::Insert, Some(row(2, "b")), 700)
        );
        let StreamItem::Commit(b) = &got[2] else {
            panic!()
        };
        // PostgreSQL: a change carries its commit record's start, the Commit the record's end.
        let (SourcePosition::Pg(at), SourcePosition::Pg(end)) = (&e.source_pos, &b.commit_pos)
        else {
            panic!("{got:?}")
        };
        assert_eq!(end.0, at.0 + 8);
        let chunk = src.read_chunk(&tables[0], None, 10).await.unwrap();
        assert_eq!(chunk.rows, vec![row(1, "a"), row(2, "b")]);
        s.acknowledge(&b.commit_pos).await.unwrap();
        assert_eq!(src.slot(), Some(b.commit_pos.clone()));
        let resumed = src.prepare(Some(&b.commit_pos), &[t()]).await.unwrap();
        assert_eq!(resumed.plan, StartPlan::Resume);
    }

    #[tokio::test]
    async fn a_stalled_commit_streams_before_chunks_see_it() {
        let src = FakeSource::new(vec![schema()]);
        src.load(&t(), vec![row(1, "a")]);
        let tables = src.resolve_tables().await.unwrap().tables;
        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
        let xid = src.commit_stalled(FakeTxn::new().update(&t(), row(1, "b")));
        let got = items(&mut s, 3).await;
        assert!(matches!(&got[2], StreamItem::Commit(b) if b.xid == xid));
        let chunk = src.read_chunk(&tables[0], None, 10).await.unwrap();
        assert_eq!(chunk.rows, vec![row(1, "a")], "the old row");
        let Some(ChunkVisibility::Xids(snap)) = &chunk.visibility else {
            panic!()
        };
        assert!(!snap.sees(xid));
        let in_flight = src.in_flight().await.unwrap().unwrap();
        assert_eq!(
            in_flight,
            vec![InFlightTxn {
                xid,
                tables: vec![1000]
            }]
        );
        src.release(xid);
        let chunk = src.read_chunk(&tables[0], None, 10).await.unwrap();
        assert_eq!(chunk.rows, vec![row(1, "b")]);
        let Some(ChunkVisibility::Xids(snap)) = &chunk.visibility else {
            panic!()
        };
        assert!(snap.sees(xid));
    }

    #[tokio::test]
    async fn changes_keys_truncates_and_unchanged_values_stream_as_sent() {
        let src = FakeSource::new(vec![schema()]);
        let tables = src.resolve_tables().await.unwrap().tables;
        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
        src.commit(
            FakeTxn::new()
                .insert(&t(), row(1, "a"))
                .update(
                    &t(),
                    Row(vec![
                        ("id".into(), Val::Int64(1)),
                        ("v".into(), Val::Unchanged),
                    ]),
                )
                .update_key(&t(), key(1), row(2, "c"))
                .delete(&t(), key(2))
                .truncate(&[t()]),
        );
        let got = items(&mut s, 7).await;
        let ops: Vec<_> = got
            .iter()
            .filter_map(|i| match i {
                StreamItem::Change(e) => Some(e.op),
                _ => None,
            })
            .collect();
        assert_eq!(ops, [Op::Insert, Op::Update, Op::Update, Op::Delete]);
        let StreamItem::Change(e) = &got[2] else {
            panic!()
        };
        assert_eq!(e.after.as_ref().unwrap().get("v"), Some(&Val::Unchanged));
        // REPLICA IDENTITY DEFAULT: no old tuple unless the key changes.
        assert_eq!(e.before, None);
        let StreamItem::Change(e) = &got[3] else {
            panic!()
        };
        assert_eq!(e.before, Some(key(1)));
        assert!(matches!(&got[5], StreamItem::Truncate { tables, .. } if tables == &[t()]));
        assert!(src.rows(&t()).is_empty());
    }

    #[tokio::test]
    async fn an_alter_signals_before_the_next_change() {
        let src = FakeSource::new(vec![schema()]);
        let tables = src.resolve_tables().await.unwrap().tables;
        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
        let wider = TableSchema::new(
            t(),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("v", V::Text, "text"),
                ColumnSchema::new("n", V::Int32, "integer"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        src.alter(&t(), wider.clone());
        let mut r = row(1, "a");
        r.0.push(("n".into(), Val::Int32(5)));
        src.commit(FakeTxn::new().insert(&t(), r));
        let got = items(&mut s, 4).await;
        let StreamItem::SchemaSignal { table, hint, .. } = &got[1] else {
            panic!("{got:?}")
        };
        assert_eq!(table, &t());
        assert!(matches!(hint, SchemaHint::Columns(c) if c.len() == 3));
        let (delta, now) = src.augment(&tables[0], hint).await.unwrap();
        assert!(
            matches!(&delta.changes[..], [SchemaChange::AddColumn(c)] if c.name == "n"),
            "{delta:?}"
        );
        assert_eq!(now.unwrap().source_id(), Some(1000));
        let report = src.rescan(&tables).await.unwrap();
        assert_eq!(report.changed.len(), 1);
    }

    #[tokio::test]
    async fn faults_fire_once_and_a_lost_slot_fails_the_stream() {
        let src = FakeSource::new(vec![schema()]);
        src.fail(Method::ReadChunk, SourceError::Transient("reset".into()));
        let tables = src.resolve_tables().await.unwrap().tables;
        assert!(
            src.read_chunk(&tables[0], None, 1)
                .await
                .unwrap_err()
                .is_transient()
        );
        assert!(src.read_chunk(&tables[0], None, 1).await.is_ok());
        let p = src.prepare(None, &[t()]).await.unwrap();
        let StartPlan::Snapshot { from, .. } = p.plan else {
            panic!()
        };
        let mut s = src.open_stream(&from, &tables).await.unwrap();
        src.lose_slot();
        assert!(matches!(
            s.next().await,
            Err(SourceError::PositionUnreachable { .. })
        ));
        let again = src.prepare(Some(&from), &[t()]).await.unwrap();
        assert!(matches!(
            again.plan,
            StartPlan::Snapshot {
                reason: SnapshotReason::PositionLost,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_held_read_returns_its_earlier_snapshot() {
        let src = FakeSource::new(vec![schema()]);
        src.load(&t(), vec![row(1, "a")]);
        let tables = src.resolve_tables().await.unwrap().tables;
        src.hold_reads();
        let reader = {
            let src = src.clone();
            let t0 = tables[0].clone();
            tokio::spawn(async move { src.read_chunk(&t0, None, 10).await })
        };
        while src.reads(&t()) == 0 {
            tokio::task::yield_now().await;
        }
        let xid = src.commit(FakeTxn::new().update(&t(), row(1, "b")));
        src.release_reads();
        let chunk = reader.await.unwrap().unwrap();
        assert_eq!(chunk.rows, vec![row(1, "a")]);
        let Some(ChunkVisibility::Xids(snap)) = &chunk.visibility else {
            panic!()
        };
        assert!(!snap.sees(xid), "committed after the snapshot");
    }
}
