//! The source connector contract (roadmap §3.6, Data-Flow Phases 0–2): what the pipeline asks of a
//! source engine. Each source crate implements it; nothing here performs I/O.

use std::any::Any;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CdcEvent, ColumnSchema, ColumnValue, HaltReason, Row, SchemaDelta, SourcePosition, SourceSeq,
    TableRef, TableSchema, TransactionBoundary,
};

/// Why a source call failed (Data-Flow §Error Paths). The class decides what the pipeline does.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SourceError {
    /// Lost connection, timeout, server restart, a busy server: discard the uncommitted buffer and
    /// reopen from the checkpoint after a backoff.
    #[error("transient source error: {0}")]
    Transient(String),
    /// A configuration, privilege, server-setting or protocol problem: the pipeline halts until an
    /// operator acts. Retrying would only repeat it.
    #[error("{0}")]
    Permanent(String),
    /// The source can no longer serve the stream from `position` (PostgreSQL WAL removed or slot
    /// invalidated, MySQL binlog purged). Recovery is a new stream position and a re-snapshot
    /// (Postgres_Slot… D-15.3, `Schema_Change…` §8.2).
    #[error("the source can't serve position {position}: {detail}")]
    PositionUnreachable { position: String, detail: String },
    /// One table can't be replicated; the pipeline's other tables continue.
    #[error("table {table}: {reason}")]
    TableHalt { table: TableRef, reason: HaltReason },
    /// Another reader holds the stream position (PostgreSQL: the slot is active for another
    /// walsender, SQLSTATE 55006). A quick restart meets it until the old session's walsender
    /// times out (`wal_sender_timeout`), and so does a takeover from a holder that stopped without
    /// closing its connection. The lease holder ends that reader when the source can
    /// ([`SourceConnector::end_other_reader`], DA-SRC-28), and retries for a bounded time, then
    /// gives up (`Postgres_Slot…` D-15.5).
    #[error("the source's stream position is held by another reader: {0}")]
    Contended(String),
}

impl SourceError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }

    /// The code of a pipeline-scope halt: a [`Self::Permanent`] whose message starts with an
    /// upper-case code and a colon (`BINLOG_PAYLOAD_INVALID: …`, `BINLOG_SOURCE_CHANGED: …`).
    pub fn halt_code(&self) -> Option<&str> {
        let Self::Permanent(m) = self else {
            return None;
        };
        let (code, _) = m.split_once(": ")?;
        let valid = code.contains('_')
            && code.starts_with(|c: char| c.is_ascii_uppercase())
            && code
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
        valid.then_some(code)
    }
}

/// What a pipeline-level preflight found (config §11.2, the rows that halt a pipeline). A failed
/// check is an `Err(SourceError::Permanent)`; warnings don't stop the pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Preflight {
    /// The server's version string, for status and logs.
    pub server_version: String,
    pub warnings: Vec<String>,
}

/// The tracked table set at start (channel A): the configured include/exclude globs resolved
/// against the live catalog, each table checked (config §11.2, the rows that halt a table).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolvedTables {
    /// Tables to replicate, with their baseline schemas, in table order.
    pub tables: Vec<TableSchema>,
    /// Tables that can't be replicated and halt at start: one named without a glob, or one a
    /// glob matched that fails a check a glob doesn't skip it for.
    pub halted: Vec<(TableRef, HaltReason)>,
    /// Tables a glob matched that can't be replicated (no identifier, a partitioned parent, an
    /// unlogged table): each is left out with a warning.
    pub skipped: Vec<(TableRef, String)>,
    /// Pipeline-level warnings found while resolving (V-079 unconstrained numerics, unknown types
    /// read as text).
    pub warnings: Vec<String>,
}

/// Why streaming starts with a snapshot of every table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotReason {
    /// No pipeline state and no stream position at the source: the first start.
    FirstStart,
    /// The pipeline has state, but the source lost its stream position (PostgreSQL: the slot is
    /// gone or invalidated). Warn: data was at risk of a retention gap (D-15.3).
    PositionLost,
    /// No pipeline state, but the source kept a stream position: `on_orphan: adopt_resnapshot`
    /// took it over (D-15.1).
    AdoptedOrphan,
}

/// How streaming starts (Postgres_Slot… §1, the slot state machine).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartPlan {
    /// Stream from the pipeline's checkpoint; no snapshot.
    Resume,
    /// Snapshot every table, then stream from `from`, the source's new consistent position.
    Snapshot {
        from: SourcePosition,
        reason: SnapshotReason,
    },
}

/// The source server a stream position was read from (MySQL / MariaDB). A binlog file and offset
/// mean something only on the server that wrote them: in file/position mode a start against
/// another server halts the pipeline (`BINLOG_SOURCE_CHANGED`, Data-Flow Phase 2b). Its JSON is
/// the `server` of `replication/position.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "engine", rename_all = "lowercase", deny_unknown_fields)]
pub enum ServerIdentity {
    Mysql { server_uuid: String },
    Mariadb { server_id: u32 },
}

impl fmt::Display for ServerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mysql { server_uuid } => write!(f, "MySQL server {server_uuid}"),
            Self::Mariadb { server_id } => write!(f, "MariaDB server_id {server_id}"),
        }
    }
}

/// The pipeline's durable stream position, as [`SourceConnector::prepare`] gets it: where the
/// stream resumes, and the server it was read from when the source records one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub position: SourcePosition,
    pub server: Option<ServerIdentity>,
}

impl From<SourcePosition> for Checkpoint {
    /// A position with no recorded server (PostgreSQL).
    fn from(position: SourcePosition) -> Self {
        Self {
            position,
            server: None,
        }
    }
}

/// What [`SourceConnector::prepare`] decided, and what it noticed on the way (publication
/// membership drift under `slot_management: require_existing`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    pub plan: StartPlan,
    /// Under [`StartPlan::Resume`], the tracked tables the source began publishing at this start
    /// (PostgreSQL: added to the publication). Their changes before now never reached the stream,
    /// so each needs a snapshot, whether it is new to the pipeline or was dropped from the
    /// publication behind its back. Empty when the plan snapshots every table.
    pub added: Vec<TableRef>,
    pub warnings: Vec<String>,
    /// The server the stream reads, recorded with the checkpoint so the next start can check it
    /// (MySQL / MariaDB). `None` for a source that records none (PostgreSQL).
    pub server: Option<ServerIdentity>,
}

/// A keyset cursor (Snapshot_Resumability §2): the identifier values of the last row read, in
/// identifier order, and the collation the text columns were ordered by.
#[derive(Debug, Clone, PartialEq)]
pub struct KeysetCursor {
    pub key: Row,
    /// The collations the text identifier columns were ordered in, comma-joined in identifier
    /// order (PostgreSQL: a column's own deterministic collation, else `"C"`); `None` when no
    /// identifier column is text.
    pub collation: Option<String>,
}

/// One snapshot chunk: up to `limit` rows in identifier order after the cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub rows: Vec<Row>,
    /// The cursor after the last row; `None` when the chunk is empty (the table is done).
    pub next: Option<KeysetCursor>,
    /// Which source transactions the rows reflect, so the pipeline can order them against the
    /// stream (DA-SNAP-9). `None`: the source can't say, and the pipeline falls back to the
    /// watermark window.
    pub visibility: Option<ChunkVisibility>,
}

/// What a chunk's rows reflect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkVisibility {
    /// PostgreSQL: the MVCC snapshot the chunk was read in (`pg_current_snapshot()` inside its
    /// REPEATABLE READ transaction). A transaction whose commit the stream already delivered can
    /// still be invisible to it: a synchronous commit waiting for its standby is decoded before
    /// it is visible (measured on PostgreSQL 18).
    Xids(XidSnapshot),
}

/// A PostgreSQL snapshot (`pg_snapshot`): full 64-bit transaction ids (`xid8`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XidSnapshot {
    /// The oldest transaction still in progress; every one before it has completed.
    pub xmin: u64,
    /// One past the newest completed transaction; it and every later one hadn't completed.
    pub xmax: u64,
    /// The transactions in `xmin..xmax` still in progress.
    pub xip: Vec<u64>,
}

impl XidSnapshot {
    /// True when transaction `xid` had completed (committed or aborted) when the snapshot was
    /// taken: a committed one's changes are in the rows. `xid` may be a full id or the 32-bit
    /// one the stream carries, which is placed in the epoch that puts it nearest to `xmax`.
    pub fn sees(&self, xid: u64) -> bool {
        let xid = self.full(xid);
        xid < self.xmin || (xid < self.xmax && !self.xip.contains(&xid))
    }

    fn full(&self, xid: u64) -> u64 {
        const EPOCH: u64 = 1 << 32;
        const HALF: u64 = 1 << 31;
        if xid >= EPOCH {
            return xid;
        }
        let candidate = (self.xmax & !(EPOCH - 1)) | xid;
        if candidate > self.xmax.saturating_add(HALF) && candidate >= EPOCH {
            candidate - EPOCH
        } else if candidate.saturating_add(HALF) < self.xmax {
            candidate + EPOCH
        } else {
            candidate
        }
    }
}

/// A transaction in progress at the source ([`SourceConnector::in_flight`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightTxn {
    /// Its id, as [`StreamItem::Begin`] carries it.
    pub xid: u64,
    /// The tables it holds a write lock on, by [`TableSchema::source_id`].
    pub tables: Vec<u64>,
}

/// What an in-band schema signal carries (`Schema_Change…` §3 channel B). It says when and where;
/// [`SourceConnector::augment`] (channel C) says what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaHint {
    /// The columns the stream now carries for the table, in order, as the source decodes them
    /// (PostgreSQL Relation message). `nullable` is unknown in-band and set to `true`;
    /// `source_ordinal` is `None`.
    Columns(Vec<ColumnSchema>),
    /// A DDL statement on the table (MySQL `QUERY_EVENT`).
    Ddl(String),
}

/// One item of the change stream, in source order. A transaction is `Begin`, its `Change`,
/// `Truncate`, `SchemaSignal` and `TableHalt` items, then `Commit`.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamItem {
    /// A transaction starts. PostgreSQL knows the commit position only at `Commit`.
    Begin {
        xid: u64,
        commit_ts_micros: i64,
    },
    Change(CdcEvent),
    Truncate {
        tables: Vec<TableRef>,
        seq: SourceSeq,
    },
    /// The table's shape changed at `seq`; the changes after it carry the new shape.
    SchemaSignal {
        table: TableRef,
        seq: SourceSeq,
        hint: SchemaHint,
    },
    /// A change of the table at `seq` can't be decoded (a `NaN` in a decimal column, a value
    /// beyond its type): the table halts, the stream continues.
    TableHalt {
        table: TableRef,
        seq: SourceSeq,
        reason: HaltReason,
    },
    Commit(TransactionBoundary),
    /// The source's position while no transaction is open (PostgreSQL keep-alive).
    KeepAlive(SourcePosition),
}

/// The tables channel D found changed since `known` (`Schema_Change…` §3).
/// The log the source keeps for the pipeline, read every heartbeat (PostgreSQL: the slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedLog {
    /// Bytes kept on the source's disk (PostgreSQL: the WAL from the slot's `restart_lsn`, the
    /// `max_wal_lag_bytes` guard).
    pub bytes: u64,
    /// A stream reads it now (PostgreSQL: the slot is `active`). A session whose stream is open
    /// but which reads `false` has lost its stream without hearing of it (DA-SRC-16).
    pub streamed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RescanReport {
    /// Tables gone from the source catalog: each halts with `TABLE_DROPPED`.
    pub dropped: Vec<TableRef>,
    /// Tables whose catalog schema differs from the known one: the current schemas. The stream's
    /// signals apply changes in order; the pipeline applies one of these only once the stream has
    /// passed the position after the rescan and no signal did it (an idle table).
    pub changed: Vec<TableSchema>,
    /// Tables that can't be described any more (identifier lost, …).
    pub halted: Vec<(TableRef, HaltReason)>,
}

/// A source engine (roadmap §3.6). Every method is a short call on a regular connection except
/// [`Self::open_stream`].
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// The pipeline-level start checks (config §11.2).
    async fn preflight(&self) -> Result<Preflight, SourceError>;

    /// Channel A: the configured table set, resolved and checked.
    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError>;

    /// An on-demand snapshot's tables, by exact name, whatever the config's patterns include
    /// (Data-Flow §On-Demand Snapshot): `tables` are the ones to read; `halted` the ones the
    /// source can't replicate (no identifier, unlogged, …); `skipped` a name the source doesn't
    /// have, or one that isn't a table of its own (a partitioned parent). The config's table-set
    /// checks don't apply: the tables needn't be the pipeline's.
    async fn describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables, SourceError>;

    /// Prepares the stream position for a start (PostgreSQL: the publication, then the slot state
    /// machine; MySQL: the checkpoint's server and whether its binlog is still there).
    /// `checkpoint` is the pipeline's durable position, `None` without state; `tables` is the
    /// tracked set.
    async fn prepare(
        &self,
        checkpoint: Option<&Checkpoint>,
        tables: &[TableRef],
    ) -> Result<Prepared, SourceError>;

    /// Replaces a lost stream position after [`SourceError::PositionUnreachable`] (D-15.3): the
    /// new consistent position to snapshot from and stream after, with the server it was read
    /// from when the source records one.
    async fn reset_position(&self) -> Result<Checkpoint, SourceError>;

    /// The log the source keeps for the pipeline. `None` when the source keeps nothing for it.
    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError>;

    /// The source's current position: a snapshot chunk's low and high watermarks, and the point
    /// the stream must pass before a rescan's change is applied.
    async fn current_position(&self) -> Result<SourcePosition, SourceError>;

    /// The transactions in progress now, each with the tables it writes (DA-SNAP-9). Read when a
    /// stream session opens: one of them may have committed before the session and be applied
    /// to the sink already while still invisible to a chunk (a synchronous commit waiting for its
    /// standby), so a table's chunks wait until none of them writes it. `None` for a source whose
    /// chunks carry no [`ChunkVisibility`].
    async fn in_flight(&self) -> Result<Option<Vec<InFlightTxn>>, SourceError> {
        Ok(None)
    }

    /// Up to `limit` rows of `table` after `after`, in identifier order.
    async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError>;

    /// The current rows with these identifier values (`toast_strategy: re_query`). A key with no
    /// row is left out.
    async fn fetch_rows(&self, table: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError>;

    /// Opens the change stream after `from`'s position, for `tables` as of that position. A
    /// source that records the server (MySQL file/position mode) refuses another one.
    async fn open_stream(
        &self,
        from: &Checkpoint,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError>;

    /// Ends the session that holds the stream position for another reader, when the source can
    /// tell it is the pipeline's own: a holder that stopped without closing its connection, or
    /// this node's own dropped session (DA-SRC-28). Called by the lease holder only, after a
    /// [`SourceError::Contended`]. `Ok(true)` when one was ended. A source whose server ends the
    /// old reader itself (MySQL: a new dump with the same server id) has none to end.
    async fn end_other_reader(&self) -> Result<bool, SourceError> {
        Ok(false)
    }

    /// Channel C: the change a schema signal implicates, against the `cached` as-of-position
    /// schema, scoped to the hint (`Schema_Change…` §0). Returns the delta and the new schema
    /// (`None` when the table was dropped).
    async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError>;

    /// Channel D: one batched catalog read of the tracked tables.
    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError>;

    /// Removes what the source created for the pipeline (PostgreSQL `auto_cleanup`: the slot and
    /// the publication). Called when a pipeline is deleted.
    async fn teardown(&self) -> Result<(), SourceError>;

    /// Takes a reload's settings while the source runs (config §1.2, class R): `pipeline` is
    /// the reloaded pipeline, a `data_avalanche_config::Pipeline`, which this crate doesn't
    /// name. A setting read when a connection opens, at a prepare, a resolve or a teardown is
    /// used from the next one; an open stream keeps what it opened with. A source with nothing
    /// to take keeps this default, which ignores it.
    fn reconfigure(&self, pipeline: &dyn Any) {
        let _ = pipeline;
    }
}

/// The change stream of one [`SourceConnector::open_stream`] call.
#[async_trait]
pub trait ChangeStream: Send {
    /// The next item. An error ends the stream; reopen from the checkpoint. Cancel-safe: the
    /// event loop runs it in `tokio::select!` (Data-Flow Phase 2a), and a call dropped before it
    /// returns loses nothing.
    async fn next(&mut self) -> Result<StreamItem, SourceError>;

    /// Tells the source that everything up to `durable` is committed to the sink, so it can free
    /// what it keeps for it (PostgreSQL: the slot's `confirmed_flush_lsn`). MySQL: nothing.
    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError>;

    /// Ends the stream cleanly.
    async fn close(self: Box<Self>) -> Result<(), SourceError>;
}

/// The identifier values of `row`, in `schema`'s identifier order: the key a cursor or
/// [`SourceConnector::fetch_rows`] carries. `None` when a value is missing or NULL.
pub fn identifier_values(schema: &TableSchema, row: &Row) -> Option<Row> {
    let mut key = Vec::with_capacity(schema.identifier().len());
    for column in schema.identifier() {
        match row.get(column) {
            None | Some(ColumnValue::Null | ColumnValue::Unchanged) => return None,
            Some(v) => key.push((column.clone(), v.clone())),
        }
    }
    Some(Row(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::Lsn;
    use crate::{ColumnValueType, Op};

    fn schema() -> TableSchema {
        TableSchema::new(
            TableRef::new("public", "t"),
            vec![
                ColumnSchema::new("a", ColumnValueType::Int64, "bigint").not_null(),
                ColumnSchema::new("b", ColumnValueType::Text, "text").not_null(),
                ColumnSchema::new("v", ColumnValueType::Text, "text"),
            ],
            vec!["b".into(), "a".into()],
        )
        .unwrap()
    }

    #[test]
    fn identifier_values_follow_the_identifier_order() {
        let row = Row(vec![
            ("a".into(), ColumnValue::Int64(1)),
            ("b".into(), ColumnValue::Text("x".into())),
            ("v".into(), ColumnValue::Null),
        ]);
        assert_eq!(
            identifier_values(&schema(), &row),
            Some(Row(vec![
                ("b".into(), ColumnValue::Text("x".into())),
                ("a".into(), ColumnValue::Int64(1)),
            ]))
        );
    }

    #[test]
    fn identifier_values_need_every_key_column() {
        let missing = Row(vec![("a".into(), ColumnValue::Int64(1))]);
        assert_eq!(identifier_values(&schema(), &missing), None);
        let null = Row(vec![
            ("a".into(), ColumnValue::Int64(1)),
            ("b".into(), ColumnValue::Null),
        ]);
        assert_eq!(identifier_values(&schema(), &null), None);
    }

    #[test]
    fn a_pipeline_halt_names_its_code() {
        let code = |m: &str| {
            SourceError::Permanent(m.into())
                .halt_code()
                .map(str::to_owned)
        };
        assert_eq!(
            code("BINLOG_PAYLOAD_INVALID: the compressed transaction at f:4 yielded 0 bytes"),
            Some("BINLOG_PAYLOAD_INVALID".into())
        );
        assert_eq!(
            code("BINLOG_SOURCE_CHANGED: binlog position f:4 was recorded on …"),
            Some("BINLOG_SOURCE_CHANGED".into())
        );
        for plain in [
            "MySQL 8.0.36: the source needs MySQL 8.4 or newer",
            "the binary log is off (log_bin); the source reads the binlog",
            "ERROR: no colon space",
            "binlog_format is MIXED: lower case",
            "",
        ] {
            assert_eq!(code(plain), None, "{plain}");
        }
        assert_eq!(
            SourceError::Transient("BINLOG_PAYLOAD_INVALID: x".into()).halt_code(),
            None,
            "only a permanent error halts the pipeline"
        );
    }

    #[test]
    fn only_transient_errors_are_retried() {
        assert!(SourceError::Transient("reset".into()).is_transient());
        assert!(!SourceError::Permanent("denied".into()).is_transient());
        assert!(
            !SourceError::PositionUnreachable {
                position: "0/10".into(),
                detail: "removed".into()
            }
            .is_transient()
        );
        let halt = SourceError::TableHalt {
            table: TableRef::new("public", "t"),
            reason: HaltReason::TableDropped,
        };
        assert!(!halt.is_transient());
        assert_eq!(
            halt.to_string(),
            "table public.t: the table was dropped at the source; the target is kept, and a re-created table needs an operator re-snapshot"
        );
    }

    /// A connector that serves one transaction: the traits are object-safe and usable as
    /// `Box<dyn …>`.
    struct OneTxn;

    struct OneTxnStream(Vec<StreamItem>);

    #[async_trait]
    impl ChangeStream for OneTxnStream {
        async fn next(&mut self) -> Result<StreamItem, SourceError> {
            if self.0.is_empty() {
                return Err(SourceError::Transient("end of test stream".into()));
            }
            Ok(self.0.remove(0))
        }

        async fn acknowledge(&mut self, _: &SourcePosition) -> Result<(), SourceError> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<(), SourceError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SourceConnector for OneTxn {
        async fn preflight(&self) -> Result<Preflight, SourceError> {
            Ok(Preflight::default())
        }
        async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
            Ok(ResolvedTables {
                tables: vec![schema()],
                ..ResolvedTables::default()
            })
        }
        async fn describe_tables(&self, _: &[TableRef]) -> Result<ResolvedTables, SourceError> {
            Ok(ResolvedTables::default())
        }
        async fn prepare(
            &self,
            checkpoint: Option<&Checkpoint>,
            _: &[TableRef],
        ) -> Result<Prepared, SourceError> {
            let plan = match checkpoint {
                Some(_) => StartPlan::Resume,
                None => StartPlan::Snapshot {
                    from: SourcePosition::Pg(Lsn(0x10)),
                    reason: SnapshotReason::FirstStart,
                },
            };
            Ok(Prepared {
                plan,
                added: vec![],
                warnings: vec![],
                server: None,
            })
        }
        async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
            Ok(None)
        }
        async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
            Ok(SourcePosition::Pg(Lsn(0x20)).into())
        }
        async fn current_position(&self) -> Result<SourcePosition, SourceError> {
            Ok(SourcePosition::Pg(Lsn(0x30)))
        }
        async fn read_chunk(
            &self,
            _: &TableSchema,
            _: Option<&KeysetCursor>,
            _: usize,
        ) -> Result<Chunk, SourceError> {
            Ok(Chunk {
                rows: vec![],
                next: None,
                visibility: None,
            })
        }
        async fn fetch_rows(&self, _: &TableSchema, _: &[Row]) -> Result<Vec<Row>, SourceError> {
            Ok(vec![])
        }
        async fn open_stream(
            &self,
            _: &Checkpoint,
            _: &[TableSchema],
        ) -> Result<Box<dyn ChangeStream>, SourceError> {
            let table = TableRef::new("public", "t");
            let commit_pos = SourcePosition::Pg(Lsn(0x48));
            let after = Row(vec![
                ("a".into(), ColumnValue::Int64(1)),
                ("b".into(), ColumnValue::Text("x".into())),
                ("v".into(), ColumnValue::Null),
            ]);
            Ok(Box::new(OneTxnStream(vec![
                StreamItem::Begin {
                    xid: 7,
                    commit_ts_micros: 1,
                },
                StreamItem::Change(CdcEvent {
                    op: Op::Insert,
                    table,
                    before: None,
                    after: Some(after),
                    source_pos: SourcePosition::Pg(Lsn(0x40)),
                    change_seq: SourceSeq::Pg(Lsn(0x38)),
                    xid: 7,
                    commit_ts_micros: 1,
                }),
                StreamItem::Commit(TransactionBoundary {
                    xid: 7,
                    commit_pos,
                    commit_ts_micros: 1,
                }),
            ])))
        }
        async fn augment(
            &self,
            cached: &TableSchema,
            _: &SchemaHint,
        ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
            Ok((
                SchemaDelta {
                    table: cached.table().clone(),
                    changes: vec![],
                },
                Some(cached.clone()),
            ))
        }
        async fn rescan(&self, _: &[TableSchema]) -> Result<RescanReport, SourceError> {
            Ok(RescanReport::default())
        }
        async fn teardown(&self) -> Result<(), SourceError> {
            Ok(())
        }
    }

    #[test]
    fn the_traits_work_as_trait_objects() {
        let source: Box<dyn SourceConnector> = Box::new(OneTxn);
        source.reconfigure(&"a source with nothing to take ignores it");
        let prepared = block_on(source.prepare(None, &[])).unwrap();
        assert!(matches!(
            prepared.plan,
            StartPlan::Snapshot {
                reason: SnapshotReason::FirstStart,
                ..
            }
        ));
        let from = Checkpoint::from(SourcePosition::Pg(Lsn(0x10)));
        let mut stream = block_on(source.open_stream(&from, &[schema()])).unwrap();
        assert!(matches!(
            block_on(stream.next()).unwrap(),
            StreamItem::Begin { xid: 7, .. }
        ));
        let StreamItem::Change(e) = block_on(stream.next()).unwrap() else {
            panic!("a change")
        };
        e.validate().unwrap();
        let StreamItem::Commit(b) = block_on(stream.next()).unwrap() else {
            panic!("a commit")
        };
        block_on(stream.acknowledge(&b.commit_pos)).unwrap();
        assert!(block_on(stream.next()).unwrap_err().is_transient());
        block_on(stream.close()).unwrap();
        assert_eq!(
            block_on(source.in_flight()).unwrap(),
            None,
            "a source without chunk visibility"
        );
    }

    #[test]
    fn a_snapshot_sees_the_transactions_completed_before_it() {
        // `pg_current_snapshot()` = `100:105:100,103`.
        let s = XidSnapshot {
            xmin: 100,
            xmax: 105,
            xip: vec![100, 103],
        };
        for (xid, seen) in [
            (99, true),
            (100, false),
            (101, true),
            (103, false),
            (104, true),
            (105, false),
            (106, false),
        ] {
            assert_eq!(s.sees(xid), seen, "{xid}");
        }
    }

    #[test]
    fn a_32_bit_xid_is_placed_next_to_the_snapshot() {
        // Epoch 1: an xid8 is 2^32 + the 32-bit xid the stream sends.
        let e = 1u64 << 32;
        let s = XidSnapshot {
            xmin: e + 10,
            xmax: e + 20,
            xip: vec![e + 15],
        };
        assert!(s.sees(5), "epoch 1's 5 is below xmin");
        assert!(!s.sees(15));
        assert!(!s.sees(25));
        assert!(!s.sees(e + 15), "a full xid is taken as it is");
        assert!(s.sees(e + 16));
        // Just past the wraparound: the stream's 0xFFFF_FFF0 is epoch 0's.
        let w = XidSnapshot {
            xmin: e + 1,
            xmax: e + 3,
            xip: vec![],
        };
        assert!(w.sees(0xFFFF_FFF0));
        assert!(!w.sees(3));
    }

    #[test]
    fn a_server_identity_names_its_engine_and_keeps_its_json() {
        let mysql = ServerIdentity::Mysql {
            server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
        };
        let mariadb = ServerIdentity::Mariadb { server_id: 7 };
        assert_eq!(
            mysql.to_string(),
            "MySQL server 3e11fa47-71ca-11e1-9e33-c80aa9429562"
        );
        assert_eq!(mariadb.to_string(), "MariaDB server_id 7");
        let json = serde_json::to_string(&mariadb).unwrap();
        assert_eq!(json, r#"{"engine":"mariadb","server_id":7}"#);
        assert_eq!(
            serde_json::from_str::<ServerIdentity>(&json).unwrap(),
            mariadb
        );
        assert!(
            serde_json::from_str::<ServerIdentity>(r#"{"engine":"mysql","server_id":7}"#).is_err(),
            "a field of the other engine is refused"
        );
    }

    #[test]
    fn a_position_alone_is_a_checkpoint_without_a_server() {
        let c = Checkpoint::from(SourcePosition::Pg(Lsn(0x10)));
        assert_eq!(c.position, SourcePosition::Pg(Lsn(0x10)));
        assert_eq!(c.server, None);
    }

    #[test]
    fn contended_is_retried_by_the_pipeline_not_as_transient() {
        let e = SourceError::Contended("replication slot \"s\" is active".into());
        assert!(!e.is_transient());
        assert_eq!(
            e.to_string(),
            "the source's stream position is held by another reader: replication slot \"s\" is active"
        );
    }

    /// Polls a future that never waits (the test connector's methods are all ready at once).
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        use std::task::{Context, Poll, Waker};
        let mut f = std::pin::pin!(f);
        match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("the test connector never waits"),
        }
    }
}
