//! The source connector contract (roadmap §3.6, Data-Flow Phases 0–2): what the pipeline asks of a
//! source engine. Each source crate implements it; nothing here performs I/O.

use async_trait::async_trait;
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
}

impl SourceError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
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
    /// Tables named without a glob that can't be replicated: each halts at start.
    pub halted: Vec<(TableRef, HaltReason)>,
    /// Tables a glob matched that can't be replicated (no identifier, a partitioned parent): each
    /// is left out with a warning.
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
}

/// A keyset cursor (Snapshot_Resumability §2): the identifier values of the last row read, in
/// identifier order, and the collation the text columns were ordered by.
#[derive(Debug, Clone, PartialEq)]
pub struct KeysetCursor {
    pub key: Row,
    /// `"C"` for PostgreSQL when an identifier column is text; `None` otherwise.
    pub collation: Option<String>,
}

/// One snapshot chunk: up to `limit` rows in identifier order after the cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub rows: Vec<Row>,
    /// The cursor after the last row; `None` when the chunk is empty (the table is done).
    pub next: Option<KeysetCursor>,
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RescanReport {
    /// Tables gone from the source catalog: each halts with `TABLE_DROPPED`.
    pub dropped: Vec<TableRef>,
    /// Tables whose catalog schema differs from the known one: the current schemas. Freshness and
    /// alerting only; the stream's signals apply changes in order.
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

    /// Prepares the stream position for a start (PostgreSQL: the publication, then the slot state
    /// machine). `checkpoint` is the pipeline's durable position, `None` without state; `tables`
    /// is the tracked set.
    async fn prepare(
        &self,
        checkpoint: Option<&SourcePosition>,
        tables: &[TableRef],
    ) -> Result<Prepared, SourceError>;

    /// Replaces a lost stream position after [`SourceError::PositionUnreachable`] (D-15.3): the
    /// new consistent position to snapshot from and stream after.
    async fn reset_position(&self) -> Result<SourcePosition, SourceError>;

    /// How many bytes of log the source keeps on disk for the pipeline (PostgreSQL: the WAL from
    /// the slot's `restart_lsn`, the `max_wal_lag_bytes` guard). `None` when the source keeps
    /// nothing for it.
    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError>;

    /// The source's current position: a snapshot chunk's low and high watermarks.
    async fn current_position(&self) -> Result<SourcePosition, SourceError>;

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

    /// Opens the change stream after `from`, for `tables` as of that position.
    async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError>;

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
}

/// The change stream of one [`SourceConnector::open_stream`] call.
#[async_trait]
pub trait ChangeStream: Send {
    /// The next item. An error ends the stream; reopen from the checkpoint.
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
        async fn prepare(
            &self,
            checkpoint: Option<&SourcePosition>,
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
            })
        }
        async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
            Ok(None)
        }
        async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
            Ok(SourcePosition::Pg(Lsn(0x20)))
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
            })
        }
        async fn fetch_rows(&self, _: &TableSchema, _: &[Row]) -> Result<Vec<Row>, SourceError> {
            Ok(vec![])
        }
        async fn open_stream(
            &self,
            _: &SourcePosition,
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
        let prepared = block_on(source.prepare(None, &[])).unwrap();
        assert!(matches!(
            prepared.plan,
            StartPlan::Snapshot {
                reason: SnapshotReason::FirstStart,
                ..
            }
        ));
        let mut stream =
            block_on(source.open_stream(&SourcePosition::Pg(Lsn(0x10)), &[schema()])).unwrap();
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
