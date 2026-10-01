//! The change stream (Data-Flow Phase 2a): `pgwire-replication` events and decoded pgoutput
//! messages → `StreamItem`s. Relation messages are channel B (`Schema_Change…` §3): a tracked
//! table whose columns differ from what the stream last saw gets a `SchemaSignal` before its next
//! change, at that change's position. A Relation message carries no position of its own: its
//! `XLogData` start is 0 (measured on PostgreSQL 18, D-M5.3).

use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use data_avalanche_config::Tables;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ChangeStream, ColumnSchema, ColumnValue, ColumnValueType, HaltReason, Op, Row,
    SchemaHint, SourceError, SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema,
    TransactionBoundary,
};
use pgwire_replication::{ReplicationClient, ReplicationEvent};
use tokio_postgres::Client;

use crate::catalog::{load_types, table_key};
use crate::connector::PgConnector;
use crate::error::{protocol, replication_error, sql_error};
use crate::pgoutput::{Datum, Message, OldKind, RelationColumn, decode};
use crate::position::{from_wire, lsn_of, to_wire};
use crate::types::{TypeCatalog, plan_column};
use crate::value::{self, Decode, ValueError};

/// Microseconds from the Unix epoch to PostgreSQL's (2000-01-01).
pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// One column as the stream decodes it.
#[derive(Debug, Clone)]
struct StreamColumn {
    name: String,
    key: bool,
    decode: Decode,
}

/// What the stream knows about a relation OID from its last Relation message.
#[derive(Debug)]
enum RelationState {
    /// Not a table the stream was opened for.
    Untracked,
    /// A tracked table that halted: its changes are skipped.
    Halted,
    /// A tracked table whose new columns can't be planned: it halts at its next change.
    Halting { table: TableRef, reason: HaltReason },
    Tracked {
        table: TableRef,
        columns: Vec<StreamColumn>,
        /// A shape change to announce before the next change.
        signal: Option<SchemaHint>,
    },
}

/// The open transaction.
#[derive(Debug, Clone, Copy)]
struct Txn {
    xid: u64,
    /// The commit record's LSN (Begin `final_lsn`): the events' `source_pos`, `_lsn` (D-M5.2).
    commit_lsn: Lsn,
    commit_ts_micros: i64,
}

/// The column shape channel B compares: name, decoded type and the catalog's type text.
type Shape = Vec<(String, ColumnValueType, String)>;

fn shape_of(columns: &[ColumnSchema]) -> Shape {
    columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type.clone(), c.source_type.clone()))
        .collect()
}

/// The stream of one `open_stream` call.
pub struct PgStream {
    client: ReplicationClient,
    /// Its own catalog connection, for the types and type names of Relation messages.
    catalog: Client,
    types: TypeCatalog,
    tables: Tables,
    /// The shape each tracked table had when the stream last saw it.
    shapes: HashMap<TableRef, Shape>,
    relations: HashMap<u32, RelationState>,
    type_names: HashMap<(u32, i32), String>,
    txn: Option<Txn>,
    pending: VecDeque<StreamItem>,
    start: Lsn,
    acknowledged: Lsn,
}

impl std::fmt::Debug for PgStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStream")
            .field("start", &self.start)
            .field("acknowledged", &self.acknowledged)
            .field("tables", &self.shapes.len())
            .finish_non_exhaustive()
    }
}

impl PgConnector {
    /// Opens the replication stream after `from` for `tables`, their schemas as of `from`. The
    /// stream has its own catalog connection for the types its Relation messages name.
    pub async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<Box<PgStream>, SourceError> {
        let start = lsn_of(from)?;
        let catalog = self.conn.connect().await?;
        let types = self.types.lock().await.clone();
        let config = self.conn.replication(
            &self.pg.slot,
            &self.pg.publication,
            to_wire(start),
            self.pg.heartbeat,
        );
        let client = ReplicationClient::connect(config)
            .await
            .map_err(|e| replication_error(e, Some(start.to_string())))?;
        Ok(Box::new(PgStream::new(
            client,
            catalog,
            types,
            self.pipeline.tables.clone(),
            tables,
            start,
        )))
    }
}

impl PgStream {
    fn new(
        client: ReplicationClient,
        catalog: Client,
        types: TypeCatalog,
        tables: Tables,
        schemas: &[TableSchema],
        start: Lsn,
    ) -> Self {
        Self {
            client,
            catalog,
            types,
            tables,
            shapes: schemas
                .iter()
                .map(|s| (s.table().clone(), shape_of(s.columns())))
                .collect(),
            relations: HashMap::new(),
            type_names: HashMap::new(),
            txn: None,
            pending: VecDeque::new(),
            start,
            acknowledged: start,
        }
    }

    /// `format_type` of each column's type, cached.
    async fn type_names(&mut self, columns: &[RelationColumn]) -> Result<Vec<String>, SourceError> {
        let wanted: Vec<(u32, i32)> = columns
            .iter()
            .map(|c| (c.type_oid, c.typmod))
            .filter(|k| !self.type_names.contains_key(k))
            .collect();
        if !wanted.is_empty() {
            let oids: Vec<u32> = wanted.iter().map(|w| w.0).collect();
            let mods: Vec<i32> = wanted.iter().map(|w| w.1).collect();
            let rows = self
                .catalog
                .query(
                    "SELECT x.o, x.m, pg_catalog.format_type(x.o, x.m)
                       FROM unnest($1::oid[], $2::int4[]) AS x(o, m)",
                    &[&oids, &mods],
                )
                .await
                .map_err(sql_error)?;
            for r in rows {
                self.type_names.insert((r.get(0), r.get(1)), r.get(2));
            }
        }
        Ok(columns
            .iter()
            .map(|c| self.type_names[&(c.type_oid, c.typmod)].clone())
            .collect())
    }

    /// Channel B: a Relation message. A tracked table will get a `SchemaSignal` when its shape
    /// changed, or a `TableHalt` when a column can't be planned any more ([`Self::announce`]).
    async fn relation(
        &mut self,
        oid: u32,
        table: TableRef,
        columns: Vec<RelationColumn>,
    ) -> Result<(), SourceError> {
        if !self.shapes.contains_key(&table) {
            self.relations.insert(oid, RelationState::Untracked);
            return Ok(());
        }
        if let Some(RelationState::Halted) = self.relations.get(&oid) {
            return Ok(());
        }
        load_types(
            &self.catalog,
            columns.iter().map(|c| c.type_oid),
            &mut self.types,
        )
        .await
        .map_err(sql_error)?;
        let type_names = self.type_names(&columns).await?;
        let key = table_key(&table);
        let mut schema = Vec::with_capacity(columns.len());
        let mut stream = Vec::with_capacity(columns.len());
        for (c, type_text) in columns.iter().zip(type_names) {
            let over = self
                .tables
                .column_type_overrides
                .get(&format!("{key}.{}", c.name));
            match plan_column(
                &c.name,
                c.type_oid,
                c.typmod,
                &self.types,
                self.tables.numeric_overflow_action,
                over,
            ) {
                Ok(p) => {
                    schema.push(ColumnSchema {
                        name: c.name.clone(),
                        value_type: p.value_type,
                        logical: p.logical,
                        nullable: true,
                        source_type: type_text,
                        source_ordinal: None,
                    });
                    stream.push(StreamColumn {
                        name: c.name.clone(),
                        key: c.key,
                        decode: p.decode,
                    });
                }
                Err(reason) => {
                    self.relations
                        .insert(oid, RelationState::Halting { table, reason });
                    return Ok(());
                }
            }
        }
        let shape = shape_of(&schema);
        let signal = (self.shapes.get(&table) != Some(&shape)).then(|| {
            self.shapes.insert(table.clone(), shape);
            SchemaHint::Columns(schema)
        });
        self.relations.insert(
            oid,
            RelationState::Tracked {
                table,
                columns: stream,
                signal,
            },
        );
        Ok(())
    }

    /// Before a change or TRUNCATE of `oid` at `seq`: the signal or halt its last Relation message
    /// left.
    fn announce(&mut self, oid: u32, seq: SourceSeq) {
        match self.relations.get_mut(&oid) {
            Some(RelationState::Tracked { table, signal, .. }) => {
                if let Some(hint) = signal.take() {
                    let table = table.clone();
                    self.pending
                        .push_back(StreamItem::SchemaSignal { table, seq, hint });
                }
            }
            Some(RelationState::Halting { .. }) => {
                let Some(RelationState::Halting { table, reason }) =
                    self.relations.insert(oid, RelationState::Halted)
                else {
                    unreachable!("matched above")
                };
                self.pending
                    .push_back(StreamItem::TableHalt { table, seq, reason });
            }
            _ => {}
        }
    }

    /// A tuple as a row of the relation's columns; `keys_only` keeps the replica identity columns
    /// (a `K` old tuple, whose other columns are NULL placeholders).
    fn row(
        columns: &[StreamColumn],
        tuple: Vec<Datum>,
        keys_only: bool,
    ) -> Result<Result<Row, HaltReason>, SourceError> {
        if tuple.len() != columns.len() {
            return Err(protocol(format!(
                "a tuple of {} columns for a relation of {}",
                tuple.len(),
                columns.len()
            )));
        }
        let mut row = Vec::with_capacity(columns.len());
        for (c, d) in columns.iter().zip(tuple) {
            if keys_only && !c.key {
                continue;
            }
            let v = match d {
                Datum::Null => ColumnValue::Null,
                Datum::Unchanged => ColumnValue::Unchanged,
                Datum::Text(bytes) => {
                    let Ok(text) = std::str::from_utf8(&bytes) else {
                        return Ok(Err(HaltReason::SchemaMismatch {
                            column: c.name.clone(),
                            detail: "the value isn't valid UTF-8".into(),
                        }));
                    };
                    match value::decode(&c.decode, text) {
                        Ok(v) => v,
                        Err(ValueError::Overflow) => {
                            return Ok(Err(HaltReason::NumericOverflow {
                                column: c.name.clone(),
                            }));
                        }
                        Err(ValueError::Unrepresentable(detail)) => {
                            return Ok(Err(HaltReason::SchemaMismatch {
                                column: c.name.clone(),
                                detail,
                            }));
                        }
                    }
                }
            };
            row.push((c.name.clone(), v));
        }
        Ok(Ok(Row(row)))
    }

    /// A change of `relation`, or nothing for a table the stream doesn't track.
    fn change(
        &mut self,
        relation: u32,
        op: Op,
        old: Option<(OldKind, Vec<Datum>)>,
        new: Option<Vec<Datum>>,
        seq: SourceSeq,
    ) -> Result<(), SourceError> {
        let txn = self
            .txn
            .ok_or_else(|| protocol("a change outside a transaction"))?;
        self.announce(relation, seq);
        let (table, before, after) = match self.relations.get(&relation) {
            Some(RelationState::Tracked { table, columns, .. }) => {
                let before = match old {
                    Some((kind, t)) => Some(Self::row(columns, t, kind == OldKind::Key)?),
                    None => None,
                };
                let after = match new {
                    Some(t) => Some(Self::row(columns, t, false)?),
                    None => None,
                };
                (table.clone(), before, after)
            }
            Some(_) => return Ok(()),
            None => {
                return Err(protocol(format!(
                    "a change of relation {relation} before its Relation message"
                )));
            }
        };
        let halt = |r: &Option<Result<Row, HaltReason>>| match r {
            Some(Err(h)) => Some(h.clone()),
            _ => None,
        };
        if let Some(reason) = halt(&before).or_else(|| halt(&after)) {
            self.relations.insert(relation, RelationState::Halted);
            self.pending
                .push_back(StreamItem::TableHalt { table, seq, reason });
            return Ok(());
        }
        self.pending.push_back(StreamItem::Change(CdcEvent {
            op,
            table,
            before: before.map(|r| r.expect("checked")),
            after: after.map(|r| r.expect("checked")),
            source_pos: SourcePosition::Pg(txn.commit_lsn),
            change_seq: seq,
            xid: txn.xid,
            commit_ts_micros: txn.commit_ts_micros,
        }));
        Ok(())
    }

    async fn xlog(&mut self, wal_start: Lsn, data: bytes::Bytes) -> Result<(), SourceError> {
        let seq = SourceSeq::Pg(wal_start);
        match decode(&data).map_err(protocol)? {
            Message::Relation {
                oid,
                namespace,
                name,
                columns,
                ..
            } => {
                let schema = if namespace.is_empty() {
                    "pg_catalog".into()
                } else {
                    namespace
                };
                self.relation(oid, TableRef::new(schema, name), columns)
                    .await?;
            }
            Message::Insert { relation, new } => {
                self.change(relation, Op::Insert, None, Some(new), seq)?
            }
            Message::Update { relation, old, new } => {
                self.change(relation, Op::Update, old, Some(new), seq)?
            }
            Message::Delete { relation, old } => {
                self.change(relation, Op::Delete, Some(old), None, seq)?
            }
            Message::Truncate { relations, .. } => {
                if self.txn.is_none() {
                    return Err(protocol("a TRUNCATE outside a transaction"));
                }
                for r in &relations {
                    self.announce(*r, seq);
                }
                let tables: Vec<TableRef> = relations
                    .iter()
                    .filter_map(|r| match self.relations.get(r) {
                        Some(RelationState::Tracked { table, .. }) => Some(table.clone()),
                        _ => None,
                    })
                    .collect();
                if !tables.is_empty() {
                    self.pending.push_back(StreamItem::Truncate { tables, seq });
                }
            }
            Message::Type { .. } | Message::Origin { .. } => {}
        }
        Ok(())
    }

    async fn event(&mut self, ev: ReplicationEvent) -> Result<(), SourceError> {
        match ev {
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                if self.txn.is_none() {
                    self.pending
                        .push_back(StreamItem::KeepAlive(SourcePosition::Pg(from_wire(
                            wal_end,
                        ))));
                }
            }
            ReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => {
                if let Some(open) = self.txn {
                    return Err(protocol(format!(
                        "transaction {xid} begins inside {}",
                        open.xid
                    )));
                }
                let t = Txn {
                    xid: u64::from(xid),
                    commit_lsn: from_wire(final_lsn),
                    commit_ts_micros: commit_time_micros + PG_EPOCH_MICROS,
                };
                self.txn = Some(t);
                self.pending.push_back(StreamItem::Begin {
                    xid: t.xid,
                    commit_ts_micros: t.commit_ts_micros,
                });
            }
            ReplicationEvent::Commit { lsn, end_lsn, .. } => {
                let t = self
                    .txn
                    .take()
                    .ok_or_else(|| protocol("a Commit outside a transaction"))?;
                if from_wire(lsn) != t.commit_lsn {
                    return Err(protocol(format!(
                        "transaction {} commits at {} after beginning with {}",
                        t.xid,
                        from_wire(lsn),
                        t.commit_lsn
                    )));
                }
                self.pending
                    .push_back(StreamItem::Commit(TransactionBoundary {
                        xid: t.xid,
                        commit_pos: SourcePosition::Pg(from_wire(end_lsn)),
                        commit_ts_micros: t.commit_ts_micros,
                    }));
            }
            ReplicationEvent::XLogData {
                wal_start, data, ..
            } => self.xlog(from_wire(wal_start), data).await?,
            ReplicationEvent::Message { .. } | ReplicationEvent::StoppedAt { .. } => {}
        }
        Ok(())
    }
}

#[async_trait]
impl ChangeStream for PgStream {
    async fn next(&mut self) -> Result<StreamItem, SourceError> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Ok(item);
            }
            match self.client.recv().await {
                Ok(Some(ev)) => self.event(ev).await?,
                Ok(None) => {
                    return Err(SourceError::Transient(
                        "the replication stream ended".into(),
                    ));
                }
                Err(e) => return Err(replication_error(e, Some(self.acknowledged.to_string()))),
            }
        }
    }

    /// Sets the slot's confirmed position (the status update's write, flush and apply LSNs,
    /// Data-Flow Phase 5). A position behind the last acknowledged one changes nothing.
    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError> {
        let lsn = lsn_of(durable)?;
        if lsn > self.acknowledged {
            self.acknowledged = lsn;
            self.client.update_applied_lsn(to_wire(lsn));
        }
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> Result<(), SourceError> {
        self.client
            .shutdown()
            .await
            .map_err(|e| replication_error(e, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, key: bool, decode: Decode) -> StreamColumn {
        StreamColumn {
            name: name.into(),
            key,
            decode,
        }
    }

    fn text(s: &str) -> Datum {
        Datum::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn rows_from_tuples() {
        let columns = [
            col("id", true, Decode::Int64),
            col("doc", false, Decode::Text),
            col("n", false, Decode::Numeric),
        ];
        let row = PgStream::row(
            &columns,
            vec![text("7"), Datum::Unchanged, Datum::Null],
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            row,
            Row(vec![
                ("id".into(), ColumnValue::Int64(7)),
                ("doc".into(), ColumnValue::Unchanged),
                ("n".into(), ColumnValue::Null),
            ])
        );
        let key = PgStream::row(&columns, vec![text("7"), Datum::Null, Datum::Null], true)
            .unwrap()
            .unwrap();
        assert_eq!(
            key,
            Row(vec![("id".into(), ColumnValue::Int64(7))]),
            "a K tuple keeps the key"
        );
    }

    #[test]
    fn undecodable_values_halt_the_table() {
        let columns = [col("n", false, Decode::Numeric)];
        assert_eq!(
            PgStream::row(&columns, vec![text("NaN")], false).unwrap(),
            Err(HaltReason::SchemaMismatch {
                column: "n".into(),
                detail: "NaN can't be stored in a decimal column".into()
            })
        );
        assert_eq!(
            PgStream::row(&columns, vec![text(&"9".repeat(40))], false).unwrap(),
            Err(HaltReason::NumericOverflow { column: "n".into() })
        );
        let latin1 = Datum::Text(bytes::Bytes::from_static(&[0xe9]));
        assert!(matches!(
            PgStream::row(&[col("t", false, Decode::Text)], vec![latin1], false).unwrap(),
            Err(HaltReason::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn a_tuple_of_the_wrong_width_is_a_protocol_error() {
        let e = PgStream::row(&[col("a", false, Decode::Text)], vec![], false).unwrap_err();
        assert!(matches!(e, SourceError::Permanent(m) if m.contains("a tuple of 0 columns")));
    }

    #[test]
    fn commit_times_move_to_the_unix_epoch() {
        // 2000-01-01 00:00:00 UTC.
        assert_eq!(PG_EPOCH_MICROS, 946_684_800 * 1_000_000);
    }
}
