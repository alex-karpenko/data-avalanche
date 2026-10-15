//! The binlog stream (Data-Flow Phase 2b): a binlog dump read as transactions of row changes,
//! schema signals, truncates and table halts.
//!
//! - **Positions.** A MySQL GTID dump sends the GTID set, a MariaDB one its GTID state through
//!   `@slave_connect_state`; a file/position dump its file and offset. A transaction's commit
//!   advances the position: the set gains its GTID, the state its domain's GTID, the file
//!   position becomes the commit event's end. Every transaction advances it, tracked or not, and
//!   one without tracked rows reports it in a keep-alive, at most one a second (a heartbeat comes
//!   only to an idle dump). In file/position mode a rotate between transactions moves it to the
//!   next file. A row's `_lsn` is its transaction's own GTID, or in file/position mode the
//!   transaction's start.
//! - **Order.** Each event's end offset in its file ([`Offsets`], Q-M7-4) is the `_seq` of the
//!   rows it holds.
//! - **Decoding.** Each tracked table decodes by the as-of-position schema: the pipeline's at
//!   open, then each statement's channel C answer, read here and remembered for the pipeline's
//!   `augment` of the same signal. A table map that doesn't match the schema (columns, types,
//!   names under `binlog_row_metadata = FULL`) halts the table, never misreads a row; so does a
//!   row image that isn't FULL.
//! - **Fail closed.** A compressed transaction that yields less than its size halts the pipeline
//!   (`BINLOG_PAYLOAD_INVALID`, D-V1.6); so does a two-phase XA prepare of tracked rows (they can
//!   roll back after they are logged), a rollback logged after tracked rows (MariaDB keeps the
//!   rows a savepoint or a whole rollback undid, measured on 10.11–11.8), an incident event (the
//!   server lost events), and any event type the source doesn't read.
//!
//! [`ChangeStream::next`] is cancel-safe: an event is processed without an await, and the one
//! await inside a transaction (channel C after a statement) keeps its work until it completes.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use data_avalanche_core::position::{
    BinlogFilePos, GtidSet, GtidSource, MariaGtid, MariaGtidState,
};
use data_avalanche_core::{
    CdcEvent, ChangeStream, HaltReason, Op, Row, SchemaHint, SourceError, SourcePosition,
    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary,
};
use futures_util::StreamExt;
use mysql_async::binlog::events::{
    Event, EventData, OptionalMetadataField, RowsEventData, TableMapEvent, TransactionPayloadEvent,
};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Sid};

use crate::binlog::{MariaGtidEvent, Offsets, decompress, rotate_name, types};
use crate::catalog::table_key;
use crate::connector::MySqlConnector;
use crate::ddl::{Renamed, classify, renamed};
use crate::error::mysql;
use crate::position::Mode;
use crate::server::Flavour;
use crate::start::purge_action;
use crate::types::{CatalogColumn, plan_column};
use crate::value::{Decode, Input, decode_after, halt_reason};

/// One column of a tracked table, as the stream decodes it.
#[derive(Debug, Clone)]
struct Column {
    name: String,
    data_type: String,
    decode: Decode,
    identifier: bool,
}

/// A tracked table: its as-of-position schema and how its columns decode.
#[derive(Debug, Clone)]
struct Tracked {
    schema: TableSchema,
    columns: Vec<Column>,
}

/// What a transaction's commit adds to the position.
#[derive(Debug, Clone)]
enum Gtid {
    Mysql(GtidSource, u64),
    Maria(MariaGtid),
    /// An anonymous transaction (MySQL with GTIDs off).
    None,
}

#[derive(Debug, Clone)]
struct Txn {
    xid: u64,
    commit_ts_micros: i64,
    /// Its own position: the `_lsn` of its rows.
    identity: SourcePosition,
    gtid: Gtid,
    /// Opened by `BEGIN`: it ends at `XID` (or `COMMIT`), not after its first statement.
    with_begin: bool,
    /// An XA transaction's branch (`XA START`, MariaDB's prepared-XA GTID): it ends at its
    /// prepare.
    xa: bool,
    /// Its `Begin` item went out.
    begun: bool,
}

/// A statement's schema signals still to read from the catalog, then what follows them.
#[derive(Debug)]
struct Signals {
    sql: String,
    seq: SourceSeq,
    tables: VecDeque<TableRef>,
    after: Vec<StreamItem>,
}

/// The binlog stream of one [`MySqlConnector::open_stream`] call.
pub struct MySqlStream {
    events: BinlogStream,
    /// Catalog reads for channel C, on a connection of their own.
    catalog: MySqlConnector,
    mode: Mode,
    offsets: Offsets,
    position: SourcePosition,
    tables: HashMap<TableRef, Tracked>,
    txn: Option<Txn>,
    ready: VecDeque<StreamItem>,
    signals: Option<Signals>,
    /// Ids for anonymous transactions.
    anonymous: u64,
    /// When the last keep-alive went out.
    kept_alive: Option<Instant>,
}

/// The least time between two keep-alives after transactions without tracked rows.
const KEEP_ALIVE_EVERY: Duration = Duration::from_secs(1);

/// A GTID set as the dump request's sid blocks: one per server UUID and tag.
fn sids(set: &GtidSet) -> Result<Vec<Sid<'static>>, String> {
    let text = set.to_string();
    let mut out = vec![];
    for entry in text.split(',').filter(|e| !e.is_empty()) {
        let mut parts = entry.split(':');
        let uuid = parts.next().unwrap_or_default();
        let mut groups: Vec<(Option<&str>, Vec<&str>)> = vec![(None, vec![])];
        for p in parts {
            if p.bytes().all(|b| b.is_ascii_digit() || b == b'-') {
                groups.last_mut().expect("one group").1.push(p);
            } else {
                groups.push((Some(p), vec![]));
            }
        }
        for (tag, intervals) in groups.into_iter().filter(|(_, i)| !i.is_empty()) {
            let sid = match tag {
                Some(t) => format!("{uuid}:{t}:{}", intervals.join(":")),
                None => format!("{uuid}:{}", intervals.join(":")),
            };
            out.push(sid.parse().map_err(|e| format!("GTID {sid}: {e}"))?);
        }
    }
    Ok(out)
}

/// The binlog column types a column of `data_type` arrives as.
fn arrives_as(data_type: &str) -> &'static [ColumnType] {
    use ColumnType::*;
    match data_type {
        "tinyint" => &[MYSQL_TYPE_TINY],
        "smallint" => &[MYSQL_TYPE_SHORT],
        "mediumint" => &[MYSQL_TYPE_INT24],
        "int" | "integer" => &[MYSQL_TYPE_LONG],
        "bigint" => &[MYSQL_TYPE_LONGLONG],
        "float" => &[MYSQL_TYPE_FLOAT],
        "double" | "real" => &[MYSQL_TYPE_DOUBLE],
        "decimal" | "numeric" => &[MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_DECIMAL],
        "char" | "varchar" | "binary" | "varbinary" | "uuid" | "inet4" | "inet6" => {
            &[MYSQL_TYPE_STRING, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING]
        }
        "enum" => &[MYSQL_TYPE_ENUM, MYSQL_TYPE_STRING],
        "set" => &[MYSQL_TYPE_SET, MYSQL_TYPE_STRING],
        "tinytext" | "text" | "mediumtext" | "longtext" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" => &[
            MYSQL_TYPE_BLOB,
            MYSQL_TYPE_TINY_BLOB,
            MYSQL_TYPE_MEDIUM_BLOB,
            MYSQL_TYPE_LONG_BLOB,
        ],
        "json" => &[MYSQL_TYPE_JSON, MYSQL_TYPE_BLOB],
        "date" => &[MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE],
        "time" => &[MYSQL_TYPE_TIME2, MYSQL_TYPE_TIME],
        "datetime" => &[MYSQL_TYPE_DATETIME2, MYSQL_TYPE_DATETIME],
        "timestamp" => &[MYSQL_TYPE_TIMESTAMP2, MYSQL_TYPE_TIMESTAMP],
        "year" => &[MYSQL_TYPE_YEAR],
        "bit" => &[MYSQL_TYPE_BIT],
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "geomcollection" => &[MYSQL_TYPE_GEOMETRY],
        "vector" => &[
            MYSQL_TYPE_VECTOR,
            MYSQL_TYPE_STRING,
            MYSQL_TYPE_VARCHAR,
            MYSQL_TYPE_VAR_STRING,
            MYSQL_TYPE_BLOB,
        ],
        _ => &[],
    }
}

/// Whether the table map describes the rows the schema decodes: as many columns, each of a
/// binlog type its type arrives as, and under `binlog_row_metadata = FULL` the same names.
fn check_map(tme: &TableMapEvent<'_>, t: &Tracked) -> Result<(), String> {
    let n = tme.columns_count() as usize;
    if n != t.columns.len() {
        return Err(format!(
            "the binlog's rows have {n} columns, the schema {}",
            t.columns.len()
        ));
    }
    for (i, c) in t.columns.iter().enumerate() {
        let ty = tme
            .get_column_type(i)
            .map_err(|e| format!("column {}: {e}", c.name))?
            .ok_or_else(|| format!("column {}: no binlog type", c.name))?;
        if !arrives_as(&c.data_type).contains(&ty) {
            return Err(format!(
                "column {}: the binlog's rows have {ty:?}, the schema {}",
                c.name, c.data_type
            ));
        }
    }
    for field in tme.iter_optional_meta() {
        if let Ok(OptionalMetadataField::ColumnName(names)) = field {
            for (i, name) in names.iter_names().enumerate() {
                let name = name.map_err(|e| format!("column names: {e}"))?;
                let want = t.columns.get(i).map(|c| c.name.as_str());
                if Some(name.name().as_ref()) != want {
                    return Err(format!(
                        "column {}: the binlog names it {:?}",
                        want.unwrap_or("?"),
                        name.name()
                    ));
                }
            }
        }
    }
    Ok(())
}

/// A pipeline halt for a compressed transaction that didn't yield its events (D-V1.6).
fn payload_invalid(detail: String) -> SourceError {
    SourceError::Permanent(format!("BINLOG_PAYLOAD_INVALID: {detail}"))
}

fn permanent(detail: impl std::fmt::Display) -> SourceError {
    SourceError::Permanent(detail.to_string())
}

/// The statement's leading word, upper case.
fn verb(sql: &str) -> String {
    sql.trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase()
}

impl MySqlConnector {
    /// How the stream decodes `schema`: each column planned from the schema alone
    /// ([`CatalogColumn::from_schema`]) with the pipeline's settings; a plan that doesn't give the
    /// schema's type halts the table.
    fn track(&self, schema: &TableSchema) -> Result<Tracked, HaltReason> {
        let tables = &self.pipeline.tables;
        let key = table_key(schema.table());
        let mut columns = Vec::with_capacity(schema.columns().len());
        for col in schema.columns() {
            let c = CatalogColumn::from_schema(col);
            let over = tables
                .column_type_overrides
                .get(&format!("{key}.{}", col.name));
            let identifier = schema.is_identifier(&col.name);
            let p = plan_column(
                &c,
                self.tinyint1_as_bool(),
                tables.numeric_overflow_action,
                over,
                identifier,
            )?;
            if p.value_type != col.value_type {
                return Err(HaltReason::SchemaMismatch {
                    column: col.name.clone(),
                    detail: format!(
                        "the schema's {} plans as {:?}, not {:?}",
                        col.source_type, p.value_type, col.value_type
                    ),
                });
            }
            columns.push(Column {
                name: col.name.clone(),
                data_type: c.data_type,
                decode: p.decode,
                identifier,
            });
        }
        Ok(Tracked {
            schema: schema.clone(),
            columns,
        })
    }

    /// Opens the binlog dump after `from` for `tables` as of that position (Data-Flow Phase 2b):
    /// the heartbeat period, the replica id `source.mysql.server_id`, and the position in its
    /// dialect.
    pub async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<MySqlStream, SourceError> {
        let flavour = self.flavour().await?;
        let mode = Mode::of(flavour, self.mysql.replication_type);
        let catalog = self.sibling()?;
        let mut tracked = HashMap::new();
        for s in tables {
            let t = catalog.track(s).map_err(|reason| SourceError::TableHalt {
                table: s.table().clone(),
                reason,
            })?;
            tracked.insert(s.table().clone(), t);
        }
        let at = from.to_tagged();
        let action = self.mysql.binlog_purge_action;
        let err = |e| purge_action(action, mysql(e, Some(&at)));
        let mut conn = self.conn.connect().await?;
        let id = conn.id();
        conn.query_drop(format!(
            "SET @master_heartbeat_period = {}",
            self.mysql.heartbeat.as_nanos()
        ))
        .await
        .map_err(err)?;
        if flavour == Flavour::Mariadb {
            conn.query_drop("SET @mariadb_slave_capability = 4")
                .await
                .map_err(err)?;
        }
        let file;
        let mut req = BinlogStreamRequest::new(self.mysql.server_id);
        let offsets = match (mode, from) {
            (Mode::MysqlGtid, SourcePosition::MySqlGtid(set)) => {
                req = req.with_gtid().with_gtid_set(sids(set).map_err(permanent)?);
                Offsets::new("", 4)
            }
            (Mode::MariadbGtid, SourcePosition::MariaDbGtid(state)) => {
                conn.query_drop(format!("SET @slave_connect_state = '{state}'"))
                    .await
                    .map_err(err)?;
                conn.query_drop("SET @slave_gtid_strict_mode = 1")
                    .await
                    .map_err(err)?;
                Offsets::new("", 4)
            }
            (Mode::File, SourcePosition::BinlogFile(p)) => {
                file = p.file().as_bytes().to_vec();
                req = req.with_filename(&file[..]).with_pos(p.pos());
                Offsets::new(p.file(), p.pos())
            }
            _ => {
                return Err(permanent(format!(
                    "the stream position {at} isn't a {mode:?} position"
                )));
            }
        };
        let events = conn.get_binlog_stream(req).await.map_err(err)?;
        self.shared.dump.store(id, Ordering::Relaxed);
        Ok(MySqlStream {
            events,
            catalog,
            mode,
            offsets,
            position: from.clone(),
            tables: tracked,
            txn: None,
            ready: VecDeque::new(),
            signals: None,
            anonymous: 0,
            kept_alive: None,
        })
    }
}

impl MySqlStream {
    fn seq(&self, end: u64) -> Result<SourceSeq, SourceError> {
        SourceSeq::binlog(self.offsets.file(), end).map_err(permanent)
    }

    /// Where items of the open transaction go: after a statement's pending signals, if any.
    fn emit(&mut self, item: StreamItem) {
        match &mut self.signals {
            Some(s) => s.after.push(item),
            None => self.ready.push_back(item),
        }
    }

    /// The open transaction's `Begin`, once.
    fn begin(&mut self) -> Result<(), SourceError> {
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| permanent("a change outside a transaction"))?;
        if !txn.begun {
            txn.begun = true;
            let item = StreamItem::Begin {
                xid: txn.xid,
                commit_ts_micros: txn.commit_ts_micros,
            };
            self.emit(item);
        }
        Ok(())
    }

    fn halt(
        &mut self,
        table: &TableRef,
        seq: SourceSeq,
        reason: HaltReason,
    ) -> Result<(), SourceError> {
        self.begin()?;
        self.tables.remove(table);
        self.emit(StreamItem::TableHalt {
            table: table.clone(),
            seq,
            reason,
        });
        Ok(())
    }

    /// A transaction starts at `start`, the end of the event before its GTID event.
    fn start(&mut self, xid: u64, ts: i64, gtid: Gtid, start: u64) -> Result<(), SourceError> {
        if self.txn.is_some() {
            return Err(permanent("a GTID event inside a transaction"));
        }
        let identity = match (&gtid, self.mode) {
            (_, Mode::File) | (Gtid::None, _) => SourcePosition::BinlogFile(
                BinlogFilePos::new(self.offsets.file(), start).map_err(permanent)?,
            ),
            (Gtid::Mysql(source, gno), _) => {
                let mut set = GtidSet::default();
                set.insert(source, *gno);
                SourcePosition::MySqlGtid(set)
            }
            (Gtid::Maria(g), _) => {
                let mut state = MariaGtidState::default();
                state.advance(*g);
                SourcePosition::MariaDbGtid(state)
            }
        };
        self.txn = Some(Txn {
            xid,
            commit_ts_micros: ts,
            identity,
            gtid,
            with_begin: false,
            xa: false,
            begun: false,
        });
        Ok(())
    }

    /// The open transaction commits with the event ending at `end`: the position advances, and a
    /// transaction with items ends with its `Commit`.
    fn commit(&mut self, end: u64) -> Result<(), SourceError> {
        let txn = self
            .txn
            .take()
            .ok_or_else(|| permanent("a commit outside a transaction"))?;
        self.position = match (&self.position, &txn.gtid, self.mode) {
            (_, _, Mode::File) => SourcePosition::BinlogFile(
                BinlogFilePos::new(self.offsets.file(), end).map_err(permanent)?,
            ),
            (SourcePosition::MySqlGtid(set), Gtid::Mysql(source, gno), _) => {
                let mut set = set.clone();
                set.insert(source, *gno);
                SourcePosition::MySqlGtid(set)
            }
            (SourcePosition::MariaDbGtid(state), Gtid::Maria(g), _) => {
                let mut state = state.clone();
                state.advance(*g);
                SourcePosition::MariaDbGtid(state)
            }
            (p, g, m) => {
                return Err(permanent(format!(
                    "a {g:?} transaction in a {m:?} stream at {}",
                    p.to_tagged()
                )));
            }
        };
        if txn.begun {
            let item = StreamItem::Commit(TransactionBoundary {
                xid: txn.xid,
                commit_pos: self.position.clone(),
                commit_ts_micros: txn.commit_ts_micros,
            });
            self.emit(item);
        } else if self.signals.is_none() {
            self.keep_alive(false);
        }
        Ok(())
    }

    /// A keep-alive at the position: on a heartbeat always, after a transaction without tracked
    /// rows at most once in [`KEEP_ALIVE_EVERY`].
    fn keep_alive(&mut self, heartbeat: bool) {
        let now = Instant::now();
        if heartbeat || self.kept_alive.is_none_or(|t| now - t >= KEEP_ALIVE_EVERY) {
            self.kept_alive = Some(now);
            self.ready
                .push_back(StreamItem::KeepAlive(self.position.clone()));
        }
    }

    /// A rollback logged inside a transaction with tracked rows halts the pipeline: the binlog
    /// kept rows the rollback undid, so the source can't tell which of them stand.
    fn rolled_back(&self, what: &str, end: u64) -> Result<(), SourceError> {
        match self.txn.as_ref().is_some_and(|t| t.begun) {
            true => Err(permanent(format!(
                "{what} at {}:{end} in a transaction with rows of tracked tables: the binlog \
                 kept rows the rollback undid, so the source can't tell which rows stand",
                self.offsets.file()
            ))),
            false => Ok(()),
        }
    }

    fn on_event(&mut self, ev: Event) -> Result<(), SourceError> {
        let h = ev.header();
        let (raw, log_pos, size) = (h.event_type_raw(), h.log_pos(), h.event_size());
        match raw {
            types::HEARTBEAT | types::HEARTBEAT_V2 => {
                self.offsets.heartbeat().map_err(payload_invalid)?;
                if self.txn.is_none() && self.signals.is_none() {
                    self.keep_alive(true);
                }
                return Ok(());
            }
            types::ROTATE => {
                let Some(EventData::RotateEvent(r)) = ev.read_data().map_err(permanent)? else {
                    return Err(permanent("an unreadable ROTATE event"));
                };
                let name = String::from_utf8_lossy(rotate_name(r.name_raw())).into_owned();
                self.offsets
                    .rotate(&name, r.position())
                    .map_err(payload_invalid)?;
                // Between transactions a file position moves to the next file. The dump's opening
                // rotate (no `log_pos`) names the file it opened in: the position stays there.
                let opening = log_pos == 0
                    && matches!(&self.position, SourcePosition::BinlogFile(p) if p.file() == name);
                if self.mode == Mode::File && self.txn.is_none() && !opening {
                    self.position = SourcePosition::BinlogFile(
                        BinlogFilePos::new(&name, r.position()).map_err(permanent)?,
                    );
                }
                return Ok(());
            }
            // The format description a dump resends at its start isn't in the file there.
            types::FORMAT_DESCRIPTION if log_pos == 0 => return Ok(()),
            _ => {}
        }
        let start = self.offsets.end();
        let end = self.offsets.event(log_pos, size).map_err(payload_invalid)?;
        match raw {
            types::STOP
            | types::INTVAR
            | types::RAND
            | types::USER_VAR
            | types::FORMAT_DESCRIPTION
            | types::IGNORABLE
            | types::ROWS_QUERY
            | types::PREVIOUS_GTIDS
            | types::TRANSACTION_CONTEXT
            | types::VIEW_CHANGE
            | types::TABLE_MAP
            | types::MARIADB_ANNOTATE_ROWS
            | types::MARIADB_BINLOG_CHECKPOINT
            | types::MARIADB_GTID_LIST
            | types::MARIADB_START_ENCRYPTION => Ok(()),
            types::GTID | types::GTID_TAGGED | types::ANONYMOUS_GTID => {
                let (g, anonymous) = match ev.read_data().map_err(permanent)? {
                    Some(EventData::GtidEvent(g)) => (g, false),
                    Some(EventData::AnonymousGtidEvent(g)) => (g.0, true),
                    other => return Err(permanent(format!("a GTID event reads as {other:?}"))),
                };
                let ts = match g.immediate_commit_timestamp() {
                    0 => i64::from(h.timestamp()) * 1_000_000,
                    us => us as i64,
                };
                if anonymous {
                    self.anonymous += 1;
                    return self.start(self.anonymous, ts, Gtid::None, start);
                }
                if g.gno() == 0 {
                    return Err(permanent("a GTID event with transaction number 0"));
                }
                let source = GtidSource {
                    uuid: uuid::Uuid::from_bytes(g.sid()),
                    tag: g.tag().map(|t| t.as_str().to_owned()),
                };
                self.start(g.gno(), ts, Gtid::Mysql(source, g.gno()), start)
            }
            types::MARIADB_GTID => {
                let g = MariaGtidEvent::parse(h.server_id(), ev.data()).map_err(permanent)?;
                let ts = i64::from(h.timestamp()) * 1_000_000;
                self.start(g.gtid.seq, ts, Gtid::Maria(g.gtid), start)?;
                let t = self.txn.as_mut().expect("started");
                // A transaction's GTID event stands for its BEGIN: its XID or COMMIT ends it.
                t.with_begin = !g.standalone();
                if g.flags & MariaGtidEvent::PREPARED_XA != 0 {
                    t.xa = true;
                }
                Ok(())
            }
            types::QUERY => self.query(&ev, end),
            types::MARIADB_QUERY_COMPRESSED => {
                let plain = decompress(&ev)
                    .map_err(permanent)?
                    .expect("a compressed type");
                self.query(&plain, end)
            }
            types::XID => self.commit(end),
            types::WRITE_ROWS_V1
            | types::UPDATE_ROWS_V1
            | types::DELETE_ROWS_V1
            | types::WRITE_ROWS
            | types::UPDATE_ROWS
            | types::DELETE_ROWS
            | types::PARTIAL_UPDATE_ROWS => self.rows(&ev, end),
            types::MARIADB_WRITE_ROWS_COMPRESSED_V1
            | types::MARIADB_UPDATE_ROWS_COMPRESSED_V1
            | types::MARIADB_DELETE_ROWS_COMPRESSED_V1 => {
                let plain = decompress(&ev)
                    .map_err(permanent)?
                    .expect("a compressed type");
                self.rows(&plain, end)
            }
            types::TRANSACTION_PAYLOAD => {
                let p: TransactionPayloadEvent<'_> = ev.read_event().map_err(permanent)?;
                self.offsets
                    .payload(p.uncompressed_size())
                    .map_err(payload_invalid)
            }
            types::XA_PREPARE => self.xa_prepare(&ev, end),
            types::INCIDENT => Err(permanent(format!(
                "an incident event at {}:{end}: the server lost events it didn't log",
                self.offsets.file()
            ))),
            other => Err(permanent(format!(
                "a binlog event of type {other} at {}:{end}, which the source doesn't read",
                self.offsets.file()
            ))),
        }
    }

    /// An XA branch's prepare ends its transaction. A one-phase commit (MySQL) commits it; a
    /// two-phase prepare with rows of a tracked table halts the pipeline: they can still roll back,
    /// and the source doesn't hold prepared rows for their outcome (D-M7a). Without them, it ends
    /// like any untracked transaction.
    fn xa_prepare(&mut self, ev: &Event, end: u64) -> Result<(), SourceError> {
        let one_phase = ev.data().first().is_some_and(|b| *b != 0);
        let tracked = self.txn.as_ref().is_some_and(|t| t.begun);
        if tracked && !one_phase {
            return Err(permanent(format!(
                "an XA PREPARE at {}:{end} holds rows of tracked tables: they can roll back \
                 after they are logged, and the source doesn't replicate two-phase XA",
                self.offsets.file()
            )));
        }
        self.commit(end)
    }

    fn query(&mut self, ev: &Event, end: u64) -> Result<(), SourceError> {
        let Some(EventData::QueryEvent(q)) = ev.read_data().map_err(permanent)? else {
            return Err(permanent("an unreadable QUERY event"));
        };
        let sql = q.query().into_owned();
        let db = q.schema().into_owned();
        if self.txn.is_none() {
            // No GTID event before it: an anonymous transaction.
            self.anonymous += 1;
            let ts = i64::from(ev.header().timestamp()) * 1_000_000;
            self.start(
                self.anonymous,
                ts,
                Gtid::None,
                end - u64::from(ev.header().event_size()),
            )?;
        }
        match sql.trim().to_ascii_uppercase().as_str() {
            "BEGIN" => {
                if let Some(t) = &mut self.txn {
                    t.with_begin = true;
                }
                return Ok(());
            }
            // MariaDB ends a transaction on a non-transactional engine with COMMIT; a ROLLBACK is
            // logged when the transaction's non-transactional changes stay, and after the rows of
            // a transactional table it rolled back (measured on 10.11, 11.4 and 11.8).
            "COMMIT" => return self.commit(end),
            "ROLLBACK" => {
                self.rolled_back("a ROLLBACK", end)?;
                return self.commit(end);
            }
            _ => {}
        }
        match verb(&sql).as_str() {
            // A branch's rows come between its START and END, and its prepare ends it; its
            // COMMIT or ROLLBACK is a transaction of its own, with no rows.
            "XA" => {
                let word = sql.split_whitespace().nth(1).unwrap_or_default();
                return match word.to_ascii_uppercase().as_str() {
                    "START" | "BEGIN" => {
                        if let Some(t) = &mut self.txn {
                            t.xa = true;
                        }
                        Ok(())
                    }
                    "COMMIT" | "ROLLBACK" => self.commit(end),
                    _ => Ok(()),
                };
            }
            // MariaDB logs the rows a `ROLLBACK TO` a savepoint undid, then the statement.
            "ROLLBACK" => return self.rolled_back("a savepoint rollback", end),
            "SAVEPOINT" | "RELEASE" => return Ok(()),
            _ => {}
        }
        let seq = self.seq(end)?;
        self.statement(&sql, &db, seq)?;
        if !self.txn.as_ref().is_some_and(|t| t.with_begin) {
            self.commit(end)?;
        }
        Ok(())
    }

    /// Routes a statement (channel B): halts, truncates and schema signals for the tracked tables
    /// it touches.
    fn statement(&mut self, sql: &str, db: &str, seq: SourceSeq) -> Result<(), SourceError> {
        let d = classify(sql, db);
        if d.unreadable {
            return Err(permanent(format!(
                "a table statement the source can't read the tables of: {sql}"
            )));
        }
        // A trigger or a multi-table statement can reach any table of the source's database:
        // the pipeline halts when the statement runs there or names it. (A statement in another
        // database whose trigger writes to the source's isn't seen.)
        if let Some(named) = &d.logged_as_statement {
            let source_db = &self.catalog.pipeline.source.db;
            if db == source_db.as_str() || named.iter().any(|n| n == source_db) {
                return Err(permanent(format!(
                    "a row change logged as its statement, without its rows (a session's \
                     binlog_format isn't ROW) in {source_db}: {sql}"
                )));
            }
        }
        let tracked: Vec<TableRef> = self.tables.keys().cloned().collect();
        let source_db = self.catalog.pipeline.source.db.clone();
        let mut halts: Vec<(TableRef, HaltReason)> = vec![];
        if d.dropped_database.as_deref() == Some(source_db.as_str()) {
            halts.extend(
                tracked
                    .iter()
                    .map(|t| (t.clone(), HaltReason::TableDropped)),
            );
        }
        for (t, op) in &d.unlogged {
            halts.push((
                t.clone(),
                HaltReason::UnsafeDdl {
                    detail: format!("{op} changed rows without row events; re-snapshot the table"),
                },
            ));
        }
        halts.extend(
            d.dropped
                .iter()
                .map(|t| (t.clone(), HaltReason::TableDropped)),
        );
        let mut signals: Vec<TableRef> = vec![];
        for (t, r) in renamed(&d.renamed, &tracked) {
            match r {
                Renamed::Gone => halts.push((t, HaltReason::TableDropped)),
                Renamed::Swapped { .. } => signals.push(t),
                Renamed::Kept => {}
            }
        }
        halts.retain(|(t, _)| self.tables.contains_key(t));
        let halted = |t: &TableRef| halts.iter().any(|(h, _)| h == t);
        for a in &d.altered {
            if self.tables.contains_key(&a.table)
                && !halted(&a.table)
                && !signals.contains(&a.table)
            {
                signals.push(a.table.clone());
            }
        }
        signals.retain(|t| !halted(t));
        let truncated: Vec<TableRef> = d
            .truncated
            .iter()
            .filter(|t| self.tables.contains_key(t) && !halted(t))
            .cloned()
            .collect();
        for (t, reason) in halts {
            if self.tables.contains_key(&t) {
                self.halt(&t, seq, reason)?;
            }
        }
        if !truncated.is_empty() {
            self.begin()?;
            self.emit(StreamItem::Truncate {
                tables: truncated,
                seq,
            });
        }
        if !signals.is_empty() {
            self.begin()?;
            self.signals = Some(Signals {
                sql: sql.to_owned(),
                seq,
                tables: signals.into(),
                after: vec![],
            });
        }
        Ok(())
    }

    /// Reads the pending signals' changes from the catalog, one table at a time: each answer
    /// re-plans the table, is remembered for the pipeline, and goes out as a signal.
    async fn read_signals(&mut self) -> Result<(), SourceError> {
        loop {
            let Some(s) = &self.signals else {
                return Ok(());
            };
            let Some(table) = s.tables.front().cloned() else {
                let s = self.signals.take().expect("pending");
                self.ready.extend(s.after);
                return Ok(());
            };
            let (sql, seq) = (s.sql.clone(), s.seq);
            let Some(cached) = self.tables.get(&table).map(|t| t.schema.clone()) else {
                self.signals.as_mut().expect("pending").tables.pop_front();
                continue;
            };
            let hint = SchemaHint::Ddl(sql.clone());
            let read = self.catalog.read_change(&cached, &hint).await;
            self.signals.as_mut().expect("pending").tables.pop_front();
            let item = match read {
                Ok((delta, now)) => {
                    self.catalog
                        .shared
                        .remember(&cached, &sql, (delta, now.clone()));
                    match now.map(|n| self.catalog.track(&n)) {
                        None => {
                            self.tables.remove(&table);
                        }
                        Some(Ok(t)) => {
                            self.tables.insert(table.clone(), t);
                        }
                        Some(Err(reason)) => {
                            self.tables.remove(&table);
                            self.ready
                                .push_back(StreamItem::TableHalt { table, seq, reason });
                            continue;
                        }
                    }
                    StreamItem::SchemaSignal { table, seq, hint }
                }
                Err(SourceError::TableHalt { reason, .. }) => {
                    self.tables.remove(&table);
                    StreamItem::TableHalt { table, seq, reason }
                }
                Err(e) => return Err(e),
            };
            self.ready.push_back(item);
        }
    }

    fn rows(&mut self, ev: &Event, end: u64) -> Result<(), SourceError> {
        let Some(EventData::RowsEvent(re)) = ev.read_data().map_err(permanent)? else {
            return Err(permanent("an unreadable rows event"));
        };
        let tme = self
            .events
            .get_tme(re.table_id())
            .ok_or_else(|| permanent("a rows event without its table map"))?
            .clone();
        let table = TableRef::new(tme.database_name(), tme.table_name());
        let Some(tracked) = self.tables.get(&table).cloned() else {
            return Ok(());
        };
        let seq = self.seq(end)?;
        let mismatch = |detail: String| HaltReason::SchemaMismatch {
            column: String::new(),
            detail,
        };
        if let Err(detail) = check_map(&tme, &tracked) {
            return self.halt(
                &table,
                seq,
                mismatch(format!("{detail}; re-snapshot the table")),
            );
        }
        let n = tracked.columns.len();
        let full = re
            .columns_before_image()
            .is_none_or(|b| b.iter().take(n).all(|x| *x))
            && re
                .columns_after_image()
                .is_none_or(|b| b.iter().take(n).all(|x| *x));
        if !full {
            return self.halt(
                &table,
                seq,
                mismatch("a row image that isn't FULL (a session set binlog_row_image)".into()),
            );
        }
        let op = match &re {
            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => Op::Insert,
            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => Op::Delete,
            _ => Op::Update,
        };
        self.begin()?;
        let txn = self.txn.clone().expect("begun");
        for row in re.rows(&tme) {
            let (before, after) = row.map_err(permanent)?;
            let before: Option<Vec<BinlogValue<'static>>> = before.map(|r| r.unwrap());
            let after: Option<Vec<BinlogValue<'static>>> = after.map(|r| r.unwrap());
            let decode = |values: &[BinlogValue<'static>],
                          prior: Option<&[BinlogValue<'static>]>| {
                tracked
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let v = values
                            .get(i)
                            .ok_or_else(|| mismatch(format!("a row without column {}", c.name)))?;
                        let b = prior.and_then(|p| p.get(i)).map(Input::Binlog);
                        decode_after(&c.decode, b, Input::Binlog(v), c.identifier)
                            .map(|value| (c.name.clone(), value))
                            .map_err(|e| halt_reason(c.name.clone(), e))
                    })
                    .collect::<Result<Vec<_>, HaltReason>>()
                    .map(Row)
            };
            let images = (|| {
                let b = before.as_deref().map(|v| decode(v, None)).transpose()?;
                let a = after
                    .as_deref()
                    .map(|v| decode(v, before.as_deref()))
                    .transpose()?;
                Ok::<_, HaltReason>((b, a))
            })();
            match images {
                Ok((before, after)) => self.emit(StreamItem::Change(CdcEvent {
                    op,
                    table: table.clone(),
                    before,
                    after,
                    source_pos: txn.identity.clone(),
                    change_seq: seq,
                    xid: txn.xid,
                    commit_ts_micros: txn.commit_ts_micros,
                })),
                Err(reason) => return self.halt(&table, seq, reason),
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for MySqlStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlStream")
            .field("mode", &self.mode)
            .field("offsets", &self.offsets)
            .field("position", &self.position)
            .field("txn", &self.txn)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChangeStream for MySqlStream {
    async fn next(&mut self) -> Result<StreamItem, SourceError> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return Ok(item);
            }
            if self.signals.is_some() {
                self.read_signals().await?;
                continue;
            }
            let at = self.position.to_tagged();
            let ev = match self.events.next().await {
                Some(Ok(ev)) => ev,
                Some(Err(e)) => {
                    let action = self.catalog.mysql.binlog_purge_action;
                    return Err(purge_action(action, mysql(e, Some(&at))));
                }
                None => {
                    return Err(SourceError::Transient(
                        "the server ended the binlog stream".into(),
                    ));
                }
            };
            self.on_event(ev)?;
        }
    }

    async fn acknowledge(&mut self, _: &SourcePosition) -> Result<(), SourceError> {
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), SourceError> {
        self.catalog.shared.dump.store(0, Ordering::Relaxed);
        self.events.close().await.map_err(|e| mysql(e, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gtid_set_becomes_sid_blocks_per_uuid_and_tag() {
        let set: GtidSet = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7:alpha:1-3,4e11fa47-71ca-11e1-9e33-c80aa9429562:9"
            .parse()
            .unwrap();
        let s = sids(&set).unwrap();
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].intervals().len(), 2);
        assert_eq!(s[1].tag().map(|t| t.as_str()), Some("alpha"));
        assert_eq!(
            (s[2].intervals()[0].start(), s[2].intervals()[0].end()),
            (9, 10)
        );
        assert!(sids(&GtidSet::default()).unwrap().is_empty());
    }

    #[test]
    fn statements_lead_with_their_verb() {
        assert_eq!(verb("  begin"), "BEGIN");
        assert_eq!(verb("XA START 'x'"), "XA");
        assert_eq!(verb("SAVEPOINT `s`"), "SAVEPOINT");
        assert_eq!(verb("/* c */ ALTER"), "");
    }

    #[test]
    fn column_types_arrive_as_their_binlog_types() {
        use ColumnType::*;
        assert!(arrives_as("bigint").contains(&MYSQL_TYPE_LONGLONG));
        assert!(arrives_as("enum").contains(&MYSQL_TYPE_STRING));
        assert!(arrives_as("longtext").contains(&MYSQL_TYPE_BLOB));
        assert!(arrives_as("datetime").contains(&MYSQL_TYPE_DATETIME2));
        assert!(!arrives_as("int").contains(&MYSQL_TYPE_LONGLONG));
        assert!(arrives_as("nonsense").is_empty());
    }
}
