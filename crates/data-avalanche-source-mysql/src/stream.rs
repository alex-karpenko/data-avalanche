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
//!   `augment` of the same signal. A table map that doesn't match the schema halts the table,
//!   never misreads a row: its columns and types, and what `binlog_row_metadata = FULL` adds
//!   (names, signedness, character sets, ENUM and SET labels; a table map without names halts
//!   too). So does a row image that isn't FULL.
//! - **Names.** Under `lower_case_table_names` other than 0, table and database names compare
//!   without case.
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
    CdcEvent, ChangeStream, Checkpoint, HaltReason, Op, Row, SchemaHint, SourceError,
    SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary,
};
use futures_util::StreamExt;
use mysql_async::binlog::events::{
    DefaultCharset, Event, EventData, OptionalMetadataField, RowsEventData, TableMapEvent,
    TransactionPayloadEvent,
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
use crate::position::{Mode, identity, identity_query};
use crate::server::{Flavour, Fold};
use crate::start::{purge_action, same_server};
use crate::types::{CatalogColumn, members, plan_column};
use crate::value::{Charset, Decode, Input, decode_after, halt_reason, text_of};

/// One column of a tracked table, as the stream decodes it and checks table maps against it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Column {
    name: String,
    data_type: String,
    decode: Decode,
    identifier: bool,
    unsigned: bool,
    /// The character set, `binary` for a column without one.
    charset: String,
    /// An ENUM's or a SET's labels.
    labels: Option<Vec<String>>,
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
    /// The last `Begin.xid` of a MySQL transaction, or of one without a GTID: a per-session
    /// counter (D-M7a.19).
    xids: u64,
    /// How the server compares table and database names.
    fold: Fold,
    /// The character set of each collation id the server lists.
    charsets: HashMap<u16, String>,
    /// The table map each tracked table's schema was last found to match.
    checked: HashMap<TableRef, TableMapEvent<'static>>,
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

/// What a table map says of its columns: each one's binlog type (an ENUM's or a SET's, not the
/// `STRING` it is logged as) and the optional metadata `binlog_row_metadata = FULL` carries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct MapMeta {
    types: Vec<ColumnType>,
    names: Option<Vec<String>>,
    /// One per numeric column, in column order: `true` is unsigned.
    unsigned: Option<Vec<bool>>,
    /// The collation of each character column, in column order.
    collations: Option<Vec<u16>>,
    /// The collation of each ENUM and SET column, in column order.
    enum_set_collations: Option<Vec<u16>>,
    /// The labels of each ENUM column, and of each SET column, in column order, as logged.
    enums: Vec<Vec<Vec<u8>>>,
    sets: Vec<Vec<Vec<u8>>>,
}

/// The collations of `n` columns from a default and its exceptions.
fn expand(d: &DefaultCharset<'_>, n: usize) -> Result<Vec<u16>, String> {
    let mut out = vec![d.default_charset(); n];
    for x in d.iter_non_default() {
        let x = x.map_err(|e| format!("the table map's character sets: {e}"))?;
        *out.get_mut(x.column_index() as usize)
            .ok_or("the table map's character sets name a column it hasn't")? = x.charset();
    }
    Ok(out)
}

/// Reads what the table map says of its columns.
fn map_meta(tme: &TableMapEvent<'_>) -> Result<MapMeta, String> {
    let mut m = MapMeta::default();
    for i in 0..tme.columns_count() as usize {
        let ty = tme
            .get_column_type(i)
            .map_err(|e| format!("column {i}: {e}"))?
            .ok_or_else(|| format!("column {i}: no binlog type"))?;
        m.types.push(ty);
    }
    let count = |f: fn(&ColumnType) -> bool| m.types.iter().filter(|t| f(t)).count();
    let (characters, enum_sets) = (
        count(ColumnType::is_character_type),
        count(ColumnType::is_enum_or_set_type),
    );
    let bad = |e: std::io::Error| format!("the table map's metadata: {e}");
    for field in tme.iter_optional_meta() {
        match field.map_err(bad)? {
            OptionalMetadataField::ColumnName(c) => {
                m.names = Some(
                    c.iter_names()
                        .map(|n| n.map(|n| n.name().into_owned()))
                        .collect::<Result<_, _>>()
                        .map_err(bad)?,
                )
            }
            OptionalMetadataField::Signedness(b) => m.unsigned = Some(b.iter().by_vals().collect()),
            OptionalMetadataField::DefaultCharset(d) => {
                m.collations = Some(expand(&d, characters)?)
            }
            OptionalMetadataField::ColumnCharset(c) => {
                m.collations = Some(c.iter_charsets().collect::<Result<_, _>>().map_err(bad)?)
            }
            OptionalMetadataField::EnumAndSetDefaultCharset(d) => {
                m.enum_set_collations = Some(expand(&d, enum_sets)?)
            }
            OptionalMetadataField::EnumAndSetColumnCharset(c) => {
                m.enum_set_collations =
                    Some(c.iter_charsets().collect::<Result<_, _>>().map_err(bad)?)
            }
            OptionalMetadataField::EnumStrValue(e) => {
                for labels in e.iter_values() {
                    let labels = labels.map_err(bad)?;
                    m.enums.push(
                        labels
                            .values()
                            .iter()
                            .map(|v| v.value_raw().to_vec())
                            .collect(),
                    );
                }
            }
            OptionalMetadataField::SetStrValue(e) => {
                for labels in e.iter_values() {
                    let labels = labels.map_err(bad)?;
                    m.sets.push(
                        labels
                            .values()
                            .iter()
                            .map(|v| v.value_raw().to_vec())
                            .collect(),
                    );
                }
            }
            _ => {}
        }
    }
    Ok(m)
}

/// A character set's name as both the catalog and the collation table spell it.
fn charset_name(name: &str) -> &str {
    match name {
        "utf8" => "utf8mb3",
        other => other,
    }
}

/// Whether the table map describes the rows the schema decodes (config §11.2): as many columns,
/// each of a binlog type its type arrives as, and in the metadata `binlog_row_metadata = FULL`
/// gives, the same names (without case), signedness, character sets (each collation id mapped by
/// `charsets`, the server's table) and ENUM and SET labels. A table map without names, or without
/// what it should carry for the columns it has, doesn't describe them.
fn check_meta(
    m: &MapMeta,
    columns: &[Column],
    charsets: &HashMap<u16, String>,
) -> Result<(), String> {
    let n = m.types.len();
    if n != columns.len() {
        return Err(format!(
            "the binlog's rows have {n} columns, the schema {}",
            columns.len()
        ));
    }
    for (c, ty) in columns.iter().zip(&m.types) {
        if !arrives_as(&c.data_type).contains(ty) {
            return Err(format!(
                "column {}: the binlog's rows have {ty:?}, the schema {}",
                c.name, c.data_type
            ));
        }
    }
    let Some(names) = &m.names else {
        return Err(
            "the binlog's table map names no columns (binlog_row_metadata isn't FULL)".into(),
        );
    };
    if names.len() != n {
        return Err(format!(
            "the binlog's table map names {} of {n} columns",
            names.len()
        ));
    }
    for (c, name) in columns.iter().zip(names) {
        if !(c.name == *name || c.name.to_lowercase() == name.to_lowercase()) {
            return Err(format!("column {}: the binlog names it {name:?}", c.name));
        }
    }
    // Each kind of column with its list in the table map: one entry per column of the kind.
    let of_kind = |f: fn(&ColumnType) -> bool| -> Vec<&Column> {
        columns
            .iter()
            .zip(&m.types)
            .filter(|(_, t)| f(t))
            .map(|(c, _)| c)
            .collect()
    };
    let missing = |what: &str, have: usize, want: usize| {
        format!("the binlog's table map carries {what} for {have} of {want} columns")
    };
    let numeric = of_kind(ColumnType::is_numeric_type);
    if !numeric.is_empty() {
        let flags = m.unsigned.as_deref().unwrap_or_default();
        if flags.len() != numeric.len() {
            return Err(missing("signedness", flags.len(), numeric.len()));
        }
        for (c, unsigned) in numeric.iter().zip(flags) {
            // The server flags every YEAR unsigned; its values don't depend on it.
            if c.data_type != "year" && c.unsigned != *unsigned {
                let sign = |u: bool| if u { "unsigned" } else { "signed" };
                return Err(format!(
                    "column {}: the binlog has it {}, the schema {}",
                    c.name,
                    sign(*unsigned),
                    sign(c.unsigned)
                ));
            }
        }
    }
    let charset = |c: &Column, id: &u16| match charsets.get(id) {
        None => Err(format!(
            "column {}: collation {id} isn't one the server lists",
            c.name
        )),
        Some(cs) if charset_name(cs) != charset_name(&c.charset) => Err(format!(
            "column {}: the binlog has it in character set {cs}, the schema in {}",
            c.name, c.charset
        )),
        Some(_) => Ok(()),
    };
    for (kind, list, what) in [
        (
            ColumnType::is_character_type as fn(&ColumnType) -> bool,
            &m.collations,
            "character sets",
        ),
        (
            ColumnType::is_enum_or_set_type,
            &m.enum_set_collations,
            "ENUM and SET character sets",
        ),
    ] {
        let of = of_kind(kind);
        if of.is_empty() {
            continue;
        }
        let ids = list.as_deref().unwrap_or_default();
        if ids.len() != of.len() {
            return Err(missing(what, ids.len(), of.len()));
        }
        for (c, id) in of.iter().zip(ids) {
            charset(c, id)?;
        }
    }
    for (ty, logged, what) in [
        (ColumnType::MYSQL_TYPE_ENUM, &m.enums, "ENUM labels"),
        (ColumnType::MYSQL_TYPE_SET, &m.sets, "SET labels"),
    ] {
        let of: Vec<&Column> = columns
            .iter()
            .zip(&m.types)
            .filter(|(_, t)| **t == ty)
            .map(|(c, _)| c)
            .collect();
        if logged.len() != of.len() {
            return Err(missing(what, logged.len(), of.len()));
        }
        for (c, labels) in of.iter().zip(logged) {
            // In a character set the source doesn't read, the labels can only be counted.
            let Some(cs) = Charset::of(&c.charset) else {
                let want = c.labels.as_ref().map(Vec::len);
                if Some(labels.len()) != want {
                    return Err(format!(
                        "column {}: the binlog has {} labels, the schema {}",
                        c.name,
                        labels.len(),
                        want.map_or("none".into(), |n| n.to_string())
                    ));
                }
                continue;
            };
            let read: Option<Vec<String>> = labels.iter().map(|l| text_of(cs, l)).collect();
            if read.as_ref() != c.labels.as_ref() {
                return Err(format!(
                    "column {}: the binlog's labels are {read:?}, the schema's {:?}",
                    c.name, c.labels
                ));
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

/// Whether a statement run in `db` can name a table of `source_db`: it runs there, or its text
/// holds the database's name (without case, so an unquoted or a quoted one alike). One that
/// can't routes nowhere, unread: most of a busy server's statements are other databases'.
fn may_name(sql: &str, db: &str, source_db: &str, fold: Fold) -> bool {
    // A name folded beyond ASCII can't be found bytewise, nor one with a quote character, which
    // a quoted name doubles.
    if fold.eq(db, source_db)
        || (fold.fold && !source_db.is_ascii())
        || source_db.is_empty()
        || source_db.contains(['`', '"'])
    {
        return true;
    }
    sql.as_bytes()
        .windows(source_db.len())
        .any(|w| w.eq_ignore_ascii_case(source_db.as_bytes()))
}

/// What a statement does to the tracked tables (channel B).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Routed {
    halts: Vec<(TableRef, HaltReason)>,
    truncated: Vec<TableRef>,
    signals: Vec<TableRef>,
}

/// Routes a statement run in `db`: the halts, truncates and schema signals of the `tracked`
/// tables it touches, their names compared as `fold` says (`lower_case_table_names`).
fn route(
    sql: &str,
    db: &str,
    source_db: &str,
    tracked: &[TableRef],
    fold: Fold,
) -> Result<Routed, SourceError> {
    if !may_name(sql, db, source_db, fold) {
        return Ok(Routed::default());
    }
    let d = classify(sql, db);
    if d.unreadable {
        return Err(permanent(format!(
            "a table statement the source can't read the tables of: {sql}"
        )));
    }
    // A trigger or a multi-table statement can reach any table of the source's database:
    // the pipeline halts when the statement runs there or names it. (A statement in another
    // database whose trigger writes to the source's isn't seen.)
    if let Some(named) = &d.logged_as_statement
        && (fold.eq(db, source_db) || named.iter().any(|n| fold.eq(n, source_db)))
    {
        return Err(permanent(format!(
            "a row change logged as its statement, without its rows (a session's \
             binlog_format isn't ROW) in {source_db}: {sql}"
        )));
    }
    // The tracked table a statement's name is.
    let find = |t: &TableRef| tracked.iter().find(|k| fold.same(k, t)).cloned();
    let mut halts: Vec<(TableRef, HaltReason)> = vec![];
    if d.dropped_database
        .as_deref()
        .is_some_and(|x| fold.eq(x, source_db))
    {
        halts.extend(
            tracked
                .iter()
                .map(|t| (t.clone(), HaltReason::TableDropped)),
        );
    }
    for (t, op) in &d.unlogged {
        if let Some(t) = find(t) {
            halts.push((
                t,
                HaltReason::UnsafeDdl {
                    detail: format!("{op} changed rows without row events; re-snapshot the table"),
                },
            ));
        }
    }
    halts.extend(
        d.dropped
            .iter()
            .filter_map(find)
            .map(|t| (t, HaltReason::TableDropped)),
    );
    let mut signals: Vec<TableRef> = vec![];
    let renames: Vec<(TableRef, TableRef)> = d
        .renamed
        .iter()
        .map(|(a, b)| (fold.key(a), fold.key(b)))
        .collect();
    let keys: Vec<TableRef> = tracked.iter().map(|t| fold.key(t)).collect();
    for (k, r) in renamed(&renames, &keys) {
        let t = find(&k).expect("a tracked table's key");
        match r {
            Renamed::Gone => halts.push((t, HaltReason::TableDropped)),
            Renamed::Swapped { .. } => signals.push(t),
            Renamed::Kept => {}
        }
    }
    let halted = |t: &TableRef| halts.iter().any(|(h, _)| h == t);
    for t in d.altered.iter().filter_map(|a| find(&a.table)) {
        if !halted(&t) && !signals.contains(&t) {
            signals.push(t);
        }
    }
    signals.retain(|t| !halted(t));
    let truncated: Vec<TableRef> = d
        .truncated
        .iter()
        .filter_map(find)
        .filter(|t| !halted(t))
        .collect();
    Ok(Routed {
        halts,
        truncated,
        signals,
    })
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
            let labels =
                matches!(c.data_type.as_str(), "enum" | "set").then(|| members(&c.column_type));
            columns.push(Column {
                name: col.name.clone(),
                unsigned: c.unsigned(),
                charset: c.charset.clone().unwrap_or_else(|| "binary".into()),
                labels,
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

    /// The character set of each collation id: MySQL's `COLLATIONS`; MariaDB's
    /// `COLLATION_CHARACTER_SET_APPLICABILITY`, where its 10.10+ collations (`uca1400`) have their
    /// ids (`COLLATIONS` lists them without one; measured on 10.11, 11.4 and 11.8).
    async fn charsets(&self, flavour: Flavour) -> Result<HashMap<u16, String>, SourceError> {
        let table = match flavour {
            Flavour::Mysql => "COLLATIONS",
            Flavour::Mariadb => "COLLATION_CHARACTER_SET_APPLICABILITY",
        };
        let rows: Vec<(Option<u64>, Option<String>)> = self
            .conn
            .command("read the collations", move |c| {
                Box::pin(async move {
                    c.query(format!(
                        "SELECT ID, CHARACTER_SET_NAME FROM information_schema.{table}"
                    ))
                    .await
                })
            })
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(id, cs)| Some((u16::try_from(id?).ok()?, cs?)))
            .collect())
    }

    /// Opens the binlog dump after `from` for `tables` as of that position (Data-Flow Phase 2b):
    /// the heartbeat period, the replica id `source.mysql.server_id`, and the position in its
    /// dialect. The server's name comparison and collations are read once, for the stream's
    /// routing and table map checks. In file/position mode the dump's own connection must reach
    /// the server `from` was recorded on (`BINLOG_SOURCE_CHANGED`, Q-M7-2): a reconnect after a
    /// failover reaches another server, whose files the position means nothing in.
    pub async fn open_stream(
        &self,
        from: &Checkpoint,
        tables: &[TableSchema],
    ) -> Result<MySqlStream, SourceError> {
        let (recorded, from) = (from, &from.position);
        let flavour = self.flavour().await?;
        let mode = Mode::of(flavour, self.mysql.replication_type);
        let catalog = self.sibling()?;
        let fold = catalog.fold().await?;
        let charsets = catalog.charsets(flavour).await?;
        let mut tracked = HashMap::new();
        for s in tables {
            let t = catalog.track(s).map_err(|reason| SourceError::TableHalt {
                table: s.table().clone(),
                reason,
            })?;
            tracked.insert(s.table().clone(), t);
        }
        let at = from.to_tagged();
        let live = self.live();
        let action = live.binlog_purge_action;
        let err = |e| purge_action(action, mysql(e, Some(&at)));
        let mut conn = self.conn.connect().await?;
        let id = conn.id();
        conn.query_drop(format!(
            "SET @master_heartbeat_period = {}",
            live.heartbeat.as_nanos()
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
                let answer = conn
                    .query_first(identity_query(flavour))
                    .await
                    .map_err(err)?;
                same_server(recorded, &identity(flavour, answer)?)?;
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
            xids: 0,
            fold,
            charsets,
            checked: HashMap::new(),
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

    fn next_xid(&mut self) -> u64 {
        self.xids += 1;
        self.xids
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
        self.checked.remove(table);
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
                let xid = self.next_xid();
                if anonymous {
                    return self.start(xid, ts, Gtid::None, start);
                }
                if g.gno() == 0 {
                    return Err(permanent("a GTID event with transaction number 0"));
                }
                let source = GtidSource {
                    uuid: uuid::Uuid::from_bytes(g.sid()),
                    tag: g.tag().map(|t| t.as_str().to_owned()),
                };
                self.start(xid, ts, Gtid::Mysql(source, g.gno()), start)
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
            let xid = self.next_xid();
            let ts = i64::from(ev.header().timestamp()) * 1_000_000;
            self.start(
                xid,
                ts,
                Gtid::None,
                end - u64::from(ev.header().event_size()),
            )?;
        }
        let whole = sql.trim();
        if whole.eq_ignore_ascii_case("BEGIN") {
            if let Some(t) = &mut self.txn {
                t.with_begin = true;
            }
            return Ok(());
        }
        // MariaDB ends a transaction on a non-transactional engine with COMMIT; a ROLLBACK is
        // logged when the transaction's non-transactional changes stay, and after the rows of a
        // transactional table it rolled back (measured on 10.11, 11.4 and 11.8).
        if whole.eq_ignore_ascii_case("COMMIT") {
            return self.commit(end);
        }
        if whole.eq_ignore_ascii_case("ROLLBACK") {
            self.rolled_back("a ROLLBACK", end)?;
            return self.commit(end);
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
        let tracked: Vec<TableRef> = self.tables.keys().cloned().collect();
        let source_db = &self.catalog.pipeline.source.db;
        let Routed {
            halts,
            truncated,
            signals,
        } = route(sql, db, source_db, &tracked, self.fold)?;
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
                    self.checked.remove(&table);
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
                    self.checked.remove(&table);
                    StreamItem::TableHalt { table, seq, reason }
                }
                Err(e) => return Err(e),
            };
            self.ready.push_back(item);
        }
    }

    /// The tracked table `t` names, its names compared as the server does.
    fn tracked(&self, t: &TableRef) -> Option<TableRef> {
        if self.tables.contains_key(t) {
            return Some(t.clone());
        }
        match self.fold.fold {
            true => self.tables.keys().find(|k| self.fold.same(k, t)).cloned(),
            false => None,
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
        let Some(table) = self.tracked(&TableRef::new(tme.database_name(), tme.table_name()))
        else {
            return Ok(());
        };
        let tracked = self.tables.get(&table).cloned().expect("tracked");
        let seq = self.seq(end)?;
        let mismatch = |detail: String| HaltReason::SchemaMismatch {
            column: String::new(),
            detail,
        };
        if self.checked.get(&table) != Some(&tme) {
            let checked =
                map_meta(&tme).and_then(|m| check_meta(&m, &tracked.columns, &self.charsets));
            if let Err(detail) = checked {
                return self.halt(
                    &table,
                    seq,
                    mismatch(format!("{detail}; re-snapshot the table")),
                );
            }
            self.checked.insert(table.clone(), tme.clone());
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
                    let action = self.catalog.live().binlog_purge_action;
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

    fn shop(name: &str) -> TableRef {
        TableRef::new("shop", name)
    }

    #[test]
    fn statements_route_by_the_servers_name_comparison() {
        let tracked = [shop("orders"), shop("items")];
        let route = |sql: &str, fold: i64| route(sql, "shop", "shop", &tracked, Fold::of(fold));
        let codes = |r: &Routed| -> Vec<(String, &str)> {
            r.halts
                .iter()
                .map(|(t, h)| (t.name.clone(), h.code()))
                .collect()
        };
        // lower_case_table_names = 1: another case is the same table, the tracked spelling kept.
        let r = route("TRUNCATE TABLE Orders", 1).unwrap();
        assert_eq!(r.truncated, [shop("orders")]);
        let r = route("DROP TABLE Shop.Orders", 1).unwrap();
        assert_eq!(codes(&r), [("orders".to_owned(), "TABLE_DROPPED")]);
        let r = route("ALTER TABLE ITEMS ADD COLUMN x INT", 1).unwrap();
        assert_eq!(r.signals, [shop("items")]);
        let r = route("RENAME TABLE Orders TO old_orders", 1).unwrap();
        assert_eq!(codes(&r), [("orders".to_owned(), "TABLE_DROPPED")]);
        let r = route(
            "RENAME TABLE Orders TO _orders_del, _Orders_gho TO ORDERS",
            1,
        )
        .unwrap();
        assert_eq!((r.signals, r.halts), (vec![shop("orders")], vec![]));
        let r = route("ALTER TABLE Orders DROP PARTITION p0", 1).unwrap();
        assert_eq!(codes(&r), [("orders".to_owned(), "UNSAFE_DDL")]);
        let r = route("DROP DATABASE SHOP", 1).unwrap();
        assert_eq!(r.halts.len(), 2);
        assert!(route("INSERT INTO t VALUES (1)", 1).is_err());
        assert!(
            route_in("other", "INSERT INTO SHOP.t SELECT 1", 1).is_err(),
            "a qualifier in another case names the source's database"
        );
        // lower_case_table_names = 0 (Linux): names are as written, and none of these match.
        for sql in [
            "TRUNCATE TABLE Orders",
            "DROP TABLE Shop.Orders",
            "ALTER TABLE ITEMS ADD COLUMN x INT",
            "RENAME TABLE Orders TO old_orders",
            "DROP DATABASE SHOP",
        ] {
            assert_eq!(route(sql, 0).unwrap(), Routed::default(), "{sql}");
        }
        assert!(route_in("other", "INSERT INTO SHOP.t SELECT 1", 0).is_ok());
        // As written, the statements reach the tables either way.
        assert_eq!(
            route("TRUNCATE TABLE orders", 0).unwrap().truncated,
            [shop("orders")]
        );
    }

    #[test]
    fn other_databases_statements_route_nowhere_unread() {
        let exact = Fold::default();
        // Another database's statement that doesn't name the source's: not classified, so even
        // one the classifier can't read routes nowhere.
        assert!(!may_name(
            "ALTER TABLE x ADD COLUMN y INT",
            "other",
            "shop",
            exact
        ));
        assert_eq!(
            route_in("other", "DROP TABLE", 0).unwrap(),
            Routed::default()
        );
        assert!(route_in("shop", "DROP TABLE", 0).is_err());
        // Naming it, in any case or quoted, still classifies.
        assert!(may_name("TRUNCATE `SHOP`.orders", "other", "shop", exact));
        assert_eq!(
            route_in("other", "TRUNCATE TABLE shop.orders", 0)
                .unwrap()
                .truncated,
            [shop("orders")]
        );
        assert!(route_in("other", "INSERT INTO shop.orders SELECT 1", 0).is_err());
        // In the source's database, or under a fold beyond ASCII, always.
        assert!(may_name("TRUNCATE orders", "shop", "shop", exact));
        assert!(may_name("TRUNCATE orders", "SHOP", "shop", Fold::of(1)));
        assert!(may_name("TRUNCATE x", "other", "çafé", Fold::of(1)));
        assert!(may_name("TRUNCATE `a``b`.orders", "other", "a`b", exact));
    }

    fn route_in(db: &str, sql: &str, fold: i64) -> Result<Routed, SourceError> {
        route(sql, db, "shop", &[shop("orders")], Fold::of(fold))
    }

    fn column(name: &str, data_type: &str, charset: &str) -> Column {
        Column {
            name: name.into(),
            data_type: data_type.into(),
            decode: Decode::Bool,
            identifier: false,
            unsigned: false,
            charset: charset.into(),
            labels: None,
        }
    }

    /// A table `(id INT UNSIGNED, v VARCHAR latin1, b VARBINARY, e ENUM('Active','ç'), s SET('X'),
    /// y YEAR)` as the schema has it, and its table map under FULL (measured shapes: YEAR flagged
    /// unsigned, binary columns in binary's collation 63).
    fn table() -> (Vec<Column>, MapMeta, HashMap<u16, String>) {
        use ColumnType::*;
        let mut id = column("id", "int", "binary");
        id.unsigned = true;
        let mut e = column("e", "enum", "utf8mb4");
        e.labels = Some(vec!["Active".into(), "ç".into()]);
        let mut st = column("s", "set", "latin1");
        st.labels = Some(vec!["X".into()]);
        let columns = vec![
            id,
            column("v", "varchar", "latin1"),
            column("b", "varbinary", "binary"),
            e,
            st,
            column("y", "year", "binary"),
        ];
        let m = MapMeta {
            types: vec![
                MYSQL_TYPE_LONG,
                MYSQL_TYPE_VARCHAR,
                MYSQL_TYPE_VARCHAR,
                MYSQL_TYPE_ENUM,
                MYSQL_TYPE_SET,
                MYSQL_TYPE_YEAR,
            ],
            names: Some(["id", "v", "b", "e", "s", "y"].map(String::from).to_vec()),
            unsigned: Some(vec![true, true]),
            collations: Some(vec![8, 63]),
            enum_set_collations: Some(vec![2304, 8]),
            enums: vec![vec![b"Active".to_vec(), "ç".as_bytes().to_vec()]],
            sets: vec![vec![b"X".to_vec()]],
        };
        let charsets = [(8, "latin1"), (63, "binary"), (2304, "utf8mb4")]
            .map(|(id, cs)| (id, cs.to_owned()))
            .into();
        (columns, m, charsets)
    }

    #[test]
    fn a_table_map_under_full_metadata_matches_its_schema() {
        let (columns, m, charsets) = table();
        assert_eq!(check_meta(&m, &columns, &charsets), Ok(()));
        // Names compare without case; utf8 is utf8mb3.
        let mut upper = m.clone();
        upper.names.as_mut().unwrap()[1] = "V".into();
        assert_eq!(check_meta(&upper, &columns, &charsets), Ok(()));
        let mut c = columns.clone();
        c[1].charset = "utf8".into();
        let mut cs = charsets.clone();
        cs.insert(33, "utf8mb3".into());
        let mut utf8 = m.clone();
        utf8.collations = Some(vec![33, 63]);
        assert_eq!(check_meta(&utf8, &c, &cs), Ok(()));
    }

    #[test]
    fn a_table_map_that_differs_from_its_schema_halts() {
        let (columns, m, charsets) = table();
        let fails = |m: &MapMeta, columns: &[Column], needle: &str| {
            let e = check_meta(m, columns, &charsets).unwrap_err();
            assert!(e.contains(needle), "{needle}: {e}");
        };
        let changed = |f: &dyn Fn(&mut MapMeta)| {
            let mut x = m.clone();
            f(&mut x);
            x
        };
        // Under MINIMAL or NO_LOG: no names.
        fails(
            &changed(&|x| x.names = None),
            &columns,
            "the binlog's table map names no columns (binlog_row_metadata isn't FULL)",
        );
        fails(
            &changed(&|x| x.names.as_mut().unwrap()[2] = "w".into()),
            &columns,
            "column b: the binlog names it \"w\"",
        );
        // Signedness: a flip on an integer halts; YEAR's flag isn't compared.
        fails(
            &changed(&|x| x.unsigned = Some(vec![false, true])),
            &columns,
            "column id: the binlog has it signed, the schema unsigned",
        );
        assert_eq!(
            check_meta(
                &changed(&|x| x.unsigned = Some(vec![true, false])),
                &columns,
                &charsets
            ),
            Ok(())
        );
        fails(
            &changed(&|x| x.unsigned = None),
            &columns,
            "carries signedness for 0 of 2 columns",
        );
        // Character sets: another set, an unknown collation, a missing list.
        fails(
            &changed(&|x| x.collations = Some(vec![2304, 63])),
            &columns,
            "column v: the binlog has it in character set utf8mb4, the schema in latin1",
        );
        fails(
            &changed(&|x| x.collations = Some(vec![9999, 63])),
            &columns,
            "column v: collation 9999 isn't one the server lists",
        );
        fails(
            &changed(&|x| x.collations = None),
            &columns,
            "carries character sets for 0 of 2 columns",
        );
        fails(
            &changed(&|x| x.enum_set_collations = Some(vec![8, 8])),
            &columns,
            "column e: the binlog has it in character set latin1",
        );
        // ENUM and SET labels: another case, another order, a missing list.
        fails(
            &changed(&|x| x.enums[0][0] = b"active".to_vec()),
            &columns,
            "column e: the binlog's labels are",
        );
        fails(
            &changed(&|x| x.enums[0].reverse()),
            &columns,
            "column e: the binlog's labels are",
        );
        fails(
            &changed(&|x| x.sets[0][0] = b"x".to_vec()),
            &columns,
            "column s: the binlog's labels are",
        );
        fails(
            &changed(&|x| x.sets.clear()),
            &columns,
            "carries SET labels for 0 of 1 columns",
        );
        // A latin1 label reads as latin1: 0xE7 is ç there.
        let mut latin = columns.clone();
        latin[4].labels = Some(vec!["ç".into()]);
        assert_eq!(
            check_meta(&changed(&|x| x.sets[0][0] = vec![0xE7]), &latin, &charsets),
            Ok(())
        );
        // In a character set the source doesn't read, only the number of labels.
        let mut cyrillic = columns.clone();
        cyrillic[3].charset = "cp1251".into();
        let mut cs = charsets.clone();
        cs.insert(51, "cp1251".into());
        let m1251 = changed(&|x| {
            x.enum_set_collations = Some(vec![51, 8]);
            x.enums[0] = vec![vec![0xC0], vec![0xC1]];
        });
        assert_eq!(check_meta(&m1251, &cyrillic, &cs), Ok(()));
        let e = check_meta(
            &changed(&|x| {
                x.enum_set_collations = Some(vec![51, 8]);
                x.enums[0].pop();
            }),
            &cyrillic,
            &cs,
        )
        .unwrap_err();
        assert!(
            e.contains("column e: the binlog has 1 labels, the schema 2"),
            "{e}"
        );
        // The count and the types still come first.
        fails(
            &changed(&|x| {
                x.types.pop();
            }),
            &columns,
            "the binlog's rows have 5 columns, the schema 6",
        );
        fails(
            &changed(&|x| x.types[0] = ColumnType::MYSQL_TYPE_LONGLONG),
            &columns,
            "column id: the binlog's rows have MYSQL_TYPE_LONGLONG",
        );
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
