//! Snapshot reads (Snapshot_Resumability §1–§2, §6): keyset chunks in identifier order, a text
//! identifier column ordered in its own collation when that is deterministic (its index serves the
//! order) and in `"C"` otherwise, and the keyed re-read `toast_strategy: re_query` uses. Rows are read over the simple-query protocol, whose values are the types' output-function
//! text, the same text pgoutput sends; they are decoded like stream values, so a snapshot row and
//! a streamed row of the same data are equal. (A `::text` cast isn't the output function:
//! `true::text` is `true`, pgoutput sends `t`.)
//!
//! A chunk is read in one REPEATABLE READ transaction together with its MVCC snapshot
//! (`pg_current_snapshot()`), and `in_flight` lists the transactions in progress with the tables
//! they write: the pipeline orders a chunk against the stream by transaction visibility, not by
//! WAL position (DA-SNAP-9).

use data_avalanche_core::{
    Chunk, ChunkVisibility, ColumnValue, HaltReason, InFlightTxn, KeysetCursor, Row, SourceError,
    TableSchema, XidSnapshot, identifier_values,
};
use tokio_postgres::SimpleQueryMessage;

use crate::catalog::{NOT_NULL, load_types, table_key};
use crate::connect::{SERVER_FIRST, server_millis};
use crate::connector::PgConnector;
use crate::error::sqlstate;
use crate::slot::{ident, qualified};
use crate::types::plan_column;
use crate::value::{self, Decode, ValueError, text_of};

/// Keys per `fetch_rows` statement, which bounds its size.
pub const FETCH_BATCH: usize = 1000;

/// How to read one column of a table.
#[derive(Debug, Clone)]
pub(crate) struct ReadColumn {
    name: String,
    decode: Decode,
    /// The catalog's type text, for casting a key value.
    type_text: String,
    /// The collation a key comparison uses; `None` for a type without one.
    collation: Option<KeyCollation>,
    /// One of the table's identifier columns: a value that clamps halts the table (Q-M5-18).
    identifier: bool,
}

/// The collation a text key column is ordered and compared in (Snapshot_Resumability §2): one
/// deterministic collation for both, so the order and the cursor agree.
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeyCollation {
    /// The column's own, deterministic: its index serves the order.
    Own(String),
    /// A nondeterministic collation's column, compared in `"C"`.
    C,
}

impl KeyCollation {
    fn name(&self) -> &str {
        match self {
            KeyCollation::Own(name) => name,
            KeyCollation::C => "C",
        }
    }
}

/// The read plan of a table, checked against `schema`: the table is still the one the schema
/// describes, and every column still exists and decodes to the type the schema says. A table gone,
/// or another under its name, halts it; a key column that became nullable halts it (a NULL key
/// sorts after every cursor, so its rows would be skipped). Another difference is transient: the
/// stream's schema signal hasn't caught up with the catalog yet.
async fn read_plan(c: &PgConnector, schema: &TableSchema) -> Result<Vec<ReadColumn>, SourceError> {
    let client = c.conn.client().await?;
    // NULL when the table or its schema is gone: `to_regclass` doesn't raise (§2.1).
    let found = c
        .conn
        .command(
            "find the table",
            client.query_one(
                "SELECT pg_catalog.to_regclass($1)::oid",
                &[&qualified(schema.table())],
            ),
        )
        .await
        .map_err(|e| dropped(schema, e))?;
    let Some(oid) = found.get::<_, Option<u32>>(0) else {
        return Err(halt_dropped(schema));
    };
    if schema.source_id().is_some_and(|id| id != u64::from(oid)) {
        return Err(halt_dropped(schema));
    }
    let names: Vec<&str> = schema.columns().iter().map(|c| c.name.as_str()).collect();
    let rows = c
        .conn
        .command(
            "read the table's columns",
            client.query(
                &format!(
                    "SELECT a.attname, a.atttypid, a.atttypmod,
                            pg_catalog.format_type(a.atttypid, a.atttypmod), co.collname::text,
                            co.collisdeterministic, {NOT_NULL}
                       FROM pg_catalog.pg_attribute a
                       LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
                      WHERE a.attrelid = $1 AND a.attname = ANY($2)
                        AND a.attnum > 0 AND NOT a.attisdropped"
                ),
                &[&oid, &names],
            ),
        )
        .await
        .map_err(|e| dropped(schema, e))?;
    let mut types = c.types.lock().await;
    c.conn
        .command(
            "load types",
            load_types(&client, rows.iter().map(|r| r.get::<_, u32>(1)), &mut types),
        )
        .await?;
    let key = table_key(schema.table());
    let mut out = Vec::with_capacity(names.len());
    for col in schema.columns() {
        let r = rows
            .iter()
            .find(|r| r.get::<_, &str>(0) == col.name)
            .ok_or_else(|| changed(schema, format!("column {} is gone", col.name)))?;
        if schema.is_identifier(&col.name) && !r.get::<_, bool>(6) {
            return Err(SourceError::TableHalt {
                table: schema.table().clone(),
                reason: HaltReason::IdentifierNotUsable {
                    column: col.name.clone(),
                    reason: "it is nullable now".into(),
                },
            });
        }
        let over = c
            .pipeline
            .tables
            .column_type_overrides
            .get(&format!("{key}.{}", col.name));
        let p = plan_column(
            &col.name,
            r.get(1),
            r.get(2),
            &types,
            c.pipeline.tables.numeric_overflow_action,
            over,
        )
        .map_err(|h| SourceError::TableHalt {
            table: schema.table().clone(),
            reason: h,
        })?;
        if p.value_type != col.value_type {
            return Err(changed(
                schema,
                format!(
                    "column {} is now {:?}, the schema has {:?}",
                    col.name, p.value_type, col.value_type
                ),
            ));
        }
        let collation = match (r.get::<_, Option<String>>(4), r.get::<_, Option<bool>>(5)) {
            (Some(name), Some(true)) => Some(KeyCollation::Own(name)),
            (Some(_), _) => Some(KeyCollation::C),
            (None, _) => None,
        };
        out.push(ReadColumn {
            name: col.name.clone(),
            decode: p.decode,
            type_text: r.get(3),
            collation,
            identifier: schema.identifier().contains(&col.name),
        });
    }
    Ok(out)
}

/// The table's alias in snapshot queries. Columns are qualified with it.
const ALIAS: &str = "src";

/// The select list: every column, in schema order. A `"char"` is read as its byte, 0–255:
/// PostgreSQL 14 prints a byte above 127 as itself, which isn't UTF-8.
fn select_list(columns: &[ReadColumn]) -> String {
    columns
        .iter()
        .map(|c| {
            if c.decode.reads_char() {
                format!("(({ALIAS}.{})::\"char\"::int4 + 256) % 256", ident(&c.name))
            } else {
                format!("{ALIAS}.{}", ident(&c.name))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A key column in a comparison: `COLLATE "C"` on a column of a nondeterministic collation, its
/// own collation otherwise (Snapshot_Resumability §2).
fn key_expr(c: &ReadColumn) -> String {
    let column = format!("{ALIAS}.{}", ident(&c.name));
    if c.collation == Some(KeyCollation::C) {
        format!("{column} COLLATE \"C\"")
    } else {
        column
    }
}

/// A string literal (`standard_conforming_strings` is pinned on, so only quotes are doubled).
fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A key value of the column's type: its text, cast. A `"char"` (or a domain over one) travels
/// as its byte, 0–255, and the `int4` → `"char"` cast takes a signed byte.
fn cast(c: &ReadColumn, text: &str) -> String {
    if c.decode.reads_char() {
        let byte = format!("(({}::int4 + 128) % 256 - 128)::\"char\"", quote(text));
        if c.type_text == "\"char\"" {
            byte
        } else {
            format!("{byte}::{}", c.type_text)
        }
    } else {
        format!("{}::{}", quote(text), c.type_text)
    }
}

/// A key value in a keyset comparison: [`cast`], collated like the column's [`key_expr`]. A cast
/// literal's collation yields to the column's own.
fn key_value(c: &ReadColumn, text: &str) -> String {
    let cast = cast(c, text);
    if c.collation == Some(KeyCollation::C) {
        format!("{cast} COLLATE \"C\"")
    } else {
        cast
    }
}

/// The key's collations, for the cursor: one name per collatable identifier column, in order.
fn cursor_collation(keys: &[&ReadColumn]) -> Option<String> {
    let names: Vec<&str> = keys
        .iter()
        .filter_map(|c| c.collation.as_ref().map(KeyCollation::name))
        .collect();
    (!names.is_empty()).then(|| names.join(","))
}

fn decode_row(
    schema: &TableSchema,
    columns: &[ReadColumn],
    row: &tokio_postgres::SimpleQueryRow,
) -> Result<Row, SourceError> {
    let mut out = Vec::with_capacity(columns.len());
    for (i, c) in columns.iter().enumerate() {
        let halt = |reason| SourceError::TableHalt {
            table: schema.table().clone(),
            reason,
        };
        let mismatch = |detail: String| {
            halt(HaltReason::SchemaMismatch {
                column: c.name.clone(),
                detail,
            })
        };
        // A `"char"[]` element above 127 on PostgreSQL 14 isn't UTF-8 either: the table halts,
        // as in the stream.
        let text = row
            .try_get(i)
            .map_err(|_| mismatch("the value isn't valid UTF-8".into()))?;
        let decoded = match text {
            None => Ok(ColumnValue::Null),
            Some(code) if c.decode.reads_char() => {
                let byte: u8 = code
                    .parse()
                    .map_err(|_| mismatch(format!("not a \"char\" code: {code:?}")))?;
                value::decode(&c.decode, &format!("\\{byte:03o}"))
            }
            Some(text) => value::decode(&c.decode, text),
        };
        let v = decoded.map_err(|e| match e {
            ValueError::Overflow => halt(HaltReason::NumericOverflow {
                column: c.name.clone(),
            }),
            ValueError::Unrepresentable(detail) => mismatch(detail),
        })?;
        if c.identifier && text.is_some_and(|t| value::clamped(&v, t)) {
            return Err(mismatch(value::CLAMPED_KEY.into()));
        }
        out.push((c.name.clone(), v));
    }
    Ok(Row(out))
}

/// Runs `sql`, one or more statements in one implicit transaction. The server cancels each after
/// `timeouts.command` (`statement_timeout`, local to the transaction).
async fn simple_query(
    c: &PgConnector,
    what: &str,
    schema: &TableSchema,
    sql: &str,
) -> Result<Vec<SimpleQueryMessage>, SourceError> {
    let client = c.conn.client().await?;
    let command = c.conn.timeouts.command;
    let sql = format!(
        "SET LOCAL statement_timeout = {}; {sql}",
        server_millis(command)
    );
    c.conn
        .bounded(what, command + SERVER_FIRST, client.simple_query(&sql))
        .await
        .map_err(|e| dropped(schema, e))
}

fn decode_rows<'a>(
    schema: &TableSchema,
    columns: &[ReadColumn],
    messages: impl IntoIterator<Item = &'a SimpleQueryMessage>,
) -> Result<Vec<Row>, SourceError> {
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(decode_row(schema, columns, r)),
            _ => None,
        })
        .collect()
}

/// Runs `sql` and decodes its rows.
async fn rows(
    c: &PgConnector,
    what: &str,
    schema: &TableSchema,
    columns: &[ReadColumn],
    sql: &str,
) -> Result<Vec<Row>, SourceError> {
    let messages = simple_query(c, what, schema, sql).await?;
    decode_rows(schema, columns, &messages)
}

/// A `pg_snapshot` in its text form, `xmin:xmax:xip,…` (`735:740:736,738`).
fn parse_snapshot(text: &str) -> Result<XidSnapshot, SourceError> {
    let bad = || SourceError::Permanent(format!("unexpected pg_current_snapshot() {text:?}"));
    let mut parts = text.splitn(3, ':');
    let mut xid =
        || -> Result<u64, SourceError> { parts.next().ok_or_else(bad)?.parse().map_err(|_| bad()) };
    let (xmin, xmax) = (xid()?, xid()?);
    let xip = match parts.next().ok_or_else(bad)? {
        "" => vec![],
        list => list
            .split(',')
            .map(|x| x.parse().map_err(|_| bad()))
            .collect::<Result<_, _>>()?,
    };
    Ok(XidSnapshot { xmin, xmax, xip })
}

/// Every transaction id `pg_locks` holds (top-level and subtransactions), with the relations of
/// this database its virtual transaction holds a write lock on. One scan of the lock table.
const IN_FLIGHT: &str = "WITH l AS MATERIALIZED (SELECT * FROM pg_catalog.pg_locks)
    SELECT x.transactionid::text,
           COALESCE(pg_catalog.array_agg(DISTINCT r.relation::int8)
                      FILTER (WHERE r.relation IS NOT NULL), '{}')
      FROM l x
      LEFT JOIN l r
        ON r.virtualtransaction = x.virtualtransaction
       AND r.locktype = 'relation'
       AND r.database = (SELECT oid FROM pg_catalog.pg_database
                          WHERE datname = pg_catalog.current_database())
       AND r.mode IN ('RowExclusiveLock', 'ShareRowExclusiveLock', 'ExclusiveLock',
                      'AccessExclusiveLock')
     WHERE x.locktype = 'transactionid' AND x.mode = 'ExclusiveLock'
     GROUP BY x.transactionid::text";

/// The first identifier column `row` holds NULL in.
fn null_key(schema: &TableSchema, row: &Row) -> String {
    schema
        .identifier()
        .iter()
        .find(|k| {
            row.0
                .iter()
                .any(|(n, v)| n == *k && matches!(v, ColumnValue::Null))
        })
        .cloned()
        .unwrap_or_default()
}

fn halt_dropped(schema: &TableSchema) -> SourceError {
    SourceError::TableHalt {
        table: schema.table().clone(),
        reason: HaltReason::TableDropped,
    }
}

/// The table's columns differ from `schema`'s: transient, since the stream's schema signal hasn't
/// caught up with the catalog yet.
fn changed(schema: &TableSchema, detail: String) -> SourceError {
    SourceError::Transient(format!(
        "{} changed under the snapshot: {detail}; read again once the stream's schema caught up",
        table_key(schema.table())
    ))
}

/// A table, or its schema, gone from the catalog (SQLSTATE 42P01, 3F000) halts that table, not
/// the pipeline (§2.1). A column gone (42703) is `read_plan`'s own finding come late: dropped
/// after the plan's catalog read and before the query took its lock, it is transient the same
/// way.
fn dropped(schema: &TableSchema, e: SourceError) -> SourceError {
    match &e {
        SourceError::Permanent(m) if matches!(sqlstate(m), Some("42P01" | "3F000")) => {
            halt_dropped(schema)
        }
        SourceError::Permanent(m) if sqlstate(m) == Some("42703") => changed(schema, m.clone()),
        _ => e,
    }
}

/// The identifier's read columns, in identifier order.
fn key_columns<'a>(schema: &TableSchema, columns: &'a [ReadColumn]) -> Vec<&'a ReadColumn> {
    schema
        .identifier()
        .iter()
        .map(|k| {
            columns
                .iter()
                .find(|c| &c.name == k)
                .expect("the identifier is a column")
        })
        .collect()
}

/// A key's values as bind parameters.
fn key_texts(key: &Row, schema: &TableSchema) -> Result<Vec<String>, SourceError> {
    schema
        .identifier()
        .iter()
        .map(|k| {
            key.get(k).and_then(text_of).ok_or_else(|| {
                SourceError::Permanent(format!(
                    "{}: the key has no usable value for {k}",
                    schema.table()
                ))
            })
        })
        .collect()
}

impl PgConnector {
    /// One chunk: `SELECT … WHERE (key) > (cursor) ORDER BY key LIMIT n` in a REPEATABLE READ
    /// transaction, after `pg_current_snapshot()`: the chunk's [`ChunkVisibility`] is the snapshot
    /// its rows were read in (DA-SNAP-9).
    pub async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        let columns = read_plan(self, table).await?;
        let keys = key_columns(table, &columns);
        let collation = cursor_collation(&keys);
        let mut sql = format!(
            "SELECT {} FROM {} AS {ALIAS}",
            select_list(&columns),
            qualified(table.table())
        );
        if let Some(cursor) = after {
            if cursor.collation != collation {
                return Err(SourceError::TableHalt {
                    table: table.table().clone(),
                    reason: HaltReason::SchemaMismatch {
                        column: table.identifier().join(","),
                        detail: format!(
                            "the key's collation is {collation:?}, the snapshot's cursor was \
                             ordered by {:?}; re-snapshot the table",
                            cursor.collation
                        ),
                    },
                });
            }
            let texts = key_texts(&cursor.key, table)?;
            let left: Vec<String> = keys.iter().map(|c| key_expr(c)).collect();
            let right: Vec<String> = keys
                .iter()
                .zip(&texts)
                .map(|(c, t)| key_value(c, t))
                .collect();
            sql += &format!(" WHERE ({}) > ({})", left.join(", "), right.join(", "));
        }
        let order: Vec<String> = keys.iter().map(|c| key_expr(c)).collect();
        sql += &format!(" ORDER BY {} LIMIT {limit}", order.join(", "));
        // The first query of a REPEATABLE READ transaction takes its snapshot; the chunk reads in
        // the same one.
        let sql = format!(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
             SELECT pg_catalog.pg_current_snapshot()::text; {sql}"
        );
        let messages = simple_query(self, "read a snapshot chunk", table, &sql).await?;
        // Each SELECT's rows follow its row description: the snapshot's, then the chunk's.
        let mut selects = messages
            .split_inclusive(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
            .filter(|s| {
                s.iter()
                    .any(|m| matches!(m, SimpleQueryMessage::RowDescription(_)))
            });
        let snapshot = selects
            .next()
            .and_then(|s| {
                s.iter().find_map(|m| match m {
                    SimpleQueryMessage::Row(r) => r.get(0),
                    _ => None,
                })
            })
            .ok_or_else(|| SourceError::Permanent("no pg_current_snapshot() row".into()))?;
        let visibility = ChunkVisibility::Xids(parse_snapshot(snapshot)?);
        let rows = decode_rows(table, &columns, selects.next().unwrap_or_default())?;
        let next = match rows.last() {
            None => None,
            Some(last) => Some(KeysetCursor {
                // A key column nullable now halts in `read_plan`; this is the backstop.
                key: identifier_values(table, last).ok_or_else(|| SourceError::TableHalt {
                    table: table.table().clone(),
                    reason: HaltReason::NullIdentifier {
                        column: null_key(table, last),
                    },
                })?,
                collation,
            }),
        };
        Ok(Chunk {
            rows,
            next,
            visibility: Some(visibility),
        })
    }

    /// The transactions in progress now with the tables they write (DA-SNAP-9): every
    /// transaction id `pg_locks` holds, with the relations its virtual transaction holds a write
    /// lock on in this database. A synchronous commit waiting for its standby keeps both until it
    /// is visible (measured on PostgreSQL 18). The ids are 32-bit, like the stream's.
    pub async fn in_flight(&self) -> Result<Option<Vec<InFlightTxn>>, SourceError> {
        let client = self.conn.client().await?;
        let rows = self
            .conn
            .command(
                "read the transactions in progress",
                client.query(IN_FLIGHT, &[]),
            )
            .await?;
        rows.iter()
            .map(|r| {
                let xid: String = r.get(0);
                Ok(InFlightTxn {
                    xid: xid.parse().map_err(|_| {
                        SourceError::Permanent(format!("unexpected transaction id {xid:?}"))
                    })?,
                    tables: r
                        .get::<_, Vec<i64>>(1)
                        .into_iter()
                        .map(|t| t as u64)
                        .collect(),
                })
            })
            .collect::<Result<_, _>>()
            .map(Some)
    }

    /// The current rows of these keys (`toast_strategy: re_query`, Q-M5-6), [`FETCH_BATCH`] keys
    /// per statement. A key with no row is left out; rows come in no particular order.
    pub async fn fetch_rows(
        &self,
        table: &TableSchema,
        keys: &[Row],
    ) -> Result<Vec<Row>, SourceError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let columns = read_plan(self, table).await?;
        let key_cols = key_columns(table, &columns);
        let left: Vec<String> = key_cols
            .iter()
            .map(|c| format!("{ALIAS}.{}", ident(&c.name)))
            .collect();
        let mut out = vec![];
        for batch in keys.chunks(FETCH_BATCH) {
            let tuples = batch
                .iter()
                .map(|key| {
                    let texts = key_texts(key, table)?;
                    let values: Vec<String> = key_cols
                        .iter()
                        .zip(&texts)
                        .map(|(c, t)| cast(c, t))
                        .collect();
                    Ok(format!("({})", values.join(", ")))
                })
                .collect::<Result<Vec<_>, SourceError>>()?;
            let sql = format!(
                "SELECT {} FROM {} AS {ALIAS} WHERE ({}) IN ({})",
                select_list(&columns),
                qualified(table.table()),
                left.join(", "),
                tuples.join(", ")
            );
            out.extend(rows(self, "re-read rows", table, &columns, &sql).await?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnSchema, ColumnValueType, TableRef};

    use super::*;

    /// `s.t`: `id bigint` (the key) and `extra text`.
    fn schema() -> TableSchema {
        TableSchema::new(
            TableRef::new("s", "t"),
            vec![
                ColumnSchema::new("id", ColumnValueType::Int64, "bigint").not_null(),
                ColumnSchema::new("extra", ColumnValueType::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    /// A chunk query that meets a column dropped after its read plan (SQLSTATE 42703) is the plan's
    /// own finding come late: transient, so the pipeline rescans and reads again (owner ruling,
    /// Batch F concern 4). A table gone still halts it; other errors pass as they are.
    #[test]
    fn a_column_gone_under_the_query_is_transient() {
        let gone =
            SourceError::Permanent("column src.extra does not exist (SQLSTATE 42703)".into());
        match dropped(&schema(), gone) {
            SourceError::Transient(m) => assert!(
                m.contains("changed under the snapshot")
                    && m.contains("column src.extra does not exist"),
                "{m}"
            ),
            other => panic!("{other:?}"),
        }
        let table =
            SourceError::Permanent("relation \"s.t\" does not exist (SQLSTATE 42P01)".into());
        assert!(matches!(
            dropped(&schema(), table),
            SourceError::TableHalt {
                reason: HaltReason::TableDropped,
                ..
            }
        ));
        let other = SourceError::Permanent("permission denied (SQLSTATE 42501)".into());
        assert!(matches!(
            dropped(&schema(), other),
            SourceError::Permanent(_)
        ));
    }

    fn rc(name: &str, type_text: &str, collation: Option<KeyCollation>) -> ReadColumn {
        ReadColumn {
            name: name.into(),
            decode: Decode::Text,
            type_text: type_text.into(),
            collation,
            identifier: false,
        }
    }

    fn char_column(name: &str, type_text: &str) -> ReadColumn {
        ReadColumn {
            decode: Decode::Char,
            ..rc(name, type_text, None)
        }
    }

    #[test]
    fn snapshots_parse_from_their_text() {
        assert_eq!(
            parse_snapshot("735:740:736,738").unwrap(),
            XidSnapshot {
                xmin: 735,
                xmax: 740,
                xip: vec![736, 738]
            }
        );
        assert_eq!(
            parse_snapshot("4294967306:4294967306:").unwrap(),
            XidSnapshot {
                xmin: 4_294_967_306,
                xmax: 4_294_967_306,
                xip: vec![]
            }
        );
        for bad in ["", "1:2", "a:2:", "1:2:x", "1:2:3,"] {
            assert!(parse_snapshot(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn keys_compare_in_one_deterministic_collation() {
        let own = || Some(KeyCollation::Own("en_US.utf8".into()));
        assert_eq!(
            key_expr(&rc("code", "text", Some(KeyCollation::C))),
            "src.\"code\" COLLATE \"C\""
        );
        assert_eq!(key_expr(&rc("code", "text", own())), "src.\"code\"");
        assert_eq!(key_expr(&rc("id", "bigint", None)), "src.\"id\"");
        assert_eq!(
            key_value(
                &rc("code", "character varying(10)", Some(KeyCollation::C)),
                "it's"
            ),
            "'it''s'::character varying(10) COLLATE \"C\""
        );
        assert_eq!(
            key_value(&rc("code", "character varying(10)", own()), "it's"),
            "'it''s'::character varying(10)",
            "the column's own collation wins over the literal's"
        );
        assert_eq!(
            key_value(&rc("id", "numeric(10,2)", None), "1.50"),
            "'1.50'::numeric(10,2)"
        );
        assert_eq!(
            key_value(&char_column("c", "\"char\""), "255"),
            "(('255'::int4 + 128) % 256 - 128)::\"char\""
        );
        assert_eq!(
            key_value(&char_column("c", "it.flag"), "97"),
            "(('97'::int4 + 128) % 256 - 128)::\"char\"::it.flag",
            "a domain over \"char\""
        );
        assert_eq!(
            select_list(&[rc("a b", "text", own()), char_column("c", "\"char\"")]),
            "src.\"a b\", ((src.\"c\")::\"char\"::int4 + 256) % 256"
        );
        let a = rc("a", "text", own());
        let b = rc("b", "text", Some(KeyCollation::C));
        let id = rc("id", "int", None);
        assert_eq!(
            cursor_collation(&[&a, &id, &b]).as_deref(),
            Some("en_US.utf8,C")
        );
        assert_eq!(cursor_collation(&[&id]), None);
        assert_eq!(
            quote("a'b\\c"),
            "'a''b\\c'",
            "backslashes stay: standard_conforming_strings"
        );
    }
}
