//! Snapshot chunks (Snapshot_Resumability §1.1, §2; DA-SNAP-10): keyset chunks in identifier
//! order, each read in a short READ COMMITTED transaction with `LOCK IN SHARE MODE`. The shared
//! row locks wait for a writer of the chunk's rows still committing (a semi-sync commit waiting at
//! AFTER_SYNC is in the binlog before a plain read sees it, S-MY11), then read its committed value.
//! A lock wait past `innodb_lock_wait_timeout` fails with error 1205, which is transient: the chunk
//! is read again after a backoff.
//!
//! The key is ordered and compared the way its index orders it: a text column in its own
//! collation (a primary key is unique under it, so the order is total), an `ENUM` by member index,
//! a `SET` by bitmap, a `BIT` as a number. The cursor's values travel as parameters in that form.
//! The keyset predicate is spelled out (`a > ? OR (a = ? AND b > ?)`): a row-constructor
//! comparison `(a, b) > (?, ?)` walks the index from its start on MySQL 8.4 and MariaDB 10.11 and
//! 11.8, where the expanded form is a range scan (measured with EXPLAIN, 2026-10-14).
//!
//! Rows come from a prepared statement and decode like binlog values (one plan, two inputs), so a
//! snapshot row and a streamed row of the same data are equal. A chunk carries no
//! [`data_avalanche_core::ChunkVisibility`]: the pipeline orders it against the stream by the
//! window rule (Data-Flow, snapshot step c).

use chrono::{Datelike, Timelike};
use data_avalanche_core::{
    Chunk, ColumnValue, HaltReason, KeysetCursor, Row, SourceError, TableRef, TableSchema,
    identifier_values,
};
use mysql_async::prelude::Queryable;
use mysql_async::{IsolationLevel, TxOpts, Value};

use crate::catalog::table_key;
use crate::connector::MySqlConnector;
use crate::types::{CatalogColumn, plan_column};
use crate::value::{self, Decode, Input};

/// How to read one column of a table.
#[derive(Debug, Clone)]
pub(crate) struct ReadColumn {
    catalog: CatalogColumn,
    decode: Decode,
    /// One of the table's identifier columns.
    identifier: bool,
}

/// A name as a MySQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `db`.`table`.
pub(crate) fn qualified(t: &TableRef) -> String {
    format!("{}.{}", ident(&t.schema), ident(&t.name))
}

fn halt(schema: &TableSchema, reason: HaltReason) -> SourceError {
    SourceError::TableHalt {
        table: schema.table().clone(),
        reason,
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

/// The read plan of a table, checked against `schema`: the table is still there, and every column
/// still exists and decodes to the type the schema says. A table gone halts it; a key column that
/// became nullable halts it (a NULL key sorts before every cursor, so its rows would be skipped).
/// Another difference is transient: the stream's schema signal hasn't caught up yet.
async fn read_plan(
    c: &MySqlConnector,
    schema: &TableSchema,
) -> Result<Vec<ReadColumn>, SourceError> {
    let name = schema.table().name.clone();
    let Some(d) = c.describe(std::slice::from_ref(&name)).await?.remove(&name) else {
        return Err(halt(schema, HaltReason::TableDropped));
    };
    let tables = &c.pipeline.tables;
    let key = table_key(schema.table());
    let mut out = Vec::with_capacity(schema.columns().len());
    for col in schema.columns() {
        let catalog = d
            .columns
            .iter()
            .find(|x| x.name == col.name)
            .ok_or_else(|| changed(schema, format!("column {} is gone", col.name)))?;
        let identifier = schema.is_identifier(&col.name);
        if identifier && catalog.nullable {
            return Err(halt(
                schema,
                HaltReason::IdentifierNotUsable {
                    column: col.name.clone(),
                    reason: "it is nullable now".into(),
                },
            ));
        }
        let over = tables
            .column_type_overrides
            .get(&format!("{key}.{}", col.name));
        let p = plan_column(
            catalog,
            c.tinyint1_as_bool(),
            tables.numeric_overflow_action,
            over,
            identifier,
        )
        .map_err(|h| halt(schema, h))?;
        if p.value_type != col.value_type {
            return Err(changed(
                schema,
                format!(
                    "column {} is now {:?}, the schema has {:?}",
                    col.name, p.value_type, col.value_type
                ),
            ));
        }
        out.push(ReadColumn {
            catalog: catalog.clone(),
            decode: p.decode,
            identifier,
        });
    }
    Ok(out)
}

/// The decode an override wraps: the column's own type, which orders the key.
fn base(d: &Decode) -> &Decode {
    match d {
        Decode::String(inner) | Decode::Convert { from: inner, .. } => base(inner),
        d => d,
    }
}

/// An integer key value: a decoded integer, a decimal an override widened it to, or its text.
fn integer_of(v: &ColumnValue) -> Option<i128> {
    match v {
        ColumnValue::Bool(b) => Some(i128::from(*b)),
        ColumnValue::Int16(n) => Some(i128::from(*n)),
        ColumnValue::Int32(n) => Some(i128::from(*n)),
        ColumnValue::Int64(n) => Some(i128::from(*n)),
        ColumnValue::UInt64(n) => Some(i128::from(*n)),
        ColumnValue::Numeric(d) => {
            let text = d.to_string();
            let (whole, fraction) = text.split_once('.').unwrap_or((&text, ""));
            fraction
                .bytes()
                .all(|b| b == b'0')
                .then(|| whole.parse().ok())
                .flatten()
        }
        ColumnValue::Text(t) => t.parse().ok(),
        _ => None,
    }
}

fn integer_param(n: i128) -> Value {
    match u64::try_from(n) {
        Ok(u) => Value::UInt(u),
        Err(_) => Value::Int(n as i64),
    }
}

/// A date and time as a parameter. `micros` counts from 1970-01-01 00:00.
fn clock_param(micros: i64) -> Option<Value> {
    let t = chrono::DateTime::from_timestamp_micros(micros)?.naive_utc();
    Some(Value::Date(
        u16::try_from(t.year()).ok()?,
        t.month() as u8,
        t.day() as u8,
        t.hour() as u8,
        t.minute() as u8,
        t.second() as u8,
        t.nanosecond() / 1000,
    ))
}

fn time_param(micros: i64) -> Value {
    let neg = micros < 0;
    let m = micros.unsigned_abs();
    let (secs, us) = (m / 1_000_000, (m % 1_000_000) as u32);
    let hours = secs / 3600;
    Value::Time(
        neg,
        (hours / 24) as u32,
        (hours % 24) as u8,
        (secs / 60 % 60) as u8,
        (secs % 60) as u8,
        us,
    )
}

fn text_param(t: &str) -> Value {
    Value::Bytes(t.as_bytes().to_vec())
}

/// A key column's comparison operand: the SQL that reads the parameter in the column's type and
/// order, and the parameter, from the cursor's value `v`.
fn key_operand(c: &ReadColumn, v: &ColumnValue) -> Result<(String, Value), String> {
    let cat = &c.catalog;
    let unusable = || format!("{}: no key parameter from {v:?}", cat.name);
    let plain = |p: Value| Ok(("?".to_owned(), p));
    match (base(&c.decode), v) {
        (Decode::Bool | Decode::Int { .. } | Decode::Year, v) => {
            plain(integer_param(integer_of(v).ok_or_else(unusable)?))
        }
        // A float or SET key is an identifier only under a `string` override (core refuses
        // float equality and arrays); its text is Rust's shortest form, which parses back exactly.
        (Decode::Float32, ColumnValue::Text(t)) => {
            plain(Value::Float(t.parse().map_err(|_| unusable())?))
        }
        (Decode::Float64, ColumnValue::Text(t)) => {
            plain(Value::Double(t.parse().map_err(|_| unusable())?))
        }
        // The text parameter, cast to the column's decimal: MySQL 8.4 and MariaDB 11.8 compare
        // it exactly uncast too (measured), but a string against a decimal compares as a double
        // by the documented rules.
        (Decode::Decimal | Decode::DecimalText, ColumnValue::Numeric(_) | ColumnValue::Text(_)) => {
            let text = match v {
                ColumnValue::Numeric(d) => d.to_string(),
                ColumnValue::Text(t) => t.clone(),
                _ => unreachable!("matched above"),
            };
            let (p, s) = (cat.precision.unwrap_or(65), cat.scale.unwrap_or(30));
            Ok((format!("CAST(? AS DECIMAL({p},{s}))"), text_param(&text)))
        }
        (Decode::Text { .. }, ColumnValue::Text(t)) => {
            let sql = match (&cat.charset, &cat.collation) {
                (Some(cs), Some(co)) => format!("CONVERT(? USING {cs}) COLLATE {co}"),
                _ => "?".to_owned(),
            };
            Ok((sql, text_param(t)))
        }
        (Decode::Bytes { .. }, ColumnValue::Bytes(b)) if cat.data_type == "bit" => plain(
            Value::UInt(b.iter().fold(0u64, |n, byte| n << 8 | u64::from(*byte))),
        ),
        // Bytes travel as hex: a parameter is a string in the connection's character set.
        (Decode::Bytes { .. }, ColumnValue::Bytes(b)) => {
            let hex: String = b.iter().map(|x| format!("{x:02X}")).collect();
            Ok(("UNHEX(?)".to_owned(), text_param(&hex)))
        }
        (Decode::Date, ColumnValue::Date(days)) => {
            plain(clock_param(i64::from(*days) * 86_400_000_000).ok_or_else(unusable)?)
        }
        (
            Decode::Date | Decode::DateTime { .. } | Decode::Timestamp { .. },
            ColumnValue::Timestamp { micros, .. },
        ) => plain(clock_param(*micros).ok_or_else(unusable)?),
        (Decode::Time { .. }, ColumnValue::Time(micros)) => plain(time_param(*micros)),
        (
            Decode::Date
            | Decode::DateTime { .. }
            | Decode::Timestamp { .. }
            | Decode::Time { .. }
            | Decode::Uuid
            | Decode::Inet4
            | Decode::Inet6,
            ColumnValue::Text(t),
        ) => plain(text_param(t)),
        (Decode::Uuid, ColumnValue::Uuid(u)) => plain(text_param(&u.hyphenated().to_string())),
        // ENUM and SET order by member index and bitmap, and compare that way with a number.
        (Decode::Enum { members }, ColumnValue::Text(label)) => {
            let index = members
                .iter()
                .position(|m| m == label)
                .map_or(0, |i| i as u64 + 1);
            if index == 0 && !label.is_empty() {
                return Err(unusable());
            }
            plain(Value::UInt(index))
        }
        (Decode::Set { members }, ColumnValue::Text(t)) => {
            let mut bits = 0u64;
            for l in t.split(',').filter(|l| !l.is_empty()) {
                let i = members.iter().position(|m| m == l).ok_or_else(unusable)?;
                bits |= 1 << i;
            }
            plain(Value::UInt(bits))
        }
        _ => Err(unusable()),
    }
}

/// The keyset predicate after the cursor, spelled out: `k1 > ?1 OR (k1 = ?1 AND k2 > ?2) …`, with
/// its parameters in order.
fn keyset_predicate(keys: &[(String, String, Value)]) -> (String, Vec<Value>) {
    let mut terms = vec![];
    let mut params = vec![];
    for i in 0..keys.len() {
        let mut parts = vec![];
        for (column, operand, param) in &keys[..i] {
            parts.push(format!("{column} = {operand}"));
            params.push(param.clone());
        }
        let (column, operand, param) = &keys[i];
        parts.push(format!("{column} > {operand}"));
        params.push(param.clone());
        terms.push(format!("({})", parts.join(" AND ")));
    }
    (terms.join(" OR "), params)
}

/// The key's collations, for the cursor: one name per text identifier column, in order.
fn cursor_collation(keys: &[&ReadColumn]) -> Option<String> {
    let names: Vec<&str> = keys
        .iter()
        .filter(|c| matches!(base(&c.decode), Decode::Text { .. }))
        .filter_map(|c| c.catalog.collation.as_deref())
        .collect();
    (!names.is_empty()).then(|| names.join(","))
}

/// The identifier's read columns, in identifier order.
fn key_columns<'a>(schema: &TableSchema, columns: &'a [ReadColumn]) -> Vec<&'a ReadColumn> {
    schema
        .identifier()
        .iter()
        .map(|k| {
            columns
                .iter()
                .find(|c| &c.catalog.name == k)
                .expect("the identifier is a column")
        })
        .collect()
}

fn decode_row(
    schema: &TableSchema,
    columns: &[ReadColumn],
    row: mysql_async::Row,
) -> Result<Row, SourceError> {
    let values = row.unwrap();
    let mut out = Vec::with_capacity(columns.len());
    for (c, v) in columns.iter().zip(&values) {
        let column = c.catalog.name.clone();
        let decoded = value::decode(&c.decode, Input::Row(v), c.identifier)
            .map_err(|e| halt(schema, value::halt_reason(column, e)))?;
        out.push((c.catalog.name.clone(), decoded));
    }
    Ok(Row(out))
}

/// The first identifier column `row` holds NULL in.
fn null_key(schema: &TableSchema, row: &Row) -> String {
    schema
        .identifier()
        .iter()
        .find(|k| matches!(row.get(k), None | Some(ColumnValue::Null)))
        .cloned()
        .unwrap_or_default()
}

/// Server errors of a table or column gone between the plan's catalog read and the query.
const NO_TABLE: u16 = 1146;
const NO_COLUMN: u16 = 1054;

impl MySqlConnector {
    /// One chunk: `SELECT … WHERE <after the cursor> ORDER BY key LIMIT n LOCK IN SHARE MODE` in
    /// a READ COMMITTED transaction that commits at once (DA-SNAP-10).
    pub async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        let columns = read_plan(self, table).await?;
        let keys = key_columns(table, &columns);
        let collation = cursor_collation(&keys);
        let list: Vec<String> = columns.iter().map(|c| ident(&c.catalog.name)).collect();
        let mut sql = format!(
            "SELECT {} FROM {}",
            list.join(", "),
            qualified(table.table())
        );
        let mut params = vec![];
        if let Some(cursor) = after {
            if cursor.collation != collation {
                return Err(halt(
                    table,
                    HaltReason::SchemaMismatch {
                        column: table.identifier().join(","),
                        detail: format!(
                            "the key's collation is {collation:?}, the snapshot's cursor was \
                             ordered by {:?}; re-snapshot the table",
                            cursor.collation
                        ),
                    },
                ));
            }
            let operands = keys
                .iter()
                .map(|c| {
                    let v = cursor.key.get(&c.catalog.name).ok_or_else(|| {
                        format!("{}: the cursor has no {}", table.table(), c.catalog.name)
                    })?;
                    let (operand, param) = key_operand(c, v)?;
                    Ok((ident(&c.catalog.name), operand, param))
                })
                .collect::<Result<Vec<_>, String>>()
                .map_err(SourceError::Permanent)?;
            let (predicate, p) = keyset_predicate(&operands);
            sql += &format!(" WHERE {predicate}");
            params = p;
        }
        let order: Vec<String> = keys.iter().map(|c| ident(&c.catalog.name)).collect();
        sql += &format!(
            " ORDER BY {} LIMIT {limit} LOCK IN SHARE MODE",
            order.join(", ")
        );
        let read = self
            .conn
            .command("read a snapshot chunk", move |c| {
                Box::pin(async move {
                    let mut opts = TxOpts::new();
                    opts.with_isolation_level(IsolationLevel::ReadCommitted);
                    let mut tx = c.start_transaction(opts).await?;
                    match tx.exec::<mysql_async::Row, _, _>(sql, params).await {
                        Ok(rows) => {
                            tx.commit().await?;
                            Ok(Ok(rows))
                        }
                        Err(mysql_async::Error::Server(e))
                            if matches!(e.code, NO_TABLE | NO_COLUMN) =>
                        {
                            tx.rollback().await?;
                            Ok(Err(e))
                        }
                        Err(e) => Err(e),
                    }
                })
            })
            .await?;
        let rows = match read {
            Ok(rows) => rows,
            Err(e) if e.code == NO_TABLE => return Err(halt(table, HaltReason::TableDropped)),
            Err(e) => return Err(changed(table, e.message)),
        };
        let rows = rows
            .into_iter()
            .map(|r| decode_row(table, &columns, r))
            .collect::<Result<Vec<_>, _>>()?;
        let next = match rows.last() {
            None => None,
            Some(last) => Some(KeysetCursor {
                // A key column nullable now halts in `read_plan`; this is the backstop.
                key: identifier_values(table, last).ok_or_else(|| {
                    halt(
                        table,
                        HaltReason::NullIdentifier {
                            column: null_key(table, last),
                        },
                    )
                })?,
                collation,
            }),
        };
        Ok(Chunk {
            rows,
            next,
            visibility: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::Decimal;

    use super::*;
    use crate::value::Charset;

    fn column(data_type: &str, decode: Decode) -> ReadColumn {
        ReadColumn {
            catalog: CatalogColumn {
                name: "k".into(),
                data_type: data_type.into(),
                ..Default::default()
            },
            decode,
            identifier: true,
        }
    }

    fn operand(c: &ReadColumn, v: ColumnValue) -> (String, Value) {
        key_operand(c, &v).unwrap()
    }

    #[test]
    fn integers_and_their_overrides_travel_as_integers() {
        let int = |unsigned| column("bigint", Decode::Int { bits: 64, unsigned });
        assert_eq!(
            operand(&int(true), ColumnValue::UInt64(u64::MAX)),
            ("?".into(), Value::UInt(u64::MAX))
        );
        assert_eq!(
            operand(&int(false), ColumnValue::Int64(-5)),
            ("?".into(), Value::Int(-5))
        );
        let as_string = column(
            "int",
            Decode::String(Box::new(Decode::Int {
                bits: 32,
                unsigned: false,
            })),
        );
        assert_eq!(
            operand(&as_string, ColumnValue::Text("-7".into())).1,
            Value::Int(-7)
        );
        let as_decimal = column(
            "int",
            Decode::Convert {
                from: Box::new(Decode::Int {
                    bits: 32,
                    unsigned: false,
                }),
                to: data_avalanche_core::ColumnValueType::Numeric {
                    precision: 12,
                    scale: 2,
                },
            },
        );
        assert_eq!(
            operand(
                &as_decimal,
                ColumnValue::Numeric(Decimal::parse("42.00").unwrap())
            )
            .1,
            Value::UInt(42)
        );
        assert_eq!(
            operand(&column("tinyint", Decode::Bool), ColumnValue::Bool(true)).1,
            Value::UInt(1)
        );
    }

    #[test]
    fn decimals_cast_and_text_keys_use_their_collation() {
        let mut d = column("decimal", Decode::Decimal);
        (d.catalog.precision, d.catalog.scale) = (Some(30), Some(10));
        assert_eq!(
            operand(
                &d,
                ColumnValue::Numeric(Decimal::parse("12345678901234567890.0000000001").unwrap())
            ),
            (
                "CAST(? AS DECIMAL(30,10))".into(),
                text_param("12345678901234567890.0000000001")
            )
        );
        let mut t = column(
            "varchar",
            Decode::Text {
                charset: Charset::Latin1,
            },
        );
        t.catalog.charset = Some("latin1".into());
        t.catalog.collation = Some("latin1_swedish_ci".into());
        assert_eq!(
            operand(&t, ColumnValue::Text("é".into())),
            (
                "CONVERT(? USING latin1) COLLATE latin1_swedish_ci".into(),
                text_param("é")
            )
        );
        assert_eq!(cursor_collation(&[&t]), Some("latin1_swedish_ci".into()));
        assert_eq!(
            cursor_collation(&[&column(
                "int",
                Decode::Int {
                    bits: 32,
                    unsigned: false
                }
            )]),
            None
        );
    }

    #[test]
    fn bytes_bits_enums_and_sets_take_their_index_order() {
        let bytes = column("varbinary", Decode::Bytes { pad: None });
        assert_eq!(
            operand(&bytes, ColumnValue::Bytes(vec![0, 0xff, b'a'])),
            ("UNHEX(?)".into(), text_param("00FF61"))
        );
        let bit = column("bit", Decode::Bytes { pad: None });
        assert_eq!(
            operand(&bit, ColumnValue::Bytes(vec![0x02, 0x01])).1,
            Value::UInt(513)
        );
        let members = vec!["z".to_owned(), "a".to_owned(), "m".to_owned()];
        let e = column(
            "enum",
            Decode::Enum {
                members: members.clone(),
            },
        );
        assert_eq!(operand(&e, ColumnValue::Text("a".into())).1, Value::UInt(2));
        assert_eq!(operand(&e, ColumnValue::Text("".into())).1, Value::UInt(0));
        assert!(key_operand(&e, &ColumnValue::Text("q".into())).is_err());
        let s_text = column("set", Decode::String(Box::new(Decode::Set { members })));
        assert_eq!(
            operand(&s_text, ColumnValue::Text("z,m".into())).1,
            Value::UInt(0b101)
        );
    }

    #[test]
    fn clocks_travel_as_dates_and_times() {
        let date = column("date", Decode::Date);
        assert_eq!(
            operand(&date, ColumnValue::Date(-1)).1,
            Value::Date(1969, 12, 31, 0, 0, 0, 0)
        );
        let dt = column("datetime", Decode::DateTime { fsp: 6 });
        assert_eq!(
            operand(
                &dt,
                ColumnValue::Timestamp {
                    micros: 1_700_000_000_123_456,
                    with_tz: false
                }
            )
            .1,
            Value::Date(2023, 11, 14, 22, 13, 20, 123_456)
        );
        let tm = column("time", Decode::Time { fsp: 6 });
        assert_eq!(
            operand(
                &tm,
                ColumnValue::Time(-(838 * 3600 + 59 * 60 + 59) * 1_000_000 - 1)
            )
            .1,
            Value::Time(true, 34, 22, 59, 59, 1)
        );
    }

    #[test]
    fn the_keyset_predicate_is_spelled_out() {
        let keys = vec![
            ("`a`".to_owned(), "?".to_owned(), Value::Int(1)),
            ("`b`".to_owned(), "UNHEX(?)".to_owned(), text_param("00")),
        ];
        let (sql, params) = keyset_predicate(&keys);
        assert_eq!(sql, "(`a` > ?) OR (`a` = ? AND `b` > UNHEX(?))");
        assert_eq!(params, [Value::Int(1), Value::Int(1), text_param("00")]);
        assert_eq!(ident("we`ird"), "`we``ird`");
    }
}
