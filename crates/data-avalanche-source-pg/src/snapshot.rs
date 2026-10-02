//! Snapshot reads (Snapshot_Resumability §1–§2, §6): keyset chunks in identifier order, text
//! identifier columns ordered with `COLLATE "C"`, and the keyed re-read `toast_strategy: re_query`
//! uses. Rows are read over the simple-query protocol, whose values are the types' output-function
//! text, the same text pgoutput sends; they are decoded like stream values, so a snapshot row and
//! a streamed row of the same data are equal. (A `::text` cast isn't the output function:
//! `true::text` is `true`, pgoutput sends `t`.)

use data_avalanche_core::{
    Chunk, ColumnValue, HaltReason, KeysetCursor, Row, SourceError, TableSchema, identifier_values,
};
use tokio_postgres::SimpleQueryMessage;

use crate::catalog::{load_types, table_key};
use crate::connector::PgConnector;
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
    collatable: bool,
}

/// The read plan of a table, checked against `schema`: every column still exists and decodes to
/// the type the schema says. A difference is transient: the stream's schema signal hasn't caught
/// up with the catalog yet.
async fn read_plan(c: &PgConnector, schema: &TableSchema) -> Result<Vec<ReadColumn>, SourceError> {
    let client = c.conn.client().await?;
    let names: Vec<&str> = schema.columns().iter().map(|c| c.name.as_str()).collect();
    let rows = c
        .conn
        .command(
            "read the table's columns",
            client.query(
                "SELECT a.attname, a.atttypid, a.atttypmod,
                        pg_catalog.format_type(a.atttypid, a.atttypmod), t.typcollation <> 0
                   FROM pg_catalog.pg_attribute a
                   JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                  WHERE a.attrelid = $1::text::regclass AND a.attname = ANY($2)
                    AND a.attnum > 0 AND NOT a.attisdropped",
                &[&qualified(schema.table()), &names],
            ),
        )
        .await?;
    let mut types = c.types.lock().await;
    c.conn
        .command(
            "load types",
            load_types(&client, rows.iter().map(|r| r.get::<_, u32>(1)), &mut types),
        )
        .await?;
    let key = table_key(schema.table());
    let changed = |detail: String| {
        SourceError::Transient(format!(
            "{key} changed under the snapshot: {detail}; read again once the stream's schema caught up"
        ))
    };
    let mut out = Vec::with_capacity(names.len());
    for col in schema.columns() {
        let r = rows
            .iter()
            .find(|r| r.get::<_, &str>(0) == col.name)
            .ok_or_else(|| changed(format!("column {} is gone", col.name)))?;
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
            return Err(changed(format!(
                "column {} is now {:?}, the schema has {:?}",
                col.name, p.value_type, col.value_type
            )));
        }
        out.push(ReadColumn {
            name: col.name.clone(),
            decode: p.decode,
            type_text: r.get(3),
            collatable: r.get(4),
        });
    }
    Ok(out)
}

/// The table's alias in snapshot queries. Columns are qualified with it.
const ALIAS: &str = "src";

/// The select list: every column, in schema order.
fn select_list(columns: &[ReadColumn]) -> String {
    columns
        .iter()
        .map(|c| format!("{ALIAS}.{}", ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A key column in a comparison: `COLLATE "C"` on a collatable column (Snapshot_Resumability §2).
fn key_expr(c: &ReadColumn) -> String {
    if c.collatable {
        format!("{ALIAS}.{} COLLATE \"C\"", ident(&c.name))
    } else {
        format!("{ALIAS}.{}", ident(&c.name))
    }
}

/// A string literal (`standard_conforming_strings` is pinned on, so only quotes are doubled).
fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A key value of the column's type: its text, cast. A `"char"` travels as its byte, 0–255, and
/// the `int4` → `"char"` cast takes a signed byte.
fn cast(c: &ReadColumn, text: &str) -> String {
    if c.type_text == "\"char\"" {
        format!("(({}::int4 + 128) % 256 - 128)::\"char\"", quote(text))
    } else {
        format!("{}::{}", quote(text), c.type_text)
    }
}

/// A key value in a keyset comparison: [`cast`], collated like the column's [`key_expr`].
fn key_value(c: &ReadColumn, text: &str) -> String {
    let cast = cast(c, text);
    if c.collatable {
        format!("{cast} COLLATE \"C\"")
    } else {
        cast
    }
}

fn decode_row(
    schema: &TableSchema,
    columns: &[ReadColumn],
    row: &tokio_postgres::SimpleQueryRow,
) -> Result<Row, SourceError> {
    let mut out = Vec::with_capacity(columns.len());
    for (i, c) in columns.iter().enumerate() {
        let v = match row.get(i) {
            None => ColumnValue::Null,
            Some(text) => value::decode(&c.decode, text).map_err(|e| SourceError::TableHalt {
                table: schema.table().clone(),
                reason: match e {
                    ValueError::Overflow => HaltReason::NumericOverflow {
                        column: c.name.clone(),
                    },
                    ValueError::Unrepresentable(detail) => HaltReason::SchemaMismatch {
                        column: c.name.clone(),
                        detail,
                    },
                },
            })?,
        };
        out.push((c.name.clone(), v));
    }
    Ok(Row(out))
}

/// Runs `sql` and decodes its rows.
async fn rows(
    c: &PgConnector,
    what: &str,
    schema: &TableSchema,
    columns: &[ReadColumn],
    sql: &str,
) -> Result<Vec<Row>, SourceError> {
    let client = c.conn.client().await?;
    let messages = c.conn.command(what, client.simple_query(sql)).await?;
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(decode_row(schema, columns, r)),
            _ => None,
        })
        .collect()
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
    /// One chunk: `SELECT … WHERE (key) > (cursor) ORDER BY key LIMIT n`, a single statement, so
    /// one READ COMMITTED snapshot (Snapshot_Resumability §1).
    pub async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        let columns = read_plan(self, table).await?;
        let keys = key_columns(table, &columns);
        let mut sql = format!(
            "SELECT {} FROM {} AS {ALIAS}",
            select_list(&columns),
            qualified(table.table())
        );
        if let Some(cursor) = after {
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
        let rows = rows(self, "read a snapshot chunk", table, &columns, &sql).await?;
        let collation = keys.iter().any(|c| c.collatable).then(|| "C".to_owned());
        let next = match rows.last() {
            None => None,
            Some(last) => Some(KeysetCursor {
                key: identifier_values(table, last).ok_or_else(|| {
                    SourceError::Permanent(format!("{}: a row with a NULL key", table.table()))
                })?,
                collation,
            }),
        };
        Ok(Chunk { rows, next })
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
    use super::*;

    fn rc(name: &str, type_text: &str, collatable: bool) -> ReadColumn {
        ReadColumn {
            name: name.into(),
            decode: Decode::Text,
            type_text: type_text.into(),
            collatable,
        }
    }

    #[test]
    fn keys_compare_in_the_c_collation() {
        assert_eq!(
            key_expr(&rc("code", "text", true)),
            "src.\"code\" COLLATE \"C\""
        );
        assert_eq!(key_expr(&rc("id", "bigint", false)), "src.\"id\"");
        assert_eq!(
            key_value(&rc("code", "character varying(10)", true), "it's"),
            "'it''s'::character varying(10) COLLATE \"C\""
        );
        assert_eq!(
            key_value(&rc("id", "numeric(10,2)", false), "1.50"),
            "'1.50'::numeric(10,2)"
        );
        assert_eq!(
            key_value(&rc("c", "\"char\"", false), "255"),
            "(('255'::int4 + 128) % 256 - 128)::\"char\""
        );
        assert_eq!(
            select_list(&[rc("a b", "text", true), rc("c", "int", false)]),
            "src.\"a b\", src.\"c\""
        );
        assert_eq!(
            quote("a'b\\c"),
            "'a''b\\c'",
            "backslashes stay: standard_conforming_strings"
        );
    }
}
