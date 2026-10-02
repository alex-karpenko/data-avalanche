//! Channels C and D (`Schema_Change…` §3, §6): the change a schema signal implicates, proven
//! with `pg_attribute.attnum`, and the batched idle rescan.

use data_avalanche_core::{
    ColumnSchema, HaltReason, RescanReport, SchemaChange, SchemaDelta, SchemaHint, SourceError,
    TableRef, TableSchema,
};

use crate::catalog::{self, table_key};
use crate::connector::PgConnector;

/// The first length or precision in a type's text: `character varying(100)` → 100,
/// `timestamp(3) with time zone` → 3. A time type without one has the default precision, 6;
/// another type without one is unbounded (`None`).
fn bound(type_text: &str) -> Option<i64> {
    let explicit = || {
        let open = type_text.find('(')?;
        let close = type_text[open..].find([')', ','])? + open;
        type_text[open + 1..close].trim().parse().ok()
    };
    explicit().or_else(|| {
        let word = type_text.split(['(', ' ', '[']).next().unwrap_or_default();
        matches!(word, "timestamp" | "time" | "interval").then_some(6)
    })
}

/// Whether `to` holds fewer values than `from` when both map to the same logical type: a shorter
/// length, a lower time precision, a bound where there was none (`text` → `varchar(10)`). Only
/// the source can tell (`SchemaChange::ChangeType`).
pub fn narrowed(from: &ColumnSchema, to: &ColumnSchema) -> bool {
    if from.logical != to.logical {
        return false;
    }
    match (bound(&from.source_type), bound(&to.source_type)) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(f), Some(t)) => t < f,
    }
}

/// The schema the stream's columns imply (§0: scoped to the change, never the catalog's "now"):
/// each hint column takes its type from the stream and its identity (`attnum`) and nullability
/// from the catalog. It is found by name; else by the `attnum` it had in `cached` (renamed since
/// the signal), or with that `attnum` when the catalog no longer has it (dropped since the signal:
/// an `attnum` is never reused). The catalog may be ahead of the stream, so a placement that
/// doesn't fit the stream's columns is a guess, and the table halts (Q-M5-17):
/// - two columns on one `attnum`, or `attnum`s out of the stream's order (pgoutput sends them in
///   `attnum` order): renames or a swap since the signal;
/// - a column of `cached` the catalog still has, with no stream column on it: it existed at the
///   signal, so the stream carries it under some name (renamed away, and its name taken);
/// - a column placed as dropped since the signal while another column of `cached` is gone too and
///   placed nowhere: either could be the one the name meant;
/// - a column of the catalog with no stream column on it, below the highest placed column the
///   catalog still has: created before that one, it existed at the signal (a name re-added since);
/// - a column placed as dropped since the signal while a column of the catalog has no stream
///   column on it: a drop and an add since the signal read the same as a column re-added before it
///   and renamed since.
///
/// A column found no way halts too.
pub fn scope(
    cached: &TableSchema,
    hint: &[ColumnSchema],
    now: &TableSchema,
) -> Result<TableSchema, HaltReason> {
    let in_now = |o: Option<i32>| {
        now.columns()
            .iter()
            .find(|c| o.is_some() && c.source_ordinal == o)
    };
    let guess = |column: &str, why: &str| HaltReason::SchemaMismatch {
        column: column.into(),
        detail: format!("{why}; re-snapshot"),
    };
    let mut columns: Vec<ColumnSchema> = Vec::with_capacity(hint.len());
    let mut dropped_since = false;
    for h in hint {
        let (ordinal, nullable) = if let Some(c) = now.column(&h.name) {
            (c.source_ordinal, c.nullable)
        } else if let Some(was) = cached.column(&h.name) {
            match in_now(was.source_ordinal) {
                Some(c) => (c.source_ordinal, c.nullable),
                None => {
                    dropped_since = true;
                    (was.source_ordinal, was.nullable)
                }
            }
        } else {
            return Err(guess(
                &h.name,
                "the stream carries a column the catalog doesn't have by that name or position",
            ));
        };
        let ordinal = ordinal.ok_or_else(|| guess(&h.name, "a column without an attnum"))?;
        if let Some(last) = columns.last()
            && last.source_ordinal >= Some(ordinal)
        {
            return Err(guess(
                &h.name,
                "the catalog, ahead of the stream, doesn't place its columns in their order",
            ));
        }
        columns.push(ColumnSchema {
            nullable,
            source_ordinal: Some(ordinal),
            ..h.clone()
        });
    }
    let placed = |o: Option<i32>| columns.iter().any(|c| c.source_ordinal == o);
    for was in cached.columns() {
        let gone = in_now(was.source_ordinal).is_none();
        if !gone && !placed(was.source_ordinal) {
            return Err(guess(
                &was.name,
                "the catalog, ahead of the stream, has a column no stream column is placed on",
            ));
        }
        if dropped_since && gone && !placed(was.source_ordinal) {
            return Err(guess(
                &was.name,
                "two columns are gone from the catalog, and the stream's names don't say which",
            ));
        }
    }
    let top = columns
        .iter()
        .filter(|c| in_now(c.source_ordinal).is_some())
        .filter_map(|c| c.source_ordinal)
        .max();
    for c in now.columns() {
        if placed(c.source_ordinal) {
            continue;
        }
        if c.source_ordinal < top {
            return Err(guess(
                &c.name,
                "the catalog, ahead of the stream, has a column older than one the stream carries, \
                 and no stream column is placed on it",
            ));
        }
        if dropped_since {
            return Err(guess(
                &c.name,
                "a column dropped and one added since the signal read the same as a column \
                 re-added before it",
            ));
        }
    }
    let identifier = now
        .identifier()
        .iter()
        .map(|k| {
            let ordinal = now.column(k).and_then(|c| c.source_ordinal);
            columns
                .iter()
                .find(|c| c.source_ordinal == ordinal)
                .map(|c| c.name.clone())
                .ok_or_else(|| HaltReason::SchemaMismatch {
                    column: k.clone(),
                    detail: "the identifier column isn't in the stream's columns".into(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let new = TableSchema::new(cached.table().clone(), columns, identifier)?;
    Ok(match now.source_id() {
        Some(id) => new.with_source_id(id),
        None => new,
    })
}

/// `cached` and `now` describe the table alike. A cached schema without the source's identity
/// (persisted without it, or another source's) compares without it.
fn unchanged(cached: &TableSchema, now: &TableSchema) -> bool {
    match (cached.source_id(), now.source_id()) {
        (None, Some(id)) => cached.clone().with_source_id(id) == *now,
        _ => cached == now,
    }
}

/// Whether the catalog's table under `cached`'s name is another table: dropped (or renamed away)
/// and its name taken (`Schema_Change…` §2.1, §7.2: a re-created table doesn't resume).
fn replaced(cached: &TableSchema, now_oid: u32) -> bool {
    cached
        .source_id()
        .is_some_and(|id| id != u64::from(now_oid))
}

/// `cached` → `new` as changes, columns matched by `attnum`: the same `attnum` under another name
/// is a proven rename (§6).
pub fn diff(cached: &TableSchema, new: &TableSchema) -> Vec<SchemaChange> {
    let find = |o: Option<i32>| {
        new.columns()
            .iter()
            .find(|c| o.is_some() && c.source_ordinal == o)
    };
    let mut changes = vec![];
    for c in cached.columns() {
        let Some(n) = find(c.source_ordinal) else {
            changes.push(SchemaChange::DropColumn(c.name.clone()));
            continue;
        };
        if n.name != c.name {
            changes.push(SchemaChange::RenameColumn {
                from: c.name.clone(),
                to: n.name.clone(),
                proven: true,
            });
        }
        if n.value_type != c.value_type || n.source_type != c.source_type || n.logical != c.logical
        {
            changes.push(SchemaChange::ChangeType {
                column: n.name.clone(),
                from: c.clone(),
                to: n.clone(),
                narrowed: narrowed(c, n),
            });
        }
        match (c.nullable, n.nullable) {
            (true, false) => changes.push(SchemaChange::SetNotNull(n.name.clone())),
            (false, true) => changes.push(SchemaChange::DropNotNull(n.name.clone())),
            _ => {}
        }
    }
    for n in new.columns() {
        let known = cached
            .columns()
            .iter()
            .any(|c| c.source_ordinal.is_some() && c.source_ordinal == n.source_ordinal);
        if !known {
            changes.push(SchemaChange::AddColumn(n.clone()));
        }
    }
    let ordinals = |s: &TableSchema| -> Vec<Option<i32>> {
        s.identifier()
            .iter()
            .map(|k| s.column(k).and_then(|c| c.source_ordinal))
            .collect()
    };
    if ordinals(cached) != ordinals(new) {
        changes.push(SchemaChange::IdentifierChanged);
    }
    changes
}

impl PgConnector {
    /// The table's schema as the catalog has it now; `None` when `cached`'s table was dropped,
    /// another taking its name or not.
    async fn current(
        &self,
        table: &TableRef,
        cached: &TableSchema,
    ) -> Result<Option<TableSchema>, SourceError> {
        let client = self.conn.client().await?;
        let oids = self
            .conn
            .command(
                "find the table",
                catalog::oids_by_name(&client, std::slice::from_ref(table)),
            )
            .await?;
        let Some(oid) = oids.get(table).filter(|oid| !replaced(cached, **oid)) else {
            return Ok(None);
        };
        let mut schemas = self.schemas(&[*oid]).await?;
        match schemas.remove(oid) {
            None => Ok(None),
            Some((_, Ok((s, _)))) => Ok(Some(s)),
            Some((_, Err(reason))) => Err(SourceError::TableHalt {
                table: table.clone(),
                reason,
            }),
        }
    }

    /// Channel C: the change `hint` implicates, against `cached` (`Schema_Change…` §0, §3, §6).
    /// A catalog that can't place the stream's columns halts the table (fail-safe).
    pub async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        let table = cached.table().clone();
        let SchemaHint::Columns(columns) = hint else {
            return Err(SourceError::Permanent(format!(
                "{}: a DDL hint on a PostgreSQL source",
                table_key(&table)
            )));
        };
        let Some(now) = self.current(&table, cached).await? else {
            return Ok((
                SchemaDelta {
                    table,
                    changes: vec![SchemaChange::TableDropped],
                },
                None,
            ));
        };
        let new = scope(cached, columns, &now).map_err(|reason| SourceError::TableHalt {
            table: table.clone(),
            reason,
        })?;
        let changes = diff(cached, &new);
        Ok((SchemaDelta { table, changes }, Some(new)))
    }

    /// Channel D: one batched catalog read of the tracked tables. A table gone from `pg_class`,
    /// or whose name another table took, is dropped (§2.1, `TABLE_DROPPED`); a table whose
    /// catalog schema differs is reported with its current schema, for freshness and alerting
    /// only.
    pub async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        let client = self.conn.client().await?;
        let tables: Vec<TableRef> = known.iter().map(|s| s.table().clone()).collect();
        let oids = self
            .conn
            .command("find the tables", catalog::oids_by_name(&client, &tables))
            .await?;
        let present: Vec<u32> = oids.values().copied().collect();
        let mut schemas = self.schemas(&present).await?;
        let mut report = RescanReport::default();
        for k in known {
            let t = k.table();
            let oid = oids.get(t).filter(|oid| !replaced(k, **oid));
            match oid.and_then(|oid| schemas.remove(oid)) {
                None => report.dropped.push(t.clone()),
                Some((_, Err(reason))) => report.halted.push((t.clone(), reason)),
                Some((_, Ok((now, _)))) if !unchanged(k, &now) => report.changed.push(now),
                Some(_) => {}
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_core::ColumnValueType as V;

    fn col(name: &str, v: V, ty: &str, ordinal: i32) -> ColumnSchema {
        ColumnSchema {
            source_ordinal: Some(ordinal),
            ..ColumnSchema::new(name, v, ty)
        }
    }

    fn table(columns: Vec<ColumnSchema>, id: &[&str]) -> TableSchema {
        TableSchema::new(
            TableRef::new("public", "t"),
            columns,
            id.iter().map(|s| s.to_string()).collect(),
        )
        .unwrap()
    }

    #[test]
    fn a_schema_without_the_identity_compares_without_it() {
        assert!(unchanged(&base(), &base().with_source_id(5)));
        assert!(unchanged(
            &base().with_source_id(5),
            &base().with_source_id(5)
        ));
        assert!(!unchanged(
            &base().with_source_id(4),
            &base().with_source_id(5)
        ));
    }

    fn base() -> TableSchema {
        table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a", V::Text, "character varying(100)", 2),
                col("b", V::Int32, "integer", 3),
            ],
            &["id"],
        )
    }

    #[test]
    fn bounds_and_narrowing() {
        assert_eq!(bound("character varying(100)"), Some(100));
        assert_eq!(bound("numeric(10,2)"), Some(10));
        assert_eq!(bound("timestamp(3) with time zone"), Some(3));
        assert_eq!(bound("text"), None);
        assert_eq!(bound("timestamp without time zone"), Some(6), "the default");
        assert_eq!(bound("interval"), Some(6));
        assert_eq!(bound("time with time zone[]"), Some(6));
        assert_eq!(bound("timeslot"), None, "a user type");
        let ts = |t: &str| col("t", V::Timestamp { with_tz: false }, t, 4);
        assert!(!narrowed(
            &ts("timestamp without time zone"),
            &ts("timestamp(6) without time zone")
        ));
        assert!(narrowed(
            &ts("timestamp without time zone"),
            &ts("timestamp(3) without time zone")
        ));
        let a = |t: &str| col("a", V::Text, t, 2);
        assert!(narrowed(
            &a("character varying(100)"),
            &a("character varying(10)")
        ));
        assert!(narrowed(&a("text"), &a("character varying(10)")));
        assert!(!narrowed(&a("character varying(10)"), &a("text")));
        assert!(!narrowed(
            &a("character varying(10)"),
            &a("character varying(20)")
        ));
        assert!(!narrowed(
            &col("b", V::Int32, "integer", 3),
            &col("b", V::Int64, "bigint", 3)
        ));
    }

    #[test]
    fn diffs_by_attnum() {
        let new = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a2", V::Text, "character varying(10)", 2).not_null(),
                col("c", V::Bool, "boolean", 4),
            ],
            &["id"],
        );
        let changes = diff(&base(), &new);
        assert_eq!(
            changes,
            vec![
                SchemaChange::RenameColumn {
                    from: "a".into(),
                    to: "a2".into(),
                    proven: true
                },
                SchemaChange::ChangeType {
                    column: "a2".into(),
                    from: base().columns()[1].clone(),
                    to: new.columns()[1].clone(),
                    narrowed: true,
                },
                SchemaChange::SetNotNull("a2".into()),
                SchemaChange::DropColumn("b".into()),
                SchemaChange::AddColumn(new.columns()[2].clone()),
            ]
        );
        assert!(diff(&base(), &base()).is_empty());
    }

    #[test]
    fn an_identifier_on_other_columns_is_a_change() {
        let mut columns = base().columns().to_vec();
        columns[2] = columns[2].clone().not_null();
        let new = table(columns, &["b"]);
        assert!(diff(&base(), &new).contains(&SchemaChange::IdentifierChanged));
    }

    #[test]
    fn scope_takes_the_stream_s_types_and_the_catalog_s_identity() {
        // The catalog is ahead: `a` was renamed to `a2` after the signal, and `z` added.
        let now = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a2", V::Text, "text", 2).not_null(),
                col("b", V::Int64, "bigint", 3),
                col("z", V::Text, "text", 5),
            ],
            &["id"],
        );
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
        ];
        let scoped = scope(&base(), &hint, &now).unwrap();
        let got: Vec<(&str, &V, Option<i32>, bool)> = scoped
            .columns()
            .iter()
            .map(|c| (c.name.as_str(), &c.value_type, c.source_ordinal, c.nullable))
            .collect();
        assert_eq!(
            got,
            [
                ("id", &V::Int32, Some(1), false),
                ("a", &V::Text, Some(2), false),
                ("b", &V::Int32, Some(3), true),
            ],
            "nothing past the stream's position: not z, not the rename, not bigint"
        );
        assert_eq!(
            diff(&base(), &scoped),
            vec![SchemaChange::SetNotNull("a".into())]
        );
        let unknown = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("q", V::Text, "text"),
        ];
        assert_eq!(
            scope(&base(), &unknown, &now).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
    }

    #[test]
    fn scope_halts_where_the_catalog_ahead_would_be_a_guess() {
        let id = || col("id", V::Int32, "integer", 1).not_null();
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
        ];
        let halts = |now: TableSchema| scope(&base(), &hint, &now).unwrap_err().code();
        // `a` → `x`, then `b` → `a`, after the signal: `a` and `b` land on attnum 3.
        let chain = table(
            vec![
                id(),
                col("x", V::Text, "character varying(100)", 2),
                col("a", V::Int32, "integer", 3),
            ],
            &["id"],
        );
        assert_eq!(halts(chain), "SCHEMA_MISMATCH");
        // `a` and `b` swapped after the signal: attnums out of order.
        let swap = table(
            vec![
                id(),
                col("b", V::Text, "character varying(100)", 2),
                col("a", V::Int32, "integer", 3),
            ],
            &["id"],
        );
        assert_eq!(halts(swap), "SCHEMA_MISMATCH");
        // `b` renamed away and a new `b` added after the signal: in order, but `b` names
        // another column while its own still exists.
        let taken = table(
            vec![
                id(),
                col("a", V::Text, "character varying(100)", 2),
                col("b_old", V::Int32, "integer", 3),
                col("b", V::Int32, "integer", 4),
            ],
            &["id"],
        );
        assert_eq!(halts(taken), "SCHEMA_MISMATCH");
        // Before the signal `a` dropped and `b` renamed to `a` (the stream has `id, a`); after
        // it, `a` renamed to `x`, or dropped. Placing `a` as dropped since the signal would read
        // as `DropColumn b`.
        let replaced = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Int32, "integer"),
        ];
        let renamed = table(vec![id(), col("x", V::Int32, "integer", 3)], &["id"]);
        assert_eq!(
            scope(&base(), &replaced, &renamed).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        let dropped = table(vec![id()], &["id"]);
        assert_eq!(
            scope(&base(), &replaced, &dropped).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        // Before the signal, `a` → `x` then `b` → `a`: the stream has the new names, and the
        // placement fits.
        let before = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("x", V::Text, "character varying(100)"),
            ColumnSchema::new("a", V::Int32, "integer"),
        ];
        let now = table(
            vec![
                id(),
                col("x", V::Text, "character varying(100)", 2),
                col("a", V::Int32, "integer", 3),
            ],
            &["id"],
        );
        let scoped = scope(&base(), &before, &now).unwrap();
        assert_eq!(
            diff(&base(), &scoped),
            vec![
                SchemaChange::RenameColumn {
                    from: "a".into(),
                    to: "x".into(),
                    proven: true
                },
                SchemaChange::RenameColumn {
                    from: "b".into(),
                    to: "a".into(),
                    proven: true
                },
            ]
        );
    }

    #[test]
    fn scope_halts_on_a_name_re_added_since_the_signal() {
        // `c` added at the signal; after it, `c` renamed to `x` and a new `c` added: the stream's
        // `c` would land on the new one.
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
            ColumnSchema::new("c", V::Int32, "integer"),
        ];
        let mut columns = base().columns().to_vec();
        columns.push(col("x", V::Int32, "integer", 4));
        columns.push(col("c", V::Int32, "integer", 5));
        let now = table(columns, &["id"]);
        assert_eq!(
            scope(&base(), &hint, &now).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        // `b` dropped and re-added before the signal, then renamed to `x` after it: the same
        // inputs as `b` dropped and `x` added after the signal.
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
        ];
        let now = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a", V::Text, "character varying(100)", 2),
                col("x", V::Int32, "integer", 4),
            ],
            &["id"],
        );
        assert_eq!(
            scope(&base(), &hint, &now).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
    }

    #[test]
    fn scope_keeps_a_column_dropped_after_the_signal() {
        // Expand and contract: `c` added (the signal), then `b` dropped in the catalog.
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
            ColumnSchema::new("c", V::Bool, "boolean"),
        ];
        let now = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a", V::Text, "character varying(100)", 2),
                col("c", V::Bool, "boolean", 4),
            ],
            &["id"],
        );
        let scoped = scope(&base(), &hint, &now).unwrap();
        assert_eq!(scoped.column("b").unwrap().source_ordinal, Some(3));
        assert_eq!(
            diff(&base(), &scoped),
            vec![SchemaChange::AddColumn(scoped.columns()[3].clone())]
        );
    }
}
