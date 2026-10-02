//! Channels C and D (`Schema_Change…` §3, §6): the change a schema signal implicates, proven
//! with `pg_attribute.attnum`, and the batched idle rescan.

use data_avalanche_core::{
    ColumnSchema, HaltReason, RescanReport, SchemaChange, SchemaDelta, SchemaHint, SourceError,
    TableRef, TableSchema,
};

use crate::catalog::{self, table_key};
use crate::connector::PgConnector;

/// The first length or precision in a type's text: `character varying(100)` → 100,
/// `timestamp(3) with time zone` → 3; `None` without one (unbounded, or the type's default).
fn bound(type_text: &str) -> Option<i64> {
    let open = type_text.find('(')?;
    let close = type_text[open..].find([')', ','])? + open;
    type_text[open + 1..close].trim().parse().ok()
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
/// from the catalog, found by name, else by the `attnum` it had in `cached` (a later rename). A
/// column found neither way can't be placed: the table halts rather than guess.
pub fn scope(
    cached: &TableSchema,
    hint: &[ColumnSchema],
    now: &TableSchema,
) -> Result<TableSchema, HaltReason> {
    let by_ordinal = |o: Option<i32>| {
        now.columns()
            .iter()
            .find(|c| o.is_some() && c.source_ordinal == o)
    };
    let mut columns = Vec::with_capacity(hint.len());
    for h in hint {
        let catalog = now
            .column(&h.name)
            .or_else(|| cached.column(&h.name).and_then(|c| by_ordinal(c.source_ordinal)))
            .ok_or_else(|| HaltReason::SchemaMismatch {
                column: h.name.clone(),
                detail: "the stream carries a column the catalog doesn't have by that name or position; re-snapshot".into(),
            })?;
        columns.push(ColumnSchema {
            nullable: catalog.nullable,
            source_ordinal: catalog.source_ordinal,
            ..h.clone()
        });
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
    TableSchema::new(cached.table().clone(), columns, identifier)
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
    /// The table's schema as the catalog has it now; `None` when it was dropped.
    async fn current(&self, table: &TableRef) -> Result<Option<TableSchema>, SourceError> {
        let client = self.conn.client().await?;
        let oids = self
            .conn
            .command(
                "find the table",
                catalog::oids_by_name(&client, std::slice::from_ref(table)),
            )
            .await?;
        let Some(oid) = oids.get(table) else {
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
        let Some(now) = self.current(&table).await? else {
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

    /// Channel D: one batched catalog read of the tracked tables. A table gone from `pg_class`
    /// is dropped (§2.1, `TABLE_DROPPED`); a table whose catalog schema differs is reported with
    /// its current schema, for freshness and alerting only.
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
            match oids.get(t).and_then(|oid| schemas.remove(oid)) {
                None => report.dropped.push(t.clone()),
                Some((_, Err(reason))) => report.halted.push((t.clone(), reason)),
                Some((_, Ok((now, _)))) if *k != now => report.changed.push(now),
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
}
