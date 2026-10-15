//! Catalog reads over `information_schema` (`Schema_Change…` §3 channels A, C, D; config §11.2):
//! which tables of `source.db` the globs select, their columns and primary key, and each table's
//! schema and decode plans. MySQL table patterns and per-table keys are bare names (V-064).

use std::collections::BTreeMap;

use data_avalanche_config::{
    Tables, Tinyint1Detection, check_partitioning, check_table_set, glob_match,
};
use data_avalanche_core::{
    ColumnSchema, HaltReason, ResolvedTables, SourceError, TableRef, TableSchema,
};
use mysql_async::prelude::Queryable;

use crate::connector::MySqlConnector;
use crate::types::{CatalogColumn, ColumnPlan, plan_column};

/// A table the globs can select: `BASE TABLE`, or MariaDB's `SYSTEM VERSIONED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub table: TableRef,
    pub versioned: bool,
}

/// What the globs selected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selection {
    /// Each table with `true` when a pattern without `*` named it.
    pub tables: Vec<(Listed, bool)>,
    pub skipped: Vec<(TableRef, String)>,
    pub halted: Vec<(TableRef, HaltReason)>,
    pub warnings: Vec<String>,
}

/// Why a system-versioned table isn't replicated.
const VERSIONED: &str = "a MariaDB system-versioned table: its history rows and period columns \
    aren't replicated (DROP SYSTEM VERSIONING, or exclude it)";

/// The table's config key: its bare name (V-064).
pub fn table_key(t: &TableRef) -> &str {
    &t.name
}

/// Applies `include` minus `exclude` to the database's tables. A system-versioned table halts
/// (`TABLE_NOT_REPLICABLE`) when its name was listed and is skipped with a warning when a glob
/// found it. An include pattern that matches nothing warns.
pub fn select(include: &[String], exclude: &[String], listed: &[Listed]) -> Selection {
    let mut out = Selection::default();
    for p in include {
        if !listed.iter().any(|l| glob_match(p, &l.table.name)) {
            out.warnings
                .push(format!("tables.include {p} matches no table"));
        }
    }
    for l in listed {
        let named = include
            .iter()
            .filter(|p| glob_match(p, &l.table.name))
            .map(|p| !p.contains('*'))
            .reduce(|a, b| a || b);
        let Some(explicit) = named else { continue };
        if exclude.iter().any(|p| glob_match(p, &l.table.name)) {
            continue;
        }
        if l.versioned {
            match explicit {
                true => out.halted.push((
                    l.table.clone(),
                    HaltReason::NotReplicable {
                        reason: VERSIONED.into(),
                    },
                )),
                false => out.skipped.push((l.table.clone(), VERSIONED.into())),
            }
            continue;
        }
        out.tables.push((l.clone(), explicit));
    }
    out
}

/// A table as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Described {
    pub table: TableRef,
    /// In `ORDINAL_POSITION` order.
    pub columns: Vec<CatalogColumn>,
    pub primary_key: Vec<String>,
}

/// What planning a table noticed that isn't a halt.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Notes {
    /// Decimals wider than 38 digits kept as text: `(column, type)`.
    pub read_as_text: Vec<(String, String)>,
}

/// A planned table: its schema, and each column's decode plan in schema order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub schema: TableSchema,
    pub plans: Vec<ColumnPlan>,
    pub notes: Notes,
}

/// A table's schema and plans, checked in this order (config §11.2): every column plans
/// ([`plan_column`]: overrides, charsets, decimal caps); the identifier (the
/// `tables.primary_key` entry, else the primary key) passes [`TableSchema::new`] (reserved names,
/// a non-empty identifier of usable columns); `tables.partitioning` fits the columns.
pub fn plan_table(
    d: &Described,
    tables: &Tables,
    tinyint1_as_bool: bool,
) -> Result<Planned, HaltReason> {
    let key = table_key(&d.table);
    let identifier = tables
        .primary_key
        .get(key)
        .cloned()
        .unwrap_or_else(|| d.primary_key.clone());
    let mut notes = Notes::default();
    let mut columns = Vec::with_capacity(d.columns.len());
    let mut plans = Vec::with_capacity(d.columns.len());
    for c in &d.columns {
        let over = tables
            .column_type_overrides
            .get(&format!("{key}.{}", c.name));
        let p = plan_column(
            c,
            tinyint1_as_bool,
            tables.numeric_overflow_action,
            over,
            identifier.contains(&c.name),
        )?;
        if let Some(t) = &p.read_as_text {
            notes.read_as_text.push((c.name.clone(), t.clone()));
        }
        columns.push(ColumnSchema {
            name: c.name.clone(),
            value_type: p.value_type.clone(),
            logical: p.logical.clone(),
            nullable: c.nullable,
            source_type: c.column_type.clone(),
            source_ordinal: Some(c.ordinal as i32),
        });
        plans.push(p);
    }
    let schema = TableSchema::new(d.table.clone(), columns, identifier)?;
    if let Some(fields) = tables.partitioning.get(key) {
        check_partitioning(&schema, fields)?;
    }
    Ok(Planned {
        schema,
        plans,
        notes,
    })
}

/// `information_schema` rows of one table name per query: an `IN` list this long at most.
const NAMES_PER_QUERY: usize = 500;

impl MySqlConnector {
    /// `tinyint1_as_bool` with `tinyint1_detection: information_schema` (config §9.2.2).
    pub(crate) fn tinyint1_as_bool(&self) -> bool {
        self.mysql.tinyint1_as_bool
            && self.mysql.tinyint1_detection == Tinyint1Detection::InformationSchema
    }

    /// The tables of `source.db` the globs can select.
    pub async fn list_tables(&self) -> Result<Vec<Listed>, SourceError> {
        let db = self.pipeline.source.db.clone();
        let rows: Vec<(String, String)> = self
            .conn
            .command("list tables", move |c| {
                Box::pin(async move {
                    c.exec(
                        "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
                          WHERE TABLE_SCHEMA = ? AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED')
                          ORDER BY TABLE_NAME",
                        (db,),
                    )
                    .await
                })
            })
            .await?;
        Ok(rows
            .into_iter()
            .map(|(name, kind)| Listed {
                table: TableRef::new(self.pipeline.source.db.clone(), name),
                versioned: kind == "SYSTEM VERSIONED",
            })
            .collect())
    }

    /// The columns and primary keys of the tables named `names` in `source.db`; a table that
    /// isn't there (dropped since it was listed) is left out.
    pub async fn describe(
        &self,
        names: &[String],
    ) -> Result<BTreeMap<String, Described>, SourceError> {
        let mut out: BTreeMap<String, Described> = BTreeMap::new();
        for chunk in names.chunks(NAMES_PER_QUERY) {
            let db = self.pipeline.source.db.clone();
            let list = vec!["?"; chunk.len()].join(", ");
            let mut params: Vec<mysql_async::Value> = vec![db.clone().into()];
            params.extend(chunk.iter().map(|n| n.clone().into()));
            let keys_params = params.clone();
            let (columns, keys): (Vec<mysql_async::Row>, Vec<(String, String)>) = self
                .conn
                .command("describe tables", move |c| {
                    Box::pin(async move {
                        let columns = c
                            .exec(
                                format!(
                                    "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE,
                                            COLUMN_TYPE, CHARACTER_SET_NAME, NUMERIC_PRECISION,
                                            NUMERIC_SCALE, DATETIME_PRECISION,
                                            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                                       FROM information_schema.COLUMNS
                                      WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ({list})
                                      ORDER BY TABLE_NAME, ORDINAL_POSITION"
                                ),
                                params,
                            )
                            .await?;
                        let keys = c
                            .exec(
                                format!(
                                    "SELECT TABLE_NAME, COLUMN_NAME
                                       FROM information_schema.KEY_COLUMN_USAGE
                                      WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ({list})
                                        AND CONSTRAINT_NAME = 'PRIMARY'
                                      ORDER BY TABLE_NAME, ORDINAL_POSITION"
                                ),
                                keys_params,
                            )
                            .await?;
                        Ok((columns, keys))
                    })
                })
                .await?;
            for r in columns {
                let table: String = r.get(0).unwrap_or_default();
                let d = out.entry(table.clone()).or_insert_with(|| Described {
                    table: TableRef::new(db.clone(), table),
                    columns: vec![],
                    primary_key: vec![],
                });
                d.columns.push(CatalogColumn {
                    name: r.get(1).unwrap_or_default(),
                    ordinal: r.get(2).unwrap_or_default(),
                    data_type: r
                        .get::<String, _>(3)
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    column_type: r
                        .get::<String, _>(4)
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    charset: r.get(5).flatten(),
                    precision: r.get(6).flatten(),
                    scale: r.get(7).flatten(),
                    fsp: r.get(8).flatten(),
                    max_length: r.get(9).flatten(),
                    nullable: r.get::<String, _>(10).is_some_and(|n| n == "YES"),
                });
            }
            for (table, column) in keys {
                if let Some(d) = out.get_mut(&table) {
                    d.primary_key.push(column);
                }
            }
        }
        Ok(out)
    }

    /// Channel A: the configured table set, resolved against the catalog and checked (config
    /// §11.2): a table a glob found without a primary key (and no `tables.primary_key` entry) is
    /// skipped with a warning; one named without a glob halts.
    pub async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        let listed = self.list_tables().await?;
        let t = &self.pipeline.tables;
        let selection = select(&t.include, &t.exclude, &listed);
        let names: Vec<String> = selection
            .tables
            .iter()
            .map(|(l, _)| l.table.name.clone())
            .collect();
        let mut described = self.describe(&names).await?;
        let mut out = ResolvedTables {
            skipped: selection.skipped,
            halted: selection.halted,
            warnings: selection.warnings,
            ..ResolvedTables::default()
        };
        for (listed, explicit) in selection.tables {
            let Some(d) = described.remove(&listed.table.name) else {
                continue; // dropped since it was listed
            };
            match plan_table(&d, t, self.tinyint1_as_bool()) {
                Ok(p) => {
                    for (column, ty) in p.notes.read_as_text {
                        out.warnings.push(format!(
                            "{}.{column}: {ty} is wider than 38 digits and is read as text",
                            listed.table.name
                        ));
                    }
                    out.tables.push(p.schema);
                }
                Err(HaltReason::NoIdentifier) if !explicit => out.skipped.push((
                    listed.table,
                    "no primary key and no tables.primary_key entry".into(),
                )),
                Err(reason) => out.halted.push((listed.table, reason)),
            }
        }
        // Halted tables are in the set: their per-table config keys aren't stale (V-106).
        let resolved: Vec<TableRef> = out
            .tables
            .iter()
            .map(|s| s.table().clone())
            .chain(out.halted.iter().map(|(t, _)| t.clone()))
            .collect();
        let report = check_table_set(&self.pipeline, &resolved);
        if let Some(e) = report.errors.first() {
            return Err(SourceError::Permanent(format!(
                "{} {}: {}",
                e.rule, e.path, e.message
            )));
        }
        out.warnings.extend(
            report
                .warnings
                .iter()
                .map(|w| format!("{} {}: {}", w.rule, w.path, w.message)),
        );
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listed(name: &str, versioned: bool) -> Listed {
        Listed {
            table: TableRef::new("shop", name),
            versioned,
        }
    }

    fn strings(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn globs_select_bare_names() {
        let all = [
            listed("orders", false),
            listed("order_items", false),
            listed("audit", false),
            listed("history", true),
        ];
        let s = select(
            &strings(&["order*", "audit", "nope"]),
            &strings(&["*_items"]),
            &all,
        );
        assert_eq!(
            s.tables,
            vec![
                (listed("orders", false), false),
                (listed("audit", false), true)
            ]
        );
        assert_eq!(s.warnings, ["tables.include nope matches no table"]);
        // A system-versioned table: halted when named, skipped when a glob found it.
        let named = select(&strings(&["history"]), &[], &all);
        assert!(matches!(
            named.halted[0].1,
            HaltReason::NotReplicable { .. }
        ));
        let globbed = select(&strings(&["*"]), &[], &all);
        assert_eq!(globbed.skipped.len(), 1);
        assert_eq!(globbed.tables.len(), 3);
    }
}
