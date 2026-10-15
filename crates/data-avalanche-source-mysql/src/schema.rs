//! Channels C and D (`Schema_Change…` §0, §3, §6): the change a DDL statement implicates, read from
//! `information_schema` and scoped to the columns the statement names, and the batched idle
//! rescan. MySQL has no column identity that survives a rename: a column is its name, and a
//! rename is proven only by the statement (`RENAME COLUMN`, `CHANGE old new`). Where the
//! classifier couldn't read the statement, the whole catalog table is the change, and a rename
//! reads as a drop and an add (§6, degraded with a warning by the evolver).
//!
//! The catalog may be ahead of the stream (§0): a scoped change takes only its columns from it.
//! The stream checks every table map against the schema, so a change taken early (a later DDL's
//! column) halts the table instead of misreading rows.

use std::collections::BTreeMap;

use data_avalanche_core::{
    ColumnSchema, HaltReason, RescanReport, SchemaChange, SchemaDelta, SchemaHint, SourceError,
    TableRef, TableSchema,
};

use crate::catalog::{plan_table, table_key};
use crate::connector::MySqlConnector;
use crate::ddl::{Renamed, Scope, classify, renamed};
use crate::types::{CatalogColumn, members};

/// The change a statement makes to `table`: its scope and proven renames. A cut-over swap into the
/// table's name scopes the whole table, with nothing proven.
pub fn scope_of(sql: &str, table: &TableRef) -> (Scope, Vec<(String, String)>) {
    let d = classify(sql, &table.schema);
    let swapped = renamed(&d.renamed, std::slice::from_ref(table))
        .iter()
        .any(|(_, r)| matches!(r, Renamed::Swapped { .. }));
    match d.altered.iter().find(|a| &a.table == table) {
        Some(a) if !swapped => (a.scope.clone(), a.renames.clone()),
        _ => (Scope::Table, vec![]),
    }
}

/// The bound a type's text puts on its values, where one type holds fewer than another of the
/// same logical type: a length, a precision and scale, fractional digits, a text or blob size.
fn bound(c: &CatalogColumn) -> Option<(u64, u64)> {
    let n = |v: Option<u32>| v.map(u64::from);
    let length = || {
        let open = c.column_type.find('(')?;
        let close = c.column_type[open..].find(')')? + open;
        c.column_type[open + 1..close].trim().parse().ok()
    };
    Some(match c.data_type.as_str() {
        "char" | "varchar" | "binary" | "varbinary" | "bit" => (length()?, 0),
        "tinytext" | "tinyblob" => (255, 0),
        "text" | "blob" => (65_535, 0),
        "mediumtext" | "mediumblob" => (16_777_215, 0),
        "longtext" | "longblob" => (4_294_967_295, 0),
        "decimal" | "numeric" => (n(c.precision)? - n(c.scale)?, n(c.scale)?),
        "time" | "datetime" | "timestamp" => (n(c.fsp)?, 0),
        _ => return None,
    })
}

/// Whether `to` holds fewer values than `from` of the same logical type: a shorter length or text
/// size, fewer integer or fraction digits, fewer fractional seconds.
pub fn narrowed(from: &ColumnSchema, to: &ColumnSchema) -> bool {
    if from.logical != to.logical {
        return false;
    }
    let (f, t) = (
        CatalogColumn::from_schema(from),
        CatalogColumn::from_schema(to),
    );
    match (bound(&f), bound(&t)) {
        (Some((fa, fb)), Some((ta, tb))) => ta < fa || tb < fb,
        _ => false,
    }
}

fn guess(column: &str, why: &str) -> HaltReason {
    HaltReason::SchemaMismatch {
        column: column.into(),
        detail: format!("{why}; re-snapshot the table"),
    }
}

/// The schema after the statement (§0: scoped, never the catalog's "now" beyond the statement's
/// columns): `cached` with the proven renames applied at once (a swap, a chain; a column whose
/// name a rename takes is gone), each other scoped column taken from `now` (changed, added, or
/// dropped when `now` hasn't it), and each scoped column `now` has placed after the nearest
/// column before it in the catalog that the schema has (an add, `AFTER`, `FIRST`). A renamed
/// column the schema hasn't, a rename `now` doesn't show, or a scoped column neither schema has,
/// can't be placed: the table halts. Positions are numbered in the result's order; the
/// identifier is `now`'s.
pub fn scope(
    cached: &TableSchema,
    scope: &Scope,
    renames: &[(String, String)],
    now: &TableSchema,
) -> Result<TableSchema, HaltReason> {
    let Scope::Columns(names) = scope else {
        return Ok(now.clone());
    };
    for (old, _) in renames {
        if cached.column(old).is_none() {
            return Err(guess(old, "a renamed column the schema doesn't have"));
        }
    }
    let target = |n: &str| renames.iter().find(|(o, _)| o == n).map(|(_, r)| r);
    let taken = |n: &str| renames.iter().any(|(_, r)| r == n);
    let mut columns: Vec<ColumnSchema> = vec![];
    for c in cached.columns() {
        match target(&c.name) {
            Some(new) => match now.column(new) {
                Some(n) => columns.push(n.clone()),
                None => {
                    return Err(guess(
                        new,
                        "the catalog, ahead of the stream, doesn't have the renamed column",
                    ));
                }
            },
            None if taken(&c.name) => {}
            None => columns.push(c.clone()),
        }
    }
    let renamed = |n: &String| renames.iter().any(|(o, r)| o == n || r == n);
    for name in names.iter().filter(|n| !renamed(n)) {
        let at = columns.iter().position(|c| &c.name == name);
        match (at, now.column(name)) {
            (Some(at), Some(c)) => columns[at] = c.clone(),
            (Some(at), None) => {
                columns.remove(at);
            }
            (None, Some(c)) => columns.push(c.clone()),
            (None, None) => {
                return Err(guess(
                    name,
                    "a column the statement names that neither the schema nor the catalog has",
                ));
            }
        }
    }
    // The binlog's rows are positional: each scoped column goes after the nearest column before
    // it in the catalog that the schema has, in the catalog's order.
    let scoped = |n: &str| names.iter().any(|x| x == n) || taken(n);
    for c in now.columns().iter().filter(|c| scoped(&c.name)) {
        let Some(at) = columns.iter().position(|x| x.name == c.name) else {
            continue;
        };
        let moved = columns.remove(at);
        let before = now
            .columns()
            .iter()
            .take_while(|x| x.name != c.name)
            .filter_map(|x| columns.iter().position(|y| y.name == x.name))
            .last();
        columns.insert(before.map_or(0, |b| b + 1), moved);
    }
    for (i, c) in columns.iter_mut().enumerate() {
        c.source_ordinal = Some(i as i32 + 1);
    }
    TableSchema::new(cached.table().clone(), columns, now.identifier().to_vec())
}

/// The ENUM or SET change between two column types of the same column, if both are one.
fn members_change(column: &str, from: &CatalogColumn, to: &CatalogColumn) -> Option<SchemaChange> {
    let kind = |c: &CatalogColumn| matches!(c.data_type.as_str(), "enum" | "set");
    if !kind(from) || from.data_type != to.data_type {
        return None;
    }
    let (f, t) = (members(&from.column_type), members(&to.column_type));
    Some(match t.starts_with(&f) {
        true => SchemaChange::EnumValuesAppended(column.into()),
        false => SchemaChange::EnumValuesChanged(column.into()),
    })
}

/// `cached` → `new` as changes, columns matched by name and by the proven `renames`.
pub fn diff(
    cached: &TableSchema,
    new: &TableSchema,
    renames: &[(String, String)],
) -> Vec<SchemaChange> {
    let mut changes = vec![];
    fn target<'a>(renames: &'a [(String, String)], name: &'a str) -> &'a str {
        renames
            .iter()
            .find(|(o, _)| o == name)
            .map_or(name, |(_, n)| n.as_str())
    }
    // `DROP b, CHANGE a b`: the old `b` goes before `a` takes its name.
    let taken =
        |n: &str| !renames.iter().any(|(o, _)| o == n) && renames.iter().any(|(_, r)| r == n);
    for c in cached.columns().iter().filter(|c| taken(&c.name)) {
        changes.push(SchemaChange::DropColumn(c.name.clone()));
    }
    for c in cached.columns().iter().filter(|c| !taken(&c.name)) {
        let to = target(renames, &c.name);
        let Some(n) = new.column(to) else {
            changes.push(SchemaChange::DropColumn(c.name.clone()));
            continue;
        };
        if to != c.name {
            changes.push(SchemaChange::RenameColumn {
                from: c.name.clone(),
                to: to.to_owned(),
                proven: true,
            });
        }
        let (f, t) = (CatalogColumn::from_schema(c), CatalogColumn::from_schema(n));
        if n.value_type != c.value_type || n.logical != c.logical || f.column_type != t.column_type
        {
            match members_change(to, &f, &t) {
                Some(m) if n.value_type == c.value_type => changes.push(m),
                _ => changes.push(SchemaChange::ChangeType {
                    column: to.to_owned(),
                    from: c.clone(),
                    to: n.clone(),
                    narrowed: narrowed(c, n),
                }),
            }
        } else if f.charset != t.charset {
            changes.push(SchemaChange::NoTargetEffect(format!(
                "column {to:?}: character set {:?} → {:?}",
                f.charset, t.charset
            )));
        }
        match (c.nullable, n.nullable) {
            (true, false) => changes.push(SchemaChange::SetNotNull(to.to_owned())),
            (false, true) => changes.push(SchemaChange::DropNotNull(to.to_owned())),
            _ => {}
        }
    }
    let renamed_to = |n: &str| renames.iter().any(|(_, r)| r == n);
    for n in new.columns() {
        if cached.column(&n.name).is_none() && !renamed_to(&n.name) {
            changes.push(SchemaChange::AddColumn(n.clone()));
        }
    }
    let identifier = |s: &TableSchema| -> Vec<String> {
        s.identifier()
            .iter()
            .map(|k| target(renames, k).to_owned())
            .collect()
    };
    if identifier(cached) != new.identifier() {
        changes.push(SchemaChange::IdentifierChanged);
    }
    changes
}

impl MySqlConnector {
    /// The tables' schemas as the catalog has them now, by name; a table not there is left out.
    async fn current(
        &self,
        names: &[String],
    ) -> Result<BTreeMap<String, Result<TableSchema, HaltReason>>, SourceError> {
        let described = self.describe(names).await?;
        Ok(described
            .into_iter()
            .map(|(name, d)| {
                let planned = plan_table(&d, &self.pipeline.tables, self.tinyint1_as_bool());
                (name, planned.map(|p| p.schema))
            })
            .collect())
    }

    /// Channel C: the change the statement `hint` makes to `cached` (`Schema_Change…` §0, §3,
    /// §6). A table gone from the catalog is dropped.
    pub async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        let table = cached.table().clone();
        let SchemaHint::Ddl(sql) = hint else {
            return Err(SourceError::Permanent(format!(
                "{}: a column hint on a MySQL source",
                table_key(&table)
            )));
        };
        let halt = |reason| SourceError::TableHalt {
            table: table.clone(),
            reason,
        };
        let Some(now) = self
            .current(std::slice::from_ref(&table.name))
            .await?
            .remove(&table.name)
        else {
            return Ok((
                SchemaDelta {
                    table: table.clone(),
                    changes: vec![SchemaChange::TableDropped],
                },
                None,
            ));
        };
        let now = now.map_err(halt)?;
        let (columns, renames) = scope_of(sql, &table);
        let new = scope(cached, &columns, &renames, &now).map_err(halt)?;
        let changes = diff(cached, &new, &renames);
        Ok((SchemaDelta { table, changes }, Some(new)))
    }

    /// Channel D: one batched catalog read of the tracked tables. A table gone from the catalog is
    /// dropped; one whose catalog schema differs is reported with it, for freshness and alerting
    /// only.
    pub async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        let names: Vec<String> = known.iter().map(|s| s.table().name.clone()).collect();
        let mut now = self.current(&names).await?;
        let mut report = RescanReport::default();
        for k in known {
            let t = k.table();
            match now.remove(&t.name) {
                None => report.dropped.push(t.clone()),
                Some(Err(reason)) => report.halted.push((t.clone(), reason)),
                Some(Ok(s)) if s != *k => report.changed.push(s),
                Some(Ok(_)) => {}
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::ColumnValueType as V;

    use super::*;

    fn col(name: &str, t: V, source: &str, ordinal: i32) -> ColumnSchema {
        ColumnSchema {
            source_ordinal: Some(ordinal),
            ..ColumnSchema::new(name, t, source)
        }
    }

    fn table(columns: Vec<ColumnSchema>) -> TableSchema {
        let id = ColumnSchema {
            nullable: false,
            ..col("id", V::Int32, "int", 1)
        };
        let mut all = vec![id];
        all.extend(columns);
        TableSchema::new(TableRef::new("shop", "t"), all, vec!["id".into()]).unwrap()
    }

    fn names(s: &TableSchema) -> Vec<&str> {
        s.columns().iter().map(|c| c.name.as_str()).collect()
    }

    fn cols(n: &[&str]) -> Scope {
        Scope::Columns(n.iter().map(|x| x.to_string()).collect())
    }

    #[test]
    fn a_scoped_change_takes_only_its_columns_from_the_catalog() {
        let cached = table(vec![
            col("a", V::Int32, "int", 2),
            col("b", V::Text, "text", 3),
        ]);
        // The catalog is ahead: `ADD c AFTER id` is the statement, `DROP b` comes later.
        let now = table(vec![
            col("c", V::Int64, "bigint", 2),
            col("a", V::Int32, "int", 3),
        ]);
        let s = scope(&cached, &cols(&["c"]), &[], &now).unwrap();
        assert_eq!(names(&s), ["id", "c", "a", "b"]);
        assert_eq!(
            diff(&cached, &s, &[]),
            [SchemaChange::AddColumn(col("c", V::Int64, "bigint", 2))]
        );
        // The whole table, when the statement wasn't read.
        assert_eq!(
            names(&scope(&cached, &Scope::Table, &[], &now).unwrap()),
            ["id", "c", "a"]
        );
    }

    #[test]
    fn a_proven_rename_keeps_its_place() {
        let cached = table(vec![
            col("a", V::Int32, "int", 2),
            col("b", V::Text, "text", 3),
        ]);
        let now = table(vec![
            col("aa", V::Int64, "bigint", 2),
            col("b", V::Text, "text", 3),
        ]);
        let renames = [("a".to_owned(), "aa".to_owned())];
        let s = scope(&cached, &cols(&["a", "aa"]), &renames, &now).unwrap();
        assert_eq!(names(&s), ["id", "aa", "b"]);
        let d = diff(&cached, &s, &renames);
        assert_eq!(
            d[0],
            SchemaChange::RenameColumn {
                from: "a".into(),
                to: "aa".into(),
                proven: true
            }
        );
        assert!(
            matches!(&d[1], SchemaChange::ChangeType { column, narrowed: false, .. } if column == "aa")
        );
        // Unproven (the whole table): a drop and an add.
        let d = diff(&cached, &now, &[]);
        assert!(d.contains(&SchemaChange::DropColumn("a".into())));
        assert!(matches!(d.last(), Some(SchemaChange::AddColumn(c)) if c.name == "aa"));
    }

    #[test]
    fn what_the_catalog_cant_place_halts() {
        let cached = table(vec![col("a", V::Int32, "int", 2)]);
        let now = table(vec![col("b", V::Int32, "int", 2)]);
        // Renamed again since: the catalog doesn't show the rename's target.
        assert!(
            scope(
                &cached,
                &cols(&["a", "x"]),
                &[("a".into(), "x".into())],
                &now
            )
            .is_err()
        );
        // Added and dropped again since.
        assert!(scope(&cached, &cols(&["q"]), &[], &now).is_err());
    }

    #[test]
    fn moves_swaps_chains_and_taken_names() {
        let int = |name: &str, ordinal| col(name, V::Int32, "int", ordinal);
        let cached = table(vec![int("a", 2), int("b", 3)]);
        let ordinals = |s: &TableSchema| -> Vec<Option<i32>> {
            s.columns().iter().map(|c| c.source_ordinal).collect()
        };
        let rename = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(o, n)| (o.to_string(), n.to_string()))
                .collect()
        };
        let proven = |from: &str, to: &str| SchemaChange::RenameColumn {
            from: from.into(),
            to: to.into(),
            proven: true,
        };
        // `MODIFY a INT AFTER b`: the rows after it have b before a.
        let moved = table(vec![int("b", 2), int("a", 3)]);
        let s = scope(&cached, &cols(&["a"]), &[], &moved).unwrap();
        assert_eq!(names(&s), ["id", "b", "a"]);
        assert_eq!(ordinals(&s), [Some(1), Some(2), Some(3)]);
        // `MODIFY b INT FIRST`.
        let id = ColumnSchema {
            nullable: false,
            ..int("id", 2)
        };
        let first = TableSchema::new(
            TableRef::new("shop", "t"),
            vec![int("b", 1), id, int("a", 3)],
            vec!["id".into()],
        )
        .unwrap();
        let s = scope(&cached, &cols(&["b"]), &[], &first).unwrap();
        assert_eq!(names(&s), ["b", "id", "a"]);
        assert_eq!(ordinals(&s), [Some(1), Some(2), Some(3)]);
        // `CHANGE a b INT, CHANGE b a INT`: a swap, in place.
        let swap = rename(&[("a", "b"), ("b", "a")]);
        let s = scope(&cached, &cols(&["a", "b", "b", "a"]), &swap, &moved).unwrap();
        assert_eq!(names(&s), ["id", "b", "a"]);
        assert_eq!(
            diff(&cached, &s, &swap),
            [proven("a", "b"), proven("b", "a")]
        );
        // `CHANGE a b INT, CHANGE b c INT`: a chain.
        let chain = rename(&[("a", "b"), ("b", "c")]);
        let now = table(vec![int("b", 2), int("c", 3)]);
        let s = scope(&cached, &cols(&["a", "b", "b", "c"]), &chain, &now).unwrap();
        assert_eq!(names(&s), ["id", "b", "c"]);
        assert_eq!(
            diff(&cached, &s, &chain),
            [proven("a", "b"), proven("b", "c")]
        );
        // `DROP b, CHANGE a b INT`: the old b is gone, then a takes its name.
        let taken = rename(&[("a", "b")]);
        let now = table(vec![int("b", 2)]);
        let s = scope(&cached, &cols(&["b", "a", "b"]), &taken, &now).unwrap();
        assert_eq!(names(&s), ["id", "b"]);
        assert_eq!(
            diff(&cached, &s, &taken),
            [SchemaChange::DropColumn("b".into()), proven("a", "b")]
        );
    }

    #[test]
    fn types_members_charsets_and_keys_classify() {
        let cached = table(vec![
            col("e", V::Text, "enum('a','b') character set utf8mb4", 2),
            col("v", V::Text, "varchar(100) character set latin1", 3),
            col(
                "d",
                V::Numeric {
                    precision: 10,
                    scale: 2,
                },
                "decimal(10,2)",
                4,
            ),
        ]);
        let changed = |e: &str, v: &str, d: &str| {
            table(vec![
                col("e", V::Text, e, 2),
                col("v", V::Text, v, 3),
                col(
                    "d",
                    V::Numeric {
                        precision: 10,
                        scale: 2,
                    },
                    d,
                    4,
                ),
            ])
        };
        let d = diff(
            &cached,
            &changed(
                "enum('a','b','c') character set utf8mb4",
                "varchar(100) character set utf8mb4",
                "decimal(10,2)",
            ),
            &[],
        );
        assert_eq!(d[0], SchemaChange::EnumValuesAppended("e".into()));
        assert!(matches!(&d[1], SchemaChange::NoTargetEffect(m) if m.contains("latin1")));
        let d = diff(
            &cached,
            &changed(
                "enum('b','a') character set utf8mb4",
                "varchar(10) character set latin1",
                "decimal(10,2)",
            ),
            &[],
        );
        assert_eq!(d[0], SchemaChange::EnumValuesChanged("e".into()));
        assert!(matches!(
            &d[1],
            SchemaChange::ChangeType { narrowed: true, .. }
        ));
        let mut keyed = cached.columns().to_vec();
        keyed[1].nullable = false;
        let rekeyed = TableSchema::new(
            TableRef::new("shop", "t"),
            keyed,
            vec!["id".into(), "e".into()],
        )
        .unwrap();
        assert_eq!(
            diff(&cached, &rekeyed, &[]),
            [
                SchemaChange::SetNotNull("e".into()),
                SchemaChange::IdentifierChanged
            ]
        );
    }

    #[test]
    fn narrowing_reads_lengths_sizes_digits_and_fractions() {
        let c = |t: &str, v: V| ColumnSchema::new("c", v, t);
        let text = |a: &str, b: &str| narrowed(&c(a, V::Text), &c(b, V::Text));
        assert!(text("varchar(100)", "varchar(10)"));
        assert!(!text("varchar(10)", "varchar(100)"));
        assert!(text("text", "varchar(100)"));
        assert!(!text("varchar(100)", "mediumtext"));
        assert!(text(
            "longtext character set utf8mb4",
            "text character set utf8mb4"
        ));
        let ts = |a: &str, b: &str| {
            narrowed(
                &c(a, V::Timestamp { with_tz: false }),
                &c(b, V::Timestamp { with_tz: false }),
            )
        };
        assert!(ts("datetime(6)", "datetime(3)"));
        assert!(!ts("datetime", "datetime(6)"));
        let n = V::Numeric {
            precision: 10,
            scale: 2,
        };
        assert!(narrowed(
            &c("decimal(10,4)", n.clone()),
            &c("decimal(10,2)", n.clone())
        ));
        // MariaDB keeps integer display widths: they bound nothing.
        assert!(!narrowed(&c("int(11)", V::Int32), &c("int(5)", V::Int32)));
    }

    #[test]
    fn a_statement_scopes_its_table() {
        let t = TableRef::new("shop", "orders");
        assert_eq!(
            scope_of("ALTER TABLE orders ADD COLUMN x INT", &t),
            (cols(&["x"]), vec![])
        );
        assert_eq!(
            scope_of("ALTER TABLE orders ENGINE=InnoDB", &t).0,
            Scope::Table
        );
        assert_eq!(
            scope_of(
                "RENAME TABLE orders TO _orders_del, _orders_gho TO orders",
                &t
            ),
            (Scope::Table, vec![])
        );
    }
}
