//! The DDL classifier (channel B, `MySQL_DDL_Classifier.md`): what a `QUERY_EVENT` statement does
//! to which tables. It routes; channel C reads what changed from the catalog. Two tiers, fail-soft
//! (D-19a): a tokenizer, aware of quotes and comments, reads every statement's verb and tables;
//! `sqlparser` (`MySqlDialect`) reads an `ALTER TABLE`'s operations, which scope channel C to the
//! columns the statement names (`Schema_Change…` §0) and prove its column renames (§6). When
//! `sqlparser` can't read a statement (`ENGINE=`, `CONVERT TO CHARACTER SET`, MariaDB's
//! `IF EXISTS`, measured on 0.62), the scope is the whole table and no rename is proven.
//!
//! A partition or tablespace operation that removes or replaces rows (`DROP`, `TRUNCATE`,
//! `EXCHANGE`, `IMPORT`, `DISCARD` or MariaDB `CONVERT` of a partition) changes rows without row
//! events: the table halts (`UNSAFE_DDL`), never diverges silently.

use data_avalanche_core::TableRef;
use sqlparser::ast::{AlterTableOperation, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// The columns a table's change may touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// The columns the statement names; empty for an index, key or option change.
    Columns(Vec<String>),
    /// Any column: the statement's operations couldn't be read.
    Table,
}

/// A table whose shape the statement may change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Altered {
    pub table: TableRef,
    pub scope: Scope,
    /// Column renames the statement proves (`RENAME COLUMN`, `CHANGE old new`): `(old, new)`.
    pub renames: Vec<(String, String)>,
}

/// What one statement does to tables. Names are qualified with the event's default database.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Ddl {
    /// `ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`.
    pub altered: Vec<Altered>,
    /// `RENAME TABLE` pairs and `ALTER TABLE … RENAME TO`, in statement order.
    pub renamed: Vec<(TableRef, TableRef)>,
    /// `DROP TABLE`; MariaDB `CREATE OR REPLACE TABLE`, which drops the table it replaces.
    pub dropped: Vec<TableRef>,
    /// `DROP DATABASE`, MariaDB `CREATE OR REPLACE DATABASE`.
    pub dropped_database: Option<String>,
    pub truncated: Vec<TableRef>,
    /// Tables whose rows the statement changes without row events, with the operation.
    pub unlogged: Vec<(TableRef, String)>,
    /// A table statement neither tier read a table name from.
    pub unreadable: bool,
    /// A row change logged as its statement (a session's `binlog_format` STATEMENT or MIXED),
    /// its rows not in the binlog: the names it qualifies others with (`db.t`, `t.c`).
    pub logged_as_statement: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// A bare word: a keyword or an unquoted name.
    Word(String),
    /// A backtick-quoted name, unquoted.
    Quoted(String),
    /// A string literal.
    Str,
    Punct(char),
}

/// The statement's tokens. Comments are skipped, except executable ones (`/*!80023 … */`,
/// MariaDB `/*M!… */`), whose text the server runs.
fn tokens(sql: &str) -> Vec<Token> {
    let b: Vec<char> = sql.chars().collect();
    let mut out = vec![];
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let next = b.get(i + 1).copied();
        match c {
            _ if c.is_whitespace() => i += 1,
            '#' => {
                while i < b.len() && b[i] != '\n' {
                    i += 1;
                }
            }
            '-' if next == Some('-') && b.get(i + 2).is_none_or(|c| c.is_whitespace()) => {
                while i < b.len() && b[i] != '\n' {
                    i += 1;
                }
            }
            '/' if next == Some('*') => {
                let executable = b.get(i + 2) == Some(&'!')
                    || (b.get(i + 2) == Some(&'M') && b.get(i + 3) == Some(&'!'));
                if executable {
                    // Skip the marker and its version digits; the closing `*/` is skipped below.
                    i += if b[i + 2] == '!' { 3 } else { 4 };
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                } else {
                    i += 2;
                    while i < b.len() && !(b[i] == '*' && b.get(i + 1) == Some(&'/')) {
                        i += 1;
                    }
                    i += 2;
                }
            }
            '*' if next == Some('/') => i += 2,
            '`' => {
                let mut name = String::new();
                i += 1;
                while i < b.len() {
                    if b[i] == '`' {
                        if b.get(i + 1) == Some(&'`') {
                            name.push('`');
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    name.push(b[i]);
                    i += 1;
                }
                i += 1;
                out.push(Token::Quoted(name));
            }
            '\'' | '"' => {
                i += 1;
                while i < b.len() {
                    match b[i] {
                        '\\' => i += 2,
                        q if q == c && b.get(i + 1) == Some(&c) => i += 2,
                        q if q == c => break,
                        _ => i += 1,
                    }
                }
                i += 1;
                out.push(Token::Str);
            }
            _ if c.is_alphanumeric() || c == '_' || c == '$' => {
                let start = i;
                while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == '$') {
                    i += 1;
                }
                out.push(Token::Word(b[start..i].iter().collect()));
            }
            _ => {
                out.push(Token::Punct(c));
                i += 1;
            }
        }
    }
    out
}

/// A cursor over the tokens.
struct Words<'a> {
    t: &'a [Token],
    i: usize,
    db: &'a str,
}

impl Words<'_> {
    /// The next token is the keyword `k` (case-insensitive): consumes it.
    fn eat(&mut self, k: &str) -> bool {
        match self.t.get(self.i) {
            Some(Token::Word(w)) if w.eq_ignore_ascii_case(k) => {
                self.i += 1;
                true
            }
            _ => false,
        }
    }

    fn eat_all(&mut self, ks: &[&str]) -> bool {
        let at = self.i;
        if ks.iter().all(|k| self.eat(k)) {
            return true;
        }
        self.i = at;
        false
    }

    fn punct(&mut self, p: char) -> bool {
        if self.t.get(self.i) == Some(&Token::Punct(p)) {
            self.i += 1;
            return true;
        }
        false
    }

    fn ident(&mut self) -> Option<String> {
        let name = match self.t.get(self.i)? {
            Token::Word(w) | Token::Quoted(w) => w.clone(),
            _ => return None,
        };
        self.i += 1;
        Some(name)
    }

    /// `name` or `db.name`, qualified with the default database.
    fn table(&mut self) -> Option<TableRef> {
        let first = self.ident()?;
        if self.punct('.') {
            let name = self.ident()?;
            return Some(TableRef::new(first, name));
        }
        Some(TableRef::new(self.db, first))
    }

    /// `name {, name}`.
    fn tables(&mut self) -> Option<Vec<TableRef>> {
        let mut out = vec![self.table()?];
        while self.punct(',') {
            out.push(self.table()?);
        }
        Some(out)
    }

    /// Skips to the keyword `k` at any depth; consumes it.
    fn find(&mut self, k: &str) -> bool {
        while self.i < self.t.len() {
            if self.eat(k) {
                return true;
            }
            self.i += 1;
        }
        false
    }

    fn rest(&self) -> &[Token] {
        &self.t[self.i..]
    }
}

fn is_word(t: &Token, k: &str) -> bool {
    matches!(t, Token::Word(w) if w.eq_ignore_ascii_case(k))
}

/// Partition and tablespace operations that remove or replace rows without row events, as the
/// adjacent words that start them.
const UNLOGGED: [(&str, &str); 9] = [
    ("DROP", "PARTITION"),
    ("TRUNCATE", "PARTITION"),
    ("EXCHANGE", "PARTITION"),
    ("IMPORT", "PARTITION"),
    ("IMPORT", "TABLESPACE"),
    ("DISCARD", "PARTITION"),
    ("DISCARD", "TABLESPACE"),
    ("CONVERT", "PARTITION"),
    ("CONVERT", "TABLE"),
];

fn unlogged(rest: &[Token]) -> Option<String> {
    rest.windows(2).find_map(|w| {
        UNLOGGED
            .iter()
            .find(|(a, b)| is_word(&w[0], a) && is_word(&w[1], b))
            .map(|(a, b)| format!("{a} {b}"))
    })
}

/// `RENAME [TO | AS] name` among an `ALTER TABLE`'s operations (not `RENAME COLUMN`, `INDEX`
/// or `KEY`).
fn renamed_to(w: &mut Words<'_>) -> Option<TableRef> {
    while w.find("RENAME") {
        if w.eat("COLUMN") || w.eat("INDEX") || w.eat("KEY") {
            continue;
        }
        let _ = w.eat("TO") || w.eat("AS");
        return w.table();
    }
    None
}

/// Tier 2: the verb and the tables.
fn read(sql: &str, db: &str) -> Ddl {
    let t = tokens(sql);
    let mut w = Words { t: &t, i: 0, db };
    let mut d = Ddl::default();
    let unreadable = |mut d: Ddl| {
        d.unreadable = true;
        d
    };
    if ["INSERT", "UPDATE", "DELETE", "REPLACE", "LOAD", "WITH"]
        .iter()
        .any(|k| w.eat(k))
    {
        let named = t
            .windows(2)
            .filter_map(|p| match p {
                [Token::Word(n) | Token::Quoted(n), Token::Punct('.')] => Some(n.clone()),
                _ => None,
            })
            .collect();
        d.logged_as_statement = Some(named);
        return d;
    }
    if w.eat("ALTER") {
        let _ = w.eat("ONLINE") || w.eat("OFFLINE");
        let _ = w.eat("IGNORE");
        if !w.eat("TABLE") {
            return d;
        }
        let _ = w.eat_all(&["IF", "EXISTS"]);
        let Some(table) = w.table() else {
            return unreadable(d);
        };
        if let Some(op) = unlogged(w.rest()) {
            d.unlogged.push((table.clone(), op.clone()));
            // The other table of an exchange or a conversion changes rows without events too.
            let before = match op.as_str() {
                "EXCHANGE PARTITION" => Some("WITH"),
                "CONVERT TABLE" => Some("CONVERT"),
                _ => None,
            };
            if let Some(k) = before {
                let mut x = Words {
                    t: &t,
                    i: w.i,
                    db: w.db,
                };
                let found = x.find(k) && x.eat("TABLE");
                match found.then(|| x.table()).flatten() {
                    Some(other) => d.unlogged.push((other, op)),
                    None => return unreadable(d),
                }
            }
        }
        let mut r = Words {
            t: &t,
            i: w.i,
            db: &table.schema,
        };
        if let Some(to) = renamed_to(&mut r) {
            d.renamed.push((table.clone(), to));
        }
        d.altered.push(Altered {
            table,
            scope: Scope::Table,
            renames: vec![],
        });
    } else if w.eat("CREATE") {
        let replace = w.eat_all(&["OR", "REPLACE"]);
        let temporary = w.eat("TEMPORARY");
        if w.eat("TABLE") {
            let _ = w.eat_all(&["IF", "NOT", "EXISTS"]);
            match w.table() {
                Some(t) if replace && !temporary => d.dropped.push(t),
                Some(_) => {}
                None => return unreadable(d),
            }
        } else if w.eat("DATABASE") || w.eat("SCHEMA") {
            let _ = w.eat_all(&["IF", "NOT", "EXISTS"]);
            if replace {
                match w.ident() {
                    Some(name) => d.dropped_database = Some(name),
                    None => return unreadable(d),
                }
            }
        } else {
            let _ = w.eat("ONLINE") || w.eat("OFFLINE");
            let _ = w.eat("UNIQUE") || w.eat("FULLTEXT") || w.eat("SPATIAL");
            if w.eat("INDEX") {
                if !w.find("ON") {
                    return unreadable(d);
                }
                let Some(table) = w.table() else {
                    return unreadable(d);
                };
                d.altered.push(Altered {
                    table,
                    scope: Scope::Columns(vec![]),
                    renames: vec![],
                });
            }
        }
    } else if w.eat("DROP") {
        let temporary = w.eat("TEMPORARY");
        if w.eat("TABLE") || w.eat("TABLES") {
            let _ = w.eat_all(&["IF", "EXISTS"]);
            match w.tables() {
                Some(tables) if !temporary => d.dropped = tables,
                Some(_) => {}
                None => return unreadable(d),
            }
        } else if w.eat("DATABASE") || w.eat("SCHEMA") {
            let _ = w.eat_all(&["IF", "EXISTS"]);
            match w.ident() {
                Some(name) => d.dropped_database = Some(name),
                None => return unreadable(d),
            }
        } else {
            let _ = w.eat("ONLINE") || w.eat("OFFLINE");
            if w.eat("INDEX") {
                if !w.find("ON") {
                    return unreadable(d);
                }
                let Some(table) = w.table() else {
                    return unreadable(d);
                };
                d.altered.push(Altered {
                    table,
                    scope: Scope::Columns(vec![]),
                    renames: vec![],
                });
            }
        }
    } else if w.eat("RENAME") {
        if !(w.eat("TABLE") || w.eat("TABLES")) {
            return d;
        }
        loop {
            let (Some(from), true, Some(to)) = (w.table(), w.eat("TO"), w.table()) else {
                return unreadable(d);
            };
            d.renamed.push((from, to));
            if !w.punct(',') {
                break;
            }
        }
    } else if w.eat("TRUNCATE") {
        let _ = w.eat("TABLE");
        match w.table() {
            Some(t) => d.truncated.push(t),
            None => return unreadable(d),
        }
    }
    d
}

/// Tier 1: `table`'s change as an `ALTER TABLE`'s operations say it; `None` when `sqlparser`
/// can't read them all.
fn operations(sql: &str, table: &TableRef) -> Option<Altered> {
    // `sqlparser` skips an executable comment, whose text the server runs.
    if sql.contains("/*!") || sql.contains("/*M!") {
        return None;
    }
    let stmts = Parser::parse_sql(&MySqlDialect {}, sql).ok()?;
    let [Statement::AlterTable(a)] = stmts.as_slice() else {
        return None;
    };
    let mut columns: Vec<String> = vec![];
    let mut renames = vec![];
    for op in &a.operations {
        use AlterTableOperation as O;
        match op {
            O::AddColumn { column_def, .. } => columns.push(column_def.name.value.clone()),
            O::DropColumn { column_names, .. } => {
                columns.extend(column_names.iter().map(|c| c.value.clone()))
            }
            O::ModifyColumn { col_name, .. } => columns.push(col_name.value.clone()),
            O::AlterColumn { column_name, .. } => columns.push(column_name.value.clone()),
            O::ChangeColumn {
                old_name, new_name, ..
            }
            | O::RenameColumn {
                old_column_name: old_name,
                new_column_name: new_name,
            } => {
                columns.extend([old_name.value.clone(), new_name.value.clone()]);
                if old_name.value != new_name.value {
                    renames.push((old_name.value.clone(), new_name.value.clone()));
                }
            }
            // Keys, indexes, constraints and options change no column; channel C reads the
            // identifier on every signal.
            O::AddConstraint { .. }
            | O::DropConstraint { .. }
            | O::DropPrimaryKey { .. }
            | O::DropForeignKey { .. }
            | O::DropIndex { .. }
            | O::RenameTable { .. }
            | O::Algorithm { .. }
            | O::Lock { .. }
            | O::AutoIncrement { .. } => {}
            _ => return None,
        }
    }
    Some(Altered {
        table: table.clone(),
        scope: Scope::Columns(columns),
        renames,
    })
}

/// Classifies one statement; `db` is the event's default database (`QUERY_EVENT.db`).
pub fn classify(sql: &str, db: &str) -> Ddl {
    let mut d = read(sql, db);
    if let [a] = d.altered.as_mut_slice()
        && a.scope == Scope::Table
        && let Some(read) = operations(sql, &a.table)
    {
        *a = read;
    }
    d
}

/// What became of a tracked table after a statement's renames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Renamed {
    /// The name holds the table it held.
    Kept,
    /// An online schema change cut-over moved the shadow table `from` into the name (gh-ost
    /// `_<t>_gho`, pt-online-schema-change `_<t>_new`): the stream follows it.
    Swapped { from: TableRef },
    /// The table left the name, or another table took it: the table halts (`TABLE_DROPPED`).
    Gone,
}

/// Whether `shadow` is an online schema change tool's shadow table of `t`: gh-ost `_t_gho`,
/// pt-online-schema-change `_t_new` (with more leading underscores when that name is taken).
pub fn shadow_of(shadow: &TableRef, t: &TableRef) -> bool {
    if shadow.schema != t.schema {
        return false;
    }
    let Some(rest) = shadow.name.strip_prefix('_') else {
        return false;
    };
    let rest = rest.trim_start_matches('_');
    [format!("{}_gho", t.name), format!("{}_new", t.name)].contains(&rest.to_owned())
}

/// Applies `renames` in order and says what each `tracked` table's name holds afterwards.
pub fn renamed(renames: &[(TableRef, TableRef)], tracked: &[TableRef]) -> Vec<(TableRef, Renamed)> {
    // Each name's occupant: the table that held the name before the statement.
    let mut at: Vec<(TableRef, TableRef)> = vec![];
    let occupant = |at: &[(TableRef, TableRef)], n: &TableRef| {
        at.iter()
            .find(|(name, _)| name == n)
            .map(|(_, o)| o.clone())
    };
    for (from, to) in renames {
        let moving = occupant(&at, from).unwrap_or_else(|| from.clone());
        at.retain(|(name, _)| name != from && name != to);
        // `from` is empty now unless something moves into it later.
        at.push((from.clone(), TableRef::new("", "")));
        at.push((to.clone(), moving));
    }
    tracked
        .iter()
        .filter_map(|t| {
            let o = occupant(&at, t)?;
            let outcome = if &o == t {
                Renamed::Kept
            } else if !o.name.is_empty() && !tracked.contains(&o) && shadow_of(&o, t) {
                Renamed::Swapped { from: o }
            } else {
                Renamed::Gone
            };
            Some((t.clone(), outcome))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(name: &str) -> TableRef {
        TableRef::new("shop", name)
    }

    fn c(sql: &str) -> Ddl {
        classify(sql, "shop")
    }

    fn altered(sql: &str) -> Altered {
        let d = c(sql);
        assert_eq!(d.altered.len(), 1, "{sql}: {d:?}");
        d.altered[0].clone()
    }

    fn columns(names: &[&str]) -> Scope {
        Scope::Columns(names.iter().map(|n| n.to_string()).collect())
    }

    #[test]
    fn alter_operations_scope_the_columns_and_prove_renames() {
        let a = altered(
            "ALTER TABLE `shop`.`orders` ADD COLUMN `x` INT NOT NULL DEFAULT 0 AFTER id, DROP COLUMN y",
        );
        assert_eq!(a.table, t("orders"));
        assert_eq!(a.scope, columns(&["x", "y"]));
        assert_eq!(
            altered("ALTER TABLE orders CHANGE COLUMN a b BIGINT").renames,
            [("a".into(), "b".into())]
        );
        assert_eq!(
            altered("alter table orders rename column a to b").renames,
            [("a".into(), "b".into())]
        );
        let modify = altered("ALTER TABLE orders CHANGE a a VARCHAR(10)");
        assert_eq!(
            (modify.scope, modify.renames),
            (columns(&["a", "a"]), vec![])
        );
        assert_eq!(
            altered("ALTER TABLE orders MODIFY COLUMN a BIGINT UNSIGNED").scope,
            columns(&["a"])
        );
        assert_eq!(
            altered("ALTER TABLE orders ADD INDEX i (a), ALGORITHM=INSTANT").scope,
            columns(&[])
        );
        assert_eq!(
            altered("ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)").scope,
            columns(&[])
        );
        assert_eq!(
            altered("ALTER TABLE orders ADD COLUMN `weird name` INT, ADD COLUMN `ba``ck` INT")
                .scope,
            columns(&["weird name", "ba`ck"])
        );
    }

    #[test]
    fn what_sqlparser_cant_read_scopes_the_whole_table() {
        for sql in [
            "ALTER TABLE orders ENGINE=InnoDB",
            "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4",
            "ALTER TABLE orders ADD COLUMN (p INT, q INT)",
            "ALTER ONLINE TABLE orders ADD COLUMN x INT",
            "ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS x INT",
            "ALTER TABLE orders ADD COLUMN x INT /*!80023 INVISIBLE */",
        ] {
            let a = altered(sql);
            assert_eq!(
                (a.table, a.scope, a.renames),
                (t("orders"), Scope::Table, vec![]),
                "{sql}"
            );
        }
    }

    #[test]
    fn a_deeply_nested_statement_stops_at_the_recursion_limit() {
        let deep = format!(
            "ALTER TABLE orders ADD COLUMN x INT DEFAULT ({}1{})",
            "(".repeat(10_000),
            ")".repeat(10_000)
        );
        assert_eq!(altered(&deep).scope, Scope::Table);
    }

    #[test]
    fn table_statements_name_their_tables() {
        assert_eq!(
            c("DROP TABLE `orders` /* generated by server */").dropped,
            [t("orders")]
        );
        assert_eq!(
            c("DROP TABLE IF EXISTS `a`,`other`.`b` /* generated by server */").dropped,
            [t("a"), TableRef::new("other", "b")]
        );
        assert_eq!(c("DROP TEMPORARY TABLE IF EXISTS orders"), Ddl::default());
        assert_eq!(c("TRUNCATE TABLE orders").truncated, [t("orders")]);
        assert_eq!(c("truncate orders").truncated, [t("orders")]);
        assert_eq!(
            c("RENAME TABLE a TO b, other.c TO d").renamed,
            [(t("a"), t("b")), (TableRef::new("other", "c"), t("d"))]
        );
        assert_eq!(
            c("ALTER TABLE orders RENAME TO archive").renamed,
            [(t("orders"), t("archive"))]
        );
        assert_eq!(c("ALTER TABLE orders RENAME COLUMN a TO b").renamed, []);
        assert_eq!(
            c("CREATE INDEX i ON orders (a)").altered[0].table,
            t("orders")
        );
        assert_eq!(
            c("DROP INDEX `PRIMARY` ON orders").altered[0].scope,
            columns(&[])
        );
        assert_eq!(
            c("CREATE OR REPLACE TABLE orders (id INT PRIMARY KEY)").dropped,
            [t("orders")]
        );
        assert_eq!(
            c("CREATE TABLE IF NOT EXISTS orders LIKE x"),
            Ddl::default()
        );
        assert_eq!(
            c("DROP DATABASE IF EXISTS `shop`")
                .dropped_database
                .as_deref(),
            Some("shop")
        );
        assert_eq!(
            c("CREATE OR REPLACE DATABASE shop")
                .dropped_database
                .as_deref(),
            Some("shop")
        );
        assert!(c("DROP DATABASE \"shop\"").unreadable);
        assert!(c("CREATE OR REPLACE DATABASE \"shop\"").unreadable);
    }

    #[test]
    fn statements_on_no_table_route_nowhere() {
        for sql in [
            "BEGIN",
            "GRANT SELECT ON *.* TO 'u'@'%'",
            "CREATE DATABASE x",
            "CREATE DEFINER=`root`@`%` TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1",
            "CREATE VIEW v AS SELECT 1",
            "OPTIMIZE TABLE orders",
            "ANALYZE TABLE orders",
            "ALTER DATABASE shop CHARACTER SET utf8mb4",
            "DROP VIEW v",
            "FLUSH PRIVILEGES",
        ] {
            assert_eq!(c(sql), Ddl::default(), "{sql}");
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "/* app */ update t SET a = 1",
            "DELETE t, u FROM t JOIN u USING (id)",
            "REPLACE INTO t VALUES (1)",
            "LOAD DATA INFILE 'x' INTO TABLE t",
            "WITH x AS (SELECT 1) DELETE FROM t",
        ] {
            assert_eq!(c(sql).logged_as_statement, Some(vec![]), "{sql}");
        }
        assert_eq!(
            c("INSERT INTO `shop`.t SELECT * FROM other.u /* x.y */").logged_as_statement,
            Some(vec!["shop".into(), "other".into()])
        );
        assert_eq!(c("CREATE TABLE t (id INT)").logged_as_statement, None);
    }

    #[test]
    fn a_table_statement_without_a_name_is_unreadable() {
        for sql in [
            "ALTER TABLE",
            "TRUNCATE TABLE",
            "RENAME TABLE a TO",
            "DROP TABLE ,",
        ] {
            assert!(c(sql).unreadable, "{sql}");
        }
    }

    #[test]
    fn partition_operations_that_move_rows_are_unlogged() {
        for (sql, op) in [
            ("ALTER TABLE orders DROP PARTITION p0", "DROP PARTITION"),
            (
                "ALTER TABLE orders TRUNCATE PARTITION p0, p1",
                "TRUNCATE PARTITION",
            ),
            (
                "ALTER TABLE orders DISCARD TABLESPACE",
                "DISCARD TABLESPACE",
            ),
            (
                "ALTER TABLE orders IMPORT PARTITION p0 TABLESPACE",
                "IMPORT PARTITION",
            ),
            (
                "ALTER TABLE orders CONVERT PARTITION p0 TO TABLE x",
                "CONVERT PARTITION",
            ),
        ] {
            assert_eq!(c(sql).unlogged, [(t("orders"), op.to_owned())], "{sql}");
        }
        assert_eq!(
            c("ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE x").unlogged,
            [
                (t("orders"), "EXCHANGE PARTITION".to_owned()),
                (t("x"), "EXCHANGE PARTITION".to_owned())
            ]
        );
        assert_eq!(
            c("ALTER TABLE orders CONVERT TABLE x TO PARTITION p1 VALUES LESS THAN (10)").unlogged,
            [
                (t("orders"), "CONVERT TABLE".to_owned()),
                (t("x"), "CONVERT TABLE".to_owned())
            ]
        );
        assert_eq!(
            c("ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE other.x WITHOUT VALIDATION")
                .unlogged,
            [
                (t("orders"), "EXCHANGE PARTITION".to_owned()),
                (TableRef::new("other", "x"), "EXCHANGE PARTITION".to_owned())
            ]
        );
        for sql in [
            "ALTER TABLE orders ADD PARTITION (PARTITION p9 VALUES LESS THAN (99))",
            "ALTER TABLE orders PARTITION BY HASH(id) PARTITIONS 4",
            "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4",
            "ALTER TABLE orders ADD COLUMN `drop` INT, ADD COLUMN `partition` INT",
            "ALTER TABLE orders COMMENT 'drop partition'",
        ] {
            assert_eq!(c(sql).unlogged, [], "{sql}");
        }
    }

    #[test]
    fn comments_and_strings_hide_their_words() {
        assert_eq!(
            tokens("/* DROP TABLE x */ -- RENAME\n# ALTER\nTRUNCATE 'a''b' \"c\\\"d\" `e`"),
            [
                Token::Word("TRUNCATE".into()),
                Token::Str,
                Token::Str,
                Token::Quoted("e".into())
            ]
        );
        assert_eq!(
            tokens("ALTER /*!50100 TABLE */ t"),
            [
                Token::Word("ALTER".into()),
                Token::Word("TABLE".into()),
                Token::Word("t".into())
            ]
        );
        assert_eq!(c("/*M!100100 DROP TABLE */ orders").dropped, [t("orders")]);
    }

    #[test]
    fn a_cut_over_swap_follows_the_shadow_table() {
        let tracked = [t("orders"), t("items")];
        let swap = |sql: &str| renamed(&c(sql).renamed, &tracked);
        assert_eq!(
            swap("RENAME TABLE orders TO _orders_del, _orders_gho TO orders"),
            [(
                t("orders"),
                Renamed::Swapped {
                    from: t("_orders_gho")
                }
            )]
        );
        assert_eq!(
            swap("RENAME TABLE `orders` TO `_orders_old`, `__orders_new` TO `orders`"),
            [(
                t("orders"),
                Renamed::Swapped {
                    from: t("__orders_new")
                }
            )]
        );
        // Another table into the name, a table away, two tracked tables swapped.
        assert_eq!(
            swap("RENAME TABLE orders TO orders_old, archive TO orders"),
            [(t("orders"), Renamed::Gone)]
        );
        assert_eq!(
            swap("RENAME TABLE orders TO o2"),
            [(t("orders"), Renamed::Gone)]
        );
        assert_eq!(
            swap("RENAME TABLE orders TO tmp, items TO orders, tmp TO items"),
            [(t("orders"), Renamed::Gone), (t("items"), Renamed::Gone)]
        );
        // Away and back is the same table; an untracked rename touches nothing.
        assert_eq!(
            swap("RENAME TABLE orders TO tmp, tmp TO orders"),
            [(t("orders"), Renamed::Kept)]
        );
        assert_eq!(swap("RENAME TABLE a TO b"), []);
        // The shadow of another table, or in another database, isn't followed.
        assert_eq!(
            swap("RENAME TABLE orders TO x, _items_gho TO orders"),
            [(t("orders"), Renamed::Gone)]
        );
        assert!(!shadow_of(
            &TableRef::new("other", "_orders_gho"),
            &t("orders")
        ));
    }
}
