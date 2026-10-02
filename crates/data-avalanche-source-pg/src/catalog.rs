//! Catalog reads (`Schema_Change…` §3 channels A, C, D; config §11.2): which tables the globs
//! select, their columns, identifier and replica identity, and the `pg_type` rows the planner needs.

use std::collections::{BTreeMap, HashMap};

use data_avalanche_config::{PartitionTransform, Tables};
use data_avalanche_core::{ColumnSchema, HaltReason, LogicalType, TableRef, TableSchema};
use tokio_postgres::Client;

use crate::types::{TypeCatalog, TypeInfo, missing, plan_column};

/// `*` matches within one name (it doesn't cross the `.`); matching is case-sensitive
/// (config §9.3 glob grammar).
pub fn glob_match(pattern: &str, name: &str) -> bool {
    fn go(p: &[u8], n: &[u8]) -> bool {
        match p.split_first() {
            None => n.is_empty(),
            Some((b'*', rest)) => (0..=n.len()).any(|i| go(rest, &n[i..])),
            Some((c, rest)) => n.first() == Some(c) && go(rest, &n[1..]),
        }
    }
    go(pattern.as_bytes(), name.as_bytes())
}

/// A PostgreSQL include/exclude pattern (`schema.table`, V-117) against a table.
pub fn pattern_matches(pattern: &str, table: &TableRef) -> bool {
    match pattern.split_once('.') {
        Some((s, t)) => glob_match(s, &table.schema) && glob_match(t, &table.name),
        None => false,
    }
}

/// A `pg_class` row the globs can select.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub table: TableRef,
    /// `r` table (including a leaf partition), `p` partitioned table.
    pub kind: u8,
    /// `relpersistence = 'p'`. PostgreSQL doesn't replicate an unlogged table.
    pub logged: bool,
}

/// What the globs selected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selection {
    /// Tables to describe; `true` when a pattern without `*` named the table.
    pub tables: Vec<(Relation, bool)>,
    pub skipped: Vec<(TableRef, String)>,
    /// Named tables the source can't replicate.
    pub halted: Vec<(TableRef, HaltReason)>,
    pub warnings: Vec<String>,
}

/// Applies `include` minus `exclude` to the catalog's tables. A partitioned parent isn't a table
/// of its own (Q-M5-5): it is skipped with a warning naming its partitions as the way to track
/// it. An unlogged table isn't replicated by PostgreSQL: skipped with a warning when a glob found
/// it, halted (`TABLE_NOT_REPLICABLE`) when its name was listed. An include pattern that matches
/// nothing warns.
pub fn select(include: &[String], exclude: &[String], relations: &[Relation]) -> Selection {
    let mut out = Selection::default();
    for p in include {
        if !relations.iter().any(|r| pattern_matches(p, &r.table)) {
            out.warnings
                .push(format!("tables.include {p} matches no table"));
        }
    }
    for r in relations {
        let named = include
            .iter()
            .filter(|p| pattern_matches(p, &r.table))
            .map(|p| !p.contains('*'))
            .reduce(|a, b| a || b);
        let Some(explicit) = named else { continue };
        if exclude.iter().any(|p| pattern_matches(p, &r.table)) {
            continue;
        }
        if r.kind == b'p' {
            out.skipped.push((
                r.table.clone(),
                "a partitioned table: its changes arrive under its partitions; include those"
                    .into(),
            ));
            continue;
        }
        if !r.logged {
            let why = UNLOGGED;
            if explicit {
                out.halted.push((
                    r.table.clone(),
                    HaltReason::NotReplicable { reason: why.into() },
                ));
            } else {
                out.skipped.push((r.table.clone(), why.into()));
            }
            continue;
        }
        out.tables.push((r.clone(), explicit));
    }
    out
}

/// One live, stored column (`attnum > 0`, not dropped, not generated: generated columns aren't in
/// the stream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub attnum: i16,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub not_null: bool,
    /// `format_type(atttypid, atttypmod)`.
    pub type_text: String,
    /// The column's collation when it is nondeterministic: a snapshot orders such a key column
    /// by `"C"` (Snapshot_Resumability §2).
    pub nondeterministic: Option<String>,
}

/// Why an unlogged table isn't replicated.
const UNLOGGED: &str =
    "an unlogged table: PostgreSQL doesn't replicate it (ALTER TABLE … SET LOGGED)";

/// A table as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Described {
    pub oid: u32,
    pub table: TableRef,
    /// `relreplident`: `d` default (the primary key), `n` nothing, `f` full, `i` index.
    pub replica_identity: u8,
    /// `relpersistence = 'p'`: an unlogged table isn't replicated, so it doesn't plan.
    pub logged: bool,
    pub columns: Vec<Attribute>,
    pub primary_key: Vec<String>,
    /// The columns of the `REPLICA IDENTITY USING INDEX` index.
    pub identity_index: Vec<String>,
}

/// The table's config keys: `schema.table` (config §9.3).
pub fn table_key(t: &TableRef) -> String {
    format!("{}.{}", t.schema, t.name)
}

/// What building a schema noticed that isn't a halt.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Notes {
    /// Columns of types kept as text with a warning: `(column, type)`.
    pub read_as_text: Vec<(String, String)>,
    /// `numeric` columns without a declared precision (V-079).
    pub unconstrained: Vec<String>,
    /// Identifier columns in a nondeterministic collation: `(column, collation)`. Snapshot chunks
    /// order them by `"C"`, which their index doesn't serve.
    pub c_ordered_keys: Vec<(String, String)>,
}

/// The replica identity's columns.
fn identity_columns(d: &Described) -> Vec<&str> {
    match d.replica_identity {
        b'f' => d.columns.iter().map(|c| c.name.as_str()).collect(),
        b'i' => d.identity_index.iter().map(String::as_str).collect(),
        b'd' => d.primary_key.iter().map(String::as_str).collect(),
        _ => vec![],
    }
}

/// A table's schema, checked in this order (config §11.2): every column plans
/// ([`plan_column`]: overrides, numeric caps); the identifier (the `tables.primary_key` entry,
/// else the primary key) passes [`TableSchema::new`] (reserved names, a non-empty identifier of
/// usable columns); the identifier is inside the REPLICA IDENTITY; partition columns exist and
/// temporal transforms name temporal columns.
pub fn table_schema(
    d: &Described,
    tables: &Tables,
    types: &TypeCatalog,
) -> Result<(TableSchema, Notes), HaltReason> {
    if !d.logged {
        return Err(HaltReason::NotReplicable {
            reason: UNLOGGED.into(),
        });
    }
    let key = table_key(&d.table);
    let mut notes = Notes::default();
    let mut columns = Vec::with_capacity(d.columns.len());
    for a in &d.columns {
        let over = tables
            .column_type_overrides
            .get(&format!("{key}.{}", a.name));
        let p = plan_column(
            &a.name,
            a.type_oid,
            a.typmod,
            types,
            tables.numeric_overflow_action,
            over,
        )?;
        if let Some(t) = p.read_as_text {
            notes.read_as_text.push((a.name.clone(), t));
        }
        if p.unconstrained_numeric {
            notes.unconstrained.push(a.name.clone());
        }
        columns.push(ColumnSchema {
            name: a.name.clone(),
            value_type: p.value_type,
            logical: p.logical,
            nullable: !a.not_null,
            source_type: a.type_text.clone(),
            source_ordinal: Some(i32::from(a.attnum)),
        });
    }
    let identifier = tables
        .primary_key
        .get(&key)
        .cloned()
        .unwrap_or_else(|| d.primary_key.clone());
    let schema = TableSchema::new(d.table.clone(), columns, identifier)?;
    let identity = identity_columns(d);
    let missing: Vec<String> = schema
        .identifier()
        .iter()
        .filter(|c| !identity.contains(&c.as_str()))
        .cloned()
        .collect();
    if !missing.is_empty() {
        return Err(HaltReason::IdentifierNotInReplicaIdentity { missing });
    }
    for k in schema.identifier() {
        if let Some(collation) = d
            .columns
            .iter()
            .find(|a| &a.name == k)
            .and_then(|a| a.nondeterministic.clone())
        {
            notes.c_ordered_keys.push((k.clone(), collation));
        }
    }
    for f in tables.partitioning.get(&key).into_iter().flatten() {
        let temporal = !matches!(
            f.transform,
            PartitionTransform::Identity
                | PartitionTransform::Bucket { .. }
                | PartitionTransform::Truncate { .. }
        );
        if f.column == "_ts" {
            continue;
        }
        let Some(c) = schema.column(&f.column) else {
            return Err(HaltReason::SchemaMismatch {
                column: f.column.clone(),
                detail: "tables.partitioning names a column the table doesn't have".into(),
            });
        };
        if temporal
            && !matches!(
                c.logical,
                LogicalType::Date | LogicalType::TimestampTz | LogicalType::TimestampNtz
            )
        {
            return Err(HaltReason::SchemaMismatch {
                column: f.column.clone(),
                detail: format!(
                    "a {:?} partition transform needs a date or timestamp column",
                    f.transform
                ),
            });
        }
    }
    Ok((schema, notes))
}

/// The tables the globs can select: plain and partitioned tables outside the system schemas.
pub async fn relations(client: &Client) -> Result<Vec<Relation>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT c.oid, n.nspname, c.relname, c.relkind::text, c.relpersistence = 'p'
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relkind IN ('r', 'p')
                AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
              ORDER BY n.nspname, c.relname",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| Relation {
            oid: r.get(0),
            table: TableRef::new(r.get::<_, String>(1), r.get::<_, String>(2)),
            kind: r.get::<_, String>(3).as_bytes()[0],
            logged: r.get(4),
        })
        .collect())
}

/// The OIDs of `tables` that still exist, by name.
pub async fn oids_by_name(
    client: &Client,
    tables: &[TableRef],
) -> Result<HashMap<TableRef, u32>, tokio_postgres::Error> {
    let schemas: Vec<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    let rows = client
        .query(
            "SELECT c.oid, n.nspname, c.relname
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               JOIN unnest($1::text[], $2::text[]) AS w(s, t) ON w.s = n.nspname AND w.t = c.relname
              WHERE c.relkind IN ('r', 'p')",
            &[&schemas, &names],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                TableRef::new(r.get::<_, String>(1), r.get::<_, String>(2)),
                r.get(0),
            )
        })
        .collect())
}

/// Describes the tables with these OIDs in three batched queries. An OID with no table is left
/// out (the table was dropped).
pub async fn describe(
    client: &Client,
    oids: &[u32],
) -> Result<BTreeMap<u32, Described>, tokio_postgres::Error> {
    let mut out: BTreeMap<u32, Described> = BTreeMap::new();
    for r in client
        .query(
            "SELECT c.oid, n.nspname, c.relname, c.relreplident::text, c.relpersistence = 'p'
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.oid = ANY($1)",
            &[&oids],
        )
        .await?
    {
        let oid: u32 = r.get(0);
        out.insert(
            oid,
            Described {
                oid,
                table: TableRef::new(r.get::<_, String>(1), r.get::<_, String>(2)),
                replica_identity: r.get::<_, String>(3).as_bytes()[0],
                logged: r.get(4),
                columns: vec![],
                primary_key: vec![],
                identity_index: vec![],
            },
        );
    }
    for r in client
        .query(
            "SELECT a.attrelid, a.attnum, a.attname, a.atttypid, a.atttypmod, a.attnotnull,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    CASE WHEN NOT co.collisdeterministic THEN co.collname::text END
               FROM pg_catalog.pg_attribute a
               LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
              WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped
                AND a.attgenerated = ''
              ORDER BY a.attrelid, a.attnum",
            &[&oids],
        )
        .await?
    {
        if let Some(d) = out.get_mut(&r.get::<_, u32>(0)) {
            d.columns.push(Attribute {
                attnum: r.get(1),
                name: r.get(2),
                type_oid: r.get(3),
                typmod: r.get(4),
                not_null: r.get(5),
                type_text: r.get(6),
                nondeterministic: r.get(7),
            });
        }
    }
    for r in client
        .query(
            "SELECT i.indrelid, i.indisprimary, i.indisreplident,
                    ARRAY(SELECT a.attname
                            FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                            JOIN pg_catalog.pg_attribute a
                              ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                           ORDER BY k.ord)
               FROM pg_catalog.pg_index i
              WHERE i.indrelid = ANY($1) AND (i.indisprimary OR i.indisreplident)",
            &[&oids],
        )
        .await?
    {
        if let Some(d) = out.get_mut(&r.get::<_, u32>(0)) {
            let columns: Vec<String> = r.get(3);
            if r.get::<_, bool>(1) {
                d.primary_key = columns.clone();
            }
            if r.get::<_, bool>(2) {
                d.identity_index = columns;
            }
        }
    }
    Ok(out)
}

/// Adds the `pg_type` rows of `oids` and of everything they refer to (array elements, domain
/// bases) to `types`. Only types the planner doesn't know by OID are fetched.
pub async fn load_types(
    client: &Client,
    oids: impl IntoIterator<Item = u32>,
    types: &mut TypeCatalog,
) -> Result<(), tokio_postgres::Error> {
    let mut wanted: Vec<u32> = oids.into_iter().filter_map(|o| missing(o, types)).collect();
    wanted.sort_unstable();
    wanted.dedup();
    if wanted.is_empty() {
        return Ok(());
    }
    for r in client
        .query(
            "WITH RECURSIVE t(oid) AS (
                 SELECT unnest($1::oid[])
                 UNION
                 SELECT v.x FROM pg_catalog.pg_type p JOIN t ON p.oid = t.oid,
                        LATERAL (VALUES (p.typelem), (p.typbasetype)) AS v(x)
                  WHERE v.x <> 0)
             SELECT p.oid, p.typname, n.nspname, p.typtype::text, p.typcategory::text,
                    p.typelem, p.typbasetype, p.typtypmod, p.typdelim::text
               FROM pg_catalog.pg_type p
               JOIN pg_catalog.pg_namespace n ON n.oid = p.typnamespace
              WHERE p.oid IN (SELECT oid FROM t)",
            &[&wanted],
        )
        .await?
    {
        let info = TypeInfo {
            oid: r.get(0),
            name: r.get(1),
            schema: r.get(2),
            kind: r.get::<_, String>(3).as_bytes()[0],
            category: r.get::<_, String>(4).as_bytes()[0],
            element: r.get(5),
            base: r.get(6),
            base_typmod: r.get(7),
            delimiter: r.get::<_, String>(8).as_bytes()[0],
        };
        types.insert(info.oid, info);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::pipeline_with;
    use data_avalanche_core::ColumnValueType as V;

    fn rel(oid: u32, s: &str, t: &str, kind: u8) -> Relation {
        Relation {
            oid,
            table: TableRef::new(s, t),
            kind,
            logged: true,
        }
    }

    #[test]
    fn globs_stay_inside_a_name() {
        assert!(glob_match("*", "orders"));
        assert!(glob_match("ord*", "orders"));
        assert!(glob_match("*ers", "orders"));
        assert!(glob_match("o*r*s", "orders"));
        assert!(!glob_match("Orders", "orders"), "case-sensitive");
        assert!(!glob_match("ord", "orders"));
        let t = TableRef::new("public", "orders");
        assert!(pattern_matches("public.*", &t));
        assert!(pattern_matches("*.orders", &t));
        assert!(!pattern_matches("*", &t), "a pattern needs its schema part");
        assert!(
            !pattern_matches("pub*orders", &t),
            "`*` doesn't cross the dot"
        );
    }

    #[test]
    fn selection() {
        let relations = [
            rel(1, "public", "orders", b'r'),
            rel(2, "public", "order_items", b'r'),
            rel(3, "public", "events", b'p'),
            rel(4, "public", "events_2024", b'r'),
            rel(5, "audit", "orders", b'r'),
            rel(6, "public", "tmp_x", b'r'),
        ];
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let sel = select(
            &s(&["public.*", "audit.orders", "public.missing"]),
            &s(&["public.tmp_*"]),
            &relations,
        );
        let got: Vec<(u32, bool)> = sel.tables.iter().map(|(r, e)| (r.oid, *e)).collect();
        assert_eq!(got, vec![(1, false), (2, false), (4, false), (5, true)]);
        assert_eq!(sel.skipped.len(), 1);
        assert_eq!(sel.skipped[0].0, TableRef::new("public", "events"));
        assert_eq!(
            sel.warnings,
            vec!["tables.include public.missing matches no table"]
        );
        let explicit = select(&s(&["public.*", "public.orders"]), &[], &relations[..1]);
        assert!(explicit.tables[0].1, "an exact name wins over a glob");
    }

    #[test]
    fn unlogged_tables_are_skipped_or_halted() {
        let unlogged = |oid, t| Relation {
            logged: false,
            ..rel(oid, "public", t, b'r')
        };
        let relations = [
            rel(1, "public", "orders", b'r'),
            unlogged(2, "cache"),
            unlogged(3, "cache_named"),
        ];
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let sel = select(&s(&["public.*", "public.cache_named"]), &[], &relations);
        let got: Vec<u32> = sel.tables.iter().map(|(r, _)| r.oid).collect();
        assert_eq!(got, [1]);
        assert_eq!(sel.skipped.len(), 1, "a glob-matched unlogged table");
        assert_eq!(sel.skipped[0].0, TableRef::new("public", "cache"));
        assert_eq!(sel.halted.len(), 1, "a named unlogged table");
        assert_eq!(sel.halted[0].0, TableRef::new("public", "cache_named"));
        assert_eq!(sel.halted[0].1.code(), "TABLE_NOT_REPLICABLE");
    }

    fn attr(
        attnum: i16,
        name: &str,
        type_oid: u32,
        typmod: i32,
        not_null: bool,
        text: &str,
    ) -> Attribute {
        Attribute {
            attnum,
            name: name.into(),
            type_oid,
            typmod,
            not_null,
            type_text: text.into(),
            nondeterministic: None,
        }
    }

    fn orders(replica_identity: u8) -> Described {
        Described {
            oid: 1,
            table: TableRef::new("public", "orders"),
            replica_identity,
            logged: true,
            columns: vec![
                attr(1, "id", 20, -1, true, "bigint"),
                attr(2, "code", 25, -1, true, "text"),
                attr(4, "amount", 1700, -1, false, "numeric"),
                attr(5, "at", 1184, -1, false, "timestamp with time zone"),
            ],
            primary_key: vec!["id".into()],
            identity_index: vec!["code".into()],
        }
    }

    fn schema_of(d: &Described, tables: &str) -> Result<(TableSchema, Notes), HaltReason> {
        table_schema(
            d,
            &pipeline_with("public.orders", tables).tables,
            &TypeCatalog::new(),
        )
    }

    #[test]
    fn a_table_schema_from_the_catalog() {
        let (s, notes) = schema_of(&orders(b'd'), "").unwrap();
        assert_eq!(s.identifier(), ["id"]);
        let names: Vec<&str> = s.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "code", "amount", "at"]);
        let at = s.column("at").unwrap();
        assert_eq!(at.value_type, V::Timestamp { with_tz: true });
        assert_eq!(at.source_ordinal, Some(5), "attnum, gaps and all");
        assert!(at.nullable);
        assert!(!s.column("id").unwrap().nullable);
        assert_eq!(
            s.column("amount").unwrap().value_type,
            V::Text,
            "bare numeric under `string`"
        );
        assert_eq!(notes.unconstrained, ["amount"]);
        assert!(notes.c_ordered_keys.is_empty());
    }

    #[test]
    fn an_unlogged_table_doesnt_plan() {
        let mut d = orders(b'd');
        d.logged = false;
        assert_eq!(
            schema_of(&d, "").unwrap_err().code(),
            "TABLE_NOT_REPLICABLE"
        );
    }

    #[test]
    fn keys_in_a_nondeterministic_collation_are_noted() {
        let mut d = orders(b'i');
        d.columns[1].nondeterministic = Some("ci".into());
        let over = "      primary_key: { public.orders: [code] }";
        let (_, notes) = schema_of(&d, over).unwrap();
        assert_eq!(notes.c_ordered_keys, [("code".into(), "ci".into())]);
        let mut full = orders(b'f');
        full.columns[1].nondeterministic = Some("ci".into());
        let (_, notes) = schema_of(&full, "").unwrap();
        assert!(notes.c_ordered_keys.is_empty(), "only identifier columns");
    }

    #[test]
    fn identifiers_and_replica_identity() {
        // The identifier must be inside the replica identity.
        assert_eq!(
            schema_of(&orders(b'n'), "").unwrap_err(),
            HaltReason::IdentifierNotInReplicaIdentity {
                missing: vec!["id".into()]
            }
        );
        assert!(
            schema_of(&orders(b'f'), "").is_ok(),
            "FULL holds every column"
        );
        // REPLICA IDENTITY USING INDEX on `code` and the override naming it.
        let over = "      primary_key: { public.orders: [code] }";
        assert_eq!(
            schema_of(&orders(b'i'), over).unwrap().0.identifier(),
            ["code"]
        );
        assert_eq!(
            schema_of(&orders(b'i'), "").unwrap_err(),
            HaltReason::IdentifierNotInReplicaIdentity {
                missing: vec!["id".into()]
            }
        );
        // No primary key, no override.
        let mut no_pk = orders(b'f');
        no_pk.primary_key.clear();
        assert_eq!(schema_of(&no_pk, "").unwrap_err(), HaltReason::NoIdentifier);
        // An override on a nullable column.
        let e = schema_of(
            &orders(b'f'),
            "      primary_key: { public.orders: [amount] }",
        )
        .unwrap_err();
        assert_eq!(e.code(), "IDENTIFIER_NOT_USABLE");
    }

    #[test]
    fn overrides_and_numeric_policy() {
        let t = "      column_type_overrides: { public.orders.amount: \"decimal(38,10)\", public.orders.code: string }";
        let (s, notes) = schema_of(&orders(b'd'), t).unwrap();
        assert_eq!(
            s.column("amount").unwrap().value_type,
            V::Numeric {
                precision: 38,
                scale: 10
            }
        );
        assert!(notes.unconstrained.is_empty());
        let e = schema_of(
            &orders(b'd'),
            "      column_type_overrides: { public.orders.at: int64 }",
        )
        .unwrap_err();
        assert_eq!(e.code(), "SCHEMA_MISMATCH");
        let (s, _) = schema_of(&orders(b'd'), "      numeric_overflow_action: error").unwrap();
        assert_eq!(
            s.column("amount").unwrap().value_type,
            V::Numeric {
                precision: 38,
                scale: 18
            }
        );
    }

    #[test]
    fn reserved_names_halt() {
        let mut d = orders(b'd');
        d.columns.push(attr(6, "_OP", 25, -1, false, "text"));
        assert_eq!(
            schema_of(&d, "").unwrap_err().code(),
            "RESERVED_COLUMN_COLLISION"
        );
    }

    #[test]
    fn partition_columns() {
        use data_avalanche_config::PartitionField;
        // Built directly: a Delta pipeline's config refuses temporal transforms (V-086), but the
        // start check is sink-neutral.
        let check = |column: &str, transform| {
            let mut tables = pipeline_with("public.orders", "").tables;
            tables.partitioning.insert(
                "public.orders".into(),
                vec![PartitionField {
                    column: column.into(),
                    transform,
                }],
            );
            table_schema(&orders(b'd'), &tables, &TypeCatalog::new()).map(|_| ())
        };
        use PartitionTransform as P;
        assert!(check("code", P::Identity).is_ok());
        assert!(check("at", P::Day).is_ok());
        assert!(check("_ts", P::Hour).is_ok());
        assert_eq!(
            check("nope", P::Identity).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        let not_temporal = check("code", P::Month).unwrap_err();
        assert!(
            not_temporal.to_string().contains("date or timestamp"),
            "{not_temporal}"
        );
    }
}
