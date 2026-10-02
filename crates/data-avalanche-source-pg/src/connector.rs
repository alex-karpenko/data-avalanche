//! `PgConnector`: the PostgreSQL source of one pipeline.

use std::collections::BTreeMap;

use async_trait::async_trait;
use data_avalanche_config::{
    Pipeline, Postgres, SourceType, check_table_set, check_unbounded_numeric,
};
use data_avalanche_core::{
    ChangeStream, Chunk, HaltReason, KeysetCursor, Preflight, Prepared, RescanReport,
    ResolvedTables, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError, SourcePosition,
    TableRef, TableSchema,
};
use tokio::sync::Mutex;

use crate::catalog::{self, Described, Notes, table_key, table_schema};
use crate::connect::Connector;
use crate::position::parse_lsn;
use crate::types::TypeCatalog;

/// The oldest server the source supports (P-8).
pub const MIN_SERVER_VERSION: i32 = 140_000;

/// The PostgreSQL source of one pipeline (roadmap §3.6). Built from the resolved pipeline
/// configuration; connections open on first use.
#[derive(Debug)]
pub struct PgConnector {
    pub(crate) pipeline: Pipeline,
    pub(crate) pg: Postgres,
    pub(crate) conn: Connector,
    /// `pg_type` rows fetched so far.
    pub(crate) types: Mutex<TypeCatalog>,
}

impl PgConnector {
    /// Refuses a pipeline whose source isn't PostgreSQL, and TLS material that doesn't parse.
    pub fn new(pipeline: &Pipeline) -> Result<Self, SourceError> {
        let source = &pipeline.source;
        let pg = match (source.kind, &source.postgres) {
            (SourceType::Postgres, Some(pg)) => pg.clone(),
            _ => {
                return Err(SourceError::Permanent(format!(
                    "pipeline {}: the source isn't PostgreSQL",
                    pipeline.name
                )));
            }
        };
        Ok(Self {
            pipeline: pipeline.clone(),
            pg,
            conn: Connector::new(source)?,
            types: Mutex::new(TypeCatalog::new()),
        })
    }

    /// The pipeline-level start checks (config §11.2, `Schema_Change…` §9): the server is
    /// PostgreSQL 14 or newer with `wal_level = logical`, the database encoding is UTF8 (D-M5.8),
    /// and the user can read the catalogs channels A/C/D query. A user without the `REPLICATION`
    /// attribute warns: a managed service may grant replication another way.
    pub async fn preflight(&self) -> Result<Preflight, SourceError> {
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "preflight",
                client.query_one(
                    "SELECT current_setting('server_version_num')::int,
                            current_setting('server_version'),
                            current_setting('wal_level'),
                            pg_encoding_to_char(d.encoding),
                            r.rolreplication OR r.rolsuper,
                            has_table_privilege('pg_catalog.pg_class', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_attribute', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_index', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_type', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_namespace', 'SELECT')
                       FROM pg_catalog.pg_database d, pg_catalog.pg_roles r
                      WHERE d.datname = current_database() AND r.rolname = current_user",
                    &[],
                ),
            )
            .await?;
        let (num, version, wal_level, encoding, replication, catalogs): (
            i32,
            String,
            String,
            String,
            bool,
            bool,
        ) = (
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
        );
        let refuse = |m: String| Err(SourceError::Permanent(m));
        if num < MIN_SERVER_VERSION {
            return refuse(format!(
                "PostgreSQL {version}: the source needs 14 or newer"
            ));
        }
        if wal_level != "logical" {
            return refuse(format!(
                "wal_level is {wal_level}; logical replication needs wal_level = logical"
            ));
        }
        if encoding != "UTF8" {
            return refuse(format!(
                "the database encoding is {encoding}; the source reads UTF8 databases only"
            ));
        }
        if !catalogs {
            return refuse(
                "the user can't SELECT from the system catalogs the schema channels read".into(),
            );
        }
        let mut warnings = vec![];
        if !replication {
            warnings.push(
                "the user has neither the REPLICATION attribute nor superuser; the replication connection may be refused".into(),
            );
        }
        Ok(Preflight {
            server_version: version,
            warnings,
        })
    }

    /// Describes `oids` and builds their schemas, fetching the types they use.
    pub(crate) async fn schemas(
        &self,
        oids: &[u32],
    ) -> Result<BTreeMap<u32, (Described, Result<(TableSchema, Notes), HaltReason>)>, SourceError>
    {
        let client = self.conn.client().await?;
        let described = self
            .conn
            .command("describe tables", catalog::describe(&client, oids))
            .await?;
        let mut types = self.types.lock().await;
        let type_oids: Vec<u32> = described
            .values()
            .flat_map(|d| d.columns.iter().map(|c| c.type_oid))
            .collect();
        self.conn
            .command(
                "load types",
                catalog::load_types(&client, type_oids, &mut types),
            )
            .await?;
        Ok(described
            .into_iter()
            .map(|(oid, d)| {
                let s = table_schema(&d, &self.pipeline.tables, &types);
                (oid, (d, s))
            })
            .collect())
    }

    /// Channel A (`Schema_Change…` §3): the include/exclude globs against the live catalog, each
    /// table checked (config §11.2). A table without an identifier is skipped with a warning when
    /// a glob found it, and halts when its name was listed; every other problem halts the table.
    pub async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        let client = self.conn.client().await?;
        let relations = self
            .conn
            .command("list tables", catalog::relations(&client))
            .await?;
        let t = &self.pipeline.tables;
        let selection = catalog::select(&t.include, &t.exclude, &relations);
        let oids: Vec<u32> = selection.tables.iter().map(|(r, _)| r.oid).collect();
        let mut schemas = self.schemas(&oids).await?;
        let mut out = ResolvedTables {
            skipped: selection.skipped,
            halted: selection.halted,
            warnings: selection.warnings,
            ..ResolvedTables::default()
        };
        let mut unconstrained = vec![];
        for (relation, explicit) in selection.tables {
            let Some((_, result)) = schemas.remove(&relation.oid) else {
                continue; // dropped since it was listed
            };
            match result {
                Ok((schema, notes)) => {
                    let key = table_key(schema.table());
                    for (column, ty) in notes.read_as_text {
                        out.warnings
                            .push(format!("{key}.{column}: {ty} is read as text"));
                    }
                    for (column, collation) in notes.c_ordered_keys {
                        out.warnings.push(format!(
                            "{key}.{column}: the key's collation {collation} is nondeterministic, \
                             so snapshot chunks order it by \"C\": each chunk scans the table \
                             unless an index has ({column} COLLATE \"C\")"
                        ));
                    }
                    unconstrained.extend(
                        notes
                            .unconstrained
                            .into_iter()
                            .map(|c| (schema.table().clone(), c)),
                    );
                    out.tables.push(schema);
                }
                Err(HaltReason::NoIdentifier) if !explicit => out.skipped.push((
                    relation.table,
                    "no primary key and no tables.primary_key entry".into(),
                )),
                Err(reason) => out.halted.push((relation.table, reason)),
            }
        }
        // Halted tables are in the set: their per-table config keys aren't stale (V-106).
        let resolved: Vec<TableRef> = out
            .tables
            .iter()
            .map(|s| s.table().clone())
            .chain(out.halted.iter().map(|(t, _)| t.clone()))
            .collect();
        let mut report = check_table_set(&self.pipeline, &resolved);
        report.extend(check_unbounded_numeric(&self.pipeline, &unconstrained));
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

    /// `pg_current_wal_lsn()`: a snapshot chunk's watermark (Snapshot_Resumability §1).
    pub async fn current_position(&self) -> Result<SourcePosition, SourceError> {
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "current position",
                client.query_one("SELECT pg_current_wal_lsn()::text", &[]),
            )
            .await?;
        Ok(SourcePosition::Pg(parse_lsn(row.get(0))?))
    }
}

/// The trait is the inherent methods of the modules: `catalog` and here (channel A), `slot`
/// (the lifecycle), `snapshot`, `stream`, `schema` (channels C and D).
#[async_trait]
impl SourceConnector for PgConnector {
    async fn preflight(&self) -> Result<Preflight, SourceError> {
        PgConnector::preflight(self).await
    }

    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        PgConnector::resolve_tables(self).await
    }

    async fn prepare(
        &self,
        checkpoint: Option<&SourcePosition>,
        tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        PgConnector::prepare(self, checkpoint, tables).await
    }

    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
        PgConnector::reset_position(self).await
    }

    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
        PgConnector::retained_bytes(self).await
    }

    async fn current_position(&self) -> Result<SourcePosition, SourceError> {
        PgConnector::current_position(self).await
    }

    async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        PgConnector::read_chunk(self, table, after, limit).await
    }

    async fn fetch_rows(&self, table: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError> {
        PgConnector::fetch_rows(self, table, keys).await
    }

    async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError> {
        Ok(PgConnector::open_stream(self, from, tables).await?)
    }

    async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        PgConnector::augment(self, cached, hint).await
    }

    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        PgConnector::rescan(self, known).await
    }

    async fn teardown(&self) -> Result<(), SourceError> {
        PgConnector::teardown(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::pipeline;

    #[test]
    fn a_connector_is_a_source_connector() {
        let c: Box<dyn SourceConnector> =
            Box::new(PgConnector::new(&pipeline("public.t")).unwrap());
        drop(c);
    }

    #[test]
    fn only_postgres_pipelines() {
        let mut p = pipeline("public.t");
        assert!(PgConnector::new(&p).is_ok());
        p.source.postgres = None;
        let e = PgConnector::new(&p).unwrap_err();
        assert_eq!(
            e,
            SourceError::Permanent("pipeline p: the source isn't PostgreSQL".into())
        );
    }
}
