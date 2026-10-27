//! `PgConnector`: the PostgreSQL source of one pipeline.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::PoisonError;
use std::time::Duration;

use async_trait::async_trait;
use data_avalanche_config::{
    Pipeline, Postgres, SlotOnOrphan, SourceType, Tables, check_table_set, check_unbounded_numeric,
};
use data_avalanche_core::{
    ChangeStream, Checkpoint, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared,
    RescanReport, ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector,
    SourceError, SourcePosition, TableRef, TableSchema,
};
use tokio::sync::Mutex;

use crate::catalog::{self, Described, Notes, table_key, table_schema};
use crate::connect::Connector;
use crate::position::parse_lsn;
use crate::types::TypeCatalog;

/// The oldest server the source supports (P-8).
pub const MIN_SERVER_VERSION: i32 = 140_000;

/// The settings a reload changes while the connector runs (config §1.2, class R).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Live {
    /// The replication connection's status interval, from the next stream.
    pub heartbeat: Duration,
    /// From the next prepare.
    pub slot_on_orphan: SlotOnOrphan,
    /// At the teardown.
    pub auto_cleanup: bool,
    /// `tables.include` and `tables.exclude`, from the next resolve: in place only while the
    /// table set they resolve to is the same (the pipeline restarts otherwise).
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl Live {
    fn of(pg: &Postgres, tables: &Tables) -> Self {
        Self {
            heartbeat: pg.heartbeat,
            slot_on_orphan: pg.slot_on_orphan,
            auto_cleanup: pg.auto_cleanup,
            include: tables.include.clone(),
            exclude: tables.exclude.clone(),
        }
    }
}

/// The PostgreSQL source of one pipeline (roadmap §3.6). Built from the resolved pipeline
/// configuration; connections open on first use.
#[derive(Debug)]
pub struct PgConnector {
    pub(crate) pipeline: Pipeline,
    /// As built: a reload's settings are in `live`.
    pub(crate) pg: Postgres,
    pub(crate) conn: Connector,
    /// `pg_type` rows fetched so far.
    pub(crate) types: Mutex<TypeCatalog>,
    live: std::sync::Mutex<Live>,
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
            live: std::sync::Mutex::new(Live::of(&pg, &pipeline.tables)),
            pg,
            conn: Connector::new(source)?,
            types: Mutex::new(TypeCatalog::new()),
        })
    }

    /// The reloadable settings in force.
    pub(crate) fn live(&self) -> Live {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
        let relations = self.relations().await?;
        let live = self.live();
        let selection = catalog::select(&live.include, &live.exclude, &relations);
        let (mut out, unconstrained) = self.described(selection).await?;
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

    /// The tables of `selection`, described: each with its schema, or halted, or skipped (a
    /// table without an identifier that a glob found). Returns the unconstrained numeric columns
    /// too, for V-079.
    async fn described(
        &self,
        selection: catalog::Selection,
    ) -> Result<(ResolvedTables, Vec<(TableRef, String)>), SourceError> {
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
        Ok((out, unconstrained))
    }

    /// An on-demand snapshot's tables, by exact name (Data-Flow §On-Demand Snapshot): the
    /// config's patterns and its table-set checks don't apply.
    pub async fn describe_tables(
        &self,
        tables: &[TableRef],
    ) -> Result<ResolvedTables, SourceError> {
        let relations = self.relations().await?;
        Ok(self.described(catalog::named(tables, &relations)).await?.0)
    }

    async fn relations(&self) -> Result<Vec<catalog::Relation>, SourceError> {
        let client = self.conn.client().await?;
        self.conn
            .command("list tables", catalog::relations(&client))
            .await
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

    async fn describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables, SourceError> {
        PgConnector::describe_tables(self, tables).await
    }

    async fn prepare(
        &self,
        checkpoint: Option<&Checkpoint>,
        tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        PgConnector::prepare(self, checkpoint, tables).await
    }

    async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
        PgConnector::reset_position(self)
            .await
            .map(Checkpoint::from)
    }

    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
        PgConnector::retained(self).await
    }

    async fn current_position(&self) -> Result<SourcePosition, SourceError> {
        PgConnector::current_position(self).await
    }

    async fn in_flight(&self) -> Result<Option<Vec<InFlightTxn>>, SourceError> {
        PgConnector::in_flight(self).await
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
        from: &Checkpoint,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError> {
        Ok(PgConnector::open_stream(self, &from.position, tables).await?)
    }

    async fn end_other_reader(&self) -> Result<bool, SourceError> {
        PgConnector::end_other_reader(self).await
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

    fn reconfigure(&self, pipeline: &dyn Any) {
        if let Some(p) = pipeline.downcast_ref::<Pipeline>()
            && let Some(pg) = p.source.postgres.as_ref()
        {
            *self.live.lock().unwrap_or_else(PoisonError::into_inner) = Live::of(pg, &p.tables);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use data_avalanche_config::SlotOnOrphan;

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

    /// A reload's heartbeat, orphan policy, cleanup and table patterns reach the running
    /// connector; the slot, restart-only, stays, and what isn't a pipeline is ignored.
    #[test]
    fn a_reload_reaches_the_reloadable_settings() {
        let p = pipeline("public.t");
        let c = PgConnector::new(&p).unwrap();
        let mut next = p.clone();
        let pg = next.source.postgres.as_mut().unwrap();
        pg.heartbeat = Duration::from_secs(7);
        pg.slot_on_orphan = SlotOnOrphan::AdoptResnapshot;
        pg.auto_cleanup = true;
        pg.slot = "another_slot".into();
        next.tables.include = vec!["public.t".into(), "public.u".into()];
        next.tables.exclude = vec!["public.v".into()];
        c.reconfigure(&next);
        let live = Live {
            heartbeat: Duration::from_secs(7),
            slot_on_orphan: SlotOnOrphan::AdoptResnapshot,
            auto_cleanup: true,
            include: vec!["public.t".into(), "public.u".into()],
            exclude: vec!["public.v".into()],
        };
        assert_eq!(c.live(), live);
        assert_eq!(c.pg.slot, "p_slot");
        c.reconfigure(&next.source);
        assert_eq!(c.live(), live, "a source block isn't a pipeline");
    }

    /// A teardown after a reload turned `auto_cleanup` off keeps the slot: it doesn't even
    /// connect (the server here doesn't answer).
    #[tokio::test]
    async fn a_teardown_after_a_reload_takes_its_auto_cleanup() {
        let mut p = pipeline("public.t");
        p.source.port = 1;
        p.source.postgres.as_mut().unwrap().auto_cleanup = true;
        let c = PgConnector::new(&p).unwrap();
        assert!(c.teardown().await.is_err(), "it connects to clean up");
        let mut next = p.clone();
        next.source.postgres.as_mut().unwrap().auto_cleanup = false;
        c.reconfigure(&next);
        c.teardown().await.unwrap();
    }
}
