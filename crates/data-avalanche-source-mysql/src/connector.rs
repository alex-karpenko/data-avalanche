//! `MySqlConnector`: the MySQL or MariaDB source of one pipeline.

use data_avalanche_config::{BinlogPurgeAction, Mysql, Pipeline, SourceType, Tables};
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use data_avalanche_core::{
    ChangeStream, Checkpoint, Chunk, KeysetCursor, Preflight, Prepared, RescanReport,
    ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError,
    SourcePosition, TableRef, TableSchema,
};
use mysql_async::prelude::Queryable;
use tokio::sync::OnceCell;

use crate::connect::Connector;
use crate::server::{
    Coverage, Flavour, Fold, Settings, access_denied, check_coverage, check_settings,
    grants_replication,
};
use crate::start::{CaughtUp, ReadAt, now_micros};

/// The settings a reload changes while the connector runs (config §1.2, class R).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Live {
    /// From the next prepare, and for the open stream's next error.
    pub binlog_purge_action: BinlogPurgeAction,
    /// `@master_heartbeat_period`, from the next stream.
    pub heartbeat: Duration,
    /// `tables.include` and `tables.exclude`, from the next resolve: in place only while the
    /// table set they resolve to is the same (the pipeline restarts otherwise).
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl Live {
    fn of(m: &Mysql, tables: &Tables) -> Self {
        Self {
            binlog_purge_action: m.binlog_purge_action,
            heartbeat: m.heartbeat,
            include: tables.include.clone(),
            exclude: tables.exclude.clone(),
        }
    }
}

/// What a connector and its stream share.
#[derive(Debug)]
pub(crate) struct Shared {
    /// The change the stream read for a schema signal, kept for the pipeline's `augment` of the
    /// same signal: one catalog read per statement, and one answer for both.
    remembered: Mutex<Vec<Remembered>>,
    /// The connection id of the open binlog dump, 0 without one: the watchdog finds the dump by it.
    pub(crate) dump: AtomicU32,
    /// `lower_case_table_names` isn't 0, as the stream read it at its open.
    fold: AtomicBool,
    /// The reloadable settings in force.
    live: Mutex<Live>,
    /// Where the open stream has read to, `None` without one: the heartbeat's lag.
    read: Mutex<Option<ReadAt>>,
}

impl Shared {
    fn new(m: &Mysql, tables: &Tables) -> Self {
        Self {
            remembered: Mutex::default(),
            dump: AtomicU32::default(),
            fold: AtomicBool::default(),
            live: Mutex::new(Live::of(m, tables)),
            read: Mutex::default(),
        }
    }

    fn read_at(&self) -> std::sync::MutexGuard<'_, Option<ReadAt>> {
        self.read.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A stream opened at `from`.
    pub(crate) fn opened(&self, from: &SourcePosition) {
        *self.read_at() = Some(ReadAt {
            position: from.clone(),
            commit_micros: None,
            caught_up: None,
        });
    }

    /// The stream read a transaction that committed at `commit_micros`, up to `position`.
    pub(crate) fn read_to(&self, position: &SourcePosition, commit_micros: i64) {
        *self.read_at() = Some(ReadAt {
            position: position.clone(),
            commit_micros: Some(commit_micros),
            caught_up: None,
        });
    }

    /// A heartbeat: the dump has nothing more to send, as of now.
    pub(crate) fn caught_up(&self) {
        if let Some(r) = self.read_at().as_mut() {
            r.caught_up = Some(CaughtUp {
                at_micros: now_micros(),
                head: None,
            });
        }
    }

    /// The server's write position seen at the first check after the heartbeat, kept only if the
    /// stream is still caught up and has no such position yet.
    pub(crate) fn caught_up_head(&self, head: &SourcePosition) {
        if let Some(c) = self.read_at().as_mut().and_then(|r| r.caught_up.as_mut())
            && c.head.is_none()
        {
            c.head = Some(head.clone());
        }
    }

    pub(crate) fn closed(&self) {
        *self.read_at() = None;
    }

    pub(crate) fn read(&self) -> Option<ReadAt> {
        self.read_at().clone()
    }
}

#[derive(Debug)]
struct Remembered {
    cached: TableSchema,
    sql: String,
    change: Change,
}

/// A channel C answer: the delta, and the schema after it (`None`: the table was dropped).
pub(crate) type Change = (SchemaDelta, Option<TableSchema>);

impl Shared {
    /// Keeps the stream's answer for `cached` and `sql`, in place of an older one for the table.
    pub(crate) fn remember(&self, cached: &TableSchema, sql: &str, change: Change) {
        let mut r = self.remembered.lock().expect("not poisoned");
        r.retain(|x| x.cached.table() != cached.table());
        r.push(Remembered {
            cached: cached.clone(),
            sql: sql.to_owned(),
            change,
        });
    }

    /// How the server compares table names, as the stream last read it.
    pub(crate) fn fold(&self) -> Fold {
        Fold {
            fold: self.fold.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn set_fold(&self, f: Fold) {
        self.fold.store(f.fold, Ordering::Relaxed);
    }

    /// The stream's answer for this signal, if it read one from the same schema.
    pub(crate) fn take(&self, cached: &TableSchema, sql: &str) -> Option<Change> {
        let mut r = self.remembered.lock().expect("not poisoned");
        let i = r.iter().position(|x| &x.cached == cached && x.sql == sql)?;
        Some(r.remove(i).change)
    }
}

/// The MySQL or MariaDB source of one pipeline (roadmap §3.6). Built from the resolved pipeline
/// configuration; connections open on first use.
#[derive(Debug)]
pub struct MySqlConnector {
    pub(crate) pipeline: Pipeline,
    pub(crate) mysql: Mysql,
    pub(crate) conn: Connector,
    /// `VERSION()`, read once: the dialect every later statement depends on.
    version: OnceCell<String>,
    pub(crate) shared: Arc<Shared>,
}

impl MySqlConnector {
    /// Refuses a pipeline whose source isn't MySQL. Nothing connects yet: TLS material is read
    /// at the first connection, and a certificate or key that doesn't parse fails it.
    pub fn new(pipeline: &Pipeline) -> Result<Self, SourceError> {
        let source = &pipeline.source;
        let mysql = match (source.kind, &source.mysql) {
            (SourceType::Mysql, Some(m)) => m.clone(),
            _ => {
                return Err(SourceError::Permanent(format!(
                    "pipeline {}: the source isn't MySQL",
                    pipeline.name
                )));
            }
        };
        Ok(Self {
            pipeline: pipeline.clone(),
            shared: Arc::new(Shared::new(&mysql, &pipeline.tables)),
            mysql,
            conn: Connector::new(source)?,
            version: OnceCell::new(),
        })
    }

    /// The reloadable settings in force, the same for every sibling.
    pub(crate) fn live(&self) -> Live {
        self.shared
            .live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A connector for the stream's catalog reads: its own catalog connection, the same
    /// configuration, version and shared state.
    pub(crate) fn sibling(&self) -> Result<Self, SourceError> {
        Ok(Self {
            pipeline: self.pipeline.clone(),
            mysql: self.mysql.clone(),
            conn: Connector::new(&self.pipeline.source)?,
            version: self.version.clone(),
            shared: self.shared.clone(),
        })
    }

    /// The server's `VERSION()`.
    pub async fn server_version(&self) -> Result<&str, SourceError> {
        self.version
            .get_or_try_init(|| {
                self.conn.command("read the server version", |c| {
                    Box::pin(async move {
                        let v: Option<String> = c.query_first("SELECT VERSION()").await?;
                        Ok(v.unwrap_or_default())
                    })
                })
            })
            .await
            .map(String::as_str)
    }

    /// MySQL or MariaDB.
    pub async fn flavour(&self) -> Result<Flavour, SourceError> {
        Ok(Flavour::of(self.server_version().await?))
    }

    /// The pipeline-level start checks (config §11.2, the MySQL rows): a supported server
    /// (P-8) with the binary log on, `binlog_format = ROW`, `binlog_row_image = FULL`,
    /// `binlog_row_metadata = FULL`, GTIDs on for `replication_type: gtid` on MySQL, a binlog
    /// that carries `source.db`'s changes (no `binlog_do_db` / `binlog_ignore_db` leaving it out,
    /// and a replica that logs what it applies), and a user that can read binlog positions
    /// (`REPLICATION CLIENT`) and dump the binlog (`REPLICATION SLAVE`). A replica setting the
    /// user can't check warns.
    pub async fn preflight(&self) -> Result<Preflight, SourceError> {
        let flavour = self.flavour().await?;
        let settings = self
            .conn
            .command("preflight", move |c| {
                Box::pin(async move {
                    let gtid_mode = match flavour {
                        Flavour::Mysql => "@@gtid_mode",
                        Flavour::Mariadb => "NULL",
                    };
                    let row: Option<mysql_async::Row> = c
                        .query_first(format!(
                            "SELECT VERSION(), @@log_bin, @@binlog_format, @@binlog_row_image,
                                    @@binlog_row_metadata, {gtid_mode}"
                        ))
                        .await?;
                    Ok(row.map(|r| Settings {
                        version: r.get(0).unwrap_or_default(),
                        log_bin: r.get::<i64, _>(1) == Some(1),
                        binlog_format: r.get(2).unwrap_or_default(),
                        binlog_row_image: r.get(3).unwrap_or_default(),
                        binlog_row_metadata: r.get::<Option<String>, _>(4).flatten(),
                        gtid_mode: r.get::<Option<String>, _>(5).flatten(),
                    }))
                })
            })
            .await?
            .ok_or_else(|| SourceError::Permanent("the server returned no settings".into()))?;
        let mut warnings = check_settings(flavour, self.mysql.replication_type, &settings)
            .map_err(SourceError::Permanent)?;
        let status = flavour.status_statement();
        let filters = self
            .conn
            .command("preflight", move |c| {
                Box::pin(async move {
                    match c.query_first::<mysql_async::Row, _>(status).await {
                        Ok(row) => Ok(Ok(row.map(|r| {
                            let column = |name: &str| {
                                r.get_opt::<String, _>(name)
                                    .and_then(Result::ok)
                                    .unwrap_or_default()
                            };
                            (column("Binlog_Do_DB"), column("Binlog_Ignore_DB"))
                        }))),
                        Err(mysql_async::Error::Server(e)) if access_denied(e.code) => {
                            Ok(Err(e.message))
                        }
                        Err(e) => Err(e),
                    }
                })
            })
            .await?;
        let (do_db, ignore_db) = match filters {
            Ok(f) => f.unwrap_or_default(),
            Err(why) => {
                return Err(SourceError::Permanent(format!(
                    "the user can't read the binlog position ({status}: {why}); grant {}",
                    flavour.status_privilege()
                )));
            }
        };
        if let Some(w) = self.coverage(flavour, do_db, ignore_db).await? {
            warnings.push(w);
        }
        if !self.grants_replication(flavour).await? {
            return Err(SourceError::Permanent(
                "the user has no REPLICATION SLAVE privilege on *.*; the binlog dump needs it"
                    .into(),
            ));
        }
        Ok(Preflight {
            server_version: settings.version,
            warnings,
        })
    }

    /// `@@lower_case_table_names`, also kept for channel C's reads.
    pub(crate) async fn fold(&self) -> Result<Fold, SourceError> {
        let n: Option<i64> = self
            .conn
            .command("read lower_case_table_names", |c| {
                Box::pin(async move { c.query_first("SELECT @@lower_case_table_names").await })
            })
            .await?;
        let fold = Fold::of(n.unwrap_or_default());
        self.shared.set_fold(fold);
        Ok(fold)
    }

    /// Whether `source.db`'s changes reach the binlog (config §11.2): no binlog filter, and on a
    /// server with `log_replica_updates` off whether it replicates from another.
    async fn coverage(
        &self,
        flavour: Flavour,
        do_db: String,
        ignore_db: String,
    ) -> Result<Option<String>, SourceError> {
        // Channel C's reads compare names by it from here on.
        self.fold().await?;
        let setting = flavour.replica_updates();
        let replica_updates: Option<i64> = self
            .conn
            .command("read the replica settings", move |c| {
                Box::pin(async move { c.query_first(format!("SELECT @@{setting}")).await })
            })
            .await?;
        let replica_updates = replica_updates == Some(1);
        let replica = match replica_updates {
            true => Some(false),
            false => {
                let statement = flavour.replica_statement();
                self.conn
                    .command("read the replication status", move |c| {
                        Box::pin(async move {
                            match c.query::<mysql_async::Row, _>(statement).await {
                                Ok(rows) => Ok(Some(!rows.is_empty())),
                                Err(mysql_async::Error::Server(e)) if access_denied(e.code) => {
                                    Ok(None)
                                }
                                Err(e) => Err(e),
                            }
                        })
                    })
                    .await?
            }
        };
        let c = Coverage {
            do_db,
            ignore_db,
            replica_updates,
            replica,
        };
        check_coverage(flavour, &c).map_err(SourceError::Permanent)
    }

    /// `REPLICATION SLAVE` from the user's grants, or from its active roles' (MySQL `SHOW GRANTS`
    /// lists a role's privileges only with `USING`).
    async fn grants_replication(&self, flavour: Flavour) -> Result<bool, SourceError> {
        self.conn
            .command("read the user's grants", move |c| {
                Box::pin(async move {
                    let grants: Vec<String> = c.query("SHOW GRANTS").await?;
                    if grants_replication(&grants) || flavour == Flavour::Mariadb {
                        return Ok(grants_replication(&grants));
                    }
                    let roles: Option<String> = c.query_first("SELECT CURRENT_ROLE()").await?;
                    match roles.filter(|r| r != "NONE") {
                        None => Ok(false),
                        Some(roles) => {
                            let grants: Vec<String> = c
                                .query(format!("SHOW GRANTS FOR CURRENT_USER() USING {roles}"))
                                .await?;
                            Ok(grants_replication(&grants))
                        }
                    }
                })
            })
            .await
    }
}

/// The trait is the inherent methods of the modules: `catalog` (channel A), `start` (the start
/// position and the dump watch), `position`, `snapshot`, `stream`, `schema` (channels C and D).
#[async_trait]
impl SourceConnector for MySqlConnector {
    async fn preflight(&self) -> Result<Preflight, SourceError> {
        MySqlConnector::preflight(self).await
    }

    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        MySqlConnector::resolve_tables(self).await
    }

    async fn describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables, SourceError> {
        MySqlConnector::describe_tables(self, tables).await
    }

    async fn prepare(
        &self,
        checkpoint: Option<&Checkpoint>,
        _tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        MySqlConnector::prepare(self, checkpoint).await
    }

    /// The server's write position now: the binlog keeps no position for the pipeline to lose.
    async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
        self.checkpoint_now().await
    }

    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
        MySqlConnector::retained(self).await
    }

    async fn current_position(&self) -> Result<SourcePosition, SourceError> {
        self.position_now().await
    }

    async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        MySqlConnector::read_chunk(self, table, after, limit).await
    }

    /// `toast_strategy: re_query` is PostgreSQL's: a FULL row image carries every column.
    async fn fetch_rows(
        &self,
        table: &TableSchema,
        _keys: &[Row],
    ) -> Result<Vec<Row>, SourceError> {
        Err(SourceError::Permanent(format!(
            "table {}: a MySQL source doesn't re-query rows (toast_strategy is PostgreSQL's)",
            table.table()
        )))
    }

    async fn open_stream(
        &self,
        from: &Checkpoint,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError> {
        Ok(Box::new(
            MySqlConnector::open_stream(self, from, tables).await?,
        ))
    }

    async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        MySqlConnector::augment(self, cached, hint).await
    }

    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        MySqlConnector::rescan(self, known).await
    }

    /// The source creates nothing for a pipeline: no slot, no publication.
    async fn teardown(&self) -> Result<(), SourceError> {
        Ok(())
    }

    fn reconfigure(&self, pipeline: &dyn Any) {
        if let Some(p) = pipeline.downcast_ref::<Pipeline>()
            && let Some(m) = p.source.mysql.as_ref()
        {
            *self
                .shared
                .live
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Live::of(m, &p.tables);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use data_avalanche_config::{BinlogPurgeAction, FingerprintKey, load_str};

    use super::*;

    fn pipeline() -> Pipeline {
        let yaml = r#"
config_version: 1
globals:
  state: { bucket: app-state }
pipelines:
  p:
    id: p
    source: { type: mysql, host: db.example.com, db: shop, user: cdc, password: pw, mysql: {} }
    tables: { include: [t] }
    sink: { type: delta, bucket: lake, delta: {} }
"#;
        load_str(yaml, &BTreeMap::new(), &FingerprintKey::from_bytes([1; 32]))
            .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
            .config
            .pipelines
            .into_values()
            .next()
            .unwrap()
    }

    /// A reload's purge action, heartbeat and table patterns reach the running connector and
    /// the stream's catalog connector beside it; the replica id, restart-only, stays, and what
    /// isn't a pipeline is ignored.
    #[test]
    fn a_reload_reaches_the_reloadable_settings() {
        let p = pipeline();
        let c = MySqlConnector::new(&p).unwrap();
        let sibling = c.sibling().unwrap();
        let mut next = p.clone();
        let m = next.source.mysql.as_mut().unwrap();
        m.binlog_purge_action = BinlogPurgeAction::ReSnapshot;
        m.heartbeat = Duration::from_secs(7);
        m.server_id = 4242;
        next.tables.include = vec!["t".into(), "u".into()];
        next.tables.exclude = vec!["v".into()];
        c.reconfigure(&next);
        let live = Live {
            binlog_purge_action: BinlogPurgeAction::ReSnapshot,
            heartbeat: Duration::from_secs(7),
            include: vec!["t".into(), "u".into()],
            exclude: vec!["v".into()],
        };
        assert_eq!(c.live(), live);
        assert_eq!(sibling.live(), live, "the stream's catalog takes it too");
        assert_eq!(
            c.mysql.server_id,
            p.source.mysql.as_ref().unwrap().server_id
        );
        c.reconfigure(&next.source);
        assert_eq!(c.live(), live, "a source block isn't a pipeline");
    }
}
