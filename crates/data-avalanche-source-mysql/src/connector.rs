//! `MySqlConnector`: the MySQL or MariaDB source of one pipeline.

use data_avalanche_config::{Mysql, Pipeline, SourceType};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

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

/// What a connector and its stream share.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    /// The change the stream read for a schema signal, kept for the pipeline's `augment` of the
    /// same signal: one catalog read per statement, and one answer for both.
    remembered: Mutex<Vec<Remembered>>,
    /// The connection id of the open binlog dump, 0 without one: the watchdog finds the dump by it.
    pub(crate) dump: AtomicU32,
    /// `lower_case_table_names` isn't 0, as the stream read it at its open.
    fold: AtomicBool,
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
            mysql,
            conn: Connector::new(source)?,
            version: OnceCell::new(),
            shared: Arc::default(),
        })
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

    /// Whether `source.db`'s changes reach the binlog (config §11.2): the binlog filters, and on
    /// a server with `log_replica_updates` off whether it replicates from another.
    async fn coverage(
        &self,
        flavour: Flavour,
        do_db: String,
        ignore_db: String,
    ) -> Result<Option<String>, SourceError> {
        let fold = self.fold().await?;
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
        check_coverage(flavour, &self.pipeline.source.db, fold, &c).map_err(SourceError::Permanent)
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

    async fn prepare(
        &self,
        checkpoint: Option<&Checkpoint>,
        _tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        MySqlConnector::prepare(self, checkpoint).await
    }

    /// The server's write position now: the binlog keeps no position for the pipeline to lose.
    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
        self.position_now().await
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
        from: &SourcePosition,
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
}
