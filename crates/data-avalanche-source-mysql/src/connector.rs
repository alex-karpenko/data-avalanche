//! `MySqlConnector`: the MySQL or MariaDB source of one pipeline.

use data_avalanche_config::{Mysql, Pipeline, SourceType};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use data_avalanche_core::{Preflight, SchemaDelta, SourceError, TableSchema};
use mysql_async::prelude::Queryable;
use tokio::sync::OnceCell;

use crate::connect::Connector;
use crate::server::{Flavour, Settings, check_settings, grants_replication};

/// What a connector and its stream share.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    /// The change the stream read for a schema signal, kept for the pipeline's `augment` of the
    /// same signal: one catalog read per statement, and one answer for both.
    remembered: Mutex<Vec<Remembered>>,
    /// The connection id of the open binlog dump, 0 without one: the watchdog finds the dump by it.
    pub(crate) dump: AtomicU32,
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
    /// Refuses a pipeline whose source isn't MySQL, and TLS material that doesn't parse.
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
    /// (P-8) with the binary log on, `binlog_format = ROW`, `binlog_row_image = FULL`, GTIDs on for
    /// `replication_type: gtid` on MySQL, and a user that can read binlog positions (`REPLICATION
    /// CLIENT`) and dump the binlog (`REPLICATION SLAVE`). `binlog_row_metadata` other than FULL
    /// warns.
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
        let warnings = check_settings(flavour, self.mysql.replication_type, &settings)
            .map_err(SourceError::Permanent)?;
        let status = flavour.status_statement();
        let refused = self
            .conn
            .command("preflight", move |c| {
                Box::pin(async move {
                    match c.query_drop(status).await {
                        Ok(()) => Ok(None),
                        Err(mysql_async::Error::Server(e))
                            if crate::server::access_denied(e.code) =>
                        {
                            Ok(Some(e.message))
                        }
                        Err(e) => Err(e),
                    }
                })
            })
            .await?;
        if let Some(why) = refused {
            return Err(SourceError::Permanent(format!(
                "the user can't read the binlog position ({status}: {why}); grant {}",
                flavour.status_privilege()
            )));
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
