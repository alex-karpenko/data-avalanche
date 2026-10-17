//! The source under test (Q-M7b-1): PostgreSQL by default, MySQL or MariaDB with
//! `DATA_AVALANCHE_E2E_SOURCE=mysql` (the server `DATA_AVALANCHE_MYSQL` names, else `mysql-84`),
//! and an admin connection that speaks either.

use mysql_async::prelude::Queryable;
use tokio::sync::Mutex;
use tokio_postgres::{NoTls, SimpleQueryMessage};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418).
pub fn pg_port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

/// A MySQL or MariaDB server of `test-infra/compose.yaml`, and the position mode a pipeline
/// reads it in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target {
    pub name: &'static str,
    pub port: u16,
    pub mariadb: bool,
    /// `replication_type`: `gtid` or `binlog`.
    pub replication: &'static str,
    /// The compose service (its container is `data-avalanche-test-<service>-1`).
    pub service: &'static str,
}

const TARGETS: [Target; 6] = [
    Target {
        name: "mysql-84",
        port: 53384,
        mariadb: false,
        replication: "gtid",
        service: "mysql-84",
    },
    Target {
        name: "mysql-84-nogtid",
        port: 53385,
        mariadb: false,
        replication: "binlog",
        service: "mysql-84-nogtid",
    },
    Target {
        name: "mariadb-118",
        port: 53318,
        mariadb: true,
        replication: "gtid",
        service: "mariadb-118",
    },
    Target {
        name: "mariadb-118-binlog",
        port: 53318,
        mariadb: true,
        replication: "binlog",
        service: "mariadb-118",
    },
    Target {
        name: "mariadb-114",
        port: 53314,
        mariadb: true,
        replication: "gtid",
        service: "mariadb-114",
    },
    Target {
        name: "mariadb-1011",
        port: 53311,
        mariadb: true,
        replication: "gtid",
        service: "mariadb-1011",
    },
];

/// A MySQL target by name.
///
/// # Panics
/// When `name` isn't one of [`TARGETS`].
pub fn target(name: &str) -> Target {
    TARGETS
        .into_iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("{name}: not a test-infra MySQL target"))
}

/// The source the suite runs against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Postgres { port: u16 },
    Mysql(Target),
}

impl Source {
    /// From `DATA_AVALANCHE_E2E_SOURCE` (`postgres`, the default, or `mysql`).
    ///
    /// # Panics
    /// On another value, or a `DATA_AVALANCHE_MYSQL` that names no target.
    pub fn from_env() -> Self {
        match std::env::var("DATA_AVALANCHE_E2E_SOURCE").as_deref() {
            Err(_) | Ok("" | "postgres") => Self::Postgres { port: pg_port() },
            Ok("mysql") => Self::Mysql(target(
                &std::env::var("DATA_AVALANCHE_MYSQL").unwrap_or_else(|_| "mysql-84".into()),
            )),
            Ok(other) => panic!("DATA_AVALANCHE_E2E_SOURCE={other}: postgres or mysql"),
        }
    }

    pub fn is_postgres(self) -> bool {
        matches!(self, Self::Postgres { .. })
    }

    /// The server's container: `postgres-<major>` on `554<major>`, a MySQL target's service.
    pub fn container(self) -> String {
        match self {
            Self::Postgres { port } => format!("data-avalanche-test-postgres-{}-1", port - 55400),
            Self::Mysql(t) => format!("data-avalanche-test-{}-1", t.service),
        }
    }
}

/// Skips the test, with a line saying so, unless the suite runs against PostgreSQL.
#[macro_export]
macro_rules! postgres_only {
    () => {
        if !$crate::source::Source::from_env().is_postgres() {
            eprintln!("skipped: a PostgreSQL scenario");
            return;
        }
    };
}

/// Skips the test, with a line saying so, unless the suite runs against MySQL or MariaDB; else
/// the target.
#[macro_export]
macro_rules! mysql_only {
    () => {
        match $crate::source::Source::from_env() {
            $crate::source::Source::Mysql(t) => t,
            $crate::source::Source::Postgres { .. } => {
                eprintln!("skipped: a MySQL scenario");
                return;
            }
        }
    };
}

/// An admin connection to the source: PostgreSQL's `avalanche` database, or MySQL's server as
/// `root`.
pub enum Db {
    Pg(tokio_postgres::Client),
    Mysql(Mutex<mysql_async::Conn>),
}

impl Db {
    pub async fn connect(source: Source) -> Self {
        match source {
            Source::Postgres { port } => {
                let dsn = format!(
                    "host=127.0.0.1 port={port} user=avalanche password=avalanche dbname=avalanche"
                );
                let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
                    .await
                    .expect("connect to test PostgreSQL");
                tokio::spawn(conn);
                Self::Pg(client)
            }
            Source::Mysql(t) => Self::Mysql(Mutex::new(mysql(t.port).await)),
        }
    }

    /// Runs `sql`, one or more statements.
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        match self {
            Self::Pg(c) => c.batch_execute(sql).await.map_err(|e| e.to_string()),
            Self::Mysql(c) => c
                .lock()
                .await
                .query_drop(sql)
                .await
                .map_err(|e| e.to_string()),
        }
    }

    /// The rows of one query, each cell as the server prints it (`None` for a NULL).
    pub async fn text(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
        match self {
            Self::Pg(c) => Ok(c
                .simple_query(sql)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => {
                        Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
                    }
                    _ => None,
                })
                .collect()),
            Self::Mysql(c) => {
                let rows: Vec<mysql_async::Row> =
                    c.lock().await.query(sql).await.map_err(|e| e.to_string())?;
                Ok(rows
                    .into_iter()
                    .map(|r| {
                        r.unwrap()
                            .into_iter()
                            .map(|v| match v {
                                mysql_async::Value::NULL => None,
                                mysql_async::Value::Bytes(b) => {
                                    Some(String::from_utf8_lossy(&b).into_owned())
                                }
                                // The text protocol sends every value as bytes.
                                other => Some(other.as_sql(true)),
                            })
                            .collect()
                    })
                    .collect())
            }
        }
    }

    /// Whether `sql` returns a row.
    pub async fn any(&self, sql: &str) -> bool {
        !self.text(sql).await.expect("the source reads").is_empty()
    }

    /// The PostgreSQL client, for a PostgreSQL scenario.
    ///
    /// # Panics
    /// On MySQL.
    pub fn pg(&self) -> &tokio_postgres::Client {
        match self {
            Self::Pg(c) => c,
            Self::Mysql(_) => panic!("a PostgreSQL scenario on MySQL"),
        }
    }
}

/// A `root` connection to the MySQL server on `port`, in no database.
pub async fn mysql(port: u16) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("avalanche"))
        .prefer_socket(false);
    mysql_async::Conn::new(opts)
        .await
        .expect("connect to the test MySQL server")
}
