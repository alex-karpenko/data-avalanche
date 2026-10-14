//! Shared fixtures. The server under test is `DATA_AVALANCHE_MYSQL` (default `mysql-84`); each
//! test owns a database `it_<test>`, dropped and re-created at its start, so a failed run leaves
//! nothing that breaks the next one. The pipeline id is the database name, so the derived replica
//! ids of tests running at once differ (V-120).

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_source_mysql::{Flavour, MySqlConnector};
use mysql_async::Conn;
use mysql_async::prelude::Queryable;

/// A server of `test-infra/compose.yaml` and the replication mode the tests use on it.
#[derive(Debug, Clone, Copy)]
pub struct Target {
    pub name: &'static str,
    pub port: u16,
    pub flavour: Flavour,
    /// `gtid` or `binlog`.
    pub replication: &'static str,
}

const TARGETS: [Target; 6] = [
    Target {
        name: "mysql-84",
        port: 53384,
        flavour: Flavour::Mysql,
        replication: "gtid",
    },
    Target {
        name: "mysql-84-nogtid",
        port: 53385,
        flavour: Flavour::Mysql,
        replication: "binlog",
    },
    Target {
        name: "mariadb-118",
        port: 53318,
        flavour: Flavour::Mariadb,
        replication: "gtid",
    },
    Target {
        name: "mariadb-118-binlog",
        port: 53318,
        flavour: Flavour::Mariadb,
        replication: "binlog",
    },
    Target {
        name: "mariadb-114",
        port: 53314,
        flavour: Flavour::Mariadb,
        replication: "gtid",
    },
    Target {
        name: "mariadb-1011",
        port: 53311,
        flavour: Flavour::Mariadb,
        replication: "gtid",
    },
];

/// `DATA_AVALANCHE_MYSQL`, else `mysql-84`.
pub fn target() -> Target {
    let name = std::env::var("DATA_AVALANCHE_MYSQL").unwrap_or_else(|_| "mysql-84".into());
    TARGETS
        .into_iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("DATA_AVALANCHE_MYSQL={name}: not a test-infra MySQL target"))
}

/// A root connection to the target for test setup, in `db`.
pub async fn admin_in(db: &str) -> Conn {
    let url = format!("mysql://root:avalanche@127.0.0.1:{}/{db}", target().port);
    Conn::from_url(url)
        .await
        .expect("connect to the test server")
}

/// The test's database `it_<test>`.
pub struct Names {
    pub db: String,
}

impl Names {
    pub fn new(test: &str) -> Self {
        Self {
            db: format!("it_{test}"),
        }
    }
}

/// Drops what an earlier run of `test` left and creates its database again, then runs `ddl` in
/// it. Returns the names and a root connection in the database.
pub async fn fresh(test: &str, ddl: &str) -> (Names, Conn) {
    let n = Names::new(test);
    let mut a = admin_in("avalanche").await;
    a.query_drop(format!(
        "DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db}",
        db = n.db
    ))
    .await
    .expect("the test database");
    drop(a);
    let mut a = admin_in(&n.db).await;
    if !ddl.is_empty() {
        a.query_drop(ddl).await.expect("test DDL");
    }
    (n, a)
}

/// A pipeline `n.db` on the target as `user`, reading `include`, with more `source` keys (YAML,
/// indented six spaces), `mysql` keys (YAML flow entries; a `replication_type` there replaces the
/// target's) and `tables` keys (indented six).
pub fn pipeline_as(
    n: &Names,
    user: &str,
    source: &str,
    mysql: &str,
    include: &[&str],
    tables: &str,
) -> Pipeline {
    let t = target();
    let replication = match mysql.contains("replication_type") {
        true => String::new(),
        false => format!("replication_type: {}, ", t.replication),
    };
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: {db}
    source:
      type: mysql
      host: 127.0.0.1
      port: {port}
      db: {db}
      user: {user}
      password: avalanche
{source}
      mysql: {{ {replication}heartbeat: 1s{mysql} }}
    tables:
      include: [{include}]
{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#,
        db = n.db,
        port = t.port,
        include = include.join(", "),
    );
    let loaded = load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
    loaded
        .config
        .pipelines
        .into_values()
        .next()
        .expect("one pipeline")
}

/// A pipeline as root without TLS.
pub fn pipeline(n: &Names, include: &[&str], tables: &str, mysql: &str) -> Pipeline {
    pipeline_as(n, "root", "      ssl_mode: disable", mysql, include, tables)
}

pub fn connector(p: &Pipeline) -> MySqlConnector {
    data_avalanche_testkit::install_crypto_provider();
    MySqlConnector::new(p).expect("connector")
}

/// The test's database, dropped at the end of a passing test.
pub async fn cleanup(n: &Names) {
    let mut a = admin_in("avalanche").await;
    a.query_drop(format!("DROP DATABASE IF EXISTS {}", n.db))
        .await
        .expect("drop the test database");
}
