//! Shared fixtures: each test owns a schema `it_<test>`, a slot `it_<test>` and a publication
//! `it_<test>_pub`, dropped and re-created at its start, so a failed run leaves nothing that
//! breaks the next one.

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::TableRef;
use data_avalanche_source_pg::PgConnector;
use tokio_postgres::{Client, NoTls};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418). The
/// nightly matrix sets it for PostgreSQL 14–17.
pub fn port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

/// A superuser connection for test setup.
pub async fn admin() -> Client {
    let dsn = format!(
        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
        port()
    );
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(conn);
    client
}

/// The test's names: schema and slot `it_<test>`, publication `it_<test>_pub`.
pub struct Names {
    pub schema: String,
    pub slot: String,
    pub publication: String,
}

impl Names {
    pub fn new(test: &str) -> Self {
        Self {
            schema: format!("it_{test}"),
            slot: format!("it_{test}"),
            publication: format!("it_{test}_pub"),
        }
    }

    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }
}

/// Drops what an earlier run of `test` left (its slot, publication and schema) and creates the
/// schema again, then runs `ddl` in it.
pub async fn fresh(admin: &Client, test: &str, ddl: &str) -> Names {
    let n = Names::new(test);
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&n.slot],
        )
        .await
        .expect("drop the test slot");
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {pubn};
             DROP SCHEMA IF EXISTS {s} CASCADE;
             CREATE SCHEMA {s};
             SET search_path = {s};
             {ddl}
             RESET search_path;",
            pubn = n.publication,
            s = n.schema
        ))
        .await
        .expect("test DDL");
    n
}

/// A pipeline on the test server reading `include`, with more `tables` keys (YAML, indented six
/// spaces) and more `postgres` keys (YAML flow entries).
pub fn pipeline(n: &Names, include: &[&str], tables: &str, postgres: &str) -> Pipeline {
    let include: Vec<String> = include
        .iter()
        .map(|i| format!("{}.{i}", n.schema))
        .collect();
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      host: 127.0.0.1
      port: {port}
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: {publication}, slot: {slot}, heartbeat: 1s{postgres} }}
    tables:
      include: [{include}]
{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#,
        port = port(),
        publication = n.publication,
        slot = n.slot,
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

pub fn connector(p: &Pipeline) -> PgConnector {
    data_avalanche_testkit::install_crypto_provider();
    PgConnector::new(p).expect("connector")
}

/// The test's slot, publication and schema, dropped at the end of a passing test.
pub async fn cleanup(admin: &Client, n: &Names) {
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&n.slot],
        )
        .await
        .expect("drop the test slot");
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}; DROP SCHEMA IF EXISTS {} CASCADE;",
            n.publication, n.schema
        ))
        .await
        .expect("drop the test schema");
}

/// `connector`'s resolved tables, by name.
pub async fn resolved(c: &PgConnector) -> data_avalanche_core::ResolvedTables {
    c.resolve_tables().await.expect("resolve tables")
}
