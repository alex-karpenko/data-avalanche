//! The raw model: the YAML document as written (config §3–§9). Every field is optional so that
//! presence is checked by V-rules (with IDs) rather than by serde; every struct denies unknown
//! fields (§2); every scalar is a [`Leaf`] (text + location, coerced later). `defaults` has the
//! same shape as a pipeline (§7).

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

use data_avalanche_core::LogicalType;
use serde::Deserialize;

use crate::coerce::LogFilter;
use crate::enums::*;
use crate::leaf::{Leaf, Tri, tri};
use crate::units::{ByteSize, HostPort};

type L<T> = Option<Leaf<T>>;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawConfig {
    pub config_version: L<u32>,
    pub globals: Option<RawGlobals>,
    pub defaults: Option<RawPipeline>,
    pub pipelines: Option<BTreeMap<String, RawPipeline>>,
}

// ---- globals (§6) ----

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawGlobals {
    pub cloud: L<Cloud>,
    pub state: Option<RawGlobalState>,
    pub api: Option<RawApi>,
    pub log: Option<RawLog>,
    pub cluster: Option<RawCluster>,
    pub tracing: Option<RawTracing>,
    pub azure: Option<RawAzure>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawGlobalState {
    pub bucket: L<String>,
    pub prefix: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawApi {
    pub address: L<HostPort>,
    pub auth: L<ApiAuth>,
    pub user: L<String>,
    pub password: L<String>,
    pub credentials_file: L<String>,
    pub jwt: Option<RawJwt>,
    pub oauth2: Option<RawApiOauth2>,
    pub role_mapping: Option<BTreeMap<String, Vec<Leaf<String>>>>,
    pub tls: Option<RawApiTls>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawJwt {
    pub issuer: L<String>,
    pub audience: L<String>,
    pub jwks_url: L<String>,
    pub roles_claim: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawApiOauth2 {
    pub issuer: L<String>,
    pub audience: L<String>,
    pub jwks_url: L<String>,
    pub discovery_url: L<String>,
    pub roles_claim: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawApiTls {
    pub enabled: L<bool>,
    pub cert: L<String>,
    pub key: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawLog {
    pub level: L<LogFilter>,
    pub format: L<LogFormat>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCluster {
    pub tokens: L<u32>,
    pub lock_backend: L<LockBackend>,
    pub lease_duration: L<Duration>,
    pub lease_renewal_interval: L<Duration>,
    pub lease_safety_margin: L<f64>,
    pub rebalance_stabilization: L<Duration>,
    pub max_concurrent_handoffs: L<u32>,
    pub max_concurrent_takeovers: L<u32>,
    pub drain_timeout: L<Duration>,
    pub discovery: L<Discovery>,
    pub discovery_dns_name: L<String>,
    pub memberlist: Option<RawMemberlist>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawMemberlist {
    pub join_members: Option<Vec<Leaf<HostPort>>>,
    pub node_name: L<String>,
    pub advertise_addr: L<String>,
    pub advertise_port: L<u16>,
    pub bind_addr: Option<Vec<Leaf<IpAddr>>>,
    pub bind_port: L<u16>,
    pub packet_dial_timeout: L<Duration>,
    pub packet_write_timeout: L<Duration>,
    pub compression_enabled: L<bool>,
    pub gossip_interval: L<Duration>,
    pub gossip_nodes: L<u32>,
    pub probe_interval: L<Duration>,
    pub probe_timeout: L<Duration>,
    pub dead_node_reclaim_time: L<Duration>,
    pub push_pull_interval: L<Duration>,
    pub retransmit_factor: L<u32>,
    pub lifeguard_enabled: L<bool>,
    pub tls: Option<RawMemberlistTls>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawMemberlistTls {
    pub enabled: L<bool>,
    pub cert: L<String>,
    pub key: L<String>,
    pub ca: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawTracing {
    pub otlp_endpoint: L<String>,
    pub sample_ratio: L<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawAzure {
    pub account_name: L<String>,
}

// ---- pipelines and defaults (§7, §9) ----

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPipeline {
    pub id: L<String>,
    pub source: Option<RawSource>,
    pub tables: Option<RawTables>,
    pub sink: Option<RawSink>,
    pub state: Option<RawPipelineState>,
    pub schema_evolution: Option<RawSchemaEvolution>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSource {
    #[serde(rename = "type")]
    pub kind: L<SourceType>,
    pub host: L<String>,
    pub port: L<u16>,
    pub db: L<String>,
    pub user: L<String>,
    pub password: L<String>,
    pub credentials_file: L<String>,
    pub ssl_mode: L<SslMode>,
    pub ssl_cert: L<String>,
    pub ssl_key: L<String>,
    pub ssl_ca: L<String>,
    pub timeouts: Option<RawTimeouts>,
    pub toast_strategy: L<ToastStrategy>,
    pub postgres: Option<RawPostgres>,
    pub mysql: Option<RawMysql>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawTimeouts {
    pub connect: L<Duration>,
    pub command: L<Duration>,
    pub snapshot: L<Duration>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPostgres {
    pub publication: L<String>,
    pub slot: L<String>,
    pub auto_cleanup: L<bool>,
    pub heartbeat: L<Duration>,
    pub slot_management: L<SlotManagement>,
    pub slot_on_orphan: L<SlotOnOrphan>,
    #[serde(default, deserialize_with = "tri")]
    pub max_wal_lag_bytes: Tri<ByteSize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawMysql {
    pub replication_type: L<ReplicationType>,
    pub tinyint1_as_bool: L<bool>,
    pub tinyint1_detection: L<Tinyint1Detection>,
    pub binlog_purge_action: L<BinlogPurgeAction>,
    pub server_id: L<u32>,
    pub heartbeat: L<Duration>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawTables {
    /// Tri-state (§9.4.1, config §8.3): absent → inherit `defaults`; explicit `null` → no list (an
    /// empty list, not "inherit"); a value → that list. Not the `Tri<T>` alias: that wraps a
    /// scalar leaf, and a list is not itself a leaf.
    #[serde(default, deserialize_with = "tri")]
    pub include: Option<Option<Vec<Leaf<String>>>>,
    #[serde(default, deserialize_with = "tri")]
    pub exclude: Option<Option<Vec<Leaf<String>>>>,
    pub snapshots: Option<RawSnapshots>,
    pub column_type_overrides: Option<BTreeMap<String, Leaf<LogicalType>>>,
    pub numeric_overflow_action: L<NumericOverflowAction>,
    pub primary_key: Option<BTreeMap<String, Vec<Leaf<String>>>>,
    pub partitioning: Option<BTreeMap<String, Vec<RawPartitionField>>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSnapshots {
    pub mode: L<SnapshotMode>,
    pub parallelism: L<u32>,
    pub chunk_size: L<u64>,
    pub overrides: Option<BTreeMap<String, Leaf<SnapshotMode>>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPartitionField {
    pub column: L<String>,
    pub transform: L<Transform>,
    pub n: L<u32>,
    pub width: L<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSink {
    #[serde(rename = "type")]
    pub kind: L<SinkType>,
    pub write_mode: L<WriteMode>,
    pub bucket: L<String>,
    pub prefix: L<String>,
    pub flush: Option<RawFlush>,
    #[serde(default, deserialize_with = "tri")]
    pub max_transaction_batch_rows: Tri<u64>,
    pub max_transaction_buffer_bytes: L<ByteSize>,
    pub buffer_capacity_rows: L<u64>,
    pub metrics: Option<RawSinkMetrics>,
    pub iceberg: Option<RawIceberg>,
    pub delta: Option<RawDelta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawFlush {
    #[serde(default, deserialize_with = "tri")]
    pub seconds: Tri<u64>,
    #[serde(default, deserialize_with = "tri")]
    pub rows: Tri<u64>,
    #[serde(default, deserialize_with = "tri")]
    pub bytes: Tri<ByteSize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSinkMetrics {
    pub per_table: L<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawIceberg {
    pub strategy: L<IcebergStrategy>,
    pub catalog: Option<RawCatalog>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCatalog {
    #[serde(rename = "type")]
    pub kind: L<CatalogKind>,
    pub glue: Option<RawGlue>,
    pub hive: Option<RawHive>,
    pub rest: Option<RawRest>,
    pub retry: Option<RawRetry>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawGlue {
    pub name: L<String>,
    pub database: L<String>,
    pub strip_schema: L<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawHive {
    pub url: L<String>,
    pub auth: L<HiveAuth>,
    pub user: L<String>,
    pub password: L<String>,
    pub credentials_file: L<String>,
    pub kerberos: Option<RawKerberos>,
    pub tls: Option<RawHiveTls>,
    pub database: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawKerberos {
    pub principal: L<String>,
    pub keytab: L<String>,
    pub service_name: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawHiveTls {
    pub enabled: L<bool>,
    pub ca: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRest {
    pub url: L<String>,
    pub warehouse: L<String>,
    pub prefix: L<String>,
    pub auth: L<RestAuth>,
    pub token: L<String>,
    pub token_file: L<String>,
    pub oauth2: Option<RawRestOauth2>,
    pub sigv4: Option<RawSigv4>,
    pub tls: Option<RawRestTls>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRestOauth2 {
    pub client_id: L<String>,
    pub client_secret: L<String>,
    pub credentials_file: L<String>,
    pub token_url: L<String>,
    pub scope: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSigv4 {
    pub region: L<String>,
    pub service: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRestTls {
    pub ca: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRetry {
    pub max_attempts: L<u32>,
    pub base_backoff: L<Duration>,
    pub max_backoff: L<Duration>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawDelta {
    pub column_mapping: L<ColumnMapping>,
    pub checkpoint_interval: L<u64>,
    pub log_retention: L<Duration>,
    pub timestamp_ntz_as: L<TimestampNtzAs>,
    pub table_properties: Option<BTreeMap<String, Leaf<String>>>,
    pub catalog: Option<RawDeltaCatalog>,
    #[serde(default, deserialize_with = "tri")]
    pub merge_memory_limit: Tri<ByteSize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawDeltaCatalog {
    #[serde(rename = "type")]
    pub kind: L<DeltaCatalogType>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPipelineState {
    pub bucket: L<String>,
    pub prefix: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSchemaEvolution {
    pub enabled: L<bool>,
    pub rescan_interval: L<Duration>,
}

#[cfg(test)]
mod tests {
    //! The raw model accepts the spec's complete example (§14) and rejects an unknown key in every
    //! block. Moved from `tests/raw.rs` (FU-E): `RawConfig` and `parse_document` are crate-internal.

    use super::*;
    use crate::diag::PARSE;
    use crate::parse::parse_document;

    const SPEC_EXAMPLE: &str = include_str!("../tests/fixtures/spec_example.yaml");

    #[test]
    fn parses_the_spec_example() {
        let raw: RawConfig = parse_document(SPEC_EXAMPLE).unwrap();
        assert_eq!(raw.config_version.unwrap().text(), "1");
        let globals = raw.globals.unwrap();
        let cluster = globals.cluster.unwrap();
        assert_eq!(cluster.lease_duration.unwrap().text(), "15s");
        let tls = cluster.memberlist.unwrap().tls.unwrap();
        assert_eq!(tls.key.unwrap().text(), "${GOSSIP_TLS_KEY}");
        let defaults = raw.defaults.unwrap();
        let rest = defaults
            .sink
            .unwrap()
            .iceberg
            .unwrap()
            .catalog
            .unwrap()
            .rest
            .unwrap();
        assert_eq!(
            rest.oauth2.unwrap().scope.unwrap().text(),
            "catalog:read catalog:write"
        );
        let pipelines = raw.pipelines.unwrap();
        assert_eq!(
            pipelines.keys().collect::<Vec<_>>(),
            ["events-mysql", "orders-pg"]
        );
        let orders = &pipelines["orders-pg"];
        let flush = orders.sink.as_ref().unwrap().flush.as_ref().unwrap();
        assert!(
            flush.seconds.is_none(),
            "seconds is inherited, not disabled"
        );
        assert_eq!(
            flush.rows.as_ref().unwrap().as_ref().unwrap().text(),
            "50000"
        );
        let tables = orders.tables.as_ref().unwrap();
        let part = &tables.partitioning.as_ref().unwrap()["public.orders"][0];
        assert_eq!(part.transform.as_ref().unwrap().text(), "day");
    }

    /// One document per raw struct, each with an unknown key `bogus` inside that struct. A struct
    /// that forgot `deny_unknown_fields` makes its case parse and fails the test.
    const UNKNOWN_KEY_CASES: &[&str] = &[
        "bogus: 1",
        "globals: {bogus: 1}",
        "globals: {state: {bogus: 1}}",
        "globals: {api: {bogus: 1}}",
        "globals: {api: {jwt: {bogus: 1}}}",
        "globals: {api: {oauth2: {bogus: 1}}}",
        "globals: {api: {tls: {bogus: 1}}}",
        "globals: {log: {bogus: 1}}",
        "globals: {cluster: {bogus: 1}}",
        "globals: {cluster: {memberlist: {bogus: 1}}}",
        "globals: {cluster: {memberlist: {tls: {bogus: 1}}}}",
        "globals: {tracing: {bogus: 1}}",
        "globals: {azure: {bogus: 1}}",
        "defaults: {bogus: 1}",
        "pipelines: {p: {bogus: 1}}",
        "pipelines: {p: {source: {bogus: 1}}}",
        "pipelines: {p: {source: {timeouts: {bogus: 1}}}}",
        "pipelines: {p: {source: {postgres: {bogus: 1}}}}",
        "pipelines: {p: {source: {mysql: {bogus: 1}}}}",
        "pipelines: {p: {tables: {bogus: 1}}}",
        "pipelines: {p: {tables: {snapshots: {bogus: 1}}}}",
        "pipelines: {p: {tables: {partitioning: {t: [{bogus: 1}]}}}}",
        "pipelines: {p: {sink: {bogus: 1}}}",
        "pipelines: {p: {sink: {flush: {bogus: 1}}}}",
        "pipelines: {p: {sink: {metrics: {bogus: 1}}}}",
        "pipelines: {p: {sink: {iceberg: {bogus: 1}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {bogus: 1}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {glue: {bogus: 1}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {hive: {bogus: 1}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {hive: {kerberos: {bogus: 1}}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {hive: {tls: {bogus: 1}}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {rest: {bogus: 1}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {rest: {oauth2: {bogus: 1}}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {rest: {sigv4: {bogus: 1}}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {rest: {tls: {bogus: 1}}}}}}}",
        "pipelines: {p: {sink: {iceberg: {catalog: {retry: {bogus: 1}}}}}}",
        "pipelines: {p: {sink: {delta: {bogus: 1}}}}",
        "pipelines: {p: {sink: {delta: {catalog: {bogus: 1}}}}}",
        "pipelines: {p: {state: {bogus: 1}}}",
        "pipelines: {p: {schema_evolution: {bogus: 1}}}",
    ];

    #[test]
    fn rejects_an_unknown_key_in_every_block() {
        for case in UNKNOWN_KEY_CASES {
            let err = parse_document::<RawConfig>(case).expect_err(case);
            assert_eq!(err.rule, PARSE, "{case}");
            assert!(
                err.message.contains("unknown field `bogus`"),
                "{case}: {}",
                err.message
            );
        }
    }

    #[test]
    fn old_draft_spellings_are_unknown_keys() {
        // §15: the draft's kebab-case `ssl-mode` and the pre-multi-sink pipeline-level `catalog`.
        for case in [
            "pipelines: {p: {source: {ssl-mode: require}}}",
            "pipelines: {p: {catalog: {glue: {name: x}}}}",
            "pipelines: {p: {sink: {strategy: mor}}}",
        ] {
            let err = parse_document::<RawConfig>(case).expect_err(case);
            assert!(
                err.message.contains("unknown field"),
                "{case}: {}",
                err.message
            );
        }
    }
}
