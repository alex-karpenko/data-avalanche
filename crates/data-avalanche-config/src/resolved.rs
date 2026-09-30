//! The resolved config (config §11.1): every reference substituted, every file read, every default
//! applied. The only representation the runtime uses. Field names follow the spec's paths so the
//! reload diff (§12) can classify a change by its path. `Serialize` exists for that diff: secrets
//! serialize as fingerprints (see [`Secret`]).

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use data_avalanche_core::LogicalType;
use serde::{Serialize, Serializer};

use crate::coerce::LogFilter;
use crate::enums::*;
use crate::material::{FileRef, Pem};
use crate::secret::Secret;
use crate::units::{HostPort, ser_duration};

fn ser_logical_map<S: Serializer>(
    map: &BTreeMap<String, LogicalType>,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.collect_map(map.iter().map(|(k, v)| (k, v.to_string())))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResolvedConfig {
    pub config_version: u32,
    pub globals: Globals,
    /// Keyed by pipeline `id` (the immutable identity, §9.1), not by the human name.
    pub pipelines: BTreeMap<String, Pipeline>,
}

// ---- globals ----

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Globals {
    pub cloud: Cloud,
    pub state: GlobalState,
    pub api: Api,
    pub log: Log,
    pub cluster: Cluster,
    pub tracing: Tracing,
    pub azure: Azure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GlobalState {
    pub bucket: String,
    /// `""` = bucket root.
    pub prefix: String,
}

/// A `user`/`password` pair given inline or through a `credentials_file` (§4.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum Credentials {
    Inline {
        user: String,
        password: Secret,
    },
    File {
        path: PathBuf,
        user: String,
        password: Secret,
    },
}

impl Credentials {
    pub fn user(&self) -> &str {
        match self {
            Self::Inline { user, .. } | Self::File { user, .. } => user,
        }
    }

    pub fn password(&self) -> &Secret {
        match self {
            Self::Inline { password, .. } | Self::File { password, .. } => password,
        }
    }
}

/// A TLS server identity (`cert` + `key`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TlsIdentity {
    pub cert: Pem,
    pub key: Pem,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Api {
    pub address: HostPort,
    pub auth: ApiAuth,
    /// Set when `auth = basic`.
    pub credentials: Option<Credentials>,
    pub jwt: Option<Jwt>,
    pub oauth2: Option<ApiOauth2>,
    /// Empty = identity mapping (§6.3).
    pub role_mapping: BTreeMap<Role, Vec<String>>,
    /// `None` = TLS disabled.
    pub tls: Option<TlsIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Jwt {
    pub issuer: String,
    pub audience: Option<String>,
    pub jwks_url: String,
    pub roles_claim: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApiOauth2 {
    pub issuer: String,
    pub audience: Option<String>,
    pub jwks_url: Option<String>,
    pub discovery_url: Option<String>,
    pub roles_claim: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Log {
    pub level: LogFilter,
    pub format: LogFormat,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Cluster {
    pub tokens: u32,
    pub lock_backend: LockBackend,
    #[serde(serialize_with = "ser_duration")]
    pub lease_duration: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub lease_renewal_interval: Duration,
    pub lease_safety_margin: f64,
    #[serde(serialize_with = "ser_duration")]
    pub rebalance_stabilization: Duration,
    pub max_concurrent_handoffs: u32,
    pub max_concurrent_takeovers: u32,
    #[serde(serialize_with = "ser_duration")]
    pub drain_timeout: Duration,
    pub discovery: Discovery,
    pub discovery_dns_name: Option<String>,
    pub memberlist: Memberlist,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Memberlist {
    pub join_members: Vec<HostPort>,
    /// `None` = the OS hostname, looked up by the runtime (§6.5.1).
    pub node_name: Option<String>,
    /// `""` = bind address or first private IP.
    pub advertise_addr: String,
    pub advertise_port: u16,
    pub bind_addr: Vec<IpAddr>,
    pub bind_port: u16,
    #[serde(serialize_with = "ser_duration")]
    pub packet_dial_timeout: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub packet_write_timeout: Duration,
    pub compression_enabled: bool,
    #[serde(serialize_with = "ser_duration")]
    pub gossip_interval: Duration,
    pub gossip_nodes: u32,
    #[serde(serialize_with = "ser_duration")]
    pub probe_interval: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub probe_timeout: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub dead_node_reclaim_time: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub push_pull_interval: Duration,
    pub retransmit_factor: u32,
    pub lifeguard_enabled: bool,
    /// `None` = TLS disabled.
    pub tls: Option<MemberlistTls>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemberlistTls {
    pub cert: Pem,
    pub key: Pem,
    /// Presence implies mutual TLS (§6.5.2).
    pub ca: Option<Pem>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tracing {
    /// `None` = tracing disabled.
    pub otlp_endpoint: Option<String>,
    pub sample_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Azure {
    /// `None` = from the credential chain.
    pub account_name: Option<String>,
}

// ---- pipelines ----

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Pipeline {
    /// The human name: the key under `pipelines` (`_pipeline` lineage value, metric label).
    pub name: String,
    pub id: String,
    pub source: Source,
    pub tables: Tables,
    pub sink: Sink,
    pub state: StateLocation,
    pub schema_evolution: SchemaEvolution,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Source {
    #[serde(rename = "type")]
    pub kind: SourceType,
    pub host: String,
    pub port: u16,
    pub db: String,
    pub credentials: Credentials,
    pub ssl_mode: SslMode,
    pub ssl_cert: Option<Pem>,
    pub ssl_key: Option<Pem>,
    pub ssl_ca: Option<Pem>,
    pub timeouts: Timeouts,
    pub toast_strategy: ToastStrategy,
    /// Set iff `type = postgres`.
    pub postgres: Option<Postgres>,
    /// Set iff `type = mysql`.
    pub mysql: Option<Mysql>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Timeouts {
    #[serde(serialize_with = "ser_duration")]
    pub connect: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub command: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub snapshot: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Postgres {
    pub publication: String,
    pub slot: String,
    pub auto_cleanup: bool,
    #[serde(serialize_with = "ser_duration")]
    pub heartbeat: Duration,
    pub slot_management: SlotManagement,
    pub slot_on_orphan: SlotOnOrphan,
    /// `None` = alert only (§9.2.1).
    pub max_wal_lag_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Mysql {
    pub replication_type: ReplicationType,
    pub tinyint1_as_bool: bool,
    pub tinyint1_detection: Tinyint1Detection,
    pub binlog_purge_action: BinlogPurgeAction,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tables {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub snapshots: Snapshots,
    #[serde(serialize_with = "ser_logical_map")]
    pub column_type_overrides: BTreeMap<String, LogicalType>,
    pub numeric_overflow_action: NumericOverflowAction,
    pub primary_key: BTreeMap<String, Vec<String>>,
    pub partitioning: BTreeMap<String, Vec<PartitionField>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Snapshots {
    pub mode: SnapshotMode,
    pub parallelism: u32,
    pub chunk_size: u64,
    pub overrides: BTreeMap<String, SnapshotMode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionField {
    pub column: String,
    pub transform: PartitionTransform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionTransform {
    Identity,
    Bucket { n: u32 },
    Truncate { width: u32 },
    Year,
    Month,
    Day,
    Hour,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Sink {
    #[serde(rename = "type")]
    pub kind: SinkType,
    pub write_mode: WriteMode,
    pub bucket: String,
    /// Defaults to the pipeline `id`.
    pub prefix: String,
    pub flush: Flush,
    /// `None` = never split (§9.4).
    pub max_transaction_batch_rows: Option<u64>,
    pub max_transaction_buffer_bytes: u64,
    pub buffer_capacity_rows: u64,
    pub metrics: SinkMetrics,
    /// Set iff `type = iceberg`.
    pub iceberg: Option<Iceberg>,
    /// Set iff `type = delta`.
    pub delta: Option<Delta>,
}

/// Flush triggers (§9.4.1); `None` = that trigger is disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Flush {
    pub seconds: Option<u64>,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SinkMetrics {
    pub per_table: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Iceberg {
    pub strategy: IcebergStrategy,
    pub catalog: Catalog,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Catalog {
    #[serde(rename = "type")]
    pub kind: CatalogKind,
    /// Exactly the block matching `type` is set.
    pub glue: Option<Glue>,
    pub hive: Option<Hive>,
    pub rest: Option<Rest>,
    pub retry: Retry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Glue {
    pub name: String,
    /// Defaults to `source.db`.
    pub database: String,
    pub strip_schema: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Hive {
    pub url: String,
    pub auth: HiveAuth,
    /// Set when `auth = ldap`.
    pub credentials: Option<Credentials>,
    /// Set when `auth = kerberos`.
    pub kerberos: Option<Kerberos>,
    pub tls: HiveTls,
    /// Defaults to `source.db`.
    pub database: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Kerberos {
    pub principal: String,
    pub keytab: FileRef,
    pub service_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HiveTls {
    pub enabled: bool,
    pub ca: Option<Pem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rest {
    pub url: String,
    pub warehouse: Option<String>,
    pub prefix: Option<String>,
    pub auth: RestAuth,
    /// Set when `auth = bearer`.
    pub token: Option<BearerToken>,
    /// Set when `auth = oauth2`.
    pub oauth2: Option<RestOauth2>,
    /// Set when `auth = sigv4`.
    pub sigv4: Option<Sigv4>,
    pub tls_ca: Option<Pem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum BearerToken {
    Inline { token: Secret },
    File { path: PathBuf, token: Secret },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestOauth2 {
    pub client: OauthClient,
    pub token_url: String,
    pub scope: Option<String>,
}

/// `client_id`/`client_secret` inline or from `oauth2.credentials_file` (§4.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum OauthClient {
    Inline {
        client_id: String,
        client_secret: Secret,
    },
    File {
        path: PathBuf,
        client_id: String,
        client_secret: Secret,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Sigv4 {
    pub region: String,
    pub service: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Retry {
    pub max_attempts: u32,
    #[serde(serialize_with = "ser_duration")]
    pub base_backoff: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub max_backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delta {
    pub column_mapping: ColumnMapping,
    pub checkpoint_interval: u64,
    #[serde(serialize_with = "ser_duration")]
    pub log_retention: Duration,
    pub timestamp_ntz_as: TimestampNtzAs,
    pub table_properties: BTreeMap<String, String>,
    pub catalog_type: DeltaCatalogType,
}

/// Where the pipeline's operational state lives (§9.6, §10).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "location", rename_all = "snake_case")]
pub enum StateLocation {
    /// `<sink.bucket>/<sink.prefix>/.pipeline-state/`: no `state.bucket` on the pipeline or in
    /// `defaults`. `defaults.state.prefix` has no effect here.
    CoLocated,
    /// `<bucket>/[<parent_prefix>/]<prefix>/`: `parent_prefix` is `defaults.state.prefix`,
    /// `prefix` is the pipeline's `state.prefix` or its `id`.
    Explicit {
        bucket: String,
        parent_prefix: Option<String>,
        prefix: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SchemaEvolution {
    pub enabled: bool,
    /// `0` = idle rescan disabled.
    #[serde(serialize_with = "ser_duration")]
    pub rescan_interval: Duration,
}
