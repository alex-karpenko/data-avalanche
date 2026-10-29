//! The configuration reference's prose (M11b): one line per key of [`crate::config_keys`], with
//! its default. The key's type, value rule and mutability come from the model itself; this table
//! carries what the model can't — what the key means — and is tied to the model by tests: every
//! key has exactly one row, in the model's order, and every default stated as a value is the one
//! the resolved config applies.

use serde::Serialize;

/// A key's default, as the reference states it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "text")]
pub enum DocDefault {
    /// No default: required, or absent means off.
    None,
    /// The value the resolved config applies, spelled as it would be written.
    Value(&'static str),
    /// A default said in words: one that depends on other keys, or that the resolved config
    /// holds as the block's absence (`tls: null` for `tls.enabled: false`).
    Note(&'static str),
}

/// The reference's row for one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FieldDoc {
    /// The key's path, as [`crate::Key::path`] spells it.
    pub path: &'static str,
    pub default: DocDefault,
    /// One or two sentences: what the key does, and the constraint an operator meets first.
    pub doc: &'static str,
}

use DocDefault::{None as Required, Note, Value};

const fn f(path: &'static str, default: DocDefault, doc: &'static str) -> FieldDoc {
    FieldDoc { path, default, doc }
}

/// One row per key, in [`crate::config_keys`]'s order.
pub const FIELD_DOCS: &[FieldDoc] = &[
    f(
        "config_version",
        Required,
        "The file's format version. Required; the only version is `1`.",
    ),
    // ---- globals (config §6) ----
    f(
        "globals.cloud",
        Value("aws"),
        "The object-storage provider for every bucket in the file. Credentials come from the provider's default chain (instance role, workload identity, environment); the file holds none. On `azure` every `bucket` is a container.",
    ),
    f(
        "globals.state.bucket",
        Required,
        "The bucket of the cluster's own state: the anchor, config generations, membership and the pipelines' leases.",
    ),
    f(
        "globals.state.prefix",
        Value(""),
        "The key prefix of the cluster's state under `bucket`; empty is the bucket's root.",
    ),
    f(
        "globals.api.address",
        Value("0.0.0.0:8080"),
        "Where the management API, `/metrics` and the health probes listen.",
    ),
    f(
        "globals.api.auth",
        Value("none"),
        "How the API authenticates a caller: `none` (open), `basic` (one user, `admin`), or a bearer JWT (`jwt`, `oauth2`) whose roles map through `role_mapping`. The health probes and `/metrics` never ask.",
    ),
    f(
        "globals.api.user",
        Required,
        "The user for `auth: basic`, or give `credentials_file`.",
    ),
    f(
        "globals.api.password",
        Required,
        "The password for `auth: basic`; write it as `${VAR}`.",
    ),
    f(
        "globals.api.credentials_file",
        Required,
        "A JSON file `{\"user\": …, \"password\": …}` in place of `user` and `password`; replace its contents and reload to rotate them.",
    ),
    f(
        "globals.api.jwt.issuer",
        Required,
        "The `iss` a token must carry (`auth: jwt`).",
    ),
    f(
        "globals.api.jwt.audience",
        Required,
        "The `aud` a token must carry, when set.",
    ),
    f(
        "globals.api.jwt.jwks_url",
        Required,
        "Where the API fetches the issuer's signing keys.",
    ),
    f(
        "globals.api.jwt.roles_claim",
        Value("roles"),
        "The claim, a dotted JSON path, that holds the caller's roles.",
    ),
    f(
        "globals.api.oauth2.issuer",
        Required,
        "The `iss` a token must carry (`auth: oauth2`).",
    ),
    f(
        "globals.api.oauth2.audience",
        Required,
        "The `aud` a token must carry, when set.",
    ),
    f(
        "globals.api.oauth2.jwks_url",
        Required,
        "Where the API fetches the signing keys; optional when `discovery_url` is set.",
    ),
    f(
        "globals.api.oauth2.discovery_url",
        Required,
        "The issuer's OpenID discovery document, which names its keys.",
    ),
    f(
        "globals.api.oauth2.roles_claim",
        Value("roles"),
        "The claim, a dotted JSON path, that holds the caller's roles.",
    ),
    f(
        "globals.api.role_mapping.*",
        Note("each role maps to itself"),
        "Maps the local roles `viewer`, `operator` and `admin` to the IdP's role names; a caller gets the highest role any of its roles maps to.",
    ),
    f(
        "globals.api.tls.enabled",
        Note("false"),
        "Serve the API over TLS. Off when a proxy in front terminates TLS.",
    ),
    f(
        "globals.api.tls.cert",
        Required,
        "The server certificate: a path, inline PEM, or `${VAR}`. Required with `enabled`.",
    ),
    f(
        "globals.api.tls.key",
        Required,
        "The certificate's private key: a path, inline PEM, or `${VAR}`. Required with `enabled`.",
    ),
    f(
        "globals.log.level",
        Value("info"),
        "The log filter: a level (`info`) or `tracing` directives (`info,data_avalanche_sink_delta=debug`). The `--log-level` flag overrides it.",
    ),
    f(
        "globals.log.format",
        Value("json"),
        "The log format on stderr: `json`, `compact` or `pretty`. The `--log-format` flag overrides it.",
    ),
    f(
        "globals.cluster.tokens",
        Value("128"),
        "Virtual nodes per node on the hash ring. The first node pins it in the cluster's state; a node with another value refuses to start.",
    ),
    f(
        "globals.cluster.lock_backend",
        Value("storage"),
        "What arbitrates a pipeline's lease: `storage`, a compare-and-swap object in `globals.state`. `kubernetes` is reserved and refused in v1.",
    ),
    f(
        "globals.cluster.lease_duration",
        Value("15s"),
        "How long a pipeline's lease lasts without a renewal: the longest a dead node's pipelines wait for another node. Pinned cluster-wide.",
    ),
    f(
        "globals.cluster.lease_renewal_interval",
        Value("5s"),
        "How often the holder renews its leases; at most a third of `lease_duration`.",
    ),
    f(
        "globals.cluster.lease_safety_margin",
        Value("0.15"),
        "The share of `lease_duration` a holder keeps in hand: it stops writing before the lease could lapse.",
    ),
    f(
        "globals.cluster.rebalance_stabilization",
        Value("15s"),
        "How long membership must stay unchanged before pipelines move to a joined node. A dead node's pipelines don't wait for it.",
    ),
    f(
        "globals.cluster.max_concurrent_handoffs",
        Value("2"),
        "How many pipelines a node drains at once when it hands them off.",
    ),
    f(
        "globals.cluster.max_concurrent_takeovers",
        Value("4"),
        "How many pipelines a node starts at once when it takes over a dead node's.",
    ),
    f(
        "globals.cluster.drain_timeout",
        Value("60s"),
        "The longest a graceful hand-off may take (flush, commit, checkpoint, release) before the next owner replays from the last checkpoint instead.",
    ),
    f(
        "globals.cluster.discovery",
        Value("static"),
        "How a node finds its peers: `static` from `memberlist.join_members`, `dns` from `discovery_dns_name`.",
    ),
    f(
        "globals.cluster.discovery_dns_name",
        Required,
        "The name whose addresses are the peers (a Kubernetes headless Service). Required with `discovery: dns`.",
    ),
    f(
        "globals.cluster.memberlist.join_members",
        Value("[]"),
        "Peers to join, as `host:port` of their advertised addresses. Empty, with no discovery, is a single node.",
    ),
    f(
        "globals.cluster.memberlist.node_name",
        Note("the host name"),
        "The node's stable identity on the ring. On Kubernetes, the StatefulSet pod's name (`${POD_NAME}`).",
    ),
    f(
        "globals.cluster.memberlist.advertise_addr",
        Note("the bind address, or the first private IP"),
        "The address peers reach this node at. On Kubernetes, `${POD_IP}`.",
    ),
    f(
        "globals.cluster.memberlist.advertise_port",
        Value("7946"),
        "The port peers reach this node at.",
    ),
    f(
        "globals.cluster.memberlist.bind_addr",
        Value("[0.0.0.0]"),
        "The addresses the gossip listens on.",
    ),
    f(
        "globals.cluster.memberlist.bind_port",
        Value("7946"),
        "The UDP port the gossip listens on.",
    ),
    f(
        "globals.cluster.memberlist.gossip_interval",
        Value("200ms"),
        "How often a node sends its pending membership updates.",
    ),
    f(
        "globals.cluster.memberlist.gossip_nodes",
        Value("3"),
        "How many peers each round of updates goes to.",
    ),
    f(
        "globals.cluster.memberlist.probe_interval",
        Value("1s"),
        "How often a node probes a peer. A suspect is declared down after `4 · max(1, log10 n)` probe intervals.",
    ),
    f(
        "globals.cluster.memberlist.probe_timeout",
        Value("500ms"),
        "How long a probe waits before asking other peers to probe; below `probe_interval`.",
    ),
    f(
        "globals.cluster.memberlist.push_pull_interval",
        Value("30s"),
        "How often a node exchanges its full member list with a peer, so a healed partition rejoins.",
    ),
    f(
        "globals.cluster.memberlist.retransmit_factor",
        Value("4"),
        "How many times an update is resent, times `ceil(log10(n + 1))` for `n` members.",
    ),
    f(
        "globals.tracing.otlp_endpoint",
        Required,
        "The OTLP/HTTP collector's base URL (`http://otel-collector:4318`); spans go to `<endpoint>/v1/traces`. Absent turns tracing off.",
    ),
    f(
        "globals.tracing.sample_ratio",
        Note("0.05 with an endpoint, else 0"),
        "The share of flushes traced, from 0 to 1.",
    ),
    f(
        "globals.azure.account_name",
        Note("from the credential chain"),
        "The Azure storage account of every container (`cloud: azure`).",
    ),
    // ---- a pipeline (config §9) ----
    f(
        "pipelines.*.id",
        Required,
        "The pipeline's immutable identity: its state, its default prefixes and its lease are keyed by it. Changing it removes the pipeline and adds a new one. Lowercase letters, digits, `_` and `-`, at most 63.",
    ),
    f(
        "pipelines.*.source.type",
        Value("postgres"),
        "The source engine: `postgres`, or `mysql` (MySQL and MariaDB).",
    ),
    f(
        "pipelines.*.source.host",
        Required,
        "The source server's host name or address.",
    ),
    f(
        "pipelines.*.source.port",
        Note("5432 for PostgreSQL, 3306 for MySQL"),
        "The source server's port.",
    ),
    f(
        "pipelines.*.source.db",
        Required,
        "The one database the pipeline replicates. PostgreSQL table patterns are `schema.table` within it; MySQL's are bare table names.",
    ),
    f(
        "pipelines.*.source.user",
        Required,
        "The replication user, or give `credentials_file`.",
    ),
    f(
        "pipelines.*.source.password",
        Required,
        "The user's password; write it as `${VAR}`. Read again at every reconnect.",
    ),
    f(
        "pipelines.*.source.credentials_file",
        Required,
        "A JSON file `{\"user\": …, \"password\": …}` in place of `user` and `password`; replace its contents and reload to rotate them.",
    ),
    f(
        "pipelines.*.source.ssl_mode",
        Value("require"),
        "TLS to the source: `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. `allow` isn't valid for MySQL.",
    ),
    f(
        "pipelines.*.source.ssl_cert",
        Required,
        "A client certificate for the source: a path, inline PEM, or `${VAR}`.",
    ),
    f(
        "pipelines.*.source.ssl_key",
        Required,
        "The client certificate's key: a path, inline PEM, or `${VAR}`.",
    ),
    f(
        "pipelines.*.source.ssl_ca",
        Required,
        "The CA that signed the source's certificate, for `verify-ca` and `verify-full`.",
    ),
    f(
        "pipelines.*.source.timeouts.connect",
        Value("10s"),
        "The longest a connection may take to set up: TCP, TLS and authentication.",
    ),
    f(
        "pipelines.*.source.timeouts.command",
        Value("30s"),
        "The longest a catalog query or command may take.",
    ),
    f(
        "pipelines.*.source.timeouts.snapshot",
        Value("6h"),
        "The longest one table's snapshot may take; at least `10s`.",
    ),
    f(
        "pipelines.*.source.toast_strategy",
        Value("carry_forward"),
        "PostgreSQL: an UPDATE that leaves a large (TOAST) column out keeps its last value (`carry_forward`) or reads the row again (`re_query`).",
    ),
    f(
        "pipelines.*.source.postgres.publication",
        Required,
        "The publication the pipeline creates and keeps in step with its tables. Unique per database across pipelines; case-sensitive.",
    ),
    f(
        "pipelines.*.source.postgres.slot",
        Required,
        "The logical replication slot. Lowercase letters, digits and `_`; unique per server and database across pipelines.",
    ),
    f(
        "pipelines.*.source.postgres.auto_cleanup",
        Value("true"),
        "Drop the slot and the publication when the pipeline is removed or its `id` changes.",
    ),
    f(
        "pipelines.*.source.postgres.heartbeat",
        Value("60s"),
        "How often the pipeline confirms its position to the server; a stream silent for three heartbeats, while the heartbeat's reads of the slot fail or find no reader, is reconnected.",
    ),
    f(
        "pipelines.*.source.postgres.slot_management",
        Value("managed"),
        "`managed` creates the slot when absent; `require_existing` uses a slot a DBA created and needs `auto_cleanup: false`.",
    ),
    f(
        "pipelines.*.source.postgres.slot_on_orphan",
        Value("halt"),
        "A slot that exists with no state of the pipeline's: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and snapshots every table again.",
    ),
    f(
        "pipelines.*.source.postgres.max_wal_lag_bytes",
        Value("null"),
        "Halt the pipeline when its slot holds more WAL than this, to protect the server's disk; at least `64MiB`. `null` only reports `wal_lag_bytes`.",
    ),
    f(
        "pipelines.*.source.mysql.replication_type",
        Value("gtid"),
        "`gtid` follows GTIDs and survives a failover (MySQL `gtid_mode=ON`, MariaDB `gtid_strict_mode=1`); `binlog` follows file and position, and halts `BINLOG_SOURCE_CHANGED` when the server changes.",
    ),
    f(
        "pipelines.*.source.mysql.tinyint1_as_bool",
        Value("true"),
        "Read `TINYINT(1)` columns as booleans.",
    ),
    f(
        "pipelines.*.source.mysql.tinyint1_detection",
        Value("information_schema"),
        "Where `TINYINT(1)` is recognised: the catalog (`information_schema`), or `none` to read every `TINYINT` as an integer.",
    ),
    f(
        "pipelines.*.source.mysql.binlog_purge_action",
        Value("error"),
        "When the server purged the binlog the pipeline must resume from: `error` halts; `re_snapshot` snapshots every table again.",
    ),
    f(
        "pipelines.*.source.mysql.server_id",
        Note("derived from the pipeline's id, in 2147483648…4294967295"),
        "The replica id the binlog stream registers with; each pipeline needs its own, or the server ends one of the streams.",
    ),
    f(
        "pipelines.*.source.mysql.heartbeat",
        Value("60s"),
        "How often an idle server sends a binlog heartbeat; a stream silent for three heartbeats, while the heartbeat's checks fail or find no binlog dump of its connection on the server, is reconnected.",
    ),
    f(
        "pipelines.*.tables.include",
        Required,
        "The tables to replicate, as globs: `schema.table` on PostgreSQL (`public.*`), bare names on MySQL (`orders`, `payment_*`). `*` stays within one name. Required, non-empty.",
    ),
    f(
        "pipelines.*.tables.exclude",
        Value("[]"),
        "Globs subtracted from `include`, in the same form.",
    ),
    f(
        "pipelines.*.tables.snapshots.mode",
        Value("never"),
        "When a table is copied before its changes stream: `never`, `initial` (once, for a table with no confirmed position), `always` (at every start), or `dump_initial` and `dump_always` (the same, with no streaming after).",
    ),
    f(
        "pipelines.*.tables.snapshots.parallelism",
        Value("1"),
        "How many tables are snapshotted at once. More than 4 warns: each PostgreSQL snapshot holds several connections.",
    ),
    f(
        "pipelines.*.tables.snapshots.chunk_size",
        Value("10000"),
        "Rows per snapshot chunk; a snapshot resumes from its last committed chunk.",
    ),
    f(
        "pipelines.*.tables.snapshots.overrides.*",
        Required,
        "A snapshot mode for one table, named exactly (no globs).",
    ),
    f(
        "pipelines.*.tables.column_type_overrides.*",
        Required,
        "A column's target type, keyed `table.column`, in the sink-neutral grammar: `int64`, `decimal(38,10)`, `string`, `timestamp_tz`, `list<int32>`…",
    ),
    f(
        "pipelines.*.tables.numeric_overflow_action",
        Value("string"),
        "A `numeric` wider than 38 digits: `string` writes its text; `error` halts the table.",
    ),
    f(
        "pipelines.*.tables.primary_key.*",
        Required,
        "The key columns of one table, in place of its primary key (for example a unique index behind `REPLICA IDENTITY USING INDEX`). Changing it means a re-snapshot.",
    ),
    f(
        "pipelines.*.tables.partitioning.*[].column",
        Required,
        "A partition column of one table (`_ts`, the commit time, is allowed).",
    ),
    f(
        "pipelines.*.tables.partitioning.*[].transform",
        Required,
        "The partition transform; the Delta sink takes `identity` only.",
    ),
    f(
        "pipelines.*.tables.partitioning.*[].n",
        Required,
        "The bucket count of a `bucket` transform.",
    ),
    f(
        "pipelines.*.tables.partitioning.*[].width",
        Required,
        "The width of a `truncate` transform.",
    ),
    f(
        "pipelines.*.sink.type",
        Required,
        "The table format: `delta`. Required, with no default. `iceberg` is accepted but refused at start: the Iceberg sink ships in v2.",
    ),
    f(
        "pipelines.*.sink.write_mode",
        Value("mirror"),
        "`mirror`: the tables mirror the source, rows upserted and deleted. `changelog` is reserved.",
    ),
    f(
        "pipelines.*.sink.bucket",
        Required,
        "The bucket (Azure: container) of the pipeline's tables.",
    ),
    f(
        "pipelines.*.sink.prefix",
        Note("the pipeline's id"),
        "The key prefix of the pipeline's tables; each table is `<prefix>/<schema>/<table>/`.",
    ),
    f(
        "pipelines.*.sink.flush.seconds",
        Value("30"),
        "Flush when the oldest buffered change is this old. `0` flushes at every transaction; `null` turns the trigger off.",
    ),
    f(
        "pipelines.*.sink.flush.rows",
        Value("100000"),
        "Flush at this many buffered rows; `null` turns the trigger off.",
    ),
    f(
        "pipelines.*.sink.flush.bytes",
        Value("512MiB"),
        "Flush at this much buffered data; `null` turns the trigger off.",
    ),
    f(
        "pipelines.*.sink.max_transaction_batch_rows",
        Value("null"),
        "Commit a source transaction larger than this in slices (its resume point stays at its start). `null` keeps every transaction whole.",
    ),
    f(
        "pipelines.*.sink.max_transaction_buffer_bytes",
        Value("1GiB"),
        "The most one transaction may buffer. Without slicing, reaching it halts the pipeline rather than run the node out of memory.",
    ),
    f(
        "pipelines.*.sink.buffer_capacity_rows",
        Value("200000"),
        "The capacity of the pipeline's internal queues: when the sink can't keep up, reading stops once they fill.",
    ),
    f(
        "pipelines.*.sink.metrics.per_table",
        Value("false"),
        "Add the `table` label to the per-table metrics; it multiplies the series.",
    ),
    f(
        "pipelines.*.sink.iceberg.strategy",
        Value("mor"),
        "v2 (the Iceberg sink): merge-on-read (`mor`) or copy-on-write (`cow`) for new tables.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.type",
        Note("the one block present"),
        "v2: the Iceberg catalog, `glue`, `hive` or `rest`.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.glue.name",
        Required,
        "v2: the Glue catalog id.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.glue.database",
        Note("source.db"),
        "v2: the Glue database.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.glue.strip_schema",
        Value("false"),
        "v2: name Glue tables `<table>` rather than `<schema>__<table>`.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.url",
        Required,
        "v2: the Hive Metastore's Thrift address.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.auth",
        Note("none"),
        "v2: the Metastore's SASL mode: `none`, `ldap` or `kerberos`.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.user",
        Required,
        "v2: the LDAP user.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.password",
        Required,
        "v2: the LDAP password.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.credentials_file",
        Required,
        "v2: a JSON file of the LDAP user and password.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.kerberos.principal",
        Required,
        "v2: the Kerberos principal.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.kerberos.keytab",
        Required,
        "v2: the Kerberos keytab.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.kerberos.service_name",
        Note("hive"),
        "v2: the Metastore's Kerberos service name.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.tls.enabled",
        Note("false"),
        "v2: TLS to the Metastore.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.tls.ca",
        Required,
        "v2: the Metastore certificate's CA.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.hive.database",
        Note("source.db"),
        "v2: the Hive database.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.url",
        Required,
        "v2: the REST catalog's base URL.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.warehouse",
        Required,
        "v2: the catalog's warehouse.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.prefix",
        Required,
        "v2: a path before the catalog's `/v1/`.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.auth",
        Value("none"),
        "v2: `none`, `bearer` or `oauth2`; `sigv4` is refused (V-116): the pinned client can't sign requests.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.token",
        Required,
        "v2: the bearer token.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.token_file",
        Required,
        "v2: a file holding the bearer token.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.oauth2.client_id",
        Required,
        "v2: the OAuth2 client id.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.oauth2.client_secret",
        Required,
        "v2: the OAuth2 client secret.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.oauth2.credentials_file",
        Required,
        "v2: a JSON file of the client id and secret.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.oauth2.token_url",
        Required,
        "v2: the IdP's token endpoint.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.oauth2.scope",
        Required,
        "v2: the scopes asked for.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.sigv4.region",
        Required,
        "v2: the SigV4 region.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.sigv4.service",
        Required,
        "v2: the SigV4 service.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.rest.tls.ca",
        Required,
        "v2: the catalog certificate's CA.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.retry.max_attempts",
        Value("5"),
        "v2: catalog retries before the pipeline degrades.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.retry.base_backoff",
        Value("1s"),
        "v2: the first retry's backoff.",
    ),
    f(
        "pipelines.*.sink.iceberg.catalog.retry.max_backoff",
        Value("30s"),
        "v2: the longest backoff.",
    ),
    f(
        "pipelines.*.sink.delta.column_mapping",
        Value("none"),
        "Delta column mapping for new tables; `name` is refused in v1, because delta-rs can't add a column to a column-mapped table.",
    ),
    f(
        "pipelines.*.sink.delta.checkpoint_interval",
        Value("10"),
        "Commits between Delta checkpoints, for new tables (`delta.checkpointInterval`).",
    ),
    f(
        "pipelines.*.sink.delta.log_retention",
        Value("30d"),
        "How long the Delta log keeps its entries, for new tables (`delta.logRetentionDuration`).",
    ),
    f(
        "pipelines.*.sink.delta.timestamp_ntz_as",
        Value("timestamp_ntz"),
        "The Delta type of a `timestamp without time zone` column in a new table: `timestamp_ntz` (reader 3 / writer 7), `timestamp` (as a UTC instant), or `string`.",
    ),
    f(
        "pipelines.*.sink.delta.table_properties.*",
        Required,
        "A table property for new tables; a `delta.*` key must be on the allow-list.",
    ),
    f(
        "pipelines.*.sink.delta.catalog.type",
        Value("none"),
        "Catalog registration of the Delta tables: `none` (path-based). The others are reserved.",
    ),
    f(
        "pipelines.*.sink.delta.merge_memory_limit",
        Value("null"),
        "The memory ceiling of one table's MERGE; at least `16MiB` when set. A MERGE over it halts the table `SINK_REJECTED`.",
    ),
    f(
        "pipelines.*.state.bucket",
        Note("sink.bucket"),
        "The bucket of the pipeline's state (positions, snapshot cursors, status). By default it sits under the sink's prefix, in `.pipeline-state/`.",
    ),
    f(
        "pipelines.*.state.prefix",
        Note("the pipeline's id"),
        "The state's prefix when `state.bucket` is set; `defaults.state.prefix` is its parent.",
    ),
    f(
        "pipelines.*.schema_evolution.enabled",
        Value("true"),
        "Apply the source's safe schema changes to the tables; off, any change halts its table.",
    ),
    f(
        "pipelines.*.schema_evolution.rescan_interval",
        Value("60m"),
        "How often an idle table's schema is read again; `0` turns the rescan off.",
    ),
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::time::Duration;

    use serde_json::Value as Json;

    use super::*;
    use crate::{ByteSize, FingerprintKey, config_keys, key_mutability, load_str};

    #[test]
    fn every_key_of_the_model_has_one_row_in_its_order() {
        let keys: Vec<String> = config_keys().into_iter().map(|k| k.path).collect();
        let rows: Vec<&str> = FIELD_DOCS.iter().map(|d| d.path).collect();
        assert_eq!(rows, keys);
    }

    #[test]
    fn every_key_has_a_mutability_class() {
        for k in config_keys() {
            assert!(key_mutability(&k.path).is_some(), "{}", k.path);
        }
    }

    /// A PostgreSQL, a MySQL and two Iceberg pipelines (REST and Glue: valid in the file, refused
    /// at start), each with every defaulted key left out; `{auth}` is the API's block, so the JWT
    /// and OAuth2 blocks each resolve once.
    const MINIMAL: &str = r#"
config_version: 1
globals:
  state: { bucket: state }
  api: {auth}
pipelines:
  pg:
    id: pg
    source: { host: h, db: d, user: u, password: p, postgres: { publication: pb, slot: sl } }
    tables: { include: ["public.*"] }
    sink: { type: delta, bucket: lake }
  my:
    id: my
    source: { type: mysql, host: h, db: d, user: u, password: p }
    tables: { include: ["*"] }
    sink: { type: delta, bucket: lake }
  rest:
    id: rest
    source: { host: h, db: d, user: u, password: p, postgres: { publication: pr, slot: sr } }
    tables: { include: ["public.*"] }
    sink: { type: iceberg, bucket: lake, iceberg: { catalog: { rest: { url: "https://c" } } } }
  glue:
    id: glue
    source: { host: h, db: d, user: u, password: p, postgres: { publication: pg2, slot: sg } }
    tables: { include: ["public.*"] }
    sink: { type: iceberg, bucket: lake, iceberg: { catalog: { glue: { name: "1" } } } }
"#;

    fn at<'a>(root: &'a Json, path: &str) -> Option<&'a Json> {
        path.split('.').try_fold(root, |v, seg| v.get(seg))
    }

    /// The resolved value at a key's path: the one global value, or the value in each pipeline
    /// that leaves the key out of the file (`written` is the file as JSON).
    fn resolved_values<'a>(resolved: &'a Json, written: &Json, path: &str) -> Vec<&'a Json> {
        match path.strip_prefix("pipelines.*.") {
            Some(rest) => resolved["pipelines"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(id, _)| at(&written["pipelines"][id.as_str()], rest).is_none())
                .filter_map(|(_, p)| at(p, rest))
                .collect(),
            None => at(resolved, path)
                .filter(|v| !v.is_null())
                .into_iter()
                .collect(),
        }
    }

    /// Whether a default as the reference spells it is the resolved value, compared by the key's
    /// type: `60s` is `1m`, `512MiB` is `536870912`.
    fn same(expects: &str, written: &str, resolved: &Json) -> bool {
        match (expects, resolved) {
            (_, Json::Null) => written == "null",
            ("a duration", Json::String(s)) => {
                humantime::parse_duration(written).ok() == humantime::parse_duration(s).ok()
            }
            ("a byte size", Json::Number(n)) => {
                ByteSize::from_str(written).ok().map(|b| b.bytes()) == n.as_u64()
            }
            (_, Json::Number(n)) => written.parse::<f64>().ok() == n.as_f64(),
            (_, Json::Bool(b)) => written == b.to_string(),
            (_, Json::String(s)) => written == s,
            (_, Json::Array(_)) => {
                let list: Vec<String> = written
                    .trim_matches(['[', ']'])
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
                serde_json::to_value(list).unwrap() == *resolved
            }
            _ => false,
        }
    }

    #[test]
    fn every_default_stated_as_a_value_is_the_resolved_one() {
        let env: BTreeMap<String, String> = BTreeMap::new();
        // The first document that resolves a key is the one it is checked in: the API's own
        // default auth is the plain one's.
        let resolved: Vec<(Json, Json)> = [
            "{}",
            "{ auth: jwt, jwt: { issuer: i, jwks_url: \"https://k\" } }",
            "{ auth: oauth2, oauth2: { issuer: i, jwks_url: \"https://k\" } }",
        ]
        .iter()
        .map(|auth| {
            let text = MINIMAL.replace("{auth}", auth);
            let loaded = load_str(&text, &env, &FingerprintKey::from_bytes([7; 32])).unwrap();
            let written: Json = serde_saphyr::from_str(&text).unwrap();
            (serde_json::to_value(&loaded.config).unwrap(), written)
        })
        .collect();
        let keys: BTreeMap<String, &'static str> = config_keys()
            .into_iter()
            .map(|k| (k.path, k.expects))
            .collect();
        let mut wrong = Vec::new();
        for doc in FIELD_DOCS {
            let DocDefault::Value(written) = doc.default else {
                continue;
            };
            let path = crate::keys::resolved_path(doc.path);
            let values = resolved
                .iter()
                .map(|(r, written)| resolved_values(r, written, path))
                .find(|v| !v.is_empty())
                .unwrap_or_default();
            if values.is_empty() {
                wrong.push(format!("{}: not in the resolved config", doc.path));
            }
            for v in values {
                if !same(keys[doc.path], written, v) {
                    wrong.push(format!("{}: {written}, resolved {v}", doc.path));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[test]
    fn durations_compare_by_value() {
        assert!(same("a duration", "60s", &Json::String("1m".into())));
        assert!(!same("a duration", "60s", &Json::String("30s".into())));
        assert_eq!(
            humantime::parse_duration("30d").unwrap(),
            Duration::from_secs(30 * 86_400)
        );
    }
}
