//! The master validation table (config §13) as data: every V-rule's ID, severity, locus, the
//! input it needs, the field it covers and its text. The checks themselves live in `rules`; this
//! table drives the fixture-coverage test and, later, the generated configuration reference.

use crate::diag::Locus;

/// The severity column of §13. `ErrorAndWarning` rules error outside a hard range and warn outside
/// a recommended band (e.g. V-026 `tokens`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleSeverity {
    Error,
    Warning,
    ErrorAndWarning,
}

/// The §13 "Locus" column: where a rule's diagnostics are reported. When a rule runs (at startup,
/// at schema resolution) is its [`RuleInput`], not its locus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleLocus {
    /// `config`: the config as a whole (`globals`, cross-pipeline checks).
    Config,
    /// `pipeline`: a named pipeline.
    Pipeline,
    /// `config / pipeline`: either, wherever the value is written (an unset `${VAR}`, V-063).
    ConfigOrPipeline,
    /// `reload`: a rule that can only fire on reload (V-061).
    Reload,
}

impl RuleLocus {
    /// Whether a diagnostic of this rule may sit at `locus` and `path`. A value written in
    /// `defaults` (§8.6) reports locus `config` and a `defaults.…` path, whatever the rule's
    /// column says: the column describes the rule as it applies to a pipeline (§13).
    pub fn allows(self, locus: &Locus, path: &str) -> bool {
        let in_defaults = path == "defaults" || path.starts_with("defaults.");
        if in_defaults && *locus == Locus::Config {
            return true;
        }
        match self {
            Self::Config => *locus == Locus::Config,
            Self::Pipeline => matches!(locus, Locus::Pipeline(_)),
            Self::ConfigOrPipeline => matches!(locus, Locus::Config | Locus::Pipeline(_)),
            Self::Reload => *locus == Locus::Reload,
        }
    }
}

/// What a rule needs besides the config file itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleInput {
    /// The config file (Phase 1, and again after resolution).
    Config,
    /// The environment and referenced files (Phase 2, §13.9).
    Resolution,
    /// The previously running config (reload only, V-061).
    Reload,
    /// The cluster anchor stored in `globals.state` (startup, V-062/V-076/V-094).
    ClusterAnchor,
    /// The pipeline's resolved table set (pipeline start or table-set change).
    TableSet,
    /// Source column types (schema resolution, V-079).
    SourceSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleMeta {
    pub id: &'static str,
    pub severity: RuleSeverity,
    /// The §13 "Locus" column.
    pub locus: RuleLocus,
    pub input: RuleInput,
    /// The §13 "Path / Scope" column.
    pub field: &'static str,
    /// The §13 "Rule" column (shortened where long).
    pub text: &'static str,
}

const fn rule(
    id: &'static str,
    severity: RuleSeverity,
    locus: RuleLocus,
    input: RuleInput,
    field: &'static str,
    text: &'static str,
) -> RuleMeta {
    RuleMeta {
        id,
        severity,
        locus,
        input,
        field,
        text,
    }
}

use RuleInput::*;
// An alias, not a glob: `RuleLocus` shares `Config` and `Reload` with `RuleInput`.
use RuleLocus as L;
use RuleSeverity::*;

/// All V-rules of config §13, ordered by ID.
pub const RULES: &[RuleMeta] = &[
    rule(
        "V-001",
        Error,
        L::Pipeline,
        Config,
        r#"source"#,
        r#"After merge, the engine block matching type is present when that engine has required fields: postgres must be present; an absent mysql block means all defaults. (The foreign block: V-003.)"#,
    ),
    rule(
        "V-002",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog"#,
        r#"Exactly one sub-block (glue/hive/rest); if type is set it must equal the present block."#,
    ),
    rule(
        "V-003",
        Error,
        L::Pipeline,
        Config,
        r#"source.{postgres,mysql}"#,
        r#"Engine block present must match type; the foreign engine block is forbidden."#,
    ),
    rule(
        "V-004",
        Error,
        L::Config,
        Config,
        r#"top"#,
        r#"pipelines key present (may be {})."#,
    ),
    rule(
        "V-005",
        Error,
        L::Config,
        Config,
        r#"top"#,
        r#"globals present."#,
    ),
    rule(
        "V-006",
        Error,
        L::Config,
        Config,
        r#"globals.state.bucket"#,
        r#"Present."#,
    ),
    rule(
        "V-007",
        Error,
        L::Pipeline,
        Config,
        r#"tables.include"#,
        r#"Present and non-empty."#,
    ),
    rule(
        "V-008",
        Error,
        L::Pipeline,
        Config,
        r#"source.host"#,
        r#"Resolvable (pipeline or defaults)."#,
    ),
    rule(
        "V-009",
        Error,
        L::Pipeline,
        Config,
        r#"source.db"#,
        r#"Resolvable."#,
    ),
    rule(
        "V-010",
        Error,
        L::Pipeline,
        Config,
        r#"source credentials"#,
        r#"Resolvable: either user+password, or credentials_file (exactly one — V-065)."#,
    ),
    rule(
        "V-011",
        Error,
        L::Pipeline,
        Config,
        r#"source.postgres.{publication,slot}"#,
        r#"Present when type=postgres."#,
    ),
    rule(
        "V-012",
        Error,
        L::Pipeline,
        Config,
        r#"sink.bucket"#,
        r#"Resolvable."#,
    ),
    rule(
        "V-013",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog"#,
        r#"When sink.type = iceberg: a catalog block resolvable (pipeline or defaults)."#,
    ),
    rule(
        "V-014",
        Error,
        L::Config,
        Config,
        r#"config_version"#,
        r#"Present and supported by this binary."#,
    ),
    rule(
        "V-015",
        Error,
        L::Config,
        Config,
        r#"globals.cloud"#,
        r#"∈ {aws, google, azure}."#,
    ),
    rule(
        "V-016",
        Error,
        L::Config,
        Config,
        r#"globals.api.auth"#,
        r#"∈ {none, basic, jwt, oauth2}."#,
    ),
    rule(
        "V-017",
        Error,
        L::Config,
        Config,
        r#"globals.log.level"#,
        r#"Valid level / EnvFilter syntax."#,
    ),
    rule(
        "V-018",
        Error,
        L::Config,
        Config,
        r#"globals.log.format"#,
        r#"∈ {json, compact, pretty}."#,
    ),
    rule(
        "V-019",
        Error,
        L::Pipeline,
        Config,
        r#"source.type"#,
        r#"∈ {postgres, mysql}."#,
    ),
    rule(
        "V-020",
        Error,
        L::Pipeline,
        Config,
        r#"source.ssl_mode"#,
        r#"∈ {disable, allow, prefer, require, verify-ca, verify-full}."#,
    ),
    rule(
        "V-021",
        Error,
        L::Pipeline,
        Config,
        r#"source.mysql.replication_type"#,
        r#"∈ {binlog, gtid}."#,
    ),
    rule(
        "V-022",
        Error,
        L::Pipeline,
        Config,
        r#"tables.snapshots.mode (and overrides values)"#,
        r#"∈ {never, initial, always, dump_always, dump_initial}."#,
    ),
    rule(
        "V-023",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.strategy"#,
        r#"∈ {mor, cow}."#,
    ),
    rule(
        "V-025",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.hive.auth / sink.iceberg.catalog.rest.auth"#,
        r#"hive ∈ {none,ldap,kerberos}; rest ∈ {none,bearer,oauth2,sigv4}."#,
    ),
    rule(
        "V-026",
        ErrorAndWarning,
        L::Config,
        Config,
        r#"globals.cluster.tokens"#,
        r#"1 ≤ tokens ≤ 4096. Outside 64–256 → also warning."#,
    ),
    rule(
        "V-027",
        Error,
        L::Pipeline,
        Config,
        r#"tables.snapshots.parallelism"#,
        r#"≥ 1."#,
    ),
    rule(
        "V-028",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.glue"#,
        r#"Requires globals.cloud = aws."#,
    ),
    rule(
        "V-029",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.auth = sigv4"#,
        r#"Requires globals.cloud = aws."#,
    ),
    rule(
        "V-030",
        Error,
        L::Pipeline,
        Config,
        r#"source.ssl_mode = allow"#,
        r#"Invalid for MySQL."#,
    ),
    rule(
        "V-033",
        Error,
        L::Pipeline,
        Config,
        r#"tables.snapshots.overrides keys"#,
        r#"Must be full schema.table; globs forbidden."#,
    ),
    rule(
        "V-034",
        Warning,
        L::Pipeline,
        TableSet,
        r#"tables.snapshots.overrides keys"#,
        r#"Key not in the resolved include/exclude set."#,
    ),
    rule(
        "V-035",
        Error,
        L::Pipeline,
        Config,
        r#"sink.flush"#,
        r#"All of seconds/rows/bytes null/disabled."#,
    ),
    rule(
        "V-036",
        Warning,
        L::Pipeline,
        Config,
        r#"sink.flush.seconds = null"#,
        r#"With at least one other trigger set (time-based disabled)."#,
    ),
    rule(
        "V-037",
        Warning,
        L::Pipeline,
        Config,
        r#"sink.flush.seconds"#,
        r#"0 < seconds < 5 (write amplification)."#,
    ),
    rule(
        "V-038",
        Warning,
        L::Pipeline,
        Config,
        r#"tables.snapshots.parallelism"#,
        r#"> 4 (connection budget)."#,
    ),
    rule(
        "V-040",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.hive.auth = ldap"#,
        r#"Requires user+password, or credentials_file (exactly one — V-065)."#,
    ),
    rule(
        "V-041",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.hive.auth = kerberos"#,
        r#"Requires kerberos.principal + kerberos.keytab."#,
    ),
    rule(
        "V-042",
        Warning,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.hive.auth = ldap without tls.enabled"#,
        r#"Password in cleartext."#,
    ),
    rule(
        "V-043",
        Warning,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.hive.tls.enabled = true w/ private CA but no tls.ca"#,
        r#"May fail cert validation against system store."#,
    ),
    rule(
        "V-044",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.auth = bearer"#,
        r#"Requires token or token_file (exactly one — V-070)."#,
    ),
    rule(
        "V-045",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.auth = oauth2"#,
        r#"Requires token_url, and either oauth2.client_id+client_secret or oauth2.credentials_file (exactly one — V-066)."#,
    ),
    rule(
        "V-046",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.auth = sigv4"#,
        r#"Requires sigv4.region + service."#,
    ),
    rule(
        "V-047",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.auth = sigv4"#,
        r#"Requires globals.cloud = aws. (Same constraint as V-029.)"#,
    ),
    rule(
        "V-048",
        Warning,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.auth = oauth2, token_url = <url>/v1/oauth/tokens"#,
        r#"Deprecated built-in endpoint (Iceberg ≥1.6, removed 2.0)."#,
    ),
    rule(
        "V-049",
        Error,
        L::Config,
        Config,
        r#"globals.api.auth = basic"#,
        r#"Requires user+password, or credentials_file (exactly one — V-065)."#,
    ),
    rule(
        "V-050",
        Error,
        L::Config,
        Config,
        r#"globals.api.auth = jwt"#,
        r#"Requires jwt.issuer + jwt.jwks_url."#,
    ),
    rule(
        "V-051",
        Error,
        L::Config,
        Config,
        r#"globals.api.auth = oauth2"#,
        r#"Requires oauth2.issuer + (jwks_url or discovery_url)."#,
    ),
    rule(
        "V-052",
        Warning,
        L::Config,
        Config,
        r#"globals.api.auth ≠ none without tls.enabled"#,
        r#"Credentials/tokens in cleartext unless TLS terminated upstream."#,
    ),
    rule(
        "V-053",
        Error,
        L::Config,
        Config,
        r#"globals.api.tls.enabled = true"#,
        r#"Requires cert + key."#,
    ),
    rule(
        "V-054",
        Error,
        L::Config,
        Config,
        r#"globals.cluster.memberlist.tls.enabled = true"#,
        r#"Requires cert + key."#,
    ),
    rule(
        "V-055",
        Error,
        L::Pipeline,
        Config,
        r#"pipeline.id"#,
        r#"Matches ^[a-z0-9][a-z0-9_-]{0,62}$, ≤ 63 chars."#,
    ),
    rule(
        "V-056",
        Error,
        L::Config,
        Config,
        r#"pipeline.id"#,
        r#"Unique across all pipelines."#,
    ),
    rule(
        "V-057",
        Error,
        L::Pipeline,
        Config,
        r#"pipeline name (map key)"#,
        r#"Matches ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$."#,
    ),
    rule(
        "V-058",
        Error,
        L::Config,
        Config,
        r#"source.postgres.slot"#,
        r#"Matches ^[a-z_][a-z0-9_]{0,62}$; unique per source server (host + port) across pipelines — PostgreSQL scopes slot names to the server, not the database."#,
    ),
    rule(
        "V-059",
        ErrorAndWarning,
        L::Pipeline,
        Config,
        r#"source.postgres.publication"#,
        r#"Valid identifier (warning when it needs quoting); unique per source database (host + port + db) across pipelines: an error when a sharing pipeline has slot_management managed, a warning otherwise."#,
    ),
    rule(
        "V-060",
        Error,
        L::Config,
        Config,
        r#"defaults"#,
        r#"Must not set per-pipeline-unique resource names (postgres.publication, postgres.slot, id)."#,
    ),
    rule(
        "V-061",
        Error,
        L::Reload,
        Reload,
        r#"any PROC-class field"#,
        r#"Changed during a live reload."#,
    ),
    rule(
        "V-062",
        Error,
        L::Config,
        ClusterAnchor,
        r#"globals.cluster.tokens"#,
        r#"Differs from the cluster-established value in globals.state."#,
    ),
    rule(
        "V-063",
        Error,
        L::ConfigOrPipeline,
        Resolution,
        r#"any ${VAR}"#,
        r#"Unset with no :-default."#,
    ),
    rule(
        "V-064",
        Error,
        L::Pipeline,
        Config,
        r#"tables.include/exclude globs and snapshots.overrides keys, when source.type = mysql"#,
        r#"Must be bare table names — no . namespace prefix (the database is fixed by source.db)."#,
    ),
    rule(
        "V-065",
        Error,
        L::ConfigOrPipeline,
        Config,
        r#"globals.api, source, sink.iceberg.catalog.hive"#,
        r#"credentials_file is mutually exclusive with user/password in the same block — providing both (any of user/password alongside …"#,
    ),
    rule(
        "V-066",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.oauth2"#,
        r#"oauth2.credentials_file is mutually exclusive with oauth2.client_id/client_secret — providing both is an error."#,
    ),
    rule(
        "V-067",
        Error,
        L::ConfigOrPipeline,
        Resolution,
        r#"any credentials_file (incl. oauth2.credentials_file)"#,
        r#"Resolved path exists and is readable; content is non-empty, valid JSON, and contains the required non-empty fields (user+password, or …"#,
    ),
    rule(
        "V-068",
        Error,
        L::ConfigOrPipeline,
        Resolution,
        r#"TLS material (cert/key/ca across globals.api, memberlist, source.ssl_*, sink.iceberg.catalog.*)"#,
        r#"Resolved value (inline / file / env) is non-empty and parses as valid PEM (and a private key where a key is expected)."#,
    ),
    rule(
        "V-069",
        Error,
        L::Pipeline,
        Resolution,
        r#"any other referenced file (e.g. sink.iceberg.catalog.hive.kerberos.keytab)"#,
        r#"Resolved path exists, is readable, and is non-empty."#,
    ),
    rule(
        "V-070",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest"#,
        r#"token_file is mutually exclusive with token — providing both is an error."#,
    ),
    rule(
        "V-071",
        Error,
        L::Pipeline,
        Resolution,
        r#"sink.iceberg.catalog.rest.token_file"#,
        r#"Resolved path exists and is readable; content (the raw token, trailing whitespace trimmed) is non-empty."#,
    ),
    rule(
        "V-072",
        ErrorAndWarning,
        L::Config,
        Config,
        r#"globals.cluster.lease_safety_margin"#,
        r#"0 ≤ margin ≤ 0.5. Outside 0.05–0.30 → also warning. Fraction of lease_duration held back as the renewal safety-deadline guard …"#,
    ),
    rule(
        "V-073",
        Error,
        L::Config,
        Config,
        r#"globals.cluster.lease_renewal_interval"#,
        r#"Must be ≤ lease_duration / 3 (the holder must get ≥2 renew attempts inside the renewal safety deadline). Hard error — promoted from the …"#,
    ),
    rule(
        "V-074",
        Error,
        L::Config,
        Config,
        r#"globals.cluster.max_concurrent_handoffs / globals.cluster.max_concurrent_takeovers"#,
        r#"≥ 1 (per-node concurrency caps; local resource knobs, not cluster-wide-consistent). Clustering doc §8.5/§20.2 (G-9, D-10)."#,
    ),
    rule(
        "V-075",
        Error,
        L::Config,
        Config,
        r#"globals.cluster.lock_backend"#,
        r#"∈ {storage, kubernetes}. kubernetes is reserved / not yet implemented → error in v1. Clustering doc §5.6 (D-2)."#,
    ),
    rule(
        "V-076",
        Error,
        L::Config,
        ClusterAnchor,
        r#"globals.cluster.lock_backend"#,
        r#"Differs from the cluster-established value in the globals.state anchor."#,
    ),
    rule(
        "V-077",
        Warning,
        L::Pipeline,
        Config,
        r#"source.toast_strategy = re_query with tables.snapshots.mode = always"#,
        r#"re_query on high-volume snapshots may saturate the control connection pool."#,
    ),
    rule(
        "V-078",
        Error,
        L::Pipeline,
        Config,
        r#"tables.column_type_overrides values"#,
        r#"Each value parses in the sink-neutral logical type grammar (§9.3); else error "invalid logical type <value> in column_type_overrides for …"#,
    ),
    rule(
        "V-079",
        Warning,
        L::Pipeline,
        SourceSchema,
        r#"tables.numeric_overflow_action = error with any source numeric column lacking declared precision"#,
        r#"Such a column may halt the table unexpectedly under error; consider a column_type_overrides entry. (Emitted at schema resolution.)"#,
    ),
    rule(
        "V-080",
        Error,
        L::Pipeline,
        Config,
        r#"source.toast_strategy"#,
        r#"∈ {carry_forward, re_query} (PostgreSQL only; ignored for MySQL). research/Type_Mapping.md."#,
    ),
    rule(
        "V-081",
        Error,
        L::Pipeline,
        Config,
        r#"source.mysql.tinyint1_detection"#,
        r#"∈ {information_schema, none}."#,
    ),
    rule(
        "V-082",
        Error,
        L::Pipeline,
        Config,
        r#"tables.numeric_overflow_action"#,
        r#"∈ {string, error}."#,
    ),
    rule(
        "V-083",
        Error,
        L::Pipeline,
        Config,
        r#"sink.type"#,
        r#"∈ {iceberg, delta} and compiled into this binary (Cargo feature sink-iceberg / sink-delta). v1: iceberg is a known value this version …"#,
    ),
    rule(
        "V-084",
        Error,
        L::Pipeline,
        Config,
        r#"sink.{iceberg,delta}"#,
        r#"After merge, the format block matching sink.type is present when that format has required fields: iceberg must be present; an absent delta block means all defaults. The foreign block is forbidden."#,
    ),
    rule(
        "V-085",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.strategy"#,
        r#"Only meaningful with write_mode: mirror."#,
    ),
    rule(
        "V-086",
        Error,
        L::Pipeline,
        Config,
        r#"tables.partitioning transforms"#,
        r#"Each transform supported by the pipeline's sink (Delta v1: identity only). Active once tables.partitioning is promoted from §16."#,
    ),
    rule(
        "V-087",
        Error,
        L::Pipeline,
        Config,
        r#"tables.column_type_overrides values"#,
        r#"Logical type mappable by the pipeline's sink."#,
    ),
    rule(
        "V-088",
        Error,
        L::Pipeline,
        Config,
        r#"sink.delta.catalog.type"#,
        r#"∈ {none, glue, unity, hive}; anything but none is reserved → error in v1 (D-MS.12)."#,
    ),
    rule(
        "V-089",
        Error,
        L::Pipeline,
        Config,
        r#"sink.write_mode"#,
        r#"∈ {mirror, changelog}; changelog is reserved → error in v1 (D-MS.5)."#,
    ),
    rule(
        "V-091",
        Error,
        L::Pipeline,
        Config,
        r#"sink.type"#,
        r#"Present after merge (pipeline or defaults). No implicit default."#,
    ),
    rule(
        "V-092",
        Error,
        L::Pipeline,
        Config,
        r#"sink.delta.column_mapping"#,
        r#"∈ {none, name}; name → error in v1 (delta-rs refuses ADD COLUMN under column mapping; R-1)."#,
    ),
    rule(
        "V-093",
        Error,
        L::Pipeline,
        Config,
        r#"sink.delta.table_properties keys"#,
        r#"Only allow-listed delta.* keys; delta.columnMapping.mode / delta.enableDeletionVectors / delta.appendOnly are managed by the sink and …"#,
    ),
    rule(
        "V-094",
        Error,
        L::Config,
        ClusterAnchor,
        r#"globals.cluster.lease_duration"#,
        r#"Differs from the cluster-established value in the globals.state anchor (lease_duration_ms)."#,
    ),
    rule(
        "V-095",
        Error,
        L::Config,
        Config,
        r#"globals.cluster.discovery"#,
        r#"∈ {static, dns}."#,
    ),
    rule(
        "V-096",
        Error,
        L::Config,
        Config,
        r#"globals.cluster.discovery = dns"#,
        r#"Requires discovery_dns_name."#,
    ),
    rule(
        "V-097",
        Error,
        L::Config,
        Config,
        r#"globals.tracing.sample_ratio"#,
        r#"∈ [0, 1]."#,
    ),
    rule(
        "V-098",
        Warning,
        L::Config,
        Config,
        r#"globals.tracing.sample_ratio > 0 without otlp_endpoint"#,
        r#"Sampling configured but tracing disabled."#,
    ),
    rule(
        "V-099",
        Error,
        L::Config,
        Config,
        r#"globals.api.role_mapping"#,
        r#"Keys ∈ {viewer, operator, admin}; values are lists of strings."#,
    ),
    rule(
        "V-100",
        Warning,
        L::Config,
        Config,
        r#"globals.api.role_mapping with auth ∈ {none, basic}"#,
        r#"Ignored — roles come from the open mode / single basic principal."#,
    ),
    rule(
        "V-101",
        Error,
        L::Config,
        Config,
        r#"globals.azure.account_name"#,
        r#"Valid storage-account name (3–24 chars, lowercase letters and digits)."#,
    ),
    rule(
        "V-102",
        Error,
        L::Pipeline,
        Config,
        r#"source.postgres.slot_management / slot_on_orphan"#,
        r#"slot_management ∈ {managed, require_existing}; slot_on_orphan ∈ {halt, adopt_resnapshot}."#,
    ),
    rule(
        "V-103",
        Error,
        L::Pipeline,
        Config,
        r#"source.postgres.slot_management = require_existing"#,
        r#"Requires auto_cleanup = false (the app must not drop a slot it did not create)."#,
    ),
    rule(
        "V-104",
        Error,
        L::Pipeline,
        Config,
        r#"source.postgres.max_wal_lag_bytes"#,
        r#"≥ 64 MiB when set."#,
    ),
    rule(
        "V-105",
        Error,
        L::Pipeline,
        Config,
        r#"source.mysql.binlog_purge_action"#,
        r#"∈ {error, re_snapshot}."#,
    ),
    rule(
        "V-106",
        ErrorAndWarning,
        L::Pipeline,
        Config,
        r#"tables.primary_key / tables.partitioning keys"#,
        r#"Same key form as snapshots.overrides (full schema.table / MySQL bare name, no globs — V-033/V-064); column lists non-empty. A key not in …"#,
    ),
    rule(
        "V-107",
        Error,
        L::Pipeline,
        Config,
        r#"tables.partitioning fields"#,
        r#"transform ∈ {identity, bucket, truncate, year, month, day, hour}; bucket requires n ≥ 1; truncate requires width ≥ 1."#,
    ),
    rule(
        "V-108",
        ErrorAndWarning,
        L::Pipeline,
        Config,
        r#"tables.snapshots.chunk_size"#,
        r#"≥ 1. > 1,000,000 → also warning (long chunk transactions, large memory)."#,
    ),
    rule(
        "V-109",
        ErrorAndWarning,
        L::Pipeline,
        Config,
        r#"sink.max_transaction_batch_rows"#,
        r#"≥ 1 when set. < 1000 → also warning (commit amplification)."#,
    ),
    rule(
        "V-110",
        Error,
        L::Pipeline,
        Config,
        r#"sink.max_transaction_buffer_bytes"#,
        r#"≥ 16 MiB."#,
    ),
    rule(
        "V-111",
        Error,
        L::Pipeline,
        Config,
        r#"sink.buffer_capacity_rows"#,
        r#"≥ 1."#,
    ),
    rule(
        "V-112",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.retry"#,
        r#"max_attempts ≥ 1; base_backoff ≤ max_backoff."#,
    ),
    rule(
        "V-113",
        Error,
        L::Pipeline,
        TableSet,
        r#"sink.iceberg.catalog.glue.strip_schema = true"#,
        r#"Two resolved tables share a bare name across schemas. Evaluated when the table set is resolved (start / table-set change); refuses the …"#,
    ),
    rule(
        "V-114",
        Error,
        L::Pipeline,
        Config,
        r#"sink.delta.timestamp_ntz_as"#,
        r#"∈ {timestamp_ntz, timestamp, string}."#,
    ),
    rule(
        "V-115",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.type / sink.iceberg.catalog.hive"#,
        r#"hive → error: the pinned iceberg-rust HMS catalog cannot commit (update_table is FeatureUnsupported; DA-CAT-6). Lifted when a pinned …"#,
    ),
    rule(
        "V-116",
        Error,
        L::Pipeline,
        Config,
        r#"sink.iceberg.catalog.rest.auth"#,
        r#"sigv4 → error: the pinned iceberg-rust REST client has no SigV4 signing (DA-CAT-7; upstream #3082/#3092). none/bearer/oauth2 unaffected; …"#,
    ),
    rule(
        "V-117",
        Error,
        L::Pipeline,
        Config,
        r#"tables.include/exclude globs, when source.type = postgres"#,
        r#"Must be schema.table: one . between a non-empty schema part and a non-empty table part (globs allowed within each part)."#,
    ),
    rule(
        "V-118",
        Error,
        L::Pipeline,
        Config,
        r#"source.timeouts.snapshot"#,
        r#"≥ 10s."#,
    ),
];

/// The registry entry for `id` (`"V-007"`).
pub fn rule_meta(id: &str) -> Option<&'static RuleMeta> {
    RULES.iter().find(|r| r.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_every_spec_rule_once_in_order() {
        assert_eq!(RULES.len(), 113);
        assert!(
            RULES.windows(2).all(|w| w[0].id < w[1].id),
            "sorted, no duplicates"
        );
        // IDs retired by the spec (§15 item 18) or never allocated in it.
        for gone in ["V-024", "V-031", "V-032", "V-039", "V-090"] {
            assert!(rule_meta(gone).is_none(), "{gone}");
        }
        assert_eq!(
            rule_meta("V-026").unwrap().severity,
            RuleSeverity::ErrorAndWarning
        );
        assert_eq!(rule_meta("V-061").unwrap().input, RuleInput::Reload);
        assert_eq!(rule_meta("V-061").unwrap().locus, RuleLocus::Reload);
        assert_eq!(
            rule_meta("V-063").unwrap().locus,
            RuleLocus::ConfigOrPipeline
        );
    }

    #[test]
    fn a_locus_allows_its_own_diagnostics_and_defaults() {
        let p = Locus::Pipeline("orders".into());
        assert!(RuleLocus::Config.allows(&Locus::Config, "globals.api"));
        assert!(!RuleLocus::Config.allows(&p, "pipelines.orders.id"));
        assert!(RuleLocus::Pipeline.allows(&p, "pipelines.orders.source"));
        assert!(!RuleLocus::Pipeline.allows(&Locus::Config, "globals"));
        assert!(!RuleLocus::Pipeline.allows(&Locus::Reload, "pipelines.orders"));
        assert!(RuleLocus::ConfigOrPipeline.allows(&Locus::Config, "globals.api"));
        assert!(RuleLocus::ConfigOrPipeline.allows(&p, "pipelines.orders.source"));
        assert!(!RuleLocus::ConfigOrPipeline.allows(&Locus::Reload, ""));
        assert!(RuleLocus::Reload.allows(&Locus::Reload, "pipelines.orders.id"));
        assert!(!RuleLocus::Reload.allows(&p, "pipelines.orders.id"));
        // A value written in `defaults` reports locus `config`, whatever the rule's column.
        assert!(RuleLocus::Pipeline.allows(&Locus::Config, "defaults.source.postgres"));
        assert!(RuleLocus::Pipeline.allows(&Locus::Config, "defaults"));
        assert!(!RuleLocus::Pipeline.allows(&Locus::Config, "defaultsx"));
        assert!(!RuleLocus::Reload.allows(&p, "defaults.sink"));
    }
}
