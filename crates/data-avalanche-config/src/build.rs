//! Phase 3 (config §11): the merged raw model becomes the [`ResolvedConfig`], with every default of
//! the spec's field tables applied. The build visits every leaf, so a leaf no rule reads still
//! reports a coercion failure. Presence is the rules' job: an absent required value makes the
//! build return `None` without a diagnostic of its own. Files are read only when the context is
//! resolving (§11: Phase 1 touches neither the environment nor the filesystem).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::coerce::{LeafType, LogFilter};
use crate::ctx::{At, Ctx};
use crate::enums::*;
use crate::leaf::{Leaf, Tri};
use crate::material::{Pem, PemKind, load_pem, read_file_ref, read_json_pair, read_token_file};
use crate::path::YamlPath;
use crate::raw::*;
use crate::resolved::*;
use crate::units::{ByteSize, HostPort};

type L<T> = Option<Leaf<T>>;

/// Spec defaults (config §6–§9) that other modules also need.
pub const DEFAULT_TOKENS: u32 = 128;
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const DEFAULT_LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_FLUSH_SECONDS: u64 = 30;
pub const DEFAULT_FLUSH_ROWS: u64 = 100_000;
pub const DEFAULT_FLUSH_BYTES: u64 = 512 * ByteSize::MIB;

/// A field that has a default: absent → `Some(default)`; present → its value (`None` if unknown
/// or invalid).
fn or<T: LeafType>(ctx: &mut Ctx, leaf: &L<T>, at: &At, key: &str, default: T) -> Option<T> {
    match leaf {
        None => Some(default),
        Some(l) => ctx.value(l, &at.key(key)),
    }
}

/// A required field: absent → `None` (its presence rule reports it).
fn req<T: LeafType>(ctx: &mut Ctx, leaf: &L<T>, at: &At, key: &str) -> Option<T> {
    leaf.as_ref().and_then(|l| ctx.value(l, &at.key(key)))
}

/// An optional field without a default: absent → `Some(None)`.
fn opt<T: LeafType>(ctx: &mut Ctx, leaf: &L<T>, at: &At, key: &str) -> Option<Option<T>> {
    match leaf {
        None => Some(None),
        Some(l) => ctx.value(l, &at.key(key)).map(Some),
    }
}

/// A tri-state field (§9.4.1): absent → `default`, `null` → disabled, value → the value.
fn tri<T: LeafType>(
    ctx: &mut Ctx,
    t: &Tri<T>,
    at: &At,
    key: &str,
    default: Option<T>,
) -> Option<Option<T>> {
    match t {
        None => Some(default),
        Some(None) => Some(None),
        Some(Some(l)) => ctx.value(l, &at.key(key)).map(Some),
    }
}

/// A list of scalars; absent → empty. Every element is visited.
fn list<T: LeafType>(
    ctx: &mut Ctx,
    v: &Option<Vec<Leaf<T>>>,
    at: &At,
    key: &str,
) -> Option<Vec<T>> {
    let at = at.key(key);
    let items: Vec<Option<T>> = v
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, l)| ctx.value(l, &at.index(i)))
        .collect();
    items.into_iter().collect()
}

/// A map of scalars; absent → empty. Every value is visited.
fn map<T: LeafType>(
    ctx: &mut Ctx,
    m: &Option<BTreeMap<String, Leaf<T>>>,
    at: &At,
    key: &str,
) -> Option<BTreeMap<String, T>> {
    let at = at.key(key);
    let items: Vec<(String, Option<T>)> = m
        .iter()
        .flatten()
        .map(|(k, l)| (k.clone(), ctx.value(l, &at.key(k))))
        .collect();
    items.into_iter().map(|(k, v)| v.map(|v| (k, v))).collect()
}

/// F1 (config §13.9): a diagnostic-safe label for a file-reference field's value (`cert`/`key`/
/// `ca`, `credentials_file`, `token_file`, `keytab`) — what a message calls it, never the value
/// itself. A diagnostic must show only what the operator wrote, never a value that came from the
/// environment or from a file's content:
/// - templated (substitution changed the text): the leaf's written text, hiding the resolved
///   value, e.g. `the file named by "${VAR}" (resolved value hidden)`;
/// - literal but not path-shaped (JSON pasted into the field, inline PEM text, control characters,
///   or over 256 bytes): a placeholder, since printing it back out would print the pasted value;
/// - otherwise (a plausible literal path): the value itself, quoted.
fn file_label<T>(leaf: &Leaf<T>, text: &str) -> String {
    if leaf.text() != text {
        return format!(
            "the file named by {:?} (resolved value hidden)",
            leaf.text()
        );
    }
    let looks_like_a_path = text.len() <= 256
        && !text.chars().any(|c| c.is_whitespace() || c.is_control())
        && !text.starts_with('{')
        && !text.starts_with("-----");
    if looks_like_a_path {
        format!("{text:?}")
    } else {
        "the configured value (not shown: it does not look like a file path)".to_string()
    }
}

/// A path to a file read in Phase 2, plus its `file_label`. `None` in the structural pass (not an
/// error) and on failure.
fn file_path(ctx: &mut Ctx, leaf: &Leaf<String>, at: &At) -> Option<(PathBuf, String)> {
    let text = ctx.text(leaf, at)?;
    ctx.is_resolving()
        .then(|| (PathBuf::from(&text), file_label(leaf, &text)))
}

/// Optional PEM (`cert`/`key`/`ca`, §4.4); V-068 on invalid material.
fn pem(ctx: &mut Ctx, leaf: &L<String>, at: &At, key: &str, kind: PemKind) -> Option<Option<Pem>> {
    let Some(l) = leaf else { return Some(None) };
    let at = at.key(key);
    let text = ctx.text(l, &at)?;
    if !ctx.is_resolving() {
        return None;
    }
    let label = file_label(l, &text);
    match load_pem(&text, &label, kind) {
        Ok(p) => Some(Some(p)),
        Err(e) => {
            ctx.error("V-068", &at, l.loc(), e);
            None
        }
    }
}

/// A `user`/`password` pair or a `credentials_file` (§4.6); V-067 on a bad file. `None` when
/// neither (or both) is given: V-010/V-040/V-049 and V-065 report that.
fn credentials(
    ctx: &mut Ctx,
    user: &L<String>,
    password: &L<String>,
    file: &L<String>,
    at: &At,
) -> Option<Credentials> {
    let user_v = user.as_ref().and_then(|l| ctx.text(l, &at.key("user")));
    let password_v = password
        .as_ref()
        .and_then(|l| ctx.text(l, &at.key("password")));
    match (file, user, password) {
        (Some(f), None, None) => {
            let at = at.key("credentials_file");
            let (path, label) = file_path(ctx, f, &at)?;
            match read_json_pair(&path, &label, "user", "password") {
                Ok((user, password)) => Some(Credentials::File {
                    path,
                    user,
                    password,
                }),
                Err(e) => {
                    ctx.error("V-067", &at, f.loc(), e);
                    None
                }
            }
        }
        (None, Some(_), Some(_)) => Some(Credentials::Inline {
            user: user_v?,
            password: crate::secret::Secret::new(password_v?),
        }),
        _ => None,
    }
}

fn read_or_report<T>(
    ctx: &mut Ctx,
    rule: &str,
    at: &At,
    leaf: &Leaf<String>,
    read: impl FnOnce(&Path, &str) -> Result<T, String>,
) -> Option<(PathBuf, T)> {
    let (path, label) = file_path(ctx, leaf, at)?;
    match read(&path, &label) {
        Ok(v) => Some((path, v)),
        Err(e) => {
            ctx.error(rule, at, leaf.loc(), e);
            None
        }
    }
}

// ---- globals ----

fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
    let at = At::config(YamlPath::root().key("globals"));
    let cloud = or(ctx, &g.cloud, &at, "cloud", Cloud::Aws);
    let state = g.state.clone().unwrap_or_default();
    let state_at = at.key("state");
    let bucket = req(ctx, &state.bucket, &state_at, "bucket");
    let prefix = or(ctx, &state.prefix, &state_at, "prefix", String::new());
    let api = api(&g.api.clone().unwrap_or_default(), &at.key("api"), ctx);
    let log = g.log.clone().unwrap_or_default();
    let log_at = at.key("log");
    let level = or(ctx, &log.level, &log_at, "level", LogFilter("info".into()));
    let format = or(ctx, &log.format, &log_at, "format", LogFormat::Json);
    let cluster = cluster(
        &g.cluster.clone().unwrap_or_default(),
        &at.key("cluster"),
        ctx,
    );
    let tracing = g.tracing.clone().unwrap_or_default();
    let tracing_at = at.key("tracing");
    let otlp_endpoint = opt(ctx, &tracing.otlp_endpoint, &tracing_at, "otlp_endpoint");
    let ratio_default = if tracing.otlp_endpoint.is_some() {
        0.05
    } else {
        0.0
    };
    let sample_ratio = or(
        ctx,
        &tracing.sample_ratio,
        &tracing_at,
        "sample_ratio",
        ratio_default,
    );
    let azure = g.azure.clone().unwrap_or_default();
    let account_name = opt(ctx, &azure.account_name, &at.key("azure"), "account_name");
    Some(Globals {
        cloud: cloud?,
        state: GlobalState {
            bucket: bucket?,
            prefix: prefix?,
        },
        api: api?,
        log: Log {
            level: level?,
            format: format?,
        },
        cluster: cluster?,
        tracing: Tracing {
            otlp_endpoint: otlp_endpoint?,
            sample_ratio: sample_ratio?,
        },
        azure: Azure {
            account_name: account_name?,
        },
    })
}

fn api(a: &RawApi, at: &At, ctx: &mut Ctx) -> Option<Api> {
    let address = or(
        ctx,
        &a.address,
        at,
        "address",
        HostPort {
            host: "0.0.0.0".into(),
            port: 8080,
        },
    );
    let auth = or(ctx, &a.auth, at, "auth", ApiAuth::None);
    let creds = credentials(ctx, &a.user, &a.password, &a.credentials_file, at);
    let jwt = a.jwt.as_ref().map(|j| {
        let at = at.key("jwt");
        let issuer = req(ctx, &j.issuer, &at, "issuer");
        let audience = opt(ctx, &j.audience, &at, "audience");
        let jwks_url = req(ctx, &j.jwks_url, &at, "jwks_url");
        let roles_claim = or(ctx, &j.roles_claim, &at, "roles_claim", "roles".to_string());
        Some(Jwt {
            issuer: issuer?,
            audience: audience?,
            jwks_url: jwks_url?,
            roles_claim: roles_claim?,
        })
    });
    let oauth2 = a.oauth2.as_ref().map(|o| {
        let at = at.key("oauth2");
        let issuer = req(ctx, &o.issuer, &at, "issuer");
        let audience = opt(ctx, &o.audience, &at, "audience");
        let jwks_url = opt(ctx, &o.jwks_url, &at, "jwks_url");
        let discovery_url = opt(ctx, &o.discovery_url, &at, "discovery_url");
        let roles_claim = or(ctx, &o.roles_claim, &at, "roles_claim", "roles".to_string());
        Some(ApiOauth2 {
            issuer: issuer?,
            audience: audience?,
            jwks_url: jwks_url?,
            discovery_url: discovery_url?,
            roles_claim: roles_claim?,
        })
    });
    let role_mapping = role_mapping(ctx, &a.role_mapping, &at.key("role_mapping"));
    let tls = a.tls.clone().unwrap_or_default();
    let tls_at = at.key("tls");
    let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
    let cert = pem(ctx, &tls.cert, &tls_at, "cert", PemKind::Certificates);
    let key = pem(ctx, &tls.key, &tls_at, "key", PemKind::PrivateKey);
    // Only the block the auth mode uses is kept: an unused block can't fail the load.
    let auth = auth?;
    let (credentials, jwt, oauth2) = match auth {
        ApiAuth::None => (None, None, None),
        ApiAuth::Basic => (Some(creds?), None, None),
        ApiAuth::Jwt => (None, Some(jwt??), None),
        ApiAuth::Oauth2 => (None, None, Some(oauth2??)),
    };
    let tls = match enabled? {
        true => Some(TlsIdentity {
            cert: cert??,
            key: key??,
        }),
        false => None,
    };
    Some(Api {
        address: address?,
        auth,
        credentials,
        jwt,
        oauth2,
        role_mapping: role_mapping?,
        tls,
    })
}

/// `role_mapping` keys must be roles (V-099; reported by the rules); values are IdP role strings.
fn role_mapping(
    ctx: &mut Ctx,
    m: &Option<BTreeMap<String, Vec<Leaf<String>>>>,
    at: &At,
) -> Option<BTreeMap<Role, Vec<String>>> {
    let mut out = BTreeMap::new();
    let mut ok = true;
    for (k, values) in m.iter().flatten() {
        let vals = list(ctx, &Some(values.clone()), at, k);
        match (k.parse::<Role>(), vals) {
            (Ok(role), Some(v)) => {
                out.insert(role, v);
            }
            _ => ok = false,
        }
    }
    ok.then_some(out)
}

fn cluster(c: &RawCluster, at: &At, ctx: &mut Ctx) -> Option<Cluster> {
    let tokens = or(ctx, &c.tokens, at, "tokens", DEFAULT_TOKENS);
    let lock_backend = or(
        ctx,
        &c.lock_backend,
        at,
        "lock_backend",
        LockBackend::Storage,
    );
    let lease_duration = or(
        ctx,
        &c.lease_duration,
        at,
        "lease_duration",
        DEFAULT_LEASE_DURATION,
    );
    let lease_renewal_interval = or(
        ctx,
        &c.lease_renewal_interval,
        at,
        "lease_renewal_interval",
        DEFAULT_LEASE_RENEWAL_INTERVAL,
    );
    let lease_safety_margin = or(ctx, &c.lease_safety_margin, at, "lease_safety_margin", 0.15);
    let rebalance_stabilization = or(
        ctx,
        &c.rebalance_stabilization,
        at,
        "rebalance_stabilization",
        Duration::from_secs(15),
    );
    let max_concurrent_handoffs = or(
        ctx,
        &c.max_concurrent_handoffs,
        at,
        "max_concurrent_handoffs",
        2,
    );
    let max_concurrent_takeovers = or(
        ctx,
        &c.max_concurrent_takeovers,
        at,
        "max_concurrent_takeovers",
        4,
    );
    let drain_timeout = or(
        ctx,
        &c.drain_timeout,
        at,
        "drain_timeout",
        Duration::from_secs(60),
    );
    let discovery = or(ctx, &c.discovery, at, "discovery", Discovery::Static);
    let discovery_dns_name = opt(ctx, &c.discovery_dns_name, at, "discovery_dns_name");
    let memberlist = memberlist(
        &c.memberlist.clone().unwrap_or_default(),
        &at.key("memberlist"),
        ctx,
    );
    Some(Cluster {
        tokens: tokens?,
        lock_backend: lock_backend?,
        lease_duration: lease_duration?,
        lease_renewal_interval: lease_renewal_interval?,
        lease_safety_margin: lease_safety_margin?,
        rebalance_stabilization: rebalance_stabilization?,
        max_concurrent_handoffs: max_concurrent_handoffs?,
        max_concurrent_takeovers: max_concurrent_takeovers?,
        drain_timeout: drain_timeout?,
        discovery: discovery?,
        discovery_dns_name: discovery_dns_name?,
        memberlist: memberlist?,
    })
}

fn memberlist(m: &RawMemberlist, at: &At, ctx: &mut Ctx) -> Option<Memberlist> {
    let secs = Duration::from_secs;
    let join_members = list(ctx, &m.join_members, at, "join_members");
    let node_name = opt(ctx, &m.node_name, at, "node_name");
    let advertise_addr = or(ctx, &m.advertise_addr, at, "advertise_addr", String::new());
    let advertise_port = or(ctx, &m.advertise_port, at, "advertise_port", 7946);
    let bind_addr = match &m.bind_addr {
        None => Some(vec![IpAddr::V4(Ipv4Addr::UNSPECIFIED)]),
        some => list(ctx, some, at, "bind_addr"),
    };
    let bind_port = or(ctx, &m.bind_port, at, "bind_port", 7946);
    let packet_dial_timeout = or(
        ctx,
        &m.packet_dial_timeout,
        at,
        "packet_dial_timeout",
        secs(5),
    );
    let packet_write_timeout = or(
        ctx,
        &m.packet_write_timeout,
        at,
        "packet_write_timeout",
        secs(5),
    );
    let compression_enabled = or(ctx, &m.compression_enabled, at, "compression_enabled", true);
    let gossip_interval = or(
        ctx,
        &m.gossip_interval,
        at,
        "gossip_interval",
        Duration::from_millis(200),
    );
    let gossip_nodes = or(ctx, &m.gossip_nodes, at, "gossip_nodes", 3);
    let probe_interval = or(ctx, &m.probe_interval, at, "probe_interval", secs(1));
    let probe_timeout = or(
        ctx,
        &m.probe_timeout,
        at,
        "probe_timeout",
        Duration::from_millis(500),
    );
    let dead_node_reclaim_time = or(
        ctx,
        &m.dead_node_reclaim_time,
        at,
        "dead_node_reclaim_time",
        secs(30),
    );
    let push_pull_interval = or(
        ctx,
        &m.push_pull_interval,
        at,
        "push_pull_interval",
        secs(30),
    );
    let retransmit_factor = or(ctx, &m.retransmit_factor, at, "retransmit_factor", 4);
    let lifeguard_enabled = or(ctx, &m.lifeguard_enabled, at, "lifeguard_enabled", true);
    let tls = m.tls.clone().unwrap_or_default();
    let tls_at = at.key("tls");
    let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
    let cert = pem(ctx, &tls.cert, &tls_at, "cert", PemKind::Certificates);
    let key = pem(ctx, &tls.key, &tls_at, "key", PemKind::PrivateKey);
    let ca = pem(ctx, &tls.ca, &tls_at, "ca", PemKind::Certificates);
    let tls = match enabled? {
        true => Some(MemberlistTls {
            cert: cert??,
            key: key??,
            ca: ca?,
        }),
        false => None,
    };
    Some(Memberlist {
        join_members: join_members?,
        node_name: node_name?,
        advertise_addr: advertise_addr?,
        advertise_port: advertise_port?,
        bind_addr: bind_addr?,
        bind_port: bind_port?,
        packet_dial_timeout: packet_dial_timeout?,
        packet_write_timeout: packet_write_timeout?,
        compression_enabled: compression_enabled?,
        gossip_interval: gossip_interval?,
        gossip_nodes: gossip_nodes?,
        probe_interval: probe_interval?,
        probe_timeout: probe_timeout?,
        dead_node_reclaim_time: dead_node_reclaim_time?,
        push_pull_interval: push_pull_interval?,
        retransmit_factor: retransmit_factor?,
        lifeguard_enabled: lifeguard_enabled?,
        tls,
    })
}

// ---- pipelines ----

fn pipeline(
    name: &str,
    p: &RawPipeline,
    parent_state_prefix: Option<&Leaf<String>>,
    ctx: &mut Ctx,
) -> Option<Pipeline> {
    let at = At::pipeline(name, YamlPath::root().key("pipelines").key(name));
    let id = req(ctx, &p.id, &at, "id");
    let source = source(
        &p.source.clone().unwrap_or_default(),
        &at.key("source"),
        ctx,
    );
    let tables = tables(
        &p.tables.clone().unwrap_or_default(),
        &at.key("tables"),
        ctx,
    );
    let db = source.as_ref().map(|s| s.db.clone());
    let sink = sink(
        &p.sink.clone().unwrap_or_default(),
        &at.key("sink"),
        id.as_deref(),
        db.as_deref(),
        ctx,
    );
    let state = p.state.clone().unwrap_or_default();
    let state_at = at.key("state");
    let state_bucket = opt(ctx, &state.bucket, &state_at, "bucket");
    let state_prefix = opt(ctx, &state.prefix, &state_at, "prefix");
    let parent_at = At::config(YamlPath::root().key("defaults").key("state").key("prefix"));
    let parent = match parent_state_prefix {
        None => Some(None),
        Some(l) => ctx
            .value(l, &parent_at)
            .map(|v| Some(v).filter(|p: &String| !p.is_empty())),
    };
    let evo = p.schema_evolution.clone().unwrap_or_default();
    let evo_at = at.key("schema_evolution");
    let enabled = or(ctx, &evo.enabled, &evo_at, "enabled", true);
    let rescan_interval = or(
        ctx,
        &evo.rescan_interval,
        &evo_at,
        "rescan_interval",
        Duration::from_secs(3600),
    );
    let id = id?;
    let state = match state_bucket? {
        None => StateLocation::CoLocated,
        Some(bucket) => StateLocation::Explicit {
            bucket,
            parent_prefix: parent?,
            prefix: state_prefix?.unwrap_or_else(|| id.clone()),
        },
    };
    Some(Pipeline {
        name: name.to_string(),
        id,
        source: source?,
        tables: tables?,
        sink: sink?,
        state,
        schema_evolution: SchemaEvolution {
            enabled: enabled?,
            rescan_interval: rescan_interval?,
        },
    })
}

fn source(s: &RawSource, at: &At, ctx: &mut Ctx) -> Option<Source> {
    let kind = or(ctx, &s.kind, at, "type", SourceType::Postgres);
    let host = req(ctx, &s.host, at, "host");
    let port = opt(ctx, &s.port, at, "port");
    let db = req(ctx, &s.db, at, "db");
    let credentials = credentials(ctx, &s.user, &s.password, &s.credentials_file, at);
    let ssl_mode = or(ctx, &s.ssl_mode, at, "ssl_mode", SslMode::Require);
    let ssl_cert = pem(ctx, &s.ssl_cert, at, "ssl_cert", PemKind::Certificates);
    let ssl_key = pem(ctx, &s.ssl_key, at, "ssl_key", PemKind::PrivateKey);
    let ssl_ca = pem(ctx, &s.ssl_ca, at, "ssl_ca", PemKind::Certificates);
    let t = s.timeouts.clone().unwrap_or_default();
    let t_at = at.key("timeouts");
    let connect = or(ctx, &t.connect, &t_at, "connect", Duration::from_secs(10));
    let command = or(ctx, &t.command, &t_at, "command", Duration::from_secs(30));
    let snapshot = or(
        ctx,
        &t.snapshot,
        &t_at,
        "snapshot",
        Duration::from_secs(6 * 3600),
    );
    let toast_strategy = or(
        ctx,
        &s.toast_strategy,
        at,
        "toast_strategy",
        ToastStrategy::CarryForward,
    );
    let postgres = s
        .postgres
        .as_ref()
        .map(|p| postgres(p, &at.key("postgres"), ctx));
    // Every `mysql` field has a default, so an absent block means all defaults (D-M1.5).
    let mysql = mysql(
        s.mysql.as_ref().unwrap_or(&RawMysql::default()),
        &at.key("mysql"),
        ctx,
    );
    let kind = kind?;
    let (postgres, mysql) = match kind {
        SourceType::Postgres => (Some(postgres??), None),
        SourceType::Mysql => (None, Some(mysql?)),
    };
    let default_port = match kind {
        SourceType::Postgres => 5432,
        SourceType::Mysql => 3306,
    };
    Some(Source {
        kind,
        host: host?,
        port: port?.unwrap_or(default_port),
        db: db?,
        credentials: credentials?,
        ssl_mode: ssl_mode?,
        ssl_cert: ssl_cert?,
        ssl_key: ssl_key?,
        ssl_ca: ssl_ca?,
        timeouts: Timeouts {
            connect: connect?,
            command: command?,
            snapshot: snapshot?,
        },
        toast_strategy: toast_strategy?,
        postgres,
        mysql,
    })
}

fn postgres(p: &RawPostgres, at: &At, ctx: &mut Ctx) -> Option<Postgres> {
    let publication = req(ctx, &p.publication, at, "publication");
    let slot = req(ctx, &p.slot, at, "slot");
    let auto_cleanup = or(ctx, &p.auto_cleanup, at, "auto_cleanup", true);
    let heartbeat = or(ctx, &p.heartbeat, at, "heartbeat", Duration::from_secs(60));
    let slot_management = or(
        ctx,
        &p.slot_management,
        at,
        "slot_management",
        SlotManagement::Managed,
    );
    let slot_on_orphan = or(
        ctx,
        &p.slot_on_orphan,
        at,
        "slot_on_orphan",
        SlotOnOrphan::Halt,
    );
    let max_wal_lag_bytes = tri(ctx, &p.max_wal_lag_bytes, at, "max_wal_lag_bytes", None);
    Some(Postgres {
        publication: publication?,
        slot: slot?,
        auto_cleanup: auto_cleanup?,
        heartbeat: heartbeat?,
        slot_management: slot_management?,
        slot_on_orphan: slot_on_orphan?,
        max_wal_lag_bytes: max_wal_lag_bytes?.map(ByteSize::bytes),
    })
}

fn mysql(m: &RawMysql, at: &At, ctx: &mut Ctx) -> Option<Mysql> {
    let replication_type = or(
        ctx,
        &m.replication_type,
        at,
        "replication_type",
        ReplicationType::Gtid,
    );
    let tinyint1_as_bool = or(ctx, &m.tinyint1_as_bool, at, "tinyint1_as_bool", true);
    let tinyint1_detection = or(
        ctx,
        &m.tinyint1_detection,
        at,
        "tinyint1_detection",
        Tinyint1Detection::InformationSchema,
    );
    let binlog_purge_action = or(
        ctx,
        &m.binlog_purge_action,
        at,
        "binlog_purge_action",
        BinlogPurgeAction::Error,
    );
    Some(Mysql {
        replication_type: replication_type?,
        tinyint1_as_bool: tinyint1_as_bool?,
        tinyint1_detection: tinyint1_detection?,
        binlog_purge_action: binlog_purge_action?,
    })
}

fn tables(t: &RawTables, at: &At, ctx: &mut Ctx) -> Option<Tables> {
    let include = list(ctx, &t.include, at, "include");
    let exclude = list(ctx, &t.exclude, at, "exclude");
    let s = t.snapshots.clone().unwrap_or_default();
    let s_at = at.key("snapshots");
    let mode = or(ctx, &s.mode, &s_at, "mode", SnapshotMode::Never);
    let parallelism = or(ctx, &s.parallelism, &s_at, "parallelism", 1);
    let chunk_size = or(ctx, &s.chunk_size, &s_at, "chunk_size", 10_000);
    let overrides = map(ctx, &s.overrides, &s_at, "overrides");
    let cto_at = at.key("column_type_overrides");
    let column_type_overrides: Option<BTreeMap<_, _>> = t
        .column_type_overrides
        .iter()
        .flatten()
        .map(|(column, l)| {
            let v = ctx.value_with(l, &cto_at.key(column), |text, _| {
                format!("invalid logical type {text} in column_type_overrides for {column}")
            });
            (column.clone(), v)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|(k, v)| v.map(|v| (k, v)))
        .collect();
    let numeric_overflow_action = or(
        ctx,
        &t.numeric_overflow_action,
        at,
        "numeric_overflow_action",
        NumericOverflowAction::String,
    );
    let pk_at = at.key("primary_key");
    let primary_key: Option<BTreeMap<_, _>> = t
        .primary_key
        .iter()
        .flatten()
        .map(|(table, cols)| (table.clone(), list(ctx, &Some(cols.clone()), &pk_at, table)))
        .collect::<Vec<_>>()
        .into_iter()
        .map(|(k, v)| v.map(|v| (k, v)))
        .collect();
    let part_at = at.key("partitioning");
    let partitioning: Option<BTreeMap<_, _>> = t
        .partitioning
        .iter()
        .flatten()
        .map(|(table, fields)| {
            let at = part_at.key(table);
            let built: Vec<Option<PartitionField>> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| partition_field(f, &at.index(i), ctx))
                .collect();
            (table.clone(), built.into_iter().collect::<Option<Vec<_>>>())
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|(k, v)| v.map(|v| (k, v)))
        .collect();
    Some(Tables {
        include: include?,
        exclude: exclude?,
        snapshots: Snapshots {
            mode: mode?,
            parallelism: parallelism?,
            chunk_size: chunk_size?,
            overrides: overrides?,
        },
        column_type_overrides: column_type_overrides?,
        numeric_overflow_action: numeric_overflow_action?,
        primary_key: primary_key?,
        partitioning: partitioning?,
    })
}

fn partition_field(f: &RawPartitionField, at: &At, ctx: &mut Ctx) -> Option<PartitionField> {
    let column = req(ctx, &f.column, at, "column");
    let transform = req(ctx, &f.transform, at, "transform");
    let n = opt(ctx, &f.n, at, "n");
    let width = opt(ctx, &f.width, at, "width");
    let transform = match transform? {
        Transform::Identity => PartitionTransform::Identity,
        Transform::Bucket => PartitionTransform::Bucket { n: n?? },
        Transform::Truncate => PartitionTransform::Truncate { width: width?? },
        Transform::Year => PartitionTransform::Year,
        Transform::Month => PartitionTransform::Month,
        Transform::Day => PartitionTransform::Day,
        Transform::Hour => PartitionTransform::Hour,
    };
    Some(PartitionField {
        column: column?,
        transform,
    })
}

fn sink(s: &RawSink, at: &At, id: Option<&str>, db: Option<&str>, ctx: &mut Ctx) -> Option<Sink> {
    let kind = req(ctx, &s.kind, at, "type");
    let write_mode = or(ctx, &s.write_mode, at, "write_mode", WriteMode::Mirror);
    let bucket = req(ctx, &s.bucket, at, "bucket");
    let prefix = opt(ctx, &s.prefix, at, "prefix");
    let f = s.flush.clone().unwrap_or_default();
    let f_at = at.key("flush");
    let seconds = tri(
        ctx,
        &f.seconds,
        &f_at,
        "seconds",
        Some(DEFAULT_FLUSH_SECONDS),
    );
    let rows = tri(ctx, &f.rows, &f_at, "rows", Some(DEFAULT_FLUSH_ROWS));
    let bytes = tri(
        ctx,
        &f.bytes,
        &f_at,
        "bytes",
        Some(ByteSize(DEFAULT_FLUSH_BYTES)),
    );
    let batch_rows = tri(
        ctx,
        &s.max_transaction_batch_rows,
        at,
        "max_transaction_batch_rows",
        None,
    );
    let buffer_bytes = or(
        ctx,
        &s.max_transaction_buffer_bytes,
        at,
        "max_transaction_buffer_bytes",
        ByteSize(ByteSize::GIB),
    );
    let capacity = or(
        ctx,
        &s.buffer_capacity_rows,
        at,
        "buffer_capacity_rows",
        200_000,
    );
    let metrics = s.metrics.clone().unwrap_or_default();
    let per_table = or(
        ctx,
        &metrics.per_table,
        &at.key("metrics"),
        "per_table",
        false,
    );
    let iceberg = s
        .iceberg
        .as_ref()
        .map(|i| iceberg(i, &at.key("iceberg"), db, ctx));
    // Every `delta` field has a default, so an absent block means all defaults (D-M1.5).
    let delta = delta(
        s.delta.as_ref().unwrap_or(&RawDelta::default()),
        &at.key("delta"),
        ctx,
    );
    let kind = kind?;
    let (iceberg, delta) = match kind {
        SinkType::Iceberg => (Some(iceberg??), None),
        SinkType::Delta => (None, Some(delta?)),
    };
    Some(Sink {
        kind,
        write_mode: write_mode?,
        bucket: bucket?,
        prefix: prefix?.or_else(|| id.map(str::to_string))?,
        flush: Flush {
            seconds: seconds?,
            rows: rows?,
            bytes: bytes?.map(ByteSize::bytes),
        },
        max_transaction_batch_rows: batch_rows?,
        max_transaction_buffer_bytes: buffer_bytes?.bytes(),
        buffer_capacity_rows: capacity?,
        metrics: SinkMetrics {
            per_table: per_table?,
        },
        iceberg,
        delta,
    })
}

fn iceberg(i: &RawIceberg, at: &At, db: Option<&str>, ctx: &mut Ctx) -> Option<Iceberg> {
    let strategy = or(ctx, &i.strategy, at, "strategy", IcebergStrategy::Mor);
    let catalog = i
        .catalog
        .as_ref()
        .map(|c| catalog(c, &at.key("catalog"), db, ctx));
    Some(Iceberg {
        strategy: strategy?,
        catalog: catalog??,
    })
}

fn catalog(c: &RawCatalog, at: &At, db: Option<&str>, ctx: &mut Ctx) -> Option<Catalog> {
    let declared = opt(ctx, &c.kind, at, "type");
    let glue = c.glue.as_ref().map(|g| {
        let at = at.key("glue");
        let name = req(ctx, &g.name, &at, "name");
        let database = opt(ctx, &g.database, &at, "database");
        let strip_schema = or(ctx, &g.strip_schema, &at, "strip_schema", false);
        Some(Glue {
            name: name?,
            database: database?.or_else(|| db.map(str::to_string))?,
            strip_schema: strip_schema?,
        })
    });
    let hive = c.hive.as_ref().map(|h| hive(h, &at.key("hive"), db, ctx));
    let rest = c.rest.as_ref().map(|r| rest(r, &at.key("rest"), ctx));
    let r = c.retry.clone().unwrap_or_default();
    let r_at = at.key("retry");
    let max_attempts = or(ctx, &r.max_attempts, &r_at, "max_attempts", 5);
    let base_backoff = or(
        ctx,
        &r.base_backoff,
        &r_at,
        "base_backoff",
        Duration::from_secs(1),
    );
    let max_backoff = or(
        ctx,
        &r.max_backoff,
        &r_at,
        "max_backoff",
        Duration::from_secs(30),
    );
    let inferred = match (&glue, &hive, &rest) {
        (Some(_), None, None) => Some(CatalogKind::Glue),
        (None, Some(_), None) => Some(CatalogKind::Hive),
        (None, None, Some(_)) => Some(CatalogKind::Rest),
        _ => None,
    };
    let kind = declared?.or(inferred)?;
    let (glue, hive, rest) = match kind {
        CatalogKind::Glue => (Some(glue??), None, None),
        CatalogKind::Hive => (None, Some(hive??), None),
        CatalogKind::Rest => (None, None, Some(rest??)),
    };
    Some(Catalog {
        kind,
        glue,
        hive,
        rest,
        retry: Retry {
            max_attempts: max_attempts?,
            base_backoff: base_backoff?,
            max_backoff: max_backoff?,
        },
    })
}

fn hive(h: &RawHive, at: &At, db: Option<&str>, ctx: &mut Ctx) -> Option<Hive> {
    let url = req(ctx, &h.url, at, "url");
    let auth = or(ctx, &h.auth, at, "auth", HiveAuth::None);
    let creds = credentials(ctx, &h.user, &h.password, &h.credentials_file, at);
    let kerberos = h.kerberos.as_ref().map(|k| {
        let at = at.key("kerberos");
        let principal = req(ctx, &k.principal, &at, "principal");
        let keytab = k.keytab.as_ref().and_then(|l| {
            read_or_report(ctx, "V-069", &at.key("keytab"), l, read_file_ref).map(|(_, f)| f)
        });
        let service_name = or(
            ctx,
            &k.service_name,
            &at,
            "service_name",
            "hive".to_string(),
        );
        Some(Kerberos {
            principal: principal?,
            keytab: keytab?,
            service_name: service_name?,
        })
    });
    let tls = h.tls.clone().unwrap_or_default();
    let tls_at = at.key("tls");
    let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
    let ca = pem(ctx, &tls.ca, &tls_at, "ca", PemKind::Certificates);
    let database = opt(ctx, &h.database, at, "database");
    let auth = auth?;
    let (credentials, kerberos) = match auth {
        HiveAuth::None => (None, None),
        HiveAuth::Ldap => (Some(creds?), None),
        HiveAuth::Kerberos => (None, Some(kerberos??)),
    };
    Some(Hive {
        url: url?,
        auth,
        credentials,
        kerberos,
        tls: HiveTls {
            enabled: enabled?,
            ca: ca?,
        },
        database: database?.or_else(|| db.map(str::to_string))?,
    })
}

fn rest(r: &RawRest, at: &At, ctx: &mut Ctx) -> Option<Rest> {
    use crate::secret::Secret;
    let url = req(ctx, &r.url, at, "url");
    let warehouse = opt(ctx, &r.warehouse, at, "warehouse");
    let prefix = opt(ctx, &r.prefix, at, "prefix");
    let auth = or(ctx, &r.auth, at, "auth", RestAuth::None);
    let token = match (&r.token, &r.token_file) {
        (Some(t), None) => ctx.text(t, &at.key("token")).map(|t| BearerToken::Inline {
            token: Secret::new(t),
        }),
        (None, Some(f)) => read_or_report(ctx, "V-071", &at.key("token_file"), f, read_token_file)
            .map(|(path, token)| BearerToken::File { path, token }),
        _ => None,
    };
    let oauth2 = r.oauth2.as_ref().map(|o| {
        let at = at.key("oauth2");
        let id = o
            .client_id
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("client_id")));
        let secret = o
            .client_secret
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("client_secret")));
        let client = match (&o.credentials_file, &o.client_id, &o.client_secret) {
            (Some(f), None, None) => {
                read_or_report(ctx, "V-067", &at.key("credentials_file"), f, |p, label| {
                    read_json_pair(p, label, "client_id", "client_secret")
                })
                .map(|(path, (client_id, client_secret))| OauthClient::File {
                    path,
                    client_id,
                    client_secret,
                })
            }
            (None, Some(_), Some(_)) => Some(OauthClient::Inline {
                client_id: id?,
                client_secret: Secret::new(secret?),
            }),
            _ => None,
        };
        let token_url = req(ctx, &o.token_url, &at, "token_url");
        let scope = opt(ctx, &o.scope, &at, "scope");
        Some(RestOauth2 {
            client: client?,
            token_url: token_url?,
            scope: scope?,
        })
    });
    let sigv4 = r.sigv4.as_ref().map(|s| {
        let at = at.key("sigv4");
        let region = req(ctx, &s.region, &at, "region");
        let service = req(ctx, &s.service, &at, "service");
        Some(Sigv4 {
            region: region?,
            service: service?,
        })
    });
    let tls = r.tls.clone().unwrap_or_default();
    let tls_ca = pem(ctx, &tls.ca, &at.key("tls"), "ca", PemKind::Certificates);
    let auth = auth?;
    let (token, oauth2, sigv4) = match auth {
        RestAuth::None => (None, None, None),
        RestAuth::Bearer => (Some(token?), None, None),
        RestAuth::Oauth2 => (None, Some(oauth2??), None),
        RestAuth::Sigv4 => (None, None, Some(sigv4??)),
    };
    Some(Rest {
        url: url?,
        warehouse: warehouse?,
        prefix: prefix?,
        auth,
        token,
        oauth2,
        sigv4,
        tls_ca: tls_ca?,
    })
}

fn delta(d: &RawDelta, at: &At, ctx: &mut Ctx) -> Option<Delta> {
    let column_mapping = or(
        ctx,
        &d.column_mapping,
        at,
        "column_mapping",
        ColumnMapping::None,
    );
    let checkpoint_interval = or(ctx, &d.checkpoint_interval, at, "checkpoint_interval", 10);
    let log_retention = or(
        ctx,
        &d.log_retention,
        at,
        "log_retention",
        Duration::from_secs(30 * 86_400),
    );
    let timestamp_ntz_as = or(
        ctx,
        &d.timestamp_ntz_as,
        at,
        "timestamp_ntz_as",
        TimestampNtzAs::TimestampNtz,
    );
    let table_properties = map(ctx, &d.table_properties, at, "table_properties");
    let catalog = d.catalog.clone().unwrap_or_default();
    let catalog_type = or(
        ctx,
        &catalog.kind,
        &at.key("catalog"),
        "type",
        DeltaCatalogType::None,
    );
    Some(Delta {
        column_mapping: column_mapping?,
        checkpoint_interval: checkpoint_interval?,
        log_retention: log_retention?,
        timestamp_ntz_as: timestamp_ntz_as?,
        table_properties: table_properties?,
        catalog_type: catalog_type?,
    })
}

/// Every pipeline merged with `defaults` (§8), in name order.
pub fn merge_all(raw: &RawConfig, ctx: &Ctx) -> Vec<(String, RawPipeline)> {
    raw.pipelines
        .iter()
        .flatten()
        .map(|(name, p)| {
            (
                name.clone(),
                crate::merge::merge_pipeline(raw.defaults.as_ref(), p, ctx),
            )
        })
        .collect()
}

/// Builds the resolved config. Visits every leaf in both passes; returns `None` in the structural
/// pass, and in the resolving pass when anything is missing or invalid.
pub fn build(
    raw: &RawConfig,
    merged: &[(String, RawPipeline)],
    ctx: &mut Ctx,
) -> Option<ResolvedConfig> {
    let version_at = At::config(YamlPath::root());
    let config_version = req(ctx, &raw.config_version, &version_at, "config_version");
    let globals = raw.globals.as_ref().map(|g| globals(g, ctx));
    let parent = raw
        .defaults
        .as_ref()
        .and_then(|d| d.state.as_ref())
        .and_then(|s| s.prefix.as_ref());
    let pipelines: Vec<Option<Pipeline>> = merged
        .iter()
        .map(|(name, p)| pipeline(name, p, parent, ctx))
        .collect();
    raw.pipelines.as_ref()?;
    let mut by_id = BTreeMap::new();
    for p in pipelines {
        let p = p?;
        by_id.insert(p.id.clone(), p);
    }
    if !ctx.is_resolving() {
        return None;
    }
    Some(ResolvedConfig {
        config_version: config_version?,
        globals: globals??,
        pipelines: by_id,
    })
}
