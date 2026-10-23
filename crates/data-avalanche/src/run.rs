//! `data-avalanche run`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};

use data_avalanche_api::{Api, Auth, Node, ServeError, bind, router};
use data_avalanche_cluster::{
    ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
    Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
    ReconcilerSettings, Seeds, Udp, advertise, discover, interfaces, join as join_gossip, leave,
    node_identity,
};
use data_avalanche_config::{
    Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
};
use data_avalanche_pipeline::{Backoff, Tuning};
use data_avalanche_state::{ConfigGenerations, Leases};
use data_avalanche_telemetry::Metrics;
use thiserror::Error;
use tokio::sync::{Semaphore, oneshot};

use crate::startup::{Buckets, join};
use crate::{Factory, RunArgs, logging};

/// Why the process stops before running its pipelines.
#[derive(Debug, Error)]
pub enum Fatal {
    /// The config, or the command line, is refused: exit 2.
    #[error("{0}")]
    Config(String),
    /// Storage, the anchor or the generation couldn't be reached or read: exit 1.
    #[error("{0}")]
    Startup(String),
}

impl Fatal {
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Config(_) => 2,
            Self::Startup(_) => 1,
        }
    }
}

/// One diagnostic as a line: `error V-nnn at <path> (line:col): <message>`.
pub(crate) fn line(d: &Diagnostic) -> String {
    let severity = match d.severity {
        Severity::Error => "error",
        Severity::Warning => "warning",
    };
    let at = match (&d.path, &d.loc) {
        (p, Some(loc)) if !p.is_empty() => format!(" at {p} ({loc})"),
        (p, None) if !p.is_empty() => format!(" at {p}"),
        (_, Some(loc)) => format!(" ({loc})"),
        _ => String::new(),
    };
    format!("{severity} {}{at}: {}", d.rule, d.message)
}

/// A config report, a line per diagnostic.
pub(crate) fn render(report: &Report) -> String {
    report.iter().map(line).collect::<Vec<_>>().join("\n")
}

/// The controllers' timings from `globals.cluster`.
pub(crate) fn settings(config: &ResolvedConfig) -> ControllerSettings {
    let c = &config.globals.cluster;
    ControllerSettings {
        timing: LeaseTiming::new(c),
        drain_timeout: c.drain_timeout,
        restart: Backoff::default(),
        start_jitter: Duration::from_millis(500),
        tuning: Tuning::default(),
    }
}

/// How long a leave waits for its hand-offs: `drain_timeout` for each round of
/// `max_concurrent_handoffs` pipelines, and two `lease_renewal_interval`s over, which cover one
/// resync for a controller started again and the release's or the claim's I/O.
pub(crate) fn leave_within(config: &ResolvedConfig) -> Duration {
    let c = &config.globals.cluster;
    let pipelines = u32::try_from(config.pipelines.len()).unwrap_or(u32::MAX);
    let rounds = pipelines.div_ceil(c.max_concurrent_handoffs.max(1));
    c.drain_timeout.saturating_mul(rounds) + 2 * c.lease_renewal_interval
}

/// Runs every pipeline until SIGTERM or SIGINT.
///
/// # Errors
/// [`Fatal`]: the config is refused, or `globals.state` can't be joined.
pub async fn run(args: RunArgs) -> Result<(), Fatal> {
    let started = SystemTime::now();
    let text = std::fs::read_to_string(&args.config).map_err(|e| {
        Fatal::Config(format!(
            "cannot read config file {}: {e}",
            args.config.display()
        ))
    })?;
    // 1. A first load with a throwaway key: only globals.state and the cluster settings matter.
    let first = load_str(&text, &ProcessEnv, &FingerprintKey::random())
        .map_err(|r| Fatal::Config(render(&r)))?;
    let logger = Arc::new(logging::init(&first.config.globals.log, &args).map_err(Fatal::Config)?);
    for w in &first.warnings {
        tracing::warn!("config: {}", line(w));
    }
    // The listener answers from here: live at once, ready once every controller runs.
    let metrics = Metrics::new();
    let ready = Arc::new(AtomicBool::new(false));
    let node = Arc::new(OnceLock::new());
    let api = &first.config.globals.api;
    let auth = Arc::new(Auth::new(api));
    let bound = bind(&api.address, api.tls.as_ref())
        .await
        .map_err(|e| match e {
            ServeError::Tls(_) => Fatal::Config(e.to_string()),
            ServeError::Bind { .. } => Fatal::Startup(e.to_string()),
        })?;
    let (stop_http, http_stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(bound.serve(
        router(Api {
            metrics: metrics.clone(),
            ready: Arc::clone(&ready),
            node: Arc::clone(&node),
            auth: Arc::clone(&auth),
        }),
        async {
            let _ = http_stopped.await;
        },
    ));
    let holder = node_identity(&first.config.globals.cluster, started)
        .map_err(|e| Fatal::Config(e.to_string()))?;
    let buckets = Arc::new(Buckets::new(&first.config.globals));
    let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
    let config = joined.config;
    logger.apply(&config.globals.log);
    auth.set(&config.globals.api);
    tracing::info!(
        node = %holder.node_id,
        incarnation = holder.incarnation,
        pipelines = config.pipelines.len(),
        "joined the cluster"
    );

    let c = &config.globals.cluster;
    let deps = ControllerDeps {
        leases: Leases::new(joined.globals.clone()),
        holder,
        factory: Arc::new(Factory::new(&config.globals).map_err(Fatal::Config)?),
        metrics: metrics.clone(),
        settings: settings(&config),
        starts: Arc::new(Semaphore::new(
            usize::try_from(c.max_concurrent_takeovers).unwrap_or(usize::MAX),
        )),
        handoffs: Arc::new(Semaphore::new(
            usize::try_from(c.max_concurrent_handoffs).unwrap_or(usize::MAX),
        )),
    };
    let mut pipelines = Vec::new();
    for p in config.pipelines.values() {
        pipelines.push((p.clone(), buckets.pipeline_docs(p)?));
    }
    // The gossip (Clustering §2–§3): joining until a peer answers, then active.
    let m = &c.memberlist;
    let bind: Vec<SocketAddr> = m
        .bind_addr
        .iter()
        .map(|ip| SocketAddr::new(*ip, m.bind_port))
        .collect();
    let (udp, inbox) = Udp::bind(&bind)
        .await
        .map_err(|e| Fatal::Startup(e.to_string()))?;
    let addr = advertise(m, &interfaces())
        .await
        .map_err(|e| Fatal::Config(e.to_string()))?;
    let mut own = udp.local_addrs();
    own.push(addr);
    let me = NodeIdentity {
        node_id: deps.holder.node_id.clone(),
        incarnation: deps.holder.incarnation,
        rev: 0,
        addr,
        meta: NodeMeta {
            phase: Phase::Joining,
            config_generation: joined.generation.doc.generation,
            build: env!("CARGO_PKG_VERSION").to_owned(),
        },
    };
    let key = GossipKey::derive(&joined.fingerprint_key);
    let gossip = Gossip::start(me, key, GossipSettings::new(m), Arc::new(udp), inbox);
    tracing::info!(%addr, bound = ?bind, "gossip started");
    let seeds = Seeds::new(c);
    let seeded = !seeds.is_empty();
    let discovery = discover(seeds, own, gossip.announcer(), gossip.members());
    join_gossip(&gossip, seeded).await;
    let reconciler = ClusterReconciler::start(
        pipelines,
        &deps,
        gossip.members(),
        ReconcilerSettings::new(c),
    );
    // Each newer config generation, this node's reloads included (Clustering §9).
    let docs = Arc::clone(&buckets);
    let (log, reloaded_auth) = (Arc::clone(&logger), Arc::clone(&auth));
    let coordinator = Arc::new(ConfigCoordinator::start(
        config.clone(),
        joined.generation,
        CoordinatorDeps {
            generations: ConfigGenerations::new(joined.globals.clone()),
            env: Arc::new(ProcessEnv),
            key: FingerprintKey::from_bytes(joined.fingerprint_key),
            node_id: deps.holder.node_id.clone(),
            docs: Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string())),
            factory: Arc::clone(&deps.factory),
            reloader: reconciler.reloader(),
            advertiser: gossip.advertiser(),
            metrics: metrics.clone(),
            globals: Arc::new(move |g| {
                log.apply(&g.log);
                reloaded_auth.set(&g.api);
            }),
        },
        gossip.members(),
        c.lease_renewal_interval,
    ));
    let _ = node.set(Node {
        coordinator: Arc::clone(&coordinator),
        members: gossip.members(),
        file: args.config.clone(),
    });
    ready.store(true, Ordering::SeqCst);
    shutdown_signal().await;
    tracing::info!("shutting down: handing off every pipeline and releasing its lease");
    ready.store(false, Ordering::SeqCst);
    discovery.abort();
    // No generation is adopted during the leave; it hands off what the last one configures.
    coordinator.stop().await;
    let config = coordinator.config().await;
    leave(gossip, reconciler, leave_within(&config)).await;
    let _ = stop_http.send(());
    let _ = server.await;
    tracing::info!("stopped");
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(e) => {
                tracing::warn!("SIGTERM can't be watched ({e}); only SIGINT stops the process");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use super::*;

    pub(crate) const CONFIG: &str = r#"
config_version: 1
globals:
  cloud: aws
  state: { bucket: app-state, prefix: cluster-a }
  cluster: { lease_duration: 30s, lease_renewal_interval: 10s, drain_timeout: 45s, max_concurrent_takeovers: 2 }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: pw
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      prefix: shop
      delta: { checkpoint_interval: 20, timestamp_ntz_as: string, merge_memory_limit: 64MiB }
"#;

    pub(crate) fn config() -> ResolvedConfig {
        load_str(
            CONFIG,
            &BTreeMap::new(),
            &FingerprintKey::from_bytes([1; 32]),
        )
        .unwrap_or_else(|r| panic!("{}", render(&r)))
        .config
    }

    #[test]
    fn a_report_prints_a_line_per_diagnostic() {
        let r = load_str(
            "config_version: 1\nglobals: { state: { bucket: s } }\npipelines: {}\nbogus: 1\n",
            &BTreeMap::new(),
            &FingerprintKey::from_bytes([1; 32]),
        )
        .unwrap_err();
        let text = render(&r);
        assert!(text.starts_with("error "), "{text}");
        assert!(text.contains("bogus"), "{text}");
    }

    #[test]
    fn a_leave_waits_a_drain_timeout_a_hand_off_round_and_two_renewals() {
        let mut c = config();
        // One pipeline, two hand-offs at a time: one round of 45 s, and 2 × 10 s.
        assert_eq!(leave_within(&c), Duration::from_secs(65));
        let orders = c.pipelines["orders"].clone();
        for id in ["a", "b", "c", "d", "e"] {
            c.pipelines.insert(id.into(), orders.clone());
        }
        // Six pipelines, two at a time: three rounds; four at a time: two.
        assert_eq!(leave_within(&c), Duration::from_secs(3 * 45 + 20));
        c.globals.cluster.max_concurrent_handoffs = 4;
        assert_eq!(leave_within(&c), Duration::from_secs(2 * 45 + 20));
    }

    #[test]
    fn the_controllers_take_the_cluster_timings() {
        let s = settings(&config());
        assert_eq!(s.timing.duration, Duration::from_secs(30));
        assert_eq!(s.timing.renewal_interval, Duration::from_secs(10));
        assert_eq!(s.drain_timeout, Duration::from_secs(45));
    }
}
