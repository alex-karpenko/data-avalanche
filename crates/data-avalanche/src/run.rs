//! `data-avalanche run`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use data_avalanche_cluster::{
    ClusterReconciler, ControllerDeps, ControllerSettings, LeaseTiming, node_identity,
};
use data_avalanche_config::{
    Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
};
use data_avalanche_pipeline::{Backoff, Tuning};
use data_avalanche_state::Leases;
use data_avalanche_telemetry::Metrics;
use thiserror::Error;
use tokio::sync::Semaphore;

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

/// What this build doesn't serve yet, refused before anything starts.
pub(crate) fn unsupported(config: &ResolvedConfig) -> Option<String> {
    config.globals.api.tls.as_ref().map(|_| {
        "globals.api.tls: the API listener serves TLS from M9; this build serves plain HTTP only"
            .to_owned()
    })
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
    logging::init(&first.config.globals.log, &args).map_err(Fatal::Config)?;
    for w in &first.warnings {
        tracing::warn!("config: {}", line(w));
    }
    if let Some(why) = unsupported(&first.config) {
        return Err(Fatal::Config(why));
    }
    let holder = node_identity(&first.config.globals.cluster, started).map_err(Fatal::Config)?;
    let buckets = Buckets::new(&first.config.globals);
    let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
    let config = joined.config;
    if let Some(why) = unsupported(&config) {
        return Err(Fatal::Config(why));
    }
    tracing::info!(
        node = %holder.node_id,
        incarnation = holder.incarnation,
        pipelines = config.pipelines.len(),
        "joined the cluster"
    );

    let metrics = Metrics::new();
    let c = &config.globals.cluster;
    let deps = ControllerDeps {
        leases: Leases::new(joined.globals),
        holder,
        factory: Arc::new(Factory::new(&config.globals)),
        metrics: metrics.clone(),
        settings: settings(&config),
        starts: Arc::new(Semaphore::new(
            usize::try_from(c.max_concurrent_takeovers).unwrap_or(usize::MAX),
        )),
    };
    let mut pipelines = Vec::new();
    for p in config.pipelines.values() {
        pipelines.push((p.clone(), buckets.pipeline_docs(p)?));
    }
    let reconciler = ClusterReconciler::start(pipelines, &deps);
    shutdown_signal().await;
    tracing::info!("shutting down: draining every pipeline and releasing its lease");
    reconciler.shutdown().await;
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

    use data_avalanche_config::{Origin, Pem, Secret, TlsIdentity};

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
    fn api_tls_is_refused_until_m9() {
        let mut c = config();
        assert_eq!(unsupported(&c), None);
        let key = FingerprintKey::from_bytes([1; 32]);
        let pem = || Pem {
            origin: Origin::Inline,
            text: Secret::new("-----BEGIN-----", &key),
        };
        c.globals.api.tls = Some(TlsIdentity {
            cert: pem(),
            key: pem(),
        });
        assert!(unsupported(&c).unwrap().contains("M9"));
    }

    #[test]
    fn the_controllers_take_the_cluster_timings() {
        let s = settings(&config());
        assert_eq!(s.timing.duration, Duration::from_secs(30));
        assert_eq!(s.timing.renewal_interval, Duration::from_secs(10));
        assert_eq!(s.drain_timeout, Duration::from_secs(45));
    }
}
