//! What the runs measure, and where it goes: one JSON line per measurement in
//! `<DATA_AVALANCHE_LOAD_OUT>/results.jsonl` (`cargo xtask` sets it to `target/acceptance`), and
//! a line on stdout. The acceptance report (`doc/v1.0/v1-acceptance.md`) quotes them.

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use crate::harness::Proc;

/// A run's knob: `name`'s value, else `default`.
pub fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .map_or(default, |v| {
            v.parse()
                .unwrap_or_else(|_| panic!("{name}={v}: a whole number"))
        })
}

/// Records `value` (in `unit`) of `run`'s `metric`; `target`, when there is one, and whether it
/// was met.
pub fn record(run: &str, metric: &str, value: f64, unit: &str, target: Option<(&str, bool)>) {
    let line = serde_json::json!({
        "run": run,
        "metric": metric,
        "value": value,
        "unit": unit,
        "target": target.map(|(t, _)| t),
        "met": target.map(|(_, m)| m),
        "at": chrono::Utc::now().to_rfc3339(),
        "source": std::env::var("DATA_AVALANCHE_E2E_SOURCE").unwrap_or_else(|_| "postgres".into()),
    });
    println!("acceptance: {line}");
    let dir = std::env::var("DATA_AVALANCHE_LOAD_OUT").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/acceptance"),
        PathBuf::from,
    );
    std::fs::create_dir_all(&dir).expect("the results directory");
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("results.jsonl"))
        .expect("the results file");
    writeln!(f, "{line}").expect("a result written");
}

/// `pipeline`'s `data_avalanche_replication_lag_seconds` on `p`, if `p` runs it.
pub async fn lag(p: &Proc, pipeline: &str) -> Option<f64> {
    let label = format!("pipeline=\"{pipeline}\"");
    p.metrics().await.lines().find_map(|l| {
        let rest = l.strip_prefix("data_avalanche_replication_lag_seconds{")?;
        let (labels, value) = rest.split_once("} ")?;
        labels
            .split(',')
            .any(|kv| kv == label)
            .then(|| value.trim().parse().ok())?
    })
}

/// The sum and the count of a histogram of `pipeline` on `p` (`<name>_sum`, `<name>_count`).
pub async fn histogram(p: &Proc, name: &str, pipeline: &str) -> (f64, f64) {
    let label = format!("pipeline=\"{pipeline}\"");
    let text = p.metrics().await;
    let pick = |suffix: &str| {
        text.lines()
            .filter_map(|l| {
                let rest = l.strip_prefix(&format!("{name}_{suffix}{{"))?;
                let (labels, value) = rest.split_once("} ")?;
                labels
                    .split(',')
                    .any(|kv| kv == label)
                    .then(|| value.trim().parse::<f64>().ok())?
            })
            .sum::<f64>()
    };
    (pick("sum"), pick("count"))
}

/// Lag samples, one a second.
#[derive(Debug, Default)]
pub struct Samples(pub Vec<f64>);

impl Samples {
    /// The `q` quantile (0..=1), nearest rank.
    pub fn quantile(&self, q: f64) -> f64 {
        let mut v = self.0.clone();
        v.sort_by(f64::total_cmp);
        if v.is_empty() {
            return f64::NAN;
        }
        let rank = (q * v.len() as f64).ceil() as usize;
        v[rank.clamp(1, v.len()) - 1]
    }

    /// Samples `pipeline`'s lag on `p` every second for `window`; a second the metric is missing
    /// (no worker yet) isn't a sample.
    pub async fn take(p: &Proc, pipeline: &str, window: Duration) -> Self {
        let mut s = Self::default();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let until = tokio::time::Instant::now() + window;
        while tokio::time::Instant::now() < until {
            tick.tick().await;
            if let Some(v) = lag(p, pipeline).await {
                s.0.push(v);
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::Samples;

    #[test]
    fn the_quantile_is_the_nearest_rank() {
        let s = Samples((1..=100).map(f64::from).collect());
        assert_eq!(s.quantile(0.99), 99.0);
        assert_eq!(s.quantile(0.5), 50.0);
        assert_eq!(s.quantile(1.0), 100.0);
        assert_eq!(Samples(vec![7.0]).quantile(0.99), 7.0);
        assert!(Samples::default().quantile(0.99).is_nan());
    }
}
