//! The process's memory and CPU, and its container's limits (Admin_Console §12, D-M13b.7): read
//! from `/proc/self` and the cgroup on Linux, absent elsewhere. Every figure is optional: a host
//! without a limit has none, never a `0` or a sentinel standing in for "unlimited".

use std::path::Path;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::Metrics;

/// How often the sampler reads the figures; the CPU's rate is over this interval.
pub const PROCESS_SAMPLE: Duration = Duration::from_secs(15);

/// Linux's `USER_HZ`, the unit of `/proc/self/stat`'s CPU times, on every architecture it runs
/// on (the process can't ask `sysconf` without `unsafe`).
const TICKS_PER_SECOND: f64 = 100.0;

/// A cgroup v1 memory limit at or over this is the kernel's "no limit" (`PAGE_COUNTER_MAX`
/// pages, rounded).
const V1_NO_LIMIT: u64 = 1 << 60;

/// One reading of the figures.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Reading {
    pub memory_bytes: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    /// User and system CPU time since the process started.
    pub cpu_seconds: Option<f64>,
    pub cpu_limit_cores: Option<f64>,
}

/// What a node's page shows: the last reading, and the CPU used over the interval before it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ProcessFigures {
    pub reading: Reading,
    /// Cores used on average over the last [`PROCESS_SAMPLE`]; `None` until two readings.
    pub cpu_cores: Option<f64>,
}

/// `VmRSS` of `/proc/self/status`, in bytes.
fn resident(status: &str) -> Option<u64> {
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kb.checked_mul(1024)
}

/// `utime + stime` of `/proc/self/stat`, in seconds. The fields are counted after the command's
/// closing parenthesis, which a command name may itself contain.
fn cpu_seconds(stat: &str) -> Option<f64> {
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // After the `)`: state is field 3 of the line, so utime (14) and stime (15) are at 11, 12.
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) / TICKS_PER_SECOND)
}

/// cgroup v2's `memory.max`: bytes, or `max` for none.
fn v2_memory(text: &str) -> Option<u64> {
    text.trim().parse().ok()
}

/// cgroup v2's `cpu.max`: `<quota> <period>` in microseconds, or `max <period>` for none.
fn v2_cpu(text: &str) -> Option<f64> {
    let mut parts = text.split_whitespace();
    let quota: f64 = parts.next()?.parse().ok()?;
    let period: f64 = parts.next()?.parse().ok()?;
    (period > 0.0).then(|| quota / period)
}

/// cgroup v1's `memory.limit_in_bytes`: a huge number for none.
fn v1_memory(text: &str) -> Option<u64> {
    text.trim().parse().ok().filter(|n| *n < V1_NO_LIMIT)
}

/// cgroup v1's `cpu.cfs_quota_us` (`-1` for none) over `cpu.cfs_period_us`.
fn v1_cpu(quota: &str, period: &str) -> Option<f64> {
    let quota: f64 = quota.trim().parse().ok()?;
    let period: f64 = period.trim().parse().ok()?;
    (quota > 0.0 && period > 0.0).then(|| quota / period)
}

/// The figures under `root` (`/` on a real host), so a test can lay out a fake `/proc` and
/// `/sys/fs/cgroup`. cgroup v2 is read at the process's own cgroup (`/proc/self/cgroup`'s
/// `0::<path>`), then at the hierarchy's root; v1 at its controllers' roots. The parents in
/// between aren't walked: a limit set only on a pod's cgroup, above the container's own (a
/// Kubernetes pod-level limit, not a container's `resources.limits`), isn't seen.
pub fn read_process_at(root: &Path) -> Reading {
    let read = |p: &str| std::fs::read_to_string(root.join(p.trim_start_matches('/'))).ok();
    let mut r = Reading {
        memory_bytes: read("/proc/self/status").as_deref().and_then(resident),
        cpu_seconds: read("/proc/self/stat").as_deref().and_then(cpu_seconds),
        ..Reading::default()
    };
    let own = read("/proc/self/cgroup")
        .and_then(|t| {
            t.lines()
                .find_map(|l| l.strip_prefix("0::").map(|p| p.trim().to_owned()))
        })
        .unwrap_or_default();
    let v2 = [format!("/sys/fs/cgroup{own}"), "/sys/fs/cgroup".to_owned()]
        .into_iter()
        .find(|dir| read(&format!("{dir}/cgroup.controllers")).is_some());
    match v2 {
        Some(dir) => {
            r.memory_limit_bytes = read(&format!("{dir}/memory.max"))
                .as_deref()
                .and_then(v2_memory);
            r.cpu_limit_cores = read(&format!("{dir}/cpu.max")).as_deref().and_then(v2_cpu);
        }
        None => {
            r.memory_limit_bytes = read("/sys/fs/cgroup/memory/memory.limit_in_bytes")
                .as_deref()
                .and_then(v1_memory);
            r.cpu_limit_cores = read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
                .zip(read("/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
                .and_then(|(q, p)| v1_cpu(&q, &p));
        }
    }
    r
}

/// The figures of this process: on Linux from the real files, elsewhere none.
pub fn read_process() -> Reading {
    if cfg!(target_os = "linux") {
        read_process_at(Path::new("/"))
    } else {
        Reading::default()
    }
}

impl ProcessFigures {
    /// The figures after `now`, read `elapsed` after the previous one.
    pub fn next(&self, now: Reading, elapsed: Duration) -> Self {
        let cpu_cores = match (self.reading.cpu_seconds, now.cpu_seconds) {
            (Some(before), Some(after)) if !elapsed.is_zero() && after >= before => {
                Some((after - before) / elapsed.as_secs_f64())
            }
            _ => None,
        };
        Self {
            reading: now,
            cpu_cores,
        }
    }
}

/// Reads the figures every [`PROCESS_SAMPLE`], publishes them and sets the process metrics, until
/// every receiver is gone.
pub async fn sample_process(metrics: Metrics, figures: watch::Sender<ProcessFigures>) {
    let mut at = Instant::now();
    loop {
        let now = read_process();
        metrics.process(&now);
        let elapsed = at.elapsed();
        at = Instant::now();
        figures.send_modify(|f| *f = f.next(now, elapsed));
        if figures.is_closed() {
            return;
        }
        tokio::time::sleep(PROCESS_SAMPLE).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake root: `files` laid out under a temporary directory.
    fn root(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (path, text) in files {
            let p = dir.path().join(path.trim_start_matches('/'));
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, text).unwrap();
        }
        dir
    }

    const STATUS: &str = "Name:\tdata-avalanche\nVmPeak:\t 2000000 kB\nVmRSS:\t 1363148 kB\n";
    // A command name with a space and a parenthesis; utime 4500, stime 500 ticks.
    const STAT: &str = "42 (data (av)) S 1 42 42 0 -1 4194560 100 0 0 0 4500 500 0 0 20 0 9 0 \
                        100 2000000 340787 18446744073709551615";

    #[test]
    fn memory_and_cpu_come_from_proc_self() {
        assert_eq!(resident(STATUS), Some(1_363_148 * 1024));
        assert_eq!(cpu_seconds(STAT), Some(50.0));
        assert_eq!(resident("Name:\tx\n"), None);
        assert_eq!(cpu_seconds("42 (x) S 1"), None);
    }

    #[test]
    fn a_cgroup_v2_limit_is_read_at_the_process_s_own_cgroup() {
        let dir = root(&[
            ("/proc/self/status", STATUS),
            ("/proc/self/stat", STAT),
            ("/proc/self/cgroup", "0::/kubepods/pod1/cdc\n"),
            ("/sys/fs/cgroup/cgroup.controllers", "cpu memory"),
            ("/sys/fs/cgroup/memory.max", "max\n"),
            (
                "/sys/fs/cgroup/kubepods/pod1/cdc/cgroup.controllers",
                "cpu memory",
            ),
            (
                "/sys/fs/cgroup/kubepods/pod1/cdc/memory.max",
                "2147483648\n",
            ),
            (
                "/sys/fs/cgroup/kubepods/pod1/cdc/cpu.max",
                "200000 100000\n",
            ),
        ]);
        let r = read_process_at(dir.path());
        assert_eq!(r.memory_limit_bytes, Some(2 << 30));
        assert_eq!(r.cpu_limit_cores, Some(2.0));
        assert_eq!(r.memory_bytes, Some(1_363_148 * 1024));
    }

    #[test]
    fn no_limit_reads_as_none() {
        let dir = root(&[
            ("/proc/self/cgroup", "0::/\n"),
            ("/sys/fs/cgroup/cgroup.controllers", "cpu memory"),
            ("/sys/fs/cgroup/memory.max", "max\n"),
            ("/sys/fs/cgroup/cpu.max", "max 100000\n"),
        ]);
        let r = read_process_at(dir.path());
        assert_eq!((r.memory_limit_bytes, r.cpu_limit_cores), (None, None));
        assert_eq!(read_process_at(root(&[]).path()), Reading::default());
    }

    #[test]
    fn cgroup_v1_s_sentinel_is_no_limit() {
        let unlimited = root(&[
            (
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                "9223372036854771712\n",
            ),
            ("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "-1\n"),
            ("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "100000\n"),
        ]);
        let r = read_process_at(unlimited.path());
        assert_eq!((r.memory_limit_bytes, r.cpu_limit_cores), (None, None));
        let limited = root(&[
            (
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                "1073741824\n",
            ),
            ("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "50000\n"),
            ("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "100000\n"),
        ]);
        let r = read_process_at(limited.path());
        assert_eq!(
            (r.memory_limit_bytes, r.cpu_limit_cores),
            (Some(1 << 30), Some(0.5))
        );
    }

    #[test]
    fn the_cpu_rate_is_over_the_interval_between_two_readings() {
        let at = |s: f64| Reading {
            cpu_seconds: Some(s),
            ..Reading::default()
        };
        let first = ProcessFigures::default().next(at(10.0), Duration::from_secs(15));
        assert_eq!(first.cpu_cores, None, "one reading has no rate");
        let second = first.next(at(19.0), Duration::from_secs(15));
        assert_eq!(second.cpu_cores, Some(0.6));
        let none = second.next(Reading::default(), Duration::from_secs(15));
        assert_eq!(none.cpu_cores, None);
    }
}
