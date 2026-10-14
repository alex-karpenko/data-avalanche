//! E2E crash points (feature `e2e-hooks`, Q-M6b-5): `DATA_AVALANCHE_E2E_CRASH=<point>[:<n>]`
//! aborts the process at the `n`th time (default 1) the worker reaches `point`, as a `kill -9`
//! there would. Points: `sink_committed`, `checkpoint_persisted`, `chunk_emitted`,
//! `schemas_cached`. Never built into a release.

use std::sync::atomic::{AtomicUsize, Ordering};

use data_avalanche_pipeline::{HookAction, HookPoint, Hooks};

pub const CRASH_VAR: &str = "DATA_AVALANCHE_E2E_CRASH";

#[derive(Debug)]
pub struct CrashAt {
    point: &'static str,
    nth: usize,
    seen: AtomicUsize,
}

fn name(point: &HookPoint) -> &'static str {
    match point {
        HookPoint::SinkCommitted { .. } => "sink_committed",
        HookPoint::CheckpointPersisted { .. } => "checkpoint_persisted",
        HookPoint::ChunkEmitted { .. } => "chunk_emitted",
        HookPoint::SchemasCached => "schemas_cached",
    }
}

impl CrashAt {
    /// # Errors
    /// The value names no point, or its count isn't a positive number.
    pub fn parse(value: &str) -> Result<Self, String> {
        let (point, nth) = match value.split_once(':') {
            Some((p, n)) => (
                p,
                n.parse::<usize>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| format!("{CRASH_VAR}: {n:?} isn't a positive count"))?,
            ),
            None => (value, 1),
        };
        let point = [
            "sink_committed",
            "checkpoint_persisted",
            "chunk_emitted",
            "schemas_cached",
        ]
        .into_iter()
        .find(|p| *p == point)
        .ok_or_else(|| format!("{CRASH_VAR}: {point:?} isn't a crash point"))?;
        Ok(Self {
            point,
            nth,
            seen: AtomicUsize::new(0),
        })
    }

    fn due(&self, point: &HookPoint) -> bool {
        name(point) == self.point && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.nth
    }
}

impl Hooks for CrashAt {
    fn at(&self, point: &HookPoint) -> HookAction {
        if self.due(point) {
            eprintln!("data-avalanche: {CRASH_VAR}: aborting at {point:?}");
            std::process::abort();
        }
        HookAction::Continue
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::TableRef;

    use super::*;

    #[test]
    fn a_crash_point_is_due_at_its_nth_pass() {
        let c = CrashAt::parse("sink_committed:2").unwrap();
        let commit = HookPoint::SinkCommitted {
            table: TableRef::new("public", "a"),
        };
        assert!(!c.due(&HookPoint::SchemasCached));
        assert!(!c.due(&commit));
        assert!(c.due(&commit));
        assert!(!c.due(&commit), "once");
        assert!(
            CrashAt::parse("schemas_cached")
                .unwrap()
                .due(&HookPoint::SchemasCached)
        );
        assert!(CrashAt::parse("nowhere").is_err());
        assert!(CrashAt::parse("chunk_emitted:0").is_err());
    }
}
