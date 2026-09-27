//! Mutability classes (config §1.2) of the resolved config's paths: what a change to a field
//! triggers on reload.

use serde::Serialize;

/// Ordered by blast radius.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mutability {
    /// Applied in place, no interruption.
    R,
    /// `tables.include`/`exclude`: `Pr` if the resolved table set changes, else `R` (§1.2).
    PrIfTableSetChanges,
    /// The pipeline restarts (§12.4).
    Pr,
    /// Needs a process restart: the reload is rejected (V-061).
    Proc,
}

use Mutability::*;

/// Path patterns over the resolved config's serialized form (`*` = one segment: a pipeline id).
/// The longest matching pattern wins, so a block's class can be refined field by field.
/// Source: the Mutability columns of config §3, §6, §9.
pub const CLASSES: &[(&str, Mutability)] = &[
    ("config_version", Proc),
    ("globals.cloud", Proc),
    ("globals.state", Proc),
    ("globals.api.address", Proc),
    ("globals.api.auth", R),
    ("globals.api.credentials", R),
    ("globals.api.jwt", R),
    ("globals.api.oauth2", R),
    ("globals.api.role_mapping", R),
    ("globals.api.tls", Proc),
    ("globals.log", R),
    ("globals.cluster", Proc),
    ("globals.cluster.rebalance_stabilization", R),
    ("globals.cluster.max_concurrent_handoffs", R),
    ("globals.cluster.max_concurrent_takeovers", R),
    ("globals.cluster.drain_timeout", R),
    ("globals.tracing", R),
    ("globals.azure", Proc),
    ("pipelines.*.name", R),
    ("pipelines.*.id", Pr), // Diffs match pipelines by `id`, so a field-level `id` change never happens (an `id` change is remove + add, §9.1). This entry exists only so `unclassified_paths` covers the path.
    ("pipelines.*.source", Pr),
    ("pipelines.*.source.postgres.auto_cleanup", R),
    ("pipelines.*.source.postgres.heartbeat", R),
    ("pipelines.*.source.postgres.slot_on_orphan", R),
    ("pipelines.*.source.postgres.max_wal_lag_bytes", R),
    ("pipelines.*.source.mysql.binlog_purge_action", R),
    ("pipelines.*.tables", Pr),
    ("pipelines.*.tables.include", PrIfTableSetChanges),
    ("pipelines.*.tables.exclude", PrIfTableSetChanges),
    ("pipelines.*.tables.snapshots.parallelism", R),
    ("pipelines.*.tables.snapshots.chunk_size", R),
    ("pipelines.*.sink", Pr),
    ("pipelines.*.sink.flush", R),
    ("pipelines.*.sink.max_transaction_batch_rows", R),
    ("pipelines.*.sink.max_transaction_buffer_bytes", R),
    ("pipelines.*.sink.buffer_capacity_rows", R),
    ("pipelines.*.sink.metrics", R),
    ("pipelines.*.sink.iceberg.catalog.retry", R),
    ("pipelines.*.sink.iceberg.catalog.hive.credentials", R),
    ("pipelines.*.sink.iceberg.catalog.hive.kerberos", R),
    ("pipelines.*.sink.iceberg.catalog.rest.token", R),
    ("pipelines.*.sink.iceberg.catalog.rest.oauth2", R),
    ("pipelines.*.sink.iceberg.catalog.rest.sigv4", R),
    ("pipelines.*.state", Pr),
    ("pipelines.*.schema_evolution", R),
];

fn matches(pattern: &str, path: &[&str]) -> Option<usize> {
    let pat: Vec<&str> = pattern.split('.').collect();
    (pat.len() <= path.len() && pat.iter().zip(path).all(|(p, s)| *p == "*" || p == s))
        .then_some(pat.len())
}

/// The class of a resolved-config path given as segments (`["pipelines", "orders", "sink",
/// "flush", "rows"]`), or `None` when no pattern covers it.
pub fn classify(path: &[&str]) -> Option<Mutability> {
    CLASSES
        .iter()
        .filter_map(|(pattern, class)| matches(pattern, path).map(|len| (len, *class)))
        .max_by_key(|(len, _)| *len)
        .map(|(_, class)| class)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_pattern_wins() {
        assert_eq!(classify(&["globals", "cluster", "tokens"]), Some(Proc));
        assert_eq!(classify(&["globals", "cluster", "drain_timeout"]), Some(R));
        assert_eq!(
            classify(&["globals", "cluster", "memberlist", "tls", "cert", "text"]),
            Some(Proc)
        );
        assert_eq!(classify(&["pipelines", "p", "source", "host"]), Some(Pr));
        assert_eq!(
            classify(&["pipelines", "p", "source", "postgres", "heartbeat"]),
            Some(R)
        );
        assert_eq!(
            classify(&["pipelines", "p", "tables", "include"]),
            Some(PrIfTableSetChanges)
        );
        assert_eq!(
            classify(&["pipelines", "p", "sink", "flush", "rows"]),
            Some(R)
        );
        assert_eq!(classify(&["nowhere"]), None);
        assert!(Proc > Pr && Pr > PrIfTableSetChanges && PrIfTableSetChanges > R);
    }
}
