//! Rules that need more than the config file. The runtime calls these with the extra input:
//! - the cluster anchor from `globals.state` at startup (V-062, V-076, V-094; storage in M2);
//! - a pipeline's resolved table set at start or on a table-set change (V-034, the warning half of
//!   V-106, V-113; sources in M5/M7);
//! - source column types at schema resolution (V-079).
//!
//! V-061 (a PROC field changed on reload) is part of [`crate::diff()`].

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use data_avalanche_core::TableRef;

use crate::ctx::{At, Ctx};
use crate::diag::Report;
use crate::enums::{LockBackend, NumericOverflowAction, SourceType};
use crate::path::YamlPath;
use crate::resolved::{Globals, Pipeline};
use crate::units::format_duration;

/// The values the first node wrote to the `globals.state` cluster anchor (Clustering §5.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterAnchor {
    pub tokens: u32,
    pub lock_backend: LockBackend,
    pub lease_duration: Duration,
}

/// V-062, V-076, V-094: this node's cluster-wide settings must equal the anchor's. An error means
/// the node refuses to start.
///
/// V-062 and V-094 print only the field name and the anchor's value, never this node's
/// `globals.cluster` value: it may have come from `${VAR}`, and by the time it reaches the
/// resolved `Globals` there's no leaf left to say so, so the value can't be redacted the way a
/// templated leaf's can — it's simply never printed. V-076 (`lock_backend`) is a parsed enum, not
/// a number or a secret-shaped value, and keeps printing both sides (controller ruling).
pub fn check_cluster_anchor(globals: &Globals, anchor: &ClusterAnchor) -> Report {
    let mut ctx = Ctx::structural();
    let at = At::config(YamlPath::root().key("globals").key("cluster"));
    let c = &globals.cluster;
    if c.tokens != anchor.tokens {
        ctx.error(
            "V-062",
            &at.key("tokens"),
            None,
            format!(
                "tokens differs from the cluster's established value {}; every node must use the same value",
                anchor.tokens
            ),
        );
    }
    if c.lock_backend != anchor.lock_backend {
        ctx.error("V-076", &at.key("lock_backend"), None, format!("lock_backend is {} but the cluster uses {}; a mixed-backend cluster has two arbiters", c.lock_backend, anchor.lock_backend));
    }
    if c.lease_duration != anchor.lease_duration {
        ctx.error(
            "V-094",
            &at.key("lease_duration"),
            None,
            format!(
                "lease_duration differs from the cluster's established value {}",
                format_duration(anchor.lease_duration)
            ),
        );
    }
    ctx.into_report()
}

/// A per-table config key as a table: PG `schema.table`; MySQL `name` in `source.db` (§10).
fn key_table(p: &Pipeline, key: &str) -> Option<TableRef> {
    match p.source.kind {
        SourceType::Postgres => key.split_once('.').map(|(s, t)| TableRef::new(s, t)),
        SourceType::Mysql => Some(TableRef::new(p.source.db.clone(), key)),
    }
}

fn pipeline_at(p: &Pipeline) -> At {
    At::pipeline(&p.name, YamlPath::root().key("pipelines").key(&p.name))
}

/// V-034 and the warning half of V-106 (per-table keys naming no resolved table), and V-113 (Glue
/// `strip_schema: true` with two tables of the same name). An error refuses the pipeline.
pub fn check_table_set(p: &Pipeline, tables: &[TableRef]) -> Report {
    let mut ctx = Ctx::structural();
    let at = pipeline_at(p).key("tables");
    let set: BTreeSet<&TableRef> = tables.iter().collect();
    let unknown = |key: &str| key_table(p, key).is_none_or(|t| !set.contains(&t));
    for key in p.tables.snapshots.overrides.keys().filter(|k| unknown(k)) {
        ctx.warning(
            "V-034",
            &at.key("snapshots").key("overrides").key(key),
            None,
            format!("{key} is not in the pipeline's resolved table set"),
        );
    }
    for (field, keys) in [
        (
            "primary_key",
            p.tables.primary_key.keys().collect::<Vec<_>>(),
        ),
        ("partitioning", p.tables.partitioning.keys().collect()),
    ] {
        for key in keys.into_iter().filter(|k| unknown(k)) {
            ctx.warning(
                "V-106",
                &at.key(field).key(key),
                None,
                format!("{key} is not in the pipeline's resolved table set"),
            );
        }
    }
    let strip = p
        .sink
        .iceberg
        .as_ref()
        .and_then(|i| i.catalog.glue.as_ref())
        .is_some_and(|g| g.strip_schema);
    if strip {
        let mut by_name: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for t in tables {
            by_name.entry(&t.name).or_default().insert(t.to_string());
        }
        for (name, list) in by_name.iter().filter(|(_, l)| l.len() > 1) {
            ctx.error("V-113", &pipeline_at(p).key("sink").key("iceberg").key("catalog").key("glue").key("strip_schema"), None, format!(
                "strip_schema: true maps {} to the one Glue table {name}; set strip_schema: false or exclude all but one",
                list.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    ctx.into_report()
}

/// V-079: with `numeric_overflow_action: error`, source `numeric` columns without a declared
/// precision may halt their table. `columns` are those columns, found at schema resolution.
pub fn check_unbounded_numeric(p: &Pipeline, columns: &[(TableRef, String)]) -> Report {
    let mut ctx = Ctx::structural();
    if p.tables.numeric_overflow_action == NumericOverflowAction::Error && !columns.is_empty() {
        let list: Vec<String> = columns.iter().map(|(t, c)| format!("{t}.{c}")).collect();
        ctx.warning("V-079", &pipeline_at(p).key("tables").key("numeric_overflow_action"), None, format!(
            "numeric_overflow_action error with unconstrained numeric columns {}: a value beyond 38 digits halts the table; consider a column_type_overrides entry",
            list.join(", ")
        ));
    }
    ctx.into_report()
}
