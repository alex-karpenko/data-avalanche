//! §13.4 cross-field and conditional rules.

use data_avalanche_core::LogicalType;

use super::{missing, pipeline_at};
use crate::build::{DEFAULT_LEASE_DURATION, DEFAULT_LEASE_RENEWAL_INTERVAL};
use crate::ctx::{At, Ctx, is_templated, shown, shown_value};
use crate::enums::*;
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;
use crate::units::format_duration;

/// `delta.*` table properties a pipeline may pass through (V-093; decided at M1 planning,
/// 2026-09-27). Tuning and statistics only; everything that changes the protocol, the write path,
/// the `txn` fence or the sink's own fields is excluded. Keys without the `delta.` prefix are user
/// metadata and pass through.
pub(crate) const DELTA_PROPERTY_ALLOW_LIST: [&str; 7] = [
    "delta.dataSkippingNumIndexedCols",
    "delta.dataSkippingStatsColumns",
    "delta.deletedFileRetentionDuration",
    "delta.targetFileSize",
    "delta.tuneFileSizesForRewrites",
    "delta.checkpoint.writeStatsAsJson",
    "delta.checkpoint.writeStatsAsStruct",
];

/// Whether `sink` can map `t` (V-087). In v1 both sinks map every type of the grammar: Delta has no
/// `uuid` or `time` and stores them as strings (Type_Mapping Hop 3b), Iceberg stores `json` as a
/// string (Hop 3a).
pub(crate) fn sink_maps(sink: SinkType, t: &LogicalType) -> bool {
    match (sink, t) {
        (_, LogicalType::List(inner)) => sink_maps(sink, inner),
        (SinkType::Delta | SinkType::Iceberg, _) => true,
    }
}

/// Whether `sink` supports partition transform `t` (V-086; Delta v1: `identity` only).
pub(crate) fn sink_partitions(sink: SinkType, t: Transform) -> bool {
    match sink {
        SinkType::Iceberg => true,
        SinkType::Delta => t == Transform::Identity,
    }
}

fn has_glob(s: &str) -> bool {
    s.contains(['*', '?', '['])
}

pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    let default = RawGlobals::default();
    let g = raw.globals.as_ref().unwrap_or(&default);
    let cloud = if g.cloud.is_none() {
        Some(Cloud::Aws)
    } else {
        ctx.peek(&g.cloud)
    };
    globals(g, ctx);
    for (name, p) in merged {
        pipeline(name, p, cloud, ctx);
    }
}

fn globals(g: &RawGlobals, ctx: &mut Ctx) {
    let root = At::config(YamlPath::root().key("globals"));
    if let Some(c) = &g.cluster {
        let at = root.key("cluster");
        // V-073
        let duration = c
            .lease_duration
            .as_ref()
            .map_or(Some(DEFAULT_LEASE_DURATION), |l| {
                ctx.value(l, &at.key("lease_duration"))
            });
        let renewal = c
            .lease_renewal_interval
            .as_ref()
            .map_or(Some(DEFAULT_LEASE_RENEWAL_INTERVAL), |l| {
                ctx.value(l, &at.key("lease_renewal_interval"))
            });
        if let (Some(d), Some(r)) = (duration, renewal)
            && r > d / 3
        {
            // Duration has no `Display`, so `shown_value` doesn't apply; redact by hand from the
            // same "is this leaf a template" test it uses. `lease_duration` (unlike
            // `lease_renewal_interval`) appears only divided by 3, not verbatim, so it is shown
            // as `lease_duration / 3` regardless — never the raw configured duration.
            let renewal_shown = match &c.lease_renewal_interval {
                Some(l) if is_templated(l) => format!("{:?} (resolved value hidden)", l.text()),
                _ => format_duration(r),
            };
            ctx.error(
                "V-073",
                &at.key("lease_renewal_interval"),
                c.lease_renewal_interval.as_ref().and_then(|l| l.loc()),
                format!(
                    "lease_renewal_interval {} exceeds lease_duration / 3 ({}): the holder needs at least two renewals inside the safety deadline",
                    renewal_shown,
                    format_duration(d / 3)
                ),
            );
        }
        // V-096
        if ctx.peek(&c.discovery) == Some(Discovery::Dns)
            && missing(ctx, &c.discovery_dns_name, &at.key("discovery_dns_name"))
        {
            ctx.error(
                "V-096",
                &at.key("discovery_dns_name"),
                c.discovery.as_ref().and_then(|l| l.loc()),
                "discovery dns needs discovery_dns_name",
            );
        }
    }
    // V-098
    if let Some(t) = &g.tracing
        && t.otlp_endpoint.is_none()
        && let Some(l) = &t.sample_ratio
        && ctx
            .value(l, &root.key("tracing").key("sample_ratio"))
            .is_some_and(|r| r > 0.0)
    {
        ctx.warning(
            "V-098",
            &root.key("tracing").key("sample_ratio"),
            l.loc(),
            "sample_ratio > 0 has no effect: tracing is disabled without otlp_endpoint",
        );
    }
}

/// A per-table key (`snapshots.overrides`, `primary_key`, `partitioning`): no globs (V-033); PG
/// `schema.table` (V-033), MySQL a bare name (V-064). `rule` overrides the ID for V-106.
/// `schema.table`: one `.` between a non-empty schema part and a non-empty table part.
fn is_schema_table(s: &str) -> bool {
    matches!(s.split_once('.'), Some((schema, table)) if !schema.is_empty() && !table.is_empty() && !table.contains('.'))
}

fn table_key(ctx: &mut Ctx, key: &str, at: &At, mysql: bool, rule: Option<&str>) {
    if has_glob(key) {
        ctx.error(
            rule.unwrap_or("V-033"),
            at,
            None,
            format!("{key:?}: globs are not allowed in a per-table key"),
        );
    } else if mysql && key.contains('.') {
        ctx.error(
            rule.unwrap_or("V-064"),
            at,
            None,
            format!("{key:?}: a MySQL table key is a bare table name (the database is source.db)"),
        );
    } else if !mysql && !is_schema_table(key) {
        ctx.error(
            rule.unwrap_or("V-033"),
            at,
            None,
            format!("{key:?}: a PostgreSQL table key is schema.table"),
        );
    }
}

fn pipeline(name: &str, p: &RawPipeline, cloud: Option<Cloud>, ctx: &mut Ctx) {
    let at = pipeline_at(name);
    let default_source = RawSource::default();
    let src = p.source.as_ref().unwrap_or(&default_source);
    let kind = if src.kind.is_none() {
        Some(SourceType::Postgres)
    } else {
        ctx.peek(&src.kind)
    };
    let s_at = at.key("source");
    // V-030
    if kind == Some(SourceType::Mysql) && ctx.peek(&src.ssl_mode) == Some(SslMode::Allow) {
        ctx.error(
            "V-030",
            &s_at.key("ssl_mode"),
            src.ssl_mode.as_ref().and_then(|l| l.loc()),
            "ssl_mode allow is not valid for MySQL",
        );
    }
    // V-103
    if let Some(pg) = &src.postgres
        && ctx.peek(&pg.slot_management) == Some(SlotManagement::RequireExisting)
        && ctx.peek(&pg.auto_cleanup) != Some(false)
    {
        ctx.error("V-103", &s_at.key("postgres").key("auto_cleanup"), pg.slot_management.as_ref().and_then(|l| l.loc()), "slot_management require_existing needs auto_cleanup: false (the slot belongs to the DBA)");
    }
    let sink_kind = p.sink.as_ref().and_then(|s| ctx.peek(&s.kind));
    if let Some(t) = &p.tables {
        tables(t, &at, kind, sink_kind, src, ctx);
    }
    if let Some(s) = &p.sink {
        sink(s, &at.key("sink"), sink_kind, cloud, ctx);
    }
}

fn tables(
    t: &RawTables,
    p_at: &At,
    kind: Option<SourceType>,
    sink_kind: Option<SinkType>,
    src: &RawSource,
    ctx: &mut Ctx,
) {
    let at = &p_at.key("tables");
    let mysql = kind == Some(SourceType::Mysql);
    // V-064, V-117: the per-engine pattern form
    if kind.is_some() {
        for key in ["include", "exclude"] {
            let list = if key == "include" {
                &t.include
            } else {
                &t.exclude
            };
            for (i, l) in list.iter().flatten().flatten().enumerate() {
                let Some(g) = ctx.text(l, &at.key(key).index(i)) else {
                    continue;
                };
                if mysql && g.contains('.') {
                    ctx.error(
                        "V-064",
                        &at.key(key).index(i),
                        l.loc(),
                        format!(
                            "{}: MySQL table patterns are bare names (the database is source.db)",
                            shown(l, &g)
                        ),
                    );
                } else if !mysql && !is_schema_table(&g) {
                    ctx.error(
                        "V-117",
                        &at.key(key).index(i),
                        l.loc(),
                        format!(
                            "{}: PostgreSQL table patterns are schema.table (e.g. public.* or public.orders)",
                            shown(l, &g)
                        ),
                    );
                }
            }
        }
    }
    // V-033, V-064 (override keys); V-038, V-077
    if let Some(s) = &t.snapshots {
        let s_at = at.key("snapshots");
        if kind.is_some() {
            for key in s.overrides.iter().flatten().map(|(k, _)| k) {
                table_key(ctx, key, &s_at.key("overrides").key(key), mysql, None);
            }
        }
        if let Some(l) = &s.parallelism
            && ctx
                .value(l, &s_at.key("parallelism"))
                .is_some_and(|n| n > 4)
        {
            ctx.warning("V-038", &s_at.key("parallelism"), l.loc(), "snapshots.parallelism > 4: each PostgreSQL snapshot job uses at least 3 connections");
        }
        if ctx.peek(&src.toast_strategy) == Some(ToastStrategy::ReQuery)
            && ctx.peek(&s.mode) == Some(SnapshotMode::Always)
        {
            ctx.warning("V-077", &p_at.key("source").key("toast_strategy"), src.toast_strategy.as_ref().and_then(|l| l.loc()), "toast_strategy re_query with snapshots.mode always may saturate the control connection pool");
        }
    }
    // V-106
    if kind.is_some() {
        for (key, cols) in t.primary_key.iter().flatten() {
            let k_at = at.key("primary_key").key(key);
            table_key(ctx, key, &k_at, mysql, Some("V-106"));
            if cols.is_empty() {
                ctx.error("V-106", &k_at, None, "primary_key column list is empty");
            }
        }
        for (key, fields) in t.partitioning.iter().flatten() {
            let k_at = at.key("partitioning").key(key);
            table_key(ctx, key, &k_at, mysql, Some("V-106"));
            if fields.is_empty() {
                ctx.error("V-106", &k_at, None, "partitioning field list is empty");
            }
        }
    }
    let Some(sink_kind) = sink_kind else { return };
    // V-086
    for (key, fields) in t.partitioning.iter().flatten() {
        for (i, f) in fields.iter().enumerate() {
            if let Some(tr) = ctx.peek(&f.transform)
                && !sink_partitions(sink_kind, tr)
            {
                let l = f.transform.as_ref().expect("peek returned Some");
                ctx.error(
                    "V-086",
                    &at.key("partitioning").key(key).index(i).key("transform"),
                    l.loc(),
                    format!(
                        "the {sink_kind} sink does not support partition transform {} (v1 Delta: identity only)",
                        shown_value(l, &tr)
                    ),
                );
            }
        }
    }
    // V-087
    for (column, l) in t.column_type_overrides.iter().flatten() {
        if let Some(lt) = ctx.peek::<LogicalType>(&Some(l.clone()))
            && !sink_maps(sink_kind, &lt)
        {
            ctx.error(
                "V-087",
                &at.key("column_type_overrides").key(column),
                l.loc(),
                format!(
                    "the {sink_kind} sink cannot map logical type {}",
                    shown_value(l, &lt)
                ),
            );
        }
    }
}

fn sink(s: &RawSink, at: &At, kind: Option<SinkType>, cloud: Option<Cloud>, ctx: &mut Ctx) {
    // V-035, V-036, V-037
    if let Some(f) = &s.flush {
        let f_at = at.key("flush");
        let disabled = |t: &Option<Option<Leaf<u64>>>| matches!(t, Some(None));
        let bytes_disabled = matches!(f.bytes, Some(None));
        if disabled(&f.seconds) && disabled(&f.rows) && bytes_disabled {
            ctx.error(
                "V-035",
                &f_at,
                None,
                "all flush triggers are null: nothing would ever flush",
            );
        } else if disabled(&f.seconds) {
            ctx.warning("V-036", &f_at.key("seconds"), None, "time-based flushing is disabled: a quiet source may hold data unflushed for a long time");
        }
        if let Some(Some(l)) = &f.seconds
            && ctx
                .value(l, &f_at.key("seconds"))
                .is_some_and(|v| (1..5).contains(&v))
        {
            ctx.warning(
                "V-037",
                &f_at.key("seconds"),
                l.loc(),
                "flush.seconds below 5 causes write amplification",
            );
        }
    }
    // V-085
    if kind == Some(SinkType::Iceberg)
        && ctx.peek(&s.write_mode) == Some(WriteMode::Changelog)
        && let Some(l) = s.iceberg.as_ref().and_then(|i| i.strategy.as_ref())
    {
        ctx.error(
            "V-085",
            &at.key("iceberg").key("strategy"),
            l.loc(),
            "sink.iceberg.strategy applies only to write_mode mirror",
        );
    }
    // V-093
    if kind == Some(SinkType::Delta)
        && let Some(d) = &s.delta
    {
        for (key, l) in d.table_properties.iter().flatten() {
            if key.starts_with("delta.") && !DELTA_PROPERTY_ALLOW_LIST.contains(&key.as_str()) {
                ctx.error(
                    "V-093",
                    &at.key("delta").key("table_properties").key(key),
                    l.loc(),
                    format!(
                        "table property {key} is not allowed; allowed delta.* keys: {}",
                        DELTA_PROPERTY_ALLOW_LIST.join(", ")
                    ),
                );
            }
        }
    }
    // V-028, V-029 (and V-047 in `auth`)
    if kind == Some(SinkType::Iceberg)
        && let Some(c) = s.iceberg.as_ref().and_then(|i| i.catalog.as_ref())
        && cloud.is_some_and(|c| c != Cloud::Aws)
    {
        let c_at = at.key("iceberg").key("catalog");
        if c.glue.is_some() {
            ctx.error(
                "V-028",
                &c_at.key("glue"),
                None,
                "the Glue catalog requires globals.cloud aws",
            );
        }
        if let Some(r) = &c.rest
            && ctx.peek(&r.auth) == Some(RestAuth::Sigv4)
        {
            ctx.error(
                "V-029",
                &c_at.key("rest").key("auth"),
                r.auth.as_ref().and_then(|l| l.loc()),
                "rest auth sigv4 requires globals.cloud aws",
            );
        }
    }
}
