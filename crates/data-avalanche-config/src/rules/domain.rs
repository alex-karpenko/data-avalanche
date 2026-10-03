//! §13.3 value domain. Enumerations are checked by coercion (each enum carries its V-rule, see
//! `crate::enums`); this module checks ranges, reserved values and the v1 rejections.

use std::time::Duration;

use super::pipeline_at;
use crate::coerce::LeafType;
use crate::ctx::{At, Ctx, is_templated, shown, shown_value};
use crate::enums::*;
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;
use crate::units::ByteSize;

/// The value of `leaf` at `at`, with its location for the message.
fn val<T: LeafType>(
    ctx: &mut Ctx,
    leaf: &Option<Leaf<T>>,
    at: &At,
) -> Option<(T, Option<crate::leaf::Loc>)> {
    let l = leaf.as_ref()?;
    ctx.value(l, at).map(|v| (v, l.loc()))
}

fn at_least<T: LeafType + PartialOrd + std::fmt::Display>(
    ctx: &mut Ctx,
    rule: &str,
    leaf: &Option<Leaf<T>>,
    at: &At,
    min: T,
    field: &str,
) {
    if let Some((v, loc)) = val(ctx, leaf, at)
        && v < min
    {
        let l = leaf.as_ref().expect("val returned Some");
        ctx.error(
            rule,
            at,
            loc,
            format!("{field} must be at least {min}, got {}", shown_value(l, &v)),
        );
    }
}

pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    if let Some(g) = &raw.globals {
        globals(g, ctx);
    }
    for (name, p) in merged {
        pipeline_fields(&pipeline_at(name), p, ctx, true);
    }
}

/// §8.6: the single-field rules of this module, run directly on `raw.defaults` at its own path
/// (`defaults`, locus config) — before any pipeline merges it in, so an invalid value rejects the
/// config even when nothing inherits it. `cross_field: false` skips the two checks below that
/// relate sibling fields rather than reading one on its own; everything else here is unconditional
/// and doesn't need the pipeline's engine or sink format, so `pipeline_fields` runs it unchanged.
pub(crate) fn check_defaults(defaults: &RawPipeline, ctx: &mut Ctx) {
    let at = At::config(YamlPath::root().key("defaults"));
    pipeline_fields(&at, defaults, ctx, false);
}

fn globals(g: &RawGlobals, ctx: &mut Ctx) {
    let root = At::config(YamlPath::root().key("globals"));
    if let Some(c) = &g.cluster {
        let at = root.key("cluster");
        // V-026
        if let Some((tokens, loc)) = val(ctx, &c.tokens, &at.key("tokens")) {
            let l = c.tokens.as_ref().expect("val returned Some");
            if !(1..=4096).contains(&tokens) {
                ctx.error(
                    "V-026",
                    &at.key("tokens"),
                    loc,
                    format!("tokens must be 1..=4096, got {}", shown_value(l, &tokens)),
                );
            } else if !(64..=256).contains(&tokens) {
                ctx.warning(
                    "V-026",
                    &at.key("tokens"),
                    loc,
                    format!(
                        "tokens {} is outside the recommended 64..=256",
                        shown_value(l, &tokens)
                    ),
                );
            }
        }
        // V-072
        if let Some((m, loc)) = val(ctx, &c.lease_safety_margin, &at.key("lease_safety_margin")) {
            let l = c.lease_safety_margin.as_ref().expect("val returned Some");
            if !(0.0..=0.5).contains(&m) {
                ctx.error(
                    "V-072",
                    &at.key("lease_safety_margin"),
                    loc,
                    format!(
                        "lease_safety_margin must be 0..=0.5, got {}",
                        shown_value(l, &m)
                    ),
                );
            } else if !(0.05..=0.30).contains(&m) {
                ctx.warning(
                    "V-072",
                    &at.key("lease_safety_margin"),
                    loc,
                    format!(
                        "lease_safety_margin {} is outside the recommended 0.05..=0.30",
                        shown_value(l, &m)
                    ),
                );
            }
        }
        // V-074
        at_least(
            ctx,
            "V-074",
            &c.max_concurrent_handoffs,
            &at.key("max_concurrent_handoffs"),
            1,
            "max_concurrent_handoffs",
        );
        at_least(
            ctx,
            "V-074",
            &c.max_concurrent_takeovers,
            &at.key("max_concurrent_takeovers"),
            1,
            "max_concurrent_takeovers",
        );
        // V-075
        if let Some((LockBackend::Kubernetes, loc)) =
            val(ctx, &c.lock_backend, &at.key("lock_backend"))
        {
            ctx.error(
                "V-075",
                &at.key("lock_backend"),
                loc,
                "lock_backend kubernetes is reserved and not implemented in v1; use storage",
            );
        }
    }
    // V-097
    if let Some(t) = &g.tracing
        && let Some((r, loc)) = val(
            ctx,
            &t.sample_ratio,
            &root.key("tracing").key("sample_ratio"),
        )
        && !(0.0..=1.0).contains(&r)
    {
        let l = t.sample_ratio.as_ref().expect("val returned Some");
        ctx.error(
            "V-097",
            &root.key("tracing").key("sample_ratio"),
            loc,
            format!("sample_ratio must be 0..=1, got {}", shown_value(l, &r)),
        );
    }
    // V-101
    if let Some(a) = &g.azure
        && let Some((name, loc)) = val(ctx, &a.account_name, &root.key("azure").key("account_name"))
    {
        let ok = (3..=24).contains(&name.len())
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
        if !ok {
            let leaf = a.account_name.as_ref().expect("val returned Some");
            ctx.error(
                "V-101",
                &root.key("azure").key("account_name"),
                loc,
                format!(
                    "{} is not a storage account name (3-24 lower-case letters and digits)",
                    shown(leaf, &name)
                ),
            );
        }
    }
}

/// The rules of this module that read one field of a `RawPipeline`-shaped block. Shared by the
/// per-pipeline pass (`cross_field: true`, unchanged) and the `defaults`-alone pass
/// (`cross_field: false`, which skips V-107 — a partition's `n`/`width` against its `transform` —
/// and V-112's backoff-order half — `base_backoff` against `max_backoff`).
fn pipeline_fields(at: &At, p: &RawPipeline, ctx: &mut Ctx, cross_field: bool) {
    // V-118
    if let Some(t) = p.source.as_ref().and_then(|s| s.timeouts.as_ref()) {
        let snap_at = at.key("source").key("timeouts").key("snapshot");
        if let Some((v, loc)) = val(ctx, &t.snapshot, &snap_at)
            && v < Duration::from_secs(10)
        {
            let l = t.snapshot.as_ref().expect("val returned Some");
            // Duration has no `Display`: redact by hand, like V-112.
            let shown = if is_templated(l) {
                format!("{:?} (resolved value hidden)", l.text())
            } else {
                crate::units::format_duration(v)
            };
            ctx.error(
                "V-118",
                &snap_at,
                loc,
                format!("timeouts.snapshot must be at least 10s, got {shown}"),
            );
        }
    }
    if let Some(pg) = p.source.as_ref().and_then(|s| s.postgres.as_ref()) {
        // V-104
        let lag_at = at.key("source").key("postgres").key("max_wal_lag_bytes");
        if let Some(Some(l)) = &pg.max_wal_lag_bytes
            && let Some(v) = ctx.value(l, &lag_at)
            && v.bytes() < 64 * ByteSize::MIB
        {
            ctx.error(
                "V-104",
                &lag_at,
                l.loc(),
                format!(
                    "max_wal_lag_bytes must be at least 64MiB when set, got {}",
                    shown_value(l, &v)
                ),
            );
        }
    }
    if let Some(t) = &p.tables {
        let t_at = at.key("tables");
        if let Some(s) = &t.snapshots {
            let s_at = t_at.key("snapshots");
            // V-027, V-108
            at_least(
                ctx,
                "V-027",
                &s.parallelism,
                &s_at.key("parallelism"),
                1,
                "snapshots.parallelism",
            );
            if let Some((n, loc)) = val(ctx, &s.chunk_size, &s_at.key("chunk_size")) {
                if n < 1 {
                    ctx.error(
                        "V-108",
                        &s_at.key("chunk_size"),
                        loc,
                        "snapshots.chunk_size must be at least 1",
                    );
                } else if n > 1_000_000 {
                    let l = s.chunk_size.as_ref().expect("val returned Some");
                    ctx.warning(
                        "V-108",
                        &s_at.key("chunk_size"),
                        loc,
                        format!(
                            "snapshots.chunk_size {} is above 1000000: long chunk transactions and large memory",
                            shown_value(l, &n)
                        ),
                    );
                }
            }
        }
        // V-107: relates a partition's `transform` to its `n`/`width`, so it's skipped for the
        // `defaults`-alone pass (§8.6) — it still runs once the pipeline is merged.
        if cross_field {
            for (table, fields) in t.partitioning.iter().flatten() {
                for (i, f) in fields.iter().enumerate() {
                    partition_field(f, &t_at.key("partitioning").key(table).index(i), ctx);
                }
            }
        }
    }
    if let Some(s) = &p.sink {
        sink(s, &at.key("sink"), ctx, cross_field);
    }
}

fn partition_field(f: &RawPartitionField, at: &At, ctx: &mut Ctx) {
    let Some((transform, loc)) = val(ctx, &f.transform, &at.key("transform")) else {
        return;
    };
    let n = val(ctx, &f.n, &at.key("n"));
    let width = val(ctx, &f.width, &at.key("width"));
    match transform {
        Transform::Bucket => match n {
            Some((v, _)) if v >= 1 => {}
            _ => ctx.error("V-107", &at.key("n"), loc, "transform bucket needs n >= 1"),
        },
        Transform::Truncate => match width {
            Some((v, _)) if v >= 1 => {}
            _ => ctx.error(
                "V-107",
                &at.key("width"),
                loc,
                "transform truncate needs width >= 1",
            ),
        },
        _ => {}
    }
    if transform != Transform::Bucket && f.n.is_some() {
        let l = f.transform.as_ref().expect("val returned Some");
        ctx.error(
            "V-107",
            &at.key("n"),
            loc,
            format!(
                "n applies only to transform bucket, not {}",
                shown_value(l, &transform)
            ),
        );
    }
    if transform != Transform::Truncate && f.width.is_some() {
        let l = f.transform.as_ref().expect("val returned Some");
        ctx.error(
            "V-107",
            &at.key("width"),
            loc,
            format!(
                "width applies only to transform truncate, not {}",
                shown_value(l, &transform)
            ),
        );
    }
}

fn sink(s: &RawSink, at: &At, ctx: &mut Ctx, cross_field: bool) {
    // V-089
    if let Some((WriteMode::Changelog, loc)) = val(ctx, &s.write_mode, &at.key("write_mode")) {
        ctx.error(
            "V-089",
            &at.key("write_mode"),
            loc,
            "write_mode changelog is reserved and not available in v1; use mirror",
        );
    }
    // V-109
    let rows_at = at.key("max_transaction_batch_rows");
    if let Some(Some(l)) = &s.max_transaction_batch_rows
        && let Some(v) = ctx.value(l, &rows_at)
    {
        if v < 1 {
            ctx.error(
                "V-109",
                &rows_at,
                l.loc(),
                "max_transaction_batch_rows must be at least 1 when set",
            );
        } else if v < 1000 {
            ctx.warning(
                "V-109",
                &rows_at,
                l.loc(),
                format!(
                    "max_transaction_batch_rows {} is below 1000: many small commits",
                    shown_value(l, &v)
                ),
            );
        }
    }
    // V-110, V-111
    if let Some((v, loc)) = val(
        ctx,
        &s.max_transaction_buffer_bytes,
        &at.key("max_transaction_buffer_bytes"),
    ) && v.bytes() < 16 * ByteSize::MIB
    {
        let l = s
            .max_transaction_buffer_bytes
            .as_ref()
            .expect("val returned Some");
        ctx.error(
            "V-110",
            &at.key("max_transaction_buffer_bytes"),
            loc,
            format!(
                "max_transaction_buffer_bytes must be at least 16MiB, got {}",
                shown_value(l, &v)
            ),
        );
    }
    at_least(
        ctx,
        "V-111",
        &s.buffer_capacity_rows,
        &at.key("buffer_capacity_rows"),
        1,
        "buffer_capacity_rows",
    );
    if let Some(d) = &s.delta {
        let d_at = at.key("delta");
        // V-092, V-088
        if let Some((ColumnMapping::Name, loc)) =
            val(ctx, &d.column_mapping, &d_at.key("column_mapping"))
        {
            ctx.error("V-092", &d_at.key("column_mapping"), loc, "column_mapping name is rejected in v1: delta-rs refuses ADD COLUMN on column-mapped tables");
        }
        let cat_at = d_at.key("catalog").key("type");
        if let Some(c) = &d.catalog
            && let Some((kind, loc)) = val(ctx, &c.kind, &cat_at)
            && kind != DeltaCatalogType::None
        {
            ctx.error(
                "V-088",
                &cat_at,
                loc,
                format!(
                    "delta catalog type {kind} is reserved; v1 writes path-based tables (none)"
                ),
            );
        }
    }
    if let Some(c) = s.iceberg.as_ref().and_then(|i| i.catalog.as_ref()) {
        catalog(c, &at.key("iceberg").key("catalog"), ctx, cross_field);
    }
}

fn catalog(c: &RawCatalog, at: &At, ctx: &mut Ctx, cross_field: bool) {
    // V-112
    if let Some(r) = &c.retry {
        let r_at = at.key("retry");
        at_least(
            ctx,
            "V-112",
            &r.max_attempts,
            &r_at.key("max_attempts"),
            1,
            "retry.max_attempts",
        );
        // The backoff-order half relates `base_backoff` to `max_backoff`, so it's skipped for the
        // `defaults`-alone pass (§8.6) — it still runs once the pipeline is merged.
        if cross_field {
            let base = r
                .base_backoff
                .as_ref()
                .map_or(Some(Duration::from_secs(1)), |l| {
                    ctx.value(l, &r_at.key("base_backoff"))
                });
            let max = r
                .max_backoff
                .as_ref()
                .map_or(Some(Duration::from_secs(30)), |l| {
                    ctx.value(l, &r_at.key("max_backoff"))
                });
            if let (Some(b), Some(m)) = (base, max)
                && b > m
            {
                // Duration has no `Display`, so `shown_value` doesn't apply; redact by hand from
                // the same "is this leaf a template" test it uses.
                let shown_duration = |leaf: &Option<Leaf<Duration>>, d: Duration| match leaf {
                    Some(l) if is_templated(l) => format!("{:?} (resolved value hidden)", l.text()),
                    _ => crate::units::format_duration(d),
                };
                ctx.error(
                    "V-112",
                    &r_at,
                    None,
                    format!(
                        "retry.base_backoff {} is above max_backoff {}",
                        shown_duration(&r.base_backoff, b),
                        shown_duration(&r.max_backoff, m)
                    ),
                );
            }
        }
    }
    // V-115
    let hive_declared = ctx.peek(&c.kind) == Some(CatalogKind::Hive);
    if let Some(h) = &c.hive {
        ctx.error("V-115", &at.key("hive"), h.url.as_ref().and_then(|l| l.loc()), "the Hive catalog is rejected: the pinned iceberg-rust HMS catalog cannot commit (DA-CAT-6)");
    } else if hive_declared {
        ctx.error("V-115", &at.key("type"), c.kind.as_ref().and_then(|l| l.loc()), "the Hive catalog is rejected: the pinned iceberg-rust HMS catalog cannot commit (DA-CAT-6)");
    }
    // V-116
    if let Some(r) = &c.rest
        && let Some((RestAuth::Sigv4, loc)) = val(ctx, &r.auth, &at.key("rest").key("auth"))
    {
        ctx.error("V-116", &at.key("rest").key("auth"), loc, "rest auth sigv4 is rejected: the pinned iceberg-rust REST client has no SigV4 signing (DA-CAT-7); on AWS use catalog.glue");
    }
}
