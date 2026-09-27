//! Merge & override (config §8): effective pipeline = `defaults` deep-merged with the pipeline, the
//! pipeline winning. Scalars and maps merge key by key; lists are replaced; the three union
//! subtrees keep only the active branch from `defaults`.

use std::collections::BTreeMap;

use crate::ctx::Ctx;
use crate::enums::{CatalogKind, SinkType, SourceType};
use crate::leaf::Leaf;
use crate::raw::*;

/// `base` is the `defaults` value, `over` the pipeline's. The context resolves union
/// discriminators (`source.type`, …) without reporting; validation reports them later.
pub trait Merge: Sized {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self;
}

/// A scalar: the pipeline's value if present.
impl<T> Merge for Option<Leaf<T>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        over.or(base)
    }
}

/// A tri-state scalar (§9.4.1): an explicit `null` in the pipeline overrides a default value.
impl<T> Merge for Option<Option<Leaf<T>>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        over.or(base)
    }
}

/// A list is replaced wholesale, never merged element-wise (§8.3).
impl<T> Merge for Option<Vec<T>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        over.or(base)
    }
}

/// A tri-state list (§9.4.1 applied to a list field, e.g. `tables.include`/`exclude`): an explicit
/// `null` in the pipeline (`Some(None)`, meaning "no list") overrides a default list, same as a
/// tri-state scalar.
impl<T> Merge for Option<Option<Vec<T>>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        over.or(base)
    }
}

/// A map merges key by key; a key in the pipeline replaces the same key in `defaults` (§8.2).
impl<V> Merge for Option<BTreeMap<String, V>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        match (base, over) {
            (Some(mut b), Some(o)) => {
                b.extend(o);
                Some(b)
            }
            (b, o) => o.or(b),
        }
    }
}

/// Field-by-field merge for a plain object. Also generates [`crate::walk::Walk`] for the same
/// struct from the same field list (`crate::walk::walk_fields!`), so the raw-model walker (config
/// §11 Phase 2, §8.6) can't drift from this one by hand-maintaining a second copy of it.
macro_rules! merge_struct {
    ($t:ident { $($f:ident),+ $(,)? }) => {
        impl Merge for Option<$t> {
            fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
                match (base, over) {
                    (Some(b), Some(o)) => Some($t { $($f: Merge::merge(b.$f, o.$f, ctx)),+ }),
                    (b, o) => o.or(b),
                }
            }
        }
        crate::walk::walk_fields!($t { $($f),+ });
    };
}

merge_struct!(RawPipeline {
    id,
    source,
    tables,
    sink,
    state,
    schema_evolution
});
merge_struct!(RawTimeouts {
    connect,
    command,
    snapshot
});
merge_struct!(RawPostgres {
    publication,
    slot,
    auto_cleanup,
    heartbeat,
    slot_management,
    slot_on_orphan,
    max_wal_lag_bytes,
});
merge_struct!(RawMysql {
    replication_type,
    tinyint1_as_bool,
    tinyint1_detection,
    binlog_purge_action
});
merge_struct!(RawTables {
    include,
    exclude,
    snapshots,
    column_type_overrides,
    numeric_overflow_action,
    primary_key,
    partitioning,
});
merge_struct!(RawSnapshots {
    mode,
    parallelism,
    chunk_size,
    overrides
});
merge_struct!(RawFlush {
    seconds,
    rows,
    bytes
});
merge_struct!(RawSinkMetrics { per_table });
merge_struct!(RawIceberg { strategy, catalog });
merge_struct!(RawGlue {
    name,
    database,
    strip_schema
});
merge_struct!(RawHive {
    url,
    auth,
    user,
    password,
    credentials_file,
    kerberos,
    tls,
    database
});
merge_struct!(RawKerberos {
    principal,
    keytab,
    service_name
});
merge_struct!(RawHiveTls { enabled, ca });
merge_struct!(RawRest {
    url,
    warehouse,
    prefix,
    auth,
    token,
    token_file,
    oauth2,
    sigv4,
    tls
});
merge_struct!(RawRestOauth2 {
    client_id,
    client_secret,
    credentials_file,
    token_url,
    scope
});
merge_struct!(RawSigv4 { region, service });
merge_struct!(RawRestTls { ca });
merge_struct!(RawRetry {
    max_attempts,
    base_backoff,
    max_backoff
});
merge_struct!(RawDelta {
    column_mapping,
    checkpoint_interval,
    log_retention,
    timestamp_ntz_as,
    table_properties,
    catalog
});
merge_struct!(RawDeltaCatalog { kind });
merge_struct!(RawSchemaEvolution {
    enabled,
    rescan_interval
});

/// Both sides, defaulted when one is absent; `None` when both are.
fn both<T: Default>(base: Option<T>, over: Option<T>) -> Option<(T, T)> {
    match (base, over) {
        (None, None) => None,
        (b, o) => Some((b.unwrap_or_default(), o.unwrap_or_default())),
    }
}

/// Union §8.4: the `source` engine block. The active branch is `source.type` after merge (default
/// `postgres`); `defaults`' other engine block is dropped. With a template type the branch is
/// unknown in the structural pass: only the pipeline's own blocks are kept.
impl Merge for Option<RawSource> {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        let kind = Merge::merge(b.kind, o.kind, ctx);
        let active = match &kind {
            None => Some(SourceType::Postgres),
            Some(_) => ctx.peek(&kind),
        };
        let (postgres, mysql) = match active {
            Some(SourceType::Postgres) => (Merge::merge(b.postgres, o.postgres, ctx), o.mysql),
            Some(SourceType::Mysql) => (o.postgres, Merge::merge(b.mysql, o.mysql, ctx)),
            None => (o.postgres, o.mysql),
        };
        Some(RawSource {
            kind,
            host: Merge::merge(b.host, o.host, ctx),
            port: Merge::merge(b.port, o.port, ctx),
            db: Merge::merge(b.db, o.db, ctx),
            user: Merge::merge(b.user, o.user, ctx),
            password: Merge::merge(b.password, o.password, ctx),
            credentials_file: Merge::merge(b.credentials_file, o.credentials_file, ctx),
            ssl_mode: Merge::merge(b.ssl_mode, o.ssl_mode, ctx),
            ssl_cert: Merge::merge(b.ssl_cert, o.ssl_cert, ctx),
            ssl_key: Merge::merge(b.ssl_key, o.ssl_key, ctx),
            ssl_ca: Merge::merge(b.ssl_ca, o.ssl_ca, ctx),
            timeouts: Merge::merge(b.timeouts, o.timeouts, ctx),
            toast_strategy: Merge::merge(b.toast_strategy, o.toast_strategy, ctx),
            postgres,
            mysql,
        })
    }
}

/// Union §8.4: the `sink` format block, selected by `sink.type` after merge (no default, V-091).
impl Merge for Option<RawSink> {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        let kind = Merge::merge(b.kind, o.kind, ctx);
        let (iceberg, delta) = match ctx.peek(&kind) {
            Some(SinkType::Iceberg) => (Merge::merge(b.iceberg, o.iceberg, ctx), o.delta),
            Some(SinkType::Delta) => (o.iceberg, Merge::merge(b.delta, o.delta, ctx)),
            None => (o.iceberg, o.delta),
        };
        Some(RawSink {
            kind,
            write_mode: Merge::merge(b.write_mode, o.write_mode, ctx),
            bucket: Merge::merge(b.bucket, o.bucket, ctx),
            prefix: Merge::merge(b.prefix, o.prefix, ctx),
            flush: Merge::merge(b.flush, o.flush, ctx),
            max_transaction_batch_rows: Merge::merge(
                b.max_transaction_batch_rows,
                o.max_transaction_batch_rows,
                ctx,
            ),
            max_transaction_buffer_bytes: Merge::merge(
                b.max_transaction_buffer_bytes,
                o.max_transaction_buffer_bytes,
                ctx,
            ),
            buffer_capacity_rows: Merge::merge(b.buffer_capacity_rows, o.buffer_capacity_rows, ctx),
            metrics: Merge::merge(b.metrics, o.metrics, ctx),
            iceberg,
            delta,
        })
    }
}

fn single_block(c: &RawCatalog) -> Option<CatalogKind> {
    match (c.glue.is_some(), c.hive.is_some(), c.rest.is_some()) {
        (true, false, false) => Some(CatalogKind::Glue),
        (false, true, false) => Some(CatalogKind::Hive),
        (false, false, true) => Some(CatalogKind::Rest),
        _ => None,
    }
}

fn block_count(c: &RawCatalog) -> usize {
    [c.glue.is_some(), c.hive.is_some(), c.rest.is_some()]
        .into_iter()
        .filter(|b| *b)
        .count()
}

/// Union §8.4: `sink.iceberg.catalog`. The pipeline picks the branch by its `type`, else by its
/// single sub-block; a pipeline with no sub-block inherits `defaults`' branch. `defaults`' `type`
/// is dropped when the pipeline picks by block, so a defaulted `type: glue` never contradicts a
/// pipeline's `rest:` (§8.5).
impl Merge for Option<RawCatalog> {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        let (kind, active) = if o.kind.is_some() {
            let active = ctx.peek(&o.kind);
            (o.kind.clone(), active)
        } else {
            match block_count(&o) {
                0 => (
                    b.kind.clone(),
                    ctx.peek(&b.kind).or_else(|| single_block(&b)),
                ),
                1 => (None, single_block(&o)),
                _ => (None, None),
            }
        };
        let pick = |k: CatalogKind| active == Some(k);
        Some(RawCatalog {
            kind,
            glue: if pick(CatalogKind::Glue) {
                Merge::merge(b.glue, o.glue, ctx)
            } else {
                o.glue
            },
            hive: if pick(CatalogKind::Hive) {
                Merge::merge(b.hive, o.hive, ctx)
            } else {
                o.hive
            },
            rest: if pick(CatalogKind::Rest) {
                Merge::merge(b.rest, o.rest, ctx)
            } else {
                o.rest
            },
            retry: Merge::merge(b.retry, o.retry, ctx),
        })
    }
}

/// `state.bucket` merges normally; `state.prefix` does not: `defaults.state.prefix` is a parent
/// prefix of every explicit state location (§9.6, §10), which the resolved build applies. Were it
/// inherited, every pipeline would share one state location.
impl Merge for Option<RawPipelineState> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        Some(RawPipelineState {
            bucket: o.bucket.or(b.bucket),
            prefix: o.prefix,
        })
    }
}

/// The effective pipeline (§8.1): `defaults` merged with `pipeline`.
pub fn merge_pipeline(
    defaults: Option<&RawPipeline>,
    pipeline: &RawPipeline,
    ctx: &Ctx,
) -> RawPipeline {
    Merge::merge(defaults.cloned(), Some(pipeline.clone()), ctx).unwrap_or_default()
}
