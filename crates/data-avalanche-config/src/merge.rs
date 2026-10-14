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
pub(crate) trait Merge: Sized {
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
    binlog_purge_action,
    server_id,
    heartbeat
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
    catalog,
    merge_memory_limit
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
pub(crate) fn merge_pipeline(
    defaults: Option<&RawPipeline>,
    pipeline: &RawPipeline,
    ctx: &Ctx,
) -> RawPipeline {
    Merge::merge(defaults.cloned(), Some(pipeline.clone()), ctx).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    //! Merge & override (config §8), including the §8.5 worked example and the §14 example. Moved
    //! from `tests/merge.rs` (FU-E): `RawPipeline`, `RawConfig`, `merge_pipeline`, `Ctx` and
    //! `parse_document` are crate-internal.

    use super::*;
    use crate::parse::parse_document;

    fn effective(yaml: &str, name: &str) -> RawPipeline {
        let raw: RawConfig = parse_document(yaml).unwrap();
        let pipelines = raw.pipelines.unwrap();
        merge_pipeline(raw.defaults.as_ref(), &pipelines[name], &Ctx::structural())
    }

    /// §8.5, with the elided `oauth2: {...}` written out.
    const WORKED: &str = r#"
defaults:
  source: { type: postgres, host: db.internal, port: 5432, ssl_mode: require }
  sink:
    type: iceberg
    write_mode: mirror
    flush: { seconds: 30, rows: 100000, bytes: 512MiB }
    iceberg:
      strategy: mor
      catalog:
        glue: { name: "123456789012", database: analytics }
pipelines:
  orders:
    id: orders
    source: { db: orders, user: cdc, password: "${PW}", postgres: { publication: p, slot: s } }
    sink:
      bucket: lake
      flush: { rows: 50000 }
      iceberg:
        catalog: { rest: { url: "https://cat.example.com", auth: oauth2, oauth2: { token_url: "https://idp/t" } } }
  clicks:
    id: clicks
    source: { db: clicks, user: cdc, password: "${PW}", postgres: { publication: pc, slot: sc } }
    sink:
      type: delta
      bucket: lake
      delta: { checkpoint_interval: 20 }
"#;

    #[test]
    fn worked_example_orders() {
        let p = effective(WORKED, "orders");
        let src = p.source.unwrap();
        assert_eq!(src.host.unwrap().text(), "db.internal");
        assert_eq!(src.ssl_mode.unwrap().text(), "require");
        assert_eq!(src.db.unwrap().text(), "orders");
        assert_eq!(src.postgres.unwrap().slot.unwrap().text(), "s");
        let sink = p.sink.unwrap();
        assert_eq!(sink.kind.unwrap().text(), "iceberg");
        assert_eq!(sink.write_mode.unwrap().text(), "mirror");
        let flush = sink.flush.unwrap();
        assert_eq!(flush.seconds.unwrap().unwrap().text(), "30");
        assert_eq!(flush.rows.unwrap().unwrap().text(), "50000");
        assert_eq!(flush.bytes.unwrap().unwrap().text(), "512MiB");
        let iceberg = sink.iceberg.unwrap();
        assert_eq!(iceberg.strategy.unwrap().text(), "mor");
        let catalog = iceberg.catalog.unwrap();
        assert!(
            catalog.glue.is_none(),
            "the defaulted glue branch is dropped"
        );
        assert_eq!(
            catalog.rest.unwrap().url.unwrap().text(),
            "https://cat.example.com"
        );
        assert!(sink.delta.is_none());
    }

    #[test]
    fn worked_example_clicks_drops_the_defaulted_iceberg_block() {
        let sink = effective(WORKED, "clicks").sink.unwrap();
        assert_eq!(sink.kind.unwrap().text(), "delta");
        assert!(sink.iceberg.is_none());
        assert_eq!(
            sink.delta.unwrap().checkpoint_interval.unwrap().text(),
            "20"
        );
        assert_eq!(sink.write_mode.unwrap().text(), "mirror");
        assert_eq!(sink.flush.unwrap().rows.unwrap().unwrap().text(), "100000");
    }

    #[test]
    fn spec_example_mysql_pipeline_drops_both_defaulted_branches() {
        let p = effective(
            include_str!("../tests/fixtures/spec_example.yaml"),
            "events-mysql",
        );
        let src = p.source.unwrap();
        assert!(
            src.postgres.is_none(),
            "defaults.source.postgres is dropped for type: mysql"
        );
        assert_eq!(src.mysql.unwrap().replication_type.unwrap().text(), "gtid");
        assert_eq!(src.timeouts.unwrap().snapshot.unwrap().text(), "6h");
        let sink = p.sink.unwrap();
        assert!(sink.iceberg.is_none());
        assert_eq!(
            p.schema_evolution.unwrap().rescan_interval.unwrap().text(),
            "60m"
        );
    }

    #[test]
    fn spec_example_pg_pipeline_merges_engine_block_and_inherits_catalog() {
        let p = effective(
            include_str!("../tests/fixtures/spec_example.yaml"),
            "orders-pg",
        );
        let pg = p.source.unwrap().postgres.unwrap();
        assert_eq!(pg.slot.unwrap().text(), "orders_slot");
        assert_eq!(
            pg.heartbeat.unwrap().text(),
            "60s",
            "same-branch fields merge"
        );
        let sink = p.sink.unwrap();
        let rest = sink.iceberg.unwrap().catalog.unwrap().rest.unwrap();
        assert_eq!(rest.warehouse.unwrap().text(), "analytics");
        assert!(p.state.is_some());
        assert!(
            p.state.unwrap().prefix.is_none(),
            "defaults.state.prefix is a parent, not inherited"
        );
    }

    #[test]
    fn lists_replace_maps_merge_by_key_and_null_overrides() {
        let yaml = r#"
defaults:
  tables:
    include: [a.x, a.y]
    snapshots: { overrides: { a.x: always, a.y: never } }
  sink: { flush: { seconds: 30 }, max_transaction_batch_rows: 5000 }
pipelines:
  p:
    tables:
      include: [b.z]
      snapshots: { overrides: { a.y: initial } }
    sink: { flush: { seconds: null }, max_transaction_batch_rows: null }
"#;
        let p = effective(yaml, "p");
        let tables = p.tables.unwrap();
        let include: Vec<_> = tables
            .include
            .unwrap()
            .unwrap()
            .iter()
            .map(|l| l.text().to_string())
            .collect();
        assert_eq!(include, ["b.z"]);
        let overrides = tables.snapshots.unwrap().overrides.unwrap();
        assert_eq!(overrides["a.x"].text(), "always");
        assert_eq!(overrides["a.y"].text(), "initial");
        let sink = p.sink.unwrap();
        assert!(
            matches!(sink.flush.unwrap().seconds, Some(None)),
            "explicit null wins over 30"
        );
        assert!(matches!(sink.max_transaction_batch_rows, Some(None)));
    }

    /// F3: `null` on `tables.include`/`exclude` means "no list", not "inherit `defaults`" (Review
    /// Focus 2).
    #[test]
    fn explicit_null_clears_an_inherited_exclude_list() {
        let yaml = r#"
defaults:
  tables:
    exclude: [public.tmp_x]
pipelines:
  p:
    tables:
      exclude: null
"#;
        let tables = effective(yaml, "p").tables.unwrap();
        assert!(
            matches!(tables.exclude, Some(None)),
            "null overrides the inherited list, it doesn't inherit it: {:?}",
            tables.exclude
        );
    }

    #[test]
    fn explicit_null_on_include_leaves_no_list_for_v007() {
        let yaml = r#"
defaults:
  tables:
    include: [public.orders]
pipelines:
  p:
    tables:
      include: null
"#;
        let tables = effective(yaml, "p").tables.unwrap();
        assert!(
            matches!(tables.include, Some(None)),
            "null overrides the inherited list; V-007 then sees an empty list and fires: {:?}",
            tables.include
        );
    }

    #[test]
    fn absent_pipeline_exclude_still_inherits_defaults() {
        let yaml = r#"
defaults:
  tables:
    exclude: [public.tmp_x]
pipelines:
  p:
    tables: {}
"#;
        let tables = effective(yaml, "p").tables.unwrap();
        let exclude: Vec<_> = tables
            .exclude
            .unwrap()
            .unwrap()
            .iter()
            .map(|l| l.text().to_string())
            .collect();
        assert_eq!(exclude, ["public.tmp_x"]);
    }

    #[test]
    fn catalog_type_from_defaults_is_dropped_when_the_pipeline_picks_a_block() {
        let yaml = r#"
defaults:
  sink: { type: iceberg, iceberg: { catalog: { type: glue, glue: { name: a }, retry: { max_attempts: 3 } } } }
pipelines:
  p:
    sink: { iceberg: { catalog: { rest: { url: "https://c" } } } }
  q:
    sink: { iceberg: { catalog: { retry: { max_backoff: 10s } } } }
"#;
        let c = effective(yaml, "p")
            .sink
            .unwrap()
            .iceberg
            .unwrap()
            .catalog
            .unwrap();
        assert!(c.kind.is_none() && c.glue.is_none() && c.rest.is_some());
        assert_eq!(c.retry.unwrap().max_attempts.unwrap().text(), "3");
        // A pipeline without a sub-block inherits the defaults' branch and type.
        let c = effective(yaml, "q")
            .sink
            .unwrap()
            .iceberg
            .unwrap()
            .catalog
            .unwrap();
        assert_eq!(c.kind.unwrap().text(), "glue");
        assert_eq!(c.glue.unwrap().name.unwrap().text(), "a");
        let retry = c.retry.unwrap();
        assert_eq!(retry.max_backoff.unwrap().text(), "10s");
        assert_eq!(retry.max_attempts.unwrap().text(), "3");
    }

    #[test]
    fn template_discriminator_keeps_only_the_pipelines_blocks() {
        let yaml = r#"
defaults:
  source: { type: postgres, postgres: { heartbeat: 30s } }
pipelines:
  p:
    source: { type: "${SRC}", mysql: { replication_type: gtid } }
"#;
        let src = effective(yaml, "p").source.unwrap();
        assert!(
            src.postgres.is_none(),
            "unknown branch: nothing inherited from defaults"
        );
        assert!(src.mysql.is_some());
    }

    #[test]
    fn pipeline_without_a_source_still_drops_the_defaults_foreign_block() {
        let yaml = r#"
defaults:
  source: { type: mysql, mysql: { replication_type: binlog }, postgres: { heartbeat: 30s } }
pipelines:
  p: { id: p }
"#;
        let src = effective(yaml, "p").source.unwrap();
        assert!(src.postgres.is_none());
        assert_eq!(
            src.mysql.unwrap().replication_type.unwrap().text(),
            "binlog"
        );
    }
}
