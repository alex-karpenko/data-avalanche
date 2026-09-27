//! Merge & override (config §8), including the §8.5 worked example and the §14 example.

use data_avalanche_config::raw::RawPipeline;
use data_avalanche_config::{Ctx, RawConfig, merge_pipeline, parse_document};

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
    let p = effective(include_str!("fixtures/spec_example.yaml"), "events-mysql");
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
    let p = effective(include_str!("fixtures/spec_example.yaml"), "orders-pg");
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
