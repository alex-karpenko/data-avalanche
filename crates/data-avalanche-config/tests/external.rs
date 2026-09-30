//! Rules that need input besides the config file: the cluster anchor, the resolved table set,
//! source column types (and V-061, which `tests/diff.rs` covers).

mod support;

use std::time::Duration;

use data_avalanche_config::{
    ClusterAnchor, LockBackend, RULES, ResolvedConfig, RuleInput, check_cluster_anchor,
    check_table_set, check_unbounded_numeric, load_str,
};
use data_avalanche_core::TableRef;
use support::{MINIMAL, env, fp_key};

fn load(yaml: &str) -> ResolvedConfig {
    load_str(yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config
}

fn anchor() -> ClusterAnchor {
    ClusterAnchor {
        tokens: 128,
        lock_backend: LockBackend::Storage,
        lease_duration: Duration::from_secs(15),
    }
}

fn rules(r: &data_avalanche_config::Report) -> Vec<&str> {
    r.iter().map(|d| d.rule.as_str()).collect()
}

#[test]
fn anchor_rules_v062_v076_v094() {
    let cfg = load(MINIMAL);
    assert!(check_cluster_anchor(&cfg.globals, &anchor()).is_ok());
    let other = ClusterAnchor {
        tokens: 64,
        lock_backend: LockBackend::Kubernetes,
        lease_duration: Duration::from_secs(30),
    };
    let report = check_cluster_anchor(&cfg.globals, &other);
    // Diagnostics come sorted by path.
    assert_eq!(rules(&report), ["V-094", "V-076", "V-062"]);
    assert!(
        report
            .errors
            .iter()
            .all(|d| d.path.starts_with("globals.cluster."))
    );
}

#[test]
fn table_set_rules_v034_v106() {
    let yaml = MINIMAL.replace(
        "include: [public.orders]",
        "include: [public.*]\n      snapshots: { overrides: { public.orders: always, public.gone: always } }\n      primary_key: { public.gone: [id] }",
    );
    let cfg = load(&yaml);
    let p = &cfg.pipelines["orders"];
    let report = check_table_set(p, &[TableRef::new("public", "orders")]);
    assert_eq!(rules(&report), ["V-106", "V-034"]);
    assert!(report.errors.is_empty(), "both are warnings");
    let report = check_table_set(
        p,
        &[
            TableRef::new("public", "orders"),
            TableRef::new("public", "gone"),
        ],
    );
    assert!(report.iter().next().is_none());
}

#[test]
fn mysql_keys_resolve_in_source_db() {
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "type: mysql\n      host: db.example.com",
        )
        .replace(
            "postgres: { publication: orders_pub, slot: orders_slot }",
            "mysql: {}",
        )
        .replace(
            "include: [public.orders]",
            "include: [orders]\n      snapshots: { overrides: { orders: always } }",
        );
    let cfg = load(&yaml);
    let report = check_table_set(&cfg.pipelines["orders"], &[TableRef::new("shop", "orders")]);
    assert!(report.iter().next().is_none(), "{report:#?}");
}

#[test]
fn glue_strip_schema_collision_v113() {
    let yaml = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        "sink:\n      type: iceberg\n      bucket: lake\n      iceberg:\n        catalog:\n          glue: { name: \"123\", strip_schema: true }",
    );
    let cfg = load(&yaml);
    let p = &cfg.pipelines["orders"];
    let report = check_table_set(
        p,
        &[
            TableRef::new("public", "orders"),
            TableRef::new("archive", "orders"),
        ],
    );
    assert_eq!(rules(&report), ["V-113"]);
    assert!(
        report.errors[0]
            .message
            .contains("archive.orders, public.orders")
    );
    assert!(
        check_table_set(
            p,
            &[
                TableRef::new("public", "orders"),
                TableRef::new("archive", "old")
            ]
        )
        .is_ok()
    );
}

#[test]
fn unbounded_numeric_v079() {
    let unbounded = [(TableRef::new("public", "orders"), "amount".to_string())];
    let cfg = load(MINIMAL);
    assert!(
        check_unbounded_numeric(&cfg.pipelines["orders"], &unbounded)
            .iter()
            .next()
            .is_none(),
        "default action string"
    );
    let cfg = load(&MINIMAL.replace(
        "include: [public.orders]",
        "include: [public.orders]\n      numeric_overflow_action: error",
    ));
    let report = check_unbounded_numeric(&cfg.pipelines["orders"], &unbounded);
    assert_eq!(rules(&report), ["V-079"]);
    assert!(report.warnings[0].message.contains("public.orders.amount"));
    assert!(
        check_unbounded_numeric(&cfg.pipelines["orders"], &[])
            .iter()
            .next()
            .is_none()
    );
}

/// Every rule that needs input besides the file has a test in this file or in `tests/diff.rs`.
#[test]
fn every_external_rule_is_covered() {
    const COVERED: [&str; 7] = [
        "V-034", "V-061", "V-062", "V-076", "V-079", "V-094", "V-113",
    ];
    let external: Vec<&str> = RULES
        .iter()
        .filter(|r| !matches!(r.input, RuleInput::Config | RuleInput::Resolution))
        .map(|r| r.id)
        .collect();
    assert_eq!(external, COVERED);
}
