//! The reload diff (config §12) and the mutability classes (§1.2).

mod support;

use data_avalanche_config::diff::unclassified_paths;
use data_avalanche_config::{Mutability, PipelineChange, ResolvedConfig, diff, load_str};
use support::{Files, MINIMAL, cert, env, key, spec_example};

fn load(yaml: &str) -> ResolvedConfig {
    load_str(yaml, &env(&[("PW", "pw")]))
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config
}

fn unknown(
    _: &data_avalanche_config::Pipeline,
    _: &data_avalanche_config::Pipeline,
) -> Option<bool> {
    None
}

fn modified(
    d: &data_avalanche_config::ConfigDiff,
) -> (bool, Vec<(String, Mutability)>, Option<String>) {
    match d.pipelines.as_slice() {
        [
            PipelineChange::Modified {
                restart,
                fields,
                renamed_from,
                ..
            },
        ] => (
            *restart,
            fields.iter().map(|f| (f.path.clone(), f.class)).collect(),
            renamed_from.clone(),
        ),
        other => panic!("expected one modified pipeline, got {other:#?}"),
    }
}

#[test]
fn identical_configs_have_no_changes() {
    let d = diff(&load(MINIMAL), &load(MINIMAL), &unknown);
    assert!(d.is_empty() && !d.is_rejected());
}

#[test]
fn r_change_applies_in_place() {
    let new = MINIMAL.replace("delta: {}", "delta: {}\n      flush: { rows: 5000 }");
    let (restart, fields, _) = modified(&diff(&load(MINIMAL), &load(&new), &unknown));
    assert!(!restart);
    assert_eq!(
        fields,
        [(
            "pipelines.orders.sink.flush.rows".to_string(),
            Mutability::R
        )]
    );
}

#[test]
fn pr_change_restarts_the_pipeline() {
    let new = MINIMAL.replace("db.example.com", "db2.example.com");
    let (restart, fields, _) = modified(&diff(&load(MINIMAL), &load(&new), &unknown));
    assert!(restart);
    assert_eq!(
        fields,
        [("pipelines.orders.source.host".to_string(), Mutability::Pr)]
    );
}

#[test]
fn proc_change_rejects_the_reload_with_v061() {
    let new = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  cluster: { tokens: 64 }",
    );
    let d = diff(&load(MINIMAL), &load(&new), &unknown);
    assert!(d.is_rejected());
    let e = &d.report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-061", "globals.cluster.tokens")
    );
    assert_eq!(d.globals[0].class, Mutability::Proc);
}

#[test]
fn log_level_is_hot() {
    let new = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  log: { level: debug }",
    );
    let d = diff(&load(MINIMAL), &load(&new), &unknown);
    assert!(!d.is_rejected());
    assert_eq!(d.globals[0].path, "globals.log.level");
    assert_eq!(d.globals[0].class, Mutability::R);
}

#[test]
fn rotated_credentials_file_is_detected_without_a_text_change() {
    let files = Files::new();
    let path = files.write("creds.json", r#"{"user": "cdc", "password": "old"}"#);
    let yaml = MINIMAL.replace(
        "user: cdc\n      password: ${PW}",
        &format!("credentials_file: {}", path.display()),
    );
    let old = load(&yaml);
    files.write("creds.json", r#"{"user": "cdc", "password": "new"}"#);
    let new = load(&yaml);
    let d = diff(&old, &new, &unknown);
    let (restart, fields, _) = modified(&d);
    assert!(restart, "source credentials are PR");
    assert_eq!(fields[0].0, "pipelines.orders.source.credentials.password");
    let json = serde_json::to_string(&d).unwrap();
    assert!(
        !json.contains("old") && !json.contains("new\""),
        "values never appear: {json}"
    );
}

#[test]
fn rename_keeps_the_pipeline() {
    let new = MINIMAL.replace("  orders:\n    id: orders", "  orders-v2:\n    id: orders");
    let (restart, fields, renamed_from) = modified(&diff(&load(MINIMAL), &load(&new), &unknown));
    assert!(!restart);
    assert_eq!(renamed_from.as_deref(), Some("orders"));
    assert_eq!(
        fields,
        [("pipelines.orders.name".to_string(), Mutability::R)]
    );
}

#[test]
fn id_change_is_remove_plus_add() {
    let new = MINIMAL.replace("id: orders", "id: orders2");
    let d = diff(&load(MINIMAL), &load(&new), &unknown);
    assert!(matches!(&d.pipelines[..], [
        PipelineChange::Removed { id: a, .. },
        PipelineChange::Added { id: b, .. },
    ] if a == "orders" && b == "orders2"));
}

#[test]
fn include_change_restarts_only_when_the_table_set_changes() {
    let new = MINIMAL.replace("[public.orders]", "[public.orders, public.order_*]");
    let (old, new) = (load(MINIMAL), load(&new));
    let (restart, fields, _) = modified(&diff(&old, &new, &|_, _| Some(false)));
    assert!(!restart);
    assert_eq!(fields[0].1, Mutability::R);
    assert!(modified(&diff(&old, &new, &|_, _| Some(true))).0);
    assert!(
        modified(&diff(&old, &new, &unknown)).0,
        "unknown counts as changed"
    );
}

/// Every leaf of these configs is covered by a mutability pattern.
#[test]
fn every_resolved_path_has_a_class() {
    let files = Files::new();
    let (spec, spec_env) = spec_example(&files);
    let token = files.write("token", "tok\n");
    let (pem_cert, pem_key) = (cert(), key());
    let rich = MINIMAL
        .replace(
            "globals:\n  state: { bucket: app-state }",
            &format!(
                "globals:\n  cloud: azure\n  state: {{ bucket: app-state, prefix: app }}\n  azure: {{ account_name: lakeacct }}\n  api:\n    auth: basic\n    user: admin\n    password: x\n    tls: {{ enabled: true, cert: {c:?}, key: {k:?} }}\n  cluster:\n    memberlist:\n      join_members: [\"a:7946\"]\n      tls: {{ enabled: true, cert: {c:?}, key: {k:?}, ca: {c:?} }}\n  tracing: {{ otlp_endpoint: \"http://otel:4317\" }}",
                c = pem_cert,
                k = pem_key
            ),
        )
        .replace(
            "postgres: { publication: orders_pub, slot: orders_slot }",
            &format!("ssl_ca: {pem_cert:?}\n      postgres: {{ publication: orders_pub, slot: orders_slot, max_wal_lag_bytes: 1GiB }}"),
        )
        .replace(
            "include: [public.orders]",
            "include: [public.orders]\n      exclude: [public.tmp_*]\n      snapshots: { mode: initial, overrides: { public.orders: always } }\n      column_type_overrides: { public.orders.amount: \"decimal(38,10)\" }\n      primary_key: { public.orders: [id] }\n      partitioning: { public.orders: [ { column: region, transform: identity } ] }",
        )
        .replace(
            "delta: {}",
            "delta: { table_properties: { delta.targetFileSize: \"134217728\" } }\n      max_transaction_batch_rows: 50000\n    state: { bucket: state }",
        );
    let bearer = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        &format!("sink:\n      type: iceberg\n      bucket: lake\n      iceberg:\n        catalog:\n          rest: {{ url: \"https://c\", auth: bearer, token_file: {}, tls: {{ ca: {pem_cert:?} }} }}", token.display()),
    );
    let glue = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        "sink:\n      type: iceberg\n      bucket: lake\n      iceberg:\n        catalog:\n          glue: { name: \"123\" }\n          retry: { max_attempts: 3 }",
    );
    let configs = [
        load_str(&spec, &spec_env).unwrap().config,
        load(&rich),
        load(&bearer),
        load(&glue),
    ];
    for cfg in &configs {
        assert_eq!(unclassified_paths(cfg), Vec::<String>::new());
    }
}
