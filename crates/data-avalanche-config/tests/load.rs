//! The load pipeline end to end: resolution, defaults, file material, and the phase boundary.

mod support;

use std::time::Duration;

use data_avalanche_config::*;
use support::{Files, MINIMAL, env, fp_key, spec_example};

#[test]
fn spec_example_resolves() {
    let files = Files::new();
    let (yaml, env) = spec_example(&files);
    let loaded = load_str(&yaml, &env, &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
    let cfg = loaded.config;
    assert_eq!(cfg.config_version, 1);
    assert_eq!(cfg.globals.state.bucket, "123456789012-cdc-app-state");
    let api = &cfg.globals.api;
    assert_eq!(api.auth, ApiAuth::Jwt);
    assert_eq!(api.jwt.as_ref().unwrap().roles_claim, "realm_access.roles");
    let tls = api.tls.as_ref().unwrap();
    assert!(matches!(tls.cert.origin, Origin::File(_)));
    assert_eq!(tls.key.origin, Origin::Inline);
    let cluster = &cfg.globals.cluster;
    assert_eq!(cluster.discovery, Discovery::Dns);
    assert_eq!(cluster.memberlist.join_members.len(), 1);
    assert_eq!(cfg.globals.tracing.sample_ratio, 0.05);
    assert_eq!(
        cfg.pipelines.keys().collect::<Vec<_>>(),
        ["events_mysql", "orders_pg"]
    );

    let orders = &cfg.pipelines["orders_pg"];
    assert_eq!(orders.name, "orders-pg");
    assert_eq!(orders.source.port, 5432);
    assert_eq!(orders.source.credentials.password().expose(), "orders-pw");
    assert_eq!(
        orders.source.postgres.as_ref().unwrap().heartbeat,
        Duration::from_secs(60)
    );
    assert_eq!(orders.sink.kind, SinkType::Iceberg);
    assert_eq!(orders.sink.bucket, "123456789012-lake");
    assert_eq!(
        orders.sink.flush,
        Flush {
            seconds: Some(30),
            rows: Some(50_000),
            bytes: Some(512 << 20)
        }
    );
    let rest = orders
        .sink
        .iceberg
        .as_ref()
        .unwrap()
        .catalog
        .rest
        .as_ref()
        .unwrap();
    assert_eq!(rest.auth, RestAuth::Oauth2);
    assert!(matches!(
        rest.oauth2.as_ref().unwrap().client,
        OauthClient::Inline { .. }
    ));
    assert_eq!(
        orders.tables.snapshots.overrides["public.orders"],
        SnapshotMode::Always
    );
    assert_eq!(
        orders.state,
        StateLocation::CoLocated,
        "defaults.state.prefix alone has no effect"
    );

    let events = &cfg.pipelines["events_mysql"];
    assert_eq!(events.source.kind, SourceType::Mysql);
    assert_eq!(events.source.port, 3306);
    assert!(events.source.postgres.is_none());
    assert!(
        matches!(&events.source.credentials, Credentials::File { user, .. } if user == "events")
    );
    assert_eq!(
        events.source.mysql.as_ref().unwrap().replication_type,
        ReplicationType::Gtid
    );
    assert_eq!(events.sink.kind, SinkType::Delta);
    assert_eq!(
        events.sink.prefix, "events_mysql",
        "sink.prefix defaults to the id"
    );
    assert_eq!(
        events.schema_evolution.rescan_interval,
        Duration::from_secs(3600)
    );
}

#[test]
fn applies_spec_defaults() {
    let cfg = load_str(MINIMAL, &env(&[("PW", "pw")]), &fp_key())
        .unwrap()
        .config;
    let g = &cfg.globals;
    assert_eq!(g.cloud, Cloud::Aws);
    assert_eq!(g.api.address.to_string(), "0.0.0.0:8080");
    assert_eq!(g.api.auth, ApiAuth::None);
    assert_eq!(g.log.level.0, "info");
    assert_eq!(g.log.format, LogFormat::Json);
    assert_eq!(g.cluster.tokens, 128);
    assert_eq!(g.cluster.lease_duration, Duration::from_secs(15));
    assert_eq!(g.cluster.lease_renewal_interval, Duration::from_secs(5));
    assert_eq!(g.cluster.lease_safety_margin, 0.15);
    assert_eq!(
        g.cluster.memberlist.bind_addr,
        ["0.0.0.0".parse::<std::net::IpAddr>().unwrap()]
    );
    assert_eq!(
        g.cluster.memberlist.gossip_interval,
        Duration::from_millis(200)
    );
    assert_eq!(
        g.tracing,
        Tracing {
            otlp_endpoint: None,
            sample_ratio: 0.0
        }
    );
    let p = &cfg.pipelines["orders"];
    assert_eq!(p.source.ssl_mode, SslMode::Require);
    assert_eq!(p.source.timeouts.snapshot, Duration::from_secs(6 * 3600));
    assert_eq!(p.source.toast_strategy, ToastStrategy::CarryForward);
    let pg = p.source.postgres.as_ref().unwrap();
    assert!(pg.auto_cleanup);
    assert_eq!(
        (pg.slot_management, pg.slot_on_orphan),
        (SlotManagement::Managed, SlotOnOrphan::Halt)
    );
    assert_eq!(pg.max_wal_lag_bytes, None);
    assert_eq!(p.tables.snapshots.mode, SnapshotMode::Never);
    assert_eq!(p.tables.snapshots.chunk_size, 10_000);
    assert_eq!(p.sink.write_mode, WriteMode::Mirror);
    assert_eq!(
        p.sink.flush,
        Flush {
            seconds: Some(30),
            rows: Some(100_000),
            bytes: Some(512 << 20)
        }
    );
    assert_eq!(p.sink.max_transaction_batch_rows, None);
    assert_eq!(p.sink.max_transaction_buffer_bytes, 1 << 30);
    assert_eq!(p.sink.buffer_capacity_rows, 200_000);
    let delta = p.sink.delta.as_ref().unwrap();
    assert_eq!(delta.checkpoint_interval, 10);
    assert_eq!(delta.log_retention, Duration::from_secs(30 * 86_400));
    assert_eq!(delta.timestamp_ntz_as, TimestampNtzAs::TimestampNtz);
    assert_eq!(delta.catalog_type, DeltaCatalogType::None);
    assert!(p.schema_evolution.enabled);
}

#[test]
fn absent_mysql_and_delta_blocks_take_every_default() {
    // Every field of `source.mysql` and `sink.delta` has a default, so the blocks may be omitted.
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "type: mysql\n      host: db.example.com",
        )
        .replace(
            "      postgres: { publication: orders_pub, slot: orders_slot }\n",
            "",
        )
        .replace("include: [public.orders]", "include: [orders]")
        .replace("      delta: {}\n", "");
    assert!(
        !yaml.contains("mysql:") && !yaml.contains("delta:"),
        "{yaml}"
    );
    let loaded =
        load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
    let p = &loaded.config.pipelines["orders"];
    let mysql = p.source.mysql.as_ref().unwrap();
    assert_eq!(mysql.replication_type, ReplicationType::Gtid);
    assert!(mysql.tinyint1_as_bool);
    let delta = p.sink.delta.as_ref().unwrap();
    assert_eq!(delta.checkpoint_interval, 10);
    assert_eq!(delta.column_mapping, ColumnMapping::None);
    assert_eq!(delta.merge_memory_limit, None, "no ceiling by default");
}

#[test]
fn a_mysql_server_id_and_heartbeat_resolve() {
    let mysql = |extra: &str| {
        MINIMAL
            .replace(
                "host: db.example.com",
                "type: mysql\n      host: db.example.com",
            )
            .replace(
                "      postgres: { publication: orders_pub, slot: orders_slot }\n",
                &format!("      mysql: {{ {extra} }}\n"),
            )
            .replace("include: [public.orders]", "include: [orders]")
    };
    let resolve = |yaml: String| {
        let loaded =
            load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
        loaded.config.pipelines["orders"]
            .source
            .mysql
            .clone()
            .unwrap()
    };
    let derived = resolve(mysql(""));
    assert_eq!(derived.server_id, derived_server_id("orders"));
    assert_eq!(derived.heartbeat, Duration::from_secs(60));
    let set = resolve(mysql("server_id: 42, heartbeat: 5s"));
    assert_eq!(set.server_id, 42);
    assert_eq!(set.heartbeat, Duration::from_secs(5));
}

#[test]
fn a_merge_memory_limit_resolves_to_bytes() {
    let yaml = MINIMAL.replace("delta: {}", "delta: { merge_memory_limit: 256MiB }");
    assert!(yaml.contains("merge_memory_limit"), "{yaml}");
    let loaded =
        load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
    let delta = loaded.config.pipelines["orders"]
        .sink
        .delta
        .clone()
        .unwrap();
    assert_eq!(delta.merge_memory_limit, Some(256 << 20));
}

#[test]
fn structural_pass_stops_before_env_and_files() {
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "host: db.example.com\n      port: abc",
        )
        .replace(
            "user: cdc\n      password: ${PW}",
            "credentials_file: /nonexistent/creds.json",
        );
    let report = load_str(&yaml, &env(&[]), &fp_key()).unwrap_err();
    let rules: Vec<_> = report.errors.iter().map(|d| d.rule.as_str()).collect();
    assert_eq!(rules, ["TYPE"], "{report:#?}");
    assert_eq!(report.errors[0].path, "pipelines.orders.source.port");
    assert_eq!(report.errors[0].loc.unwrap().line, 10);
}

#[test]
fn unset_variable_is_v063_at_its_path() {
    let report = load_str(MINIMAL, &env(&[]), &fp_key()).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-063", "pipelines.orders.source.password")
    );
    assert_eq!(e.message, "environment variable PW is not set");
}

#[test]
fn template_in_a_union_discriminator_resolves_in_phase_2() {
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "type: ${SRC}\n      host: db.example.com",
        )
        .replace(
            "postgres: { publication: orders_pub, slot: orders_slot }",
            "mysql: {}",
        )
        .replace("[public.orders]", "[orders]");
    let cfg = load_str(&yaml, &env(&[("PW", "pw"), ("SRC", "mysql")]), &fp_key())
        .unwrap()
        .config;
    assert_eq!(cfg.pipelines["orders"].source.kind, SourceType::Mysql);
    assert_eq!(cfg.pipelines["orders"].source.port, 3306);
}

#[test]
fn bad_credentials_file_is_v067() {
    let files = Files::new();
    let path = files.write("creds.json", r#"{"user": "cdc"}"#);
    let yaml = MINIMAL.replace(
        "user: cdc\n      password: ${PW}",
        &format!("credentials_file: {}", path.display()),
    );
    let report = load_str(&yaml, &env(&[]), &fp_key()).unwrap_err();
    assert_eq!(report.errors[0].rule, "V-067");
    assert_eq!(
        report.errors[0].path,
        "pipelines.orders.source.credentials_file"
    );
}

#[test]
fn bad_pem_is_v068() {
    let files = Files::new();
    let key = files.write("key.pem", support::key());
    let yaml = MINIMAL.replace(
        "globals:\n  state: { bucket: app-state }",
        &format!(
            "globals:\n  state: {{ bucket: app-state }}\n  api:\n    tls: {{ enabled: true, cert: {k}, key: {k} }}",
            k = key.display()
        ),
    );
    let report = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-068", "globals.api.tls.cert")
    );
    assert!(e.message.contains("no certificate"), "{}", e.message);
}

// FU-B (config §11 Phase 2): a file reference is read only when the setting that uses it is in
// effect; an unused one is never read and cannot fail the load.

#[test]
fn unused_api_credentials_file_is_not_read() {
    let yaml = MINIMAL.replace(
        "globals:\n  state: { bucket: app-state }",
        "globals:\n  state: { bucket: app-state }\n  api:\n    auth: none\n    credentials_file: /nonexistent/x.json",
    );
    let loaded =
        load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
}

#[test]
fn unused_api_tls_material_is_not_read() {
    let yaml = MINIMAL.replace(
        "globals:\n  state: { bucket: app-state }",
        "globals:\n  state: { bucket: app-state }\n  api:\n    tls: { enabled: false, cert: /nonexistent/cert.pem, key: /nonexistent/key.pem }",
    );
    let loaded =
        load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
}

#[test]
fn unused_ssl_ca_is_not_read_when_ssl_mode_is_disable() {
    let yaml = MINIMAL.replace(
        "postgres: { publication: orders_pub, slot: orders_slot }",
        "postgres: { publication: orders_pub, slot: orders_slot }\n      ssl_mode: disable\n      ssl_ca: /nonexistent/ca.pem",
    );
    let loaded =
        load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
}

// G4: "not in effect" only skips the file read (or PEM parse) and the value built from it —
// `${VAR}` substitution still runs on every leaf, so an unset variable is still V-063 even when
// the field it names isn't in effect.

#[test]
fn unused_api_password_still_substitutes_and_reports_v063() {
    let yaml = MINIMAL.replace(
        "globals:\n  state: { bucket: app-state }",
        "globals:\n  state: { bucket: app-state }\n  api:\n    auth: none\n    password: \"${UNSET}\"",
    );
    let report = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-063")
        .unwrap_or_else(|| panic!("no V-063 diagnostic: {report:#?}"));
    assert!(e.path.ends_with("api.password"), "{}", e.path);
}

#[test]
fn unused_ssl_ca_still_substitutes_and_reports_v063_when_ssl_mode_is_disable() {
    let yaml = MINIMAL.replace(
        "postgres: { publication: orders_pub, slot: orders_slot }",
        "postgres: { publication: orders_pub, slot: orders_slot }\n      ssl_mode: disable\n      ssl_ca: \"${UNSET}\"",
    );
    let report = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-063")
        .unwrap_or_else(|| panic!("no V-063 diagnostic: {report:#?}"));
    assert!(e.path.ends_with("source.ssl_ca"), "{}", e.path);
}

// Contrast: the same `credentials_file` under `auth: basic` is in effect, and a bad path fails
// the load as before.
#[test]
fn api_credentials_file_is_read_when_auth_is_basic() {
    let yaml = MINIMAL.replace(
        "globals:\n  state: { bucket: app-state }",
        "globals:\n  state: { bucket: app-state }\n  api:\n    auth: basic\n    credentials_file: /nonexistent/x.json",
    );
    let report = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    assert_eq!(report.errors[0].rule, "V-067");
}

// F1: a diagnostic never prints a file-reference field's resolved value, whether that value came
// from the environment or is literal text that only looks like something else (config §13.9).

#[test]
fn inline_key_with_leading_whitespace_is_accepted() {
    let yaml = MINIMAL.replace(
        "postgres: { publication: orders_pub, slot: orders_slot }",
        "postgres: { publication: orders_pub, slot: orders_slot }\n      ssl_key: \"${KEY}\"",
    );
    let leading = format!("\n{}", support::key());
    let cfg = load_str(&yaml, &env(&[("PW", "pw"), ("KEY", &leading)]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert!(matches!(
        cfg.pipelines["orders"]
            .source
            .ssl_key
            .as_ref()
            .unwrap()
            .origin,
        Origin::Inline
    ));
}

#[test]
fn credentials_file_that_looks_like_json_is_never_shown() {
    let yaml = MINIMAL.replace(
        "user: cdc\n      password: ${PW}",
        r#"credentials_file: '{"user":"u","password":"hunter2"}'"#,
    );
    let report = load_str(&yaml, &env(&[]), &fp_key()).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(e.rule, "V-067");
    assert!(!e.message.contains("hunter2"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("hunter2"));
}

#[test]
fn env_resolved_ssl_key_path_is_never_shown() {
    let yaml = MINIMAL.replace(
        "postgres: { publication: orders_pub, slot: orders_slot }",
        "postgres: { publication: orders_pub, slot: orders_slot }\n      ssl_key: \"${KEY}\"",
    );
    let report = load_str(
        &yaml,
        &env(&[("PW", "pw"), ("KEY", "c2VjcmV0LWtleS1tYXRlcmlhbA==")]),
        &fp_key(),
    )
    .unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-068", "pipelines.orders.source.ssl_key")
    );
    assert!(
        !e.message.contains("c2VjcmV0LWtleS1tYXRlcmlhbA=="),
        "{}",
        e.message
    );
    assert!(e.message.contains("${KEY}"), "{}", e.message);
    assert!(
        !report
            .to_json()
            .to_string()
            .contains("c2VjcmV0LWtleS1tYXRlcmlhbA==")
    );
}

#[test]
fn env_resolved_token_file_path_is_never_shown() {
    let yaml = with_catalog("rest: { url: \"https://c\", auth: bearer, token_file: \"${TOK}\" }");
    let report = load_str(
        &yaml,
        &env(&[("PW", "pw"), ("TOK", "tok-abc123-secret")]),
        &fp_key(),
    )
    .unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        (
            "V-071",
            "pipelines.orders.sink.iceberg.catalog.rest.token_file"
        )
    );
    assert!(!e.message.contains("tok-abc123-secret"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("tok-abc123-secret"));
}

// F2: a value obtained by `${VAR}` substitution is never printed in a validation message either,
// whether the coercion failure comes from the generic value path (TYPE, a config enum) or from a
// rule that builds its own message from the resolved text (V-055).

#[test]
fn env_resolved_port_hides_the_value_in_a_type_error() {
    let yaml = MINIMAL.replace(
        "host: db.example.com",
        "host: db.example.com\n      port: \"${SECRET}\"",
    );
    let report = load_str(
        &yaml,
        &env(&[("PW", "pw"), ("SECRET", "s3cr3t-value-123")]),
        &fp_key(),
    )
    .unwrap_err();
    let e = &report.errors[0];
    assert_eq!(e.rule, "TYPE");
    assert!(!e.message.contains("s3cr3t-value-123"), "{}", e.message);
    assert!(e.message.contains("${SECRET}"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("s3cr3t-value-123"));
}

#[test]
fn env_resolved_pipeline_id_hides_the_value_in_v055() {
    // Unlike the crate's usual `s3cr3t-value-123`, this needs an uppercase letter: that value
    // happens to be a syntactically valid pipeline id (lower-case letters, digits, `_`/`-`), so it
    // would not trigger V-055's format check at all.
    let yaml = MINIMAL.replace("id: orders", "id: \"${SECRET}\"");
    let report = load_str(
        &yaml,
        &env(&[("PW", "pw"), ("SECRET", "S3CR3T-VALUE-123")]),
        &fp_key(),
    )
    .unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-055")
        .unwrap_or_else(|| panic!("no V-055 diagnostic: {report:#?}"));
    assert!(!e.message.contains("S3CR3T-VALUE-123"), "{}", e.message);
    assert!(e.message.contains("${SECRET}"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("S3CR3T-VALUE-123"));
}

#[test]
fn env_resolved_enum_hides_the_value_in_its_rule() {
    let yaml = MINIMAL.replace(
        "include: [public.orders]",
        "include: [public.orders]\n      snapshots: { mode: \"${SECRET}\" }",
    );
    let report = load_str(
        &yaml,
        &env(&[("PW", "pw"), ("SECRET", "s3cr3t-value-123")]),
        &fp_key(),
    )
    .unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-022")
        .unwrap_or_else(|| panic!("no V-022 diagnostic: {report:#?}"));
    assert!(!e.message.contains("s3cr3t-value-123"), "{}", e.message);
    assert!(e.message.contains("${SECRET}"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("s3cr3t-value-123"));
}

// F3b: a rule that coerces a value successfully and then fails its own range/consistency check
// builds its message from the *parsed* value, which bypasses `value_with`'s F2 rewrite. `shown_value`
// (next to `Ctx::shown`) closes that: the message names the templated leaf's own `${VAR}` text
// instead of the value that came out of it.

#[test]
fn env_resolved_range_check_hides_the_value_in_v026() {
    let yaml = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  cluster: { tokens: \"${SECRET}\" }",
    );
    let report =
        load_str(&yaml, &env(&[("PW", "pw"), ("SECRET", "99999")]), &fp_key()).unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-026")
        .unwrap_or_else(|| panic!("no V-026 diagnostic: {report:#?}"));
    assert!(!e.message.contains("99999"), "{}", e.message);
    assert!(e.message.contains("${SECRET}"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("99999"));
}

#[test]
fn env_resolved_at_least_check_hides_the_value_in_v074() {
    let yaml = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  cluster: { max_concurrent_handoffs: \"${SECRET}\" }",
    );
    let report = load_str(&yaml, &env(&[("PW", "pw"), ("SECRET", "0")]), &fp_key()).unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-074")
        .unwrap_or_else(|| panic!("no V-074 diagnostic: {report:#?}"));
    assert!(!e.message.contains("got 0"), "{}", e.message);
    assert!(!e.message.contains(" 0"), "{}", e.message);
    assert!(e.message.contains("${SECRET}"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("\"0\""));
}

#[test]
fn env_resolved_lease_renewal_interval_hides_the_value_in_v073() {
    let yaml = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  cluster: { lease_renewal_interval: \"${SECRET}\" }",
    );
    // Default lease_duration is 15s, so anything above 5s (15s / 3) trips V-073.
    let report = load_str(&yaml, &env(&[("PW", "pw"), ("SECRET", "20s")]), &fp_key()).unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-073")
        .unwrap_or_else(|| panic!("no V-073 diagnostic: {report:#?}"));
    assert!(!e.message.contains("20s"), "{}", e.message);
    assert!(e.message.contains("${SECRET}"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("20s"));

    // G6: a templated `lease_duration` must not leak through the computed bound either — printing
    // `lease_duration / 3` as a duration would let dividing by 3 recover the env value.
    let yaml = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  cluster: { lease_duration: \"${DUR}\" }",
    );
    // Default lease_renewal_interval is 5s; lease_duration 9s / 3 = 3s, so 5s trips V-073.
    let report = load_str(&yaml, &env(&[("PW", "pw"), ("DUR", "9s")]), &fp_key()).unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-073")
        .unwrap_or_else(|| panic!("no V-073 diagnostic: {report:#?}"));
    assert!(!e.message.contains("9s"), "{}", e.message);
    assert!(!e.message.contains("3s"), "{}", e.message);
    assert!(e.message.contains("lease_duration / 3"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("9s"));
}

#[test]
fn env_resolved_duplicate_slot_hides_the_value_in_v058() {
    let yaml = r#"
config_version: 1
globals:
  state: { bucket: app-state }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders_pub, slot: "${S}" }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
  orders2:
    id: orders2
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders2_pub, slot: "${S}" }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
"#;
    let report = load_str(yaml, &env(&[("PW", "pw"), ("S", "dup_slot_x")]), &fp_key()).unwrap_err();
    let hits: Vec<_> = report.errors.iter().filter(|d| d.rule == "V-058").collect();
    assert_eq!(hits.len(), 2, "{report:#?}");
    for e in &hits {
        assert!(!e.message.contains("dup_slot_x"), "{}", e.message);
        assert!(e.message.contains("${S}"), "{}", e.message);
    }
    assert!(!report.to_json().to_string().contains("dup_slot_x"));
}

// F3: `null` on `tables.include`/`exclude` clears an inherited list instead of falling back to it
// (end to end; the merge-level behavior is covered in `tests/merge.rs`).

#[test]
fn explicit_null_exclude_resolves_to_an_empty_list() {
    let yaml = MINIMAL
        .replace(
            "pipelines:",
            "defaults:\n  tables: { exclude: [public.tmp_x] }\npipelines:",
        )
        .replace(
            "include: [public.orders]",
            "include: [public.orders]\n      exclude: null",
        );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert!(cfg.pipelines["orders"].tables.exclude.is_empty());
}

#[test]
fn explicit_null_include_fails_v007() {
    let yaml = MINIMAL
        .replace(
            "pipelines:",
            "defaults:\n  tables: { include: [public.orders] }\npipelines:",
        )
        .replace("include: [public.orders]\n", "include: null\n");
    let report = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    assert!(
        report.errors.iter().any(|d| d.rule == "V-007"),
        "{report:#?}"
    );
}

const ICEBERG_SINK: &str = "sink:
      type: iceberg
      bucket: lake
      iceberg:
        catalog:
          ";

fn with_catalog(catalog: &str) -> String {
    MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        &format!("{ICEBERG_SINK}{catalog}"),
    )
}

#[test]
fn empty_token_file_is_v071() {
    let files = Files::new();
    let token = files.write("token", "\n");
    let yaml = with_catalog(&format!(
        "rest: {{ url: \"https://c\", auth: bearer, token_file: {} }}",
        token.display()
    ));
    let report = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        (
            "V-071",
            "pipelines.orders.sink.iceberg.catalog.rest.token_file"
        )
    );
}

#[test]
fn explicit_state_location_takes_the_defaults_prefix_as_parent() {
    let yaml = MINIMAL.replace(
        "pipelines:",
        "defaults:\n  state: { bucket: state-bucket, prefix: parent }\npipelines:",
    );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap()
        .config;
    assert_eq!(
        cfg.pipelines["orders"].state,
        StateLocation::Explicit {
            bucket: "state-bucket".into(),
            parent_prefix: Some("parent".into()),
            prefix: "orders".into(),
        }
    );
}

#[test]
fn secrets_never_print() {
    let cfg = load_str(MINIMAL, &env(&[("PW", "s3cr3t-pw")]), &fp_key())
        .unwrap()
        .config;
    assert!(!format!("{cfg:?}").contains("s3cr3t-pw"));
    assert!(!serde_json::to_string(&cfg).unwrap().contains("s3cr3t-pw"));
}

#[test]
fn unreadable_config_file_is_a_parse_error() {
    let report = load_file(
        std::path::Path::new("/nonexistent/data-avalanche.yaml"),
        &env(&[]),
        &fp_key(),
    )
    .unwrap_err();
    assert_eq!(report.errors[0].rule, "PARSE");
    assert!(
        report.errors[0]
            .message
            .starts_with("cannot read config file")
    );
}

// FU-C (config §11 Phase 2): an optional value that resolves to empty is absent, so its default
// applies — for a tri-state field, the default, not `null`.

#[test]
fn empty_optional_otlp_endpoint_is_absent() {
    let yaml = MINIMAL.replace(
        "globals:\n  state: { bucket: app-state }",
        "globals:\n  state: { bucket: app-state }\n  tracing: { otlp_endpoint: \"${OTLP:-}\" }",
    );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert_eq!(
        cfg.globals.tracing,
        Tracing {
            otlp_endpoint: None,
            sample_ratio: 0.0,
        },
        "an otlp_endpoint that resolves empty is absent, exactly as if it were never written"
    );
}

#[test]
fn empty_optional_ssl_mode_falls_back_through_defaults() {
    let yaml = MINIMAL
        .replace(
            "pipelines:",
            "defaults:\n  source: { ssl_mode: disable }\npipelines:",
        )
        .replace(
            "postgres: { publication: orders_pub, slot: orders_slot }",
            "postgres: { publication: orders_pub, slot: orders_slot }\n      ssl_mode: \"${M:-}\"",
        );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert_eq!(
        cfg.pipelines["orders"].source.ssl_mode,
        SslMode::Disable,
        "the pipeline's own ssl_mode resolves empty, so it's absent and defaults.source.ssl_mode applies"
    );
}

#[test]
fn empty_optional_tri_state_flush_seconds_falls_back_to_the_default_not_disabled() {
    let yaml = MINIMAL.replace(
        "delta: {}",
        "delta: {}\n      flush: { seconds: \"${S:-}\" }",
    );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert_eq!(
        cfg.pipelines["orders"].sink.flush.seconds,
        Some(30),
        "an empty tri-state value is absent (apply the default), not null (disabled)"
    );
}

// §11 Phase 2 exception: a `prefix` field that resolves to `""` keeps the empty prefix — the
// bucket root (§10) — rather than being treated as absent, unlike every other optional leaf above.

#[test]
fn absent_sink_prefix_is_still_the_pipeline_id() {
    let cfg = load_str(MINIMAL, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert_eq!(
        cfg.pipelines["orders"].sink.prefix, "orders",
        "an absent sink.prefix still defaults to the pipeline id"
    );
}

#[test]
fn explicit_empty_sink_prefix_is_the_bucket_root_not_the_pipeline_id() {
    let yaml = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        "sink:\n      type: delta\n      bucket: lake\n      prefix: \"\"\n      delta: {}",
    );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert_eq!(
        cfg.pipelines["orders"].sink.prefix, "",
        "a sink.prefix that resolves to \"\" keeps the empty prefix (the bucket root), not the pipeline id"
    );
}

#[test]
fn explicit_empty_sink_prefix_overrides_a_defaults_prefix() {
    let yaml = MINIMAL
        .replace(
            "pipelines:",
            "defaults:\n  sink: { prefix: shared }\npipelines:",
        )
        .replace(
            "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
            "sink:\n      type: delta\n      bucket: lake\n      prefix: \"\"\n      delta: {}",
        );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert_eq!(
        cfg.pipelines["orders"].sink.prefix, "",
        "the pipeline's own empty prefix wins over defaults.sink.prefix, same as any other explicit value"
    );
}

#[test]
fn sink_prefix_from_an_unset_env_var_default_is_the_bucket_root() {
    let yaml = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        "sink:\n      type: delta\n      bucket: lake\n      prefix: \"${P:-}\"\n      delta: {}",
    );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config;
    assert_eq!(
        cfg.pipelines["orders"].sink.prefix, "",
        "a prefix templated to an unset variable with an empty default resolves to \"\", still the bucket root"
    );
}

// FU-C (config §8.6): `defaults` is validated on its own, at its own path, before any pipeline
// merges it in — and reported once even when several pipelines would otherwise duplicate it.

#[test]
fn invalid_defaults_value_rejects_the_config_even_with_no_pipelines() {
    let yaml = r#"
config_version: 1
globals:
  state: { bucket: app-state }
defaults:
  sink: { flush: { rows: abc } }
pipelines: {}
"#;
    let report = load_str(yaml, &env(&[]), &fp_key()).unwrap_err();
    assert_eq!(report.errors.len(), 1, "{report:#?}");
    let e = &report.errors[0];
    assert_eq!(e.rule, "TYPE");
    assert_eq!(e.path, "defaults.sink.flush.rows");
    assert_eq!(e.locus, Locus::Config);
}

#[test]
fn invalid_defaults_value_is_reported_once_however_many_pipelines_inherit_it() {
    let yaml = r#"
config_version: 1
globals:
  state: { bucket: app-state }
defaults:
  sink: { flush: { rows: abc } }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
  clicks:
    id: clicks
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: clicks_pub, slot: clicks_slot }
    tables:
      include: [public.clicks]
    sink:
      type: delta
      bucket: lake
      delta: {}
"#;
    let report = load_str(yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    let hits: Vec<_> = report
        .errors
        .iter()
        .filter(|d| d.rule == "TYPE" && d.path.contains("flush.rows"))
        .collect();
    assert_eq!(hits.len(), 1, "{report:#?}");
    assert_eq!(hits[0].path, "defaults.sink.flush.rows");
    assert_eq!(hits[0].locus, Locus::Config);
}

#[test]
fn invalid_enum_in_defaults_is_v020_at_its_defaults_path() {
    let yaml = MINIMAL.replace(
        "pipelines:",
        "defaults:\n  source: { ssl_mode: nope }\npipelines:",
    );
    let report = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    let hits: Vec<_> = report.errors.iter().filter(|d| d.rule == "V-020").collect();
    assert_eq!(hits.len(), 1, "{report:#?}");
    assert_eq!(hits[0].path, "defaults.source.ssl_mode");
    assert_eq!(hits[0].locus, Locus::Config);
}

// Fix round 1, item 4: a literal (non-templated) invalid `defaults` value is reported exactly
// once, however many pipelines inherit it. Literal text coerces the same way regardless of the
// environment, so it's already caught in the structural pass (Phase 1) — `dedupe_defaults` runs
// there too, before the early return, not only on the resolving-pass report.
#[test]
fn literal_invalid_defaults_value_is_reported_once_with_two_pipelines() {
    let yaml = r#"
config_version: 1
globals:
  state: { bucket: app-state }
defaults:
  source: { ssl_mode: nope }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
  clicks:
    id: clicks
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: clicks_pub, slot: clicks_slot }
    tables:
      include: [public.clicks]
    sink:
      type: delta
      bucket: lake
      delta: {}
"#;
    let report = load_str(yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_err();
    let hits: Vec<_> = report.errors.iter().filter(|d| d.rule == "V-020").collect();
    assert_eq!(hits.len(), 1, "{report:#?}");
    assert_eq!(hits[0].path, "defaults.source.ssl_mode");
    assert_eq!(hits[0].locus, Locus::Config);
}

// G8: a *templated* invalid `defaults` value is unknown in Phase 1 (structural) and only fails to
// coerce once Phase 2 resolves it, so this pins `dedupe_defaults` on the resolving-pass report
// specifically (`load_str`'s second call) — the literal case above only ever exercises the
// structural-pass one. G3 also applies: the invalid resolved value must not leak into a message.
#[test]
fn templated_invalid_defaults_value_is_reported_once_and_hides_the_value() {
    let yaml = r#"
config_version: 1
globals:
  state: { bucket: app-state }
defaults:
  source: { ssl_mode: "${MODE}" }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
  clicks:
    id: clicks
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: clicks_pub, slot: clicks_slot }
    tables:
      include: [public.clicks]
    sink:
      type: delta
      bucket: lake
      delta: {}
"#;
    let report = load_str(yaml, &env(&[("PW", "pw"), ("MODE", "nope")]), &fp_key()).unwrap_err();
    let hits: Vec<_> = report.errors.iter().filter(|d| d.rule == "V-020").collect();
    assert_eq!(hits.len(), 1, "{report:#?}");
    assert_eq!(hits[0].path, "defaults.source.ssl_mode");
    assert_eq!(hits[0].locus, Locus::Config);
    assert!(!hits[0].message.contains("nope"), "{}", hits[0].message);
    assert!(!report.to_json().to_string().contains("nope"));
}
