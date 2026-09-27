//! The load pipeline end to end: resolution, defaults, file material, and the phase boundary.

mod support;

use std::time::Duration;

use data_avalanche_config::enums::*;
use data_avalanche_config::resolved::*;
use data_avalanche_config::{Origin, load_file, load_str};
use support::{Files, MINIMAL, env, spec_example};

#[test]
fn spec_example_resolves() {
    let files = Files::new();
    let (yaml, env) = spec_example(&files);
    let loaded = load_str(&yaml, &env).unwrap_or_else(|r| panic!("{r:#?}"));
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
    assert!(cluster.memberlist.tls.as_ref().unwrap().ca.is_some());
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
    let cfg = load_str(MINIMAL, &env(&[("PW", "pw")])).unwrap().config;
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
    let loaded = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_or_else(|r| panic!("{r:#?}"));
    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
    let p = &loaded.config.pipelines["orders"];
    let mysql = p.source.mysql.as_ref().unwrap();
    assert_eq!(mysql.replication_type, ReplicationType::Gtid);
    assert!(mysql.tinyint1_as_bool);
    let delta = p.sink.delta.as_ref().unwrap();
    assert_eq!(delta.checkpoint_interval, 10);
    assert_eq!(delta.column_mapping, ColumnMapping::None);
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
    let report = load_str(&yaml, &env(&[])).unwrap_err();
    let rules: Vec<_> = report.errors.iter().map(|d| d.rule.as_str()).collect();
    assert_eq!(rules, ["TYPE"], "{report:#?}");
    assert_eq!(report.errors[0].path, "pipelines.orders.source.port");
    assert_eq!(report.errors[0].loc.unwrap().line, 10);
}

#[test]
fn unset_variable_is_v063_at_its_path() {
    let report = load_str(MINIMAL, &env(&[])).unwrap_err();
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
    let cfg = load_str(&yaml, &env(&[("PW", "pw"), ("SRC", "mysql")]))
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
    let report = load_str(&yaml, &env(&[])).unwrap_err();
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
    let report = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-068", "globals.api.tls.cert")
    );
    assert!(e.message.contains("no certificate"), "{}", e.message);
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
    let cfg = load_str(&yaml, &env(&[("PW", "pw"), ("KEY", &leading)]))
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
    let report = load_str(&yaml, &env(&[])).unwrap_err();
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
    let report = load_str(&yaml, &env(&[("PW", "pw"), ("TOK", "tok-abc123-secret")])).unwrap_err();
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
    let report =
        load_str(&yaml, &env(&[("PW", "pw"), ("SECRET", "s3cr3t-value-123")])).unwrap_err();
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
    let report =
        load_str(&yaml, &env(&[("PW", "pw"), ("SECRET", "S3CR3T-VALUE-123")])).unwrap_err();
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
    let report =
        load_str(&yaml, &env(&[("PW", "pw"), ("SECRET", "s3cr3t-value-123")])).unwrap_err();
    let e = report
        .errors
        .iter()
        .find(|d| d.rule == "V-022")
        .unwrap_or_else(|| panic!("no V-022 diagnostic: {report:#?}"));
    assert!(!e.message.contains("s3cr3t-value-123"), "{}", e.message);
    assert!(e.message.contains("${SECRET}"), "{}", e.message);
    assert!(!report.to_json().to_string().contains("s3cr3t-value-123"));
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
    let report = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_err();
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
    let cfg = load_str(&yaml, &env(&[("PW", "pw")])).unwrap().config;
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
    let cfg = load_str(MINIMAL, &env(&[("PW", "s3cr3t-pw")]))
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
    )
    .unwrap_err();
    assert_eq!(report.errors[0].rule, "PARSE");
    assert!(
        report.errors[0]
            .message
            .starts_with("cannot read config file")
    );
}
