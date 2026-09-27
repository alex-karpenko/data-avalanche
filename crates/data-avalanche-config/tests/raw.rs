//! The raw model accepts the spec's complete example (§14) and rejects an unknown key in every block.

use data_avalanche_config::{PARSE, RawConfig, parse_document};

const SPEC_EXAMPLE: &str = include_str!("fixtures/spec_example.yaml");

#[test]
fn parses_the_spec_example() {
    let raw: RawConfig = parse_document(SPEC_EXAMPLE).unwrap();
    assert_eq!(raw.config_version.unwrap().text(), "1");
    let globals = raw.globals.unwrap();
    let cluster = globals.cluster.unwrap();
    assert_eq!(cluster.lease_duration.unwrap().text(), "15s");
    let tls = cluster.memberlist.unwrap().tls.unwrap();
    assert_eq!(tls.key.unwrap().text(), "${GOSSIP_TLS_KEY}");
    let defaults = raw.defaults.unwrap();
    let rest = defaults
        .sink
        .unwrap()
        .iceberg
        .unwrap()
        .catalog
        .unwrap()
        .rest
        .unwrap();
    assert_eq!(
        rest.oauth2.unwrap().scope.unwrap().text(),
        "catalog:read catalog:write"
    );
    let pipelines = raw.pipelines.unwrap();
    assert_eq!(
        pipelines.keys().collect::<Vec<_>>(),
        ["events-mysql", "orders-pg"]
    );
    let orders = &pipelines["orders-pg"];
    let flush = orders.sink.as_ref().unwrap().flush.as_ref().unwrap();
    assert!(
        flush.seconds.is_none(),
        "seconds is inherited, not disabled"
    );
    assert_eq!(
        flush.rows.as_ref().unwrap().as_ref().unwrap().text(),
        "50000"
    );
    let tables = orders.tables.as_ref().unwrap();
    let part = &tables.partitioning.as_ref().unwrap()["public.orders"][0];
    assert_eq!(part.transform.as_ref().unwrap().text(), "day");
}

/// One document per raw struct, each with an unknown key `bogus` inside that struct. A struct
/// that forgot `deny_unknown_fields` makes its case parse and fails the test.
const UNKNOWN_KEY_CASES: &[&str] = &[
    "bogus: 1",
    "globals: {bogus: 1}",
    "globals: {state: {bogus: 1}}",
    "globals: {api: {bogus: 1}}",
    "globals: {api: {jwt: {bogus: 1}}}",
    "globals: {api: {oauth2: {bogus: 1}}}",
    "globals: {api: {tls: {bogus: 1}}}",
    "globals: {log: {bogus: 1}}",
    "globals: {cluster: {bogus: 1}}",
    "globals: {cluster: {memberlist: {bogus: 1}}}",
    "globals: {cluster: {memberlist: {tls: {bogus: 1}}}}",
    "globals: {tracing: {bogus: 1}}",
    "globals: {azure: {bogus: 1}}",
    "defaults: {bogus: 1}",
    "pipelines: {p: {bogus: 1}}",
    "pipelines: {p: {source: {bogus: 1}}}",
    "pipelines: {p: {source: {timeouts: {bogus: 1}}}}",
    "pipelines: {p: {source: {postgres: {bogus: 1}}}}",
    "pipelines: {p: {source: {mysql: {bogus: 1}}}}",
    "pipelines: {p: {tables: {bogus: 1}}}",
    "pipelines: {p: {tables: {snapshots: {bogus: 1}}}}",
    "pipelines: {p: {tables: {partitioning: {t: [{bogus: 1}]}}}}",
    "pipelines: {p: {sink: {bogus: 1}}}",
    "pipelines: {p: {sink: {flush: {bogus: 1}}}}",
    "pipelines: {p: {sink: {metrics: {bogus: 1}}}}",
    "pipelines: {p: {sink: {iceberg: {bogus: 1}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {bogus: 1}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {glue: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {hive: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {hive: {kerberos: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {hive: {tls: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {oauth2: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {sigv4: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {tls: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {retry: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {delta: {bogus: 1}}}}",
    "pipelines: {p: {sink: {delta: {catalog: {bogus: 1}}}}}",
    "pipelines: {p: {state: {bogus: 1}}}",
    "pipelines: {p: {schema_evolution: {bogus: 1}}}",
];

#[test]
fn rejects_an_unknown_key_in_every_block() {
    for case in UNKNOWN_KEY_CASES {
        let err = parse_document::<RawConfig>(case).expect_err(case);
        assert_eq!(err.rule, PARSE, "{case}");
        assert!(
            err.message.contains("unknown field `bogus`"),
            "{case}: {}",
            err.message
        );
    }
}

#[test]
fn old_draft_spellings_are_unknown_keys() {
    // §15: the draft's kebab-case `ssl-mode` and the pre-multi-sink pipeline-level `catalog`.
    for case in [
        "pipelines: {p: {source: {ssl-mode: require}}}",
        "pipelines: {p: {catalog: {glue: {name: x}}}}",
        "pipelines: {p: {sink: {strategy: mor}}}",
    ] {
        let err = parse_document::<RawConfig>(case).expect_err(case);
        assert!(
            err.message.contains("unknown field"),
            "{case}: {}",
            err.message
        );
    }
}
