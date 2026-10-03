//! Fixtures for the crate's unit tests.

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};

/// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
/// indented six spaces), more `sink` and `postgres` keys (YAML flow entries).
pub(crate) fn pipeline(tables: &str, sink: &str, postgres: &str) -> Pipeline {
    let sep = |s: &str| {
        if s.is_empty() {
            String::new()
        } else {
            format!(", {s}")
        }
    };
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      host: 127.0.0.1
      port: 55418
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: p_pub, slot: p_slot{postgres} }}
    tables:
      include: [public.*]{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}}{sink} }}
"#,
        postgres = sep(postgres),
        sink = sep(sink),
    );
    load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
    .config
    .pipelines
    .into_values()
    .next()
    .expect("one pipeline")
}
