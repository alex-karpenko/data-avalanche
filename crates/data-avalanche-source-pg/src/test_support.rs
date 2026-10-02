//! Fixtures for the crate's unit tests: a resolved pipeline for the compose `postgres-18`
//! service.

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};

/// A pipeline reading `include` from the `postgres-18` service, with `tables` (YAML, the rest of
/// the `tables` block) added.
pub(crate) fn pipeline_with(include: &str, tables: &str) -> Pipeline {
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
      postgres: {{ publication: p_pub, slot: p_slot }}
    tables:
      include: [{include}]
{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#
    );
    let loaded = load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
    loaded.config.pipelines.into_values().next().unwrap()
}

pub(crate) fn pipeline(include: &str) -> Pipeline {
    pipeline_with(include, "")
}
