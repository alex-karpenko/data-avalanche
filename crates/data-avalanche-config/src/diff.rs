//! The reload diff (config §12): the new resolved config against the running one, per global field
//! and per pipeline (keyed by `id`). Computed on resolved values (§11.1), so a changed
//! `credentials_file` or certificate is detected even when the config text is unchanged. The output
//! names fields and classes only, never values.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::diag::{Diagnostic, Locus, Report, Severity};
use crate::mutability::{Mutability, classify};
use crate::path::YamlPath;
use crate::resolved::{Pipeline, ResolvedConfig};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldChange {
    /// The changed path (`globals.log.level`, `pipelines.orders.sink.flush.rows`), in the
    /// resolved model's field names; a pipeline is keyed by its `id`, not its human name
    /// (`pipelines.<id>.…`).
    pub path: String,
    /// `R`, `Pr` or `Proc`, after the table-set check.
    pub class: Mutability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum PipelineChange {
    /// A new `id` (§12.3): start it.
    Added { id: String, name: String },
    /// An `id` gone (§12.3): stop, drain, clean up per `auto_cleanup`. An `id` change is a
    /// removal plus an addition.
    Removed { id: String, name: String },
    /// Same `id`, different fields. `restart` = some field is PR (§12.4); otherwise every change
    /// applies in place. `renamed_from` = the map key changed (a pure rename, §12.3).
    Modified {
        id: String,
        name: String,
        renamed_from: Option<String>,
        restart: bool,
        fields: Vec<FieldChange>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ConfigDiff {
    pub globals: Vec<FieldChange>,
    pub pipelines: Vec<PipelineChange>,
    /// V-061 errors: a PROC field changed. Any error rejects the reload (§11).
    pub report: Report,
}

impl ConfigDiff {
    pub fn is_empty(&self) -> bool {
        self.globals.is_empty() && self.pipelines.is_empty()
    }

    /// True when the reload must be rejected and the running config kept.
    pub fn is_rejected(&self) -> bool {
        !self.report.is_ok()
    }
}

/// Flattens a serialized value into `path → leaf`. Objects (structs and maps) recurse; arrays are
/// leaves, since a list changes as a whole (§8.3).
fn flatten(prefix: &[String], v: &Value, out: &mut BTreeMap<Vec<String>, Value>) {
    match v {
        Value::Object(map) => {
            for (k, v) in map {
                let mut path = prefix.to_vec();
                path.push(k.clone());
                flatten(&path, v, out);
            }
        }
        leaf => {
            out.insert(prefix.to_vec(), leaf.clone());
        }
    }
}

fn flat<T: Serialize>(prefix: &[&str], v: &T) -> BTreeMap<Vec<String>, Value> {
    let mut out = BTreeMap::new();
    let value = serde_json::to_value(v).expect("the resolved config serializes");
    flatten(
        &prefix.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        &value,
        &mut out,
    );
    out
}

/// Paths whose value differs, appears or disappears.
fn changed_paths(
    old: &BTreeMap<Vec<String>, Value>,
    new: &BTreeMap<Vec<String>, Value>,
) -> Vec<Vec<String>> {
    let mut paths: Vec<Vec<String>> = old
        .iter()
        .filter(|(k, v)| new.get(*k) != Some(v))
        .map(|(k, _)| k.clone())
        .chain(new.keys().filter(|k| !old.contains_key(*k)).cloned())
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

/// The class of a changed path; an uncovered path counts as `Pr` (a restart is always safe).
fn class_of(path: &[String]) -> Mutability {
    let segments: Vec<&str> = path.iter().map(String::as_str).collect();
    classify(&segments).unwrap_or(Mutability::Pr)
}

/// A flattened path in the crate's dotted form (`YamlPath`), so a key such as `public.orders`
/// prints as `["public.orders"]` rather than as two segments.
fn dotted(path: &[String]) -> String {
    path.iter()
        .fold(YamlPath::root(), |p, k| p.key(k.as_str()))
        .to_string()
}

/// Diffs `new` against `old`. `table_set_changed(old, new)` answers whether a pipeline's resolved
/// table set changed (it needs the live source); `None` means "unknown", which counts as changed.
/// This call is synchronous, so a caller whose table-set resolution is async precomputes the
/// answer for each pipeline whose `include`/`exclude` changed and has this closure look it up.
/// `old` and `new` must have been loaded with the same [`crate::FingerprintKey`] (see the crate
/// docs): a key mismatch makes every secret and file fingerprint differ, so it reports every
/// secret field as changed rather than hiding a real rotation.
pub fn diff(
    old: &ResolvedConfig,
    new: &ResolvedConfig,
    table_set_changed: &dyn Fn(&Pipeline, &Pipeline) -> Option<bool>,
) -> ConfigDiff {
    let mut out = ConfigDiff::default();
    let mut old_g = flat(&["globals"], &old.globals);
    let mut new_g = flat(&["globals"], &new.globals);
    old_g.insert(vec!["config_version".into()], old.config_version.into());
    new_g.insert(vec!["config_version".into()], new.config_version.into());
    for path in changed_paths(&old_g, &new_g) {
        let class = class_of(&path);
        let text = dotted(&path);
        if class == Mutability::Proc {
            out.report.push(Diagnostic {
                locus: Locus::Reload,
                path: text.clone(),
                rule: "V-061".into(),
                severity: Severity::Error,
                loc: None,
                message: format!(
                    "{text} changed: it needs a process restart, so the reload is rejected"
                ),
            });
        }
        out.globals.push(FieldChange { path: text, class });
    }

    for (id, o) in &old.pipelines {
        if !new.pipelines.contains_key(id) {
            out.pipelines.push(PipelineChange::Removed {
                id: id.clone(),
                name: o.name.clone(),
            });
        }
    }
    for (id, n) in &new.pipelines {
        let Some(o) = old.pipelines.get(id) else {
            out.pipelines.push(PipelineChange::Added {
                id: id.clone(),
                name: n.name.clone(),
            });
            continue;
        };
        let paths = changed_paths(&flat(&["pipelines", id], o), &flat(&["pipelines", id], n));
        if paths.is_empty() {
            continue;
        }
        let mut table_set: Option<bool> = None;
        let fields: Vec<FieldChange> = paths
            .iter()
            .map(|path| {
                let class = match class_of(path) {
                    Mutability::PrIfTableSetChanges => {
                        let changed = *table_set
                            .get_or_insert_with(|| table_set_changed(o, n).unwrap_or(true));
                        if changed {
                            Mutability::Pr
                        } else {
                            Mutability::R
                        }
                    }
                    c => c,
                };
                FieldChange {
                    path: dotted(path),
                    class,
                }
            })
            .collect();
        let restart = fields.iter().any(|f| f.class >= Mutability::Pr);
        out.pipelines.push(PipelineChange::Modified {
            id: id.clone(),
            name: n.name.clone(),
            renamed_from: (o.name != n.name).then(|| o.name.clone()),
            restart,
            fields,
        });
    }
    out
}

/// Every leaf path of `cfg` that no mutability pattern covers (for tests: must be empty). Only
/// used by this module's own unit test.
#[cfg(test)]
fn unclassified_paths(cfg: &ResolvedConfig) -> Vec<String> {
    let mut all = flat(&["globals"], &cfg.globals);
    for (id, p) in &cfg.pipelines {
        all.extend(flat(&["pipelines", id], p));
    }
    all.keys()
        .filter(|p| classify(&p.iter().map(String::as_str).collect::<Vec<_>>()).is_none())
        .map(|p| dotted(p))
        .collect()
}

#[cfg(test)]
mod tests {
    //! Every leaf of a rich resolved config is covered by a mutability pattern. Moved from
    //! `tests/diff.rs` (FU-E): `unclassified_paths` is crate-internal. The fixtures below are a
    //! minimal duplicate of `tests/support/mod.rs` (FU-E: "a shared fixture it needs can be
    //! duplicated minimally into a `#[cfg(test)]` helper module in `src/`") — only what this one
    //! test needs.

    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::OnceLock;

    use super::*;
    use crate::load::load_str;
    use crate::secret::FingerprintKey;

    const SPEC_EXAMPLE: &str = include_str!("../tests/fixtures/spec_example.yaml");

    fn fp_key() -> FingerprintKey {
        FingerprintKey::from_bytes([0x42; 32])
    }

    /// A minimal valid config: one PostgreSQL → Delta pipeline. Needs `PW` in the environment.
    const MINIMAL: &str = r#"
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
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
"#;

    fn pem_pair() -> &'static (String, String) {
        static PAIR: OnceLock<(String, String)> = OnceLock::new();
        PAIR.get_or_init(|| {
            let rcgen::CertifiedKey { cert, signing_key } =
                rcgen::generate_simple_self_signed(vec!["data-avalanche-test".to_string()])
                    .unwrap();
            (cert.pem(), signing_key.serialize_pem())
        })
    }

    fn cert() -> &'static str {
        &pem_pair().0
    }

    fn key() -> &'static str {
        &pem_pair().1
    }

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    struct Files {
        dir: tempfile::TempDir,
    }

    impl Files {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().unwrap(),
            }
        }

        fn dir(&self) -> String {
            self.dir.path().display().to_string()
        }

        fn write(&self, name: &str, content: &str) -> PathBuf {
            let path = self.dir.path().join(name);
            std::fs::write(&path, content).unwrap();
            path
        }
    }

    fn spec_example(files: &Files) -> (String, BTreeMap<String, String>) {
        files.write("api.crt", cert());
        files.write("gossip.crt", cert());
        files.write("gossip-ca.crt", cert());
        let creds = files.write(
            "events-db.json",
            r#"{"user": "events", "password": "events-pw"}"#,
        );
        let yaml = SPEC_EXAMPLE.replace("/etc/cdc/tls/", &format!("{}/", files.dir()));
        let env = env(&[
            ("ACCOUNT_ID", "123456789012"),
            ("API_TLS_KEY", key()),
            ("GOSSIP_TLS_KEY", key()),
            ("CATALOG_CLIENT_ID", "cdc-client"),
            ("CATALOG_CLIENT_SECRET", "client-secret"),
            ("ORDERS_DB_PASSWORD", "orders-pw"),
            ("EVENTS_DB_CREDS_FILE", creds.to_str().unwrap()),
        ]);
        (yaml, env)
    }

    fn load(yaml: &str) -> ResolvedConfig {
        load_str(yaml, &env(&[("PW", "pw")]), &fp_key())
            .unwrap_or_else(|r| panic!("{r:#?}"))
            .config
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
            load_str(&spec, &spec_env, &fp_key()).unwrap().config,
            load(&rich),
            load(&bearer),
            load(&glue),
        ];
        for cfg in &configs {
            assert_eq!(unclassified_paths(cfg), Vec::<String>::new());
        }
    }
}
