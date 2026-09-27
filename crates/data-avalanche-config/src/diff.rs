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
    /// The changed path (`globals.log.level`, `pipelines.orders.sink.flush.rows`).
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

/// Every leaf path of `cfg` that no mutability pattern covers (for tests: must be empty).
pub fn unclassified_paths(cfg: &ResolvedConfig) -> Vec<String> {
    let mut all = flat(&["globals"], &cfg.globals);
    for (id, p) in &cfg.pipelines {
        all.extend(flat(&["pipelines", id], p));
    }
    all.keys()
        .filter(|p| classify(&p.iter().map(String::as_str).collect::<Vec<_>>()).is_none())
        .map(|p| dotted(p))
        .collect()
}
