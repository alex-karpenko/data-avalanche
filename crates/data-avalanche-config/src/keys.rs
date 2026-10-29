//! Every key the raw model takes (config §3–§9), with what its value expects and the rule that
//! checks it: the spine of the generated configuration reference (M11b). `walk_fields!` generates
//! [`Keys`] for each raw struct from the same field list as [`crate::walk::Walk`], so the
//! reference can't drift from the model.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::coerce::LeafType;
use crate::leaf::{Leaf, Tri};
use crate::mutability::{Mutability, classify};
use crate::raw::{RawConfig, RawPartitionField, RawPipeline};

/// What a key holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyShape {
    /// One value.
    Scalar,
    /// One value, or `null` to turn the setting off (§9.4.1).
    Nullable,
    /// A list of values.
    List,
    /// A list of values, or `null` for none (§8.3).
    NullableList,
    /// A map whose keys the operator names (the path ends in `*`), of one value each.
    Map,
    /// A map whose keys the operator names, of a list of values each.
    MapOfLists,
}

/// One key of the config file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Key {
    /// Dotted, with `*` for a segment the operator names: `pipelines.*.source.port`.
    pub path: String,
    pub shape: KeyShape,
    /// What the value must be (`"a duration"`, `"one of: aws, google, azure"`).
    pub expects: &'static str,
    /// The rule that reports a value that isn't one (`TYPE`, or the enum's V-rule).
    pub rule: &'static str,
}

/// Every key of the config file, in the model's order: `config_version`, `globals`, then a
/// pipeline's keys under `pipelines.*` (the same keys `defaults` takes, §7).
pub fn config_keys() -> Vec<Key> {
    let RawConfig {
        config_version,
        globals,
        defaults: _,
        pipelines: _,
    } = RawConfig::default();
    let mut out = Vec::new();
    config_version.keys("config_version", &mut out);
    globals.keys("globals", &mut out);
    RawPipeline::default().keys("pipelines.*", &mut out);
    out
}

pub(crate) trait Keys {
    fn keys(&self, path: &str, out: &mut Vec<Key>);
}

pub(crate) fn child(path: &str, key: &str) -> String {
    format!("{path}.{key}")
}

fn leaf<T: LeafType>(path: &str, shape: KeyShape, out: &mut Vec<Key>) {
    out.push(Key {
        path: path.to_string(),
        shape,
        expects: T::EXPECTED,
        rule: T::RULE,
    });
}

impl<T: LeafType> Keys for Option<Leaf<T>> {
    fn keys(&self, path: &str, out: &mut Vec<Key>) {
        leaf::<T>(path, KeyShape::Scalar, out);
    }
}

impl<T: LeafType> Keys for Tri<T> {
    fn keys(&self, path: &str, out: &mut Vec<Key>) {
        leaf::<T>(path, KeyShape::Nullable, out);
    }
}

impl<T: LeafType> Keys for Option<Vec<Leaf<T>>> {
    fn keys(&self, path: &str, out: &mut Vec<Key>) {
        leaf::<T>(path, KeyShape::List, out);
    }
}

impl<T: LeafType> Keys for Option<Option<Vec<Leaf<T>>>> {
    fn keys(&self, path: &str, out: &mut Vec<Key>) {
        leaf::<T>(path, KeyShape::NullableList, out);
    }
}

impl<T: LeafType> Keys for Option<BTreeMap<String, Leaf<T>>> {
    fn keys(&self, path: &str, out: &mut Vec<Key>) {
        leaf::<T>(&child(path, "*"), KeyShape::Map, out);
    }
}

impl<T: LeafType> Keys for Option<BTreeMap<String, Vec<Leaf<T>>>> {
    fn keys(&self, path: &str, out: &mut Vec<Key>) {
        leaf::<T>(&child(path, "*"), KeyShape::MapOfLists, out);
    }
}

impl Keys for Option<BTreeMap<String, Vec<RawPartitionField>>> {
    fn keys(&self, path: &str, out: &mut Vec<Key>) {
        RawPartitionField::default().keys(&format!("{path}.*[]"), out);
    }
}

/// The resolved config's spelling of a key the resolved form renames: a credential pair and its
/// file are one `credentials`, a bearer token and its file one `token`, the Delta catalog's type
/// `catalog_type`.
pub(crate) fn resolved_path(path: &str) -> &str {
    const RENAMED: &[(&str, &str)] = &[
        ("globals.api.user", "globals.api.credentials"),
        ("globals.api.password", "globals.api.credentials"),
        ("globals.api.credentials_file", "globals.api.credentials"),
        (
            "pipelines.*.sink.iceberg.catalog.hive.user",
            "pipelines.*.sink.iceberg.catalog.hive.credentials",
        ),
        (
            "pipelines.*.sink.iceberg.catalog.hive.password",
            "pipelines.*.sink.iceberg.catalog.hive.credentials",
        ),
        (
            "pipelines.*.sink.iceberg.catalog.hive.credentials_file",
            "pipelines.*.sink.iceberg.catalog.hive.credentials",
        ),
        (
            "pipelines.*.sink.iceberg.catalog.rest.token_file",
            "pipelines.*.sink.iceberg.catalog.rest.token",
        ),
        (
            "pipelines.*.sink.delta.catalog.type",
            "pipelines.*.sink.delta.catalog_type",
        ),
    ];
    RENAMED
        .iter()
        .find(|(raw, _)| *raw == path)
        .map_or(path, |(_, resolved)| resolved)
}

/// What a change to the key does on a reload (config §1.2): applied in place, a pipeline
/// restart, or a process restart. `None` only for a path that isn't a key.
pub fn key_mutability(path: &str) -> Option<Mutability> {
    let segments: Vec<&str> = resolved_path(path).split('.').collect();
    classify(&segments)
}
