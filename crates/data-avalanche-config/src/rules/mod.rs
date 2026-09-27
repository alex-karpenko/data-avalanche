//! The config-time V-rules of §13 (the master table is `crate::registry::RULES`). Every check runs
//! twice: in the structural pass, where a template's value is unknown and value checks skip it,
//! and in the resolving pass, where every value is known. Diagnostics are deduplicated.

use crate::ctx::{At, Ctx};
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::{RawConfig, RawPipeline};

mod domain;
mod identity;
mod structure;

/// Runs every config-time rule over the parsed document and its merged pipelines.
pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    structure::check(raw, merged, ctx);
    identity::check(raw, merged, ctx);
    domain::check(raw, merged, ctx);
}

/// `pipelines.<name>` as a pipeline-locus location.
pub(crate) fn pipeline_at(name: &str) -> At {
    At::pipeline(name, YamlPath::root().key("pipelines").key(name))
}

/// True when a required string is absent or known to be empty (§11 Phase 2: "any required value
/// that resolves to empty is an error"). A template not yet resolved counts as present.
pub(crate) fn missing(ctx: &mut Ctx, leaf: &Option<Leaf<String>>, at: &At) -> bool {
    match leaf {
        None => true,
        Some(l) => ctx.text(l, at).is_some_and(|t| t.trim().is_empty()),
    }
}
