//! The config-time V-rules of §13 (the master table is `crate::registry::RULES`). Every check runs
//! twice: in the structural pass, where a template's value is unknown and value checks skip it,
//! and in the resolving pass, where every value is known. Diagnostics are deduplicated.

use crate::ctx::{At, Ctx};
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::{RawConfig, RawPipeline};
use crate::walk::Walk;

mod auth;
pub(crate) mod cross;
mod domain;
mod identity;
mod structure;

/// Runs every config-time rule over the parsed document and its merged pipelines.
pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    check_defaults(raw, ctx);
    structure::check(raw, merged, ctx);
    identity::check(raw, merged, ctx);
    domain::check(raw, merged, ctx);
    cross::check(raw, merged, ctx);
    auth::check(raw, merged, ctx);
}

/// §8.6: `defaults` validated on its own, before any pipeline inherits it — every leaf coerced
/// (`TYPE` / its own V-rule / V-063, via the raw-model walker), plus `domain.rs`'s single-field
/// rules (`domain::check_defaults`; it lists which rules run and which are skipped). Locus config,
/// path `defaults`. No files are read here (§8.6) — Phase 1 never reads files regardless, and
/// Phase 2 only reads a file for a setting that's in effect on a *merged* pipeline (§11 Phase 2),
/// which `defaults` alone never is.
fn check_defaults(raw: &RawConfig, ctx: &mut Ctx) {
    let at = At::config(YamlPath::root().key("defaults"));
    raw.defaults.check_values(ctx, &at);
    if let Some(defaults) = &raw.defaults {
        domain::check_defaults(defaults, ctx);
    }
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
