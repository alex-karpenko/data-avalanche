//! The load pipeline (config §11): parse → structural pass → resolving pass → resolved config.
//! The same pipeline serves startup, reload and the validation endpoint.

use std::path::Path;

use crate::build::{build, merge_all};
use crate::ctx::Ctx;
use crate::diag::{Diagnostic, Locus, PARSE, Report, Severity};
use crate::parse::parse_document;
use crate::raw::RawConfig;
use crate::resolved::ResolvedConfig;
use crate::rules;
use crate::template::Env;
use crate::walk::Walk;

/// A config that passed every phase. `warnings` never block loading (§1.3).
#[derive(Debug, Clone)]
pub struct Loaded {
    pub config: ResolvedConfig,
    pub warnings: Vec<Diagnostic>,
}

fn only(d: Diagnostic) -> Report {
    let mut r = Report::default();
    r.push(d);
    r
}

/// §11 Phase 2 ("an optional value that resolves to empty is absent") and §8.6 ("`defaults`
/// validated on its own"): before merging, every optional leaf of `raw.globals`, `raw.defaults`
/// and each raw pipeline whose resolved text is empty becomes absent, so "absent" uniformly means
/// "inherit from `defaults`, then the spec default" (§8). Run once per pass — the structural pass
/// (`ctx.is_resolving() == false`) only ever finds a literal `""` empty (an unresolved template
/// isn't known to be empty yet); the resolving pass additionally drops a template that resolves to
/// `""`, on top of whatever the structural pass already dropped.
fn drop_empty(raw: &mut RawConfig, ctx: &Ctx) {
    raw.globals.drop_empty(ctx);
    raw.defaults.drop_empty(ctx);
    if let Some(pipelines) = &mut raw.pipelines {
        for p in pipelines.values_mut() {
            p.drop_empty(ctx);
        }
    }
}

/// §8.6: a per-pipeline diagnostic that duplicates one the `defaults`-alone pass already emitted
/// (`rules::check_defaults`) is dropped — that value came from `defaults` and is already reported
/// once, at its `defaults` path. A per-pipeline diagnostic matches when its `rule` and `loc` equal
/// a config-locus diagnostic at a `defaults` path: `loc` is the value's exact source position, so a
/// leaf a pipeline didn't override carries the very same `loc` it had under `defaults` (merge
/// clones the leaf unchanged), and nothing else in the document can coincide with it. A diagnostic
/// with no `loc` (built in code rather than read from the file) is never deduped this way.
/// Cross-field diagnostics never collide, because the `defaults` pass never emits them (§8.6).
fn dedupe_defaults(report: &mut Report) {
    let from_defaults: std::collections::HashSet<(String, crate::leaf::Loc)> = report
        .errors
        .iter()
        .chain(&report.warnings)
        .filter(|d| d.locus == Locus::Config && d.path.starts_with("defaults"))
        .filter_map(|d| d.loc.map(|loc| (d.rule.clone(), loc)))
        .collect();
    let keep = |d: &Diagnostic| {
        !(matches!(d.locus, Locus::Pipeline(_))
            && d.loc
                .is_some_and(|loc| from_defaults.contains(&(d.rule.clone(), loc))))
    };
    report.errors.retain(keep);
    report.warnings.retain(keep);
}

/// Loads config text. Any error rejects the whole config (§1.4) and the report lists every error
/// of the phase that failed (the first one only for a Phase 0 parse error).
pub fn load_str(text: &str, env: &dyn Env) -> Result<Loaded, Report> {
    let mut raw: RawConfig = parse_document(text).map_err(|d| only(*d))?;

    // Phase 1: references unresolved; no environment, no files.
    let mut ctx = Ctx::structural();
    drop_empty(&mut raw, &ctx);
    let merged = merge_all(&raw, &ctx);
    rules::check(&raw, &merged, &mut ctx);
    build(&raw, &merged, &mut ctx);
    let mut structural = ctx.into_report();
    dedupe_defaults(&mut structural);
    if !structural.is_ok() {
        return Err(structural);
    }

    // Phases 2 and 3: substitute, read files, apply defaults, check everything again with the
    // now-known values (a rule skipped on a template in Phase 1 runs here).
    let mut ctx = Ctx::resolving(env);
    drop_empty(&mut raw, &ctx);
    let merged = merge_all(&raw, &ctx);
    rules::check(&raw, &merged, &mut ctx);
    let config = build(&raw, &merged, &mut ctx);
    let mut report = ctx.into_report();
    dedupe_defaults(&mut report);
    report.extend(structural);
    match config {
        Some(config) if report.is_ok() => Ok(Loaded {
            config,
            warnings: report.warnings,
        }),
        None if report.is_ok() => {
            // Every missing value must be reported by a rule; reaching here is a bug.
            report.push(Diagnostic {
                locus: Locus::Config,
                path: String::new(),
                rule: PARSE.into(),
                severity: Severity::Error,
                loc: None,
                message: "internal: the config could not be resolved but no rule reported why"
                    .into(),
            });
            Err(report)
        }
        _ => Err(report),
    }
}

/// Loads the config file at `path` (§2: the path comes from the command line).
pub fn load_file(path: &Path, env: &dyn Env) -> Result<Loaded, Report> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        only(Diagnostic {
            locus: Locus::Config,
            path: String::new(),
            rule: PARSE.into(),
            severity: Severity::Error,
            loc: None,
            message: format!("cannot read config file {}: {e}", path.display()),
        })
    })?;
    load_str(&text, env)
}

/// The validation endpoint's dry run (§11): the full pipeline, returning only the report. The
/// resolved config (it holds secrets) is never part of it (§11.1).
pub fn validate_str(text: &str, env: &dyn Env) -> Report {
    match load_str(text, env) {
        Ok(loaded) => Report {
            errors: Vec::new(),
            warnings: loaded.warnings,
        },
        Err(report) => report,
    }
}
