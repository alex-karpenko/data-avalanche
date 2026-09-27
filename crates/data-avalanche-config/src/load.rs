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

/// Loads config text. Any error rejects the whole config (§1.4) and the report lists every error
/// of the phase that failed (the first one only for a Phase 0 parse error).
pub fn load_str(text: &str, env: &dyn Env) -> Result<Loaded, Report> {
    let raw: RawConfig = parse_document(text).map_err(|d| only(*d))?;

    // Phase 1: references unresolved; no environment, no files.
    let mut ctx = Ctx::structural();
    let merged = merge_all(&raw, &ctx);
    rules::check(&raw, &merged, &mut ctx);
    build(&raw, &merged, &mut ctx);
    let structural = ctx.into_report();
    if !structural.is_ok() {
        return Err(structural);
    }

    // Phases 2 and 3: substitute, read files, apply defaults, check everything again with the
    // now-known values (a rule skipped on a template in Phase 1 runs here).
    let mut ctx = Ctx::resolving(env);
    let merged = merge_all(&raw, &ctx);
    rules::check(&raw, &merged, &mut ctx);
    let config = build(&raw, &merged, &mut ctx);
    let mut report = ctx.into_report();
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
