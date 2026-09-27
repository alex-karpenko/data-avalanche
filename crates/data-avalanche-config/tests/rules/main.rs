//! Every config-time V-rule, one passing and one failing case at least (M1 exit criterion). Cases
//! are small edits of valid base configs; a config is a JSON value, which is valid YAML 1.2.

#[path = "../support/mod.rs"]
mod support;

mod auth;
mod cases;
mod cross;
mod domain;
mod identity;
mod resolution;
mod structure;

use cases::{Case, Expect};
use data_avalanche_config::{Locus, RULES, RuleInput, Severity, rule_meta, validate_str};

/// Whether `locus` is one the registry's §13 "Locus" column allows for a rule at `path`, given the
/// column's text exactly as `src/registry.rs` writes it (`"config"`, `"pipeline"`,
/// `"pipeline (resolution)"`, `"config / pipeline"`, `"config (startup)"`, `"reload"`). Panics on
/// any other spelling — see `every_registry_locus_is_recognized`, which checks every `RULES` entry
/// against that panic.
///
/// §13 intro (spec commit c591bdb): a diagnostic for a value written in `defaults` (§8.6) always
/// has locus `config` and a `defaults.…` path, whatever the rule's own Locus column says — that
/// column describes the rule as it applies to a pipeline. So a `config`-locus diagnostic at a
/// `defaults` path is allowed regardless of the rule's own column.
fn locus_allowed(column: &str, locus: &Locus, path: &str) -> bool {
    if path.starts_with("defaults") && matches!(locus, Locus::Config) {
        return true;
    }
    match column {
        "config" | "config (startup)" => matches!(locus, Locus::Config),
        "pipeline" | "pipeline (resolution)" => matches!(locus, Locus::Pipeline(_)),
        "config / pipeline" => matches!(locus, Locus::Config | Locus::Pipeline(_)),
        "reload" => matches!(locus, Locus::Reload),
        other => panic!("unrecognized registry locus column {other:?}"),
    }
}

/// So a future §13 locus spelling can't silently slip past `locus_allowed`'s `other => panic!`
/// arm (and so `rule_cases` skip its check for that rule): every `RULES` entry's locus column must
/// be one `locus_allowed` recognizes, independent of which rule fired. `path` is empty so the
/// `defaults` special case never short-circuits this check.
#[test]
fn every_registry_locus_is_recognized() {
    for rule in RULES {
        locus_allowed(rule.locus, &Locus::Config, "");
    }
}

fn all_cases() -> Vec<Case> {
    let mut all = Vec::new();
    all.extend(structure::cases());
    all.extend(domain::cases());
    all.extend(cross::cases());
    all.extend(auth::cases());
    all.extend(identity::cases());
    all.extend(resolution::cases());
    all
}

/// Rules with no failing input through `validate_str` in v1:
/// - V-069: the only "other referenced file" is `catalog.hive.kerberos.keytab`, and V-115 rejects
///   every Hive catalog in Phase 1, so Phase 2 never reads it. `material::read_file_ref` has unit
///   tests for the check itself.
/// - V-087: every logical type of the grammar maps to both sinks (Delta turns `time`/`uuid` into
///   strings), so nothing can fail it until a sink loses a type.
const NO_FAILING_INPUT: &[&str] = &["V-069", "V-087"];

#[test]
fn base_configs_are_clean() {
    for (name, doc) in cases::bases() {
        let report = validate_str(&doc.to_string(), &cases::base_env());
        assert!(
            report.errors.is_empty() && report.warnings.is_empty(),
            "{name}: {report:#?}"
        );
    }
}

#[test]
fn rule_cases() {
    let mut failures = Vec::new();
    for case in all_cases() {
        let files = support::Files::new();
        let fixture = (case.build)(&files);
        let report = validate_str(&fixture.doc.to_string(), &fixture.env);
        let hits: Vec<_> = report.iter().filter(|d| d.rule == case.rule).collect();
        let want_severity = match case.expect {
            Expect::Error => Some(Severity::Error),
            Expect::Warning => Some(Severity::Warning),
            Expect::Silent => None,
        };
        let ok = match want_severity {
            Some(s) => hits.iter().any(|d| d.severity == s),
            None => hits.is_empty(),
        };
        // Every diagnostic that makes this case pass must also sit at a locus the registry's §13
        // "Locus" column allows for the rule — otherwise the rule fired in the wrong place.
        // `PARSE`/`TYPE` cover diagnostics with no V-rule (§13 intro) and so have no registry row;
        // only a real `V-nnn` rule has a locus column to check against.
        let mut bad_locus = Vec::new();
        if let (Some(s), Some(meta)) = (want_severity, rule_meta(case.rule)) {
            for d in hits.iter().filter(|d| d.severity == s) {
                if !locus_allowed(meta.locus, &d.locus, &d.path) {
                    bad_locus.push(format!(
                        "{:?} is not allowed by registry locus {:?}",
                        d.locus, meta.locus
                    ));
                }
            }
        }
        let internal = report.iter().any(|d| d.message.starts_with("internal:"));
        if !ok || internal || !bad_locus.is_empty() {
            failures.push(format!(
                "{} {:?} ({}): got {:#?}{}",
                case.rule,
                case.expect,
                case.name,
                report
                    .iter()
                    .map(|d| format!("{} {:?} {} {}", d.rule, d.severity, d.path, d.message))
                    .collect::<Vec<_>>(),
                if bad_locus.is_empty() {
                    String::new()
                } else {
                    format!("; locus mismatch: {}", bad_locus.join("; "))
                }
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failing case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn every_config_rule_has_a_passing_and_a_failing_case() {
    let cases = all_cases();
    let with = |id: &str, pass: bool| {
        cases
            .iter()
            .any(|c| c.rule == id && (c.expect == Expect::Silent) == pass)
    };
    let mut missing = Vec::new();
    for rule in RULES {
        if !matches!(rule.input, RuleInput::Config | RuleInput::Resolution) {
            continue;
        }
        if !with(rule.id, true) {
            missing.push(format!("{}: no passing case", rule.id));
        }
        if !with(rule.id, false) && !NO_FAILING_INPUT.contains(&rule.id) {
            missing.push(format!("{}: no failing case", rule.id));
        }
    }
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}
