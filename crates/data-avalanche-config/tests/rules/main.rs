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
use data_avalanche_config::{RULES, RuleInput, Severity, validate_str};

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
        let ok = match case.expect {
            Expect::Error => hits.iter().any(|d| d.severity == Severity::Error),
            Expect::Warning => hits.iter().any(|d| d.severity == Severity::Warning),
            Expect::Silent => hits.is_empty(),
        };
        let internal = report.iter().any(|d| d.message.starts_with("internal:"));
        if !ok || internal {
            failures.push(format!(
                "{} {:?} ({}): got {:#?}",
                case.rule,
                case.expect,
                case.name,
                report
                    .iter()
                    .map(|d| format!("{} {:?} {} {}", d.rule, d.severity, d.path, d.message))
                    .collect::<Vec<_>>()
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
