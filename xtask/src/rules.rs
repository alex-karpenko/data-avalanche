//! `reference/validation-rules.md`: every V-rule of config §13, and which key or block each one
//! checks (read from the rule's "Path / Scope" column).

use std::collections::BTreeSet;
use std::fmt::Write;

use data_avalanche_config::{RULES, RuleSeverity, config_keys};

use crate::{cell, header};

/// What a rule's "Path / Scope" names, as the configuration reference places it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RuleTarget {
    /// One key (`globals.cluster.tokens`).
    Key(String),
    /// A block (`pipelines.*.source`): the rule relates its keys.
    Block(String),
    /// Neither: the whole file, a pipeline's name, any `${VAR}`.
    Elsewhere,
}

/// The paths a rule's "Path / Scope" text names: its leading dotted path (`globals.api.auth` of
/// `globals.api.auth = basic`), each alternative of `a / b`, each branch of `{x,y}`; a pipeline's
/// field (`source.host`) under `pipelines.*`.
pub(crate) fn named_paths(field: &str) -> Vec<String> {
    let mut out = Vec::new();
    for alternative in field.split(" / ") {
        let lead: String = alternative
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || "._{},".contains(*c))
            .collect();
        let lead = lead
            .strip_prefix("pipeline.")
            .unwrap_or(&lead)
            .trim_end_matches('.');
        let expanded = match (lead.find('{'), lead.find('}')) {
            (Some(open), Some(close)) if open < close => lead[open + 1..close]
                .split(',')
                .map(|branch| format!("{}{branch}{}", &lead[..open], &lead[close + 1..]))
                .collect(),
            _ => vec![lead.to_string()],
        };
        for path in expanded {
            if path.is_empty() {
                continue;
            }
            out.push(if path.starts_with("globals") || path == "config_version" {
                path
            } else {
                format!("pipelines.*.{path}")
            });
        }
    }
    out
}

/// Each rule with the key or block of the config file it checks; a rule naming several gets one
/// entry each.
pub(crate) fn targets() -> Vec<(&'static str, RuleTarget)> {
    let keys: BTreeSet<String> = config_keys().into_iter().map(|k| k.path).collect();
    let mut out = Vec::new();
    for rule in RULES {
        let mut placed = false;
        for path in named_paths(rule.field) {
            let target = if keys.contains(&path) {
                RuleTarget::Key(path)
            } else if keys.iter().any(|k| k.starts_with(&format!("{path}."))) {
                RuleTarget::Block(path)
            } else {
                continue;
            };
            placed = true;
            out.push((rule.id, target));
        }
        if !placed {
            out.push((rule.id, RuleTarget::Elsewhere));
        }
    }
    out
}

pub(crate) fn page() -> String {
    let mut out = header(
        "Validation rules",
        "The rules a config file must pass, from the config's master validation table.",
        2,
        "the config model (`data_avalanche_config::RULES`)",
    );
    out.push_str(
        "A config with any **error** is rejected whole: at startup the node refuses to start, \
         and on a reload the running config stays in force. A **warning** is logged and the config \
         loads. `POST /v1/config/validate` runs the same checks without applying anything. \
         Rules marked *error and warning* error outside a hard range and warn outside the \
         recommended one.\n\n\
         | Rule | Severity | Where | Rule text |\n|---|---|---|---|\n",
    );
    for rule in RULES {
        let severity = match rule.severity {
            RuleSeverity::Error => "error",
            RuleSeverity::Warning => "warning",
            RuleSeverity::ErrorAndWarning => "error and warning",
        };
        let _ = writeln!(
            out,
            "| <span id=\"{}\"></span>{} | {severity} | {} | {} |",
            rule.id.to_lowercase(),
            rule.id,
            cell(&format!("`{}`", rule.field.replace('`', ""))),
            cell(rule.text),
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rule_names_its_key_its_block_or_neither() {
        let target = |id: &str| {
            targets()
                .into_iter()
                .filter(|(i, _)| *i == id)
                .map(|(_, t)| t)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            target("V-026"),
            [RuleTarget::Key("globals.cluster.tokens".into())]
        );
        assert_eq!(
            target("V-001"),
            [RuleTarget::Block("pipelines.*.source".into())]
        );
        assert_eq!(
            target("V-011"),
            [
                RuleTarget::Key("pipelines.*.source.postgres.publication".into()),
                RuleTarget::Key("pipelines.*.source.postgres.slot".into()),
            ]
        );
        assert_eq!(
            target("V-049"),
            [RuleTarget::Key("globals.api.auth".into())]
        );
        assert_eq!(target("V-063"), [RuleTarget::Elsewhere]);
    }
}
