//! Environment-variable substitution on string leaves (config §4.3): `${VAR}`, `${VAR:-default}`,
//! and the `$${` escape. Substitution is not recursive: a substituted value is never re-scanned.

use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

/// Where `${VAR}` values come from. The process environment in production; a map in tests (so
/// tests never mutate the process environment, which is `unsafe` in edition 2024).
pub trait Env {
    fn var(&self, name: &str) -> Option<String>;
}

/// The process environment. A variable whose value isn't valid UTF-8 counts as unset.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnv;

impl Env for ProcessEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

impl Env for BTreeMap<String, String> {
    fn var(&self, name: &str) -> Option<String> {
        self.get(name).cloned()
    }
}

impl Env for HashMap<String, String> {
    fn var(&self, name: &str) -> Option<String> {
        self.get(name).cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum TemplateError {
    #[error("unterminated ${{...}} reference")]
    Unterminated,
    #[error("{0:?} is not a variable name (letters, digits and _, not starting with a digit)")]
    BadName(String),
    #[error(
        "a ${{...}} reference inside a :- default is not supported (substitution is not recursive)"
    )]
    NestedDefault,
    #[error("environment variable {0} is not set")]
    Unset(String),
}

/// The outcome of expanding one leaf's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expanded {
    /// The final text: references substituted and `$${` unescaped.
    Text(String),
    /// The text has references and no environment was given (structural validation, §11 Phase 1).
    Unresolved,
}

/// Expands `text`. With `env: None`, only checks the syntax: returns [`Expanded::Unresolved`] if
/// there is any reference, else the unescaped text. Syntax errors are reported in both modes.
pub(crate) fn expand(text: &str, env: Option<&dyn Env>) -> Result<Expanded, TemplateError> {
    let mut out = String::with_capacity(text.len());
    let mut unresolved = false;
    let mut rest = text;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        let tail = &rest[dollar..];
        if let Some(after) = tail.strip_prefix("$${") {
            out.push_str("${");
            rest = after;
        } else if let Some(after) = tail.strip_prefix("${") {
            let end = after.find('}').ok_or(TemplateError::Unterminated)?;
            let inner = &after[..end];
            let (name, default) = match inner.split_once(":-") {
                Some((n, d)) => (n, Some(d)),
                None => (inner, None),
            };
            if !is_var_name(name) {
                return Err(TemplateError::BadName(name.to_string()));
            }
            if default.is_some_and(|d| d.contains("${")) {
                return Err(TemplateError::NestedDefault);
            }
            match env {
                None => unresolved = true,
                Some(env) => match (env.var(name), default) {
                    (Some(v), Some(d)) if v.is_empty() => out.push_str(d),
                    (Some(v), _) => out.push_str(&v),
                    (None, Some(d)) => out.push_str(d),
                    (None, None) => return Err(TemplateError::Unset(name.to_string())),
                },
            }
            rest = &after[end + 1..];
        } else {
            out.push('$');
            rest = &tail[1..];
        }
    }
    out.push_str(rest);
    Ok(if unresolved {
        Expanded::Unresolved
    } else {
        Expanded::Text(out)
    })
}

fn is_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> BTreeMap<String, String> {
        [
            ("ACCOUNT_ID", "12345"),
            ("EMPTY", ""),
            ("PG_PORT", "5432"),
            ("LOOP", "${ACCOUNT_ID}"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn text(t: &str) -> String {
        match expand(t, Some(&env())).unwrap() {
            Expanded::Text(s) => s,
            Expanded::Unresolved => panic!("unresolved with an env"),
        }
    }

    #[test]
    fn substitutes_whole_and_partial_values() {
        assert_eq!(text("${PG_PORT}"), "5432");
        assert_eq!(text("${ACCOUNT_ID}-state"), "12345-state");
        assert_eq!(text("a${ACCOUNT_ID}b${PG_PORT}c"), "a12345b5432c");
        assert_eq!(text("plain"), "plain");
    }

    #[test]
    fn default_applies_when_unset_or_empty() {
        assert_eq!(text("${MISSING:-fallback}"), "fallback");
        assert_eq!(text("${EMPTY:-fallback}"), "fallback");
        assert_eq!(text("${PG_PORT:-1}"), "5432");
        assert_eq!(text("${MISSING:-}"), "");
        // Set but empty, no default: the empty string (a required field then fails its own rule).
        assert_eq!(text("${EMPTY}"), "");
    }

    #[test]
    fn escape_yields_a_literal_reference() {
        assert_eq!(text("$${ACCOUNT_ID}"), "${ACCOUNT_ID}");
        assert_eq!(text("cost: $5 and $$"), "cost: $5 and $$");
        assert_eq!(
            expand("$${X}", None).unwrap(),
            Expanded::Text("${X}".into())
        );
    }

    #[test]
    fn is_not_recursive() {
        assert_eq!(text("${LOOP}"), "${ACCOUNT_ID}");
    }

    #[test]
    fn reports_unset_and_malformed_references() {
        assert_eq!(
            expand("${NOPE}", Some(&env())).unwrap_err(),
            TemplateError::Unset("NOPE".into())
        );
        assert_eq!(
            expand("${OPEN", None).unwrap_err(),
            TemplateError::Unterminated
        );
        assert_eq!(
            expand("${1X}", None).unwrap_err(),
            TemplateError::BadName("1X".into())
        );
        assert_eq!(
            expand("${}", None).unwrap_err(),
            TemplateError::BadName("".into())
        );
        assert_eq!(
            expand("${A:-${B}}", None).unwrap_err(),
            TemplateError::NestedDefault
        );
    }

    #[test]
    fn without_env_references_stay_unresolved() {
        assert_eq!(expand("${PG_PORT}", None).unwrap(), Expanded::Unresolved);
        assert_eq!(expand("x", None).unwrap(), Expanded::Text("x".into()));
    }
}
