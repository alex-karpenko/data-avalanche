//! The evaluation context shared by merge, rules and the resolved-config build: it turns leaves
//! into values (substituting `${VAR}` when resolving) and collects diagnostics.

use crate::coerce::LeafType;
use crate::diag::{Diagnostic, Locus, Report, Severity};
use crate::leaf::{Leaf, Loc};
use crate::path::YamlPath;
use crate::template::{Env, Expanded, expand};

/// Where a check applies: the locus (config, a named pipeline, or reload) and the YAML path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct At {
    pub locus: Locus,
    pub path: YamlPath,
}

/// F2: the text to embed in a diagnostic message for a leaf's resolved value — never the
/// env-resolved value itself. `text` is `leaf`'s resolved text (after `${VAR}` substitution).
/// Quoted verbatim when `leaf` is literal (nothing was substituted); otherwise the leaf's own
/// written text (e.g. `${VAR}`), quoted, with the resolution called out.
pub(crate) fn shown<T>(leaf: &Leaf<T>, text: &str) -> String {
    if leaf.text() == text {
        format!("{text:?}")
    } else {
        format!("{:?} (resolved value hidden)", leaf.text())
    }
}

impl At {
    /// A config-level location (`globals…`, top-level keys).
    pub fn config(path: YamlPath) -> Self {
        Self {
            locus: Locus::Config,
            path,
        }
    }

    /// Inside pipeline `name` (its key under `pipelines`); `path` is the full path from the root.
    pub fn pipeline(name: &str, path: YamlPath) -> Self {
        Self {
            locus: Locus::Pipeline(name.to_string()),
            path,
        }
    }

    pub fn key(&self, key: impl Into<String>) -> Self {
        Self {
            locus: self.locus.clone(),
            path: self.path.key(key),
        }
    }

    pub fn index(&self, index: usize) -> Self {
        Self {
            locus: self.locus.clone(),
            path: self.path.index(index),
        }
    }
}

/// One evaluation pass. `structural()` is config §11 Phase 1 (no environment, no files);
/// `resolving(env)` is Phase 2 (references substituted, files read by the build).
pub struct Ctx<'e> {
    env: Option<&'e dyn Env>,
    report: Report,
}

impl std::fmt::Debug for Ctx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("resolving", &self.env.is_some())
            .field("report", &self.report)
            .finish()
    }
}

impl<'e> Ctx<'e> {
    pub fn structural() -> Self {
        Self {
            env: None,
            report: Report::default(),
        }
    }

    pub fn resolving(env: &'e dyn Env) -> Self {
        Self {
            env: Some(env),
            report: Report::default(),
        }
    }

    pub fn is_resolving(&self) -> bool {
        self.env.is_some()
    }

    /// The leaf's final text: `${VAR}` substituted and `$${` unescaped. `None` when the text is a
    /// template and this is the structural pass, or when substitution fails (V-063, reported).
    pub fn text<T>(&mut self, leaf: &Leaf<T>, at: &At) -> Option<String> {
        match expand(leaf.text(), self.env) {
            Ok(Expanded::Text(t)) => Some(t),
            Ok(Expanded::Unresolved) => None,
            Err(e) => {
                self.error("V-063", at, leaf.loc(), e.to_string());
                None
            }
        }
    }

    /// The leaf coerced to `T`. `None` when its text is unknown (see [`Ctx::text`]) or when it
    /// doesn't coerce; the latter is reported under `T::RULE` (a value-domain V-rule or `TYPE`).
    pub fn value<T: LeafType>(&mut self, leaf: &Leaf<T>, at: &At) -> Option<T> {
        self.value_with(leaf, at, |_, reason| reason)
    }

    /// [`Ctx::value`] with a caller-built message (`text`, `reason`) → message, for rules whose
    /// message the spec fixes (V-078).
    ///
    /// F2: a value obtained by substituting `${VAR}` is never printed in a diagnostic. When `leaf`
    /// was templated (its written text differs from the resolved `text`), every occurrence of the
    /// resolved value in the built message — Debug-quoted or raw — is replaced by the leaf's
    /// written text with the resolution called out (`shown`). A literal (non-templated) leaf's
    /// message is unchanged.
    pub fn value_with<T: LeafType>(
        &mut self,
        leaf: &Leaf<T>,
        at: &At,
        message: impl FnOnce(&str, String) -> String,
    ) -> Option<T> {
        let text = self.text(leaf, at)?;
        match T::parse_leaf(&text) {
            Ok(v) => Some(v),
            Err(reason) => {
                let mut msg = message(&text, reason);
                if leaf.text() != text {
                    let hidden = shown(leaf, &text);
                    msg = msg.replace(&format!("{text:?}"), &hidden);
                    if text.len() >= 4 && msg.contains(text.as_str()) {
                        msg = msg.replace(text.as_str(), &hidden);
                    }
                }
                self.error(T::RULE, at, leaf.loc(), msg);
                None
            }
        }
    }

    /// [`Ctx::value`] of an optional field; `None` when absent.
    pub fn opt<T: LeafType>(&mut self, leaf: &Option<Leaf<T>>, at: &At) -> Option<T> {
        leaf.as_ref().and_then(|l| self.value(l, at))
    }

    /// The leaf's value without reporting anything: for choices made before validation (the
    /// union branch in merge). `None` when absent, unknown or invalid.
    pub fn peek<T: LeafType>(&self, leaf: &Option<Leaf<T>>) -> Option<T> {
        match expand(leaf.as_ref()?.text(), self.env) {
            Ok(Expanded::Text(t)) => T::parse_leaf(&t).ok(),
            _ => None,
        }
    }

    pub fn error(&mut self, rule: &str, at: &At, loc: Option<Loc>, message: impl Into<String>) {
        self.push(rule, Severity::Error, at, loc, message.into());
    }

    pub fn warning(&mut self, rule: &str, at: &At, loc: Option<Loc>, message: impl Into<String>) {
        self.push(rule, Severity::Warning, at, loc, message.into());
    }

    fn push(&mut self, rule: &str, severity: Severity, at: &At, loc: Option<Loc>, message: String) {
        self.report.push(Diagnostic {
            locus: at.locus.clone(),
            path: at.path.to_string(),
            rule: rule.to_string(),
            severity,
            loc,
            message,
        });
    }

    pub fn report(&self) -> &Report {
        &self.report
    }

    pub fn into_report(self) -> Report {
        self.report
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;
    use crate::enums::Cloud;

    fn at() -> At {
        At::pipeline(
            "p",
            YamlPath::root()
                .key("pipelines")
                .key("p")
                .key("source")
                .key("port"),
        )
    }

    #[test]
    fn structural_pass_leaves_templates_unknown_and_checks_literals() {
        let mut ctx = Ctx::structural();
        assert_eq!(ctx.value(&Leaf::<u16>::new("${PG_PORT}"), &at()), None);
        assert!(
            ctx.report().is_ok(),
            "a template is not an error before resolution"
        );
        assert_eq!(ctx.value(&Leaf::<u16>::new("5432"), &at()), Some(5432));
        assert_eq!(ctx.value(&Leaf::<u16>::new("abc"), &at()), None);
        let e = &ctx.report().errors[0];
        assert_eq!(
            (e.rule.as_str(), e.path.as_str()),
            ("TYPE", "pipelines.p.source.port")
        );
        assert_eq!(e.locus, Locus::Pipeline("p".into()));
    }

    #[test]
    fn resolving_pass_substitutes_then_coerces() {
        let env: BTreeMap<String, String> = [("PG_PORT".to_string(), "5432".to_string())].into();
        let mut ctx = Ctx::resolving(&env);
        assert_eq!(
            ctx.value(&Leaf::<u16>::new("${PG_PORT}"), &at()),
            Some(5432)
        );
        assert_eq!(ctx.value(&Leaf::<u16>::new("${NOPE}"), &at()), None);
        assert_eq!(ctx.report().errors[0].rule, "V-063");
        assert_eq!(
            ctx.report().errors[0].message,
            "environment variable NOPE is not set"
        );
    }

    #[test]
    fn malformed_template_is_v063_even_before_resolution() {
        let mut ctx = Ctx::structural();
        assert_eq!(ctx.text(&Leaf::<String>::new("${OPEN"), &at()), None);
        assert_eq!(ctx.report().errors[0].rule, "V-063");
    }

    #[test]
    fn enum_failures_carry_their_value_domain_rule() {
        let mut ctx = Ctx::structural();
        let at = At::config(YamlPath::root().key("globals").key("cloud"));
        assert_eq!(ctx.value(&Leaf::<Cloud>::new("gcp"), &at), None);
        assert_eq!(ctx.report().errors[0].rule, "V-015");
        assert_eq!(
            ctx.value(&Leaf::<Duration>::new("15s"), &at),
            Some(Duration::from_secs(15))
        );
    }

    #[test]
    fn peek_reports_nothing() {
        let ctx = Ctx::structural();
        assert_eq!(
            ctx.peek(&Some(Leaf::<Cloud>::new("azure"))),
            Some(Cloud::Azure)
        );
        assert_eq!(ctx.peek(&Some(Leaf::<Cloud>::new("gcp"))), None);
        assert_eq!(ctx.peek::<Cloud>(&None), None);
        assert!(ctx.report().is_ok());
    }
}
