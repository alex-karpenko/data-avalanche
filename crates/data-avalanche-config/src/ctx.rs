//! The evaluation context shared by merge, rules and the resolved-config build: it turns leaves
//! into values (substituting `${VAR}` when resolving) and collects diagnostics.

use crate::coerce::LeafType;
use crate::diag::{Diagnostic, Locus, Report, Severity};
use crate::leaf::{Leaf, Loc};
use crate::path::YamlPath;
use crate::secret::FingerprintKey;
use crate::template::{Env, Expanded, expand};

/// Where a check applies: the locus (config, a named pipeline, or reload) and the YAML path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct At {
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

/// Whether `leaf`'s own written text is a `${VAR}` reference (resolved or not) — decided from the
/// text alone, never from the resolved value's shape. `$${` escapes don't count: `expand` already
/// unescapes them without leaving a reference behind.
pub(crate) fn is_templated<T>(leaf: &Leaf<T>) -> bool {
    matches!(expand(leaf.text(), None), Ok(Expanded::Unresolved))
}

/// F3b: [`shown`] for a value already coerced out of a leaf (a range/consistency check's `v`,
/// after `ctx.value` succeeded) — the coerced value has no resolved *text* to compare against
/// `leaf.text()`, so the redaction decision is made from the leaf's own text instead. A literal
/// leaf displays `v` normally; a templated leaf displays its written `${VAR}` text with the
/// resolution called out, never `v`.
pub(crate) fn shown_value<T: std::fmt::Display>(leaf: &Leaf<T>, v: &T) -> String {
    if is_templated(leaf) {
        format!("{:?} (resolved value hidden)", leaf.text())
    } else {
        v.to_string()
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

/// One evaluation pass. `structural()` is config §11 Phase 1 (no environment, no files, so no
/// secret is ever built — the fingerprint key would go unused); `resolving(env, key)` is Phase 2
/// (references substituted, files read by the build, [`crate::secret::Secret`] values built with
/// `key`, config §6.2).
pub(crate) struct Ctx<'e> {
    env: Option<&'e dyn Env>,
    key: Option<&'e FingerprintKey>,
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
            key: None,
            report: Report::default(),
        }
    }

    pub fn resolving(env: &'e dyn Env, key: &'e FingerprintKey) -> Self {
        Self {
            env: Some(env),
            key: Some(key),
            report: Report::default(),
        }
    }

    pub fn is_resolving(&self) -> bool {
        self.env.is_some()
    }

    /// The fingerprint key for this pass: `None` in [`Ctx::structural`] (it builds no secret,
    /// since it reads no file and a leaf's inline value never needs fingerprinting before it's
    /// known to survive the pass), `Some` in [`Ctx::resolving`].
    pub(crate) fn fingerprint_key(&self) -> Option<&'e FingerprintKey> {
        self.key
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
    /// G3 (config §11.1): a value obtained by substituting `${VAR}` is never printed in a
    /// diagnostic, and neither is any fragment of it — the parser's own failure `reason` and the
    /// caller-built `message` both quote pieces of the resolved text (e.g. `write e.g. "12s"`,
    /// `unknown time unit "xyz"`), and no amount of substring surgery on the built message can be
    /// trusted to catch every such fragment. So when `leaf` was templated (its written text differs
    /// from the resolved `text`), `reason`/`message` are never even built: the diagnostic is a fixed
    /// sentence naming the leaf's own `${VAR}` text and `T::EXPECTED`. A literal (non-templated)
    /// leaf's message is unchanged — built by `message` as always.
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
                let msg = if leaf.text() == text {
                    message(&text, reason)
                } else {
                    format!("{} is not {}", shown(leaf, &text), T::EXPECTED)
                };
                self.error(T::RULE, at, leaf.loc(), msg);
                None
            }
        }
    }

    /// The leaf's value without reporting anything: for choices made before validation (the
    /// union branch in merge). `None` when absent, unknown or invalid.
    pub fn peek<T: LeafType>(&self, leaf: &Option<Leaf<T>>) -> Option<T> {
        match expand(leaf.as_ref()?.text(), self.env) {
            Ok(Expanded::Text(t)) => T::parse_leaf(&t).ok(),
            _ => None,
        }
    }

    /// The leaf's expanded text without reporting anything (`Walk::drop_empty`, §11 Phase 2:
    /// "empty is absent"). `None` when the text is a template not yet known (structural pass) or
    /// fails to expand — its own coercion later reports that; a leaf that fails here is never
    /// treated as empty, only as "not yet known".
    pub(crate) fn peek_text<T>(&self, leaf: &Leaf<T>) -> Option<String> {
        match expand(leaf.text(), self.env) {
            Ok(Expanded::Text(t)) => Some(t),
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

    /// Only used by unit tests inline in this crate (production code takes the report by value via
    /// [`Ctx::into_report`] once a pass is done).
    #[cfg(test)]
    pub(crate) fn report(&self) -> &Report {
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
        let key = FingerprintKey::from_bytes([1; 32]);
        let mut ctx = Ctx::resolving(&env, &key);
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

    /// G3: the parser's own failure reason (`parse_duration`'s `write e.g. "12s"` /
    /// `unknown time unit "xyz"`) quotes a fragment of the resolved text — not the whole value a
    /// substring replacement could catch — so a templated leaf must skip that reason entirely,
    /// not merely redact known-equal substrings out of it.
    #[test]
    fn templated_duration_coercion_error_hides_every_fragment_of_the_resolved_text() {
        for (value, must_not_contain) in [("12", "12"), ("7xyz", "xyz")] {
            let env: BTreeMap<String, String> = [("D".to_string(), value.to_string())].into();
            let key = FingerprintKey::from_bytes([1; 32]);
            let mut ctx = Ctx::resolving(&env, &key);
            assert_eq!(ctx.value(&Leaf::<Duration>::new("${D}"), &at()), None);
            let e = &ctx.report().errors[0];
            assert!(!e.message.contains(must_not_contain), "{}", e.message);
            assert!(e.message.contains("${D}"), "{}", e.message);
        }
    }

    /// G3: a templated config enum's message still lists the allowed values (public spec text,
    /// `LeafType::EXPECTED`), but never the invalid resolved value itself. O2: the sentence reads
    /// "is not one of: …", not "is not a valid one of: …".
    #[test]
    fn templated_enum_hides_the_value_and_lists_the_allowed_choices() {
        let env: BTreeMap<String, String> = [("CLOUD".to_string(), "nope".to_string())].into();
        let key = FingerprintKey::from_bytes([2; 32]);
        let mut ctx = Ctx::resolving(&env, &key);
        let at = At::config(YamlPath::root().key("globals").key("cloud"));
        assert_eq!(ctx.value(&Leaf::<Cloud>::new("${CLOUD}"), &at), None);
        let e = &ctx.report().errors[0];
        assert_eq!(e.rule, "V-015");
        assert_eq!(
            e.message,
            "\"${CLOUD}\" (resolved value hidden) is not one of: aws, google, azure"
        );
    }

    /// O2: a templated leaf of a non-enum type reads "is not a duration" (`LeafType::EXPECTED` is
    /// now a complete phrase), not "is not a valid duration".
    #[test]
    fn templated_non_enum_coercion_error_reads_is_not_a_duration() {
        let env: BTreeMap<String, String> = [("D".to_string(), "xyz".to_string())].into();
        let key = FingerprintKey::from_bytes([3; 32]);
        let mut ctx = Ctx::resolving(&env, &key);
        assert_eq!(ctx.value(&Leaf::<Duration>::new("${D}"), &at()), None);
        let e = &ctx.report().errors[0];
        assert_eq!(
            e.message,
            "\"${D}\" (resolved value hidden) is not a duration"
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
