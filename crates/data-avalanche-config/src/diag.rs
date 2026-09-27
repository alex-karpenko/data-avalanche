//! Diagnostics: one per violated rule, collected into a [`Report`].

use serde::Serialize;

use crate::leaf::Loc;

/// Phase 0 failure with no V-rule: YAML syntax, duplicate or unknown key, several documents, or a
/// value of the wrong shape (a map where a scalar belongs). Fatal; only the first one is reported.
pub const PARSE: &str = "PARSE";
/// A scalar that doesn't coerce to its field's type (`port: abc`, `lease_duration: 5 sec`) and
/// isn't covered by a value-domain V-rule. Reported together with every other error.
pub const TYPE: &str = "TYPE";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
}

/// Where a problem is (config §13 "Locus"). Diagnostic only: any error rejects the whole config.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(tag = "kind", content = "pipeline", rename_all = "lowercase")]
pub enum Locus {
    Config,
    /// The pipeline's human name (its key under `pipelines`).
    Pipeline(String),
    Reload,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Diagnostic {
    pub locus: Locus,
    /// YAML key path, e.g. `pipelines.orders.sink.flush`; empty for the document root.
    pub path: String,
    /// `V-nnn`, [`PARSE`] or [`TYPE`].
    pub rule: String,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loc: Option<Loc>,
    pub message: String,
}

impl Diagnostic {
    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }
}

/// Every diagnostic of one load or validation run, errors and warnings apart, each list sorted by
/// locus, path and rule, without duplicates. This is the body of the validation report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Report {
    pub errors: Vec<Diagnostic>,
    pub warnings: Vec<Diagnostic>,
}

impl Report {
    /// True when there are no errors (warnings never block, config §1.3).
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn push(&mut self, d: Diagnostic) {
        let list = match d.severity {
            Severity::Error => &mut self.errors,
            Severity::Warning => &mut self.warnings,
        };
        if let Err(i) = list.binary_search(&d) {
            list.insert(i, d);
        }
    }

    pub fn extend(&mut self, other: Report) {
        for d in other.errors.into_iter().chain(other.warnings) {
            self.push(d);
        }
    }

    /// All diagnostics, errors first.
    pub fn iter(&self) -> impl Iterator<Item = &Diagnostic> {
        self.errors.iter().chain(&self.warnings)
    }

    /// True when some diagnostic carries `rule` (any severity).
    pub fn has(&self, rule: &str) -> bool {
        self.iter().any(|d| d.rule == rule)
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "valid": self.is_ok(),
            "errors": self.errors,
            "warnings": self.warnings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(rule: &str, severity: Severity, path: &str) -> Diagnostic {
        Diagnostic {
            locus: Locus::Config,
            path: path.into(),
            rule: rule.into(),
            severity,
            loc: None,
            message: format!("{rule} at {path}"),
        }
    }

    #[test]
    fn splits_by_severity_sorts_and_dedups() {
        let mut r = Report::default();
        r.push(d("V-007", Severity::Error, "b"));
        r.push(d("V-037", Severity::Warning, "a"));
        r.push(d("V-006", Severity::Error, "a"));
        r.push(d("V-007", Severity::Error, "b"));
        assert_eq!(
            r.errors.iter().map(|x| x.rule.as_str()).collect::<Vec<_>>(),
            ["V-006", "V-007"]
        );
        assert_eq!(r.warnings.len(), 1);
        assert!(!r.is_ok());
        assert!(r.has("V-037"));
    }

    #[test]
    fn json_shape_is_stable() {
        let mut r = Report::default();
        let mut e = d("V-055", Severity::Error, "pipelines.p.id");
        e.locus = Locus::Pipeline("p".into());
        e.loc = Some(Loc { line: 3, column: 9 });
        r.push(e);
        assert_eq!(
            r.to_json(),
            serde_json::json!({
                "valid": false,
                "errors": [{
                    "locus": {"kind": "pipeline", "pipeline": "p"},
                    "path": "pipelines.p.id",
                    "rule": "V-055",
                    "severity": "error",
                    "loc": {"line": 3, "column": 9},
                    "message": "V-055 at pipelines.p.id"
                }],
                "warnings": []
            })
        );
    }
}
