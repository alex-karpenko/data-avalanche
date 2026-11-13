//! `cargo xtask audit`: the RustSec advisories against `.cargo/audit.toml`'s written exceptions
//! (1.0.1's rule, checked again at every release). [`accepted`] reads the exceptions,
//! [`findings`] reads what `cargo audit --json` reports with no exception applied, [`evaluate`]
//! compares them, and [`Report::lines`] is what the command prints. An advisory nobody accepted
//! is unaccepted; an exception nothing in `Cargo.lock` matches is stale. Both fail the check.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use serde::Deserialize;

/// One advisory `cargo audit` reports: its id, the package and version it hits, and its kind
/// (`vulnerability`, or the warning kind: `unmaintained`, `unsound`, `notice`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub id: String,
    pub package: String,
    pub version: String,
    pub kind: String,
}

/// How the advisories stand against the exceptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// Reported, and no exception names it.
    pub unaccepted: Vec<Finding>,
    /// An exception that nothing reported matches.
    pub stale: Vec<String>,
    /// Reported and accepted.
    pub accepted: Vec<Finding>,
}

impl Report {
    /// One line per advisory or exception.
    pub fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for f in &self.unaccepted {
            lines.push(format!(
                "UNACCEPTED {} {} {} ({})",
                f.id, f.package, f.version, f.kind
            ));
        }
        for id in &self.stale {
            lines.push(format!("STALE      {id} (nothing in Cargo.lock matches)"));
        }
        for f in &self.accepted {
            lines.push(format!(
                "accepted   {} {} {} ({})",
                f.id, f.package, f.version, f.kind
            ));
        }
        lines
    }

    /// Whether every advisory is accepted and every exception is used.
    pub fn passes(&self) -> bool {
        self.unaccepted.is_empty() && self.stale.is_empty()
    }
}

#[derive(Deserialize)]
struct AuditToml {
    advisories: Option<Advisories>,
}

#[derive(Deserialize)]
struct Advisories {
    #[serde(default)]
    ignore: Vec<String>,
}

/// The ids in `audit.toml`'s `[advisories] ignore`.
pub fn accepted(audit_toml: &str) -> Result<Vec<String>, String> {
    let parsed: AuditToml = toml::from_str(audit_toml).map_err(|e| format!("audit.toml: {e}"))?;
    Ok(parsed.advisories.map(|a| a.ignore).unwrap_or_default())
}

#[derive(Deserialize)]
struct Entry {
    kind: Option<String>,
    advisory: Option<Advisory>,
    package: Package,
}

#[derive(Deserialize)]
struct Advisory {
    id: String,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    version: String,
}

/// Every advisory of `cargo audit --json`: the vulnerabilities and each warning list's entries
/// that carry an advisory id (a yanked crate has none and is not a finding).
pub fn findings(audit_json: &str) -> Result<Vec<Finding>, String> {
    let doc: serde_json::Value =
        serde_json::from_str(audit_json).map_err(|e| format!("cargo audit output: {e}"))?;
    // cargo audit always writes the list; without it the output isn't what we can read, and a
    // blocking gate must not pass on that.
    let list = doc
        .pointer("/vulnerabilities/list")
        .ok_or("cargo audit output: no `vulnerabilities.list`")?;
    let mut lists: Vec<(&str, &serde_json::Value)> = vec![("vulnerability", list)];
    if let Some(warnings) = doc.get("warnings").and_then(|w| w.as_object()) {
        for (kind, list) in warnings {
            lists.push((kind, list));
        }
    }
    let mut out = Vec::new();
    for (default_kind, list) in lists {
        let entries = list
            .as_array()
            .ok_or_else(|| format!("cargo audit output: `{default_kind}` is not a list"))?;
        for value in entries {
            let entry: Entry = Entry::deserialize(value)
                .map_err(|e| format!("cargo audit output: {default_kind} entry: {e}"))?;
            let Some(advisory) = entry.advisory else {
                continue;
            };
            out.push(Finding {
                id: advisory.id,
                package: entry.package.name,
                version: entry.package.version,
                kind: entry.kind.unwrap_or_else(|| default_kind.to_string()),
            });
        }
    }
    Ok(out)
}

/// Compares the exceptions with the advisories reported.
pub fn evaluate(accepted: &[String], findings: &[Finding]) -> Report {
    let listed: BTreeSet<&str> = accepted.iter().map(String::as_str).collect();
    let reported: BTreeSet<&str> = findings.iter().map(|f| f.id.as_str()).collect();
    let (accepted_findings, unaccepted) = findings
        .iter()
        .cloned()
        .partition(|f| listed.contains(f.id.as_str()));
    let stale = listed
        .iter()
        .filter(|id| !reported.contains(**id))
        .map(|id| id.to_string())
        .collect();
    Report {
        unaccepted,
        stale,
        accepted: accepted_findings,
    }
}

/// Audits `root`'s `Cargo.lock` against `.cargo/audit.toml`: `cargo audit` runs from a fresh
/// temporary directory, so the repository's own ignores don't apply and every advisory shows.
/// cargo-audit still reads a user-level `$CARGO_HOME/audit.toml`: its ignores can make an exception
/// look stale locally, never hide an unaccepted advisory (CI has none).
pub fn run(root: &Path) -> Result<Report, String> {
    let toml_path = root.join(".cargo/audit.toml");
    let text =
        std::fs::read_to_string(&toml_path).map_err(|e| format!("{}: {e}", toml_path.display()))?;
    let accepted = accepted(&text)?;
    let scratch = tempfile::tempdir().map_err(|e| format!("a temporary directory: {e}"))?;
    let output = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
        .args(["audit", "--json", "-f"])
        .arg(root.join("Cargo.lock"))
        .current_dir(scratch.path())
        .output()
        .map_err(|e| format!("cargo audit: {e}"))?;
    // It exits 1 when it finds something: the JSON decides, not the exit code.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let found = findings(&stdout).map_err(|e| {
        format!(
            "{e} (is cargo-audit installed? exit {:?}: {})",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
    })?;
    Ok(evaluate(&accepted, &found))
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUICK_XML: &str = r#"{"advisory":{"id":"RUSTSEC-2026-0195"},"package":{"name":"quick-xml","version":"0.39.4"}}"#;
    const PROC_MACRO: &str = r#"{"kind":"unmaintained","advisory":{"id":"RUSTSEC-2026-0173"},"package":{"name":"proc-macro-error2","version":"2.0.1"}}"#;
    const YANKED: &str =
        r#"{"kind":"yanked","advisory":null,"package":{"name":"gone","version":"1.0.0"}}"#;

    fn doc(vulns: &[&str], warnings: &str) -> String {
        format!(
            r#"{{"vulnerabilities":{{"found":true,"count":{n},"list":[{list}]}},"warnings":{{{warnings}}}}}"#,
            n = vulns.len(),
            list = vulns.join(",")
        )
    }

    fn ids(report: &[Finding]) -> Vec<&str> {
        report.iter().map(|f| f.id.as_str()).collect()
    }

    #[test]
    fn an_advisory_on_the_list_is_accepted() {
        let found = findings(&doc(&[QUICK_XML], "")).unwrap();
        let report = evaluate(&["RUSTSEC-2026-0195".into()], &found);
        assert_eq!(ids(&report.accepted), ["RUSTSEC-2026-0195"]);
        assert!(report.passes());
        assert_eq!(
            report.lines(),
            ["accepted   RUSTSEC-2026-0195 quick-xml 0.39.4 (vulnerability)"]
        );
    }

    #[test]
    fn an_advisory_off_the_list_is_unaccepted() {
        let found = findings(&doc(&[QUICK_XML], "")).unwrap();
        let report = evaluate(&[], &found);
        assert_eq!(ids(&report.unaccepted), ["RUSTSEC-2026-0195"]);
        assert!(!report.passes());
        assert_eq!(
            report.lines(),
            ["UNACCEPTED RUSTSEC-2026-0195 quick-xml 0.39.4 (vulnerability)"]
        );
    }

    #[test]
    fn an_exception_nothing_matches_is_stale() {
        let found = findings(&doc(&[QUICK_XML], "")).unwrap();
        let report = evaluate(
            &["RUSTSEC-2026-0195".into(), "RUSTSEC-2026-0441".into()],
            &found,
        );
        assert_eq!(report.stale, ["RUSTSEC-2026-0441"]);
        assert!(!report.passes());
        assert!(
            report.lines()[0]
                .starts_with("STALE      RUSTSEC-2026-0441 (nothing in Cargo.lock matches)")
        );
    }

    #[test]
    fn a_warning_with_an_advisory_id_counts() {
        let warnings = format!(r#""unmaintained":[{PROC_MACRO}]"#);
        let found = findings(&doc(&[], &warnings)).unwrap();
        assert_eq!(
            found,
            [Finding {
                id: "RUSTSEC-2026-0173".into(),
                package: "proc-macro-error2".into(),
                version: "2.0.1".into(),
                kind: "unmaintained".into(),
            }]
        );
    }

    #[test]
    fn a_yanked_crate_is_not_a_finding() {
        let warnings = format!(r#""yanked":[{YANKED}],"unmaintained":[{PROC_MACRO}]"#);
        let found = findings(&doc(&[], &warnings)).unwrap();
        assert_eq!(ids(&found), ["RUSTSEC-2026-0173"]);
    }

    #[test]
    fn the_ignore_list_is_read_past_its_comments() {
        let text = r#"
# header
[advisories]
ignore = [
    # why this stays
    "RUSTSEC-2026-0194",
    "RUSTSEC-2026-0195", # trailing
]
"#;
        assert_eq!(
            accepted(text).unwrap(),
            ["RUSTSEC-2026-0194", "RUSTSEC-2026-0195"]
        );
        assert!(accepted("").unwrap().is_empty());
    }

    #[test]
    fn a_clean_lock_with_no_exceptions_passes() {
        let found = findings(&doc(&[], "")).unwrap();
        assert!(found.is_empty());
        let report = evaluate(&[], &found);
        assert!(report.passes());
        assert!(report.lines().is_empty());
    }

    #[test]
    fn unparsable_output_is_an_error() {
        assert!(findings("not json").is_err());
    }

    #[test]
    fn output_without_the_vulnerabilities_list_is_an_error() {
        // A blocking gate must not pass on output it can't read: cargo audit always writes the list.
        assert!(findings("{}").is_err());
        assert!(findings(r#"{"warnings": {}}"#).is_err());
    }
}
