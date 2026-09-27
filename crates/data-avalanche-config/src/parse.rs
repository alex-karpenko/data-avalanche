//! Phase 0 (config §2, §11): strict YAML 1.2 parse into the raw model.

use serde::de::DeserializeOwned;
use serde_saphyr::{DuplicateKeyPolicy, SnippetMode};

use crate::diag::{Diagnostic, Locus, PARSE, Severity};
use crate::leaf::Loc;

/// Parses one YAML document into `T` (the raw model, whose structs deny unknown fields).
///
/// Options: duplicate keys are an error; only the exact words `true`/`false` are booleans (YAML 1.2,
/// no `yes`/`on`); one document only. The first failure is returned as a [`PARSE`] diagnostic with
/// its line and column (boxed: a `Diagnostic` is large and parsing is a cold path).
pub fn parse_document<T: DeserializeOwned>(text: &str) -> Result<T, Box<Diagnostic>> {
    let options = serde_saphyr::options! {
        strict_booleans: true,
        duplicate_keys: DuplicateKeyPolicy::Error,
    };
    serde_saphyr::from_str_with_options(text, options).map_err(|e| {
        let loc = e.location().filter(|l| l.line() > 0).map(|l| Loc {
            line: l.line(),
            column: l.column(),
        });
        let rendered = e.render_with_options(serde_saphyr::render_options! {
            snippets: SnippetMode::Off,
        });
        Box::new(Diagnostic {
            locus: Locus::Config,
            path: String::new(),
            rule: PARSE.into(),
            severity: Severity::Error,
            loc,
            message: flow_template_hint(text, loc, rendered),
        })
    })
}

/// An unquoted `${VAR}` inside a flow collection (`{ port: ${P} }`) breaks the YAML, because `{` and
/// `}` are flow indicators there. The parser's own message ("did not find expected ',' or '}'")
/// doesn't say why, so add a hint when the failing line has both a template and a flow collection.
fn flow_template_hint(text: &str, loc: Option<Loc>, message: String) -> String {
    let line = loc.and_then(|l| text.lines().nth(l.line.saturating_sub(1) as usize));
    let flow_with_template =
        line.is_some_and(|l| l.contains("${") && l.replace("${", "").contains(['{', '[']));
    if flow_with_template {
        format!("{message} (hint: quote ${{...}} references inside flow collections: \"${{VAR}}\")")
    } else {
        message
    }
}
