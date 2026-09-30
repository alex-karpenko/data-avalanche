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
pub(crate) fn parse_document<T: DeserializeOwned>(text: &str) -> Result<T, Box<Diagnostic>> {
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

#[cfg(test)]
mod tests {
    //! Phase 0 behaviour the loader relies on (roadmap R-9: serde-saphyr strictness and locations).
    //! Moved from `tests/parse.rs` (FU-E): `Leaf`, `Tri`, `leaf::tri` and `parse_document` are
    //! crate-internal.

    use serde::Deserialize;

    use super::*;
    use crate::leaf::{Leaf, Tri, tri};

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Doc {
        source: Option<Source>,
        flush: Option<Flush>,
        include: Option<Vec<Leaf<String>>>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Source {
        host: Option<Leaf<String>>,
        port: Option<Leaf<u16>>,
        password: Option<Leaf<String>>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Flush {
        #[serde(default, deserialize_with = "tri")]
        seconds: Tri<u64>,
        #[serde(default, deserialize_with = "tri")]
        rows: Tri<u64>,
    }

    fn err(text: &str) -> Diagnostic {
        *parse_document::<Doc>(text).unwrap_err()
    }

    #[test]
    fn keeps_scalar_spelling_and_location() {
        let doc: Doc =
            parse_document("source:\n  port: 5432\n  password: 0123\n  host: ${DB}-x\n").unwrap();
        let s = doc.source.unwrap();
        let port = s.port.unwrap();
        assert_eq!(port.text(), "5432");
        assert_eq!(port.loc(), Some(Loc { line: 2, column: 9 }));
        // A leading zero survives: a numeric-looking password isn't re-printed as a number.
        assert_eq!(s.password.unwrap().text(), "0123");
        assert_eq!(s.host.unwrap().text(), "${DB}-x");
    }

    #[test]
    fn quoted_template_in_flow_mapping_is_text() {
        let doc: Doc = parse_document("source: { port: \"${PG_PORT}\" }\n").unwrap();
        assert_eq!(doc.source.unwrap().port.unwrap().text(), "${PG_PORT}");
    }

    #[test]
    fn unquoted_template_in_flow_mapping_fails_with_a_hint() {
        let e = err("source: { port: ${PG_PORT} }\n");
        assert_eq!(e.rule, PARSE);
        assert!(e.message.contains("quote"), "{}", e.message);
    }

    #[test]
    fn distinguishes_absent_null_and_value() {
        let doc: Doc = parse_document("flush:\n  seconds: null\n").unwrap();
        let f = doc.flush.unwrap();
        assert!(matches!(f.seconds, Some(None)), "explicit null disables");
        assert!(f.rows.is_none(), "absent means default");
        let doc: Doc = parse_document("flush:\n  rows: ~\n  seconds: 30\n").unwrap();
        let f = doc.flush.unwrap();
        assert!(matches!(f.rows, Some(None)));
        assert_eq!(f.seconds.unwrap().unwrap().text(), "30");
    }

    #[test]
    fn unknown_key_is_rejected_with_its_location() {
        let e = err("source:\n  port: 1\n  hots: db\n");
        assert_eq!(e.rule, PARSE);
        assert_eq!(e.loc, Some(Loc { line: 3, column: 3 }));
        assert!(e.message.contains("unknown field `hots`"), "{}", e.message);
    }

    #[test]
    fn duplicate_keys_several_documents_and_tabs_are_rejected() {
        let e = err("source:\n  port: 1\n  port: 2\n");
        assert!(e.message.contains("duplicate"), "{}", e.message);
        assert_eq!(e.loc, Some(Loc { line: 3, column: 3 }));
        let e = err("source: {port: 1}\n---\nsource: {port: 2}\n");
        assert!(e.message.contains("multiple"), "{}", e.message);
        let e = err("source:\n\tport: 1\n");
        assert!(e.message.contains("tab"), "{}", e.message);
    }

    #[test]
    fn null_list_element_and_wrong_shape_are_parse_errors() {
        assert_eq!(err("include: [a, ~]\n").rule, PARSE);
        assert_eq!(err("source:\n  port: {a: 1}\n").rule, PARSE);
    }

    #[test]
    fn empty_document_parses_to_nothing() {
        let doc: Doc = parse_document("").unwrap();
        assert!(doc.source.is_none() && doc.flush.is_none() && doc.include.is_none());
    }
}
