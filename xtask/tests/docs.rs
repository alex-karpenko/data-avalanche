//! The site's generated pages match the code, and its config examples are valid configs.

use std::path::{Path, PathBuf};

use data_avalanche_config::{Env, validate_str};
use rcgen::{CertifiedKey, generate_simple_self_signed};

#[test]
fn the_generated_pages_are_current() {
    let root = xtask::repo_root();
    let stale: Vec<_> = xtask::generated()
        .into_iter()
        .filter(|(path, text)| std::fs::read_to_string(root.join(path)).ok().as_ref() != Some(text))
        .map(|(path, _)| path)
        .collect();
    assert!(stale.is_empty(), "stale, run `cargo xtask docs`: {stale:?}");
}

/// The examples' `${VAR}`s: a throwaway certificate, key or CA for a name ending `_CERT`, `_KEY`
/// or `_CA`; a credentials file for `_CREDENTIALS`; an address for `_IP`; a stand-in text for the
/// rest.
struct Placeholders {
    pair: CertifiedKey<rcgen::KeyPair>,
    credentials: PathBuf,
}

impl Env for Placeholders {
    fn var(&self, name: &str) -> Option<String> {
        Some(if name.ends_with("_CERT") || name.ends_with("_CA") {
            self.pair.cert.pem()
        } else if name.ends_with("_KEY") {
            self.pair.signing_key.serialize_pem()
        } else if name.ends_with("_CREDENTIALS") {
            self.credentials.display().to_string()
        } else if name.ends_with("_IP") {
            "10.0.0.1".into()
        } else {
            format!("placeholder-{}", name.to_lowercase())
        })
    }
}

#[test]
fn every_config_example_in_the_docs_validates() {
    let credentials = Path::new(env!("CARGO_TARGET_TMPDIR")).join("docs-credentials.json");
    std::fs::write(
        &credentials,
        r#"{"user": "cdc", "password": "placeholder"}"#,
    )
    .unwrap();
    let env = Placeholders {
        pair: generate_simple_self_signed(["localhost".to_string()]).unwrap(),
        credentials,
    };
    let examples = xtask::config_examples(&xtask::repo_root());
    assert!(!examples.is_empty(), "the pages hold config examples");
    let mut wrong = Vec::new();
    for example in &examples {
        let report = validate_str(&example.text, &env);
        if !report.is_ok() {
            wrong.push(format!(
                "{}:{}: {:?}",
                example.file.display(),
                example.line,
                report
                    .errors
                    .iter()
                    .map(|e| format!("{} {}: {}", e.rule, e.path, e.message))
                    .collect::<Vec<_>>()
            ));
        }
    }
    assert!(wrong.is_empty(), "invalid examples:\n{}", wrong.join("\n"));
}
