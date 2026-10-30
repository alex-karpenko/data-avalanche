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

    // Every `config_version:` line of a page belongs to a collected example: one that is indented,
    // or sits in a fence of another language, would otherwise go unvalidated.
    let root = xtask::repo_root();
    let mut pages = Vec::new();
    pages_under(&root.join(xtask::CONTENT), &mut pages);
    let mut lines = 0;
    let mut uncollected = Vec::new();
    for page in pages {
        let file = page.strip_prefix(&root).unwrap().to_path_buf();
        let text = std::fs::read_to_string(&page).unwrap();
        for (i, line) in text.lines().enumerate() {
            if line.trim().starts_with("config_version:") {
                lines += 1;
                // An example is recorded at its fence's line; its first line follows.
                if !examples
                    .iter()
                    .any(|e| e.file == file && e.line + 1 == i + 1)
                {
                    uncollected.push(format!("{}:{}", file.display(), i + 1));
                }
            }
        }
    }
    assert_eq!(
        lines,
        examples.len(),
        "a config_version line outside a collected example (a yaml fence whose first line is \
         `config_version:` at column 0): {}",
        uncollected.join(", ")
    );
}

fn pages_under(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            pages_under(&path, out);
        } else if path.extension().is_some_and(|e| e == "md" || e == "mdx") {
            out.push(path);
        }
    }
}
