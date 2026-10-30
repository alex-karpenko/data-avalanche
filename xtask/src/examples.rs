//! The documentation's config examples: a fenced `yaml` block whose first line is
//! `config_version:` is a whole config file, and a test validates each one.

use std::path::{Path, PathBuf};

/// One example, where it sits, and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigExample {
    /// The page, from the repository's root.
    pub file: PathBuf,
    /// The line of the opening fence, from 1.
    pub line: usize,
    pub text: String,
}

/// Every config example in the site's pages (`.md` and `.mdx` under [`crate::CONTENT`]), in
/// path order.
pub fn config_examples(root: &Path) -> Vec<ConfigExample> {
    let mut pages = Vec::new();
    collect(&root.join(crate::CONTENT), &mut pages);
    pages.sort();
    let mut out = Vec::new();
    for page in pages {
        let text = std::fs::read_to_string(&page).expect("a page reads");
        let file = page
            .strip_prefix(root)
            .expect("under the root")
            .to_path_buf();
        let mut open: Option<(usize, String)> = None;
        for (i, line) in text.lines().enumerate() {
            match &mut open {
                None if line.trim_start().starts_with("```yaml") => {
                    open = Some((i + 1, String::new()))
                }
                Some((at, body)) if line.trim_start().starts_with("```") => {
                    if body.starts_with("config_version:") {
                        out.push(ConfigExample {
                            file: file.clone(),
                            line: *at,
                            text: body.clone(),
                        });
                    }
                    open = None;
                }
                Some((_, body)) => {
                    body.push_str(line);
                    body.push('\n');
                }
                None => {}
            }
        }
    }
    out
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.extension().is_some_and(|e| e == "md" || e == "mdx") {
            out.push(path);
        }
    }
}
