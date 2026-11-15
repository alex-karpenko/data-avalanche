//! A config generation's text with its secrets masked in place (Admin_Console §4 Config,
//! D-M13b.10): the console shows a generation's text, and its diff, without the secrets written
//! in it. A secret written as exactly one `${VAR}` reference stays as written: the text holds no
//! value. A default (`${VAR:-…}`) or text around a reference is masked like a literal.
//! A literal one becomes `"•••••• blake3:<8 hex>"`, the start of its fingerprint under the
//! cluster's key (config §6.2), so that two generations show whether it changed. Everything else
//! (comments, order, spacing) is left as written.
//!
//! The masked text is parsed again and checked before it is returned: every secret is then a
//! mask or a reference. A text that fails the check isn't shown at all ([`MaskError`]). A
//! secret's text that is also written as another key's value (a `user` that is the `password`)
//! stays shown there: only the secret keys are masked. A text with a merge key (`<<`), a tag or a
//! directive isn't shown ([`MaskError::MergeKey`]): a key that overrides or retags a value would
//! hide the secret the masker reads.

use thiserror::Error;

use crate::leaf::Loc;
use crate::parse::parse_document;
use crate::raw::RawConfig;
use crate::secret::{FingerprintKey, fingerprint};
use crate::template::{Expanded, expand, is_var_name};
use crate::walk::{Walk, Written, under};

/// What a masked literal secret starts with.
pub const MASK: &str = "••••••";

/// The keys whose values are secrets, a pipeline's spelled under `pipelines.*` (they apply
/// under `defaults` too). A `pem` key is a secret only as inline PEM; a path stays shown.
const SECRET_KEYS: &[(&str, Pem)] = &[
    ("globals.api.password", Pem::No),
    ("globals.api.oauth2.client_secret", Pem::No),
    ("globals.api.tls.key", Pem::Inline),
    ("globals.tracing.headers.*", Pem::No),
    ("globals.console.session_key", Pem::No),
    ("globals.console.prometheus.basic.password", Pem::No),
    ("globals.console.prometheus.bearer_token", Pem::No),
    ("globals.console.prometheus.headers.*", Pem::No),
    ("pipelines.*.source.password", Pem::No),
    ("pipelines.*.source.ssl_key", Pem::Inline),
    ("pipelines.*.sink.iceberg.catalog.hive.password", Pem::No),
    ("pipelines.*.sink.iceberg.catalog.rest.token", Pem::No),
    (
        "pipelines.*.sink.iceberg.catalog.rest.oauth2.client_secret",
        Pem::No,
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pem {
    No,
    Inline,
}

/// Why a text can't be shown masked.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MaskError {
    #[error("the text doesn't parse as a config")]
    Parse,
    #[error("the secret at {0} couldn't be placed in the text")]
    Unplaced(String),
    #[error("the secret at {0} is still readable after masking")]
    Leaked(String),
    #[error("the text uses a YAML merge key, a tag or a directive, which can't be masked safely")]
    MergeKey,
}

/// `text` with every secret masked in place, fingerprinted with `key` (the module's doc).
///
/// # Errors
/// [`MaskError`]: the text isn't shown.
pub fn mask_secrets(text: &str, key: &FingerprintKey) -> Result<String, MaskError> {
    let secrets = secrets(text)?;
    if unmaskable(text) {
        return Err(MaskError::MergeKey);
    }
    let mut edits: Vec<(usize, usize, String)> = vec![];
    for s in &secrets {
        let Some(value) = literal(&s.text) else {
            continue;
        };
        let Some(at) = s.at else {
            return Err(MaskError::Unplaced(s.path.join(".")));
        };
        let (start, end) =
            span(text, at, &s.text).ok_or_else(|| MaskError::Unplaced(s.path.join(".")))?;
        if edits.iter().any(|e| e.0 == start) {
            continue; // one value under `defaults`, or one anchor, read by several pipelines
        }
        edits.push((start, end, format!("\"{}\"", masked(key, &value))));
    }
    edits.sort_by_key(|e| std::cmp::Reverse(e.0));
    let mut out = text.to_owned();
    for (start, end, with) in edits {
        out.replace_range(start..end, &with);
    }
    check(&secrets, &out)?;
    Ok(out)
}

/// A literal secret's mask.
fn masked(key: &FingerprintKey, value: &str) -> String {
    let fp = fingerprint(key, value.as_bytes());
    let hex = fp.strip_prefix("blake3:").unwrap_or(&fp);
    format!("{MASK} blake3:{}", &hex[..8])
}

/// The value of a leaf written as `text`, or `None` when it is exactly one bare `${VAR}`
/// reference (no default, nothing around it). Anything else is a literal: its expansion when it
/// has no reference, else the raw text, which is what gets fingerprinted.
fn literal(text: &str) -> Option<String> {
    if let Some(name) = text.strip_prefix("${").and_then(|t| t.strip_suffix('}'))
        && is_var_name(name)
    {
        return None;
    }
    match expand(text, None) {
        Ok(Expanded::Text(t)) => Some(t),
        _ => Some(text.to_owned()),
    }
}

/// Whether `text` can merge or retag a value the masker reads as written: it contains `<<` (a
/// merge key, whatever is around it), a line starting with `%` (a directive), or a `!` that
/// starts a tag on a node (after the start of a line, whitespace, or one of `[ { , : ? -`). A
/// tag can make any scalar a merge key, and the spellings of a tag are many. Over-matching, a
/// `!` after a space in a quoted literal or a comment, fails closed.
fn unmaskable(text: &str) -> bool {
    text.contains("<<")
        || text.lines().any(|line| line.starts_with('%'))
        || text.char_indices().any(|(at, c)| {
            c == '!'
                && text[..at]
                    .chars()
                    .last()
                    .is_none_or(|p| p.is_whitespace() || "[{,:?-".contains(p))
        })
}

/// Every secret leaf written in `text`.
fn secrets(text: &str) -> Result<Vec<Written>, MaskError> {
    let raw: RawConfig = parse_document(text).map_err(|_| MaskError::Parse)?;
    let mut all = vec![];
    raw.globals.leaves(&["globals".to_owned()], &mut all);
    raw.defaults.leaves(&["defaults".to_owned()], &mut all);
    for (name, p) in raw.pipelines.iter().flatten() {
        Some(p.clone()).leaves(&under(&["pipelines".to_owned()], name), &mut all);
    }
    all.retain(is_secret);
    Ok(all)
}

fn is_secret(w: &Written) -> bool {
    let mut path: Vec<&str> = w.path.iter().map(String::as_str).collect();
    match path.first() {
        Some(&"defaults") => {
            path.splice(0..1, ["pipelines", "*"]);
        }
        Some(&"pipelines") if path.len() > 1 => path[1] = "*",
        _ => {}
    }
    SECRET_KEYS.iter().any(|(pattern, pem)| {
        let pattern: Vec<&str> = pattern.split('.').collect();
        pattern.len() == path.len()
            && pattern.iter().zip(&path).all(|(p, s)| *p == "*" || p == s)
            && (*pem == Pem::No || w.text.contains("-----BEGIN"))
    })
}

/// The masked text's own check (the module's doc).
fn check(before: &[Written], masked: &str) -> Result<(), MaskError> {
    let after = secrets(masked)?;
    for b in before {
        let path = b.path.join(".");
        let Some(a) = after.iter().find(|a| a.path == b.path) else {
            // An inline PEM key masked is a secret no more: it reads as a path.
            if b.text.contains("-----BEGIN") {
                continue;
            }
            return Err(MaskError::Leaked(path));
        };
        if literal(&b.text).is_some() && !a.text.starts_with(MASK) {
            return Err(MaskError::Leaked(path));
        }
    }
    if after
        .iter()
        .any(|a| literal(&a.text).is_some() && !a.text.starts_with(MASK))
    {
        return Err(MaskError::Leaked("a key".to_owned()));
    }
    Ok(())
}

/// The byte offset of `at` (1-based line, 1-based column in characters).
fn offset(text: &str, at: Loc) -> Option<usize> {
    let line_start = if at.line == 1 {
        0
    } else {
        text.match_indices('\n')
            .nth(usize::try_from(at.line).ok()? - 2)?
            .0
            + 1
    };
    let line = &text[line_start..];
    let column = usize::try_from(at.column).ok()?.checked_sub(1)?;
    let within = line
        .char_indices()
        .nth(column)
        .map(|(i, _)| i)
        .or_else(|| (line.chars().count() == column).then_some(line.len()))?;
    Some(line_start + within)
}

/// Where the scalar written at `at` is in `text`, as a byte range to replace: a quoted scalar
/// with its quotes, a block scalar from its `|` or `>` to its last line, a plain one without its
/// comment. `None` when what is there doesn't read as `written` (the leaf's text).
fn span(text: &str, at: Loc, written: &str) -> Option<(usize, usize)> {
    let start = offset(text, at)?;
    let rest = &text[start..];
    match rest.chars().next()? {
        '"' => {
            let mut escaped = false;
            for (i, c) in rest.char_indices().skip(1) {
                match c {
                    _ if escaped => escaped = false,
                    '\\' => escaped = true,
                    '"' => return Some((start, start + i + 1)),
                    _ => {}
                }
            }
            None
        }
        '\'' => {
            let mut chars = rest.char_indices().skip(1).peekable();
            while let Some((i, c)) = chars.next() {
                if c == '\'' {
                    if chars.peek().is_some_and(|(_, n)| *n == '\'') {
                        chars.next();
                    } else {
                        return Some((start, start + i + 1));
                    }
                }
            }
            None
        }
        _ => block(text, start).or_else(|| plain(text, start, written)),
    }
}

/// A block scalar whose first line starts at `start`: from its indicator to its last line.
fn block(text: &str, start: usize) -> Option<(usize, usize)> {
    let line_start = text[..start].rfind('\n').map_or(0, |i| i + 1);
    if !text[line_start..start].chars().all(|c| c == ' ') {
        return None;
    }
    let indent = start - line_start;
    // The header: the last line above with anything on it, its comment left out.
    let header = text[..line_start.checked_sub(1)?]
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())?;
    let header_end = text[..line_start].rfind(header)? + header.len();
    let body = header.split(" #").next().unwrap_or(header).trim_end();
    let indicator = body.trim_end_matches(|c: char| c.is_ascii_digit() || c == '-' || c == '+');
    if !(indicator.ends_with('|') || indicator.ends_with('>')) {
        return None;
    }
    let from = header_end - header.len() + indicator.len() - 1;
    let mut end = start;
    let mut at = line_start;
    for line in text[line_start..].split_inclusive('\n') {
        let content = line.trim_end_matches(['\n', '\r']);
        if !content.trim().is_empty() {
            let own = content.len() - content.trim_start_matches(' ').len();
            if own < indent {
                break;
            }
            end = at + content.len();
        }
        at += line.len();
    }
    Some((from, end))
}

/// A plain scalar starting at `start`, over as many lines as `written` folds.
fn plain(text: &str, start: usize, written: &str) -> Option<(usize, usize)> {
    let line_start = text[..start].rfind('\n').map_or(0, |i| i + 1);
    let opened = text[line_start..start].chars().fold(0i32, |n, c| match c {
        '{' | '[' => n + 1,
        '}' | ']' => n - 1,
        _ => n,
    });
    let first_end = |from: usize| {
        let line = text[from..].split('\n').next().unwrap_or("");
        let mut cut = line.len();
        if let Some(i) = line.find(" #") {
            cut = cut.min(i);
        }
        if opened > 0
            && let Some(i) = line.find([',', '}', ']'])
        {
            cut = cut.min(i);
        }
        from + line[..cut].trim_end().len()
    };
    let mut end = first_end(start);
    let mut read = text[start..end].to_owned();
    while read != written {
        if read.len() >= written.len() {
            return None;
        }
        let next_line = end + text[end..].find('\n')? + 1;
        let content_start =
            next_line + (text[next_line..].len() - text[next_line..].trim_start_matches(' ').len());
        end = first_end(content_start);
        read.push(' ');
        read.push_str(&text[content_start..end]);
    }
    Some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::config_keys;

    fn key() -> FingerprintKey {
        FingerprintKey::from_bytes([7; 32])
    }

    const TEXT: &str = "\
config_version: 1
# the cluster
globals:
  state: { bucket: app-state }
  api:
    auth: basic
    user: admin
    password: 'hunter2''s pass'   # rotate monthly
    tls:
      enabled: true
      cert: /etc/da/tls.crt
      key: |
        -----BEGIN PRIVATE KEY-----
        MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7
        -----END PRIVATE KEY-----
  tracing:
    headers: { authorization: \"Bearer abcdefgh12345\", x-tenant: \"${TENANT}\" }
  console:
    session_key: ${SESSION_KEY}
defaults:
  source:
    password: &pw plain-secret-value
pipelines:
  orders:
    source:
      password: *pw
      ssl_key: /etc/da/client.key
  users:
    source:
      password:
        a folded
        plain secret
";

    /// Every literal secret is replaced in place; a reference, a path and every other line stay
    /// as written, comments included.
    #[test]
    fn a_generations_secrets_are_masked_in_place() {
        let out = mask_secrets(TEXT, &key()).unwrap();
        for gone in [
            "hunter2",
            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7",
            "abcdefgh12345",
            "plain-secret-value",
            "folded",
        ] {
            assert!(!out.contains(gone), "{gone} is still in:\n{out}");
        }
        for kept in [
            "# the cluster",
            "    password: \"•••••• blake3:",
            "   # rotate monthly",
            "      key: \"•••••• blake3:",
            "x-tenant: \"${TENANT}\" }",
            "session_key: ${SESSION_KEY}",
            "password: &pw \"•••••• blake3:",
            "      password: *pw\n",
            "ssl_key: /etc/da/client.key",
        ] {
            assert!(out.contains(kept), "{kept} is missing from:\n{out}");
        }
        assert_eq!(out.matches(MASK).count(), 5, "{out}");
        assert!(
            out.contains("      password:\n        \"•••••• blake3:"),
            "the folded scalar's two lines are one mask:\n{out}"
        );
    }

    /// Two generations show whether a literal secret changed, and nothing more.
    #[test]
    fn two_masks_differ_only_where_the_secret_does() {
        let one =
            "config_version: 1\npipelines:\n  p:\n    source:\n      password: first-secret\n";
        let two = one.replace("first-secret", "second-secret");
        let (a, b) = (
            mask_secrets(one, &key()).unwrap(),
            mask_secrets(&two, &key()).unwrap(),
        );
        assert_ne!(a, b);
        assert_eq!(a, mask_secrets(one, &key()).unwrap());
        let shown = |t: &str| t.lines().last().unwrap().to_owned();
        assert_eq!(
            shown(&a),
            format!("      password: \"{}\"", masked(&key(), "first-secret"))
        );
        assert!(!b.contains("second-secret"));
    }

    /// Only the secret keys are masked: a `user` written as the password's text stays shown, and
    /// the text is still shown (the E2E's sources write `user: avalanche`, `password: avalanche`).
    #[test]
    fn a_password_that_is_also_the_user_is_masked_only_as_the_password() {
        let text = "config_version: 1\npipelines:\n  p:\n    source: { user: avalanche, password: avalanche }\n";
        let out = mask_secrets(text, &key()).unwrap();
        assert_eq!(
            out,
            format!(
                "config_version: 1\npipelines:\n  p:\n    source: {{ user: avalanche, password: \"{}\" }}\n",
                masked(&key(), "avalanche")
            )
        );
    }

    /// A text the masker can't place a secret in isn't shown.
    #[test]
    fn a_text_that_doesnt_parse_isnt_shown() {
        assert_eq!(mask_secrets("globals: [", &key()), Err(MaskError::Parse));
    }

    /// A default or text around a reference is a literal: masked. A bare reference stays.
    #[test]
    fn a_default_or_text_around_a_reference_is_masked() {
        for value in ["${DB_PASS:-hunter2}", "hunter2-${SUFFIX}"] {
            let text = format!(
                "config_version: 1\npipelines:\n  p:\n    source:\n      password: {value}\n"
            );
            let out = mask_secrets(&text, &key()).unwrap();
            assert!(!out.contains("hunter2"), "{out}");
            assert!(out.contains("password: \"•••••• blake3:"), "{out}");
        }
        let text = "config_version: 1\npipelines:\n  p:\n    source:\n      password: ${DB_PASS}\n";
        assert_eq!(mask_secrets(text, &key()).unwrap(), text);
    }

    /// A merge key, a tag or a directive can merge or retag a value the masker never sees: the
    /// text isn't shown.
    #[test]
    fn a_merge_key_tag_or_directive_is_refused() {
        let text = "config_version: 1\npipelines:\n  p:\n    source:\n      <<: { password: hunter2 }\n      password: ${X}\n";
        assert_eq!(mask_secrets(text, &key()), Err(MaskError::MergeKey));
        let flow = "config_version: 1\nglobals:\n  api: &s { user: u }\npipelines:\n  p:\n    source: { <<: *s, password: x }\n";
        assert_eq!(mask_secrets(flow, &key()), Err(MaskError::MergeKey));
        for shape in [
            "      !!merge <<: { password: hunter2 }\n      password: ${X}\n",
            "      &k <<: { password: hunter2 }\n      password: ${X}\n",
            "      ? <<\n      : { password: hunter2 }\n      password: ${X}\n",
            "      !!m%65rge \"\\x3c\\x3c\": { password: hunter2 }\n      password: ${X}\n",
            "      !<tag:yaml.org,2002:%6Derge> \"\\x3c\\x3c\": { password: hunter2 }\n      password: ${X}\n",
            "      password: !!str hunter2\n",
        ] {
            let text = format!("config_version: 1\npipelines:\n  p:\n    source:\n{shape}");
            assert_eq!(
                mask_secrets(&text, &key()),
                Err(MaskError::MergeKey),
                "{text}"
            );
        }
        for (directive, tag) in [
            ("%TAG !y! tag:yaml.org,2002:\n---\n", "!y!merge"),
            ("%TAG !y! tag:yaml.org,2002:mer\n---\n", "!y!ge"),
        ] {
            let text = format!(
                "{directive}config_version: 1\npipelines:\n  p:\n    source:\n      {tag} \"\\x3c\\x3c\": {{ password: hunter2 }}\n      password: ${{X}}\n"
            );
            assert_eq!(
                mask_secrets(&text, &key()),
                Err(MaskError::MergeKey),
                "{text}"
            );
        }
    }

    /// A `!` inside a plain scalar is text, not a tag: the value is masked.
    #[test]
    fn a_bang_inside_a_literal_is_not_a_tag() {
        let text = "config_version: 1\npipelines:\n  p:\n    source:\n      password: hunter2!x\n";
        let out = mask_secrets(text, &key()).unwrap();
        assert!(!out.contains("hunter2"), "{out}");
        assert!(out.contains(MASK), "{out}");
    }

    /// An inline key whose PEM sits under a header line is still a secret.
    #[test]
    fn an_inline_key_with_a_header_is_masked() {
        let text = "config_version: 1\nglobals:\n  api:\n    tls:\n      enabled: false\n      key: |\n        Bag Attributes\n        -----BEGIN PRIVATE KEY-----\n        MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n        -----END PRIVATE KEY-----\n";
        let out = mask_secrets(text, &key()).unwrap();
        assert!(!out.contains("MIIEvQ"), "{out}");
        assert!(!out.contains("Bag Attributes"), "{out}");
        assert_eq!(out.matches(MASK).count(), 1, "{out}");
    }

    /// The list follows the model: every key it names exists, and every key named like a
    /// secret is in it (a file's path or a token's URL isn't one).
    #[test]
    fn the_secret_keys_are_the_models() {
        let keys: Vec<String> = config_keys().into_iter().map(|k| k.path).collect();
        for (k, _) in SECRET_KEYS {
            assert!(keys.iter().any(|m| m == k), "{k} isn't a key");
        }
        let named: Vec<&String> = keys
            .iter()
            .filter(|k| {
                let last = k.rsplit('.').next().unwrap();
                matches!(
                    last,
                    "password"
                        | "token"
                        | "bearer_token"
                        | "client_secret"
                        | "session_key"
                        | "ssl_key"
                ) || k.ends_with("tls.key")
                    || k.ends_with("headers.*")
            })
            .collect();
        let listed: Vec<&str> = SECRET_KEYS.iter().map(|(k, _)| *k).collect();
        assert_eq!(named.len(), listed.len(), "{named:?}");
        assert!(
            named.iter().all(|k| listed.contains(&k.as_str())),
            "{named:?}"
        );
    }
}
