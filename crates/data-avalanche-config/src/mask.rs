//! A config generation's text with its secrets masked in place (Admin_Console §4 Config,
//! D-M13b.10): the console shows a generation's text, and its diff, without the secrets written
//! in it. A secret written as exactly one `${VAR}` reference stays as written: the text holds no
//! value. A default (`${VAR:-…}`) or text around a reference is masked like a literal.
//! A literal one becomes `"•••••• blake3:<8 hex>"`, the start of its fingerprint under the
//! cluster's key (config §6.2), so that two generations show whether it changed. A URL written
//! with credentials, under any key (`prometheus.url: https://user:pass@prom`), shows them as
//! `••••••@`, in the text and in the diff. Every comment is shown empty, whatever it holds (an
//! old secret, say): a full-line one as its indentation and `#`, a trailing one as ` #` after the
//! value. Everything else (order, spacing) is left as written.
//!
//! A comment is told from a `#` that is content (in a quoted or a block scalar) by what the text
//! parses to: a comment's text cut leaves it the same, content's doesn't. So every `#` at a line's
//! start or after a space or tab is tried, and a cut that changes nothing is kept; a text whose
//! value the cuts would change isn't shown ([`MaskError::Comments`]).
//!
//! The masked text is parsed again and checked before it is returned: every secret is then a
//! mask or a reference, and no URL shows its credentials. A text that fails the check isn't
//! shown at all ([`MaskError`]). A secret's text that is also written as another key's value (a
//! `user` that is the `password`) stays shown there: only the secret keys are masked. A text with
//! a merge key (`<<`), a tag or a directive isn't shown ([`MaskError::MergeKey`]): a key that
//! overrides or retags a value would hide the secret the masker reads. [`diff_written`] shows no
//! text, so it needs no such refusal.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use thiserror::Error;

use crate::diff::is_fixed;
use crate::keys::key_mutability;
use crate::leaf::Loc;
use crate::mutability::Mutability;
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
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum MaskError {
    #[error("the text doesn't parse as a config")]
    Parse,
    #[error("the secret at {0} couldn't be placed in the text")]
    Unplaced(String),
    #[error("the secret at {0} is still readable after masking")]
    Leaked(String),
    #[error("the text uses a YAML merge key, a tag or a directive, which can't be masked safely")]
    MergeKey,
    #[error("the text's comments can't be told from its content")]
    Comments,
    #[error("the masking didn't finish")]
    Interrupted,
}

/// `text` with every secret masked in place, fingerprinted with `key` (the module's doc).
///
/// # Errors
/// [`MaskError`]: the text isn't shown.
pub fn mask_secrets(text: &str, key: &FingerprintKey) -> Result<String, MaskError> {
    let all = leaves(text)?;
    let secrets: Vec<Written> = all.iter().filter(|w| is_secret(w)).cloned().collect();
    if unmaskable(text) {
        return Err(MaskError::MergeKey);
    }
    let mut edits: Vec<(usize, usize, String)> = vec![];
    for w in &all {
        let Some(value) = literal(&w.text) else {
            continue;
        };
        let with = if is_secret(w) {
            format!("\"{}\"", masked(key, &value))
        } else if let Some(url) = without_userinfo(&value) {
            serde_json::to_string(&url).map_err(|_| MaskError::Unplaced(w.path.join(".")))?
        } else {
            continue;
        };
        let Some(at) = w.at else {
            return Err(MaskError::Unplaced(w.path.join(".")));
        };
        let (start, end) =
            span(text, at, &w.text).ok_or_else(|| MaskError::Unplaced(w.path.join(".")))?;
        if edits.iter().any(|e| e.0 == start) {
            continue; // one value under `defaults`, or one anchor, read by several pipelines
        }
        edits.push((start, end, with));
    }
    edits.sort_by_key(|e| std::cmp::Reverse(e.0));
    let mut out = text.to_owned();
    for (start, end, with) in edits {
        out.replace_range(start..end, &with);
    }
    let out = blank_comments(&out)?;
    check(&secrets, &out)?;
    Ok(out)
}

/// What `text` parses to, as the comments are told by: `None` when it doesn't.
fn value_of(text: &str) -> Option<serde_json::Value> {
    parse_document(text).ok()
}

/// A comment's text to cut: from `from` to `to`, replaced by `with` (`#`, or ` #` after a value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cut {
    from: usize,
    to: usize,
    trailing: bool,
}

/// Where a comment could start, line by line: each `#` at a line's start or after a space or tab,
/// to its line's end (a CR or an LF: YAML takes either as a break). Only those start one.
fn cuts(text: &str) -> Vec<Vec<Cut>> {
    let bytes = text.as_bytes();
    let mut lines: Vec<Vec<Cut>> = vec![];
    let mut line_start = 0;
    let mut current: Vec<Cut> = vec![];
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'\n' | b'\r' => {
                if !current.is_empty() {
                    lines.push(std::mem::take(&mut current));
                }
                line_start = i + 1;
            }
            b'#' if i == line_start || matches!(bytes[i - 1], b' ' | b'\t') => {
                let to = text[i..].find(['\n', '\r']).map_or(text.len(), |n| i + n);
                let before = &text[line_start..i];
                let trailing = !before.bytes().all(|c| c == b' ' || c == b'\t');
                let from = if trailing {
                    line_start + before.trim_end_matches([' ', '\t']).len()
                } else {
                    i
                };
                current.push(Cut { from, to, trailing });
            }
            _ => {}
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

/// `text` with `cuts` (in order, apart) made.
fn cut(text: &str, cuts: &[Cut]) -> String {
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    for c in cuts {
        out.push_str(&text[at..c.from]);
        out.push_str(if c.trailing { " #" } else { "#" });
        at = c.to;
    }
    out.push_str(&text[at..]);
    out
}

/// `text` with every comment shown empty (the module's doc).
fn blank_comments(text: &str) -> Result<String, MaskError> {
    blank_comments_counting(text, &mut 0)
}

/// [`blank_comments`], counting its parses in `parses`. Each line's first `#` is tried, all at
/// once; a set whose cuts change the text's value is split in halves and each tried again (with
/// the cuts kept so far), down to the one `#` that is content. A line whose `#` was content tries
/// its next one in the next round. So a few `#`s that are content among `n` comments cost about
/// `2 log2 n` parses each, not one per comment. The text with every kept cut is checked once more:
/// a value changed refuses it.
fn blank_comments_counting(text: &str, parses: &mut usize) -> Result<String, MaskError> {
    let lines = cuts(text);
    if lines.is_empty() {
        return Ok(text.to_owned());
    }
    *parses += 1;
    let want = value_of(text).ok_or(MaskError::Comments)?;
    let mut same = |cuts: &[Cut]| {
        let mut cuts = cuts.to_vec();
        cuts.sort_by_key(|c| c.from);
        *parses += 1;
        value_of(&cut(text, &cuts)).as_ref() == Some(&want)
    };
    let mut kept: Vec<Cut> = vec![];
    // (line, which of its `#`s) still to try.
    let mut round: Vec<(usize, usize)> = (0..lines.len()).map(|l| (l, 0)).collect();
    while !round.is_empty() {
        let mut content = vec![];
        settle(&round, &lines, &mut kept, &mut content, &mut same);
        round = content
            .into_iter()
            .filter(|(l, i)| i + 1 < lines[*l].len())
            .map(|(l, i)| (l, i + 1))
            .collect();
    }
    kept.sort_by_key(|c| c.from);
    let out = cut(text, &kept);
    *parses += 1;
    if value_of(&out).as_ref() != Some(&want) {
        return Err(MaskError::Comments);
    }
    Ok(out)
}

/// Tries the cuts of `set` beside `kept`: all kept when the value stays, else each half in turn;
/// a single cut that changes it is `content`.
fn settle(
    set: &[(usize, usize)],
    lines: &[Vec<Cut>],
    kept: &mut Vec<Cut>,
    content: &mut Vec<(usize, usize)>,
    same: &mut impl FnMut(&[Cut]) -> bool,
) {
    let cuts: Vec<Cut> = set.iter().map(|(l, i)| lines[*l][*i]).collect();
    let mut with = kept.clone();
    with.extend(&cuts);
    if same(&with) {
        kept.extend(cuts);
        return;
    }
    if let [one] = set {
        content.push(*one);
        return;
    }
    let (a, b) = set.split_at(set.len() / 2);
    settle(a, lines, kept, content, same);
    settle(b, lines, kept, content, same);
}

/// One value that differs between two config texts, as written, its secrets masked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WrittenChange {
    /// Dotted, a pipeline by its `id` (`pipelines.orders.source.port`); a list's element as
    /// `[i]` (`tables.include[0]`).
    pub path: String,
    /// What a reload of the change does: `in_place`, `tables` (a pipeline restart when the
    /// resolved table set changes), `pipeline_restart`, `node_restart`, or `fixed` (refused).
    pub class: &'static str,
    /// `None` where the value isn't written.
    pub before: Option<String>,
    pub after: Option<String>,
}

/// What differs between two config texts as written ([`diff_written`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct WrittenDiff {
    /// Pipelines by `id`.
    pub added: Vec<String>,
    pub removed: Vec<String>,
    /// Outside the pipelines added and removed.
    pub changes: Vec<WrittenChange>,
}

/// What differs between `from` and `to`, both parsed without the environment and compared value
/// by value as written (Admin_Console §4 Config, D-M13b.11), secrets shown as [`mask_secrets`]
/// shows them. A `${VAR}` whose value changed isn't a change here: the text is the same.
///
/// Unlike [`mask_secrets`], it doesn't refuse a text with a merge key, a tag, a directive or a
/// byte-order mark: it shows no text, only each leaf as parsed, by its path, and a secret key's
/// leaf is masked there whatever wrote it (a merge, a tag, an anchor). Comments aren't leaves.
///
/// # Errors
/// [`MaskError::Parse`]: one of them isn't a config.
pub fn diff_written(from: &str, to: &str, key: &FingerprintKey) -> Result<WrittenDiff, MaskError> {
    let (before, before_ids) = flatten(from, key)?;
    let (after, after_ids) = flatten(to, key)?;
    let pipeline = |path: &str| {
        path.strip_prefix("pipelines.")
            .and_then(|rest| rest.split('.').next())
            .map(str::to_owned)
    };
    let added: Vec<String> = after_ids.difference(&before_ids).cloned().collect();
    let removed: Vec<String> = before_ids.difference(&after_ids).cloned().collect();
    let paths: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    let changes = paths
        .into_iter()
        .filter(|p| pipeline(p).is_none_or(|id| !added.contains(&id) && !removed.contains(&id)))
        .filter(|p| before.get(*p).map(|l| &l.written) != after.get(*p).map(|l| &l.written))
        .map(|p| WrittenChange {
            path: p.clone(),
            class: class_of(p),
            before: before.get(p).map(|l| l.shown.clone()),
            after: after.get(p).map(|l| l.shown.clone()),
        })
        .collect();
    Ok(WrittenDiff {
        added,
        removed,
        changes,
    })
}

/// A written path's reload class: its key's, the pipeline's id read as `*` (as the keys are
/// spelled, a renamed one included), a `defaults` key as the pipelines'.
fn class_of(path: &str) -> &'static str {
    let mut segments: Vec<&str> = path
        .split('.')
        .map(|s| s.split('[').next().unwrap_or(s))
        .collect();
    match segments.first() {
        Some(&"defaults") => {
            segments.splice(0..1, ["pipelines", "*"]);
        }
        Some(&"pipelines") if segments.len() > 1 => segments[1] = "*",
        _ => {}
    }
    let key = segments.join(".");
    if is_fixed(&key) {
        return "fixed";
    }
    match key_mutability(&key) {
        Some(Mutability::R) => "in_place",
        Some(Mutability::PrIfTableSetChanges) => "tables",
        Some(Mutability::Pr) => "pipeline_restart",
        Some(Mutability::Proc) | None => "node_restart",
    }
}

/// Every leaf of `text` by its dotted path (a pipeline by its `id`), its secrets masked, and
/// the pipelines' ids.
fn flatten(
    text: &str,
    key: &FingerprintKey,
) -> Result<(BTreeMap<String, Leaf>, BTreeSet<String>), MaskError> {
    let raw: RawConfig = parse_document(text).map_err(|_| MaskError::Parse)?;
    let mut all = vec![];
    raw.globals.leaves(&["globals".to_owned()], &mut all);
    raw.defaults.leaves(&["defaults".to_owned()], &mut all);
    let mut ids = BTreeSet::new();
    for (name, p) in raw.pipelines.iter().flatten() {
        let id =
            p.id.as_ref()
                .map_or_else(|| name.clone(), |l| l.text().to_owned());
        Some(p.clone()).leaves(&under(&["pipelines".to_owned()], &id), &mut all);
        ids.insert(id);
    }
    let mut out = BTreeMap::new();
    for w in all {
        let shown = match literal(&w.text) {
            Some(value) if is_secret(&w) => masked(key, &value),
            Some(value) => without_userinfo(&value).unwrap_or_else(|| w.text.clone()),
            None => w.text.clone(),
        };
        let mut path = String::new();
        for s in &w.path {
            if !s.starts_with('[') && !path.is_empty() {
                path.push('.');
            }
            path.push_str(s);
        }
        out.insert(
            path,
            Leaf {
                shown,
                written: w.text,
            },
        );
    }
    Ok((out, ids))
}

/// A leaf as [`diff_written`] compares it, as written, and shows it, masked: a URL whose
/// credentials alone changed is a change, shown with them masked.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Leaf {
    shown: String,
    written: String,
}

/// The credentials of a URL written as `scheme://user:pass@host…`: the authority's part before
/// its last `@`.
fn userinfo(value: &str) -> Option<&str> {
    let (scheme, rest) = value.split_once("://")?;
    let mut chars = scheme.chars();
    if !chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        || !chars.all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
    {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    authority.rfind('@').map(|at| &authority[..at])
}

/// `value`, a URL with credentials, with them masked (`https://••••••@host`); `None` when it
/// has none, or they are masked already.
fn without_userinfo(value: &str) -> Option<String> {
    let user = userinfo(value).filter(|u| *u != MASK)?;
    let at = value.find("://")? + 3;
    Some(format!(
        "{}{MASK}{}",
        &value[..at],
        &value[at + user.len()..]
    ))
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
///
/// The `!` rule rests on the YAML parser's separators being space, tab, CR, LF and NUL (NUL
/// fails the parse); a byte-order mark is the only other character it skips, so any BOM refuses
/// the text, and with no BOM every tag starts after a line start, whitespace or `[{,:?-`.
fn unmaskable(text: &str) -> bool {
    text.contains("<<")
        || text.contains('\u{FEFF}')
        || text.lines().any(|line| line.starts_with('%'))
        || text.char_indices().any(|(at, c)| {
            c == '!'
                && text[..at]
                    .chars()
                    .last()
                    .is_none_or(|p| p.is_whitespace() || "[{,:?-".contains(p))
        })
}

/// Every leaf written in `text`, a pipeline's under its key.
fn leaves(text: &str) -> Result<Vec<Written>, MaskError> {
    let raw: RawConfig = parse_document(text).map_err(|_| MaskError::Parse)?;
    let mut all = vec![];
    raw.globals.leaves(&["globals".to_owned()], &mut all);
    raw.defaults.leaves(&["defaults".to_owned()], &mut all);
    for (name, p) in raw.pipelines.iter().flatten() {
        Some(p.clone()).leaves(&under(&["pipelines".to_owned()], name), &mut all);
    }
    Ok(all)
}

/// Every secret leaf written in `text`.
fn secrets(text: &str) -> Result<Vec<Written>, MaskError> {
    let mut all = leaves(text)?;
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
    if let Some(w) = leaves(masked)?
        .iter()
        .find(|w| literal(&w.text).is_some_and(|v| without_userinfo(&v).is_some()))
    {
        return Err(MaskError::Leaked(w.path.join(".")));
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
    /// as written, but for the comments, shown empty.
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
            "config_version: 1\n#\nglobals:",
            "    password: \"•••••• blake3:",
            "\" #\n    tls:",
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

    /// Every comment is shown empty, whatever it holds: a full-line one keeps its indentation and
    /// `#`, a trailing one is ` #` after the value. A `#` that is content stays: in a quoted
    /// scalar, in a block scalar, or not after whitespace in a plain one.
    #[test]
    fn every_comment_is_shown_empty() {
        let text = "\
# the cluster's config, old-secret rotated
config_version: 1
globals:
  api:
    tls:
      enabled: false
      cert: |
        # x
        -----BEGIN CERTIFICATE-----
pipelines:
  p:
    source:
      # password: old-secret
      user: \"a # b\"   # was: old-secret
      host: db#1 # old-secret
      password: \"a # b\"
  q:
    source:
      password: |
        # x
        hunter2
      user: 'it''s # here' #old-secret
";
        let out = mask_secrets(text, &key()).unwrap();
        assert!(!out.contains("old-secret"), "{out}");
        assert!(!out.contains("hunter2"), "{out}");
        assert!(!out.contains("a # b\"\n"), "the secret is masked: {out}");
        assert_eq!(out.matches(MASK).count(), 2, "{out}");
        for kept in [
            "#\nconfig_version: 1\n",
            "      cert: |\n        # x\n        -----BEGIN CERTIFICATE-----\n",
            "\n      #\n      user: \"a # b\" #\n",
            "      host: db#1 #\n",
            "      user: 'it''s # here' #\n",
        ] {
            assert!(out.contains(kept), "{kept:?} is missing from:\n{out}");
        }
    }

    /// A `#` that is content among hundreds of comments costs a few parses, not one per
    /// comment: the cuts are tried in halves, and only a half holding a content `#` is split.
    #[test]
    fn a_content_hash_among_many_comments_costs_a_few_parses() {
        let mut text = String::from(
            "config_version: 1\npipelines:\n  p:\n    source:\n      user: \"a # b\"\n",
        );
        for i in 0..400 {
            text.push_str(&format!("      # note {i:03}: rotated old-secret here\n"));
        }
        text.push_str("      host: db # old-secret\n");
        assert!(text.len() > 12_000, "{}", text.len());
        let mut parses = 0;
        let out = blank_comments_counting(&text, &mut parses).unwrap();
        assert!(!out.contains("old-secret"), "{out}");
        assert!(out.contains("      user: \"a # b\"\n      #\n"), "{out}");
        assert!(out.ends_with("      host: db #\n"), "{out}");
        assert!(parses <= 30, "{parses} parses");
    }

    /// A comment ends at a line break, a CR alone included.
    #[test]
    fn a_comment_ends_at_any_line_break() {
        for nl in ["\r\n", "\r"] {
            let text = format!(
                "config_version: 1{nl}# old-secret{nl}pipelines:{nl}  p:{nl}    source:{nl}      user: u # old-secret{nl}      password: ${{X}}{nl}"
            );
            let out = mask_secrets(&text, &key()).unwrap();
            assert!(!out.contains("old-secret"), "{out:?}");
            assert_eq!(out, text.replace("# old-secret", "#"), "{out:?}");
        }
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

    /// Two generations' differences, by path and class, secrets masked; a pipeline added is
    /// named, not its every key.
    #[test]
    fn a_rotated_literal_secret_shows_only_its_fingerprint() {
        let from = "\
config_version: 1
globals:
  cluster: { tokens: 128, drain_timeout: 45s }
pipelines:
  a:
    id: orders
    source: { port: 5432, password: first-secret }
    tables: { include: [public.orders] }
";
        let to = "\
config_version: 1
globals:
  cluster: { tokens: 256, drain_timeout: 40s }
pipelines:
  a:
    id: orders
    source: { port: 5432, password: second-secret }
    tables: { include: [public.orders, public.lines] }
  b:
    id: users
    source: { port: 5433 }
";
        let d = diff_written(from, to, &key()).unwrap();
        assert_eq!((d.added, d.removed), (vec!["users".to_owned()], vec![]));
        let got: Vec<(&str, &str, Option<&str>, Option<&str>)> = d
            .changes
            .iter()
            .map(|c| {
                (
                    c.path.as_str(),
                    c.class,
                    c.before.as_deref(),
                    c.after.as_deref(),
                )
            })
            .collect();
        let first = masked(&key(), "first-secret");
        let second = masked(&key(), "second-secret");
        assert_eq!(
            got,
            [
                (
                    "globals.cluster.drain_timeout",
                    "in_place",
                    Some("45s"),
                    Some("40s")
                ),
                ("globals.cluster.tokens", "fixed", Some("128"), Some("256")),
                (
                    "pipelines.orders.source.password",
                    "pipeline_restart",
                    Some(first.as_str()),
                    Some(second.as_str())
                ),
                (
                    "pipelines.orders.tables.include[1]",
                    "tables",
                    None,
                    Some("public.lines")
                ),
            ]
        );
    }

    /// A change's class is its key's, whatever the pipeline's id: a key the resolved config
    /// spells otherwise (the Delta catalog's `type`) included, a `defaults` key as the pipelines'.
    #[test]
    fn a_change_is_classed_by_its_key_whatever_the_pipeline_s_id() {
        assert_eq!(
            class_of("pipelines.orders.sink.delta.catalog.type"),
            class_of("pipelines.*.sink.delta.catalog.type")
        );
        assert_eq!(
            Some(class_of("pipelines.orders.sink.delta.catalog.type")),
            key_mutability("pipelines.*.sink.delta.catalog.type").map(|m| match m {
                Mutability::R => "in_place",
                Mutability::PrIfTableSetChanges => "tables",
                Mutability::Pr => "pipeline_restart",
                Mutability::Proc => "node_restart",
            })
        );
        assert_ne!(
            class_of("pipelines.orders.sink.delta.catalog.type"),
            "node_restart"
        );
        assert_eq!(
            class_of("pipelines.orders.sink.iceberg.catalog.rest.token_file"),
            class_of("pipelines.*.sink.iceberg.catalog.rest.token_file")
        );
        assert_eq!(
            class_of("defaults.source.port"),
            class_of("pipelines.orders.source.port")
        );
    }

    /// A pipeline removed is named, not its every key; a `defaults` change is classed as the
    /// pipelines' key it applies to.
    #[test]
    fn a_removed_pipeline_is_named_and_a_defaults_change_classed() {
        let from = "config_version: 1\ndefaults:\n  source: { port: 5432 }\npipelines:\n  a: { source: { host: h } }\n  b: { source: { host: h } }\n";
        let to = "config_version: 1\ndefaults:\n  source: { port: 5433 }\npipelines:\n  a: { source: { host: h } }\n";
        let d = diff_written(from, to, &key()).unwrap();
        assert_eq!((d.added, d.removed), (vec![], vec!["b".to_owned()]));
        assert_eq!(
            d.changes,
            [WrittenChange {
                path: "defaults.source.port".into(),
                class: class_of("pipelines.*.source.port"),
                before: Some("5432".into()),
                after: Some("5433".into()),
            }]
        );
    }

    /// A URL's credentials are masked wherever it is written, in the text and in the diff:
    /// `user:pass@` becomes `••••••@`; a URL without them stays as written.
    #[test]
    fn a_url_s_credentials_are_masked() {
        let text = |pass: &str| {
            format!(
                "config_version: 1
globals:
  api:
    jwt: {{ issuer: i, jwks_url: 'https://k:{pass}@idp.example/keys' }}
  console:
    prometheus:
      url: https://prom:{pass}@prom.example:9090/api # old
pipelines:
  p:
    sink:
      iceberg:
        catalog:
          rest: {{ url: \"http://u:{pass}@rest:8181\" }}
          hive: {{ url: thrift://hive:9083 }}
"
            )
        };
        let one = text("old-pass");
        let out = mask_secrets(&one, &key()).unwrap();
        assert!(!out.contains("old-pass"), "{out}");
        for kept in [
            "jwks_url: \"https://••••••@idp.example/keys\" }",
            "      url: \"https://••••••@prom.example:9090/api\" #\n",
            "rest: { url: \"http://••••••@rest:8181\" }",
            "hive: { url: thrift://hive:9083 }",
        ] {
            assert!(out.contains(kept), "{kept:?} is missing from:\n{out}");
        }
        let d = diff_written(&one, &text("new-pass"), &key()).unwrap();
        let shown = format!("{d:?}");
        assert!(!shown.contains("-pass"), "{shown}");
        let prom = d
            .changes
            .iter()
            .find(|c| c.path == "globals.console.prometheus.url")
            .expect("a credential's change is a change");
        assert_eq!(
            prom.after.as_deref(),
            Some("https://••••••@prom.example:9090/api")
        );
        assert_eq!(d.changes.len(), 3, "{shown}");
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
        let tail = "config_version: 1\npipelines:\n  p:\n    source:\n      password: ${X}\n";
        for text in [
            format!(
                "\u{FEFF}!!merge \"\\x3c\\x3c\": {{ pipelines: {{ p: {{ source: {{ password: hunter2 }} }} }} }}\n{tail}"
            ),
            format!(
                "# c\n\u{FEFF}!!merge \"\\x3c\\x3c\": {{ pipelines: {{ p: {{ source: {{ password: hunter2 }} }} }} }}\n{tail}"
            ),
            "\u{FEFF}config_version: 1\n".to_owned(),
        ] {
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
