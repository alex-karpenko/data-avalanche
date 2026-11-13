//! `cargo xtask deps`: every pinned dependency against its registry (1.0.1's rule: the newest
//! stable version of everything, or a written hold in `dependency-exceptions.yaml` with its reason,
//! checked again at every release). [`inventory`] lists what the repository pins, a [`Registry`]
//! says what is newest, [`check`] compares them, and [`Report::lines`] is what the command prints.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use serde::Deserialize;

/// What a pinned thing is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Crate,
    Toolchain,
    Image,
    Action,
    Tool,
    Npm,
    Node,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Crate => "crate",
            Kind::Toolchain => "toolchain",
            Kind::Image => "image",
            Kind::Action => "action",
            Kind::Tool => "tool",
            Kind::Npm => "npm",
            Kind::Node => "node",
        }
    }
}

/// One pinned thing: its name, the version in use and the file that pins it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub kind: Kind,
    pub name: String,
    pub current: String,
    pub pinned_in: String,
}

/// A written hold (`dependency-exceptions.yaml`): `held` is a version prefix at a version
/// boundary, or `*` for every version of the name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Exception {
    pub kind: Kind,
    pub name: String,
    pub held: String,
    pub reason: String,
}

/// How an item stands against its registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Current,
    Floating,
    Behind { newest: String },
    Held { newest: String, reason: String },
    Unknown { error: String },
}

/// Where the newest version comes from. `Ok(None)` means the item floats (no version to compare).
pub trait Registry {
    fn newest(&self, item: &Item) -> Result<Option<String>, String>;
}

/// The result of a check: every item with its verdict, and the exceptions that hold nothing back.
#[derive(Debug)]
pub struct Report {
    pub items: Vec<(Item, Verdict)>,
    pub stale: Vec<(Exception, String)>,
}

impl Report {
    /// No item behind or unknown, and no stale exception.
    pub fn passes(&self) -> bool {
        self.stale.is_empty()
            && self
                .items
                .iter()
                .all(|(_, v)| !matches!(v, Verdict::Behind { .. } | Verdict::Unknown { .. }))
    }

    /// One line per item (verdict, kind, name, current to newest, the file it is pinned in; a
    /// hold's line carries its reason), then one line per stale exception.
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (item, verdict) in &self.items {
            let head = format!("{} {} {}", item.kind.as_str(), item.name, item.current);
            let pinned = format!("(pinned in {})", item.pinned_in);
            out.push(match verdict {
                Verdict::Current => format!("current  {head} {pinned}"),
                Verdict::Floating => format!("floating {head} {pinned}"),
                Verdict::Behind { newest } => {
                    format!("BEHIND   {head} \u{2192} {newest} {pinned}")
                }
                Verdict::Held { newest, reason } => {
                    format!("held     {head} \u{2192} {newest} {pinned}: {reason}")
                }
                Verdict::Unknown { error } => format!("UNKNOWN  {head} {pinned}: {error}"),
            });
        }
        for (e, why) in &self.stale {
            out.push(format!(
                "STALE    exception {} {} held \"{}\": {why}",
                e.kind.as_str(),
                e.name,
                e.held
            ));
        }
        out
    }
}

/// A tag's shape: the tag with every run of digits replaced by `#`.
fn shape(tag: &str) -> String {
    let mut out = String::with_capacity(tag.len());
    let mut in_digits = false;
    for c in tag.chars() {
        if c.is_ascii_digit() {
            if !in_digits {
                out.push('#');
            }
            in_digits = true;
        } else {
            in_digits = false;
            out.push(c);
        }
    }
    out
}

/// The numbers in a tag, left to right.
fn digit_runs(tag: &str) -> Vec<u128> {
    tag.split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap_or(u128::MAX))
        .collect()
}

/// The greatest tag of `current`'s shape, compared by its numbers left to right: `1.0.6-beta`,
/// `1.0.5-amd64` and `latest` are other shapes, so they drop out. May equal `current`.
pub fn newest_of_shape(current: &str, tags: &[String]) -> Option<String> {
    let want = shape(current);
    tags.iter()
        .filter(|t| shape(t) == want)
        .max_by(|a, b| digit_runs(a).cmp(&digit_runs(b)))
        .cloned()
}

/// A tag without digits (`nonroot`, `latest`) floats by design: nothing to compare.
fn floats(tag: &str) -> bool {
    !tag.chars().any(|c| c.is_ascii_digit())
}

/// Whether `held` covers `current`: `*`, or a prefix ending at a version boundary.
fn held_matches(held: &str, current: &str) -> bool {
    held == "*"
        || current == held
        || current
            .strip_prefix(held)
            .is_some_and(|rest| rest.starts_with(['.', '-', '+']))
}

/// `current` against `newest` at current's own precision (`v7` is current under `v7.0.1`).
fn is_current(current: &str, newest: &str) -> bool {
    let strip = |s: &str| s.split('+').next().unwrap_or(s).to_string();
    let (current, newest) = (strip(current), strip(newest));
    let k = current.split('.').count();
    let truncated: Vec<&str> = newest.split('.').take(k).collect();
    current == truncated.join(".")
}

/// The verdict for `item` given what its registry said.
pub fn verdict(
    item: &Item,
    newest: Result<Option<String>, String>,
    exceptions: &[Exception],
) -> Verdict {
    match newest {
        Err(error) => Verdict::Unknown { error },
        Ok(None) => Verdict::Floating,
        Ok(Some(newest)) if is_current(&item.current, &newest) => Verdict::Current,
        Ok(Some(newest)) => {
            // A hold covers what lies outside its line: a newer version inside it (59.4.0 under
            // `held: "59"`) is a plain bump, so the item is Behind. `*` covers everything.
            let hold = exceptions.iter().find(|e| {
                e.kind == item.kind
                    && e.name == item.name
                    && held_matches(&e.held, &item.current)
                    && (e.held == "*" || !held_matches(&e.held, &newest))
            });
            match hold {
                Some(e) => Verdict::Held {
                    newest,
                    reason: e.reason.clone(),
                },
                None => Verdict::Behind { newest },
            }
        }
    }
}

/// [`check`] over a given inventory.
pub fn check_items(items: Vec<Item>, registry: &dyn Registry, exceptions: &[Exception]) -> Report {
    let items: Vec<(Item, Verdict)> = items
        .into_iter()
        .map(|item| {
            let v = verdict(&item, registry.newest(&item), exceptions);
            (item, v)
        })
        .collect();
    let mut stale = Vec::new();
    for e in exceptions.iter().filter(|e| e.held != "*") {
        let named = |i: &Item| i.kind == e.kind && i.name == e.name;
        // Held, or Behind with its newest inside the line (already failing, reported once as
        // Behind, not again as stale): the exception is doing its job or is not the problem.
        let holds = items.iter().any(|(i, v)| {
            named(i)
                && held_matches(&e.held, &i.current)
                && matches!(v, Verdict::Held { .. } | Verdict::Behind { .. })
        });
        if holds {
            continue;
        }
        let why = match items
            .iter()
            .find(|(i, _)| named(i) && held_matches(&e.held, &i.current))
        {
            Some((i, _)) => format!("holds nothing back: {} {} is current", i.name, i.current),
            None => format!(
                "nothing pins {} {} at \"{}\"",
                e.kind.as_str(),
                e.name,
                e.held
            ),
        };
        stale.push((e.clone(), why));
    }
    Report { items, stale }
}

/// Inventories the repository and checks every item against `registry`. An inventory that cannot
/// be read is a failing report, not a pass.
pub fn check(root: &Path, registry: &dyn Registry, exceptions: &[Exception]) -> Report {
    match inventory(root) {
        Ok(items) => check_items(items, registry, exceptions),
        Err(error) => Report {
            items: vec![(
                Item {
                    kind: Kind::Tool,
                    name: "inventory".into(),
                    current: "-".into(),
                    pinned_in: "-".into(),
                },
                Verdict::Unknown { error },
            )],
            stale: Vec::new(),
        },
    }
}

/// `dependency-exceptions.yaml`, at the repository's root.
pub fn exceptions(root: &Path) -> Result<Vec<Exception>, String> {
    let path = root.join("dependency-exceptions.yaml");
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let list: Vec<Exception> =
        serde_saphyr::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    if let Some(e) = list.iter().find(|e| e.reason.trim().is_empty()) {
        return Err(format!("the exception for {} has no reason", e.name));
    }
    Ok(list)
}

// ---------------------------------------------------------------------------------------------
// The inventory
// ---------------------------------------------------------------------------------------------

/// Everything the repository pins, deduplicated on (kind, name, current).
pub fn inventory(root: &Path) -> Result<Vec<Item>, String> {
    let mut items = Vec::new();
    crates(root, &mut items)?;
    toolchain(root, &mut items)?;
    images(root, &mut items)?;
    workflows(root, &mut items)?;
    duckdb(root, &mut items)?;
    npm(root, &mut items)?;
    let mut seen = BTreeSet::new();
    items.retain(|i| seen.insert((i.kind, i.name.clone(), i.current.clone())));
    Ok(items)
}

fn read(root: &Path, rel: &str) -> Result<String, String> {
    std::fs::read_to_string(root.join(rel)).map_err(|e| format!("{rel}: {e}"))
}

fn rel(root: &Path, path: &str) -> String {
    Path::new(path)
        .strip_prefix(root)
        .map_or_else(|_| path.to_string(), |p| p.display().to_string())
}

/// Direct non-path dependencies of every workspace member, at the version the resolve graph gives
/// that member.
fn crates(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
    let out = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
        .args(["metadata", "--format-version", "1", "--locked"])
        .current_dir(root)
        .output()
        .map_err(|e| format!("cargo metadata: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "cargo metadata: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let meta: serde_json::Value =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("cargo metadata: {e}"))?;
    let packages = meta["packages"].as_array().ok_or("no packages")?;
    let by_id = |id: &str| packages.iter().find(|p| p["id"] == id);
    let members: Vec<&str> = meta["workspace_members"]
        .as_array()
        .ok_or("no workspace_members")?
        .iter()
        .filter_map(|m| m.as_str())
        .collect();
    let nodes = meta["resolve"]["nodes"].as_array().ok_or("no resolve")?;
    let mut found = Vec::new();
    for member in members {
        let pkg =
            by_id(member).ok_or_else(|| format!("workspace member {member} not in packages"))?;
        let manifest = rel(root, pkg["manifest_path"].as_str().unwrap_or_default());
        let node = nodes
            .iter()
            .find(|n| n["id"] == member)
            .ok_or_else(|| format!("workspace member {member} not in the resolve graph"))?;
        for dep in node["deps"].as_array().into_iter().flatten() {
            let id = dep["pkg"]
                .as_str()
                .ok_or("a resolve dependency without a pkg id")?;
            let dep_pkg = by_id(id)
                .ok_or_else(|| format!("{id} (a dependency of {member}) not in packages"))?;
            if dep_pkg["source"].is_null() {
                continue; // a path dependency: ours
            }
            found.push(Item {
                kind: Kind::Crate,
                name: dep_pkg["name"].as_str().unwrap_or_default().to_string(),
                current: dep_pkg["version"].as_str().unwrap_or_default().to_string(),
                pinned_in: manifest.clone(),
            });
        }
    }
    found.sort_by(|a, b| {
        (&a.name, &a.current, &a.pinned_in).cmp(&(&b.name, &b.current, &b.pinned_in))
    });
    items.extend(found);
    Ok(())
}

/// A quoted or bare YAML/TOML scalar after `key`: `key = "x"` or `key: x`.
fn scalar(value: &str) -> String {
    let v = value.split(" #").next().unwrap_or(value).trim();
    v.trim_matches(|c| c == '"' || c == '\'').to_string()
}

fn toolchain(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
    let text = read(root, "rust-toolchain.toml")?;
    let channel = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("channel"))
        .find_map(|l| l.trim_start().strip_prefix('='))
        .map(scalar)
        .ok_or("rust-toolchain.toml has no channel")?;
    items.push(Item {
        kind: Kind::Toolchain,
        name: "rust".into(),
        current: channel,
        pinned_in: "rust-toolchain.toml".into(),
    });
    Ok(())
}

/// The value of `image:` in a compose line, in block form or inside a flow map.
fn compose_image(line: &str) -> Option<String> {
    let t = line.trim();
    if t.starts_with('#') {
        return None;
    }
    let at = t
        .match_indices("image:")
        .find(|(i, _)| *i == 0 || t[..*i].ends_with([' ', '{', ',', '-']))?;
    let value = &t[at.0 + "image:".len()..];
    let value = value.split([',', '}']).next()?;
    let value = scalar(value);
    (!value.is_empty()).then_some(value)
}

fn image_item(reference: &str, pinned_in: &str) -> Option<Item> {
    let reference = reference.split('@').next()?;
    let (name, tag) = match reference.rsplit_once(':') {
        Some((n, t)) if !t.contains('/') => (n, t),
        _ => return None, // untagged: a build stage's name, not a pin
    };
    Some(Item {
        kind: Kind::Image,
        name: name.to_string(),
        current: tag.to_string(),
        pinned_in: pinned_in.to_string(),
    })
}

/// The Dockerfile's `# syntax=<image>:<tag>` frontend pin, if `line` is that directive.
fn syntax_frontend(line: &str) -> Option<String> {
    let rest = line
        .trim()
        .strip_prefix('#')?
        .trim_start()
        .strip_prefix("syntax")?;
    let value = rest.trim_start().strip_prefix('=')?.trim();
    (!value.is_empty()).then(|| value.to_string())
}

/// `docker run` flags that take their value as the next word.
const DOCKER_RUN_VALUE_FLAGS: &[&str] = &[
    "-v",
    "--volume",
    "-e",
    "--env",
    "--entrypoint",
    "-w",
    "--workdir",
    "--name",
    "--network",
    "-u",
    "--user",
    "-p",
    "--publish",
    "--mount",
    "--platform",
    "--env-file",
    "-l",
    "--label",
];

/// The registry images a workflow's `run:` text starts with `docker run` (flags skipped). A name
/// without a `/` is an image the job built itself (`data-avalanche:arm64`), not a pin.
fn docker_run_images(text: &str) -> Vec<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    let mut found = Vec::new();
    for (i, w) in words.iter().enumerate() {
        if *w != "docker" || words.get(i + 1) != Some(&"run") {
            continue;
        }
        let mut rest = words[i + 2..].iter();
        while let Some(w) = rest.next() {
            if DOCKER_RUN_VALUE_FLAGS.contains(w) {
                rest.next();
            } else if !w.starts_with('-') {
                let image = w.trim_matches(|c| c == '"' || c == '\'');
                if image.contains('/') && image.contains(':') {
                    found.push(image.to_string());
                }
                break;
            }
        }
    }
    found
}

fn images(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
    let compose = "test-infra/compose.yaml";
    for line in read(root, compose)?.lines() {
        if let Some(item) = compose_image(line).and_then(|r| image_item(&r, compose)) {
            items.push(item);
        }
    }
    let docker = "deploy/docker/Dockerfile";
    for line in read(root, docker)?.lines() {
        if let Some(item) = syntax_frontend(line).and_then(|r| image_item(&r, docker)) {
            items.push(item);
        }
        let mut words = line.split_whitespace();
        if !words.next().is_some_and(|w| w.eq_ignore_ascii_case("FROM")) {
            continue;
        }
        if let Some(r) = words.find(|w| !w.starts_with("--"))
            && let Some(item) = image_item(r, docker)
        {
            items.push(item);
        }
    }
    Ok(())
}

fn workflows(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
    let dir = root.join(".github/workflows");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| matches!(p.extension().and_then(|e| e.to_str()), Some("yml" | "yaml")))
        .collect();
    files.sort();
    for file in files {
        let pinned_in = rel(root, &file.display().to_string());
        let text = std::fs::read_to_string(&file).map_err(|e| format!("{pinned_in}: {e}"))?;
        for r in docker_run_images(&text) {
            items.extend(image_item(&r, &pinned_in));
        }
        let lines: Vec<&str> = text.lines().collect();
        for (n, line) in lines.iter().enumerate() {
            let t = line.trim_start();
            let (t, dash) = match t.strip_prefix("- ") {
                Some(rest) => (rest.trim_start(), 2),
                None => (t, 0),
            };
            let Some(value) = t.strip_prefix("uses:") else {
                continue;
            };
            let value = scalar(value);
            if value.starts_with("./") || value.starts_with("docker://") {
                continue;
            }
            let Some((path, current)) = value.split_once('@') else {
                continue;
            };
            let name = path.split('/').take(2).collect::<Vec<_>>().join("/");
            items.push(Item {
                kind: Kind::Action,
                name: name.clone(),
                current: current.to_string(),
                pinned_in: pinned_in.clone(),
            });
            if name != "azure/setup-helm" {
                continue;
            }
            // The step ends at the first line indented less than its `uses:` key.
            let key_col = line.len() - line.trim_start().len() + dash;
            for next in &lines[n + 1..] {
                let indent = next.len() - next.trim_start().len();
                if !next.trim().is_empty() && indent < key_col {
                    break;
                }
                if let Some(v) = next.trim().strip_prefix("version:") {
                    let v = scalar(v);
                    if !v.is_empty() && !v.contains('$') {
                        items.push(Item {
                            kind: Kind::Tool,
                            name: "helm".into(),
                            current: v,
                            pinned_in: pinned_in.clone(),
                        });
                    }
                    break;
                }
            }
        }
    }
    Ok(())
}

fn duckdb(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
    for rel in [
        "crates/data-avalanche/tests/e2e/types.rs",
        "crates/data-avalanche-sink-delta/tests/sink/types.rs",
    ] {
        for line in read(root, rel)?.lines() {
            let Some((_, rest)) = line.split_once("duckdb==") else {
                continue;
            };
            let v: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            if !v.is_empty() {
                items.push(Item {
                    kind: Kind::Tool,
                    name: "duckdb".into(),
                    current: v,
                    pinned_in: rel.into(),
                });
            }
        }
    }
    Ok(())
}

fn npm(root: &Path, items: &mut Vec<Item>) -> Result<(), String> {
    let text = read(root, "docs/package.json")?;
    let json: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("docs/package.json: {e}"))?;
    for (name, version) in json["dependencies"].as_object().into_iter().flatten() {
        items.push(Item {
            kind: Kind::Npm,
            name: name.clone(),
            current: version.as_str().unwrap_or_default().to_string(),
            pinned_in: "docs/package.json".into(),
        });
    }
    items.push(Item {
        kind: Kind::Node,
        name: "node".into(),
        current: read(root, "docs/.nvmrc")?
            .trim()
            .trim_start_matches('v')
            .to_string(),
        pinned_in: "docs/.nvmrc".into(),
    });
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The live registries
// ---------------------------------------------------------------------------------------------

/// The real registries: crates.io, static.rust-lang.org, Docker Hub, MCR, GitHub, PyPI, npm and
/// nodejs.org.
#[derive(Debug)]
pub struct Live {
    client: reqwest::blocking::Client,
    crates_io_last: Cell<Option<Instant>>,
}

impl Default for Live {
    fn default() -> Self {
        Self::new()
    }
}

impl Live {
    pub fn new() -> Self {
        let client = reqwest::blocking::Client::builder()
            .user_agent("data-avalanche-xtask (dependency check)")
            .timeout(Duration::from_secs(60))
            .build()
            .expect("an HTTP client");
        Live {
            client,
            crates_io_last: Cell::new(None),
        }
    }

    fn get(&self, url: &str) -> Result<String, String> {
        let mut req = self.client.get(url);
        if url.starts_with("https://api.github.com/")
            && let Ok(token) = std::env::var("GITHUB_TOKEN")
            && !token.is_empty()
        {
            req = req.bearer_auth(token);
        }
        let resp = req.send().map_err(|e| format!("{url}: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("HTTP {} ({url})", status.as_u16()));
        }
        resp.text().map_err(|e| format!("{url}: {e}"))
    }

    fn json(&self, url: &str) -> Result<serde_json::Value, String> {
        serde_json::from_str(&self.get(url)?).map_err(|e| format!("{url}: {e}"))
    }

    fn field(&self, url: &str, pointer: &str) -> Result<Option<String>, String> {
        self.json(url)?
            .pointer(pointer)
            .and_then(|v| v.as_str())
            .map(|s| Some(s.to_string()))
            .ok_or_else(|| format!("{url}: no {pointer}"))
    }

    fn crate_newest(&self, name: &str) -> Result<Option<String>, String> {
        // crates.io asks crawlers for at most one request a second.
        if let Some(last) = self.crates_io_last.get() {
            let gap = Duration::from_secs(1);
            if let Some(wait) = gap.checked_sub(last.elapsed()) {
                std::thread::sleep(wait);
            }
        }
        self.crates_io_last.set(Some(Instant::now()));
        self.field(
            &format!("https://crates.io/api/v1/crates/{name}"),
            "/crate/max_stable_version",
        )
    }

    fn toolchain_newest(&self) -> Result<Option<String>, String> {
        let url = "https://static.rust-lang.org/dist/channel-rust-stable.toml";
        let text = self.get(url)?;
        let mut in_rust = false;
        for line in text.lines() {
            if line.starts_with('[') {
                in_rust = line.trim() == "[pkg.rust]";
            } else if in_rust && let Some(v) = line.strip_prefix("version = ") {
                return match scalar(v).split_whitespace().next() {
                    Some(version) => Ok(Some(version.to_string())),
                    None => Err(format!("{url}: empty [pkg.rust] version")),
                };
            }
        }
        Err(format!("{url}: no [pkg.rust] version"))
    }

    fn image_newest(&self, item: &Item) -> Result<Option<String>, String> {
        if floats(&item.current) {
            return Ok(None);
        }
        let name = item.name.as_str();
        let mut tags = Vec::new();
        let first = name.split('/').next().unwrap_or_default();
        if first == "mcr.microsoft.com" {
            let repo = &name["mcr.microsoft.com/".len()..];
            let json = self.json(&format!("https://mcr.microsoft.com/v2/{repo}/tags/list"))?;
            tags.extend(
                json["tags"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|t| t.as_str().map(String::from)),
            );
        } else if first.contains('.') || first.contains(':') {
            return Err(format!("no lookup for the registry {first}"));
        } else {
            let repo = if name.contains('/') {
                name.to_string()
            } else {
                format!("library/{name}")
            };
            let mut url = Some(format!(
                "https://hub.docker.com/v2/repositories/{repo}/tags?page_size=100&ordering=last_updated"
            ));
            // The newest 5 x 100 tags by last_updated: enough for the shapes pinned here.
            for _ in 0..5 {
                let Some(u) = url.take() else { break };
                let json = self.json(&u)?;
                tags.extend(
                    json["results"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|t| t["name"].as_str().map(String::from)),
                );
                url = json["next"].as_str().map(String::from);
            }
        }
        newest_of_shape(&item.current, &tags)
            .map(Some)
            .ok_or_else(|| format!("{name}: no tag of the shape of {}", item.current))
    }

    fn github_latest(&self, repo: &str) -> Result<Option<String>, String> {
        self.field(
            &format!("https://api.github.com/repos/{repo}/releases/latest"),
            "/tag_name",
        )
    }

    fn node_newest(&self) -> Result<Option<String>, String> {
        let url = "https://nodejs.org/dist/index.json";
        let json = self.json(url)?;
        json.as_array()
            .into_iter()
            .flatten()
            .find(|e| e["lts"] != false)
            .and_then(|e| e["version"].as_str())
            .map(|v| {
                v.trim_start_matches('v')
                    .split('.')
                    .next()
                    .unwrap_or(v)
                    .to_string()
            })
            .map(Some)
            .ok_or_else(|| format!("{url}: no LTS entry"))
    }
}

impl Registry for Live {
    fn newest(&self, item: &Item) -> Result<Option<String>, String> {
        match item.kind {
            Kind::Crate => self.crate_newest(&item.name),
            Kind::Toolchain => self.toolchain_newest(),
            Kind::Image => self.image_newest(item),
            Kind::Action => self.github_latest(&item.name),
            Kind::Tool => match item.name.as_str() {
                "helm" => self.github_latest("helm/helm"),
                "duckdb" => self.field("https://pypi.org/pypi/duckdb/json", "/info/version"),
                other => Err(format!("no lookup for the tool {other}")),
            },
            Kind::Npm => self.field(
                &format!("https://registry.npmjs.org/{}/latest", item.name),
                "/version",
            ),
            Kind::Node => self.node_newest(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Fake(HashMap<String, Result<Option<String>, String>>);

    impl Registry for Fake {
        fn newest(&self, item: &Item) -> Result<Option<String>, String> {
            self.0
                .get(&item.name)
                .cloned()
                .unwrap_or_else(|| Err("not in the fake".into()))
        }
    }

    fn fake(name: &str, newest: Result<Option<&str>, &str>) -> Fake {
        Fake(HashMap::from([(
            name.to_string(),
            newest.map(|n| n.map(String::from)).map_err(String::from),
        )]))
    }

    fn item(kind: Kind, name: &str, current: &str) -> Item {
        Item {
            kind,
            name: name.into(),
            current: current.into(),
            pinned_in: "crates/x/Cargo.toml".into(),
        }
    }

    fn hold(kind: Kind, name: &str, held: &str) -> Exception {
        Exception {
            kind,
            name: name.into(),
            held: held.into(),
            reason: "a blocking requirement".into(),
        }
    }

    fn tags(t: &[&str]) -> Vec<String> {
        t.iter().map(|s| s.to_string()).collect()
    }

    fn report(item: Item, newest: Result<Option<&str>, &str>, ex: &[Exception]) -> Report {
        let r = fake(&item.name, newest);
        check_items(vec![item], &r, ex)
    }

    #[test]
    fn newest_of_shape_compares_numerically() {
        assert_eq!(
            newest_of_shape("2.37.4", &tags(&["2.9.0", "2.40.2", "2.10.1"])).as_deref(),
            Some("2.40.2")
        );
        assert_eq!(
            newest_of_shape(
                "v1.37.1-k3s1",
                &tags(&["v1.37.2-k3s1", "v1.37.10-k3s1", "v1.38.0-rc1-k3s1"])
            )
            .as_deref(),
            Some("v1.37.10-k3s1")
        );
        assert_eq!(
            newest_of_shape("1.99.1", &tags(&["1.99.1", "1.100.0"])).as_deref(),
            Some("1.100.0")
        );
    }

    #[test]
    fn newest_of_shape_skips_other_shapes_and_prereleases() {
        let t = tags(&[
            "1.0.6-beta",
            "1.1.0-rc.1",
            "2.0.0-alpha.1",
            "1.0.5",
            "latest",
            "1.0.5-amd64",
        ]);
        assert_eq!(newest_of_shape("1.0.0", &t).as_deref(), Some("1.0.5"));
    }

    #[test]
    fn an_action_ref_is_checked_at_its_own_precision() {
        let a = || item(Kind::Action, "actions/checkout", "v7");
        assert_eq!(
            report(a(), Ok(Some("v7.0.1")), &[]).items[0].1,
            Verdict::Current
        );
        assert_eq!(
            report(a(), Ok(Some("v8.0.0")), &[]).items[0].1,
            Verdict::Behind {
                newest: "v8.0.0".into()
            }
        );
        let b = item(Kind::Action, "actions/checkout", "v10.2.0");
        assert!(matches!(
            report(b, Ok(Some("v10.3.3")), &[]).items[0].1,
            Verdict::Behind { .. }
        ));
    }

    #[test]
    fn a_behind_dependency_fails_with_its_newest_version_named() {
        let r = report(item(Kind::Crate, "uuid", "1.26.1"), Ok(Some("1.29.2")), &[]);
        assert_eq!(
            r.items[0].1,
            Verdict::Behind {
                newest: "1.29.2".into()
            }
        );
        assert!(!r.passes());
        let line = r.lines().join("\n");
        for part in ["uuid", "1.26.1", "1.29.2", "crates/x/Cargo.toml"] {
            assert!(line.contains(part), "{part} missing from {line}");
        }
    }

    #[test]
    fn a_held_dependency_passes_only_with_a_matching_exception() {
        let i = || item(Kind::Crate, "arrow-array", "59.3.2");
        let r = report(
            i(),
            Ok(Some("60.1.0")),
            &[hold(Kind::Crate, "arrow-array", "59")],
        );
        assert!(matches!(r.items[0].1, Verdict::Held { .. }));
        assert!(r.passes());
        assert!(r.lines()[0].contains("a blocking requirement"));
        let r = report(
            i(),
            Ok(Some("60.1.0")),
            &[hold(Kind::Crate, "arrow-array", "58")],
        );
        assert!(matches!(r.items[0].1, Verdict::Behind { .. }));
        // `59` is a version boundary, not a string prefix.
        assert!(!held_matches("59", "590.1.0"));
    }

    #[test]
    fn an_exception_for_a_current_dependency_is_stale() {
        let r = report(
            item(Kind::Crate, "arrow-array", "60.1.0"),
            Ok(Some("60.1.0")),
            &[hold(Kind::Crate, "arrow-array", "59")],
        );
        assert!(!r.stale.is_empty());
        assert!(!r.passes());
    }

    #[test]
    fn an_exception_for_nothing_is_stale() {
        let r = report(
            item(Kind::Crate, "uuid", "1.29.2"),
            Ok(Some("1.29.2")),
            &[hold(Kind::Crate, "gone", "1")],
        );
        assert!(!r.stale.is_empty());
        assert!(!r.passes());
        assert!(r.lines().iter().any(|l| l.contains("gone")));
    }

    #[test]
    fn a_wildcard_exception_never_goes_stale() {
        let r = report(
            item(Kind::Image, "postgres", "18"),
            Ok(Some("18")),
            &[hold(Kind::Image, "postgres", "*")],
        );
        assert!(r.stale.is_empty());
        assert!(r.passes());
    }

    #[test]
    fn a_lookup_failure_fails_the_check() {
        let r = report(item(Kind::Crate, "uuid", "1.26.1"), Err("HTTP 403"), &[]);
        assert!(matches!(&r.items[0].1, Verdict::Unknown { error } if error == "HTTP 403"));
        assert!(!r.passes());
    }

    #[test]
    fn a_tag_without_digits_floats() {
        let r = report(
            item(Kind::Image, "gcr.io/distroless/cc-debian13", "nonroot"),
            Ok(None),
            &[],
        );
        assert_eq!(r.items[0].1, Verdict::Floating);
        assert!(r.passes());
        assert!(floats("nonroot") && floats("latest"));
        assert!(!floats("18") && !floats("v1.37.2-k3s1"));
    }

    #[test]
    fn a_hold_does_not_cover_a_newer_version_inside_its_line() {
        let ex = [hold(Kind::Crate, "arrow-array", "59")];
        let i = || item(Kind::Crate, "arrow-array", "59.3.2");
        let r = report(i(), Ok(Some("59.4.0")), &ex);
        assert_eq!(
            r.items[0].1,
            Verdict::Behind {
                newest: "59.4.0".into()
            }
        );
        assert!(!r.passes());
        // Reported once, as Behind: not also as stale.
        assert!(r.stale.is_empty());
        let r = report(i(), Ok(Some("60.1.0")), &ex);
        assert!(matches!(r.items[0].1, Verdict::Held { .. }));
        assert!(r.passes());
    }

    #[test]
    fn a_hold_matches_at_a_version_boundary() {
        assert!(held_matches("0.37", "0.37.3"));
        assert!(!held_matches("0.37", "0.370.1"));
    }

    #[test]
    fn the_inventory_covers_every_kind() {
        let items = inventory(&crate::repo_root()).expect("the inventory");
        let has = |kind, name: &str| items.iter().any(|i| i.kind == kind && i.name == name);
        assert!(has(Kind::Crate, "deltalake"));
        assert!(has(Kind::Toolchain, "rust"));
        assert!(has(Kind::Image, "rustfs/rustfs"));
        assert!(has(Kind::Action, "actions/checkout"));
        assert!(has(Kind::Tool, "helm"));
        assert!(has(Kind::Tool, "duckdb"));
        assert!(has(Kind::Npm, "astro"));
        assert!(has(Kind::Node, "node"));
        assert!(
            items
                .iter()
                .all(|i| !i.pinned_in.starts_with("prototypes/")),
            "a prototype's dependency is inventoried"
        );
    }

    #[test]
    fn a_docker_run_image_in_a_workflow_step_is_inventoried() {
        let text = r#"
      - name: Alert rules
        run: >-
          docker run --rm -v "$PWD/deploy/prometheus:/rules:ro" --entrypoint promtool
          prom/prometheus:v3.16.0 test rules /rules/alerts.test.yaml
      - run: |
          helm template x \
              | docker run --rm -i ghcr.io/yannh/kubeconform:v0.8.0 -strict -summary -
      - run: docker run --rm -v "$PWD:/repo:ro" --workdir /repo rhysd/actionlint:1.7.12 -no-color
      - run: docker run --rm data-avalanche:arm64 --version
"#;
        assert_eq!(
            docker_run_images(text),
            [
                "prom/prometheus:v3.16.0",
                "ghcr.io/yannh/kubeconform:v0.8.0",
                "rhysd/actionlint:1.7.12",
            ]
        );
    }

    #[test]
    fn the_dockerfile_syntax_frontend_is_inventoried() {
        assert_eq!(
            syntax_frontend("# syntax=docker/dockerfile:1.29").as_deref(),
            Some("docker/dockerfile:1.29")
        );
        assert_eq!(
            syntax_frontend("#syntax = docker/dockerfile:1").as_deref(),
            Some("docker/dockerfile:1")
        );
        assert_eq!(syntax_frontend("# The data-avalanche image"), None);
        assert_eq!(syntax_frontend("FROM rust:1 AS build"), None);
    }

    #[test]
    fn the_exceptions_file_parses() {
        let ex = exceptions(&crate::repo_root()).expect("dependency-exceptions.yaml");
        assert!(!ex.is_empty());
        assert!(ex.iter().all(|e| !e.reason.trim().is_empty()));
    }
}
