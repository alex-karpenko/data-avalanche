//! `reference/configuration.md`: every key of the config file, block by block, with its type,
//! default, reload class, rules and meaning.

use std::collections::BTreeMap;
use std::fmt::Write;

use data_avalanche_config::{
    DocDefault, FIELD_DOCS, Key, KeyShape, Mutability, config_keys, key_mutability,
};

use crate::rules::{RuleTarget, targets};
use crate::{BASE, cell, header};

/// What each block is, said once above its table.
const BLOCKS: &[(&str, &str)] = &[
    (
        "",
        "The file's top level: its version, `globals`, and the `pipelines` map (`defaults` takes the same keys as a pipeline, merged into each, the pipeline winning).",
    ),
    (
        "globals",
        "Settings of the whole cluster, the same on every node.",
    ),
    ("globals.state", "Where the cluster keeps its own state."),
    (
        "globals.api",
        "The management API, `/metrics` and the health probes.",
    ),
    ("globals.api.jwt", "Token verification for `auth: jwt`."),
    (
        "globals.api.oauth2",
        "Token verification for `auth: oauth2`.",
    ),
    ("globals.api.tls", "TLS for the API's listener."),
    ("globals.log", "The node's log."),
    (
        "globals.cluster",
        "Membership, the hash ring and the pipelines' leases.",
    ),
    (
        "globals.cluster.memberlist",
        "The gossip (SWIM over UDP). Per-node values come from the environment through `${VAR}`.",
    ),
    (
        "globals.tracing",
        "OTLP/HTTP traces of flushes, commits, snapshot chunks and schema changes.",
    ),
    ("globals.azure", "Azure Blob Storage (`cloud: azure`)."),
    (
        "pipelines.*",
        "One pipeline: a source database, its tables, and the sink they replicate to. The map's key is the pipeline's name.",
    ),
    (
        "pipelines.*.source",
        "The source database and the connection to it.",
    ),
    (
        "pipelines.*.source.timeouts",
        "The source connection's timeouts.",
    ),
    (
        "pipelines.*.source.postgres",
        "PostgreSQL logical replication (`type: postgres`).",
    ),
    (
        "pipelines.*.source.mysql",
        "The MySQL or MariaDB binlog stream (`type: mysql`).",
    ),
    (
        "pipelines.*.tables",
        "Which tables replicate, and per-table settings. Per-table keys name the table exactly: `schema.table` on PostgreSQL, the bare name on MySQL.",
    ),
    (
        "pipelines.*.tables.snapshots",
        "Copying a table's existing rows before its changes stream.",
    ),
    (
        "pipelines.*.tables.partitioning.*[]",
        "A table's partition fields, a list of `{ column, transform }`.",
    ),
    (
        "pipelines.*.sink",
        "The table format and where the tables are written.",
    ),
    (
        "pipelines.*.sink.flush",
        "When buffered changes are committed: the first trigger met fires.",
    ),
    ("pipelines.*.sink.metrics", "The pipeline's metrics."),
    (
        "pipelines.*.sink.delta",
        "Delta Lake tables (`type: delta`).",
    ),
    (
        "pipelines.*.sink.delta.catalog",
        "Catalog registration of the Delta tables.",
    ),
    (
        "pipelines.*.state",
        "Where the pipeline keeps its state, when not beside its tables.",
    ),
    (
        "pipelines.*.schema_evolution",
        "Applying the source's schema changes.",
    ),
];

/// The Iceberg sink's blocks: accepted in the file, refused at start in v1 (it ships in v2), so
/// they are listed together at the end.
const ICEBERG: &str = "pipelines.*.sink.iceberg";

/// The block a key sits in: its path less its last segment, where a map's `*` stays with its
/// name (`snapshots.overrides.*` is the `overrides` key of `snapshots`).
fn block_of(path: &str) -> &str {
    let named = path.strip_suffix(".*").unwrap_or(path);
    named.rsplit_once('.').map_or("", |(block, _)| block)
}

/// How a path reads on the page: `<name>` for a key the operator names.
fn shown(path: &str) -> String {
    path.replace("pipelines.*", "pipelines.<name>")
        .replace(".*[]", ".<table>[]")
        .replace(".*", ".<key>")
}

fn noun(expects: &str) -> String {
    if let Some(values) = expects.strip_prefix("one of: ") {
        return values
            .split(", ")
            .map(|v| format!("`{v}`"))
            .collect::<Vec<_>>()
            .join(" | ");
    }
    match expects {
        "text" => "string".into(),
        "a non-negative integer" => "integer".into(),
        "a finite number" => "number".into(),
        "a boolean" => "boolean".into(),
        "a duration" => "duration".into(),
        "a byte size" => "byte size".into(),
        "a host:port" => "host:port".into(),
        "an IP address" => "IP address".into(),
        "a log filter" => "log filter".into(),
        "a logical type" => "logical type".into(),
        other => other.into(),
    }
}

fn type_of(key: &Key) -> String {
    let t = noun(key.expects);
    match key.shape {
        KeyShape::Scalar => t,
        KeyShape::Nullable => format!("{t} or `null`"),
        KeyShape::List => format!("list of {t}"),
        KeyShape::NullableList => format!("list of {t}, or `null`"),
        KeyShape::Map => format!("map of {t}"),
        KeyShape::MapOfLists => format!("map of lists of {t}"),
    }
}

fn default_of(d: DocDefault) -> String {
    match d {
        DocDefault::None => "—".into(),
        DocDefault::Value("") => "`\"\"`".into(),
        DocDefault::Value(v) => format!("`{v}`"),
        DocDefault::Note(n) => cell(n),
    }
}

fn reload(m: Mutability) -> &'static str {
    match m {
        Mutability::R => "applied",
        Mutability::PrIfTableSetChanges => "pipeline restart, if the table set changes",
        Mutability::Pr => "pipeline restart",
        Mutability::Proc => "process restart",
    }
}

fn rule_link(id: &str) -> String {
    format!(
        "[{id}]({BASE}/reference/validation-rules/#{})",
        id.to_lowercase()
    )
}

pub(crate) fn page() -> String {
    let docs: BTreeMap<&str, _> = FIELD_DOCS.iter().map(|d| (d.path, d)).collect();
    let mut rules: BTreeMap<RuleTarget, Vec<&str>> = BTreeMap::new();
    for (id, target) in targets() {
        rules.entry(target).or_default().push(id);
    }

    // The keys in the model's order, grouped by the block that holds them (a block where its
    // first key is); the Iceberg blocks go last, as one.
    let mut blocks: Vec<(&str, Vec<Key>)> = Vec::new();
    let mut iceberg = Vec::new();
    for key in config_keys() {
        if key.path.starts_with(ICEBERG) {
            iceberg.push(key);
            continue;
        }
        let block = BLOCKS
            .iter()
            .map(|(b, _)| *b)
            .find(|b| block_of(&key.path) == *b)
            .unwrap_or_else(|| panic!("{}: no block in BLOCKS holds it", key.path));
        match blocks.iter_mut().find(|(b, _)| *b == block) {
            Some((_, keys)) => keys.push(key),
            None => blocks.push((block, vec![key])),
        }
    }
    blocks.push((ICEBERG, iceberg));

    let mut out = header(
        "Configuration reference",
        "Every key of the config file: its type, default, reload behaviour, rules and meaning.",
        1,
        "the config model (`data_avalanche_config::config_keys` and `FIELD_DOCS`)",
    );
    let _ = write!(
        out,
        "Each key's **On reload** column says what a changed value does through \
         `POST /v1/config/reload`: *applied* in place, a *pipeline restart* (that pipeline starts \
         again from its last checkpoint), or a *process restart* (the reload is refused, naming \
         the key). \
         **Rules** link to the [validation rules]({BASE}/reference/validation-rules/) that check \
         the key. `<name>` is a pipeline's name, `<key>` a key the operator chooses. The \
         [configuration guide]({BASE}/configuration/overview/) explains the file's structure, \
         `${{VAR}}` substitution and how `defaults` merge.\n\n",
    );
    for (block, keys) in &blocks {
        let title = match *block {
            "" => "Top level".to_string(),
            ICEBERG => format!("`{}` (v2)", shown(block)),
            b => format!("`{}`", shown(b)),
        };
        let _ = writeln!(out, "## {title}\n");
        let about = if *block == ICEBERG {
            "The Iceberg sink is designed but ships in v2: a pipeline with `sink.type: iceberg` \
             validates, and is refused at start while the other pipelines run."
        } else {
            BLOCKS
                .iter()
                .find(|(b, _)| b == block)
                .map_or("", |(_, a)| a)
        };
        let _ = writeln!(out, "{about}\n");
        if let Some(ids) = rules.get(&RuleTarget::Block(block.to_string())) {
            let links: Vec<String> = ids.iter().map(|id| rule_link(id)).collect();
            let _ = writeln!(out, "Rules for the whole block: {}.\n", links.join(", "));
        }
        out.push_str(
            "| Key | Type | Default | On reload | Rules | Description |\n|---|---|---|---|---|---|\n",
        );
        for key in keys {
            let doc = docs[key.path.as_str()];
            let name = match key.path.strip_prefix(&format!("{block}.")) {
                Some(name) => shown(name),
                None => shown(&key.path),
            };
            let mut ids: Vec<&str> = Vec::new();
            if key.rule.starts_with("V-") {
                ids.push(key.rule);
            }
            for id in rules
                .get(&RuleTarget::Key(key.path.clone()))
                .into_iter()
                .flatten()
            {
                if !ids.contains(id) {
                    ids.push(id);
                }
            }
            let links: Vec<String> = ids.iter().map(|id| rule_link(id)).collect();
            let _ = writeln!(
                out,
                "| `{}` | {} | {} | {} | {} | {} |",
                name,
                cell(&type_of(key)),
                default_of(doc.default),
                key_mutability(&key.path).map_or("", reload),
                links.join(" "),
                cell(doc.doc),
            );
        }
        out.push('\n');
    }
    out
}
