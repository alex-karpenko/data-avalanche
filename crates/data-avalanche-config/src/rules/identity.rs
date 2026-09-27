//! §13.7 identity and naming.

use std::collections::BTreeMap;

use super::pipeline_at;
use crate::build::DEFAULT_POSTGRES_PORT;
use crate::ctx::{At, Ctx, shown};
use crate::enums::SourceType;
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::{RawConfig, RawPipeline};

/// V-056 dedup entries: the pipeline name and the id's redacted display text.
type IdEntries<'a> = Vec<(&'a str, String)>;
/// V-058 dedup entries: the pipeline name, then the host's and the slot's redacted display text.
type SlotEntries<'a> = Vec<(&'a str, String, String)>;
/// V-059 dedup entries: the pipeline name, then the host's, db's and publication's redacted
/// display text.
type PublicationEntries<'a> = Vec<(&'a str, String, String, String)>;

/// `^[a-z0-9][a-z0-9_-]{0,62}$` (V-055).
fn is_pipeline_id(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'_' | b'-'))
}

/// `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$` (V-057).
fn is_pipeline_name(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=128).contains(&b.len())
        && b[0].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
}

/// `^[a-z_][a-z0-9_]{0,62}$` (V-058, the PostgreSQL slot-name rule).
fn is_slot_name(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0] == b'_')
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

/// An unquoted PostgreSQL identifier, `^[A-Za-z_][A-Za-z0-9_$]{0,62}$` (V-059).
fn is_pg_identifier(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'$'))
}

pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    // V-060: per-pipeline unique names can't be defaulted.
    if let Some(d) = &raw.defaults {
        let at = At::config(YamlPath::root().key("defaults"));
        let pg = d.source.as_ref().and_then(|s| s.postgres.as_ref());
        let forbidden = [
            (
                "id",
                d.id.as_ref().and_then(|l| l.loc()),
                at.key("id"),
                d.id.is_some(),
            ),
            (
                "source.postgres.publication",
                pg.and_then(|p| p.publication.as_ref())
                    .and_then(|l| l.loc()),
                at.key("source").key("postgres").key("publication"),
                pg.is_some_and(|p| p.publication.is_some()),
            ),
            (
                "source.postgres.slot",
                pg.and_then(|p| p.slot.as_ref()).and_then(|l| l.loc()),
                at.key("source").key("postgres").key("slot"),
                pg.is_some_and(|p| p.slot.is_some()),
            ),
            (
                "tables.snapshots.overrides",
                None,
                at.key("tables").key("snapshots").key("overrides"),
                d.tables
                    .as_ref()
                    .and_then(|t| t.snapshots.as_ref())
                    .is_some_and(|s| s.overrides.is_some()),
            ),
        ];
        for (field, loc, field_at, set) in forbidden {
            if set {
                ctx.error(
                    "V-060",
                    &field_at,
                    loc,
                    format!("{field} is per pipeline and must not be set in defaults"),
                );
            }
        }
    }

    // V-058 is keyed on the source *server* (`host` + `port`), not the database: PostgreSQL
    // scopes slot names to the server. V-059 is keyed on `host` + `port` + `db`. The display
    // strings (built with `Ctx::shown`) travel next to each entry so the diagnostic never prints
    // an env-resolved id/slot/publication/host/db — only the key, used for comparison, is the
    // resolved value.
    let mut ids: BTreeMap<String, IdEntries<'_>> = BTreeMap::new();
    let mut slots: BTreeMap<(String, u16, String), SlotEntries<'_>> = BTreeMap::new();
    let mut publications: BTreeMap<(String, u16, String, String), PublicationEntries<'_>> =
        BTreeMap::new();
    for (name, p) in merged {
        let at = pipeline_at(name);
        // V-057
        if !is_pipeline_name(name) {
            ctx.error(
                "V-057",
                &at,
                None,
                format!("pipeline name {name:?} must match ^[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}$"),
            );
        }
        // V-055
        match &p.id {
            None => ctx.error("V-055", &at.key("id"), None, "pipeline id is required"),
            Some(l) => {
                if let Some(id) = ctx.text(l, &at.key("id")) {
                    if is_pipeline_id(&id) {
                        let id_shown = shown(l, &id);
                        ids.entry(id).or_default().push((name, id_shown));
                    } else {
                        ctx.error(
                            "V-055",
                            &at.key("id"),
                            l.loc(),
                            format!(
                                "pipeline id {} must match ^[a-z0-9][a-z0-9_-]{{0,62}}$",
                                shown(l, &id)
                            ),
                        );
                    }
                }
            }
        }
        // V-058, V-059
        let Some(src) = &p.source else { continue };
        let is_pg = src.kind.is_none() || ctx.peek(&src.kind) == Some(SourceType::Postgres);
        let Some(pg) = src.postgres.as_ref().filter(|_| is_pg) else {
            continue;
        };
        let pg_at = at.key("source").key("postgres");
        let host: Option<(&Leaf<String>, String)> = src
            .host
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("source").key("host")).map(|t| (l, t)));
        // The resolved port, or PostgreSQL's default when absent (build.rs uses the same
        // constant); `None` only when a given port fails to coerce.
        let port: Option<(Option<&Leaf<u16>>, u16)> = match &src.port {
            Some(l) => ctx
                .value(l, &at.key("source").key("port"))
                .map(|v| (Some(l), v)),
            None => Some((None, DEFAULT_POSTGRES_PORT)),
        };
        let db: Option<(&Leaf<String>, String)> = src
            .db
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("source").key("db")).map(|t| (l, t)));
        if let Some(l) = &pg.slot
            && let Some(slot) = ctx.text(l, &pg_at.key("slot"))
        {
            if !is_slot_name(&slot) {
                ctx.error(
                    "V-058",
                    &At::config(pg_at.key("slot").path),
                    l.loc(),
                    format!(
                        "slot {} must match ^[a-z_][a-z0-9_]{{0,62}}$",
                        shown(l, &slot)
                    ),
                );
            } else if let (Some((host_leaf, host)), Some((_, port))) = (&host, &port) {
                let slot_shown = shown(l, &slot);
                let host_shown = shown(*host_leaf, host);
                slots
                    .entry((host.clone(), *port, slot))
                    .or_default()
                    .push((name, host_shown, slot_shown));
            }
        }
        if let Some(l) = &pg.publication
            && let Some(publication) = ctx.text(l, &pg_at.key("publication"))
        {
            if !is_pg_identifier(&publication) {
                ctx.warning(
                    "V-059",
                    &pg_at.key("publication"),
                    l.loc(),
                    format!(
                        "publication {} is not a plain PostgreSQL identifier; it will need quoting",
                        shown(l, &publication)
                    ),
                );
            } else if let (Some((host_leaf, host)), Some((_, port)), Some((db_leaf, db))) =
                (&host, &port, &db)
            {
                let publication_shown = shown(l, &publication);
                let host_shown = shown(*host_leaf, host);
                let db_shown = shown(*db_leaf, db);
                publications
                    .entry((host.clone(), *port, db.clone(), publication))
                    .or_default()
                    .push((name, host_shown, db_shown, publication_shown));
            }
        }
    }
    // V-056
    for (_, entries) in ids.iter().filter(|(_, n)| n.len() > 1) {
        let names: Vec<&str> = entries.iter().map(|(n, _)| *n).collect();
        for (name, id_shown) in entries {
            ctx.error(
                "V-056",
                &At::config(pipeline_at(name).path.key("id")),
                None,
                format!("id {id_shown} is used by pipelines {}", names.join(", ")),
            );
        }
    }
    // V-058 (uniqueness per source server: host + port)
    for ((_, port, _), entries) in slots.iter().filter(|(_, n)| n.len() > 1) {
        let names: Vec<&str> = entries.iter().map(|(n, _, _)| *n).collect();
        for (name, host_shown, slot_shown) in entries {
            ctx.error(
                "V-058",
                &At::config(
                    pipeline_at(name)
                        .path
                        .key("source")
                        .key("postgres")
                        .key("slot"),
                ),
                None,
                format!(
                    "slot {slot_shown} on server {host_shown}:{port} is used by pipelines {}",
                    names.join(", ")
                ),
            );
        }
    }
    // V-059 (uniqueness recommended per source database: host + port + db)
    for ((_, port, _, _), entries) in publications.iter().filter(|(_, n)| n.len() > 1) {
        let names: Vec<&str> = entries.iter().map(|(n, _, _, _)| *n).collect();
        for (name, host_shown, db_shown, publication_shown) in entries {
            ctx.warning(
                "V-059",
                &pipeline_at(name)
                    .key("source")
                    .key("postgres")
                    .key("publication"),
                None,
                format!(
                    "publication {publication_shown} on server {host_shown}:{port}/{db_shown} is shared by pipelines {}",
                    names.join(", ")
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use crate::diag::Locus;

    #[test]
    fn charsets() {
        assert!(
            is_pipeline_id("orders_pg-1")
                && !is_pipeline_id("Orders")
                && !is_pipeline_id("-x")
                && !is_pipeline_id(&"a".repeat(64))
        );
        assert!(
            is_pipeline_name("orders-pg.v2") && !is_pipeline_name(".x") && !is_pipeline_name("a b")
        );
        assert!(
            is_slot_name("_orders_slot") && !is_slot_name("orders-slot") && !is_slot_name("9x")
        );
        assert!(is_pg_identifier("Orders_Pub$1") && !is_pg_identifier("orders-pub"));
    }

    #[test]
    fn v058_slot_format_uses_config_locus() {
        let mut env = BTreeMap::new();
        env.insert("PW".into(), "pw".into());

        let config = r#"
config_version: 1
globals:
  state:
    bucket: app-state
pipelines:
  p:
    id: p
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: "${PW}"
      postgres:
        publication: p_pub
        slot: "Bad-Slot"
    tables:
      include:
        - public.orders
    sink:
      type: delta
      bucket: lake
      delta: {}
"#;

        let report = crate::validate_str(config, &env);
        let v058_format = report
            .iter()
            .find(|d| d.rule == "V-058" && d.message.contains("must match"));
        assert!(
            v058_format.is_some(),
            "expected V-058 format diagnostic, got: {:#?}",
            report
        );
        let d = v058_format.unwrap();
        assert_eq!(d.locus, Locus::Config);
        assert!(d.path.ends_with("source.postgres.slot"));
    }
}
