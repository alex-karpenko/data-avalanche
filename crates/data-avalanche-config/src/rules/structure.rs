//! §13.1 structure and mutual exclusion, §13.2 requiredness.

use super::{missing, pipeline_at};
use crate::ctx::{At, Ctx, shown_value};
use crate::diag::PARSE;
use crate::enums::{CatalogKind, SinkType, SourceType};
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;

pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    let root = At::config(YamlPath::root());
    // V-014
    match &raw.config_version {
        None => ctx.error(
            "V-014",
            &root.key("config_version"),
            None,
            "config_version is required (current: 1)",
        ),
        Some(l) => {
            if let Some(v) = ctx.value(l, &root.key("config_version"))
                && v != 1
            {
                ctx.error(
                    "V-014",
                    &root.key("config_version"),
                    l.loc(),
                    format!("config_version {} is not supported by this binary, which reads version 1; migrate the file to version 1", shown_value(l, &v)),
                );
            }
        }
    }
    // V-005, V-006
    match &raw.globals {
        None => ctx.error("V-005", &root.key("globals"), None, "globals is required"),
        Some(g) => {
            let at = root.key("globals").key("state").key("bucket");
            let bucket = g.state.as_ref().and_then(|s| s.bucket.clone());
            if missing(ctx, &bucket, &at) {
                ctx.error(
                    "V-006",
                    &at,
                    bucket.and_then(|b| b.loc()),
                    "globals.state.bucket is required",
                );
            }
            if let Some(api) = &g.api {
                credentials_exclusive(
                    ctx,
                    &root.key("globals").key("api"),
                    &api.user,
                    &api.password,
                    &api.credentials_file,
                );
            }
        }
    }
    // V-004
    if raw.pipelines.is_none() {
        ctx.error(
            "V-004",
            &root.key("pipelines"),
            None,
            "pipelines is required (use {} for a deployment with no pipelines)",
        );
    }
    for (name, p) in merged {
        pipeline(name, p, ctx);
    }
}

/// V-065: `credentials_file` excludes `user`/`password` in the same block.
fn credentials_exclusive(
    ctx: &mut Ctx,
    at: &At,
    user: &Option<Leaf<String>>,
    password: &Option<Leaf<String>>,
    file: &Option<Leaf<String>>,
) {
    if let Some(f) = file
        && (user.is_some() || password.is_some())
    {
        ctx.error(
            "V-065",
            &at.key("credentials_file"),
            f.loc(),
            "credentials_file and user/password are mutually exclusive: give one",
        );
    }
}

fn pipeline(name: &str, p: &RawPipeline, ctx: &mut Ctx) {
    let at = pipeline_at(name);
    let default = RawSource::default();
    let src = p.source.as_ref().unwrap_or(&default);
    source(src, &at.key("source"), ctx);
    // V-007: absent, explicit `null` (no list), or an empty list are all "no include".
    let include_at = at.key("tables").key("include");
    let include = p
        .tables
        .as_ref()
        .and_then(|t| t.include.as_ref())
        .and_then(|inner| inner.as_ref());
    if include.is_none_or(|v| v.is_empty()) {
        let loc = include.and_then(|v| v.first()).and_then(|l| l.loc());
        ctx.error(
            "V-007",
            &include_at,
            loc,
            "tables.include is required and must not be empty",
        );
    }
    let default = RawSink::default();
    sink(p.sink.as_ref().unwrap_or(&default), &at.key("sink"), ctx);
    required_fields(p, &at, ctx);
}

/// Required fields that §13 gives no V-rule: a strict parse would report them as missing, but the
/// raw model keeps every field optional so that layers can merge, so they are checked after the
/// merge and reported as `PARSE`.
fn required_fields(p: &RawPipeline, at: &At, ctx: &mut Ctx) {
    // F4: spec §11 says any required value that resolves to empty is an error, not just an absent
    // one (`name: ""` must fail exactly as an absent `name` would). Same rule as `missing()`
    // (`rules::missing`), generalized: these fields aren't all `Leaf<String>`, and in the
    // structural pass an unresolved template isn't empty, so it can't yet fail here.
    fn require<T>(ctx: &mut Ctx, at: &At, key: &str, leaf: &Option<Leaf<T>>) {
        let missing = match leaf {
            None => true,
            Some(l) => ctx
                .text(l, &at.key(key))
                .is_some_and(|t| t.trim().is_empty()),
        };
        if missing {
            ctx.error(
                PARSE,
                &at.key(key),
                None,
                format!("missing required field `{key}`"),
            );
        }
    }
    let catalog = p
        .sink
        .as_ref()
        .and_then(|s| s.iceberg.as_ref())
        .and_then(|i| i.catalog.as_ref());
    if let Some(c) = catalog {
        let c_at = at.key("sink").key("iceberg").key("catalog");
        if let Some(g) = &c.glue {
            require(ctx, &c_at.key("glue"), "name", &g.name);
        }
        if let Some(h) = &c.hive {
            require(ctx, &c_at.key("hive"), "url", &h.url);
        }
        if let Some(r) = &c.rest {
            require(ctx, &c_at.key("rest"), "url", &r.url);
        }
    }
    let partitioning = p.tables.as_ref().and_then(|t| t.partitioning.as_ref());
    for (table, fields) in partitioning.into_iter().flatten() {
        let t_at = at.key("tables").key("partitioning").key(table);
        for (i, f) in fields.iter().enumerate() {
            require(ctx, &t_at.index(i), "column", &f.column);
            require(ctx, &t_at.index(i), "transform", &f.transform);
        }
    }
}

fn source(s: &RawSource, at: &At, ctx: &mut Ctx) {
    // V-008, V-009
    if missing(ctx, &s.host, &at.key("host")) {
        ctx.error(
            "V-008",
            &at.key("host"),
            s.host.as_ref().and_then(|l| l.loc()),
            "source.host is required (on the pipeline or in defaults)",
        );
    }
    if missing(ctx, &s.db, &at.key("db")) {
        ctx.error(
            "V-009",
            &at.key("db"),
            s.db.as_ref().and_then(|l| l.loc()),
            "source.db is required (on the pipeline or in defaults)",
        );
    }
    // V-065, V-010
    credentials_exclusive(ctx, at, &s.user, &s.password, &s.credentials_file);
    if s.credentials_file.is_none() {
        for (key, leaf) in [("user", &s.user), ("password", &s.password)] {
            if missing(ctx, leaf, &at.key(key)) {
                ctx.error(
                    "V-010",
                    &at.key(key),
                    leaf.as_ref().and_then(|l| l.loc()),
                    format!("source.{key} is required unless credentials_file is given"),
                );
            }
        }
    }
    // V-001, V-003, V-011
    let kind = if s.kind.is_none() {
        Some(SourceType::Postgres)
    } else {
        ctx.peek(&s.kind)
    };
    let Some(kind) = kind else { return };
    let (own, foreign, own_present, foreign_present) = match kind {
        SourceType::Postgres => ("postgres", "mysql", s.postgres.is_some(), s.mysql.is_some()),
        SourceType::Mysql => ("mysql", "postgres", s.mysql.is_some(), s.postgres.is_some()),
    };
    // Every `mysql` field has a default, so only a missing `postgres` block is an error (D-M1.5).
    if !own_present && kind == SourceType::Postgres {
        ctx.error(
            "V-001",
            &at.key(own),
            None,
            "source.type is postgres but there is no source.postgres block (publication and slot are required)",
        );
    }
    if foreign_present {
        ctx.error(
            "V-003",
            &at.key(foreign),
            None,
            format!("source.{foreign} is not allowed when source.type is {own}"),
        );
    }
    if let (SourceType::Postgres, Some(pg)) = (kind, &s.postgres) {
        let pg_at = at.key("postgres");
        for (key, leaf) in [("publication", &pg.publication), ("slot", &pg.slot)] {
            if missing(ctx, leaf, &pg_at.key(key)) {
                ctx.error(
                    "V-011",
                    &pg_at.key(key),
                    leaf.as_ref().and_then(|l| l.loc()),
                    format!("source.postgres.{key} is required"),
                );
            }
        }
    }
}

fn sink(s: &RawSink, at: &At, ctx: &mut Ctx) {
    // V-091, V-012
    if s.kind.is_none() {
        ctx.error(
            "V-091",
            &at.key("type"),
            None,
            "sink.type is required (iceberg or delta; there is no default)",
        );
    }
    if missing(ctx, &s.bucket, &at.key("bucket")) {
        ctx.error(
            "V-012",
            &at.key("bucket"),
            s.bucket.as_ref().and_then(|l| l.loc()),
            "sink.bucket is required (on the pipeline or in defaults)",
        );
    }
    // V-084
    let Some(kind) = ctx.peek(&s.kind) else {
        return;
    };
    let (own, foreign, own_present, foreign_present) = match kind {
        SinkType::Iceberg => ("iceberg", "delta", s.iceberg.is_some(), s.delta.is_some()),
        SinkType::Delta => ("delta", "iceberg", s.delta.is_some(), s.iceberg.is_some()),
    };
    // Every `delta` field has a default, so only a missing `iceberg` block is an error (D-M1.5).
    if !own_present && kind == SinkType::Iceberg {
        ctx.error(
            "V-084",
            &at.key(own),
            None,
            "sink.type is iceberg but there is no sink.iceberg block (catalog is required)",
        );
    }
    if foreign_present {
        ctx.error(
            "V-084",
            &at.key(foreign),
            None,
            format!("sink.{foreign} is not allowed when sink.type is {own}"),
        );
    }
    // V-013, V-002 and the catalog's exclusivity rules
    if let (SinkType::Iceberg, Some(iceberg)) = (kind, &s.iceberg) {
        let c_at = at.key("iceberg").key("catalog");
        match &iceberg.catalog {
            None => ctx.error(
                "V-013",
                &c_at,
                None,
                "sink.iceberg.catalog is required for an Iceberg sink",
            ),
            Some(c) => catalog(c, &c_at, ctx),
        }
    }
}

fn catalog(c: &RawCatalog, at: &At, ctx: &mut Ctx) {
    let present: Vec<CatalogKind> = [
        (CatalogKind::Glue, c.glue.is_some()),
        (CatalogKind::Hive, c.hive.is_some()),
        (CatalogKind::Rest, c.rest.is_some()),
    ]
    .into_iter()
    .filter_map(|(k, p)| p.then_some(k))
    .collect();
    match present.as_slice() {
        [] => ctx.error(
            "V-002",
            at,
            None,
            "sink.iceberg.catalog needs exactly one of glue, hive, rest",
        ),
        [one] => {
            if let Some(declared) = ctx.peek(&c.kind)
                && declared != *one
            {
                let l = c.kind.as_ref().expect("peek returned Some");
                ctx.error(
                    "V-002",
                    &at.key("type"),
                    l.loc(),
                    format!(
                        "catalog type is {} but the block present is {one}",
                        shown_value(l, &declared)
                    ),
                );
            }
        }
        many => {
            let names: Vec<_> = many.iter().map(|k| k.as_str()).collect();
            ctx.error(
                "V-002",
                at,
                None,
                format!(
                    "sink.iceberg.catalog has {} blocks; give exactly one",
                    names.join(" and ")
                ),
            );
        }
    }
    if let Some(h) = &c.hive {
        credentials_exclusive(
            ctx,
            &at.key("hive"),
            &h.user,
            &h.password,
            &h.credentials_file,
        );
    }
    if let Some(r) = &c.rest {
        let r_at = at.key("rest");
        if let (Some(f), Some(_)) = (&r.token_file, &r.token) {
            ctx.error(
                "V-070",
                &r_at.key("token_file"),
                f.loc(),
                "token_file and token are mutually exclusive: give one",
            );
        }
        if let Some(o) = &r.oauth2
            && let Some(f) = &o.credentials_file
            && (o.client_id.is_some() || o.client_secret.is_some())
        {
            ctx.error("V-066", &r_at.key("oauth2").key("credentials_file"), f.loc(), "oauth2.credentials_file and client_id/client_secret are mutually exclusive: give one");
        }
    }
}
