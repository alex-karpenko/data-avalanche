//! §13.5 catalog authentication (the nine rules of `Catalog_Auth_Research.md`) and §13.6
//! management-API security.

use super::{missing, pipeline_at};
use crate::ctx::{At, Ctx};
use crate::enums::*;
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;

type L = Option<Leaf<String>>;

/// `user`+`password` or a `credentials_file` (V-010-style requirement for V-040/V-049).
fn require_credentials(
    ctx: &mut Ctx,
    rule: &str,
    at: &At,
    user: &L,
    password: &L,
    file: &L,
    what: &str,
) {
    if file.is_some() {
        return;
    }
    for (key, leaf) in [("user", user), ("password", password)] {
        if missing(ctx, leaf, &at.key(key)) {
            ctx.error(
                rule,
                &at.key(key),
                leaf.as_ref().and_then(|l| l.loc()),
                format!("{what} needs user and password, or credentials_file"),
            );
        }
    }
}

fn require(ctx: &mut Ctx, rule: &str, at: &At, key: &str, leaf: &L, what: &str) {
    if missing(ctx, leaf, &at.key(key)) {
        ctx.error(
            rule,
            &at.key(key),
            leaf.as_ref().and_then(|l| l.loc()),
            format!("{what} needs {key}"),
        );
    }
}

pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    let default = RawGlobals::default();
    let g = raw.globals.as_ref().unwrap_or(&default);
    let cloud = if g.cloud.is_none() {
        Some(Cloud::Aws)
    } else {
        ctx.peek(&g.cloud)
    };
    if let Some(api) = &g.api {
        management_api(
            api,
            &At::config(YamlPath::root().key("globals").key("api")),
            ctx,
        );
    }
    for (name, p) in merged {
        let is_iceberg = p
            .sink
            .as_ref()
            .is_some_and(|s| ctx.peek(&s.kind) == Some(SinkType::Iceberg));
        let Some(c) = p
            .sink
            .as_ref()
            .and_then(|s| s.iceberg.as_ref())
            .and_then(|i| i.catalog.as_ref())
        else {
            continue;
        };
        if !is_iceberg {
            continue;
        }
        let at = pipeline_at(name).key("sink").key("iceberg").key("catalog");
        if let Some(h) = &c.hive {
            hive(h, &at.key("hive"), ctx);
        }
        if let Some(r) = &c.rest {
            rest(r, &at.key("rest"), cloud, ctx);
        }
    }
}

fn hive(h: &RawHive, at: &At, ctx: &mut Ctx) {
    let tls_enabled = h
        .tls
        .as_ref()
        .is_some_and(|t| ctx.peek(&t.enabled) == Some(true));
    match ctx.peek(&h.auth) {
        Some(HiveAuth::Ldap) => {
            // V-040, V-042
            require_credentials(
                ctx,
                "V-040",
                at,
                &h.user,
                &h.password,
                &h.credentials_file,
                "hive auth ldap",
            );
            if !tls_enabled {
                ctx.warning(
                    "V-042",
                    &at.key("auth"),
                    h.auth.as_ref().and_then(|l| l.loc()),
                    "hive auth ldap without tls.enabled sends the password in cleartext",
                );
            }
        }
        Some(HiveAuth::Kerberos) => {
            // V-041
            let k = h.kerberos.clone().unwrap_or_default();
            let k_at = at.key("kerberos");
            require(
                ctx,
                "V-041",
                &k_at,
                "principal",
                &k.principal,
                "hive auth kerberos",
            );
            require(
                ctx,
                "V-041",
                &k_at,
                "keytab",
                &k.keytab,
                "hive auth kerberos",
            );
        }
        _ => {}
    }
    // V-043: a private CA can't be detected from the config, so warn whenever TLS has no CA.
    if tls_enabled && h.tls.as_ref().is_some_and(|t| t.ca.is_none()) {
        ctx.warning("V-043", &at.key("tls"), None, "hive tls without tls.ca: if the HMS certificate is signed by a private CA, set tls.ca or validation fails against the system store");
    }
}

fn rest(r: &RawRest, at: &At, cloud: Option<Cloud>, ctx: &mut Ctx) {
    match ctx.peek(&r.auth) {
        Some(RestAuth::Bearer) => {
            // V-044
            if r.token_file.is_none() && missing(ctx, &r.token, &at.key("token")) {
                ctx.error(
                    "V-044",
                    &at.key("token"),
                    r.auth.as_ref().and_then(|l| l.loc()),
                    "rest auth bearer needs token or token_file",
                );
            }
        }
        Some(RestAuth::Oauth2) => {
            // V-045, V-048
            let o = r.oauth2.clone().unwrap_or_default();
            let o_at = at.key("oauth2");
            require(
                ctx,
                "V-045",
                &o_at,
                "token_url",
                &o.token_url,
                "rest auth oauth2",
            );
            if o.credentials_file.is_none() {
                require(
                    ctx,
                    "V-045",
                    &o_at,
                    "client_id",
                    &o.client_id,
                    "rest auth oauth2 without credentials_file",
                );
                require(
                    ctx,
                    "V-045",
                    &o_at,
                    "client_secret",
                    &o.client_secret,
                    "rest auth oauth2 without credentials_file",
                );
            }
            let url = r.url.as_ref().and_then(|l| ctx.text(l, &at.key("url")));
            let token_url = o
                .token_url
                .as_ref()
                .and_then(|l| ctx.text(l, &o_at.key("token_url")));
            if let (Some(url), Some(token_url)) = (url, token_url)
                && token_url.trim_end_matches('/')
                    == format!("{}/v1/oauth/tokens", url.trim_end_matches('/'))
            {
                ctx.warning("V-048", &o_at.key("token_url"), o.token_url.as_ref().and_then(|l| l.loc()), "token_url is the catalog's built-in /v1/oauth/tokens endpoint, deprecated in Iceberg 1.6 and removed in 2.0; point it at the identity provider");
            }
        }
        Some(RestAuth::Sigv4) => {
            // V-046, V-047
            let s = r.sigv4.clone().unwrap_or_default();
            let s_at = at.key("sigv4");
            require(ctx, "V-046", &s_at, "region", &s.region, "rest auth sigv4");
            require(
                ctx,
                "V-046",
                &s_at,
                "service",
                &s.service,
                "rest auth sigv4",
            );
            if cloud.is_some_and(|c| c != Cloud::Aws) {
                ctx.error(
                    "V-047",
                    &at.key("auth"),
                    r.auth.as_ref().and_then(|l| l.loc()),
                    "rest auth sigv4 requires globals.cloud aws",
                );
            }
        }
        _ => {}
    }
}

fn management_api(a: &RawApi, at: &At, ctx: &mut Ctx) {
    let auth = if a.auth.is_none() {
        Some(ApiAuth::None)
    } else {
        ctx.peek(&a.auth)
    };
    let tls_enabled = a
        .tls
        .as_ref()
        .is_some_and(|t| ctx.peek(&t.enabled) == Some(true));
    match auth {
        // V-049
        Some(ApiAuth::Basic) => require_credentials(
            ctx,
            "V-049",
            at,
            &a.user,
            &a.password,
            &a.credentials_file,
            "api auth basic",
        ),
        // V-050
        Some(ApiAuth::Jwt) => {
            let j = a.jwt.clone().unwrap_or_default();
            require(
                ctx,
                "V-050",
                &at.key("jwt"),
                "issuer",
                &j.issuer,
                "api auth jwt",
            );
            require(
                ctx,
                "V-050",
                &at.key("jwt"),
                "jwks_url",
                &j.jwks_url,
                "api auth jwt",
            );
        }
        // V-051
        Some(ApiAuth::Oauth2) => {
            let o = a.oauth2.clone().unwrap_or_default();
            let o_at = at.key("oauth2");
            require(ctx, "V-051", &o_at, "issuer", &o.issuer, "api auth oauth2");
            if missing(ctx, &o.jwks_url, &o_at.key("jwks_url"))
                && missing(ctx, &o.discovery_url, &o_at.key("discovery_url"))
            {
                ctx.error(
                    "V-051",
                    &o_at,
                    None,
                    "api auth oauth2 needs jwks_url or discovery_url",
                );
            }
        }
        _ => {}
    }
    // V-052
    if auth.is_some_and(|a| a != ApiAuth::None) && !tls_enabled {
        ctx.warning("V-052", &at.key("auth"), a.auth.as_ref().and_then(|l| l.loc()), "api auth without tls.enabled sends credentials in cleartext unless TLS is terminated upstream");
    }
    // V-099, V-100
    if let Some(m) = &a.role_mapping {
        for key in m.keys() {
            if key.parse::<Role>().is_err() {
                ctx.error(
                    "V-099",
                    &at.key("role_mapping").key(key),
                    None,
                    format!("role_mapping key {key:?} is not a role (viewer, operator, admin)"),
                );
            }
        }
        if matches!(auth, Some(ApiAuth::None | ApiAuth::Basic)) {
            ctx.warning(
                "V-100",
                &at.key("role_mapping"),
                None,
                "role_mapping is ignored unless api auth is jwt or oauth2",
            );
        }
    }
    // V-053
    if let Some(tls) = &a.tls
        && tls_enabled
    {
        require(ctx, "V-053", &at.key("tls"), "cert", &tls.cert, "api TLS");
        require(ctx, "V-053", &at.key("tls"), "key", &tls.key, "api TLS");
    }
}
