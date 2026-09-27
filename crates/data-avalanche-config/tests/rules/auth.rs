//! §13.5 catalog authentication and §13.6 management-API security.

use serde_json::{Value, json};

use crate::cases::{Case, Edit, Expect::*, case, pg_delta, pg_iceberg};
use crate::support::{cert, key};

fn hive(block: Value) -> Value {
    pg_iceberg().set(
        "/pipelines/p/sink/iceberg/catalog",
        json!({ "hive": block }),
    )
}

fn rest(extra: Value) -> Value {
    let mut block = json!({"url": "https://catalog.example.com"});
    block
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/rest", block)
}

pub fn cases() -> Vec<Case> {
    vec![
        case("V-040", Error, "ldap without credentials", || {
            hive(json!({"url": "thrift://hms:9083", "auth": "ldap"}))
        }),
        case("V-040", Silent, "ldap with credentials_file", || {
            hive(json!({"url": "thrift://hms:9083", "auth": "ldap", "credentials_file": "/x"}))
        }),
        case("V-041", Error, "kerberos without keytab", || {
            hive(
                json!({"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc"}}),
            )
        }),
        case("V-041", Silent, "kerberos complete", || {
            hive(
                json!({"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc", "keytab": "/k"}}),
            )
        }),
        case("V-042", Warning, "ldap in cleartext", || {
            hive(json!({"url": "thrift://hms:9083", "auth": "ldap", "user": "u", "password": "p"}))
        }),
        case("V-042", Silent, "ldap over tls", || {
            hive(
                json!({"url": "thrift://hms:9083", "auth": "ldap", "user": "u", "password": "p", "tls": {"enabled": true, "ca": cert()}}),
            )
        }),
        case("V-043", Warning, "tls without ca", || {
            hive(json!({"url": "thrift://hms:9083", "tls": {"enabled": true}}))
        }),
        case("V-043", Silent, "tls with ca", || {
            hive(json!({"url": "thrift://hms:9083", "tls": {"enabled": true, "ca": cert()}}))
        }),
        case("V-044", Error, "bearer without token", || {
            rest(json!({"auth": "bearer"}))
        }),
        case("V-044", Silent, "bearer with token_file", || {
            rest(json!({"auth": "bearer", "token_file": "/t"}))
        }),
        case("V-045", Error, "oauth2 without token_url", || {
            rest(json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b"}}))
        }),
        case("V-045", Error, "oauth2 without client", || {
            rest(json!({"auth": "oauth2", "oauth2": {"token_url": "https://idp/t"}}))
        }),
        case("V-045", Silent, "oauth2 complete", || {
            rest(
                json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b", "token_url": "https://idp/t"}}),
            )
        }),
        case("V-046", Error, "sigv4 without region", || {
            rest(json!({"auth": "sigv4", "sigv4": {"service": "glue"}}))
        }),
        case("V-046", Silent, "sigv4 complete", || {
            rest(json!({"auth": "sigv4", "sigv4": {"region": "eu-west-1", "service": "glue"}}))
        }),
        case("V-047", Error, "sigv4 on google", || {
            rest(json!({"auth": "sigv4", "sigv4": {"region": "eu-west-1", "service": "glue"}}))
                .set("/globals/cloud", json!("google"))
        }),
        case("V-047", Silent, "sigv4 on aws", || {
            rest(json!({"auth": "sigv4", "sigv4": {"region": "eu-west-1", "service": "glue"}}))
        }),
        case("V-048", Warning, "built-in token endpoint", || {
            rest(
                json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b", "token_url": "https://catalog.example.com/v1/oauth/tokens"}}),
            )
        }),
        case("V-048", Silent, "external IdP", || {
            rest(
                json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b", "token_url": "https://idp.example.com/token"}}),
            )
        }),
        case("V-049", Error, "basic without password", || {
            pg_delta().set("/globals/api", json!({"auth": "basic", "user": "admin"}))
        }),
        case("V-049", Silent, "basic with credentials_file", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "basic", "credentials_file": "${NONE:-/x}"}),
            )
        }),
        case("V-050", Error, "jwt without jwks_url", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "jwt", "jwt": {"issuer": "https://idp"}}),
            )
        }),
        case("V-050", Silent, "jwt complete", || {
            pg_delta().set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
        }),
        case("V-051", Error, "oauth2 without key source", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp"}}),
            )
        }),
        case("V-051", Silent, "oauth2 with discovery", || {
            pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "discovery_url": "https://idp/.well-known/openid-configuration"}}))
        }),
        case("V-052", Warning, "jwt without tls", || {
            pg_delta().set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
        }),
        case("V-052", Silent, "jwt over tls", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "tls": {"enabled": true, "cert": cert(), "key": key()}
            }))
        }),
        case("V-053", Error, "api tls without key", || {
            pg_delta().set("/globals/api/tls", json!({"enabled": true, "cert": cert()}))
        }),
        case("V-053", Silent, "api tls complete", || {
            pg_delta().set(
                "/globals/api/tls",
                json!({"enabled": true, "cert": cert(), "key": key()}),
            )
        }),
        case("V-054", Error, "gossip tls without cert", || {
            pg_delta().set(
                "/globals/cluster/memberlist/tls",
                json!({"enabled": true, "key": key()}),
            )
        }),
        case("V-054", Silent, "gossip tls disabled", || {
            pg_delta().set("/globals/cluster/memberlist/tls", json!({"enabled": false}))
        }),
        case("V-099", Error, "unknown role", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "role_mapping": {"superuser": ["cdc-admins"]}
            }))
        }),
        case("V-099", Silent, "mapped roles", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "role_mapping": {"admin": ["cdc-admins"], "viewer": ["everyone"]}
            }))
        }),
        case("V-100", Warning, "mapping with basic auth", || {
            pg_delta().set("/globals/api", json!({"auth": "basic", "user": "a", "password": "b", "role_mapping": {"admin": ["x"]}}))
        }),
        case("V-100", Silent, "mapping with jwt", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "role_mapping": {"admin": ["x"]}
            }))
        }),
    ]
}
