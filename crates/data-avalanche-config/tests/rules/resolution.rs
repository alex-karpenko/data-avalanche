//! §13.9 resolution and materialization (Phase 2): references, credential and token files, PEM,
//! other files, and the logical-type grammar.

use serde_json::json;

use crate::cases::{
    Case, Edit, Expect::*, Fixture, base_env, case, case_with, pg_delta, pg_iceberg,
};
use crate::support::{cert, key};

pub fn cases() -> Vec<Case> {
    vec![
        case("V-063", Error, "unset variable", || {
            pg_delta().set("/pipelines/p/source/password", json!("${NOT_SET}"))
        }),
        case("V-063", Error, "malformed reference", || {
            pg_delta().set("/pipelines/p/source/host", json!("db-${OPEN"))
        }),
        case("V-063", Silent, "default for an unset variable", || {
            pg_delta().set("/pipelines/p/source/host", json!("${NOT_SET:-db}"))
        }),
        case_with(
            "V-067",
            Error,
            "credentials file without password",
            |files| {
                let path = files.write("creds.json", r#"{"user": "cdc"}"#);
                Fixture {
                    doc: pg_delta()
                        .del("/pipelines/p/source/user")
                        .del("/pipelines/p/source/password")
                        .set("/pipelines/p/source/credentials_file", json!(path)),
                    env: base_env(),
                }
            },
        ),
        case_with(
            "V-067",
            Silent,
            "valid credentials file via a variable",
            |files| {
                let path = files.write("creds.json", r#"{"user": "cdc", "password": "pw"}"#);
                let mut env = base_env();
                env.insert("CREDS".into(), path.display().to_string());
                Fixture {
                    doc: pg_delta()
                        .del("/pipelines/p/source/user")
                        .del("/pipelines/p/source/password")
                        .set("/pipelines/p/source/credentials_file", json!("${CREDS}")),
                    env,
                }
            },
        ),
        case_with("V-068", Error, "key given as cert", |files| {
            let key = files.write("key.pem", key());
            Fixture {
                doc: pg_delta().set("/pipelines/p/source/ssl_cert", json!(key)),
                env: base_env(),
            }
        }),
        case("V-068", Silent, "inline certificate", || {
            pg_delta().set("/pipelines/p/source/ssl_ca", json!(cert()))
        }),
        case_with("V-069", Error, "empty keytab", |files| {
            let keytab = files.write("cdc.keytab", "");
            Fixture {
                doc: pg_iceberg().set(
                    "/pipelines/p/sink/iceberg/catalog",
                    json!({"hive": {"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc", "keytab": keytab}}}),
                ),
                env: base_env(),
            }
        }),
        case_with("V-069", Silent, "readable keytab", |files| {
            let keytab = files.write("cdc.keytab", "\u{5}\u{2}binary");
            Fixture {
                doc: pg_iceberg().set(
                    "/pipelines/p/sink/iceberg/catalog",
                    json!({"hive": {"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc", "keytab": keytab}}}),
                ),
                env: base_env(),
            }
        }),
        case_with("V-071", Error, "whitespace-only token file", |files| {
            let token = files.write("token", " \n");
            Fixture {
                doc: pg_iceberg()
                    .set(
                        "/pipelines/p/sink/iceberg/catalog/rest/auth",
                        json!("bearer"),
                    )
                    .set(
                        "/pipelines/p/sink/iceberg/catalog/rest/token_file",
                        json!(token),
                    ),
                env: base_env(),
            }
        }),
        case_with(
            "V-071",
            Silent,
            "token file with a trailing newline",
            |files| {
                let token = files.write("token", "tok\n");
                Fixture {
                    doc: pg_iceberg()
                        .set(
                            "/pipelines/p/sink/iceberg/catalog/rest/auth",
                            json!("bearer"),
                        )
                        .set(
                            "/pipelines/p/sink/iceberg/catalog/rest/token_file",
                            json!(token),
                        ),
                    env: base_env(),
                }
            },
        ),
        case("V-078", Error, "bare timestamp", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.ts": "timestamp"}),
            )
        }),
        case("V-078", Error, "decimal beyond 38 digits", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.amount": "decimal(40,2)"}),
            )
        }),
        case("V-078", Silent, "grammar types", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.amount": "decimal(38,10)", "public.orders.tags": "list<string>", "public.orders.at": "timestamp_ntz"}),
            )
        }),
    ]
}
