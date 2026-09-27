//! §13.1 structure and mutual exclusion, §13.2 requiredness.

use serde_json::json;

use data_avalanche_config::diag::PARSE;

use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta, pg_iceberg};

pub fn cases() -> Vec<Case> {
    vec![
        // V-001: the engine block matching source.type
        case("V-001", Error, "postgres without a postgres block", || {
            pg_delta().del("/pipelines/p/source/postgres")
        }),
        case(
            "V-001",
            Silent,
            "mysql block omitted: every field has a default",
            || mysql_delta().del("/pipelines/p/source/mysql"),
        ),
        case(
            "V-001",
            Silent,
            "engine block inherited from defaults",
            || {
                // The merged block lacks publication/slot (V-011), but it exists.
                pg_delta()
                    .set("/defaults/source/postgres", json!({"heartbeat": "30s"}))
                    .del("/pipelines/p/source/postgres")
            },
        ),
        // V-002: exactly one catalog sub-block, matching `type`
        case("V-002", Error, "no sub-block", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog",
                json!({"retry": {"max_attempts": 3}}),
            )
        }),
        case("V-002", Error, "two sub-blocks", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/glue",
                json!({"name": "a"}),
            )
        }),
        case("V-002", Error, "type contradicts block", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/type", json!("glue"))
        }),
        case("V-002", Silent, "type matches block", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/type", json!("rest"))
        }),
        // V-003: foreign engine block
        case("V-003", Error, "mysql block on a postgres source", || {
            pg_delta().set("/pipelines/p/source/mysql", json!({}))
        }),
        case(
            "V-003",
            Silent,
            "defaults' foreign block is dropped",
            || {
                pg_delta().set(
                    "/defaults/source/mysql",
                    json!({"replication_type": "gtid"}),
                )
            },
        ),
        // V-004, V-005
        case("V-004", Error, "no pipelines key", || {
            pg_delta().del("/pipelines")
        }),
        case("V-004", Silent, "empty pipelines map", || {
            pg_delta().set("/pipelines", json!({}))
        }),
        case("V-005", Error, "no globals", || pg_delta().del("/globals")),
        case("V-005", Silent, "globals present", pg_delta),
        // V-006
        case("V-006", Error, "no state bucket", || {
            pg_delta().set("/globals/state", json!({"prefix": "x"}))
        }),
        case("V-006", Error, "empty state bucket", || {
            pg_delta().set("/globals/state/bucket", json!(""))
        }),
        case("V-006", Silent, "state bucket set", pg_delta),
        // V-007
        case("V-007", Error, "no include", || {
            pg_delta().del("/pipelines/p/tables/include")
        }),
        case("V-007", Error, "empty include", || {
            pg_delta().set("/pipelines/p/tables/include", json!([]))
        }),
        case("V-007", Silent, "include from defaults", || {
            pg_delta()
                .set("/defaults/tables/include", json!(["public.*"]))
                .del("/pipelines/p/tables/include")
        }),
        // V-008, V-009, V-010
        case("V-008", Error, "no host", || {
            pg_delta().del("/pipelines/p/source/host")
        }),
        case("V-008", Silent, "host from defaults", || {
            pg_delta()
                .set("/defaults/source/host", json!("db"))
                .del("/pipelines/p/source/host")
        }),
        case("V-009", Error, "no db", || {
            pg_delta().del("/pipelines/p/source/db")
        }),
        case("V-009", Silent, "db set", pg_delta),
        case("V-010", Error, "user without password", || {
            pg_delta().del("/pipelines/p/source/password")
        }),
        case("V-010", Error, "password resolves empty", || {
            pg_delta().set("/pipelines/p/source/password", json!("${EMPTY:-}"))
        }),
        case("V-010", Silent, "credentials_file instead", || {
            pg_delta()
                .del("/pipelines/p/source/user")
                .del("/pipelines/p/source/password")
                .set(
                    "/pipelines/p/source/credentials_file",
                    json!("${NO_SUCH_FILE:-/x}"),
                )
        }),
        // V-011
        case("V-011", Error, "no slot", || {
            pg_delta().del("/pipelines/p/source/postgres/slot")
        }),
        case("V-011", Silent, "publication and slot", pg_delta),
        // V-012, V-013, V-091
        case("V-012", Error, "no sink bucket", || {
            pg_delta().del("/pipelines/p/sink/bucket")
        }),
        case("V-012", Silent, "bucket from defaults", || {
            pg_delta()
                .set("/defaults/sink/bucket", json!("lake"))
                .del("/pipelines/p/sink/bucket")
        }),
        case("V-013", Error, "iceberg without catalog", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg", json!({"strategy": "mor"}))
        }),
        case("V-013", Silent, "catalog from defaults", || {
            pg_iceberg()
                .set(
                    "/defaults/sink/iceberg/catalog",
                    json!({"glue": {"name": "123"}}),
                )
                .set("/pipelines/p/sink/iceberg", json!({}))
        }),
        case("V-091", Error, "no sink type", || {
            pg_delta().del("/pipelines/p/sink/type")
        }),
        case("V-091", Silent, "type from defaults", || {
            pg_delta()
                .set("/defaults/sink/type", json!("delta"))
                .del("/pipelines/p/sink/type")
        }),
        // V-014
        case("V-014", Error, "missing", || {
            pg_delta().del("/config_version")
        }),
        case("V-014", Error, "unsupported version", || {
            pg_delta().set("/config_version", json!(2))
        }),
        case("V-014", Silent, "version 1", pg_delta),
        // V-084
        case(
            "V-084",
            Silent,
            "delta block omitted: every field has a default",
            || pg_delta().del("/pipelines/p/sink/delta"),
        ),
        case("V-084", Error, "iceberg without iceberg block", || {
            pg_iceberg().del("/pipelines/p/sink/iceberg")
        }),
        case("V-084", Error, "iceberg block on a delta sink", || {
            pg_delta().set("/pipelines/p/sink/iceberg", json!({}))
        }),
        case(
            "V-084",
            Silent,
            "defaults' iceberg dropped for delta",
            || pg_delta().set("/defaults/sink/iceberg/strategy", json!("cow")),
        ),
        // V-065, V-066, V-070
        case("V-065", Error, "source credentials_file with user", || {
            pg_delta()
                .del("/pipelines/p/source/password")
                .set("/pipelines/p/source/credentials_file", json!("/x"))
        }),
        case("V-065", Error, "api credentials_file with password", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "basic", "password": "x", "credentials_file": "/x"}),
            )
        }),
        case("V-065", Silent, "user and password only", pg_delta),
        case(
            "V-066",
            Error,
            "oauth2 credentials_file with client_id",
            || {
                pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/oauth2",
                json!({"client_id": "a", "credentials_file": "/x", "token_url": "https://idp/t"}),
            )
            },
        ),
        case("V-066", Silent, "no oauth2 block", pg_iceberg),
        case("V-070", Error, "token and token_file", || {
            pg_iceberg()
                .set("/pipelines/p/sink/iceberg/catalog/rest/token", json!("t"))
                .set(
                    "/pipelines/p/sink/iceberg/catalog/rest/token_file",
                    json!("/x"),
                )
        }),
        case("V-070", Silent, "token only", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/rest/token", json!("t"))
        }),
        // Required fields with no V-rule are PARSE errors, checked after the merge.
        case(PARSE, Error, "rest without url", || {
            pg_iceberg().del("/pipelines/p/sink/iceberg/catalog/rest/url")
        }),
        case(PARSE, Silent, "rest url from defaults", || {
            pg_iceberg()
                .set(
                    "/defaults/sink/iceberg/catalog/rest/url",
                    json!("https://c.example.com"),
                )
                .del("/pipelines/p/sink/iceberg/catalog/rest/url")
        }),
        case(PARSE, Error, "glue without name", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog", json!({"glue": {}}))
        }),
        case(PARSE, Error, "hive without url", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog", json!({"hive": {}}))
        }),
        case(PARSE, Error, "partition field without transform", || {
            pg_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "region"}]}),
            )
        }),
        case(PARSE, Error, "partition field without column", || {
            pg_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"transform": "identity"}]}),
            )
        }),
    ]
}
