//! §13.4 cross-field and conditional rules.

use serde_json::json;

use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta, pg_iceberg};

pub fn cases() -> Vec<Case> {
    vec![
        case("V-028", Error, "glue on google", || {
            pg_iceberg().set("/globals/cloud", json!("google")).set(
                "/pipelines/p/sink/iceberg/catalog",
                json!({"glue": {"name": "123"}}),
            )
        }),
        case("V-028", Silent, "glue on aws", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog",
                json!({"glue": {"name": "123"}}),
            )
        }),
        case("V-029", Error, "sigv4 on azure", || {
            pg_iceberg().set("/globals/cloud", json!("azure")).set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("sigv4"),
            )
        }),
        case("V-029", Silent, "sigv4 on aws", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("sigv4"),
            )
        }),
        case("V-030", Error, "ssl allow on mysql", || {
            mysql_delta().set("/pipelines/p/source/ssl_mode", json!("allow"))
        }),
        case("V-030", Silent, "ssl allow on postgres", || {
            pg_delta().set("/pipelines/p/source/ssl_mode", json!("allow"))
        }),
        case("V-073", Error, "renewal 6s of 15s", || {
            pg_delta().set("/globals/cluster/lease_renewal_interval", json!("6s"))
        }),
        case(
            "V-073",
            Error,
            "duration shortened below 3x the default renewal",
            || pg_delta().set("/globals/cluster/lease_duration", json!("12s")),
        ),
        case("V-073", Silent, "renewal exactly a third", || {
            pg_delta()
                .set("/globals/cluster/lease_duration", json!("30s"))
                .set("/globals/cluster/lease_renewal_interval", json!("10s"))
        }),
        case("V-033", Error, "glob key", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"public.*": "always"}),
            )
        }),
        case("V-033", Error, "key without schema", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"orders": "always"}),
            )
        }),
        case("V-033", Silent, "schema.table key", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"public.orders": "always"}),
            )
        }),
        case("V-035", Error, "all triggers null", || {
            pg_delta().set(
                "/pipelines/p/sink/flush",
                json!({"seconds": null, "rows": null, "bytes": null}),
            )
        }),
        case("V-035", Silent, "rows only", || {
            pg_delta().set(
                "/pipelines/p/sink/flush",
                json!({"seconds": null, "bytes": null}),
            )
        }),
        case("V-036", Warning, "time trigger off", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(null))
        }),
        case("V-036", Silent, "time trigger on", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(60))
        }),
        case("V-037", Warning, "seconds 2", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(2))
        }),
        case("V-037", Silent, "seconds 0 = every transaction", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(0))
        }),
        case("V-037", Silent, "seconds 5", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(5))
        }),
        case("V-038", Warning, "parallelism 5", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(5))
        }),
        case("V-038", Silent, "parallelism 4", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(4))
        }),
        case("V-064", Error, "mysql include with db prefix", || {
            mysql_delta().set("/pipelines/p/tables/include", json!(["events.orders"]))
        }),
        case("V-064", Error, "mysql override with prefix", || {
            mysql_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"events.orders": "always"}),
            )
        }),
        case("V-064", Silent, "mysql bare names", || {
            mysql_delta()
                .set(
                    "/pipelines/p/tables/include",
                    json!(["orders", "payment_*"]),
                )
                .set(
                    "/pipelines/p/tables/snapshots/overrides",
                    json!({"orders": "always"}),
                )
        }),
        case("V-077", Warning, "re_query with always", || {
            pg_delta()
                .set("/pipelines/p/source/toast_strategy", json!("re_query"))
                .set("/pipelines/p/tables/snapshots/mode", json!("always"))
        }),
        case("V-077", Silent, "carry_forward with always", || {
            pg_delta().set("/pipelines/p/tables/snapshots/mode", json!("always"))
        }),
        case("V-085", Error, "strategy with changelog", || {
            pg_iceberg()
                .set("/pipelines/p/sink/write_mode", json!("changelog"))
                .set("/pipelines/p/sink/iceberg/strategy", json!("mor"))
        }),
        case("V-085", Silent, "strategy with mirror", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/strategy", json!("cow"))
        }),
        case("V-086", Error, "day transform on delta", || {
            pg_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "_ts", "transform": "day"}]}),
            )
        }),
        case("V-086", Silent, "identity on delta", || {
            pg_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "region", "transform": "identity"}]}),
            )
        }),
        case("V-086", Silent, "day on iceberg", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "_ts", "transform": "day"}]}),
            )
        }),
        case("V-087", Silent, "time and uuid map to delta", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.t": "time", "public.orders.u": "list<uuid>"}),
            )
        }),
        case("V-093", Error, "managed property", || {
            pg_delta().set(
                "/pipelines/p/sink/delta/table_properties",
                json!({"delta.appendOnly": "true"}),
            )
        }),
        case("V-093", Error, "fence retention property", || {
            pg_delta().set(
                "/pipelines/p/sink/delta/table_properties",
                json!({"delta.setTransactionRetentionDuration": "interval 1 day"}),
            )
        }),
        case("V-093", Silent, "allow-listed and user keys", || {
            pg_delta().set(
                "/pipelines/p/sink/delta/table_properties",
                json!({"delta.targetFileSize": 134217728, "owner": "team-x"}),
            )
        }),
        case("V-096", Error, "dns without name", || {
            pg_delta().set("/globals/cluster/discovery", json!("dns"))
        }),
        case("V-096", Silent, "dns with name", || {
            pg_delta()
                .set("/globals/cluster/discovery", json!("dns"))
                .set("/globals/cluster/discovery_dns_name", json!("cdc-headless"))
        }),
        case("V-098", Warning, "ratio without endpoint", || {
            pg_delta().set("/globals/tracing/sample_ratio", json!(0.1))
        }),
        case("V-098", Silent, "ratio 0 without endpoint", || {
            pg_delta().set("/globals/tracing/sample_ratio", json!(0))
        }),
        case(
            "V-103",
            Error,
            "require_existing with default cleanup",
            || {
                pg_delta().set(
                    "/pipelines/p/source/postgres/slot_management",
                    json!("require_existing"),
                )
            },
        ),
        case("V-103", Silent, "require_existing without cleanup", || {
            pg_delta()
                .set(
                    "/pipelines/p/source/postgres/slot_management",
                    json!("require_existing"),
                )
                .set("/pipelines/p/source/postgres/auto_cleanup", json!(false))
        }),
        case("V-106", Error, "glob primary_key key", || {
            pg_delta().set(
                "/pipelines/p/tables/primary_key",
                json!({"public.*": ["id"]}),
            )
        }),
        case("V-106", Error, "empty column list", || {
            pg_delta().set(
                "/pipelines/p/tables/primary_key",
                json!({"public.orders": []}),
            )
        }),
        case("V-106", Error, "mysql partitioning key with prefix", || {
            mysql_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"events.orders": [{"column": "region", "transform": "identity"}]}),
            )
        }),
        case("V-106", Silent, "override identifier", || {
            pg_delta().set(
                "/pipelines/p/tables/primary_key",
                json!({"public.orders": ["tenant_id", "order_id"]}),
            )
        }),
        // V-117: PostgreSQL patterns are schema.table
        case("V-117", Error, "pg include without a schema", || {
            pg_delta().set("/pipelines/p/tables/include", json!(["orders"]))
        }),
        case("V-117", Error, "pg exclude with three parts", || {
            pg_delta().set("/pipelines/p/tables/exclude", json!(["public.orders.x"]))
        }),
        case("V-117", Error, "pg include with an empty schema", || {
            pg_delta().set("/pipelines/p/tables/include", json!([".orders"]))
        }),
        case("V-117", Silent, "pg globs in either part", || {
            pg_delta().set(
                "/pipelines/p/tables/include",
                json!(["public.*", "*.orders"]),
            )
        }),
    ]
}
