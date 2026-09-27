//! §13.3 value domain: enumerations (checked by coercion), ranges, reserved values, v1 rejections.

use serde_json::{Value, json};

use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta, pg_iceberg};

fn catalog(value: Value) -> Value {
    pg_iceberg().set("/pipelines/p/sink/iceberg/catalog", value)
}

pub fn cases() -> Vec<Case> {
    vec![
        // enumerations
        case("V-015", Error, "cloud gcp", || {
            pg_delta().set("/globals/cloud", json!("gcp"))
        }),
        case("V-015", Silent, "cloud google", || {
            pg_delta().set("/globals/cloud", json!("google"))
        }),
        case("V-016", Error, "api auth ldap", || {
            pg_delta().set("/globals/api/auth", json!("ldap"))
        }),
        case("V-016", Silent, "api auth none", || {
            pg_delta().set("/globals/api/auth", json!("none"))
        }),
        case("V-017", Error, "broken filter", || {
            pg_delta().set("/globals/log/level", json!("info,[="))
        }),
        case("V-017", Silent, "per-module filter", || {
            pg_delta().set(
                "/globals/log/level",
                json!("info,data_avalanche_config=debug"),
            )
        }),
        case("V-018", Error, "format xml", || {
            pg_delta().set("/globals/log/format", json!("xml"))
        }),
        case("V-018", Silent, "format pretty", || {
            pg_delta().set("/globals/log/format", json!("pretty"))
        }),
        case("V-019", Error, "source oracle", || {
            pg_delta().set("/pipelines/p/source/type", json!("oracle"))
        }),
        case("V-019", Silent, "source postgres", || {
            pg_delta().set("/pipelines/p/source/type", json!("postgres"))
        }),
        case("V-020", Error, "ssl_mode strict", || {
            pg_delta().set("/pipelines/p/source/ssl_mode", json!("strict"))
        }),
        case("V-020", Silent, "ssl_mode verify-full", || {
            pg_delta().set("/pipelines/p/source/ssl_mode", json!("verify-full"))
        }),
        // §13 intro (spec c591bdb): a diagnostic for a `defaults` value has locus config and a
        // `defaults.…` path, whatever V-020's own (pipeline) Locus column says.
        case(
            "V-020",
            Error,
            "ssl_mode invalid in defaults: config locus",
            || pg_delta().set("/defaults/source/ssl_mode", json!("nope")),
        ),
        case("V-021", Error, "replication_type position", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/replication_type",
                json!("position"),
            )
        }),
        case("V-021", Silent, "replication_type binlog", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/replication_type",
                json!("binlog"),
            )
        }),
        case("V-022", Error, "mode sometimes", || {
            pg_delta().set("/pipelines/p/tables/snapshots/mode", json!("sometimes"))
        }),
        case("V-022", Error, "override value", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"public.orders": "sometimes"}),
            )
        }),
        case("V-022", Silent, "mode dump_initial", || {
            pg_delta().set("/pipelines/p/tables/snapshots/mode", json!("dump_initial"))
        }),
        case("V-023", Error, "strategy append", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/strategy", json!("append"))
        }),
        case("V-023", Silent, "strategy cow", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/strategy", json!("cow"))
        }),
        case("V-025", Error, "rest auth basic", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("basic"),
            )
        }),
        case("V-025", Silent, "rest auth none", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/rest/auth", json!("none"))
        }),
        case("V-080", Error, "toast refetch", || {
            pg_delta().set("/pipelines/p/source/toast_strategy", json!("refetch"))
        }),
        case("V-080", Silent, "toast re_query", || {
            pg_delta().set("/pipelines/p/source/toast_strategy", json!("re_query"))
        }),
        case("V-081", Error, "detection catalog", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/tinyint1_detection",
                json!("catalog"),
            )
        }),
        case("V-081", Silent, "detection none", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/tinyint1_detection",
                json!("none"),
            )
        }),
        case("V-082", Error, "overflow truncate", || {
            pg_delta().set(
                "/pipelines/p/tables/numeric_overflow_action",
                json!("truncate"),
            )
        }),
        case("V-082", Silent, "overflow error", || {
            pg_delta().set(
                "/pipelines/p/tables/numeric_overflow_action",
                json!("error"),
            )
        }),
        case("V-083", Error, "sink hudi", || {
            pg_delta().set("/pipelines/p/sink/type", json!("hudi"))
        }),
        case(
            "V-083",
            Silent,
            "sink iceberg is valid config in v1",
            pg_iceberg,
        ),
        case("V-095", Error, "discovery consul", || {
            pg_delta().set("/globals/cluster/discovery", json!("consul"))
        }),
        case("V-095", Silent, "discovery static", || {
            pg_delta().set("/globals/cluster/discovery", json!("static"))
        }),
        case("V-102", Error, "slot_management external", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/slot_management",
                json!("external"),
            )
        }),
        case("V-102", Error, "slot_on_orphan adopt", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/slot_on_orphan",
                json!("adopt"),
            )
        }),
        case("V-102", Silent, "adopt_resnapshot", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/slot_on_orphan",
                json!("adopt_resnapshot"),
            )
        }),
        case("V-105", Error, "purge ignore", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/binlog_purge_action",
                json!("ignore"),
            )
        }),
        case("V-105", Silent, "purge re_snapshot", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/binlog_purge_action",
                json!("re_snapshot"),
            )
        }),
        case("V-114", Error, "ntz as date", || {
            pg_delta().set("/pipelines/p/sink/delta/timestamp_ntz_as", json!("date"))
        }),
        case("V-114", Silent, "ntz as string", || {
            pg_delta().set("/pipelines/p/sink/delta/timestamp_ntz_as", json!("string"))
        }),
        // ranges
        case("V-026", Error, "tokens 0", || {
            pg_delta().set("/globals/cluster/tokens", json!(0))
        }),
        case("V-026", Error, "tokens 4097", || {
            pg_delta().set("/globals/cluster/tokens", json!(4097))
        }),
        case("V-026", Warning, "tokens 32", || {
            pg_delta().set("/globals/cluster/tokens", json!(32))
        }),
        case("V-026", Silent, "tokens 256", || {
            pg_delta().set("/globals/cluster/tokens", json!(256))
        }),
        case("V-027", Error, "parallelism 0", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(0))
        }),
        case("V-027", Silent, "parallelism 1", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(1))
        }),
        case("V-072", Error, "margin 0.6", || {
            pg_delta().set("/globals/cluster/lease_safety_margin", json!(0.6))
        }),
        case("V-072", Warning, "margin 0.4", || {
            pg_delta().set("/globals/cluster/lease_safety_margin", json!(0.4))
        }),
        case("V-072", Silent, "margin 0.15", || {
            pg_delta().set("/globals/cluster/lease_safety_margin", json!(0.15))
        }),
        case("V-074", Error, "handoffs 0", || {
            pg_delta().set("/globals/cluster/max_concurrent_handoffs", json!(0))
        }),
        case("V-074", Error, "takeovers 0", || {
            pg_delta().set("/globals/cluster/max_concurrent_takeovers", json!(0))
        }),
        case("V-074", Silent, "takeovers 1", || {
            pg_delta().set("/globals/cluster/max_concurrent_takeovers", json!(1))
        }),
        case("V-097", Error, "ratio 1.5", || {
            pg_delta().set(
                "/globals/tracing",
                json!({"otlp_endpoint": "http://otel:4317", "sample_ratio": 1.5}),
            )
        }),
        case("V-097", Silent, "ratio 1", || {
            pg_delta().set(
                "/globals/tracing",
                json!({"otlp_endpoint": "http://otel:4317", "sample_ratio": 1}),
            )
        }),
        case("V-101", Error, "account with a dash", || {
            pg_delta().set("/globals/azure/account_name", json!("my-account"))
        }),
        case("V-101", Error, "account too short", || {
            pg_delta().set("/globals/azure/account_name", json!("ab"))
        }),
        case("V-101", Silent, "account name", || {
            pg_delta().set("/globals/azure/account_name", json!("lakeaccount01"))
        }),
        case("V-104", Error, "lag 32MiB", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/max_wal_lag_bytes",
                json!("32MiB"),
            )
        }),
        case("V-104", Silent, "lag 64MiB", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/max_wal_lag_bytes",
                json!("64MiB"),
            )
        }),
        case("V-104", Silent, "lag null", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/max_wal_lag_bytes",
                json!(null),
            )
        }),
        case("V-107", Error, "transform monthly", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "_ts", "transform": "monthly"}]}),
            )
        }),
        case("V-107", Error, "bucket without n", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "id", "transform": "bucket"}]}),
            )
        }),
        case("V-107", Error, "width on identity", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "id", "transform": "identity", "width": 4}]}),
            )
        }),
        case("V-107", Silent, "bucket 16 and truncate 4", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "id", "transform": "bucket", "n": 16}, {"column": "sku", "transform": "truncate", "width": 4}]}),
            )
        }),
        case("V-108", Error, "chunk 0", || {
            pg_delta().set("/pipelines/p/tables/snapshots/chunk_size", json!(0))
        }),
        case("V-108", Warning, "chunk 2000000", || {
            pg_delta().set("/pipelines/p/tables/snapshots/chunk_size", json!(2_000_000))
        }),
        case("V-108", Silent, "chunk 1000000", || {
            pg_delta().set("/pipelines/p/tables/snapshots/chunk_size", json!(1_000_000))
        }),
        case("V-109", Error, "batch rows 0", || {
            pg_delta().set("/pipelines/p/sink/max_transaction_batch_rows", json!(0))
        }),
        case("V-109", Warning, "batch rows 500", || {
            pg_delta().set("/pipelines/p/sink/max_transaction_batch_rows", json!(500))
        }),
        case("V-109", Silent, "batch rows 1000", || {
            pg_delta().set("/pipelines/p/sink/max_transaction_batch_rows", json!(1000))
        }),
        case("V-110", Error, "buffer 8MiB", || {
            pg_delta().set(
                "/pipelines/p/sink/max_transaction_buffer_bytes",
                json!("8MiB"),
            )
        }),
        case("V-110", Silent, "buffer 16MiB", || {
            pg_delta().set(
                "/pipelines/p/sink/max_transaction_buffer_bytes",
                json!("16MiB"),
            )
        }),
        case("V-111", Error, "capacity 0", || {
            pg_delta().set("/pipelines/p/sink/buffer_capacity_rows", json!(0))
        }),
        case("V-111", Silent, "capacity 1", || {
            pg_delta().set("/pipelines/p/sink/buffer_capacity_rows", json!(1))
        }),
        case("V-112", Error, "no attempts", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/retry/max_attempts",
                json!(0),
            )
        }),
        case("V-112", Error, "base above default max", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/retry/base_backoff",
                json!("1m"),
            )
        }),
        case("V-112", Silent, "base equals max", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/retry",
                json!({"base_backoff": "5s", "max_backoff": "5s"}),
            )
        }),
        // reserved values and v1 rejections
        case("V-075", Error, "kubernetes lock", || {
            pg_delta().set("/globals/cluster/lock_backend", json!("kubernetes"))
        }),
        case("V-075", Silent, "storage lock", || {
            pg_delta().set("/globals/cluster/lock_backend", json!("storage"))
        }),
        case("V-088", Error, "unity catalog", || {
            pg_delta().set("/pipelines/p/sink/delta/catalog/type", json!("unity"))
        }),
        case("V-088", Silent, "no catalog", || {
            pg_delta().set("/pipelines/p/sink/delta/catalog/type", json!("none"))
        }),
        case("V-089", Error, "changelog", || {
            pg_delta().set("/pipelines/p/sink/write_mode", json!("changelog"))
        }),
        case("V-089", Silent, "mirror", || {
            pg_delta().set("/pipelines/p/sink/write_mode", json!("mirror"))
        }),
        case("V-092", Error, "column mapping name", || {
            pg_delta().set("/pipelines/p/sink/delta/column_mapping", json!("name"))
        }),
        case("V-092", Silent, "column mapping none", || {
            pg_delta().set("/pipelines/p/sink/delta/column_mapping", json!("none"))
        }),
        case("V-115", Error, "hive block", || {
            catalog(json!({"hive": {"url": "thrift://hms:9083"}}))
        }),
        case("V-115", Silent, "glue block", || {
            catalog(json!({"glue": {"name": "123456789012"}}))
        }),
        case("V-116", Error, "rest sigv4", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("sigv4"),
            )
        }),
        case("V-116", Silent, "rest bearer", || {
            pg_iceberg()
                .set(
                    "/pipelines/p/sink/iceberg/catalog/rest/auth",
                    json!("bearer"),
                )
                .set("/pipelines/p/sink/iceberg/catalog/rest/token", json!("t"))
        }),
    ]
}
