//! §13.7 identity and naming.

use serde_json::{Value, json};

use crate::cases::{Case, Edit, Expect::*, case, pg_delta};

/// The base config with a second pipeline `q` copied from `p`, then given its own id, slot and
/// publication so the two don't collide.
fn two_pipelines() -> Value {
    let doc = pg_delta();
    let q = doc.pointer("/pipelines/p").unwrap().clone();
    doc.set("/pipelines/q", q)
        .set("/pipelines/q/id", json!("q"))
        .set("/pipelines/q/source/postgres/slot", json!("q_slot"))
        .set("/pipelines/q/source/postgres/publication", json!("q_pub"))
}

pub fn cases() -> Vec<Case> {
    vec![
        case("V-055", Error, "missing id", || {
            pg_delta().del("/pipelines/p/id")
        }),
        case("V-055", Error, "upper-case id", || {
            pg_delta().set("/pipelines/p/id", json!("Orders"))
        }),
        case("V-055", Error, "64 characters", || {
            pg_delta().set("/pipelines/p/id", json!("a".repeat(64)))
        }),
        case("V-055", Silent, "63 characters", || {
            pg_delta().set("/pipelines/p/id", json!("a".repeat(63)))
        }),
        case("V-056", Error, "duplicate id", || {
            two_pipelines().set("/pipelines/q/id", json!("p"))
        }),
        case("V-056", Silent, "distinct ids", two_pipelines),
        case("V-057", Error, "name with a space", || {
            let p = pg_delta().pointer("/pipelines/p").unwrap().clone();
            pg_delta().set("/pipelines", json!({ "bad name": p }))
        }),
        case("V-057", Silent, "name with dots and dashes", || {
            let p = pg_delta().pointer("/pipelines/p").unwrap().clone();
            pg_delta().set("/pipelines", json!({ "orders-pg.v2": p }))
        }),
        case("V-058", Error, "slot with a dash", || {
            pg_delta().set("/pipelines/p/source/postgres/slot", json!("p-slot"))
        }),
        case("V-058", Error, "slot shared on one host+db", || {
            two_pipelines().set("/pipelines/q/source/postgres/slot", json!("p_slot"))
        }),
        // V-058 scopes the slot to the source *server* (`host` + `port`), not the database:
        // PostgreSQL slots live on the server, so a different `db` doesn't excuse a collision.
        case(
            "V-058",
            Error,
            "slot shared on one host, different db",
            || {
                two_pipelines()
                    .set("/pipelines/q/source/postgres/slot", json!("p_slot"))
                    .set("/pipelines/q/source/db", json!("other"))
            },
        ),
        case(
            "V-058",
            Silent,
            "same slot name on a different port",
            || {
                two_pipelines()
                    .set("/pipelines/p/source/port", json!(5432))
                    .set("/pipelines/q/source/port", json!(5433))
                    .set("/pipelines/q/source/postgres/slot", json!("p_slot"))
            },
        ),
        case("V-059", Warning, "publication needs quoting", || {
            pg_delta().set("/pipelines/p/source/postgres/publication", json!("p-pub"))
        }),
        case("V-059", Warning, "publication shared", || {
            two_pipelines().set("/pipelines/q/source/postgres/publication", json!("p_pub"))
        }),
        case("V-059", Silent, "own publication", two_pipelines),
        case("V-060", Error, "slot in defaults", || {
            pg_delta().set("/defaults/source/postgres/slot", json!("s"))
        }),
        case("V-060", Error, "id in defaults", || {
            pg_delta().set("/defaults/id", json!("x"))
        }),
        case("V-060", Error, "snapshot overrides in defaults", || {
            pg_delta().set(
                "/defaults/tables/snapshots/overrides",
                json!({"public.a": "always"}),
            )
        }),
        case("V-060", Silent, "shared settings in defaults", || {
            pg_delta().set("/defaults/source/postgres/heartbeat", json!("30s"))
        }),
    ]
}
