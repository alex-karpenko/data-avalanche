//! A pipeline's tables and figures through any node (Admin_Console §4; M13d), on three nodes: a
//! node that doesn't run the test's pipeline answers its live figures from the holder; the
//! table's page brings the Delta table's facts and its columns, a column dropped at the source
//! shown soft-dropped; every pipeline's tables are listed, each with the node that runs it.

use std::time::Duration;

use data_avalanche_testkit::require_it;
use serde_json::{Value, json};

use crate::harness::{Doc, eventually};
use crate::nodes::Cluster;

/// The state a column of the table's page is in.
fn column<'v>(page: &'v Value, name: &str) -> &'v Value {
    page["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("column {name} in {page}"))
}

#[tokio::test]
async fn a_pipeline_s_tables_are_shown_through_any_node() {
    require_it!();
    let mut c = Cluster::new("console_pipelines").await;
    c.start_all().await;
    let test = c.e.test.clone();
    c.e.insert_series("items", 5, Doc::Row).await;
    c.landed().await;
    let holders = c.holders().await;
    let holder = holders[&test].0.clone();
    let other = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != holder)
        .expect("another node");
    let o = c.proc(other);
    let items = if c.e.source.is_postgres() {
        format!("{}.items", c.e.schema)
    } else {
        "items".to_owned()
    };

    // The live figures, from the holder; the source's detail once a heartbeat read it.
    let live = format!("/v1/pipelines/{test}/live");
    eventually("the live figures", Duration::from_secs(60), async || {
        let (code, v) = o.call("GET", &live).await;
        code == 200 && !v["source"]["read_at"].is_null()
    })
    .await;
    let (_, v) = o.call("GET", &live).await;
    assert_eq!(
        (&v["node"], &v["actual"]),
        (&json!(holder), &json!("running")),
        "{v}"
    );
    let row = v["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["table"] == items.as_str())
        .unwrap_or_else(|| panic!("{items} in {v}"))
        .clone();
    assert_eq!(row["state"], "streaming", "{row}");
    assert!(row["changes"].as_u64() >= Some(5), "{row}");
    assert!(row["version"].is_u64() && row["files"].is_u64(), "{row}");
    let detail = &v["source"]["detail"];
    if c.e.source.is_postgres() {
        assert_eq!(detail["engine"], "postgres", "{v}");
        assert_eq!(detail["slot"]["active"], true, "{v}");
        assert_eq!(detail["publication"]["tables"], 1, "{v}");
    } else {
        assert_eq!(detail["engine"], "mysql", "{v}");
        assert!(detail["head"].is_string(), "{v}");
    }

    // The table's page: the Delta facts and the columns.
    let page = format!("/v1/pipelines/{test}/tables/{items}");
    let (code, p) = o.call("GET", &page).await;
    assert_eq!(code, 200, "{p}");
    assert_eq!(p["node"], json!(holder), "{p}");
    assert_eq!(p["delta"]["rows"], 5, "{p}");
    assert!(
        p["delta"]["version"].as_u64() >= row["version"].as_u64(),
        "{p}"
    );
    assert_eq!(column(&p, "id")["key"], true, "{p}");
    assert_eq!(column(&p, "doc")["state"], "kept", "{p}");
    assert_eq!(column(&p, "_op")["state"], "lineage", "{p}");

    // A column dropped at the source stays in Delta: soft-dropped, once the facts kept expire.
    c.e.exec("ALTER TABLE items DROP COLUMN doc; INSERT INTO items VALUES (100)")
        .await;
    eventually("doc soft-dropped", Duration::from_secs(90), async || {
        let (code, p) = o.call("GET", &page).await;
        code == 200 && column(&p, "doc")["state"] == "soft_dropped"
    })
    .await;

    // Every pipeline's tables, each with the node that runs it.
    let (code, t) = o.call("GET", "/v1/tables").await;
    assert_eq!(code, 200, "{t}");
    assert_eq!(t["counts"]["all"], 3, "{t}");
    assert_eq!(t["unreachable"], json!([]), "{t}");
    for row in t["tables"].as_array().unwrap() {
        let id = row["pipeline"].as_str().unwrap();
        assert_eq!(row["node"], json!(holders[id].0), "{row}");
    }
    c.stop().await;
}
