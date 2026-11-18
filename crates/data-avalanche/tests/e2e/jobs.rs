//! The on-demand snapshot jobs (M9b): a job asked through the API reads a streamed table again
//! and copies one outside the config, clears a dropped table's halt, and outlives a kill of the
//! process that ran it. A job is cancelled and retried (M13c).

use std::cell::Cell;
use std::time::Duration;

use data_avalanche_testkit::require_it;
use serde_json::Value;

use crate::harness::{Doc, E2e, Options, Proc, Source, eventually};

/// A table as a request names it: the source's per-table config key.
fn key(e: &E2e, table: &str) -> String {
    if e.source.is_postgres() {
        format!("{}.{table}", e.schema)
    } else {
        table.to_owned()
    }
}

/// `POST /v1/pipelines/<id>/snapshots` of `tables`: the code and the job.
async fn ask(p: &Proc, e: &E2e, tables: &[&str]) -> (u16, Value) {
    let names: Vec<String> = tables.iter().map(|t| key(e, t)).collect();
    let body = serde_json::json!({ "tables": names }).to_string();
    let path = format!("/v1/pipelines/{}/snapshots", e.test);
    let (code, _, text) = p
        .send(None, "POST", &path, &body)
        .await
        .expect("the API answers");
    (
        code,
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}")),
    )
}

/// Waits until job `id` ended, and returns it.
async fn ended(p: &Proc, id: &str) -> Value {
    let path = format!("/v1/snapshots/{id}");
    eventually(
        &format!("job {id} ends"),
        Duration::from_secs(120),
        async || {
            let status = p.call("GET", &path).await.1["status"].clone();
            status == "completed" || status == "failed" || status == "cancelled"
        },
    )
    .await;
    p.call("GET", &path).await.1
}

/// Under `mode: never` the rows there before the start stay out of the lake; a job reads the
/// streamed table again and copies one the config excludes. The copy isn't streamed after.
#[tokio::test]
async fn a_job_reads_a_table_again_and_copies_one_outside_the_config() {
    require_it!();
    let e = E2e::new(
        "jobs_read",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items VALUES (1, 'a'), (2, 'b');
         CREATE TABLE extra (id bigint PRIMARY KEY, doc text);
         INSERT INTO extra VALUES (7, 'x');",
        Options {
            tables: ", exclude: ['@SCHEMA@.extra'], snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    eventually(
        "the streamed row lands",
        Duration::from_secs(60),
        async || e.ids("items").await == [3],
    )
    .await;

    let (code, v) = ask(&p, &e, &["items", "extra"]).await;
    assert_eq!(code, 202, "{v}");
    let id = v["job_id"].as_str().unwrap().to_owned();
    let job = ended(&p, &id).await;
    assert_eq!(job["status"], "completed", "{job}");
    assert_eq!(job["tables"][0]["rows_done"], 3, "{job}");
    assert_eq!(job["tables"][1]["rows_done"], 1, "{job}");
    assert_eq!(e.ids("items").await, [1, 2, 3]);
    assert_eq!(e.ids("extra").await, [7]);

    e.exec("INSERT INTO extra VALUES (8, 'y'); INSERT INTO items VALUES (4, 'd')")
        .await;
    e.converged("items").await;
    assert_eq!(e.ids("extra").await, [7], "a copy isn't streamed");
    let path = format!("/v1/pipelines/{}/snapshots", e.test);
    let (code, all) = p.call("GET", &path).await;
    assert_eq!(code, 200);
    assert_eq!(all["jobs"][0]["job_id"], id.as_str(), "{all}");
    assert!(e.halts().await.tables.is_empty());
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A table dropped and created again halts (`TABLE_DROPPED`); a job clears the halt, reads the
/// new table into the lake table, and the stream carries its changes again. The old table's
/// row the new one hasn't stays in the lake: a job upserts (Q-M9b-2).
#[tokio::test]
async fn a_job_clears_a_dropped_table_s_halt() {
    require_it!();
    let e = E2e::new(
        "jobs_halt",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         CREATE TABLE gone (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            pipeline: "schema_evolution: { rescan_interval: 2s }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO gone VALUES (1, 'old')").await;
    e.converged("gone").await;
    e.exec(
        "DROP TABLE gone; CREATE TABLE gone (id bigint PRIMARY KEY, doc text, note text);
         INSERT INTO gone VALUES (5, 'new', 'n')",
    )
    .await;
    eventually("gone halts", Duration::from_secs(60), async || {
        e.halts()
            .await
            .tables
            .iter()
            .any(|h| h.table.name == "gone")
    })
    .await;

    let (code, v) = ask(&p, &e, &["gone"]).await;
    assert_eq!(code, 202, "{v}");
    let job = ended(&p, v["job_id"].as_str().unwrap()).await;
    assert_eq!(job["status"], "completed", "{job}");
    assert!(e.halts().await.tables.is_empty(), "the halt is cleared");
    assert_eq!(
        e.ids("gone").await,
        [1, 5],
        "the old row stays: a job upserts"
    );

    e.exec("INSERT INTO gone VALUES (6, 'streamed', 'm')").await;
    eventually(
        "the stream carries gone again",
        Duration::from_secs(60),
        async || e.ids("gone").await == [1, 5, 6],
    )
    .await;
    assert_eq!(
        e.rows("gone", &["id", "note"]).await,
        [
            vec!["1".to_string(), "NULL".into()],
            vec!["5".into(), "n".into()],
            vec!["6".into(), "m".into()]
        ]
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A job whose snapshot runs when its process is killed is listed by the next process, which
/// reads on from the table's cursor and ends the job.
#[tokio::test]
async fn a_job_outlives_a_kill_of_its_process() {
    require_it!();
    let e = E2e::new(
        "jobs_kill",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never, chunk_size: 10 }",
            ..Options::default()
        },
    )
    .await;
    e.insert_series("items", 1_000, Doc::Row).await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    let (code, v) = ask(&p, &e, &["items"]).await;
    assert_eq!(code, 202, "{v}");
    let id = v["job_id"].as_str().unwrap().to_owned();
    let path = format!("/v1/snapshots/{id}");
    let before = Cell::new(0);
    eventually(
        "the job's table reads, partway",
        Duration::from_secs(60),
        async || {
            let job = p.call("GET", &path).await.1;
            let n = job["tables"][0]["rows_done"].as_u64().unwrap_or(0);
            before.set(n);
            job["status"] == "running" && n > 0 && n < 1_000
        },
    )
    .await;
    p.kill().await;

    let mut p = e.start(&[]);
    p.ready().await;
    let (code, all) = p
        .call("GET", &format!("/v1/pipelines/{}/snapshots", e.test))
        .await;
    assert_eq!(code, 200);
    assert_eq!(
        all["jobs"][0]["job_id"],
        id.as_str(),
        "listed after the kill"
    );
    // A resume reads on from the cursor: the job's count never falls below the one seen before
    // the kill, as a snapshot started over from the first row would.
    let low = Cell::new(u64::MAX);
    eventually(
        &format!("job {id} ends"),
        Duration::from_secs(120),
        async || {
            let job = p.call("GET", &path).await.1;
            if let Some(n) = job["tables"][0]["rows_done"].as_u64() {
                low.set(low.get().min(n));
            }
            job["status"] == "completed" || job["status"] == "failed"
        },
    )
    .await;
    assert!(
        low.get() >= before.get(),
        "rows_done fell from {} to {} after the kill",
        before.get(),
        low.get()
    );
    let job = p.call("GET", &path).await.1;
    assert_eq!(job["status"], "completed", "{job}");
    assert_eq!(job["tables"][0]["rows_done"], 1_000, "{job}");
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `POST path` with JSON `body`: the code and the answer.
async fn post(p: &Proc, path: &str, body: &str) -> (u16, Value) {
    let (code, _, text) = p
        .send(None, "POST", path, body)
        .await
        .expect("the API answers");
    (
        code,
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}")),
    )
}

/// D-M13c.2: a job bringing a dropped table back is cancelled partway, its pipeline paused: the
/// restart's start ends it, and the table halts again, `COPY_CANCELLED`; the pipeline stays
/// paused. The cancel and the job's end are in the events. Resumed, a retry of the job copies the
/// table and clears the halt; `GET /v1/snapshots` lists both jobs, the retry first, with the
/// source's row estimate (exact after `ANALYZE` on PostgreSQL, InnoDB's approximate
/// `TABLE_ROWS` on MySQL).
#[tokio::test]
async fn a_cancelled_copy_halts_its_table_and_a_retry_copies_it() {
    require_it!();
    let e = E2e::new(
        "jobs_cancel",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         CREATE TABLE gone (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never, chunk_size: 10 }",
            pipeline: "schema_evolution: { rescan_interval: 2s }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("DROP TABLE gone; CREATE TABLE gone (id bigint PRIMARY KEY, doc text)")
        .await;
    e.insert_series("gone", 1_000, Doc::Row).await;
    e.exec(match e.source {
        Source::Postgres { .. } => "ANALYZE gone",
        Source::Mysql(_) => "ANALYZE TABLE gone",
    })
    .await;
    eventually("gone halts", Duration::from_secs(60), async || {
        e.halts()
            .await
            .tables
            .iter()
            .any(|h| h.table.name == "gone")
    })
    .await;

    let (code, v) = ask(&p, &e, &["gone"]).await;
    assert_eq!(code, 202, "{v}");
    let id = v["job_id"].as_str().unwrap().to_owned();
    let path = format!("/v1/snapshots/{id}");
    eventually(
        "the copy runs, partway",
        Duration::from_secs(60),
        async || {
            let job = p.call("GET", &path).await.1;
            let n = job["tables"][0]["rows_done"].as_u64().unwrap_or(0);
            job["status"] == "running" && n > 0 && n < 1_000
        },
    )
    .await;
    let job = p.call("GET", &path).await.1;
    assert_eq!(job["tables"][0]["from_halt"], "TABLE_DROPPED", "{job}");
    assert!(job["tables"][0]["started_at"].is_string(), "{job}");
    let pipeline = format!("/v1/pipelines/{}", e.test);
    let (code, v) = post(&p, &format!("{pipeline}/pause"), "").await;
    assert_eq!(code, 202, "{v}");

    let (code, v) = post(
        &p,
        &format!("{path}/cancel"),
        r#"{"note": "the wrong table"}"#,
    )
    .await;
    assert_eq!(code, 202, "{v}");
    assert_eq!(
        v["restart"], true,
        "a paused pipeline's worker restarts: {v}"
    );
    let job = ended(&p, &id).await;
    assert_eq!(job["status"], "cancelled", "{job}");
    assert_eq!(job["cancel"]["note"], "the wrong table", "{job}");
    let halts = e.halts().await.tables;
    let halt = halts
        .iter()
        .find(|h| h.table.name == "gone")
        .expect("gone halts again");
    assert_eq!(halt.reason.code(), "COPY_CANCELLED", "{halts:?}");
    let (_, status) = p.call("GET", &pipeline).await;
    assert_eq!(status["desired_state"], "paused", "{status}");
    let events = format!("/v1/events?object={id}");
    eventually(
        "the cancel and the end are recorded",
        Duration::from_secs(30),
        async || {
            let kinds: Vec<String> = p.call("GET", &events).await.1["events"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|e| e["kind"].as_str().unwrap_or_default().to_owned())
                .collect();
            kinds.contains(&"action.snapshot.cancel".to_owned())
                && kinds.contains(&"job.cancelled".to_owned())
        },
    )
    .await;

    let (code, v) = post(&p, &format!("{pipeline}/resume"), "").await;
    assert_eq!(code, 202, "{v}");
    let (code, v) = post(&p, &format!("{path}/retry"), "").await;
    assert_eq!(code, 202, "{v}");
    assert_eq!(v["retry_of"], id.as_str());
    let retry = v["job_id"].as_str().unwrap().to_owned();
    let job = ended(&p, &retry).await;
    assert_eq!(job["status"], "completed", "{job}");
    assert_eq!(job["tables"][0]["rows_done"], 1_000, "{job}");
    match e.source {
        Source::Postgres { .. } => {
            assert_eq!(job["tables"][0]["estimate"], 1_000, "analyzed: {job}");
        }
        Source::Mysql(_) => {
            let n = job["tables"][0]["estimate"].as_u64();
            assert!(
                n.is_some_and(|n| (500..=2_000).contains(&n)),
                "InnoDB's estimate is near 1000: {job}"
            );
        }
    }
    assert_eq!(job["tables"][0]["lake"], "complete", "{job}");
    assert!(e.halts().await.tables.is_empty(), "the halt is cleared");
    e.exec("INSERT INTO gone VALUES (1001, 'streamed')").await;
    e.converged("gone").await;

    let (code, all) = p
        .call("GET", &format!("/v1/snapshots?pipeline={}", e.test))
        .await;
    assert_eq!(code, 200, "{all}");
    let listed: Vec<(&str, &str)> = all["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| (j["job_id"].as_str().unwrap(), j["status"].as_str().unwrap()))
        .collect();
    assert_eq!(
        listed,
        [(retry.as_str(), "completed"), (id.as_str(), "cancelled")]
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
