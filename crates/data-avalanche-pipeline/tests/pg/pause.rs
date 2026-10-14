//! A paused worker keeps its replication connection (Q-M6b-1): past `wal_sender_timeout` the
//! walsender is the same one, and a resume reads on. The test's own role sets
//! `wal_sender_timeout` for its connections (`ALTER ROLE … SET`), so the server's setting is
//! untouched.

use std::time::Duration;

use data_avalanche_pipeline::{Command, Exit, Phase};
use data_avalanche_testkit::require_it;
use tokio_postgres::Client;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline_as, start};

const ROLE: &str = "pl_pause";

/// The walsender serving the slot now, if any.
async fn walsender(a: &Client, slot: &str) -> Option<i32> {
    a.query_one(
        "SELECT (SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1)",
        &[&slot],
    )
    .await
    .expect("read the slot")
    .get(0)
}

#[tokio::test]
async fn a_pause_keeps_the_connection_past_wal_sender_timeout() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "pause",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);",
    )
    .await;
    a.batch_execute(&format!(
        "DROP ROLE IF EXISTS {ROLE};
         CREATE ROLE {ROLE} SUPERUSER LOGIN PASSWORD '{ROLE}';
         ALTER ROLE {ROLE} SET wal_sender_timeout = '2s';"
    ))
    .await
    .expect("the test role");
    let (role, conn) = tokio_postgres::connect(
        &crate::common::dsn().replace(
            "user=avalanche password=avalanche",
            "user=pl_pause password=pl_pause",
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect as the test role");
    tokio::spawn(conn);
    let timeout: String = role
        .query_one("SHOW wal_sender_timeout", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(timeout, "2s", "the role's sessions time out after 2s");
    drop(role);
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline_as(&n, &["t"], ROLE), &lake, None);
    // `mode: never`: a row is streamed only once the slot exists.
    run.wait("streaming", |st| st.phase == Phase::Streaming)
        .await;
    a.batch_execute(&format!("INSERT INTO {t} VALUES (1, 'a')"))
        .await
        .unwrap();
    converge(&a, &lake, &t).await;
    let before = walsender(&a, &n.slot).await.expect("a walsender");
    run.handle.control.send(Command::Pause).await.unwrap();
    run.wait("paused", |st| st.phase == Phase::Paused).await;
    // Four times the role's wal_sender_timeout.
    tokio::time::sleep(Duration::from_secs(8)).await;
    a.batch_execute(&format!("INSERT INTO {t} VALUES (2, 'b')"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(lake.rows(&t), [(1, Some("a".into()))], "paused");
    assert_eq!(
        walsender(&a, &n.slot).await,
        Some(before),
        "the same walsender: the connection outlived wal_sender_timeout"
    );
    run.handle.control.send(Command::Resume).await.unwrap();
    converge(&a, &lake, &t).await;
    assert_eq!(walsender(&a, &n.slot).await, Some(before));
    assert_eq!(run.drain().await.unwrap(), Exit::Drained);
    cleanup(&a, &n).await;
    a.batch_execute(&format!("DROP ROLE IF EXISTS {ROLE}"))
        .await
        .expect("drop the test role");
}
