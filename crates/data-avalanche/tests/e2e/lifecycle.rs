//! Scenarios 7–9: the desired state across a restart, one pipeline under one lease with a second
//! process waiting, and an Iceberg pipeline refused beside a running Delta one.

use std::time::Duration;

use data_avalanche_state::DesiredState;
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

fn never() -> Options<'static> {
    Options {
        tables: ", snapshots: { mode: never }",
        ..Options::default()
    }
}

/// Pause holds the stream with the source connection open; stop closes it and is kept across a
/// restart; resume reads on from where each left off.
#[tokio::test]
async fn pause_stop_and_resume_hold_across_a_restart() {
    require_it!();
    // MySQL's dump thread sees the stop's closed connection at its next write, a heartbeat at
    // the latest: 1 s, not the default minute.
    let e = E2e::new(
        "life_states",
        ITEMS,
        Options {
            mysql: ", heartbeat: 1s",
            ..never()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;

    e.docs()
        .set_desired_state(DesiredState::Paused)
        .await
        .expect("paused");
    p.metric(&e.status("paused")).await;
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(e.ids("items").await, [1], "nothing is read while paused");
    assert!(
        e.stream_open().await,
        "a paused pipeline keeps the source warm"
    );

    e.docs()
        .set_desired_state(DesiredState::Running)
        .await
        .expect("running");
    p.metric(&e.status("running")).await;
    e.converged("items").await;

    e.docs()
        .set_desired_state(DesiredState::Stopped)
        .await
        .expect("stopped");
    p.metric(&e.status("stopped")).await;
    eventually(
        "the stream is closed",
        Duration::from_secs(30),
        async || !e.stream_open().await,
    )
    .await;
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        e.ids("items").await,
        [1, 2],
        "still stopped after a restart"
    );
    assert!(!e.stream_open().await);

    e.docs()
        .set_desired_state(DesiredState::Running)
        .await
        .expect("running");
    p.metric(&e.status("running")).await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A second process waits while the first holds the lease, and takes over after a `kill -9`
/// once the lease runs out, with the next owner epoch.
#[tokio::test]
async fn a_second_process_takes_over_after_the_first_dies() {
    require_it!();
    let e = E2e::new("life_lease", ITEMS, never()).await;
    let mut a = e.start(&[]);
    a.ready().await;
    e.streaming().await;
    let mut b = e.start_node("n2");
    b.ready().await;
    a.metric("data_avalanche_owned_pipelines 1").await;
    // Past a lease duration (6 s) and a renewal interval (2 s) since n2 first saw the lease: n2
    // takes over only a lease that stopped changing, and n1 renews it.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let lease = e.lease().await.expect("a lease").doc;
    assert_eq!(
        (lease.holder_node_id.as_str(), lease.owner_epoch),
        ("n1", 1)
    );
    b.metric("data_avalanche_owned_pipelines 0").await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;

    let status = a.kill().await;
    assert!(!status.success());
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    b.metric("data_avalanche_owned_pipelines 1").await;
    e.converged("items").await;
    let lease = e.lease().await.expect("a lease").doc;
    assert_eq!(
        (lease.holder_node_id.as_str(), lease.owner_epoch),
        ("n2", 2)
    );
    assert!(b.terminate().await.success());
    e.cleanup().await;
}

/// An Iceberg pipeline is refused (its status is error) while the Delta pipeline beside it runs.
#[tokio::test]
async fn an_iceberg_pipeline_is_refused_beside_a_running_one() {
    require_it!();
    let e = E2e::new(
        "life_iceberg",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            pipelines: r#"  ice:
    id: ice
@SOURCE(_ice)@
    tables: { include: ['@INCLUDE@'] }
    sink:
      type: iceberg
      bucket: @BUCKET@
      prefix: @PREFIX@/ice
      iceberg: { catalog: { glue: { name: "123" } } }"#,
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status_of("ice", "error")).await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;
    p.metric(&e.status("running")).await;
    assert!(
        p.log().contains("the Iceberg sink ships in v2"),
        "{}",
        p.log()
    );
    if e.source.is_postgres() {
        let ice_slot = format!("{}_ice", e.schema);
        assert!(
            !e.db
                .any(&format!(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{ice_slot}'"
                ))
                .await,
            "the refused pipeline touched no source"
        );
    }
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
