//! The binary end to end: a snapshot, the stream, a SIGTERM that releases the lease, a restart.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

#[tokio::test]
async fn a_pipeline_runs_stops_on_sigterm_and_resumes() {
    require_it!();
    let e = E2e::new(
        "smoke",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items VALUES (1, 'a'), (2, 'b');",
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1; DELETE FROM items WHERE id = 2").await;
    e.converged("items").await;
    // At `info`, a first start that creates its table logs no error, and the Delta libraries
    // only what they warn of (config §6.4).
    let log = p.log();
    assert!(!log.contains("ERROR"), "{log}");
    assert!(
        !log.contains("buoyant_kernel") && !log.contains("deltalake_core"),
        "{log}"
    );
    let labels = r#"pipeline="smoke",source_host="127.0.0.1",source_db="avalanche""#;
    p.metric(&format!(
        r#"data_avalanche_pipeline_status{{{labels},status="running"}} 1"#
    ))
    .await;
    p.metric("data_avalanche_owned_pipelines 1").await;
    assert!(e.lease().await.unwrap().doc.owner_epoch == 1);

    let status = p.terminate().await;
    assert!(status.success(), "{status}");
    assert!(
        e.lease().await.unwrap().doc.released,
        "a SIGTERM releases the lease"
    );

    e.exec("INSERT INTO items VALUES (4, 'd')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    assert_eq!(e.ids("items").await, [1, 3, 4]);
    assert_eq!(e.lease().await.unwrap().doc.owner_epoch, 2);
    let status = p.terminate().await;
    assert!(status.success(), "{status}");
    e.cleanup().await;
}
