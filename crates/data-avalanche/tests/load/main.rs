//! The v1 acceptance runs (roadmap §4 M12c and §8; Q-M12-3, Q-M12c-2, Q-M12c-3), on test-infra:
//! D-V1.3's lag targets measured at the product's defaults, in steady state and across a node's
//! loss, and MERGE's cost under random-key churn (`bench`); a storage outage shorter and longer
//! than the lease, a source restart and a network partition, under load (`chaos`); a long soak
//! with churn (`soak`). Each ends with the lake holding the source's rows, each key once, and its
//! owner epochs never going back. The binary is the E2E suite's, built in release mode by
//! `cargo xtask bench|chaos|soak`, which writes the measurements to `target/acceptance/`.
//!
//! Needs `DATA_AVALANCHE_LOAD=1` beside `DATA_AVALANCHE_IT=1`, and the `e2e-hooks` feature:
//!
//! `cargo test --release -p data-avalanche --features e2e-hooks --test load -- --test-threads=1`

#[allow(dead_code)]
#[path = "../e2e/forwarder.rs"]
mod forwarder;
#[allow(dead_code)]
#[path = "../e2e/harness.rs"]
mod harness;
#[allow(dead_code)]
#[path = "../e2e/nodes.rs"]
mod nodes;
#[allow(dead_code)]
#[path = "../e2e/proxy.rs"]
mod proxy;
#[allow(dead_code, unused_macros)]
#[path = "../e2e/source.rs"]
mod source;
#[allow(dead_code)]
#[path = "../e2e/store.rs"]
mod store;

mod bench;
mod chaos;
mod report;
mod soak;
mod workload;

use std::time::{Duration, Instant};

use harness::E2e;

/// Returns early, with a line saying so, unless `DATA_AVALANCHE_LOAD` is set and the ITs are on:
/// the runs take minutes to hours, and need test-infra.
#[macro_export]
macro_rules! require_load {
    () => {
        data_avalanche_testkit::require_it!();
        if std::env::var("DATA_AVALANCHE_LOAD").map_or(true, |v| v.is_empty()) {
            eprintln!("skipped: set DATA_AVALANCHE_LOAD=1 (cargo xtask bench|chaos|soak)");
            return;
        }
    };
}

/// Waits until every one of `tables` finished its snapshot in the test's pipeline, up to
/// `within`: a prefilled table's copy outlasts the suite's minute.
pub(crate) async fn snapshotted(e: &E2e, tables: &[String], within: Duration) {
    let until = Instant::now() + within;
    for t in tables {
        loop {
            if e.snapshot(t)
                .await
                .is_some_and(|s| s.completed_at.is_some())
            {
                break;
            }
            assert!(Instant::now() < until, "{t}'s snapshot within {within:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// The owner epochs of `lake`'s `table` in commit order never go back (a zombie's write after a
/// takeover would), and some commit carries one.
pub(crate) async fn epochs_never_go_back(e: &E2e, lake: &str, table: &str) {
    let mut epochs = vec![];
    for actions in e.log_in(lake, table).await {
        let info = actions
            .iter()
            .find(|a| a.get("commitInfo").is_some())
            .expect("a commit has its commitInfo");
        if let Some(epoch) = info["commitInfo"]["data-avalanche.owner_epoch"].as_u64() {
            epochs.push(epoch);
        }
    }
    assert!(
        !epochs.is_empty(),
        "{lake}/{table}: no epoch in its commits"
    );
    assert!(
        epochs.is_sorted(),
        "{lake}/{table}: an epoch went back: {epochs:?}"
    );
}

/// The lake `lake`'s `table` holds the source's rows on [`workload::COLS`], each key once, within
/// `within`: first the ids (cheap to compare while they move), then the rows, in every lake.
pub(crate) async fn lands(e: &E2e, lake: &str, table: &str, within: Duration) {
    e.converged_in(lake, table, within).await;
    e.mirrored_in(lake, table, workload::COLS, Duration::from_secs(60))
        .await;
    epochs_never_go_back(e, lake, table).await;
}
