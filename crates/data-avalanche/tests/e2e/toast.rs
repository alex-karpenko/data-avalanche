//! Scenario 5, TOAST (D-IMPL-3): an UPDATE that leaves a TOASTed column alone sends it as
//! unchanged, and the lake keeps the value, whether the row's earlier version is in the lake
//! already or in the same batch.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const BIG: usize = 100_000;

/// The lake's `id`, `n` and the length of `big`.
async fn lake(e: &E2e) -> Vec<(String, String, usize)> {
    e.rows("blobs", &["id", "n", "big"])
        .await
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone(), r[2].len()))
        .collect()
}

fn row(id: u32, n: u32) -> (String, String, usize) {
    (id.to_string(), n.to_string(), BIG)
}

#[tokio::test]
async fn an_update_that_leaves_a_toasted_column_keeps_its_value() {
    require_it!();
    // EXTERNAL: stored out of line, uncompressed, so the value is TOASTed.
    let e = E2e::new(
        "toast",
        "CREATE TABLE blobs (id bigint PRIMARY KEY, n int, big text);
         ALTER TABLE blobs ALTER COLUMN big SET STORAGE EXTERNAL;",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;

    e.exec(&format!(
        "INSERT INTO blobs VALUES (1, 0, repeat('x', {BIG}))"
    ))
    .await;
    e.mirrored("blobs", &["id", "n", "big"]).await;

    // Across batches: the row's value is in the lake already.
    e.exec("UPDATE blobs SET n = 1 WHERE id = 1").await;
    e.mirrored("blobs", &["id", "n"]).await;
    assert_eq!(lake(&e).await, [row(1, 1)]);

    // Within one batch: the insert and the updates collapse into one upsert.
    e.exec(&format!(
        "BEGIN;
         INSERT INTO blobs VALUES (2, 0, repeat('y', {BIG}));
         UPDATE blobs SET n = 2 WHERE id = 2;
         UPDATE blobs SET n = 2 WHERE id = 1;
         UPDATE blobs SET n = 3 WHERE id = 1;
         COMMIT"
    ))
    .await;
    e.mirrored("blobs", &["id", "n"]).await;
    assert_eq!(lake(&e).await, [row(1, 3), row(2, 2)]);
    e.mirrored("blobs", &["id", "n", "big"]).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
