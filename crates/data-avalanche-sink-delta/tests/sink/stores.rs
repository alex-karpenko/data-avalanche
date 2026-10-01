//! The scenarios on object storage (roadmap M4 exit, Q-M4-4): RustFS and Azurite from
//! `test-infra/compose.yaml`, and the real GCS bucket, each under a unique prefix that is deleted
//! afterwards. Gated like every integration test (`DATA_AVALANCHE_IT`, GCS also by
//! `DATA_AVALANCHE_SKIP_GCS`).

use data_avalanche_sink_delta::DeltaLocation;
use data_avalanche_storage::Backend;
use data_avalanche_testkit::endpoints::{
    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
};
use data_avalanche_testkit::{
    delete_prefix, install_crypto_provider, require_gcs, require_it, stores, unique_prefix,
};

use crate::scenarios;

fn options(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Every scenario, each under its own prefix below `prefix`.
async fn run_all(location: impl Fn(String) -> DeltaLocation, prefix: &str) {
    let at = |name: &str| location(format!("{prefix}/{name}"));
    scenarios::mirror_and_replay(at("mirror")).await;
    scenarios::fence_and_guard_survive_a_foreign_optimize(at("fence")).await;
    scenarios::truncate_then_rows(at("truncate")).await;
    scenarios::evolution(at("evolution")).await;
    scenarios::masks_and_empty_sets(at("masks")).await;
    scenarios::escaped_names(at("escaped"), scenarios::odd_names()).await;
}

#[tokio::test]
async fn scenarios_on_rustfs() {
    require_it!();
    install_crypto_provider();
    let prefix = unique_prefix("sink-delta");
    let location = |prefix: String| DeltaLocation::Bucket {
        backend: Backend::S3,
        bucket: TEST_BUCKET.into(),
        prefix,
        options: options(&[
            ("endpoint", RUSTFS_URL),
            ("allow_http", "true"),
            ("region", "us-east-1"),
            ("access_key_id", RUSTFS_ACCESS_KEY),
            ("secret_access_key", RUSTFS_SECRET_KEY),
        ]),
    };
    run_all(location, &prefix).await;
    delete_prefix(stores::rustfs().as_ref(), &prefix).await;
}

#[tokio::test]
async fn scenarios_on_azurite() {
    require_it!();
    install_crypto_provider();
    let prefix = unique_prefix("sink-delta");
    let location = |prefix: String| DeltaLocation::Bucket {
        backend: Backend::Azure { account: None },
        bucket: TEST_BUCKET.into(),
        prefix,
        options: options(&[("use_emulator", "true")]),
    };
    run_all(location, &prefix).await;
    delete_prefix(stores::azurite().as_ref(), &prefix).await;
}

#[tokio::test]
async fn scenarios_on_real_gcs() {
    require_gcs!();
    install_crypto_provider();
    let bucket = data_avalanche_testkit::required_env("DATA_AVALANCHE_GCS_BUCKET");
    let prefix = unique_prefix("sink-delta");
    let location = |prefix: String| DeltaLocation::Bucket {
        backend: Backend::Gcs,
        bucket: bucket.clone(),
        prefix,
        options: vec![],
    };
    run_all(location, &prefix).await;
    delete_prefix(stores::gcs().as_ref(), &prefix).await;
}
