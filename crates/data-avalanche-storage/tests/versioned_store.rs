//! `ObjectStoreVersioned` against the emulators and the real GCS bucket: the same answers as on
//! `InMemory`, on every backend.

use std::sync::Arc;

use bytes::Bytes;
use data_avalanche_storage::{
    Backend, BucketSpec, ObjectStoreVersioned, StoreError, Timeouts, VersionedStore, open_bucket,
};
use data_avalanche_testkit::endpoints::{
    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
};
use data_avalanche_testkit::{delete_prefix, require_gcs, require_it, stores, unique_prefix};
use object_store::ObjectStore;

async fn check(store: Arc<dyn ObjectStore>, test: &str) {
    let prefix = unique_prefix(test);
    let s = ObjectStoreVersioned::new(store.clone(), &prefix).unwrap();
    let b = |t: &'static str| Bytes::from_static(t.as_bytes());

    let v1 = s
        .create("doc.json", b("one"))
        .await
        .expect("create on an empty key");
    let again = s.create("doc.json", b("two")).await;
    assert!(
        matches!(again, Err(StoreError::Conflict { .. })),
        "create on an existing key: {again:?}"
    );
    assert_eq!(
        s.get("doc.json").await.unwrap(),
        Some((b("one"), v1.clone()))
    );

    let v2 = s
        .update("doc.json", &v1, b("two"))
        .await
        .expect("update with the current version");
    assert_ne!(v1, v2);
    let stale = s.update("doc.json", &v1, b("three")).await;
    assert!(
        matches!(stale, Err(StoreError::Conflict { .. })),
        "update with a stale version: {stale:?}"
    );
    let gone = s.update("missing.json", &v2, b("x")).await;
    assert!(
        matches!(gone, Err(StoreError::Conflict { .. })),
        "update of a missing object: {gone:?}"
    );
    assert_eq!(s.get("missing.json").await.unwrap(), None);

    s.put("snapshots/public.orders.json", b("s")).await.unwrap();
    s.put("snapshots/jobs/j1.json", b("j")).await.unwrap();
    assert_eq!(
        s.list("snapshots").await.unwrap(),
        vec!["snapshots/jobs/j1.json", "snapshots/public.orders.json"]
    );
    assert_eq!(s.get("doc.json").await.unwrap().unwrap().0, b("two"));

    delete_prefix(store.as_ref(), &prefix).await;
}

#[tokio::test]
async fn rustfs() {
    require_it!();
    check(stores::rustfs(), "storage_versioned_rustfs").await;
}

#[tokio::test]
async fn azurite() {
    require_it!();
    check(stores::azurite(), "storage_versioned_azurite").await;
}

#[tokio::test]
async fn real_gcs() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    check(stores::gcs(), "storage_versioned_gcs").await;
}

fn spec(backend: Backend, bucket: &str, options: &[(&str, &str)]) -> BucketSpec {
    BucketSpec {
        backend,
        bucket: bucket.to_owned(),
        options: options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        timeouts: Timeouts::COORDINATION,
    }
}

#[tokio::test]
async fn rustfs_via_open_bucket() {
    require_it!();
    let s = spec(
        Backend::S3,
        TEST_BUCKET,
        &[
            ("endpoint", RUSTFS_URL),
            ("allow_http", "true"),
            ("region", "us-east-1"),
            ("access_key_id", RUSTFS_ACCESS_KEY),
            ("secret_access_key", RUSTFS_SECRET_KEY),
        ],
    );
    check(open_bucket(&s).unwrap(), "storage_open_rustfs").await;
}

#[tokio::test]
async fn azurite_via_open_bucket() {
    require_it!();
    let s = spec(
        Backend::Azure { account: None },
        TEST_BUCKET,
        &[("use_emulator", "true")],
    );
    check(open_bucket(&s).unwrap(), "storage_open_azurite").await;
}

/// GCS through the production path: credentials from `GOOGLE_*` (here the service-account file).
#[tokio::test]
async fn real_gcs_via_open_bucket() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    let bucket = std::env::var("DATA_AVALANCHE_GCS_BUCKET").expect("DATA_AVALANCHE_GCS_BUCKET");
    check(
        open_bucket(&spec(Backend::Gcs, &bucket, &[])).unwrap(),
        "storage_open_gcs",
    )
    .await;
}
