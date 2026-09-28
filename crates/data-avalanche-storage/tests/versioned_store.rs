//! `ObjectStoreVersioned` against the emulators and the real GCS bucket: the same answers as on
//! `InMemory`, on every backend.

use std::sync::Arc;

use bytes::Bytes;
use data_avalanche_storage::{ObjectStoreVersioned, StoreError, VersionedStore};
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
