//! Does each storage backend honour create-if-absent and update-if-version-matches? The results
//! decide where the M2 lease conformance suite runs (roadmap R-3).

use std::sync::Arc;

use data_avalanche_testkit::{require_gcs, require_it, stores, unique_prefix};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload, UpdateVersion};

async fn check_conditional_writes(store: Arc<dyn ObjectStore>, test: &str) {
    let key = Path::from(format!("{}/cas", unique_prefix(test)));

    let first = store
        .put_opts(
            &key,
            PutPayload::from_static(b"one"),
            PutMode::Create.into(),
        )
        .await
        .expect("create on an empty key must succeed");

    let again = store
        .put_opts(
            &key,
            PutPayload::from_static(b"two"),
            PutMode::Create.into(),
        )
        .await;
    assert!(
        matches!(again, Err(object_store::Error::AlreadyExists { .. })),
        "create on an existing key: {again:?}"
    );

    let v1: UpdateVersion = first.into();
    store
        .put_opts(
            &key,
            PutPayload::from_static(b"two"),
            PutMode::Update(v1.clone()).into(),
        )
        .await
        .expect("update with the current version must succeed");

    let stale = store
        .put_opts(
            &key,
            PutPayload::from_static(b"three"),
            PutMode::Update(v1).into(),
        )
        .await;
    assert!(
        matches!(stale, Err(object_store::Error::Precondition { .. })),
        "update with a stale version: {stale:?}"
    );

    let body = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(&body[..], b"two", "the stale update must not have landed");

    store.delete(&key).await.expect("cleanup");
}

#[tokio::test]
async fn rustfs_honours_conditional_writes() {
    require_it!();
    check_conditional_writes(stores::rustfs(), "rustfs_cas").await;
}

#[tokio::test]
async fn azurite_honours_conditional_writes() {
    require_it!();
    check_conditional_writes(stores::azurite(), "azurite_cas").await;
}

#[tokio::test]
async fn real_gcs_honours_conditional_writes() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    check_conditional_writes(stores::gcs(), "gcs_cas").await;
}
