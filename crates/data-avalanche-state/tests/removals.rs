//! The removal record's conditional done on real backends: a record of the removal since the
//! done's read moves the record's version, so the done loses, on S3 as well, where a rewrite of
//! the same bytes keeps the ETag (the record's counter makes the bytes differ).

use std::sync::Arc;

use chrono::Utc;
use data_avalanche_state::{DocStore, RemovalDoc, Removals};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_testkit::{delete_prefix, require_gcs, require_it, stores, unique_prefix};
use object_store::ObjectStore;

fn removed(generation: u64) -> RemovalDoc {
    RemovalDoc {
        pipeline_id: "p".into(),
        generation,
        hash: format!("blake3:{generation}"),
        removed_at: Utc::now(),
        done_at: None,
        records: 0,
    }
}

async fn a_record_since_the_read_wins(store: Arc<dyn ObjectStore>, test: &str) {
    let prefix = unique_prefix(test);
    let removals = Removals::new(DocStore::new(Arc::new(
        ObjectStoreVersioned::new(store.clone(), &prefix).unwrap(),
    )));
    removals.record(&removed(3)).await.unwrap();
    let read = removals.pending_versioned("p").await.unwrap().unwrap();
    // Another node adopts a generation that removes `p` again: the removal under way is kept.
    removals.record(&removed(4)).await.unwrap();
    let done = removals.done_if(&read, Utc::now()).await.unwrap();
    assert!(
        !done,
        "a done against the version read before the record is refused"
    );
    let kept = removals
        .pending("p")
        .await
        .unwrap()
        .expect("still under way");
    assert_eq!((kept.generation, kept.records), (3, 2));
    let read = removals.pending_versioned("p").await.unwrap().unwrap();
    assert!(removals.done_if(&read, Utc::now()).await.unwrap());
    assert_eq!(removals.pending("p").await.unwrap(), None);
    delete_prefix(store.as_ref(), &prefix).await;
}

#[tokio::test]
async fn rustfs() {
    require_it!();
    a_record_since_the_read_wins(stores::rustfs(), "state_removals_rustfs").await;
}

#[tokio::test]
async fn azurite() {
    require_it!();
    a_record_since_the_read_wins(stores::azurite(), "state_removals_azurite").await;
}

#[tokio::test]
async fn real_gcs() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    a_record_since_the_read_wins(stores::gcs(), "state_removals_gcs").await;
}
