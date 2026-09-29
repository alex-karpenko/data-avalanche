//! CAS races on real backends: N concurrent creators → exactly one winner; N concurrent
//! read-modify-writers → every change applied once.

use std::sync::Arc;

use data_avalanche_state::{DocStore, Document};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_testkit::{delete_prefix, require_gcs, require_it, stores, unique_prefix};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tally {
    writers: Vec<u32>,
}

impl Document for Tally {
    const KIND: &'static str = "tally";
    const SCHEMA_VERSION: u64 = 1;
}

const N: u32 = 8;

async fn races(store: Arc<dyn ObjectStore>, test: &str) {
    let prefix = unique_prefix(test);
    let docs = DocStore::new(Arc::new(
        ObjectStoreVersioned::new(store.clone(), &prefix).unwrap(),
    ));

    let creators: Vec<_> = (0..N)
        .map(|i| {
            let docs = docs.clone();
            tokio::spawn(async move {
                docs.create("created.json", &Tally { writers: vec![i] })
                    .await
            })
        })
        .collect();
    let mut wins = 0;
    for t in creators {
        match t.await.unwrap() {
            Ok(_) => wins += 1,
            Err(e) => assert!(e.is_race(), "a losing create must be a race error: {e:?}"),
        }
    }
    assert_eq!(wins, 1, "exactly one concurrent create may win");

    let writers: Vec<_> = (0..N)
        .map(|i| {
            let docs = docs.clone();
            tokio::spawn(async move {
                docs.modify("modified.json", move |cur: Option<Tally>| {
                    let mut t = cur.unwrap_or(Tally { writers: vec![] });
                    if !t.writers.contains(&i) {
                        t.writers.push(i);
                    }
                    Ok(t)
                })
                .await
            })
        })
        .collect();
    for t in writers {
        t.await
            .unwrap()
            .expect("every modify lands within MODIFY_ATTEMPTS");
    }
    let mut got = docs
        .load::<Tally>("modified.json")
        .await
        .unwrap()
        .unwrap()
        .doc
        .writers;
    got.sort();
    assert_eq!(got, (0..N).collect::<Vec<_>>());

    delete_prefix(store.as_ref(), &prefix).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs() {
    require_it!();
    races(stores::rustfs(), "state_races_rustfs").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn azurite() {
    require_it!();
    races(stores::azurite(), "state_races_azurite").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_gcs() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    races(stores::gcs(), "state_races_gcs").await;
}
