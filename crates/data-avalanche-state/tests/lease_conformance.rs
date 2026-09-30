//! T1–T10: the lease's compare-and-swap guarantees (Clustering §17.4), re-proven through
//! `object_store` and `Leases` on every backend: in memory (always), RustFS and Azurite
//! (`DATA_AVALANCHE_IT`), the real GCS bucket, and real S3 / Azure (`DATA_AVALANCHE_CLOUD`,
//! `cloud.yml`). Evidence replaces `prototypes/lease-conformance` (roadmap R-2, P-5).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use data_avalanche_state::{Acquired, DocStore, Holder, LeaseDoc, Leases, StateError, Versioned};
use data_avalanche_storage::{
    Backend, BucketSpec, ObjectStoreVersioned, Timeouts, VersionedStore, open_bucket,
};
use data_avalanche_testkit::{
    delete_prefix, require_cloud, require_gcs, require_it, required_env, stores, unique_prefix,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

const DUR: Duration = Duration::from_secs(15);
const WRITERS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Token {
    /// The backend compares the ETag (S3, Azure, in memory).
    ETag,
    /// The backend compares the generation (GCS).
    Generation,
}

fn node(id: impl Into<String>) -> Holder {
    Holder {
        node_id: id.into(),
        incarnation: 1,
    }
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

async fn mine(l: &Leases, id: &str, who: &str) -> Versioned<LeaseDoc> {
    match l.acquire(id, &node(who), DUR, now()).await.unwrap() {
        Acquired::Mine(lease) => lease,
        other => panic!("{id}: expected to acquire, got {other:?}"),
    }
}

/// T1 — acquire on an empty key succeeds.
async fn t1(l: &Leases) {
    let lease = mine(l, "t1", "a").await;
    assert_eq!(lease.doc.owner_epoch, 1, "T1");
}

/// T2 — acquire on a held key leaves the holder alone.
async fn t2(l: &Leases) {
    let a = mine(l, "t2", "a").await;
    let got = l.acquire("t2", &node("b"), DUR, now()).await.unwrap();
    assert_eq!(got, Acquired::HeldBy(a), "T2");
}

/// T3 — a renew with the current version advances the version.
async fn t3(l: &Leases) {
    let a0 = mine(l, "t3", "a").await;
    let a1 = l
        .renew(&a0, now())
        .await
        .expect("T3: renew with the current version");
    assert_ne!(a0.version, a1.version, "T3: the version must advance");
    assert_eq!(l.observe("t3").await.unwrap(), Some(a1), "T3");
}

/// T4 — a write with a stale version is rejected with Conflict (the core safety property).
async fn t4(l: &Leases) {
    let a0 = mine(l, "t4", "a").await;
    let a1 = l.renew(&a0, now()).await.unwrap();
    let stale = l.renew(&a0, now()).await.unwrap_err();
    assert!(stale.is_conflict(), "T4: stale renew: {stale:?}");
    let stale = l.take_over(&a0, &node("b"), DUR, now()).await.unwrap_err();
    assert!(stale.is_conflict(), "T4: stale takeover: {stale:?}");
    assert_eq!(
        l.observe("t4").await.unwrap(),
        Some(a1),
        "T4: nothing stale landed"
    );
}

/// Every writer CAS-writes from the same version at once; returns (winners, losers' errors).
async fn contend(
    l: &Leases,
    from: &Versioned<LeaseDoc>,
) -> (Vec<Versioned<LeaseDoc>>, Vec<StateError>) {
    let tasks: Vec<_> = (0..WRITERS)
        .map(|i| {
            let (l, from) = (l.clone(), from.clone());
            tokio::spawn(
                async move { l.take_over(&from, &node(format!("n{i}")), DUR, now()).await },
            )
        })
        .collect();
    let (mut wins, mut losses) = (vec![], vec![]);
    for r in futures_util::future::join_all(tasks).await {
        match r.unwrap() {
            Ok(v) => wins.push(v),
            Err(e) => losses.push(e),
        }
    }
    (wins, losses)
}

/// T5 — N concurrent writes from one version: exactly one wins (single writer).
async fn t5(l: &Leases) {
    let a = mine(l, "t5", "a").await;
    let (wins, losses) = contend(l, &a).await;
    assert_eq!(
        wins.len(),
        1,
        "T5: exactly one concurrent swap may win; losers: {losses:?}"
    );
    assert_eq!(
        l.observe("t5").await.unwrap(),
        Some(wins[0].clone()),
        "T5: the winner is stored"
    );
}

/// T6 — release with the current version leaves a tombstone; acquire then succeeds.
async fn t6(l: &Leases) {
    let a = mine(l, "t6", "a").await;
    let tomb = l.release(&a, now()).await.expect("T6: release");
    assert!(tomb.doc.released, "T6");
    let b = mine(l, "t6", "b").await;
    assert_eq!(
        b.doc.owner_epoch, 2,
        "T6: the epoch continues past the tombstone"
    );
}

/// T7 — release with a stale version is rejected.
async fn t7(l: &Leases) {
    let a0 = mine(l, "t7", "a").await;
    let a1 = l.renew(&a0, now()).await.unwrap();
    let e = l.release(&a0, now()).await.unwrap_err();
    assert!(e.is_conflict(), "T7: {e:?}");
    assert_eq!(l.observe("t7").await.unwrap(), Some(a1), "T7: still held");
}

/// T8 — re-acquire right after a release, with no wait (planned handoff).
async fn t8(l: &Leases) {
    let a = mine(l, "t8", "a").await;
    l.release(&a, now()).await.unwrap();
    let b = l.acquire("t8", &node("b"), DUR, now()).await.unwrap();
    assert!(
        matches!(&b, Acquired::Mine(v) if v.doc.holder_node_id == "b"),
        "T8: {b:?}"
    );
}

/// T9 — contention never surfaces as a hard error: over several rounds of concurrent writes,
/// every loser gets Conflict or Contention (S3 409), never Transient or Permanent.
async fn t9(l: &Leases) {
    let mut current = mine(l, "t9", "a").await;
    for round in 0..5 {
        let (wins, losses) = contend(l, &current).await;
        for e in &losses {
            assert!(
                e.is_race(),
                "T9 round {round}: a loser got a hard error: {e:?}"
            );
        }
        assert!(wins.len() <= 1, "T9 round {round}: {} winners", wins.len());
        current = l.observe("t9").await.unwrap().unwrap();
    }
}

/// T10 — the round-tripped token is the one the backend compares (GCS: the generation): the
/// pre-write token fails after the object advanced, the post-write token succeeds.
async fn t10(l: &Leases, token: Token) {
    let v0 = mine(l, "t10", "a").await;
    let present = match token {
        Token::ETag => v0.version.e_tag.is_some(),
        Token::Generation => v0.version.version.is_some(),
    };
    assert!(present, "T10: {token:?} missing from {:?}", v0.version);
    let v1 = l.renew(&v0, now()).await.expect("T10: current token");
    assert!(
        l.renew(&v0, now()).await.unwrap_err().is_conflict(),
        "T10: pre-write token"
    );
    let v2 = l.renew(&v1, now()).await.expect("T10: post-write token");
    assert_ne!(v1.version, v2.version, "T10");
}

async fn suite(store: Arc<dyn ObjectStore>, root: &str, token: Token) {
    let versioned: Arc<dyn VersionedStore> =
        Arc::new(ObjectStoreVersioned::new(store, root).unwrap());
    let l = Leases::new(DocStore::new(versioned));
    t1(&l).await;
    t2(&l).await;
    t3(&l).await;
    t4(&l).await;
    t5(&l).await;
    t6(&l).await;
    t7(&l).await;
    t8(&l).await;
    t9(&l).await;
    t10(&l, token).await;
}

async fn suite_and_cleanup(store: Arc<dyn ObjectStore>, test: &str, token: Token) {
    let prefix = unique_prefix(test);
    suite(store.clone(), &prefix, token).await;
    delete_prefix(store.as_ref(), &prefix).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_memory() {
    suite(Arc::new(InMemory::new()), "globals", Token::ETag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs() {
    require_it!();
    suite_and_cleanup(stores::rustfs(), "lease_rustfs", Token::ETag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn azurite() {
    require_it!();
    suite_and_cleanup(stores::azurite(), "lease_azurite", Token::ETag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_gcs() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    suite_and_cleanup(stores::gcs(), "lease_gcs", Token::Generation).await;
}

fn cloud_bucket(backend: Backend, bucket_var: &str) -> Arc<dyn ObjectStore> {
    let spec = BucketSpec {
        backend,
        bucket: required_env(bucket_var),
        options: vec![],
        timeouts: Timeouts::COORDINATION,
    };
    open_bucket(&spec).unwrap_or_else(|e| panic!("{e}"))
}

/// Real S3 through the production path (`open_bucket`, AWS default credential chain).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_s3() {
    require_cloud!("s3");
    data_avalanche_testkit::install_crypto_provider();
    let store = cloud_bucket(Backend::S3, "DATA_AVALANCHE_S3_BUCKET");
    suite_and_cleanup(store, "lease_s3", Token::ETag).await;
}

/// Real Azure Blob through the production path (`open_bucket`, `AZURE_*` credentials).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_azure() {
    require_cloud!("azure");
    data_avalanche_testkit::install_crypto_provider();
    let store = cloud_bucket(
        Backend::Azure { account: None },
        "DATA_AVALANCHE_AZURE_CONTAINER",
    );
    suite_and_cleanup(store, "lease_azure", Token::ETag).await;
}
