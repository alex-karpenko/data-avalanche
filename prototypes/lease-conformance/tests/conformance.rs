//! D-5 conformance suite (T1–T10) from `Clustering_Architecture_Research.md` §17.4.
//!
//! The same suite runs against an emulator (MinIO / fake-gcs-server / Azurite)
//! for functional coverage and against the **real** service for the safety-
//! critical cases. Emulators sometimes implement preconditions loosely, so
//! **T4, T5 and T10 MUST be run against the real backend at least once.**
//!
//! Backend is selected from the environment so one suite covers all three:
//!
//! ```bash
//! # MinIO example
//! export LEASE_TEST_SCHEME=s3
//! export OPENDAL_S3_BUCKET=leasetest OPENDAL_S3_ENDPOINT=http://localhost:9000 \
//!        OPENDAL_S3_REGION=us-east-1 OPENDAL_S3_ACCESS_KEY_ID=minioadmin \
//!        OPENDAL_S3_SECRET_ACCESS_KEY=minioadmin OPENDAL_S3_ROOT=/lease
//! cargo test -p lease-conformance -- --test-threads=1
//! ```
//!
//! Tests are no-ops (skip with a printed notice) when `LEASE_TEST_SCHEME` is
//! unset, so `cargo test` stays green in environments with no backend.

use std::collections::HashMap;
use std::env;

use lease_conformance::{LeaseError, LeaseStore, OpendalLeaseStore, Version};
use opendal::{Operator, Scheme};

/// Build an Operator from env, or return None to skip (no backend configured).
fn store() -> Option<OpendalLeaseStore> {
    let scheme = env::var("LEASE_TEST_SCHEME").ok()?;
    let scheme: Scheme = scheme.parse().expect("LEASE_TEST_SCHEME must be s3|gcs|azblob");

    // OpenDAL reads OPENDAL_<SCHEME>_<KEY> env vars for service config.
    let prefix = format!("OPENDAL_{}_", scheme.to_string().to_uppercase());
    let map: HashMap<String, String> = env::vars()
        .filter_map(|(k, v)| k.strip_prefix(&prefix).map(|s| (s.to_lowercase(), v)))
        .collect();

    let op = Operator::via_iter(scheme, map).expect("failed to build Operator from env");
    OpendalLeaseStore::assert_capabilities(&op).expect("backend lacks conditional-write capability");
    Some(OpendalLeaseStore::new(op))
}

fn unique_key(tag: &str) -> String {
    format!("leases/conformance-{tag}-{}.json", uuid::Uuid::new_v4())
}

macro_rules! skip_if_no_backend {
    () => {
        match store() {
            Some(s) => s,
            None => {
                eprintln!("LEASE_TEST_SCHEME unset — skipping (configure a backend to run)");
                return;
            }
        }
    };
}

/// T1 — acquire on an empty key succeeds.
#[tokio::test]
async fn t1_acquire_empty() {
    let s = skip_if_no_backend!();
    let k = unique_key("t1");
    let v0 = s.acquire(&k, b"{}").await.expect("acquire on empty must succeed");
    let _ = s.release(&k, &v0).await;
    assert!(!v0.0.is_empty(), "acquire must return a non-empty version token");
}

/// T2 — acquire on a held key is rejected (create-if-absent).
#[tokio::test]
async fn t2_acquire_held() {
    let s = skip_if_no_backend!();
    let k = unique_key("t2");
    let v0 = s.acquire(&k, b"{}").await.unwrap();
    match s.acquire(&k, b"{}").await {
        Err(LeaseError::Conflict) => {}
        other => panic!("acquire on held key must Conflict, got {other:?}"),
    }
    let _ = s.release(&k, &v0).await;
}

/// T3 — swap with the correct version advances the token.
#[tokio::test]
async fn t3_swap_correct() {
    let s = skip_if_no_backend!();
    let k = unique_key("t3");
    let v0 = s.acquire(&k, b"{\"n\":0}").await.unwrap();
    let v1 = s.swap(&k, &v0, b"{\"n\":1}").await.expect("swap with correct version must succeed");
    assert_ne!(v0, v1, "swap must advance the version token");
    let _ = s.release(&k, &v1).await;
}

/// T4 — swap with a stale version is rejected. **Core safety property.**
#[tokio::test]
async fn t4_swap_stale() {
    let s = skip_if_no_backend!();
    let k = unique_key("t4");
    let v0 = s.acquire(&k, b"{\"n\":0}").await.unwrap();
    let v1 = s.swap(&k, &v0, b"{\"n\":1}").await.unwrap();
    match s.swap(&k, &v0, b"{\"n\":2}").await {
        Err(LeaseError::Conflict) => {}
        other => panic!("swap with stale version MUST Conflict (safety!), got {other:?}"),
    }
    let _ = s.release(&k, &v1).await;
}

/// T5 — N concurrent swaps on the same version: exactly one wins. **Proves single-writer.**
#[tokio::test]
async fn t5_concurrent_swaps() {
    let s = skip_if_no_backend!();
    let k = unique_key("t5");
    let v0 = s.acquire(&k, b"{\"n\":0}").await.unwrap();

    const N: usize = 8;
    let mut handles = Vec::new();
    for i in 0..N {
        // Each task contends with the same starting version v0.
        let key = k.clone();
        let ver = v0.clone();
        // NB: build an independent store per task (Operators are cheap clones in
        // real code; here we rebuild from env to avoid sharing).
        handles.push(tokio::spawn(async move {
            let s = store().unwrap();
            s.swap(&key, &ver, format!("{{\"w\":{i}}}").as_bytes()).await
        }));
    }
    let results = futures::future::join_all(handles).await;
    let wins = results
        .into_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.is_ok())
        .count();
    assert_eq!(wins, 1, "exactly one concurrent swap may win; got {wins}");

    // cleanup: whoever holds it now — re-acquire fresh version then release
    if let Ok(meta_v) = s.acquire(&unique_key("t5-noop"), b"{}").await {
        let _ = s.release(&unique_key("t5-noop"), &meta_v).await;
    }
}

/// T6 — release with the correct version removes the object.
#[tokio::test]
async fn t6_release_correct() {
    let s = skip_if_no_backend!();
    let k = unique_key("t6");
    let v0 = s.acquire(&k, b"{}").await.unwrap();
    s.release(&k, &v0).await.expect("release with correct version must succeed");
    // After release the key is free → acquire must succeed again (T8 fast path).
    let v1 = s.acquire(&k, b"{}").await.expect("acquire after release must succeed immediately");
    let _ = s.release(&k, &v1).await;
}

/// T7 — release with a stale version is rejected.
#[tokio::test]
async fn t7_release_stale() {
    let s = skip_if_no_backend!();
    let k = unique_key("t7");
    let v0 = s.acquire(&k, b"{\"n\":0}").await.unwrap();
    let v1 = s.swap(&k, &v0, b"{\"n\":1}").await.unwrap();
    match s.release(&k, &v0).await {
        Err(LeaseError::Conflict) => {}
        other => panic!("release with stale version must Conflict, got {other:?}"),
    }
    let _ = s.release(&k, &v1).await;
}

/// T8 — acquire immediately after release (no wait): the planned-handoff fast path.
#[tokio::test]
async fn t8_reacquire_no_wait() {
    let s = skip_if_no_backend!();
    let k = unique_key("t8");
    let v0 = s.acquire(&k, b"{}").await.unwrap();
    s.release(&k, &v0).await.unwrap();
    let v1 = s.acquire(&k, b"{}").await.expect("planned handoff must re-acquire with no wait");
    let _ = s.release(&k, &v1).await;
}

/// T9 — (S3 real-service) contention surfaces 409 ConditionalRequestConflict as
/// a retryable Conflict, not a hard error. Functionally covered by T5; this test
/// documents the requirement and is meaningful only against real S3 under load.
#[tokio::test]
async fn t9_s3_conflict_is_retryable() {
    let s = skip_if_no_backend!();
    if env::var("LEASE_TEST_SCHEME").as_deref() != Ok("s3") {
        eprintln!("T9 is S3-specific — skipping");
        return;
    }
    // Same shape as T5; the assertion of interest is that no result is a hard
    // Backend error — every loser is a clean Conflict.
    let k = unique_key("t9");
    let v0 = s.acquire(&k, b"{}").await.unwrap();
    let v_clone = v0.clone();
    let k2 = k.clone();
    let h = tokio::spawn(async move {
        let s2 = store().unwrap();
        s2.swap(&k2, &v_clone, b"{\"x\":1}").await
    });
    let r1 = s.swap(&k, &v0, b"{\"y\":1}").await;
    let r2 = h.await.unwrap();
    for r in [r1, r2] {
        assert!(
            matches!(r, Ok(_) | Err(LeaseError::Conflict)),
            "contention must yield Ok or Conflict, never a hard Backend error: {r:?}"
        );
    }
}

/// T10 — (GCS real-service) **the trap.** The token round-tripped into `swap`
/// must behave as the **generation**: a swap with the pre-swap token must fail
/// after the object has advanced, and a swap with the post-swap token must
/// succeed. If this fails on GCS, OpenDAL is not mapping `if_match` to
/// `x-goog-if-generation-match` — drop GCS to the native SDK behind `LeaseStore`.
#[tokio::test]
async fn t10_gcs_generation_semantics() {
    let s = skip_if_no_backend!();
    if env::var("LEASE_TEST_SCHEME").as_deref() != Ok("gcs") {
        eprintln!("T10 is GCS-specific — skipping");
        return;
    }
    let k = unique_key("t10");
    let v0 = s.acquire(&k, b"{\"n\":0}").await.unwrap();
    let v1 = s.swap(&k, &v0, b"{\"n\":1}").await.expect("swap with current gen must succeed");
    // Pre-swap token (v0) must now be stale → Conflict (generation moved).
    assert!(
        matches!(s.swap(&k, &v0, b"{\"n\":2}").await, Err(LeaseError::Conflict)),
        "GCS: pre-swap generation token must be rejected after the object advanced"
    );
    // Post-swap token (v1) must succeed.
    let v2 = s.swap(&k, &v1, b"{\"n\":3}").await.expect("GCS: current generation token must succeed");
    assert_ne!(v1, v2);
    let _ = s.release(&k, &v2).await;
}
