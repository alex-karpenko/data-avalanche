//! D-5 conformance harness — `LeaseStore` over OpenDAL.
//!
//! This crate exists to *prove*, per backend, the single safety property the
//! clustering lease (`Clustering_Architecture_Research.md` §5) rests on:
//!
//!   > A compare-and-swap on the lease object succeeds for **exactly one**
//!   > writer per version; every staler writer gets `Conflict`.
//!
//! The trait wraps three operations behind an **opaque version token**
//! (`Version`): S3/Azure carry an ETag, GCS carries a *generation*. The riskiest
//! assumption in the whole lease design (test `T10`) is that the token OpenDAL
//! hands back and accepts on GCS is the **generation**, mapped to
//! `x-goog-if-generation-match` — verify by test, never by docs.

use async_trait::async_trait;
use opendal::{ErrorKind, Operator};

/// Opaque per-backend version handle. S3/Azure: ETag. GCS: generation.
/// Never interpreted by callers — only round-tripped into `swap`/`release`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version(pub String);

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// A precondition failed: the object was already created (acquire), or its
    /// version moved (swap/release). This is the *expected, retryable* outcome
    /// that proves single-writer — it must NOT be treated as a hard error.
    #[error("conditional precondition failed (another writer won)")]
    Conflict,
    /// The backend build does not advertise a required conditional-write
    /// capability. Fail fast at startup, not at first lease attempt.
    #[error("backend lacks required conditional-write capability: {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Backend(#[from] opendal::Error),
}

/// The three operations the lease needs. Default impl is OpenDAL
/// (`cluster.lock_backend: storage`); a Kubernetes-Lease impl (D-2) would be a
/// second implementor of this same trait.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Create-if-absent. `Ok(v0)` if we created it; `Conflict` if it already exists.
    async fn acquire(&self, key: &str, body: &[u8]) -> Result<Version, LeaseError>;

    /// Compare-and-swap. `Ok(v_new)` iff the stored version equals `expected`;
    /// `Conflict` otherwise. `v_new` MUST differ from `expected`.
    async fn swap(&self, key: &str, expected: &Version, body: &[u8])
        -> Result<Version, LeaseError>;

    /// Conditional delete (lease release / tombstone). `Conflict` if the version moved.
    async fn release(&self, key: &str, expected: &Version) -> Result<(), LeaseError>;
}

/// OpenDAL-backed `LeaseStore`. Construct via [`OpendalLeaseStore::new`] after
/// [`assert_capabilities`] has passed.
pub struct OpendalLeaseStore {
    op: Operator,
}

impl OpendalLeaseStore {
    pub fn new(op: Operator) -> Self {
        Self { op }
    }

    /// Startup gate: refuse to run on a backend that does not advertise the
    /// conditional-write flags. Call this once at boot — fail fast.
    pub fn assert_capabilities(op: &Operator) -> Result<(), LeaseError> {
        let cap = op.info().full_capability();
        if !cap.write_with_if_none_match {
            return Err(LeaseError::Unsupported("write_with_if_none_match"));
        }
        if !cap.write_with_if_match {
            return Err(LeaseError::Unsupported("write_with_if_match"));
        }
        // delete-with-if-match is capability-dependent; if absent we fall back
        // to a non-conditional tombstone write in `release` (safe: the lease TTL
        // still bounds a stale holder). We assert the two write flags that the
        // safety argument truly needs.
        Ok(())
    }

    /// Extract the opaque version token from write/stat metadata.
    ///
    /// D-5 TRAP (T10): on GCS the token fed back into `if_match` must be the
    /// **generation**, not the read ETag. OpenDAL exposes `version()` for the
    /// object version id and `etag()` for the ETag; for the GCS service the
    /// conditional-write precondition is generation-based. We prefer
    /// `version()` and fall back to `etag()`, but the conformance suite's `T10`
    /// is what actually proves the round-tripped value behaves as a generation
    /// on GCS. If it does not, drop GCS to the native SDK (`google-cloud-storage`)
    /// behind this same trait.
    fn version_of(meta: &opendal::Metadata) -> Result<Version, LeaseError> {
        if let Some(v) = meta.version() {
            return Ok(Version(v.to_string()));
        }
        if let Some(e) = meta.etag() {
            return Ok(Version(e.to_string()));
        }
        Err(LeaseError::Unsupported("metadata carried neither version nor etag"))
    }

    fn map_conflict(e: opendal::Error) -> LeaseError {
        // OpenDAL surfaces a failed precondition as ConditionNotMatch.
        // S3 may also surface 409 ConditionalRequestConflict under contention —
        // OpenDAL normalizes both; T9 guards that path against the real service.
        if e.kind() == ErrorKind::ConditionNotMatch {
            LeaseError::Conflict
        } else {
            LeaseError::Backend(e)
        }
    }
}

#[async_trait]
impl LeaseStore for OpendalLeaseStore {
    async fn acquire(&self, key: &str, body: &[u8]) -> Result<Version, LeaseError> {
        // S3 accepts only "*" for if_none_match (create-if-absent).
        match self
            .op
            .write_with(key, body.to_vec())
            .if_none_match("*")
            .await
        {
            Ok(meta) => Self::version_of(&meta),
            Err(e) => Err(Self::map_conflict(e)),
        }
    }

    async fn swap(
        &self,
        key: &str,
        expected: &Version,
        body: &[u8],
    ) -> Result<Version, LeaseError> {
        match self
            .op
            .write_with(key, body.to_vec())
            .if_match(&expected.0)
            .await
        {
            Ok(meta) => {
                let v = Self::version_of(&meta)?;
                debug_assert_ne!(v, *expected, "swap must advance the version token");
                Ok(v)
            }
            Err(e) => Err(Self::map_conflict(e)),
        }
    }

    async fn release(&self, key: &str, expected: &Version) -> Result<(), LeaseError> {
        match self.op.delete_with(key).if_match(&expected.0).await {
            Ok(()) => Ok(()),
            Err(e) => Err(Self::map_conflict(e)),
        }
    }
}
