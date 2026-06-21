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

/// Body written by `release` to mark a lease as voluntarily released.
///
/// OpenDAL exposes a conditional *write* (`if_match`) but **no conditional
/// delete**, so release is a CAS write of this tombstone rather than a
/// conditional `DELETE` — the design's "PUT a tombstone … If-Match" path
/// (`Clustering_Architecture_Research.md` §8.3). `acquire` recognizes the
/// tombstone and CAS-takes the object, so a released lease is re-acquirable with
/// no wait. The marker is distinct from any plausible holder body.
const TOMBSTONE: &[u8] = br#"{"_cdc_lease":"released"}"#;

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
    /// A non-OpenDAL backend error (e.g. the native GCS SDK on Route B).
    #[error("backend error: {0}")]
    Other(String),
}

/// The three operations the lease needs. Default impl is OpenDAL
/// (`cluster.lock_backend: storage`); a Kubernetes-Lease impl (D-2) would be a
/// second implementor of this same trait.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Create-if-absent, or CAS-take a previously *released* (tombstoned) object.
    /// `Ok(v0)` if we took it; `Conflict` if a **live** holder exists.
    async fn acquire(&self, key: &str, body: &[u8]) -> Result<Version, LeaseError>;

    /// Compare-and-swap. `Ok(v_new)` iff the stored version equals `expected`;
    /// `Conflict` otherwise. `v_new` MUST differ from `expected`.
    async fn swap(&self, key: &str, expected: &Version, body: &[u8])
        -> Result<Version, LeaseError>;

    /// Release via a **conditional tombstone write** (OpenDAL has no conditional
    /// delete). `Conflict` if the version moved (a challenger took over first).
    async fn release(&self, key: &str, expected: &Version) -> Result<(), LeaseError>;
}

/// Which metadata field carries the CAS token for `if_match` on this backend.
///
/// **Sourcing the wrong one silently breaks CAS.** S3/Azure `If-Match` compares the
/// **ETag**; GCS compares the **generation** (OpenDAL exposes it via
/// `Metadata::version()`). Crucially, on a **versioning-enabled S3 bucket**
/// `meta.version()` becomes populated with the object *version-id* — which `If-Match`
/// does NOT accept — so a naive "prefer version() over etag()" breaks every swap.
/// The token kind is therefore fixed by backend, not inferred from metadata.
#[derive(Clone, Copy, Debug)]
pub enum TokenKind {
    /// S3, Azure Blob — `if_match` compares the ETag.
    Etag,
    /// GCS — `if_match` maps to `x-goog-if-generation-match`; token is the generation.
    Generation,
}

/// OpenDAL-backed `LeaseStore`. Construct via [`OpendalLeaseStore::new`] after
/// [`assert_capabilities`] has passed.
pub struct OpendalLeaseStore {
    op: Operator,
    token: TokenKind,
}

impl OpendalLeaseStore {
    pub fn new(op: Operator, token: TokenKind) -> Self {
        Self { op, token }
    }

    /// Startup gate: refuse to run on a backend that does not advertise the
    /// conditional-write flags. Call this once at boot — fail fast.
    ///
    /// REAL-S3 FINDING (2026-06): S3's `If-None-Match` only supports the wildcard
    /// `*` (create-if-absent), so OpenDAL models that as the dedicated
    /// **`write_with_if_not_exists`** capability and reports `write_with_if_none_match
    /// = false` (it reserves if-none-match for arbitrary-ETag matching, which S3
    /// does not support on writes). So acquire uses `.if_not_exists(true)` and we
    /// assert `write_with_if_not_exists` — NOT `write_with_if_none_match`. The CAS
    /// update (swap/release) uses `.if_match(<etag>)` → `write_with_if_match`.
    pub fn assert_capabilities(op: &Operator) -> Result<(), LeaseError> {
        let cap = op.info().full_capability();
        // Print the conditional-write matrix so a missing flag is self-evident.
        // This is a STATIC check on operator metadata — no network call, no auth —
        // so it fails the same way regardless of credentials or bucket access.
        eprintln!(
            "[lease-conformance] conditional-write capabilities: \
             if_not_exists={} if_match={} if_none_match={}",
            cap.write_with_if_not_exists, cap.write_with_if_match, cap.write_with_if_none_match
        );
        if !cap.write_with_if_not_exists {
            return Err(LeaseError::Unsupported(
                "write_with_if_not_exists (create-if-absent)",
            ));
        }
        if !cap.write_with_if_match {
            // EXPECTED ON GCS: GCS write preconditions are generation-based
            // (`ifGenerationMatch`), not ETag/`If-Match`. OpenDAL exposes the
            // generation only as `if_not_exists` (=0) for create and has no
            // arbitrary generation-match write option, so it cannot express the
            // lease's CAS UPDATE on GCS. This is NOT an auth/bucket-access problem.
            // Resolution: implement the GCS `LeaseStore` over the native GCS SDK
            // (`ifGenerationMatch`) behind this same trait (Route B).
            return Err(LeaseError::Unsupported(
                "write_with_if_match (CAS update) — on GCS this is expected; use the native GCS SDK (Route B)",
            ));
        }
        // delete-with-if-match is capability-dependent; release tombstones via a
        // conditional `write_with(...).if_match(...)` instead, so it needs no
        // separate delete capability. These two write flags are all the safety
        // argument requires.
        Ok(())
    }

    /// Extract the CAS token from write/stat metadata, sourced per backend
    /// (`self.token`): **ETag** for S3/Azure, **generation** (`version()`) for GCS.
    ///
    /// D-5 TRAP (T10): on GCS the token fed back into `if_match` must behave as the
    /// generation, mapped to `x-goog-if-generation-match`; T10 proves it. On S3 it
    /// must be the ETag — note a versioning-enabled bucket also populates
    /// `version()` with the version-id, which `If-Match` rejects (see [`TokenKind`]).
    /// If GCS does not honor the round-tripped token, drop GCS to the native SDK
    /// behind this same trait.
    fn version_of(&self, meta: &opendal::Metadata) -> Result<Version, LeaseError> {
        let tok = match self.token {
            TokenKind::Generation => meta.version(),
            TokenKind::Etag => meta.etag(),
        };
        tok.map(|s| Version(s.to_string()))
            .ok_or(LeaseError::Unsupported(
                "metadata missing the CAS token (etag for S3/Azure, generation for GCS)",
            ))
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
        // Fast path: create-if-absent. Use the portable `if_not_exists(true)`
        // (S3 If-None-Match:* / GCS if-generation-match:0 / Azure If-None-Match:*)
        // rather than `if_none_match("*")` — S3 does not advertise arbitrary
        // if-none-match on writes (see `assert_capabilities`).
        match self
            .op
            .write_with(key, body.to_vec())
            .if_not_exists(true)
            .await
        {
            Ok(meta) => return self.version_of(&meta),
            // Object exists — fall through to the tombstone check. A failed
            // create precondition surfaces as ConditionNotMatch or AlreadyExists.
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists
                ) => {}
            Err(e) => return Err(LeaseError::Backend(e)),
        }

        // The object exists. It is re-acquirable ONLY if it is a released
        // tombstone (planned-handoff fast path, §8.3); a live holder is rejected.
        // The take below is `if_match`-guarded, so two racers over the same
        // tombstone still yield exactly one winner.
        let meta = match self.op.stat(key).await {
            Ok(m) => m,
            // Raced with a real delete/expiry — treat as contended; caller retries.
            Err(e) if e.kind() == ErrorKind::NotFound => return Err(LeaseError::Conflict),
            Err(e) => return Err(LeaseError::Backend(e)),
        };
        let cur = self.version_of(&meta)?;
        let cur_body = self
            .op
            .read(key)
            .await
            .map_err(LeaseError::Backend)?
            .to_bytes();
        if cur_body.as_ref() == TOMBSTONE {
            self.swap(key, &cur, body).await
        } else {
            Err(LeaseError::Conflict)
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
                let v = self.version_of(&meta)?;
                debug_assert_ne!(v, *expected, "swap must advance the version token");
                Ok(v)
            }
            Err(e) => Err(Self::map_conflict(e)),
        }
    }

    async fn release(&self, key: &str, expected: &Version) -> Result<(), LeaseError> {
        // OpenDAL's delete builder has no `if_match`, so a conditional release is
        // a CAS *write of a tombstone* (the design's "PUT a tombstone … If-Match"
        // path, §8.3). Preserves the stale-version → Conflict guarantee; `acquire`
        // recognizes the tombstone and re-takes it with no wait.
        match self
            .op
            .write_with(key, TOMBSTONE.to_vec())
            .if_match(&expected.0)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(Self::map_conflict(e)),
        }
    }
}

// ===========================================================================
// Route B — native GCS SDK (`--features gcs`)
// ===========================================================================
//
// OpenDAL cannot run the lease on GCS: it reports `write_with_if_match = false`
// because GCS write preconditions are generation-based (`ifGenerationMatch`),
// not ETag/`If-Match`, and OpenDAL exposes no arbitrary generation-match write
// option. So GCS uses the native `google-cloud-storage` SDK directly:
//   acquire  = upload with ifGenerationMatch = 0   (create-if-absent)
//   swap     = upload with ifGenerationMatch = N   (CAS to the holder's generation)
//   release  = upload TOMBSTONE with ifGenerationMatch = N
// The `Version` token is the object **generation** (i64, stored as a string).
//
// SCAFFOLD: written against yoshidan's `google-cloud-storage` documented API.
// Every `// VERIFY:` marks a call to reconcile against the pinned crate version
// before the first `--features gcs` compile.
#[cfg(feature = "gcs")]
pub mod gcs {
    //! Native GCS `LeaseStore` (Route B) on the **official** `google-cloud-storage`
    //! 1.x SDK (googleapis/google-cloud-rust). CAS via `ifGenerationMatch`:
    //!   acquire = write `set_if_generation_match(0)`   (create-if-absent)
    //!   swap    = write `set_if_generation_match(N)`    (CAS to the holder's gen)
    //!   release = write TOMBSTONE `set_if_generation_match(N)`
    //! Token = the object **generation** (i64 as string). Bucket is the official
    //! `projects/_/buckets/{id}` resource form. ADC auth (GOOGLE_APPLICATION_CREDENTIALS).
    //!
    //! Verified against google-cloud-storage **1.15**: T1–T10 pass on real GCS
    //! (incl. the t10 generation-CAS gate).
    use async_trait::async_trait;
    use google_cloud_storage::client::Storage;

    use crate::{LeaseError, LeaseStore, Version, TOMBSTONE};

    pub struct GcsLeaseStore {
        client: Storage,
        /// `projects/_/buckets/{bucket_id}`
        bucket: String,
    }

    impl GcsLeaseStore {
        /// Build from Application Default Credentials. `bucket_id` is the plain bucket name.
        pub async fn from_env(bucket_id: impl AsRef<str>) -> Result<Self, LeaseError> {
            let client = Storage::builder()
                .build()
                .await
                .map_err(|e| LeaseError::Other(format!("GCS client: {e}")))?;
            Ok(Self {
                client,
                bucket: format!("projects/_/buckets/{}", bucket_id.as_ref()),
            })
        }

        fn parse_gen(v: &Version) -> Result<i64, LeaseError> {
            v.0.parse::<i64>()
                .map_err(|_| LeaseError::Other(format!("non-integer generation token {:?}", v.0)))
        }

        /// A failed `ifGenerationMatch` precondition (lost the CAS) surfaces as 412 /
        /// FAILED_PRECONDITION. NOTE the official 1.x SDK reports it as `code UNKNOWN`
        /// with the message "At least one of the pre-conditions you specified did not
        /// hold." (hyphen in "pre-conditions"), so match on that wording too.
        fn is_conflict(e: &impl std::fmt::Display) -> bool {
            let s = e.to_string().to_ascii_lowercase();
            s.contains("precondition")
                || s.contains("pre-condition")
                || s.contains("did not hold")
                || s.contains("failed_precondition")
                || s.contains("412")
                || s.contains("conflict")
        }

        /// Write `body` to `object` gated by `ifGenerationMatch = if_gen`; return the new generation.
        async fn put(&self, object: &str, body: &[u8], if_gen: i64) -> Result<Version, LeaseError> {
            let resp = self
                .client
                .write_object(
                    self.bucket.clone(),
                    object.to_string(),
                    bytes::Bytes::from(body.to_vec()),
                )
                .set_if_generation_match(if_gen)
                .send_buffered()
                .await
                .map_err(|e| {
                    if Self::is_conflict(&e) {
                        LeaseError::Conflict
                    } else {
                        LeaseError::Other(format!("GCS write: {e}"))
                    }
                })?;
            // Confirmed (1.15): the write response exposes `.generation: i64`.
            Ok(Version(resp.generation.to_string()))
        }

        /// Read an object's body + current generation (for the tombstone re-acquire).
        async fn read(&self, object: &str) -> Result<(Vec<u8>, Version), LeaseError> {
            let mut resp = self
                .client
                .read_object(self.bucket.clone(), object.to_string())
                .send()
                .await
                .map_err(|e| LeaseError::Other(format!("GCS read: {e}")))?;
            // Confirmed (1.15): the read response exposes the object metadata via
            // `resp.object()`, with `.generation: i64`.
            let gen = Version(resp.object().generation.to_string());
            let mut body = Vec::new();
            while let Some(chunk) = resp
                .next()
                .await
                .transpose()
                .map_err(|e| LeaseError::Other(format!("GCS read body: {e}")))?
            {
                body.extend_from_slice(&chunk);
            }
            Ok((body, gen))
        }
    }

    #[async_trait]
    impl LeaseStore for GcsLeaseStore {
        async fn acquire(&self, key: &str, body: &[u8]) -> Result<Version, LeaseError> {
            // create-if-absent = ifGenerationMatch:0
            match self.put(key, body, 0).await {
                Ok(v) => return Ok(v),
                Err(LeaseError::Conflict) => {} // exists → tombstone check
                Err(e) => return Err(e),
            }
            // Re-acquirable only if it is a released tombstone; CAS-take by its generation.
            let (cur_body, cur_gen) = self.read(key).await?;
            if cur_body.as_slice() == TOMBSTONE {
                self.swap(key, &cur_gen, body).await
            } else {
                Err(LeaseError::Conflict)
            }
        }

        async fn swap(
            &self,
            key: &str,
            expected: &Version,
            body: &[u8],
        ) -> Result<Version, LeaseError> {
            self.put(key, body, Self::parse_gen(expected)?).await
        }

        async fn release(&self, key: &str, expected: &Version) -> Result<(), LeaseError> {
            self.put(key, TOMBSTONE, Self::parse_gen(expected)?)
                .await
                .map(|_| ())
        }
    }
}

// ===========================================================================
// Route B — native Azure Blob (`--features azure`)
// ===========================================================================
//
// OpenDAL 0.57's azblob reports `write_with_if_match = false`, so it cannot do
// the lease's CAS update on Azure — an OpenDAL limitation, NOT an Azure one
// (Azure Blob natively supports `If-Match` conditional PUT). This impl talks the
// Blob REST API directly with `reqwest`, authenticated by a **SAS token**:
//   acquire = PUT  If-None-Match: *     (create-if-absent)
//   swap    = PUT  If-Match: <etag>     (CAS to the holder's ETag)
//   release = PUT  If-Match: <etag>     (tombstone body)
// Token = the blob **ETag**. A 409/412 response = Conflict.
//
// SAS is used because it needs no request signing — generate one with:
//   az storage container generate-sas --name <c> --account-name <a> \
//     --permissions racwdl --expiry <ts> --account-key <k> -o tsv
#[cfg(feature = "azure")]
pub mod azure {
    use async_trait::async_trait;
    use reqwest::{Client, StatusCode};

    use crate::{LeaseError, LeaseStore, Version, TOMBSTONE};

    // Blob REST API version. VERIFY/bump if the service rejects it (400).
    const X_MS_VERSION: &str = "2021-08-06";

    pub struct AzureLeaseStore {
        http: Client,
        /// `https://{account}.blob.core.windows.net/{container}`
        base: String,
        /// SAS query string, without a leading `?`.
        sas: String,
    }

    impl AzureLeaseStore {
        /// From env: `AZURE_STORAGE_ACCOUNT`, `AZURE_CONTAINER`, `AZURE_STORAGE_SAS_TOKEN`.
        pub fn from_env() -> Result<Self, LeaseError> {
            let account = std::env::var("AZURE_STORAGE_ACCOUNT")
                .map_err(|_| LeaseError::Other("set AZURE_STORAGE_ACCOUNT".into()))?;
            let container = std::env::var("AZURE_CONTAINER")
                .map_err(|_| LeaseError::Other("set AZURE_CONTAINER".into()))?;
            let sas = std::env::var("AZURE_STORAGE_SAS_TOKEN").map_err(|_| {
                LeaseError::Other("set AZURE_STORAGE_SAS_TOKEN (perms: racwdl)".into())
            })?;
            Ok(Self {
                http: Client::new(),
                base: format!("https://{account}.blob.core.windows.net/{container}"),
                sas: sas.trim_start_matches('?').to_string(),
            })
        }

        fn url(&self, key: &str) -> String {
            format!("{}/{}?{}", self.base, key, self.sas)
        }

        fn etag(resp: &reqwest::Response) -> Result<Version, LeaseError> {
            resp.headers()
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| Version(s.to_string()))
                .ok_or_else(|| LeaseError::Other("Azure response missing ETag".into()))
        }

        fn is_conflict(s: StatusCode) -> bool {
            s == StatusCode::PRECONDITION_FAILED || s == StatusCode::CONFLICT
        }

        /// PUT a block blob with one conditional header `(name, value)`.
        async fn put(
            &self,
            key: &str,
            body: &[u8],
            cond: (&str, &str),
        ) -> Result<Version, LeaseError> {
            let resp = self
                .http
                .put(self.url(key))
                .header("x-ms-blob-type", "BlockBlob")
                .header("x-ms-version", X_MS_VERSION)
                .header(cond.0, cond.1)
                .body(body.to_vec())
                .send()
                .await
                .map_err(|e| LeaseError::Other(format!("Azure PUT: {e}")))?;
            let status = resp.status();
            if Self::is_conflict(status) {
                return Err(LeaseError::Conflict);
            }
            if !status.is_success() {
                return Err(LeaseError::Other(format!("Azure PUT status {status}")));
            }
            Self::etag(&resp)
        }
    }

    #[async_trait]
    impl LeaseStore for AzureLeaseStore {
        async fn acquire(&self, key: &str, body: &[u8]) -> Result<Version, LeaseError> {
            // create-if-absent
            match self.put(key, body, ("If-None-Match", "*")).await {
                Ok(v) => return Ok(v),
                Err(LeaseError::Conflict) => {} // exists → tombstone check
                Err(e) => return Err(e),
            }

            // The blob exists. Re-acquirable only if it is a released tombstone;
            // CAS-take it by its current ETag. (A live holder → Conflict.)
            let resp = self
                .http
                .get(self.url(key))
                .header("x-ms-version", X_MS_VERSION)
                .send()
                .await
                .map_err(|e| LeaseError::Other(format!("Azure GET: {e}")))?;
            if !resp.status().is_success() {
                // vanished / error → treat as contended; caller retries.
                return Err(LeaseError::Conflict);
            }
            let cur = Self::etag(&resp)?;
            let cur_body = resp
                .bytes()
                .await
                .map_err(|e| LeaseError::Other(format!("Azure GET body: {e}")))?;
            if cur_body.as_ref() == TOMBSTONE {
                self.swap(key, &cur, body).await
            } else {
                Err(LeaseError::Conflict)
            }
        }

        async fn swap(
            &self,
            key: &str,
            expected: &Version,
            body: &[u8],
        ) -> Result<Version, LeaseError> {
            self.put(key, body, ("If-Match", &expected.0)).await
        }

        async fn release(&self, key: &str, expected: &Version) -> Result<(), LeaseError> {
            self.put(key, TOMBSTONE, ("If-Match", &expected.0))
                .await
                .map(|_| ())
        }
    }
}
