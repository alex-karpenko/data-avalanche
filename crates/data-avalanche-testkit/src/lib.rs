//! Helpers for integration tests that run against `test-infra/compose.yaml` and, for GCS, a real
//! bucket. Tests call `require_it!()` / `require_gcs!()` first, so `cargo test` stays hermetic unless
//! `DATA_AVALANCHE_IT` is set.

use std::sync::Arc;

pub mod memory_sink;
pub use memory_sink::MemorySink;

use futures_util::TryStreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;

fn env_set(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| !v.is_empty())
}

/// True when `DATA_AVALANCHE_IT` is set to a non-empty value (an empty value must not enable tests).
pub fn it_enabled() -> bool {
    env_set("DATA_AVALANCHE_IT")
}

/// True when integration tests are enabled and GCS tests were not explicitly opted out with
/// `DATA_AVALANCHE_SKIP_GCS` (used by CI for fork PRs, which receive no secrets).
pub fn gcs_enabled() -> bool {
    it_enabled() && !env_set("DATA_AVALANCHE_SKIP_GCS")
}

/// True when `DATA_AVALANCHE_CLOUD` (a comma-separated list, e.g. `s3` or `s3,azure`) names
/// `cloud`. Real-cloud tests run only in `cloud.yml` or by hand, never in `ci.yml`.
pub fn cloud_enabled(cloud: &str) -> bool {
    std::env::var("DATA_AVALANCHE_CLOUD").is_ok_and(|list| names(&list, cloud))
}

fn names(list: &str, cloud: &str) -> bool {
    list.split(',').any(|c| c.trim() == cloud)
}

/// Returns early unless `DATA_AVALANCHE_CLOUD` names this cloud (`"s3"` or `"azure"`).
#[macro_export]
macro_rules! require_cloud {
    ($cloud:literal) => {
        if !$crate::cloud_enabled($cloud) {
            eprintln!(concat!(
                "skipped: set DATA_AVALANCHE_CLOUD=",
                $cloud,
                " with its credentials"
            ));
            return;
        }
    };
}

/// Returns early from a test when integration tests are disabled.
#[macro_export]
macro_rules! require_it {
    () => {
        if !$crate::it_enabled() {
            eprintln!("skipped: set DATA_AVALANCHE_IT=1 after running test-infra/up.sh");
            return;
        }
    };
}

/// Returns early unless GCS tests are enabled. When they are enabled, `stores::gcs()` fails loudly on
/// missing configuration instead of skipping, so a misconfigured CI can't pass silently.
#[macro_export]
macro_rules! require_gcs {
    () => {
        if !$crate::gcs_enabled() {
            eprintln!("skipped: GCS tests need DATA_AVALANCHE_IT=1 and no DATA_AVALANCHE_SKIP_GCS");
            return;
        }
    };
}

/// Installs aws-lc-rs as the process-wide rustls provider. Several dependencies link different
/// providers; without an explicit choice the first TLS handshake panics (DA-DEP-5). Idempotent.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// A key prefix no other test run uses: `data-avalanche-it/<test>/<uuid v4>`. Tests on the shared real
/// GCS bucket must write only under such a prefix and delete it when done.
pub fn unique_prefix(test: &str) -> String {
    format!("data-avalanche-it/{test}/{}", uuid::Uuid::new_v4())
}

/// Deletes every object under `prefix` (test cleanup). Panics on a storage error.
pub async fn delete_prefix(store: &dyn ObjectStore, prefix: &str) {
    let prefix = object_store::path::Path::from(prefix);
    let keys: Vec<_> = store
        .list(Some(&prefix))
        .map_ok(|m| Ok(m.location))
        .try_collect()
        .await
        .expect("list for cleanup");
    let deleted: Vec<_> = store
        .delete_stream(Box::pin(futures_util::stream::iter(keys)))
        .try_collect()
        .await
        .expect("delete for cleanup");
    drop(deleted);
}

/// The value of a required environment variable for a real-cloud test.
///
/// # Panics
/// When it is unset or empty, naming it.
pub fn required_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| panic!("{name} is not set (see test-infra/README.md)"))
}

/// Fixed endpoints from `test-infra/compose.yaml`.
pub mod endpoints {
    pub const POSTGRES_18: &str =
        "host=127.0.0.1 port=55418 user=avalanche password=avalanche dbname=avalanche";
    pub const MYSQL_84: &str = "mysql://root:avalanche@127.0.0.1:53384/avalanche";
    pub const MARIADB_118: &str = "mysql://root:avalanche@127.0.0.1:53318/avalanche";
    pub const RUSTFS_URL: &str = "http://127.0.0.1:59000";
    pub const RUSTFS_ACCESS_KEY: &str = "avalanche";
    pub const RUSTFS_SECRET_KEY: &str = "avalanche-secret";
    pub const TEST_BUCKET: &str = "data-avalanche-test";
}

/// Object stores for integration tests.
pub mod stores {
    use super::*;
    use endpoints::*;

    pub fn rustfs() -> Arc<dyn ObjectStore> {
        let store = AmazonS3Builder::new()
            .with_endpoint(RUSTFS_URL)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(TEST_BUCKET)
            .with_access_key_id(RUSTFS_ACCESS_KEY)
            .with_secret_access_key(RUSTFS_SECRET_KEY)
            .build()
            .expect("valid static RustFS configuration");
        Arc::new(store)
    }

    pub fn azurite() -> Arc<dyn ObjectStore> {
        let store = MicrosoftAzureBuilder::new()
            .with_use_emulator(true)
            .with_container_name(TEST_BUCKET)
            .build()
            .expect("valid static Azurite configuration");
        Arc::new(store)
    }

    /// The real GCS bucket named by `DATA_AVALANCHE_GCS_BUCKET`. Credentials come from the standard
    /// `GOOGLE_*` variables read by `object_store` (`GOOGLE_SERVICE_ACCOUNT_PATH`,
    /// `GOOGLE_SERVICE_ACCOUNT_KEY` or `GOOGLE_APPLICATION_CREDENTIALS`).
    ///
    /// # Panics
    /// When `DATA_AVALANCHE_GCS_BUCKET` is unset or the builder rejects the credentials, naming what to set.
    pub fn gcs() -> Arc<dyn ObjectStore> {
        let bucket = std::env::var("DATA_AVALANCHE_GCS_BUCKET")
            .ok()
            .filter(|b| !b.is_empty())
            .expect(
                "DATA_AVALANCHE_GCS_BUCKET is not set: export it and GOOGLE_SERVICE_ACCOUNT_PATH, \
             or set DATA_AVALANCHE_SKIP_GCS=1 (see test-infra/README.md)",
            );
        let store = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("GCS credentials: set GOOGLE_SERVICE_ACCOUNT_PATH (see test-infra/README.md)");
        Arc::new(store)
    }
}

#[cfg(test)]
mod tests {
    use super::names;

    #[test]
    fn cloud_list_parsing() {
        assert!(names("s3", "s3"));
        assert!(names("s3, azure", "azure"));
        assert!(!names("", "s3"));
        assert!(!names("s3x", "s3"));
        assert!(!names("azure", "s3"));
    }
}
