//! Where one E2E run keeps `globals.state` and the lake: RustFS (the default), or the store
//! `DATA_AVALANCHE_E2E_STORE` names. `azurite` runs the Azure path on test-infra's emulator; `s3`,
//! `gcs` and `azure` are the real clouds of the cloud runs (Q-M12-2), each through the bucket and
//! the credentials its conformance suite already uses (`test-infra/README.md`). The binary gets
//! those credentials from the test's environment, never from the config.

use std::sync::Arc;

use data_avalanche_storage::{Backend, BucketSpec, Timeouts, open_bucket};
use data_avalanche_testkit::endpoints::{
    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
};
use data_avalanche_testkit::{required_env, stores};
use object_store::ObjectStore;

/// The credential variables a cloud run passes on to the binary, when set and not empty (a
/// workflow sets the other clouds' to ''): the binary's environment is cleared, and these are the
/// ones `object_store` reads.
const PASSED: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "GOOGLE_SERVICE_ACCOUNT_PATH",
    "GOOGLE_SERVICE_ACCOUNT_KEY",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Store {
    RustFs,
    Azurite,
    S3,
    Gcs,
    Azure,
}

impl Store {
    /// `DATA_AVALANCHE_E2E_STORE`: `rustfs` (or unset), `azurite`, `s3`, `gcs` or `azure`.
    pub fn from_env() -> Self {
        match std::env::var("DATA_AVALANCHE_E2E_STORE").as_deref() {
            Err(_) | Ok("" | "rustfs") => Self::RustFs,
            Ok("azurite") => Self::Azurite,
            Ok("s3") => Self::S3,
            Ok("gcs") => Self::Gcs,
            Ok("azure") => Self::Azure,
            Ok(other) => panic!(
                "DATA_AVALANCHE_E2E_STORE={other}: expected rustfs, azurite, s3, gcs or azure"
            ),
        }
    }

    /// `globals.cloud`.
    pub fn cloud(self) -> &'static str {
        match self {
            Self::RustFs | Self::S3 => "aws",
            Self::Gcs => "google",
            Self::Azurite | Self::Azure => "azure",
        }
    }

    pub fn backend(self) -> Backend {
        match self {
            Self::RustFs | Self::S3 => Backend::S3,
            Self::Gcs => Backend::Gcs,
            Self::Azurite | Self::Azure => Backend::Azure { account: None },
        }
    }

    /// The bucket (Azure's container) of `globals.state` and the lake.
    pub fn bucket(self) -> String {
        match self {
            Self::RustFs | Self::Azurite => TEST_BUCKET.to_owned(),
            Self::S3 => required_env("DATA_AVALANCHE_S3_BUCKET"),
            Self::Gcs => required_env("DATA_AVALANCHE_GCS_BUCKET"),
            Self::Azure => required_env("DATA_AVALANCHE_AZURE_CONTAINER"),
        }
    }

    /// The store the tests read the lake's and the state's objects through.
    pub fn objects(self) -> Arc<dyn ObjectStore> {
        match self {
            Self::RustFs => stores::rustfs(),
            Self::Azurite => stores::azurite(),
            Self::S3 | Self::Gcs | Self::Azure => open_bucket(&BucketSpec {
                backend: self.backend(),
                bucket: self.bucket(),
                options: vec![],
                timeouts: Timeouts::COORDINATION,
            })
            .unwrap_or_else(|e| panic!("{e}")),
        }
    }

    /// The Delta sink's storage options for a reader in the test: the emulators' endpoints and
    /// keys; none for a cloud, whose credentials come from the environment.
    pub fn lake_options(self) -> Vec<(String, String)> {
        let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
        match self {
            Self::RustFs => vec![
                opt("endpoint", RUSTFS_URL),
                opt("allow_http", "true"),
                opt("region", "us-east-1"),
                opt("access_key_id", RUSTFS_ACCESS_KEY),
                opt("secret_access_key", RUSTFS_SECRET_KEY),
            ],
            Self::Azurite => vec![opt("use_emulator", "true")],
            Self::S3 | Self::Gcs | Self::Azure => vec![],
        }
    }

    /// What the binary's environment gets for this store.
    pub fn env(self) -> Vec<(String, String)> {
        let var = |k: &str, v: &str| (k.to_owned(), v.to_owned());
        match self {
            Self::RustFs => vec![
                var("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY),
                var("AWS_SECRET_ACCESS_KEY", RUSTFS_SECRET_KEY),
                var("AWS_REGION", "us-east-1"),
                var("AWS_ENDPOINT_URL", RUSTFS_URL),
                var("AWS_ALLOW_HTTP", "true"),
            ],
            Self::Azurite => vec![var("AZURE_STORAGE_USE_EMULATOR", "true")],
            Self::S3 | Self::Gcs | Self::Azure => PASSED
                .iter()
                .filter_map(|k| {
                    std::env::var(k)
                        .ok()
                        .filter(|v| !v.is_empty())
                        .map(|v| ((*k).to_owned(), v))
                })
                .collect(),
        }
    }
}
