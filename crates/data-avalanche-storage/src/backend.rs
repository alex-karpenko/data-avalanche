//! Opening a bucket: `object_store` builders fed from the environment's credential chain, with
//! explicit options and bounded timeouts on top.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::{BackoffConfig, ClientConfigKey, ObjectStore, RetryConfig};

/// Which object store a bucket lives in (config `globals.cloud`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// Credentials from the AWS default chain (`AWS_*` variables, web identity, instance profile).
    S3,
    /// Credentials from `GOOGLE_*` variables or Application Default Credentials.
    Gcs,
    /// Credentials from `AZURE_*` variables, workload identity or managed identity. `account` is
    /// `globals.azure.account_name`; `None` takes `AZURE_STORAGE_ACCOUNT_NAME`.
    Azure { account: Option<String> },
}

/// How long one storage call may take. Each request is bounded by `request` (`connect` for the
/// TCP/TLS connect), and retries stop once `retry_timeout` has passed since the first attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    pub request: Duration,
    pub connect: Duration,
    pub max_retries: usize,
    pub retry_timeout: Duration,
}

impl Timeouts {
    /// For leases and state documents: a hung or failing store surfaces within about
    /// `retry_timeout + request` = 8 s, about half the default 15 s lease, so the caller decides
    /// instead of waiting. (`object_store`'s own defaults allow 30 s per request and 3 minutes of
    /// retries.)
    pub const COORDINATION: Self = Self {
        request: Duration::from_secs(3),
        connect: Duration::from_secs(1),
        max_retries: 2,
        retry_timeout: Duration::from_secs(5),
    };

    fn retry(&self) -> RetryConfig {
        RetryConfig {
            backoff: BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(2),
                base: 2.0,
            },
            max_retries: self.max_retries,
            retry_timeout: self.retry_timeout,
        }
    }
}

/// One bucket (Azure: container) to open.
#[derive(Clone, PartialEq, Eq)]
pub struct BucketSpec {
    pub backend: Backend,
    pub bucket: String,
    /// `object_store` config keys for the backend's builder (e.g. `endpoint`, `allow_http`,
    /// `region`, `use_emulator`), applied over the environment.
    pub options: Vec<(String, String)>,
    pub timeouts: Timeouts,
}

impl std::fmt::Debug for BucketSpec {
    /// Option values can be secrets (`secret_access_key`, `sas_token`); only their keys print.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BucketSpec")
            .field("backend", &self.backend)
            .field("bucket", &self.bucket)
            .field(
                "options",
                &self
                    .options
                    .iter()
                    .map(|(k, _)| (k.as_str(), "***"))
                    .collect::<Vec<_>>(),
            )
            .field("timeouts", &self.timeouts)
            .finish()
    }
}

/// A bucket that can't be opened. The message names an option key, never its value.
#[derive(Debug, thiserror::Error)]
#[error("cannot open bucket {bucket}: {reason}")]
pub struct OpenError {
    pub bucket: String,
    pub reason: String,
}

fn ms(d: Duration) -> String {
    format!("{}ms", d.as_millis())
}

/// Replaces every non-empty option value that appears in a builder's error text with `***`,
/// so a rejected value (e.g. an unparsable `allow_http`) never reaches `OpenError.reason`.
fn redact(mut reason: String, options: &[(String, String)]) -> String {
    for (_, v) in options {
        if !v.is_empty() {
            reason = reason.replace(v.as_str(), "***");
        }
    }
    reason
}

/// Builds the store for `spec`. Nothing is contacted until the first request.
///
/// # Errors
/// An unknown option key, or a configuration the builder rejects (e.g. no Azure account).
pub fn open_bucket(spec: &BucketSpec) -> Result<Arc<dyn ObjectStore>, OpenError> {
    let err = |reason: String| OpenError {
        bucket: spec.bucket.clone(),
        reason,
    };
    let key_err = |k: &str| err(format!("unknown option {k:?}"));
    let t = &spec.timeouts;
    let store: Arc<dyn ObjectStore> = match &spec.backend {
        Backend::S3 => {
            let mut b = AmazonS3Builder::from_env().with_bucket_name(&spec.bucket);
            for (k, v) in &spec.options {
                b = b.with_config(AmazonS3ConfigKey::from_str(k).map_err(|_| key_err(k))?, v);
            }
            let b = b
                .with_config(
                    AmazonS3ConfigKey::Client(ClientConfigKey::Timeout),
                    ms(t.request),
                )
                .with_config(
                    AmazonS3ConfigKey::Client(ClientConfigKey::ConnectTimeout),
                    ms(t.connect),
                )
                .with_retry(t.retry());
            Arc::new(
                b.build()
                    .map_err(|e| err(redact(e.to_string(), &spec.options)))?,
            )
        }
        Backend::Gcs => {
            let mut b = GoogleCloudStorageBuilder::from_env().with_bucket_name(&spec.bucket);
            for (k, v) in &spec.options {
                b = b.with_config(GoogleConfigKey::from_str(k).map_err(|_| key_err(k))?, v);
            }
            let b = b
                .with_config(
                    GoogleConfigKey::Client(ClientConfigKey::Timeout),
                    ms(t.request),
                )
                .with_config(
                    GoogleConfigKey::Client(ClientConfigKey::ConnectTimeout),
                    ms(t.connect),
                )
                .with_retry(t.retry());
            Arc::new(
                b.build()
                    .map_err(|e| err(redact(e.to_string(), &spec.options)))?,
            )
        }
        Backend::Azure { account } => {
            let mut b = MicrosoftAzureBuilder::from_env().with_container_name(&spec.bucket);
            if let Some(account) = account {
                b = b.with_account(account);
            }
            for (k, v) in &spec.options {
                b = b.with_config(AzureConfigKey::from_str(k).map_err(|_| key_err(k))?, v);
            }
            let b = b
                .with_config(
                    AzureConfigKey::Client(ClientConfigKey::Timeout),
                    ms(t.request),
                )
                .with_config(
                    AzureConfigKey::Client(ClientConfigKey::ConnectTimeout),
                    ms(t.connect),
                )
                .with_retry(t.retry());
            Arc::new(
                b.build()
                    .map_err(|e| err(redact(e.to_string(), &spec.options)))?,
            )
        }
    };
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_option_values_but_keeps_keys() {
        let spec = BucketSpec {
            backend: Backend::S3,
            bucket: "lake".into(),
            options: vec![("secret_access_key".into(), "shh-dont-tell".into())],
            timeouts: Timeouts::COORDINATION,
        };
        let text = format!("{spec:?}");
        assert!(text.contains("secret_access_key"), "{text}");
        assert!(!text.contains("shh-dont-tell"), "{text}");
    }
}
