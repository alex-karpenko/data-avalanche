//! What the Delta sink is built from: the pipeline, where its tables live, and the settings a new
//! table is created with (config §9.4.3). The runtime (M6) fills this from the resolved config.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use data_avalanche_core::TableRef;
use data_avalanche_storage::{Backend, table_root};
use url::Url;

/// How a source `timestamp without time zone` column is stored in a **new** Delta table
/// (`sink.delta.timestamp_ntz_as`, D-MS.16). An existing column keeps its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NtzAs {
    /// Delta `timestamp_ntz`; the table needs reader 3 / writer 7 (`timestampNtz`).
    #[default]
    TimestampNtz,
    /// Delta `timestamp`: the wall-clock value stored as a UTC instant.
    Timestamp,
    /// Delta `string`, `YYYY-MM-DD HH:MM:SS.ffffff`.
    String,
}

/// The settings a table is created with (`sink.delta`, config §9.4.3). Existing tables keep theirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDefaults {
    /// `delta.checkpointInterval`.
    pub checkpoint_interval: u64,
    /// `delta.logRetentionDuration`, written in whole seconds.
    pub log_retention: Duration,
    pub timestamp_ntz_as: NtzAs,
    /// Passed through as table properties; config V-093 has already checked the `delta.*` keys.
    pub table_properties: BTreeMap<String, String>,
}

impl Default for TableDefaults {
    /// The config defaults: 10 commits, 30 days, `timestamp_ntz`, no extra properties.
    fn default() -> Self {
        Self {
            checkpoint_interval: 10,
            log_retention: Duration::from_secs(30 * 24 * 3600),
            timestamp_ntz_as: NtzAs::TimestampNtz,
            table_properties: BTreeMap::new(),
        }
    }
}

/// Where a pipeline's tables live: `<bucket>/<prefix>/<schema>/<table>/` (config §10), or a local
/// directory (tests and local runs).
#[derive(Clone, PartialEq, Eq)]
pub enum DeltaLocation {
    Local {
        root: PathBuf,
    },
    Bucket {
        backend: Backend,
        bucket: String,
        prefix: String,
        /// `object_store` option keys (`endpoint`, `allow_http`, `use_emulator`, …). Credentials
        /// come from each cloud's standard environment variables, as for the storage crate.
        options: Vec<(String, String)>,
    },
}

impl std::fmt::Debug for DeltaLocation {
    /// Option values can be secrets; only their keys print.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { root } => f.debug_struct("Local").field("root", root).finish(),
            Self::Bucket {
                backend,
                bucket,
                prefix,
                options,
            } => f
                .debug_struct("Bucket")
                .field("backend", backend)
                .field("bucket", bucket)
                .field("prefix", prefix)
                .field(
                    "options",
                    &options
                        .iter()
                        .map(|(k, _)| (k.as_str(), "***"))
                        .collect::<Vec<_>>(),
                )
                .finish(),
        }
    }
}

impl DeltaLocation {
    /// The table's root as a URL with a trailing `/`. The segments are the storage crate's
    /// (`table_root`: config §10, `~XX` escapes, never a `%`), percent-encoded for the URL; the
    /// object keys are exactly those segments.
    pub fn table_url(&self, table: &TableRef) -> Url {
        let (mut url, root) = match self {
            Self::Local { root } => (
                Url::from_directory_path(root).expect("the local root is an absolute path"),
                table_root("", "", &table.schema, &table.name).root,
            ),
            Self::Bucket {
                backend,
                bucket,
                prefix,
                ..
            } => {
                let scheme = match backend {
                    Backend::S3 => "s3",
                    Backend::Gcs => "gs",
                    Backend::Azure { .. } => "az",
                };
                let base = Url::parse(&format!("{scheme}://{bucket}/"))
                    .expect("a bucket name is a valid URL host");
                (
                    base,
                    table_root(bucket, prefix, &table.schema, &table.name).root,
                )
            }
        };
        {
            let mut segments = url.path_segments_mut().expect("a base URL");
            segments.pop_if_empty();
            for s in root.split('/') {
                segments.push(s);
            }
            segments.push("");
        }
        url
    }

    /// The options delta-rs opens a table with. delta-rs reads the `AWS_*` and `GOOGLE_*`
    /// variables itself but not `AZURE_*`, so for Azure they are added here (lower-cased, as
    /// `object_store` spells its keys); an explicit option wins.
    pub fn storage_options(
        &self,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> HashMap<String, String> {
        let Self::Bucket {
            backend, options, ..
        } = self
        else {
            return HashMap::new();
        };
        let mut out: HashMap<String, String> = HashMap::new();
        if let Backend::Azure { account } = backend {
            for (k, v) in env {
                if k.starts_with("AZURE_") {
                    out.insert(k.to_ascii_lowercase(), v);
                }
            }
            if let Some(account) = account {
                out.insert("azure_storage_account_name".into(), account.clone());
            }
        }
        for (k, v) in options {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

/// Everything a [`crate::DeltaSink`] needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaSinkConfig {
    /// The pipeline `id`: the fence's `app_id` is `data-avalanche/<pipeline_id>/owner`.
    pub pipeline_id: String,
    pub location: DeltaLocation,
    pub table: TableDefaults,
    /// A DataFusion memory ceiling for one MERGE (R-6). A MERGE that needs more fails
    /// (`Transient`) instead of growing the process. `None`: no ceiling.
    pub merge_memory_limit: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(backend: Backend, options: Vec<(String, String)>) -> DeltaLocation {
        DeltaLocation::Bucket {
            backend,
            bucket: "lake".into(),
            prefix: "orders/v1".into(),
            options,
        }
    }

    #[test]
    fn table_urls_follow_config_section_10() {
        let t = TableRef::new("public", "orders");
        assert_eq!(
            bucket(Backend::S3, vec![]).table_url(&t).as_str(),
            "s3://lake/orders/v1/public/orders/"
        );
        assert_eq!(
            bucket(Backend::Gcs, vec![]).table_url(&t).as_str(),
            "gs://lake/orders/v1/public/orders/"
        );
        assert_eq!(
            bucket(Backend::Azure { account: None }, vec![])
                .table_url(&t)
                .as_str(),
            "az://lake/orders/v1/public/orders/"
        );
        let local = DeltaLocation::Local {
            root: PathBuf::from("/data/lake"),
        };
        assert_eq!(
            local.table_url(&t).as_str(),
            "file:///data/lake/public/orders/"
        );
    }

    #[test]
    fn escaped_names_keep_the_storage_crates_segments() {
        // `table_root` escapes `%` and `/` as `~XX` (Q-M4-5); the URL encodes a space and
        // non-ASCII text, which delta-rs decodes back.
        let t = TableRef::new("50%", "a/b c.ü");
        let url = bucket(Backend::S3, vec![]).table_url(&t);
        assert_eq!(url.as_str(), "s3://lake/orders/v1/50~25/a~2Fb%20c.%C3%BC/");
        let empty_prefix = DeltaLocation::Bucket {
            backend: Backend::S3,
            bucket: "lake".into(),
            prefix: String::new(),
            options: vec![],
        };
        assert_eq!(
            empty_prefix.table_url(&TableRef::new("s", "t")).as_str(),
            "s3://lake/s/t/"
        );
    }

    #[test]
    fn azure_takes_its_environment_and_explicit_options_win() {
        let loc = bucket(
            Backend::Azure {
                account: Some("acct".into()),
            },
            vec![("azure_client_id".into(), "explicit".into())],
        );
        let env = vec![
            ("AZURE_CLIENT_ID".to_string(), "from-env".to_string()),
            ("AZURE_TENANT_ID".to_string(), "tenant".to_string()),
            ("AWS_REGION".to_string(), "ignored".to_string()),
        ];
        let opts = loc.storage_options(env);
        assert_eq!(opts["azure_client_id"], "explicit");
        assert_eq!(opts["azure_tenant_id"], "tenant");
        assert_eq!(opts["azure_storage_account_name"], "acct");
        assert!(!opts.contains_key("aws_region"));
        let s3 = bucket(Backend::S3, vec![("endpoint".into(), "http://x".into())]);
        let opts = s3.storage_options(vec![("AZURE_CLIENT_ID".into(), "x".into())]);
        assert_eq!(opts.len(), 1, "S3 reads its own environment: {opts:?}");
    }

    #[test]
    fn debug_hides_option_values() {
        let loc = bucket(
            Backend::S3,
            vec![("secret_access_key".into(), "shh-dont-tell".into())],
        );
        let text = format!("{loc:?}");
        assert!(text.contains("secret_access_key"), "{text}");
        assert!(!text.contains("shh-dont-tell"), "{text}");
    }

    #[test]
    fn defaults_are_the_config_defaults() {
        let d = TableDefaults::default();
        assert_eq!(d.checkpoint_interval, 10);
        assert_eq!(d.log_retention, Duration::from_secs(2_592_000));
        assert_eq!(d.timestamp_ntz_as, NtzAs::TimestampNtz);
    }
}
