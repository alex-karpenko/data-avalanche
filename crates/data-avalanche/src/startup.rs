//! The buckets, and the M2 startup sequence in `globals.state` (config crate docs, "Startup and
//! reload"): the cluster anchor, this node's settings against it, the config generation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::Utc;
use data_avalanche_config::{
    Cloud, ClusterAnchor, FingerprintKey, Globals, LockBackend, Pipeline, ResolvedConfig,
    StateLocation, check_cluster_anchor, load_str,
};
use data_avalanche_state::{
    AnchorDoc, ConfigGenerations, DocStore, HashAlgo, PipelineDocs, establish_anchor,
};
use data_avalanche_storage::{
    Backend, BucketSpec, Location, ObjectStoreVersioned, Timeouts, colocated_state, explicit_state,
    globals_state, open_bucket,
};
use object_store::ObjectStore;

use crate::run::Fatal;

/// The store `globals.cloud` names.
pub(crate) fn backend(globals: &Globals) -> Backend {
    match globals.cloud {
        Cloud::Aws => Backend::S3,
        Cloud::Google => Backend::Gcs,
        Cloud::Azure => Backend::Azure {
            account: globals.azure.account_name.clone(),
        },
    }
}

/// Where a pipeline's state lives (config §10).
pub(crate) fn state_location(p: &Pipeline) -> Location {
    match &p.state {
        StateLocation::CoLocated => colocated_state(&p.sink.bucket, &p.sink.prefix),
        StateLocation::Explicit {
            bucket,
            parent_prefix,
            prefix,
        } => explicit_state(bucket, parent_prefix.as_deref(), prefix),
    }
}

/// The process's buckets, each opened once. Credentials and endpoints come from the cloud's
/// standard environment variables.
#[derive(Debug)]
pub(crate) struct Buckets {
    backend: Backend,
    opened: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl Buckets {
    pub fn new(globals: &Globals) -> Self {
        Self {
            backend: backend(globals),
            opened: Mutex::new(HashMap::new()),
        }
    }

    /// The documents at `at`, with the coordination timeouts (a hung store surfaces within half
    /// a default lease).
    pub fn docs(&self, at: &Location) -> Result<DocStore, Fatal> {
        let mut opened = self.opened.lock().unwrap_or_else(PoisonError::into_inner);
        let store = match opened.get(&at.bucket) {
            Some(s) => Arc::clone(s),
            None => {
                let store = open_bucket(&BucketSpec {
                    backend: self.backend.clone(),
                    bucket: at.bucket.clone(),
                    options: vec![],
                    timeouts: Timeouts::COORDINATION,
                })
                .map_err(|e| Fatal::Startup(e.to_string()))?;
                opened.insert(at.bucket.clone(), Arc::clone(&store));
                store
            }
        };
        let versioned = ObjectStoreVersioned::new(store, &at.root)
            .map_err(|e| Fatal::Startup(format!("state at {}/{}: {e}", at.bucket, at.root)))?;
        Ok(DocStore::new(Arc::new(versioned)))
    }

    pub fn pipeline_docs(&self, p: &Pipeline) -> Result<PipelineDocs, Fatal> {
        Ok(PipelineDocs::new(self.docs(&state_location(p))?))
    }
}

/// The running config and the `globals.state` documents, once this node has joined.
#[derive(Debug)]
pub(crate) struct Joined {
    pub config: ResolvedConfig,
    pub globals: DocStore,
}

/// Steps 2–5 of the startup sequence; `first` is the config loaded with a throwaway key.
pub(crate) async fn join(
    buckets: &Buckets,
    node: &str,
    text: &str,
    first: &ResolvedConfig,
) -> Result<Joined, Fatal> {
    let g = &first.globals;
    let globals = buckets.docs(&globals_state(&g.state.bucket, &g.state.prefix))?;
    let proposal = AnchorDoc {
        tokens: g.cluster.tokens,
        lease_duration_ms: u64::try_from(g.cluster.lease_duration.as_millis()).unwrap_or(u64::MAX),
        lock_backend: g.cluster.lock_backend.to_string(),
        hash_algo: HashAlgo::Xxh3,
        fingerprint_key: *FingerprintKey::random().as_bytes(),
        established_by: node.to_owned(),
        established_at: Utc::now(),
    };
    let anchor = establish_anchor(&globals, &proposal)
        .await
        .map_err(|e| Fatal::Startup(format!("the cluster anchor: {e}")))?
        .doc;
    let established = ClusterAnchor {
        tokens: anchor.tokens,
        lock_backend: anchor
            .lock_backend
            .parse::<LockBackend>()
            .map_err(|e| Fatal::Startup(format!("the cluster anchor: {e}")))?,
        lease_duration: Duration::from_millis(anchor.lease_duration_ms),
    };
    let report = check_cluster_anchor(g, &established);
    if !report.is_ok() {
        return Err(Fatal::Config(crate::run::render(&report)));
    }
    let (generation, created) = ConfigGenerations::new(globals.clone())
        .bootstrap_or_adopt(text, node, Utc::now())
        .await
        .map_err(|e| Fatal::Startup(format!("the config generation: {e}")))?;
    if !created && generation.text != text {
        tracing::warn!(
            generation = generation.pointer.doc.generation,
            "the cluster runs config generation {} from globals.state, not this node's file",
            generation.pointer.doc.generation
        );
    }
    let key = FingerprintKey::from_bytes(anchor.fingerprint_key);
    let config = load_str(&generation.text, &data_avalanche_config::ProcessEnv, &key)
        .map_err(|r| Fatal::Config(crate::run::render(&r)))?
        .config;
    Ok(Joined { config, globals })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::tests::config;

    #[test]
    fn the_cloud_names_the_backend_and_the_state_sits_beside_the_tables() {
        let mut c = config();
        assert_eq!(backend(&c.globals), Backend::S3);
        c.globals.cloud = Cloud::Azure;
        c.globals.azure.account_name = Some("acct".into());
        assert_eq!(
            backend(&c.globals),
            Backend::Azure {
                account: Some("acct".into())
            }
        );
        let at = state_location(&c.pipelines["orders"]);
        assert_eq!(at, colocated_state("lake", "shop"));
    }
}
