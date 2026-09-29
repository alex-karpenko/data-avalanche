//! The M2 startup sequence end to end, on in-memory buckets (config crate docs, "Startup and
//! reload"): load the config with a random fingerprint key to learn `globals.state`; read or
//! create the cluster anchor there; check this node's cluster settings against it (V-062, V-076,
//! V-094); load again with the anchor's key; adopt or bootstrap the config generation; open each
//! pipeline's state.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use data_avalanche_config::{
    ClusterAnchor, FingerprintKey, LockBackend, Pipeline, ResolvedConfig, StateLocation,
    check_cluster_anchor, load_str,
};
use data_avalanche_state::{
    AnchorDoc, ConfigGeneration, ConfigGenerations, DesiredState, DocStore, HashAlgo, PipelineDocs,
    StateError, establish_anchor,
};
use data_avalanche_storage::{
    Location, ObjectStoreVersioned, colocated_state, explicit_state, globals_state,
};
use object_store::memory::InMemory;

const CONFIG: &str = r#"
config_version: 1
globals:
  state: { bucket: app-state, prefix: cluster-a }
  cluster: { tokens: 64, lease_duration: 20s }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
  invoices:
    id: invoices
    source:
      host: db.example.com
      db: billing
      user: cdc
      password: ${PW}
      postgres: { publication: invoices_pub, slot: invoices_slot }
    tables:
      include: [public.invoices]
    sink:
      type: delta
      bucket: lake
      delta: {}
    state: { bucket: ops, prefix: invoices-state }
"#;

/// One in-memory object store per bucket name, shared by every simulated node.
#[derive(Default)]
struct Buckets(Mutex<HashMap<String, Arc<InMemory>>>);

impl Buckets {
    fn docs(&self, at: &Location) -> DocStore {
        let store = self
            .0
            .lock()
            .unwrap()
            .entry(at.bucket.clone())
            .or_default()
            .clone();
        DocStore::new(Arc::new(
            ObjectStoreVersioned::new(store, &at.root).unwrap(),
        ))
    }
}

fn env() -> BTreeMap<String, String> {
    BTreeMap::from([("PW".to_string(), "s3cret".to_string())])
}

fn load(text: &str, key: &FingerprintKey) -> ResolvedConfig {
    load_str(text, &env(), key)
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config
}

fn state_location(p: &Pipeline) -> Location {
    match &p.state {
        StateLocation::CoLocated => colocated_state(&p.sink.bucket, &p.sink.prefix),
        StateLocation::Explicit {
            bucket,
            parent_prefix,
            prefix,
        } => explicit_state(bucket, parent_prefix.as_deref(), prefix),
    }
}

struct Started {
    config: ResolvedConfig,
    anchor: AnchorDoc,
    generation: ConfigGeneration,
}

/// Steps 1–5 for one node. `Err` carries the refusal a real node would print.
async fn start(buckets: &Buckets, node: &str, text: &str) -> Result<Started, String> {
    // 1. First load: only globals.state and the cluster settings matter; the key is throwaway.
    let first = load(text, &FingerprintKey::random());
    let g = &first.globals;
    let globals = buckets.docs(&globals_state(&g.state.bucket, &g.state.prefix));

    // 2. Read or create the anchor; the creator generates the fingerprint key.
    let proposal = AnchorDoc {
        tokens: g.cluster.tokens,
        lease_duration_ms: u64::try_from(g.cluster.lease_duration.as_millis()).unwrap(),
        lock_backend: g.cluster.lock_backend.to_string(),
        hash_algo: HashAlgo::Xxh3,
        fingerprint_key: *FingerprintKey::random().as_bytes(),
        established_by: node.to_owned(),
        established_at: Utc::now(),
    };
    let anchor = establish_anchor(&globals, &proposal)
        .await
        .map_err(|e| e.to_string())?
        .doc;

    // 3. This node's settings must equal the cluster's.
    let established = ClusterAnchor {
        tokens: anchor.tokens,
        lock_backend: anchor
            .lock_backend
            .parse::<LockBackend>()
            .map_err(|e| e.to_string())?,
        lease_duration: Duration::from_millis(anchor.lease_duration_ms),
    };
    let report = check_cluster_anchor(g, &established);
    if !report.is_ok() {
        return Err(report
            .iter()
            .map(|d| format!("{} {}", d.rule, d.message))
            .collect::<Vec<_>>()
            .join("; "));
    }

    // 4. Adopt the cloud config generation, or bootstrap it from this file when none exists yet
    //    (Clustering §9.3): the generation's text, not this node's local file, is the running
    //    config from here on.
    let (generation, _created) = ConfigGenerations::new(globals)
        .bootstrap_or_adopt(text, node, Utc::now())
        .await
        .map_err(|e| e.to_string())?;

    // 5. The running config: the adopted generation's text, loaded with the cluster's key.
    let config = load(
        &generation.text,
        &FingerprintKey::from_bytes(anchor.fingerprint_key),
    );

    Ok(Started {
        config,
        anchor,
        generation,
    })
}

#[tokio::test]
async fn nodes_share_the_anchor_and_fingerprint_key() {
    let buckets = Buckets::default();
    let a = start(&buckets, "node-a", CONFIG).await.unwrap();
    let b = start(&buckets, "node-b", CONFIG).await.unwrap();
    assert_eq!(a.anchor, b.anchor, "node-b adopts node-a's anchor");
    assert_eq!(
        (a.anchor.established_by.as_str(), a.anchor.tokens),
        ("node-a", 64)
    );
    let creds = |s: &Started| {
        serde_json::to_value(&s.config.pipelines["orders"].source.credentials).unwrap()
    };
    assert_eq!(
        creds(&a),
        creds(&b),
        "the same secret fingerprints the same on every node"
    );
    assert!(!creds(&a).to_string().contains("s3cret"));

    let random = load(CONFIG, &FingerprintKey::random());
    let other = serde_json::to_value(&random.pipelines["orders"].source.credentials).unwrap();
    assert_ne!(
        creds(&a),
        other,
        "without the anchor's key the fingerprints would disagree"
    );
}

#[tokio::test]
async fn a_node_with_other_cluster_settings_refuses_to_start() {
    let buckets = Buckets::default();
    start(&buckets, "node-a", CONFIG).await.unwrap();
    let other = CONFIG.replace(
        "tokens: 64, lease_duration: 20s",
        "tokens: 128, lease_duration: 20s",
    );
    let e = start(&buckets, "node-b", &other)
        .await
        .err()
        .expect("node-b must refuse");
    assert_eq!(
        e,
        "V-062 tokens differs from the cluster's established value 64; every node must use the same value"
    );
    let elsewhere = other.replace("prefix: cluster-a", "prefix: cluster-b");
    start(&buckets, "node-c", &elsewhere)
        .await
        .expect("another globals.state prefix is another cluster");
}

#[tokio::test]
async fn a_restarted_node_adopts_the_cloud_generation_not_its_file() {
    let buckets = Buckets::default();
    start(&buckets, "node-a", CONFIG).await.unwrap();
    // A change that lands in `ResolvedConfig`, not just a comment: the running config must come
    // from the adopted generation, not from this local, edited file.
    let edited = CONFIG.replace(
        "include: [public.orders]",
        "include: [public.orders, public.orders_edited]",
    );
    assert_ne!(edited, CONFIG, "the replace must actually match");
    let restarted = start(&buckets, "node-a", &edited).await.unwrap();
    let globals = buckets.docs(&globals_state("app-state", "cluster-a"));
    let current = ConfigGenerations::new(globals)
        .current()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (current.pointer.doc.generation, current.text.as_str()),
        (1, CONFIG)
    );
    assert_eq!(
        (
            restarted.generation.pointer.doc.generation,
            restarted.generation.text.as_str()
        ),
        (1, CONFIG),
        "the node's own adopted generation is also generation 1 with the original text"
    );
    assert_eq!(
        restarted.config.pipelines["orders"].tables.include,
        vec!["public.orders".to_string()],
        "the running config is loaded from the adopted generation's text, not the local file \
         (Clustering §9.3)"
    );
}

#[tokio::test]
async fn pipeline_state_lives_where_config_section_10_puts_it() {
    let buckets = Buckets::default();
    let node = start(&buckets, "node-a", CONFIG).await.unwrap();
    let mut locations = vec![];
    for p in node.config.pipelines.values() {
        let at = state_location(p);
        let docs = PipelineDocs::new(buckets.docs(&at));
        let opened = docs.open(&p.id, &p.sink.kind.to_string()).await.unwrap();
        assert_eq!(opened.doc.desired_state, DesiredState::Running);
        locations.push((p.id.clone(), at));
    }
    locations.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        locations,
        vec![
            (
                "invoices".into(),
                Location {
                    bucket: "ops".into(),
                    root: "invoices-state".into()
                }
            ),
            (
                "orders".into(),
                Location {
                    bucket: "lake".into(),
                    root: "orders/.pipeline-state".into()
                }
            ),
        ]
    );

    let orders = &node.config.pipelines["orders"];
    let docs = PipelineDocs::new(buckets.docs(&state_location(orders)));
    let e = docs.open("orders", "iceberg").await.unwrap_err();
    assert!(matches!(e, StateError::SinkTypeMismatch { .. }), "{e:?}");
}
