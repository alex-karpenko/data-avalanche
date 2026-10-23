//! Test-only helpers shared across `data-avalanche-api`'s unit tests: a client, the listener on a
//! free port, and a lone node.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use data_avalanche_cluster::{
    ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
    Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Parts, Phase,
    ReconcilerSettings, Udp, WorkerFactory,
};
use data_avalanche_config::{FingerprintKey, HostPort, Pipeline, ProcessEnv, load_str};
use data_avalanche_pipeline::{Backoff, Tuning};
use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_telemetry::Metrics;
use object_store::memory::InMemory;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use crate::{Api, Node, bind, router};

pub(crate) const CONFIG: &str = r#"
config_version: 1
globals:
  cloud: aws
  state: { bucket: app-state, prefix: cluster-a }
  cluster: { lease_duration: 30s, lease_renewal_interval: 10s, drain_timeout: 45s, max_concurrent_takeovers: 2 }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: pw
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      prefix: shop
      delta: { checkpoint_interval: 20, timestamp_ntz_as: string, merge_memory_limit: 64MiB }
"#;

/// A plain HTTP/1.1 request: the status code, the headers, the body.
pub(crate) async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String, String) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n");
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    s.write_all(head.as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let (head, body) = out.split_once("\r\n\r\n").unwrap();
    let code = head[9..12].parse().unwrap();
    (code, head.to_owned(), body.to_owned())
}

pub(crate) async fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
    request(addr, "GET", path, &[], "").await
}

/// The code and the JSON body of a request without one.
pub(crate) async fn json_of(addr: SocketAddr, method: &str, path: &str) -> (u16, Value) {
    let (code, _, body) = request(addr, method, path, &[], "").await;
    (
        code,
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}")),
    )
}

/// The listener on a free loopback port, plain HTTP.
pub(crate) async fn serve(api: Api) -> SocketAddr {
    let bound = bind(
        &HostPort {
            host: "127.0.0.1".into(),
            port: 0,
        },
        None,
    )
    .await
    .unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve(router(api), std::future::pending()));
    addr
}

/// Builds no worker: the lone node runs no pipeline.
#[derive(Debug)]
struct NoWorkers;

impl WorkerFactory for NoWorkers {
    fn build(&self, p: &Pipeline) -> Result<Parts, String> {
        Err(format!("{}: the tests run no worker", p.id))
    }
}

/// A lone node `node-a` running generation 1 of [`CONFIG`], read from `file`: its gossip on a
/// loopback port, its reloads reaching a reconciler without pipelines.
pub(crate) struct Lone {
    pub node: Node,
    pub _reconciler: ClusterReconciler,
    pub _gossip: Gossip,
}

pub(crate) async fn lone(file: PathBuf) -> Lone {
    let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
    let docs = DocStore::new(Arc::new(store));
    let generations = ConfigGenerations::new(docs.clone());
    let (first, _) = generations
        .bootstrap_or_adopt(CONFIG, "node-a", chrono::Utc::now())
        .await
        .unwrap();
    let key = FingerprintKey::from_bytes([1; 32]);
    let config = load_str(CONFIG, &ProcessEnv, &key).unwrap().config;
    let (udp, inbox) = Udp::bind(&["127.0.0.1:0".parse().unwrap()]).await.unwrap();
    let me = NodeIdentity {
        node_id: "node-a".into(),
        incarnation: 1,
        rev: 0,
        addr: udp.local_addrs()[0],
        meta: NodeMeta {
            phase: Phase::Active,
            config_generation: 1,
            build: "1.0.0".into(),
        },
    };
    let gossip = Gossip::start(
        me,
        GossipKey::derive(&[7; 32]),
        GossipSettings::new(&config.globals.cluster.memberlist),
        Arc::new(udp),
        inbox,
    );
    let metrics = Metrics::new();
    let c = &config.globals.cluster;
    let deps = ControllerDeps {
        leases: Leases::new(docs),
        holder: Holder {
            node_id: "node-a".into(),
            incarnation: 1,
        },
        factory: Arc::new(NoWorkers),
        metrics: metrics.clone(),
        settings: ControllerSettings {
            timing: LeaseTiming::new(c),
            drain_timeout: c.drain_timeout,
            restart: Backoff::default(),
            start_jitter: Duration::ZERO,
            tuning: Tuning::default(),
        },
        starts: Arc::new(Semaphore::new(1)),
        handoffs: Arc::new(Semaphore::new(1)),
    };
    let reconciler =
        ClusterReconciler::start(vec![], &deps, gossip.members(), ReconcilerSettings::new(c));
    let coordinator = ConfigCoordinator::start(
        config,
        first.pointer,
        CoordinatorDeps {
            generations,
            env: Arc::new(ProcessEnv),
            key,
            node_id: "node-a".into(),
            docs: Arc::new(|p| Err(p.id.clone())),
            factory: deps.factory,
            reloader: reconciler.reloader(),
            advertiser: gossip.advertiser(),
            metrics,
            globals: Arc::new(|_| {}),
        },
        gossip.members(),
        Duration::from_secs(10),
    );
    Lone {
        node: Node {
            coordinator: Arc::new(coordinator),
            members: gossip.members(),
            file,
        },
        _reconciler: reconciler,
        _gossip: gossip,
    }
}
