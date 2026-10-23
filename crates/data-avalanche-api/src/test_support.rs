//! Test-only helpers shared across `data-avalanche-api`'s unit tests: a client, the listener on a
//! free port, a lone node, and an identity provider with keys made at run time.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::routing::get as get_route;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use data_avalanche_cluster::{
    ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
    Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Parts, Phase,
    ReconcilerSettings, Udp, WorkerFactory,
};
use data_avalanche_config::{
    ApiAuth, Env, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig, load_str,
};
use data_avalanche_pipeline::{Backoff, Tuning};
use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases, PipelineDocs};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_telemetry::Metrics;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use object_store::memory::InMemory;
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use crate::{Api, Auth, Control, Node, Stop, bind, router};

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

pub(crate) fn config() -> ResolvedConfig {
    load_str(CONFIG, &ProcessEnv, &FingerprintKey::from_bytes([1; 32]))
        .unwrap()
        .config
}

/// `globals.api` of [`CONFIG`]: `auth: none`.
pub(crate) fn open() -> Arc<Auth> {
    Arc::new(Auth::new(&config().globals.api))
}

/// The routes' state before the node runs its generation: the `/v1` routes answer 503.
pub(crate) fn api(auth: Arc<Auth>) -> Api {
    Api {
        metrics: Metrics::new(),
        ready: Arc::new(AtomicBool::new(true)),
        node: Arc::default(),
        auth,
    }
}

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
    /// The leases the node reads: a test takes them as another node would.
    pub leases: Leases,
    /// What the API asked of the node.
    pub stops: tokio::sync::watch::Receiver<Stop>,
    pub _reconciler: ClusterReconciler,
    pub _gossip: Gossip,
}

/// `${VAR}` values a test sets, and changes.
#[derive(Debug, Default)]
pub(crate) struct Vars(Mutex<BTreeMap<String, String>>);

impl Env for Vars {
    fn var(&self, name: &str) -> Option<String> {
        self.0.lock().unwrap().get(name).cloned()
    }
}

impl Vars {
    pub fn set(&self, name: &str, value: &str) {
        self.0.lock().unwrap().insert(name.into(), value.into());
    }
}

pub(crate) async fn lone(file: PathBuf) -> Lone {
    lone_with(file, CONFIG, Arc::new(ProcessEnv)).await
}

/// [`lone`], running generation 1 of `text`, resolved in `env`.
pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send + Sync>) -> Lone {
    let mem = Arc::new(InMemory::new());
    let store = ObjectStoreVersioned::new(mem.clone(), "globals").unwrap();
    let docs = DocStore::new(Arc::new(store));
    let leases = Leases::new(docs.clone());
    let generations = ConfigGenerations::new(docs.clone());
    let (first, _) = generations
        .bootstrap_or_adopt(text, "node-a", chrono::Utc::now())
        .await
        .unwrap();
    let key = FingerprintKey::from_bytes([1; 32]);
    let config = load_str(text, &*env, &key).unwrap().config;
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
        leases: leases.clone(),
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
            env,
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
    let (control, stops) = Control::new();
    Lone {
        node: Node {
            coordinator: Arc::new(coordinator),
            members: gossip.members(),
            file,
            docs: Arc::new(move |p| {
                let root = format!("lake/{}", p.sink.prefix);
                let store =
                    ObjectStoreVersioned::new(mem.clone(), &root).map_err(|e| e.to_string())?;
                Ok(PipelineDocs::new(DocStore::new(Arc::new(store))))
            }),
            leases: leases.clone(),
            control,
        },
        leases,
        stops,
        _reconciler: reconciler,
        _gossip: gossip,
    }
}

/// An identity provider: ES256 keys made at run time, its JWKS at `/jwks` and its OIDC discovery
/// document at `/.well-known/openid-configuration`, on a loopback port.
pub(crate) struct Issuer {
    pub url: String,
    keys: Arc<Mutex<Vec<(String, KeyPair)>>>,
    /// JWKS fetches so far.
    pub fetches: Arc<AtomicUsize>,
    down: Arc<AtomicBool>,
    hanging: Arc<AtomicBool>,
}

#[derive(Clone)]
struct Served {
    url: String,
    keys: Arc<Mutex<Vec<(String, KeyPair)>>>,
    fetches: Arc<AtomicUsize>,
    down: Arc<AtomicBool>,
    hanging: Arc<AtomicBool>,
}

fn jwk(kid: &str, key: &KeyPair) -> Value {
    // An uncompressed P-256 point: 0x04, then x and y.
    let raw = key.public_key_raw();
    json!({
        "kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": kid,
        "x": URL_SAFE_NO_PAD.encode(&raw[1..33]),
        "y": URL_SAFE_NO_PAD.encode(&raw[33..65]),
    })
}

async fn jwks(State(s): State<Served>) -> Result<Json<Value>, axum::http::StatusCode> {
    s.fetches.fetch_add(1, Ordering::SeqCst);
    if s.hanging.load(Ordering::SeqCst) {
        // Accepted, never answered.
        std::future::pending::<()>().await;
    }
    if s.down.load(Ordering::SeqCst) {
        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }
    let keys = s.keys.lock().unwrap();
    Ok(Json(
        json!({ "keys": keys.iter().map(|(kid, k)| jwk(kid, k)).collect::<Vec<_>>() }),
    ))
}

async fn discovery(State(s): State<Served>) -> Json<Value> {
    Json(json!({ "issuer": s.url, "jwks_uri": format!("{}/jwks", s.url) }))
}

impl Issuer {
    /// Serves one key, `k1`.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let keys = Arc::new(Mutex::new(vec![]));
        let fetches = Arc::new(AtomicUsize::new(0));
        let down = Arc::new(AtomicBool::new(false));
        let hanging = Arc::new(AtomicBool::new(false));
        let served = Served {
            down: Arc::clone(&down),
            hanging: Arc::clone(&hanging),
            url: url.clone(),
            keys: Arc::clone(&keys),
            fetches: Arc::clone(&fetches),
        };
        let app = axum::Router::new()
            .route("/jwks", get_route(jwks))
            .route("/.well-known/openid-configuration", get_route(discovery))
            .with_state(served);
        tokio::spawn(async move { axum::serve(listener, app).await });
        let issuer = Self {
            url,
            keys,
            fetches,
            down,
            hanging,
        };
        issuer.add_key("k1");
        issuer
    }

    /// A key rotation: the JWKS serves one more key.
    pub fn add_key(&self, kid: &str) {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        self.keys.lock().unwrap().push((kid.to_owned(), key));
    }

    /// A key withdrawn.
    pub fn remove_key(&self, kid: &str) {
        self.keys.lock().unwrap().retain(|(k, _)| k != kid);
    }

    /// While down, the JWKS answers 500.
    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    /// While hanging, the JWKS request is taken and never answered.
    pub fn set_hanging(&self, hanging: bool) {
        self.hanging.store(hanging, Ordering::SeqCst);
    }

    /// `globals.api.jwt` for this issuer, its tokens for audience `cdc`.
    pub fn jwt(&self, roles_claim: &str) -> Jwt {
        Jwt {
            issuer: self.url.clone(),
            audience: Some("cdc".into()),
            jwks_url: format!("{}/jwks", self.url),
            roles_claim: roles_claim.into(),
        }
    }

    /// `globals.api` with `auth: jwt` for this issuer.
    pub fn api(&self, roles_claim: &str) -> data_avalanche_config::Api {
        let mut api = config().globals.api;
        api.auth = ApiAuth::Jwt;
        api.jwt = Some(self.jwt(roles_claim));
        api
    }

    /// Claims this issuer's verifier takes, for `sub` alice, expiring in ten minutes.
    pub fn claims(&self, roles: Value) -> Value {
        let now = jsonwebtoken::get_current_timestamp();
        json!({ "iss": self.url, "aud": "cdc", "sub": "alice", "exp": now + 600, "roles": roles })
    }

    /// `claims` signed with key `kid`.
    pub fn sign(&self, kid: &str, claims: &Value) -> String {
        let keys = self.keys.lock().unwrap();
        let (_, key) = keys
            .iter()
            .find(|(k, _)| k == kid)
            .expect("a key of this issuer");
        sign_with(key, Some(kid), claims)
    }

    /// A token with `roles`, signed with `k1`.
    pub fn token(&self, roles: Value) -> String {
        self.sign("k1", &self.claims(roles))
    }
}

/// `claims` signed with `key` (ES256).
pub(crate) fn sign_with(key: &KeyPair, kid: Option<&str>, claims: &Value) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = kid.map(str::to_owned);
    jsonwebtoken::encode(
        &header,
        claims,
        &EncodingKey::from_ec_der(&key.serialize_der()),
    )
    .unwrap()
}
