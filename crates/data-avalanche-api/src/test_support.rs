//! Test-only helpers shared across `data-avalanche-api`'s unit tests: a client, the listener on a
//! free port, a lone node, and an identity provider with keys made at run time.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::routing::get as get_route;
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use data_avalanche_cluster::{
    ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
    Gossip, GossipKey, GossipSettings, LeaseTiming, Member, Members, NodeIdentity, NodeMeta, Parts,
    Phase, ReconcilerSettings, Recorder, Udp, WorkerFactory, file_hash, removed_pipelines,
};
use data_avalanche_config::{
    ApiAuth, Console, Env, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig,
    Secret, load_str,
};
use data_avalanche_pipeline::{Backoff, Tuning};
use data_avalanche_state::{
    ConfigGenerations, DocStore, Events, Holder, Leases, PipelineDocs, Removals,
};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_telemetry::Metrics;
use data_avalanche_testkit::{FakeSource, MemorySink};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use object_store::memory::InMemory;
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Semaphore, watch};

use crate::peers::{FORWARDED_BY, TARGET};
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

/// `globals.console` of [`CONFIG`]: disabled, its defaults.
pub(crate) fn console() -> Console {
    config().globals.console
}

/// [`console`], naming a token's caller by `username_claim`.
pub(crate) fn console_naming(username_claim: &str) -> Console {
    Console {
        username_claim: username_claim.into(),
        ..console()
    }
}

/// The session key the tests' consoles seal with.
pub(crate) const SESSION_KEY: &str = "the tests' session key, of 32 bytes or more";

/// [`console`], enabled, its sessions sealed with [`SESSION_KEY`] and lasting `ttl`.
pub(crate) fn sessions(ttl: Duration) -> Console {
    Console {
        enabled: true,
        session_key: Some(Secret::new(
            SESSION_KEY,
            &FingerprintKey::from_bytes([1; 32]),
        )),
        session_ttl: ttl,
        ..console()
    }
}

/// `globals.api` with `auth: basic`, its one user `ops` with password `password`.
pub(crate) fn basic_api(password: &str) -> data_avalanche_config::Api {
    let mut api = config().globals.api;
    api.auth = ApiAuth::Basic;
    api.credentials = Some(data_avalanche_config::Credentials::Inline {
        user: "ops".into(),
        password: Secret::new(password, &FingerprintKey::from_bytes([1; 32])),
    });
    api
}

/// The `Cookie` header that carries `session`, sealed with [`SESSION_KEY`].
pub(crate) fn session_cookie(session: &crate::session::Session) -> String {
    let sealer = crate::session::Sealer::new(SESSION_KEY);
    crate::session::as_sent(&session.cookies(&sealer, 3600).unwrap())
}

/// A basic session of user `ops`, who gave `password`, signed in `ago` before now.
pub(crate) fn basic_session(password: &str, ago: Duration) -> crate::session::Session {
    let sealer = crate::session::Sealer::new(SESSION_KEY);
    crate::session::Session::new(
        crate::session::By::Basic {
            user: "ops".into(),
            fingerprint: sealer.fingerprint(password),
        },
        chrono::Utc::now().timestamp() - i64::try_from(ago.as_secs()).unwrap(),
    )
}

/// What a request with these headers presented.
pub(crate) fn presented(headers: &[(&str, &str)]) -> crate::auth::Presented {
    let mut h = HeaderMap::new();
    for (k, v) in headers {
        h.append(
            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    crate::auth::Presented::of(&h)
}

/// `globals.api` of [`CONFIG`]: `auth: none`.
pub(crate) fn open() -> Arc<Auth> {
    Arc::new(Auth::new(&config().globals.api, &console()))
}

/// The routes' state before the node runs its generation: the `/v1` routes answer 503.
pub(crate) fn api(auth: Arc<Auth>) -> Api {
    Api {
        metrics: Metrics::new(),
        ready: Arc::new(AtomicBool::new(true)),
        node: Arc::default(),
        auth,
        app: crate::App::of(&[]),
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

/// Builds every pipeline's parts on one shared [`MemorySink`]: a node reads tables' facts
/// through it.
#[derive(Debug)]
struct Lakes(MemorySink);

impl WorkerFactory for Lakes {
    fn build(&self, _: &Pipeline) -> Result<Parts, String> {
        Ok(Parts {
            source: Arc::new(FakeSource::new(vec![])),
            sink: Arc::new(self.0.clone()),
            hooks: Arc::new(data_avalanche_pipeline::NoHooks),
        })
    }
}

/// A lone node `node-a` running generation 1 of [`CONFIG`], read from `file`: its gossip on a
/// loopback port, its reloads reaching a reconciler without pipelines.
pub(crate) struct Lone {
    pub node: Node,
    /// The node's timeline: what the API recorded.
    pub events: Arc<Events>,
    /// The leases the node reads: a test takes them as another node would.
    pub leases: Leases,
    /// The removal tombstones the node's coordinator records: a test records them as a reload
    /// would.
    pub removals: Removals,
    /// What the API asked of the node.
    pub stops: tokio::sync::watch::Receiver<Stop>,
    pub _reconciler: ClusterReconciler,
    pub _gossip: Gossip,
    /// The sink every pipeline's [`Node::factory`] builds: a test creates and commits its tables.
    pub sink: MemorySink,
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
            build: "1.1.0".into(),
            api_url: "http://127.0.0.1:8080".into(),
            file_hash: data_avalanche_cluster::file_hash(&key, text),
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
    let events = Arc::new(Events::new(docs.clone()));
    let feed = data_avalanche_cluster::Feed::new();
    let (recorder, _) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone(), &feed);
    let c = &config.globals.cluster;
    let removals = Removals::new(docs.clone());
    let deps = ControllerDeps {
        leases: leases.clone(),
        removals: removals.clone(),
        removed: removed_pipelines(
            generations.clone(),
            Arc::clone(&env),
            key,
            Arc::new(|p| Err(p.id.clone())),
        ),
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
        recorder: recorder.clone(),
    };
    let reconciler =
        ClusterReconciler::start(vec![], &deps, gossip.members(), ReconcilerSettings::new(c));
    let coordinator = ConfigCoordinator::start(
        config,
        first.pointer,
        CoordinatorDeps {
            generations,
            removals: removals.clone(),
            env,
            key,
            node_id: "node-a".into(),
            docs: Arc::new(|p| Err(p.id.clone())),
            factory: deps.factory,
            reloader: reconciler.reloader(),
            advertiser: gossip.advertiser(),
            metrics,
            globals: Arc::new(|_| {}),
            recorder: recorder.clone(),
        },
        gossip.members(),
        Duration::from_secs(10),
    );
    let (control, stops) = Control::new();
    let sink = MemorySink::new();
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
            removals: removals.clone(),
            control,
            validates: Arc::new(crate::RateLimit::validate()),
            recorder,
            denials: Arc::new(crate::RateLimit::denials()),
            events: Arc::clone(&events),
            peers: crate::Peers::new("node-a", None).unwrap(),
            process: tokio::sync::watch::channel(Default::default()).1,
            gossip: gossip.figures(),
            ended_jobs: Arc::default(),
            statuses: reconciler.watch_statuses(),
            factory: Arc::new(Lakes(sink.clone())),
            lake: Arc::default(),
            charts: Arc::new(data_avalanche_state::Charts::new(docs.clone())),
            prometheus: Arc::default(),
            feed,
        },
        events,
        leases,
        removals,
        stops,
        _reconciler: reconciler,
        _gossip: gossip,
        sink,
    }
}

/// An identity provider: ES256 keys made at run time, its JWKS at `/jwks` and its OIDC discovery
/// document at `/.well-known/openid-configuration`, on a loopback port. It signs the console's
/// client in too: `/authorize` approves at once, `/token` exchanges a code for its PKCE verifier
/// and refreshes, `/end-session` signs out.
pub(crate) struct Issuer {
    pub url: String,
    keys: Arc<Mutex<Vec<(String, KeyPair)>>>,
    /// JWKS fetches so far.
    pub fetches: Arc<AtomicUsize>,
    down: Arc<AtomicBool>,
    hanging: Arc<AtomicBool>,
    pub sign_ins: Arc<SignIns>,
}

/// A token request's form, and its `Authorization`.
pub(crate) type TokenRequest = (BTreeMap<String, String>, Option<String>);

/// What the provider did for the console's client.
#[derive(Default)]
pub(crate) struct SignIns {
    /// The roles of the tokens it issues.
    pub roles: Mutex<Value>,
    /// The authorization requests' queries, as asked.
    pub asked: Mutex<Vec<BTreeMap<String, String>>>,
    /// Each code issued, with its PKCE challenge and redirect URI.
    codes: Mutex<BTreeMap<String, (String, String)>>,
    /// The token requests' forms, and their `Authorization`.
    pub token_requests: Mutex<Vec<TokenRequest>>,
    /// Refreshes asked for.
    pub refreshes: AtomicUsize,
    /// Refuses every refresh, as a provider that revoked the sign-in.
    pub refusing: AtomicBool,
    /// Answers every refresh with this status, when not 0: a provider that is down (503) or
    /// that limits its callers (429).
    pub failing: AtomicU16,
    /// Fields that replace or add to the discovery document's, when an object.
    pub discovery: Mutex<Value>,
    /// A refresh token whose refresh waits for [`Self::release`].
    pub held: Mutex<Option<String>>,
    /// Lets the held refresh go on.
    pub release: tokio::sync::Notify,
}

#[derive(Clone)]
struct Served {
    url: String,
    keys: Arc<Mutex<Vec<(String, KeyPair)>>>,
    fetches: Arc<AtomicUsize>,
    down: Arc<AtomicBool>,
    hanging: Arc<AtomicBool>,
    sign_ins: Arc<SignIns>,
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
    let mut doc = json!({
        "issuer": s.url,
        "jwks_uri": format!("{}/jwks", s.url),
        "authorization_endpoint": format!("{}/authorize", s.url),
        "token_endpoint": format!("{}/token", s.url),
        "end_session_endpoint": format!("{}/end-session", s.url),
    });
    if let Some(changes) = s.sign_ins.discovery.lock().unwrap().as_object() {
        for (k, v) in changes {
            doc[k] = v.clone();
        }
    }
    Json(doc)
}

/// Approves at once: back to the client's redirect URI with a code and the state.
async fn authorize(
    State(s): State<Served>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let q: BTreeMap<String, String> =
        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    s.sign_ins.asked.lock().unwrap().push(q.clone());
    let code = format!("code-{}", s.sign_ins.asked.lock().unwrap().len());
    let redirect = q.get("redirect_uri").cloned().unwrap_or_default();
    s.sign_ins.codes.lock().unwrap().insert(
        code.clone(),
        (
            q.get("code_challenge").cloned().unwrap_or_default(),
            redirect.clone(),
        ),
    );
    let state =
        url::form_urlencoded::byte_serialize(q.get("state").map_or("", String::as_str).as_bytes())
            .collect::<String>();
    (
        StatusCode::SEE_OTHER,
        [(
            axum::http::header::LOCATION,
            format!("{redirect}?code={code}&state={state}"),
        )],
    )
        .into_response()
}

/// Exchanges a code whose PKCE verifier matches, or refreshes, for tokens with the provider's
/// roles.
async fn token(
    State(s): State<Served>,
    headers: HeaderMap,
    form: Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let form: BTreeMap<String, String> = url::form_urlencoded::parse(&form).into_owned().collect();
    let authorization = headers
        .get("authorization")
        .map(|v| v.to_str().unwrap().to_owned());
    s.sign_ins
        .token_requests
        .lock()
        .unwrap()
        .push((form.clone(), authorization));
    let refused = |why: &str| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_grant", "error_description": why })),
        )
            .into_response()
    };
    let n = match form.get("grant_type").map(String::as_str) {
        Some("authorization_code") => {
            let code = form.get("code").cloned().unwrap_or_default();
            let Some((challenge, redirect)) = s.sign_ins.codes.lock().unwrap().remove(&code) else {
                return refused("no such code");
            };
            let verifier = form.get("code_verifier").cloned().unwrap_or_default();
            if crate::oauth2::challenge(&verifier) != challenge
                || form.get("redirect_uri") != Some(&redirect)
            {
                return refused("the verifier or the redirect URI doesn't match");
            }
            0
        }
        Some("refresh_token") => {
            let held = s.sign_ins.held.lock().unwrap().clone();
            if held.is_some() && held.as_ref() == form.get("refresh_token") {
                s.sign_ins.release.notified().await;
            }
            let failing = s.sign_ins.failing.load(Ordering::SeqCst);
            if failing != 0 {
                return (
                    StatusCode::from_u16(failing).unwrap(),
                    Json(json!({ "error": "temporarily_unavailable" })),
                )
                    .into_response();
            }
            if s.sign_ins.refusing.load(Ordering::SeqCst) {
                return refused("the refresh token was revoked");
            }
            s.sign_ins.refreshes.fetch_add(1, Ordering::SeqCst) + 1
        }
        _ => return refused("unsupported grant"),
    };
    let now = jsonwebtoken::get_current_timestamp();
    let claims = json!({
        "iss": s.url, "aud": "cdc", "sub": "alice", "preferred_username": "alice.smith",
        "exp": now + 600, "roles": s.sign_ins.roles.lock().unwrap().clone(),
    });
    let keys = s.keys.lock().unwrap();
    let (kid, key) = &keys[0];
    let access = sign_with(key, Some(kid), &claims);
    let id = sign_with(
        key,
        Some(kid),
        &json!({ "iss": s.url, "aud": "cdc-console", "sub": "alice", "exp": now + 600 }),
    );
    Json(json!({
        "access_token": access,
        "token_type": "Bearer",
        "expires_in": 600,
        "refresh_token": format!("refresh-{n}"),
        "id_token": id,
    }))
    .into_response()
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
        let sign_ins = Arc::new(SignIns {
            roles: Mutex::new(json!(["operator"])),
            ..SignIns::default()
        });
        let served = Served {
            down: Arc::clone(&down),
            hanging: Arc::clone(&hanging),
            url: url.clone(),
            keys: Arc::clone(&keys),
            fetches: Arc::clone(&fetches),
            sign_ins: Arc::clone(&sign_ins),
        };
        let app = axum::Router::new()
            .route("/jwks", get_route(jwks))
            .route("/.well-known/openid-configuration", get_route(discovery))
            .route("/authorize", get_route(authorize))
            .route("/token", axum::routing::post(token))
            .route("/end-session", get_route(|| async { "signed out" }))
            .with_state(served);
        tokio::spawn(async move { axum::serve(listener, app).await });
        let issuer = Self {
            url,
            keys,
            fetches,
            down,
            hanging,
            sign_ins,
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

    /// `globals.api` with `auth: oauth2` for this issuer, its keys at its JWKS, the console's
    /// client `cdc-console`.
    pub fn oauth2_api(&self, roles_claim: &str) -> data_avalanche_config::Api {
        let mut api = config().globals.api;
        api.auth = ApiAuth::Oauth2;
        api.oauth2 = Some(data_avalanche_config::ApiOauth2 {
            issuer: self.url.clone(),
            audience: Some("cdc".into()),
            jwks_url: Some(format!("{}/jwks", self.url)),
            discovery_url: None,
            roles_claim: roles_claim.into(),
            client_id: Some("cdc-console".into()),
            client_secret: None,
            scopes: vec!["openid".into(), "profile".into()],
            idp_name: None,
        });
        api
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

/// What a fake node saw of one request.
#[derive(Debug, Clone)]
pub(crate) struct Seen {
    pub method: Method,
    pub uri: String,
    pub authorization: Option<String>,
    pub cookie: Option<String>,
    pub csrf: Option<String>,
    pub target: Option<String>,
    pub forwarded_by: Option<String>,
    pub body: String,
}

pub(crate) type Log = Arc<Mutex<Vec<Seen>>>;

/// A fake node: answers every request `202 {"node": "node-b"}` after `delay`, keeping what it
/// saw.
pub(crate) async fn fake(delay: Duration) -> (String, Log) {
    fake_answering(delay, StatusCode::ACCEPTED, json!({ "node": "node-b" })).await
}

/// A fake node: answers every request `code` with `answer` after `delay`, keeping what it saw.
pub(crate) async fn fake_answering(
    delay: Duration,
    code: StatusCode,
    answer: Value,
) -> (String, Log) {
    let log: Log = Arc::default();
    let kept = Arc::clone(&log);
    let app = Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, bytes: Bytes| {
            let (kept, answer) = (Arc::clone(&kept), answer.clone());
            async move {
                let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
                kept.lock().unwrap().push(Seen {
                    method,
                    uri: uri.to_string(),
                    authorization: h("authorization"),
                    cookie: h("cookie"),
                    csrf: h(crate::CSRF_HEADER),
                    target: h(TARGET),
                    forwarded_by: h(FORWARDED_BY),
                    body: String::from_utf8(bytes.to_vec()).unwrap(),
                });
                tokio::time::sleep(delay).await;
                crate::body(code, &answer)
            }
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, log)
}

pub(crate) fn member(api_url: &str, phase: Phase) -> Member {
    Member {
        addr: "127.0.0.1:7946".parse().unwrap(),
        incarnation: 1_731_600_000_000,
        meta: NodeMeta {
            phase,
            config_generation: 1,
            build: "1.1.0".into(),
            api_url: api_url.into(),
            file_hash: file_hash(&FingerprintKey::from_bytes([1; 32]), CONFIG),
        },
        suspect: false,
    }
}

/// node-a's API under `auth`; its view holds itself at its own address and node-b at
/// `b_url`.
pub(crate) async fn served(b_url: &str, auth: Arc<Auth>) -> (SocketAddr, Lone, TempDir) {
    served_with(b_url, auth, |_| {}).await
}

/// [`served`], its node changed by `change` before it serves.
pub(crate) async fn served_with(
    b_url: &str,
    auth: Arc<Auth>,
    change: impl FnOnce(&mut Node),
) -> (SocketAddr, Lone, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("config.yaml");
    std::fs::write(&file, CONFIG).unwrap();
    let mut lone = lone(file).await;
    let (view, members) = watch::channel(Members {
        me: "node-a".into(),
        nodes: Default::default(),
    });
    lone.node.members = members;
    change(&mut lone.node);
    let a = api(auth);
    a.node.set(lone.node.clone()).unwrap();
    let addr = serve(a).await;
    view.send_replace(Members {
        me: "node-a".into(),
        nodes: [
            (
                "node-a".to_owned(),
                member(&format!("http://{addr}"), Phase::Active),
            ),
            ("node-b".to_owned(), member(b_url, Phase::Active)),
        ]
        .into(),
    });
    std::mem::forget(view);
    (addr, lone, dir)
}

/// A `GET /v1/stream` read as a page reads it: its code and head, then one message at a time.
pub(crate) struct Stream {
    pub code: u16,
    /// The response's headers, `name: value` a line, names in lower case.
    pub head: String,
    response: reqwest::Response,
    buffer: String,
}

/// Opens a stream at `path` with `headers`.
pub(crate) async fn stream(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Stream {
    let mut asked = reqwest::Client::new().get(format!("http://{addr}{path}"));
    for (k, v) in headers {
        asked = asked.header(*k, *v);
    }
    let response = asked.send().await.unwrap();
    let head = response
        .headers()
        .iter()
        .map(|(k, v)| format!("{k}: {}\n", v.to_str().unwrap_or("")))
        .collect();
    Stream {
        code: response.status().as_u16(),
        head,
        response,
        buffer: String::new(),
    }
}

impl Stream {
    /// The next message's kind and data, keep-alive comments skipped; within 5 s.
    pub async fn next(&mut self) -> (String, Value) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                while let Some(end) = self.buffer.find("\n\n") {
                    let frame: String = self.buffer.drain(..end + 2).collect();
                    let (mut kind, mut data) = (None, String::new());
                    for line in frame.lines() {
                        if let Some(k) = line.strip_prefix("event:") {
                            kind = Some(k.trim().to_owned());
                        } else if let Some(d) = line.strip_prefix("data:") {
                            data.push_str(d.trim_start());
                        }
                    }
                    if let Some(kind) = kind {
                        return (kind, serde_json::from_str(&data).unwrap());
                    }
                }
                let chunk = self
                    .response
                    .chunk()
                    .await
                    .unwrap()
                    .expect("the stream ended");
                self.buffer.push_str(std::str::from_utf8(&chunk).unwrap());
            }
        })
        .await
        .expect("a message within 5 s")
    }

    /// Whether the response ends within 3 s, what it still sends read past.
    pub async fn ended(&mut self) -> bool {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.response.chunk().await.ok().flatten().is_some() {}
        })
        .await
        .is_ok()
    }
}
