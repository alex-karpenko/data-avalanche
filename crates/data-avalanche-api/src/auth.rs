//! Who calls (config §6.3, API_RBAC §3–§4). `none`: every caller is `admin`. `basic`: the one
//! user is `admin`. `jwt` and `oauth2`: a bearer token signed by a key of the issuer's JWKS, its
//! roles read at `roles_claim` and mapped by `role_mapping` (identity when empty), the highest
//! one taken; a token with no role it maps is refused (403), not taken as `viewer`. `oauth2`
//! finds the JWKS through OIDC discovery when it has no `jwks_url`; tokens are verified locally,
//! never introspected (no client credentials are configured). The settings are R: [`Auth::set`]
//! takes a reload's.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use data_avalanche_config::{Api as ApiConfig, ApiAuth, Credentials, Role};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{AlgorithmFamily, DecodingKey, Validation, decode, decode_header};
use serde_json::Value;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

/// A JWKS fetch, or a discovery, takes at most this long.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// A token signed by a key the cached JWKS lacks fetches the JWKS again, at most this often
/// (a key rotation; a flood of tokens with made-up key ids fetches nothing more).
pub const REFETCH_AFTER: Duration = Duration::from_secs(30);

/// An authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The basic user, the token's `sub`, or `anonymous`.
    pub name: String,
    pub role: Role,
}

/// Why a caller is turned away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Denied {
    /// 401: no credentials, or ones that don't verify. `challenge` is `WWW-Authenticate`.
    Unauthenticated {
        challenge: &'static str,
        why: String,
    },
    /// 403: verified, but no role this endpoint takes.
    Forbidden(String),
    /// 503: the issuer's keys can't be fetched, so no token can be verified.
    Unavailable(String),
}

const BASIC: &str = r#"Basic realm="data-avalanche""#;
const BEARER: &str = r#"Bearer realm="data-avalanche""#;

/// The settings that decide who calls: `auth` and what it reads.
#[derive(Debug, Clone, PartialEq)]
struct Settings {
    auth: ApiAuth,
    credentials: Option<Credentials>,
    jwt: Option<data_avalanche_config::Jwt>,
    oauth2: Option<data_avalanche_config::ApiOauth2>,
    role_mapping: BTreeMap<Role, Vec<String>>,
}

impl Settings {
    fn of(api: &ApiConfig) -> Self {
        Self {
            auth: api.auth,
            credentials: api.credentials.clone(),
            jwt: api.jwt.clone(),
            oauth2: api.oauth2.clone(),
            role_mapping: api.role_mapping.clone(),
        }
    }
}

#[derive(Debug)]
enum Mode {
    Open,
    Basic(Credentials),
    Token(Verifier),
}

#[derive(Debug)]
struct Current {
    settings: Settings,
    mode: Arc<Mode>,
}

/// The node's authentication, swapped by a reload.
#[derive(Debug)]
pub struct Auth {
    current: RwLock<Current>,
    refetch_after: Duration,
}

impl Auth {
    pub fn new(api: &ApiConfig) -> Self {
        Self::with_refetch_after(api, REFETCH_AFTER)
    }

    pub(crate) fn with_refetch_after(api: &ApiConfig, refetch_after: Duration) -> Self {
        let settings = Settings::of(api);
        Self {
            current: RwLock::new(Current {
                mode: Arc::new(mode(&settings, refetch_after)),
                settings,
            }),
            refetch_after,
        }
    }

    /// Takes a reload's settings. Unchanged ones keep the cached JWKS.
    pub fn set(&self, api: &ApiConfig) {
        let settings = Settings::of(api);
        let mut current = self.current.write().unwrap_or_else(PoisonError::into_inner);
        if current.settings != settings {
            current.mode = Arc::new(mode(&settings, self.refetch_after));
            current.settings = settings;
        }
    }

    /// The caller of a request with these headers.
    pub(crate) async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
        let mode = Arc::clone(
            &self
                .current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .mode,
        );
        match &*mode {
            Mode::Open => Ok(Principal {
                name: "anonymous".into(),
                role: Role::Admin,
            }),
            Mode::Basic(credentials) => basic(credentials, headers),
            Mode::Token(verifier) => verifier.verify(headers).await,
        }
    }
}

fn mode(s: &Settings, refetch_after: Duration) -> Mode {
    let token = |issuer: &str, audience: &Option<String>, roles_claim: &str, keys| {
        Mode::Token(Verifier {
            issuer: issuer.to_owned(),
            audience: audience.clone(),
            roles_claim: roles_claim.to_owned(),
            mapping: s.role_mapping.clone(),
            keys: Keys::new(keys, refetch_after),
        })
    };
    match (s.auth, &s.credentials, &s.jwt, &s.oauth2) {
        (ApiAuth::Basic, Some(c), _, _) => Mode::Basic(c.clone()),
        (ApiAuth::Jwt, _, Some(j), _) => token(
            &j.issuer,
            &j.audience,
            &j.roles_claim,
            KeySource::Jwks(j.jwks_url.clone()),
        ),
        (ApiAuth::Oauth2, _, _, Some(o)) => token(
            &o.issuer,
            &o.audience,
            &o.roles_claim,
            match (&o.jwks_url, &o.discovery_url) {
                (Some(url), _) => KeySource::Jwks(url.clone()),
                // V-051 requires one of them.
                (None, url) => KeySource::Discovery {
                    url: url.clone().unwrap_or_default(),
                    issuer: o.issuer.clone(),
                },
            },
        ),
        // V-049…V-051 require the settings each mode reads; none means open.
        _ => Mode::Open,
    }
}

fn unauthenticated(challenge: &'static str, why: impl Into<String>) -> Denied {
    Denied::Unauthenticated {
        challenge,
        why: why.into(),
    }
}

/// The value of `Authorization: <scheme> <value>`, the scheme matched without case.
fn credential<'h>(headers: &'h HeaderMap, scheme: &str) -> Option<&'h str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (given, rest) = value.split_once(' ')?;
    given.eq_ignore_ascii_case(scheme).then(|| rest.trim())
}

fn basic(c: &Credentials, headers: &HeaderMap) -> Result<Principal, Denied> {
    let given = credential(headers, "Basic")
        .ok_or_else(|| unauthenticated(BASIC, "basic credentials are required"))?;
    let decoded = STANDARD
        .decode(given)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| unauthenticated(BASIC, "the basic credentials don't decode"))?;
    let (user, password) = decoded
        .split_once(':')
        .ok_or_else(|| unauthenticated(BASIC, "the basic credentials don't decode"))?;
    // Both compared in full, whichever differs.
    let user_ok = user.as_bytes().ct_eq(c.user().as_bytes());
    let password_ok = password.as_bytes().ct_eq(c.password().expose().as_bytes());
    if bool::from(user_ok & password_ok) {
        Ok(Principal {
            name: user.to_owned(),
            role: Role::Admin,
        })
    } else {
        Err(unauthenticated(BASIC, "wrong user or password"))
    }
}

#[derive(Debug)]
struct Verifier {
    issuer: String,
    audience: Option<String>,
    roles_claim: String,
    mapping: BTreeMap<Role, Vec<String>>,
    keys: Keys,
}

impl Verifier {
    async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
        let token = credential(headers, "Bearer")
            .ok_or_else(|| unauthenticated(BEARER, "a bearer token is required"))?;
        let header = decode_header(token)
            .map_err(|e| unauthenticated(BEARER, format!("not a token: {e}")))?;
        if AlgorithmFamily::Hmac == header.alg.family() {
            return Err(unauthenticated(
                BEARER,
                format!(
                    "{:?} is a shared-secret algorithm: tokens are verified by the issuer's public keys",
                    header.alg
                ),
            ));
        }
        let key = self.keys.key(header.kid.as_deref()).await?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        match &self.audience {
            Some(aud) => validation.set_audience(&[aud]),
            None => validation.validate_aud = false,
        }
        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|e| unauthenticated(BEARER, format!("the token doesn't verify: {e}")))?
            .claims;
        let name = claims
            .get("sub")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let role = roles(&claims, &self.roles_claim)
            .iter()
            .filter_map(|r| map(&self.mapping, r))
            .max()
            .ok_or_else(|| {
                Denied::Forbidden(format!(
                    "the token's {} name no role this node maps",
                    self.roles_claim
                ))
            })?;
        Ok(Principal { name, role })
    }
}

/// The strings at a dotted path (`realm_access.roles`): a list of them, or one.
fn roles(claims: &Value, path: &str) -> Vec<String> {
    let mut at = claims;
    for part in path.split('.') {
        match at.get(part) {
            Some(next) => at = next,
            None => return vec![],
        }
    }
    match at {
        Value::String(s) => vec![s.clone()],
        Value::Array(items) => items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
        _ => vec![],
    }
}

/// An IdP role's local role: `role_mapping`, or the three names themselves without one.
fn map(mapping: &BTreeMap<Role, Vec<String>>, idp: &str) -> Option<Role> {
    if mapping.is_empty() {
        return idp.parse().ok();
    }
    mapping
        .iter()
        .filter(|(_, names)| names.iter().any(|n| n == idp))
        .map(|(role, _)| *role)
        .max()
}

#[derive(Debug, Clone)]
enum KeySource {
    Jwks(String),
    /// OIDC discovery: the document's `jwks_uri`, once its `issuer` is found to be the
    /// configured one (OIDC Discovery §4.3).
    Discovery {
        url: String,
        issuer: String,
    },
}

#[derive(Debug)]
struct Keys {
    source: KeySource,
    http: reqwest::Client,
    cache: Mutex<Cache>,
    refetch_after: Duration,
}

#[derive(Debug, Default)]
struct Cache {
    set: Option<JwkSet>,
    fetched: Option<Instant>,
}

impl Keys {
    fn new(source: KeySource, refetch_after: Duration) -> Self {
        Self {
            source,
            refetch_after,
            http: reqwest::Client::builder()
                .timeout(FETCH_TIMEOUT)
                .build()
                .expect("the HTTP client builds"),
            cache: Mutex::new(Cache::default()),
        }
    }

    /// The key a token names (`kid`; without one, the set's only key), fetching the JWKS when
    /// it isn't cached, or lacks the key and was fetched [`REFETCH_AFTER`] ago or earlier.
    async fn key(&self, kid: Option<&str>) -> Result<DecodingKey, Denied> {
        let mut cache = self.cache.lock().await;
        if let Some(key) = cache.set.as_ref().and_then(|s| find(s, kid)) {
            return key;
        }
        if cache
            .fetched
            .is_none_or(|at| at.elapsed() >= self.refetch_after)
        {
            cache.fetched = Some(Instant::now());
            match self.fetch().await {
                Ok(set) => cache.set = Some(set),
                Err(why) => {
                    tracing::warn!("the API's token keys: {why}");
                    if cache.set.is_none() {
                        return Err(Denied::Unavailable(why));
                    }
                }
            }
        }
        cache
            .set
            .as_ref()
            .and_then(|s| find(s, kid))
            .unwrap_or_else(|| {
                Err(unauthenticated(
                    BEARER,
                    format!("no key {kid:?} in the issuer's JWKS"),
                ))
            })
    }

    async fn fetch(&self) -> Result<JwkSet, String> {
        let url = match &self.source {
            KeySource::Jwks(url) => url.clone(),
            KeySource::Discovery { url, issuer } => {
                let doc: Value = self.get(url).await?;
                let found = doc.get("issuer").and_then(Value::as_str);
                if found != Some(issuer.as_str()) {
                    return Err(format!(
                        "{url}: the issuer is {found:?}, not the configured {issuer:?}"
                    ));
                }
                doc.get("jwks_uri")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{url}: no jwks_uri"))?
                    .to_owned()
            }
        };
        self.get(&url).await
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, String> {
        let body = self
            .http
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| format!("{url}: {e}"))?
            .bytes()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        serde_json::from_slice(&body).map_err(|e| format!("{url}: {e}"))
    }
}

fn find(set: &JwkSet, kid: Option<&str>) -> Option<Result<DecodingKey, Denied>> {
    let jwk = match kid {
        Some(kid) => set.find(kid)?,
        None if set.keys.len() == 1 => &set.keys[0],
        None => return None,
    };
    Some(
        DecodingKey::from_jwk(jwk)
            .map_err(|e| unauthenticated(BEARER, format!("the issuer's key: {e}"))),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use axum::http::HeaderValue;
    use data_avalanche_config::{ApiOauth2, FingerprintKey, Secret};
    use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
    use serde_json::json;

    use super::*;
    use crate::test_support::{Issuer, config, sign_with};

    fn bearer(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        h
    }

    fn denied_401(r: Result<Principal, Denied>) -> String {
        match r {
            Err(Denied::Unauthenticated { why, .. }) => why,
            other => panic!("not a 401: {other:?}"),
        }
    }

    #[tokio::test]
    async fn without_auth_every_caller_is_admin() {
        let auth = Auth::new(&config().globals.api);
        let p = auth.verify(&HeaderMap::new()).await.unwrap();
        assert_eq!((p.name.as_str(), p.role), ("anonymous", Role::Admin));
    }

    #[tokio::test]
    async fn basic_lets_its_one_user_in_as_admin() {
        let mut api = config().globals.api;
        api.auth = ApiAuth::Basic;
        api.credentials = Some(Credentials::Inline {
            user: "ops".into(),
            password: Secret::new("s3cret", &FingerprintKey::from_bytes([1; 32])),
        });
        let auth = Auth::new(&api);
        let with = |user_pass: &str| {
            let mut h = HeaderMap::new();
            let v = format!("basic {}", STANDARD.encode(user_pass));
            h.insert(AUTHORIZATION, HeaderValue::from_str(&v).unwrap());
            h
        };
        let p = auth.verify(&with("ops:s3cret")).await.unwrap();
        assert_eq!((p.name.as_str(), p.role), ("ops", Role::Admin));
        for wrong in ["ops:nope", "nobody:s3cret", "ops", "ops:s3cret:"] {
            denied_401(auth.verify(&with(wrong)).await);
        }
        match auth.verify(&HeaderMap::new()).await {
            Err(Denied::Unauthenticated { challenge, .. }) => assert_eq!(challenge, BASIC),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_token_s_highest_mapped_role_is_the_caller_s() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.api("roles"));
        let role = |roles| {
            let token = idp.token(roles);
            let auth = &auth;
            async move { auth.verify(&bearer(&token)).await }
        };
        assert_eq!(role(json!(["viewer"])).await.unwrap().role, Role::Viewer);
        let p = role(json!(["viewer", "admin", "other"])).await.unwrap();
        assert_eq!((p.name.as_str(), p.role), ("alice", Role::Admin));
        assert_eq!(role(json!("operator")).await.unwrap().role, Role::Operator);
        assert!(matches!(
            role(json!(["cdc-admin"])).await,
            Err(Denied::Forbidden(_))
        ));
        assert!(matches!(role(json!([])).await, Err(Denied::Forbidden(_))));

        // Keycloak's realm roles, mapped: the three names themselves no longer count.
        let mut api = idp.api("realm_access.roles");
        api.role_mapping = BTreeMap::from([
            (Role::Viewer, vec!["readonly".into()]),
            (Role::Admin, vec!["cdc-admin".into()]),
        ]);
        let auth = Auth::new(&api);
        let realm = |roles| {
            let mut claims = idp.claims(json!(null));
            claims["realm_access"] = json!({ "roles": roles });
            bearer(&idp.sign("k1", &claims))
        };
        let p = auth.verify(&realm(json!(["readonly", "cdc-admin"]))).await;
        assert_eq!(p.unwrap().role, Role::Admin);
        assert_eq!(
            auth.verify(&realm(json!(["readonly"]))).await.unwrap().role,
            Role::Viewer
        );
        assert!(matches!(
            auth.verify(&realm(json!(["admin"]))).await,
            Err(Denied::Forbidden(_))
        ));
    }

    #[tokio::test]
    async fn a_token_that_doesnt_verify_is_refused() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.api("roles"));
        let now = jsonwebtoken::get_current_timestamp();
        let with = |f: &dyn Fn(&mut Value)| {
            let mut c = idp.claims(json!(["admin"]));
            f(&mut c);
            bearer(&idp.sign("k1", &c))
        };
        let expired = with(&|c| c["exp"] = json!(now - 120));
        assert!(denied_401(auth.verify(&expired).await).contains("Expired"));
        denied_401(
            auth.verify(&with(&|c| c["iss"] = json!("https://elsewhere")))
                .await,
        );
        denied_401(
            auth.verify(&with(&|c| c["aud"] = json!("another-api")))
                .await,
        );
        denied_401(
            auth.verify(&with(&|c| c.as_object_mut().unwrap().clear()))
                .await,
        );
        // Signed by a key the issuer never published, under a key id it does.
        let stranger = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let forged = sign_with(&stranger, Some("k1"), &idp.claims(json!(["admin"])));
        denied_401(auth.verify(&bearer(&forged)).await);
        // A shared-secret token: nothing here holds a secret to check it with.
        let hs = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &idp.claims(json!(["admin"])),
            &jsonwebtoken::EncodingKey::from_secret(b"guess"),
        )
        .unwrap();
        assert!(denied_401(auth.verify(&bearer(&hs)).await).contains("shared-secret"));
        denied_401(auth.verify(&bearer("not.a.token")).await);
        match auth.verify(&HeaderMap::new()).await {
            Err(Denied::Unauthenticated { challenge, .. }) => assert_eq!(challenge, BEARER),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_rotated_key_is_fetched_when_a_token_names_it() {
        let idp = Issuer::start().await;
        let auth = Auth::with_refetch_after(&idp.api("roles"), Duration::ZERO);
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1, "the JWKS is cached");
        idp.add_key("k2");
        let rotated = idp.sign("k2", &idp.claims(json!(["viewer"])));
        auth.verify(&bearer(&rotated)).await.unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unknown_key_ids_fetch_at_most_once_a_refetch_interval() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.api("roles"));
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        idp.add_key("k2");
        for _ in 0..5 {
            let made_up = idp.sign("k2", &idp.claims(json!(["viewer"])));
            denied_401(auth.verify(&bearer(&made_up)).await);
        }
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn oauth2_finds_the_keys_by_discovery_from_its_issuer() {
        let idp = Issuer::start().await;
        let mut api = config().globals.api;
        api.auth = ApiAuth::Oauth2;
        let oauth2 = |issuer: &str| ApiOauth2 {
            issuer: issuer.into(),
            audience: Some("cdc".into()),
            jwks_url: None,
            discovery_url: Some(format!("{}/.well-known/openid-configuration", idp.url)),
            roles_claim: "roles".into(),
        };
        api.oauth2 = Some(oauth2(&idp.url));
        let token = bearer(&idp.token(json!(["operator"])));
        let p = Auth::new(&api).verify(&token).await.unwrap();
        assert_eq!(p.role, Role::Operator);

        // A discovery document of another issuer is not this one's.
        api.oauth2 = Some(oauth2("https://idp.example.com"));
        match Auth::new(&api).verify(&token).await {
            Err(Denied::Unavailable(why)) => assert!(why.contains("not the configured"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn without_its_keys_no_token_verifies() {
        let idp = Issuer::start().await;
        let mut api = idp.api("roles");
        // A port nothing listens on.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/jwks", closed.local_addr().unwrap());
        drop(closed);
        api.jwt.as_mut().unwrap().jwks_url = url;
        let r = Auth::new(&api)
            .verify(&bearer(&idp.token(json!(["admin"]))))
            .await;
        assert!(matches!(r, Err(Denied::Unavailable(_))), "{r:?}");
    }

    #[tokio::test]
    async fn a_reload_keeps_the_keys_unless_the_auth_settings_change() {
        let idp = Issuer::start().await;
        let api = idp.api("roles");
        let auth = Auth::new(&api);
        let token = bearer(&idp.token(json!(["viewer"])));
        auth.verify(&token).await.unwrap();
        let mut moved = api.clone();
        moved.address.port = 9_999;
        auth.set(&moved);
        auth.verify(&token).await.unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);
        let mut other = api.clone();
        other.jwt.as_mut().unwrap().audience = Some("another-api".into());
        auth.set(&other);
        denied_401(auth.verify(&token).await);
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2);
        auth.set(&config().globals.api);
        assert_eq!(
            auth.verify(&HeaderMap::new()).await.unwrap().role,
            Role::Admin
        );
    }
}
