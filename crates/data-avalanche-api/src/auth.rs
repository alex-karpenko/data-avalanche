//! Who calls (config §6.3, API_RBAC §3–§4). `none`: every caller is `admin`. `basic`: the one
//! user is `admin`. `jwt` and `oauth2`: a bearer token signed by a key of the issuer's JWKS, its
//! roles read at `roles_claim` and mapped by `role_mapping` (identity when empty), the highest
//! one taken; a token with no role it maps is refused (403), not taken as `viewer`. `oauth2`
//! finds the JWKS through OIDC discovery when it has no `jwks_url`; tokens are verified locally,
//! never introspected (no client credentials are configured). A token's caller is named by
//! `globals.console.username_claim`, else its `sub`. The settings are R: [`Auth::set`] takes a
//! reload's. Settings without what their mode reads refuse every request (503).
//!
//! With the console enabled and `auth` `basic` or `oauth2`, a request without `Authorization` may
//! carry the console's session cookie instead (Admin_Console §9, [`crate::session`]): every node
//! opens it with the session key, and verifies its person again as it verifies credentials, on
//! every request. A session ends `session_ttl` after sign-in, when the basic user or password
//! changes, when its provider's token no longer verifies, or when the key or the mode changes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::{Duration, Instant};

use axum::http::header::{AUTHORIZATION, COOKIE};
use axum::http::{HeaderMap, HeaderValue};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use data_avalanche_config::{Api as ApiConfig, ApiAuth, Console, Credentials, Role, Secret};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{AlgorithmFamily, DecodingKey, Validation, decode, decode_header};
use serde_json::Value;
use subtle::ConstantTimeEq;
use tokio::sync::watch;

use crate::session::{By, Sealer, Session, Unsealed, session_cookies};

/// A JWKS fetch, or a discovery, takes at most this long.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest JWKS or discovery document read (DA-API-16): a longer one fails the fetch.
pub const MAX_KEY_DOCUMENT: usize = 1 << 20;
/// A token signed by a key the cached JWKS lacks fetches the JWKS again, at most this often
/// (a key rotation; a flood of tokens with made-up key ids fetches nothing more).
pub const REFETCH_AFTER: Duration = Duration::from_secs(30);
/// A cached JWKS older than this is fetched again at the next verification, even for a token
/// whose `kid` it holds: an issuer that withdraws a key is heard within this long. A failed fetch
/// keeps the old keys and is tried again after [`REFETCH_AFTER`].
pub const KEYS_MAX_AGE: Duration = Duration::from_secs(600);

/// The 503 while the keys can't be fetched: the reason is logged, not answered.
const KEYS_UNAVAILABLE: &str = "the token issuer's keys can't be fetched";
/// The 503 of auth settings without what their mode reads.
const MISCONFIGURED: &str = "the API's auth settings are inconsistent: every request is refused";

#[derive(Debug, Clone, Copy)]
struct Timing {
    refetch_after: Duration,
    max_age: Duration,
}

/// The header a mutating request the console's session authenticates carries its anti-forgery
/// token in (Admin_Console §9).
pub const CSRF_HEADER: &str = "x-csrf-token";

/// An authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The basic user, the token's `username_claim` (else its `sub`), or `anonymous`.
    pub name: String,
    pub role: Role,
    /// When its credentials stop verifying: a token's `exp`, a session's end (the earlier of
    /// the two for an `oauth2` session). `None`: they don't expire (basic credentials, `none`).
    pub expires: Option<DateTime<Utc>>,
}

/// A caller verified, with the session that verified them, if one did.
#[derive(Debug, Clone)]
pub(crate) struct Verified {
    pub principal: Principal,
    pub session: Option<Session>,
}

/// The credentials a caller presented, passed on to a peer as they came (Admin_Console §10): its
/// `Authorization`, its session's cookies and its anti-forgery token. They never print.
#[derive(Clone, Default)]
pub(crate) struct Presented {
    authorization: Option<HeaderValue>,
    cookie: Option<HeaderValue>,
    csrf: Option<HeaderValue>,
}

impl std::fmt::Debug for Presented {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shown = |v: &Option<HeaderValue>| v.as_ref().map(|_| "<redacted>");
        f.debug_struct("Presented")
            .field("authorization", &shown(&self.authorization))
            .field("cookie", &shown(&self.cookie))
            .field("csrf", &shown(&self.csrf))
            .finish()
    }
}

impl Presented {
    /// What a request with these headers presented: of its cookies, the session's alone.
    pub(crate) fn of(headers: &HeaderMap) -> Self {
        Self {
            authorization: headers.get(AUTHORIZATION).cloned(),
            cookie: session_cookies(headers).and_then(|c| HeaderValue::from_str(&c).ok()),
            csrf: headers.get(CSRF_HEADER).cloned(),
        }
    }

    /// As headers of a request to a peer, or of the check [`Auth::verify`] makes again.
    pub(crate) fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (name, value) in [
            (AUTHORIZATION.as_str(), &self.authorization),
            (COOKIE.as_str(), &self.cookie),
            (CSRF_HEADER, &self.csrf),
        ] {
            if let Some(v) = value {
                h.insert(name, v.clone());
            }
        }
        h
    }
}

/// Why a caller is turned away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Denied {
    /// 401: no credentials, or ones that don't verify. `challenge` is `WWW-Authenticate`.
    Unauthenticated {
        challenge: &'static str,
        why: String,
    },
    /// 403: verified as `name`, but with no role this node maps.
    Forbidden { name: String, why: String },
    /// 503: the issuer's keys can't be fetched, or the auth settings don't hold together, so
    /// no caller can be verified.
    Unavailable(String),
}

const BASIC: &str = r#"Basic realm="data-avalanche""#;
const BEARER: &str = r#"Bearer realm="data-avalanche""#;
/// The challenge of a request without `Authorization` while the console's sessions are on: a
/// scheme no browser answers with its own credentials popup (Admin_Console §9).
const SESSION: &str = r#"Session realm="data-avalanche""#;

/// The settings that decide who calls: `auth` and what it reads.
#[derive(Debug, Clone, PartialEq)]
struct Settings {
    auth: ApiAuth,
    credentials: Option<Credentials>,
    jwt: Option<data_avalanche_config::Jwt>,
    oauth2: Option<data_avalanche_config::ApiOauth2>,
    role_mapping: BTreeMap<Role, Vec<String>>,
    /// The token claim that names the caller (`globals.console.username_claim`).
    username_claim: String,
    /// The console's session key and `session_ttl`: set when the console is enabled with `auth`
    /// `basic` or `oauth2` and a key (V-135).
    sessions: Option<(Secret, Duration)>,
}

impl Settings {
    fn of(api: &ApiConfig, console: &Console) -> Self {
        let sessions = (console.enabled && matches!(api.auth, ApiAuth::Basic | ApiAuth::Oauth2))
            .then(|| console.session_key.clone())
            .flatten()
            .map(|key| (key, console.session_ttl));
        Self {
            auth: api.auth,
            credentials: api.credentials.clone(),
            jwt: api.jwt.clone(),
            oauth2: api.oauth2.clone(),
            role_mapping: api.role_mapping.clone(),
            username_claim: console.username_claim.clone(),
            sessions,
        }
    }
}

/// The console's sessions: the sealer of the session key, and how long a session lasts.
#[derive(Debug)]
pub(crate) struct Sessions {
    pub(crate) sealer: Sealer,
    pub(crate) ttl: Duration,
}

impl Sessions {
    fn of(s: &Settings) -> Option<Arc<Self>> {
        s.sessions.as_ref().map(|(key, ttl)| {
            Arc::new(Self {
                sealer: Sealer::new(key.expose()),
                ttl: *ttl,
            })
        })
    }

    /// When a session signed in at `signed_in` (Unix seconds) ends.
    pub(crate) fn ends(&self, signed_in: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(signed_in, 0).unwrap_or_default()
            + chrono::Duration::from_std(self.ttl).unwrap_or_default()
    }
}

#[derive(Debug)]
enum Mode {
    Open,
    Basic(Credentials),
    Token(Box<Verifier>),
    /// A mode without the settings it reads (V-049…V-051 refuse such a config): fails closed.
    Refused,
}

#[derive(Debug)]
struct Current {
    settings: Settings,
    mode: Arc<Mode>,
    sessions: Option<Arc<Sessions>>,
}

/// The node's authentication, swapped by a reload.
#[derive(Debug)]
pub struct Auth {
    current: RwLock<Current>,
    timing: Timing,
}

impl Auth {
    /// The authentication of `globals.api`, a token's caller named by `console`'s
    /// `username_claim`, else its `sub`; with the console's sessions when `console` enables them.
    pub fn new(api: &ApiConfig, console: &Console) -> Self {
        Self::build(api, console, REFETCH_AFTER, KEYS_MAX_AGE)
    }

    #[cfg(test)]
    pub(crate) fn with_refetch_after(api: &ApiConfig, refetch_after: Duration) -> Self {
        Self::build(
            api,
            &crate::test_support::console(),
            refetch_after,
            KEYS_MAX_AGE,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_timing(api: &ApiConfig, refetch_after: Duration, max_age: Duration) -> Self {
        Self::build(api, &crate::test_support::console(), refetch_after, max_age)
    }

    fn build(
        api: &ApiConfig,
        console: &Console,
        refetch_after: Duration,
        max_age: Duration,
    ) -> Self {
        let timing = Timing {
            refetch_after,
            max_age,
        };
        let settings = Settings::of(api, console);
        Self {
            current: RwLock::new(Current {
                mode: Arc::new(mode(&settings, timing, None)),
                sessions: Sessions::of(&settings),
                settings,
            }),
            timing,
        }
    }

    /// Takes a reload's settings. Unchanged ones keep the cached JWKS, and so do changed ones
    /// whose keys come from where they did (the same issuer and JWKS or discovery URL): a
    /// change of the audience, the roles claim, the role mapping, the username claim or the
    /// console's sessions fetches nothing.
    pub fn set(&self, api: &ApiConfig, console: &Console) {
        let settings = Settings::of(api, console);
        let mut current = self.current.write().unwrap_or_else(PoisonError::into_inner);
        if current.settings != settings {
            let keys = match &*current.mode {
                Mode::Token(v) => Some(Arc::clone(&v.keys)),
                _ => None,
            };
            current.mode = Arc::new(mode(&settings, self.timing, keys));
            current.sessions = Sessions::of(&settings);
            current.settings = settings;
        }
    }

    /// The caller of a request with these headers.
    pub(crate) async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
        self.verify_caller(headers).await.map(|v| v.principal)
    }

    /// The caller of a request with these headers, and their session: one is opened when the
    /// request has no `Authorization` and the console's sessions are on.
    pub(crate) async fn verify_caller(&self, headers: &HeaderMap) -> Result<Verified, Denied> {
        let sessions_on = self
            .current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .sessions
            .is_some();
        let verified = self.verified(headers).await;
        match verified {
            Err(Denied::Unauthenticated { why, .. })
                if sessions_on && !headers.contains_key(AUTHORIZATION) =>
            {
                Err(unauthenticated(SESSION, why))
            }
            other => other,
        }
    }

    async fn verified(&self, headers: &HeaderMap) -> Result<Verified, Denied> {
        let (mode, sessions) = {
            let current = self.current.read().unwrap_or_else(PoisonError::into_inner);
            (Arc::clone(&current.mode), current.sessions.clone())
        };
        if !headers.contains_key(AUTHORIZATION)
            && let Some(sessions) = &sessions
            && let Some(opened) = Session::of(headers, &sessions.sealer)
        {
            return session(&mode, sessions, opened, Utc::now()).await;
        }
        let principal = match &*mode {
            Mode::Open => Ok(Principal {
                name: "anonymous".into(),
                role: Role::Admin,
                expires: None,
            }),
            Mode::Basic(credentials) => basic(credentials, headers),
            Mode::Token(verifier) => verifier.verify(headers).await,
            Mode::Refused => Err(Denied::Unavailable(MISCONFIGURED.into())),
        }?;
        Ok(Verified {
            principal,
            session: None,
        })
    }

    /// Waits for the JWKS fetch under way, if any.
    #[cfg(test)]
    pub(crate) async fn settled(&self) {
        let mode = Arc::clone(
            &self
                .current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .mode,
        );
        if let Mode::Token(v) = &*mode {
            v.keys.settled().await;
        }
    }
}

/// The mode of `s`; a token mode takes `keys`, the cached JWKS, when they come from the same
/// source.
fn mode(s: &Settings, timing: Timing, keys: Option<Arc<Keys>>) -> Mode {
    let token = |issuer: &str, audience: &Option<String>, roles_claim: &str, source| {
        let keys = keys
            .filter(|k| k.issuer == issuer && k.source == source)
            .unwrap_or_else(|| Arc::new(Keys::new(issuer, source, timing)));
        Mode::Token(Box::new(Verifier {
            issuer: issuer.to_owned(),
            audience: audience.clone(),
            roles_claim: roles_claim.to_owned(),
            username_claim: s.username_claim.clone(),
            mapping: s.role_mapping.clone(),
            keys,
        }))
    };
    match (s.auth, &s.credentials, &s.jwt, &s.oauth2) {
        (ApiAuth::None, ..) => Mode::Open,
        (ApiAuth::Basic, Some(c), _, _) => Mode::Basic(c.clone()),
        (ApiAuth::Jwt, _, Some(j), _) => token(
            &j.issuer,
            &j.audience,
            &j.roles_claim,
            KeySource::Jwks(j.jwks_url.clone()),
        ),
        (ApiAuth::Oauth2, _, _, Some(o)) if o.jwks_url.is_some() || o.discovery_url.is_some() => {
            token(
                &o.issuer,
                &o.audience,
                &o.roles_claim,
                match (&o.jwks_url, &o.discovery_url) {
                    (Some(url), _) => KeySource::Jwks(url.clone()),
                    (None, url) => KeySource::Discovery {
                        url: url.clone().unwrap_or_default(),
                    },
                },
            )
        }
        // V-049…V-051 require the settings each mode reads: without them, nobody is let in.
        (auth, ..) => {
            tracing::error!(
                "globals.api.auth is {auth:?} without the settings it reads: every API request is refused (503)"
            );
            Mode::Refused
        }
    }
}

/// A session's person, verified again at `now`: it ends `ttl` after sign-in, whatever its use;
/// a basic session's user and password are still the configured ones; an oauth2 session's
/// access token verifies as a bearer token does.
async fn session(
    mode: &Mode,
    sessions: &Sessions,
    opened: Result<Session, Unsealed>,
    now: DateTime<Utc>,
) -> Result<Verified, Denied> {
    let challenge = match mode {
        Mode::Basic(_) => BASIC,
        _ => BEARER,
    };
    let session = opened.map_err(|e| unauthenticated(challenge, e.to_string()))?;
    let ends = sessions.ends(session.signed_in);
    if now >= ends {
        return Err(unauthenticated(
            challenge,
            format!(
                "the session expired at {} ({} after sign-in)",
                ends.to_rfc3339_opts(SecondsFormat::Secs, true),
                span(sessions.ttl)
            ),
        ));
    }
    let principal = match (mode, &session.by) {
        (Mode::Basic(c), By::Basic { user, fingerprint }) => {
            let now_password = sessions.sealer.fingerprint(c.password().expose());
            if !bool::from(same(user, c.user()) & same(fingerprint, &now_password)) {
                return Err(unauthenticated(
                    BASIC,
                    "the user or the password changed since the sign-in",
                ));
            }
            Principal {
                name: user.clone(),
                role: Role::Admin,
                expires: Some(ends),
            }
        }
        (Mode::Token(verifier), By::Oauth2 { access, .. }) => {
            let mut p = verifier.verify_token(access).await.map_err(|d| match d {
                Denied::Unauthenticated { challenge, why } => {
                    unauthenticated(challenge, format!("the provider's token: {why}"))
                }
                other => other,
            })?;
            p.expires = Some(p.expires.map_or(ends, |at| at.min(ends)));
            p
        }
        (Mode::Refused, _) => return Err(Denied::Unavailable(MISCONFIGURED.into())),
        _ => {
            return Err(unauthenticated(
                challenge,
                "the cluster's sign-in changed since the session began",
            ));
        }
    };
    Ok(Verified {
        principal,
        session: Some(session),
    })
}

/// A duration as a page says it: `8 h`, `90 min`, `45 s`.
pub(crate) fn span(d: Duration) -> String {
    match d.as_secs() {
        s if s % 3600 == 0 => format!("{} h", s / 3600),
        s if s % 60 == 0 => format!("{} min", s / 60),
        s => format!("{s} s"),
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

/// Equal in constant time, whatever the lengths: fixed-length digests of both are compared.
fn same(given: &str, expected: &str) -> subtle::Choice {
    blake3::hash(given.as_bytes())
        .as_bytes()
        .ct_eq(blake3::hash(expected.as_bytes()).as_bytes())
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
    let user_ok = same(user, c.user());
    let password_ok = same(password, c.password().expose());
    if bool::from(user_ok & password_ok) {
        Ok(Principal {
            name: user.to_owned(),
            role: Role::Admin,
            expires: None,
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
    username_claim: String,
    mapping: BTreeMap<Role, Vec<String>>,
    keys: Arc<Keys>,
}

impl Verifier {
    async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
        let token = credential(headers, "Bearer")
            .ok_or_else(|| unauthenticated(BEARER, "a bearer token is required"))?;
        self.verify_token(token).await
    }

    /// The caller `token` names, once it verifies.
    pub(crate) async fn verify_token(&self, token: &str) -> Result<Principal, Denied> {
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
        // `nbf` too, when the token has one, within the library's leeway (60 s).
        validation.validate_nbf = true;
        // The library checks `iss` and `aud` only when the token has them: require them.
        match &self.audience {
            Some(aud) => {
                validation.set_audience(&[aud]);
                validation.set_required_spec_claims(&["exp", "iss", "aud"]);
            }
            None => {
                validation.validate_aud = false;
                validation.set_required_spec_claims(&["exp", "iss"]);
            }
        }
        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|e| unauthenticated(BEARER, format!("the token doesn't verify: {e}")))?
            .claims;
        let name = claims
            .get(&self.username_claim)
            .and_then(Value::as_str)
            .or_else(|| claims.get("sub").and_then(Value::as_str))
            .unwrap_or("unknown")
            .to_owned();
        let expires = claims
            .get("exp")
            .and_then(Value::as_i64)
            .and_then(|exp| DateTime::from_timestamp(exp, 0));
        let Some(role) = roles(&claims, &self.roles_claim)
            .iter()
            .filter_map(|r| map(&self.mapping, r))
            .max()
        else {
            return Err(Denied::Forbidden {
                why: format!(
                    "the token's {} name no role this node maps",
                    self.roles_claim
                ),
                name,
            });
        };
        Ok(Principal {
            name,
            role,
            expires,
        })
    }
}

/// The strings at `path`: a list of them, or one. `path` is a claim's name
/// (`https://example.com/roles`), or else a dotted path (`realm_access.roles`).
fn roles(claims: &Value, path: &str) -> Vec<String> {
    let at = claims
        .get(path)
        .or_else(|| path.split('.').try_fold(claims, |at, part| at.get(part)));
    match at {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(items)) => items
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum KeySource {
    Jwks(String),
    /// OIDC discovery: the document's `jwks_uri`, once its `issuer` is found to be the
    /// configured one (OIDC Discovery §4.3).
    Discovery {
        url: String,
    },
}

/// The issuer's JWKS, cached. A fetch runs in a task of its own, one at a time, and the cache's
/// lock is never held across it: a caller whose key is cached verifies at once, whatever the
/// issuer does.
#[derive(Debug)]
struct Keys {
    /// The configured issuer: a discovery document must name it.
    issuer: String,
    source: KeySource,
    http: reqwest::Client,
    cache: Mutex<Cache>,
    timing: Timing,
}

#[derive(Debug, Default)]
struct Cache {
    set: Option<JwkSet>,
    /// When `set` was fetched.
    set_at: Option<Instant>,
    /// When a fetch was last started.
    fetched: Option<Instant>,
    /// The last fetch failed, until one succeeds (its reason is logged).
    failed: bool,
    /// The fetch under way: its sender is dropped once the outcome is stored.
    in_flight: Option<watch::Receiver<()>>,
}

impl Keys {
    fn new(issuer: &str, source: KeySource, timing: Timing) -> Self {
        Self {
            issuer: issuer.to_owned(),
            source,
            timing,
            http: reqwest::Client::builder()
                .timeout(FETCH_TIMEOUT)
                // The key URLs are the configured ones: a redirect is refused (DA-API-16).
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("the HTTP client builds"),
            cache: Mutex::new(Cache::default()),
        }
    }

    fn cache(&self) -> MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The key a token names (`kid`; without one, the set's only key). The JWKS is fetched when
    /// none is cached, when the cached one is [`KEYS_MAX_AGE`] old, or when it lacks the key;
    /// never more often than every [`REFETCH_AFTER`], and one fetch at a time. A key the cached
    /// set holds is taken at once, an old set's too (the fetch runs behind it). Only a caller
    /// whose key isn't cached waits for the fetch: with no set ever fetched, or with the last
    /// fetch failed, the keys are unavailable (503); a fetched set that lacks the key is a 401.
    /// A failed fetch keeps an older set.
    async fn key(self: &Arc<Self>, kid: Option<&str>) -> Result<DecodingKey, Denied> {
        let wait = {
            let mut cache = self.cache();
            let may_fetch = cache.in_flight.is_none()
                && cache
                    .fetched
                    .is_none_or(|at| at.elapsed() >= self.timing.refetch_after);
            if let Some(key) = cache.set.as_ref().and_then(|s| find(s, kid)) {
                let old = cache
                    .set_at
                    .is_some_and(|at| at.elapsed() >= self.timing.max_age);
                if old && may_fetch {
                    self.start(&mut cache);
                }
                return key;
            }
            if may_fetch {
                self.start(&mut cache);
            }
            cache.in_flight.clone()
        };
        if let Some(mut done) = wait {
            // Err once the fetch's outcome is stored: its sender is dropped.
            let _ = done.changed().await;
        }
        let cache = self.cache();
        match cache.set.as_ref().and_then(|s| find(s, kid)) {
            Some(key) => key,
            None if cache.failed || cache.set.is_none() => {
                Err(Denied::Unavailable(KEYS_UNAVAILABLE.into()))
            }
            None => Err(unauthenticated(
                BEARER,
                format!("no key {kid:?} in the issuer's JWKS"),
            )),
        }
    }

    /// Starts a fetch, its outcome stored in the cache when it ends.
    fn start(self: &Arc<Self>, cache: &mut Cache) {
        cache.fetched = Some(Instant::now());
        let (done, waited) = watch::channel(());
        cache.in_flight = Some(waited);
        let keys = Arc::clone(self);
        tokio::spawn(async move {
            let fetched = keys.fetch().await;
            let mut cache = keys.cache();
            match fetched {
                Ok(set) => {
                    cache.set = Some(set);
                    cache.set_at = Some(Instant::now());
                    cache.failed = false;
                }
                Err(why) => {
                    tracing::warn!("the API's token keys can't be fetched: {why}");
                    cache.failed = true;
                }
            }
            cache.in_flight = None;
            drop(cache);
            drop(done);
        });
    }

    /// Waits for the fetch under way, if any.
    #[cfg(test)]
    async fn settled(&self) {
        let in_flight = self.cache().in_flight.clone();
        if let Some(mut done) = in_flight {
            let _ = done.changed().await;
        }
    }

    async fn fetch(&self) -> Result<JwkSet, String> {
        let url = match &self.source {
            KeySource::Jwks(url) => url.clone(),
            KeySource::Discovery { url } => {
                let doc: Value = self.get(url).await?;
                let found = doc.get("issuer").and_then(Value::as_str);
                if found != Some(self.issuer.as_str()) {
                    return Err(format!(
                        "{url}: the issuer is {found:?}, not the configured {:?}",
                        self.issuer
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

    /// The document at `url`: a redirect isn't followed, and a body past
    /// [`MAX_KEY_DOCUMENT`] isn't read (DA-API-16).
    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, String> {
        let mut r = self
            .http
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| format!("{url}: {e}"))?;
        if r.status().is_redirection() {
            return Err(format!(
                "{url}: answered {}, and redirects aren't followed: configure the URL it names",
                r.status()
            ));
        }
        let too_long = || format!("{url}: the document is longer than {MAX_KEY_DOCUMENT} bytes");
        if r.content_length()
            .is_some_and(|n| n > MAX_KEY_DOCUMENT as u64)
        {
            return Err(too_long());
        }
        let mut body = Vec::new();
        while let Some(chunk) = r.chunk().await.map_err(|e| format!("{url}: {e}"))? {
            if body.len() + chunk.len() > MAX_KEY_DOCUMENT {
                return Err(too_long());
            }
            body.extend_from_slice(&chunk);
        }
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
    use crate::session::{By, Session};
    use crate::test_support::{
        Issuer, basic_api, basic_session, config, console, console_naming, session_cookie,
        sessions, sign_with,
    };

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

    /// DA-API-16: a key document's fetch follows no redirect and reads at most
    /// [`MAX_KEY_DOCUMENT`] bytes.
    #[tokio::test]
    async fn a_key_fetch_follows_no_redirect_and_reads_a_bounded_body() {
        use axum::response::{IntoResponse, Redirect};
        let app = axum::Router::new()
            .route(
                "/moved",
                axum::routing::get(|| async { Redirect::temporary("/keys") }),
            )
            .route("/keys", axum::routing::get(|| async { r#"{"keys": []}"# }))
            .route(
                "/big",
                axum::routing::get(|| async { "x".repeat(MAX_KEY_DOCUMENT + 1).into_response() }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        // A chunked body, no length announced.
        let raw = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let streamed = format!("http://{}/streamed", raw.local_addr().unwrap());
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut sock, _) = raw.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = sock.read(&mut request).await;
            let head = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
            sock.write_all(head.as_bytes()).await.unwrap();
            let chunk = format!("1000\r\n{}\r\n", " ".repeat(4096));
            for _ in 0..=MAX_KEY_DOCUMENT / 4096 {
                if sock.write_all(chunk.as_bytes()).await.is_err() {
                    return;
                }
            }
            let _ = sock.write_all(b"0\r\n\r\n").await;
        });
        let timing = Timing {
            refetch_after: Duration::ZERO,
            max_age: Duration::from_secs(60),
        };
        let keys = Keys::new(&url, KeySource::Jwks(String::new()), timing);
        let set: JwkSet = keys.get(&format!("{url}/keys")).await.unwrap();
        assert!(set.keys.is_empty());
        let e = keys
            .get::<JwkSet>(&format!("{url}/moved"))
            .await
            .unwrap_err();
        assert!(e.contains("redirects aren't followed"), "{e}");
        for long in [format!("{url}/big"), streamed] {
            let e = keys.get::<JwkSet>(&long).await.unwrap_err();
            assert!(e.contains("longer than 1048576 bytes"), "{long}: {e}");
        }
    }

    #[tokio::test]
    async fn without_auth_every_caller_is_admin() {
        let auth = Auth::new(&config().globals.api, &console());
        let p = auth.verify(&HeaderMap::new()).await.unwrap();
        assert_eq!((p.name.as_str(), p.role), ("anonymous", Role::Admin));
    }

    /// A mode without the settings it reads lets nobody in: 503 for every request.
    #[tokio::test]
    async fn auth_settings_that_dont_hold_together_refuse_every_request() {
        let idp = Issuer::start().await;
        let jwt_without = {
            let mut api = idp.api("roles");
            api.jwt = None;
            api
        };
        let basic_without = {
            let mut api = config().globals.api;
            api.auth = ApiAuth::Basic;
            api
        };
        let oauth2_without_urls = {
            let mut api = config().globals.api;
            api.auth = ApiAuth::Oauth2;
            api.oauth2 = Some(ApiOauth2 {
                issuer: idp.url.clone(),
                audience: None,
                jwks_url: None,
                discovery_url: None,
                roles_claim: "roles".into(),
                client_id: None,
                client_secret: None,
                scopes: Vec::new(),
                idp_name: None,
            });
            api
        };
        let token = bearer(&idp.token(json!(["admin"])));
        for api in [jwt_without, basic_without, oauth2_without_urls] {
            let auth = Auth::new(&api, &console());
            for headers in [HeaderMap::new(), token.clone()] {
                let why = unavailable(auth.verify(&headers).await);
                assert!(why.contains("inconsistent"), "{:?}: {why}", api.auth);
            }
        }
    }

    #[tokio::test]
    async fn basic_lets_its_one_user_in_as_admin() {
        let mut api = config().globals.api;
        api.auth = ApiAuth::Basic;
        api.credentials = Some(Credentials::Inline {
            user: "ops".into(),
            password: Secret::new("s3cret", &FingerprintKey::from_bytes([1; 32])),
        });
        let auth = Auth::new(&api, &console());
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
        let auth = Auth::new(&idp.api("roles"), &console());
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
            Err(Denied::Forbidden { .. })
        ));
        assert!(matches!(
            role(json!([])).await,
            Err(Denied::Forbidden { .. })
        ));

        // Keycloak's realm roles, mapped: the three names themselves no longer count.
        let mut api = idp.api("realm_access.roles");
        api.role_mapping = BTreeMap::from([
            (Role::Viewer, vec!["readonly".into()]),
            (Role::Admin, vec!["cdc-admin".into()]),
        ]);
        let auth = Auth::new(&api, &console());
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
            Err(Denied::Forbidden { .. })
        ));
    }

    /// Config §6.8: the caller is named by `username_claim` when the token has it as a string,
    /// else by `sub`.
    #[tokio::test]
    async fn a_token_s_caller_is_named_by_the_username_claim_else_sub() {
        let idp = Issuer::start().await;
        let named = |claims: &Value, username_claim: &str| {
            let (auth, token) = (
                Auth::new(&idp.api("roles"), &console_naming(username_claim)),
                bearer(&idp.sign("k1", claims)),
            );
            async move { auth.verify(&token).await.unwrap().name }
        };
        let mut claims = idp.claims(json!(["viewer"]));
        claims["sub"] = json!("u-123");
        claims["preferred_username"] = json!("oleksii");
        claims["email"] = json!("oleksii@example.com");
        assert_eq!(named(&claims, "preferred_username").await, "oleksii");
        assert_eq!(named(&claims, "email").await, "oleksii@example.com");
        claims.as_object_mut().unwrap().remove("preferred_username");
        assert_eq!(named(&claims, "preferred_username").await, "u-123");
        claims["preferred_username"] = json!(7);
        assert_eq!(
            named(&claims, "preferred_username").await,
            "u-123",
            "not a string"
        );
        // A reload's claim is the one taken.
        let auth = Auth::new(&idp.api("roles"), &console_naming("preferred_username"));
        auth.set(&idp.api("roles"), &console_naming("email"));
        let p = auth.verify(&bearer(&idp.sign("k1", &claims))).await;
        assert_eq!(p.unwrap().name, "oleksii@example.com");
        // A token with no role this node maps is refused under its name.
        let mut unmapped = claims.clone();
        unmapped["roles"] = json!(["cdc-reader"]);
        match auth.verify(&bearer(&idp.sign("k1", &unmapped))).await {
            Err(Denied::Forbidden { name, .. }) => assert_eq!(name, "oleksii@example.com"),
            other => panic!("not a 403: {other:?}"),
        }
    }

    /// A namespaced claim (Auth0's `https://…/roles`) is a name, not a dotted path.
    #[tokio::test]
    async fn a_namespaced_roles_claim_is_read_by_its_name() {
        let idp = Issuer::start().await;
        let claim = "https://example.com/roles";
        let auth = Auth::new(&idp.api(claim), &console());
        let mut claims = idp.claims(json!(null));
        claims[claim] = json!(["operator"]);
        let p = auth.verify(&bearer(&idp.sign("k1", &claims))).await;
        assert_eq!(p.unwrap().role, Role::Operator);
    }

    #[tokio::test]
    async fn a_token_that_doesnt_verify_is_refused() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.api("roles"), &console());
        let now = jsonwebtoken::get_current_timestamp();
        let with = |f: &dyn Fn(&mut Value)| {
            let mut c = idp.claims(json!(["admin"]));
            f(&mut c);
            bearer(&idp.sign("k1", &c))
        };
        let expired = with(&|c| c["exp"] = json!(now - 120));
        assert!(denied_401(auth.verify(&expired).await).contains("Expired"));
        // Not before ten minutes from now: past the 60 s leeway. Within it, taken.
        let early = with(&|c| c["nbf"] = json!(now + 600));
        assert!(denied_401(auth.verify(&early).await).contains("ImmatureSignature"));
        auth.verify(&with(&|c| c["nbf"] = json!(now + 30)))
            .await
            .unwrap();
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
        // A claim the verifier checks, left out: absent is not accepted.
        denied_401(
            auth.verify(&with(&|c| {
                c.as_object_mut().unwrap().remove("iss");
            }))
            .await,
        );
        denied_401(
            auth.verify(&with(&|c| {
                c.as_object_mut().unwrap().remove("aud");
            }))
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
            &jsonwebtoken::EncodingKey::from_secret(b"guess").unwrap(),
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
        let auth = Auth::new(&idp.api("roles"), &console());
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
            client_id: None,
            client_secret: None,
            scopes: Vec::new(),
            idp_name: None,
        };
        api.oauth2 = Some(oauth2(&idp.url));
        let token = bearer(&idp.token(json!(["operator"])));
        let p = Auth::new(&api, &console()).verify(&token).await.unwrap();
        assert_eq!(p.role, Role::Operator);

        // A discovery document of another issuer is not this one's.
        api.oauth2 = Some(oauth2("https://idp.example.com"));
        // The reason is logged; the caller isn't told the URL or the error.
        let why = unavailable(Auth::new(&api, &console()).verify(&token).await);
        assert_eq!(why, "the token issuer's keys can't be fetched");
        assert!(!why.contains(&idp.url), "{why}");
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
        let auth = Auth::new(&api, &console());
        let token = bearer(&idp.token(json!(["admin"])));
        // Again, inside the refetch window: still no keys, still 503.
        for _ in 0..2 {
            let why = unavailable(auth.verify(&token).await);
            assert_eq!(why, "the token issuer's keys can't be fetched");
        }
    }

    #[tokio::test]
    async fn a_withdrawn_key_stops_verifying_once_the_keys_are_old() {
        let idp = Issuer::start().await;
        let api = idp.api("roles");
        let token = bearer(&idp.token(json!(["viewer"])));

        // Young keys are trusted: the withdrawal isn't heard yet.
        let young = Auth::new(&api, &console());
        young.verify(&token).await.unwrap();
        idp.remove_key("k1");
        idp.add_key("k2");
        young.verify(&token).await.unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);

        // Old keys are fetched again, and the withdrawn one is gone.
        let old = Auth::with_timing(&api, Duration::ZERO, Duration::ZERO);
        let k2 = bearer(&idp.sign("k2", &idp.claims(json!(["viewer"]))));
        old.verify(&k2).await.unwrap();
        idp.add_key("k1");
        let token = bearer(&idp.token(json!(["viewer"])));
        old.verify(&token).await.unwrap();
        idp.remove_key("k1");
        // The old set still holds k1: taken at once, the keys fetched again behind it.
        old.verify(&token).await.unwrap();
        old.settled().await;
        denied_401(old.verify(&token).await);
    }

    #[tokio::test]
    async fn a_failed_refetch_of_old_keys_keeps_the_old_keys() {
        let idp = Issuer::start().await;
        let auth = Auth::with_timing(&idp.api("roles"), Duration::ZERO, Duration::ZERO);
        let token = bearer(&idp.token(json!(["viewer"])));
        auth.verify(&token).await.unwrap();
        idp.set_down(true);
        auth.verify(&token).await.unwrap();
        auth.settled().await;
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2, "it did try");
        auth.verify(&token).await.unwrap();
        idp.set_down(false);
    }

    /// A token with `claims`, signed by a key the issuer never published, under key id `k9`.
    fn unknown_kid(idp: &Issuer) -> HeaderMap {
        let stranger = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        bearer(&sign_with(
            &stranger,
            Some("k9"),
            &idp.claims(json!(["admin"])),
        ))
    }

    fn unavailable(r: Result<Principal, Denied>) -> String {
        match r {
            Err(Denied::Unavailable(why)) => why,
            other => panic!("not a 503: {other:?}"),
        }
    }

    /// Well under [`FETCH_TIMEOUT`].
    const AT_ONCE: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn old_keys_verify_at_once_while_the_issuer_hangs() {
        let idp = Issuer::start().await;
        let auth = Auth::with_timing(&idp.api("roles"), Duration::ZERO, Duration::ZERO);
        let token = bearer(&idp.token(json!(["viewer"])));
        auth.verify(&token).await.unwrap();
        idp.set_hanging(true);
        for _ in 0..2 {
            let r = tokio::time::timeout(AT_ONCE, auth.verify(&token))
                .await
                .expect("not held up by the issuer's fetch");
            assert_eq!(r.unwrap().role, Role::Viewer);
        }
        // The fetch reaches the issuer, and hangs there; no other starts beside it.
        while idp.fetches.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        auth.verify(&token).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2, "one fetch at a time");
    }

    #[tokio::test]
    async fn a_made_up_key_id_does_not_hold_up_a_good_token() {
        let idp = Issuer::start().await;
        let auth = Arc::new(Auth::with_timing(
            &idp.api("roles"),
            Duration::ZERO,
            Duration::ZERO,
        ));
        let token = bearer(&idp.token(json!(["viewer"])));
        auth.verify(&token).await.unwrap();
        idp.set_hanging(true);
        let made_up = tokio::spawn({
            let (auth, made_up) = (Arc::clone(&auth), unknown_kid(&idp));
            async move { auth.verify(&made_up).await }
        });
        // Until the made-up key id's fetch reaches the issuer, and hangs there.
        while idp.fetches.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let r = tokio::time::timeout(AT_ONCE, auth.verify(&token))
            .await
            .expect("not held up by the made-up key id's fetch");
        assert_eq!(r.unwrap().role, Role::Viewer);
        made_up.abort();
    }

    #[tokio::test]
    async fn a_key_missing_after_a_failed_refetch_is_unavailable() {
        let idp = Issuer::start().await;
        let auth = Auth::with_refetch_after(&idp.api("roles"), Duration::ZERO);
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        idp.set_down(true);
        unavailable(auth.verify(&unknown_kid(&idp)).await);
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2, "it did try");
        idp.set_down(false);
        denied_401(auth.verify(&unknown_kid(&idp)).await);
    }

    #[tokio::test]
    async fn after_a_failed_refetch_an_unknown_key_id_waits_the_refetch_interval() {
        let idp = Issuer::start().await;
        let interval = Duration::from_millis(300);
        let auth = Auth::with_refetch_after(&idp.api("roles"), interval);
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        tokio::time::sleep(interval + Duration::from_millis(50)).await;
        idp.set_down(true);
        unavailable(auth.verify(&unknown_kid(&idp)).await);
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2);
        // Throttled after the failure: no fetch, and still 503, not "no such key".
        unavailable(auth.verify(&unknown_kid(&idp)).await);
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn without_an_audience_a_token_without_aud_verifies() {
        let idp = Issuer::start().await;
        let mut api = idp.api("roles");
        api.jwt.as_mut().unwrap().audience = None;
        let auth = Auth::new(&api, &console());
        let mut claims = idp.claims(json!(["operator"]));
        claims.as_object_mut().unwrap().remove("aud");
        let p = auth.verify(&bearer(&idp.sign("k1", &claims))).await;
        assert_eq!(p.unwrap().role, Role::Operator);
    }

    #[tokio::test]
    async fn a_reload_of_the_role_mapping_keeps_the_keys() {
        let idp = Issuer::start().await;
        let api = idp.api("roles");
        let auth = Auth::new(&api, &console());
        let token = bearer(&idp.token(json!(["viewer"])));
        assert_eq!(auth.verify(&token).await.unwrap().role, Role::Viewer);
        idp.set_down(true);
        let mut mapped = api.clone();
        mapped.role_mapping = BTreeMap::from([(Role::Admin, vec!["viewer".into()])]);
        auth.set(&mapped, &console());
        let p = auth.verify(&token).await;
        assert_eq!(
            p.unwrap().role,
            Role::Admin,
            "the new mapping, the old keys"
        );
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);
        idp.set_down(false);
    }

    #[tokio::test]
    async fn a_reload_keeps_the_keys_unless_their_source_changes() {
        let idp = Issuer::start().await;
        let api = idp.api("roles");
        let auth = Auth::new(&api, &console());
        let token = bearer(&idp.token(json!(["viewer"])));
        auth.verify(&token).await.unwrap();
        let mut moved = api.clone();
        moved.address.port = 9_999;
        auth.set(&moved, &console());
        auth.verify(&token).await.unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);
        let mut other = api.clone();
        other.jwt.as_mut().unwrap().audience = Some("another-api".into());
        auth.set(&other, &console());
        denied_401(auth.verify(&token).await);
        assert_eq!(
            idp.fetches.load(Ordering::SeqCst),
            1,
            "the keys don't name an audience"
        );
        let mut moved = api.clone();
        moved.jwt.as_mut().unwrap().jwks_url = format!("{}/jwks?again", idp.url);
        auth.set(&moved, &console());
        auth.verify(&token).await.unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2, "another JWKS URL");
        auth.set(&config().globals.api, &console());
        assert_eq!(
            auth.verify(&HeaderMap::new()).await.unwrap().role,
            Role::Admin
        );
    }

    fn with_cookie(cookie: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(COOKIE, HeaderValue::from_str(cookie).unwrap());
        h
    }

    const HOUR: Duration = Duration::from_secs(3600);

    /// Admin_Console §9: every node opens a session with the key every node has, and verifies its
    /// person again as it verifies basic credentials.
    #[tokio::test]
    async fn a_basic_session_verifies_on_every_node_with_the_key() {
        let session = basic_session("s3cret", Duration::from_secs(60));
        let ends = DateTime::from_timestamp(session.signed_in, 0).unwrap() + HOUR;
        let cookie = with_cookie(&format!("theme=dark; {}", session_cookie(&session)));
        for _node in 0..2 {
            let auth = Auth::new(&basic_api("s3cret"), &sessions(HOUR));
            let v = auth.verify_caller(&cookie).await.unwrap();
            assert_eq!(
                v.principal,
                Principal {
                    name: "ops".into(),
                    role: Role::Admin,
                    expires: Some(ends),
                }
            );
            assert_eq!(v.session, Some(session.clone()));
        }
        // Credentials sent beside it are the ones taken.
        let mut both = cookie.clone();
        both.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Basic b3BzOm5vcGU="),
        );
        let auth = Auth::new(&basic_api("s3cret"), &sessions(HOUR));
        assert_eq!(
            denied_401(auth.verify(&both).await),
            "wrong user or password"
        );
    }

    /// S-M14.1: a session ends `session_ttl` after sign-in, however it was used.
    #[tokio::test]
    async fn a_session_ends_its_ttl_after_sign_in() {
        let auth = Auth::new(&basic_api("s3cret"), &sessions(8 * HOUR));
        let old = basic_session("s3cret", 8 * HOUR + Duration::from_secs(1));
        let why = denied_401(auth.verify(&with_cookie(&session_cookie(&old))).await);
        let ends = DateTime::from_timestamp(old.signed_in, 0).unwrap() + 8 * HOUR;
        assert_eq!(
            why,
            format!(
                "the session expired at {} (8 h after sign-in)",
                ends.to_rfc3339_opts(SecondsFormat::Secs, true)
            )
        );
        // A shorter `session_ttl`, reloaded, ends older sessions at once.
        let young = basic_session("s3cret", 2 * HOUR);
        let cookie = with_cookie(&session_cookie(&young));
        auth.verify(&cookie).await.unwrap();
        auth.set(&basic_api("s3cret"), &sessions(HOUR));
        assert!(denied_401(auth.verify(&cookie).await).contains("(1 h after sign-in)"));
    }

    /// Config §6.8: a password or a session key changed, the sign-in's mode changed, or the
    /// console switched off, and a session no longer lets its person in.
    #[tokio::test]
    async fn a_session_ends_with_its_password_its_key_or_its_mode() {
        let cookie = with_cookie(&session_cookie(&basic_session("s3cret", HOUR / 2)));
        let auth = Auth::new(&basic_api("s3cret"), &sessions(HOUR));
        auth.verify(&cookie).await.unwrap();
        auth.set(&basic_api("rotated"), &sessions(HOUR));
        assert_eq!(
            denied_401(auth.verify(&cookie).await),
            "the user or the password changed since the sign-in"
        );
        let mut rekeyed = sessions(HOUR);
        rekeyed.session_key = Some(Secret::new(
            "another session key, of 32 bytes or more",
            &FingerprintKey::from_bytes([1; 32]),
        ));
        auth.set(&basic_api("s3cret"), &rekeyed);
        assert_eq!(
            denied_401(auth.verify(&cookie).await),
            "the session key changed"
        );
        auth.set(&basic_api("s3cret"), &sessions(HOUR));
        assert_eq!(
            denied_401(auth.verify(&with_cookie("__Host-da_session=AAAA")).await),
            "the session cookie doesn't decrypt"
        );
        // The console off: the cookie isn't looked at.
        auth.set(&basic_api("s3cret"), &console());
        assert_eq!(
            denied_401(auth.verify(&cookie).await),
            "basic credentials are required"
        );
        // Another mode: a basic session isn't an oauth2 one.
        let idp = Issuer::start().await;
        auth.set(&idp.oauth2_api("roles"), &sessions(HOUR));
        assert_eq!(
            denied_401(auth.verify(&cookie).await),
            "the cluster's sign-in changed since the session began"
        );
        // `jwt` and `none` have no sessions.
        for api in [idp.api("roles"), config().globals.api] {
            auth.set(&api, &sessions(HOUR));
            assert!(
                auth.verify_caller(&cookie)
                    .await
                    .map_or(true, |v| v.session.is_none())
            );
        }
    }

    /// Admin_Console §9: an oauth2 session's role and name come from its access token, verified as
    /// a bearer token is; it lasts while both the token and the session do.
    #[tokio::test]
    async fn an_oauth2_session_verifies_its_token_as_a_bearer_token_is() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.oauth2_api("roles"), &sessions(8 * HOUR));
        let session_of = |claims: &Value| {
            Session::new(
                By::Oauth2 {
                    access: idp.sign("k1", claims),
                    refresh: None,
                },
                Utc::now().timestamp() - 60,
            )
        };
        let mut claims = idp.claims(json!(["operator"]));
        claims["preferred_username"] = json!("alice.smith");
        let cookie = with_cookie(&session_cookie(&session_of(&claims)));
        let p = auth.verify(&cookie).await.unwrap();
        let exp = claims["exp"].as_i64().unwrap();
        assert_eq!(
            (p.name.as_str(), p.role, p.expires),
            (
                "alice.smith",
                Role::Operator,
                DateTime::from_timestamp(exp, 0)
            ),
            "the token's expiry, before the session's"
        );
        // The session's end, when the token outlives it.
        let short = Auth::new(
            &idp.oauth2_api("roles"),
            &sessions(Duration::from_secs(300)),
        );
        let s = session_of(&claims);
        let ends = DateTime::from_timestamp(s.signed_in + 300, 0);
        let p = short.verify(&with_cookie(&session_cookie(&s))).await;
        assert_eq!(p.unwrap().expires, ends);
        // A token expired (past the verifier's minute of leeway) or with no role mapped.
        let mut expired = claims.clone();
        expired["exp"] = json!(Utc::now().timestamp() - 120);
        let why = denied_401(
            auth.verify(&with_cookie(&session_cookie(&session_of(&expired))))
                .await,
        );
        assert!(why.starts_with("the provider's token: "), "{why}");
        let mut unmapped = claims.clone();
        unmapped["roles"] = json!(["cdc-reader"]);
        assert!(matches!(
            auth.verify(&with_cookie(&session_cookie(&session_of(&unmapped))))
                .await,
            Err(Denied::Forbidden { name, .. }) if name == "alice.smith"
        ));
    }

    fn challenge_of(r: Result<Principal, Denied>) -> &'static str {
        match r {
            Err(Denied::Unauthenticated { challenge, .. }) => challenge,
            other => panic!("not a 401: {other:?}"),
        }
    }

    /// Admin_Console §9: with the sessions on, a 401 to a request without `Authorization`
    /// carries a challenge no browser answers with its credentials popup.
    #[tokio::test]
    async fn a_refused_session_or_no_credentials_are_challenged_for_a_session() {
        let on = Auth::new(&basic_api("s3cret"), &sessions(HOUR));
        let old = basic_session("s3cret", 2 * HOUR);
        let refused = on.verify(&with_cookie(&session_cookie(&old))).await;
        assert_eq!(challenge_of(refused), SESSION);
        assert_eq!(challenge_of(on.verify(&HeaderMap::new()).await), SESSION);
        // Credentials sent keep their scheme's challenge.
        let mut wrong = HeaderMap::new();
        wrong.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Basic b3BzOm5vcGU="),
        );
        assert_eq!(challenge_of(on.verify(&wrong).await), BASIC);
        // The console off: nothing changes.
        let off = Auth::new(&basic_api("s3cret"), &console());
        assert_eq!(challenge_of(off.verify(&HeaderMap::new()).await), BASIC);
        let idp = Issuer::start().await;
        let oauth2 = Auth::new(&idp.oauth2_api("roles"), &sessions(HOUR));
        assert_eq!(
            challenge_of(oauth2.verify(&HeaderMap::new()).await),
            SESSION
        );
        assert_eq!(challenge_of(oauth2.verify(&bearer("nope")).await), BEARER);
    }

    /// A verifier that can't check refuses no one: a session too is answered 503.
    #[tokio::test]
    async fn a_session_under_inconsistent_settings_is_answered_unavailable() {
        let cookie = with_cookie(&session_cookie(&basic_session("s3cret", HOUR / 2)));
        let mut api = basic_api("s3cret");
        api.credentials = None;
        let auth = Auth::new(&api, &sessions(HOUR));
        assert_eq!(
            auth.verify_caller(&cookie).await.map(|_| ()),
            Err(Denied::Unavailable(MISCONFIGURED.into()))
        );
    }

    /// What a peer is passed of a caller's credentials never prints.
    #[test]
    fn what_a_caller_presented_never_prints() {
        let p = crate::test_support::presented(&[
            ("authorization", "Bearer the-token"),
            ("cookie", "__Host-da_session=the-session"),
            (CSRF_HEADER, "the-csrf"),
        ]);
        let printed = format!("{p:?}");
        for secret in ["the-token", "the-session", "the-csrf"] {
            assert!(!printed.contains(secret), "{printed}");
        }
        assert_eq!(p.headers().len(), 3);
    }
}
