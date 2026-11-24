//! The console's `oauth2` sign-in (Admin_Console §9; config §6.3.1): authorization code with
//! PKCE (S256) against the cluster's provider, the code exchanged by the server with the
//! console's client (`globals.api.oauth2.client_id`, and its secret for a confidential client);
//! the session refreshed with the provider's refresh token, when it issued one; and the sign-out
//! taken on to the provider's end-session endpoint, when it has one. The endpoints come from the
//! provider's OIDC discovery document (`discovery_url`, else the issuer's
//! `/.well-known/openid-configuration`), whose `issuer` must be the configured one.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use aws_lc_rs::digest::{SHA256, digest};
use axum::Extension;
use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use chrono::Utc;
use data_avalanche_config::{ApiAuth, ApiOauth2};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::{Denied, FETCH_TIMEOUT, MAX_KEY_DOCUMENT, Sessions, SignIn};
use crate::console::{
    challenged, client, frame, html, params, safe_next, same_origin, see_other, session_cookies,
    session_json, too_many_json, with_cookies,
};
use crate::pages;
use crate::session::{By, KEPT_AFTER, Session, as_sent, cookie, random_token, removed, set_cookie};
use crate::{Api, body};

/// How long a sign-in may stay at the provider: the state it left with lasts this long.
pub const SIGN_IN_WITHIN: Duration = Duration::from_secs(600);
/// An `oauth2` session's access token is refreshed once it expires within this long.
pub const REFRESH_AHEAD: Duration = Duration::from_secs(60);
/// The sign-in page goes to the provider by itself after this long, unless the person stays
/// (Admin_Console §9).
pub const AUTO_REDIRECT: Duration = Duration::from_secs(5);
/// The provider's discovery document is read again after this long.
const ENDPOINTS_MAX_AGE: Duration = Duration::from_secs(600);
/// A refresh's outcome is given again, for this long, to another request that brings the same
/// refresh token: a provider that rotates refresh tokens refuses one used twice, and may end
/// the whole sign-in for it.
const REFRESHED_KEPT: Duration = Duration::from_secs(30);

/// The provider's endpoints the console uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Endpoints {
    pub authorization: String,
    pub token: String,
    pub end_session: Option<String>,
    /// The client authenticates with HTTP Basic (`client_secret_basic`, the default), else in
    /// the form (`client_secret_post`), as the provider says it takes.
    pub secret_in_form: bool,
}

/// What the token endpoint answered.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Tokens {
    pub access: String,
    pub refresh: Option<String>,
    pub id: Option<String>,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tokens").finish_non_exhaustive()
    }
}

/// When a refresh was made, and what it gave.
type Refreshed = (Instant, Result<Tokens, String>);

/// The discovery URL read and the issuer it was checked against, its endpoints, and when.
type Discovered = (String, String, Endpoints, Instant);

/// A refresh token's digest, which keys the refreshes in flight and their outcomes.
type Digest = [u8; 32];

/// The client of the cluster's provider: its endpoints cached, and the refreshes just made.
#[derive(Debug)]
pub(crate) struct Provider {
    http: reqwest::Client,
    /// The discovery URL read and the issuer checked, its endpoints, and when.
    endpoints: Mutex<Option<Discovered>>,
    /// A refresh token's digest, and what its refresh gave.
    refreshed: Mutex<HashMap<Digest, Refreshed>>,
    /// The refreshes in flight, each held through its refresh: two requests that bring the same
    /// refresh token at once make one, and other tokens' refreshes don't wait for it.
    refreshing: Mutex<HashMap<Digest, Arc<tokio::sync::Mutex<()>>>>,
}

/// A refresh in flight: its entry in [`Provider::refreshing`], removed when it ends, however it
/// ends (a request whose client went away included).
struct InFlight<'p> {
    refreshing: &'p Mutex<HashMap<Digest, Arc<tokio::sync::Mutex<()>>>>,
    key: Digest,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let mut in_flight = self
            .refreshing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // The last of the requests that share this entry removes it (the map's and this one's
        // are the only holders left; another takes its own under the map's lock).
        if in_flight
            .get(&self.key)
            .is_some_and(|l| Arc::ptr_eq(l, &self.lock))
            && Arc::strong_count(&self.lock) == 2
        {
            in_flight.remove(&self.key);
        }
    }
}

impl Default for Provider {
    fn default() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(FETCH_TIMEOUT)
                // The provider's URLs are the configured and discovered ones (DA-API-16).
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("the HTTP client builds"),
            endpoints: Mutex::default(),
            refreshed: Mutex::default(),
            refreshing: Mutex::default(),
        }
    }
}

/// The discovery document of `o`.
fn discovery_url(o: &ApiOauth2) -> String {
    o.discovery_url.clone().unwrap_or_else(|| {
        format!(
            "{}/.well-known/openid-configuration",
            o.issuer.trim_end_matches('/')
        )
    })
}

/// A PKCE verifier's challenge (RFC 7636 §4.2, `S256`).
pub(crate) fn challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, verifier.as_bytes()).as_ref())
}

/// The origin of `url`, as a Content-Security-Policy source.
pub(crate) fn origin(url: &str) -> Option<String> {
    let u = url::Url::parse(url).ok()?;
    Some(u.origin().ascii_serialization()).filter(|o| o != "null")
}

/// The seconds until the JWT `token` expires, read without verifying it (only to decide when to
/// refresh: the new token is verified as any is); `None` when it can't be read.
pub(crate) fn expires_in(token: &str, now: i64) -> Option<i64> {
    let payload = token.split('.').nth(1)?;
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
    Some(claims.get("exp")?.as_i64()? - now)
}

/// `form` as the console's client posts it to the token endpoint, and its `Authorization`: a
/// confidential client's secret goes in HTTP Basic (`client_secret_basic`, RFC 6749 §2.3.1),
/// or in the form when the provider takes only that (`client_secret_post`); a public client
/// names itself in the form.
fn client_form(o: &ApiOauth2, e: &Endpoints, form: &[(&str, &str)]) -> (String, Option<String>) {
    let client_id = o.client_id.as_deref().unwrap_or_default();
    let mut body = url::form_urlencoded::Serializer::new(String::new());
    body.extend_pairs(form);
    let mut basic = None;
    match &o.client_secret {
        Some(secret) if e.secret_in_form => {
            body.append_pair("client_id", client_id)
                .append_pair("client_secret", secret.expose());
        }
        Some(secret) => {
            // Each form-encoded, then joined by `:`.
            let enc =
                |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
            let pair = format!("{}:{}", enc(client_id), enc(secret.expose()));
            basic = Some(format!("Basic {}", STANDARD.encode(pair)));
        }
        None => {
            body.append_pair("client_id", client_id);
        }
    }
    (body.finish(), basic)
}

impl Provider {
    /// The endpoints of `o`'s provider: discovered, then cached for [`ENDPOINTS_MAX_AGE`].
    ///
    /// # Errors
    /// The discovery document can't be read, names another issuer, or lacks an endpoint.
    pub(crate) async fn endpoints(&self, o: &ApiOauth2) -> Result<Endpoints, String> {
        let url = discovery_url(o);
        if let Some((at, issuer, e, when)) = &*self
            .endpoints
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            && *at == url
            && *issuer == o.issuer
            && when.elapsed() < ENDPOINTS_MAX_AGE
        {
            return Ok(e.clone());
        }
        let doc: Value = serde_json::from_slice(&self.read(self.http.get(&url)).await?)
            .map_err(|e| format!("{url}: {e}"))?;
        let found = doc.get("issuer").and_then(Value::as_str);
        if found != Some(o.issuer.as_str()) {
            return Err(format!(
                "{url}: the issuer is {found:?}, not the configured {:?}",
                o.issuer
            ));
        }
        let text = |name: &str| doc.get(name).and_then(Value::as_str).map(str::to_owned);
        let methods: Vec<&str> = doc
            .get("token_endpoint_auth_methods_supported")
            .and_then(Value::as_array)
            .map(|m| m.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let e = Endpoints {
            authorization: text("authorization_endpoint")
                .ok_or_else(|| format!("{url}: no authorization_endpoint"))?,
            token: text("token_endpoint").ok_or_else(|| format!("{url}: no token_endpoint"))?,
            end_session: text("end_session_endpoint"),
            secret_in_form: !methods.is_empty()
                && !methods.contains(&"client_secret_basic")
                && methods.contains(&"client_secret_post"),
        };
        *self
            .endpoints
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            Some((url, o.issuer.clone(), e.clone(), Instant::now()));
        Ok(e)
    }

    /// The endpoints cached for `o`, if any: what the Content-Security-Policy reads.
    pub(crate) fn cached(&self, o: &ApiOauth2) -> Option<Endpoints> {
        let url = discovery_url(o);
        self.endpoints
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|(at, issuer, _, _)| *at == url && *issuer == o.issuer)
            .map(|(_, _, e, _)| e.clone())
    }

    /// The body of `req`'s answer: a redirect isn't followed, an answer other than `200` is an
    /// error with the provider's `error_description` (or `error`), and a body past
    /// [`MAX_KEY_DOCUMENT`] isn't read.
    async fn read(&self, req: reqwest::RequestBuilder) -> Result<Vec<u8>, String> {
        let mut r = req.send().await.map_err(|e| crate::peers::cause(&e))?;
        let code = r.status();
        let mut body = Vec::new();
        while let Some(chunk) = r.chunk().await.map_err(|e| crate::peers::cause(&e))? {
            if body.len() + chunk.len() > MAX_KEY_DOCUMENT {
                return Err(format!(
                    "the provider's answer is longer than {MAX_KEY_DOCUMENT} bytes"
                ));
            }
            body.extend_from_slice(&chunk);
        }
        if code != reqwest::StatusCode::OK {
            let said: Option<String> = serde_json::from_slice::<Value>(&body).ok().and_then(|v| {
                v.get("error_description")
                    .or_else(|| v.get("error"))
                    .and_then(Value::as_str)
                    .map(|s| crate::actions::clipped(s, crate::MAX_ERROR).into_owned())
            });
            return Err(match said {
                Some(s) => format!("the provider answered {}: {s}", code.as_u16()),
                None => format!("the provider answered {}", code.as_u16()),
            });
        }
        Ok(body)
    }

    /// Posts `form` to the token endpoint as the console's client.
    async fn token(
        &self,
        o: &ApiOauth2,
        e: &Endpoints,
        form: &[(&str, &str)],
    ) -> Result<Tokens, String> {
        let (form, basic) = client_form(o, e, form);
        let mut req = self
            .http
            .post(&e.token)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            );
        if let Some(basic) = basic {
            req = req.header(reqwest::header::AUTHORIZATION, basic);
        }
        let answer = self.read(req.body(form)).await?;
        let v: Value = serde_json::from_slice(&answer)
            .map_err(|e| format!("the provider's token answer: {e}"))?;
        let text = |name: &str| v.get(name).and_then(Value::as_str).map(str::to_owned);
        Ok(Tokens {
            access: text("access_token")
                .ok_or("the provider's token answer has no access_token")?,
            refresh: text("refresh_token"),
            id: text("id_token"),
        })
    }

    /// Exchanges a sign-in's `code` (RFC 6749 §4.1.3, with the PKCE `verifier`).
    ///
    /// # Errors
    /// The provider refused it, or answered what isn't a token answer.
    pub(crate) async fn exchange(
        &self,
        o: &ApiOauth2,
        e: &Endpoints,
        code: &str,
        verifier: &str,
        redirect_uri: &str,
    ) -> Result<Tokens, String> {
        self.token(
            o,
            e,
            &[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", redirect_uri),
                ("code_verifier", verifier),
            ],
        )
        .await
    }

    /// Refreshes with `refresh` (RFC 6749 §6); a provider that issues no new refresh token keeps
    /// the old one. Another request bringing the same refresh token within
    /// [`REFRESHED_KEPT`] gets this one's outcome, without asking the provider again.
    ///
    /// # Errors
    /// The provider refused it.
    pub(crate) async fn refresh(
        &self,
        o: &ApiOauth2,
        e: &Endpoints,
        refresh: &str,
    ) -> Result<Tokens, String> {
        let key = *blake3::hash(refresh.as_bytes()).as_bytes();
        let in_flight = InFlight {
            refreshing: &self.refreshing,
            key,
            lock: Arc::clone(
                self.refreshing
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entry(key)
                    .or_default(),
            ),
        };
        let _one_at_a_time = in_flight.lock.lock().await;
        {
            let mut done = self
                .refreshed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            done.retain(|_, (at, _)| at.elapsed() < REFRESHED_KEPT);
            if let Some((_, outcome)) = done.get(&key) {
                return outcome.clone();
            }
        }
        let outcome = self
            .token(
                o,
                e,
                &[("grant_type", "refresh_token"), ("refresh_token", refresh)],
            )
            .await
            .map(|mut t| {
                t.refresh = t.refresh.or_else(|| Some(refresh.to_owned()));
                t
            });
        self.refreshed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, (Instant::now(), outcome.clone()));
        outcome
    }
}

/// The cookie a sign-in leaves for the provider with: its state, PKCE verifier and `next`,
/// sealed. Sent back only to the callback, and `SameSite=Lax`: the provider's redirect back is
/// another site's navigation, which a `Strict` cookie doesn't follow.
pub const SIGN_IN_COOKIE: &str = "da_oauth2";
/// The cookie that keeps the provider's ID token, sealed, for the sign-out's `id_token_hint`:
/// sent only to `/ui/sign-out`.
pub const ID_COOKIE: &str = "da_id";
/// The callback, under the console's `public_url`: the redirect URI the provider registered.
const CALLBACK: &str = "/ui/oauth2/callback";
/// The sign-out, the only path the ID token's cookie is sent to.
const SIGN_OUT: &str = "/ui/sign-out";

/// What a sign-in left with, in [`SIGN_IN_COOKIE`].
#[derive(Serialize, Deserialize)]
struct Started {
    state: String,
    verifier: String,
    next: String,
    /// When it began, Unix seconds.
    at: i64,
}

/// What the `oauth2` routes read: the provider's settings, the sessions, the redirect URI.
pub(crate) struct Client<'s> {
    pub o: &'s ApiOauth2,
    pub sessions: &'s Sessions,
    pub public_url: &'s str,
}

impl<'s> Client<'s> {
    /// The console's client, with `auth: oauth2` and what it reads (V-135, V-136).
    pub(crate) fn of(sign_in: &'s SignIn) -> Option<Self> {
        match (
            sign_in.auth,
            &sign_in.oauth2,
            &sign_in.sessions,
            &sign_in.public_url,
        ) {
            (ApiAuth::Oauth2, Some(o), Some(sessions), Some(url)) if o.client_id.is_some() => {
                Some(Self {
                    o,
                    sessions,
                    public_url: url.trim_end_matches('/'),
                })
            }
            _ => None,
        }
    }

    fn redirect_uri(&self) -> String {
        format!("{}{CALLBACK}", self.public_url)
    }

    /// The provider as the sign-in page names it: `idp_name`, else the issuer's host.
    pub(crate) fn name(&self) -> String {
        self.o.idp_name.clone().unwrap_or_else(|| {
            url::Url::parse(&self.o.issuer)
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned))
                .unwrap_or_else(|| self.o.issuer.clone())
        })
    }
}

/// `form-action`'s other sources: the provider's origins a sign-out form goes on to (the
/// issuer's, and its end-session endpoint's once discovered).
pub(crate) fn form_action(api: &Api, sign_in: &SignIn) -> String {
    let Some(c) = Client::of(sign_in) else {
        return String::new();
    };
    let mut origins: BTreeSet<String> = origin(&c.o.issuer).into_iter().collect();
    if let Some(end) = api
        .auth
        .provider
        .cached(c.o)
        .and_then(|e| e.end_session)
        .and_then(|e| origin(&e))
    {
        origins.insert(end);
    }
    origins.iter().map(|o| format!(" {o}")).collect()
}

/// The `oauth2` sign-in page: it goes to the provider by itself after [`AUTO_REDIRECT`], unless
/// it follows a sign-out or a failed sign-in, or the person chose to stay (no loop).
pub(crate) fn sign_in_page(
    api: &Api,
    sign_in: &SignIn,
    q: &BTreeMap<String, String>,
    next: &str,
) -> Response {
    let Some(c) = Client::of(sign_in) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let stays = ["signed_out", "failed", "stay"]
        .iter()
        .any(|k| q.contains_key(*k));
    let info = q.contains_key("signed_out").then_some("You signed out.");
    html(
        StatusCode::OK,
        pages::oauth2_sign_in(
            &frame(api, sign_in),
            next,
            &c.name(),
            (!stays).then_some(AUTO_REDIRECT.as_secs()),
            None,
            info,
        ),
    )
}

/// A failed sign-in: the sign-in page again, saying why, without the countdown; the sign-in's
/// state is spent.
fn failed(
    api: &Api,
    sign_in: &SignIn,
    c: &Client<'_>,
    code: StatusCode,
    next: &str,
    why: &str,
) -> Response {
    with_cookies(
        not_signed_in(api, sign_in, c, code, next, why),
        [removed(SIGN_IN_COOKIE, CALLBACK)],
    )
}

/// The sign-in page again, saying why, without the countdown; a sign-in in progress is kept.
fn not_signed_in(
    api: &Api,
    sign_in: &SignIn,
    c: &Client<'_>,
    code: StatusCode,
    next: &str,
    why: &str,
) -> Response {
    let r = html(
        code,
        pages::oauth2_sign_in(&frame(api, sign_in), next, &c.name(), None, Some(why), None),
    );
    if code == StatusCode::UNAUTHORIZED {
        challenged(r, crate::console::challenge(sign_in.auth))
    } else {
        r
    }
}

/// `GET /ui/oauth2/start`: to the provider's authorization endpoint (authorization code, PKCE
/// `S256`, a new state), what came back checked against [`SIGN_IN_COOKIE`].
pub(crate) async fn start(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(c) = Client::of(&sign_in) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let next = safe_next(params(raw.as_deref()).get("next").map(String::as_str));
    let endpoints = match api.auth.provider.endpoints(c.o).await {
        Ok(e) => e,
        Err(e) => {
            let why = format!("{} can't be reached: {e}", c.name());
            return failed(
                &api,
                &sign_in,
                &c,
                StatusCode::SERVICE_UNAVAILABLE,
                &next,
                &why,
            );
        }
    };
    let started = Started {
        state: random_token(),
        verifier: random_token(),
        next,
        at: Utc::now().timestamp(),
    };
    let mut to = match url::Url::parse(&endpoints.authorization) {
        Ok(u) => u,
        Err(e) => {
            let why = format!(
                "{} can't be reached: its authorization_endpoint {:?} isn't a URL: {e}",
                c.name(),
                endpoints.authorization
            );
            return failed(
                &api,
                &sign_in,
                &c,
                StatusCode::SERVICE_UNAVAILABLE,
                &started.next,
                &why,
            );
        }
    };
    to.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", c.o.client_id.as_deref().unwrap_or_default())
        .append_pair("redirect_uri", &c.redirect_uri())
        .append_pair("scope", &c.o.scopes.join(" "))
        .append_pair("state", &started.state)
        .append_pair("code_challenge", &challenge(&started.verifier))
        .append_pair("code_challenge_method", "S256");
    let sealed = c.sessions.sealer.seal(
        "oauth2",
        &serde_json::to_vec(&started).expect("a sign-in serializes"),
    );
    with_cookies(
        see_other(to.as_str()),
        [set_cookie(
            SIGN_IN_COOKIE,
            &sealed,
            CALLBACK,
            SIGN_IN_WITHIN.as_secs(),
            "Lax",
        )],
    )
}

/// `GET /ui/oauth2/callback`: the sign-in checked against what it left with, its code
/// exchanged, the provider's token verified as the API verifies one, and a session started. A
/// token with no role this node maps is "Forbidden" at once, naming the roles it carried.
pub(crate) async fn callback(
    State(api): State<Api>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(c) = Client::of(&sign_in) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let q = params(raw.as_deref());
    let started: Option<Started> = cookie(&headers, SIGN_IN_COOKIE)
        .and_then(|v| c.sessions.sealer.open("oauth2", v).ok())
        .and_then(|plain| serde_json::from_slice(&plain).ok())
        .filter(|s: &Started| {
            Utc::now().timestamp() - s.at
                < i64::try_from(SIGN_IN_WITHIN.as_secs()).unwrap_or(i64::MAX)
        });
    let Some(started) = started else {
        let why = format!(
            "The sign-in took more than {}, or began in another browser: sign in again.",
            crate::auth::span(SIGN_IN_WITHIN)
        );
        return failed(&api, &sign_in, &c, StatusCode::BAD_REQUEST, "/ui/", &why);
    };
    // The state first: an answer that isn't this sign-in's says nothing of it (not even the
    // provider's error, whose text whoever sent it chose), and the sign-in stays in progress.
    let state = q.get("state").map_or("", String::as_str);
    if !crate::auth::same_secret(state, &started.state) {
        return not_signed_in(
            &api,
            &sign_in,
            &c,
            StatusCode::BAD_REQUEST,
            &started.next,
            "The provider's answer isn't this sign-in's: sign in again.",
        );
    }
    let fail = |code, why: &str| failed(&api, &sign_in, &c, code, &started.next, why);
    if let Some(e) = q.get("error") {
        let said = q.get("error_description").unwrap_or(e);
        let why = format!(
            "{} refused the sign-in: {}",
            c.name(),
            crate::actions::clipped(said, crate::MAX_ERROR)
        );
        return fail(StatusCode::BAD_REQUEST, &why);
    }
    let Some(code) = q.get("code") else {
        return fail(
            StatusCode::BAD_REQUEST,
            "The provider's answer has no code: sign in again.",
        );
    };
    let tokens = match api.auth.provider.endpoints(c.o).await {
        Ok(e) => {
            api.auth
                .provider
                .exchange(c.o, &e, code, &started.verifier, &c.redirect_uri())
                .await
        }
        Err(e) => Err(e),
    };
    let tokens = match tokens {
        Ok(t) => t,
        Err(e) => {
            let why = format!("{} didn't exchange the sign-in's code: {e}", c.name());
            return fail(StatusCode::BAD_GATEWAY, &why);
        }
    };
    match api.auth.verify_token(&tokens.access).await {
        Ok(_) => {}
        Err(Denied::Forbidden { name, why }) => {
            return with_cookies(
                html(
                    StatusCode::FORBIDDEN,
                    pages::forbidden(&frame(&api, &sign_in), &name, &why, false),
                ),
                [removed(SIGN_IN_COOKIE, CALLBACK)],
            );
        }
        Err(Denied::Unauthenticated { why, .. }) => {
            let why = format!("The provider's token didn't verify: {why}.");
            return fail(StatusCode::UNAUTHORIZED, &why);
        }
        Err(Denied::Unavailable(why)) => {
            let why = format!("The provider's token can't be checked now: {why}.");
            return fail(StatusCode::SERVICE_UNAVAILABLE, &why);
        }
    }
    let session = Session::new(
        By::Oauth2 {
            access: tokens.access,
            refresh: tokens.refresh,
        },
        Utc::now().timestamp(),
    );
    let cookies = match session_cookies(&session, c.sessions) {
        Ok(cookies) => cookies,
        Err(e) => return fail(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    let r = with_cookies(
        html(
            StatusCode::OK,
            pages::signing_in(&frame(&api, &sign_in), &started.next),
        ),
        cookies
            .into_iter()
            .chain([removed(SIGN_IN_COOKIE, CALLBACK)]),
    );
    let id = tokens
        .id
        .map(|id| c.sessions.sealer.seal("id_token", id.as_bytes()))
        .filter(|sealed| sealed.len() <= crate::COOKIE_CHUNK);
    let max_age = (c.sessions.ttl + KEPT_AFTER).as_secs();
    with_cookies(
        r,
        id.map(|sealed| set_cookie(ID_COOKIE, &sealed, SIGN_OUT, max_age, "Strict")),
    )
}

/// `session` with its access token refreshed; it keeps when it signed in and its anti-forgery
/// token, so it ends when it would have.
///
/// # Errors
/// It has no refresh token, or the provider refused it.
pub(crate) async fn renewed(
    api: &Api,
    c: &Client<'_>,
    session: &Session,
) -> Result<Session, String> {
    let By::Oauth2 {
        refresh: Some(refresh),
        ..
    } = &session.by
    else {
        return Err("the provider issued no refresh token".into());
    };
    let e = api.auth.provider.endpoints(c.o).await?;
    let t = api.auth.provider.refresh(c.o, &e, refresh).await?;
    Ok(Session {
        by: By::Oauth2 {
            access: t.access,
            refresh: t.refresh,
        },
        ..session.clone()
    })
}

/// The refresh a page's request makes: an `oauth2` session, not ended, whose access token
/// expires within [`REFRESH_AHEAD`] and which has a refresh token, refreshed. `None`: nothing to
/// refresh.
pub(crate) async fn due(
    api: &Api,
    sign_in: &SignIn,
    headers: &HeaderMap,
) -> Option<Result<Session, String>> {
    let c = Client::of(sign_in)?;
    let session = Session::of(headers, &c.sessions.sealer)?.ok()?;
    let By::Oauth2 {
        access,
        refresh: Some(_),
    } = &session.by
    else {
        return None;
    };
    let now = Utc::now();
    if now >= c.sessions.ends(session.signed_in) {
        return None;
    }
    let left = expires_in(access, now.timestamp())?;
    if left > i64::try_from(REFRESH_AHEAD.as_secs()).unwrap_or(i64::MAX) {
        return None;
    }
    Some(renewed(api, &c, &session).await)
}

/// `POST /ui/session/refresh`: the console's app refreshes its session ahead of its token's
/// expiry, sending the session's anti-forgery token; it answers as `GET /ui/session` does, with
/// the new cookies.
pub(crate) async fn refresh(
    State(api): State<Api>,
    info: Option<Extension<ConnectInfo<crate::server::Client>>>,
    headers: HeaderMap,
) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(c) = Client::of(&sign_in) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let unauthorized = |why: String| {
        challenged(
            body(StatusCode::UNAUTHORIZED, &json!({ "error": why })),
            crate::console::challenge(sign_in.auth),
        )
    };
    // A session verification would refuse whatever its token: its cookies removed, and the
    // provider not asked (an ended session's refresh token isn't used).
    let over = |why: String| with_cookies(unauthorized(why), Session::removed());
    let session = match Session::of(&headers, &c.sessions.sealer) {
        None => return unauthorized("not signed in".into()),
        Some(Err(e)) => return over(e.to_string()),
        Some(Ok(s)) => s,
    };
    if !matches!(session.by, By::Oauth2 { .. }) {
        return over("the cluster's sign-in changed since the session began".into());
    }
    let ends = c.sessions.ends(session.signed_in);
    if Utc::now() >= ends {
        return over(format!(
            "the session expired at {} ({} after sign-in)",
            ends.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            crate::auth::span(c.sessions.ttl)
        ));
    }
    if !same_origin(&headers) || !crate::unforged(&Method::POST, &headers, &session.csrf) {
        return body(StatusCode::FORBIDDEN, &json!({ "error": crate::FORGED }));
    }
    if !matches!(
        session.by,
        By::Oauth2 {
            refresh: Some(_),
            ..
        }
    ) {
        return body(
            StatusCode::CONFLICT,
            &json!({ "error": "the provider issued no refresh token: sign in again once the session ends" }),
        );
    }
    let new = match renewed(&api, &c, &session).await {
        Ok(s) => s,
        Err(e) => return unauthorized(format!("the provider's token couldn't be refreshed: {e}")),
    };
    let cookies = match session_cookies(&new, c.sessions) {
        Ok(cookies) => cookies,
        Err(e) => {
            return body(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({ "error": e.to_string() }),
            );
        }
    };
    let mut sent = headers.clone();
    sent.insert(
        header::COOKIE,
        as_sent(&cookies).parse().expect("cookies are header text"),
    );
    let verified = match api.auth.verify_from(&sent, client(info)).await {
        Ok(verified) => verified,
        Err(wait) => return too_many_json(wait),
    };
    let r = match verified {
        Ok(v) => body(StatusCode::OK, &session_json(&sign_in, v)),
        Err(Denied::Forbidden { why, name }) => body(
            StatusCode::FORBIDDEN,
            &json!({ "error": why, "name": name }),
        ),
        Err(Denied::Unauthenticated { why, .. }) => unauthorized(why),
        Err(Denied::Unavailable(why)) => {
            body(StatusCode::SERVICE_UNAVAILABLE, &json!({ "error": why }))
        }
    };
    // Set whatever the answer: a provider that rotates refresh tokens has spent the old one.
    with_cookies(r, cookies)
}

/// Where a sign-out goes on to: the provider's end-session endpoint, when it has one, with the
/// console's client, the ID token kept, and the way back to the sign-in.
pub(crate) async fn end_session(
    api: &Api,
    sign_in: &SignIn,
    headers: &HeaderMap,
) -> Option<String> {
    let c = Client::of(sign_in)?;
    let end = api.auth.provider.endpoints(c.o).await.ok()?.end_session?;
    let mut u = url::Url::parse(&end).ok()?;
    {
        let mut q = u.query_pairs_mut();
        q.append_pair("client_id", c.o.client_id.as_deref().unwrap_or_default())
            .append_pair(
                "post_logout_redirect_uri",
                &format!("{}/ui/sign-in?signed_out", c.public_url),
            );
        let hint = cookie(headers, ID_COOKIE)
            .and_then(|v| c.sessions.sealer.open("id_token", v).ok())
            .and_then(|plain| String::from_utf8(plain).ok());
        if let Some(hint) = hint {
            q.append_pair("id_token_hint", &hint);
        }
    }
    Some(u.to_string())
}

/// The sign-out's removal of the ID token's cookie.
pub(crate) fn id_removed() -> String {
    removed(ID_COOKIE, SIGN_OUT)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use data_avalanche_config::{Console, FingerprintKey, Secret};

    use super::*;
    use crate::test_support::{Issuer, api, request, serve, sessions};
    use crate::{Auth, SESSION_COOKIE};

    const HOUR: Duration = Duration::from_secs(3600);
    const PUBLIC: &str = "https://console.example";

    /// The console of the tests' oauth2 nodes: sessions of `ttl`, reached at [`PUBLIC`].
    fn console(ttl: Duration) -> Console {
        Console {
            public_url: Some(PUBLIC.into()),
            ..sessions(ttl)
        }
    }

    /// A node signing in through `idp`, as `change` sets its `oauth2`.
    async fn node(idp: &Issuer, change: impl FnOnce(&mut ApiOauth2)) -> SocketAddr {
        let mut a = idp.oauth2_api("roles");
        change(a.oauth2.as_mut().unwrap());
        serve(api(Arc::new(Auth::new(&a, &console(8 * HOUR))))).await
    }

    fn header_of<'h>(head: &'h str, name: &str) -> Vec<&'h str> {
        head.lines()
            .filter_map(|l| l.split_once(": "))
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
            .collect()
    }

    /// The `Cookie` a browser sends `path` after these `Set-Cookie`s: the ones it keeps whose
    /// path covers it.
    fn kept_for(head: &str, path: &str) -> String {
        header_of(head, "set-cookie")
            .into_iter()
            .filter(|c| !c.contains("Max-Age=0"))
            .filter(|c| {
                c.split("; ")
                    .find_map(|a| a.strip_prefix("Path="))
                    .is_some_and(|p| path.starts_with(p))
            })
            .map(|c| c.split(';').next().unwrap())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// The query of `url`, by name.
    fn query(url: &str) -> BTreeMap<String, String> {
        url::Url::parse(url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect()
    }

    /// A sign-in through the provider, as a browser makes it: the start, the provider's
    /// approval, the callback. The callback's code, head and page.
    async fn signed_in(addr: SocketAddr, next: &str) -> (u16, String, String) {
        let start = format!("/ui/oauth2/start?next={}", pages::encode(next));
        let (code, head, _) = request(addr, "GET", &start, &[], "").await;
        assert_eq!(code, 303);
        let at_provider = header_of(&head, "location")[0].to_owned();
        let left_with = kept_for(&head, CALLBACK);
        let back = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
            .get(&at_provider)
            .send()
            .await
            .unwrap();
        let back = back.headers()["location"].to_str().unwrap().to_owned();
        let path = back
            .strip_prefix(PUBLIC)
            .expect("back at the console")
            .to_owned();
        request(addr, "GET", &path, &[("Cookie", left_with.as_str())], "").await
    }

    /// Admin_Console §9: the sign-in page names the provider (`idp_name`, else the issuer's
    /// host) and goes there after 5 s, but not after a sign-out, a failed sign-in, or a choice to
    /// stay; the provider's origin may receive the sign-out form.
    #[tokio::test]
    async fn the_sign_in_page_names_the_provider_and_goes_there_unless_told_not_to() {
        let idp = Issuer::start().await;
        let addr = node(&idp, |o| o.idp_name = Some("Company SSO".into())).await;
        let (code, head, page) = request(addr, "GET", "/ui/sign-in?next=/ui/x", &[], "").await;
        assert_eq!(code, 200);
        assert!(
            page.contains(r#"<a class="btn" id="continue" href="/ui/oauth2/start?next=%2Fui%2Fx">Continue with Company SSO</a>"#),
            "{page}"
        );
        assert!(page.contains(r#"data-seconds="5""#), "{page}");
        assert!(
            page.contains(r#"<script src="/ui/assets/sign-in.js"></script>"#),
            "{page}"
        );
        let csp = header_of(&head, "content-security-policy")[0].to_owned();
        assert!(
            csp.contains(&format!("form-action 'self' {}", idp.url)),
            "{csp}"
        );
        for no_loop in ["signed_out", "failed", "stay"] {
            let (_, _, page) =
                request(addr, "GET", &format!("/ui/sign-in?{no_loop}"), &[], "").await;
            assert!(!page.contains("data-seconds"), "{no_loop}: {page}");
        }
        let addr = node(&idp, |_| {}).await;
        let host = url::Url::parse(&idp.url)
            .unwrap()
            .host_str()
            .unwrap()
            .to_owned();
        let (_, _, page) = request(addr, "GET", "/ui/sign-in", &[], "").await;
        assert!(
            page.contains(&format!("Continue with {host}</a>")),
            "{page}"
        );
        let (code, _, script) = request(addr, "GET", "/ui/assets/sign-in.js", &[], "").await;
        assert!(code == 200 && script.contains("countdown"), "{script}");
    }

    /// Admin_Console §9, config §6.3.1: authorization code with PKCE (S256), the code exchanged
    /// by the server as the public client, and a session that every page then reads.
    #[tokio::test]
    async fn a_sign_in_goes_to_the_provider_with_pkce_and_comes_back_signed_in() {
        let idp = Issuer::start().await;
        let addr = node(&idp, |_| {}).await;
        let (code, head, page) = signed_in(addr, "/ui/pipelines/orders").await;
        assert_eq!(code, 200, "{page}");
        assert!(
            page.contains(r#"<meta http-equiv="refresh" content="0;url=/ui/pipelines/orders">"#),
            "the page goes on from the console's own: {page}"
        );
        let asked = idp.sign_ins.asked.lock().unwrap()[0].clone();
        assert_eq!(asked["response_type"], "code");
        assert_eq!(asked["client_id"], "cdc-console");
        assert_eq!(
            asked["redirect_uri"],
            format!("{PUBLIC}/ui/oauth2/callback")
        );
        assert_eq!(asked["scope"], "openid profile");
        assert_eq!(asked["code_challenge_method"], "S256");
        assert_eq!(asked["state"].len(), 43);
        let (form, authorization) = idp.sign_ins.token_requests.lock().unwrap()[0].clone();
        assert_eq!(form["grant_type"], "authorization_code");
        assert_eq!(
            form["client_id"], "cdc-console",
            "a public client names itself"
        );
        assert_eq!(authorization, None);
        let set = header_of(&head, "set-cookie");
        assert!(
            set.iter()
                .any(|c| c.starts_with("da_oauth2=;") && c.contains("Max-Age=0")),
            "the sign-in's state is spent: {set:?}"
        );
        assert!(
            set.iter()
                .any(|c| c.starts_with("da_id=") && c.contains("Path=/ui/sign-out")),
            "{set:?}"
        );
        let cookie = kept_for(&head, "/ui/session");
        assert!(
            !cookie.contains("da_id"),
            "the ID token goes only to the sign-out"
        );
        let (code, _, text) = request(
            addr,
            "GET",
            "/ui/session",
            &[("Cookie", cookie.as_str())],
            "",
        )
        .await;
        assert_eq!(code, 200, "{text}");
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            (&v["name"], &v["role"], &v["auth"]),
            (&json!("alice.smith"), &json!("operator"), &json!("oauth2"))
        );
    }

    /// A callback that isn't the sign-in's own (no state cookie, another state) fails, and its
    /// page doesn't go back to the provider by itself; one with another state, a provider's
    /// error included, says nothing of it and leaves the sign-in in progress. The provider's
    /// refusal of this sign-in is shown, and spends it.
    #[tokio::test]
    async fn a_callback_that_isnt_the_sign_in_s_own_fails_without_a_loop() {
        let idp = Issuer::start().await;
        let addr = node(&idp, |_| {}).await;
        let (code, _, page) =
            request(addr, "GET", "/ui/oauth2/callback?code=c&state=s", &[], "").await;
        assert_eq!(code, 400);
        assert!(page.contains("began in another browser"), "{page}");
        assert!(!page.contains("data-seconds"), "no countdown: {page}");
        let (_, head, _) = request(addr, "GET", "/ui/oauth2/start?next=/ui/x", &[], "").await;
        let state = query(header_of(&head, "location")[0])["state"].clone();
        let left_with = kept_for(&head, CALLBACK);
        let with = [("Cookie", left_with.as_str())];
        let spent = |head: &str| {
            header_of(head, "set-cookie")
                .iter()
                .any(|c| c.starts_with("da_oauth2=;"))
        };
        let refused = "/ui/oauth2/callback?error=access_denied&error_description=The+user+said+no";
        for unbound in [
            "/ui/oauth2/callback?code=c&state=another".to_owned(),
            refused.to_owned(),
            format!("{refused}&state=another"),
        ] {
            let (code, head, page) = request(addr, "GET", &unbound, &with, "").await;
            assert_eq!(code, 400, "{unbound}");
            assert!(page.contains("isn&#39;t this sign-in&#39;s"), "{page}");
            assert!(!page.contains("The user said no"), "{page}");
            assert!(!spent(&head), "the sign-in stays in progress: {head}");
        }
        let (code, head, page) =
            request(addr, "GET", &format!("{refused}&state={state}"), &with, "").await;
        assert_eq!(code, 400);
        assert!(
            page.contains("refused the sign-in: The user said no"),
            "{page}"
        );
        assert!(!page.contains("data-seconds"), "{page}");
        assert!(spent(&head), "{head}");
        assert!(
            idp.sign_ins.token_requests.lock().unwrap().is_empty(),
            "nothing exchanged"
        );
    }

    /// Mockup `02-sign-in.html`: a token whose roles map to nothing is "Forbidden" at sign-in,
    /// naming the roles it carried; no session starts.
    #[tokio::test]
    async fn a_token_whose_roles_map_to_nothing_is_forbidden_at_sign_in() {
        let idp = Issuer::start().await;
        *idp.sign_ins.roles.lock().unwrap() = json!(["cdc-ops", "reporting"]);
        let addr = node(&idp, |_| {}).await;
        let (code, head, page) = signed_in(addr, "/ui/").await;
        assert_eq!(code, 403);
        assert!(
            page.contains("You are <b>alice.smith</b>, and the token&#39;s roles (cdc-ops, reporting) name no role this node maps."),
            "{page}"
        );
        assert!(
            !header_of(&head, "set-cookie")
                .iter()
                .any(|c| c.starts_with(&format!("{SESSION_COOKIE}=")))
        );
    }

    /// A confidential client authenticates at the token endpoint with HTTP Basic.
    #[tokio::test]
    async fn a_confidential_client_authenticates_with_its_secret() {
        let idp = Issuer::start().await;
        let secret = Secret::new("s3cr:t", &FingerprintKey::from_bytes([1; 32]));
        let addr = node(&idp, |o| o.client_secret = Some(secret)).await;
        assert_eq!(signed_in(addr, "/ui/").await.0, 200);
        let (form, authorization) = idp.sign_ins.token_requests.lock().unwrap()[0].clone();
        assert!(!form.contains_key("client_secret") && !form.contains_key("client_id"));
        assert_eq!(
            authorization.as_deref(),
            Some(format!("Basic {}", STANDARD.encode("cdc-console:s3cr%3At")).as_str())
        );
    }

    /// A session about to expire is refreshed by the page asked for, once however many pages ask
    /// at once; it keeps when it signed in, so it ends when it would have.
    #[tokio::test]
    async fn a_session_whose_token_is_about_to_expire_is_refreshed_once() {
        let idp = Issuer::start().await;
        let addr = node(&idp, |_| {}).await;
        let mut claims = idp.claims(json!(["operator"]));
        claims["exp"] = json!(Utc::now().timestamp() + 30);
        let old = Session::new(
            By::Oauth2 {
                access: idp.sign("k1", &claims),
                refresh: Some("refresh-0".into()),
            },
            Utc::now().timestamp() - 600,
        );
        let cookie = crate::test_support::session_cookie(&old);
        let page = |cookie: String| async move {
            request(addr, "GET", "/ui/", &[("Cookie", cookie.as_str())], "").await
        };
        let (a, b) = tokio::join!(page(cookie.clone()), page(cookie));
        for (code, head, body) in [&a, &b] {
            assert_eq!(*code, 200, "{body}");
            let renewed = kept_for(head, "/ui/");
            let sealer = crate::session::Sealer::new(crate::test_support::SESSION_KEY);
            let mut h = HeaderMap::new();
            h.insert(header::COOKIE, renewed.parse().unwrap());
            let new = Session::of(&h, &sealer).unwrap().unwrap();
            assert_eq!((new.signed_in, &new.csrf), (old.signed_in, &old.csrf));
            assert_ne!(new.by, old.by, "a new token");
        }
        assert_eq!(idp.sign_ins.refreshes.load(Ordering::SeqCst), 1);
    }

    /// A token expired whose refresh the provider refuses ends the session, saying so.
    #[tokio::test]
    async fn a_refresh_the_provider_refuses_ends_the_session_saying_why() {
        let idp = Issuer::start().await;
        idp.sign_ins.refusing.store(true, Ordering::SeqCst);
        let addr = node(&idp, |_| {}).await;
        let mut claims = idp.claims(json!(["operator"]));
        claims["exp"] = json!(Utc::now().timestamp() - 120);
        let old = Session::new(
            By::Oauth2 {
                access: idp.sign("k1", &claims),
                refresh: Some("refresh-0".into()),
            },
            Utc::now().timestamp() - 600,
        );
        let cookie = crate::test_support::session_cookie(&old);
        let (code, _, page) =
            request(addr, "GET", "/ui/x", &[("Cookie", cookie.as_str())], "").await;
        assert_eq!(code, 401);
        assert!(
            page.contains("Your session has ended")
                && page.contains("The provider&#39;s token expired and couldn&#39;t be refreshed: the provider answered 400: the refresh token was revoked."),
            "{page}"
        );
    }

    /// The console's app refreshes its session with the session's anti-forgery token, from its
    /// own page; a session without a refresh token can't be refreshed.
    #[tokio::test]
    async fn the_app_refreshes_its_session_with_its_anti_forgery_token() {
        let idp = Issuer::start().await;
        let addr = node(&idp, |_| {}).await;
        let (_, head, _) = signed_in(addr, "/ui/").await;
        let cookie = kept_for(&head, "/ui/session");
        let (_, _, text) = request(
            addr,
            "GET",
            "/ui/session",
            &[("Cookie", cookie.as_str())],
            "",
        )
        .await;
        let csrf = serde_json::from_str::<Value>(&text).unwrap()["csrf"]
            .as_str()
            .unwrap()
            .to_owned();
        let (code, _, _) = request(
            addr,
            "POST",
            "/ui/session/refresh",
            &[("Cookie", cookie.as_str())],
            "",
        )
        .await;
        assert_eq!(code, 403, "without the token");
        let with = [
            ("Cookie", cookie.as_str()),
            (crate::CSRF_HEADER, csrf.as_str()),
        ];
        let (code, head, text) = request(addr, "POST", "/ui/session/refresh", &with, "").await;
        assert_eq!(code, 200, "{text}");
        assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["csrf"], csrf);
        assert!(kept_for(&head, "/ui/").starts_with(&format!("{SESSION_COOKIE}=")));
        assert_eq!(idp.sign_ins.refreshes.load(Ordering::SeqCst), 1);
        let none = Session::new(
            By::Oauth2 {
                access: idp.token(json!(["operator"])),
                refresh: None,
            },
            Utc::now().timestamp(),
        );
        let cookie = crate::test_support::session_cookie(&none);
        let with = [
            ("Cookie", cookie.as_str()),
            (crate::CSRF_HEADER, none.csrf.as_str()),
        ];
        assert_eq!(
            request(addr, "POST", "/ui/session/refresh", &with, "")
                .await
                .0,
            409
        );
    }

    /// Admin_Console §9: the sign-out clears the session and signs out at the provider, with the
    /// ID token kept and the way back to the sign-in.
    #[tokio::test]
    async fn the_sign_out_goes_on_to_the_provider() {
        let idp = Issuer::start().await;
        let addr = node(&idp, |_| {}).await;
        let (_, head, _) = signed_in(addr, "/ui/").await;
        let cookie = kept_for(&head, SIGN_OUT);
        assert!(cookie.contains("da_id="), "{cookie}");
        let (code, head, _) =
            request(addr, "POST", SIGN_OUT, &[("Cookie", cookie.as_str())], "").await;
        assert_eq!(code, 303);
        let to = header_of(&head, "location")[0];
        assert!(to.starts_with(&format!("{}/end-session?", idp.url)), "{to}");
        let q = query(to);
        assert_eq!(q["client_id"], "cdc-console");
        assert_eq!(
            q["post_logout_redirect_uri"],
            format!("{PUBLIC}/ui/sign-in?signed_out")
        );
        assert_eq!(q["id_token_hint"].split('.').count(), 3, "a JWT");
        let removed: Vec<&str> = header_of(&head, "set-cookie")
            .into_iter()
            .filter(|c| c.contains("Max-Age=0"))
            .map(|c| c.split('=').next().unwrap())
            .collect();
        let expected: Vec<String> = (0..crate::MAX_CHUNKS)
            .map(|i| match i {
                0 => SESSION_COOKIE.to_owned(),
                i => format!("{SESSION_COOKIE}.{i}"),
            })
            .chain([ID_COOKIE.to_owned()])
            .collect();
        assert_eq!(removed, expected);
    }

    /// The session cookies' removals in `head`, by name.
    fn removals(head: &str) -> Vec<&str> {
        header_of(head, "set-cookie")
            .into_iter()
            .filter(|c| c.contains("Max-Age=0"))
            .map(|c| c.split('=').next().unwrap())
            .collect()
    }

    /// The session cookies' names, in order.
    fn session_names() -> Vec<String> {
        (0..crate::MAX_CHUNKS)
            .map(|i| match i {
                0 => SESSION_COOKIE.to_owned(),
                i => format!("{SESSION_COOKIE}.{i}"),
            })
            .collect()
    }

    /// `POST /ui/session/refresh` with `session`'s cookie and anti-forgery token.
    async fn refresh_of(addr: SocketAddr, session: &Session) -> (u16, String, String) {
        let cookie = crate::test_support::session_cookie(session);
        let with = [
            ("Cookie", cookie.as_str()),
            (crate::CSRF_HEADER, session.csrf.as_str()),
        ];
        request(addr, "POST", "/ui/session/refresh", &with, "").await
    }

    /// An oauth2 session with `refresh`, signed in at `signed_in`, whose token expires at `exp`.
    fn oauth2_session(idp: &Issuer, refresh: &str, signed_in: i64, exp: i64) -> Session {
        let mut claims = idp.claims(json!(["operator"]));
        claims["exp"] = json!(exp);
        Session::new(
            By::Oauth2 {
                access: idp.sign("k1", &claims),
                refresh: Some(refresh.into()),
            },
            signed_in,
        )
    }

    /// A session that ended (still kept by the browser a day after) isn't refreshed: the
    /// provider isn't asked, and its cookies are removed; nor is one that doesn't open.
    #[tokio::test]
    async fn an_ended_session_isn_t_refreshed() {
        let idp = Issuer::start().await;
        let addr = node(&idp, |_| {}).await;
        let now = Utc::now().timestamp();
        let ended = oauth2_session(&idp, "refresh-0", now - 9 * 3600, now - 60);
        let (code, head, text) = refresh_of(addr, &ended).await;
        assert_eq!(code, 401, "{text}");
        assert!(text.contains("the session expired at"), "{text}");
        assert_eq!(header_of(&head, "www-authenticate"), [crate::auth::SESSION]);
        assert_eq!(removals(&head), session_names());
        let garbled = format!("{SESSION_COOKIE}=garbled");
        let with = [
            ("Cookie", garbled.as_str()),
            (crate::CSRF_HEADER, "anything"),
        ];
        let (code, head, _) = request(addr, "POST", "/ui/session/refresh", &with, "").await;
        assert_eq!(code, 401);
        assert_eq!(removals(&head), session_names());
        assert!(
            idp.sign_ins.token_requests.lock().unwrap().is_empty(),
            "the provider wasn't asked"
        );
    }

    /// Keys that can't be fetched make the provider's token uncheckable, not wrong: the
    /// callback's sign-in page and the app's refresh say 503.
    #[tokio::test]
    async fn a_token_that_can_t_be_checked_is_unavailable_not_unauthorized() {
        let idp = Issuer::start().await;
        idp.set_down(true);
        let addr = node(&idp, |_| {}).await;
        let (code, _, page) = signed_in(addr, "/ui/").await;
        assert_eq!(code, 503, "{page}");
        assert!(page.contains("can&#39;t be checked now"), "{page}");
        assert!(!page.contains("data-seconds"), "{page}");
        let addr = node(&idp, |_| {}).await;
        let now = Utc::now().timestamp();
        let session = oauth2_session(&idp, "refresh-0", now, now + 30);
        let (code, _, text) = refresh_of(addr, &session).await;
        assert_eq!(code, 503, "{text}");
        let v: Value = serde_json::from_str(&text).unwrap();
        assert!(
            v["error"].as_str().unwrap().contains("can't be fetched"),
            "{v}"
        );
    }

    /// Tokens too long for the session's cookies are the node's failure: a page says the
    /// session ended and why, the app's refresh answers 500.
    #[tokio::test]
    async fn tokens_too_long_for_the_cookies_are_said_not_dropped() {
        let idp = Issuer::start().await;
        let roles: Vec<String> = (0..2000).map(|i| format!("role-{i:05}")).collect();
        *idp.sign_ins.roles.lock().unwrap() = json!(roles);
        let addr = node(&idp, |_| {}).await;
        let now = Utc::now().timestamp();
        let old = oauth2_session(&idp, "refresh-page", now - 600, now + 30);
        let cookie = crate::test_support::session_cookie(&old);
        let (code, _, page) =
            request(addr, "GET", "/ui/x", &[("Cookie", cookie.as_str())], "").await;
        assert_eq!(code, 401);
        assert!(
            page.contains("Your session has ended")
                && page.contains("The refreshed session can&#39;t be kept: the session needs"),
            "{page}"
        );
        let old = oauth2_session(&idp, "refresh-app", now - 600, now + 30);
        let (code, _, text) = refresh_of(addr, &old).await;
        assert_eq!(code, 500, "{text}");
        assert!(text.contains("too long"), "{text}");
    }

    /// Two refreshes with one token make one request, and leave nothing in flight behind them.
    #[tokio::test]
    async fn the_refreshes_in_flight_are_forgotten_once_done() {
        let idp = Issuer::start().await;
        let o = idp.oauth2_api("roles").oauth2.unwrap();
        let provider = Provider::default();
        let e = provider.endpoints(&o).await.unwrap();
        let (a, b) = tokio::join!(
            provider.refresh(&o, &e, "refresh-0"),
            provider.refresh(&o, &e, "refresh-0")
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(idp.sign_ins.refreshes.load(Ordering::SeqCst), 1);
        assert!(provider.refreshing.lock().unwrap().is_empty());
    }

    /// One session's refresh, however long the provider takes, doesn't hold up another's.
    #[tokio::test]
    async fn different_sessions_refresh_at_once() {
        let idp = Issuer::start().await;
        *idp.sign_ins.held.lock().unwrap() = Some("refresh-a".into());
        let addr = node(&idp, |_| {}).await;
        let now = Utc::now().timestamp();
        let page = |session: Session| {
            let cookie = crate::test_support::session_cookie(&session);
            async move { request(addr, "GET", "/ui/", &[("Cookie", cookie.as_str())], "").await }
        };
        let a = tokio::spawn(page(oauth2_session(&idp, "refresh-a", now, now + 30)));
        while idp.sign_ins.token_requests.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let b = tokio::time::timeout(
            Duration::from_secs(5),
            page(oauth2_session(&idp, "refresh-b", now, now + 30)),
        )
        .await
        .expect("b's refresh doesn't wait for a's");
        assert_eq!(b.0, 200, "{}", b.2);
        assert!(!a.is_finished(), "a's refresh is still held");
        idp.sign_ins.release.notify_one();
        let a = a.await.unwrap();
        assert_eq!(a.0, 200, "{}", a.2);
        assert_eq!(idp.sign_ins.refreshes.load(Ordering::SeqCst), 2);
    }

    /// The discovery document must name the configured issuer: one that names another fails the
    /// sign-in without going to the provider, and a reload that changes the issuer checks again.
    #[tokio::test]
    async fn the_provider_must_be_the_configured_issuer() {
        let idp = Issuer::start().await;
        let mut o = idp.oauth2_api("roles").oauth2.unwrap();
        o.discovery_url = Some(format!("{}/.well-known/openid-configuration", idp.url));
        let provider = Provider::default();
        assert!(provider.endpoints(&o).await.is_ok());
        assert!(provider.cached(&o).is_some());
        o.issuer = "https://elsewhere.example".into();
        assert!(provider.cached(&o).is_none(), "another issuer's");
        let e = provider.endpoints(&o).await.unwrap_err();
        assert!(e.contains("not the configured"), "{e}");
        *idp.sign_ins.discovery.lock().unwrap() = json!({ "issuer": "https://elsewhere.example" });
        let addr = node(&idp, |_| {}).await;
        let (code, head, page) = request(addr, "GET", "/ui/oauth2/start?next=/ui/x", &[], "").await;
        assert_eq!(code, 503, "{page}");
        assert!(header_of(&head, "location").is_empty(), "{head}");
        assert!(page.contains("not the configured"), "{page}");
        assert!(idp.sign_ins.asked.lock().unwrap().is_empty());
    }

    /// An authorization endpoint that isn't a URL fails the sign-in: no redirect to it as is.
    #[tokio::test]
    async fn an_authorization_endpoint_that_isn_t_a_url_fails_the_sign_in() {
        let idp = Issuer::start().await;
        *idp.sign_ins.discovery.lock().unwrap() = json!({ "authorization_endpoint": "not a url" });
        let addr = node(&idp, |_| {}).await;
        let (code, head, page) = request(addr, "GET", "/ui/oauth2/start?next=/ui/x", &[], "").await;
        assert_eq!(code, 503, "{page}");
        assert!(header_of(&head, "location").is_empty(), "{head}");
        assert!(page.contains("isn&#39;t a URL"), "{page}");
    }

    /// A provider that takes the client's secret only in the form (`client_secret_post`) gets
    /// it there, with the client's ID, and no HTTP Basic.
    #[tokio::test]
    async fn a_confidential_client_posts_its_secret_where_the_provider_takes_only_that() {
        let idp = Issuer::start().await;
        *idp.sign_ins.discovery.lock().unwrap() =
            json!({ "token_endpoint_auth_methods_supported": ["client_secret_post"] });
        let secret = Secret::new("s3cr:t", &FingerprintKey::from_bytes([1; 32]));
        let addr = node(&idp, |o| o.client_secret = Some(secret)).await;
        assert_eq!(signed_in(addr, "/ui/").await.0, 200);
        let (form, authorization) = idp.sign_ins.token_requests.lock().unwrap()[0].clone();
        assert_eq!(
            (form["client_id"].as_str(), form["client_secret"].as_str()),
            ("cdc-console", "s3cr:t")
        );
        assert_eq!(authorization, None);
    }

    /// RFC 7636 §4.2: `S256` is the URL-safe base64 of the verifier's SHA-256, unpadded; the
    /// digest of `abc` is FIPS 180-2's first example.
    #[test]
    fn a_pkce_challenge_is_the_verifier_s_sha_256() {
        let fips = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert_eq!(
            challenge("abc"),
            "ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0"
        );
        let digest = URL_SAFE_NO_PAD.decode(challenge("abc")).unwrap();
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, fips);
    }
}
