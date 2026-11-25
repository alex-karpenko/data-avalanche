//! The console's own routes under `/ui/` (Admin_Console §9, §10): the sign-in and the sign-out,
//! the session as the console's app reads it, and the pages a browser asks for. They are served
//! while `globals.console.enabled`, read at each request, so a reload turns them on or off: `404`
//! otherwise. Every answer forbids framing and runs no inline script or style.
//!
//! A page asked for without credentials goes to the sign-in (`basic`, `oauth2`); with a session
//! that no longer verifies it is "Your session has ended" (401), saying why; with `jwt`, "No valid
//! token" (401); with a role the node doesn't map, "Forbidden" (403).

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Router};
use chrono::{SecondsFormat, Utc};
use data_avalanche_config::ApiAuth;
use serde_json::{Value, json};

use crate::auth::{BEARER, Denied, SESSION, Sessions, SignIn, Verified};
use crate::pages::{self, Frame};
use crate::session::{By, KEPT_AFTER, Session, TooLong, as_sent, sealed_value};
use crate::{Api, body, oauth2};

/// Failed basic sign-ins in a row from one address before it waits.
pub const SIGN_IN_ATTEMPTS: u32 = 5;
/// How long an address waits after [`SIGN_IN_ATTEMPTS`] failed sign-ins.
pub const SIGN_IN_WAIT: Duration = Duration::from_secs(60);
/// At most this many addresses' failures are kept; past it, those not waiting are forgotten.
const ADDRESSES_KEPT: usize = 10_000;
/// A full table is pruned at most this often.
const PRUNE_EVERY: Duration = Duration::from_secs(1);

/// The Content-Security-Policy of every `/ui/` answer: the console's own scripts, styles and
/// connections, nothing inline, no framing. `form-action` adds the provider's end-session
/// origin, where the sign-out goes on with `oauth2`.
fn policy(form_action: &str) -> String {
    format!(
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'{form_action}; frame-ancestors 'none'; base-uri 'none'"
    )
}

/// The console's routes.
pub(crate) fn routes(api: &Api) -> Router<Api> {
    Router::new()
        .route("/ui", get(|| async { see_other("/ui/") }))
        .route("/ui/", get(page))
        .route("/ui/{*page}", get(page))
        .route("/ui/sign-in", get(sign_in_page).post(sign_in))
        .route("/ui/sign-out", post(sign_out))
        .route("/ui/session", get(session))
        .route("/ui/session/refresh", post(oauth2::refresh))
        .route("/ui/oauth2/start", get(oauth2::start))
        .route("/ui/oauth2/callback", get(oauth2::callback))
        .route("/ui/assets/{file}", get(asset))
        .layer(from_fn_with_state(api.clone(), guard))
}

/// Serves `/ui/` while the console is enabled; every answer, the refusal too, has the security
/// headers.
async fn guard(State(api): State<Api>, req: Request, next: Next) -> Response {
    let sign_in = api.auth.sign_in();
    let mut r = if sign_in.is_none() {
        (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "The console is off on this cluster: globals.console.enabled.\n",
        )
            .into_response()
    } else {
        next.run(req).await
    };
    // Read after the answer: a sign-in or a sign-out may just have discovered the provider. A
    // page (whose forms the policy governs) discovers it, when no request has yet.
    let page = r
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|t| t.as_bytes().starts_with(b"text/html"));
    let form_action = match &sign_in {
        Some(s) => oauth2::form_action(&api, s, page).await,
        None => String::new(),
    };
    let h = r.headers_mut();
    let csp = policy(&form_action);
    if let Ok(v) = HeaderValue::from_str(&csp) {
        h.insert(header::CONTENT_SECURITY_POLICY, v);
    }
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.entry(header::CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-store"));
    r
}

/// `303 See Other` to `to`.
pub(crate) fn see_other(to: &str) -> Response {
    (StatusCode::SEE_OTHER, [(header::LOCATION, to)]).into_response()
}

/// An HTML page with `code`.
pub(crate) fn html(code: StatusCode, page: String) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        page,
    )
        .into_response()
}

/// `r` with a `Set-Cookie` for each of `cookies` (base64 and ASCII, so always a header value).
pub(crate) fn with_cookies(mut r: Response, cookies: impl IntoIterator<Item = String>) -> Response {
    for c in cookies {
        r.headers_mut().append(
            header::SET_COOKIE,
            HeaderValue::from_str(&c).expect("a cookie is ASCII"),
        );
    }
    r
}

/// `r` with a `WWW-Authenticate` challenge.
pub(crate) fn challenged(mut r: Response, challenge: &'static str) -> Response {
    r.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static(challenge),
    );
    r
}

/// The seconds of `wait`, rounded up: what `Retry-After` says.
pub(crate) fn retry_secs(wait: Duration) -> u64 {
    wait.as_secs() + u64::from(wait.subsec_nanos() > 0)
}

/// The challenge of a `/ui/` 401: the session's, or the token's under `jwt`; never `Basic`, whose
/// popup the sign-in form replaces.
pub(crate) fn challenge(auth: ApiAuth) -> &'static str {
    if auth == ApiAuth::Jwt {
        BEARER
    } else {
        SESSION
    }
}

/// `r` with a `Retry-After` of `wait`.
pub(crate) fn retry_after(mut r: Response, wait: Duration) -> Response {
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry_secs(wait)));
    r
}

/// 429 for a JSON answer: the address waits `wait`.
pub(crate) fn too_many_json(wait: Duration) -> Response {
    let secs = retry_secs(wait);
    let mut r = body(
        StatusCode::TOO_MANY_REQUESTS,
        &json!({ "error": format!("too many failed sign-ins from this address; try again in {secs} s") }),
    );
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(secs));
    r
}

/// 429 for an address that waits: `Retry-After` says how long.
fn too_many(page: String, wait: Duration) -> Response {
    let mut r = html(StatusCode::TOO_MANY_REQUESTS, page);
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry_secs(wait)));
    r
}

/// `next` when it is a page of the console's own: a path under `/ui/`, never another site's.
pub(crate) fn safe_next(next: Option<&str>) -> String {
    match next {
        Some(n)
            if n.starts_with("/ui/")
                && !n.contains("//")
                && !n.contains('\\')
                && !n.chars().any(char::is_control)
                && !climbs(n) =>
        {
            n.to_owned()
        }
        _ => "/ui/".to_owned(),
    }
}

/// Whether the path of `next` has a `.` or `..` segment, spelled out or percent-encoded: a
/// browser resolves it out of `/ui/`.
fn climbs(next: &str) -> bool {
    let path = next.split(['?', '#']).next().unwrap_or_default();
    path.to_ascii_lowercase()
        .replace("%2e", ".")
        .split('/')
        .any(|segment| segment == "." || segment == "..")
}

/// The query's parameters, the last of each name taken.
pub(crate) fn params(raw: Option<&str>) -> BTreeMap<String, String> {
    url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
        .into_owned()
        .collect()
}

/// What every page shows around its card, for this node and these settings.
pub(crate) fn frame(api: &Api, sign_in: &SignIn) -> Frame {
    Frame {
        node: api
            .node
            .get()
            .map_or_else(String::new, |n| n.members.borrow().me.clone()),
        open: sign_in.auth == ApiAuth::None,
    }
}

/// A page under `/ui/` (the app's, from M15): the landing page for a caller who verifies, the
/// sign-in for one who brought nothing, else why not. An `oauth2` session whose token is about
/// to expire is refreshed first, its new cookies set with the page; once the token expired, a
/// provider that refuses the refresh ends the session (401), one that can't be reached answers
/// "try again" (503, `Retry-After`), the session kept.
async fn page(
    State(api): State<Api>,
    info: Option<Extension<ConnectInfo<crate::server::Client>>>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let frame = frame(&api, &sign_in);
    let here = safe_next(uri.path_and_query().map(|p| p.as_str()));
    let mut headers = headers;
    let mut renewed = Vec::new();
    let mut not_refreshed = None;
    match oauth2::due(&api, &sign_in, &headers).await {
        Some(Ok(new)) => match sign_in.sessions.as_ref().map(|s| session_cookies(&new, s)) {
            Some(Ok(cookies)) => {
                headers.insert(
                    header::COOKIE,
                    as_sent(&cookies).parse().expect("cookies are header text"),
                );
                // Set whatever the page then says: a provider that rotates refresh tokens has
                // spent the old one.
                renewed = cookies;
            }
            Some(Err(e)) => {
                let why = format!("the refreshed session can't be kept: {e}");
                return challenged(
                    html(StatusCode::UNAUTHORIZED, pages::ended(&frame, &why, &here)),
                    challenge(sign_in.auth),
                );
            }
            // `due` refreshes only with the console's sessions on.
            None => {}
        },
        Some(Err(e)) => not_refreshed = Some(e),
        None => {}
    }
    let verified = match api.auth.verify_from(&headers, client(info)).await {
        Ok(verified) => verified,
        Err(wait) => return too_many(pages::waiting(&frame, &here, retry_secs(wait)), wait),
    };
    let r = match verified {
        Ok(Verified { principal, session }) => html(
            StatusCode::OK,
            pages::signed_in(
                &frame,
                &principal.name,
                &principal.role.to_string(),
                session.is_some(),
            ),
        ),
        // The provider is down, not the session over: try again in a moment.
        Err(Denied::Unauthenticated { .. })
            if let Some(oauth2::Failed::Unreachable(e)) = &not_refreshed =>
        {
            let why = format!("the provider's token couldn't be refreshed: {e}");
            retry_after(
                html(
                    StatusCode::SERVICE_UNAVAILABLE,
                    pages::unavailable(&frame, &why),
                ),
                oauth2::REFRESH_RETRY_AFTER,
            )
        }
        Err(Denied::Unauthenticated { .. })
            if let Some(oauth2::Failed::Refused(e)) = &not_refreshed =>
        {
            let why = format!("the provider's token expired and couldn't be refreshed: {e}");
            challenged(
                html(StatusCode::UNAUTHORIZED, pages::ended(&frame, &why, &here)),
                challenge(sign_in.auth),
            )
        }
        Err(denied) => refused(&frame, &sign_in, &headers, &here, denied),
    };
    with_cookies(r, renewed)
}

/// Why a page isn't shown: the sign-in for one who brought no credentials (`basic`, `oauth2`),
/// else its page.
fn refused(
    frame: &Frame,
    sign_in: &SignIn,
    headers: &HeaderMap,
    here: &str,
    denied: Denied,
) -> Response {
    match denied {
        Denied::Unauthenticated { why, .. } => match sign_in.auth {
            ApiAuth::Jwt => challenged(
                html(StatusCode::UNAUTHORIZED, pages::no_token(frame, &why)),
                challenge(sign_in.auth),
            ),
            _ if sealed_value(headers).is_none()
                && !headers.contains_key(header::AUTHORIZATION) =>
            {
                see_other(&format!("/ui/sign-in?next={}", pages::encode(here)))
            }
            _ => challenged(
                html(StatusCode::UNAUTHORIZED, pages::ended(frame, &why, here)),
                challenge(sign_in.auth),
            ),
        },
        Denied::Forbidden { name, why } => html(
            StatusCode::FORBIDDEN,
            pages::forbidden(frame, &name, &why, sealed_value(headers).is_some()),
        ),
        Denied::Unavailable(why) => html(
            StatusCode::SERVICE_UNAVAILABLE,
            pages::unavailable(frame, &why),
        ),
    }
}

/// `GET /ui/sign-in`: the form (`basic`), or the way to the provider (`oauth2`); a caller
/// already signed in goes on to `next`.
async fn sign_in_page(
    State(api): State<Api>,
    info: Option<Extension<ConnectInfo<crate::server::Client>>>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let q = params(raw.as_deref());
    let next = safe_next(q.get("next").map(String::as_str));
    match sign_in.auth {
        ApiAuth::None | ApiAuth::Jwt => return see_other(&next),
        ApiAuth::Basic | ApiAuth::Oauth2 => {}
    }
    let frame = frame(&api, &sign_in);
    match api.auth.verify_from(&headers, client(info)).await {
        Err(wait) => return too_many(pages::waiting(&frame, &next, retry_secs(wait)), wait),
        Ok(Ok(Verified {
            session: Some(_), ..
        })) => return see_other(&next),
        Ok(_) => {}
    }
    match sign_in.auth {
        ApiAuth::Basic => {
            let info = q.contains_key("signed_out").then_some("You signed out.");
            html(
                StatusCode::OK,
                pages::basic_sign_in(&frame, &next, "", None, info),
            )
        }
        _ => oauth2::sign_in_page(&api, &sign_in, &q, &next),
    }
}

/// Whether a form posted to `/ui/` came from the console's own pages, as the browser says
/// (`Sec-Fetch-Site`, else `Origin` against `Host`); a client that says neither (`curl`) is
/// taken at its word.
pub(crate) fn same_origin(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return site == "same-origin" || site == "none";
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok());
    let origin = origin.to_str().ok().and_then(|o| url::Url::parse(o).ok());
    match (origin, host) {
        (Some(o), Some(host)) => {
            let authority = match o.port() {
                Some(port) => format!("{}:{port}", o.host_str().unwrap_or_default()),
                None => o.host_str().unwrap_or_default().to_owned(),
            };
            authority.eq_ignore_ascii_case(host)
        }
        _ => false,
    }
}

/// The client's address, as the connection says (a proxy's own, behind one).
pub(crate) fn client(
    info: Option<Extension<ConnectInfo<crate::server::Client>>>,
) -> Option<IpAddr> {
    info.map(|Extension(ConnectInfo(c))| c.0.ip())
}

/// `POST /ui/sign-in` (`basic`): the user and password checked as the API checks them; right,
/// a session and on to `next`; wrong, the form again, with how many attempts are left before the
/// address waits [`SIGN_IN_WAIT`].
async fn sign_in(
    State(api): State<Api>,
    info: Option<Extension<ConnectInfo<crate::server::Client>>>,
    headers: HeaderMap,
    form: Bytes,
) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let frame = frame(&api, &sign_in);
    let (ApiAuth::Basic, Some(credentials), Some(sessions)) =
        (sign_in.auth, &sign_in.credentials, &sign_in.sessions)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !same_origin(&headers) {
        return html(StatusCode::FORBIDDEN, pages::cross_site(&frame));
    }
    let form: BTreeMap<String, String> = url::form_urlencoded::parse(&form).into_owned().collect();
    let field = |name: &str| form.get(name).map_or("", String::as_str);
    let next = safe_next(form.get("next").map(String::as_str));
    let from = client(info);
    // Without an address (never, behind the listener) nothing is counted.
    let left = match from.map(|from| api.auth.attempts.attempt(from, Instant::now())) {
        Some(Ok(left)) => left,
        Some(Err(wait)) => {
            return too_many(pages::waiting(&frame, &next, retry_secs(wait)), wait);
        }
        None => SIGN_IN_ATTEMPTS - 1,
    };
    if !crate::auth::basic_matches(credentials, field("user"), field("password")) {
        let error = if left == 0 {
            format!(
                "Wrong user or password. Wait {} before trying again.",
                crate::auth::span(SIGN_IN_WAIT)
            )
        } else {
            format!(
                "Wrong user or password. {left} attempt{s} left before a {} wait.",
                crate::auth::span(SIGN_IN_WAIT),
                s = if left == 1 { "" } else { "s" },
            )
        };
        return challenged(
            html(
                StatusCode::UNAUTHORIZED,
                pages::basic_sign_in(&frame, &next, field("user"), Some(&error), None),
            ),
            SESSION,
        );
    }
    if let Some(from) = from {
        api.auth.attempts.succeeded(from);
    }
    let session = Session::new(
        By::Basic {
            user: field("user").to_owned(),
            fingerprint: sessions.sealer.fingerprint(field("password")),
        },
        Utc::now().timestamp(),
    );
    match session_cookies(&session, sessions) {
        Ok(cookies) => with_cookies(see_other(&next), cookies),
        Err(e) => body(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({ "error": e.to_string() }),
        ),
    }
}

/// The `Set-Cookie` values of `session`, kept until a day after it ends.
///
/// # Errors
/// [`TooLong`]: it needs more cookies than a browser is sent.
pub(crate) fn session_cookies(
    session: &Session,
    sessions: &Sessions,
) -> Result<Vec<String>, TooLong> {
    // In whole seconds: a session starts on one.
    let left = sessions.ends(session.signed_in).timestamp() - Utc::now().timestamp();
    let left = u64::try_from(left).unwrap_or_default();
    session.cookies(&sessions.sealer, left + KEPT_AFTER.as_secs())
}

/// `POST /ui/sign-out`: the session's cookies removed, then the sign-in, or, with `oauth2`, the
/// provider's end-session endpoint when it has one. Only from the console's own pages: a
/// sign-out another site posts is refused.
async fn sign_out(State(api): State<Api>, headers: HeaderMap) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !same_origin(&headers) {
        return html(
            StatusCode::FORBIDDEN,
            pages::cross_site(&frame(&api, &sign_in)),
        );
    }
    let to = oauth2::end_session(&api, &sign_in, &headers).await;
    let mut cookies = Session::removed();
    if sign_in.auth == ApiAuth::Oauth2 {
        cookies.push(oauth2::id_removed());
    }
    with_cookies(
        see_other(to.as_deref().unwrap_or("/ui/sign-in?signed_out")),
        cookies,
    )
}

/// `GET /ui/session`: who the caller is, as the console's app reads it: name, role, how they
/// signed in, until when, and the session's anti-forgery token.
async fn session(
    State(api): State<Api>,
    info: Option<Extension<ConnectInfo<crate::server::Client>>>,
    headers: HeaderMap,
) -> Response {
    let Some(sign_in) = api.auth.sign_in() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let verified = match api.auth.verify_from(&headers, client(info)).await {
        Ok(verified) => verified,
        Err(wait) => return too_many_json(wait),
    };
    match verified {
        Ok(v) => body(StatusCode::OK, &session_json(&sign_in, v)),
        Err(Denied::Unauthenticated { why, .. }) => challenged(
            body(StatusCode::UNAUTHORIZED, &json!({ "error": why })),
            challenge(sign_in.auth),
        ),
        Err(Denied::Forbidden { name, why }) => body(
            StatusCode::FORBIDDEN,
            &json!({ "error": why, "name": name }),
        ),
        Err(Denied::Unavailable(why)) => {
            body(StatusCode::SERVICE_UNAVAILABLE, &json!({ "error": why }))
        }
    }
}

/// The caller as `GET /ui/session` describes them.
pub(crate) fn session_json(sign_in: &SignIn, v: Verified) -> Value {
    let at = |t: chrono::DateTime<Utc>| t.to_rfc3339_opts(SecondsFormat::Secs, true);
    json!({
        "name": v.principal.name,
        "role": v.principal.role,
        "auth": sign_in.auth,
        "expires": v.principal.expires.map(at),
        "signed_in": v
            .session
            .as_ref()
            .and_then(|s| chrono::DateTime::from_timestamp(s.signed_in, 0))
            .map(at),
        "csrf": v.session.map(|s| s.csrf),
    })
}

/// The pages' stylesheet and scripts, built into the binary.
async fn asset(Path(file): Path<String>) -> Response {
    let (text, kind) = match file.as_str() {
        "console.css" => (
            include_str!("../assets/console.css"),
            "text/css; charset=utf-8",
        ),
        "sign-in.js" => (
            include_str!("../assets/sign-in.js"),
            "text/javascript; charset=utf-8",
        ),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    (
        [
            (header::CONTENT_TYPE, kind),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        text,
    )
        .into_response()
}

/// The failed basic sign-ins of each address (an IPv6 client by its /64): [`SIGN_IN_ATTEMPTS`]
/// in a row, and it waits [`SIGN_IN_WAIT`]; a sign-in that succeeds starts the count again. Kept
/// by each node for itself, in memory, for at most [`ADDRESSES_KEPT`] addresses: past that,
/// failures of addresses not yet counted are not counted, so a flood can lock nobody out.
#[derive(Debug)]
pub(crate) struct Attempts(Mutex<Table>);

#[derive(Debug)]
struct Table {
    by_address: HashMap<IpAddr, Failures>,
    /// When the addresses that neither wait nor failed lately were last dropped.
    pruned_at: Instant,
}

impl Default for Attempts {
    fn default() -> Self {
        Self(Mutex::new(Table {
            by_address: HashMap::new(),
            pruned_at: Instant::now(),
        }))
    }
}

#[derive(Debug, Clone, Copy)]
struct Failures {
    count: u32,
    last: Instant,
    until: Option<Instant>,
}

/// What a client is counted by: an IPv4 address; an IPv4-mapped IPv6 address as its IPv4; any
/// other IPv6 address by its /64 prefix, which one subscriber holds whole.
fn counted_by(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let mut octets = v6.octets();
                octets[8..].fill(0);
                IpAddr::V6(octets.into())
            }
        },
    }
}

impl Attempts {
    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// An attempt to sign in from `from`, under one lock. `Err` with the wait left while the
    /// address waits (nothing is compared then; a wait over starts the count again). Otherwise
    /// the attempt is counted as failed, provisionally, and `Ok` has the attempts left before it
    /// waits (0: it waits now); a match then calls [`Self::succeeded`].
    pub(crate) fn attempt(&self, from: IpAddr, now: Instant) -> Result<u32, Duration> {
        let key = counted_by(from);
        let mut table = self.lock();
        match table.by_address.get(&key).and_then(|f| f.until) {
            Some(until) if until > now => return Err(until - now),
            Some(_) => {
                table.by_address.remove(&key);
            }
            None => {}
        }
        if !table.by_address.contains_key(&key) && table.by_address.len() >= ADDRESSES_KEPT {
            if now.saturating_duration_since(table.pruned_at) >= PRUNE_EVERY {
                table.pruned_at = now;
                table.by_address.retain(|_, f| {
                    f.until.is_some_and(|u| u > now)
                        || now.saturating_duration_since(f.last) < SIGN_IN_WAIT
                });
            }
            if table.by_address.len() >= ADDRESSES_KEPT {
                return Ok(SIGN_IN_ATTEMPTS - 1);
            }
        }
        let f = table.by_address.entry(key).or_insert(Failures {
            count: 0,
            last: now,
            until: None,
        });
        f.count += 1;
        f.last = now;
        let left = SIGN_IN_ATTEMPTS.saturating_sub(f.count);
        if left == 0 {
            f.until = Some(now + SIGN_IN_WAIT);
        }
        Ok(left)
    }

    /// Gives back the attempt [`Self::attempt`] counted for `from`: the failures before it stay.
    pub(crate) fn refund(&self, from: IpAddr) {
        let key = counted_by(from);
        let mut table = self.lock();
        if let Some(f) = table.by_address.get_mut(&key) {
            f.count = f.count.saturating_sub(1);
            if f.count == 0 {
                table.by_address.remove(&key);
            } else if f.count < SIGN_IN_ATTEMPTS {
                f.until = None;
            }
        }
    }

    /// A sign-in from `from` that succeeded.
    pub(crate) fn succeeded(&self, from: IpAddr) {
        self.lock().by_address.remove(&counted_by(from));
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use data_avalanche_config::Console;
    use serde_json::Value;

    use super::*;
    use crate::Auth;
    use crate::session::SESSION_COOKIE;
    use crate::test_support::{
        Issuer, api, basic_api, basic_session, config, console, request, serve, session_cookie,
        sessions,
    };

    const HOUR: Duration = Duration::from_secs(3600);

    /// The console enabled without sessions, as `none` and `jwt` run it.
    fn enabled() -> Console {
        Console {
            enabled: true,
            ..console()
        }
    }

    /// A node serving `globals.api` and `globals.console` as given.
    async fn node(api_config: &data_avalanche_config::Api, c: &Console) -> SocketAddr {
        serve(api(Arc::new(Auth::new(api_config, c)))).await
    }

    /// The value of header `name` in `head`, in lower case.
    fn header_of<'h>(head: &'h str, name: &str) -> Vec<&'h str> {
        head.lines()
            .filter_map(|l| l.split_once(": "))
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
            .collect()
    }

    /// The `Cookie` a browser sends back after `head`'s `Set-Cookie`s.
    fn kept(head: &str) -> String {
        header_of(head, "set-cookie")
            .into_iter()
            .filter(|c| !c.contains("Max-Age=0"))
            .map(|c| c.split(';').next().unwrap())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Admin_Console §3: the console's routes answer only while it is on; a reload turns it on.
    #[tokio::test]
    async fn the_console_is_served_only_while_it_is_enabled() {
        let auth = Arc::new(Auth::new(&config().globals.api, &console()));
        let addr = serve(api(Arc::clone(&auth))).await;
        for path in [
            "/ui/",
            "/ui/sign-in",
            "/ui/session",
            "/ui/assets/console.css",
        ] {
            let (code, _, body) = request(addr, "GET", path, &[], "").await;
            assert_eq!(code, 404, "{path}");
            assert!(body.contains("globals.console.enabled"), "{body}");
        }
        auth.set(&config().globals.api, &enabled());
        let (code, head, body) = request(addr, "GET", "/ui/", &[], "").await;
        assert_eq!(code, 200, "{body}");
        assert_eq!(
            header_of(&head, "content-security-policy"),
            [
                "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
            ]
        );
        assert_eq!(header_of(&head, "x-frame-options"), ["DENY"]);
        assert_eq!(header_of(&head, "x-content-type-options"), ["nosniff"]);
        assert_eq!(header_of(&head, "referrer-policy"), ["no-referrer"]);
        assert_eq!(header_of(&head, "cache-control"), ["no-store"]);
        let (code, head, _) = request(addr, "GET", "/ui", &[], "").await;
        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/"]));
    }

    /// Admin_Console §9, `basic`: a page asked for without a session goes to the sign-in form,
    /// and the right password comes back to it signed in, with a session every page reads.
    #[tokio::test]
    async fn a_basic_sign_in_comes_back_to_the_page_asked_for() {
        let addr = node(&basic_api("s3cret"), &sessions(8 * HOUR)).await;
        let (code, head, _) =
            request(addr, "GET", "/ui/pipelines/orders?tab=tables", &[], "").await;
        assert_eq!(code, 303);
        let to = "/ui/sign-in?next=%2Fui%2Fpipelines%2Forders%3Ftab%3Dtables";
        assert_eq!(header_of(&head, "location"), [to]);
        let (code, _, page) = request(addr, "GET", to, &[], "").await;
        assert_eq!(code, 200);
        assert!(page.contains("Sign in to the cluster"), "{page}");
        assert!(
            page.contains(r#"name="next" value="/ui/pipelines/orders?tab=tables""#),
            "{page}"
        );
        let form = "user=ops&password=s3cret&next=%2Fui%2Fpipelines%2Forders%3Ftab%3Dtables";
        let (code, head, _) = request(addr, "POST", "/ui/sign-in", &[], form).await;
        assert_eq!(code, 303);
        assert_eq!(
            header_of(&head, "location"),
            ["/ui/pipelines/orders?tab=tables"]
        );
        let set = header_of(&head, "set-cookie");
        assert!(
            set[0].starts_with(&format!("{SESSION_COOKIE}="))
                && set[0].ends_with(&format!(
                    "; Path=/; Max-Age={}; HttpOnly; Secure; SameSite=Strict",
                    (8 * HOUR + KEPT_AFTER).as_secs()
                )),
            "{set:?}"
        );
        let cookie = kept(&head);
        let with = [("Cookie", cookie.as_str())];
        let (code, _, page) = request(addr, "GET", "/ui/pipelines/orders", &with, "").await;
        assert_eq!(code, 200);
        assert!(
            page.contains("You are <b>ops</b>, signed in as <b>admin</b>.")
                && page.contains(r#"action="/ui/sign-out""#),
            "{page}"
        );
        let (code, _, text) = request(addr, "GET", "/ui/session", &with, "").await;
        assert_eq!(code, 200);
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            (&v["name"], &v["role"], &v["auth"]),
            (&"ops".into(), &"admin".into(), &"basic".into())
        );
        assert!(v["csrf"].as_str().is_some_and(|c| c.len() == 43), "{v}");
        assert!(
            v["expires"].is_string() && v["signed_in"].is_string(),
            "{v}"
        );
        // Signed in, the sign-in page goes on to `next`.
        let (code, head, _) = request(addr, "GET", "/ui/sign-in?next=/ui/x", &with, "").await;
        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/x"]));
    }

    /// Mockup `02-sign-in.html`: a wrong password says how many attempts are left; after
    /// [`SIGN_IN_ATTEMPTS`] the address waits [`SIGN_IN_WAIT`], the right password too.
    #[tokio::test]
    async fn wrong_passwords_leave_fewer_attempts_then_a_wait() {
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        for left in (1..SIGN_IN_ATTEMPTS).rev() {
            let (code, _, page) =
                request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=nope").await;
            assert_eq!(code, 401);
            let s = if left == 1 { "" } else { "s" };
            assert!(
                page.contains(&format!(
                    "Wrong user or password. {left} attempt{s} left before a 1 min wait."
                )),
                "{page}"
            );
            assert!(page.contains(r#"value="ops""#), "the user is kept: {page}");
        }
        let (code, _, page) =
            request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=nope").await;
        assert_eq!(code, 401);
        assert!(page.contains("Wait 1 min before trying again."), "{page}");
        let (code, _, page) =
            request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=s3cret").await;
        assert_eq!(code, 429, "{page}");
        assert!(page.contains("Too many failed sign-ins"), "{page}");
    }

    #[test]
    fn failures_are_counted_by_address_and_a_success_starts_again() {
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        let attempts = Attempts::default();
        let t0 = Instant::now();
        assert_eq!(attempts.attempt(a, t0), Ok(SIGN_IN_ATTEMPTS - 1));
        attempts.succeeded(a);
        for i in 1..=SIGN_IN_ATTEMPTS {
            assert_eq!(attempts.attempt(a, t0), Ok(SIGN_IN_ATTEMPTS - i));
        }
        assert_eq!(attempts.attempt(a, t0), Err(SIGN_IN_WAIT));
        assert_eq!(
            attempts.attempt(b, t0),
            Ok(SIGN_IN_ATTEMPTS - 1),
            "another address"
        );
        let later = t0 + SIGN_IN_WAIT;
        assert_eq!(
            attempts.attempt(a, later),
            Ok(SIGN_IN_ATTEMPTS - 1),
            "the wait over, the count starts again"
        );
    }

    #[test]
    fn a_refunded_attempt_leaves_the_failures_before_it() {
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let attempts = Attempts::default();
        let t0 = Instant::now();
        for _ in 0..3 {
            attempts.attempt(a, t0).unwrap();
        }
        attempts.attempt(a, t0).unwrap();
        attempts.refund(a);
        assert_eq!(attempts.attempt(a, t0), Ok(SIGN_IN_ATTEMPTS - 4));
        // The attempt that made it wait, given back, doesn't leave it waiting.
        assert_eq!(attempts.attempt(a, t0), Ok(0));
        attempts.refund(a);
        assert_eq!(attempts.attempt(a, t0), Ok(0), "four failures stay counted");
        attempts.refund(a);
        attempts.succeeded(a);
        attempts.refund(a);
        assert_eq!(attempts.attempt(a, t0), Ok(SIGN_IN_ATTEMPTS - 1));
    }

    #[test]
    fn an_ipv6_client_is_counted_by_its_64_and_a_mapped_ipv4_as_its_ipv4() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let attempts = Attempts::default();
        let t0 = Instant::now();
        assert_eq!(attempts.attempt(ip("2001:db8::1"), t0), Ok(4));
        assert_eq!(
            attempts.attempt(ip("2001:db8::ffff:2"), t0),
            Ok(3),
            "the same /64 shares the count"
        );
        assert_eq!(
            attempts.attempt(ip("2001:db8:0:1::1"), t0),
            Ok(4),
            "another /64 doesn't"
        );
        assert_eq!(attempts.attempt(ip("10.0.0.1"), t0), Ok(4));
        assert_eq!(
            attempts.attempt(ip("::ffff:10.0.0.1"), t0),
            Ok(3),
            "a mapped address is its IPv4"
        );
        attempts.succeeded(ip("2001:db8::9"));
        assert_eq!(attempts.attempt(ip("2001:db8::1"), t0), Ok(4), "reset");
    }

    #[test]
    fn the_table_is_bounded_and_a_flood_locks_nobody_out() {
        let attempts = Attempts::default();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let ip = |i: u32| IpAddr::V4(Ipv4Addr::from(i));
        let size = || attempts.lock().by_address.len();
        let waiting = ip(u32::MAX);
        for _ in 0..SIGN_IN_ATTEMPTS {
            attempts.attempt(waiting, at(50)).unwrap();
        }
        for i in 1..ADDRESSES_KEPT as u32 {
            attempts.attempt(ip(i), t0).unwrap();
        }
        assert_eq!(size(), ADDRESSES_KEPT);
        // Full, nothing old enough to drop: a new address isn't counted, one in the table is.
        let new = ip(1_000_000);
        assert_eq!(attempts.attempt(new, at(51)), Ok(SIGN_IN_ATTEMPTS - 1));
        assert_eq!(attempts.attempt(new, at(51)), Ok(SIGN_IN_ATTEMPTS - 1));
        assert_eq!(size(), ADDRESSES_KEPT);
        assert_eq!(attempts.attempt(ip(1), at(51)), Ok(SIGN_IN_ATTEMPTS - 2));
        // Later the old ones are dropped, the waiting one is not, and the new one is counted.
        assert_eq!(attempts.attempt(new, at(70)), Ok(SIGN_IN_ATTEMPTS - 1));
        assert_eq!(attempts.attempt(new, at(70)), Ok(SIGN_IN_ATTEMPTS - 2));
        assert_eq!(
            size(),
            3,
            "the waiting, the one counted at 51 s, the new one"
        );
        assert!(attempts.attempt(waiting, at(70)).is_err(), "still waits");
    }

    /// Admin_Console §9: a session that no longer verifies is "Your session has ended", saying
    /// why, with a way back to the page; captioned in words.
    #[tokio::test]
    async fn an_ended_session_says_why_and_leads_back() {
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        let old = session_cookie(&basic_session("s3cret", 2 * HOUR));
        let with = [("Cookie", old.as_str())];
        let (code, _, page) = request(addr, "GET", "/ui/nodes/node-b", &with, "").await;
        assert_eq!(code, 401);
        assert!(page.contains("Your session has ended"), "{page}");
        assert!(page.contains("The session expired at "), "{page}");
        assert!(page.contains("(1 h after sign-in)."), "{page}");
        assert!(
            page.contains(r#"href="/ui/sign-in?next=%2Fui%2Fnodes%2Fnode-b""#),
            "{page}"
        );
        assert!(!page.contains("401"), "in words, not codes");
        let (code, _, text) = request(addr, "GET", "/ui/session", &with, "").await;
        assert_eq!(code, 401, "{text}");
        let wrong_key =
            format!("{SESSION_COOKIE}=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let (_, _, page) =
            request(addr, "GET", "/ui/", &[("Cookie", wrong_key.as_str())], "").await;
        assert!(page.contains("doesn&#39;t decrypt"), "{page}");
    }

    /// Admin_Console §9, `jwt`: no sign-in page and no session: "No valid token", no button; a
    /// token whose roles map to nothing is "Forbidden", naming the roles it carried.
    #[tokio::test]
    async fn with_jwt_a_page_needs_a_valid_token() {
        let idp = Issuer::start().await;
        let addr = node(&idp.api("roles"), &enabled()).await;
        let (code, head, page) = request(addr, "GET", "/ui/", &[], "").await;
        assert_eq!(code, 401);
        assert_eq!(
            header_of(&head, "www-authenticate"),
            [r#"Bearer realm="data-avalanche""#]
        );
        assert!(
            page.contains("No valid token") && page.contains("a bearer token is required"),
            "{page}"
        );
        assert!(!page.contains("Sign in"), "nothing to sign in to: {page}");
        let (code, head, _) = request(addr, "GET", "/ui/sign-in?next=/ui/x", &[], "").await;
        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/x"]));
        let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
        let (code, _, page) = request(
            addr,
            "GET",
            "/ui/",
            &[("Authorization", token.as_str())],
            "",
        )
        .await;
        assert_eq!(code, 200);
        assert!(
            page.contains("signed in as <b>viewer</b>") && !page.contains("Sign out"),
            "{page}"
        );
        let token = format!(
            "Bearer {}",
            idp.token(serde_json::json!(["cdc-reader", "reporting"]))
        );
        let (code, _, page) = request(
            addr,
            "GET",
            "/ui/",
            &[("Authorization", token.as_str())],
            "",
        )
        .await;
        assert_eq!(code, 403);
        assert!(
            page.contains("Forbidden")
                && page.contains("You are <b>alice</b>, and the token&#39;s roles (cdc-reader, reporting) name no role this node maps."),
            "{page}"
        );
    }

    /// Admin_Console §9, `none`: straight in as admin, and every page says so.
    #[tokio::test]
    async fn with_none_every_page_warns_there_is_no_authentication() {
        let addr = node(&config().globals.api, &enabled()).await;
        let (code, _, page) = request(addr, "GET", "/ui/overview", &[], "").await;
        assert_eq!(code, 200);
        assert!(
            page.contains(
                "This cluster has no authentication: anyone who can reach it is an admin."
            ) && page.contains("You are <b>anonymous</b>, signed in as <b>admin</b>."),
            "{page}"
        );
        let (code, head, _) = request(addr, "GET", "/ui/sign-in", &[], "").await;
        assert_eq!((code, header_of(&head, "location")), (303, vec!["/ui/"]));
    }

    /// Admin_Console §9: a form another site posts (a sign-in to the attacker's account, a
    /// sign-out) is refused; the console's own sign-out removes every session cookie.
    #[tokio::test]
    async fn a_form_another_site_posts_is_refused() {
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        let form = "user=ops&password=s3cret";
        for from in [
            [("Sec-Fetch-Site", "cross-site"), ("Origin", "http://t")],
            [("Sec-Fetch-Site", "same-site"), ("Origin", "http://t")],
            [("Origin", "https://evil.example"), ("X-Other", "")],
            [("Origin", "null"), ("X-Other", "")],
        ] {
            let (code, _, page) = request(addr, "POST", "/ui/sign-in", &from, form).await;
            assert_eq!(code, 403, "{from:?}");
            assert!(page.contains("sent from another site"), "{page}");
            let (code, _, _) = request(addr, "POST", "/ui/sign-out", &from, "").await;
            assert_eq!(code, 403, "{from:?}");
        }
        let own = [("Sec-Fetch-Site", "same-origin"), ("Origin", "http://t")];
        let (code, _, _) = request(addr, "POST", "/ui/sign-in", &own, form).await;
        assert_eq!(code, 303);
        let (code, _, _) =
            request(addr, "POST", "/ui/sign-in", &[("Origin", "http://t")], form).await;
        assert_eq!(code, 303, "Origin against Host");
        let (code, head, _) = request(addr, "POST", "/ui/sign-out", &own, "").await;
        assert_eq!(code, 303);
        assert_eq!(header_of(&head, "location"), ["/ui/sign-in?signed_out"]);
        let removed = header_of(&head, "set-cookie");
        assert_eq!(removed.len(), crate::MAX_CHUNKS);
        assert!(
            removed.iter().all(|c| c.contains("Max-Age=0")),
            "{removed:?}"
        );
        let (_, _, page) = request(addr, "GET", "/ui/sign-in?signed_out", &[], "").await;
        assert!(page.contains("You signed out."), "{page}");
    }

    #[test]
    fn next_is_always_a_console_page() {
        for (asked, taken) in [
            (
                Some("/ui/pipelines/orders?tab=tables"),
                "/ui/pipelines/orders?tab=tables",
            ),
            (None, "/ui/"),
            (Some("https://evil.example/ui/"), "/ui/"),
            (Some("//evil.example/ui/"), "/ui/"),
            (Some("/ui//evil.example"), "/ui/"),
            (Some("/ui/\\evil.example"), "/ui/"),
            (Some("/v1/pipelines"), "/ui/"),
            (Some("/ui/\r\nSet-Cookie: x"), "/ui/"),
            (Some("/ui/../v1/pipelines"), "/ui/"),
            (Some("/ui/%2e%2e/v1/x"), "/ui/"),
            (Some("/ui/./x"), "/ui/"),
            (Some("/ui/.%2E/x"), "/ui/"),
            (Some("/ui/%2E/x"), "/ui/"),
            (
                Some("/ui/pipelines/a.b?from=../x"),
                "/ui/pipelines/a.b?from=../x",
            ),
        ] {
            assert_eq!(safe_next(asked), taken, "{asked:?}");
        }
    }

    #[test]
    fn pages_escape_what_they_show() {
        let frame = Frame {
            node: "<b>n</b>".into(),
            open: false,
        };
        let page = pages::forbidden(&frame, "<script>x</script>", "it's \"so\" & so", false);
        assert!(
            page.contains("&lt;script&gt;x&lt;/script&gt;")
                && page.contains("it&#39;s &quot;so&quot; &amp; so")
                && page.contains("&lt;b&gt;n&lt;/b&gt;"),
            "{page}"
        );
        assert!(!page.contains("<script>x"), "{page}");
    }

    #[test]
    fn the_sign_in_form_escapes_its_attributes() {
        let frame = Frame {
            node: "n".into(),
            open: false,
        };
        let page = pages::basic_sign_in(&frame, "/ui/x\"<y", "u\"<v", None, None);
        assert!(
            page.contains(r#"name="next" value="/ui/x&quot;&lt;y""#)
                && page.contains(r#"name="user" value="u&quot;&lt;v""#),
            "{page}"
        );
        assert!(!page.contains("x\"<y") && !page.contains("u\"<v"), "{page}");
    }

    /// Every answer of `/ui/`, a redirect, a 401 page, a 429 and the refusal while off, forbids
    /// framing and runs nothing inline; a 401 says how to authenticate and a 429 when to retry.
    #[tokio::test]
    async fn every_answer_is_hardened_and_the_401_and_429_say_what_to_do() {
        let hardened = |head: &str| {
            assert_eq!(header_of(head, "x-frame-options"), ["DENY"], "{head}");
            assert_eq!(header_of(head, "x-content-type-options"), ["nosniff"]);
            assert_eq!(header_of(head, "referrer-policy"), ["no-referrer"]);
            assert_eq!(header_of(head, "cache-control"), ["no-store"]);
            assert_eq!(header_of(head, "content-security-policy").len(), 1);
        };
        let off = node(&config().globals.api, &console()).await;
        let (code, head, _) = request(off, "GET", "/ui/", &[], "").await;
        assert_eq!(code, 404);
        hardened(&head);
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        let (code, head, _) = request(addr, "GET", "/ui/", &[], "").await;
        assert_eq!(code, 303);
        hardened(&head);
        let old = session_cookie(&basic_session("s3cret", 2 * HOUR));
        let with = [("Cookie", old.as_str())];
        let (code, head, _) = request(addr, "GET", "/ui/", &with, "").await;
        assert_eq!(code, 401);
        hardened(&head);
        assert_eq!(
            header_of(&head, "www-authenticate"),
            [r#"Session realm="data-avalanche""#]
        );
        let (code, head, _) = request(addr, "GET", "/ui/session", &with, "").await;
        assert_eq!(code, 401);
        assert_eq!(
            header_of(&head, "www-authenticate"),
            [r#"Session realm="data-avalanche""#]
        );
        for _ in 0..SIGN_IN_ATTEMPTS {
            let (code, head, _) =
                request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=x").await;
            assert_eq!(code, 401);
            hardened(&head);
            assert_eq!(header_of(&head, "www-authenticate").len(), 1);
        }
        let (code, head, _) =
            request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=x").await;
        assert_eq!(code, 429);
        hardened(&head);
        assert_eq!(header_of(&head, "retry-after"), ["60"]);
    }

    /// `/v1` limits its Basic credentials by address too, on the sign-in's counter: a refused
    /// one counts, a right one resets, a bearer token or a session isn't counted.
    #[tokio::test]
    async fn v1_basic_credentials_are_limited_with_the_sign_in() {
        use base64::Engine as _;
        let basic = |password: &str| {
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("ops:{password}"))
            )
        };
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        let get = |credential: String| async move {
            request(
                addr,
                "GET",
                "/v1/pipelines",
                &[("Authorization", credential.as_str())],
                "",
            )
            .await
        };
        for _ in 0..SIGN_IN_ATTEMPTS {
            assert_eq!(get(basic("nope")).await.0, 401);
        }
        let (code, head, text) = get(basic("s3cret")).await;
        assert_eq!(code, 429, "even the right one waits: {text}");
        assert_eq!(header_of(&head, "retry-after"), ["60"]);
        assert!(
            text.contains("too many failed sign-ins from this address; try again in 60 s"),
            "{text}"
        );
        // The form, on the same address, waits too.
        let (code, _, _) =
            request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=s3cret").await;
        assert_eq!(code, 429);

        // A success resets; bearer tokens are not counted.
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        let get = |credential: String| async move {
            request(
                addr,
                "GET",
                "/v1/pipelines",
                &[("Authorization", credential.as_str())],
                "",
            )
            .await
        };
        for _ in 0..SIGN_IN_ATTEMPTS - 1 {
            assert_eq!(get(basic("nope")).await.0, 401);
        }
        assert_ne!(get(basic("s3cret")).await.0, 401, "right");
        for _ in 0..SIGN_IN_ATTEMPTS - 1 {
            assert_eq!(get(basic("nope")).await.0, 401, "the count started again");
        }
        let (code, _, _) = get(basic("s3cret")).await;
        assert!(code != 401 && code != 429, "{code}");
        for _ in 0..2 * SIGN_IN_ATTEMPTS {
            assert_eq!(get("Bearer x".into()).await.0, 401);
        }
        let (code, _, _) = get(basic("s3cret")).await;
        assert!(
            code != 401 && code != 429,
            "bearer failures aren't counted: {code}"
        );

        // The form and `/v1` share the count: 3 and 2 failures make the sixth attempt wait.
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        for _ in 0..3 {
            let (code, _, _) =
                request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=x").await;
            assert_eq!(code, 401);
        }
        for _ in 0..2 {
            let auth = basic("nope");
            let (code, _, _) = request(
                addr,
                "GET",
                "/v1/pipelines",
                &[("Authorization", auth.as_str())],
                "",
            )
            .await;
            assert_eq!(code, 401);
        }
        let auth = basic("s3cret");
        let (code, _, _) = request(
            addr,
            "GET",
            "/v1/pipelines",
            &[("Authorization", auth.as_str())],
            "",
        )
        .await;
        assert_eq!(code, 429);
    }

    /// The console's pages and `/ui/session` take Basic credentials too, so they are limited as
    /// the form and `/v1` are, on the same counter; their 401 never offers the `Basic` popup.
    #[tokio::test]
    async fn the_console_s_pages_share_the_basic_limit() {
        use base64::Engine as _;
        let basic = |password: &str| {
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("ops:{password}"))
            )
        };
        let session = [r#"Session realm="data-avalanche""#];
        for path in ["/ui/", "/ui/session"] {
            let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
            for _ in 0..SIGN_IN_ATTEMPTS {
                let auth = basic("nope");
                let (code, head, _) =
                    request(addr, "GET", path, &[("Authorization", auth.as_str())], "").await;
                assert_eq!(code, 401, "{path}");
                assert_eq!(header_of(&head, "www-authenticate"), session, "{path}");
            }
            let auth = basic("s3cret");
            let (code, head, text) =
                request(addr, "GET", path, &[("Authorization", auth.as_str())], "").await;
            assert_eq!(code, 429, "{path}: {text}");
            assert_eq!(header_of(&head, "retry-after"), ["60"]);
            assert_eq!(header_of(&head, "x-frame-options"), ["DENY"]);
            assert!(
                text.contains("Too many failed sign-ins") || text.contains("try again in 60 s"),
                "{text}"
            );
            let (code, _, _) =
                request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=s3cret").await;
            assert_eq!(code, 429, "the form shares the count");
        }
        // Form, /v1 and a page share the count; a success resets it.
        let addr = node(&basic_api("s3cret"), &sessions(HOUR)).await;
        for _ in 0..2 {
            let (code, _, _) =
                request(addr, "POST", "/ui/sign-in", &[], "user=ops&password=x").await;
            assert_eq!(code, 401);
        }
        let wrong = basic("nope");
        let with = [("Authorization", wrong.as_str())];
        for path in ["/v1/pipelines", "/ui/session"] {
            assert_eq!(request(addr, "GET", path, &with, "").await.0, 401, "{path}");
        }
        let right = basic("s3cret");
        let (code, _, _) = request(
            addr,
            "GET",
            "/ui/",
            &[("Authorization", right.as_str())],
            "",
        )
        .await;
        assert_eq!(code, 200, "the fifth attempt is the right one");
        let (code, _, _) = request(
            addr,
            "GET",
            "/ui/",
            &[("Authorization", right.as_str())],
            "",
        )
        .await;
        assert_eq!(code, 200, "it reset the count");
    }

    #[tokio::test]
    async fn the_stylesheet_is_served_and_nothing_else() {
        let addr = node(&config().globals.api, &enabled()).await;
        let (code, head, css) = request(addr, "GET", "/ui/assets/console.css", &[], "").await;
        assert_eq!(code, 200);
        assert_eq!(
            header_of(&head, "content-type"),
            ["text/css; charset=utf-8"]
        );
        assert!(css.contains("prefers-color-scheme: dark"), "both themes");
        for other in ["/ui/assets/other.js", "/ui/assets/..%2Fsrc%2Flib.rs"] {
            assert_eq!(request(addr, "GET", other, &[], "").await.0, 404, "{other}");
        }
    }
}
