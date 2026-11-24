//! The console's session (Admin_Console §9): with `auth` `basic` or `oauth2`, a person signed in
//! holds an encrypted cookie that every node opens with `globals.console.session_key`, so a load
//! balancer may send any request anywhere. It carries what verifies the person again on every
//! request (the basic user and a fingerprint of the password; the provider's access token, and
//! its refresh token when one was issued), when they signed in, and the anti-forgery token their
//! mutating requests send back. Nothing of it is kept anywhere else: a key change ends every
//! session, since no node opens the old cookies any more.

use std::fmt::Write as _;

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use axum::http::HeaderMap;
use axum::http::header::COOKIE;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

/// The session cookie; one too long for a cookie goes on in `__Host-da_session.1`, `__Host-da_session.2`, ….
pub const SESSION_COOKIE: &str = "__Host-da_session";
/// A cookie's value is at most this long: browsers keep a cookie of 4096 bytes, its name and
/// attributes included.
pub const COOKIE_CHUNK: usize = 3800;
/// A session takes at most this many cookies: a provider whose tokens need more can't start one.
pub const MAX_CHUNKS: usize = 4;
/// A session's cookies outlive it by this long, so its page can still say when it ended.
pub const KEPT_AFTER: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

/// The sealed format's version, its first byte.
const VERSION: u8 = 1;
/// The key's id: a cookie sealed with another key is told from one altered.
const ID_LEN: usize = 8;

/// Seals and opens what the console keeps in cookies, with keys derived from the session key:
/// AES-256-GCM, a random nonce for each seal, and the cookie's purpose as associated data, so a
/// value sealed for one purpose doesn't open as another.
pub(crate) struct Sealer {
    key: LessSafeKey,
    id: [u8; ID_LEN],
    fingerprints: [u8; 32],
}

impl std::fmt::Debug for Sealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sealer").finish_non_exhaustive()
    }
}

/// Why a sealed value doesn't open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Unsealed {
    /// Sealed with another session key: the key changed since.
    #[error("the session key changed")]
    KeyChanged,
    /// Not sealed by this cluster, altered, or sealed for another purpose.
    #[error("the session cookie doesn't decrypt")]
    Garbled,
}

impl Sealer {
    /// The sealer of `globals.console.session_key` (at least 32 bytes, V-128).
    pub(crate) fn new(session_key: &str) -> Self {
        let derived = |context: &str| blake3::derive_key(context, session_key.as_bytes());
        let key = UnboundKey::new(
            &AES_256_GCM,
            &derived("data-avalanche 2026-11-24 console cookie encryption"),
        )
        .expect("a 32-byte AES-256 key");
        let mut id = [0; ID_LEN];
        id.copy_from_slice(&derived("data-avalanche 2026-11-24 console cookie key id")[..ID_LEN]);
        Self {
            key: LessSafeKey::new(key),
            id,
            fingerprints: derived("data-avalanche 2026-11-24 console secret fingerprint"),
        }
    }

    /// `plain`, sealed for `purpose`, as URL-safe base64.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
    )]
    pub(crate) fn seal(&self, purpose: &str, plain: &[u8]) -> String {
        let mut nonce = [0; NONCE_LEN];
        aws_lc_rs::rand::fill(&mut nonce).expect("the system's random source");
        let mut sealed = plain.to_vec();
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(purpose.as_bytes()),
                &mut sealed,
            )
            .expect("AES-GCM seals any length a cookie takes");
        let mut out = Vec::with_capacity(1 + ID_LEN + NONCE_LEN + sealed.len());
        out.push(VERSION);
        out.extend_from_slice(&self.id);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        URL_SAFE_NO_PAD.encode(out)
    }

    /// What [`Self::seal`] sealed for `purpose`.
    ///
    /// # Errors
    /// [`Unsealed`]: sealed with another key, or not by this one for `purpose`.
    pub(crate) fn open(&self, purpose: &str, sealed: &str) -> Result<Vec<u8>, Unsealed> {
        let raw = URL_SAFE_NO_PAD
            .decode(sealed)
            .map_err(|_| Unsealed::Garbled)?;
        if raw.len() < 1 + ID_LEN + NONCE_LEN || raw[0] != VERSION {
            return Err(Unsealed::Garbled);
        }
        if raw[1..=ID_LEN] != self.id {
            return Err(Unsealed::KeyChanged);
        }
        let mut nonce = [0; NONCE_LEN];
        nonce.copy_from_slice(&raw[1 + ID_LEN..1 + ID_LEN + NONCE_LEN]);
        let mut body = raw[1 + ID_LEN + NONCE_LEN..].to_vec();
        let plain = self
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(purpose.as_bytes()),
                &mut body,
            )
            .map_err(|_| Unsealed::Garbled)?;
        Ok(plain.to_vec())
    }

    /// A keyed fingerprint of `secret`: a session holds the basic password's, so a password
    /// changed ends it, and holds nothing the password could be read back from.
    pub(crate) fn fingerprint(&self, secret: &str) -> String {
        URL_SAFE_NO_PAD.encode(blake3::keyed_hash(&self.fingerprints, secret.as_bytes()).as_bytes())
    }
}

/// A random token, URL-safe: 32 bytes of the system's random source.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
)]
pub(crate) fn random_token() -> String {
    let mut b = [0; 32];
    aws_lc_rs::rand::fill(&mut b).expect("the system's random source");
    URL_SAFE_NO_PAD.encode(b)
}

/// A signed-in person's session.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Session {
    /// When they signed in, Unix seconds: the session ends `session_ttl` after, whatever they
    /// do (S-M14.1).
    pub signed_in: i64,
    /// The anti-forgery token: a mutating request the session authenticates sends it back.
    pub csrf: String,
    /// What verifies them again.
    pub by: By,
}

/// The anti-forgery token never prints.
impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("signed_in", &self.signed_in)
            .field("by", &self.by)
            .finish_non_exhaustive()
    }
}

/// What a session verifies its person by, again on each request.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub(crate) enum By {
    /// The configured user, and the [`Sealer::fingerprint`] of the password they gave.
    Basic { user: String, fingerprint: String },
    /// The provider's access token, verified as a bearer token is; its refresh token, when it
    /// issued one.
    Oauth2 {
        access: String,
        refresh: Option<String>,
    },
}

/// The tokens never print.
impl std::fmt::Debug for By {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Basic { user, .. } => f
                .debug_struct("Basic")
                .field("user", user)
                .finish_non_exhaustive(),
            Self::Oauth2 { refresh, .. } => f
                .debug_struct("Oauth2")
                .field("refresh", &refresh.as_ref().map(|_| "<redacted>"))
                .finish_non_exhaustive(),
        }
    }
}

/// A session too long for [`MAX_CHUNKS`] cookies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
)]
#[error(
    "the session needs {0} bytes of cookies, more than the {MAX_CHUNKS} cookies of {COOKIE_CHUNK} bytes a browser is sent: the provider's tokens are too long"
)]
pub(crate) struct TooLong(pub usize);

/// The purpose a session is sealed for.
const SESSION: &str = "session";

impl Session {
    /// A session signed in at `signed_in`, with a new anti-forgery token.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
    )]
    pub(crate) fn new(by: By, signed_in: i64) -> Self {
        Self {
            signed_in,
            csrf: random_token(),
            by,
        }
    }

    /// The `Set-Cookie` values that hold this session, kept `max_age` seconds: its cookies, then
    /// the removal of each further one an earlier, longer session may have left.
    ///
    /// # Errors
    /// [`TooLong`]: it needs more than [`MAX_CHUNKS`] cookies.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
    )]
    pub(crate) fn cookies(&self, sealer: &Sealer, max_age: u64) -> Result<Vec<String>, TooLong> {
        let plain = serde_json::to_vec(self).expect("a session serializes");
        let sealed = sealer.seal(SESSION, &plain);
        let chunks: Vec<&str> = sealed
            .as_bytes()
            .chunks(COOKIE_CHUNK)
            .map(|c| std::str::from_utf8(c).expect("base64 is ASCII"))
            .collect();
        if chunks.len() > MAX_CHUNKS {
            return Err(TooLong(sealed.len()));
        }
        let mut out: Vec<String> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| set_cookie(&chunk_name(i), c, "/", max_age, "Strict"))
            .collect();
        out.extend((chunks.len()..MAX_CHUNKS).map(|i| removed(&chunk_name(i), "/")));
        Ok(out)
    }

    /// The session the request's cookies hold: `None` without its cookie.
    ///
    /// # Errors
    /// [`Unsealed`]: its cookies don't open, or what they hold isn't a session.
    pub(crate) fn of(headers: &HeaderMap, sealer: &Sealer) -> Option<Result<Self, Unsealed>> {
        let sealed = sealed_value(headers)?;
        Some(
            sealer
                .open(SESSION, &sealed)
                .and_then(|plain| serde_json::from_slice(&plain).map_err(|_| Unsealed::Garbled)),
        )
    }
}

/// The session's cookies, joined in order: `None` without its first.
pub(crate) fn sealed_value(headers: &HeaderMap) -> Option<String> {
    let mut sealed = String::new();
    for i in 0..MAX_CHUNKS {
        match cookie(headers, &chunk_name(i)) {
            Some(v) => sealed.push_str(v),
            None if i == 0 => return None,
            None => break,
        }
    }
    Some(sealed)
}

/// The request's session cookies alone, as one `Cookie` header's value: what a node passes on to
/// a peer with the caller's other credentials (Admin_Console §10).
pub(crate) fn session_cookies(headers: &HeaderMap) -> Option<String> {
    let mut out = String::new();
    for i in 0..MAX_CHUNKS {
        let name = chunk_name(i);
        let Some(v) = cookie(headers, &name) else {
            break;
        };
        if !out.is_empty() {
            out.push_str("; ");
        }
        let _ = write!(out, "{name}={v}");
    }
    (!out.is_empty()).then_some(out)
}

fn chunk_name(i: usize) -> String {
    if i == 0 {
        SESSION_COOKIE.to_owned()
    } else {
        format!("{SESSION_COOKIE}.{i}")
    }
}

/// The value of cookie `name` in the request's `Cookie` headers.
pub(crate) fn cookie<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(n, _)| *n == name)
        .map(|(_, v)| v)
}

/// A `Set-Cookie` value: `HttpOnly` and `Secure`, for `path`, kept `max_age` seconds, `same_site`
/// `Strict` or `Lax`.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
)]
pub(crate) fn set_cookie(
    name: &str,
    value: &str,
    path: &str,
    max_age: u64,
    same_site: &str,
) -> String {
    format!(
        "{name}={value}; Path={path}; Max-Age={max_age}; HttpOnly; Secure; SameSite={same_site}"
    )
}

/// The `Set-Cookie` value that removes cookie `name` at `path`.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the sign-in (Task 3) starts sessions")
)]
pub(crate) fn removed(name: &str, path: &str) -> String {
    format!("{name}=; Path={path}; Max-Age=0; HttpOnly; Secure; SameSite=Strict")
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    const KEY: &str = "a session key of at least thirty-two bytes";

    fn basic() -> Session {
        Session::new(
            By::Basic {
                user: "ops".into(),
                fingerprint: Sealer::new(KEY).fingerprint("s3cret"),
            },
            1_700_000_000,
        )
    }

    /// The `Cookie` header a browser sends back for these `Set-Cookie` values.
    fn sent_back(set: &[String]) -> HeaderMap {
        let pairs: Vec<&str> = set
            .iter()
            .filter(|c| !c.contains("Max-Age=0"))
            .map(|c| c.split(';').next().unwrap())
            .collect();
        let mut h = HeaderMap::new();
        h.insert(COOKIE, HeaderValue::from_str(&pairs.join("; ")).unwrap());
        h
    }

    #[test]
    fn a_session_sealed_by_one_node_opens_on_another_with_the_same_key() {
        let s = basic();
        let set = s.cookies(&Sealer::new(KEY), 3600).unwrap();
        assert_eq!(
            set[0].split_once('=').unwrap().0,
            "__Host-da_session",
            "the first cookie"
        );
        assert!(
            set[0].ends_with("; Path=/; Max-Age=3600; HttpOnly; Secure; SameSite=Strict"),
            "{}",
            set[0]
        );
        let other = Sealer::new(KEY);
        assert_eq!(Session::of(&sent_back(&set), &other), Some(Ok(s)));
    }

    #[test]
    fn a_session_key_changed_is_told_from_a_cookie_altered() {
        let set = basic().cookies(&Sealer::new(KEY), 3600).unwrap();
        let other = Sealer::new("another session key of thirty-two bytes");
        assert_eq!(
            Session::of(&sent_back(&set), &other),
            Some(Err(Unsealed::KeyChanged))
        );
        // One character of the ciphertext changed.
        let mut altered = set[0].clone();
        let at = altered.find(';').unwrap() - 3;
        let flipped = if &altered[at..=at] == "A" { "B" } else { "A" };
        altered.replace_range(at..=at, flipped);
        assert_eq!(
            Session::of(&sent_back(&[altered]), &Sealer::new(KEY)),
            Some(Err(Unsealed::Garbled))
        );
        assert_eq!(Session::of(&HeaderMap::new(), &Sealer::new(KEY)), None);
    }

    #[test]
    fn a_value_sealed_for_one_purpose_doesnt_open_as_another() {
        let sealer = Sealer::new(KEY);
        let sealed = sealer.seal("oauth2", b"{}");
        assert_eq!(sealer.open("oauth2", &sealed).unwrap(), b"{}");
        assert_eq!(sealer.open("session", &sealed), Err(Unsealed::Garbled));
    }

    #[test]
    fn a_long_session_takes_several_cookies_and_too_long_a_one_none() {
        let sealer = Sealer::new(KEY);
        let token = |n: usize| "t".repeat(n);
        let s = Session::new(
            By::Oauth2 {
                access: token(3000),
                refresh: Some(token(1500)),
            },
            1_700_000_000,
        );
        let set = s.cookies(&sealer, 60).unwrap();
        let names: Vec<&str> = set.iter().map(|c| c.split('=').next().unwrap()).collect();
        assert_eq!(
            names,
            [
                "__Host-da_session",
                "__Host-da_session.1",
                "__Host-da_session.2",
                "__Host-da_session.3"
            ]
        );
        assert!(
            set[1].len() < 4096 && set[2].contains("Max-Age=0"),
            "{set:?}"
        );
        assert!(
            set[3].contains("Max-Age=0"),
            "an older session's cookie removed"
        );
        let back = sent_back(&set);
        assert_eq!(Session::of(&back, &sealer), Some(Ok(s)));
        assert_eq!(
            session_cookies(&back)
                .unwrap()
                .matches("__Host-da_session")
                .count(),
            2
        );
        let huge = Session::new(
            By::Oauth2 {
                access: token(16_000),
                refresh: None,
            },
            1_700_000_000,
        );
        assert!(matches!(huge.cookies(&sealer, 60), Err(TooLong(_))));
    }

    #[test]
    fn the_session_cookies_alone_are_passed_on() {
        let mut h = HeaderMap::new();
        h.insert(
            COOKIE,
            HeaderValue::from_static("theme=dark; __Host-da_session=abc; other=x"),
        );
        h.append(COOKIE, HeaderValue::from_static("__Host-da_session.1=def"));
        assert_eq!(
            session_cookies(&h).as_deref(),
            Some("__Host-da_session=abc; __Host-da_session.1=def")
        );
        assert_eq!(sealed_value(&h).as_deref(), Some("abcdef"));
    }

    #[test]
    fn a_password_s_fingerprint_depends_on_the_key_and_the_password() {
        let a = Sealer::new(KEY);
        assert_eq!(
            a.fingerprint("s3cret"),
            Sealer::new(KEY).fingerprint("s3cret")
        );
        assert_ne!(a.fingerprint("s3cret"), a.fingerprint("s3cret2"));
        let b = Sealer::new("another session key of thirty-two bytes");
        assert_ne!(a.fingerprint("s3cret"), b.fingerprint("s3cret"));
    }

    #[test]
    fn a_session_s_tokens_never_print() {
        let mut s = Session::new(
            By::Oauth2 {
                access: "the-access-token".into(),
                refresh: Some("the-refresh-token".into()),
            },
            0,
        );
        s.csrf = "the-csrf-token".into();
        let printed = format!("{s:?}");
        for secret in ["the-access-token", "the-refresh-token", "the-csrf-token"] {
            assert!(!printed.contains(secret), "{printed}");
        }
    }
}
