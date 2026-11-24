//! The API's credentials for a scenario, generated at run time: a token issuer for `auth: jwt`
//! (one ES256 key, its JWKS served on loopback) and a self-signed certificate for
//! `globals.api.tls`.

use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use rustls::pki_types::CertificateDer;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

/// Issues the tokens a node with [`Idp::api`] takes.
pub struct Idp {
    issuer: String,
    key: KeyPair,
    served: JoinHandle<()>,
}

impl Idp {
    /// Serves the JWKS, at any path, on a loopback port.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        // An uncompressed P-256 point: 0x04, then x and y.
        let raw = key.public_key_raw();
        let jwks = json!({ "keys": [{
            "kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": "k1",
            "x": URL_SAFE_NO_PAD.encode(&raw[1..33]),
            "y": URL_SAFE_NO_PAD.encode(&raw[33..65]),
        }] })
        .to_string();
        let served = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let jwks = jwks.clone();
                tokio::spawn(async move {
                    let mut head = [0; 4096];
                    let _ = s.read(&mut head).await;
                    let answer = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{jwks}",
                        jwks.len()
                    );
                    let _ = s.write_all(answer.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        Self {
            issuer,
            key,
            served,
        }
    }

    /// `globals.api` entries: `auth: jwt` for this issuer, audience `cdc`, the roles in the
    /// `roles` claim; `more` adds entries (a `role_mapping`).
    pub fn api(&self, more: &str) -> String {
        format!(
            r#", auth: jwt, jwt: {{ issuer: "{i}", audience: cdc, jwks_url: "{i}/jwks", roles_claim: roles }}{more}"#,
            i = self.issuer
        )
    }

    /// A token for `alice` with `roles`, valid ten minutes.
    pub fn token(&self, roles: &[&str]) -> String {
        let now = jsonwebtoken::get_current_timestamp();
        let claims = json!({
            "iss": self.issuer, "aud": "cdc", "sub": "alice", "exp": now + 600, "roles": roles,
        });
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some("k1".into());
        jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_ec_der(&self.key.serialize_der()),
        )
        .unwrap()
    }
}

impl Drop for Idp {
    fn drop(&mut self) {
        self.served.abort();
    }
}

/// An OpenID provider for the console's `oauth2` sign-in, on a loopback port: its discovery
/// document, its JWKS, a token endpoint that exchanges a code it issued once, only with the PKCE
/// verifier of the code's challenge and the redirect URI of its request (`invalid_grant`
/// otherwise), and refreshes, and an end-session endpoint. Its authorization step approves at once, in the test's own process
/// ([`Provider::approve`]). Its tokens name `alice` (`preferred_username` `alice.smith`) with
/// `roles`.
pub struct Provider {
    pub issuer: String,
    served: JoinHandle<()>,
    codes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    issued: Issued,
}

/// The codes issued and not yet exchanged: each with its request's `code_challenge` and
/// `redirect_uri`.
type Issued = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, (String, String)>>>;

/// `BASE64URL-NOPAD(SHA-256(verifier))`: a PKCE S256 challenge.
fn s256(verifier: &str) -> String {
    let suite = rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256
        .tls13()
        .expect("a TLS 1.3 suite");
    let hash = suite.common.hash_provider.hash(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hash.as_ref())
}

/// `s`, percent-decoded (a query's value).
fn decoded(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap()
}

/// The value of `name` in `url`'s query.
pub fn query_value(url: &str, name: &str) -> Option<String> {
    let query = url.split_once('?')?.1;
    query
        .split('&')
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| decoded(v))
}

/// Reads one HTTP request from `s`: its request line and its body.
async fn read_request(s: &mut tokio::net::TcpStream) -> Option<(String, String)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let length: usize = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())?
        })
        .unwrap_or(0);
    while buf.len() < head_end + length {
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let line = head.lines().next()?.to_owned();
    let body = String::from_utf8_lossy(&buf[head_end..]).into_owned();
    Some((line, body))
}

impl Provider {
    /// Serves on a loopback port; its tokens carry `roles`.
    pub async fn start(roles: &[&str]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let key = std::sync::Arc::new(KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap());
        let raw = key.public_key_raw();
        let jwks = json!({ "keys": [{
            "kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": "k1",
            "x": URL_SAFE_NO_PAD.encode(&raw[1..33]),
            "y": URL_SAFE_NO_PAD.encode(&raw[33..65]),
        }] })
        .to_string();
        let discovery = json!({
            "issuer": issuer,
            "jwks_uri": format!("{issuer}/jwks"),
            "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"),
            "end_session_endpoint": format!("{issuer}/end-session"),
        })
        .to_string();
        let roles: Vec<String> = roles.iter().map(|r| (*r).to_owned()).collect();
        let at = issuer.clone();
        let issued = Issued::default();
        let issued_in = std::sync::Arc::clone(&issued);
        let served = tokio::spawn(async move {
            let refreshes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let (jwks, discovery, key, roles, issuer, refreshes, issued) = (
                    jwks.clone(),
                    discovery.clone(),
                    std::sync::Arc::clone(&key),
                    roles.clone(),
                    at.clone(),
                    std::sync::Arc::clone(&refreshes),
                    std::sync::Arc::clone(&issued_in),
                );
                tokio::spawn(async move {
                    let Some((line, body)) = read_request(&mut s).await else {
                        return;
                    };
                    let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
                    let (code, answer) = if path.starts_with("/.well-known/openid-configuration") {
                        (200, discovery)
                    } else if path.starts_with("/jwks") {
                        (200, jwks)
                    } else if path.starts_with("/token") {
                        let n = refreshes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let now = jsonwebtoken::get_current_timestamp();
                        let sign = |claims: serde_json::Value| {
                            let mut header = Header::new(Algorithm::ES256);
                            header.kid = Some("k1".into());
                            jsonwebtoken::encode(
                                &header,
                                &claims,
                                &EncodingKey::from_ec_der(&key.serialize_der()),
                            )
                            .unwrap()
                        };
                        let access = sign(json!({
                            "iss": issuer, "aud": "cdc", "sub": "alice",
                            "preferred_username": "alice.smith", "exp": now + 600, "roles": roles,
                        }));
                        let id = sign(json!({
                            "iss": issuer, "aud": "cdc-console", "sub": "alice", "exp": now + 600,
                        }));
                        let form = format!("{path}?{body}");
                        let exchanged = if body.contains("grant_type=authorization_code") {
                            let code = query_value(&form, "code").unwrap_or_default();
                            let issued = issued.lock().unwrap().remove(&code);
                            issued.is_some_and(|(challenge, redirect)| {
                                let verifier =
                                    query_value(&form, "code_verifier").unwrap_or_default();
                                s256(&verifier) == challenge
                                    && query_value(&form, "redirect_uri").as_deref()
                                        == Some(redirect.as_str())
                            })
                        } else {
                            body.contains("grant_type=refresh_token")
                        };
                        let granted = body.contains("grant_type=authorization_code")
                            || body.contains("grant_type=refresh_token");
                        if granted && !exchanged {
                            (400, json!({ "error": "invalid_grant" }).to_string())
                        } else if granted {
                            (
                                200,
                                json!({
                                    "access_token": access, "token_type": "Bearer",
                                    "expires_in": 600, "refresh_token": format!("refresh-{n}"),
                                    "id_token": id,
                                })
                                .to_string(),
                            )
                        } else {
                            (
                                400,
                                json!({ "error": "unsupported_grant_type" }).to_string(),
                            )
                        }
                    } else if path.starts_with("/end-session") {
                        (200, "{}".to_owned())
                    } else {
                        (404, "{}".to_owned())
                    };
                    let reason = if code == 200 { "OK" } else { "Error" };
                    let answer = format!(
                        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                        answer.len()
                    );
                    let _ = s.write_all(answer.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        Self {
            issuer,
            served,
            codes: std::sync::Arc::default(),
            issued,
        }
    }

    /// `globals.api` entries: `auth: oauth2` for this provider, found through its discovery
    /// document, audience `cdc`, the roles in `roles`, and the console's client `cdc-console`.
    pub fn api(&self) -> String {
        format!(
            r#", auth: oauth2, oauth2: {{ issuer: "{i}", audience: cdc, discovery_url: "{i}/.well-known/openid-configuration", roles_claim: roles, client_id: cdc-console }}"#,
            i = self.issuer
        )
    }

    /// The provider's approval of the authorization request at `location`: where it sends the
    /// browser back, with a code and the request's state.
    pub fn approve(&self, location: &str) -> String {
        assert!(
            location.starts_with(&format!("{}/authorize?", self.issuer)),
            "{location}"
        );
        assert_eq!(
            query_value(location, "code_challenge_method").as_deref(),
            Some("S256")
        );
        let back = query_value(location, "redirect_uri").expect("a redirect URI");
        let state = query_value(location, "state").expect("a state");
        let challenge = query_value(location, "code_challenge").expect("a code challenge");
        let n = self.codes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.issued
            .lock()
            .unwrap()
            .insert(format!("code-{n}"), (challenge, back.clone()));
        format!("{back}?code=code-{n}&state={state}")
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.served.abort();
    }
}

/// A certificate for `127.0.0.1` and its key, written to `dir`: the `globals.api` entry that
/// serves them, and the certificate to trust.
pub fn tls_files(dir: &Path) -> (String, CertificateDer<'static>) {
    let c = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let (cert, key) = (dir.join("api.crt"), dir.join("api.key"));
    std::fs::write(&cert, c.cert.pem()).unwrap();
    std::fs::write(&key, c.signing_key.serialize_pem()).unwrap();
    let entry = format!(
        r#", tls: {{ enabled: true, cert: "{}", key: "{}" }}"#,
        cert.display(),
        key.display()
    );
    (entry, c.cert.der().clone())
}
