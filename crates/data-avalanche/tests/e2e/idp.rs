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
