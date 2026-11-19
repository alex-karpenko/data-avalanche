//! Another node's API, asked on a caller's behalf (Admin_Console §10, D-M13b.4): a node's live
//! detail, and the actions only the node itself takes (leave, shut down, check and reload its own
//! file). The request carries the caller's own credentials and nothing of this node's, so the
//! node it reaches verifies the caller, and records the action, as if the caller had come
//! straight to it: no node-to-node trust. It names its target, so a node that isn't it (an
//! address since reused) refuses it, and it is never forwarded again.

use std::time::Duration;

use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use data_avalanche_config::TlsIdentity;
use serde_json::json;

use crate::body;

/// The node a forwarded request is for: a node that isn't it answers `421`.
pub const TARGET: &str = "x-data-avalanche-target";
/// The node that forwarded a request: a request carrying it is never forwarded again.
pub const FORWARDED_BY: &str = "x-data-avalanche-forwarded-by";
/// On a forwarded answer: the node that gave it.
pub const ANSWERED_BY: &str = "x-data-avalanche-node";

/// How long a node's detail may take (Admin_Console §8: each source its own limit).
pub const DETAIL_LIMIT: Duration = Duration::from_secs(3);
/// How long a node action may take to be answered (the action itself goes on after).
pub const ACTION_LIMIT: Duration = Duration::from_secs(10);
/// How long a node's config status may take, asked of every node at once.
pub const STATUS_LIMIT: Duration = Duration::from_secs(2);
/// The largest answer passed back.
pub const MAX_ANSWER: usize = 8 << 20;

/// The client every forwarded request goes through.
#[derive(Debug, Clone)]
pub struct Peers {
    pub(crate) me: String,
    pub(crate) http: reqwest::Client,
}

/// Why the peers' client can't be built.
#[derive(Debug, thiserror::Error)]
pub enum PeersError {
    /// This node's `api.tls.cert` isn't PEM.
    #[error("the API certificate as a peer's trust: {0}")]
    Certificate(String),
    /// The HTTP client itself.
    #[error("the peers' HTTP client: {0}")]
    Client(String),
}

/// A request to forward: the caller's method, credentials, content type and body.
#[derive(Clone)]
pub(crate) struct Asked {
    pub method: Method,
    pub authorization: Option<HeaderValue>,
    pub content_type: Option<HeaderValue>,
    pub body: Bytes,
}

/// The caller's credentials never print.
impl std::fmt::Debug for Asked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Asked")
            .field("method", &self.method)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("content_type", &self.content_type)
            .field("body", &format_args!("{} bytes", self.body.len()))
            .finish()
    }
}

impl Asked {
    /// What the caller sent, for forwarding.
    pub(crate) fn from(method: Method, headers: &HeaderMap, body: Bytes) -> Self {
        Self {
            method,
            authorization: headers.get(header::AUTHORIZATION).cloned(),
            content_type: headers.get(header::CONTENT_TYPE).cloned(),
            body,
        }
    }
}

impl Peers {
    /// The client of node `me`. Over HTTPS it trusts the platform's roots and every certificate
    /// of this node's own `api.tls.cert` (the cluster's nodes usually share a CA), with the usual
    /// hostname checks; redirects and proxies are not followed. A node with `api.tls` forwards
    /// over HTTPS only: a caller's credentials never go in clear to a peer that gossips an
    /// `http://` address (one not yet restarted with TLS, say), which answers `502` instead.
    ///
    /// # Errors
    /// [`PeersError`]: the certificate isn't PEM, or the client can't be built.
    pub fn new(me: impl Into<String>, tls: Option<&TlsIdentity>) -> Result<Self, PeersError> {
        let mut b = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .https_only(tls.is_some());
        if let Some(tls) = tls {
            let certs = reqwest::Certificate::from_pem_bundle(tls.cert.text.expose().as_bytes())
                .map_err(|e| PeersError::Certificate(e.to_string()))?;
            b = b.tls_certs_merge(certs);
        }
        Ok(Self {
            me: me.into(),
            http: b.build().map_err(|e| PeersError::Client(e.to_string()))?,
        })
    }

    /// Sends `asked` to `path_and_query` on node `node` at `api_url`, within `limit`: its answer
    /// as it gave it (its body, and its `Content-Type`, `Retry-After` and `WWW-Authenticate`,
    /// with [`ANSWERED_BY`]), or `502` when it can't be reached or answers over [`MAX_ANSWER`]
    /// bytes, `504` past the limit.
    pub(crate) async fn send(
        &self,
        node: &str,
        api_url: &str,
        path_and_query: &str,
        asked: Asked,
        limit: Duration,
    ) -> Response {
        let mut req = self
            .http
            .request(asked.method, format!("{api_url}{path_and_query}"))
            .timeout(limit)
            .header(TARGET, node)
            .header(FORWARDED_BY, &self.me)
            .body(asked.body);
        if let Some(a) = asked.authorization {
            req = req.header(header::AUTHORIZATION, a);
        }
        if let Some(c) = asked.content_type {
            req = req.header(header::CONTENT_TYPE, c);
        }
        let unanswered =
            |code: StatusCode, why: String| body(code, &json!({ "error": why, "node": node }));
        let answer = match req.send().await {
            Ok(a) => a,
            Err(e) if e.is_timeout() => {
                return unanswered(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("{node} didn't answer within {limit:?}"),
                );
            }
            Err(e) => {
                return unanswered(
                    StatusCode::BAD_GATEWAY,
                    format!("{node} didn't answer: {}", cause(&e)),
                );
            }
        };
        let code = answer.status();
        let kept: Vec<(header::HeaderName, HeaderValue)> = [
            header::CONTENT_TYPE,
            header::RETRY_AFTER,
            header::WWW_AUTHENTICATE,
        ]
        .into_iter()
        .flat_map(|name| {
            answer
                .headers()
                .get_all(&name)
                .iter()
                .map(|v| (name.clone(), v.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
        let over = || {
            unanswered(
                StatusCode::BAD_GATEWAY,
                format!("{node}'s answer is over {MAX_ANSWER} bytes"),
            )
        };
        if answer
            .content_length()
            .is_some_and(|n| n > MAX_ANSWER as u64)
        {
            return over();
        }
        // Read a chunk at a time against the cap: an answer without a length is never held
        // whole past it.
        let mut answer = answer;
        let mut bytes = Vec::new();
        loop {
            match answer.chunk().await {
                Ok(Some(chunk)) => {
                    if bytes.len() + chunk.len() > MAX_ANSWER {
                        return over();
                    }
                    bytes.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Err(e) if e.is_timeout() => {
                    return unanswered(
                        StatusCode::GATEWAY_TIMEOUT,
                        format!("{node} didn't answer within {limit:?}"),
                    );
                }
                Err(e) => {
                    return unanswered(
                        StatusCode::BAD_GATEWAY,
                        format!("{node}'s answer: {}", cause(&e)),
                    );
                }
            }
        }
        let mut r = (code, Bytes::from(bytes)).into_response();
        // The target's own, in place of the body's default type.
        for (name, _) in &kept {
            r.headers_mut().remove(name);
        }
        for (name, value) in kept {
            r.headers_mut().append(name, value);
        }
        if let Ok(n) = HeaderValue::from_str(node) {
            r.headers_mut().insert(ANSWERED_BY, n);
        }
        r
    }
}

/// The innermost cause: reqwest's own message says only "error sending request".
pub(crate) fn cause(e: &(dyn std::error::Error + 'static)) -> String {
    let mut inner = e;
    while let Some(next) = inner.source() {
        inner = next;
    }
    inner.to_string()
}
