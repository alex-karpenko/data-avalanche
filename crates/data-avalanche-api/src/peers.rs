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
    me: String,
    http: reqwest::Client,
}

/// A request to forward: the caller's method, credentials, content type and body.
#[derive(Debug, Clone)]
pub(crate) struct Asked {
    pub method: Method,
    pub authorization: Option<HeaderValue>,
    pub content_type: Option<HeaderValue>,
    pub body: Bytes,
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
    /// hostname checks; redirects and proxies are not followed.
    ///
    /// # Errors
    /// The certificate isn't PEM, or the client can't be built.
    pub fn new(me: impl Into<String>, tls: Option<&TlsIdentity>) -> Result<Self, String> {
        let mut b = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy();
        if let Some(tls) = tls {
            let certs = reqwest::Certificate::from_pem_bundle(tls.cert.text.expose().as_bytes())
                .map_err(|e| format!("the API certificate as a peer's trust: {e}"))?;
            b = b.tls_certs_merge(certs);
        }
        Ok(Self {
            me: me.into(),
            http: b
                .build()
                .map_err(|e| format!("the peers' HTTP client: {e}"))?,
        })
    }

    /// Sends `asked` to `path_and_query` on node `node` at `api_url`, within `limit`: its answer
    /// as it gave it (with [`ANSWERED_BY`]), or `502` when it can't be reached, `504` past the
    /// limit.
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
        let content_type = answer.headers().get(header::CONTENT_TYPE).cloned();
        if answer
            .content_length()
            .is_some_and(|n| n > MAX_ANSWER as u64)
        {
            return unanswered(
                StatusCode::BAD_GATEWAY,
                format!("{node}'s answer is over {MAX_ANSWER} bytes"),
            );
        }
        let bytes = match answer.bytes().await {
            Ok(b) if b.len() <= MAX_ANSWER => b,
            Ok(_) => {
                return unanswered(
                    StatusCode::BAD_GATEWAY,
                    format!("{node}'s answer is over {MAX_ANSWER} bytes"),
                );
            }
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
        };
        let mut r = (code, bytes).into_response();
        if let Some(c) = content_type {
            r.headers_mut().insert(header::CONTENT_TYPE, c);
        }
        if let Ok(n) = HeaderValue::from_str(node) {
            r.headers_mut().insert(ANSWERED_BY, n);
        }
        r
    }
}

/// The innermost cause: reqwest's own message says only "error sending request".
fn cause(e: &(dyn std::error::Error + 'static)) -> String {
    let mut inner = e;
    while let Some(next) = inner.source() {
        inner = next;
    }
    inner.to_string()
}
