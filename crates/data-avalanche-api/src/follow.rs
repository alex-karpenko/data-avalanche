//! Another node's live stream, followed on a caller's behalf (M13f, D-M13f.5): the request
//! carries the caller's own credentials, names its target, and says which node forwarded it, so
//! the node it reaches sends its own messages alone and follows none. Its messages are read one
//! at a time, each at most [`MAX_FRAME`] bytes.

use std::time::Duration;

use axum::http::{HeaderValue, header};

use crate::peers::{DETAIL_LIMIT, FORWARDED_BY, Peers, TARGET};

/// The largest message read from a followed stream.
pub const MAX_FRAME: usize = 1 << 20;
/// How long a followed stream may send nothing, its keep-alive included, before it is dropped.
pub const FOLLOW_SILENCE: Duration = Duration::from_secs(45);

/// One message: its kind (`event:`) and its data, its `data:` lines joined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Frame {
    pub kind: String,
    pub data: String,
}

/// Where the first message in `buffer` ends, just past its blank line. A line ends at CR LF, CR
/// or LF; a blank line is a line ending right after another, or at the buffer's start. A CR
/// that ends the buffer after a line's text may still be followed by its LF, so it isn't yet an
/// end; after a blank line's start it is one (a leftover LF is an empty message, skipped).
fn message_end(buffer: &[u8]) -> Option<usize> {
    let (mut i, mut at_start) = (0, true);
    while i < buffer.len() {
        let width = match buffer[i] {
            b'\r' if buffer.get(i + 1) == Some(&b'\n') => 2,
            b'\r' if i + 1 == buffer.len() && !at_start => return None,
            b'\r' | b'\n' => 1,
            _ => {
                at_start = false;
                i += 1;
                continue;
            }
        };
        i += width;
        if at_start {
            return Some(i);
        }
        at_start = true;
    }
    None
}

/// The messages of a stream's bytes, as they come.
#[derive(Debug, Default)]
pub(crate) struct Frames {
    buffer: Vec<u8>,
}

impl Frames {
    /// Adds `bytes` read.
    pub(crate) fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// The next whole message, comments and messages without a kind skipped; an error when the
    /// one being read is over [`MAX_FRAME`] or isn't UTF-8.
    pub(crate) fn next(&mut self) -> Result<Option<Frame>, String> {
        loop {
            let Some(end) = message_end(&self.buffer) else {
                if self.buffer.len() > MAX_FRAME {
                    return Err(format!("a message over {MAX_FRAME} bytes"));
                }
                return Ok(None);
            };
            if end > MAX_FRAME + 2 {
                return Err(format!("a message over {MAX_FRAME} bytes"));
            }
            let raw: Vec<u8> = self.buffer.drain(..end).collect();
            let text = std::str::from_utf8(&raw).map_err(|_| "a message that isn't UTF-8")?;
            let (mut kind, mut data) = (None, Vec::new());
            // A line ends at a CR, an LF or both: none is left in a kind or a datum.
            for line in text.split(['\r', '\n']) {
                if let Some(k) = line.strip_prefix("event:") {
                    kind = Some(k.trim().to_owned());
                } else if let Some(d) = line.strip_prefix("data:") {
                    data.push(d.strip_prefix(' ').unwrap_or(d));
                }
            }
            if let Some(kind) = kind.filter(|k| !k.is_empty()) {
                return Ok(Some(Frame {
                    kind,
                    data: data.join("\n"),
                }));
            }
        }
    }
}

/// A node's stream, followed.
#[derive(Debug)]
pub(crate) struct Followed {
    answer: reqwest::Response,
    frames: Frames,
    silence: Duration,
}

impl Followed {
    /// The next message; `Ok(None)` when the stream ended, an error when it failed, sent a
    /// message over [`MAX_FRAME`], or sent nothing for its silence.
    pub(crate) async fn next(&mut self) -> Result<Option<Frame>, String> {
        loop {
            if let Some(f) = self.frames.next()? {
                return Ok(Some(f));
            }
            match tokio::time::timeout(self.silence, self.answer.chunk()).await {
                Err(_) => return Err(format!("it sent nothing for {:?}", self.silence)),
                Ok(Err(e)) => return Err(crate::peers::cause(&e)),
                Ok(Ok(None)) => return Ok(None),
                Ok(Ok(Some(bytes))) => self.frames.push(&bytes),
            }
        }
    }
}

impl Peers {
    /// Opens node `node`'s stream at `api_url` + `path_and_query` with the caller's
    /// `authorization`, answered within [`DETAIL_LIMIT`]; dropped once it sends nothing for
    /// `silence`. An answer other than `200` is an error naming its code and its own.
    pub(crate) async fn follow(
        &self,
        node: &str,
        api_url: &str,
        path_and_query: &str,
        authorization: Option<HeaderValue>,
        silence: Duration,
    ) -> Result<Followed, String> {
        let mut req = self
            .http
            .get(format!("{api_url}{path_and_query}"))
            .header(TARGET, node)
            .header(FORWARDED_BY, &self.me)
            .header(header::ACCEPT, "text/event-stream");
        if let Some(a) = authorization {
            req = req.header(header::AUTHORIZATION, a);
        }
        let answer = match tokio::time::timeout(DETAIL_LIMIT, req.send()).await {
            Err(_) => return Err(format!("it didn't answer within {DETAIL_LIMIT:?}")),
            Ok(Err(e)) => return Err(crate::peers::cause(&e)),
            Ok(Ok(a)) => a,
        };
        let code = answer.status();
        if code != reqwest::StatusCode::OK {
            let said = tokio::time::timeout(DETAIL_LIMIT, answer.bytes())
                .await
                .ok()
                .and_then(Result::ok)
                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                .and_then(|v| v["error"].as_str().map(str::to_owned));
            return Err(match said {
                Some(e) => format!("it answered {}: {e}", code.as_u16()),
                None => format!("it answered {}", code.as_u16()),
            });
        }
        Ok(Followed {
            answer,
            frames: Frames::default(),
            silence,
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::http::HeaderMap;
    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use futures_util::stream;

    use super::*;

    fn frame(kind: &str, data: &str) -> Frame {
        Frame {
            kind: kind.into(),
            data: data.into(),
        }
    }

    #[test]
    fn a_message_is_read_once_whole_whatever_its_chunks() {
        let mut f = Frames::default();
        f.push(b": keep-alive\n\nevent: fig");
        assert_eq!(f.next(), Ok(None));
        f.push(b"ures\ndata: {\"n\":1}\n");
        assert_eq!(f.next(), Ok(None));
        f.push(b"\nevent: node\r\ndata: a\r\ndata: b\r\n\nid: 4\ndata: x\n\n");
        assert_eq!(f.next(), Ok(Some(frame("figures", "{\"n\":1}"))));
        assert_eq!(f.next(), Ok(Some(frame("node", "a\nb"))));
        assert_eq!(f.next(), Ok(None), "a message without a kind is skipped");
    }

    #[test]
    fn a_message_ends_at_a_blank_line_under_any_line_ending() {
        let mut f = Frames::default();
        f.push(b"event: a\r\ndata: 1\r\n\r\nevent: b\rdata: 2\r\revent: c\r\ndata: 3\n\n");
        assert_eq!(f.next(), Ok(Some(frame("a", "1"))));
        assert_eq!(f.next(), Ok(Some(frame("b", "2"))));
        assert_eq!(f.next(), Ok(Some(frame("c", "3"))));
        assert_eq!(f.next(), Ok(None));
        // Split between a CR and its LF, at the blank line and inside the message.
        let mut f = Frames::default();
        f.push(b"event: a\r\ndata: 1\r\n\r");
        assert_eq!(f.next(), Ok(Some(frame("a", "1"))));
        f.push(b"\nevent: b\r");
        assert_eq!(f.next(), Ok(None));
        f.push(b"\ndata: 2\r\n\r\n");
        assert_eq!(f.next(), Ok(Some(frame("b", "2"))));
        assert_eq!(f.next(), Ok(None));
    }

    #[test]
    fn a_message_over_the_cap_is_refused_as_it_is_read() {
        let mut f = Frames::default();
        f.push(&vec![b'x'; MAX_FRAME + 1]);
        assert_eq!(
            f.next(),
            Err(format!("a message over {MAX_FRAME} bytes")),
            "before its end comes"
        );
        let mut f = Frames::default();
        f.push(b"event: x\ndata: \xff\n\n");
        assert_eq!(f.next(), Err("a message that isn't UTF-8".into()));
    }

    /// A node whose stream sends `hello` and then nothing, keeping what each request carried.
    async fn quiet_node() -> (String, std::sync::Arc<std::sync::Mutex<Vec<HeaderMap>>>) {
        let seen = std::sync::Arc::<std::sync::Mutex<Vec<HeaderMap>>>::default();
        let kept = std::sync::Arc::clone(&seen);
        let app = Router::new().route(
            "/v1/stream",
            get(move |headers: HeaderMap| {
                kept.lock().unwrap().push(headers);
                async move {
                    let hello = Event::default().event("hello").data("{}");
                    let events = stream::iter([Ok::<_, std::convert::Infallible>(hello)])
                        .chain(stream::pending());
                    Sse::new(events)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, seen)
    }

    use futures_util::StreamExt;

    #[tokio::test]
    async fn a_followed_stream_carries_the_callers_credentials_and_is_dropped_once_silent() {
        let (url, seen) = quiet_node().await;
        let peers = Peers::new("node-a", None).unwrap();
        let silence = Duration::from_millis(300);
        let mut f = peers
            .follow(
                "node-b",
                &url,
                "/v1/stream?kind=node",
                Some(HeaderValue::from_static("Basic dmlld2VyOnZpZXdlcg==")),
                silence,
            )
            .await
            .unwrap();
        assert_eq!(f.next().await, Ok(Some(frame("hello", "{}"))));
        assert_eq!(
            f.next().await,
            Err(format!("it sent nothing for {silence:?}"))
        );
        let h = &seen.lock().unwrap()[0];
        assert_eq!(h[TARGET], "node-b");
        assert_eq!(h[FORWARDED_BY], "node-a");
        assert_eq!(h[header::AUTHORIZATION], "Basic dmlld2VyOnZpZXdlcg==");
    }

    #[tokio::test]
    async fn a_refusal_names_its_code_and_its_reason() {
        let (url, _log) = crate::test_support::fake_answering(
            Duration::ZERO,
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "no pipeline \"x\"" }),
        )
        .await;
        let peers = Peers::new("node-a", None).unwrap();
        let err = peers
            .follow("node-b", &url, "/v1/stream", None, FOLLOW_SILENCE)
            .await
            .unwrap_err();
        assert_eq!(err, "it answered 404: no pipeline \"x\"");
        let err = peers
            .follow(
                "node-b",
                "http://127.0.0.1:9",
                "/v1/stream",
                None,
                FOLLOW_SILENCE,
            )
            .await
            .unwrap_err();
        assert!(!err.is_empty(), "the connection's own cause");
    }
}
