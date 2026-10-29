//! A fake OTLP/HTTP collector (config §6.6), on a thread of its own: it answers every request
//! 200 (or the status it is given) and hands over its path and body.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, mpsc};

/// One export the collector took.
#[derive(Debug, Clone)]
pub struct OtlpRequest {
    pub path: String,
    /// The protobuf body: span names and attribute values are in it verbatim.
    pub body: Vec<u8>,
}

impl OtlpRequest {
    /// Whether `text` is in the body.
    #[must_use]
    pub fn has(&self, text: &str) -> bool {
        self.body.windows(text.len()).any(|w| w == text.as_bytes())
    }
}

/// A collector on `127.0.0.1`: its base URL and the requests it takes.
///
/// # Panics
/// When it can't bind a port, or a request isn't HTTP/1.1 with a `content-length`.
#[must_use]
pub fn otlp_collector() -> (String, mpsc::Receiver<OtlpRequest>) {
    otlp_collector_answering(Arc::new(AtomicU16::new(200)))
}

/// A collector that answers each request with the HTTP status `status` holds at the time.
///
/// # Panics
/// As [`otlp_collector`].
#[must_use]
pub fn otlp_collector_answering(status: Arc<AtomicU16>) -> (String, mpsc::Receiver<OtlpRequest>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let mut conn = conn.unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header == "\r\n" {
                    break;
                }
                if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let status = status.load(Ordering::Relaxed);
            conn.write_all(format!("HTTP/1.1 {status} S\r\ncontent-length: 0\r\n\r\n").as_bytes())
                .unwrap();
            if tx.send(OtlpRequest { path, body }).is_err() {
                return;
            }
        }
    });
    (url, rx)
}
