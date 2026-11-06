//! A fake OTLP/HTTP collector (config §6.6), on a thread of its own: it answers every request
//! 200 (or the status it is given) and hands over its path, headers and body. Plain HTTP, or TLS
//! with a certificate the test makes ([`otlp_collector_tls`]).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, mpsc};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// One export the collector took.
#[derive(Debug, Clone)]
pub struct OtlpRequest {
    pub path: String,
    /// Each header, its name lowercased, its value trimmed, in the order sent.
    pub headers: Vec<(String, String)>,
    /// The protobuf body: span names and attribute values are in it verbatim.
    pub body: Vec<u8>,
}

impl OtlpRequest {
    /// Whether `text` is in the body.
    #[must_use]
    pub fn has(&self, text: &str) -> bool {
        self.body.windows(text.len()).any(|w| w == text.as_bytes())
    }

    /// The value of header `name` (lowercase).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
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
            if !answer(conn.unwrap(), &status, &tx) {
                return;
            }
        }
    });
    (url, rx)
}

/// A collector over TLS with the certificate `cert_pem` (for `localhost`) and its key: its base
/// URL, `https://localhost:<port>`, and the requests it takes. A connection whose handshake fails
/// (a client that doesn't trust the certificate) is dropped.
///
/// # Panics
/// As [`otlp_collector`], and when the certificate or key doesn't parse.
#[must_use]
pub fn otlp_collector_tls(cert_pem: &str, key_pem: &str) -> (String, mpsc::Receiver<OtlpRequest>) {
    let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .unwrap();
    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let (tx, rx) = mpsc::channel();
    let status = Arc::new(AtomicU16::new(200));
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let tls = rustls::ServerConnection::new(Arc::clone(&config)).unwrap();
            let mut stream = rustls::StreamOwned::new(tls, conn.unwrap());
            // The handshake runs on the first read: a client that refuses it ends here.
            if stream.conn.complete_io(&mut stream.sock).is_err() {
                continue;
            }
            if !answer(stream, &status, &tx) {
                return;
            }
        }
    });
    (url, rx)
}

/// Reads one request from `conn`, answers it, and hands it over; false once nobody takes them.
fn answer<S: Read + Write>(
    mut conn: S,
    status: &AtomicU16,
    tx: &mpsc::Sender<OtlpRequest>,
) -> bool {
    let (path, headers, body) = {
        let mut reader = BufReader::new(&mut conn);
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return true;
        }
        let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
        let mut headers = vec![];
        let mut length = 0;
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            if header == "\r\n" || header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                let (name, value) = (name.trim().to_ascii_lowercase(), value.trim().to_owned());
                if name == "content-length" {
                    length = value.parse().unwrap();
                }
                headers.push((name, value));
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        (path, headers, body)
    };
    let status = status.load(Ordering::Relaxed);
    conn.write_all(format!("HTTP/1.1 {status} S\r\ncontent-length: 0\r\n\r\n").as_bytes())
        .unwrap();
    let _ = conn.flush();
    tx.send(OtlpRequest {
        path,
        headers,
        body,
    })
    .is_ok()
}
