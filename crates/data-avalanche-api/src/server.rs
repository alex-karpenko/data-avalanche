//! The listener on `globals.api.address`: plain HTTP, or TLS with `globals.api.tls` (config
//! §6.3.2). Both are PROC: a reload changes neither. A TLS handshake runs off the accept loop,
//! bounded by [`HANDSHAKE_TIMEOUT`], so a client that never finishes it holds up no other.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::serve::Listener;
use data_avalanche_config::{HostPort, TlsIdentity};
use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// How long a client has to finish its TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum ServeError {
    #[error("globals.api.address {address} can't be bound: {source}")]
    Bind {
        address: String,
        #[source]
        source: io::Error,
    },
    #[error("globals.api.tls: {0}")]
    Tls(String),
}

/// The bound listener: plain, or TLS.
pub struct Bound {
    tcp: TcpListener,
    tls: Option<TlsAcceptor>,
}

impl std::fmt::Debug for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bound")
            .field("tcp", &self.tcp)
            .field("tls", &self.tls.is_some())
            .finish()
    }
}

/// Binds `address`, with TLS when `tls` is set.
///
/// # Errors
/// The address can't be bound, or the certificate or key isn't usable.
pub async fn bind(address: &HostPort, tls: Option<&TlsIdentity>) -> Result<Bound, ServeError> {
    let tls = tls.map(acceptor).transpose()?;
    let tcp = TcpListener::bind((address.host.as_str(), address.port))
        .await
        .map_err(|source| ServeError::Bind {
            address: format!("{}:{}", address.host, address.port),
            source,
        })?;
    Ok(Bound { tcp, tls })
}

impl Bound {
    /// # Errors
    /// The socket's address can't be read.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }

    /// Serves `router` until `stop` resolves, then lets the open requests finish.
    ///
    /// # Errors
    /// The server failed.
    pub async fn serve(
        self,
        router: Router,
        stop: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        match self.tls {
            None => {
                axum::serve(self.tcp, router)
                    .with_graceful_shutdown(stop)
                    .await
            }
            Some(tls) => {
                let listener = TlsListener::start(self.tcp, tls)?;
                axum::serve(listener, router)
                    .with_graceful_shutdown(stop)
                    .await
            }
        }
    }
}

/// The server's TLS setup: HTTP/1.1 only, as the listener serves it.
fn acceptor(id: &TlsIdentity) -> Result<TlsAcceptor, ServeError> {
    let certs = CertificateDer::pem_slice_iter(id.cert.text.expose().as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServeError::Tls(format!("cert: {e}")))?;
    if certs.is_empty() {
        return Err(ServeError::Tls("cert: no certificate in the PEM".into()));
    }
    let key = PrivateKeyDer::from_pem_slice(id.key.text.expose().as_bytes())
        .map_err(|e| ServeError::Tls(format!("key: {e}")))?;
    // The provider is named, not the process default: two are linked (DA-DEP-5).
    let mut config = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| ServeError::Tls(e.to_string()))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| ServeError::Tls(e.to_string()))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Accepted connections, each handed over once its handshake is done.
struct TlsListener {
    addr: SocketAddr,
    done: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
}

impl TlsListener {
    fn start(tcp: TcpListener, tls: TlsAcceptor) -> io::Result<Self> {
        let addr = tcp.local_addr()?;
        let (tx, done) = mpsc::channel(64);
        tokio::spawn(async move {
            let mut tcp = tcp;
            loop {
                // Retries a failed accept after a pause, as axum's plain listener does. Ends
                // once the server is done with the listener, so the port is let go at once.
                let (stream, peer) = tokio::select! {
                    accepted = Listener::accept(&mut tcp) => accepted,
                    () = tx.closed() => return,
                };
                let (tls, handed) = (tls.clone(), tx.clone());
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await {
                        Ok(Ok(stream)) => {
                            let _ = handed.send((stream, peer)).await;
                        }
                        Ok(Err(e)) => tracing::debug!(%peer, "TLS handshake failed: {e}"),
                        Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
                    }
                });
            }
        });
        Ok(Self { addr, done })
    }
}

impl Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.done.recv().await {
            Some(conn) => conn,
            // The accept loop ended: nothing more comes.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.addr)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, OnceLock};

    use data_avalanche_config::{FingerprintKey, Origin, Pem, Secret};
    use data_avalanche_telemetry::Metrics;
    use rustls::RootCertStore;
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::TlsConnector;

    use super::*;
    use crate::{Api, router};

    /// A throwaway certificate for `localhost`, generated at run time.
    fn identity() -> (TlsIdentity, CertificateDer<'static>) {
        let c = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = FingerprintKey::from_bytes([1; 32]);
        let pem = |text: String| Pem {
            origin: Origin::Inline,
            text: Secret::new(text, &key),
        };
        let der = c.cert.der().clone();
        (
            TlsIdentity {
                cert: pem(c.cert.pem()),
                key: pem(c.signing_key.serialize_pem()),
            },
            der,
        )
    }

    async fn plain(addr: SocketAddr) -> String {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /health/live HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = vec![];
        let _ = s.read_to_end(&mut out).await;
        String::from_utf8_lossy(&out).into_owned()
    }

    #[tokio::test]
    async fn with_tls_the_listener_serves_only_tls() {
        let (id, der) = identity();
        let bound = bind(
            &HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            Some(&id),
        )
        .await
        .unwrap();
        let addr = bound.local_addr().unwrap();
        let api = Api {
            metrics: Metrics::new(),
            ready: Arc::new(AtomicBool::new(true)),
            node: Arc::new(OnceLock::new()),
            auth: crate::test_support::open(),
        };
        tokio::spawn(bound.serve(router(api), std::future::pending()));

        let mut roots = RootCertStore::empty();
        roots.add(der).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut tls = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        tls.write_all(b"GET /health/live HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = String::new();
        tls.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(out.ends_with("live\n"), "{out}");

        let answer = plain(addr).await;
        assert!(!answer.contains("HTTP/1.1 200"), "{answer}");
    }

    /// Once the server stops, the accept loop ends with it: the port is free without another
    /// connection coming.
    #[tokio::test]
    async fn a_stopped_tls_listener_releases_its_port() {
        let (id, _) = identity();
        let bound = bind(
            &HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            Some(&id),
        )
        .await
        .unwrap();
        let addr = bound.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let api = crate::test_support::api(crate::test_support::open());
        let served = tokio::spawn(bound.serve(router(api), async {
            let _ = stopped.await;
        }));
        stop.send(()).unwrap();
        served.await.unwrap().unwrap();
        let mut free = false;
        for _ in 0..100 {
            if TcpListener::bind(addr).await.is_ok() {
                free = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(free, "{addr} is still bound");
    }

    #[tokio::test]
    async fn a_cert_without_a_certificate_is_refused_at_bind() {
        let (mut id, _) = identity();
        id.cert = id.key.clone();
        let err = bind(
            &HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            Some(&id),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "globals.api.tls: cert: no certificate in the PEM"
        );
    }

    #[tokio::test]
    async fn a_key_that_isnt_one_is_refused_at_bind() {
        let (mut id, _) = identity();
        id.key = id.cert.clone();
        let err = bind(
            &HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            Some(&id),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().starts_with("globals.api.tls: key"), "{err}");
    }
}
