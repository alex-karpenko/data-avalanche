//! The listener on `globals.api.address`: plain HTTP, or TLS with `globals.api.tls` (config
//! §6.3.2). Both are PROC: a reload changes neither. A TLS handshake runs off the accept loop,
//! bounded by [`HANDSHAKE_TIMEOUT`], so a client that never finishes it holds up no other. At
//! most [`MAX_CONNECTIONS`] are open at once, handshakes included (DA-API-17): past it, a new
//! connection waits in the socket's backlog until one closes. A connection that reads and writes
//! nothing for [`IDLE_TIMEOUT`] is closed, so silent ones can't hold every place.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::serve::Listener;
use data_avalanche_config::{HostPort, TlsIdentity};
use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::{Instant, Sleep};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// How long a client has to finish its TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The most connections open at once (DA-API-17).
pub const MAX_CONNECTIONS: usize = 1024;

/// A connection that reads and writes nothing for this long is closed, giving its place back
/// (DA-API-17): a silent connection, a keep-alive one between requests, or a request whose
/// handler answers nothing for as long.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

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
    /// [`MAX_CONNECTIONS`]; a test lowers it.
    max_connections: usize,
    /// [`IDLE_TIMEOUT`]; a test lowers it.
    idle_timeout: Duration,
}

impl std::fmt::Debug for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bound")
            .field("tcp", &self.tcp)
            .field("tls", &self.tls.is_some())
            .field("max_connections", &self.max_connections)
            .field("idle_timeout", &self.idle_timeout)
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
    Ok(Bound {
        tcp,
        tls,
        max_connections: MAX_CONNECTIONS,
        idle_timeout: IDLE_TIMEOUT,
    })
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
        let open = Arc::new(Semaphore::new(self.max_connections));
        match self.tls {
            None => {
                let listener = Capped {
                    tcp: self.tcp,
                    open,
                    idle: self.idle_timeout,
                };
                axum::serve(listener, router)
                    .with_graceful_shutdown(stop)
                    .await
            }
            Some(tls) => {
                let listener = TlsListener::start(self.tcp, tls, open, self.idle_timeout)?;
                axum::serve(listener, router)
                    .with_graceful_shutdown(stop)
                    .await
            }
        }
    }
}

/// A connection with its place among the open ones, given back when it closes. A read or a
/// write that waits once nothing moved for `idle` fails, and the server drops the connection.
pub(crate) struct Counted<S> {
    io: S,
    _open: OwnedSemaphorePermit,
    idle: Duration,
    deadline: Pin<Box<Sleep>>,
}

impl<S> Counted<S> {
    fn new(io: S, open: OwnedSemaphorePermit, idle: Duration) -> Self {
        Self {
            io,
            _open: open,
            idle,
            deadline: Box::pin(tokio::time::sleep(idle)),
        }
    }

    /// Something moved: the deadline starts again.
    fn moved(&mut self) {
        let next = Instant::now() + self.idle;
        self.deadline.as_mut().reset(next);
    }

    /// The inner stream waits: past the deadline, the wait fails instead.
    fn waits<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        match self.deadline.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the connection was idle too long",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn after<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match polled {
            Poll::Pending => self.waits(cx),
            ready => {
                self.moved();
                ready
            }
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Counted<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.io).poll_read(cx, buf);
        this.after(cx, polled)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Counted<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.io).poll_write(cx, buf);
        this.after(cx, polled)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.io).poll_flush(cx);
        this.after(cx, polled)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        this.after(cx, polled)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}

/// The plain listener, capped: it accepts only while a place is free.
struct Capped {
    tcp: TcpListener,
    open: Arc<Semaphore>,
    idle: Duration,
}

impl Listener for Capped {
    type Io = Counted<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let permit = Arc::clone(&self.open)
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");
        let (io, peer) = Listener::accept(&mut self.tcp).await;
        (Counted::new(io, permit, self.idle), peer)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.tcp.local_addr()
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
    done: mpsc::Receiver<(Counted<TlsStream<TcpStream>>, SocketAddr)>,
}

impl TlsListener {
    fn start(
        tcp: TcpListener,
        tls: TlsAcceptor,
        open: Arc<Semaphore>,
        idle: Duration,
    ) -> io::Result<Self> {
        let addr = tcp.local_addr()?;
        let (tx, done) = mpsc::channel(64);
        tokio::spawn(async move {
            let mut tcp = tcp;
            loop {
                // A place first, handshakes included; then the accept, retried after a pause
                // as axum's plain listener does. Ends once the server is done with the
                // listener, so the port is let go at once.
                let permit = tokio::select! {
                    permit = Arc::clone(&open).acquire_owned() => {
                        permit.expect("the semaphore is never closed")
                    }
                    () = tx.closed() => return,
                };
                let (stream, peer) = tokio::select! {
                    accepted = Listener::accept(&mut tcp) => accepted,
                    () = tx.closed() => return,
                };
                let (tls, handed) = (tls.clone(), tx.clone());
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await {
                        Ok(Ok(io)) => {
                            let _ = handed.send((Counted::new(io, permit, idle), peer)).await;
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
    type Io = Counted<TlsStream<TcpStream>>;
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

    /// DA-API-17: past the cap, a new connection waits until an open one closes; plain and TLS
    /// alike, a TLS handshake holding a place.
    #[tokio::test]
    async fn past_the_cap_a_connection_waits_for_one_to_close() {
        let (id, _) = identity();
        for tls in [None, Some(&id)] {
            let mut bound = bind(
                &HostPort {
                    host: "127.0.0.1".into(),
                    port: 0,
                },
                tls,
            )
            .await
            .unwrap();
            bound.max_connections = 2;
            let addr = bound.local_addr().unwrap();
            let api = crate::test_support::api(crate::test_support::open());
            tokio::spawn(bound.serve(router(api), std::future::pending()));
            // Two connections that say nothing: a TLS one holds its place in the handshake.
            let held = [
                TcpStream::connect(addr).await.unwrap(),
                TcpStream::connect(addr).await.unwrap(),
            ];
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut third = TcpStream::connect(addr).await.unwrap();
            third
                .write_all(b"GET /health/live HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut byte = [0; 1];
            let waited =
                tokio::time::timeout(Duration::from_millis(300), third.read(&mut byte)).await;
            assert!(
                waited.is_err(),
                "tls {}: the third is answered at once",
                tls.is_some()
            );
            drop(held);
            let answered = tokio::time::timeout(Duration::from_secs(2), third.read(&mut byte))
                .await
                .expect("the third is taken once one closes");
            if tls.is_none() {
                assert_eq!(answered.unwrap(), 1, "plain: an answer");
            }
        }
    }

    /// A TLS client trusting `der`, its handshake done.
    async fn tls_client(
        addr: SocketAddr,
        der: &CertificateDer<'static>,
    ) -> tokio_rustls::client::TlsStream<TcpStream> {
        let mut roots = RootCertStore::empty();
        roots.add(der.clone()).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let tcp = TcpStream::connect(addr).await.unwrap();
        TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap()
    }

    /// Reads one `/health/live` answer off a kept-alive connection.
    async fn live(s: &mut (impl AsyncRead + AsyncWrite + Unpin)) -> String {
        s.write_all(b"GET /health/live HTTP/1.1\r\nHost: t\r\n\r\n")
            .await
            .unwrap();
        let mut out = vec![];
        let mut chunk = [0; 512];
        while !out.ends_with(b"live\n") {
            let n = s.read(&mut chunk).await.unwrap();
            assert!(
                n > 0,
                "closed mid-answer: {}",
                String::from_utf8_lossy(&out)
            );
            out.extend_from_slice(&chunk[..n]);
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    /// DA-API-17: a connection that reads and writes nothing for the idle timeout is closed and
    /// gives its place back: a silent one, plain or past its TLS handshake, and a kept-alive one
    /// between requests. Without it, silent connections would hold every place for good.
    #[tokio::test]
    async fn an_idle_connection_is_closed_and_gives_its_place_back() {
        let (id, der) = identity();
        for tls in [None, Some(&id)] {
            let mut bound = bind(
                &HostPort {
                    host: "127.0.0.1".into(),
                    port: 0,
                },
                tls,
            )
            .await
            .unwrap();
            bound.max_connections = 1;
            bound.idle_timeout = Duration::from_millis(300);
            let addr = bound.local_addr().unwrap();
            let api = crate::test_support::api(crate::test_support::open());
            tokio::spawn(bound.serve(router(api), std::future::pending()));
            let what = if tls.is_some() { "tls" } else { "plain" };

            // The one place, held by a connection that says nothing.
            let mut held: Box<dyn AsyncRead + Unpin + Send> = match tls {
                None => Box::new(TcpStream::connect(addr).await.unwrap()),
                Some(_) => Box::new(tls_client(addr, &der).await),
            };
            let mut byte = [0; 1];
            let closed = tokio::time::timeout(Duration::from_secs(3), held.read(&mut byte))
                .await
                .unwrap_or_else(|_| panic!("{what}: the silent connection is still open"));
            assert!(matches!(closed, Ok(0) | Err(_)), "{what}: {closed:?}");

            // Its place is free: the next connection is answered, then closed once it idles.
            let answered = tokio::time::timeout(Duration::from_secs(3), async {
                match tls {
                    None => {
                        let mut s = TcpStream::connect(addr).await.unwrap();
                        let answer = live(&mut s).await;
                        (answer, Box::new(s) as Box<dyn AsyncRead + Unpin + Send>)
                    }
                    Some(_) => {
                        let mut s = tls_client(addr, &der).await;
                        let answer = live(&mut s).await;
                        (answer, Box::new(s) as Box<dyn AsyncRead + Unpin + Send>)
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{what}: the place wasn't given back"));
            let (answer, mut kept) = answered;
            assert!(answer.starts_with("HTTP/1.1 200"), "{what}: {answer}");
            let closed = tokio::time::timeout(Duration::from_secs(3), kept.read(&mut byte))
                .await
                .unwrap_or_else(|_| panic!("{what}: the kept-alive connection is still open"));
            assert!(matches!(closed, Ok(0) | Err(_)), "{what}: {closed:?}");
        }
    }

    /// A connection in use isn't cut: requests more often than the idle timeout keep it open
    /// well past it.
    #[tokio::test]
    async fn a_connection_in_use_outlives_the_idle_timeout() {
        let mut bound = bind(
            &HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            None,
        )
        .await
        .unwrap();
        bound.idle_timeout = Duration::from_millis(300);
        let addr = bound.local_addr().unwrap();
        let api = crate::test_support::api(crate::test_support::open());
        tokio::spawn(bound.serve(router(api), std::future::pending()));
        let mut s = TcpStream::connect(addr).await.unwrap();
        for _ in 0..8 {
            let answer = live(&mut s).await;
            assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
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
