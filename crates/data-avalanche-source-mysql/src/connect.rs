//! Connections to the source (config §4.4): options from the resolved `source`, TLS from the
//! `ssl_*` PEM text (in memory: `mysql_async` reads a buffer as well as a file), the session
//! settings every connection runs with, and the shared catalog connection.

use std::fmt;
use std::time::Duration;

use data_avalanche_config::{Source, SslMode};
use data_avalanche_core::SourceError;
use futures_util::future::BoxFuture;
use mysql_async::{ClientIdentity, Conn, DriverError, Error, Opts, OptsBuilder, SslOpts};
use tokio::sync::Mutex;

use crate::error::{mysql, timed_out};

/// What every connection runs first: UTC for `TIMESTAMP` values (a snapshot reads them as text in
/// the session time zone, the binlog carries them in UTC), and UTF-8 results whatever the server's
/// handshake default is.
pub const SESSION_SETUP: [&str; 2] = ["SET time_zone = '+00:00'", "SET NAMES utf8mb4"];

/// The `ssl_mode` → `mysql_async` TLS options (`allow` is refused for MySQL by V-030 and connects
/// like `prefer`). `require` and `prefer` encrypt without verifying, `verify-ca` checks the chain
/// and `verify-full` the chain and the host name. Roots are `ssl_ca`, else the Mozilla roots.
pub(crate) fn ssl_opts(source: &Source) -> Result<Option<SslOpts>, String> {
    if source.ssl_mode == SslMode::Disable {
        return Ok(None);
    }
    let mut opts = SslOpts::default();
    if let Some(ca) = &source.ssl_ca {
        opts = opts
            .with_root_certs(vec![ca.text.expose().as_bytes().to_vec().into()])
            .with_disable_built_in_roots(true);
    }
    match (&source.ssl_cert, &source.ssl_key) {
        (Some(cert), Some(key)) => {
            opts = opts.with_client_identity(Some(ClientIdentity::new(
                cert.text.expose().as_bytes().to_vec().into(),
                key.text.expose().as_bytes().to_vec().into(),
            )));
        }
        (None, None) => {}
        _ => return Err("ssl_cert and ssl_key go together".into()),
    }
    Ok(Some(match source.ssl_mode {
        SslMode::VerifyFull => opts,
        SslMode::VerifyCa => opts.with_danger_skip_domain_validation(true),
        _ => opts.with_danger_accept_invalid_certs(true),
    }))
}

/// What the source's connections need, built once from `source`. `Debug` leaves out the password.
pub(crate) struct Connector {
    opts: Opts,
    /// `prefer` falls back to a connection without TLS when the server offers none.
    prefer: bool,
    pub(crate) connect: Duration,
    pub(crate) command: Duration,
    catalog: Mutex<Option<Conn>>,
}

impl fmt::Debug for Connector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connector")
            .field("host", &self.opts.ip_or_hostname())
            .field("port", &self.opts.tcp_port())
            .field("user", &self.opts.user())
            .field("db", &self.opts.db_name())
            .field("tls", &self.opts.ssl_opts().is_some())
            .finish_non_exhaustive()
    }
}

impl Connector {
    pub(crate) fn new(source: &Source) -> Result<Self, SourceError> {
        let ssl = ssl_opts(source).map_err(SourceError::Permanent)?;
        let opts = OptsBuilder::default()
            .ip_or_hostname(source.host.clone())
            .tcp_port(source.port)
            .db_name(Some(source.db.clone()))
            .user(Some(source.credentials.user()))
            .pass(Some(source.credentials.password().expose()))
            .init(SESSION_SETUP.to_vec())
            .ssl_opts(ssl);
        Ok(Self {
            opts: opts.into(),
            prefer: matches!(source.ssl_mode, SslMode::Prefer | SslMode::Allow),
            connect: source.timeouts.connect,
            command: source.timeouts.command,
            catalog: Mutex::new(None),
        })
    }

    /// A new connection within `timeouts.connect`: a server that accepts the TCP connection but
    /// never answers the handshake would otherwise hold it forever (DA-SRC-16).
    pub(crate) async fn connect(&self) -> Result<Conn, SourceError> {
        let after = self.connect;
        tokio::time::timeout(after, self.open())
            .await
            .map_err(|_| timed_out("connect to the source", after))?
    }

    async fn open(&self) -> Result<Conn, SourceError> {
        match Conn::new(self.opts.clone()).await {
            Err(Error::Driver(DriverError::NoClientSslFlagFromServer)) if self.prefer => {
                let plain = OptsBuilder::from_opts(self.opts.clone()).ssl_opts(None);
                Conn::new(plain).await.map_err(|e| mysql(e, None))
            }
            other => other.map_err(|e| mysql(e, None)),
        }
    }

    /// Runs `f` on the shared catalog connection under `timeouts.command`, connecting first when
    /// there is none. The connection is out of the slot while `f` runs and goes back only when
    /// the call completes with a value or a non-fatal error: a call dropped before it completes
    /// (the caller's future cancelled) drops the connection with it, because its reply may be
    /// half-read or unread, and the next query on it would read that reply as its own (DA-SRC-27).
    /// A connection that failed fatally or timed out is dropped too: the next call opens another.
    pub(crate) async fn command<T>(
        &self,
        what: &str,
        f: impl for<'c> FnOnce(&'c mut Conn) -> BoxFuture<'c, mysql_async::Result<T>>,
    ) -> Result<T, SourceError> {
        let mut slot = self.catalog.lock().await;
        let mut conn = match slot.take() {
            Some(c) => c,
            None => self.connect().await?,
        };
        match tokio::time::timeout(self.command, f(&mut conn)).await {
            Ok(Ok(v)) => {
                *slot = Some(conn);
                Ok(v)
            }
            Ok(Err(e)) => {
                if !e.is_fatal() {
                    *slot = Some(conn);
                }
                Err(named(what, mysql(e, None)))
            }
            Err(_) => Err(timed_out(what, self.command)),
        }
    }
}

/// Names the call in a transient error. A permanent one is left as it is: a halt code is read
/// from the start of its message (`SourceError::halt_code`).
fn named(what: &str, e: SourceError) -> SourceError {
    match e {
        SourceError::Transient(m) => SourceError::Transient(format!("{what}: {m}")),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_config::{FingerprintKey, load_str};
    use data_avalanche_testkit::require_it;
    use mysql_async::prelude::Queryable;
    use std::collections::BTreeMap;

    /// A source with `ssl` (YAML flow entries) and `env` for its `${…}` references.
    fn source(ssl: &str, env: &[(&str, &str)]) -> Source {
        source_at("db.example.com", 3306, "shop", "cdc", "hunter2", ssl, env)
    }

    fn source_at(
        host: &str,
        port: u16,
        db: &str,
        user: &str,
        password: &str,
        ssl: &str,
        env: &[(&str, &str)],
    ) -> Source {
        let yaml = format!(
            r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      type: mysql
      host: {host}
      port: {port}
      db: {db}
      user: {user}
      password: {password}
      {ssl}
      mysql: {{}}
    tables:
      include: [t]
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#
        );
        let env: BTreeMap<String, String> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let loaded = load_str(&yaml, &env, &FingerprintKey::from_bytes([1; 32]))
            .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
        loaded.config.pipelines.into_values().next().unwrap().source
    }

    fn ca() -> String {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.self_signed(&key).unwrap().pem()
    }

    #[test]
    fn each_ssl_mode_maps_to_its_checks() {
        assert_eq!(ssl_opts(&source("ssl_mode: disable", &[])).unwrap(), None);
        for mode in ["prefer", "require"] {
            let o = ssl_opts(&source(&format!("ssl_mode: {mode}"), &[]))
                .unwrap()
                .unwrap();
            assert!(o.accept_invalid_certs(), "{mode}");
        }
        let ca = ca();
        let o = ssl_opts(&source(
            "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
            &[("CA", &ca)],
        ))
        .unwrap()
        .unwrap();
        assert!(!o.accept_invalid_certs());
        assert!(o.skip_domain_validation());
        assert!(
            o.disable_built_in_roots(),
            "ssl_ca replaces the Mozilla roots"
        );
        assert_eq!(o.root_certs(), &[ca.as_bytes().to_vec().into()]);
        let o = ssl_opts(&source(
            "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
            &[("CA", &ca)],
        ))
        .unwrap()
        .unwrap();
        assert!(!o.accept_invalid_certs() && !o.skip_domain_validation());
        // Without ssl_ca, the Mozilla roots.
        let o = ssl_opts(&source("ssl_mode: verify-full", &[]))
            .unwrap()
            .unwrap();
        assert!(!o.disable_built_in_roots() && o.root_certs().is_empty());
    }

    #[test]
    fn the_debug_form_leaves_out_the_password() {
        let c = Connector::new(&source("ssl_mode: require", &[])).unwrap();
        let text = format!("{c:?}");
        assert!(
            text.contains("db.example.com") && text.contains("cdc"),
            "{text}"
        );
        assert!(!text.contains("hunter2"), "{text}");
    }

    /// A source for the live test server (`DATA_AVALANCHE_MYSQL`, default `mysql-84`).
    fn live_source() -> Source {
        let name = std::env::var("DATA_AVALANCHE_MYSQL").unwrap_or_else(|_| "mysql-84".into());
        let port = match name.as_str() {
            "mysql-84" => 53384,
            "mysql-84-nogtid" => 53385,
            "mariadb-118" => 53318,
            other => panic!("DATA_AVALANCHE_MYSQL={other}: not a target of this test"),
        };
        source_at(
            "127.0.0.1",
            port,
            "avalanche",
            "root",
            "avalanche",
            "ssl_mode: disable",
            &[],
        )
    }

    fn one(c: &mut Conn) -> BoxFuture<'_, mysql_async::Result<Option<u8>>> {
        Box::pin(async move { c.query_first("SELECT 1").await })
    }

    /// DA-SRC-27: a call dropped mid-query must not leave its half-read connection in the slot.
    #[tokio::test]
    async fn a_call_dropped_mid_query_does_not_poison_the_next() {
        require_it!();
        let c = Connector::new(&live_source()).unwrap();
        assert_eq!(c.command("first", one).await.unwrap(), Some(1));
        let slow = c.command("sleep", |c| {
            Box::pin(async move { c.query_first::<u8, _>("SELECT SLEEP(3)").await })
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(200), slow)
                .await
                .is_err(),
            "the call was meant to be dropped mid-query"
        );
        let t = std::time::Instant::now();
        let got = c.command("after", one).await;
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
        assert_eq!(got.unwrap(), Some(1));
    }
}
