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
    /// there is none. A connection that failed or timed out is dropped: the next call opens
    /// another.
    pub(crate) async fn command<T>(
        &self,
        what: &str,
        f: impl for<'c> FnOnce(&'c mut Conn) -> BoxFuture<'c, mysql_async::Result<T>>,
    ) -> Result<T, SourceError> {
        let mut slot = self.catalog.lock().await;
        if slot.is_none() {
            *slot = Some(self.connect().await?);
        }
        let conn = slot.as_mut().expect("connected above");
        match tokio::time::timeout(self.command, f(conn)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                if e.is_fatal() {
                    *slot = None;
                }
                Err(mysql(e, None))
            }
            Err(_) => {
                *slot = None;
                Err(timed_out(what, self.command))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_config::{FingerprintKey, load_str};
    use std::collections::BTreeMap;

    /// A source with `ssl` (YAML flow entries) and `env` for its `${…}` references.
    fn source(ssl: &str, env: &[(&str, &str)]) -> Source {
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
      host: db.example.com
      port: 3306
      db: shop
      user: cdc
      password: hunter2
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
}
