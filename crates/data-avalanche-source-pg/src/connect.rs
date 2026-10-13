//! The two connections to the source: the catalog connection (`tokio-postgres`: catalog channels,
//! snapshot chunks, slot and publication DDL) and the replication connection
//! (`pgwire-replication`). Both run with the same pinned session settings, so the text the
//! decoder parses has one form (D-M5.5).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use data_avalanche_config::{Source, SslMode, Timeouts};
use data_avalanche_core::SourceError;
use pgwire_replication::{Lsn, ReplicationConfig};
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{sql_error, timed_out};

/// How much longer the client waits than a server-side timeout (`statement_timeout`,
/// `lock_timeout`): the server cancels a slow statement first, so the shared catalog connection
/// is free again when the client gives up.
pub(crate) const SERVER_FIRST: Duration = Duration::from_secs(5);

/// `d` as a server-side timeout in milliseconds: at least 1 (0 turns a timeout off) and at most
/// what the setting holds (`i32::MAX`).
pub(crate) fn server_millis(d: Duration) -> u128 {
    d.as_millis().clamp(1, i32::MAX as u128)
}
use crate::tls::{PemFiles, control_ssl_mode, control_tls, replication_tls};

/// Session settings on both connections: UTC instants, ISO dates, ISO-8601 intervals, round-trip
/// floats, hex `bytea`, `money` without a locale's currency symbol or grouping, and string
/// literals without backslash escapes (snapshot keys travel as literals).
pub const SESSION_OPTIONS: &str = "-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=iso_8601 \
     -c extra_float_digits=3 -c bytea_output=hex -c lc_monetary=C \
     -c standard_conforming_strings=on";

/// The `application_name` the catalog connections report (`pg_stat_activity`). The replication
/// connection reports `pgwire-replication`: the crate (0.4.1) hard-codes it in its startup packet.
pub const APPLICATION_NAME: &str = "data-avalanche";

/// What both connections need, built once from `source`. `Debug` leaves out the password.
pub(crate) struct Connector {
    host: String,
    port: u16,
    db: String,
    user: String,
    password: String,
    ssl_mode: SslMode,
    pub(crate) timeouts: Timeouts,
    files: PemFiles,
    tls: Option<MakeRustlsConnect>,
    client: Mutex<Option<Arc<Client>>>,
}

impl Connector {
    pub(crate) fn new(source: &Source) -> Result<Self, SourceError> {
        // Inline PEMs go to private files only for a connection that uses TLS.
        let files = match source.ssl_mode {
            SslMode::Disable => PemFiles::default(),
            _ => PemFiles::new(source).map_err(SourceError::Permanent)?,
        };
        let tls = match source.ssl_mode {
            SslMode::Disable => None,
            _ => Some(MakeRustlsConnect::new(
                control_tls(source).map_err(SourceError::Permanent)?,
            )),
        };
        Ok(Self {
            host: source.host.clone(),
            port: source.port,
            db: source.db.clone(),
            user: source.credentials.user().to_owned(),
            password: source.credentials.password().expose().to_owned(),
            ssl_mode: source.ssl_mode,
            timeouts: source.timeouts.clone(),
            files,
            tls,
            client: Mutex::new(None),
        })
    }

    fn control_config(&self) -> tokio_postgres::Config {
        let mut c = tokio_postgres::Config::new();
        c.host(&self.host)
            .port(self.port)
            .dbname(&self.db)
            .user(&self.user)
            .password(&self.password)
            .options(SESSION_OPTIONS)
            .application_name(APPLICATION_NAME)
            .connect_timeout(self.timeouts.connect)
            .ssl_mode(control_ssl_mode(self.ssl_mode));
        c
    }

    /// A new catalog connection, its driver task spawned, within `timeouts.connect`: the
    /// driver's own `connect_timeout` bounds only the TCP connect, and a server that accepts the
    /// connection but never answers (frozen, behind a black hole) would hold the startup, and
    /// every call queued behind the shared connection, forever (DA-SRC-16).
    pub(crate) async fn connect(&self) -> Result<Client, SourceError> {
        let after = self.timeouts.connect;
        tokio::time::timeout(after, self.open())
            .await
            .map_err(|_| timed_out("connect to the source", after))?
    }

    async fn open(&self) -> Result<Client, SourceError> {
        let config = self.control_config();
        let client = match &self.tls {
            None => {
                let (client, conn) = config
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(sql_error)?;
                tokio::spawn(conn);
                client
            }
            Some(tls) => {
                let (client, conn) = config.connect(tls.clone()).await.map_err(sql_error)?;
                tokio::spawn(conn);
                client
            }
        };
        Ok(client)
    }

    /// The shared catalog connection, reconnected when it was closed.
    pub(crate) async fn client(&self) -> Result<Arc<Client>, SourceError> {
        let mut slot = self.client.lock().await;
        if let Some(c) = slot.as_ref().filter(|c| !c.is_closed()) {
            return Ok(c.clone());
        }
        let c = Arc::new(self.connect().await?);
        *slot = Some(c.clone());
        Ok(c)
    }

    /// Runs a catalog query under `timeouts.command`.
    pub(crate) async fn command<T>(
        &self,
        what: &str,
        f: impl Future<Output = Result<T, tokio_postgres::Error>>,
    ) -> Result<T, SourceError> {
        self.bounded(what, self.timeouts.command, f).await
    }

    /// `f`, on the shared catalog connection, under a client-side timeout. A timeout abandons the
    /// connection: the server is asked to cancel the statement, and the next call connects
    /// afresh, so a retry doesn't queue behind a statement the server still runs.
    pub(crate) async fn bounded<T>(
        &self,
        what: &str,
        after: Duration,
        f: impl Future<Output = Result<T, tokio_postgres::Error>>,
    ) -> Result<T, SourceError> {
        // The connection `f` runs on: the caller has just taken it from [`Self::client`]. Had
        // another call replaced it in between, a timeout abandons the newer one.
        let at_start = self.client.lock().await.clone();
        match tokio::time::timeout(after, f).await {
            Ok(r) => r.map_err(sql_error),
            Err(_) => {
                if let Some(client) = at_start {
                    self.abandon(&client).await;
                }
                Err(timed_out(what, after))
            }
        }
    }

    /// Drops `client` from the cache and asks the server to cancel what it runs there, within
    /// `timeouts.connect`; a cancel that fails changes nothing. A client another call abandoned
    /// already, and one connected since, are left alone. Calls that share the connection can see
    /// their own statement cancelled: each is retried.
    async fn abandon(&self, client: &Arc<Client>) {
        {
            let mut slot = self.client.lock().await;
            if !slot.as_ref().is_some_and(|c| Arc::ptr_eq(c, client)) {
                return;
            }
            *slot = None;
        }
        let token = client.cancel_token();
        let _ = tokio::time::timeout(self.timeouts.connect, async {
            match &self.tls {
                None => token.cancel_query(tokio_postgres::NoTls).await,
                Some(tls) => token.cancel_query(tls.clone()).await,
            }
        })
        .await;
    }

    /// The replication connection for `slot` and `publication`, starting after `start`. The status
    /// and wake-up intervals are the heartbeat (Data-Flow Phase 2a).
    pub(crate) fn replication(
        &self,
        slot: &str,
        publication: &str,
        start: Lsn,
        heartbeat: Duration,
    ) -> ReplicationConfig {
        ReplicationConfig::new(
            self.host.clone(),
            self.user.clone(),
            self.password.clone(),
            self.db.clone(),
            slot,
            // Quoted: pgoutput reads `publication_names` as identifiers and folds an unquoted
            // `MyPub` to `mypub`, which the publication created as `"MyPub"` isn't.
            crate::slot::ident(publication).as_str(),
        )
        .with_port(self.port)
        .with_tls(replication_tls(self.ssl_mode, &self.files))
        .with_start_lsn(start)
        .with_status_interval(heartbeat)
        .with_wakeup_interval(heartbeat)
        .with_options(SESSION_OPTIONS)
    }
}

impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("db", &self.db)
            .field("user", &self.user)
            .field("ssl_mode", &self.ssl_mode)
            .finish_non_exhaustive()
    }
}

/// `f` under a client-side timeout; a timeout is transient.
pub(crate) async fn within<T>(
    what: &str,
    after: Duration,
    f: impl Future<Output = Result<T, tokio_postgres::Error>>,
) -> Result<T, SourceError> {
    match tokio::time::timeout(after, f).await {
        Ok(r) => r.map_err(sql_error),
        Err(_) => Err(timed_out(what, after)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::pipeline;

    #[test]
    fn server_timeouts_stay_in_the_settings_range() {
        assert_eq!(
            server_millis(Duration::ZERO),
            1,
            "0 would turn the timeout off"
        );
        assert_eq!(server_millis(Duration::from_micros(10)), 1);
        assert_eq!(server_millis(Duration::from_secs(30)), 30_000);
        assert_eq!(
            server_millis(Duration::from_secs(10_000_000)),
            i32::MAX as u128
        );
    }

    #[test]
    fn the_catalog_connection_pins_the_session() {
        let c = Connector::new(&pipeline("public.t").source).unwrap();
        let config = c.control_config();
        assert_eq!(config.get_options(), Some(SESSION_OPTIONS));
        assert_eq!(config.get_application_name(), Some(APPLICATION_NAME));
        assert_eq!(config.get_dbname(), Some("avalanche"));
        assert_eq!(config.get_user(), Some("avalanche"));
        assert_eq!(config.get_ports(), &[55418]);
        assert_eq!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable
        );
    }

    /// A server that accepts the connection and never answers: the startup times out.
    #[tokio::test]
    async fn a_connect_to_a_server_that_never_answers_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let held = tokio::spawn(async move {
            let mut sockets = vec![];
            while let Ok((s, _)) = listener.accept().await {
                sockets.push(s);
            }
        });
        let mut source = pipeline("public.t").source;
        source.port = port;
        source.timeouts.connect = Duration::from_millis(300);
        let c = Connector::new(&source).unwrap();
        let err = tokio::time::timeout(Duration::from_secs(5), c.client())
            .await
            .expect("the startup is bounded by timeouts.connect")
            .unwrap_err();
        assert!(err.is_transient(), "{err}");
        assert!(
            err.to_string().contains("connect to the source timed out"),
            "{err}"
        );
        held.abort();
    }

    #[test]
    fn the_replication_connection_pins_the_same_session() {
        let c = Connector::new(&pipeline("public.t").source).unwrap();
        let r = c.replication("s", "MyPub", Lsn(0x10), Duration::from_secs(7));
        assert_eq!(r.options.as_deref(), Some(SESSION_OPTIONS));
        assert_eq!(r.slot, "s");
        assert_eq!(
            r.publication.names(),
            ["\"MyPub\""],
            "quoted, or pgoutput folds it to mypub"
        );
        assert_eq!(r.start_lsn, Lsn(0x10));
        assert_eq!(r.status_interval, Duration::from_secs(7));
        assert_eq!(r.idle_wakeup_interval, Duration::from_secs(7));
        assert!(!r.binary, "text tuples only (Q-M5-3)");
    }
}
