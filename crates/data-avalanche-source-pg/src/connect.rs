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
        let files = PemFiles::new(source).map_err(SourceError::Permanent)?;
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

    /// A new catalog connection, its driver task spawned.
    pub(crate) async fn connect(&self) -> Result<Client, SourceError> {
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
        within(what, self.timeouts.command, f).await
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
            publication,
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

    #[test]
    fn the_replication_connection_pins_the_same_session() {
        let c = Connector::new(&pipeline("public.t").source).unwrap();
        let r = c.replication("s", "p", Lsn(0x10), Duration::from_secs(7));
        assert_eq!(r.options.as_deref(), Some(SESSION_OPTIONS));
        assert_eq!(r.slot, "s");
        assert_eq!(r.publication.names(), ["p"]);
        assert_eq!(r.start_lsn, Lsn(0x10));
        assert_eq!(r.status_interval, Duration::from_secs(7));
        assert_eq!(r.idle_wakeup_interval, Duration::from_secs(7));
        assert!(!r.binary, "text tuples only (Q-M5-3)");
    }
}
