//! Error classes (Data-Flow §Error Paths): which server and client failures are worth a retry.

use data_avalanche_core::SourceError;
use mysql_async::{DriverError, Error, IoError};

/// Server error codes a reconnect or a retry can outlive: a lock wait timeout (1205) or deadlock
/// (1213), too many connections (1040), a shutdown in progress (1053), a killed query or
/// connection (1317, 1927 MariaDB, 3169 MySQL), and the server's own inactivity disconnect (4031).
fn transient_code(code: u16) -> bool {
    matches!(code, 1040 | 1053 | 1205 | 1213 | 1317 | 1927 | 3169 | 4031)
}

/// A server error. Error 1236 carries several meanings, told apart by the message: another binlog
/// dump took over this pipeline's replica id (MySQL; MariaDB says it with 4052), or the requested
/// binlog is gone. The purge prechecks in `prepare` catch a gone binlog first; this is the
/// fallback when it goes between the check and the dump. `position` names the stream position a
/// purge is about.
pub(crate) fn server(code: u16, message: &str, position: Option<&str>) -> SourceError {
    let text = format!("{message} (error {code})");
    match code {
        4052 => SourceError::Contended(text),
        1236 if message.contains("same server_uuid/server_id") => SourceError::Contended(text),
        1236 if purged(message) => SourceError::PositionUnreachable {
            position: position.unwrap_or_default().to_owned(),
            detail: text,
        },
        c if transient_code(c) => SourceError::Transient(text),
        _ => SourceError::Permanent(text),
    }
}

/// The 1236 messages of a binlog the server no longer has (S-MY10e, S-MY10k): a file gone from
/// the index, purged GTIDs (MySQL), a GTID state no binlog holds (MariaDB).
fn purged(message: &str) -> bool {
    message.contains("Could not find first log file name")
        || message.contains("purged required binary logs")
        || message.contains("Could not find GTID state")
}

/// A `mysql_async` error. I/O and a closed connection are transient; a server error by its code;
/// TLS, authentication and protocol failures are permanent.
pub(crate) fn mysql(e: Error, position: Option<&str>) -> SourceError {
    match e {
        Error::Server(s) => server(s.code, &s.message, position),
        Error::Io(IoError::Tls(t)) => SourceError::Permanent(format!("TLS: {t}")),
        // A handshake failure (an unknown issuer, a name mismatch) arrives as I/O around the
        // rustls error: a retry meets the same certificate.
        Error::Io(IoError::Io(io)) if io.get_ref().is_some_and(|e| e.is::<rustls::Error>()) => {
            SourceError::Permanent(format!("TLS: {io}"))
        }
        Error::Io(IoError::Io(io)) => SourceError::Transient(io.to_string()),
        Error::Driver(DriverError::ConnectionClosed) => {
            SourceError::Transient("the server closed the connection".into())
        }
        Error::Driver(DriverError::NoClientSslFlagFromServer) => SourceError::Permanent(
            "TLS: the server doesn't offer TLS, and the ssl_mode requires it".into(),
        ),
        other => SourceError::Permanent(other.to_string()),
    }
}

/// A client-side timeout of a connect, a catalog or a snapshot query.
pub(crate) fn timed_out(what: &str, after: std::time::Duration) -> SourceError {
    SourceError::Transient(format!("{what} timed out after {after:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::ServerError;

    fn server_error(code: u16, message: &str) -> Error {
        Error::Server(ServerError {
            code,
            message: message.into(),
            state: "HY000".into(),
        })
    }

    #[test]
    fn lock_waits_deadlocks_and_shutdowns_are_transient() {
        for code in [1040, 1053, 1205, 1213, 1317, 1927, 3169, 4031] {
            assert!(
                mysql(server_error(code, "x"), None).is_transient(),
                "{code}"
            );
        }
        let io = Error::Io(IoError::Io(std::io::Error::other("reset")));
        assert!(mysql(io, None).is_transient());
        assert!(mysql(Error::Driver(DriverError::ConnectionClosed), None).is_transient());
    }

    #[test]
    fn privileges_and_settings_are_permanent() {
        for (code, m) in [
            (1045, "Access denied for user 'u'@'%' (using password: YES)"),
            (
                1227,
                "Access denied; you need (at least one of) the REPLICATION CLIENT privilege(s)",
            ),
            (3159, "Connections using insecure transport are prohibited"),
        ] {
            assert_eq!(
                mysql(server_error(code, m), None),
                SourceError::Permanent(format!("{m} (error {code})"))
            );
        }
        assert!(matches!(
            mysql(Error::Driver(DriverError::NoClientSslFlagFromServer), None),
            SourceError::Permanent(m) if m.starts_with("TLS:")
        ));
    }

    #[test]
    fn a_tls_error_wrapped_in_io_is_permanent() {
        let io = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert!(matches!(
            mysql(Error::Io(IoError::Io(io)), None),
            SourceError::Permanent(m) if m.starts_with("TLS:") && m.contains("UnknownIssuer")
        ));
    }

    #[test]
    fn another_dump_with_the_replica_id_is_contended() {
        // S-MY10h: the older dump ends when a newer one registers the same replica id.
        let mysql_kick = "A replica with the same server_uuid/server_id as this replica has \
                          connected to the source; the first event 'mysql-bin.000018' at 593";
        let maria_kick = "A slave with the same server_id is already connected";
        assert!(matches!(
            mysql(server_error(1236, mysql_kick), Some("x")),
            SourceError::Contended(_)
        ));
        assert!(matches!(
            mysql(server_error(4052, maria_kick), None),
            SourceError::Contended(_)
        ));
    }

    #[test]
    fn a_purged_binlog_is_an_unreachable_position() {
        for m in [
            "Could not find first log file name in binary log index file",
            "Cannot replicate because the source purged required binary logs. Replicate the \
             missing transactions from elsewhere",
            "Could not find GTID state requested by slave in any binlog files",
        ] {
            assert_eq!(
                mysql(server_error(1236, m), Some("mysql-bin.000017:1193")),
                SourceError::PositionUnreachable {
                    position: "mysql-bin.000017:1193".into(),
                    detail: format!("{m} (error 1236)"),
                }
            );
        }
        // Another 1236 is neither.
        assert!(matches!(
            mysql(
                server_error(1236, "log event entry exceeded max_allowed_packet"),
                None
            ),
            SourceError::Permanent(_)
        ));
    }
}
