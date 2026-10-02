//! Error classes (Data-Flow §Error Paths): which server and client failures are worth a retry.

use data_avalanche_core::SourceError;
use pgwire_replication::PgWireError;

/// SQLSTATE classes and codes a reconnect can outlive: connection exceptions (`08`), insufficient
/// resources (`53`), operator intervention (`57P01`–`57P05`, a statement timeout `57014`),
/// serialization failures and deadlocks (`40001`, `40P01`), a lock not available (`55P03`).
fn transient_state(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        || code.starts_with("57P")
        || matches!(code, "57014" | "40001" | "40P01" | "55P03")
}

/// A server error on the replication connection, as `pgwire-replication` reports it:
/// `"<message> (SQLSTATE <code>)"`.
pub(crate) fn sqlstate(message: &str) -> Option<&str> {
    let start = message.rfind("(SQLSTATE ")? + "(SQLSTATE ".len();
    message.get(start..start + 5)
}

/// The class of a server error. `position` names the stream position a removed-WAL error is about.
fn classify(code: Option<&str>, message: String, position: Option<String>) -> SourceError {
    match code {
        // `requested WAL segment … has already been removed` (58P01); a slot invalidated by
        // `max_slot_wal_keep_size` or an idle timeout (55000, "can no longer get changes").
        Some("58P01") if message.contains("WAL") => SourceError::PositionUnreachable {
            position: position.unwrap_or_default(),
            detail: message,
        },
        Some("55000") if message.contains("can no longer") => SourceError::PositionUnreachable {
            position: position.unwrap_or_default(),
            detail: message,
        },
        // `replication slot "…" is active for PID …` (55006): another holder exists. Fatal, never
        // retried into a busy loop (Postgres_Slot… D-15.5).
        Some("55006") => SourceError::Permanent(message),
        Some(c) if transient_state(c) => SourceError::Transient(message),
        _ => SourceError::Permanent(message),
    }
}

/// A replication-connection error. I/O and worker failures are transient; authentication, TLS and
/// protocol failures are not.
pub(crate) fn replication_error(e: PgWireError, position: Option<String>) -> SourceError {
    match e {
        PgWireError::Io(_) | PgWireError::Task(_) => SourceError::Transient(e.to_string()),
        // A `next` cancelled while the ended worker was being joined: the next call reports the
        // worker's own error as this. Reopening the stream recovers.
        PgWireError::Internal(ref m) if m.contains("already joined") => {
            SourceError::Transient(e.to_string())
        }
        PgWireError::Server(m) => classify(sqlstate(&m).map(str::to_owned).as_deref(), m, position),
        other => SourceError::Permanent(other.to_string()),
    }
}

/// True when a TLS failure is somewhere in the error's chain: a certificate the client refuses
/// won't be accepted on a retry.
fn tls_failure(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if err.is::<rustls::Error>() {
            return true;
        }
        if let Some(io) = err.downcast_ref::<std::io::Error>()
            && io
                .get_ref()
                .is_some_and(|inner| inner.is::<rustls::Error>())
        {
            return true;
        }
        cur = err.source();
    }
    false
}

/// The error and its sources, `: `-separated: `tokio-postgres` keeps the cause (an unknown
/// issuer, a refused connection) out of its own message.
fn chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(err) = cur {
        let text = err.to_string();
        if !out.ends_with(&text) {
            out = format!("{out}: {text}");
        }
        cur = err.source();
    }
    out
}

/// A catalog-connection error (`tokio-postgres`).
pub(crate) fn sql_error(e: tokio_postgres::Error) -> SourceError {
    if let Some(db) = e.as_db_error() {
        let message = format!("{} (SQLSTATE {})", db.message(), db.code().code());
        return classify(Some(db.code().code()), message, None);
    }
    if tls_failure(&e) {
        return SourceError::Permanent(format!("TLS: {}", chain(&e)));
    }
    // Closed connections, I/O and connect timeouts.
    SourceError::Transient(chain(&e))
}

/// A client-side timeout of a catalog or snapshot query.
pub(crate) fn timed_out(what: &str, after: std::time::Duration) -> SourceError {
    SourceError::Transient(format!("{what} timed out after {after:?}"))
}

/// A protocol message the decoder can't read: the stream can't continue past it.
pub(crate) fn protocol(detail: impl std::fmt::Display) -> SourceError {
    SourceError::Permanent(format!("pgoutput protocol error: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn server(m: &str) -> PgWireError {
        PgWireError::Server(m.into())
    }

    #[test]
    fn sqlstate_comes_from_the_message_tail() {
        assert_eq!(sqlstate("boom (SQLSTATE 55006)"), Some("55006"));
        assert_eq!(sqlstate("no state"), None);
        assert_eq!(sqlstate("cut (SQLSTATE 55"), None);
    }

    #[test]
    fn a_busy_slot_is_fatal() {
        let e = replication_error(
            server("replication slot \"s\" is active for PID 42 (SQLSTATE 55006)"),
            None,
        );
        assert!(matches!(e, SourceError::Permanent(m) if m.contains("active for PID")));
    }

    #[test]
    fn removed_wal_and_invalidated_slots_are_unreachable_positions() {
        let removed = replication_error(
            server(
                "requested WAL segment 000000010000000000000003 has already been removed (SQLSTATE 58P01)",
            ),
            Some("0/3000000".into()),
        );
        assert_eq!(
            removed,
            SourceError::PositionUnreachable {
                position: "0/3000000".into(),
                detail: "requested WAL segment 000000010000000000000003 has already been removed (SQLSTATE 58P01)".into(),
            }
        );
        let invalidated = replication_error(
            server("can no longer get changes from replication slot \"s\" (SQLSTATE 55000)"),
            Some("0/10".into()),
        );
        assert!(matches!(
            invalidated,
            SourceError::PositionUnreachable { .. }
        ));
        // The same SQLSTATE for another reason isn't a lost position.
        assert!(matches!(
            replication_error(
                server("logical decoding requires wal_level >= logical (SQLSTATE 55000)"),
                None
            ),
            SourceError::Permanent(_)
        ));
    }

    #[test]
    fn shutdowns_and_io_are_transient() {
        for m in [
            "terminating connection due to administrator command (SQLSTATE 57P01)",
            "the database system is starting up (SQLSTATE 57P03)",
            "sorry, too many clients already (SQLSTATE 53300)",
            "connection failure (SQLSTATE 08006)",
        ] {
            assert!(replication_error(server(m), None).is_transient(), "{m}");
        }
        let io = PgWireError::Io(Arc::new(std::io::Error::other("reset")));
        assert!(replication_error(io, None).is_transient());
    }

    #[test]
    fn auth_tls_protocol_and_privileges_are_permanent() {
        for e in [
            PgWireError::Auth("password authentication failed".into()),
            PgWireError::Tls("handshake failed".into()),
            PgWireError::Protocol("bad frame".into()),
            server("permission denied to start WAL sender (SQLSTATE 42501)"),
        ] {
            assert!(matches!(
                replication_error(e, None),
                SourceError::Permanent(_)
            ));
        }
    }

    #[test]
    fn a_worker_joined_twice_is_transient() {
        let e = PgWireError::Internal("replication worker already joined".into());
        assert!(replication_error(e, None).is_transient());
        let e = PgWireError::Internal("something else".into());
        assert!(!replication_error(e, None).is_transient());
    }

    #[test]
    fn a_tls_error_wrapped_in_io_is_found() {
        let io = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert!(tls_failure(&io));
        assert!(!tls_failure(&std::io::Error::other("reset")));
    }
}
