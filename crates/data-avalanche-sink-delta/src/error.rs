//! delta-rs errors → [`SinkError`]. A failure is `Permanent` only when retrying the same call
//! can't help (the data or the table is wrong, or delta-rs lacks a feature); everything else,
//! storage and network errors included, is `Transient` and the pipeline retries with backoff
//! (Data-Flow §Sink Commit Failure).

use data_avalanche_core::SinkError;
use deltalake::DeltaTableError;
use deltalake::kernel::transaction::TransactionError;

pub(crate) fn permanent(reason: impl Into<String>) -> SinkError {
    SinkError::Permanent {
        reason: reason.into(),
    }
}

pub(crate) fn sink_error(e: DeltaTableError) -> SinkError {
    use DeltaTableError as E;
    let reason = e.to_string();
    match e {
        E::SchemaMismatch { .. }
        | E::InvalidData { .. }
        | E::MetadataError(_)
        | E::CommitValidation { .. }
        | E::UnsupportedColumnMapping { .. }
        | E::MissingFeature { .. }
        | E::InvalidTableLocation(_)
        | E::NotATable(_)
        | E::Transaction {
            source:
                TransactionError::UnsupportedTableFeatures(_)
                | TransactionError::TableFeaturesRequired(_)
                | TransactionError::DeltaTableAppendOnly,
        } => SinkError::Permanent { reason },
        _ => SinkError::Transient { reason },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_errors_are_permanent_and_storage_errors_transient() {
        let e = sink_error(DeltaTableError::SchemaMismatch { msg: "x".into() });
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
        let e = sink_error(DeltaTableError::Generic("connection reset".into()));
        assert!(e.is_transient(), "{e}");
        let e = sink_error(DeltaTableError::Transaction {
            source: TransactionError::DeltaTableAppendOnly,
        });
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
    }
}
