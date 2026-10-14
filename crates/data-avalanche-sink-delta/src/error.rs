//! delta-rs errors → [`SinkError`]. A failure is `Permanent` only when retrying the same call
//! can't help (the data or the table is wrong, or delta-rs lacks a feature); everything else,
//! storage and network errors included, is `Transient` and the pipeline retries with backoff
//! (Data-Flow §Sink Commit Failure).

use data_avalanche_core::SinkError;
use deltalake::DeltaTableError;
use deltalake::kernel::transaction::{CommitConflictError, TransactionError};

pub(crate) fn permanent(reason: impl Into<String>) -> SinkError {
    SinkError::Permanent {
        reason: reason.into(),
    }
}

/// delta-rs's invariant / CHECK constraint failure, which reaches us as text: DataFusion runs the
/// check and `From<DataFusionError> for DeltaTableError` turns its `External` error into
/// `Generic` (`delta_datafusion/data_validation.rs`, `mod.rs`).
fn failed_validation(text: &str) -> bool {
    text.contains("failed validation check") || text.contains("validation check failed")
}

/// DataFusion's memory pool refused an allocation (`ResourcesExhausted`, which reaches us as
/// `Generic` text). The pool is bounded only by `merge_memory_limit`, and the same set needs
/// the same memory on every replay.
fn out_of_memory(text: &str) -> bool {
    text.contains("Resources exhausted")
}

pub(crate) fn sink_error(e: DeltaTableError) -> SinkError {
    use CommitConflictError as C;
    use DeltaTableError as E;
    let reason = e.to_string();
    match e {
        E::Generic(text) if failed_validation(&text) => SinkError::Permanent { reason },
        E::Generic(text) if out_of_memory(&text) => SinkError::Permanent {
            reason: format!(
                "the MERGE needs more memory than sink.delta.merge_memory_limit allows (raise it, or \
                 flush fewer rows per commit): {text}"
            ),
        },
        // The conflicts that aren't a lost race: retrying reads the same table and fails again.
        E::Transaction {
            source:
                TransactionError::CommitConflict(
                    C::Predicate { .. }
                    | C::UnsupportedWriterVersion(_)
                    | C::UnsupportedReaderVersion(_)
                    | C::CorruptedState { .. }
                    | C::NoMetadata,
                ),
        } => SinkError::Permanent { reason },
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
    use deltalake::logstore::object_store::Error as ObjectStoreError;

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
        let e = sink_error(DeltaTableError::InvalidData {
            message: "x".into(),
        });
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
        let e = sink_error(DeltaTableError::ObjectStore {
            source: ObjectStoreError::Generic {
                store: "S3",
                source: "connection reset".into(),
            },
        });
        assert!(e.is_transient(), "{e}");
        let conflict = |c| DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(c),
        };
        let e = sink_error(conflict(CommitConflictError::ConcurrentTransaction));
        assert!(e.is_transient(), "{e}");
        let e = sink_error(conflict(CommitConflictError::Predicate {
            source: "a bad predicate".into(),
        }));
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
        // delta-rs's invariant check, as `From<DataFusionError>` wraps it (the real error is
        // built in `write.rs`'s tests).
        let e = sink_error(DeltaTableError::Generic(
            "External error: Invalid data found: 1 rows failed validation check.".into(),
        ));
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
        // DataFusion's memory pool, past `merge_memory_limit`.
        let e = sink_error(DeltaTableError::Generic(
            "Resources exhausted: Failed to allocate additional 1.0 MB for HashJoinInput".into(),
        ));
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
        assert!(e.to_string().contains("merge_memory_limit"), "{e}");
    }
}
