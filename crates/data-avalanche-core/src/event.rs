use std::cmp::Ordering;
use std::fmt;

use thiserror::Error;

use crate::position::{BinlogPosError, Lsn, parse_binlog_index};
use crate::{Row, SourcePosition, TableRef};

/// Per-change source order — the keep-last key of the collapse (`_seq`, `Ghost_Row…` §4).
/// Comparable only within one dialect (one pipeline has one source).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceSeq {
    /// The change's own WAL LSN.
    Pg(Lsn),
    /// Binlog file index (from the `.NNNNNN` suffix) and the event's `end_log_pos` (D-IMPL-5).
    Binlog { file_index: u64, end_log_pos: u64 },
}

impl SourceSeq {
    pub fn binlog(file: &str, end_log_pos: u64) -> Result<Self, BinlogPosError> {
        let (_, file_index) = parse_binlog_index(file)?;
        Ok(Self::Binlog {
            file_index,
            end_log_pos,
        })
    }
}

impl PartialOrd for SourceSeq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Some(a.cmp(b)),
            (
                Self::Binlog {
                    file_index: fa,
                    end_log_pos: pa,
                },
                Self::Binlog {
                    file_index: fb,
                    end_log_pos: pb,
                },
            ) => Some((fa, pa).cmp(&(fb, pb))),
            _ => None,
        }
    }
}

impl fmt::Display for SourceSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pg(lsn) => write!(f, "{lsn}"),
            Self::Binlog {
                file_index,
                end_log_pos,
            } => write!(f, "{file_index:06}:{end_log_pos}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Insert,
    Update,
    Delete,
    Snapshot,
}

impl Op {
    /// The `_op` lineage value. `D` never reaches a mirror table (deletes remove rows); it is kept
    /// for the reserved `changelog` mode.
    pub fn lineage_code(self) -> char {
        match self {
            Self::Insert => 'I',
            Self::Update => 'U',
            Self::Delete => 'D',
            Self::Snapshot => 'r',
        }
    }
}

/// One row change, identical for both sources (`Data-Flow.md` §CdcEvent Structure).
#[derive(Debug, Clone, PartialEq)]
pub struct CdcEvent {
    pub op: Op,
    pub table: TableRef,
    pub before: Option<Row>,
    pub after: Option<Row>,
    /// Commit position of the enclosing transaction.
    pub source_pos: SourcePosition,
    pub change_seq: SourceSeq,
    pub xid: u64,
    /// Server commit time, microseconds since the Unix epoch (UTC).
    pub commit_ts_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EventShapeError {
    #[error("{op:?} event for {table} is missing its after-image")]
    MissingAfter { op: Op, table: TableRef },
    #[error("{op:?} event for {table} is missing its before-image")]
    MissingBefore { op: Op, table: TableRef },
    #[error("{op:?} event for {table} must not carry a before-image")]
    UnexpectedBefore { op: Op, table: TableRef },
    #[error("{op:?} event for {table} must not carry an after-image")]
    UnexpectedAfter { op: Op, table: TableRef },
}

impl CdcEvent {
    /// Checks the image shape per op: Insert/Snapshot = after only; Update = after (+ optional before);
    /// Delete = before only.
    pub fn validate(&self) -> Result<(), EventShapeError> {
        let (op, table) = (self.op, self.table.clone());
        match (self.op, self.before.is_some(), self.after.is_some()) {
            (Op::Insert | Op::Snapshot, true, _) => {
                Err(EventShapeError::UnexpectedBefore { op, table })
            }
            (Op::Insert | Op::Snapshot | Op::Update, _, false) => {
                Err(EventShapeError::MissingAfter { op, table })
            }
            (Op::Delete, false, _) => Err(EventShapeError::MissingBefore { op, table }),
            (Op::Delete, true, true) => Err(EventShapeError::UnexpectedAfter { op, table }),
            _ => Ok(()),
        }
    }
}

/// The end of a source transaction; only complete transactions are flushed.
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionBoundary {
    pub xid: u64,
    pub commit_pos: SourcePosition,
    pub commit_ts_micros: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::Lsn;
    use crate::{ColumnValue, Row, SourcePosition, TableRef};
    use std::cmp::Ordering;

    fn row() -> Option<Row> {
        Some(Row(vec![("id".into(), ColumnValue::Int64(1))]))
    }

    fn event(op: Op, before: Option<Row>, after: Option<Row>) -> CdcEvent {
        CdcEvent {
            op,
            table: TableRef::new("public", "t"),
            before,
            after,
            source_pos: SourcePosition::Pg(Lsn(100)),
            change_seq: SourceSeq::Pg(Lsn(90)),
            xid: 7,
            commit_ts_micros: 0,
        }
    }

    #[test]
    fn seq_orders_within_a_dialect_only() {
        assert!(SourceSeq::Pg(Lsn(1)) < SourceSeq::Pg(Lsn(2)));
        let a = SourceSeq::binlog("mysql-bin.000009", 900).unwrap();
        let b = SourceSeq::binlog("mysql-bin.000010", 4).unwrap();
        assert!(a < b);
        assert_eq!(SourceSeq::Pg(Lsn(1)).partial_cmp(&a), None::<Ordering>);
    }

    #[test]
    fn seq_displays_for_the_seq_column() {
        assert_eq!(SourceSeq::Pg(Lsn(0x16B_3748)).to_string(), "0/16B3748");
        assert_eq!(
            SourceSeq::binlog("mysql-bin.000123", 4567)
                .unwrap()
                .to_string(),
            "000123:4567"
        );
    }

    #[test]
    fn lineage_codes_match_the_spec() {
        assert_eq!(
            [Op::Insert, Op::Update, Op::Delete, Op::Snapshot].map(Op::lineage_code),
            ['I', 'U', 'D', 'r']
        );
    }

    #[test]
    fn well_formed_events_validate() {
        assert!(event(Op::Insert, None, row()).validate().is_ok());
        assert!(
            event(Op::Update, None, row()).validate().is_ok(),
            "PG DEFAULT identity may omit the old image"
        );
        assert!(event(Op::Update, row(), row()).validate().is_ok());
        assert!(event(Op::Delete, row(), None).validate().is_ok());
        assert!(event(Op::Snapshot, None, row()).validate().is_ok());
    }

    #[test]
    fn malformed_events_are_rejected() {
        assert!(matches!(
            event(Op::Insert, None, None).validate(),
            Err(EventShapeError::MissingAfter { .. })
        ));
        assert!(matches!(
            event(Op::Insert, row(), row()).validate(),
            Err(EventShapeError::UnexpectedBefore { .. })
        ));
        assert!(matches!(
            event(Op::Update, row(), None).validate(),
            Err(EventShapeError::MissingAfter { .. })
        ));
        assert!(matches!(
            event(Op::Delete, None, None).validate(),
            Err(EventShapeError::MissingBefore { .. })
        ));
        assert!(matches!(
            event(Op::Delete, row(), row()).validate(),
            Err(EventShapeError::UnexpectedAfter { .. })
        ));
        assert!(matches!(
            event(Op::Snapshot, row(), row()).validate(),
            Err(EventShapeError::UnexpectedBefore { .. })
        ));
    }
}
