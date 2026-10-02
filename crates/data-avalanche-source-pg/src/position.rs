//! PostgreSQL positions: the core `Lsn` and the replication crate's.

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{SourceError, SourcePosition};

/// A `pg_lsn` as PostgreSQL prints it (`16/B374D848`).
pub(crate) fn parse_lsn(text: &str) -> Result<Lsn, SourceError> {
    text.parse()
        .map_err(|e: data_avalanche_core::position::LsnParseError| {
            SourceError::Permanent(e.to_string())
        })
}

/// The LSN of a PostgreSQL position; another dialect is a caller error.
pub(crate) fn lsn_of(p: &SourcePosition) -> Result<Lsn, SourceError> {
    match p {
        SourcePosition::Pg(l) => Ok(*l),
        other => Err(SourceError::Permanent(format!(
            "a {} position given to the PostgreSQL source",
            other.dialect()
        ))),
    }
}

pub(crate) fn to_wire(l: Lsn) -> pgwire_replication::Lsn {
    pgwire_replication::Lsn(l.0)
}

pub(crate) fn from_wire(l: pgwire_replication::Lsn) -> Lsn {
    Lsn(l.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsns() {
        assert_eq!(parse_lsn("16/B374D848").unwrap(), Lsn(0x16_B374_D848));
        assert!(parse_lsn("x").is_err());
        assert_eq!(lsn_of(&SourcePosition::Pg(Lsn(7))).unwrap(), Lsn(7));
        let mysql = SourcePosition::parse_tagged("binlog:mysql-bin.000001:4").unwrap();
        assert!(lsn_of(&mysql).is_err());
        assert_eq!(from_wire(to_wire(Lsn(42))), Lsn(42));
    }
}
