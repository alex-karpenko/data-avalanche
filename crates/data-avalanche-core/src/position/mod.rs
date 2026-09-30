//! Source positions per engine dialect (`Delivery_Guarantee_and_Position_Guard.md` §2).

pub mod lsn;

pub use lsn::{Lsn, LsnParseError};

pub mod gtid;

pub use gtid::{GtidParseError, GtidSet, GtidSource};

pub mod mariadb;

pub use mariadb::{MariaGtid, MariaGtidParseError, MariaGtidState};

pub mod binlog;

pub use binlog::{BinlogFilePos, BinlogPosError, parse_binlog_index};

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// A source position in one of the four engine dialects (roadmap §3.2). `committed_position`
/// markers and state files store it as a self-describing tagged string (D-IMPL-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePosition {
    Pg(Lsn),
    MySqlGtid(GtidSet),
    MariaDbGtid(MariaGtidState),
    BinlogFile(BinlogFilePos),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PositionError {
    #[error("cannot combine a {left} position with a {right} position")]
    DialectMismatch {
        left: &'static str,
        right: &'static str,
    },
    #[error("binlog positions from different log series: {left} vs {right}")]
    DifferentBinlogSeries { left: String, right: String },
    #[error("invalid position {input:?}: {reason}")]
    Parse { input: String, reason: String },
}

impl SourcePosition {
    pub fn dialect(&self) -> &'static str {
        match self {
            Self::Pg(_) => "pg",
            Self::MySqlGtid(_) => "mysql-gtid",
            Self::MariaDbGtid(_) => "mariadb-gtid",
            Self::BinlogFile(_) => "binlog",
        }
    }

    fn mismatch(&self, other: &Self) -> PositionError {
        PositionError::DialectMismatch {
            left: self.dialect(),
            right: other.dialect(),
        }
    }

    fn binlog_order(a: &BinlogFilePos, b: &BinlogFilePos) -> Result<Ordering, PositionError> {
        a.compare(b)
            .ok_or_else(|| PositionError::DifferentBinlogSeries {
                left: a.to_string(),
                right: b.to_string(),
            })
    }

    /// True when `other` is already included in `self` (PG/binlog: `>=`; GTID: subset;
    /// MariaDB: every domain at an equal or later sequence).
    pub fn covers(&self, other: &Self) -> Result<bool, PositionError> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Ok(a >= b),
            (Self::MySqlGtid(a), Self::MySqlGtid(b)) => Ok(a.covers(b)),
            (Self::MariaDbGtid(a), Self::MariaDbGtid(b)) => Ok(a.covers(b)),
            (Self::BinlogFile(a), Self::BinlogFile(b)) => {
                Ok(Self::binlog_order(a, b)? != Ordering::Less)
            }
            _ => Err(self.mismatch(other)),
        }
    }

    pub fn union(&self, other: &Self) -> Result<Self, PositionError> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Ok(Self::Pg(*a.max(b))),
            (Self::MySqlGtid(a), Self::MySqlGtid(b)) => Ok(Self::MySqlGtid(a.union(b))),
            (Self::MariaDbGtid(a), Self::MariaDbGtid(b)) => Ok(Self::MariaDbGtid(a.union(b))),
            (Self::BinlogFile(a), Self::BinlogFile(b)) => {
                let later = if Self::binlog_order(a, b)? == Ordering::Less {
                    b
                } else {
                    a
                };
                Ok(Self::BinlogFile(later.clone()))
            }
            _ => Err(self.mismatch(other)),
        }
    }

    pub fn intersect(&self, other: &Self) -> Result<Self, PositionError> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Ok(Self::Pg(*a.min(b))),
            (Self::MySqlGtid(a), Self::MySqlGtid(b)) => Ok(Self::MySqlGtid(a.intersect(b))),
            (Self::MariaDbGtid(a), Self::MariaDbGtid(b)) => Ok(Self::MariaDbGtid(a.intersect(b))),
            (Self::BinlogFile(a), Self::BinlogFile(b)) => {
                let earlier = if Self::binlog_order(a, b)? == Ordering::Greater {
                    b
                } else {
                    a
                };
                Ok(Self::BinlogFile(earlier.clone()))
            }
            _ => Err(self.mismatch(other)),
        }
    }

    /// The position in its dialect's own text form, without the tag: `0/16B3748`, a canonical
    /// GTID set, `0-1-42`, `mysql-bin.000003:154`. The `_lsn` lineage column holds this.
    pub fn untagged(&self) -> String {
        match self {
            Self::Pg(v) => v.to_string(),
            Self::MySqlGtid(v) => v.to_string(),
            Self::MariaDbGtid(v) => v.to_string(),
            Self::BinlogFile(v) => v.to_string(),
        }
    }

    pub fn to_tagged(&self) -> String {
        match self {
            Self::Pg(v) => format!("pg:{v}"),
            Self::MySqlGtid(v) => format!("mysql-gtid:{v}"),
            Self::MariaDbGtid(v) => format!("mariadb-gtid:{v}"),
            Self::BinlogFile(v) => format!("binlog:{v}"),
        }
    }

    pub fn parse_tagged(s: &str) -> Result<Self, PositionError> {
        let err = |reason: String| PositionError::Parse {
            input: s.to_string(),
            reason,
        };
        let (tag, rest) = s
            .split_once(':')
            .ok_or_else(|| err("missing dialect tag".into()))?;
        match tag {
            "pg" => rest.parse().map(Self::Pg).map_err(|e| err(e.to_string())),
            "mysql-gtid" => rest
                .parse()
                .map(Self::MySqlGtid)
                .map_err(|e| err(e.to_string())),
            "mariadb-gtid" => rest
                .parse()
                .map(Self::MariaDbGtid)
                .map_err(|e| err(e.to_string())),
            "binlog" => rest
                .parse()
                .map(Self::BinlogFile)
                .map_err(|e| err(e.to_string())),
            other => Err(err(format!("unknown dialect {other:?}"))),
        }
    }
}

impl fmt::Display for SourcePosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_tagged())
    }
}

impl FromStr for SourcePosition {
    type Err = PositionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_tagged(s)
    }
}

impl Serialize for SourcePosition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SourcePosition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn pos(s: &str) -> SourcePosition {
        s.parse().unwrap()
    }

    #[test]
    fn untagged_is_the_dialect_text_without_the_tag() {
        for (tagged, untagged) in [
            ("pg:16/B374D848", "16/B374D848"),
            (&format!("mysql-gtid:{A}:1-5"), &format!("{A}:1-5")),
            ("mariadb-gtid:0-1-42", "0-1-42"),
            ("binlog:mysql-bin.000003:154", "mysql-bin.000003:154"),
        ] {
            assert_eq!(pos(tagged).untagged(), untagged);
        }
    }

    #[test]
    fn tagged_strings_round_trip_for_every_dialect() {
        for s in [
            "pg:16/B374D848".to_string(),
            format!("mysql-gtid:{A}:1-5"),
            "mysql-gtid:".to_string(),
            "mariadb-gtid:0-1-100,1-2-50".to_string(),
            "binlog:mysql-bin.000123:4567".to_string(),
        ] {
            assert_eq!(pos(&s).to_tagged(), s);
        }
    }

    #[test]
    fn rejects_unknown_or_malformed_tags() {
        for s in [
            "",
            "pg",
            "pg:zz",
            "oracle:1",
            "binlog:nofile",
            "mysql-gtid:bad",
        ] {
            assert!(
                matches!(
                    s.parse::<SourcePosition>(),
                    Err(PositionError::Parse { .. })
                ),
                "{s:?}"
            );
        }
    }

    #[test]
    fn covers_dispatches_per_dialect() {
        assert!(pos("pg:0/20").covers(&pos("pg:0/10")).unwrap());
        assert!(!pos("pg:0/10").covers(&pos("pg:0/20")).unwrap());
        assert!(
            pos(&format!("mysql-gtid:{A}:1-9"))
                .covers(&pos(&format!("mysql-gtid:{A}:3")))
                .unwrap()
        );
        assert!(
            pos("mariadb-gtid:0-1-9")
                .covers(&pos("mariadb-gtid:0-1-3"))
                .unwrap()
        );
        assert!(
            pos("binlog:b.000002:4")
                .covers(&pos("binlog:b.000001:900"))
                .unwrap()
        );
    }

    #[test]
    fn union_and_intersect_for_scalar_dialects() {
        assert_eq!(
            pos("pg:0/10").union(&pos("pg:0/20")).unwrap(),
            pos("pg:0/20")
        );
        assert_eq!(
            pos("pg:0/10").intersect(&pos("pg:0/20")).unwrap(),
            pos("pg:0/10")
        );
        assert_eq!(
            pos("binlog:b.000001:9")
                .union(&pos("binlog:b.000002:4"))
                .unwrap(),
            pos("binlog:b.000002:4")
        );
        assert_eq!(
            pos("binlog:b.000001:9")
                .intersect(&pos("binlog:b.000002:4"))
                .unwrap(),
            pos("binlog:b.000001:9")
        );
    }

    #[test]
    fn mixing_dialects_is_an_error() {
        let err = pos("pg:0/10")
            .covers(&pos("mariadb-gtid:0-1-1"))
            .unwrap_err();
        assert_eq!(
            err,
            PositionError::DialectMismatch {
                left: "pg",
                right: "mariadb-gtid"
            }
        );
    }

    #[test]
    fn different_binlog_series_is_an_error() {
        assert!(matches!(
            pos("binlog:a.000001:4").covers(&pos("binlog:b.000001:4")),
            Err(PositionError::DifferentBinlogSeries { .. })
        ));
    }

    #[test]
    fn serializes_as_a_tagged_json_string() {
        let p = pos("pg:0/16B3748");
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "\"pg:0/16B3748\"");
        assert_eq!(serde_json::from_str::<SourcePosition>(&json).unwrap(), p);
        assert!(serde_json::from_str::<SourcePosition>("\"nope:1\"").is_err());
    }
}
