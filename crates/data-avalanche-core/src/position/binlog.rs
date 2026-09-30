use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// A binlog file/position (MySQL or MariaDB `replication_type: binlog`, P-4). Ordered by the
/// numeric file index, then the position; positions from different log series (base names) are
/// not comparable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BinlogFilePos {
    file: String,
    base_len: usize,
    index: u64,
    pos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid binlog position {0:?} (expected <base>.<NNNNNN>:<pos>)")]
pub struct BinlogPosError(pub String);

/// Splits `mysql-bin.000123` into (`mysql-bin`, 123).
pub fn parse_binlog_index(file: &str) -> Result<(&str, u64), BinlogPosError> {
    let err = || BinlogPosError(file.to_string());
    let (base, suffix) = file.rsplit_once('.').ok_or_else(err)?;
    if base.is_empty() || suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err());
    }
    Ok((base, suffix.parse().map_err(|_| err())?))
}

impl BinlogFilePos {
    pub fn new(file: &str, pos: u64) -> Result<Self, BinlogPosError> {
        let (base, index) = parse_binlog_index(file)?;
        Ok(Self {
            file: file.to_string(),
            base_len: base.len(),
            index,
            pos,
        })
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn base(&self) -> &str {
        &self.file[..self.base_len]
    }

    pub fn file_index(&self) -> u64 {
        self.index
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Order within one log series; `None` across series (e.g. after a source failover).
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        (self.base() == other.base()).then(|| (self.index, self.pos).cmp(&(other.index, other.pos)))
    }
}

impl FromStr for BinlogFilePos {
    type Err = BinlogPosError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (file, pos) = s
            .rsplit_once(':')
            .ok_or_else(|| BinlogPosError(s.to_string()))?;
        let pos: u64 = pos.parse().map_err(|_| BinlogPosError(s.to_string()))?;
        Self::new(file, pos).map_err(|_| BinlogPosError(s.to_string()))
    }
}

impl fmt::Display for BinlogFilePos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    fn p(s: &str) -> BinlogFilePos {
        s.parse().unwrap()
    }

    #[test]
    fn parses_file_and_position() {
        let pos = p("mysql-bin.000123:4567");
        assert_eq!(
            (pos.file(), pos.base(), pos.file_index(), pos.pos()),
            ("mysql-bin.000123", "mysql-bin", 123, 4567)
        );
        assert_eq!(pos.to_string(), "mysql-bin.000123:4567");
    }

    #[test]
    fn orders_across_rotation_by_index_not_text() {
        assert_eq!(
            p("mysql-bin.000999:900").compare(&p("mysql-bin.001000:4")),
            Some(Ordering::Less)
        );
        assert_eq!(
            p("mysql-bin.000010:4").compare(&p("mysql-bin.000009:500")),
            Some(Ordering::Greater)
        );
        assert_eq!(
            p("mysql-bin.000010:4").compare(&p("mysql-bin.000010:4")),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn different_log_series_do_not_compare() {
        assert_eq!(
            p("mysql-bin.000001:4").compare(&p("other-bin.000001:4")),
            None
        );
    }

    #[test]
    fn rejects_malformed_positions() {
        for s in [
            "mysql-bin.000001",
            "mysql-bin:4",
            "mysql-bin.abc:4",
            "mysql-bin.000001:x",
            ":4",
            "mysql-bin.:4",
        ] {
            assert!(s.parse::<BinlogFilePos>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn extracts_index_from_file_name() {
        assert_eq!(
            parse_binlog_index("mariadb-bin.000042").unwrap(),
            ("mariadb-bin", 42)
        );
        assert!(parse_binlog_index("no-suffix").is_err());
    }
}
