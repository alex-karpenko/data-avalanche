use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// A PostgreSQL WAL position (`pg_lsn`): 64 bits, written `X/Y` with two hex halves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Lsn(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid LSN {0:?} (expected X/Y with 1-8 hex digits each)")]
pub struct LsnParseError(pub String);

impl FromStr for Lsn {
    type Err = LsnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || LsnParseError(s.to_string());
        let (hi, lo) = s.split_once('/').ok_or_else(err)?;
        let half = |part: &str| {
            if part.is_empty() || part.len() > 8 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(err());
            }
            u32::from_str_radix(part, 16).map_err(|_| err())
        };
        Ok(Lsn((u64::from(half(hi)?) << 32) | u64::from(half(lo)?)))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_postgres_text_form() {
        assert_eq!("0/16B3748".parse::<Lsn>().unwrap(), Lsn(0x16B_3748));
        assert_eq!("16/B374D848".parse::<Lsn>().unwrap(), Lsn(0x16_B374_D848));
        assert_eq!(
            "0/16b3748".parse::<Lsn>().unwrap(),
            Lsn(0x16B_3748),
            "lower-case hex accepted"
        );
    }

    #[test]
    fn prints_upper_case_like_postgres() {
        assert_eq!(Lsn(0x16_B374_D848).to_string(), "16/B374D848");
        assert_eq!(Lsn(0).to_string(), "0/0");
    }

    #[test]
    fn orders_numerically() {
        assert!("0/FFFFFFFF".parse::<Lsn>().unwrap() < "1/0".parse::<Lsn>().unwrap());
    }

    #[test]
    fn rejects_malformed_text() {
        for s in ["", "0", "/1", "1/", "0/123456789", "g/1", "0/1/2", " 0/1"] {
            assert!(s.parse::<Lsn>().is_err(), "{s:?}");
        }
    }

    proptest! {
        #[test]
        fn round_trips(v in any::<u64>()) {
            prop_assert_eq!(Lsn(v).to_string().parse::<Lsn>().unwrap(), Lsn(v));
        }
    }
}
