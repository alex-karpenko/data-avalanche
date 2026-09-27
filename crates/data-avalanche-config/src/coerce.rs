//! Scalar coercion: the text of a [`crate::Leaf`] becomes the field's type.

use std::net::IpAddr;
use std::time::Duration;

use data_avalanche_core::LogicalType;
use serde::{Serialize, Serializer};

use crate::diag::TYPE;
use crate::units::{ByteSize, HostPort, parse_duration};

/// A type a scalar leaf can coerce to.
pub(crate) trait LeafType: Sized {
    /// The rule that reports a failed coercion: the value-domain V-rule for enums (V-015, …),
    /// [`TYPE`] for plain types.
    const RULE: &'static str = TYPE;

    fn parse_leaf(text: &str) -> Result<Self, String>;
}

impl LeafType for String {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        Ok(text.to_string())
    }
}

impl LeafType for bool {
    /// Only `true` and `false` (YAML 1.2): `yes`, `on`, `True` are not booleans.
    fn parse_leaf(text: &str) -> Result<Self, String> {
        match text {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!("{text:?} is not a boolean (true or false)")),
        }
    }
}

macro_rules! integer_leaf {
    ($($t:ty),+) => {$(
        impl LeafType for $t {
            /// Decimal digits only; no hex, octal or exponent forms.
            fn parse_leaf(text: &str) -> Result<Self, String> {
                if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
                    text.parse().map_err(|_| format!("{text:?} is out of range for {}", stringify!($t)))
                } else {
                    Err(format!("{text:?} is not a non-negative integer"))
                }
            }
        }
    )+};
}
integer_leaf!(u16, u32, u64);

impl LeafType for f64 {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        match text.parse::<f64>() {
            Ok(v) if v.is_finite() => Ok(v),
            _ => Err(format!("{text:?} is not a finite number")),
        }
    }
}

impl LeafType for Duration {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        parse_duration(text)
    }
}

impl LeafType for ByteSize {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse::<ByteSize>()
            .map_err(|e: crate::units::ByteSizeError| e.to_string())
    }
}

impl LeafType for HostPort {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse::<HostPort>()
            .map_err(|e: crate::units::HostPortError| e.to_string())
    }
}

impl LeafType for IpAddr {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse()
            .map_err(|_| format!("{text:?} is not an IP address"))
    }
}

impl LeafType for LogicalType {
    const RULE: &'static str = "V-078";

    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse()
            .map_err(|e: data_avalanche_core::LogicalTypeError| e.reason)
    }
}

/// `globals.log.level`: a level (`info`) or a `tracing` EnvFilter directive list
/// (`info,data_avalanche_sink=debug`), checked with the same parser the runtime uses (V-017).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFilter(pub String);

impl LeafType for LogFilter {
    const RULE: &'static str = "V-017";

    fn parse_leaf(text: &str) -> Result<Self, String> {
        if text.trim().is_empty() {
            return Err("log level is empty".into());
        }
        tracing_subscriber::EnvFilter::builder()
            .parse(text)
            .map(|_| LogFilter(text.to_string()))
            .map_err(|e| format!("{text:?} is not a log level or EnvFilter directive: {e}"))
    }
}

impl Serialize for LogFilter {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn booleans_are_only_true_and_false() {
        assert!(bool::parse_leaf("true").unwrap());
        for bad in ["yes", "on", "True", "1", ""] {
            assert!(bool::parse_leaf(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn integers_are_decimal_digits_in_range() {
        assert_eq!(u16::parse_leaf("5432").unwrap(), 5432);
        assert!(
            u16::parse_leaf("99999")
                .unwrap_err()
                .contains("out of range")
        );
        for bad in ["-1", "0x10", "1e3", "+5", " 5", ""] {
            assert!(u64::parse_leaf(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn floats_must_be_finite() {
        assert_eq!(f64::parse_leaf("0.15").unwrap(), 0.15);
        for bad in ["NaN", "inf", "abc"] {
            assert!(f64::parse_leaf(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn logical_types_and_log_filters_carry_their_rules() {
        assert_eq!(<LogicalType as LeafType>::RULE, "V-078");
        assert!(LogicalType::parse_leaf("decimal(38,10)").is_ok());
        assert!(LogicalType::parse_leaf("timestamp").is_err());
        assert!(LogFilter::parse_leaf("info").is_ok());
        assert!(LogFilter::parse_leaf("info,data_avalanche_sink=debug").is_ok());
        assert!(LogFilter::parse_leaf("info,[=").is_err());
        assert!(LogFilter::parse_leaf("").is_err());
        assert_eq!(<LogFilter as LeafType>::RULE, "V-017");
    }
}
