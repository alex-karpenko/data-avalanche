use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// Largest precision any v1 target stores (Arrow `Decimal128`, Delta/Iceberg `decimal`).
pub const MAX_DECIMAL_PRECISION: u8 = 38;

/// A fixed-point decimal: `unscaled × 10^-scale`, up to 38 significant digits (D-IMPL-1).
/// `rust_decimal` holds only 28–29 digits, too few for `decimal(38, S)`.
///
/// `Eq`/`Hash` compare the representation (`unscaled`, `scale`), not the mathematical value:
/// `1.0 != 1.00` because their `scale`s differ, even though both denote the same number.
/// Callers that key or compare by value (e.g. collapse-by-key, map lookups) must first
/// [`rescale`](Decimal::rescale) both sides to the column's declared scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    unscaled: i128,
    scale: i8,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecimalError {
    #[error("not a decimal number: {0:?}")]
    Invalid(String),
    #[error("decimal is not finite: {0:?}")]
    NotFinite(String),
    #[error("decimal {0:?} has more than 38 significant digits or fraction digits")]
    TooManyDigits(String),
    #[error("rescaling {value} to scale {scale} would lose digits or exceed 38 digits")]
    Rescale { value: String, scale: i8 },
}

impl Decimal {
    pub fn new(unscaled: i128, scale: i8) -> Self {
        Self { unscaled, scale }
    }

    pub fn unscaled(&self) -> i128 {
        self.unscaled
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }

    /// Parses the text form PostgreSQL and MySQL print (`-12.340`), keeping the text's own scale.
    /// No exponent notation; `NaN`/`Infinity` are rejected as non-finite.
    pub fn parse(text: &str) -> Result<Self, DecimalError> {
        let t = text.trim();
        let bare = t.trim_start_matches(['+', '-']).to_ascii_lowercase();
        if matches!(bare.as_str(), "nan" | "infinity" | "inf") {
            return Err(DecimalError::NotFinite(text.to_string()));
        }
        let (negative, body) = match t.as_bytes().first() {
            Some(b'-') => (true, &t[1..]),
            Some(b'+') => (false, &t[1..]),
            _ => (false, t),
        };
        let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
        let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        if (int_part.is_empty() && frac_part.is_empty())
            || !all_digits(int_part)
            || !all_digits(frac_part)
        {
            return Err(DecimalError::Invalid(text.to_string()));
        }
        if frac_part.len() > usize::from(MAX_DECIMAL_PRECISION) {
            return Err(DecimalError::TooManyDigits(text.to_string()));
        }
        let digits = format!("{int_part}{frac_part}");
        let significant = digits.trim_start_matches('0');
        if significant.len() > usize::from(MAX_DECIMAL_PRECISION) {
            return Err(DecimalError::TooManyDigits(text.to_string()));
        }
        // At most 38 digits: cannot overflow i128 (max ≈ 1.7 × 10^38).
        let mut unscaled: i128 = 0;
        for b in significant.bytes() {
            unscaled = unscaled * 10 + i128::from(b - b'0');
        }
        if negative {
            unscaled = -unscaled;
        }
        let scale = i8::try_from(frac_part.len())
            .map_err(|_| DecimalError::TooManyDigits(text.to_string()))?;
        Ok(Self { unscaled, scale })
    }

    /// Number of decimal digits in the unscaled value (zero has precision 1).
    pub fn precision(&self) -> u8 {
        let mut n = self.unscaled.unsigned_abs();
        let mut digits = 1u8;
        while n >= 10 {
            n /= 10;
            digits += 1;
        }
        digits
    }

    /// Changes the scale without losing digits: padding zeros when the scale grows, dropping only
    /// zero digits when it shrinks. The result must stay within 38 digits.
    pub fn rescale(&self, scale: i8) -> Result<Self, DecimalError> {
        let err = || DecimalError::Rescale {
            value: self.to_string(),
            scale,
        };
        let diff = i16::from(scale) - i16::from(self.scale);
        if diff == 0 {
            return Ok(*self);
        }
        let factor = 10i128
            .checked_pow(u32::from(diff.unsigned_abs()))
            .ok_or_else(err)?;
        let out = if diff > 0 {
            Self {
                unscaled: self.unscaled.checked_mul(factor).ok_or_else(err)?,
                scale,
            }
        } else {
            if self.unscaled % factor != 0 {
                return Err(err());
            }
            Self {
                unscaled: self.unscaled / factor,
                scale,
            }
        };
        if out.precision() > MAX_DECIMAL_PRECISION {
            return Err(err());
        }
        Ok(out)
    }

    /// True when the value can be stored as `decimal(precision, scale)` without loss.
    pub fn fits(&self, precision: u8, scale: i8) -> bool {
        self.rescale(scale)
            .is_ok_and(|v| v.precision() <= precision)
    }
}

impl FromStr for Decimal {
    type Err = DecimalError;

    /// Same as [`Decimal::parse`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.unscaled < 0 { "-" } else { "" };
        let digits = self.unscaled.unsigned_abs().to_string();
        if self.scale <= 0 {
            let zeros = if self.unscaled == 0 {
                String::new()
            } else {
                "0".repeat(usize::from(self.scale.unsigned_abs()))
            };
            return write!(f, "{sign}{digits}{zeros}");
        }
        let scale = usize::from(self.scale.unsigned_abs());
        let padded = if digits.len() <= scale {
            format!("{}{digits}", "0".repeat(scale - digits.len() + 1))
        } else {
            digits
        };
        let (int, frac) = padded.split_at(padded.len() - scale);
        write!(f, "{sign}{int}.{frac}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_and_keeps_the_text_scale() {
        assert_eq!(Decimal::parse("-12.340").unwrap(), Decimal::new(-12_340, 3));
        assert_eq!(Decimal::parse("100").unwrap(), Decimal::new(100, 0));
        assert_eq!(Decimal::parse("+0.5").unwrap(), Decimal::new(5, 1));
        assert_eq!(Decimal::parse(" 7.0 ").unwrap(), Decimal::new(70, 1));
        assert_eq!(Decimal::parse("-0.00").unwrap(), Decimal::new(0, 2));
    }

    #[test]
    fn displays_with_exact_scale() {
        for s in ["-12.340", "100", "0.5", "-0.5", "123456.000001"] {
            assert_eq!(Decimal::parse(s).unwrap().to_string(), s);
        }
    }

    #[test]
    fn from_str_is_parse() {
        let d: Decimal = "-12.340".parse().unwrap();
        assert_eq!(d, Decimal::parse("-12.340").unwrap());
        assert_eq!(d.to_string(), "-12.340");
        assert_eq!("NaN".parse::<Decimal>(), Decimal::parse("NaN"));
    }

    #[test]
    fn displays_leading_zeros_for_small_values() {
        assert_eq!(Decimal::new(5, 4).to_string(), "0.0005");
        assert_eq!(Decimal::new(-5, 4).to_string(), "-0.0005");
        assert_eq!(Decimal::new(0, 2).to_string(), "0.00");
    }

    #[test]
    fn rejects_malformed_text() {
        for s in ["", ".", "-", "1.2.3", "1e5", "abc", "1 2", "--1", "0x10"] {
            assert!(
                matches!(Decimal::parse(s), Err(DecimalError::Invalid(_))),
                "{s:?}"
            );
        }
    }

    #[test]
    fn rejects_non_finite_values() {
        for s in ["NaN", "nan", "Infinity", "-Infinity", "+infinity", "inf"] {
            assert!(
                matches!(Decimal::parse(s), Err(DecimalError::NotFinite(_))),
                "{s:?}"
            );
        }
    }

    #[test]
    fn accepts_38_significant_digits_but_not_39() {
        let d38 = "9".repeat(38);
        assert!(Decimal::parse(&d38).is_ok());
        assert!(
            Decimal::parse(&format!("000{d38}")).is_ok(),
            "leading zeros are not significant"
        );
        let d39 = "9".repeat(39);
        assert!(matches!(
            Decimal::parse(&d39),
            Err(DecimalError::TooManyDigits(_))
        ));
    }

    #[test]
    fn rejects_scale_above_38() {
        let s = format!("0.{}1", "0".repeat(38));
        assert!(matches!(
            Decimal::parse(&s),
            Err(DecimalError::TooManyDigits(_))
        ));
    }

    #[test]
    fn counts_precision_in_digits() {
        assert_eq!(Decimal::new(0, 0).precision(), 1);
        assert_eq!(Decimal::new(-12_340, 3).precision(), 5);
        assert_eq!(Decimal::new(10i128.pow(37), 0).precision(), 38);
    }

    #[test]
    fn rescales_up_and_down_without_loss() {
        let d = Decimal::parse("12.30").unwrap();
        assert_eq!(d.rescale(4).unwrap(), Decimal::new(123_000, 4));
        assert_eq!(d.rescale(1).unwrap(), Decimal::new(123, 1));
        assert_eq!(d.rescale(2).unwrap(), d);
    }

    #[test]
    fn refuses_lossy_or_overflowing_rescale() {
        assert!(Decimal::parse("12.34").unwrap().rescale(1).is_err());
        assert!(
            Decimal::new(10i128.pow(37), 0).rescale(2).is_err(),
            "would exceed 38 digits"
        );
    }

    #[test]
    fn fits_checks_integer_and_fraction_room() {
        let d = Decimal::parse("123.45").unwrap();
        assert!(d.fits(5, 2));
        assert!(d.fits(6, 3));
        assert!(!d.fits(4, 2));
        assert!(!Decimal::parse("1.005").unwrap().fits(5, 2));
        assert!(Decimal::new(0, 0).fits(1, 1));
    }

    proptest! {
        #[test]
        fn display_then_parse_round_trips(
            unscaled in -(10i128.pow(38) - 1)..=(10i128.pow(38) - 1),
            scale in 0i8..=38,
        ) {
            let d = Decimal::new(unscaled, scale);
            prop_assert_eq!(Decimal::parse(&d.to_string()).unwrap(), d);
        }
    }
}
