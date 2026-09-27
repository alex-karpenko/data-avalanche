//! Value types with a text grammar of their own: durations (§4.1), byte sizes (§4.2), `host:port`.

use std::fmt;
use std::net::Ipv6Addr;
use std::str::FromStr;
use std::time::Duration;

use serde::{Serialize, Serializer};

/// A text that is not a byte size (config §4.2).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ByteSizeError(String);

/// A text that is not `host:port`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct HostPortError(String);

/// Parses a `humantime` duration (`200ms`, `5s`, `1h30m`, `30d`). A bare integer is not a duration
/// (§4.1), except `0`, which `schema_evolution.rescan_interval` uses to mean "disabled".
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    if text == "0" {
        return Ok(Duration::ZERO);
    }
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{text:?} has no unit; write e.g. \"{text}s\""));
    }
    humantime::parse_duration(text).map_err(|e| format!("{text:?} is not a duration: {e}"))
}

/// `humantime` text form of a duration (`15s`, `1h 30m`), for messages and the reload diff.
pub fn format_duration(d: Duration) -> String {
    humantime::format_duration(d).to_string()
}

/// `#[serde(serialize_with = "crate::units::ser_duration")]`: a duration serializes as its text form.
pub fn ser_duration<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format_duration(*d))
}

/// A byte count (§4.2): `512MiB`, `256MB`, `1GiB`, or a bare integer meaning bytes. Binary
/// (`KiB`/`MiB`/`GiB`) and decimal (`KB`/`MB`/`GB`) units are both accepted and are not equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub const KIB: u64 = 1 << 10;
    pub const MIB: u64 = 1 << 20;
    pub const GIB: u64 = 1 << 30;

    pub fn bytes(self) -> u64 {
        self.0
    }
}

const UNITS: [(&str, u64); 6] = [
    ("KiB", ByteSize::KIB),
    ("MiB", ByteSize::MIB),
    ("GiB", ByteSize::GIB),
    ("KB", 1_000),
    ("MB", 1_000_000),
    ("GB", 1_000_000_000),
];

impl FromStr for ByteSize {
    type Err = ByteSizeError;

    fn from_str(text: &str) -> Result<Self, ByteSizeError> {
        let digits = text.bytes().take_while(u8::is_ascii_digit).count();
        let (number, unit) = text.split_at(digits);
        let bad = || {
            ByteSizeError(format!(
                "{text:?} is not a byte size; write e.g. 512MiB, 256MB or a number of bytes"
            ))
        };
        if number.is_empty() {
            return Err(bad());
        }
        let n: u64 = number.parse().map_err(|_| bad())?;
        let factor = if unit.is_empty() {
            1
        } else {
            UNITS
                .iter()
                .find(|(u, _)| *u == unit)
                .map(|(_, f)| *f)
                .ok_or_else(bad)?
        };
        n.checked_mul(factor)
            .map(ByteSize)
            .ok_or_else(|| ByteSizeError(format!("{text:?} is too large")))
    }
}

impl fmt::Display for ByteSize {
    /// The largest unit that divides the value exactly (`512MiB`, `512MB`), else plain bytes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let units = [
            ("GiB", Self::GIB),
            ("MiB", Self::MIB),
            ("GB", 1_000_000_000),
            ("MB", 1_000_000),
            ("KiB", Self::KIB),
            ("KB", 1_000),
        ];
        for (unit, factor) in units {
            if self.0 != 0 && self.0.is_multiple_of(factor) {
                return write!(f, "{}{unit}", self.0 / factor);
            }
        }
        write!(f, "{}", self.0)
    }
}

/// `host:port` (§6.3 `api.address`, `memberlist.join_members`): an IPv4 address, a DNS name, or a
/// bracketed IPv6 address (`[::1]:8080`), then a port in 1..=65535.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl FromStr for HostPort {
    type Err = HostPortError;

    fn from_str(text: &str) -> Result<Self, HostPortError> {
        let bad = |why: &str| HostPortError(format!("{text:?} is not host:port: {why}"));
        let (host, port) = if let Some(rest) = text.strip_prefix('[') {
            let (h, p) = rest
                .split_once("]:")
                .ok_or_else(|| bad("expected [ipv6]:port"))?;
            h.parse::<Ipv6Addr>()
                .map_err(|_| bad("not an IPv6 address in brackets"))?;
            (h, p)
        } else {
            let (h, p) = text.rsplit_once(':').ok_or_else(|| bad("missing :port"))?;
            if h.contains(':') {
                return Err(bad("put an IPv6 address in brackets"));
            }
            (h, p)
        };
        if host.is_empty() {
            return Err(bad("empty host"));
        }
        let port: u16 = port.parse().map_err(|_| bad("port is not 1..=65535"))?;
        if port == 0 {
            return Err(bad("port is not 1..=65535"));
        }
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl Serialize for HostPort {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_need_a_unit_except_zero() {
        assert_eq!(parse_duration("200ms").unwrap(), Duration::from_millis(200));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(
            parse_duration("30d").unwrap(),
            Duration::from_secs(30 * 86_400)
        );
        assert_eq!(parse_duration("60m").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert!(parse_duration("30").unwrap_err().contains("no unit"));
        assert!(parse_duration("5 parsecs").is_err());
        assert!(parse_duration("").is_err());
        assert_eq!(format_duration(Duration::from_secs(15)), "15s");
    }

    #[test]
    fn byte_sizes_distinguish_binary_and_decimal_units() {
        assert_eq!("512MiB".parse::<ByteSize>().unwrap().bytes(), 536_870_912);
        assert_eq!("512MB".parse::<ByteSize>().unwrap().bytes(), 512_000_000);
        assert_eq!("1GiB".parse::<ByteSize>().unwrap().bytes(), 1 << 30);
        assert_eq!("4096".parse::<ByteSize>().unwrap().bytes(), 4096);
        for bad in [
            "",
            "MiB",
            "1.5GiB",
            "512 MiB",
            "512mib",
            "-1",
            "1TiB",
            "99999999999GiB",
        ] {
            assert!(bad.parse::<ByteSize>().is_err(), "{bad:?}");
        }
        assert_eq!(ByteSize(536_870_912).to_string(), "512MiB");
        assert_eq!(ByteSize(512_000_000).to_string(), "512MB");
        assert_eq!(ByteSize(4096).to_string(), "4KiB");
        assert_eq!(ByteSize(1001).to_string(), "1001");
    }

    #[test]
    fn host_port_accepts_names_ipv4_and_bracketed_ipv6() {
        let hp: HostPort = "0.0.0.0:8080".parse().unwrap();
        assert_eq!((hp.host.as_str(), hp.port), ("0.0.0.0", 8080));
        let hp: HostPort = "cdc-headless.svc.cluster.local:7946".parse().unwrap();
        assert_eq!(hp.port, 7946);
        let hp: HostPort = "[::1]:9000".parse().unwrap();
        assert_eq!(hp.host, "::1");
        assert_eq!(hp.to_string(), "[::1]:9000");
        for bad in ["host", ":80", "h:0", "h:70000", "::1:80", "[zz]:1", "[::1]"] {
            assert!(bad.parse::<HostPort>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn byte_size_error_display_text() {
        assert_eq!(
            "12XB".parse::<ByteSize>().unwrap_err().to_string(),
            "\"12XB\" is not a byte size; write e.g. 512MiB, 256MB or a number of bytes"
        );
    }

    #[test]
    fn host_port_error_display_text() {
        assert_eq!(
            "host:99999".parse::<HostPort>().unwrap_err().to_string(),
            "\"host:99999\" is not host:port: port is not 1..=65535"
        );
    }
}
