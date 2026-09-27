//! In-memory secret material (config §11.1): never printed, never in the diff output or the
//! validation report.

use std::fmt;

use serde::{Serialize, Serializer};
use xxhash_rust::xxh3::xxh3_128;

/// A resolved secret (password, token, private key, certificate text). `Debug` prints `***`.
/// `Serialize` writes only a fingerprint, so the reload diff (§11.1) sees "changed / unchanged"
/// without the value. The fingerprint is a fast non-cryptographic hash for change detection; it
/// never leaves the process.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&fingerprint(self.0.as_bytes()))
    }
}

/// `xxh3:<32 hex digits>` of `bytes`.
pub fn fingerprint(bytes: &[u8]) -> String {
    format!("xxh3:{:032x}", xxh3_128(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_shows_the_value() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?}"), "Secret(***)");
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("hunter2"));
        assert!(json.starts_with("\"xxh3:"));
        assert_eq!(
            json,
            serde_json::to_string(&Secret::new("hunter2")).unwrap()
        );
        assert_ne!(
            json,
            serde_json::to_string(&Secret::new("hunter3")).unwrap()
        );
        assert_eq!(s.expose(), "hunter2");
    }
}
