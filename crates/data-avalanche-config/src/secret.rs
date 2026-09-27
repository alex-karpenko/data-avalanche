//! In-memory secret material (config §11.1): never printed, never in the diff output or the
//! validation report.

use std::fmt;

use serde::{Serialize, Serializer};

/// The cluster-wide key every fingerprint is keyed with (config §6.2, "Fingerprint key" note): 32
/// random bytes generated once by the node that creates the `globals.state` cluster anchor, with
/// the same create-if-absent conditional write that pins `tokens`, and never changed afterward.
/// Keying the hash means fingerprints agree cluster-wide and a leaked fingerprint can't be guessed
/// offline without it. It is not a config field, so it carries no `Serialize` impl.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FingerprintKey([u8; 32]);

impl FingerprintKey {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// 32 bytes from the OS random number generator (config §6.2: the node that creates the
    /// cluster anchor generates this once, then it never changes). Panics only if the OS RNG
    /// itself fails to produce randomness — a condition every `getrandom`-supported platform
    /// treats as unrecoverable, and there is no safe fallback for a security-sensitive key.
    pub fn random() -> Self {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("the OS random number generator is unavailable");
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for FingerprintKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FingerprintKey(***)")
    }
}

/// A resolved secret (password, token, private key, certificate text). `Debug` prints `***`.
/// `Serialize` writes the fingerprint computed at construction, so the reload diff (§11.1) sees
/// "changed / unchanged" without the value. `PartialEq` compares the value only — two secrets with
/// the same value are equal even if (hypothetically) fingerprinted with different keys.
#[derive(Clone)]
pub struct Secret {
    value: String,
    fingerprint: String,
}

impl Secret {
    pub fn new(value: impl Into<String>, key: &FingerprintKey) -> Self {
        let value = value.into();
        let fingerprint = fingerprint(key, value.as_bytes());
        Self { value, fingerprint }
    }

    pub fn expose(&self) -> &str {
        &self.value
    }
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for Secret {}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.fingerprint)
    }
}

/// `blake3:<64 hex digits>`: the keyed BLAKE3 hash of `bytes` under `key` (config §6.2). Used for
/// [`Secret`] and for [`crate::material::FileRef`]'s content fingerprint — the same function, so a
/// secret and a referenced file's contents fingerprint identically for the same bytes and key.
pub(crate) fn fingerprint(key: &FingerprintKey, bytes: &[u8]) -> String {
    format!(
        "blake3:{}",
        blake3::keyed_hash(key.as_bytes(), bytes).to_hex()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> FingerprintKey {
        FingerprintKey::from_bytes([seed; 32])
    }

    #[test]
    fn never_shows_the_value() {
        let k = key(1);
        let s = Secret::new("hunter2", &k);
        assert_eq!(format!("{s:?}"), "Secret(***)");
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("hunter2"));
        assert!(json.starts_with("\"blake3:"));
        assert_eq!(
            json,
            serde_json::to_string(&Secret::new("hunter2", &k)).unwrap()
        );
        assert_ne!(
            json,
            serde_json::to_string(&Secret::new("hunter3", &k)).unwrap()
        );
        assert_eq!(s.expose(), "hunter2");
    }

    /// The fingerprint is keyed: the same value under two different keys fingerprints differently,
    /// and under the same key it fingerprints the same way every time.
    #[test]
    fn fingerprint_is_keyed() {
        let a = Secret::new("hunter2", &key(1));
        let b = Secret::new("hunter2", &key(1));
        let c = Secret::new("hunter2", &key(2));
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
        assert_ne!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&c).unwrap()
        );
    }

    /// The fingerprint text never embeds the value it was computed from.
    #[test]
    fn fingerprint_does_not_contain_the_value() {
        let s = Secret::new("a-very-distinctive-secret-value", &key(3));
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("a-very-distinctive-secret-value"));
    }

    #[test]
    fn fingerprint_key_debug_hides_the_bytes() {
        let k = FingerprintKey::from_bytes([0x42; 32]);
        let dbg = format!("{k:?}");
        assert_eq!(dbg, "FingerprintKey(***)");
        assert!(!dbg.contains("42"));
    }

    #[test]
    fn from_bytes_round_trips() {
        let bytes = [9u8; 32];
        let k = FingerprintKey::from_bytes(bytes);
        assert_eq!(k.as_bytes(), &bytes);
    }
}
