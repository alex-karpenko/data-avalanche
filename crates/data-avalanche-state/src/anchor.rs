//! The cluster anchor `cluster/anchor.json` in `globals.state` (Clustering §12, config §6.2): the
//! cluster-wide settings every node must share, fixed by the first node to start (first writer
//! wins). A node whose config disagrees refuses to start (V-062, V-076, V-094 — checked by the
//! config crate's `check_cluster_anchor`).

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::doc::{CREATE_ATTEMPTS, DocStore, Document, StateError};

pub const ANCHOR_KEY: &str = "cluster/anchor.json";

/// The placement hash every node uses to build the ring. v1 has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgo {
    Xxh3,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnchorDoc {
    pub tokens: u32,
    pub lease_duration_ms: u64,
    /// `globals.cluster.lock_backend` as written in config (`storage`).
    pub lock_backend: String,
    pub hash_algo: HashAlgo,
    /// The key for secret and file fingerprints (config §6.2), 64 lowercase hex digits on disk.
    #[serde(serialize_with = "hex_out", deserialize_with = "hex_in")]
    pub fingerprint_key: [u8; 32],
    pub established_by: String,
    pub established_at: DateTime<Utc>,
}

impl Document for AnchorDoc {
    const KIND: &'static str = "cluster anchor";
    const SCHEMA_VERSION: u64 = 1;
}

impl fmt::Debug for AnchorDoc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnchorDoc")
            .field("tokens", &self.tokens)
            .field("lease_duration_ms", &self.lease_duration_ms)
            .field("lock_backend", &self.lock_backend)
            .field("hash_algo", &self.hash_algo)
            .field("fingerprint_key", &"***")
            .field("established_by", &self.established_by)
            .field("established_at", &self.established_at)
            .finish()
    }
}

fn hex_out<S: Serializer>(key: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&key.iter().map(|b| format!("{b:02x}")).collect::<String>())
}

fn hex_in<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
    let text = String::deserialize(d)?;
    let invalid = || serde::de::Error::custom("fingerprint_key must be 64 lowercase hex digits");
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let mut key = [0u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).map_err(|_| invalid())?;
    }
    Ok(key)
}

/// What [`establish_anchor`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    pub doc: AnchorDoc,
    /// True when this call created it; false when it existed (or another node won the race).
    pub created: bool,
}

/// Returns the cluster anchor, creating it from `proposal` when there is none. When several
/// nodes race, exactly one creates it and all of them return that one.
///
/// # Errors
/// A storage failure, or a stored anchor that isn't a valid document.
pub async fn establish_anchor(docs: &DocStore, proposal: &AnchorDoc) -> Result<Anchor, StateError> {
    let mut attempts = 0;
    // Set once this call's own create returns a race error: an unknown outcome
    // (`Store(Contention)`) may still have landed.
    let mut attempted = false;
    loop {
        attempts += 1;
        if let Some(existing) = docs.load::<AnchorDoc>(ANCHOR_KEY).await? {
            let created = attempted && existing.doc == *proposal;
            return Ok(Anchor {
                doc: existing.doc,
                created,
            });
        }
        attempted = true;
        match docs.create(ANCHOR_KEY, proposal).await {
            Ok(_) => {
                return Ok(Anchor {
                    doc: proposal.clone(),
                    created: true,
                });
            }
            Err(e) if e.is_race() && attempts < CREATE_ATTEMPTS => continue,
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::test_support::WriteThenLoseResponse;

    fn docs() -> DocStore {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        DocStore::new(Arc::new(store))
    }

    fn proposal(node: &str, key_byte: u8) -> AnchorDoc {
        AnchorDoc {
            tokens: 128,
            lease_duration_ms: 15_000,
            lock_backend: "storage".into(),
            hash_algo: HashAlgo::Xxh3,
            fingerprint_key: [key_byte; 32],
            established_by: node.into(),
            established_at: Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn first_writer_wins() {
        let d = docs();
        let a = establish_anchor(&d, &proposal("a", 1)).await.unwrap();
        assert_eq!(
            a,
            Anchor {
                doc: proposal("a", 1),
                created: true
            }
        );
        let b = establish_anchor(&d, &proposal("b", 2)).await.unwrap();
        assert_eq!(
            b,
            Anchor {
                doc: proposal("a", 1),
                created: false
            },
            "the anchor never changes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_establishers_agree_on_one_anchor() {
        let d = docs();
        let tasks: Vec<_> = (0..8u8)
            .map(|i| {
                let d = d.clone();
                tokio::spawn(
                    async move { establish_anchor(&d, &proposal(&format!("n{i}"), i)).await },
                )
            })
            .collect();
        let mut results = vec![];
        for t in tasks {
            results.push(t.await.unwrap().unwrap());
        }
        assert_eq!(results.iter().filter(|a| a.created).count(), 1);
        let stored = d.load::<AnchorDoc>(ANCHOR_KEY).await.unwrap().unwrap().doc;
        assert!(
            results.iter().all(|a| a.doc == stored),
            "every node sees the stored anchor"
        );
    }

    #[test]
    fn anchor_json_shape() {
        let mut doc = proposal("worker-7f3a9c", 0);
        doc.fingerprint_key[0] = 0xab;
        doc.fingerprint_key[31] = 0x01;
        let json = String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap();
        assert_eq!(
            json,
            format!(
                r#"{{
  "version": 1,
  "tokens": 128,
  "lease_duration_ms": 15000,
  "lock_backend": "storage",
  "hash_algo": "xxh3",
  "fingerprint_key": "ab{}01",
  "established_by": "worker-7f3a9c",
  "established_at": "2026-09-28T10:00:00Z"
}}"#,
                "0".repeat(60)
            )
        );
        assert_eq!(
            crate::doc::decode::<AnchorDoc>("k", json.as_bytes()).unwrap(),
            doc
        );
    }

    #[tokio::test]
    async fn a_bad_stored_anchor_is_refused_without_echoing_the_key() {
        let d = docs();
        let good = String::from_utf8(crate::doc::encode(&proposal("a", 0x5c)).to_vec()).unwrap();
        let key_hex = "5c".repeat(32);
        for bad in [
            good.replace(&key_hex, &format!("{}Z", &key_hex[..63])),
            good.replace(&key_hex, &key_hex[..62]),
            good.replace(&key_hex, &key_hex.to_uppercase()),
            good.replace("xxh3", "murmur3"),
        ] {
            d.store()
                .put(ANCHOR_KEY, Bytes::from(bad.clone()))
                .await
                .unwrap();
            let e = establish_anchor(&d, &proposal("b", 1)).await.unwrap_err();
            assert!(matches!(e, StateError::Corrupt { .. }), "{bad}: {e:?}");
            let text = e.to_string();
            assert!(!text.contains("5c5c") && !text.contains("5C5C"), "{text}");
        }
    }

    #[tokio::test]
    async fn establish_anchor_recognizes_its_own_create_that_landed_despite_contention() {
        let store = WriteThenLoseResponse::new(
            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap(),
        );
        let d = DocStore::new(Arc::new(store));
        let p = proposal("a", 1);
        let a = establish_anchor(&d, &p).await.unwrap();
        assert_eq!(
            a,
            Anchor {
                doc: p,
                created: true
            },
            "the create landed even though the response reported Contention"
        );
    }

    #[test]
    fn debug_hides_the_fingerprint_key() {
        let text = format!("{:?}", proposal("a", 0x5c));
        assert!(
            text.contains("fingerprint_key: \"***\"") && !text.contains("92"),
            "{text}"
        );
    }
}
