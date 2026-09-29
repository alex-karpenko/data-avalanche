//! Config generations in `globals.state` (Clustering §9): the CAS pointer
//! `config/generation.json` and immutable snapshots of the config file text. Each node resolves
//! the text itself (its own environment and files), so a snapshot holds the raw text, never a
//! resolved config.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::doc::{DocStore, Document, StateError, Versioned};

pub const GENERATION_KEY: &str = "config/generation.json";

/// The generation pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationDoc {
    /// 1 at bootstrap, +1 per published reload.
    pub generation: u64,
    /// `blake3:<64 hex>` of the snapshot text.
    pub hash: String,
    pub written_by: String,
    pub written_at: DateTime<Utc>,
}

impl Document for GenerationDoc {
    const KIND: &'static str = "config generation";
    const SCHEMA_VERSION: u64 = 1;
}

/// A generation with its config text.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigGeneration {
    pub pointer: Versioned<GenerationDoc>,
    pub text: String,
}

/// `blake3:<64 hex>` of `text`.
pub fn config_hash(text: &str) -> String {
    format!("blake3:{}", blake3::hash(text.as_bytes()).to_hex())
}

/// `config/snapshots/<generation, 6+ digits>-<hash hex>.yaml`. Content-addressed, so two nodes
/// racing to publish the same generation never overwrite each other's snapshot.
pub fn config_snapshot_key(generation: u64, hash: &str) -> String {
    let hex = hash.strip_prefix("blake3:").unwrap_or(hash);
    format!("config/snapshots/{generation:06}-{hex}.yaml")
}

/// The config generation operations over the `globals.state` documents.
#[derive(Debug, Clone)]
pub struct ConfigGenerations {
    docs: DocStore,
}

impl ConfigGenerations {
    /// `docs` is rooted at the `globals.state` location.
    pub fn new(docs: DocStore) -> Self {
        Self { docs }
    }

    /// The current generation and its text, or `None` before the first bootstrap.
    ///
    /// # Errors
    /// A storage or decode failure; `Corrupt` when the snapshot is missing or its text doesn't
    /// match the pointer's hash.
    pub async fn current(&self) -> Result<Option<ConfigGeneration>, StateError> {
        let Some(pointer) = self.docs.load::<GenerationDoc>(GENERATION_KEY).await? else {
            return Ok(None);
        };
        let key = config_snapshot_key(pointer.doc.generation, &pointer.doc.hash);
        let corrupt = |reason: &str| StateError::Corrupt {
            key: key.clone(),
            kind: "config snapshot",
            reason: reason.to_owned(),
        };
        let Some((body, _)) = self.docs.store().get(&key).await? else {
            return Err(corrupt("missing; config/generation.json points at it"));
        };
        let text = String::from_utf8(body.to_vec()).map_err(|_| corrupt("not UTF-8"))?;
        if config_hash(&text) != pointer.doc.hash {
            return Err(corrupt(
                "its text doesn't match the hash in config/generation.json",
            ));
        }
        Ok(Some(ConfigGeneration { pointer, text }))
    }

    /// Cold start (§9.3): adopts the current generation if there is one; otherwise publishes
    /// `text` as generation 1. Concurrent bootstrappers race on the pointer's create; the losers
    /// adopt the winner's generation. Returns the generation and whether this call created it.
    ///
    /// # Errors
    /// As [`Self::current`], or a storage failure while writing.
    pub async fn bootstrap_or_adopt(
        &self,
        text: &str,
        node_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(ConfigGeneration, bool), StateError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            if let Some(current) = self.current().await? {
                return Ok((current, false));
            }
            let doc = self.write_snapshot(1, text, node_id, now).await?;
            match self.docs.create(GENERATION_KEY, &doc).await {
                Ok(version) => {
                    let pointer = Versioned { doc, version };
                    return Ok((
                        ConfigGeneration {
                            pointer,
                            text: text.to_owned(),
                        },
                        true,
                    ));
                }
                Err(e) if e.is_race() && attempts < 3 => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Reload (§9.2): publishes `text` as the generation after `base`, with a CAS on the pointer.
    ///
    /// # Errors
    /// `Store(Conflict)` when another node published since `base` was read: re-read, re-validate
    /// against the new generation and try again. The losing snapshot stays behind, unreferenced.
    pub async fn publish(
        &self,
        base: &Versioned<GenerationDoc>,
        text: &str,
        node_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ConfigGeneration, StateError> {
        let doc = self
            .write_snapshot(base.doc.generation + 1, text, node_id, now)
            .await?;
        let version = self
            .docs
            .update(GENERATION_KEY, &base.version, &doc)
            .await?;
        Ok(ConfigGeneration {
            pointer: Versioned { doc, version },
            text: text.to_owned(),
        })
    }

    async fn write_snapshot(
        &self,
        generation: u64,
        text: &str,
        node_id: &str,
        now: DateTime<Utc>,
    ) -> Result<GenerationDoc, StateError> {
        let hash = config_hash(text);
        let key = config_snapshot_key(generation, &hash);
        match self.docs.store().create(&key, text.to_owned().into()).await {
            // Same key = same generation and same text: someone already wrote these exact bytes.
            Ok(_) | Err(data_avalanche_storage::StoreError::Conflict { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(GenerationDoc {
            generation,
            hash,
            written_by: node_id.to_owned(),
            written_at: now,
        })
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

    fn gens() -> ConfigGenerations {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        ConfigGenerations::new(DocStore::new(Arc::new(store)))
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, s).unwrap()
    }

    const V1: &str = "globals:\n  state:\n    bucket: ops\n";
    const V2: &str = "globals:\n  state:\n    bucket: ops\n# reload\n";

    #[test]
    fn keys_and_hashes() {
        let h = config_hash("x");
        assert_eq!(h, format!("blake3:{}", blake3::hash(b"x").to_hex()));
        assert_eq!(
            config_snapshot_key(1, &h),
            format!("config/snapshots/000001-{}.yaml", &h[7..])
        );
        assert_eq!(
            config_snapshot_key(1_234_567, "blake3:ab"),
            "config/snapshots/1234567-ab.yaml"
        );
    }

    #[tokio::test]
    async fn bootstrap_creates_generation_1_then_later_starts_adopt_it() {
        let g = gens();
        assert!(g.current().await.unwrap().is_none());
        let (first, created) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        assert!(created);
        assert_eq!(
            first.pointer.doc,
            GenerationDoc {
                generation: 1,
                hash: config_hash(V1),
                written_by: "a".into(),
                written_at: t(0)
            }
        );
        assert_eq!(first.text, V1);
        let (adopted, created) = g.bootstrap_or_adopt(V2, "b", t(1)).await.unwrap();
        assert!(
            !created,
            "a started node adopts the cloud generation, not its own file (§9.3)"
        );
        assert_eq!(adopted, first);
        assert_eq!(g.current().await.unwrap(), Some(first));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_bootstraps_adopt_one_winner() {
        let g = gens();
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let g = g.clone();
                tokio::spawn(async move {
                    let text = format!("# node {i}\n");
                    g.bootstrap_or_adopt(&text, &format!("n{i}"), t(0)).await
                })
            })
            .collect();
        let mut results = vec![];
        for task in tasks {
            results.push(task.await.unwrap().unwrap());
        }
        assert_eq!(results.iter().filter(|(_, created)| *created).count(), 1);
        let current = g.current().await.unwrap().unwrap();
        assert!(
            results
                .iter()
                .all(|(g2, _)| g2.text == current.text && g2.pointer.doc == current.pointer.doc)
        );
    }

    #[tokio::test]
    async fn publish_advances_the_generation_with_cas() {
        let g = gens();
        let (first, _) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        let second = g.publish(&first.pointer, V2, "b", t(5)).await.unwrap();
        assert_eq!(
            (second.pointer.doc.generation, second.text.as_str()),
            (2, V2)
        );
        assert_eq!(g.current().await.unwrap(), Some(second.clone()));
        let e = g
            .publish(&first.pointer, "# racing reload\n", "c", t(6))
            .await
            .unwrap_err();
        assert!(
            e.is_conflict(),
            "a reload from a stale generation loses: {e:?}"
        );
        assert_eq!(
            g.current().await.unwrap(),
            Some(second),
            "the losing reload changed nothing"
        );
    }

    #[tokio::test]
    async fn racing_publishers_keep_their_own_snapshots() {
        let g = gens();
        let (first, _) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        let winner = g.publish(&first.pointer, V2, "b", t(5)).await.unwrap();
        g.publish(&first.pointer, "# loser\n", "c", t(6))
            .await
            .unwrap_err();
        let keys = g.docs.store().list("config/snapshots").await.unwrap();
        assert_eq!(keys.len(), 3, "{keys:?}");
        assert_eq!(
            g.current().await.unwrap().unwrap().text,
            winner.text,
            "the loser didn't overwrite generation 2"
        );
    }

    #[tokio::test]
    async fn a_tampered_or_missing_snapshot_is_refused() {
        let g = gens();
        let (first, _) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        let key = config_snapshot_key(1, &first.pointer.doc.hash);
        g.docs
            .store()
            .put(&key, Bytes::from_static(b"globals: {}\n"))
            .await
            .unwrap();
        let e = g.current().await.unwrap_err();
        assert!(e.to_string().contains("doesn't match the hash"), "{e}");
        let other = GenerationDoc {
            hash: config_hash("never written"),
            ..first.pointer.doc.clone()
        };
        g.docs
            .update(GENERATION_KEY, &first.pointer.version, &other)
            .await
            .unwrap();
        let e = g.current().await.unwrap_err();
        assert!(e.to_string().contains("missing"), "{e}");
    }
}
