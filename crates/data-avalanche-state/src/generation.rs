//! Config generations in `globals.state` (Clustering §9): the CAS pointer
//! `config/generation.json` and immutable snapshots of the config file text. Each node resolves
//! the text itself (its own environment and files), so a snapshot holds the raw text, never a
//! resolved config.

use chrono::{DateTime, Utc};
use data_avalanche_storage::StoreError;
use serde::{Deserialize, Serialize};

use crate::doc::{CREATE_ATTEMPTS, DocStore, Document, StateError, Versioned};

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
    /// adopt the winner's generation. Returns the generation and whether this call created it —
    /// which it can, even when its own `create` returned a race error, if that write in fact
    /// landed (the outcome of `Store(Contention)` is unknown until read back).
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
        // The pointer doc this call last tried to create, set when that create returned a race
        // error: an unknown outcome (`Store(Contention)`) may still have landed.
        let mut attempted: Option<GenerationDoc> = None;
        loop {
            attempts += 1;
            if let Some(current) = self.current().await? {
                let created = attempted.as_ref() == Some(&current.pointer.doc);
                return Ok((current, created));
            }
            let doc = self.write_snapshot(1, text, node_id, now).await?;
            attempted = Some(doc.clone());
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
                Err(e) if e.is_race() && attempts < CREATE_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Reload (§9.2): publishes `text` as the generation after `base`, with a CAS on the pointer.
    ///
    /// # Errors
    /// `Store(Conflict)` when another node published since `base` was read: re-read, re-validate
    /// against the new generation and try again. `Store(Contention)` (or a transient write error)
    /// means the pointer update's outcome is unknown — the same recovery applies: re-read (the
    /// pointer may already name this node's generation) and decide. The losing snapshot stays
    /// behind, unreferenced.
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

    /// Writes the immutable snapshot for `generation`, tolerating races on its content-addressed
    /// key: two writers who land on the same key always agree on its bytes.
    ///
    /// # Errors
    /// A storage failure, or `Corrupt` when the key already holds bytes that hash to something
    /// other than this write's content-addressed hash (a key collision that should be
    /// impossible barring a `blake3` break or tampering).
    async fn write_snapshot(
        &self,
        generation: u64,
        text: &str,
        node_id: &str,
        now: DateTime<Utc>,
    ) -> Result<GenerationDoc, StateError> {
        let hash = config_hash(text);
        let key = config_snapshot_key(generation, &hash);
        let mut attempts = 0;
        loop {
            attempts += 1;
            match self.docs.store().create(&key, text.to_owned().into()).await {
                Ok(_) => break,
                // `Conflict` claims the key exists (same key = same generation and same text,
                // normally — but on S3 a 409 can map to `Conflict` without the object existing);
                // `Contention` leaves the outcome unknown after object_store's own retries.
                // Either way, read the key back and settle it, rather than assuming.
                Err(e @ (StoreError::Conflict { .. } | StoreError::Contention { .. })) => {
                    match self.docs.store().get(&key).await? {
                        Some((body, _))
                            if format!("blake3:{}", blake3::hash(&body).to_hex()) == hash =>
                        {
                            break;
                        }
                        Some(_) => {
                            return Err(StateError::Corrupt {
                                key: key.clone(),
                                kind: "config snapshot",
                                reason: "already holds bytes with a different hash than this write's content-addressed key".to_owned(),
                            });
                        }
                        // Nothing landed: retry the create, bounded like any other race.
                        None if attempts < CREATE_ATTEMPTS => continue,
                        None => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
            }
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
    use crate::test_support::WriteThenLoseResponse;

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
        let e = g
            .publish(&first.pointer, "# loser\n", "c", t(6))
            .await
            .unwrap_err();
        assert!(e.is_conflict(), "{e:?}");
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_bootstraps_with_identical_text_all_succeed() {
        let g = gens();
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let g = g.clone();
                tokio::spawn(async move { g.bootstrap_or_adopt(V1, &format!("n{i}"), t(0)).await })
            })
            .collect();
        let mut results = vec![];
        for task in tasks {
            results.push(task.await.unwrap().unwrap());
        }
        assert_eq!(
            results.iter().filter(|(_, created)| *created).count(),
            1,
            "exactly one bootstrapper creates the pointer; the rest hit the snapshot's Conflict \
             arm (same key, identical text) and then adopt"
        );
        let current = g.current().await.unwrap().unwrap();
        assert!(
            results
                .iter()
                .all(|(g2, _)| g2.text == current.text && g2.pointer.doc == current.pointer.doc)
        );
    }

    #[tokio::test]
    async fn publish_retries_with_the_same_text_after_a_pointer_conflict() {
        let g = gens();
        let (first, _) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        // "b" publishes generation 2 first.
        let winner = g.publish(&first.pointer, V2, "b", t(5)).await.unwrap();
        // "c" was racing from the same stale base with its own text; it loses.
        const SAME: &str = "# c's reload\n";
        let e = g
            .publish(&first.pointer, SAME, "c", t(6))
            .await
            .unwrap_err();
        assert!(e.is_conflict(), "{e:?}");
        let current = g.current().await.unwrap().unwrap();
        assert_eq!(current, winner);
        // "c" re-reads and republishes the very same text on the new base: it succeeds.
        let retried = g.publish(&current.pointer, SAME, "c", t(7)).await.unwrap();
        assert_eq!(
            (retried.pointer.doc.generation, retried.text.as_str()),
            (3, SAME)
        );
        assert_eq!(g.current().await.unwrap(), Some(retried));
    }

    #[tokio::test]
    async fn a_snapshot_contention_whose_write_landed_resolves_to_success() {
        let key = config_snapshot_key(1, &config_hash(V1));
        let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        let store = WriteThenLoseResponse::new(inner).on_key(&key);
        let g = ConfigGenerations::new(DocStore::new(Arc::new(store)));
        let (first, created) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        assert!(created);
        assert_eq!(first.text, V1);
        assert_eq!(g.current().await.unwrap(), Some(first));
    }

    #[tokio::test]
    async fn a_snapshot_contention_whose_write_did_not_land_retries_and_succeeds() {
        let key = config_snapshot_key(1, &config_hash(V1));
        let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        let store = WriteThenLoseResponse::new(inner)
            .without_write()
            .on_key(&key);
        let g = ConfigGenerations::new(DocStore::new(Arc::new(store)));
        let (first, created) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        assert!(created);
        assert_eq!(first.text, V1);
        assert_eq!(g.current().await.unwrap(), Some(first));
    }

    #[tokio::test]
    async fn write_snapshot_treats_a_conflict_with_mismatched_bytes_as_corrupt() {
        let g = gens();
        // A real `Conflict`, not a fake one: pre-write bytes at the snapshot key that don't
        // match its content-addressed hash, so `write_snapshot`'s own `create` fails with
        // `Conflict` because the key already exists.
        let key = config_snapshot_key(1, &config_hash(V1));
        g.docs
            .store()
            .put(&key, Bytes::from_static(b"tampered"))
            .await
            .unwrap();
        let e = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap_err();
        assert!(
            matches!(&e, StateError::Corrupt { key: k, kind: "config snapshot", reason }
                if k == &key && reason.contains("different hash")),
            "{e:?}"
        );
    }

    #[tokio::test]
    async fn bootstrap_recognizes_its_own_pointer_create_that_landed_despite_contention() {
        let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        let store = WriteThenLoseResponse::new(inner).on_key(GENERATION_KEY);
        let g = ConfigGenerations::new(DocStore::new(Arc::new(store)));
        let (first, created) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        assert!(
            created,
            "the pointer create landed even though the response reported Contention"
        );
        assert_eq!(first.text, V1);
        assert_eq!(g.current().await.unwrap(), Some(first));
    }
}
