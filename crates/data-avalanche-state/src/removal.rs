//! Removal tombstones in `globals.state` (config §12.3, DA-OPS-9, DA-OPS-10): `removals/<id>.json`
//! for each pipeline a config generation removed, until what its source created for it is torn
//! down and its lease let go. Every node that applies the generation records it; the removal's
//! owner, or the node the ring places the id on when no node held it, marks it done. A pipeline
//! that takes the removed one's place waits until it is done. The store has no delete, so a done
//! tombstone stays, and a later removal of the same id records it again.

use chrono::{DateTime, Utc};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{DocStore, Document, StateError};

pub const REMOVALS_PREFIX: &str = "removals";

/// `removals/<id>.json`.
pub fn removal_key(pipeline_id: &str) -> String {
    format!("{REMOVALS_PREFIX}/{}.json", escape_segment(pipeline_id))
}

/// A removed pipeline: the generation that last had it (whose text every node resolves it
/// from), and when its removal was done.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemovalDoc {
    pub pipeline_id: String,
    /// The last generation that had the pipeline, and its text's hash.
    pub generation: u64,
    pub hash: String,
    pub removed_at: DateTime<Utc>,
    /// Set once the source's resources are torn down (`auto_cleanup`) and the lease let go.
    pub done_at: Option<DateTime<Utc>>,
}

impl Document for RemovalDoc {
    const KIND: &'static str = "removal";
    const SCHEMA_VERSION: u64 = 1;
}

impl RemovalDoc {
    pub fn is_done(&self) -> bool {
        self.done_at.is_some()
    }
}

/// The removal tombstones, over the `globals.state` documents.
#[derive(Debug, Clone)]
pub struct Removals {
    docs: DocStore,
}

impl Removals {
    /// `docs` is rooted at the `globals.state` location.
    pub fn new(docs: DocStore) -> Self {
        Self { docs }
    }

    /// Records `doc`'s removal. One under way is kept, whoever recorded it first (every node
    /// records the same generation's); a done one is replaced: the id was added again, then
    /// removed again.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn record(&self, doc: &RemovalDoc) -> Result<(), StateError> {
        let key = removal_key(&doc.pipeline_id);
        self.docs
            .modify::<RemovalDoc, _>(&key, |current| {
                Ok(match current {
                    Some(c) if !c.is_done() => c,
                    _ => doc.clone(),
                })
            })
            .await?;
        Ok(())
    }

    /// The removal of `pipeline_id` under way, if any.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn pending(&self, pipeline_id: &str) -> Result<Option<RemovalDoc>, StateError> {
        Ok(self
            .docs
            .load::<RemovalDoc>(&removal_key(pipeline_id))
            .await?
            .map(|v| v.doc)
            .filter(|d| !d.is_done()))
    }

    /// Every removal under way. A tombstone that doesn't decode is logged and skipped: it can't
    /// be torn down, and it mustn't stop the others.
    ///
    /// # Errors
    /// A storage failure listing or reading them.
    pub async fn all_pending(&self) -> Result<Vec<RemovalDoc>, StateError> {
        let mut out = vec![];
        for key in self.docs.store().list(REMOVALS_PREFIX).await? {
            match self.docs.load::<RemovalDoc>(&key).await {
                Ok(Some(v)) if !v.doc.is_done() => out.push(v.doc),
                Ok(_) => {}
                Err(e @ (StateError::Corrupt { .. } | StateError::UnsupportedVersion { .. })) => {
                    tracing::warn!("removal tombstone {key} skipped: {e}");
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Marks the removal of `pipeline_id` done at `now`. Absent or done already: nothing.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn done(&self, pipeline_id: &str, now: DateTime<Utc>) -> Result<(), StateError> {
        let key = removal_key(pipeline_id);
        if self.pending(pipeline_id).await?.is_none() {
            return Ok(());
        }
        self.docs
            .modify::<RemovalDoc, _>(&key, |current| {
                let mut d = current.ok_or_else(|| StateError::Corrupt {
                    key: key.clone(),
                    kind: RemovalDoc::KIND,
                    reason: "gone while marked done".into(),
                })?;
                d.done_at.get_or_insert(now);
                Ok(d)
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;

    fn removals() -> Removals {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        Removals::new(DocStore::new(Arc::new(store)))
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 4, 10, 0, s).unwrap()
    }

    fn removed(id: &str, generation: u64) -> RemovalDoc {
        RemovalDoc {
            pipeline_id: id.into(),
            generation,
            hash: format!("blake3:{generation}"),
            removed_at: t(0),
            done_at: None,
        }
    }

    #[test]
    fn a_removal_is_keyed_by_its_escaped_id() {
        assert_eq!(removal_key("orders"), "removals/orders.json");
        assert_eq!(removal_key("a/b"), "removals/a%2Fb.json");
    }

    /// Every node records the generation's removal: the first record stands while it is under
    /// way, and a done one is replaced by the next removal of the id.
    #[tokio::test]
    async fn the_first_record_stands_until_its_removal_is_done() {
        let r = removals();
        r.record(&removed("p", 3)).await.unwrap();
        r.record(&removed("p", 4)).await.unwrap();
        assert_eq!(r.pending("p").await.unwrap(), Some(removed("p", 3)));
        r.done("p", t(5)).await.unwrap();
        r.done("p", t(9)).await.unwrap();
        assert_eq!(r.pending("p").await.unwrap(), None);
        assert!(r.all_pending().await.unwrap().is_empty());
        r.record(&removed("p", 7)).await.unwrap();
        assert_eq!(r.pending("p").await.unwrap(), Some(removed("p", 7)));
        r.done("nope", t(5)).await.unwrap();
    }

    #[tokio::test]
    async fn every_pending_removal_is_listed_and_a_corrupt_one_skipped() {
        let r = removals();
        r.record(&removed("p", 3)).await.unwrap();
        r.record(&removed("q", 3)).await.unwrap();
        r.done("q", t(1)).await.unwrap();
        r.docs
            .store()
            .put("removals/bad.json", bytes::Bytes::from_static(b"{"))
            .await
            .unwrap();
        assert_eq!(r.all_pending().await.unwrap(), [removed("p", 3)]);
    }
}
