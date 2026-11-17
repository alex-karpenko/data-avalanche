//! Who published each config generation, and why (Admin_Console §4 Config, D-M13b.9): one
//! record per generation, `config/history/<generation:06>.json`, written once its pointer landed.
//! A generation without one (bootstrapped before 1.1.0, published by a 1.0 node, or a crash
//! between the two writes) is still listed, from its snapshot.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::doc::{Document, StateError};
use crate::events::Actor;
use crate::generation::{ConfigGenerations, GENERATION_KEY, GenerationDoc};

pub const HISTORY_PREFIX: &str = "config/history";

const SNAPSHOTS_PREFIX: &str = "config/snapshots";

/// The record of generation `generation`.
pub fn history_key(generation: u64) -> String {
    format!("{HISTORY_PREFIX}/{generation:06}.json")
}

/// One generation's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationRecord {
    pub generation: u64,
    /// `blake3:<64 hex>` of its text, as the pointer names it.
    pub hash: String,
    /// The node whose file it is.
    pub node: String,
    pub at: DateTime<Utc>,
    /// Who asked for it; none for a bootstrap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<Actor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// What it changed (`2 pipelines changed · 1 restart pending`), or `bootstrap`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl Document for GenerationRecord {
    const KIND: &'static str = "config history";
    const SCHEMA_VERSION: u64 = 1;
}

/// A generation as the history lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub generation: u64,
    /// Its text's hash: the record's, else the pointer's for the current one, else its only
    /// snapshot's; `None` when it has several snapshots and nothing says which (a publish
    /// race's loser was left behind).
    pub hash: Option<String>,
    pub record: Option<GenerationRecord>,
    /// The generation the pointer names now.
    pub current: bool,
}

impl ConfigGenerations {
    /// Writes `record`; one already written (a retried publish) is kept.
    ///
    /// # Errors
    /// A storage failure.
    pub async fn record(&self, record: &GenerationRecord) -> Result<(), StateError> {
        match self
            .docs()
            .create(&history_key(record.generation), record)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.is_race() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Every generation's snapshot hashes, by generation, and the pointer.
    async fn snapshots(
        &self,
    ) -> Result<(BTreeMap<u64, Vec<String>>, Option<GenerationDoc>), StateError> {
        let mut by: BTreeMap<u64, Vec<String>> = BTreeMap::new();
        for key in self.docs().store().list(SNAPSHOTS_PREFIX).await? {
            let name = key.rsplit('/').next().unwrap_or_default();
            let Some((n, hex)) = name
                .strip_suffix(".yaml")
                .and_then(|stem| stem.split_once('-'))
            else {
                continue;
            };
            if let Ok(n) = n.parse::<u64>() {
                by.entry(n).or_default().push(format!("blake3:{hex}"));
            }
        }
        let pointer = self
            .docs()
            .load::<GenerationDoc>(GENERATION_KEY)
            .await?
            .map(|v| v.doc);
        Ok((by, pointer))
    }

    async fn listed(
        &self,
        generation: u64,
        hashes: &[String],
        pointer: Option<&GenerationDoc>,
    ) -> Result<Listed, StateError> {
        let record = self
            .docs()
            .load::<GenerationRecord>(&history_key(generation))
            .await?
            .map(|v| v.doc);
        let current = pointer.is_some_and(|p| p.generation == generation);
        let hash = match (&record, pointer.filter(|_| current), hashes) {
            (Some(r), _, _) => Some(r.hash.clone()),
            (None, Some(p), _) => Some(p.hash.clone()),
            (None, None, [only]) => Some(only.clone()),
            _ => None,
        };
        Ok(Listed {
            generation,
            hash,
            record,
            current,
        })
    }

    /// The generations older than `before` (all when `None`), newest first, at most `limit`, each
    /// with its record. A generation newer than the pointer (a publish that lost its race) isn't
    /// one.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn history(
        &self,
        before: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Listed>, StateError> {
        let (by, pointer) = self.snapshots().await?;
        let newest = pointer.as_ref().map_or(0, |p| p.generation);
        let mut out = vec![];
        for (n, hashes) in by.iter().rev() {
            if *n > newest || before.is_some_and(|b| *n >= b) {
                continue;
            }
            if out.len() == limit {
                break;
            }
            out.push(self.listed(*n, hashes, pointer.as_ref()).await?);
        }
        Ok(out)
    }

    /// Generation `generation` with its text: `None` when there is no such generation; the text
    /// `None` when its hash is ambiguous ([`Listed::hash`]).
    ///
    /// # Errors
    /// A storage or decode failure; `Corrupt` when its snapshot doesn't match its hash.
    pub async fn one(
        &self,
        generation: u64,
    ) -> Result<Option<(Listed, Option<String>)>, StateError> {
        let (by, pointer) = self.snapshots().await?;
        let newest = pointer.as_ref().map_or(0, |p| p.generation);
        let Some(hashes) = by.get(&generation).filter(|_| generation <= newest) else {
            return Ok(None);
        };
        let listed = self.listed(generation, hashes, pointer.as_ref()).await?;
        let text = match &listed.hash {
            Some(h) => Some(self.text(generation, h).await?),
            None => None,
        };
        Ok(Some((listed, text)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::DocStore;
    use crate::generation::config_hash;

    fn t(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + s, 0).unwrap()
    }

    fn gens() -> ConfigGenerations {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        ConfigGenerations::new(DocStore::new(Arc::new(store)))
    }

    fn record(n: u64, text: &str, note: &str) -> GenerationRecord {
        GenerationRecord {
            generation: n,
            hash: config_hash(text),
            node: "node-b".into(),
            at: t(n as i64),
            by: Some(Actor {
                name: "oleksii".into(),
                role: "operator".into(),
            }),
            note: Some(note.into()),
            summary: Some("1 pipeline changed".into()),
        }
    }

    #[tokio::test]
    async fn the_history_lists_each_generation_newest_first_with_its_record() {
        let g = gens();
        let (first, _) = g
            .bootstrap_or_adopt("v: 1\n", "node-a", t(0))
            .await
            .unwrap();
        let second = g
            .publish(&first.pointer, "v: 2\n", "node-b", t(2))
            .await
            .unwrap();
        g.record(&record(2, "v: 2\n", "faster flush"))
            .await
            .unwrap();
        // A retried publish's second record is kept as the first.
        g.record(&record(2, "v: 2\n", "again")).await.unwrap();
        let _third = g
            .publish(&second.pointer, "v: 3\n", "node-a", t(3))
            .await
            .unwrap();

        let all = g.history(None, 10).await.unwrap();
        let ns: Vec<u64> = all.iter().map(|l| l.generation).collect();
        assert_eq!(ns, [3, 2, 1]);
        assert!(
            all[0].current && all[0].record.is_none(),
            "no record written for 3"
        );
        assert_eq!(all[0].hash.as_deref(), Some(config_hash("v: 3\n").as_str()));
        assert_eq!(
            all[1].record.as_ref().unwrap().note.as_deref(),
            Some("faster flush")
        );
        let boot = all[2]
            .record
            .as_ref()
            .expect("the bootstrap writes generation 1's");
        assert_eq!(
            (boot.by.as_ref(), boot.summary.as_deref()),
            (None, Some("bootstrap"))
        );

        let page = g.history(Some(3), 1).await.unwrap();
        assert_eq!(page.iter().map(|l| l.generation).collect::<Vec<_>>(), [2]);
        let (two, text) = g.one(2).await.unwrap().unwrap();
        assert_eq!((two.generation, text.as_deref()), (2, Some("v: 2\n")));
        assert!(g.one(4).await.unwrap().is_none());
    }

    /// A publish that lost its race leaves its snapshot behind: the generation it lost is told by
    /// the record, or else it is ambiguous; one beyond the pointer isn't listed.
    #[tokio::test]
    async fn a_race_s_leftover_snapshot_never_passes_for_a_generation() {
        let g = gens();
        let (first, _) = g
            .bootstrap_or_adopt("v: 1\n", "node-a", t(0))
            .await
            .unwrap();
        let won = g
            .publish(&first.pointer, "v: 2\n", "node-a", t(2))
            .await
            .unwrap();
        assert!(
            g.publish(&first.pointer, "v: 2b\n", "node-b", t(2))
                .await
                .is_err()
        );
        let _ = won;
        let (two, _) = g.one(2).await.unwrap().unwrap();
        assert_eq!(
            two.hash.as_deref(),
            Some(config_hash("v: 2\n").as_str()),
            "the pointer's"
        );
        // Once the cluster moved on, nothing tells the two apart but a record.
        let _three = g
            .publish(&won.pointer, "v: 3\n", "node-a", t(3))
            .await
            .unwrap();
        let (two, text) = g.one(2).await.unwrap().unwrap();
        assert_eq!((two.hash, text), (None, None));
        g.record(&record(2, "v: 2\n", "")).await.unwrap();
        let (_, text) = g.one(2).await.unwrap().unwrap();
        assert_eq!(text.as_deref(), Some("v: 2\n"));
    }
}
