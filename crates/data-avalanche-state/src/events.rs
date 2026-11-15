//! The cluster's events and audit trail (Admin_Console §6): one small document per event under
//! `globals.state`, `events/<YYYY-MM-DD>/<HHMMSS.ffffff>Z-<seq>-<node>.json`. Object storage has no
//! append, so each event is its own object, created once and never changed. A key orders by time,
//! so a day lists in order, and a whole day expires by deleting its folder.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use data_avalanche_storage::{StoreError, escape_segment};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::{DocStore, Document, StateError};

/// The folder of every event, under `globals.state`.
pub const EVENTS_PREFIX: &str = "events";

/// The most events one [`Events::list`] returns.
pub const MAX_EVENTS: usize = 1000;

/// The most event documents one [`Events::list`] reads: a narrow filter over a long range stops
/// here and hands back where it stopped.
pub const MAX_SCANNED: usize = 5000;

/// How many documents a listing reads at once.
const READ_CONCURRENCY: usize = 16;

/// The most keys one [`Events::record`] tries: past this many taken keys, the store is taken to
/// be misbehaving and the last conflict is returned.
pub const RECORD_ATTEMPTS: usize = 16;

/// How serious an event is. A person's or a token's action carries an [`Actor`] besides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSeverity {
    Info,
    Warning,
    Error,
}

/// Who caused an event: the name and role the server verified (never what a page says).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actor {
    pub name: String,
    pub role: String,
}

/// One event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventDoc {
    pub at: DateTime<Utc>,
    /// Dotted, the family first: `action.pipeline.pause`, `lease.takeover`, `table.halted`.
    pub kind: String,
    pub severity: EventSeverity,
    /// The node that recorded it.
    pub node: String,
    /// The pipeline it concerns, by id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// What else it concerns, by name: a table, a job id, a node, a config generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// How it ended: `accepted`, `refused`, `done`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    /// The sentence a page shows.
    pub text: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub detail: BTreeMap<String, String>,
}

impl Document for EventDoc {
    const KIND: &'static str = "event";
    const SCHEMA_VERSION: u64 = 1;
}

impl EventDoc {
    /// An event at `at`, recorded by `node`, with nothing else set.
    pub fn new(
        at: DateTime<Utc>,
        node: &str,
        kind: &str,
        severity: EventSeverity,
        text: impl Into<String>,
    ) -> Self {
        Self {
            at,
            kind: kind.to_owned(),
            severity,
            node: node.to_owned(),
            pipeline: None,
            object: None,
            actor: None,
            note: None,
            outcome: None,
            text: text.into(),
            detail: BTreeMap::new(),
        }
    }
}

/// Which events [`Events::list`] selects, and how many: every filter set must match, and an
/// unset one matches every event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventQuery {
    /// Events at or after.
    pub since: Option<DateTime<Utc>>,
    /// Events before.
    pub until: Option<DateTime<Utc>>,
    /// Kinds, each a whole kind or a family: `action` matches `action.pipeline.pause`.
    pub kinds: Vec<String>,
    /// A pipeline id, or an object's name.
    pub object: Option<String>,
    /// An actor's name.
    pub actor: Option<String>,
    /// The recording node.
    pub node: Option<String>,
    /// Text in the event's sentence or note, without case.
    pub text: Option<String>,
    /// At most this many; 0 and anything above [`MAX_EVENTS`] mean [`MAX_EVENTS`].
    pub limit: usize,
    /// Continue after this key, from a page's `next`.
    pub after: Option<String>,
}

/// One page of events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    /// Each with its key, newest first.
    pub events: Vec<(String, EventDoc)>,
    /// Set when the listing stopped early: pass it as [`EventQuery::after`] for the rest.
    pub next: Option<String>,
}

/// The key of an event at `at`: `events/<day>/<HHMMSS.ffffff>Z-<seq>-<node>.json`.
pub fn event_key(at: DateTime<Utc>, seq: u64, node: &str) -> String {
    format!(
        "{EVENTS_PREFIX}/{}/{}Z-{seq:06}-{}.json",
        at.format("%Y-%m-%d"),
        at.format("%H%M%S%.6f"),
        escape_segment(node),
    )
}

/// The cluster's events in `globals.state`.
#[derive(Debug)]
pub struct Events {
    docs: DocStore,
    seq: AtomicU64,
}

impl Events {
    pub fn new(docs: DocStore) -> Self {
        Self {
            docs,
            seq: AtomicU64::new(0),
        }
    }

    /// Writes `event` under a new key; a key taken already (another process of the same node in
    /// the same microsecond) takes the next sequence number, at most [`RECORD_ATTEMPTS`] keys in
    /// all.
    ///
    /// # Errors
    /// A storage failure, or the last conflict once [`RECORD_ATTEMPTS`] keys were taken.
    pub async fn record(&self, event: &EventDoc) -> Result<String, StateError> {
        let mut attempts = 0;
        loop {
            let seq = self.seq.fetch_add(1, Ordering::Relaxed) % 1_000_000;
            let key = event_key(event.at, seq, &event.node);
            attempts += 1;
            match self.docs.create(&key, event).await {
                Ok(_) => return Ok(key),
                Err(StateError::Store(StoreError::Conflict { .. }))
                    if attempts < RECORD_ATTEMPTS => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// The events `query` selects, newest first. Reads the days from `until` back to `since`
    /// (or to the oldest), stopping once it has `limit` events or has read [`MAX_SCANNED`]
    /// documents; then [`EventPage::next`] says where to go on. A document that can't be read
    /// (another schema version, corrupt) is left out with a warning.
    ///
    /// # Errors
    /// A storage failure.
    pub async fn list(&self, query: &EventQuery) -> Result<EventPage, StateError> {
        let limit = match query.limit {
            0 => MAX_EVENTS,
            n => n.min(MAX_EVENTS),
        };
        let store = self.docs.store();
        let mut days = store.folders(EVENTS_PREFIX).await?;
        days.retain(|d| d.parse::<NaiveDate>().is_ok());
        days.reverse();
        let first_day = query.until.map(|t| t.format("%Y-%m-%d").to_string());
        let last_day = query.since.map(|t| t.format("%Y-%m-%d").to_string());
        let mut out = Vec::new();
        let mut scanned = 0;
        for day in days {
            if first_day.as_ref().is_some_and(|f| day > *f)
                || query.after.as_ref().is_some_and(|a| {
                    a.strip_prefix(&format!("{EVENTS_PREFIX}/"))
                        .is_some_and(|rest| *rest < *day)
                })
            {
                continue;
            }
            if last_day.as_ref().is_some_and(|l| day < *l) {
                break;
            }
            let mut keys = store.list(&format!("{EVENTS_PREFIX}/{day}")).await?;
            keys.retain(|k| k.ends_with(".json"));
            keys.reverse();
            if let Some(after) = &query.after {
                keys.retain(|k| k < after);
            }
            for chunk in keys.chunks(READ_CONCURRENCY * 4) {
                let read: Vec<(String, Option<EventDoc>)> = stream::iter(chunk.iter().cloned())
                    .map(|key| async move {
                        match self.docs.load::<EventDoc>(&key).await {
                            Ok(doc) => Ok((key, doc.map(|v| v.doc))),
                            Err(StateError::Store(e)) => Err(StateError::Store(e)),
                            Err(e) => {
                                tracing::warn!(key, "an event left out of the listing: {e}");
                                Ok((key, None))
                            }
                        }
                    })
                    .buffered(READ_CONCURRENCY)
                    .try_collect()
                    .await?;
                for (key, doc) in read {
                    scanned += 1;
                    if let Some(doc) = doc
                        && matches(query, &doc)
                    {
                        out.push((key.clone(), doc));
                    }
                    if out.len() == limit || scanned == MAX_SCANNED {
                        return Ok(EventPage {
                            events: out,
                            next: Some(key),
                        });
                    }
                }
            }
        }
        Ok(EventPage {
            events: out,
            next: None,
        })
    }

    /// Deletes every event of the days that ended `retention` or more before `now`, and returns
    /// how many. Whole days only: an event lives at least `retention`, at most a day more. Safe
    /// on every node at once. A retention reaching past the earliest time a date can hold keeps
    /// everything.
    ///
    /// # Errors
    /// A storage failure; what was deleted before it stays deleted.
    pub async fn prune(
        &self,
        retention: Duration,
        now: DateTime<Utc>,
    ) -> Result<usize, StateError> {
        let Some(cutoff) = chrono::Duration::from_std(retention)
            .ok()
            .and_then(|r| now.checked_sub_signed(r))
        else {
            return Ok(0);
        };
        let keep_from = cutoff.format("%Y-%m-%d").to_string();
        let store = self.docs.store();
        let mut deleted = 0;
        for day in store.folders(EVENTS_PREFIX).await? {
            if day.parse::<NaiveDate>().is_err() || day >= keep_from {
                continue;
            }
            for key in store.list(&format!("{EVENTS_PREFIX}/{day}")).await? {
                store.delete(&key).await?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

fn matches(q: &EventQuery, e: &EventDoc) -> bool {
    let kind = q.kinds.is_empty()
        || q.kinds.iter().any(|k| {
            e.kind == *k
                || e.kind
                    .strip_prefix(k.as_str())
                    .is_some_and(|rest| rest.starts_with('.'))
        });
    let text = q.text.as_ref().is_none_or(|t| {
        let t = t.to_lowercase();
        e.text.to_lowercase().contains(&t)
            || e.note
                .as_ref()
                .is_some_and(|n| n.to_lowercase().contains(&t))
    });
    kind && text
        && q.since.is_none_or(|s| e.at >= s)
        && q.until.is_none_or(|u| e.at < u)
        && q.object
            .as_ref()
            .is_none_or(|o| e.pipeline.as_ref() == Some(o) || e.object.as_ref() == Some(o))
        && q.actor
            .as_ref()
            .is_none_or(|a| e.actor.as_ref().is_some_and(|x| x.name == *a))
        && q.node.as_ref().is_none_or(|n| e.node == *n)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use data_avalanche_storage::{ObjectStoreVersioned, VersionedStore};
    use object_store::memory::InMemory;

    use super::*;

    fn at(day: u32, h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, day, h, m, 0).unwrap()
    }

    fn events() -> (Events, Arc<dyn VersionedStore>) {
        let store: Arc<dyn VersionedStore> =
            Arc::new(ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap());
        (Events::new(DocStore::new(Arc::clone(&store))), store)
    }

    fn event(t: DateTime<Utc>, kind: &str, node: &str) -> EventDoc {
        EventDoc::new(
            t,
            node,
            kind,
            EventSeverity::Info,
            format!("{kind} on {node}"),
        )
    }

    #[test]
    fn a_key_orders_by_time_and_names_the_day() {
        let k = event_key(at(14, 13, 55), 7, "node/b");
        assert_eq!(k, "events/2026-11-14/135500.000000Z-000007-node%2Fb.json");
        assert!(event_key(at(14, 9, 0), 0, "z") < event_key(at(14, 10, 0), 0, "a"));
        assert!(event_key(at(13, 23, 59), 9, "z") < event_key(at(14, 0, 0), 0, "a"));
    }

    #[tokio::test]
    async fn events_list_newest_first_through_the_filters() {
        let (ev, _) = events();
        let mut pause = event(at(13, 10, 0), "action.pipeline.pause", "node-a");
        pause.pipeline = Some("orders".into());
        pause.actor = Some(Actor {
            name: "Oleksii".into(),
            role: "operator".into(),
        });
        pause.note = Some("Backfill after the outage".into());
        let mut halt = event(at(14, 13, 41), "table.halted", "node-b");
        halt.pipeline = Some("orders".into());
        halt.object = Some("public.refunds".into());
        halt.severity = EventSeverity::Error;
        let join = event(at(14, 13, 52), "member.joined", "node-c");
        for e in [&pause, &halt, &join] {
            ev.record(e).await.unwrap();
        }
        let list = |q: EventQuery| {
            let ev = &ev;
            async move {
                ev.list(&q)
                    .await
                    .unwrap()
                    .events
                    .into_iter()
                    .map(|(_, e)| e.kind)
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(
            list(EventQuery::default()).await,
            ["member.joined", "table.halted", "action.pipeline.pause"]
        );
        let kinds = |k: &[&str]| EventQuery {
            kinds: k.iter().map(|s| s.to_string()).collect(),
            ..EventQuery::default()
        };
        assert_eq!(list(kinds(&["action"])).await, ["action.pipeline.pause"]);
        assert!(
            list(kinds(&["act"])).await.is_empty(),
            "a family is a whole segment"
        );
        assert_eq!(
            list(kinds(&["table.halted", "member"])).await,
            ["member.joined", "table.halted"]
        );
        let q = |f: fn(&mut EventQuery)| {
            let mut q = EventQuery::default();
            f(&mut q);
            q
        };
        assert_eq!(
            list(q(|q| q.object = Some("orders".into()))).await,
            ["table.halted", "action.pipeline.pause"]
        );
        assert_eq!(
            list(q(|q| q.object = Some("public.refunds".into()))).await,
            ["table.halted"]
        );
        assert_eq!(
            list(q(|q| q.actor = Some("Oleksii".into()))).await,
            ["action.pipeline.pause"]
        );
        assert_eq!(
            list(q(|q| q.node = Some("node-c".into()))).await,
            ["member.joined"]
        );
        assert_eq!(
            list(q(|q| q.text = Some("OUTAGE".into()))).await,
            ["action.pipeline.pause"],
            "the note, without case"
        );
        assert_eq!(
            list(EventQuery {
                since: Some(at(14, 0, 0)),
                until: Some(at(14, 13, 50)),
                ..EventQuery::default()
            })
            .await,
            ["table.halted"],
            "since inclusive, until exclusive"
        );
    }

    #[tokio::test]
    async fn a_page_hands_back_where_it_stopped() {
        let (ev, _) = events();
        for (day, h) in [(12, 1), (13, 1), (13, 2), (14, 1)] {
            ev.record(&event(at(day, h, 0), "member.joined", "a"))
                .await
                .unwrap();
        }
        let first = ev
            .list(&EventQuery {
                limit: 2,
                ..EventQuery::default()
            })
            .await
            .unwrap();
        let times: Vec<_> = first.events.iter().map(|(_, e)| e.at).collect();
        assert_eq!(times, [at(14, 1, 0), at(13, 2, 0)]);
        let next = first.next.expect("more to come");
        let rest = ev
            .list(&EventQuery {
                after: Some(next),
                ..EventQuery::default()
            })
            .await
            .unwrap();
        let times: Vec<_> = rest.events.iter().map(|(_, e)| e.at).collect();
        assert_eq!(times, [at(13, 1, 0), at(12, 1, 0)]);
        assert_eq!(rest.next, None);
    }

    #[tokio::test]
    async fn two_events_in_one_microsecond_both_land() {
        let (ev, store) = events();
        let e = event(at(14, 1, 0), "member.joined", "a");
        let k1 = ev.record(&e).await.unwrap();
        // Another process of the same node, its sequence starting over.
        let k2 = Events::new(DocStore::new(Arc::clone(&store)))
            .record(&e)
            .await
            .unwrap();
        assert_ne!(k1, k2);
        assert_eq!(
            ev.list(&EventQuery::default()).await.unwrap().events.len(),
            2
        );
    }

    #[tokio::test]
    async fn an_unreadable_event_is_left_out_not_fatal() {
        let (ev, store) = events();
        ev.record(&event(at(14, 1, 0), "member.joined", "a"))
            .await
            .unwrap();
        store
            .put(
                "events/2026-11-14/020000.000000Z-000000-x.json",
                bytes::Bytes::from_static(br#"{"version": 2, "future": true}"#),
            )
            .await
            .unwrap();
        let page = ev.list(&EventQuery::default()).await.unwrap();
        assert_eq!(page.events.len(), 1);
    }

    /// A store whose every create finds its key taken, counting the attempts.
    #[derive(Debug)]
    struct AlwaysTaken {
        inner: ObjectStoreVersioned,
        creates: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl VersionedStore for AlwaysTaken {
        async fn get(
            &self,
            key: &str,
        ) -> Result<Option<(bytes::Bytes, data_avalanche_storage::ObjectVersion)>, StoreError>
        {
            self.inner.get(key).await
        }
        async fn create(
            &self,
            key: &str,
            _body: bytes::Bytes,
        ) -> Result<data_avalanche_storage::ObjectVersion, StoreError> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            Err(StoreError::Conflict { key: key.into() })
        }
        async fn update(
            &self,
            key: &str,
            expected: &data_avalanche_storage::ObjectVersion,
            body: bytes::Bytes,
        ) -> Result<data_avalanche_storage::ObjectVersion, StoreError> {
            self.inner.update(key, expected, body).await
        }
        async fn put(
            &self,
            key: &str,
            body: bytes::Bytes,
        ) -> Result<data_avalanche_storage::ObjectVersion, StoreError> {
            self.inner.put(key, body).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
    }

    /// A store that answers every create with a conflict doesn't hold the writer forever.
    #[tokio::test]
    async fn a_write_that_always_conflicts_gives_up() {
        let store = Arc::new(AlwaysTaken {
            inner: ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap(),
            creates: std::sync::atomic::AtomicUsize::new(0),
        });
        let ev = Events::new(DocStore::new(Arc::clone(&store) as Arc<dyn VersionedStore>));
        let e = ev
            .record(&event(at(14, 1, 0), "member.joined", "a"))
            .await
            .unwrap_err();
        assert!(
            matches!(e, StateError::Store(StoreError::Conflict { .. })),
            "the last conflict: {e:?}"
        );
        assert_eq!(store.creates.load(Ordering::SeqCst), RECORD_ATTEMPTS);
    }

    /// A narrow filter over many events stops at [`MAX_SCANNED`] documents and says where, so a
    /// caller can go on.
    #[tokio::test]
    async fn a_listing_stops_after_its_scan_cap_and_says_where() {
        let (ev, _) = events();
        let mut wanted = event(at(13, 1, 0), "table.halted", "a");
        wanted.pipeline = Some("orders".into());
        ev.record(&wanted).await.unwrap();
        let base = at(14, 0, 0);
        for i in 0..=MAX_SCANNED {
            let t = base + chrono::Duration::milliseconds(i64::try_from(i).unwrap());
            ev.record(&event(t, "member.joined", "a")).await.unwrap();
        }
        let q = EventQuery {
            object: Some("orders".into()),
            ..EventQuery::default()
        };
        let page = ev.list(&q).await.unwrap();
        assert!(page.events.is_empty(), "{:?}", page.events.len());
        let next = page.next.expect("where it stopped");
        let rest = ev
            .list(&EventQuery {
                after: Some(next),
                ..q
            })
            .await
            .unwrap();
        assert_eq!(rest.events.len(), 1, "found on the next page");
        assert_eq!(rest.next, None);
    }

    #[tokio::test]
    async fn a_retention_past_the_earliest_date_keeps_everything() {
        let (ev, _) = events();
        ev.record(&event(at(10, 12, 0), "member.joined", "a"))
            .await
            .unwrap();
        assert_eq!(ev.prune(Duration::MAX, at(14, 6, 0)).await.unwrap(), 0);
        assert_eq!(
            ev.list(&EventQuery::default()).await.unwrap().events.len(),
            1
        );
    }

    #[tokio::test]
    async fn prune_deletes_whole_days_past_the_retention() {
        let (ev, store) = events();
        for day in [10, 11, 12, 13, 14] {
            ev.record(&event(at(day, 12, 0), "member.joined", "a"))
                .await
                .unwrap();
        }
        // 2 days back from the 14th, 06:00 is the 12th, 06:00: the 10th and the 11th have ended.
        let deleted = ev
            .prune(Duration::from_secs(2 * 86_400), at(14, 6, 0))
            .await
            .unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(
            store.folders(EVENTS_PREFIX).await.unwrap(),
            ["2026-11-12", "2026-11-13", "2026-11-14"]
        );
        assert_eq!(
            ev.prune(Duration::from_secs(2 * 86_400), at(14, 6, 0))
                .await
                .unwrap(),
            0,
            "again: nothing left to delete"
        );
    }
}
