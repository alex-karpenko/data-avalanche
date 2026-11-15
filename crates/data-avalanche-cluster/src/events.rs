//! The recorder (Admin_Console §6): whatever records an event hands it over without waiting. One
//! task per node writes them to the cluster's timeline in `globals.state`, retrying a failing
//! store a few times; an event it can't write is counted (`data_avalanche_events_dropped_total`)
//! and logged, never allowed to slow or fail what it records. Another task expires the days past
//! `globals.console.events.retention`.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use data_avalanche_state::{EventDoc, EventSeverity, Events};
use data_avalanche_telemetry::Metrics;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// Events waiting to be written; past this, new ones are dropped and counted.
pub const EVENT_QUEUE: usize = 1024;

/// The waits between a failed write and its next attempt; after the last, the event is dropped.
pub const WRITE_RETRIES: [Duration; 3] = [
    Duration::from_millis(200),
    Duration::from_secs(1),
    Duration::from_secs(5),
];

/// How often the expired days are deleted.
pub const PRUNE_EVERY: Duration = Duration::from_secs(3600);

/// Hands events to the node's writer; cheap to clone.
#[derive(Debug, Clone)]
pub struct Recorder {
    node: Arc<str>,
    tx: Option<mpsc::Sender<EventDoc>>,
    metrics: Metrics,
}

impl Recorder {
    /// The recorder of `node`, and its writer task.
    pub fn start(events: Arc<Events>, node: &str, metrics: Metrics) -> (Self, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(EVENT_QUEUE);
        let task = tokio::spawn(write(events, rx, metrics.clone()));
        (
            Self {
                node: node.into(),
                tx: Some(tx),
                metrics,
            },
            task,
        )
    }

    /// A recorder that writes nothing (tests, a node without a timeline).
    pub fn off(node: &str) -> Self {
        Self {
            node: node.into(),
            tx: None,
            metrics: Metrics::new(),
        }
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// An event of this node, now.
    pub fn event(&self, kind: &str, severity: EventSeverity, text: impl Into<String>) -> EventDoc {
        EventDoc::new(Utc::now(), &self.node, kind, severity, text)
    }

    /// Queues `event`; never waits. A full queue drops it, counted and logged.
    pub fn record(&self, event: EventDoc) {
        let Some(tx) = &self.tx else {
            return;
        };
        if let Err(e) = tx.try_send(event) {
            self.metrics.event_dropped();
            let event = match e {
                mpsc::error::TrySendError::Full(ev) | mpsc::error::TrySendError::Closed(ev) => ev,
            };
            tracing::warn!(kind = %event.kind, "an event dropped, the recorder's queue is full or closed: {}", event.text);
        }
    }
}

async fn write(events: Arc<Events>, mut rx: mpsc::Receiver<EventDoc>, metrics: Metrics) {
    while let Some(event) = rx.recv().await {
        let mut waits = WRITE_RETRIES.iter();
        loop {
            match events.record(&event).await {
                Ok(_) => {
                    metrics.event_written();
                    break;
                }
                Err(e) => match waits.next() {
                    Some(wait) => tokio::time::sleep(*wait).await,
                    None => {
                        metrics.event_dropped();
                        tracing::warn!(kind = %event.kind, "an event dropped, the store keeps failing: {e}: {}", event.text);
                        break;
                    }
                },
            }
        }
    }
}

/// Deletes the days past the retention `retention` holds now, every [`PRUNE_EVERY`] (the first a
/// minute after the start), until the task is aborted. Every node runs it: a day deleted twice
/// is no error.
pub async fn expire_events(events: Arc<Events>, retention: watch::Receiver<Duration>) {
    tokio::time::sleep(Duration::from_secs(60)).await;
    loop {
        let keep = *retention.borrow();
        match events.prune(keep, Utc::now()).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(deleted = n, "expired events deleted"),
            Err(e) => tracing::warn!("expired events undeleted, tried again later: {e}"),
        }
        tokio::time::sleep(PRUNE_EVERY).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use data_avalanche_state::{DocStore, EventQuery};
    use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
    use object_store::memory::InMemory;

    use super::*;

    /// A store whose first `fail` creates fail.
    #[derive(Debug)]
    struct Failing {
        inner: ObjectStoreVersioned,
        fail: AtomicUsize,
    }

    #[async_trait]
    impl VersionedStore for Failing {
        async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
            self.inner.get(key).await
        }
        async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
            if self
                .fail
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(StoreError::Transient {
                    key: key.into(),
                    reason: "injected".into(),
                });
            }
            self.inner.create(key, body).await
        }
        async fn update(
            &self,
            key: &str,
            expected: &ObjectVersion,
            body: Bytes,
        ) -> Result<ObjectVersion, StoreError> {
            self.inner.update(key, expected, body).await
        }
        async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
            self.inner.put(key, body).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
    }

    fn store(fail: usize) -> Arc<Events> {
        let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap();
        Arc::new(Events::new(DocStore::new(Arc::new(Failing {
            inner,
            fail: AtomicUsize::new(fail),
        }))))
    }

    /// The texts once `n` events are written, waiting up to 20 s (of paused time: the retries').
    async fn written(events: &Events, n: usize) -> Vec<String> {
        for _ in 0..200 {
            let page = events.list(&EventQuery::default()).await.unwrap();
            if page.events.len() >= n {
                return page.events.into_iter().map(|(_, e)| e.text).collect();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("fewer than {n} events written");
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_store_is_retried_then_the_event_lands() {
        let events = store(2);
        let metrics = Metrics::new();
        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
        r.record(r.event("member.joined", EventSeverity::Info, "node-c joined"));
        assert_eq!(written(&events, 1).await, ["node-c joined"]);
        assert!(
            metrics
                .encode()
                .contains("data_avalanche_events_written_total 1")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_store_that_keeps_failing_drops_the_event_and_counts_it() {
        let events = store(WRITE_RETRIES.len() + 1);
        let metrics = Metrics::new();
        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
        r.record(r.event("member.joined", EventSeverity::Info, "lost"));
        r.record(r.event("member.joined", EventSeverity::Info, "kept"));
        assert_eq!(written(&events, 1).await, ["kept"]);
        let text = metrics.encode();
        assert!(
            text.contains("data_avalanche_events_dropped_total 1"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_full_queue_drops_without_waiting() {
        let metrics = Metrics::new();
        let (tx, _rx) = mpsc::channel(1);
        let r = Recorder {
            node: "node-a".into(),
            tx: Some(tx),
            metrics: metrics.clone(),
        };
        r.record(r.event("a.b", EventSeverity::Info, "1"));
        r.record(r.event("a.b", EventSeverity::Info, "2"));
        assert!(
            metrics
                .encode()
                .contains("data_avalanche_events_dropped_total 1")
        );
        Recorder::off("node-a").record(r.event("a.b", EventSeverity::Info, "nowhere"));
    }

    /// Writes an event dated `days` ago straight to the store.
    async fn aged(events: &Events, days: i64, text: &str) {
        let at = Utc::now() - chrono::Duration::days(days);
        let event = EventDoc::new(at, "node-a", "member.joined", EventSeverity::Info, text);
        events.record(&event).await.unwrap();
    }

    /// The texts of every stored event, newest first.
    async fn stored(events: &Events) -> Vec<String> {
        let page = events.list(&EventQuery::default()).await.unwrap();
        page.events.into_iter().map(|(_, e)| e.text).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_waits_a_minute_then_runs_hourly_with_the_latest_retention() {
        let events = store(0);
        aged(&events, 10, "ten days").await;
        aged(&events, 3, "three days").await;
        let (retention, rx) = watch::channel(Duration::from_secs(7 * 86_400));
        let _task = tokio::spawn(expire_events(Arc::clone(&events), rx));

        tokio::time::sleep(Duration::from_secs(59)).await;
        assert_eq!(
            stored(&events).await.len(),
            2,
            "nothing before the first minute"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(stored(&events).await, ["three days"]);

        retention.send(Duration::from_secs(86_400)).unwrap();
        tokio::time::sleep(PRUNE_EVERY - Duration::from_secs(10)).await;
        assert_eq!(stored(&events).await, ["three days"], "not before the hour");
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert!(
            stored(&events).await.is_empty(),
            "the latest retention applied"
        );
    }
}
