//! The node's live messages (Admin_Console §8, M13f): what `GET /v1/stream` sends. The node
//! publishes each as it happens (an event once written, a sample once taken, a pipeline's state
//! and figures, its own figures) to every stream open on it, and builds none while no stream is
//! open. A stream that falls more than [`FEED_CAPACITY`] messages behind is told to read again.

use std::sync::Arc;

use data_avalanche_state::EventDoc;
use serde_json::Value;
use tokio::sync::{broadcast, watch};

/// The messages a stream may fall behind by before it misses some (and is told so).
pub const FEED_CAPACITY: usize = 1024;

/// One live message: its kind (the stream's event name), the pipeline it is about, and its data,
/// which names the node that sent it.
#[derive(Debug, Clone, PartialEq)]
pub struct Live {
    pub kind: &'static str,
    pub pipeline: Option<String>,
    pub data: Value,
}

impl Live {
    /// The event `e` this node wrote at `key`, as `GET /v1/events` lists it.
    pub fn event(key: &str, e: &EventDoc) -> Self {
        Self {
            kind: "event",
            pipeline: e.pipeline.clone(),
            data: e.to_json(key),
        }
    }
}

/// The node's live messages, sent to every stream open on it; cheap to clone.
#[derive(Debug, Clone)]
pub struct Feed {
    tx: broadcast::Sender<Arc<Live>>,
    closed: Arc<watch::Sender<bool>>,
}

impl Default for Feed {
    fn default() -> Self {
        Self::new()
    }
}

impl Feed {
    pub fn new() -> Self {
        Self {
            tx: broadcast::Sender::new(FEED_CAPACITY),
            closed: Arc::new(watch::Sender::new(false)),
        }
    }

    /// Sends `live` to every stream open now: to none, and dropped, when none is.
    pub fn publish(&self, live: Live) {
        if self.followed() {
            let _ = self.tx.send(Arc::new(live));
        }
    }

    /// Whether a stream is open on this node: what builds a message only to publish it asks
    /// first.
    pub fn followed(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    /// How many streams are open on this node: a new one is sent every pipeline's state again.
    pub fn followers(&self) -> usize {
        self.tx.receiver_count()
    }

    /// A new stream's messages, from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Live>> {
        self.tx.subscribe()
    }

    /// Ends every stream: the node stops, and an open stream must not hold the server's stop.
    pub fn close(&self) {
        self.closed.send_replace(true);
    }

    /// Whether the feed is closed, and its change.
    pub fn closed(&self) -> watch::Receiver<bool> {
        self.closed.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use data_avalanche_state::EventSeverity;

    use super::*;

    fn live(n: u64) -> Live {
        Live {
            kind: "node",
            pipeline: None,
            data: Value::from(n),
        }
    }

    #[tokio::test]
    async fn a_message_reaches_every_stream_open_and_waits_for_none() {
        let feed = Feed::new();
        assert!(!feed.followed());
        feed.publish(live(1));
        let (mut a, mut b) = (feed.subscribe(), feed.subscribe());
        assert!(feed.followed());
        assert_eq!(feed.followers(), 2);
        feed.publish(live(2));
        assert_eq!(
            *a.recv().await.unwrap(),
            live(2),
            "not the one sent before it opened"
        );
        assert_eq!(*b.recv().await.unwrap(), live(2));
        drop((a, b));
        assert!(!feed.followed());
        assert_eq!(feed.followers(), 0);
        feed.publish(live(3));
    }

    #[tokio::test]
    async fn a_stream_far_behind_learns_it_missed_some() {
        let feed = Feed::new();
        let mut slow = feed.subscribe();
        for n in 0..=FEED_CAPACITY as u64 {
            feed.publish(live(n));
        }
        assert!(matches!(
            slow.recv().await,
            Err(broadcast::error::RecvError::Lagged(1))
        ));
        assert_eq!(*slow.recv().await.unwrap(), live(1));
    }

    #[test]
    fn a_closed_feed_says_so_to_every_stream() {
        let feed = Feed::new();
        let open = feed.closed();
        assert!(!*open.borrow());
        feed.clone().close();
        assert!(*open.borrow());
        assert!(*feed.closed().borrow());
    }

    #[test]
    fn an_event_is_sent_as_the_events_list_it() {
        let mut e = EventDoc::new(
            Utc::now(),
            "node-a",
            "action.pipeline.pause",
            EventSeverity::Info,
            "paused",
        );
        e.pipeline = Some("orders".into());
        let l = Live::event("events/k.json", &e);
        assert_eq!(l.kind, "event");
        assert_eq!(l.pipeline.as_deref(), Some("orders"));
        assert_eq!(l.data, e.to_json("events/k.json"));
    }
}
