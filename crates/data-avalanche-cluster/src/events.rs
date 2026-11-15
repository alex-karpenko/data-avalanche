//! The recorder (Admin_Console §6): whatever records an event hands it over without waiting. One
//! task per node writes them to the cluster's timeline in `globals.state`, retrying a failing
//! store a few times; an event it can't write is counted (`data_avalanche_events_dropped_total`)
//! and logged, never allowed to slow or fail what it records. Another task expires the days past
//! `globals.console.events.retention`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use data_avalanche_core::{HaltReason, TableRef};
use data_avalanche_pipeline::Phase;
use data_avalanche_state::{ActualState, EventDoc, EventSeverity, Events, HaltsDoc, StatusDoc};
use data_avalanche_telemetry::Metrics;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::gossip::{Members, Phase as NodePhase};

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

    /// An event of pipeline `id` (the console links it), now.
    pub fn of_pipeline(
        &self,
        id: &str,
        kind: &str,
        severity: EventSeverity,
        text: impl Into<String>,
    ) -> EventDoc {
        let mut e = self.event(kind, severity, text);
        e.pipeline = Some(id.to_owned());
        e
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

/// One event a watcher found: its kind, severity, object and text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Found {
    pub kind: &'static str,
    pub severity: EventSeverity,
    pub object: Option<String>,
    pub text: String,
}

fn found(kind: &'static str, severity: EventSeverity, text: String) -> Found {
    Found {
        kind,
        severity,
        object: None,
        text,
    }
}

/// The owner's pipeline state as its status writes show it, over one lease term. `Starting` is
/// passed over, so a worker rerun after a backoff shows no change; the term's first state is
/// news only when it is a failure (a carried-over pause isn't).
#[derive(Debug, Default)]
pub(crate) struct StateWatch {
    shown: Option<ActualState>,
    degraded: bool,
}

impl StateWatch {
    /// What `after`, just written, changed for pipeline `name`.
    pub fn changes(&mut self, name: &str, after: &StatusDoc) -> Vec<Found> {
        use EventSeverity::{Error, Info, Warning};
        let mut out = Vec::new();
        let why = || {
            let error = after.error.as_deref().unwrap_or("no reason given");
            match &after.code {
                Some(code) => format!("{code}: {error}"),
                None => error.to_owned(),
            }
        };
        let state = after.actual;
        if state != ActualState::Starting && self.shown.as_ref() != Some(&state) {
            let first = self.shown.is_none();
            let back = matches!(
                self.shown,
                Some(
                    ActualState::Error
                        | ActualState::Retrying
                        | ActualState::Paused
                        | ActualState::Stopped
                )
            );
            match state {
                ActualState::Error => out.push(found(
                    "pipeline.error",
                    Error,
                    format!("{name} is in ERROR: {}", why()),
                )),
                ActualState::Retrying => out.push(found(
                    "pipeline.retrying",
                    Warning,
                    format!("{name} failed and is retried: {}", why()),
                )),
                ActualState::Paused if !first => {
                    out.push(found("pipeline.paused", Info, format!("{name} is paused")))
                }
                ActualState::Stopped if !first => out.push(found(
                    "pipeline.stopped",
                    Info,
                    format!("{name} is stopped"),
                )),
                ActualState::Dumped if !first => out.push(found(
                    "pipeline.dumped",
                    Info,
                    format!("{name} copied every table: its dump is done"),
                )),
                ActualState::Running if back => out.push(found(
                    "pipeline.running",
                    Info,
                    format!("{name} runs again"),
                )),
                _ => {}
            }
            self.shown = Some(state);
        }
        match (&after.degraded, self.degraded) {
            (Some(d), false) => {
                out.push(found(
                    "pipeline.degraded",
                    Warning,
                    format!("{name}: sink commits or state writes fail, and are retried: {d}"),
                ));
                self.degraded = true;
            }
            (None, true) => {
                out.push(found(
                    "pipeline.recovered",
                    Info,
                    format!("{name}: sink commits and state writes succeed again"),
                ));
                self.degraded = false;
            }
            _ => {}
        }
        out
    }
}

/// The pipeline's halted tables over one lease term, against the halts recorded when it began:
/// a table halted since is news, and so is one a later worker streams without (its halt
/// cleared). Every worker shows the recorded halts once it streams, and only adds to them.
#[derive(Debug, Default)]
pub(crate) struct HaltWatch(Option<BTreeMap<TableRef, HaltReason>>);

impl HaltWatch {
    /// The halts recorded as the term starts (a job's copy isn't the pipeline's).
    pub fn recorded(halts: &HaltsDoc) -> Self {
        Self(Some(
            halts
                .tables
                .iter()
                .filter(|h| !h.copy)
                .map(|h| (h.table.clone(), h.reason.clone()))
                .collect(),
        ))
    }

    /// What a worker's `halted`, in `phase`, changed. Without the recorded halts (unread), the
    /// first streaming worker's are taken as them.
    pub fn changes(
        &mut self,
        phase: &Phase,
        halted: &BTreeMap<TableRef, HaltReason>,
    ) -> Vec<Found> {
        let streaming = *phase == Phase::Streaming;
        let Some(known) = self.0.as_mut() else {
            if streaming {
                self.0 = Some(halted.clone());
            }
            return Vec::new();
        };
        let mut out = Vec::new();
        for (table, reason) in halted {
            if known.get(table) != Some(reason) {
                known.insert(table.clone(), reason.clone());
                out.push(Found {
                    object: Some(table.to_string()),
                    ..found(
                        "table.halted",
                        EventSeverity::Error,
                        format!("{table} halted: {}: {reason}", reason.code()),
                    )
                });
            }
        }
        if streaming {
            known.retain(|table, _| {
                let kept = halted.contains_key(table);
                if !kept {
                    out.push(Found {
                        object: Some(table.to_string()),
                        ..found(
                            "table.resumed",
                            EventSeverity::Info,
                            format!("{table} runs again: its halt is cleared"),
                        )
                    });
                }
                kept
            });
        }
        out
    }
}

/// What changed between two views of the members, each change with the member it is about.
/// Recorded by one node only ([`records`]).
pub(crate) fn member_changes(before: &Members, after: &Members) -> Vec<(String, Found)> {
    use EventSeverity::{Info, Warning};
    let mut out = Vec::new();
    for (name, m) in &after.nodes {
        let change = match before.nodes.get(name) {
            None => Some(found(
                "member.joined",
                Info,
                format!("{name} joined ({})", m.meta.build),
            )),
            Some(was) if was.incarnation != m.incarnation => Some(found(
                "member.restarted",
                Info,
                format!("{name} restarted ({})", m.meta.build),
            )),
            Some(was) if !was.suspect && m.suspect => Some(found(
                "member.suspect",
                Warning,
                format!("{name} is suspect: it stopped answering probes"),
            )),
            Some(was) if was.suspect && !m.suspect => {
                Some(found("member.alive", Info, format!("{name} answers again")))
            }
            Some(was)
                if was.meta.phase != NodePhase::Draining && m.meta.phase == NodePhase::Draining =>
            {
                Some(found(
                    "member.draining",
                    Info,
                    format!("{name} shuts down: it hands its pipelines off"),
                ))
            }
            Some(_) => None,
        };
        out.extend(change.map(|c| (name.clone(), c)));
    }
    for (name, was) in &before.nodes {
        if !after.nodes.contains_key(name) {
            let c = if was.meta.phase == NodePhase::Leaving {
                found("member.left", Info, format!("{name} left the cluster"))
            } else {
                found(
                    "member.down",
                    Warning,
                    format!("{name} is down: it stopped answering and was declared failed"),
                )
            };
            out.push((name.clone(), c));
        }
    }
    for (name, c) in &mut out {
        c.object = Some(name.clone());
    }
    out
}

/// Whether this node records a change about `subject`: it is active in both views, and the
/// first by name of the active, unsuspected members other than `subject`. Views converge, so
/// one node records it; a view that hasn't may record it twice or not at all.
pub(crate) fn records(before: &Members, after: &Members, subject: &str) -> bool {
    let active = |m: &Members| {
        m.nodes
            .get(&m.me)
            .is_some_and(|n| n.meta.phase == NodePhase::Active)
    };
    active(before)
        && active(after)
        && after
            .nodes
            .iter()
            .find(|(name, m)| *name != subject && !m.suspect && m.meta.phase == NodePhase::Active)
            .is_some_and(|(name, _)| *name == after.me)
}

/// Records the members' changes this node is the one to record, until the gossip stops.
pub async fn record_members(recorder: Recorder, mut members: watch::Receiver<Members>) {
    let mut before = members.borrow_and_update().clone();
    while members.changed().await.is_ok() {
        let after = members.borrow_and_update().clone();
        for (subject, c) in member_changes(&before, &after) {
            if records(&before, &after, &subject) {
                let mut e = recorder.event(c.kind, c.severity, c.text);
                e.object = c.object;
                recorder.record(e);
            }
        }
        before = after;
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

    fn status(actual: ActualState, error: Option<&str>, degraded: Option<&str>) -> StatusDoc {
        StatusDoc {
            holder_node_id: "node-a".into(),
            owner_epoch: 1,
            actual,
            error: error.map(str::to_owned),
            code: None,
            degraded: degraded.map(str::to_owned),
            written_at: Utc::now(),
        }
    }

    fn kinds(found: &[Found]) -> Vec<&'static str> {
        found.iter().map(|f| f.kind).collect()
    }

    #[test]
    fn the_state_watch_records_failures_and_recoveries_not_reruns() {
        use ActualState::{Error, Paused, Retrying, Running, Starting};
        let mut w = StateWatch::default();
        assert!(
            w.changes("orders", &status(Paused, None, None)).is_empty(),
            "a carried-over pause"
        );
        assert_eq!(
            kinds(&w.changes("orders", &status(Running, None, None))),
            ["pipeline.running"]
        );
        let retry = w.changes("orders", &status(Retrying, Some("refused"), None));
        assert_eq!(retry[0].text, "orders failed and is retried: refused");
        assert!(
            w.changes("orders", &status(Starting, None, None))
                .is_empty()
        );
        assert!(
            w.changes("orders", &status(Retrying, Some("refused"), None))
                .is_empty(),
            "the rerun failed alike"
        );
        assert_eq!(
            kinds(&w.changes("orders", &status(Error, Some("gone"), None))),
            ["pipeline.error"]
        );
        assert_eq!(
            kinds(&w.changes("orders", &status(Running, None, Some("503")))),
            ["pipeline.running", "pipeline.degraded"]
        );
        assert_eq!(
            kinds(&w.changes("orders", &status(Running, None, None))),
            ["pipeline.recovered"]
        );
        let mut fresh = StateWatch::default();
        assert_eq!(
            kinds(&fresh.changes("orders", &status(Error, Some("gone"), None))),
            ["pipeline.error"]
        );
    }

    fn halted(tables: &[&str]) -> BTreeMap<TableRef, HaltReason> {
        tables
            .iter()
            .map(|t| (TableRef::new("public", *t), HaltReason::TableDropped))
            .collect()
    }

    #[test]
    fn the_halt_watch_records_new_and_cleared_halts_against_the_recorded() {
        let recorded = HaltsDoc {
            tables: vec![data_avalanche_state::TableHalt {
                table: TableRef::new("public", "a"),
                reason: HaltReason::TableDropped,
                at: data_avalanche_core::SourcePosition::Pg(data_avalanche_core::position::Lsn(1)),
                copy: false,
            }],
        };
        let mut w = HaltWatch::recorded(&recorded);
        assert!(
            w.changes(&Phase::Starting, &halted(&[])).is_empty(),
            "a starting worker shows none yet"
        );
        assert!(
            w.changes(&Phase::Streaming, &halted(&["a"])).is_empty(),
            "recorded already"
        );
        let new = w.changes(&Phase::Streaming, &halted(&["a", "b"]));
        assert_eq!(kinds(&new), ["table.halted"]);
        assert_eq!(new[0].object.as_deref(), Some("public.b"));
        assert_eq!(
            new[0].text,
            format!(
                "public.b halted: TABLE_DROPPED: {}",
                HaltReason::TableDropped
            )
        );
        let cleared = w.changes(&Phase::Streaming, &halted(&["b"]));
        assert_eq!(kinds(&cleared), ["table.resumed"]);
        assert_eq!(cleared[0].object.as_deref(), Some("public.a"));

        let mut unread = HaltWatch::default();
        assert!(
            unread
                .changes(&Phase::Streaming, &halted(&["a"]))
                .is_empty(),
            "taken as recorded"
        );
        assert_eq!(
            kinds(&unread.changes(&Phase::Streaming, &halted(&["a", "c"]))),
            ["table.halted"]
        );
    }

    fn members(me: &str, nodes: &[(&str, u64, bool, NodePhase)]) -> Members {
        Members {
            me: me.into(),
            nodes: nodes
                .iter()
                .map(|(n, inc, suspect, phase)| {
                    (
                        (*n).to_owned(),
                        crate::gossip::Member {
                            addr: "127.0.0.1:7946".parse().unwrap(),
                            incarnation: *inc,
                            meta: crate::gossip::NodeMeta {
                                phase: *phase,
                                config_generation: 1,
                                build: "1.1.0".into(),
                            },
                            suspect: *suspect,
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn one_member_records_each_membership_change() {
        use NodePhase::{Active, Joining, Leaving};
        let before = members(
            "a",
            &[
                ("a", 1, false, Active),
                ("b", 1, false, Active),
                ("c", 1, false, Leaving),
            ],
        );
        let after = members(
            "a",
            &[
                ("a", 1, false, Active),
                ("b", 1, true, Active),
                ("d", 1, false, Joining),
            ],
        );
        let changes = member_changes(&before, &after);
        let seen: Vec<_> = changes.iter().map(|(n, c)| (n.as_str(), c.kind)).collect();
        assert_eq!(
            seen,
            [
                ("b", "member.suspect"),
                ("d", "member.joined"),
                ("c", "member.left")
            ]
        );
        assert_eq!(changes[1].1.text, "d joined (1.1.0)");
        for (subject, _) in &changes {
            assert!(
                records(&before, &after, subject),
                "a is the first active other than {subject}"
            );
        }
        let at_b = |m: &Members| Members {
            me: "b".into(),
            ..m.clone()
        };
        assert!(
            !records(&at_b(&before), &at_b(&after), "d"),
            "b leaves it to a"
        );
        assert!(
            !records(&before, &after, "a"),
            "a's own change is b's or no one's"
        );
        let joining = members("d", &[("a", 1, false, Active), ("d", 1, false, Joining)]);
        assert!(
            !records(&joining, &joining, "a"),
            "a joining node records nothing"
        );
        let down = member_changes(
            &after,
            &members("a", &[("a", 1, false, Active), ("d", 2, false, Active)]),
        );
        let seen: Vec<_> = down.iter().map(|(n, c)| (n.as_str(), c.kind)).collect();
        assert_eq!(seen, [("d", "member.restarted"), ("b", "member.down")]);
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
