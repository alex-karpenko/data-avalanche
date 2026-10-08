//! A held lease and its renewer (Clustering §19). The renewer CAS-writes the lease every
//! `lease_renewal_interval`; a write it can't get through degrades it, and it is lost once the
//! safety deadline passes without one, or at once when the lease was taken or deleted. The
//! [`LeaseGuard`] answers the worker's [`CommitGuard`] from the renewer's state and deadline.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use data_avalanche_config::Cluster;
use data_avalanche_pipeline::{Backoff, CommitGuard};
use data_avalanche_state::{Holder, LeaseDoc, Leases, StateError, Versioned};
use data_avalanche_storage::StoreError;
use data_avalanche_telemetry::{LeaseState, PipelineMetrics, RenewFailure};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// The lease's timings (`globals.cluster`, Clustering §18–§19).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LeaseTiming {
    pub duration: Duration,
    pub renewal_interval: Duration,
    /// The fraction of `duration` kept as the guard before the safety deadline.
    pub safety_margin: f64,
}

impl LeaseTiming {
    pub fn new(cluster: &Cluster) -> Self {
        Self {
            duration: cluster.lease_duration,
            renewal_interval: cluster.lease_renewal_interval,
            safety_margin: cluster.lease_safety_margin,
        }
    }

    /// `last_renew_local + lease_duration − guard`, `guard = lease_duration × lease_safety_margin`
    /// (§19.3).
    pub fn deadline(&self, renewed: Instant) -> Instant {
        renewed + self.duration - self.duration.mul_f64(self.safety_margin)
    }

    /// The degraded renewer's retries: several fit in one renewal interval (§19.4).
    fn retry(&self) -> Backoff {
        Backoff {
            first: Duration::from_millis(100).min(self.renewal_interval / 8),
            max: self.renewal_interval / 4,
        }
    }
}

/// The renewer's state (§19.4); `Renewing` is the write in flight, not a state the guard sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewState {
    Held,
    /// A renewal failed and is being retried; commits go on until the deadline.
    Degraded,
    /// Terminal for this ownership: nothing more is committed.
    Lost,
}

#[derive(Debug, Clone, Copy)]
struct Proof {
    state: RenewState,
    deadline: Instant,
}

/// The worker's [`CommitGuard`] over a held lease: `may_commit` = state ∈ {Held, Degraded} ∧
/// now < deadline (§19.5), on the local clock.
#[derive(Debug)]
pub struct LeaseGuard {
    proof: watch::Sender<Proof>,
}

impl LeaseGuard {
    fn new(deadline: Instant) -> Self {
        Self {
            proof: watch::Sender::new(Proof {
                state: RenewState::Held,
                deadline,
            }),
        }
    }

    pub fn state(&self) -> RenewState {
        self.proof.borrow().state
    }

    /// Waits until the renewer has lost the lease.
    pub async fn lost(&self) {
        let mut rx = self.proof.subscribe();
        // The sender lives in `self`: the wait ends only with a state.
        let _ = rx.wait_for(|p| p.state == RenewState::Lost).await;
    }

    fn set(&self, state: RenewState, deadline: Option<Instant>) {
        self.proof.send_modify(|p| {
            p.state = state;
            if let Some(d) = deadline {
                p.deadline = d;
            }
        });
    }
}

impl CommitGuard for LeaseGuard {
    fn may_commit(&self) -> bool {
        let p = *self.proof.borrow();
        p.state != RenewState::Lost && Instant::now() < p.deadline
    }
}

/// How a renewer ended.
#[derive(Debug)]
pub enum RenewEnd {
    /// Asked to stop: the lease as last written, for a release.
    Stopped(Versioned<LeaseDoc>),
    /// The lease is gone; nothing more may be committed.
    Lost { reason: String },
}

/// A held lease with its renewer running.
#[derive(Debug)]
pub struct LeaseRun {
    pub guard: Arc<LeaseGuard>,
    /// The lease's `owner_epoch`: every commit carries it.
    pub owner_epoch: u64,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<RenewEnd>,
}

impl LeaseRun {
    /// Starts renewing `lease`, which this holder wrote at `written` (local clock, taken before
    /// the write was sent).
    pub fn start(
        leases: Leases,
        lease: Versioned<LeaseDoc>,
        holder: Holder,
        timing: LeaseTiming,
        written: Instant,
        metrics: Arc<PipelineMetrics>,
    ) -> Self {
        let guard = Arc::new(LeaseGuard::new(timing.deadline(written)));
        metrics.lease(LeaseState::Held);
        let owner_epoch = lease.doc.owner_epoch;
        let (stop, stopped) = oneshot::channel();
        let renewer = Renewer {
            leases,
            lease,
            holder,
            timing,
            renewed: written,
            guard: Arc::clone(&guard),
            metrics,
        };
        Self {
            guard,
            owner_epoch,
            stop: Some(stop),
            task: tokio::spawn(renewer.run(stopped)),
        }
    }

    /// Stops renewing; the lease as last written comes back unless it was lost meanwhile.
    pub async fn stop(mut self) -> RenewEnd {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        match (&mut self.task).await {
            Ok(end) => end,
            Err(e) => RenewEnd::Lost {
                reason: format!("the lease renewer ended abnormally: {e}"),
            },
        }
    }
}

impl Drop for LeaseRun {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Renewer {
    leases: Leases,
    lease: Versioned<LeaseDoc>,
    holder: Holder,
    timing: LeaseTiming,
    /// When the last successful write was sent.
    renewed: Instant,
    guard: Arc<LeaseGuard>,
    metrics: Arc<PipelineMetrics>,
}

impl Renewer {
    async fn run(mut self, mut stop: oneshot::Receiver<()>) -> RenewEnd {
        let retry = self.timing.retry();
        let mut failures: u32 = 0;
        // The last write's outcome is unknown (contention, a transient error): it may have
        // landed, so the lease is read before the next write.
        let mut unknown = false;
        let mut next = self.renewed + self.timing.renewal_interval;
        loop {
            let deadline = self.timing.deadline(self.renewed);
            tokio::select! {
                _ = &mut stop => return RenewEnd::Stopped(self.lease),
                () = tokio::time::sleep_until(next.min(deadline)) => {}
            }
            if Instant::now() >= deadline {
                let budget = deadline - self.renewed;
                return self.lost(format!(
                    "no renewal got through within the safety deadline ({budget:?} after the last one)"
                ));
            }
            let sent = Instant::now();
            // Bounded by the deadline: a write that hangs past it can't prove the lease anyway.
            let attempt = tokio::time::timeout_at(deadline, async {
                self.observe_if(unknown).await?;
                self.leases
                    .renew(&self.lease, Utc::now())
                    .await
                    .map_err(Failure::from)
            })
            .await;
            let Ok(outcome) = attempt else {
                return self.lost("the renewal was still unanswered at the safety deadline".into());
            };
            match outcome {
                Ok(lease) => {
                    self.lease = lease;
                    self.renewed = sent;
                    failures = 0;
                    unknown = false;
                    self.guard
                        .set(RenewState::Held, Some(self.timing.deadline(sent)));
                    self.metrics.lease(LeaseState::Held);
                    self.metrics.renewed(sent.elapsed());
                    next = sent + self.timing.renewal_interval;
                }
                Err(Failure::Retry { class }) => {
                    unknown = true;
                    self.metrics.renew_failed(class);
                    self.guard.set(RenewState::Degraded, None);
                    self.metrics.lease(LeaseState::Degraded);
                    next = Instant::now() + retry.delay(failures);
                    failures = failures.saturating_add(1);
                }
                Err(Failure::Lost(reason)) => return self.lost(reason),
            }
        }
    }

    /// After a write whose outcome is unknown: the lease is still this holder's when it names
    /// this holder with the same epoch (a takeover changes both); it continues from what is
    /// stored.
    async fn observe_if(&mut self, unknown: bool) -> Result<(), Failure> {
        if !unknown {
            return Ok(());
        }
        match self.leases.observe(&self.lease.doc.pipeline_id).await {
            Ok(Some(seen))
                if seen.doc.is_held_by(&self.holder)
                    && seen.doc.owner_epoch == self.lease.doc.owner_epoch =>
            {
                self.lease = seen;
                Ok(())
            }
            Ok(Some(_)) => Err(Failure::Lost(
                "the lease was taken by another holder".into(),
            )),
            Ok(None) => Err(Failure::Lost("the lease was deleted".into())),
            // A failed read leaves the write before it unknown: the next attempt reads again.
            Err(e) => Err(Failure::from(e)),
        }
    }

    fn lost(self, reason: String) -> RenewEnd {
        self.guard.set(RenewState::Lost, None);
        self.metrics.renew_failed(RenewFailure::Lost);
        self.metrics.lease(LeaseState::Lost);
        RenewEnd::Lost { reason }
    }
}

/// A failed renewal (§19.2).
enum Failure {
    /// Status unknown: retried until the deadline, reading the lease first (the write may have
    /// gone through).
    Retry { class: RenewFailure },
    /// Taken (412), deleted (404) or a permanent fault (403): no retry.
    Lost(String),
}

impl From<StateError> for Failure {
    fn from(e: StateError) -> Self {
        match e {
            StateError::Store(StoreError::Transient { .. }) => Self::Retry {
                class: RenewFailure::Transient,
            },
            StateError::Store(StoreError::Contention { .. }) => Self::Retry {
                class: RenewFailure::Conflict,
            },
            StateError::Store(StoreError::Conflict { .. }) => {
                Self::Lost("the lease was taken or deleted".into())
            }
            e => Self::Lost(format!("the lease can't be renewed: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        Fault, Kind, ScriptedStore, acquired, holder, leases, metrics, timing,
    };

    async fn advance(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn start(
        store: &Arc<ScriptedStore>,
        lease: Versioned<LeaseDoc>,
    ) -> (LeaseRun, data_avalanche_telemetry::Metrics) {
        let (m, p) = metrics();
        let run = LeaseRun::start(
            leases(store),
            lease,
            holder("a"),
            timing(),
            Instant::now(),
            p,
        );
        (run, m)
    }

    #[tokio::test(start_paused = true)]
    async fn the_guard_refuses_past_its_deadline_on_its_own() {
        let guard = LeaseGuard::new(Instant::now() + Duration::from_secs(1));
        assert!(guard.may_commit());
        advance(1_000).await;
        assert_eq!(guard.state(), RenewState::Held, "no renewer runs");
        assert!(!guard.may_commit());
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_lease_is_renewed_every_interval() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        let first = lease.version.clone();
        let (run, m) = start(&store, lease);
        advance(16_000).await;
        assert_eq!(run.guard.state(), RenewState::Held);
        assert!(run.guard.may_commit());
        assert!(
            m.encode()
                .contains("data_avalanche_lease_renew_latency_seconds_count{pipeline=\"p\",source_host=\"h\",source_db=\"d\"} 3"),
            "{}",
            m.encode()
        );
        let RenewEnd::Stopped(last) = run.stop().await else {
            panic!("stopped")
        };
        let stored = leases(&store).observe("p").await.unwrap().unwrap();
        assert_ne!(stored.version, first);
        assert_eq!(stored, last, "the stop hands back the lease as stored");
        leases(&store).release(&last, Utc::now()).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_renewal_degrades_and_a_retry_restores_it() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Transient,
            lands: false,
        });
        let (run, m) = start(&store, lease);
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Degraded);
        assert!(run.guard.may_commit(), "commits go on until the deadline");
        advance(1_000).await;
        assert_eq!(run.guard.state(), RenewState::Held);
        let text = m.encode();
        assert!(text.contains("class=\"transient\"} 1"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn renewals_that_keep_failing_lose_the_lease_at_the_deadline() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_always(Some(Fault {
            kind: Kind::Transient,
            lands: false,
        }));
        let (run, _m) = start(&store, lease);
        advance(12_740).await;
        assert_eq!(run.guard.state(), RenewState::Degraded);
        assert!(run.guard.may_commit());
        advance(20).await;
        assert!(!run.guard.may_commit(), "the deadline is 12.75 s");
        run.guard.lost().await;
        assert!(matches!(run.stop().await, RenewEnd::Lost { .. }));
    }

    /// The guard checks the deadline itself: a renewer stuck in a write doesn't extend it.
    #[tokio::test(start_paused = true)]
    async fn a_renewal_that_never_answers_loses_the_lease_at_the_deadline() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Hang,
            lands: false,
        });
        let (run, _m) = start(&store, lease);
        advance(12_740).await;
        assert!(run.guard.may_commit());
        advance(20).await;
        assert!(!run.guard.may_commit());
        run.guard.lost().await;
        match run.stop().await {
            RenewEnd::Lost { reason } => assert!(reason.contains("unanswered"), "{reason}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_taken_by_another_holder_is_lost_at_the_next_renewal() {
        let store = ScriptedStore::new();
        let ls = leases(&store);
        let lease = acquired(&ls, "a").await;
        let (run, _m) = start(&store, lease);
        let seen = ls.observe("p").await.unwrap().unwrap();
        ls.take_over(&seen, &holder("b"), timing().duration, Utc::now())
            .await
            .unwrap();
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Lost);
        assert!(!run.guard.may_commit());
        match run.stop().await {
            RenewEnd::Lost { reason } => assert!(reason.contains("taken"), "{reason}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_renewal_that_landed_behind_an_error_is_still_held() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Contention,
            lands: true,
        });
        let (run, m) = start(&store, lease);
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Degraded);
        advance(1_000).await;
        assert_eq!(run.guard.state(), RenewState::Held);
        assert!(m.encode().contains("class=\"conflict\"} 1"));
        advance(10_000).await;
        assert_eq!(run.guard.state(), RenewState::Held, "renewing on");
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_loses_the_lease_at_once() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Permanent,
            lands: false,
        });
        let (run, m) = start(&store, lease);
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Lost);
        assert!(m.encode().contains("class=\"lost\"} 1"));
    }
}
