//! Getting a pipeline's lease and giving it back (Clustering §5.3, §8.3). A free lease (absent,
//! or a released tombstone) is acquired at once. A live holder's lease is taken over only after
//! it stayed unchanged for a full `lease_duration` on this node's clock: a holder that renews in
//! the meantime keeps it.

use chrono::Utc;
use data_avalanche_state::{Acquired, Holder, LeaseDoc, Leases, StateError, Versioned};
use data_avalanche_storage::ObjectVersion;
use tokio::time::Instant;

use crate::LeaseTiming;

/// What one [`Claim::step`] found.
#[derive(Debug)]
pub enum Claimed {
    /// This node holds the lease; `written` is when its write was sent (the renewer's first
    /// `last_renew_local`).
    Mine {
        lease: Versioned<LeaseDoc>,
        written: Instant,
    },
    /// Another holder has it. Unless it renews, a step at or after `takeover_at` takes it over.
    HeldBy {
        node_id: String,
        takeover_at: Instant,
    },
}

/// Acquire and takeover-evaluation for one pipeline, stepped by its controller.
#[derive(Debug)]
pub struct Claim {
    leases: Leases,
    pipeline_id: String,
    holder: Holder,
    timing: LeaseTiming,
    /// The live holder's lease version this node watches, and when it first read it.
    watched: Option<(ObjectVersion, Instant)>,
}

impl Claim {
    pub fn new(leases: Leases, pipeline_id: &str, holder: Holder, timing: LeaseTiming) -> Self {
        Self {
            leases,
            pipeline_id: pipeline_id.to_owned(),
            holder,
            timing,
            watched: None,
        }
    }

    /// One attempt: acquire a free lease, or watch a live one and take it over once it went a
    /// full `lease_duration` without changing.
    ///
    /// # Errors
    /// A storage failure; the next step tries again.
    pub async fn step(&mut self) -> Result<Claimed, StateError> {
        let sent = Instant::now();
        let seen = match self
            .leases
            .acquire(
                &self.pipeline_id,
                &self.holder,
                self.timing.duration,
                Utc::now(),
            )
            .await?
        {
            Acquired::Mine(lease) => {
                self.watched = None;
                return Ok(Claimed::Mine {
                    lease,
                    written: sent,
                });
            }
            Acquired::HeldBy(seen) => seen,
        };
        // The wait runs from when the version was read, never from before: a renewal that
        // landed while the read was in flight must count.
        let read = Instant::now();
        let since = match &self.watched {
            Some((version, since)) if *version == seen.version => *since,
            _ => {
                self.watched = Some((seen.version.clone(), read));
                read
            }
        };
        let takeover_at = since + self.timing.duration;
        if read < takeover_at {
            return Ok(Claimed::HeldBy {
                node_id: seen.doc.holder_node_id,
                takeover_at,
            });
        }
        let sent = Instant::now();
        match self
            .leases
            .take_over(&seen, &self.holder, self.timing.duration, Utc::now())
            .await
        {
            Ok(lease) => {
                self.watched = None;
                Ok(Claimed::Mine {
                    lease,
                    written: sent,
                })
            }
            // The holder renewed, or another challenger won: watch again.
            Err(e) if e.is_conflict() => {
                self.watched = None;
                Ok(Claimed::HeldBy {
                    node_id: seen.doc.holder_node_id,
                    takeover_at: Instant::now() + self.timing.duration,
                })
            }
            Err(e) => {
                // An unknown outcome: the takeover landed if the lease names this holder with
                // the epoch it proposed.
                let now = self.leases.observe(&self.pipeline_id).await?;
                match now {
                    Some(lease)
                        if lease.doc.is_held_by(&self.holder)
                            && Some(lease.doc.owner_epoch)
                                == seen.doc.owner_epoch.checked_add(1) =>
                    {
                        self.watched = None;
                        Ok(Claimed::Mine {
                            lease,
                            written: sent,
                        })
                    }
                    _ => Err(e),
                }
            }
        }
    }
}

/// Gives a held lease back as a tombstone (a planned stop or drain, §8.3): the next acquirer
/// takes it without waiting. A failed write is checked against the stored lease: one that is no
/// longer this holder's live lease (released by the write itself, taken, deleted) leaves nothing
/// to give back; one that still is gets the write again.
///
/// # Errors
/// A storage failure that outlasted the attempts.
pub async fn release(leases: &Leases, lease: &Versioned<LeaseDoc>) -> Result<(), StateError> {
    const ATTEMPTS: usize = 3;
    let mut from = lease.clone();
    let mut attempt = 0;
    loop {
        attempt += 1;
        let Err(e) = leases.release(&from, Utc::now()).await else {
            return Ok(());
        };
        match leases.observe(&lease.doc.pipeline_id).await? {
            Some(now)
                if !now.doc.released
                    && now.doc.holder_node_id == lease.doc.holder_node_id
                    && now.doc.holder_incarnation == lease.doc.holder_incarnation
                    && now.doc.owner_epoch == lease.doc.owner_epoch =>
            {
                if attempt >= ATTEMPTS {
                    return Err(e);
                }
                from = now;
            }
            _ => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::LeaseRun;
    use crate::test_support::{
        Fault, Kind, ScriptedStore, acquired, holder, leases, metrics, timing,
    };

    fn claim(store: &std::sync::Arc<ScriptedStore>, node: &str) -> Claim {
        Claim::new(leases(store), "p", holder(node), timing())
    }

    async fn advance(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn epoch(c: &Claimed) -> u64 {
        match c {
            Claimed::Mine { lease, .. } => lease.doc.owner_epoch,
            Claimed::HeldBy { .. } => panic!("{c:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_free_lease_is_acquired_at_once() {
        let store = ScriptedStore::new();
        assert_eq!(epoch(&claim(&store, "b").step().await.unwrap()), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_released_lease_is_acquired_at_once_with_the_next_epoch() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        release(&leases(&store), &lease).await.unwrap();
        assert_eq!(epoch(&claim(&store, "b").step().await.unwrap()), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_that_renews_keeps_its_lease() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        let (_m, p) = metrics();
        let _run = LeaseRun::start(
            leases(&store),
            lease,
            holder("a"),
            timing(),
            Instant::now(),
            p,
        );
        let mut b = claim(&store, "b");
        for _ in 0..40 {
            match b.step().await.unwrap() {
                Claimed::HeldBy { node_id, .. } => assert_eq!(node_id, "a"),
                mine => panic!("took a live lease: {mine:?}"),
            }
            advance(1_000).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_holders_lease_is_taken_over_after_a_full_lease_duration() {
        let store = ScriptedStore::new();
        acquired(&leases(&store), "a").await;
        let mut b = claim(&store, "b");
        let start = Instant::now();
        let Claimed::HeldBy { takeover_at, .. } = b.step().await.unwrap() else {
            panic!("held by a")
        };
        assert_eq!(takeover_at, start + timing().duration);
        advance(14_990).await;
        assert!(matches!(b.step().await.unwrap(), Claimed::HeldBy { .. }));
        advance(10).await;
        let mine = b.step().await.unwrap();
        assert_eq!(epoch(&mine), 2);
        let stored = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(stored.doc.is_held_by(&holder("b")));
    }

    #[tokio::test(start_paused = true)]
    async fn a_takeover_that_landed_behind_an_error_is_this_nodes() {
        let store = ScriptedStore::new();
        acquired(&leases(&store), "a").await;
        let mut b = claim(&store, "b");
        b.step().await.unwrap();
        advance(15_000).await;
        store.fail_next(Fault {
            kind: Kind::Contention,
            lands: true,
        });
        assert_eq!(epoch(&b.step().await.unwrap()), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_takeover_that_failed_is_tried_again_at_the_next_step() {
        let store = ScriptedStore::new();
        acquired(&leases(&store), "a").await;
        let mut b = claim(&store, "b");
        b.step().await.unwrap();
        advance(15_000).await;
        store.fail_next(Fault {
            kind: Kind::Transient,
            lands: false,
        });
        assert!(b.step().await.is_err());
        assert_eq!(epoch(&b.step().await.unwrap()), 2, "no second wait");
    }

    #[tokio::test(start_paused = true)]
    async fn a_release_whose_write_landed_behind_an_error_is_done() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Transient,
            lands: true,
        });
        release(&leases(&store), &lease).await.unwrap();
        assert!(
            leases(&store)
                .observe("p")
                .await
                .unwrap()
                .unwrap()
                .doc
                .released
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_release_of_a_lease_taken_meanwhile_leaves_it_to_its_holder() {
        let store = ScriptedStore::new();
        let ls = leases(&store);
        let lease = acquired(&ls, "a").await;
        let seen = ls.observe("p").await.unwrap().unwrap();
        ls.take_over(&seen, &holder("b"), timing().duration, chrono::Utc::now())
            .await
            .unwrap();
        release(&ls, &lease).await.unwrap();
        let stored = ls.observe("p").await.unwrap().unwrap();
        assert!(stored.doc.is_held_by(&holder("b")), "{stored:?}");
    }
}
