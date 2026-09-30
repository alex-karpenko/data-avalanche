//! The per-pipeline lease object `leases/<pipeline_id>.json` in `globals.state` (Clustering §5.2)
//! and its compare-and-swap operations. The object version (ETag / GCS generation) is the real
//! lease handle; the timestamps are informational. Timing — when to renew, how long to observe
//! before a takeover, the safety deadline — belongs to the renewer (Clustering §5.3, §19).

use std::time::Duration;

use chrono::{DateTime, Utc};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{CREATE_ATTEMPTS, DocStore, Document, StateError, Versioned};

/// A lease as stored. `released: true` is the tombstone a planned release leaves behind: the next
/// acquirer takes it without waiting, and `owner_epoch` carries on from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseDoc {
    pub pipeline_id: String,
    pub holder_node_id: String,
    pub holder_incarnation: u64,
    /// +1 on every acquisition, including over a released tombstone (the app-layer fence, §5.5).
    pub owner_epoch: u64,
    pub acquired_at: DateTime<Utc>,
    pub renewed_at: DateTime<Utc>,
    pub lease_duration_ms: u64,
    pub released: bool,
}

impl Document for LeaseDoc {
    const KIND: &'static str = "lease";
    const SCHEMA_VERSION: u64 = 1;
}

/// A node as a lease holder: its `node_id` and current incarnation (Clustering §3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    pub node_id: String,
    pub incarnation: u64,
}

impl LeaseDoc {
    /// True when `holder` holds this lease (and it isn't released).
    pub fn is_held_by(&self, holder: &Holder) -> bool {
        !self.released
            && self.holder_node_id == holder.node_id
            && self.holder_incarnation == holder.incarnation
    }
}

/// The result of [`Leases::acquire`].
#[derive(Debug, Clone, PartialEq)]
pub enum Acquired {
    /// This node now holds the lease.
    Mine(Versioned<LeaseDoc>),
    /// Another holder has it; the caller may observe it and consider a takeover (§5.3). It may
    /// name this holder after an earlier call returned an error (race or transient) whose write
    /// in fact landed. This also happens on `acquire`'s own bounded retries: a race on the last
    /// attempt returns `Err` even when that write landed — the caller checks
    /// [`LeaseDoc::is_held_by`] either way.
    HeldBy(Versioned<LeaseDoc>),
}

/// The lease operations over the `globals.state` documents.
#[derive(Debug, Clone)]
pub struct Leases {
    docs: DocStore,
}

/// `leases/<pipeline_id>.json`.
pub fn lease_key(pipeline_id: &str) -> String {
    format!("leases/{}.json", escape_segment(pipeline_id))
}

/// Refuses a loaded document that is for another pipeline (a corrupt or misplaced object).
fn check_pipeline_id(
    key: &str,
    pipeline_id: &str,
    loaded: Option<Versioned<LeaseDoc>>,
) -> Result<Option<Versioned<LeaseDoc>>, StateError> {
    if let Some(v) = &loaded
        && v.doc.pipeline_id != pipeline_id
    {
        return Err(StateError::Corrupt {
            key: key.to_owned(),
            kind: LeaseDoc::KIND,
            reason: format!("it is for pipeline {:?}", v.doc.pipeline_id),
        });
    }
    Ok(loaded)
}

impl Leases {
    /// `docs` is rooted at the `globals.state` location.
    pub fn new(docs: DocStore) -> Self {
        Self { docs }
    }

    fn new_doc(
        pipeline_id: &str,
        holder: &Holder,
        epoch: u64,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> LeaseDoc {
        LeaseDoc {
            pipeline_id: pipeline_id.to_owned(),
            holder_node_id: holder.node_id.clone(),
            holder_incarnation: holder.incarnation,
            owner_epoch: epoch,
            acquired_at: now,
            renewed_at: now,
            lease_duration_ms: u64::try_from(lease_duration.as_millis()).unwrap_or(u64::MAX),
            released: false,
        }
    }

    /// Acquires a free lease: creates it (epoch 1) when absent, or takes a released tombstone
    /// (epoch + 1). A live holder is left alone and returned as [`Acquired::HeldBy`] — unless it
    /// is the very document this call itself just wrote (a `create`/`update` that landed but
    /// returned a race error), in which case it is reported as [`Acquired::Mine`].
    ///
    /// # Errors
    /// A storage or decode failure, or a race error when another acquirer won the tombstone and
    /// the lease changed again before it could be read back.
    pub async fn acquire(
        &self,
        pipeline_id: &str,
        holder: &Holder,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> Result<Acquired, StateError> {
        let key = lease_key(pipeline_id);
        let mut attempts = 0;
        // The doc this call last tried to write, set when that write returned a race error: an
        // unknown outcome (`Store(Contention)`) or a transient one may still have landed.
        let mut attempted: Option<LeaseDoc> = None;
        loop {
            attempts += 1;
            let current = check_pipeline_id(&key, pipeline_id, self.docs.load(&key).await?)?;
            if let Some(seen) = &current
                && attempted.as_ref() == Some(&seen.doc)
            {
                return Ok(Acquired::Mine(seen.clone()));
            }
            let written = match &current {
                None => {
                    let doc = Self::new_doc(pipeline_id, holder, 1, lease_duration, now);
                    attempted = Some(doc.clone());
                    self.docs
                        .create(&key, &doc)
                        .await
                        .map(|v| Versioned { doc, version: v })
                }
                Some(seen) if seen.doc.released => {
                    let Some(epoch) = seen.doc.owner_epoch.checked_add(1) else {
                        return Err(StateError::Corrupt {
                            key: key.clone(),
                            kind: LeaseDoc::KIND,
                            reason: "owner_epoch is at its maximum".to_owned(),
                        });
                    };
                    let doc = Self::new_doc(pipeline_id, holder, epoch, lease_duration, now);
                    attempted = Some(doc.clone());
                    self.docs
                        .update(&key, &seen.version, &doc)
                        .await
                        .map(|v| Versioned { doc, version: v })
                }
                Some(seen) => return Ok(Acquired::HeldBy(seen.clone())),
            };
            match written {
                Ok(lease) => return Ok(Acquired::Mine(lease)),
                // Someone else created or took it first: read again; a live winner is reported
                // as the holder on the next pass (unless it turns out to be our own landed
                // write, above).
                Err(e) if e.is_race() && attempts < CREATE_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Renews a held lease: a CAS write from `lease.version` with a new `renewed_at`.
    ///
    /// # Errors
    /// `Store(Conflict)` means the lease was taken or deleted (lost) — unless this call is
    /// itself a retry after an earlier `Store(Contention)` or a transient write error, in which
    /// case the write may have landed and the `Conflict` only means this call's expected version
    /// is now stale. Either way, after `Store(Contention)` or a transient error, call
    /// [`Self::observe`] before retrying: a stored lease that [`LeaseDoc::is_held_by`] this holder
    /// with the same `owner_epoch` is still this holder's (a takeover always changes both), so
    /// continue renewing from the observed version. Pass only a lease this holder got from
    /// [`Acquired::Mine`], [`Self::take_over`] or [`Self::renew`] — passing a `HeldBy` document
    /// would CAS-write over someone else's live lease.
    pub async fn renew(
        &self,
        lease: &Versioned<LeaseDoc>,
        now: DateTime<Utc>,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let doc = LeaseDoc {
            renewed_at: now,
            ..lease.doc.clone()
        };
        self.write(lease, doc).await
    }

    /// The lease as stored now, or `None` when it doesn't exist.
    ///
    /// # Errors
    /// A storage or decode failure, or `Corrupt` when the object at this key is for another
    /// pipeline.
    pub async fn observe(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<Versioned<LeaseDoc>>, StateError> {
        let key = lease_key(pipeline_id);
        check_pipeline_id(&key, pipeline_id, self.docs.load(&key).await?)
    }

    /// Takes over a lease the caller has observed unchanged for a full lease duration (§5.3): a
    /// CAS write from `observed.version` naming `holder`, with `owner_epoch + 1`.
    ///
    /// # Errors
    /// `Store(Conflict)` when the lease moved since it was observed (the holder renewed, or
    /// another challenger won); `Store(Contention)` (or a transient write error) means the
    /// outcome is unknown — call [`Self::observe`] before retrying and check
    /// [`LeaseDoc::is_held_by`] against `holder`: a stored lease held by `holder` with the same
    /// `owner_epoch` this call proposed is still this call's own landed write (a takeover always
    /// changes both). `Corrupt` when `owner_epoch` is already at its maximum.
    pub async fn take_over(
        &self,
        observed: &Versioned<LeaseDoc>,
        holder: &Holder,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let Some(epoch) = observed.doc.owner_epoch.checked_add(1) else {
            return Err(StateError::Corrupt {
                key: lease_key(&observed.doc.pipeline_id),
                kind: LeaseDoc::KIND,
                reason: "owner_epoch is at its maximum".to_owned(),
            });
        };
        let doc = Self::new_doc(
            &observed.doc.pipeline_id,
            holder,
            epoch,
            lease_duration,
            now,
        );
        self.write(observed, doc).await
    }

    /// Releases a held lease by CAS-writing its tombstone (object_store has no conditional
    /// delete). The epoch is kept, so the next acquisition continues from it.
    ///
    /// # Errors
    /// `Store(Conflict)` when the lease is no longer the one held; `Store(Contention)` (or a
    /// transient write error) means the outcome is unknown — call [`Self::observe`] before
    /// retrying and check `released`. Pass only a lease this holder got from [`Acquired::Mine`],
    /// [`Self::take_over`] or [`Self::renew`] — releasing a `HeldBy` document would tombstone
    /// someone else's live lease.
    pub async fn release(
        &self,
        lease: &Versioned<LeaseDoc>,
        now: DateTime<Utc>,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let doc = LeaseDoc {
            renewed_at: now,
            released: true,
            ..lease.doc.clone()
        };
        self.write(lease, doc).await
    }

    async fn write(
        &self,
        from: &Versioned<LeaseDoc>,
        doc: LeaseDoc,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let key = lease_key(&doc.pipeline_id);
        let version = self.docs.update(&key, &from.version, &doc).await?;
        Ok(Versioned { doc, version })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::test_support::WriteThenLoseResponse;

    const DUR: Duration = Duration::from_secs(15);

    fn leases() -> Leases {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        Leases::new(DocStore::new(Arc::new(store)))
    }

    fn node(id: &str) -> Holder {
        Holder {
            node_id: id.into(),
            incarnation: 7,
        }
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, s).unwrap()
    }

    async fn mine(l: &Leases, id: &str, who: &str) -> Versioned<LeaseDoc> {
        match l.acquire(id, &node(who), DUR, t(0)).await.unwrap() {
            Acquired::Mine(lease) => lease,
            other => panic!("expected to acquire, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acquire_on_absent_creates_epoch_1() {
        let l = leases();
        let lease = mine(&l, "orders", "a").await;
        assert_eq!(
            lease.doc,
            LeaseDoc {
                pipeline_id: "orders".into(),
                holder_node_id: "a".into(),
                holder_incarnation: 7,
                owner_epoch: 1,
                acquired_at: t(0),
                renewed_at: t(0),
                lease_duration_ms: 15_000,
                released: false,
            }
        );
        assert!(lease.doc.is_held_by(&node("a")));
        assert!(!lease.doc.is_held_by(&Holder {
            node_id: "a".into(),
            incarnation: 8
        }));
        assert_eq!(l.observe("orders").await.unwrap(), Some(lease));
    }

    #[test]
    fn lease_json_shape() {
        let doc = Leases::new_doc("orders", &node("worker-7f3a9c"), 87, DUR, t(10));
        assert_eq!(
            std::str::from_utf8(&crate::doc::encode(&doc)).unwrap(),
            r#"{
  "version": 1,
  "pipeline_id": "orders",
  "holder_node_id": "worker-7f3a9c",
  "holder_incarnation": 7,
  "owner_epoch": 87,
  "acquired_at": "2026-09-28T10:00:10Z",
  "renewed_at": "2026-09-28T10:00:10Z",
  "lease_duration_ms": 15000,
  "released": false
}"#
        );
    }

    #[tokio::test]
    async fn acquire_leaves_a_live_holder_alone() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        assert_eq!(
            l.acquire("orders", &node("b"), DUR, t(1)).await.unwrap(),
            Acquired::HeldBy(a)
        );
    }

    #[tokio::test]
    async fn renew_advances_the_version_and_keeps_the_epoch() {
        let l = leases();
        let a0 = mine(&l, "orders", "a").await;
        let a1 = l.renew(&a0, t(5)).await.unwrap();
        assert_ne!(a0.version, a1.version);
        assert_eq!(
            (a1.doc.owner_epoch, a1.doc.renewed_at, a1.doc.acquired_at),
            (1, t(5), t(0))
        );
        let stale = l.renew(&a0, t(6)).await.unwrap_err();
        assert!(stale.is_conflict(), "{stale:?}");
    }

    #[tokio::test]
    async fn take_over_bumps_the_epoch_and_fences_the_old_holder() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        let observed = l.observe("orders").await.unwrap().unwrap();
        let b = l
            .take_over(&observed, &node("b"), DUR, t(20))
            .await
            .unwrap();
        assert_eq!(
            (
                b.doc.holder_node_id.as_str(),
                b.doc.owner_epoch,
                b.doc.acquired_at
            ),
            ("b", 2, t(20))
        );
        assert!(
            l.renew(&a, t(21)).await.unwrap_err().is_conflict(),
            "the old holder has lost it"
        );
    }

    #[tokio::test]
    async fn take_over_fails_when_the_holder_renewed_since_observed() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        let observed = l.observe("orders").await.unwrap().unwrap();
        l.renew(&a, t(5)).await.unwrap();
        let e = l
            .take_over(&observed, &node("b"), DUR, t(20))
            .await
            .unwrap_err();
        assert!(e.is_conflict(), "{e:?}");
    }

    #[tokio::test]
    async fn release_leaves_a_tombstone_and_the_epoch_continues() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        let a = l.renew(&a, t(3)).await.unwrap();
        let tomb = l.release(&a, t(4)).await.unwrap();
        assert!(tomb.doc.released && !tomb.doc.is_held_by(&node("a")));
        assert_eq!(
            (tomb.doc.owner_epoch, tomb.doc.holder_node_id.as_str()),
            (1, "a")
        );
        let b = mine(&l, "orders", "b").await;
        assert_eq!(
            (
                b.doc.owner_epoch,
                b.doc.holder_node_id.as_str(),
                b.doc.released
            ),
            (2, "b", false)
        );
        let b = l.release(&b, t(5)).await.unwrap();
        assert_eq!(
            mine(&l, "orders", "a").await.doc.owner_epoch,
            3,
            "epochs never repeat across releases"
        );
        assert!(
            l.release(&b, t(6)).await.unwrap_err().is_conflict(),
            "a stale release conflicts"
        );
    }

    /// Known limitation: the epoch lives in the object, so deleting it restarts the count.
    #[tokio::test]
    async fn an_externally_deleted_lease_restarts_at_epoch_1() {
        let mem = Arc::new(InMemory::new());
        let store = ObjectStoreVersioned::new(mem.clone(), "globals").unwrap();
        let l = Leases::new(DocStore::new(Arc::new(store)));
        let a = mine(&l, "orders", "a").await;
        let a = l.take_over(&a, &node("b"), DUR, t(1)).await.unwrap();
        assert_eq!(a.doc.owner_epoch, 2);
        object_store::ObjectStoreExt::delete(mem.as_ref(), &"globals/leases/orders.json".into())
            .await
            .unwrap();
        assert!(
            l.renew(&a, t(2)).await.unwrap_err().is_conflict(),
            "the holder sees the delete as a loss"
        );
        assert_eq!(mine(&l, "orders", "c").await.doc.owner_epoch, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_acquirers_have_one_winner() {
        for released_first in [false, true] {
            let l = leases();
            if released_first {
                let a = mine(&l, "orders", "a").await;
                l.release(&a, t(1)).await.unwrap();
            }
            let tasks: Vec<_> = (0..8)
                .map(|i| {
                    let l = l.clone();
                    tokio::spawn(async move {
                        l.acquire("orders", &node(&format!("n{i}")), DUR, t(2))
                            .await
                    })
                })
                .collect();
            let mut winners = vec![];
            for task in tasks {
                if let Acquired::Mine(lease) = task.await.unwrap().unwrap() {
                    winners.push(lease);
                }
            }
            assert_eq!(winners.len(), 1, "released_first={released_first}");
            let stored = l.observe("orders").await.unwrap().unwrap();
            assert_eq!(stored, winners[0]);
            assert_eq!(stored.doc.owner_epoch, if released_first { 2 } else { 1 });
        }
    }

    #[test]
    fn lease_keys_escape_the_pipeline_id() {
        assert_eq!(lease_key("orders-pg"), "leases/orders-pg.json");
        assert_eq!(lease_key("a/b"), "leases/a%2Fb.json");
    }

    #[tokio::test]
    async fn acquire_recognizes_its_own_create_that_landed_despite_contention() {
        let store = WriteThenLoseResponse::new(
            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap(),
        );
        let l = Leases::new(DocStore::new(Arc::new(store)));
        let lease = match l.acquire("orders", &node("a"), DUR, t(0)).await.unwrap() {
            Acquired::Mine(lease) => lease,
            other => panic!("expected Mine (own landed write), got {other:?}"),
        };
        assert_eq!(lease.doc.owner_epoch, 1);
        assert_eq!(lease.doc.holder_node_id, "a");
        assert_eq!(lease, l.observe("orders").await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn acquire_recognizes_its_own_tombstone_take_that_landed_despite_contention() {
        let mem = Arc::new(InMemory::new());
        {
            // Acquire and release once, with no fault injection, to leave a tombstone.
            let store = ObjectStoreVersioned::new(mem.clone(), "globals").unwrap();
            let l = Leases::new(DocStore::new(Arc::new(store)));
            let a = mine(&l, "orders", "a").await;
            l.release(&a, t(1)).await.unwrap();
        }
        let store = WriteThenLoseResponse::new(ObjectStoreVersioned::new(mem, "globals").unwrap());
        let l = Leases::new(DocStore::new(Arc::new(store)));
        let lease = match l.acquire("orders", &node("b"), DUR, t(2)).await.unwrap() {
            Acquired::Mine(lease) => lease,
            other => panic!("expected Mine (own landed write), got {other:?}"),
        };
        assert_eq!(lease.doc.owner_epoch, 2);
        assert_eq!(lease.doc.holder_node_id, "b");
        assert_eq!(lease, l.observe("orders").await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn acquire_refuses_to_overflow_the_epoch_past_a_tombstone() {
        let l = leases();
        let tomb = LeaseDoc {
            pipeline_id: "orders".into(),
            holder_node_id: "a".into(),
            holder_incarnation: 7,
            owner_epoch: u64::MAX,
            acquired_at: t(0),
            renewed_at: t(0),
            lease_duration_ms: 15_000,
            released: true,
        };
        l.docs
            .store()
            .put(&lease_key("orders"), crate::doc::encode(&tomb))
            .await
            .unwrap();
        let e = l
            .acquire("orders", &node("b"), DUR, t(1))
            .await
            .unwrap_err();
        assert!(
            matches!(&e, StateError::Corrupt { kind: "lease", reason, .. } if reason.contains("maximum")),
            "{e:?}"
        );
    }

    #[tokio::test]
    async fn take_over_refuses_to_overflow_the_epoch() {
        let l = leases();
        let doc = LeaseDoc {
            pipeline_id: "orders".into(),
            holder_node_id: "a".into(),
            holder_incarnation: 7,
            owner_epoch: u64::MAX,
            acquired_at: t(0),
            renewed_at: t(0),
            lease_duration_ms: 15_000,
            released: false,
        };
        let version = l
            .docs
            .store()
            .put(&lease_key("orders"), crate::doc::encode(&doc))
            .await
            .unwrap();
        let observed = Versioned { doc, version };
        let e = l
            .take_over(&observed, &node("b"), DUR, t(1))
            .await
            .unwrap_err();
        assert!(
            matches!(&e, StateError::Corrupt { kind: "lease", reason, .. } if reason.contains("maximum")),
            "{e:?}"
        );
    }

    #[tokio::test]
    async fn observe_and_acquire_refuse_a_document_for_another_pipeline() {
        let l = leases();
        let key = lease_key("orders");
        let wrong = Leases::new_doc("shipments", &node("a"), 1, DUR, t(0));
        l.docs
            .store()
            .put(&key, crate::doc::encode(&wrong))
            .await
            .unwrap();

        let e = l.observe("orders").await.unwrap_err();
        assert!(
            matches!(&e, StateError::Corrupt { key: k, kind: "lease", .. } if k == &key),
            "{e:?}"
        );

        let e = l
            .acquire("orders", &node("b"), DUR, t(1))
            .await
            .unwrap_err();
        assert!(
            matches!(&e, StateError::Corrupt { key: k, kind: "lease", .. } if k == &key),
            "{e:?}"
        );
    }
}
