//! The management API's per-pipeline documents (Pipeline_Lifecycle §4–§5, D-M9a.2): the owner's
//! view of the pipeline, `status.json`, and the one-off actions asked of it, `requests.json`.
//! Any node answers a status from them, and any node records a request; the owner's controller
//! writes the status and takes the requests at its poll. The desired-state transitions of
//! pause, stop and resume are here too.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::doc::{Document, MODIFY_ATTEMPTS, StateError, Versioned};
use crate::pipeline::{DesiredState, PIPELINE_STATE_KEY, PipelineDocs, PipelineStateDoc};

pub const STATUS_KEY: &str = "status.json";
pub const REQUESTS_KEY: &str = "requests.json";

/// The pipeline's actual state, as its owner last saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActualState {
    Starting,
    Running,
    Paused,
    Stopped,
    /// Every table is a `dump_*` table whose snapshot is done.
    Dumped,
    /// The worker ended with an error worth another run.
    Retrying,
    /// An operator must act: a stop, then a resume.
    Error,
}

/// `status.json`: what the lease holder last wrote. It is the holder's only while
/// `owner_epoch` is the lease's: a status a former owner wrote is stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusDoc {
    pub holder_node_id: String,
    pub owner_epoch: u64,
    pub actual: ActualState,
    /// Why the pipeline is retrying or in `ERROR`.
    pub error: Option<String>,
    /// A pipeline-scope halt's code (`BINLOG_PAYLOAD_INVALID`, …).
    pub code: Option<String>,
    /// Sink commits or state writes are failing and being retried (the last error).
    pub degraded: Option<String>,
    pub written_at: DateTime<Utc>,
}

impl Document for StatusDoc {
    const KIND: &'static str = "pipeline status";
    const SCHEMA_VERSION: u64 = 1;
}

/// One request: an id the asker chose, so a write that landed unseen is set again, not repeated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: String,
    pub at: DateTime<Utc>,
}

/// `requests.json`: the last restart and the last flush asked of the pipeline. The owner takes
/// one whose id it hasn't seen in its lease term; what is there when its term starts is taken as
/// seen (a new term starts a new worker anyway).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestsDoc {
    pub restart: Option<Request>,
    pub flush: Option<Request>,
}

impl Document for RequestsDoc {
    const KIND: &'static str = "pipeline requests";
    const SCHEMA_VERSION: u64 = 1;
}

/// A one-off action (Pipeline_Lifecycle §3): not a state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Discard what is unflushed and replay from the checkpoint.
    Restart,
    /// Flush now.
    Flush,
}

/// What a pause, stop or resume did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    Changed {
        from: DesiredState,
    },
    /// The pipeline is in that state already.
    Unchanged,
    /// Pipeline_Lifecycle §3 forbids it: a pause of a stopped pipeline.
    Refused {
        current: DesiredState,
    },
}

/// Pipeline_Lifecycle §3: pause from `RUNNING`, stop from `RUNNING` or `PAUSED`, resume from
/// `PAUSED` or `STOPPED`.
fn allowed(from: DesiredState, to: DesiredState) -> bool {
    !(from == DesiredState::Stopped && to == DesiredState::Paused)
}

impl PipelineDocs {
    /// # Errors
    /// A storage or decode failure.
    pub async fn status(&self) -> Result<Option<Versioned<StatusDoc>>, StateError> {
        self.docs.load(STATUS_KEY).await
    }

    /// Writes the owner's status, and returns the one stored. A stored status of a later term
    /// (a higher `owner_epoch`) is kept: a former owner, stalled past its lease, doesn't
    /// overwrite its successor's.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn save_status(&self, doc: &StatusDoc) -> Result<Versioned<StatusDoc>, StateError> {
        self.docs
            .modify(STATUS_KEY, |stored: Option<StatusDoc>| {
                Ok(match stored {
                    Some(later) if later.owner_epoch > doc.owner_epoch => later,
                    _ => doc.clone(),
                })
            })
            .await
    }

    /// The requests, none before the first.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn requests(&self) -> Result<RequestsDoc, StateError> {
        Ok(self
            .docs
            .load::<RequestsDoc>(REQUESTS_KEY)
            .await?
            .map(|v| v.doc)
            .unwrap_or_default())
    }

    /// Records `request` as the latest of its kind.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn request(&self, action: Action, request: Request) -> Result<(), StateError> {
        self.docs
            .modify(REQUESTS_KEY, |cur: Option<RequestsDoc>| {
                let mut doc = cur.unwrap_or_default();
                match action {
                    Action::Restart => doc.restart = Some(request.clone()),
                    Action::Flush => doc.flush = Some(request.clone()),
                }
                Ok(doc)
            })
            .await?;
        Ok(())
    }

    /// Sets the desired state to `to` when Pipeline_Lifecycle §3 allows it from the current one.
    /// The pipeline must be opened ([`Self::open`]).
    ///
    /// # Errors
    /// `Corrupt` when the pipeline was never opened, a storage or decode failure, or the last
    /// race error once the attempts run out.
    pub async fn transition(&self, to: DesiredState) -> Result<Transition, StateError> {
        // After an unknown outcome the write may have landed: finding `to` then is this change.
        let mut landed: Option<DesiredState> = None;
        for attempt in 1..=MODIFY_ATTEMPTS {
            let Some(Versioned { doc, version }) = self
                .docs
                .load::<PipelineStateDoc>(PIPELINE_STATE_KEY)
                .await?
            else {
                return Err(StateError::Corrupt {
                    key: PIPELINE_STATE_KEY.into(),
                    kind: PipelineStateDoc::KIND,
                    reason: "missing; open the pipeline first".into(),
                });
            };
            let from = doc.desired_state;
            if from == to {
                return Ok(match landed {
                    Some(from) => Transition::Changed { from },
                    None => Transition::Unchanged,
                });
            }
            if !allowed(from, to) {
                return Ok(Transition::Refused { current: from });
            }
            let next = PipelineStateDoc {
                desired_state: to,
                ..doc
            };
            match self.docs.update(PIPELINE_STATE_KEY, &version, &next).await {
                Ok(_) => return Ok(Transition::Changed { from }),
                Err(e) if e.is_race() && attempt < MODIFY_ATTEMPTS => {
                    if !e.is_conflict() {
                        landed = Some(from);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("the last attempt returns")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::doc::DocStore;
    use crate::test_support::WriteThenLoseResponse;

    fn pipeline() -> PipelineDocs {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 23, 10, 0, s).unwrap()
    }

    fn request(id: &str) -> Request {
        Request {
            id: id.into(),
            at: t(0),
        }
    }

    #[tokio::test]
    async fn a_status_is_written_and_read_back() {
        let docs = pipeline();
        assert_eq!(docs.status().await.unwrap(), None);
        let status = StatusDoc {
            holder_node_id: "node-a".into(),
            owner_epoch: 3,
            actual: ActualState::Error,
            error: Some("binlog payload invalid".into()),
            code: Some("BINLOG_PAYLOAD_INVALID".into()),
            degraded: None,
            written_at: t(1),
        };
        docs.save_status(&status).await.unwrap();
        let again = StatusDoc {
            actual: ActualState::Stopped,
            error: None,
            code: None,
            ..status
        };
        docs.save_status(&again).await.unwrap();
        assert_eq!(docs.status().await.unwrap().unwrap().doc, again);
        let raw = docs.docs.store().get(STATUS_KEY).await.unwrap().unwrap().0;
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v["version"], 1);
        assert_eq!(v["actual"], "stopped");
    }

    /// A former owner, stalled past its lease, doesn't overwrite its successor's status.
    #[tokio::test]
    async fn a_status_of_an_earlier_term_keeps_the_later_one() {
        let docs = pipeline();
        let status = |node: &str, owner_epoch, actual| StatusDoc {
            holder_node_id: node.into(),
            owner_epoch,
            actual,
            error: None,
            code: None,
            degraded: None,
            written_at: t(1),
        };
        let later = status("node-b", 2, ActualState::Running);
        docs.save_status(&later).await.unwrap();
        let stale = docs
            .save_status(&status("node-a", 1, ActualState::Stopped))
            .await
            .unwrap();
        assert_eq!(stale.doc, later, "what is stored is returned");
        assert_eq!(docs.status().await.unwrap().unwrap().doc, later);
        let same_term = status("node-b", 2, ActualState::Paused);
        docs.save_status(&same_term).await.unwrap();
        let next = status("node-c", 3, ActualState::Starting);
        docs.save_status(&next).await.unwrap();
        assert_eq!(docs.status().await.unwrap().unwrap().doc, next);
    }

    #[tokio::test]
    async fn a_request_is_the_latest_of_its_kind() {
        let docs = pipeline();
        assert_eq!(docs.requests().await.unwrap(), RequestsDoc::default());
        docs.request(Action::Restart, request("r1")).await.unwrap();
        docs.request(Action::Flush, request("f1")).await.unwrap();
        docs.request(Action::Restart, request("r2")).await.unwrap();
        let r = docs.requests().await.unwrap();
        assert_eq!(r.restart.unwrap().id, "r2");
        assert_eq!(r.flush.unwrap().id, "f1");
    }

    #[tokio::test]
    async fn a_request_whose_write_landed_unseen_is_recorded_once() {
        let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        let docs = PipelineDocs::new(DocStore::new(Arc::new(WriteThenLoseResponse::new(inner))));
        docs.request(Action::Flush, request("f1")).await.unwrap();
        assert_eq!(docs.requests().await.unwrap().flush.unwrap().id, "f1");
    }

    #[tokio::test]
    async fn the_desired_state_moves_as_the_lifecycle_allows() {
        let docs = pipeline();
        assert!(
            docs.transition(DesiredState::Paused).await.is_err(),
            "never opened"
        );
        docs.open("orders", "delta").await.unwrap();
        use DesiredState::{Paused, Running, Stopped};
        assert_eq!(
            docs.transition(Paused).await.unwrap(),
            Transition::Changed { from: Running }
        );
        assert_eq!(
            docs.transition(Paused).await.unwrap(),
            Transition::Unchanged
        );
        assert_eq!(
            docs.transition(Stopped).await.unwrap(),
            Transition::Changed { from: Paused }
        );
        assert_eq!(
            docs.transition(Paused).await.unwrap(),
            Transition::Refused { current: Stopped }
        );
        assert_eq!(
            docs.transition(Running).await.unwrap(),
            Transition::Changed { from: Stopped }
        );
        assert_eq!(
            docs.transition(Running).await.unwrap(),
            Transition::Unchanged
        );
        assert_eq!(
            docs.state().await.unwrap().unwrap().doc.desired_state,
            Running
        );
    }

    #[tokio::test]
    async fn a_transition_whose_write_landed_unseen_is_a_change() {
        let mem = Arc::new(InMemory::new());
        let plain = ObjectStoreVersioned::new(mem.clone(), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(plain)))
            .open("orders", "delta")
            .await
            .unwrap();
        let inner = ObjectStoreVersioned::new(mem, "state/orders").unwrap();
        let docs = PipelineDocs::new(DocStore::new(Arc::new(
            WriteThenLoseResponse::new(inner).on_key(PIPELINE_STATE_KEY),
        )));
        assert_eq!(
            docs.transition(DesiredState::Stopped).await.unwrap(),
            Transition::Changed {
                from: DesiredState::Running
            }
        );
    }
}
