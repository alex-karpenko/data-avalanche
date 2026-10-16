//! Per-pipeline documents under the pipeline's state location (Data-Flow §State Layout):
//! `pipeline-state.json`, `replication/slot.json` and `replication/position.json`. Only the lease
//! holder writes them; a CAS read-modify-write keeps a slow writer from losing another's update.

use std::fmt::Display;
use std::str::FromStr;

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{ServerIdentity, SourcePosition};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::doc::{CREATE_ATTEMPTS, DocStore, Document, StateError, Versioned};

pub const PIPELINE_STATE_KEY: &str = "pipeline-state.json";
pub const SLOT_KEY: &str = "replication/slot.json";
pub const POSITION_KEY: &str = "replication/position.json";

/// Operator intent (Pipeline_Lifecycle §4): survives restarts, failover and config reloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DesiredState {
    Running,
    Paused,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineStateDoc {
    pub pipeline_id: String,
    /// `sink.type` when the state was created (`delta`, `iceberg`).
    pub sink_type: String,
    pub desired_state: DesiredState,
}

impl Document for PipelineStateDoc {
    const KIND: &'static str = "pipeline state";
    const SCHEMA_VERSION: u64 = 1;
}

/// PostgreSQL: the replication slot and the LSN the source may release WAL up to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotDoc {
    pub slot_name: String,
    /// As PostgreSQL prints it (`0/16B3748`).
    #[serde(serialize_with = "display", deserialize_with = "from_str")]
    pub confirmed_flush_lsn: Lsn,
}

impl Document for SlotDoc {
    const KIND: &'static str = "replication slot";
    const SCHEMA_VERSION: u64 = 1;
}

impl SlotDoc {
    /// Recorded before a first start's `prepare` creates the slot: `confirmed_flush_lsn` `0/0`,
    /// which no slot has. A start that finds it knows a slot of this name is its own (created, then
    /// the start ended before it recorded the slot's position), so the slot is recreated and the
    /// start is a first start again, not an orphan's.
    pub fn intent(slot_name: &str) -> Self {
        Self {
            slot_name: slot_name.to_owned(),
            confirmed_flush_lsn: Lsn(0),
        }
    }

    pub fn is_intent(&self) -> bool {
        self.confirmed_flush_lsn == Lsn(0)
    }
}

/// MySQL / MariaDB: the stream position (`mysql-gtid:…`, `mariadb-gtid:…`, `binlog:<file>:<pos>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionDoc {
    pub position: SourcePosition,
    pub server: ServerIdentity,
}

impl Document for PositionDoc {
    const KIND: &'static str = "replication position";
    const SCHEMA_VERSION: u64 = 1;
}

fn display<T: Display, S: Serializer>(v: &T, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(v)
}

fn from_str<'de, T, D>(d: D) -> Result<T, D::Error>
where
    T: FromStr,
    T::Err: Display,
    D: Deserializer<'de>,
{
    String::deserialize(d)?
        .parse()
        .map_err(serde::de::Error::custom)
}

/// The documents of one pipeline. `docs` is rooted at its state location
/// (`data_avalanche_storage::colocated_state` / `explicit_state`).
#[derive(Debug, Clone)]
pub struct PipelineDocs {
    pub(crate) docs: DocStore,
}

impl PipelineDocs {
    pub fn new(docs: DocStore) -> Self {
        Self { docs }
    }

    /// Pipeline start: creates `pipeline-state.json` (desired state `running`) on first start;
    /// afterwards checks it belongs to `pipeline_id` and records `sink_type`.
    ///
    /// # Errors
    /// `PipelineIdMismatch`, `SinkTypeMismatch` (refuse the pipeline, config §13.4), or a storage
    /// or decode failure.
    pub async fn open(
        &self,
        pipeline_id: &str,
        sink_type: &str,
    ) -> Result<Versioned<PipelineStateDoc>, StateError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            if let Some(existing) = self
                .docs
                .load::<PipelineStateDoc>(PIPELINE_STATE_KEY)
                .await?
            {
                let doc = &existing.doc;
                if doc.pipeline_id != pipeline_id {
                    return Err(StateError::PipelineIdMismatch {
                        recorded: doc.pipeline_id.clone(),
                        configured: pipeline_id.to_owned(),
                    });
                }
                if doc.sink_type != sink_type {
                    return Err(StateError::SinkTypeMismatch {
                        recorded: doc.sink_type.clone(),
                        configured: sink_type.to_owned(),
                    });
                }
                return Ok(existing);
            }
            let doc = PipelineStateDoc {
                pipeline_id: pipeline_id.to_owned(),
                sink_type: sink_type.to_owned(),
                desired_state: DesiredState::Running,
            };
            match self.docs.create(PIPELINE_STATE_KEY, &doc).await {
                Ok(version) => return Ok(Versioned { doc, version }),
                Err(e) if e.is_race() && attempts < CREATE_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// The pipeline's state document, read without creating it: `None` before the first start.
    /// The controller polls it for the desired state (Q-M6b-2).
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn state(&self) -> Result<Option<Versioned<PipelineStateDoc>>, StateError> {
        self.docs.load(PIPELINE_STATE_KEY).await
    }

    /// Records a pause / resume / stop (Pipeline_Lifecycle §4).
    ///
    /// # Errors
    /// `Corrupt` when the pipeline was never opened, or a storage or decode failure.
    pub async fn set_desired_state(
        &self,
        state: DesiredState,
    ) -> Result<Versioned<PipelineStateDoc>, StateError> {
        self.docs
            .modify(
                PIPELINE_STATE_KEY,
                |cur: Option<PipelineStateDoc>| match cur {
                    Some(doc) => Ok(PipelineStateDoc {
                        desired_state: state,
                        ..doc
                    }),
                    None => Err(StateError::Corrupt {
                        key: PIPELINE_STATE_KEY.into(),
                        kind: PipelineStateDoc::KIND,
                        reason: "missing; open the pipeline first".into(),
                    }),
                },
            )
            .await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn slot(&self) -> Result<Option<Versioned<SlotDoc>>, StateError> {
        self.docs.load(SLOT_KEY).await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_slot(&self, doc: &SlotDoc) -> Result<Versioned<SlotDoc>, StateError> {
        self.docs
            .modify(SLOT_KEY, |_: Option<SlotDoc>| Ok(doc.clone()))
            .await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn position(&self) -> Result<Option<Versioned<PositionDoc>>, StateError> {
        self.docs.load(POSITION_KEY).await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_position(
        &self,
        doc: &PositionDoc,
    ) -> Result<Versioned<PositionDoc>, StateError> {
        self.docs
            .modify(POSITION_KEY, |_: Option<PositionDoc>| Ok(doc.clone()))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;

    fn pipeline() -> PipelineDocs {
        let store =
            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "lake/orders/.pipeline-state")
                .unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn pos(s: &str) -> SourcePosition {
        s.parse().unwrap()
    }

    fn json<T: Document>(doc: &T) -> String {
        String::from_utf8(crate::doc::encode(doc).to_vec()).unwrap()
    }

    #[tokio::test]
    async fn open_creates_running_state_then_checks_it() {
        let p = pipeline();
        let first = p.open("orders", "delta").await.unwrap();
        assert_eq!(
            first.doc,
            PipelineStateDoc {
                pipeline_id: "orders".into(),
                sink_type: "delta".into(),
                desired_state: DesiredState::Running
            }
        );
        assert_eq!(p.open("orders", "delta").await.unwrap(), first);
    }

    #[tokio::test]
    async fn a_changed_sink_type_is_refused() {
        let p = pipeline();
        p.open("orders", "delta").await.unwrap();
        let e = p.open("orders", "iceberg").await.unwrap_err();
        assert!(
            matches!(&e, StateError::SinkTypeMismatch { recorded, configured } if recorded == "delta" && configured == "iceberg"),
            "{e:?}"
        );
        assert_eq!(
            e.to_string(),
            "pipeline state records sink type delta but the config says iceberg; positions and markers are sink-specific, so a sink type change needs a new state location (re-seed)"
        );
    }

    #[tokio::test]
    async fn another_pipelines_state_is_refused() {
        let p = pipeline();
        p.open("orders", "delta").await.unwrap();
        let e = p.open("invoices", "delta").await.unwrap_err();
        assert!(matches!(e, StateError::PipelineIdMismatch { .. }), "{e:?}");
    }

    #[tokio::test]
    async fn desired_state_round_trips() {
        let p = pipeline();
        assert!(
            p.set_desired_state(DesiredState::Paused).await.is_err(),
            "not opened yet"
        );
        assert!(p.state().await.unwrap().is_none(), "nothing created");
        p.open("orders", "delta").await.unwrap();
        for s in [
            DesiredState::Paused,
            DesiredState::Stopped,
            DesiredState::Running,
        ] {
            assert_eq!(p.set_desired_state(s).await.unwrap().doc.desired_state, s);
            assert_eq!(p.state().await.unwrap().unwrap().doc.desired_state, s);
            assert_eq!(
                p.open("orders", "delta").await.unwrap().doc.desired_state,
                s,
                "survives a restart"
            );
        }
    }

    #[test]
    fn pipeline_state_json_shape() {
        let doc = PipelineStateDoc {
            pipeline_id: "orders".into(),
            sink_type: "delta".into(),
            desired_state: DesiredState::Paused,
        };
        assert_eq!(
            json(&doc),
            "{\n  \"version\": 1,\n  \"pipeline_id\": \"orders\",\n  \"sink_type\": \"delta\",\n  \"desired_state\": \"paused\"\n}"
        );
    }

    #[tokio::test]
    async fn slot_round_trips() {
        let p = pipeline();
        assert!(p.slot().await.unwrap().is_none());
        let doc = SlotDoc {
            slot_name: "data_avalanche_orders".into(),
            confirmed_flush_lsn: "0/16B3748".parse().unwrap(),
        };
        assert_eq!(
            json(&doc),
            "{\n  \"version\": 1,\n  \"slot_name\": \"data_avalanche_orders\",\n  \"confirmed_flush_lsn\": \"0/16B3748\"\n}"
        );
        p.save_slot(&doc).await.unwrap();
        let later = SlotDoc {
            confirmed_flush_lsn: "1/0".parse().unwrap(),
            ..doc
        };
        p.save_slot(&later).await.unwrap();
        assert_eq!(p.slot().await.unwrap().unwrap().doc, later);
    }

    #[test]
    fn a_slot_intent_is_the_zero_position() {
        let intent = SlotDoc::intent("data_avalanche_orders");
        assert!(intent.is_intent());
        assert_eq!(
            json(&intent),
            "{\n  \"version\": 1,\n  \"slot_name\": \"data_avalanche_orders\",\n  \"confirmed_flush_lsn\": \"0/0\"\n}"
        );
        let started = SlotDoc {
            confirmed_flush_lsn: "0/16B3748".parse().unwrap(),
            ..intent
        };
        assert!(!started.is_intent());
    }

    #[tokio::test]
    async fn position_round_trips_for_every_mysql_dialect() {
        let p = pipeline();
        let cases = [
            (
                "mysql-gtid:3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
                ServerIdentity::Mysql {
                    server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
                },
            ),
            (
                "mariadb-gtid:0-1-100,1-2-7",
                ServerIdentity::Mariadb { server_id: 1 },
            ),
            (
                "binlog:mysql-bin.000042:1337",
                ServerIdentity::Mysql {
                    server_uuid: "u".into(),
                },
            ),
        ];
        for (text, server) in cases {
            let doc = PositionDoc {
                position: pos(text),
                server,
            };
            p.save_position(&doc).await.unwrap();
            assert_eq!(p.position().await.unwrap().unwrap().doc, doc, "{text}");
        }
    }

    #[test]
    fn position_json_shape() {
        let doc = PositionDoc {
            position: pos("binlog:mysql-bin.000042:1337"),
            server: ServerIdentity::Mariadb { server_id: 7 },
        };
        assert_eq!(
            json(&doc),
            r#"{
  "version": 1,
  "position": "binlog:mysql-bin.000042:1337",
  "server": {
    "engine": "mariadb",
    "server_id": 7
  }
}"#
        );
    }

    #[tokio::test]
    async fn malformed_positions_are_corrupt() {
        let p = pipeline();
        for body in [
            r#"{"version":1,"position":"nope:1","server":{"engine":"mariadb","server_id":1}}"#,
            r#"{"version":1,"position":"pg:0/1","server":{"engine":"oracle","sid":1}}"#,
        ] {
            p.docs
                .store()
                .put(POSITION_KEY, Bytes::from(body))
                .await
                .unwrap();
            let e = p.position().await.unwrap_err();
            assert!(matches!(e, StateError::Corrupt { .. }), "{body}: {e:?}");
        }
        p.docs
            .store()
            .put(
                SLOT_KEY,
                Bytes::from(r#"{"version":1,"slot_name":"s","confirmed_flush_lsn":"16B3748"}"#),
            )
            .await
            .unwrap();
        assert!(matches!(
            p.slot().await.unwrap_err(),
            StateError::Corrupt { .. }
        ));
    }
}
