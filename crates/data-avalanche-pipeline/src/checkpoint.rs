//! Where the pipeline's checkpoint lives (Data-Flow Phase 5): PostgreSQL's slot document
//! (`replication/slot.json`), or MySQL's position document (`replication/position.json`), which
//! records the server the position was read from with it.

use data_avalanche_core::{Checkpoint, SourcePosition};
use data_avalanche_state::{PipelineDocs, PositionDoc, SlotDoc, StateError};

use crate::{AccumulatorError, WorkerError};

/// The document that holds the checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Store {
    /// PostgreSQL: the slot's document. `intent` is the slot a first start records its creation
    /// intent for (`slot_management: managed`), `None` when the pipeline never creates it.
    Slot {
        name: String,
        intent: Option<String>,
    },
    /// MySQL / MariaDB: the position document.
    Position,
}

/// A checkpoint as the state holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub checkpoint: Checkpoint,
    /// A slot intent: a first start recorded it and ended before the slot's position was
    /// recorded. Its checkpoint (`0/0`) is no stream position.
    pub intent: bool,
}

/// One checkpoint write, as the store's document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Doc {
    Slot(SlotDoc),
    Position(PositionDoc),
}

impl Store {
    /// The checkpoint the state holds; `None` before a first start records one.
    pub async fn load(&self, docs: &PipelineDocs) -> Result<Option<Stored>, StateError> {
        Ok(match self {
            Self::Slot { .. } => docs.slot().await?.map(|v| Stored {
                checkpoint: SourcePosition::Pg(v.doc.confirmed_flush_lsn).into(),
                intent: v.doc.is_intent(),
            }),
            Self::Position => docs.position().await?.map(|v| Stored {
                checkpoint: Checkpoint {
                    position: v.doc.position,
                    server: Some(v.doc.server),
                },
                intent: false,
            }),
        })
    }

    /// The slot intent a first start records before `prepare` creates the slot; `None` for a
    /// store that records none.
    pub fn intent(&self) -> Option<SlotDoc> {
        match self {
            Self::Slot {
                intent: Some(name), ..
            } => Some(SlotDoc::intent(name)),
            _ => None,
        }
    }

    /// What the heartbeat found when the source runs no stream for the pipeline: PostgreSQL's
    /// slot has no reader, MySQL's server runs no binlog dump for it.
    pub fn unread(&self) -> &'static str {
        match self {
            Self::Slot { .. } => "no stream reads the slot",
            Self::Position => "the server runs no binlog dump for the stream",
        }
    }

    /// `checkpoint` as this store's document.
    ///
    /// # Errors
    /// A protocol error: a PostgreSQL checkpoint that isn't an LSN, or a MySQL one with no server.
    pub fn doc(&self, checkpoint: &Checkpoint) -> Result<Doc, WorkerError> {
        let protocol = |detail: String| WorkerError::from(AccumulatorError::Protocol { detail });
        match (self, checkpoint) {
            (
                Self::Slot { name, .. },
                Checkpoint {
                    position: SourcePosition::Pg(lsn),
                    ..
                },
            ) => Ok(Doc::Slot(SlotDoc {
                slot_name: name.clone(),
                confirmed_flush_lsn: *lsn,
            })),
            (Self::Slot { .. }, c) => Err(protocol(format!(
                "checkpoint {} isn't a PostgreSQL position",
                c.position
            ))),
            (
                Self::Position,
                Checkpoint {
                    position,
                    server: Some(server),
                },
            ) => Ok(Doc::Position(PositionDoc {
                position: position.clone(),
                server: server.clone(),
            })),
            (Self::Position, c) => Err(protocol(format!(
                "checkpoint {} names no server to record with it",
                c.position
            ))),
        }
    }
}

impl Doc {
    pub async fn save(&self, docs: &PipelineDocs) -> Result<(), StateError> {
        match self {
            Self::Slot(d) => docs.save_slot(d).await.map(drop),
            Self::Position(d) => docs.save_position(d).await.map(drop),
        }
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::ServerIdentity;
    use data_avalanche_core::position::Lsn;

    use super::*;
    use crate::test_support::docs;

    fn mysql() -> ServerIdentity {
        ServerIdentity::Mysql {
            server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
        }
    }

    fn pg(n: u64) -> Checkpoint {
        SourcePosition::Pg(Lsn(n)).into()
    }

    #[tokio::test]
    async fn a_slot_holds_a_postgresql_checkpoint() {
        let (d, _) = docs();
        let store = Store::Slot {
            name: "p_slot".into(),
            intent: Some("p_slot".into()),
        };
        assert_eq!(store.load(&d).await.unwrap(), None);
        d.save_slot(&store.intent().unwrap()).await.unwrap();
        let intent = store.load(&d).await.unwrap().unwrap();
        assert!(intent.intent && intent.checkpoint == pg(0));
        store.doc(&pg(0x200)).unwrap().save(&d).await.unwrap();
        assert_eq!(
            store.load(&d).await.unwrap(),
            Some(Stored {
                checkpoint: pg(0x200),
                intent: false
            })
        );
        let unmanaged = Store::Slot {
            name: "p_slot".into(),
            intent: None,
        };
        assert_eq!(unmanaged.intent(), None);
    }

    #[tokio::test]
    async fn a_position_document_holds_the_server_with_the_position() {
        let (d, _) = docs();
        let store = Store::Position;
        assert_eq!(store.load(&d).await.unwrap(), None);
        assert_eq!(store.intent(), None);
        let at = Checkpoint {
            position: SourcePosition::parse_tagged("binlog:mysql-bin.000003:154").unwrap(),
            server: Some(mysql()),
        };
        store.doc(&at).unwrap().save(&d).await.unwrap();
        assert_eq!(
            store.load(&d).await.unwrap(),
            Some(Stored {
                checkpoint: at,
                intent: false
            })
        );
        assert!(d.slot().await.unwrap().is_none(), "no slot document");
    }

    #[test]
    fn a_stream_the_source_doesnt_run_is_named_for_the_source() {
        let slot = Store::Slot {
            name: "s".into(),
            intent: None,
        };
        assert_eq!(slot.unread(), "no stream reads the slot");
        assert!(Store::Position.unread().contains("binlog dump"));
    }

    #[test]
    fn a_checkpoint_the_store_cant_hold_is_a_protocol_error() {
        let slot = Store::Slot {
            name: "s".into(),
            intent: None,
        };
        let binlog = SourcePosition::parse_tagged("binlog:mysql-bin.000003:154").unwrap();
        let e = slot.doc(&binlog.clone().into()).unwrap_err();
        assert!(e.to_string().contains("isn't a PostgreSQL position"), "{e}");
        let e = Store::Position.doc(&binlog.into()).unwrap_err();
        assert!(e.to_string().contains("names no server"), "{e}");
    }
}
