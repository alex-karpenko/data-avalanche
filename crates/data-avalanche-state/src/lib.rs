//! Durable state for data-avalanche: the per-pipeline documents (Data-Flow §State Layout), leases
//! and the `globals.state` documents (Clustering §12), each a JSON [`Document`] with a schema
//! `version`, written with compare-and-swap through `data-avalanche-storage`.

mod anchor;
mod doc;
mod generation;
mod lease;
mod pipeline;
#[cfg(test)]
mod test_support;

pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
pub use generation::{
    ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, config_hash,
    config_snapshot_key,
};
pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
pub use pipeline::{
    DesiredState, PIPELINE_STATE_KEY, POSITION_KEY, PipelineDocs, PipelineStateDoc, PositionDoc,
    SLOT_KEY, ServerIdentity, SlotDoc,
};
