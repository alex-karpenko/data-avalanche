//! Durable state for data-avalanche: the per-pipeline documents (Data-Flow §State Layout), leases
//! and the `globals.state` documents (Clustering §12), each a JSON [`Document`] with a schema
//! `version`, written with compare-and-swap through `data-avalanche-storage`.
//!
//! Every document is pretty JSON with `"version"` first. A document with another schema version
//! is refused ([`StateError::UnsupportedVersion`]) and a malformed one is
//! [`StateError::Corrupt`]; neither is ever treated as absent or overwritten. [`DocStore`] reads
//! and writes them; [`DocStore::modify`] is the read-modify-write loop that retries lost races.
//!
//! `globals.state` (one [`DocStore`] rooted at `data_avalanche_storage::globals_state`):
//! - [`establish_anchor`]: `cluster/anchor.json`, first writer wins. It holds `tokens`,
//!   `lease_duration_ms`, `lock_backend`, `hash_algo` and the `fingerprint_key` for the config
//!   crate's `data_avalanche_config::FingerprintKey` (config §6.2). Startup: load the config
//!   with a random key, establish the anchor, run the config crate's `check_cluster_anchor`,
//!   adopt or bootstrap the config generation ([`ConfigGenerations::bootstrap_or_adopt`]), then
//!   load its text with the anchor's key (`tests/startup.rs` runs the whole sequence).
//! - [`ConfigGenerations`]: `config/generation.json` (CAS pointer) and content-addressed
//!   `config/snapshots/<NNNNNN>-<hash>.yaml` holding the raw config text. A starting node adopts
//!   the current generation; the first one bootstraps generation 1 (Clustering §9.3).
//! - [`Leases`]: `leases/<pipeline_id>.json`: acquire, renew, observe, take over (epoch + 1) and
//!   release (a tombstone that keeps the epoch). Timing — renew interval, takeover wait, safety
//!   deadline — is the renewer's job (M8). T1–T10 in `tests/lease_conformance.rs` prove the CAS
//!   on every backend. Known limitation: deleting a lease object by hand restarts its epoch at 1;
//!   never delete lease objects.
//!
//! Per pipeline (one [`PipelineDocs`] rooted at the pipeline's state location):
//! [`PipelineDocs::open`] creates or checks `pipeline-state.json` (refusing another pipeline's
//! state or a changed `sink_type`), the desired state, `replication/slot.json` (PostgreSQL),
//! `replication/position.json` (MySQL / MariaDB, with the source server identity), per-table
//! snapshot state and on-demand snapshot jobs.

mod anchor;
mod doc;
mod generation;
mod lease;
mod pipeline;
mod snapshot;
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
pub use snapshot::{
    CursorKey, JOBS_PREFIX, JobStatus, JobTable, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
    TableSnapshotDoc, job_key, table_snapshot_key,
};
