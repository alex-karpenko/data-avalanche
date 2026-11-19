//! Who runs each pipeline (Clustering §2–§8, §19–§20): the gossip that tells a node who its
//! peers are, the ring that places each pipeline on one of them, the lease, its renewer and the
//! commit guard it answers, the controller that runs a pipeline's worker while it holds the
//! lease, and the reconciler that tells each controller whether to own.

pub mod advertised;
pub mod claim;
#[cfg(test)]
mod cluster_tests;
pub mod controller;
pub mod coordinator;
pub mod discovery;
mod error;
pub mod events;
pub mod gossip;
pub mod history;
mod jobs;
pub mod lease;
pub mod node;
pub mod placement;
pub mod reconciler;
pub mod ring;
#[cfg(test)]
mod test_support;
pub mod wire;

pub use advertised::{FILE_CHECK, api_url, file_hash, watch_file};
pub use claim::{Claim, Claimed, release};
pub use controller::{
    Actual, Configured, ControllerDeps, ControllerSettings, ControllerStatus, CurrentPipeline,
    Parts, PipelineController, RemovedPipelines, ResolveError, Resolved, SOURCE_CHANGE_WINDOW,
    SOURCE_CHANGES_IN_A_ROW, Target, Update, WorkerFactory,
};
pub use coordinator::{
    Attribution, Checked, ConfigCoordinator, CoordinatorDeps, DocsFn, MASKED_KEPT,
    PUBLISH_ATTEMPTS, ReloadError, Reloaded, RestartFields, removed_pipelines,
};
pub use discovery::{Seeds, advertise, discover, interfaces};
pub use error::ClusterError;
pub use events::{
    EVENT_QUEUE, MEMBERS_SETTLE, PRUNE_EVERY, Recorder, WRITE_RETRIES, expire_events,
    record_members,
};
pub use gossip::{
    Advertiser, Announcer, Gossip, GossipFigures, GossipSettings, Member, Members, NodeIdentity,
    NodeMeta, Phase, SUSPICIONS_KEPT, SUSPICIONS_LISTED,
};
pub use history::{
    HISTORY_PRUNE_EVERY, Sampler, UNWRITTEN_KEPT, WRITE_EVERY, expire_history, sample_history,
};
pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
pub use node::{JOIN_WAIT, join, leave};
pub use placement::Placement;
pub use reconciler::{
    ClusterReconciler, PipelineReload, ReconcilerSettings, Reload, Reloader, SWEEP_INTERVAL,
    node_identity,
};
pub use ring::Ring;
pub use wire::{GossipKey, Udp, Wire};
