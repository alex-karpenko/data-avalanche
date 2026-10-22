//! Who runs each pipeline (Clustering §2–§8, §19–§20): the gossip that tells a node who its
//! peers are, the ring that places each pipeline on one of them, the lease, its renewer and the
//! commit guard it answers, the controller that runs a pipeline's worker while it holds the
//! lease, and the reconciler that tells each controller whether to own.

pub mod claim;
#[cfg(test)]
mod cluster_tests;
pub mod controller;
pub mod discovery;
mod error;
pub mod gossip;
pub mod lease;
pub mod node;
pub mod placement;
pub mod reconciler;
pub mod ring;
#[cfg(test)]
mod test_support;
pub mod wire;

pub use claim::{Claim, Claimed, release};
pub use controller::{
    Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
    Target, Update, WorkerFactory,
};
pub use discovery::{Seeds, advertise, discover, interfaces};
pub use error::ClusterError;
pub use gossip::{
    Announcer, Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase,
};
pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
pub use node::{JOIN_WAIT, join, leave};
pub use placement::Placement;
pub use reconciler::{ClusterReconciler, ReconcilerSettings, node_identity};
pub use ring::Ring;
pub use wire::{GossipKey, Udp, Wire};
