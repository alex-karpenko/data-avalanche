//! Who runs each pipeline (Clustering §19–§20): the lease, its renewer and the commit guard it
//! answers, the controller that runs a pipeline's worker while it holds the lease, and the
//! reconciler that tells each controller whether to own. M6b runs them single-node: a self-only
//! ring, so every pipeline's target is "own".

pub mod claim;
pub mod controller;
pub mod lease;
pub mod reconciler;
#[cfg(test)]
mod test_support;

pub use claim::{Claim, Claimed, release};
pub use controller::{
    Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
    Target, WorkerFactory,
};
pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
pub use reconciler::{ClusterReconciler, node_identity};
