use std::net::SocketAddr;

use thiserror::Error;

/// Why a node can't take its place in the cluster.
#[derive(Debug, Error)]
pub enum ClusterError {
    /// No usable node name (Clustering §3.4, D-1).
    #[error("{0}: set globals.cluster.memberlist.node_name")]
    NodeName(String),
    /// No address the others could reach this node at.
    #[error("{0}: set globals.cluster.memberlist.advertise_addr")]
    AdvertiseAddr(String),
    /// A gossip socket couldn't be bound.
    #[error("the gossip can't bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}
