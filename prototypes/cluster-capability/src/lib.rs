//! The two membership candidates behind one tokio-friendly shape each: `memberlist_node` wraps
//! memberlist 0.8.5's TCP/UDP transport, `foca_node` drives foca 2.0's sans-IO core from an actor.

#[cfg(feature = "foca")]
pub mod foca_node;
#[cfg(feature = "memberlist")]
pub mod memberlist_node;
