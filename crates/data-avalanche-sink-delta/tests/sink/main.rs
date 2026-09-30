//! The Delta sink through the `Sink` trait: the delta-capability probes re-run against the crate
//! (roadmap M4 exit) on a local directory and on object stores, conformance with the reference
//! sink, and the Hop 3b type round trip.

mod common;
mod conformance;
mod local;
mod scenarios;
mod stores;
mod types;
