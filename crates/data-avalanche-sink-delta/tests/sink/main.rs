//! The Delta sink through the `Sink` trait: the delta-capability probes re-run against the crate
//! (roadmap M4 exit) on a local directory, and conformance with the reference sink.

mod common;
mod conformance;
mod local;
mod scenarios;
