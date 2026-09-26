//! source-capability — capability assessment of the source-side replication client crates.
//!
//! Gates DA-SRC-10 (PostgreSQL pgoutput client) and DA-SRC-11 (MySQL/MariaDB binlog client) in
//! `doc/analysis/CDC_Design_Gap_Analysis_v1.md` §26. The gate list, pass criteria and results live
//! in `README.md`; the probes are `tests/probe_pg.rs` and `tests/probe_mysql.rs` (feature-gated).
