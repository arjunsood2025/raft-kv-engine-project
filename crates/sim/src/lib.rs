//! Deterministic simulation testing for the raft-kv cluster.
//!
//! FoundationDB-style methodology: the entire distributed system — every
//! node, disk, network link, client, and fault — runs single-threaded on
//! virtual time, driven by one seeded PRNG. A run is a pure function of its
//! seed, so any failure replays exactly: print the seed, rerun it, watch
//! the same bug at the same virtual microsecond.
//!
//! - [`cluster`] — the simulator itself (virtual time, faulty network,
//!   crash/restart with a simulated disk, invariant checks, convergence).
//! - [`wgl`] — Wing & Gong linearizability checker run on every history.
//! - [`history`] — client operation records.
//! - [`rng`] — splitmix64, the single source of randomness.

pub mod cluster;
pub mod history;
pub mod rng;
pub mod wgl;

pub use cluster::{run, RunReport, SimConfig, Stats};
