//! The production host for the raft-kv node: tokio networking + real disks
//! around the same sans-IO `RaftNode` the simulator drives.
//!
//! Layout:
//! - [`logstore`]: raft durability (hard state, log entries, snapshot) on a
//!   dedicated LSM `Db` with `SyncPolicy::Always` — the fsync-before-send
//!   contract lives here.
//! - [`backend`]: `kvsm::Backend` implemented on a second LSM `Db` holding
//!   the applied key-value state.
//! - [`core`]: the single-owner event loop. All raft/state-machine mutation
//!   happens on one task fed by an mpsc channel (ticks, peer messages,
//!   client requests) — concurrency at the edges, sequential consensus in
//!   the middle, which is exactly the discipline the simulator verifies.
//! - [`net`]: peer dial/accept loops and the client connection handler.
//! - [`metrics`]: a tiny hand-rolled Prometheus text endpoint.

pub mod backend;
pub mod core;
pub mod logstore;
pub mod metrics;
pub mod net;
