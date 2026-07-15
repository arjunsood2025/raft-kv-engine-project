//! From-scratch LSM-tree storage engine.
//!
//! Layers (bottom-up):
//! - [`wal`]: CRC-framed write-ahead log with torn-write recovery.
//! - [`memtable`]: ordered in-memory buffer, MVCC versions keyed (key, seq).
//! - [`sstable`]: immutable sorted tables with sparse index + bloom filter,
//!   every block checksummed.
//! - [`manifest`]: durable catalog of live tables per level.
//! - [`iter`]: k-way merge + MVCC visibility filtering.
//! - [`db`]: the engine — write path, read path, leveled compaction,
//!   snapshots.

pub mod bloom;
mod db;
mod error;
pub mod iter;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;

pub use db::{Db, Options, Snapshot};
pub use error::{Error, Result};
pub use memtable::Entry;
pub use wal::SyncPolicy;
