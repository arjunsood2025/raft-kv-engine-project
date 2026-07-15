//! `kvsm::Backend` on the LSM engine — the applied key-value state lives in
//! a dedicated `Db` (directory `sm/` under the node's data dir).
//!
//! This Db runs with `SyncPolicy::Never`, deliberately: the state machine is
//! *derived* state. The durable sources of truth are the raft log and the
//! raft snapshot (both fsynced in [`crate::logstore`]); at startup the core
//! rebuilds this Db from the latest snapshot and re-applies whatever log
//! suffix re-commits. Fsyncing here would pay the durability tax twice for
//! no additional guarantee. (A production engine would instead persist the
//! applied index atomically with each apply batch and skip the rebuild —
//! documented tradeoff: our restart cost is O(state), theirs is O(1).)

use kvsm::Backend;
use std::path::Path;
use storage::{Db, Options, SyncPolicy};

pub struct DbBackend {
    db: Db,
}

impl DbBackend {
    pub fn open(dir: &Path) -> storage::Result<DbBackend> {
        let opts = Options {
            sync: SyncPolicy::Never,
            ..Options::default()
        };
        Ok(DbBackend {
            db: Db::open(dir, opts)?,
        })
    }
}

impl Backend for DbBackend {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).expect("storage read")
    }

    fn set(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.db.write_batch(vec![(key, value)]).expect("storage write");
    }

    fn scan(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db.scan(start, end, limit).expect("storage scan")
    }

    fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db.scan(&[], None, usize::MAX).expect("storage dump")
    }

    fn clear_and_load(&mut self, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
        let existing = self.dump();
        let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            existing.into_iter().map(|(k, _)| (k, None)).collect();
        ops.extend(kvs.into_iter().map(|(k, v)| (k, Some(v))));
        if !ops.is_empty() {
            self.db.write_batch(ops).expect("storage load");
        }
    }
}
