//! Raft durability on top of the LSM engine.
//!
//! One `Db` (directory `raft/` under the node's data dir) holds:
//! - `m/hs`   → bincode [`HardState`] (term + voted_for)
//! - `m/snap` → bincode raft [`Snapshot`] (log prefix + state machine image)
//! - `e/<index BE64>` → bincode [`Entry`], big-endian so lexicographic key
//!   order equals index order and a prefix scan replays the log in order.
//!
//! The Db is opened with `SyncPolicy::Always`: `persist()` returning means
//! the batch survived into the WAL with an fsync. The core calls `persist()`
//! BEFORE handing any message from the same `Output` to the network — the
//! rule the raft core's `Output` contract states and the simulator enforces.
//! (Why: an un-fsynced vote could be re-cast differently after a crash =
//! two leaders in one term; an un-fsynced acked entry could vanish =
//! committed-entry loss.)

use raft::{Entry, HardState, Index, Persisted, Snapshot};
use std::path::Path;
use storage::{Db, Options, SyncPolicy};

const KEY_HS: &[u8] = b"m/hs";
const KEY_SNAP: &[u8] = b"m/snap";
const ENTRY_PREFIX: &[u8] = b"e/";
/// Exclusive upper bound for entry-key scans ('0' is the byte after '/').
const ENTRY_END: &[u8] = b"e0";

fn entry_key(index: Index) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(ENTRY_PREFIX);
    k.extend_from_slice(&index.to_be_bytes());
    k
}

fn ser<T: serde::Serialize>(v: &T) -> Vec<u8> {
    bincode::serialize(v).expect("logstore serialization is infallible")
}

pub struct LogStore {
    db: Db,
    /// Lowest and highest entry index currently stored (both 0 = empty).
    first_index: Index,
    last_index: Index,
}

impl LogStore {
    /// Open (or create) the store and reconstruct [`Persisted`] for
    /// `RaftNode::new`.
    pub fn open(dir: &Path) -> storage::Result<(LogStore, Persisted)> {
        let opts = Options {
            sync: SyncPolicy::Always,
            ..Options::default()
        };
        let db = Db::open(dir, opts)?;

        let hard_state: HardState = match db.get(KEY_HS)? {
            Some(b) => bincode::deserialize(&b)
                .map_err(|e| storage::Error::Corruption(format!("hard state: {e}")))?,
            None => HardState::default(),
        };
        let snapshot: Option<Snapshot> = match db.get(KEY_SNAP)? {
            Some(b) => Some(
                bincode::deserialize(&b)
                    .map_err(|e| storage::Error::Corruption(format!("snapshot: {e}")))?,
            ),
            None => None,
        };
        let mut entries = Vec::new();
        for (_, v) in db.scan(ENTRY_PREFIX, Some(ENTRY_END), usize::MAX)? {
            let e: Entry = bincode::deserialize(&v)
                .map_err(|e| storage::Error::Corruption(format!("log entry: {e}")))?;
            entries.push(e);
        }
        let first_index = entries.first().map_or(0, |e| e.index);
        let last_index = entries.last().map_or(0, |e| e.index);

        Ok((
            LogStore {
                db,
                first_index,
                last_index,
            },
            Persisted {
                hard_state,
                entries,
                snapshot,
            },
        ))
    }

    /// Persist the durable parts of one raft `Output` as a single fsynced
    /// batch. `append` uses truncate-from-then-append semantics: an entry at
    /// index i invalidates every stored entry with index >= i (that is how a
    /// follower repairs a divergent log), so those keys are deleted in the
    /// same batch. `snapshot` here is a leader-installed snapshot, which
    /// replaces the entire log.
    pub fn persist(
        &mut self,
        hs: Option<&HardState>,
        append: &[Entry],
        snapshot: Option<&Snapshot>,
    ) -> storage::Result<()> {
        let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        if let Some(hs) = hs {
            ops.push((KEY_HS.to_vec(), Some(ser(hs))));
        }
        if let Some(snap) = snapshot {
            ops.push((KEY_SNAP.to_vec(), Some(ser(snap))));
            if self.last_index != 0 {
                for i in self.first_index..=self.last_index {
                    ops.push((entry_key(i), None));
                }
            }
            self.first_index = 0;
            self.last_index = 0;
        }
        if let Some(first_new) = append.first().map(|e| e.index) {
            // Delete the truncated suffix; puts below overwrite the rest
            // (a later op in the same batch gets a higher MVCC seq, so the
            // put wins over any delete on the same key).
            if self.last_index != 0 && first_new <= self.last_index {
                for i in first_new..=self.last_index {
                    ops.push((entry_key(i), None));
                }
            }
            for e in append {
                ops.push((entry_key(e.index), Some(ser(e))));
            }
            if self.first_index == 0 || first_new < self.first_index {
                self.first_index = first_new;
            }
            self.last_index = append.last().unwrap().index;
        }
        if !ops.is_empty() {
            self.db.write_batch(ops)?;
        }
        Ok(())
    }

    /// Record a locally-taken compaction snapshot and drop the log prefix it
    /// covers, in one batch.
    pub fn compact_to(&mut self, snap: &Snapshot) -> storage::Result<()> {
        let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            vec![(KEY_SNAP.to_vec(), Some(ser(snap)))];
        if self.last_index != 0 && self.first_index <= snap.last_index {
            let upto = snap.last_index.min(self.last_index);
            for i in self.first_index..=upto {
                ops.push((entry_key(i), None));
            }
            if snap.last_index >= self.last_index {
                self.first_index = 0;
                self.last_index = 0;
            } else {
                self.first_index = snap.last_index + 1;
            }
        }
        self.db.write_batch(ops)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::EntryPayload;

    fn e(index: Index, term: u64) -> Entry {
        Entry {
            term,
            index,
            payload: EntryPayload::Normal(vec![index as u8]),
        }
    }

    #[test]
    fn roundtrip_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut ls, p) = LogStore::open(dir.path()).unwrap();
            assert_eq!(p.entries.len(), 0);
            let hs = HardState {
                term: 3,
                voted_for: Some(2),
            };
            ls.persist(Some(&hs), &[e(1, 1), e(2, 1), e(3, 2)], None)
                .unwrap();
            // Divergence repair: index 2 replaced, 3 must disappear.
            ls.persist(None, &[e(2, 3)], None).unwrap();
        }
        let (_, p) = LogStore::open(dir.path()).unwrap();
        assert_eq!(p.hard_state.term, 3);
        assert_eq!(p.hard_state.voted_for, Some(2));
        let idx: Vec<_> = p.entries.iter().map(|e| (e.index, e.term)).collect();
        assert_eq!(idx, vec![(1, 1), (2, 3)]);
    }

    #[test]
    fn compaction_drops_prefix_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut ls, _) = LogStore::open(dir.path()).unwrap();
            ls.persist(None, &(1..=10).map(|i| e(i, 1)).collect::<Vec<_>>(), None)
                .unwrap();
            let snap = Snapshot {
                last_index: 7,
                last_term: 1,
                voters: vec![1, 2, 3],
                data: b"machine".to_vec(),
            };
            ls.compact_to(&snap).unwrap();
        }
        let (_, p) = LogStore::open(dir.path()).unwrap();
        let snap = p.snapshot.expect("snapshot persisted");
        assert_eq!(snap.last_index, 7);
        let idx: Vec<_> = p.entries.iter().map(|e| e.index).collect();
        assert_eq!(idx, vec![8, 9, 10]);
    }
}
