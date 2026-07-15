//! In-memory Raft log with a snapshot base.
//!
//! `entries[0].index == snap_index + 1` always. The host keeps the durable
//! copy (via `Output::append` / snapshot persistence); this structure is the
//! node's working view and is rebuilt from `Persisted` at restart.

use crate::types::{Entry, Index, Term};

#[derive(Debug, Default, Clone)]
pub struct RaftLog {
    pub snap_index: Index,
    pub snap_term: Term,
    pub entries: Vec<Entry>,
}

impl RaftLog {
    pub fn new(snap_index: Index, snap_term: Term, entries: Vec<Entry>) -> Self {
        let log = RaftLog {
            snap_index,
            snap_term,
            entries,
        };
        log.assert_contiguous();
        log
    }

    fn assert_contiguous(&self) {
        for (i, e) in self.entries.iter().enumerate() {
            debug_assert_eq!(
                e.index,
                self.snap_index + 1 + i as u64,
                "log entries must be contiguous from the snapshot base"
            );
        }
    }

    pub fn first_index(&self) -> Index {
        self.snap_index + 1
    }

    pub fn last_index(&self) -> Index {
        self.snap_index + self.entries.len() as u64
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(self.snap_term)
    }

    /// Term at `idx`; None if idx is beyond the log or compacted away
    /// (except the snapshot boundary itself, whose term we remember).
    pub fn term(&self, idx: Index) -> Option<Term> {
        if idx == self.snap_index {
            return Some(self.snap_term);
        }
        if idx < self.snap_index || idx > self.last_index() {
            return None;
        }
        Some(self.entries[(idx - self.snap_index - 1) as usize].term)
    }

    pub fn get(&self, idx: Index) -> Option<&Entry> {
        if idx <= self.snap_index || idx > self.last_index() {
            return None;
        }
        Some(&self.entries[(idx - self.snap_index - 1) as usize])
    }

    /// Entries in [from, from+max), clamped to the log.
    pub fn slice(&self, from: Index, max: usize) -> Vec<Entry> {
        let from = from.max(self.first_index());
        if from > self.last_index() {
            return Vec::new();
        }
        let start = (from - self.snap_index - 1) as usize;
        let end = (start + max).min(self.entries.len());
        self.entries[start..end].to_vec()
    }

    /// Append one entry that must directly follow the current last index.
    pub fn push(&mut self, e: Entry) {
        debug_assert_eq!(e.index, self.last_index() + 1);
        self.entries.push(e);
    }

    /// Drop every entry with index >= idx.
    pub fn truncate_from(&mut self, idx: Index) {
        if idx <= self.snap_index {
            panic!("attempt to truncate into the snapshot (idx {idx} <= snap {})", self.snap_index);
        }
        let keep = (idx - self.snap_index - 1) as usize;
        self.entries.truncate(keep.min(self.entries.len()));
    }

    /// Drop entries covered by a snapshot at (idx, term) — local compaction.
    pub fn compact(&mut self, idx: Index, term: Term) {
        if idx <= self.snap_index {
            return;
        }
        let drop_n = ((idx - self.snap_index) as usize).min(self.entries.len());
        self.entries.drain(..drop_n);
        self.snap_index = idx;
        self.snap_term = term;
        self.assert_contiguous();
    }

    /// Replace everything with a snapshot base (follower InstallSnapshot).
    pub fn reset_to_snapshot(&mut self, idx: Index, term: Term) {
        self.snap_index = idx;
        self.snap_term = term;
        self.entries.clear();
    }
}
