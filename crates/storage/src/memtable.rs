//! In-memory write buffer.
//!
//! Keys are ordered `(user_key ASC, seq DESC)` so that for any user key the
//! newest version is encountered first, and a snapshot read is "first version
//! with seq <= snapshot". A `BTreeMap` gives us ordered iteration for flushes
//! and range scans; a production engine would use a concurrent skiplist, but
//! this engine is single-writer by design (the Raft apply loop is the only
//! writer), so a BTreeMap is the honest choice.

use std::cmp::Reverse;
use std::collections::BTreeMap;

/// (user_key, seq, value); value None = tombstone.
pub type Entry = (Vec<u8>, u64, Option<Vec<u8>>);

#[derive(Default)]
pub struct MemTable {
    map: BTreeMap<(Vec<u8>, Reverse<u64>), Option<Vec<u8>>>,
    bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: Vec<u8>, seq: u64, value: Option<Vec<u8>>) {
        self.bytes += key.len() + value.as_ref().map_or(0, |v| v.len()) + 24;
        self.map.insert((key, Reverse(seq)), value);
    }

    /// Newest version of `key` visible at `snapshot`.
    /// Outer None = key not present in memtable at all;
    /// Some(None) = tombstone (deleted); Some(Some(v)) = live value.
    pub fn get(&self, key: &[u8], snapshot: u64) -> Option<Option<Vec<u8>>> {
        let start = (key.to_vec(), Reverse(snapshot));
        let end = (key.to_vec(), Reverse(0u64));
        self.map.range(start..=end).next().map(|(_, v)| v.clone())
    }

    pub fn approx_bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// All versions, sorted (key ASC, seq DESC) — the exact order SSTables use.
    pub fn iter(&self) -> impl Iterator<Item = Entry> + '_ {
        self.map
            .iter()
            .map(|((k, s), v)| (k.clone(), s.0, v.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_visibility() {
        let mut m = MemTable::new();
        m.insert(b"a".to_vec(), 1, Some(b"v1".to_vec()));
        m.insert(b"a".to_vec(), 3, Some(b"v3".to_vec()));
        m.insert(b"a".to_vec(), 5, None); // deleted at seq 5

        assert_eq!(m.get(b"a", 1), Some(Some(b"v1".to_vec())));
        assert_eq!(m.get(b"a", 2), Some(Some(b"v1".to_vec())));
        assert_eq!(m.get(b"a", 4), Some(Some(b"v3".to_vec())));
        assert_eq!(m.get(b"a", 5), Some(None));
        assert_eq!(m.get(b"a", 100), Some(None));
        assert_eq!(m.get(b"b", 100), None);
    }

    #[test]
    fn iter_order() {
        let mut m = MemTable::new();
        m.insert(b"b".to_vec(), 2, Some(b"x".to_vec()));
        m.insert(b"a".to_vec(), 1, Some(b"y".to_vec()));
        m.insert(b"a".to_vec(), 3, Some(b"z".to_vec()));
        let got: Vec<_> = m.iter().map(|(k, s, _)| (k, s)).collect();
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), 3),
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 2)
            ]
        );
    }
}
