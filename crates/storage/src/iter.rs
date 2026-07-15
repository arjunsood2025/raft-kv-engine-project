//! Merge iterator across memtable + all SSTables.
//!
//! Every source yields (key ASC, seq DESC) streams; the merge picks the
//! globally smallest head each step, so the merged stream is also
//! (key ASC, seq DESC). `VisibleIter` then applies MVCC visibility: skip
//! versions newer than the snapshot, emit the first (newest) visible version
//! per key, and skip the rest.
//!
//! With a handful of sources a linear scan of the heads is as fast as a
//! binary heap and much simpler; the heap becomes worth it at high level
//! counts.

use crate::memtable::Entry;

pub struct MergeIter<'a> {
    sources: Vec<std::iter::Peekable<Box<dyn Iterator<Item = Entry> + 'a>>>,
}

impl<'a> MergeIter<'a> {
    pub fn new(sources: Vec<Box<dyn Iterator<Item = Entry> + 'a>>) -> Self {
        MergeIter {
            sources: sources.into_iter().map(|s| s.peekable()).collect(),
        }
    }
}

impl Iterator for MergeIter<'_> {
    type Item = Entry;
    fn next(&mut self) -> Option<Entry> {
        // Two-pass to satisfy the borrow checker: snapshot the winning head
        // (key, seq), then advance that source.
        let mut best_idx: Option<usize> = None;
        let mut best_head: Option<(Vec<u8>, u64)> = None;
        for (i, src) in self.sources.iter_mut().enumerate() {
            if let Some((k, s, _)) = src.peek() {
                let head = (k.clone(), *s);
                let take = match &best_head {
                    None => true,
                    Some((bk, bs)) => match head.0.cmp(bk) {
                        std::cmp::Ordering::Less => true,
                        std::cmp::Ordering::Equal => head.1 > *bs, // higher seq first
                        std::cmp::Ordering::Greater => false,
                    },
                };
                if take {
                    best_head = Some(head);
                    best_idx = Some(i);
                }
            }
        }
        best_idx.and_then(|i| self.sources[i].next())
    }
}

/// MVCC visibility filter over a (key ASC, seq DESC) stream.
/// Yields at most one version per user key: the newest with seq <= snapshot.
/// Tombstones ARE yielded (value None) — callers that only want live data
/// filter them; compaction needs to see them.
pub struct VisibleIter<I: Iterator<Item = Entry>> {
    inner: I,
    snapshot: u64,
    current_key: Option<Vec<u8>>,
}

impl<I: Iterator<Item = Entry>> VisibleIter<I> {
    pub fn new(inner: I, snapshot: u64) -> Self {
        VisibleIter {
            inner,
            snapshot,
            current_key: None,
        }
    }
}

impl<I: Iterator<Item = Entry>> Iterator for VisibleIter<I> {
    type Item = (Vec<u8>, Option<Vec<u8>>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (k, s, v) = self.inner.next()?;
            if self.current_key.as_deref() == Some(k.as_slice()) {
                continue; // older version of a key we already emitted/decided
            }
            if s > self.snapshot {
                continue; // too new for this snapshot; keep looking at older versions
            }
            self.current_key = Some(k.clone());
            return Some((k, v));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(v: Vec<Entry>) -> Box<dyn Iterator<Item = Entry>> {
        Box::new(v.into_iter())
    }

    #[test]
    fn merges_in_order() {
        let a = vec![
            (b"a".to_vec(), 5, Some(b"a5".to_vec())),
            (b"c".to_vec(), 2, Some(b"c2".to_vec())),
        ];
        let b = vec![
            (b"a".to_vec(), 3, Some(b"a3".to_vec())),
            (b"b".to_vec(), 4, Some(b"b4".to_vec())),
        ];
        let merged: Vec<_> = MergeIter::new(vec![src(a), src(b)])
            .map(|(k, s, _)| (k, s))
            .collect();
        assert_eq!(
            merged,
            vec![
                (b"a".to_vec(), 5),
                (b"a".to_vec(), 3),
                (b"b".to_vec(), 4),
                (b"c".to_vec(), 2),
            ]
        );
    }

    #[test]
    fn visible_iter_applies_snapshot() {
        let entries = vec![
            (b"a".to_vec(), 5, Some(b"a5".to_vec())),
            (b"a".to_vec(), 3, Some(b"a3".to_vec())),
            (b"b".to_vec(), 9, None),
            (b"b".to_vec(), 4, Some(b"b4".to_vec())),
        ];
        // At snapshot 4: a → a3, b → b4 (tombstone at 9 invisible).
        let got: Vec<_> = VisibleIter::new(entries.clone().into_iter(), 4).collect();
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), Some(b"a3".to_vec())),
                (b"b".to_vec(), Some(b"b4".to_vec())),
            ]
        );
        // At latest: a → a5, b → tombstone.
        let got: Vec<_> = VisibleIter::new(entries.into_iter(), u64::MAX).collect();
        assert_eq!(
            got,
            vec![(b"a".to_vec(), Some(b"a5".to_vec())), (b"b".to_vec(), None)]
        );
    }
}
