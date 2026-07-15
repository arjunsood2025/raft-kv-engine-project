//! The LSM engine: WAL + memtable + leveled SSTables.
//!
//! Write path: append to WAL (fsync per `SyncPolicy`) → insert into memtable
//! → when the memtable exceeds its budget, flush it as an L0 SSTable and
//! reset the WAL. Read path: memtable, then L0 (all tables, newest version
//! wins), then L1+ (non-overlapping, binary search). MVCC: every write gets a
//! monotonically increasing sequence number; a snapshot is just a sequence
//! number, and reads return the newest version <= that number.
//!
//! Level invariant: for any user key, every version in level N is newer than
//! every version in level N+1 (compaction only moves data downward), so a hit
//! at a shallower level ends the search.
//!
//! Compaction runs inline on the write path (deterministic and simple); a
//! production engine moves it to a background thread and throttles it.

use crate::iter::{MergeIter, VisibleIter};
use crate::manifest::{Manifest, TableMeta};
use crate::memtable::{Entry, MemTable};
use crate::sstable::{SsTable, TableBuilder};
use crate::wal::{SyncPolicy, Wal, WalBatch};
use crate::Result;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Options {
    pub memtable_bytes: usize,
    pub sync: SyncPolicy,
    /// Number of L0 tables that triggers an L0→L1 compaction.
    pub l0_trigger: usize,
    /// Byte budget of L1; level N holds base * multiplier^(N-1).
    pub level_base_bytes: u64,
    pub level_multiplier: u64,
    pub target_table_bytes: u64,
    pub block_bytes: usize,
    pub bits_per_key: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_bytes: 4 << 20,
            sync: SyncPolicy::Always,
            l0_trigger: 4,
            level_base_bytes: 32 << 20,
            level_multiplier: 10,
            target_table_bytes: 4 << 20,
            block_bytes: 4096,
            bits_per_key: 10,
        }
    }
}

type SnapshotRegistry = Arc<Mutex<BTreeMap<u64, u32>>>;

/// RAII read snapshot. While alive, compaction will not garbage-collect any
/// version this snapshot can see.
pub struct Snapshot {
    pub seq: u64,
    registry: SnapshotRegistry,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let mut reg = self.registry.lock().unwrap();
        if let Some(c) = reg.get_mut(&self.seq) {
            *c -= 1;
            if *c == 0 {
                reg.remove(&self.seq);
            }
        }
    }
}

pub struct Db {
    dir: PathBuf,
    opts: Options,
    mem: MemTable,
    wal: Wal,
    levels: Vec<Vec<SsTable>>,
    manifest: Manifest,
    seq: u64,
    snapshots: SnapshotRegistry,
}

fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:08}.sst"))
}

impl Db {
    pub fn open(dir: &Path, opts: Options) -> Result<Db> {
        std::fs::create_dir_all(dir)?;
        let manifest = Manifest::load(dir)?.unwrap_or_default();
        let mut levels: Vec<Vec<SsTable>> = Vec::new();
        for level in &manifest.levels {
            let mut tables = Vec::new();
            for meta in level {
                tables.push(SsTable::open(&table_path(dir, meta.id), meta.id)?);
            }
            levels.push(tables);
        }
        if levels.is_empty() {
            levels.push(Vec::new());
        }

        // Replay the WAL into a fresh memtable. Re-inserting ops that were
        // already flushed is harmless: identical (key, seq) versions are
        // deduplicated during compaction and reads pick by seq.
        let mut seq = manifest.last_seq;
        let wal_path = dir.join("wal");
        let mut mem = MemTable::new();
        for batch in Wal::replay(&wal_path)? {
            for (i, (k, v)) in batch.ops.into_iter().enumerate() {
                let s = batch.first_seq + i as u64;
                mem.insert(k, s, v);
                seq = seq.max(s);
            }
        }
        let wal = Wal::open(&wal_path, opts.sync)?;

        let mut db = Db {
            dir: dir.to_path_buf(),
            opts,
            mem,
            wal,
            levels,
            manifest,
            seq,
            snapshots: SnapshotRegistry::default(),
        };
        if db.mem.approx_bytes() >= db.opts.memtable_bytes {
            db.flush()?;
            db.maybe_compact()?;
        }
        Ok(db)
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    // ------------------------------------------------------------- writes

    /// Atomically apply a batch: one WAL frame, contiguous sequence numbers.
    /// Returns the last sequence number assigned. Once this returns (with
    /// SyncPolicy::Always) the batch is durable.
    pub fn write_batch(&mut self, ops: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Result<u64> {
        if ops.is_empty() {
            return Ok(self.seq);
        }
        let n = ops.len() as u64;
        let first_seq = self.seq + 1;
        self.wal.append(&WalBatch {
            first_seq,
            ops: ops.clone(),
        })?;
        for (i, (k, v)) in ops.into_iter().enumerate() {
            self.mem.insert(k, first_seq + i as u64, v);
        }
        self.seq = first_seq + n - 1;
        self.maybe_flush()?;
        Ok(self.seq)
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        self.write_batch(vec![(key, Some(value))])
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<u64> {
        self.write_batch(vec![(key, None)])
    }

    // -------------------------------------------------------------- reads

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_at(key, self.seq)
    }

    pub fn get_at(&self, key: &[u8], snapshot: u64) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.mem.get(key, snapshot) {
            return Ok(v);
        }
        for (li, level) in self.levels.iter().enumerate() {
            let mut best: Option<(u64, Option<Vec<u8>>)> = None;
            if li == 0 {
                // L0 tables overlap: check all, newest seq wins.
                for t in level {
                    if let Some((s, v)) = t.get(key, snapshot)? {
                        if best.as_ref().map_or(true, |(bs, _)| s > *bs) {
                            best = Some((s, v));
                        }
                    }
                }
            } else {
                // L1+ is non-overlapping and sorted by min_key.
                let idx = level.partition_point(|t| t.max_key() < key);
                if idx < level.len() && level[idx].min_key() <= key {
                    best = t_get(&level[idx], key, snapshot)?;
                }
            }
            if let Some((_, v)) = best {
                return Ok(v); // shallower level == newer; search ends here
            }
        }
        Ok(None)
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut reg = self.snapshots.lock().unwrap();
        *reg.entry(self.seq).or_insert(0) += 1;
        Snapshot {
            seq: self.seq,
            registry: Arc::clone(&self.snapshots),
        }
    }

    pub fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_at(start, end, limit, self.seq)
    }

    /// Range scan visible at `snapshot`. Merges memtable and every table.
    pub fn scan_at(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        snapshot: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut sources: Vec<Box<dyn Iterator<Item = Entry> + '_>> = Vec::new();
        sources.push(Box::new(self.mem.iter()));
        for level in &self.levels {
            for t in level {
                sources.push(Box::new(t.iter()));
            }
        }
        let visible = VisibleIter::new(MergeIter::new(sources), snapshot);
        let mut out = Vec::new();
        for (k, v) in visible {
            if k.as_slice() < start {
                continue;
            }
            if let Some(e) = end {
                if k.as_slice() >= e {
                    break;
                }
            }
            if let Some(v) = v {
                out.push((k, v));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------ flush/compact

    fn maybe_flush(&mut self) -> Result<()> {
        if self.mem.approx_bytes() >= self.opts.memtable_bytes {
            self.flush()?;
            self.maybe_compact()?;
        }
        Ok(())
    }

    /// Write the memtable out as an L0 SSTable, then reset the WAL.
    /// Ordering matters: table fsync → manifest update → WAL reset. A crash
    /// between any two steps leaves either duplicate-but-idempotent data
    /// (table + WAL both hold the ops) — never lost data.
    pub fn flush(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let id = self.manifest.next_table_id;
        let path = table_path(&self.dir, id);
        let mut b = TableBuilder::create(&path, self.opts.block_bytes, self.opts.bits_per_key)?;
        for (k, s, v) in self.mem.iter() {
            b.add(&k, s, v.as_deref())?;
        }
        let (entries, bytes, min_key, max_key) = b.finish()?;
        let table = SsTable::open(&path, id)?;

        self.manifest.next_table_id += 1;
        self.manifest.last_seq = self.seq;
        if self.manifest.levels.is_empty() {
            self.manifest.levels.push(Vec::new());
        }
        self.manifest.levels[0].push(TableMeta {
            id,
            min_key,
            max_key,
            entries,
            bytes,
        });
        self.manifest.save(&self.dir)?;

        self.levels[0].push(table);
        self.mem = MemTable::new();
        self.wal.reset()?;
        Ok(())
    }

    fn level_bytes(&self, i: usize) -> u64 {
        self.manifest
            .levels
            .get(i)
            .map(|l| l.iter().map(|t| t.bytes).sum())
            .unwrap_or(0)
    }

    fn level_limit(&self, i: usize) -> u64 {
        self.opts.level_base_bytes * self.opts.level_multiplier.pow((i as u32).saturating_sub(1))
    }

    pub fn maybe_compact(&mut self) -> Result<()> {
        loop {
            if self.levels[0].len() >= self.opts.l0_trigger {
                self.compact_level(0)?;
                continue;
            }
            let mut compacted = false;
            for i in 1..self.levels.len() {
                if self.level_bytes(i) > self.level_limit(i) {
                    self.compact_level(i)?;
                    compacted = true;
                    break;
                }
            }
            if !compacted {
                return Ok(());
            }
        }
    }

    /// Merge tables from `level` with overlapping tables in `level + 1`.
    /// L0 compacts *all* its tables (they overlap each other); deeper levels
    /// compact their oldest table. GC rules: keep every version newer than
    /// the oldest active snapshot, plus the newest version at-or-below it
    /// (the baseline); drop older versions; drop baseline tombstones only at
    /// the bottom of the tree (below it nothing can be resurrected).
    fn compact_level(&mut self, level: usize) -> Result<()> {
        while self.levels.len() <= level + 1 {
            self.levels.push(Vec::new());
        }
        while self.manifest.levels.len() <= level + 1 {
            self.manifest.levels.push(Vec::new());
        }

        let src_ids: Vec<u64> = if level == 0 {
            self.levels[0].iter().map(|t| t.id).collect()
        } else {
            match self.levels[level].iter().map(|t| t.id).min() {
                Some(id) => vec![id],
                None => return Ok(()),
            }
        };
        if src_ids.is_empty() {
            return Ok(());
        }

        // Key range of the inputs → which next-level tables overlap.
        let mut min_k: Option<Vec<u8>> = None;
        let mut max_k: Option<Vec<u8>> = None;
        for t in &self.levels[level] {
            if src_ids.contains(&t.id) {
                if min_k.as_deref().map_or(true, |m| t.min_key() < m) {
                    min_k = Some(t.min_key().to_vec());
                }
                if max_k.as_deref().map_or(true, |m| t.max_key() > m) {
                    max_k = Some(t.max_key().to_vec());
                }
            }
        }
        let (min_k, max_k) = (min_k.unwrap(), max_k.unwrap());
        let dst_ids: Vec<u64> = self.levels[level + 1]
            .iter()
            .filter(|t| !(t.max_key() < min_k.as_slice() || t.min_key() > max_k.as_slice()))
            .map(|t| t.id)
            .collect();

        let min_snap = self
            .snapshots
            .lock()
            .unwrap()
            .keys()
            .next()
            .copied()
            .unwrap_or(self.seq);
        let bottom = (level + 2..self.levels.len()).all(|l| self.levels[l].is_empty());

        let mut next_id = self.manifest.next_table_id;
        let mut outputs: Vec<TableMeta> = Vec::new();

        {
            let mut sources: Vec<Box<dyn Iterator<Item = Entry> + '_>> = Vec::new();
            for t in &self.levels[level] {
                if src_ids.contains(&t.id) {
                    sources.push(Box::new(t.iter()));
                }
            }
            for t in &self.levels[level + 1] {
                if dst_ids.contains(&t.id) {
                    sources.push(Box::new(t.iter()));
                }
            }
            let merged = MergeIter::new(sources);

            let mut builder: Option<TableBuilder> = None;
            let mut builder_id = 0u64;
            let mut out_bytes = 0u64;
            let mut cur_key: Option<Vec<u8>> = None;
            let mut kept_baseline = false;
            let mut last_written: Option<(Vec<u8>, u64)> = None;

            for (k, s, v) in merged {
                let new_key = cur_key.as_deref() != Some(k.as_slice());
                if new_key {
                    cur_key = Some(k.clone());
                    kept_baseline = false;
                    // Rotate output tables only at user-key boundaries so one
                    // key's versions never straddle two tables (keeps L1+
                    // strictly non-overlapping).
                    if out_bytes >= self.opts.target_table_bytes {
                        if let Some(b) = builder.take() {
                            let (entries, bytes, mn, mx) = b.finish()?;
                            outputs.push(TableMeta {
                                id: builder_id,
                                min_key: mn,
                                max_key: mx,
                                entries,
                                bytes,
                            });
                        }
                        out_bytes = 0;
                    }
                }
                // Deduplicate identical (key, seq) versions (possible after
                // WAL replay overlapped a completed flush).
                if last_written.as_ref() == Some(&(k.clone(), s)) {
                    continue;
                }
                if s <= min_snap {
                    if kept_baseline {
                        continue; // shadowed below every live snapshot
                    }
                    kept_baseline = true;
                    if v.is_none() && bottom {
                        continue; // baseline tombstone at the bottom: gone for good
                    }
                }
                if builder.is_none() {
                    builder_id = next_id;
                    next_id += 1;
                    builder = Some(TableBuilder::create(
                        &table_path(&self.dir, builder_id),
                        self.opts.block_bytes,
                        self.opts.bits_per_key,
                    )?);
                }
                out_bytes += (k.len() + v.as_ref().map_or(0, |v| v.len()) + 17) as u64;
                builder.as_mut().unwrap().add(&k, s, v.as_deref())?;
                last_written = Some((k, s));
            }
            if let Some(b) = builder.take() {
                let (entries, bytes, mn, mx) = b.finish()?;
                outputs.push(TableMeta {
                    id: builder_id,
                    min_key: mn,
                    max_key: mx,
                    entries,
                    bytes,
                });
            }
        }

        // Swap the new tables in: manifest first (durable), then in-memory
        // state, then delete the dead files.
        self.manifest.next_table_id = next_id;
        self.manifest.levels[level].retain(|m| !src_ids.contains(&m.id));
        self.manifest.levels[level + 1].retain(|m| !dst_ids.contains(&m.id));
        for meta in &outputs {
            self.manifest.levels[level + 1].push(meta.clone());
        }
        self.manifest.levels[level + 1].sort_by(|a, b| a.min_key.cmp(&b.min_key));
        self.manifest.save(&self.dir)?;

        self.levels[level].retain(|t| !src_ids.contains(&t.id));
        self.levels[level + 1].retain(|t| !dst_ids.contains(&t.id));
        for meta in &outputs {
            self.levels[level + 1].push(SsTable::open(&table_path(&self.dir, meta.id), meta.id)?);
        }
        self.levels[level + 1].sort_by(|a, b| a.min_key().cmp(b.min_key()));

        for id in src_ids.iter().chain(dst_ids.iter()) {
            let _ = std::fs::remove_file(table_path(&self.dir, *id));
        }
        Ok(())
    }

    /// (level, table_count) pairs — for tests and stats endpoints.
    pub fn level_table_counts(&self) -> Vec<(usize, usize)> {
        self.levels
            .iter()
            .enumerate()
            .map(|(i, l)| (i, l.len()))
            .collect()
    }
}

fn t_get(t: &SsTable, key: &[u8], snapshot: u64) -> Result<Option<(u64, Option<Vec<u8>>)>> {
    t.get(key, snapshot)
}
