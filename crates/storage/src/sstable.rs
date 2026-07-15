//! SSTable: immutable sorted table on disk.
//!
//! Layout:
//! ```text
//! [data block 0][data block 1]...[data block N]
//! [index payload: bincode(TableIndex)]
//! [footer: index_off u64 | index_len u32 | index_crc u32 | MAGIC u64]
//! ```
//! Each data block is ~`block_bytes` of entries encoded as
//! `[klen u32][key][seq u64][kind u8][vlen u32][value]` (kind 1 = put,
//! 0 = tombstone). Entries are sorted (key ASC, seq DESC). The index holds
//! the first key + offset + length + CRC of every block (a sparse index: one
//! entry per block, not per key), plus a bloom filter over user keys.
//! Every block and the index are CRC-checked on read.

use crate::bloom::{fnv1a, BloomFilter};
use crate::memtable::Entry;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const MAGIC: u64 = 0x7266_744b_5653_5354; // "rftKVSST"

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockMeta {
    pub first_key: Vec<u8>,
    pub offset: u64,
    pub len: u32,
    pub crc: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TableIndex {
    pub blocks: Vec<BlockMeta>,
    pub bloom: Vec<u8>,
    pub entry_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub max_seq: u64,
}

// ---------------------------------------------------------------- builder

pub struct TableBuilder {
    file: File,
    path: PathBuf,
    block: Vec<u8>,
    block_first_key: Option<Vec<u8>>,
    blocks: Vec<BlockMeta>,
    offset: u64,
    key_hashes: Vec<u64>,
    last_user_key: Option<Vec<u8>>,
    entry_count: u64,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    max_seq: u64,
    block_bytes: usize,
    bits_per_key: usize,
}

impl TableBuilder {
    pub fn create(path: &Path, block_bytes: usize, bits_per_key: usize) -> Result<Self> {
        let file = File::create(path)?;
        Ok(TableBuilder {
            file,
            path: path.to_path_buf(),
            block: Vec::with_capacity(block_bytes + 512),
            block_first_key: None,
            blocks: Vec::new(),
            offset: 0,
            key_hashes: Vec::new(),
            last_user_key: None,
            entry_count: 0,
            min_key: None,
            max_key: None,
            max_seq: 0,
            block_bytes,
            bits_per_key,
        })
    }

    /// Entries MUST be added in (key ASC, seq DESC) order.
    pub fn add(&mut self, key: &[u8], seq: u64, value: Option<&[u8]>) -> Result<()> {
        if self.block_first_key.is_none() {
            self.block_first_key = Some(key.to_vec());
        }
        if self.last_user_key.as_deref() != Some(key) {
            // One bloom hash per distinct user key.
            self.key_hashes.push(fnv1a(key));
            self.last_user_key = Some(key.to_vec());
        }
        if self.min_key.is_none() {
            self.min_key = Some(key.to_vec());
        }
        self.max_key = Some(key.to_vec());
        self.max_seq = self.max_seq.max(seq);
        self.entry_count += 1;

        self.block
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.block.extend_from_slice(key);
        self.block.extend_from_slice(&seq.to_le_bytes());
        match value {
            Some(v) => {
                self.block.push(1);
                self.block.extend_from_slice(&(v.len() as u32).to_le_bytes());
                self.block.extend_from_slice(v);
            }
            None => {
                self.block.push(0);
                self.block.extend_from_slice(&0u32.to_le_bytes());
            }
        }

        if self.block.len() >= self.block_bytes {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let crc = crc32fast::hash(&self.block);
        self.file.write_all(&self.block)?;
        self.blocks.push(BlockMeta {
            first_key: self.block_first_key.take().unwrap(),
            offset: self.offset,
            len: self.block.len() as u32,
            crc,
        });
        self.offset += self.block.len() as u64;
        self.block.clear();
        Ok(())
    }

    /// Finish the table; returns (entry_count, file_bytes, min_key, max_key).
    pub fn finish(mut self) -> Result<(u64, u64, Vec<u8>, Vec<u8>)> {
        self.flush_block()?;
        let bloom = BloomFilter::from_hashes(&self.key_hashes, self.bits_per_key);
        let index = TableIndex {
            blocks: std::mem::take(&mut self.blocks),
            bloom: bloom.encode(),
            entry_count: self.entry_count,
            min_key: self.min_key.clone().unwrap_or_default(),
            max_key: self.max_key.clone().unwrap_or_default(),
            max_seq: self.max_seq,
        };
        let payload =
            bincode::serialize(&index).map_err(|e| Error::Corruption(e.to_string()))?;
        let index_off = self.offset;
        self.file.write_all(&payload)?;
        let mut footer = Vec::with_capacity(24);
        footer.extend_from_slice(&index_off.to_le_bytes());
        footer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        footer.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        self.file.write_all(&footer)?;
        // fsync before the table is referenced by the manifest: a table the
        // manifest points at must be fully durable.
        self.file.sync_all()?;
        let bytes = index_off + payload.len() as u64 + 24;
        Ok((
            self.entry_count,
            bytes,
            index.min_key,
            index.max_key,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ---------------------------------------------------------------- reader

pub struct SsTable {
    pub id: u64,
    path: PathBuf,
    pub index: TableIndex,
    bloom: BloomFilter,
}

impl SsTable {
    pub fn open(path: &Path, id: u64) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < 24 {
            return Err(Error::Corruption(format!("sstable {path:?} too short")));
        }
        file.seek(SeekFrom::End(-24))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)?;
        let magic = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::Corruption(format!("bad magic in {path:?}")));
        }
        let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u32::from_le_bytes(footer[8..12].try_into().unwrap()) as usize;
        let index_crc = u32::from_le_bytes(footer[12..16].try_into().unwrap());
        file.seek(SeekFrom::Start(index_off))?;
        let mut payload = vec![0u8; index_len];
        file.read_exact(&mut payload)?;
        if crc32fast::hash(&payload) != index_crc {
            return Err(Error::Corruption(format!("index crc mismatch in {path:?}")));
        }
        let index: TableIndex =
            bincode::deserialize(&payload).map_err(|e| Error::Corruption(e.to_string()))?;
        let bloom = BloomFilter::decode(&index.bloom)?;
        Ok(SsTable {
            id,
            path: path.to_path_buf(),
            index,
            bloom,
        })
    }

    pub fn min_key(&self) -> &[u8] {
        &self.index.min_key
    }

    pub fn max_key(&self) -> &[u8] {
        &self.index.max_key
    }

    fn read_block(&self, i: usize) -> Result<Vec<Entry>> {
        let meta = &self.index.blocks[i];
        // Open-per-read keeps the reader stateless; the OS page cache absorbs
        // most of the cost. A production engine would pool handles and add a
        // block cache.
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(meta.offset))?;
        let mut buf = vec![0u8; meta.len as usize];
        file.read_exact(&mut buf)?;
        if crc32fast::hash(&buf) != meta.crc {
            return Err(Error::Corruption(format!(
                "block {i} crc mismatch in {:?}",
                self.path
            )));
        }
        decode_block(&buf)
    }

    /// Newest version of `key` with seq <= snapshot within this table.
    /// Returns (seq, value) so callers can pick the newest hit across tables.
    pub fn get(&self, key: &[u8], snapshot: u64) -> Result<Option<(u64, Option<Vec<u8>>)>> {
        if key < self.min_key() || key > self.max_key() {
            return Ok(None);
        }
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        // Start at the last block whose first_key < key. If a block's
        // first_key == key, the key's NEWEST versions may still sit at the
        // end of the previous block (entries are seq-DESC within a key), so
        // an exact match must not skip that block.
        let mut idx = self
            .index
            .blocks
            .partition_point(|b| b.first_key.as_slice() < key);
        idx = idx.saturating_sub(1);
        // Versions of one key can straddle a block boundary; keep scanning
        // while the next block still starts at (i.e., continues) this key.
        loop {
            let entries = self.read_block(idx)?;
            for (k, seq, v) in entries {
                match k.as_slice().cmp(key) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => {
                        if seq <= snapshot {
                            return Ok(Some((seq, v)));
                        }
                    }
                    std::cmp::Ordering::Greater => return Ok(None),
                }
            }
            idx += 1;
            if idx >= self.index.blocks.len()
                || self.index.blocks[idx].first_key.as_slice() > key
            {
                return Ok(None);
            }
        }
    }

    pub fn iter(&self) -> TableIter<'_> {
        TableIter {
            table: self,
            block_idx: 0,
            entries: Vec::new().into_iter(),
        }
    }
}

fn decode_block(buf: &[u8]) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off < buf.len() {
        let need = |n: usize, off: usize| -> Result<()> {
            if off + n > buf.len() {
                Err(Error::Corruption("truncated block entry".into()))
            } else {
                Ok(())
            }
        };
        need(4, off)?;
        let klen = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        need(klen + 9, off)?;
        let key = buf[off..off + klen].to_vec();
        off += klen;
        let seq = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        off += 8;
        let kind = buf[off];
        off += 1;
        need(4, off)?;
        let vlen = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        let value = if kind == 1 {
            need(vlen, off)?;
            let v = buf[off..off + vlen].to_vec();
            off += vlen;
            Some(v)
        } else {
            None
        };
        out.push((key, seq, value));
    }
    Ok(out)
}

pub struct TableIter<'a> {
    table: &'a SsTable,
    block_idx: usize,
    entries: std::vec::IntoIter<Entry>,
}

impl Iterator for TableIter<'_> {
    type Item = Entry;
    fn next(&mut self) -> Option<Entry> {
        loop {
            if let Some(e) = self.entries.next() {
                return Some(e);
            }
            if self.block_idx >= self.table.index.blocks.len() {
                return None;
            }
            // Corruption mid-iteration (compaction path) is fatal by design
            // in this build; production would thread Result through.
            let block = self
                .table
                .read_block(self.block_idx)
                .expect("sstable block read during iteration");
            self.block_idx += 1;
            self.entries = block.into_iter();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.sst");
        let mut b = TableBuilder::create(&path, 256, 10).unwrap();
        // 100 keys, 2 versions each, plus a tombstone on key 50.
        for i in 0..100u32 {
            let key = format!("key{:04}", i).into_bytes();
            if i == 50 {
                b.add(&key, 300, None).unwrap();
            }
            b.add(&key, 200 + i as u64 % 7, Some(format!("v2-{i}").as_bytes()))
                .unwrap();
            b.add(&key, 100, Some(format!("v1-{i}").as_bytes())).unwrap();
        }
        let (count, bytes, min, max) = b.finish().unwrap();
        assert_eq!(count, 201);
        assert!(bytes > 0);
        assert_eq!(min, b"key0000".to_vec());
        assert_eq!(max, b"key0099".to_vec());

        let t = SsTable::open(&path, 1).unwrap();
        assert!(t.index.blocks.len() > 1, "should span multiple blocks");

        // Latest visible.
        let (seq, v) = t.get(b"key0003", u64::MAX).unwrap().unwrap();
        assert_eq!(v, Some(b"v2-3".to_vec()));
        assert!(seq >= 200);
        // Snapshot in the past sees the old version.
        let (seq, v) = t.get(b"key0003", 150).unwrap().unwrap();
        assert_eq!((seq, v), (100, Some(b"v1-3".to_vec())));
        // Tombstone is a *found* deletion, not a miss.
        let (_, v) = t.get(b"key0050", u64::MAX).unwrap().unwrap();
        assert_eq!(v, None);
        // Absent key.
        assert!(t.get(b"nope", u64::MAX).unwrap().is_none());

        // Full iteration preserves order and count.
        let all: Vec<_> = t.iter().collect();
        assert_eq!(all.len(), 201);
        let mut sorted = all.clone();
        sorted.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        assert_eq!(all, sorted);
    }

    #[test]
    fn corrupt_block_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let mut b = TableBuilder::create(&path, 4096, 10).unwrap();
        for i in 0..50u32 {
            b.add(format!("k{i:03}").as_bytes(), i as u64 + 1, Some(b"v"))
                .unwrap();
        }
        b.finish().unwrap();

        // Flip a byte inside the first data block.
        let mut data = std::fs::read(&path).unwrap();
        data[10] ^= 0xff;
        std::fs::write(&path, &data).unwrap();

        let t = SsTable::open(&path, 1).unwrap();
        let err = t.get(b"k000", u64::MAX).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }
}
