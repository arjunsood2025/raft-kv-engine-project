//! Write-ahead log.
//!
//! Frame format: `[len: u32 LE][crc32(payload): u32 LE][payload]` where the
//! payload is a bincode-encoded `WalBatch`. Recovery reads frames until EOF,
//! a short frame, or a CRC mismatch — everything after the last valid frame
//! is a torn write and is truncated away. This gives *prefix durability*:
//! an acknowledged (fsynced) batch is never lost, and a torn batch is never
//! half-applied.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

/// When to fsync the log.
/// `Always`: fsync before acknowledging every batch — the durable default,
/// and what Raft requires before a node acks an AppendEntries.
/// `Never`: rely on OS flush; loses recent writes on power failure. Exists so
/// benchmarks can quantify exactly what that fsync costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    Always,
    Never,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalBatch {
    /// Sequence number assigned to ops[0]; ops[i] has seq first_seq + i.
    pub first_seq: u64,
    /// (key, value); value None = tombstone.
    pub ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

pub struct Wal {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    pub policy: SyncPolicy,
}

impl Wal {
    pub fn open(path: &Path, policy: SyncPolicy) -> Result<Self> {
        // Not append-mode: Windows append handles lack FILE_WRITE_DATA, which
        // makes the set_len(0) in reset() fail with Access Denied. A plain
        // write handle seeked to the end behaves identically for our
        // single-writer log.
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        file.seek(std::io::SeekFrom::End(0))?;
        Ok(Wal {
            file,
            path: path.to_path_buf(),
            policy,
        })
    }

    pub fn append(&mut self, batch: &WalBatch) -> Result<()> {
        let payload =
            bincode::serialize(batch).map_err(|e| Error::Corruption(e.to_string()))?;
        let crc = crc32fast::hash(&payload);
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);
        self.file.write_all(&frame)?;
        if self.policy == SyncPolicy::Always {
            self.file.sync_data()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Truncate the log to empty (called after a memtable flush has made the
    /// contents durable in an SSTable).
    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Replay the valid prefix of the log; truncate any torn tail in place.
    pub fn replay(path: &Path) -> Result<Vec<WalBatch>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut data = Vec::new();
        File::open(path)?.read_to_end(&mut data)?;

        let mut batches = Vec::new();
        let mut off = 0usize;
        loop {
            if off + 8 > data.len() {
                break;
            }
            let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(data[off + 4..off + 8].try_into().unwrap());
            if off + 8 + len > data.len() {
                break; // torn: frame promised more bytes than exist
            }
            let payload = &data[off + 8..off + 8 + len];
            if crc32fast::hash(payload) != crc {
                break; // torn or bit-rotted frame
            }
            match bincode::deserialize::<WalBatch>(payload) {
                Ok(b) => batches.push(b),
                Err(_) => break,
            }
            off += 8 + len;
        }

        if off < data.len() {
            // Truncate the torn tail so future appends land on a clean prefix.
            let f = OpenOptions::new().write(true).open(path)?;
            f.set_len(off as u64)?;
            f.sync_all()?;
        }
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_replay_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        for i in 0..10u64 {
            wal.append(&WalBatch {
                first_seq: i * 2 + 1,
                ops: vec![(vec![i as u8], Some(vec![i as u8, 1]))],
            })
            .unwrap();
        }
        drop(wal);
        let batches = Wal::replay(&path).unwrap();
        assert_eq!(batches.len(), 10);
        assert_eq!(batches[3].first_seq, 7);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        for i in 0..5u64 {
            wal.append(&WalBatch {
                first_seq: i,
                ops: vec![(vec![i as u8], Some(vec![1, 2, 3]))],
            })
            .unwrap();
        }
        drop(wal);

        // Corrupt: chop bytes off the tail to simulate a torn final write.
        let full = std::fs::read(&path).unwrap();
        std::fs::write(&path, &full[..full.len() - 3]).unwrap();

        let batches = Wal::replay(&path).unwrap();
        assert_eq!(batches.len(), 4, "torn last frame must be dropped");

        // After truncation, appends resume cleanly.
        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&WalBatch {
            first_seq: 99,
            ops: vec![(b"k".to_vec(), None)],
        })
        .unwrap();
        drop(wal);
        let batches = Wal::replay(&path).unwrap();
        assert_eq!(batches.len(), 5);
        assert_eq!(batches[4].first_seq, 99);
    }
}
