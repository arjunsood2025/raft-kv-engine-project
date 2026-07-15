//! Manifest: the durable catalog of which SSTables exist at which level.
//!
//! The whole manifest is rewritten atomically on every change (write tmp,
//! fsync, rename). At this project's table counts a full rewrite is cheap;
//! RocksDB-style incremental version edits are the production answer.
//! Note on Windows: `rename` onto an existing file fails, so we remove the
//! old file first — a crash in that window is healed by also trying the tmp
//! file on load.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const MANIFEST_NAME: &str = "MANIFEST";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableMeta {
    pub id: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Manifest {
    pub levels: Vec<Vec<TableMeta>>,
    pub next_table_id: u64,
    /// Highest sequence number made durable in SSTables (WAL replay resumes
    /// from here).
    pub last_seq: u64,
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_NAME)
}

fn tmp_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST.tmp")
}

impl Manifest {
    pub fn load(dir: &Path) -> Result<Option<Manifest>> {
        for path in [manifest_path(dir), tmp_path(dir)] {
            if !path.exists() {
                continue;
            }
            let data = fs::read(&path)?;
            if data.len() < 4 {
                continue;
            }
            let (payload, crc_bytes) = data.split_at(data.len() - 4);
            let crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
            if crc32fast::hash(payload) != crc {
                continue; // torn manifest write; try fallback
            }
            let m: Manifest = bincode::deserialize(payload)
                .map_err(|e| Error::Corruption(e.to_string()))?;
            return Ok(Some(m));
        }
        Ok(None)
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        let mut payload =
            bincode::serialize(self).map_err(|e| Error::Corruption(e.to_string()))?;
        let crc = crc32fast::hash(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());

        let tmp = tmp_path(dir);
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&payload)?;
            f.sync_all()?;
        }
        let dst = manifest_path(dir);
        if dst.exists() {
            fs::remove_file(&dst)?;
        }
        fs::rename(&tmp, &dst)?;
        Ok(())
    }
}
