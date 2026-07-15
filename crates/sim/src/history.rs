//! Client operation history recorded during a simulated run.
//!
//! Timestamps are a global monotone counter (the sim is single-threaded, so
//! the counter is a total order consistent with virtual time). An op with
//! `ret == None` never received a response before the run ended — it may or
//! may not have taken effect, and the checker must allow both.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HOp {
    Read { key: u8 },
    Write { key: u8, val: u64 },
    Cas { key: u8, expect: Option<u64>, new: u64 },
}

impl HOp {
    pub fn key(&self) -> u8 {
        match self {
            HOp::Read { key } | HOp::Write { key, .. } | HOp::Cas { key, .. } => *key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HRes {
    ReadOk(Option<u64>),
    WriteOk,
    CasOk { success: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HRecord {
    pub client: u64,
    pub invoke: u64,
    pub ret: Option<u64>,
    pub op: HOp,
    pub result: Option<HRes>,
}

pub fn encode_val(v: u64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

pub fn decode_val(bytes: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b[..bytes.len().min(8)].copy_from_slice(&bytes[..bytes.len().min(8)]);
    u64::from_le_bytes(b)
}
