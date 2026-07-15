//! Bloom filter used per-SSTable to skip tables that cannot contain a key.
//!
//! Uses double hashing (Kirsch–Mitzenmacher): two 64-bit hashes h1, h2 derived
//! from one FNV-1a pass, probe positions g_i = h1 + i*h2. We store our own
//! hash rather than `std::hash` because `DefaultHasher` is not stable across
//! processes and the filter is persisted to disk.

use crate::{Error, Result};

pub fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn mix(mut x: u64) -> u64 {
    // splitmix64 finalizer — decorrelates h2 from h1.
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[derive(Clone)]
pub struct BloomFilter {
    k: u32,
    bits: Vec<u8>,
}

impl BloomFilter {
    /// Build a filter from pre-computed key hashes (one fnv1a per user key).
    /// Building from hashes lets the SSTable writer stream entries without
    /// buffering keys just for the filter.
    pub fn from_hashes(hashes: &[u64], bits_per_key: usize) -> Self {
        let nbits = (hashes.len() * bits_per_key).max(64);
        let nbits = (nbits + 7) / 8 * 8;
        // k = ln(2) * bits_per_key is the false-positive-optimal probe count.
        let k = ((bits_per_key as f64 * 0.69) as u32).clamp(1, 30);
        let mut f = BloomFilter {
            k,
            bits: vec![0u8; nbits / 8],
        };
        for &h in hashes {
            f.add_hash(h);
        }
        f
    }

    fn add_hash(&mut self, h1: u64) {
        let nbits = (self.bits.len() * 8) as u64;
        let h2 = mix(h1) | 1;
        let mut pos = h1;
        for _ in 0..self.k {
            let bit = pos % nbits;
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
            pos = pos.wrapping_add(h2);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let nbits = (self.bits.len() * 8) as u64;
        let h1 = fnv1a(key);
        let h2 = mix(h1) | 1;
        let mut pos = h1;
        for _ in 0..self.k {
            let bit = pos % nbits;
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
            pos = pos.wrapping_add(h2);
        }
        true
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bits.len());
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 5 {
            return Err(Error::Corruption("bloom filter too short".into()));
        }
        let k = u32::from_le_bytes(data[0..4].try_into().unwrap());
        Ok(BloomFilter {
            k,
            bits: data[4..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000u32).map(|i| i.to_le_bytes().to_vec()).collect();
        let hashes: Vec<u64> = keys.iter().map(|k| fnv1a(k)).collect();
        let f = BloomFilter::from_hashes(&hashes, 10);
        for k in &keys {
            assert!(f.may_contain(k));
        }
        // False positive rate should be small at 10 bits/key (~1%).
        let mut fp = 0;
        for i in 1000..11000u32 {
            if f.may_contain(&i.to_le_bytes()) {
                fp += 1;
            }
        }
        assert!(fp < 500, "false positive rate too high: {fp}/10000");
    }

    #[test]
    fn encode_decode_roundtrip() {
        let hashes: Vec<u64> = (0..100u64).map(|i| fnv1a(&i.to_le_bytes())).collect();
        let f = BloomFilter::from_hashes(&hashes, 10);
        let f2 = BloomFilter::decode(&f.encode()).unwrap();
        for i in 0..100u64 {
            assert!(f2.may_contain(&i.to_le_bytes()));
        }
    }
}
