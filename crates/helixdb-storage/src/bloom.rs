use crc32fast::Hasher;

use crate::{DbError, Result};

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    bit_count: usize,
    hash_functions: u32,
}

impl BloomFilter {
    pub fn empty() -> Self {
        Self {
            bits: Vec::new(),
            bit_count: 0,
            hash_functions: 7,
        }
    }

    pub fn from_keys<I, K>(keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(|k| k.as_ref().to_vec()).collect();
        if keys.is_empty() {
            return Self::empty();
        }

        let bit_count = (keys.len() * 10).max(64);
        let byte_len = (bit_count + 7) / 8;
        let mut filter = Self {
            bits: vec![0; byte_len],
            bit_count,
            hash_functions: 7,
        };

        for key in keys {
            filter.insert(&key);
        }

        filter
    }

    pub fn insert(&mut self, key: impl AsRef<[u8]>) {
        if self.bit_count == 0 {
            return;
        }

        let key = key.as_ref();
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.hash_functions {
            let idx =
                ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.bit_count as u64) as usize;
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }

    pub fn might_contain(&self, key: impl AsRef<[u8]>) -> bool {
        if self.bit_count == 0 {
            return false;
        }

        let key = key.as_ref();
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.hash_functions {
            let idx =
                ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.bit_count as u64) as usize;
            if self.bits[idx / 8] & (1 << (idx % 8)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.bits.len());
        out.extend_from_slice(&(self.bit_count as u64).to_le_bytes());
        out.extend_from_slice(&self.hash_functions.to_le_bytes());
        out.extend_from_slice(&self.bits);
        let mut hasher = Hasher::new();
        hasher.update(&out);
        out.extend_from_slice(&hasher.finalize().to_le_bytes());
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(DbError::Corrupt("bloom filter too short".into()));
        }

        let payload_len = bytes.len() - 4;
        let expected = u32::from_le_bytes(bytes[payload_len..].try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(&bytes[..payload_len]);
        if hasher.finalize() != expected {
            return Err(DbError::ChecksumMismatch("bloom filter"));
        }

        let bit_count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let hash_functions = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let bits = bytes[12..payload_len].to_vec();
        if bits.len() * 8 < bit_count {
            return Err(DbError::Corrupt("bloom filter length mismatch".into()));
        }
        Ok(Self {
            bits,
            bit_count,
            hash_functions,
        })
    }

    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        use std::hash::{Hash, Hasher as StdHasher};

        let mut h1 = std::collections::hash_map::DefaultHasher::new();
        0x9E3779B97F4A7C15u64.hash(&mut h1);
        key.hash(&mut h1);
        let a = h1.finish();

        let mut h2 = std::collections::hash_map::DefaultHasher::new();
        0xC2B2AE3D27D4EB4Fu64.hash(&mut h2);
        key.hash(&mut h2);
        let b = h2.finish() | 1;
        (a, b)
    }
}
