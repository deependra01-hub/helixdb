use crate::{Db, DbError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccVersion {
    pub timestamp: u64,
    pub value: Option<Vec<u8>>,
}

pub struct MvccDb {
    storage: Db,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Snapshot {
    timestamp: u64,
}

impl MvccDb {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            storage: Db::open(path)?,
        })
    }

    pub fn put_at(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl Into<Vec<u8>>,
        timestamp: u64,
    ) -> Result<()> {
        let internal_key = encode_internal_key(key.as_ref(), timestamp);
        self.storage.put(internal_key, value.into())?;
        Ok(())
    }

    pub fn delete_at(&mut self, key: impl AsRef<[u8]>, timestamp: u64) -> Result<()> {
        let internal_key = encode_internal_key(key.as_ref(), timestamp);
        self.storage.delete(internal_key)?;
        Ok(())
    }

    pub fn get_at(&self, key: impl AsRef<[u8]>, timestamp: u64) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let mut best: Option<MvccVersion> = None;

        for (internal_key, value) in self.storage.all_entries()? {
            let (user_key, version_ts) = match decode_internal_key(&internal_key) {
                Ok(decoded) => decoded,
                Err(_) => continue,
            };

            if user_key.as_slice() != key || version_ts > timestamp {
                continue;
            }

            let replace = match &best {
                Some(current) => version_ts > current.timestamp,
                None => true,
            };

            if replace {
                best = Some(MvccVersion {
                    timestamp: version_ts,
                    value: value.value,
                });
            }
        }

        Ok(best.and_then(|version| version.value))
    }

    pub fn snapshot(&self, timestamp: u64) -> Snapshot {
        Snapshot { timestamp }
    }

    pub fn gc(&mut self, safe_point: u64) -> Result<usize> {
        let mut survivors = Vec::new();
        let mut removed = 0usize;

        for (internal_key, value) in self.storage.all_entries()? {
            let (_, version_ts) = match decode_internal_key(&internal_key) {
                Ok(decoded) => decoded,
                Err(_) => continue,
            };

            if version_ts >= safe_point {
                survivors.push((internal_key, value));
            } else {
                removed += 1;
            }
        }

        self.storage.rewrite_from_entries(survivors)?;
        Ok(removed)
    }

    pub fn versions_for_key(&self, key: impl AsRef<[u8]>) -> Result<Vec<MvccVersion>> {
        let key = key.as_ref();
        let mut versions = Vec::new();

        for (internal_key, value) in self.storage.all_entries()? {
            let (user_key, version_ts) = match decode_internal_key(&internal_key) {
                Ok(decoded) => decoded,
                Err(_) => continue,
            };

            if user_key.as_slice() == key {
                versions.push(MvccVersion {
                    timestamp: version_ts,
                    value: value.value,
                });
            }
        }

        versions.sort_by_key(|version| version.timestamp);
        Ok(versions)
    }

    pub fn max_timestamp(&self) -> Result<u64> {
        let mut max_timestamp = 0u64;
        for (internal_key, _) in self.storage.all_entries()? {
            if let Ok((_, timestamp)) = decode_internal_key(&internal_key) {
                max_timestamp = max_timestamp.max(timestamp);
            }
        }
        Ok(max_timestamp)
    }
}

impl Snapshot {
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

fn encode_internal_key(key: &[u8], timestamp: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&timestamp.to_le_bytes());
    out
}

pub(crate) fn decode_internal_key(bytes: &[u8]) -> Result<(Vec<u8>, u64)> {
    if bytes.len() < 12 {
        return Err(DbError::Corrupt("internal mvcc key too short".into()));
    }

    let key_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let required = 4usize
        .checked_add(key_len)
        .and_then(|value| value.checked_add(8))
        .ok_or_else(|| DbError::Corrupt("internal mvcc key length overflow".into()))?;

    if bytes.len() != required {
        return Err(DbError::Corrupt("internal mvcc key length mismatch".into()));
    }

    let key = bytes[4..4 + key_len].to_vec();
    let timestamp = u64::from_le_bytes(bytes[4 + key_len..required].try_into().unwrap());
    Ok((key, timestamp))
}
