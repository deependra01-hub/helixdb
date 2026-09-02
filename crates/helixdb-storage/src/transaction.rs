use crate::{DbError, MvccDb, Result};

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MvccTransactionError {
    #[error("database error: {0}")]
    Db(#[from] DbError),
    #[error("write conflict on key {key:?}: observed version {observed_ts} after start_ts {start_ts}")]
    WriteConflict {
        key: Vec<u8>,
        observed_ts: u64,
        start_ts: u64,
    },
}

#[derive(Clone)]
pub struct TransactionalMvccDb {
    storage: Arc<RwLock<MvccDb>>,
    clock: Arc<AtomicU64>,
}

pub struct MvccTransaction {
    storage: Arc<RwLock<MvccDb>>,
    clock: Arc<AtomicU64>,
    start_ts: u64,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl TransactionalMvccDb {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let storage = MvccDb::open(path)?;
        let next_timestamp = storage.max_timestamp()?.saturating_add(1);
        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
            clock: Arc::new(AtomicU64::new(next_timestamp)),
        })
    }

    pub fn begin_transaction(&self) -> MvccTransaction {
        let start_ts = self.clock.fetch_add(1, Ordering::SeqCst);
        MvccTransaction {
            storage: Arc::clone(&self.storage),
            clock: Arc::clone(&self.clock),
            start_ts,
            writes: BTreeMap::new(),
        }
    }

    pub fn read_at(&self, key: impl AsRef<[u8]>, timestamp: u64) -> Result<Option<Vec<u8>>> {
        self.storage.read().unwrap().get_at(key, timestamp)
    }
}

impl MvccTransaction {
    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        if let Some(value) = self.writes.get(key) {
            return Ok(value.clone());
        }

        self.storage.read().unwrap().get_at(key, self.start_ts)
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.writes.insert(key.into(), Some(value.into()));
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.writes.insert(key.into(), None);
    }

    pub fn rollback(self) {}

    pub fn commit(self) -> std::result::Result<u64, MvccTransactionError> {
        let commit_ts = self.clock.fetch_add(1, Ordering::SeqCst);
        let mut storage = self.storage.write().unwrap();

        for key in self.writes.keys() {
            let latest = latest_committed_timestamp(&storage, key)?;
            if latest > self.start_ts {
                return Err(MvccTransactionError::WriteConflict {
                    key: key.clone(),
                    observed_ts: latest,
                    start_ts: self.start_ts,
                });
            }
        }

        for (key, value) in self.writes {
            match value {
                Some(bytes) => storage.put_at(key, bytes, commit_ts)?,
                None => storage.delete_at(key, commit_ts)?,
            }
        }

        Ok(commit_ts)
    }
}

fn latest_committed_timestamp(db: &MvccDb, key: &[u8]) -> Result<u64> {
    let mut latest = 0u64;
    for version in db.versions_for_key(key)? {
        latest = latest.max(version.timestamp);
    }
    Ok(latest)
}
