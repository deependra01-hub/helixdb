mod bloom;
mod control_plane;
mod error;
mod manifest;
mod memtable;
mod mvcc;
mod raft;
mod sharding;
mod transaction;
mod sstable;
mod types;
mod wal;

pub use bloom::BloomFilter;
pub use control_plane::{
    ControlPlane, ControlPlaneConfig, ControlPlaneError, NodeRecord, NodeStatus, RangePlacement,
    TimestampBatch,
};
pub use error::{DbError, Result};
pub use manifest::{Manifest, SstableMeta};
pub use memtable::{ImmutableMemTable, MemTable};
pub use mvcc::{MvccDb, Snapshot};
pub use raft::{RaftCluster, RaftConfig, RaftNodeState, RaftRole};
pub use sharding::{RangeDescriptor, RangeRoutingError, ShardedCluster};
pub use sstable::{Sstable, SstableReader};
pub use types::{EntryKind, ValueEntry};
pub use transaction::{MvccTransaction, MvccTransactionError, TransactionalMvccDb};
pub use wal::{WalRecord, WalWriter};

use std::fs;
use std::path::{Path, PathBuf};

use manifest::ManifestState;

/// Phase 1 database options.
#[derive(Debug, Clone)]
pub struct DbOptions {
    pub memtable_flush_threshold: usize,
    pub sstable_block_size: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            memtable_flush_threshold: 4 * 1024 * 1024,
            sstable_block_size: 64 * 1024,
        }
    }
}

/// Embedded storage engine for Phase 1.
pub struct Db {
    dir: PathBuf,
    options: DbOptions,
    manifest: Manifest,
    wal: WalWriter,
    memtable: MemTable,
    sstables: Vec<SstableReader>,
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default())
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let manifest = Manifest::load_or_create(&dir)?;
        let wal_path = manifest.active_wal_path(&dir);
        let wal = if wal_path.exists() {
            WalWriter::open(&wal_path)?
        } else if manifest.sstables.is_empty() && manifest.next_sequence == 1 {
            WalWriter::create(&wal_path)?
        } else {
            return Err(DbError::Corrupt(format!(
                "missing active wal file: {}",
                wal_path.display()
            )));
        };

        let mut memtable = MemTable::default();
        let mut next_seq = manifest.next_sequence;
        wal.replay(|record| {
            next_seq = next_seq.max(record.sequence + 1);
            memtable.apply_record(record);
            Ok(())
        })?;

        let mut sstables = Vec::new();
        for meta in &manifest.sstables {
            sstables.push(SstableReader::open(&dir.join(&meta.file_name))?);
        }

        Ok(Self {
            dir,
            options,
            manifest: manifest.with_next_sequence(next_seq),
            wal,
            memtable,
            sstables,
        })
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<u64> {
        let key = key.into();
        let value = value.into();
        let seq = self.manifest.next_sequence;
        let record = WalRecord::put(seq, key.clone(), value.clone());
        self.wal.append(&record)?;
        self.memtable.insert(key, Some(value), seq);
        self.manifest.next_sequence += 1;

        if self.memtable.approximate_bytes() >= self.options.memtable_flush_threshold {
            self.flush()?;
        }

        Ok(seq)
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<u64> {
        let key = key.into();
        let seq = self.manifest.next_sequence;
        let record = WalRecord::delete(seq, key.clone());
        self.wal.append(&record)?;
        self.memtable.insert(key, None, seq);
        self.manifest.next_sequence += 1;

        if self.memtable.approximate_bytes() >= self.options.memtable_flush_threshold {
            self.flush()?;
        }

        Ok(seq)
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        if let Some(entry) = self.memtable.get(key) {
            return Ok(entry.value.clone());
        }

        for table in self.sstables.iter().rev() {
            if let Some(value) = table.get(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn all_entries(&self) -> Result<Vec<(Vec<u8>, ValueEntry)>> {
        let mut entries = std::collections::BTreeMap::<Vec<u8>, ValueEntry>::new();

        for table in &self.sstables {
            for (key, value) in table.entries()? {
                entries.insert(key, value);
            }
        }

        for (key, value) in self.memtable.freeze().entries() {
            entries.insert(key.clone(), value.clone());
        }

        Ok(entries.into_iter().collect())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let frozen = self.memtable.freeze();
        let sstable_id = self.manifest.next_file_id;
        let wal_id = self.manifest.next_file_id + 1;
        let sstable_path = self.dir.join(format!("sst-{sstable_id:016}.sst"));
        let new_wal_path = self.dir.join(format!("wal-{wal_id:016}.log"));

        let meta = Sstable::write_from_memtable(
            &sstable_path,
            &frozen,
            self.options.sstable_block_size,
            sstable_id,
        )?;

        let new_wal = WalWriter::create(&new_wal_path)?;
        let old_wal_path = self.wal.path().to_path_buf();

        let mut next_manifest = self.manifest.clone();
        next_manifest.sstables.push(meta.clone());
        next_manifest.active_wal_id = wal_id;
        next_manifest.next_file_id += 2;
        next_manifest.save(&self.dir)?;

        let new_reader = SstableReader::open(&sstable_path)?;
        let old_wal = std::mem::replace(&mut self.wal, new_wal);
        drop(old_wal);
        self.manifest = next_manifest;
        self.memtable.clear();
        self.sstables.push(new_reader);

        let _ = fs::remove_file(old_wal_path);
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        if self.sstables.len() <= 1 {
            return Ok(());
        }

        let compacted_id = self.manifest.next_file_id;
        let compacted_path = self.dir.join(format!("sst-{compacted_id:016}.sst"));
        let old_paths: Vec<PathBuf> = self
            .sstables
            .iter()
            .map(|table| table.path().to_path_buf())
            .collect();

        let mut merged = std::collections::BTreeMap::<Vec<u8>, ValueEntry>::new();
        for table in self.sstables.iter().rev() {
            for (key, value) in table.entries()? {
                merged.entry(key).or_insert(value);
            }
        }

        let frozen = ImmutableMemTable::from_entries(merged);
        let meta = Sstable::write_from_memtable(
            &compacted_path,
            &frozen,
            self.options.sstable_block_size,
            compacted_id,
        )?;

        let mut next_manifest = self.manifest.clone();
        next_manifest.sstables = vec![meta.clone()];
        next_manifest.next_file_id += 1;
        next_manifest.save(&self.dir)?;

        let compacted_reader = SstableReader::open(&compacted_path)?;
        self.manifest = next_manifest;
        self.sstables = vec![compacted_reader];

        for path in old_paths {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

    pub(crate) fn rewrite_from_entries(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, ValueEntry)>,
    ) -> Result<()> {
        let mut rewritten = std::collections::BTreeMap::<Vec<u8>, ValueEntry>::new();
        for (key, value) in entries {
            rewritten.insert(key, value);
        }

        let old_sstable_paths: Vec<PathBuf> = self
            .sstables
            .iter()
            .map(|table| table.path().to_path_buf())
            .collect();
        let old_wal_path = self.wal.path().to_path_buf();
        let rewrite_id = self.manifest.next_file_id;
        let new_wal_id = rewrite_id + 1;
        let new_wal_path = self.dir.join(format!("wal-{new_wal_id:016}.log"));
        let new_wal = WalWriter::create(&new_wal_path)?;

        let mut next_manifest = self.manifest.clone();
        next_manifest.sstables.clear();
        next_manifest.active_wal_id = new_wal_id;

        let new_reader = if rewritten.is_empty() {
            next_manifest.next_file_id += 1;
            None
        } else {
            let sstable_path = self.dir.join(format!("sst-{rewrite_id:016}.sst"));
            let frozen = ImmutableMemTable::from_entries(rewritten);
            let meta = Sstable::write_from_memtable(
                &sstable_path,
                &frozen,
                self.options.sstable_block_size,
                rewrite_id,
            )?;
            next_manifest.sstables.push(meta);
            next_manifest.next_file_id += 2;
            Some(SstableReader::open(&sstable_path)?)
        };

        next_manifest.save(&self.dir)?;

        let old_wal = std::mem::replace(&mut self.wal, new_wal);
        drop(old_wal);
        self.manifest = next_manifest;
        self.memtable.clear();
        self.sstables = new_reader.into_iter().collect();

        for path in old_sstable_paths {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_file(old_wal_path);
        Ok(())
    }

    pub fn manifest_state(&self) -> ManifestState {
        self.manifest.state()
    }
}
