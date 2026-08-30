use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::{DbError, Result};

#[derive(Debug, Clone)]
pub struct Manifest {
    pub version: u32,
    pub next_file_id: u64,
    pub next_sequence: u64,
    pub active_wal_id: u64,
    pub sstables: Vec<SstableMeta>,
}

#[derive(Debug, Clone)]
pub struct SstableMeta {
    pub id: u64,
    pub file_name: String,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub entry_count: u64,
}

#[derive(Debug, Clone)]
pub struct ManifestState {
    pub next_file_id: u64,
    pub next_sequence: u64,
    pub active_wal_id: u64,
    pub sstable_count: usize,
}

impl Manifest {
    pub fn load_or_create(dir: &Path) -> Result<Self> {
        let path = manifest_path(dir);
        if path.exists() {
            Self::load(&path)
        } else {
            let manifest = Self::new();
            manifest.save(dir)?;
            Ok(manifest)
        }
    }

    pub fn new() -> Self {
        Self {
            version: 1,
            next_file_id: 1,
            next_sequence: 1,
            active_wal_id: 1,
            sstables: Vec::new(),
        }
    }

    pub fn with_next_sequence(mut self, next_sequence: u64) -> Self {
        self.next_sequence = next_sequence.max(self.next_sequence);
        self
    }

    pub fn active_wal_path(&self, dir: &Path) -> PathBuf {
        dir.join(format!("wal-{id:016}.log", id = self.active_wal_id))
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        let path = manifest_path(dir);
        let tmp = path.with_extension("tmp");
        let mut file = File::create(&tmp)?;
        file.write_all(b"HLXM")?;
        file.write_all(&self.version.to_le_bytes())?;
        file.write_all(&self.next_file_id.to_le_bytes())?;
        file.write_all(&self.next_sequence.to_le_bytes())?;
        file.write_all(&self.active_wal_id.to_le_bytes())?;
        file.write_all(&(self.sstables.len() as u32).to_le_bytes())?;
        for table in &self.sstables {
            write_bytes(&mut file, &table.id.to_le_bytes())?;
            write_string(&mut file, &table.file_name)?;
            write_bytes(&mut file, &(table.smallest_key.len() as u32).to_le_bytes())?;
            file.write_all(&table.smallest_key)?;
            write_bytes(&mut file, &(table.largest_key.len() as u32).to_le_bytes())?;
            file.write_all(&table.largest_key)?;
            file.write_all(&table.entry_count.to_le_bytes())?;
        }

        file.flush()?;
        file.sync_all()?;
        drop(file);
        if path.exists() {
            let _ = fs::remove_file(&path);
        }
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != b"HLXM" {
            return Err(DbError::Corrupt("manifest magic mismatch".into()));
        }

        let version = read_u32(&mut file)?;
        let next_file_id = read_u64(&mut file)?;
        let next_sequence = read_u64(&mut file)?;
        let active_wal_id = read_u64(&mut file)?;
        let sstable_count = read_u32(&mut file)? as usize;
        let mut sstables = Vec::with_capacity(sstable_count);

        for _ in 0..sstable_count {
            let id = read_u64(&mut file)?;
            let file_name = read_string(&mut file)?;
            let smallest_key = read_vec(&mut file)?;
            let largest_key = read_vec(&mut file)?;
            let entry_count = read_u64(&mut file)?;
            sstables.push(SstableMeta {
                id,
                file_name,
                smallest_key,
                largest_key,
                entry_count,
            });
        }

        Ok(Self {
            version,
            next_file_id,
            next_sequence,
            active_wal_id,
            sstables,
        })
    }

    pub fn state(&self) -> ManifestState {
        ManifestState {
            next_file_id: self.next_file_id,
            next_sequence: self.next_sequence,
            active_wal_id: self.active_wal_id,
            sstable_count: self.sstables.len(),
        }
    }
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("manifest.bin")
}

fn write_bytes(file: &mut File, bytes: &[u8]) -> Result<()> {
    file.write_all(bytes)?;
    Ok(())
}

fn write_string(file: &mut File, value: &str) -> Result<()> {
    file.write_all(&(value.len() as u32).to_le_bytes())?;
    file.write_all(value.as_bytes())?;
    Ok(())
}

fn read_string(file: &mut File) -> Result<String> {
    let len = read_u32(file)? as usize;
    let mut buf = vec![0; len];
    file.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|err| DbError::Corrupt(format!("manifest string: {err}")))
}

fn read_vec(file: &mut File) -> Result<Vec<u8>> {
    let len = read_u32(file)? as usize;
    let mut buf = vec![0; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_u32(file: &mut File) -> Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(file: &mut File) -> Result<u64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
