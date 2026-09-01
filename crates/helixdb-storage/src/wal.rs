use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::{DbError, Result};

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub sequence: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl WalRecord {
    pub fn put(sequence: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            sequence,
            key,
            value: Some(value),
        }
    }

    pub fn delete(sequence: u64, key: Vec<u8>) -> Self {
        Self {
            sequence,
            key,
            value: None,
        }
    }
}

pub struct WalWriter {
    path: PathBuf,
    file: File,
}

impl WalWriter {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(false)
            .read(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    pub fn open_or_create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let encoded = encode_record(record);
        self.file.write_all(&encoded)?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn replay(&self, mut apply: impl FnMut(WalRecord) -> Result<()>) -> Result<()> {
        let mut file = File::open(&self.path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut cursor = std::io::Cursor::new(buf);

        loop {
            match read_record(&mut cursor)? {
                Some(record) => apply(record)?,
                None => break,
            }
        }

        Ok(())
    }
}

fn encode_record(record: &WalRecord) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(if record.value.is_some() { 1 } else { 2 });
    out.extend_from_slice(&record.sequence.to_le_bytes());
    out.extend_from_slice(&(record.key.len() as u32).to_le_bytes());
    out.extend_from_slice(
        &(record.value.as_ref().map(|v| v.len()).unwrap_or(0) as u32).to_le_bytes(),
    );
    out.extend_from_slice(&record.key);
    if let Some(value) = &record.value {
        out.extend_from_slice(value);
    }

    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(&hasher.finalize().to_le_bytes());
    out
}

fn read_record(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<Option<WalRecord>> {
    let total_len = cursor.get_ref().len() as u64;
    if cursor.position() >= total_len {
        return Ok(None);
    }

    let kind = match read_u8(cursor) {
        Ok(kind) => kind,
        Err(DbError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        }
        Err(err) => return Err(err),
    };

    let sequence = match read_u64(cursor) {
        Ok(value) => value,
        Err(DbError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        }
        Err(err) => return Err(err),
    };
    let key_len = match read_u32(cursor) {
        Ok(value) => value as usize,
        Err(DbError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        }
        Err(err) => return Err(err),
    };
    let value_len = match read_u32(cursor) {
        Ok(value) => value as usize,
        Err(DbError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        }
        Err(err) => return Err(err),
    };

    let mut key = vec![0; key_len];
    if cursor.read_exact(&mut key).is_err() {
        return Ok(None);
    }

    let mut value = vec![0; value_len];
    if value_len > 0 && cursor.read_exact(&mut value).is_err() {
        return Ok(None);
    }

    let checksum = match read_u32(cursor) {
        Ok(value) => value,
        Err(DbError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        }
        Err(err) => return Err(err),
    };

    let mut payload = Vec::new();
    payload.push(kind);
    payload.extend_from_slice(&sequence.to_le_bytes());
    payload.extend_from_slice(&(key_len as u32).to_le_bytes());
    payload.extend_from_slice(&(value_len as u32).to_le_bytes());
    payload.extend_from_slice(&key);
    if value_len > 0 {
        payload.extend_from_slice(&value);
    }
    let mut hasher = Hasher::new();
    hasher.update(&payload);
    if hasher.finalize() != checksum {
        return Err(DbError::ChecksumMismatch("wal record"));
    }

    Ok(Some(WalRecord {
        sequence,
        key,
        value: if kind == 1 { Some(value) } else { None },
    }))
}

fn read_u8(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<u8> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<u64> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
