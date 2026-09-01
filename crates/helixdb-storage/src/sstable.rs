use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::bloom::BloomFilter;
use crate::manifest::SstableMeta;
use crate::memtable::ImmutableMemTable;
use crate::types::ValueEntry;
use crate::{DbError, Result};

const TABLE_MAGIC: u32 = 0x54424C45;
const TABLE_VERSION: u32 = 1;
const FOOTER_SIZE: u64 = 28;

#[derive(Debug, Clone)]
pub struct BlockIndexEntry {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
pub struct Sstable {
    pub meta: SstableMeta,
}

#[derive(Debug, Clone)]
pub struct SstableReader {
    path: PathBuf,
    index: Vec<BlockIndexEntry>,
    bloom: BloomFilter,
}

impl Sstable {
    pub fn write_from_memtable(
        path: &Path,
        memtable: &ImmutableMemTable,
        block_size: usize,
        file_id: u64,
    ) -> Result<SstableMeta> {
        let entries: Vec<(Vec<u8>, ValueEntry)> = memtable
            .entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if entries.is_empty() {
            return Err(DbError::Corrupt("cannot write empty sstable".into()));
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let mut blocks: Vec<BlockIndexEntry> = Vec::new();
        let mut block_entries: Vec<(Vec<u8>, ValueEntry)> = Vec::new();
        let bloom = BloomFilter::from_keys(entries.iter().map(|(key, _)| key.as_slice()));
        let mut current_bytes = 0usize;
        let mut offset = 0u64;

        for (key, value) in entries {
            let entry_bytes = key.len() + value.value.as_ref().map(|v| v.len()).unwrap_or(0) + 32;
            if !block_entries.is_empty() && current_bytes + entry_bytes > block_size {
                let length = write_block(&mut file, &block_entries)?;
                blocks.push(BlockIndexEntry {
                    first_key: block_entries.first().unwrap().0.clone(),
                    last_key: block_entries.last().unwrap().0.clone(),
                    offset,
                    length,
                });
                offset += length;
                block_entries.clear();
                current_bytes = 0;
            }

            current_bytes += entry_bytes;
            block_entries.push((key, value));
        }

        if !block_entries.is_empty() {
            let length = write_block(&mut file, &block_entries)?;
            blocks.push(BlockIndexEntry {
                first_key: block_entries.first().unwrap().0.clone(),
                last_key: block_entries.last().unwrap().0.clone(),
                offset,
                length,
            });
            offset += length;
        }

        let index_offset = offset;
        let index_bytes = write_index(&mut file, &blocks)?;
        let bloom_offset = index_offset + index_bytes;
        let bloom_bytes = bloom.serialize();
        file.write_all(&bloom_bytes)?;

        let mut footer = Vec::new();
        footer.extend_from_slice(&TABLE_MAGIC.to_le_bytes());
        footer.extend_from_slice(&TABLE_VERSION.to_le_bytes());
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&bloom_offset.to_le_bytes());
        let mut hasher = Hasher::new();
        hasher.update(&footer);
        footer.extend_from_slice(&hasher.finalize().to_le_bytes());
        file.write_all(&footer)?;
        file.flush()?;
        file.sync_all()?;

        let smallest_key = blocks
            .first()
            .map(|b| b.first_key.clone())
            .ok_or_else(|| DbError::Corrupt("missing sstable blocks".into()))?;
        let largest_key = blocks
            .last()
            .map(|b| b.last_key.clone())
            .ok_or_else(|| DbError::Corrupt("missing sstable blocks".into()))?;

        Ok(SstableMeta {
            id: file_id,
            file_name: path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| DbError::Corrupt("invalid sstable file name".into()))?
                .to_string(),
            smallest_key,
            largest_key,
            entry_count: memtable.len() as u64,
        })
    }
}

impl SstableReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_SIZE {
            return Err(DbError::Corrupt("sstable too short".into()));
        }

        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer = vec![0u8; FOOTER_SIZE as usize];
        file.read_exact(&mut footer)?;
        let mut footer_hasher = Hasher::new();
        footer_hasher.update(&footer[..24]);
        let footer_checksum = u32::from_le_bytes(footer[24..28].try_into().unwrap());
        if footer_hasher.finalize() != footer_checksum {
            return Err(DbError::ChecksumMismatch("sstable footer"));
        }

        let magic = u32::from_le_bytes(footer[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(footer[4..8].try_into().unwrap());
        if magic != TABLE_MAGIC || version != TABLE_VERSION {
            return Err(DbError::Corrupt("sstable footer mismatch".into()));
        }

        let index_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let index_len = bloom_offset
            .checked_sub(index_offset)
            .ok_or_else(|| DbError::Corrupt("sstable offsets out of range".into()))?;

        file.seek(SeekFrom::Start(index_offset))?;
        let index = read_index(&mut file, index_len)?;

        file.seek(SeekFrom::Start(bloom_offset))?;
        let bloom_len = len
            .checked_sub(FOOTER_SIZE)
            .and_then(|value| value.checked_sub(bloom_offset))
            .ok_or_else(|| DbError::Corrupt("sstable offsets out of range".into()))?;
        let mut bloom_bytes = vec![0u8; bloom_len as usize];
        file.read_exact(&mut bloom_bytes)?;
        let bloom = BloomFilter::deserialize(&bloom_bytes)?;

        Ok(Self {
            path: path.to_path_buf(),
            index,
            bloom,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.bloom.might_contain(key) {
            return Ok(None);
        }

        let block_index = match self.find_block(key) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let block = read_block(&self.path, &self.index[block_index])?;
        for (entry_key, entry_value) in block {
            match entry_key.as_slice().cmp(key) {
                Ordering::Equal => return Ok(entry_value.value),
                Ordering::Greater => return Ok(None),
                Ordering::Less => continue,
            }
        }

        Ok(None)
    }

    pub fn entries(&self) -> Result<Vec<(Vec<u8>, ValueEntry)>> {
        let mut out = Vec::new();
        for entry in &self.index {
            let block = read_block(&self.path, entry)?;
            out.extend(block);
        }
        Ok(out)
    }

    fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }

        let idx = self
            .index
            .partition_point(|entry| entry.last_key.as_slice() < key);
        if idx >= self.index.len() {
            return None;
        }

        let candidate = &self.index[idx];
        if candidate.first_key.as_slice() <= key && key <= candidate.last_key.as_slice() {
            Some(idx)
        } else {
            None
        }
    }
}

fn write_block(file: &mut File, entries: &[(Vec<u8>, ValueEntry)]) -> Result<u64> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (key, value) in entries {
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(
            &(value.value.as_ref().map(|v| v.len()).unwrap_or(0) as u32).to_le_bytes(),
        );
        payload.extend_from_slice(&value.sequence.to_le_bytes());
        payload.push(if value.value.is_some() { 1 } else { 2 });
        payload.extend_from_slice(key);
        if let Some(inner) = &value.value {
            payload.extend_from_slice(inner);
        }
    }

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let checksum = hasher.finalize();
    payload.extend_from_slice(&checksum.to_le_bytes());
    file.write_all(&payload)?;
    Ok(payload.len() as u64)
}

fn write_index(file: &mut File, blocks: &[BlockIndexEntry]) -> Result<u64> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    for block in blocks {
        payload.extend_from_slice(&(block.first_key.len() as u32).to_le_bytes());
        payload.extend_from_slice(&block.first_key);
        payload.extend_from_slice(&(block.last_key.len() as u32).to_le_bytes());
        payload.extend_from_slice(&block.last_key);
        payload.extend_from_slice(&block.offset.to_le_bytes());
        payload.extend_from_slice(&block.length.to_le_bytes());
    }

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    payload.extend_from_slice(&hasher.finalize().to_le_bytes());
    file.write_all(&payload)?;
    Ok(payload.len() as u64)
}

fn read_index(file: &mut File, len: u64) -> Result<Vec<BlockIndexEntry>> {
    let mut payload = vec![0u8; len as usize];
    file.read_exact(&mut payload)?;
    if payload.len() < 4 {
        return Err(DbError::Corrupt("sstable index too short".into()));
    }
    let checksum = u32::from_le_bytes(payload[payload.len() - 4..].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&payload[..payload.len() - 4]);
    if hasher.finalize() != checksum {
        return Err(DbError::ChecksumMismatch("sstable index"));
    }

    let mut cursor = std::io::Cursor::new(payload[..payload.len() - 4].to_vec());
    let count = read_u32(&mut cursor)? as usize;
    let mut blocks = Vec::with_capacity(count);

    for _ in 0..count {
        let first_key = read_vec(&mut cursor)?;
        let last_key = read_vec(&mut cursor)?;
        let offset = read_u64(&mut cursor)?;
        let length = read_u64(&mut cursor)?;
        blocks.push(BlockIndexEntry {
            first_key,
            last_key,
            offset,
            length,
        });
    }

    Ok(blocks)
}

fn read_block(path: &Path, entry: &BlockIndexEntry) -> Result<Vec<(Vec<u8>, ValueEntry)>> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut payload = vec![0u8; entry.length as usize];
    file.read_exact(&mut payload)?;

    if payload.len() < 4 {
        return Err(DbError::Corrupt("sstable block too short".into()));
    }

    let checksum = u32::from_le_bytes(payload[payload.len() - 4..].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&payload[..payload.len() - 4]);
    if hasher.finalize() != checksum {
        return Err(DbError::ChecksumMismatch("sstable block"));
    }

    let mut cursor = std::io::Cursor::new(payload[..payload.len() - 4].to_vec());
    let count = read_u32(&mut cursor)? as usize;
    let mut out = Vec::with_capacity(count);

    for _ in 0..count {
        let key_len = read_u32(&mut cursor)? as usize;
        let value_len = read_u32(&mut cursor)? as usize;
        let sequence = read_u64(&mut cursor)?;
        let flags = read_u8(&mut cursor)?;
        let key = read_bytes(&mut cursor, key_len)?;
        let value = if flags == 1 {
            Some(read_bytes(&mut cursor, value_len)?)
        } else {
            let _ = read_bytes(&mut cursor, value_len)?;
            None
        };
        out.push((key, ValueEntry { sequence, value }));
    }

    Ok(out)
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

fn read_vec(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<Vec<u8>> {
    let len = read_u32(cursor)? as usize;
    read_bytes(cursor, len)
}

fn read_bytes(cursor: &mut std::io::Cursor<Vec<u8>>, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}
