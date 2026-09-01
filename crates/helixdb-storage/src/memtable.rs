use std::collections::BTreeMap;

use crate::types::ValueEntry;

#[derive(Debug, Clone, Default)]
pub struct MemTable {
    entries: BTreeMap<Vec<u8>, ValueEntry>,
    approximate_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ImmutableMemTable {
    entries: BTreeMap<Vec<u8>, ValueEntry>,
}

impl MemTable {
    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, sequence: u64) {
        let new_entry = ValueEntry::new(sequence, value);
        let replacement_size = key.len()
            + new_entry.value.as_ref().map(|v| v.len()).unwrap_or(0)
            + std::mem::size_of::<u64>()
            + 16;

        if let Some(existing) = self.entries.insert(key, new_entry) {
            let old_size = existing.value.as_ref().map(|v| v.len()).unwrap_or(0) + 24;
            self.approximate_bytes = self.approximate_bytes.saturating_sub(old_size);
        }

        self.approximate_bytes = self.approximate_bytes.saturating_add(replacement_size);
    }

    pub fn get(&self, key: &[u8]) -> Option<&ValueEntry> {
        self.entries.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.approximate_bytes = 0;
    }

    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    pub fn freeze(&self) -> ImmutableMemTable {
        ImmutableMemTable {
            entries: self.entries.clone(),
        }
    }

    pub fn apply_record(&mut self, record: crate::wal::WalRecord) {
        self.insert(record.key, record.value, record.sequence);
    }
}

impl ImmutableMemTable {
    pub fn from_entries(entries: BTreeMap<Vec<u8>, ValueEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Vec<u8>, &ValueEntry)> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
