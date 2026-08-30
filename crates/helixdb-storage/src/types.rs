/// Mutable state stored for a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEntry {
    pub sequence: u64,
    pub value: Option<Vec<u8>>,
}

impl ValueEntry {
    pub fn new(sequence: u64, value: Option<Vec<u8>>) -> Self {
        Self { sequence, value }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EntryKind {
    Value,
    Tombstone,
}
