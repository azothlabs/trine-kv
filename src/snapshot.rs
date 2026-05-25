use crate::{
    error::Result,
    iterator::Iter,
    keyspace::Keyspace,
    types::{KeyRange, Sequence, Value},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    read_sequence: Sequence,
}

impl Snapshot {
    #[must_use]
    pub const fn new(read_sequence: Sequence) -> Self {
        Self { read_sequence }
    }

    #[must_use]
    pub const fn read_sequence(self) -> Sequence {
        self.read_sequence
    }

    pub fn get(self, keyspace: &Keyspace, key: &[u8]) -> Result<Option<Value>> {
        keyspace.get_at(&self, key)
    }

    pub fn range(self, keyspace: &Keyspace, range: &KeyRange) -> Result<Iter> {
        keyspace.range_at(&self, range)
    }

    pub fn range_reverse(self, keyspace: &Keyspace, range: &KeyRange) -> Result<Iter> {
        keyspace.range_reverse_at(&self, range)
    }

    pub fn prefix(self, keyspace: &Keyspace, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        keyspace.prefix_at(&self, prefix)
    }

    pub fn prefix_reverse(self, keyspace: &Keyspace, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        keyspace.prefix_reverse_at(&self, prefix)
    }
}
