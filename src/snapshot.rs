use crate::{
    error::Result,
    keyspace::Keyspace,
    types::{Sequence, Value},
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
}
