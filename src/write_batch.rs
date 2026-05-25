use crate::types::{KeyRange, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOperation {
    Insert {
        keyspace: String,
        key: Vec<u8>,
        value: Value,
    },
    Remove {
        keyspace: String,
        key: Vec<u8>,
    },
    RemoveRange {
        keyspace: String,
        range: KeyRange,
    },
}

impl BatchOperation {
    #[must_use]
    pub fn keyspace(&self) -> &str {
        match self {
            Self::Insert { keyspace, .. }
            | Self::Remove { keyspace, .. }
            | Self::RemoveRange { keyspace, .. } => keyspace,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

impl WriteBatch {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn insert(
        &mut self,
        keyspace: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) {
        self.operations.push(BatchOperation::Insert {
            keyspace: keyspace.into(),
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn remove(&mut self, keyspace: impl Into<String>, key: impl Into<Vec<u8>>) {
        self.operations.push(BatchOperation::Remove {
            keyspace: keyspace.into(),
            key: key.into(),
        });
    }

    pub fn remove_range(&mut self, keyspace: impl Into<String>, range: KeyRange) {
        self.operations.push(BatchOperation::RemoveRange {
            keyspace: keyspace.into(),
            range,
        });
    }

    #[must_use]
    pub fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }

    #[must_use]
    pub fn into_operations(self) -> Vec<BatchOperation> {
        self.operations
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}
