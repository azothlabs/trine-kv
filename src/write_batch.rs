use crate::types::{KeyRange, Value};

/// One operation inside an atomic write batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOperation {
    Put {
        bucket: String,
        key: Vec<u8>,
        value: Value,
    },
    Delete {
        bucket: String,
        key: Vec<u8>,
    },
    DeleteRange {
        bucket: String,
        range: KeyRange,
    },
}

impl BatchOperation {
    #[must_use]
    pub fn bucket(&self) -> &str {
        match self {
            Self::Put { bucket, .. }
            | Self::Delete { bucket, .. }
            | Self::DeleteRange { bucket, .. } => bucket,
        }
    }
}

/// Atomic group of writes that may span multiple buckets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

impl WriteBatch {
    /// Creates an empty batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Adds a key/value write for `bucket`.
    pub fn put(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) {
        self.operations.push(BatchOperation::Put {
            bucket: bucket.into(),
            key: key.into(),
            value: value.into(),
        });
    }

    /// Adds a point delete for `bucket`.
    pub fn delete(&mut self, bucket: impl Into<String>, key: impl Into<Vec<u8>>) {
        self.operations.push(BatchOperation::Delete {
            bucket: bucket.into(),
            key: key.into(),
        });
    }

    /// Adds a range delete for `bucket`.
    pub fn delete_range(&mut self, bucket: impl Into<String>, range: KeyRange) {
        self.operations.push(BatchOperation::DeleteRange {
            bucket: bucket.into(),
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
