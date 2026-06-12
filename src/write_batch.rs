use crate::{
    bucket::DEFAULT_BUCKET_NAME,
    error::{Error, Result},
    types::{KeyRange, Value},
};

/// One operation inside an atomic write batch.
///
/// Applications usually build these through [`WriteBatch`] methods instead of
/// constructing variants directly. The variants are public so callers can
/// inspect a batch before passing it to [`crate::Db::write_sync`] or
/// [`crate::Db::write`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOperation {
    /// Insert or replace one key/value pair.
    Put {
        /// Target bucket name.
        bucket: String,
        /// User key bytes.
        key: Vec<u8>,
        /// Value bytes to store.
        value: Value,
    },
    /// Delete one key.
    Delete {
        /// Target bucket name.
        bucket: String,
        /// User key bytes.
        key: Vec<u8>,
    },
    /// Delete all visible keys in a range.
    DeleteRange {
        /// Target bucket name.
        bucket: String,
        /// User-key range to delete.
        range: KeyRange,
    },
}

impl BatchOperation {
    /// Returns the bucket targeted by this operation.
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
///
/// Methods without a bucket suffix target the built-in default bucket. Methods
/// ending in `_bucket` target an optional named bucket returned by `Db::bucket`.
///
/// A batch is committed with [`crate::Db::write_sync`] or [`crate::Db::write`].
/// Trine assigns one commit sequence to the entire batch, appends the accepted
/// operations to the WAL for persistent databases, and publishes the batch to
/// the affected memtables atomically from the caller's point of view.
///
/// # Examples
///
/// ```rust
/// use trine_kv::{Db, WriteBatch, WriteOptions};
///
/// # fn main() -> trine_kv::Result<()> {
/// let db = Db::open_sync(trine_kv::DbOptions::memory())?;
/// let users = db.bucket_sync("users")?;
///
/// let mut batch = WriteBatch::new();
/// batch.put(b"system:ready", b"yes");
/// batch.put_bucket(users.name().as_str(), b"1", b"Ada")?;
///
/// let commit = db.write_sync(batch, WriteOptions::sync_all())?;
/// assert!(commit.read_version().as_u64() > 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

impl WriteBatch {
    /// Creates an empty batch.
    ///
    /// The batch does not reserve a commit sequence until it is passed to a
    /// database write method.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Adds a key/value write to the default bucket.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes for the built-in default bucket.
    /// - `value`: value bytes to store.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Value>) {
        self.operations.push(BatchOperation::Put {
            bucket: DEFAULT_BUCKET_NAME.to_owned(),
            key: key.into(),
            value: value.into(),
        });
    }

    /// Adds a key/value write for a named bucket.
    ///
    /// The bucket name must refer to an optional named bucket. Use
    /// [`WriteBatch::put`] for the built-in default bucket.
    ///
    /// # Parameters
    ///
    /// - `bucket`: target named bucket.
    /// - `key`: user key bytes.
    /// - `value`: value bytes to store.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] if `bucket` is empty or is the reserved
    /// default bucket name.
    pub fn put_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        let bucket = bucket.into();
        validate_named_bucket(&bucket)?;
        self.operations.push(BatchOperation::Put {
            bucket,
            key: key.into(),
            value: value.into(),
        });
        Ok(())
    }

    /// Adds a point delete to the default bucket.
    ///
    /// The delete hides older values for the same key after the batch commits.
    /// Snapshots older than the commit sequence can still see earlier values.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.operations.push(BatchOperation::Delete {
            bucket: DEFAULT_BUCKET_NAME.to_owned(),
            key: key.into(),
        });
    }

    /// Adds a point delete for a named bucket.
    pub fn delete_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
    ) -> Result<()> {
        let bucket = bucket.into();
        validate_named_bucket(&bucket)?;
        self.operations.push(BatchOperation::Delete {
            bucket,
            key: key.into(),
        });
        Ok(())
    }

    /// Adds a range delete to the default bucket.
    ///
    /// The delete hides all keys in `range` for read sequences after the batch
    /// commits. The operation is stored as a range tombstone and can conflict
    /// with optimistic transactions that read overlapping keys or ranges.
    pub fn delete_range(&mut self, range: KeyRange) {
        self.operations.push(BatchOperation::DeleteRange {
            bucket: DEFAULT_BUCKET_NAME.to_owned(),
            range,
        });
    }

    /// Adds a range delete for a named bucket.
    pub fn delete_range_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<()> {
        let bucket = bucket.into();
        validate_named_bucket(&bucket)?;
        self.operations
            .push(BatchOperation::DeleteRange { bucket, range });
        Ok(())
    }

    /// Returns the operations in insertion order.
    #[must_use]
    pub fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }

    /// Consumes the batch and returns its operations in insertion order.
    #[must_use]
    pub fn into_operations(self) -> Vec<BatchOperation> {
        self.operations
    }

    /// Returns the number of operations in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Returns `true` when the batch contains no operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

fn validate_named_bucket(bucket: &str) -> Result<()> {
    if bucket.is_empty() {
        return Err(Error::invalid_options("bucket name cannot be empty"));
    }
    if bucket == DEFAULT_BUCKET_NAME {
        return Err(Error::invalid_options(
            "default bucket writes use default batch methods",
        ));
    }
    Ok(())
}
