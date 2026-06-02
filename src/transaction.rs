use crate::{
    bucket::DEFAULT_BUCKET_NAME,
    db::Db,
    error::Result,
    options::WriteOptions,
    types::{CommitInfo, KeyRange, Sequence, Value},
    write_batch::WriteBatch,
};

/// Options used by optimistic transactions.
///
/// The options are copied into the transaction when it is created. Changing a
/// separate `TransactionOptions` value later does not affect an existing
/// transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionOptions {
    /// Write options used when the transaction commits.
    pub write_options: WriteOptions,
}

/// Optimistic transaction over one read snapshot and a staged write batch.
///
/// Methods without a bucket suffix read or write the built-in default bucket.
/// Methods ending in `_bucket` operate on optional named buckets.
///
/// Reads are performed at the transaction's `read_sequence` and recorded in a
/// read set. Writes are staged in memory through a [`WriteBatch`]. Commit checks
/// whether any later committed point write, point delete, or range delete
/// conflicts with the recorded reads; if so, commit returns
/// [`crate::Error::Conflict`] and none of the staged writes are accepted.
///
/// # Examples
///
/// ```rust
/// use trine_kv::{Db, TransactionOptions};
///
/// # fn main() -> trine_kv::Result<()> {
/// let db = Db::open_sync(trine_kv::DbOptions::memory())?;
/// db.put_sync(b"counter", b"0")?;
///
/// let mut tx = db.transaction(TransactionOptions::default());
/// let current = tx.get_sync(b"counter")?;
/// assert_eq!(current, Some(b"0".to_vec()));
///
/// tx.put(b"counter", b"1");
/// let commit = tx.commit_sync()?;
/// assert!(commit.sequence().get() > 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Transaction {
    db: Db,
    read_sequence: Sequence,
    options: TransactionOptions,
    writes: WriteBatch,
    point_reads: Vec<ReadKey>,
    range_reads: Vec<ReadRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadKey {
    pub(crate) bucket: String,
    pub(crate) key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadRange {
    pub(crate) bucket: String,
    pub(crate) range: KeyRange,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TransactionReadSet {
    pub(crate) point_reads: Vec<ReadKey>,
    pub(crate) range_reads: Vec<ReadRange>,
}

impl Transaction {
    #[must_use]
    pub(crate) fn new(db: Db, read_sequence: Sequence, options: TransactionOptions) -> Self {
        Self {
            db,
            read_sequence,
            options,
            writes: WriteBatch::new(),
            point_reads: Vec::new(),
            range_reads: Vec::new(),
        }
    }

    /// Returns the sequence used by this transaction's read snapshot.
    ///
    /// All transaction reads use this sequence, even if newer writes commit
    /// before the transaction commits.
    #[must_use]
    pub const fn read_sequence(&self) -> Sequence {
        self.read_sequence
    }

    /// Returns this transaction's options.
    #[must_use]
    pub const fn options(&self) -> TransactionOptions {
        self.options
    }

    /// Reads a default-bucket key and tracks it for commit conflict checks.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes in the built-in default bucket.
    ///
    /// The exact key is added to the read set after the read succeeds. Commit
    /// fails if a later committed write or delete touches the key, or if a later
    /// range delete covers it.
    pub fn get_sync(&mut self, key: &[u8]) -> Result<Option<Value>> {
        self.get_bucket_sync(DEFAULT_BUCKET_NAME, key)
    }

    /// Reads a named-bucket key and tracks it for commit conflict checks.
    ///
    /// # Parameters
    ///
    /// - `bucket`: target named bucket.
    /// - `key`: user key bytes.
    pub fn get_bucket_sync(
        &mut self,
        bucket: impl Into<String>,
        key: &[u8],
    ) -> Result<Option<Value>> {
        let bucket = bucket.into();
        let value = self.db.get_at_sequence(&bucket, key, self.read_sequence)?;
        // Record the exact user key read at the transaction's read sequence.
        // Commit validation rejects the transaction if a later committed point
        // write, point delete, or covering range delete touched it.
        self.point_reads.push(ReadKey {
            bucket,
            key: key.to_vec(),
        });

        Ok(value)
    }

    /// Reads a default-bucket range and tracks it for commit conflict checks.
    ///
    /// The range cursor is fully consumed before the range is accepted into the
    /// read set. That means table or blob read errors are returned immediately
    /// instead of being deferred until commit.
    ///
    /// Commit fails if a later committed point mutation falls inside the range
    /// or if a later range delete overlaps it.
    pub fn read_range_sync(&mut self, range: KeyRange) -> Result<()> {
        self.read_range_bucket_sync(DEFAULT_BUCKET_NAME, range)
    }

    /// Reads a named-bucket range and tracks it for commit conflict checks.
    pub fn read_range_bucket_sync(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<()> {
        self.db.ensure_open()?;
        let bucket = bucket.into();
        let iter = self.db.range_at_sequence(
            &bucket,
            &range,
            self.read_sequence,
            crate::Direction::Forward,
        )?;
        // The transaction API records a range that was actually read at the
        // transaction sequence. Consume the cursor here so table/blob read
        // errors are returned before the read set is accepted.
        for item in iter {
            item?;
        }
        // Range reads conflict with any later committed point mutation inside
        // the range, plus any later range tombstone that overlaps it.
        self.range_reads.push(ReadRange { bucket, range });

        Ok(())
    }

    /// Stages one key/value write for the default bucket.
    ///
    /// Staging only mutates the in-memory transaction batch. The write is not
    /// visible and does not reserve a commit sequence until commit succeeds.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Value>) {
        self.writes.put(key, value);
    }

    /// Stages one key/value write for a named bucket.
    pub fn put_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        self.writes.put_bucket(bucket, key, value)
    }

    /// Stages a point delete for the default bucket.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.writes.delete(key);
    }

    /// Stages a point delete for a named bucket.
    pub fn delete_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
    ) -> Result<()> {
        self.writes.delete_bucket(bucket, key)
    }

    /// Stages a range delete for the default bucket.
    pub fn delete_range(&mut self, range: KeyRange) {
        self.writes.delete_range(range);
    }

    /// Stages a range delete for a named bucket.
    pub fn delete_range_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<()> {
        self.writes.delete_range_bucket(bucket, range)
    }

    /// Commits the staged writes synchronously after conflict checks.
    ///
    /// Commit consumes the transaction. If conflict validation succeeds, Trine
    /// commits all staged writes as one atomic batch using
    /// `self.options().write_options`. If validation fails, the staged writes
    /// are not accepted.
    ///
    /// # Returns
    ///
    /// Returns [`CommitInfo`] with the assigned commit sequence.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Conflict`] if the read set was invalidated, or
    /// the same write errors as [`crate::Db::write_sync`] for storage,
    /// durability, or closed/read-only handle failures.
    pub fn commit_sync(self) -> Result<CommitInfo> {
        let read_set = TransactionReadSet {
            point_reads: self.point_reads,
            range_reads: self.range_reads,
        };

        self.db.commit_transaction(
            self.read_sequence,
            read_set,
            self.writes,
            self.options.write_options,
        )
    }
}

/// Primary async transaction read/commit API. Staged write builders stay
/// synchronous because they only mutate the in-memory transaction batch.
#[allow(clippy::unused_async)]
impl Transaction {
    /// Reads a default-bucket key and tracks it for commit conflict checks.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        self.get_bucket(DEFAULT_BUCKET_NAME, key).await
    }

    /// Reads a named-bucket key and tracks it for commit conflict checks.
    pub async fn get_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: &[u8],
    ) -> Result<Option<Value>> {
        let bucket = bucket.into();
        let value = self
            .db
            .get_at_sequence_async(&bucket, key, self.read_sequence)
            .await?;
        self.point_reads.push(ReadKey {
            bucket,
            key: key.to_vec(),
        });

        Ok(value)
    }

    /// Reads a default-bucket range and tracks it for commit conflict checks.
    pub async fn read_range(&mut self, range: KeyRange) -> Result<()> {
        self.read_range_bucket(DEFAULT_BUCKET_NAME, range).await
    }

    /// Reads a named-bucket range and tracks it for commit conflict checks.
    pub async fn read_range_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<()> {
        self.db.ensure_open()?;
        let bucket = bucket.into();
        let mut iter = self
            .db
            .range_at_sequence_async(
                &bucket,
                &range,
                self.read_sequence,
                crate::Direction::Forward,
            )
            .await?;
        while iter.next().await?.is_some() {}
        self.range_reads.push(ReadRange { bucket, range });

        Ok(())
    }

    /// Commits the staged writes asynchronously after conflict checks.
    pub async fn commit(self) -> Result<CommitInfo> {
        let read_set = TransactionReadSet {
            point_reads: self.point_reads,
            range_reads: self.range_reads,
        };

        self.db
            .commit_transaction_async(
                self.read_sequence,
                read_set,
                self.writes,
                self.options.write_options,
            )
            .await
    }
}
