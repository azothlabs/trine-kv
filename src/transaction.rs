use crate::{
    bucket::DEFAULT_BUCKET_NAME,
    db::Db,
    error::Result,
    options::WriteOptions,
    types::{CommitInfo, KeyRange, Sequence, Value},
    write_batch::WriteBatch,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionOptions {
    pub write_options: WriteOptions,
}

/// Optimistic transaction over one read snapshot and a staged write batch.
///
/// Methods without a bucket suffix read or write the built-in default bucket.
/// Methods ending in `_bucket` operate on optional named buckets.
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

    #[must_use]
    pub const fn read_sequence(&self) -> Sequence {
        self.read_sequence
    }

    #[must_use]
    pub const fn options(&self) -> TransactionOptions {
        self.options
    }

    /// Reads a default-bucket key and tracks it for commit conflict checks.
    pub fn get_sync(&mut self, key: &[u8]) -> Result<Option<Value>> {
        self.get_bucket_sync(DEFAULT_BUCKET_NAME, key)
    }

    /// Reads a named-bucket key and tracks it for commit conflict checks.
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
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        self.get_bucket(DEFAULT_BUCKET_NAME, key).await
    }

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

    pub async fn read_range(&mut self, range: KeyRange) -> Result<()> {
        self.read_range_bucket(DEFAULT_BUCKET_NAME, range).await
    }

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
