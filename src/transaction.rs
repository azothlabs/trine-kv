use crate::{
    bucket::{DEFAULT_BUCKET_NAME, require_internal_bucket, validate_user_named_bucket},
    db::Db,
    error::Result,
    iterator::Iter,
    options::WriteOptions,
    types::{CommitInfo, KeyRange, ReadVersion, Sequence, Value},
};

mod core;

#[cfg(test)]
pub(crate) use core::ReadKey;
use core::TransactionCore;
pub(crate) use core::TransactionReadSet;

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
/// read set. Writes are staged in memory through a [`crate::WriteBatch`]. Commit checks
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
/// assert!(commit.read_version().as_u64() > 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Transaction {
    db: Db,
    core: TransactionCore,
}

impl Transaction {
    #[must_use]
    pub(crate) fn new(db: Db, read_sequence: Sequence, options: TransactionOptions) -> Self {
        Self {
            db,
            core: TransactionCore::new(read_sequence, options),
        }
    }

    /// Returns the public read version used by this transaction's read
    /// snapshot.
    ///
    /// All transaction reads use this read boundary, even if newer writes
    /// commit before the transaction commits.
    #[must_use]
    pub const fn read_version(&self) -> ReadVersion {
        ReadVersion::from_sequence(self.core.read_sequence())
    }

    /// Returns this transaction's options.
    #[must_use]
    pub const fn options(&self) -> TransactionOptions {
        self.core.options()
    }

    pub(crate) const fn database(&self) -> &Db {
        &self.db
    }

    const fn read_sequence(&self) -> Sequence {
        self.core.read_sequence()
    }

    pub(crate) fn extension_claim(&self, key: &[u8]) -> Option<[u8; 16]> {
        self.core.extension_claim(key)
    }

    pub(crate) fn record_extension_claim(&mut self, key: Vec<u8>, claim: [u8; 16]) {
        self.core.record_extension_claim(key, claim);
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
        self.get_bucket_sync_unchecked(DEFAULT_BUCKET_NAME.to_owned(), key)
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
        validate_user_named_bucket(&bucket)?;
        self.get_bucket_sync_unchecked(bucket, key)
    }

    pub(crate) fn get_internal_bucket_sync(
        &mut self,
        bucket: impl Into<String>,
        key: &[u8],
    ) -> Result<Option<Value>> {
        let bucket = bucket.into();
        require_internal_bucket(&bucket)?;
        self.get_bucket_sync_unchecked(bucket, key)
    }

    fn get_bucket_sync_unchecked(&mut self, bucket: String, key: &[u8]) -> Result<Option<Value>> {
        let value = self
            .db
            .get_at_sequence(&bucket, key, self.read_sequence())?;
        // Record the exact user key read at the transaction's read sequence.
        // Commit validation rejects the transaction if a later committed point
        // write, point delete, or covering range delete touched it.
        self.core.record_point_read(bucket, key.to_vec());

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
        self.read_range_bucket_sync_unchecked(DEFAULT_BUCKET_NAME.to_owned(), range)
    }

    /// Reads a named-bucket range and tracks it for commit conflict checks.
    pub fn read_range_bucket_sync(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<()> {
        let bucket = bucket.into();
        validate_user_named_bucket(&bucket)?;
        self.read_range_bucket_sync_unchecked(bucket, range)
    }

    fn read_range_bucket_sync_unchecked(&mut self, bucket: String, range: KeyRange) -> Result<()> {
        self.db.ensure_open()?;
        let iter = self.db.range_at_sequence(
            &bucket,
            &range,
            self.read_sequence(),
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
        self.core.record_range_read(bucket, range);

        Ok(())
    }

    /// Reads the default-bucket range and returns its cursor, tracking the
    /// range for commit conflict checks.
    pub fn range_sync(&mut self, range: KeyRange) -> Result<Iter> {
        self.range_bucket_sync_unchecked(DEFAULT_BUCKET_NAME.to_owned(), range)
    }

    /// Reads a named-bucket range and returns its cursor, tracking the range for
    /// commit conflict checks.
    ///
    /// Unlike [`read_range_bucket_sync`](Self::read_range_bucket_sync), the data
    /// cursor is returned to the caller rather than consumed: this is the read
    /// path for transactions that need the range's values (e.g. a scan), not
    /// just conflict tracking. The range is recorded in the read set at the
    /// transaction's read sequence; iteration errors surface as the caller
    /// drives the returned cursor. Commit fails if a later committed point
    /// mutation falls inside the range or a later range delete overlaps it.
    pub fn range_bucket_sync(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<Iter> {
        let bucket = bucket.into();
        validate_user_named_bucket(&bucket)?;
        self.range_bucket_sync_unchecked(bucket, range)
    }

    fn range_bucket_sync_unchecked(&mut self, bucket: String, range: KeyRange) -> Result<Iter> {
        self.db.ensure_open()?;
        let iter = self.db.range_at_sequence(
            &bucket,
            &range,
            self.read_sequence(),
            crate::Direction::Forward,
        )?;
        self.core.record_range_read(bucket, range);

        Ok(iter)
    }

    /// Stages one key/value write for the default bucket.
    ///
    /// Staging only mutates the in-memory transaction batch. The write is not
    /// visible and does not reserve a commit sequence until commit succeeds.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Value>) {
        self.core.writes_mut().put(key, value);
    }

    /// Stages one key/value write for a named bucket.
    pub fn put_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        self.core.writes_mut().put_bucket(bucket, key, value)
    }

    pub(crate) fn put_internal_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        self.core
            .writes_mut()
            .put_internal_bucket(bucket, key, value)
    }

    /// Stages a named-bucket value containing this transaction's final commit
    /// sequence in unsigned big-endian form.
    ///
    /// The stored value is `prefix || sequence.to_be_bytes() || suffix`. The
    /// eight-byte sequence is filled only after optimistic read validation
    /// succeeds and Trine reserves the transaction's commit slot. The resolved
    /// bytes are then used by both the WAL and memtable publish paths, so a
    /// successful reopen observes the same value returned by
    /// [`CommitInfo::read_version`]. A conflict publishes neither the value nor
    /// a guessed sequence.
    ///
    /// This is intended for upper storage layers that must persist an
    /// instance-local visibility coordinate in the exact write it describes.
    /// It does not provide a portable logical identity and it does not expose or
    /// reserve a sequence before commit.
    ///
    /// # Parameters
    ///
    /// - `bucket`: existing named bucket that receives the value.
    /// - `key`: user key within `bucket`.
    /// - `prefix`: bytes stored before the eight-byte sequence.
    /// - `suffix`: bytes stored after the eight-byte sequence.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidOptions`] when `bucket` is empty or names the
    /// built-in default bucket, or when the staged value cannot be represented
    /// by this transaction's bounded batch format. Commit may later return
    /// [`crate::Error::Conflict`] or the ordinary storage and durability errors.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions, TransactionOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// let db = Db::open_sync(DbOptions::memory())?;
    /// let metadata = db.bucket_sync("metadata")?;
    /// let mut transaction = db.transaction(TransactionOptions::default());
    /// transaction.put_bucket_with_commit_sequence(
    ///     metadata.name().as_str(),
    ///     b"latest",
    ///     b"v1:",
    ///     b":accepted",
    /// )?;
    /// let commit = transaction.commit_sync()?;
    /// let stored = metadata.get_sync(b"latest")?.expect("committed value");
    /// assert_eq!(&stored[..3], b"v1:");
    /// assert_eq!(
    ///     u64::from_be_bytes(stored[3..11].try_into().expect("eight-byte sequence")),
    ///     commit.read_version().as_u64(),
    /// );
    /// assert_eq!(&stored[11..], b":accepted");
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_bucket_with_commit_sequence(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
        prefix: &[u8],
        suffix: &[u8],
    ) -> Result<()> {
        self.core
            .writes_mut()
            .put_bucket_with_commit_sequence(bucket, key, prefix, suffix)
    }

    pub(crate) fn put_internal_bucket_with_commit_sequence(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
        prefix: &[u8],
        suffix: &[u8],
    ) -> Result<()> {
        self.core
            .writes_mut()
            .put_internal_bucket_with_commit_sequence(bucket, key, prefix, suffix)
    }

    /// Stages a point delete for the default bucket.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.core.writes_mut().delete(key);
    }

    /// Stages a point delete for a named bucket.
    pub fn delete_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
    ) -> Result<()> {
        self.core.writes_mut().delete_bucket(bucket, key)
    }

    pub(crate) fn delete_internal_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<Vec<u8>>,
    ) -> Result<()> {
        self.core.writes_mut().delete_internal_bucket(bucket, key)
    }

    /// Stages a range delete for the default bucket.
    pub fn delete_range(&mut self, range: KeyRange) {
        self.core.writes_mut().delete_range(range);
    }

    /// Stages a range delete for a named bucket.
    pub fn delete_range_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<()> {
        self.core.writes_mut().delete_range_bucket(bucket, range)
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
        let (read_sequence, read_set, writes, options) = self.core.into_commit_parts();
        self.db
            .commit_transaction(read_sequence, read_set, writes, options.write_options)
    }
}

/// Primary async transaction read/commit API. Staged write builders stay
/// synchronous because they only mutate the in-memory transaction batch.
#[allow(clippy::unused_async)]
impl Transaction {
    /// Reads a default-bucket key and tracks it for commit conflict checks.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        self.get_bucket_unchecked(DEFAULT_BUCKET_NAME.to_owned(), key)
            .await
    }

    /// Reads a named-bucket key and tracks it for commit conflict checks.
    pub async fn get_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: &[u8],
    ) -> Result<Option<Value>> {
        let bucket = bucket.into();
        validate_user_named_bucket(&bucket)?;
        self.get_bucket_unchecked(bucket, key).await
    }

    pub(crate) async fn get_internal_bucket(
        &mut self,
        bucket: impl Into<String>,
        key: &[u8],
    ) -> Result<Option<Value>> {
        let bucket = bucket.into();
        require_internal_bucket(&bucket)?;
        self.get_bucket_unchecked(bucket, key).await
    }

    async fn get_bucket_unchecked(&mut self, bucket: String, key: &[u8]) -> Result<Option<Value>> {
        let value = self
            .db
            .get_at_sequence_async(&bucket, key, self.read_sequence())
            .await?;
        self.core.record_point_read(bucket, key.to_vec());

        Ok(value)
    }

    /// Reads a default-bucket range and tracks it for commit conflict checks.
    pub async fn read_range(&mut self, range: KeyRange) -> Result<()> {
        self.read_range_bucket_unchecked(DEFAULT_BUCKET_NAME.to_owned(), range)
            .await
    }

    /// Reads a named-bucket range and tracks it for commit conflict checks.
    pub async fn read_range_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<()> {
        let bucket = bucket.into();
        validate_user_named_bucket(&bucket)?;
        self.read_range_bucket_unchecked(bucket, range).await
    }

    async fn read_range_bucket_unchecked(&mut self, bucket: String, range: KeyRange) -> Result<()> {
        self.db.ensure_open()?;
        let mut iter = self
            .db
            .range_at_sequence_async(
                &bucket,
                &range,
                self.read_sequence(),
                crate::Direction::Forward,
            )
            .await?;
        while iter.next().await?.is_some() {}
        self.core.record_range_read(bucket, range);

        Ok(())
    }

    /// Reads the default-bucket range and returns its cursor, tracking the
    /// range for commit conflict checks.
    pub async fn range(&mut self, range: KeyRange) -> Result<Iter> {
        self.range_bucket_unchecked(DEFAULT_BUCKET_NAME.to_owned(), range)
            .await
    }

    /// Reads a named-bucket range and returns its cursor, tracking the range for
    /// commit conflict checks. The async counterpart of
    /// [`range_bucket_sync`](Self::range_bucket_sync).
    pub async fn range_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<Iter> {
        let bucket = bucket.into();
        validate_user_named_bucket(&bucket)?;
        self.range_bucket_unchecked(bucket, range).await
    }

    pub(crate) async fn range_internal_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<Iter> {
        let bucket = bucket.into();
        require_internal_bucket(&bucket)?;
        self.range_bucket_unchecked(bucket, range).await
    }

    async fn range_bucket_unchecked(&mut self, bucket: String, range: KeyRange) -> Result<Iter> {
        self.db.ensure_open()?;
        let iter = self
            .db
            .range_at_sequence_async(
                &bucket,
                &range,
                self.read_sequence(),
                crate::Direction::Forward,
            )
            .await?;
        self.core.record_range_read(bucket, range);

        Ok(iter)
    }

    /// Commits the staged writes asynchronously after conflict checks.
    pub async fn commit(self) -> Result<CommitInfo> {
        let (read_sequence, read_set, writes, options) = self.core.into_commit_parts();
        self.db
            .commit_transaction_async(read_sequence, read_set, writes, options.write_options)
            .await
    }
}
