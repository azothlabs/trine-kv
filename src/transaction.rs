use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    bucket::DEFAULT_BUCKET_NAME,
    content::{
        CONTENT_TOKEN_BUCKET, ContentAttachment, ContentAttachmentScope, ContentChangeId,
        UploadToken, UploadTokenRecord, upload_token_key,
    },
    db::Db,
    error::{Error, Result},
    iterator::Iter,
    options::WriteOptions,
    types::{CommitInfo, KeyRange, ReadVersion, Sequence, Value},
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
/// assert!(commit.read_version().as_u64() > 0);
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
    content_token_consumptions: BTreeMap<Vec<u8>, ContentChangeId>,
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
            content_token_consumptions: BTreeMap::new(),
        }
    }

    /// Returns the public read version used by this transaction's read
    /// snapshot.
    ///
    /// All transaction reads use this read boundary, even if newer writes
    /// commit before the transaction commits.
    #[must_use]
    pub const fn read_version(&self) -> ReadVersion {
        ReadVersion::from_sequence(self.read_sequence)
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

    /// Reads the default-bucket range and returns its cursor, tracking the
    /// range for commit conflict checks.
    pub fn range_sync(&mut self, range: KeyRange) -> Result<Iter> {
        self.range_bucket_sync(DEFAULT_BUCKET_NAME, range)
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
        self.db.ensure_open()?;
        let bucket = bucket.into();
        let iter = self.db.range_at_sequence(
            &bucket,
            &range,
            self.read_sequence,
            crate::Direction::Forward,
        )?;
        self.range_reads.push(ReadRange { bucket, range });

        Ok(iter)
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

    /// Verifies and stages one upload-token consumption synchronously.
    ///
    /// The supplied scope must come from the authenticated database layer. A
    /// successful call only stages the transition; the token and all other
    /// writes in this transaction become durable together at commit. Dropping
    /// the transaction leaves an available token unchanged.
    ///
    /// Repeating a committed consumption with the same `change_id` returns the
    /// same claims. A different `ChangeId` cannot reuse the token.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UploadTokenInvalid`] for an unknown bearer token,
    /// [`Error::UploadTokenScopeMismatch`] for a wrong domain or owner,
    /// [`Error::UploadTokenExpired`] for expired available authority, or
    /// [`Error::UploadTokenAlreadyConsumed`] when another `ChangeId` owns it.
    /// Storage and format errors are propagated without staging consumption.
    pub fn consume_upload_token_sync(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
    ) -> Result<ContentAttachment> {
        let now_unix_ms = current_epoch_millis()?;
        self.consume_upload_token_at_sync(token, expected_scope, change_id, now_unix_ms)
    }

    pub(crate) fn consume_upload_token_at_sync(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
        now_unix_ms: u64,
    ) -> Result<ContentAttachment> {
        let key = upload_token_key(token);
        self.require_consistent_staged_token(&key, change_id)?;
        let bytes = match self.get_bucket_sync(CONTENT_TOKEN_BUCKET, &key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(Error::BucketMissing { .. }) => {
                return Err(Error::UploadTokenInvalid);
            }
            Err(error) => return Err(error),
        };
        let consumed = UploadTokenRecord::decode(&bytes, token)?.consume(
            expected_scope,
            change_id,
            now_unix_ms,
        )?;
        self.writes
            .put_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.content_token_consumptions.insert(key, change_id);
        Ok(consumed.attachment())
    }

    fn require_consistent_staged_token(
        &self,
        key: &[u8],
        change_id: ContentChangeId,
    ) -> Result<()> {
        if self
            .content_token_consumptions
            .get(key)
            .is_some_and(|existing| *existing != change_id)
        {
            return Err(Error::UploadTokenAlreadyConsumed);
        }
        Ok(())
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

    /// Reads the default-bucket range and returns its cursor, tracking the
    /// range for commit conflict checks.
    pub async fn range(&mut self, range: KeyRange) -> Result<Iter> {
        self.range_bucket(DEFAULT_BUCKET_NAME, range).await
    }

    /// Reads a named-bucket range and returns its cursor, tracking the range for
    /// commit conflict checks. The async counterpart of
    /// [`range_bucket_sync`](Self::range_bucket_sync).
    pub async fn range_bucket(
        &mut self,
        bucket: impl Into<String>,
        range: KeyRange,
    ) -> Result<Iter> {
        self.db.ensure_open()?;
        let bucket = bucket.into();
        let iter = self
            .db
            .range_at_sequence_async(
                &bucket,
                &range,
                self.read_sequence,
                crate::Direction::Forward,
            )
            .await?;
        self.range_reads.push(ReadRange { bucket, range });

        Ok(iter)
    }

    /// Verifies and stages one upload-token consumption asynchronously.
    ///
    /// This is the async counterpart of
    /// [`consume_upload_token_sync`](Self::consume_upload_token_sync). Token
    /// state and the caller's other staged writes share this transaction's one
    /// optimistic commit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentChangeId, ContentUploadOptions, Db, DbOptions,
    ///     OwnerScopeId, StorageDomainId, TransactionOptions,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let scope = ContentAttachmentScope::new(
    ///         StorageDomainId::from_bytes([1; 16]),
    ///         OwnerScopeId::from_bytes([2; 16]),
    ///     );
    ///     let mut upload = db
    ///         .begin_content_upload(ContentUploadOptions::new(
    ///             scope,
    ///             Duration::from_secs(3600),
    ///         ))
    ///         .await?;
    ///     upload.write(b"content bytes").await?;
    ///     let sealed = upload.seal().await?;
    ///
    ///     let mut transaction = db.transaction(TransactionOptions::default());
    ///     let claims = transaction
    ///         .consume_upload_token(
    ///             sealed.upload_token(),
    ///             scope,
    ///             ContentChangeId::from_bytes([3; 16]),
    ///         )
    ///         .await?;
    ///     assert_eq!(claims.content_id(), sealed.content_id());
    ///     transaction.put(b"catalog:file", b"attached");
    ///     transaction.commit().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn consume_upload_token(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
    ) -> Result<ContentAttachment> {
        let now_unix_ms = current_epoch_millis()?;
        self.consume_upload_token_at(token, expected_scope, change_id, now_unix_ms)
            .await
    }

    pub(crate) async fn consume_upload_token_at(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
        now_unix_ms: u64,
    ) -> Result<ContentAttachment> {
        let key = upload_token_key(token);
        self.require_consistent_staged_token(&key, change_id)?;
        let bytes = match self.get_bucket(CONTENT_TOKEN_BUCKET, &key).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(Error::BucketMissing { .. }) => {
                return Err(Error::UploadTokenInvalid);
            }
            Err(error) => return Err(error),
        };
        let consumed = UploadTokenRecord::decode(&bytes, token)?.consume(
            expected_scope,
            change_id,
            now_unix_ms,
        )?;
        self.writes
            .put_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.content_token_consumptions.insert(key, change_id);
        Ok(consumed.attachment())
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

fn current_epoch_millis() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Io(std::io::Error::other(error)))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| Error::invalid_options("system time milliseconds exceed u64::MAX"))
}
