use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    bucket::DEFAULT_BUCKET_NAME,
    content::{
        CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
        CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET, ContentAccessCoordinateRecord,
        ContentAccessMode, ContentAttachment, ContentAttachmentScope, ContentChangeId,
        ContentControlRecord, ContentDescriptor, ContentLeaseId, ContentLeaseRecord,
        ContentPhysicalHoldId, ContentPhysicalHoldRecord, ContentQuarantineRecord,
        ContentQuarantineStage, ContentReaderDrainAttestationRecord, ContentReclaimAuthorization,
        ContentReclaimIntentStage, ContentTokenIndexRecord, StorageDomainId, UploadToken,
        UploadTokenRecord, content_access_coordinate_key, content_control_key,
        content_lease_prefix, content_physical_hold_prefix, content_prefix_range,
        content_quarantine_key, content_reader_drain_attestation_key, content_token_index_key,
        content_token_index_prefix, upload_token_key,
    },
    db::Db,
    error::{ContentReclaimBlocker, Error, Result},
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
    /// Returns [`Error::InvalidOptions`] when `bucket` is empty or names the
    /// built-in default bucket, or when the staged value cannot be represented
    /// by this transaction's bounded batch format. Commit may later return
    /// [`Error::Conflict`] or the ordinary storage and durability errors.
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
        self.writes
            .put_bucket_with_commit_sequence(bucket, key, prefix, suffix)
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
        let attachment = consumed.attachment();
        self.stage_content_activity_sync(
            attachment.scope().storage_domain_id(),
            attachment.content_id(),
        )?;
        self.writes.delete_bucket(
            CONTENT_TOKEN_INDEX_BUCKET,
            content_token_index_key(
                attachment.scope().storage_domain_id(),
                attachment.content_id(),
                token,
            ),
        )?;
        self.writes
            .put_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.content_token_consumptions.insert(key, change_id);
        Ok(attachment)
    }

    pub(crate) fn stage_content_activity_sync(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_bucket_sync(CONTENT_CONTROL_BUCKET, &key)? {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_bucket_sync(CONTENT_CONTROL_BUCKET, &quarantine_key)? {
            ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id)?;
            self.writes
                .delete_bucket(CONTENT_CONTROL_BUCKET, quarantine_key)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
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
        let attachment = consumed.attachment();
        self.stage_content_activity(
            attachment.scope().storage_domain_id(),
            attachment.content_id(),
        )
        .await?;
        self.writes.delete_bucket(
            CONTENT_TOKEN_INDEX_BUCKET,
            content_token_index_key(
                attachment.scope().storage_domain_id(),
                attachment.content_id(),
                token,
            ),
        )?;
        self.writes
            .put_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.content_token_consumptions.insert(key, change_id);
        Ok(attachment)
    }

    pub(crate) async fn stage_content_activity(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_bucket(CONTENT_CONTROL_BUCKET, &key).await? {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
        {
            ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id)?;
            self.writes
                .delete_bucket(CONTENT_CONTROL_BUCKET, quarantine_key)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
    }

    pub(crate) async fn stage_content_read_activity(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
        {
            let quarantine =
                ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id)?;
            return Err(Error::ContentQuarantined {
                quarantined_at: quarantine.quarantined_at,
            });
        }
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_bucket(CONTENT_CONTROL_BUCKET, &key).await? {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
    }

    fn stage_active_content_control(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
        key: Vec<u8>,
    ) -> Result<()> {
        let active = ContentControlRecord::active(storage_domain_id, content_id);
        self.writes.put_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            key,
            &active.encode_prefix(),
            &[],
        )
    }

    /// Checks physical content state and stages durable reclaim intent.
    ///
    /// The higher layer must verify logical reachability, liveness, root
    /// generation, and the opaque proof token in this same transaction before
    /// calling this method. Trine KV independently validates that the sealed
    /// descriptor exists, the proof deadline has not passed, no later durable
    /// physical activity exists, and no unexpired upload authority or read
    /// lease remains. It then stages one protected per-content intent record.
    /// None of these steps deletes, relocates, or makes content unreadable.
    ///
    /// Upload-token publication or consumption and leased open or renewal all
    /// write the same per-content control key. A concurrent operation therefore
    /// either commits first and invalidates this transaction, or conflicts after
    /// this intent commits and must retry against the newer state.
    ///
    /// # Parameters
    ///
    /// - `authorization`: exact domain/content identity, opaque proof token,
    ///   stable verification sequence `S`, and exclusive Unix-millisecond
    ///   expiry supplied by the verified higher layer.
    ///
    /// # Returns
    ///
    /// [`ContentReclaimIntentStage::Staged`] means this transaction contains a
    /// new intent write; the intent is not durable until commit succeeds.
    /// `Existing` means the exact same intent was already durable and reports
    /// its acceptance sequence.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentReclaimBlocked`] with a typed
    /// [`ContentReclaimBlocker`] while unleased access is allowed, while a
    /// leased-only barrier lacks its protected coordinate, after proof expiry,
    /// newer physical activity, or while upload, lease, or physical-hold
    /// authority remains. Returns
    /// [`Error::ContentNotFound`] for a missing descriptor, and format,
    /// corruption, bucket, storage, or later commit-conflict errors otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAccessBarrierId, ContentAttachmentScope, ContentChangeId,
    ///     ContentReclaimAuthorization, ContentReclaimIntentStage, ContentReclaimProofToken,
    ///     ContentUploadOptions, Db, DbOptions, OwnerScopeId, StorageDomainId,
    ///     TransactionOptions,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let domain = StorageDomainId::from_bytes([1; 16]);
    ///     let scope = ContentAttachmentScope::new(
    ///         domain,
    ///         OwnerScopeId::from_bytes([2; 16]),
    ///     );
    ///     let mut upload = db
    ///         .begin_content_upload(ContentUploadOptions::new(
    ///             scope,
    ///             Duration::from_secs(60),
    ///         ))
    ///         .await?;
    ///     upload.write(b"reclaim example").await?;
    ///     let sealed = upload.seal().await?;
    ///     let mut attach = db.transaction(TransactionOptions::default());
    ///     attach
    ///         .consume_upload_token(
    ///             sealed.upload_token(),
    ///             scope,
    ///             ContentChangeId::from_bytes([3; 16]),
    ///         )
    ///         .await?;
    ///     attach.commit().await?;
    ///     db.enforce_content_leased_only(domain, ContentAccessBarrierId::generate()?)
    ///         .await?;
    ///
    ///     // A real caller obtains these opaque bytes only after its own exact
    ///     // logical reachability check at `transaction.read_version()`.
    ///     let mut transaction = db.transaction(TransactionOptions::default());
    ///     let authorization = ContentReclaimAuthorization::new(
    ///         domain,
    ///         sealed.content_id(),
    ///         ContentReclaimProofToken::from_bytes([4; 49]),
    ///         transaction.read_version(),
    ///         u64::MAX,
    ///     );
    ///     assert_eq!(
    ///         transaction
    ///             .stage_content_reclaim_intent(authorization)
    ///             .await?,
    ///         ContentReclaimIntentStage::Staged,
    ///     );
    ///     transaction.commit().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn stage_content_reclaim_intent(
        &mut self,
        authorization: ContentReclaimAuthorization,
    ) -> Result<ContentReclaimIntentStage> {
        let now_unix_ms = current_epoch_millis()?;
        if now_unix_ms >= authorization.expires_at_unix_ms() {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ProofExpired {
                    expired_at_unix_ms: authorization.expires_at_unix_ms(),
                },
            });
        }
        if authorization.verified_at().as_u64() == 0
            || authorization.verified_at().as_u64() > self.read_version().as_u64()
        {
            return Err(Error::invalid_options(
                "content reclaim verification sequence is invalid for this transaction",
            ));
        }
        self.db.bucket(CONTENT_CONTROL_BUCKET).await?;
        self.db.bucket(CONTENT_TOKEN_INDEX_BUCKET).await?;
        self.db.bucket(CONTENT_LEASE_BUCKET).await?;
        self.db.bucket(CONTENT_PHYSICAL_HOLD_BUCKET).await?;
        self.require_coordinated_content_access(authorization.storage_domain_id())
            .await?;
        let descriptor = self
            .db
            .read_content_descriptor(
                authorization.storage_domain_id(),
                authorization.content_id(),
            )
            .await?
            .ok_or_else(|| Error::ContentNotFound {
                storage_domain_id: authorization.storage_domain_id().to_string(),
                content_id: authorization.content_id().to_string(),
            })?;
        ContentDescriptor::decode(
            &descriptor,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;

        let control_key = content_control_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let control_bytes = self
            .get_bucket(CONTENT_CONTROL_BUCKET, &control_key)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: "sealed content is missing its physical control record".to_owned(),
            })?;
        let control = ContentControlRecord::decode(
            &control_bytes,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;
        let physical_activity = control.physical_activity_commit_seq();
        if physical_activity > authorization.verified_at().as_u64() {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded {
                    activity_at_commit_seq: physical_activity,
                    verified_at_commit_seq: authorization.verified_at().as_u64(),
                },
            });
        }

        self.require_no_active_content_token(authorization, now_unix_ms)
            .await?;
        self.require_no_active_content_lease(authorization, now_unix_ms)
            .await?;
        self.require_no_active_content_physical_hold(authorization, now_unix_ms)
            .await?;
        if control.matches_authorization(authorization) {
            let accepted_at = control.accepted_at().ok_or_else(|| Error::Corruption {
                message: "matching reclaim intent has no acceptance sequence".to_owned(),
            })?;
            return Ok(ContentReclaimIntentStage::Existing { accepted_at });
        }

        let intent = control.reclaim_intent(authorization);
        self.writes.put_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            control_key,
            &intent.encode_prefix(),
            &[],
        )?;
        Ok(ContentReclaimIntentStage::Staged)
    }

    /// Rechecks accepted reclaim state and stages a durable content quarantine.
    ///
    /// The higher layer must revalidate its exact logical proof, liveness, and
    /// retained-root generation in this same transaction before calling this
    /// method. Trine KV then requires the exact accepted reclaim intent, the
    /// matching leased-only barrier and reader-drain attestation, a valid
    /// descriptor, and fresh absence of upload authority, read leases, and
    /// physical holds. Every read joins the optimistic conflict set.
    ///
    /// A committed quarantine blocks new leased opens but leaves the descriptor
    /// and every content byte intact. Attachment/token or physical-hold activity
    /// may atomically remove quarantine and return control to Active. This method
    /// does not start a grace timer and does not authorize or perform deletion.
    ///
    /// # Returns
    ///
    /// [`ContentQuarantineStage::Staged`] means the quarantine write is staged
    /// but not durable until this transaction commits. `Existing` reports the
    /// commit coordinate of an exact already-durable quarantine.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentReclaimBlocked`] with a typed blocker when the
    /// barrier is absent or uncoordinated, reader drain is not attested, the
    /// exact intent is missing, the proof expired or was superseded, or active
    /// token, lease, or hold authority remains. Missing/malformed protected
    /// records, descriptor errors, and later commit conflicts fail closed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAccessBarrierId, ContentAttachmentScope, ContentChangeId,
    ///     ContentQuarantineStage, ContentReaderDrainAttestationId,
    ///     ContentReaderDrainAttestationOptions, ContentReaderDrainCoordinatorId,
    ///     ContentReaderDrainEvidenceDigest, ContentReaderDrainKind,
    ///     ContentReclaimAuthorization, ContentReclaimProofToken, ContentUploadOptions, Db,
    ///     DbOptions, OwnerScopeId, StorageDomainId, TransactionOptions,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let domain = StorageDomainId::from_bytes([1; 16]);
    ///     let scope = ContentAttachmentScope::new(domain, OwnerScopeId::from_bytes([2; 16]));
    ///     let mut upload = db
    ///         .begin_content_upload(ContentUploadOptions::new(
    ///             scope,
    ///             Duration::from_secs(60),
    ///         ))
    ///         .await?;
    ///     upload.write(b"quarantine example").await?;
    ///     let sealed = upload.seal().await?;
    ///     let mut attach = db.transaction(TransactionOptions::default());
    ///     attach
    ///         .consume_upload_token(
    ///             sealed.upload_token(),
    ///             scope,
    ///             ContentChangeId::from_bytes([3; 16]),
    ///         )
    ///         .await?;
    ///     attach.commit().await?;
    ///     let barrier = db
    ///         .enforce_content_leased_only(domain, ContentAccessBarrierId::generate()?)
    ///         .await?;
    ///     db.attest_content_reader_drain(
    ///         barrier,
    ///         ContentReaderDrainAttestationId::generate()?,
    ///         ContentReaderDrainAttestationOptions::new(
    ///             ContentReaderDrainKind::DomainBootstrap,
    ///             ContentReaderDrainCoordinatorId::from_bytes([4; 16]),
    ///             ContentReaderDrainEvidenceDigest::for_bytes(b"retained deployment evidence"),
    ///         ),
    ///     )
    ///     .await?;
    ///
    ///     // A real higher layer supplies this only after exact logical absence.
    ///     let mut intent = db.transaction(TransactionOptions::default());
    ///     let authorization = ContentReclaimAuthorization::new(
    ///         domain,
    ///         sealed.content_id(),
    ///         ContentReclaimProofToken::from_bytes([5; 49]),
    ///         intent.read_version(),
    ///         u64::MAX,
    ///     );
    ///     intent.stage_content_reclaim_intent(authorization).await?;
    ///     intent.commit().await?;
    ///
    ///     // The higher layer repeats its logical checks in this transaction.
    ///     let mut quarantine = db.transaction(TransactionOptions::default());
    ///     assert_eq!(
    ///         quarantine.stage_content_quarantine(authorization).await?,
    ///         ContentQuarantineStage::Staged,
    ///     );
    ///     quarantine.commit().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn stage_content_quarantine(
        &mut self,
        authorization: ContentReclaimAuthorization,
    ) -> Result<ContentQuarantineStage> {
        let now_unix_ms = current_epoch_millis()?;
        if now_unix_ms >= authorization.expires_at_unix_ms() {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ProofExpired {
                    expired_at_unix_ms: authorization.expires_at_unix_ms(),
                },
            });
        }
        if authorization.verified_at().as_u64() == 0
            || authorization.verified_at().as_u64() > self.read_version().as_u64()
        {
            return Err(Error::invalid_options(
                "content quarantine verification sequence is invalid for this transaction",
            ));
        }
        self.db.bucket(CONTENT_CONTROL_BUCKET).await?;
        self.db.bucket(CONTENT_TOKEN_INDEX_BUCKET).await?;
        self.db.bucket(CONTENT_LEASE_BUCKET).await?;
        self.db.bucket(CONTENT_PHYSICAL_HOLD_BUCKET).await?;

        let access = self
            .require_coordinated_content_access(authorization.storage_domain_id())
            .await?;
        let drain = self
            .require_content_reader_drain_attestation(access)
            .await?;
        let intent_accepted_at = self
            .require_exact_content_reclaim_intent(authorization)
            .await?;

        self.require_no_active_content_token(authorization, now_unix_ms)
            .await?;
        self.require_no_active_content_lease(authorization, now_unix_ms)
            .await?;
        self.require_no_active_content_physical_hold(authorization, now_unix_ms)
            .await?;

        let quarantine_key = content_quarantine_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        if let Some(bytes) = self
            .get_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
        {
            let existing = ContentQuarantineRecord::decode(
                &bytes,
                authorization.storage_domain_id(),
                authorization.content_id(),
            )?;
            if existing.matches_authorization(authorization)
                && existing.intent_accepted_at == intent_accepted_at
                && existing.barrier_id == access.barrier_id
                && existing.barrier_enforced_at == access.enforced_at
                && existing.drain_attestation_id == drain.attestation_id
            {
                return Ok(ContentQuarantineStage::Existing {
                    quarantined_at: existing.quarantined_at,
                });
            }
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReclaimIntentRequired,
            });
        }

        let requested =
            ContentQuarantineRecord::requested(authorization, intent_accepted_at, access, drain);
        self.writes.put_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            quarantine_key,
            &requested.encode_prefix(),
            &[],
        )?;
        Ok(ContentQuarantineStage::Staged)
    }

    async fn require_exact_content_reclaim_intent(
        &mut self,
        authorization: ContentReclaimAuthorization,
    ) -> Result<crate::ReadVersion> {
        let descriptor = self
            .db
            .read_content_descriptor(
                authorization.storage_domain_id(),
                authorization.content_id(),
            )
            .await?
            .ok_or_else(|| Error::ContentNotFound {
                storage_domain_id: authorization.storage_domain_id().to_string(),
                content_id: authorization.content_id().to_string(),
            })?;
        ContentDescriptor::decode(
            &descriptor,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;

        let key = content_control_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let bytes = self
            .get_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: "sealed content is missing its physical control record".to_owned(),
            })?;
        let control = ContentControlRecord::decode(
            &bytes,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;
        if !control.matches_authorization(authorization) {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReclaimIntentRequired,
            });
        }
        let accepted_at = control.accepted_at().ok_or_else(|| Error::Corruption {
            message: "matching reclaim intent has no acceptance sequence".to_owned(),
        })?;
        let physical_activity = control.physical_activity_commit_seq();
        if physical_activity > authorization.verified_at().as_u64() {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded {
                    activity_at_commit_seq: physical_activity,
                    verified_at_commit_seq: authorization.verified_at().as_u64(),
                },
            });
        }
        Ok(accepted_at)
    }

    async fn require_coordinated_content_access(
        &mut self,
        storage_domain_id: StorageDomainId,
    ) -> Result<ContentAccessCoordinateRecord> {
        let barrier_id = match self.db.content_access_mode(storage_domain_id).await? {
            ContentAccessMode::CompatibleUnleased => {
                return Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::UnleasedAccessAllowed,
                });
            }
            ContentAccessMode::LeasedOnly { barrier_id } => barrier_id,
        };
        let access_key = content_access_coordinate_key(storage_domain_id);
        let Some(access_bytes) = self.get_bucket(CONTENT_CONTROL_BUCKET, &access_key).await? else {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::LeasedOnlyBarrierUncoordinated { barrier_id },
            });
        };
        let access = ContentAccessCoordinateRecord::decode(&access_bytes, storage_domain_id)?;
        if access.barrier_id != barrier_id {
            return Err(Error::Corruption {
                message: "content access barrier differs from reclaim coordinate".to_owned(),
            });
        }
        Ok(access)
    }

    async fn require_content_reader_drain_attestation(
        &mut self,
        access: ContentAccessCoordinateRecord,
    ) -> Result<ContentReaderDrainAttestationRecord> {
        let key = content_reader_drain_attestation_key(access.storage_domain_id);
        let Some(bytes) = self.get_bucket(CONTENT_CONTROL_BUCKET, &key).await? else {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReaderDrainNotAttested {
                    barrier_id: access.barrier_id,
                },
            });
        };
        let drain = ContentReaderDrainAttestationRecord::decode(&bytes, access.storage_domain_id)?;
        if drain.barrier_id != access.barrier_id || drain.barrier_enforced_at != access.enforced_at
        {
            return Err(Error::Corruption {
                message: "reader-drain attestation differs from the active barrier coordinate"
                    .to_owned(),
            });
        }
        Ok(drain)
    }

    async fn require_no_active_content_token(
        &mut self,
        authorization: ContentReclaimAuthorization,
        now_unix_ms: u64,
    ) -> Result<()> {
        let prefix = content_token_index_prefix(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let range = content_prefix_range(prefix.clone())?;
        for entry in self.range_bucket(CONTENT_TOKEN_INDEX_BUCKET, range).await? {
            let entry = entry?;
            let hash: [u8; 32] = entry
                .key
                .get(prefix.len()..)
                .ok_or_else(|| Error::Corruption {
                    message: "content token-index key is shorter than its content prefix"
                        .to_owned(),
                })?
                .try_into()
                .map_err(|_| Error::Corruption {
                    message: "content token-index key has a malformed hash length".to_owned(),
                })?;
            let token = ContentTokenIndexRecord::decode(
                &entry.value,
                authorization.storage_domain_id(),
                authorization.content_id(),
                hash,
            )?;
            if now_unix_ms < token.expires_at_unix_ms() {
                return Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::UploadToken {
                        expires_at_unix_ms: token.expires_at_unix_ms(),
                    },
                });
            }
        }
        Ok(())
    }

    async fn require_no_active_content_lease(
        &mut self,
        authorization: ContentReclaimAuthorization,
        now_unix_ms: u64,
    ) -> Result<()> {
        let prefix = content_lease_prefix(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let range = content_prefix_range(prefix.clone())?;
        for entry in self.range_bucket(CONTENT_LEASE_BUCKET, range).await? {
            let entry = entry?;
            let lease_id = ContentLeaseId::from_bytes(
                entry
                    .key
                    .get(prefix.len()..)
                    .ok_or_else(|| Error::Corruption {
                        message: "content lease key is shorter than its content prefix".to_owned(),
                    })?
                    .try_into()
                    .map_err(|_| Error::Corruption {
                        message: "content lease key has a malformed identity length".to_owned(),
                    })?,
            )?;
            let lease = ContentLeaseRecord::decode(
                &entry.value,
                authorization.storage_domain_id(),
                authorization.content_id(),
                lease_id,
            )?;
            if now_unix_ms < lease.expires_at_unix_ms {
                return Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::ReadLease {
                        expires_at_unix_ms: lease.expires_at_unix_ms,
                    },
                });
            }
        }
        Ok(())
    }

    async fn require_no_active_content_physical_hold(
        &mut self,
        authorization: ContentReclaimAuthorization,
        now_unix_ms: u64,
    ) -> Result<()> {
        let prefix = content_physical_hold_prefix(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let range = content_prefix_range(prefix.clone())?;
        for entry in self
            .range_bucket(CONTENT_PHYSICAL_HOLD_BUCKET, range)
            .await?
        {
            let entry = entry?;
            let hold_id = ContentPhysicalHoldId::from_bytes(
                entry
                    .key
                    .get(prefix.len()..)
                    .ok_or_else(|| Error::Corruption {
                        message: "content physical-hold key is shorter than its content prefix"
                            .to_owned(),
                    })?
                    .try_into()
                    .map_err(|_| Error::Corruption {
                        message: "content physical-hold key has a malformed identity length"
                            .to_owned(),
                    })?,
            )?;
            let hold = ContentPhysicalHoldRecord::decode(
                &entry.value,
                authorization.storage_domain_id(),
                authorization.content_id(),
                hold_id,
            )?;
            if hold.is_active_at(now_unix_ms) {
                return Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::PhysicalHold {
                        hold_id,
                        kind: hold.kind,
                        expires_at_unix_ms: (hold.expires_at_unix_ms != 0)
                            .then_some(hold.expires_at_unix_ms),
                    },
                });
            }
        }
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

fn current_epoch_millis() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Io(std::io::Error::other(error)))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| Error::invalid_options("system time milliseconds exceed u64::MAX"))
}
