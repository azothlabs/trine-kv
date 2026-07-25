use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET,
    ContentDescriptor, ContentId, ContentPhysicalAccountRecord, ContentPhysicalQuota,
    ContentPhysicalReservationRecord, ContentTokenIndexRecord, ContentUpload, ContentUploadInfo,
    ContentUploadMaintenanceReport, ContentUploadOptions, ContentUploadResume, Db, DurabilityMode,
    Error, Result, SealedContent, Sha256, StorageDomainId, TransactionOptions, UploadId,
    UploadSessionState, UploadSessionStatus, UploadToken, UploadTokenRecord, WriteOptions,
    content_physical_account_key, content_physical_quota_key, content_physical_reservation_key,
    content_token_index_key, current_epoch_millis, initial_upload_reservation, upload_token_key,
};
use sha2::Digest;

impl Db {
    /// Lists every durable upload state known to this database.
    ///
    /// The result is ordered by [`UploadId`]. It includes open, sealing,
    /// sealed, and aborting states so an operator can distinguish resumable
    /// work from retained idempotency records. Listing is read-only and does
    /// not reserve quota, resume sealing, or delete chunks.
    ///
    /// # Errors
    ///
    /// Returns storage, listing, decoding, and integrity errors. A malformed
    /// state fails the complete listing instead of being silently skipped.
    pub async fn list_content_uploads(&self) -> Result<Vec<ContentUploadInfo>> {
        self.ensure_open()?;
        self.list_upload_states().await.map(|states| {
            states
                .into_iter()
                .map(UploadSessionState::maintenance_info)
                .collect()
        })
    }

    /// Removes open or aborting uploads whose last durable update precedes
    /// `inactive_before_unix_ms`.
    ///
    /// Each candidate is locked and reread before cleanup. A concurrent append,
    /// seal, resume, or abort therefore either updates the timestamp or changes
    /// lifecycle and prevents stale cleanup. Successful cleanup deletes chunks,
    /// releases the exact upload reservation, and removes the state object.
    /// Sealing and sealed states are never discarded by this method.
    ///
    /// # Parameters
    ///
    /// - `inactive_before_unix_ms`: exclusive Unix-millisecond cutoff. A state
    ///   updated exactly at the cutoff is retained.
    ///
    /// # Errors
    ///
    /// Returns read-only, storage, quota-accounting, decoding, or cleanup
    /// errors. The pass is idempotent; retrying continues from durable state.
    pub async fn reap_inactive_content_uploads(
        &self,
        inactive_before_unix_ms: u64,
    ) -> Result<ContentUploadMaintenanceReport> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let candidates = self.list_upload_states().await?;
        let mut report = ContentUploadMaintenanceReport::default();
        for candidate in candidates {
            report.scanned = report.scanned.saturating_add(1);
            if candidate.updated_at_unix_ms() >= inactive_before_unix_ms {
                continue;
            }

            let upload_id = candidate.upload_id();
            let _upload = self.lock_content_upload(upload_id).await;
            let current = match self.require_upload_state(upload_id).await {
                Ok(state) => state,
                Err(Error::ContentUploadNotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            if current.updated_at_unix_ms() >= inactive_before_unix_ms {
                continue;
            }
            if matches!(
                current.status(),
                UploadSessionStatus::Open | UploadSessionStatus::Aborting
            ) {
                self.discard_open_upload(&current).await?;
                report.aborted = report.aborted.saturating_add(1);
            }
        }
        Ok(report)
    }

    /// Removes sealed upload state older than an exclusive cutoff.
    ///
    /// This only removes the upload-idempotency record. The immutable content
    /// descriptor, chunks selected by that descriptor, attachment token, and
    /// quota accounting remain unchanged. After pruning, retrying seal or resume
    /// by the old `UploadId` returns [`Error::ContentUploadNotFound`].
    ///
    /// Sealing records are retained because they may still need crash recovery.
    /// # Errors
    ///
    /// Returns read-only, storage, listing, decoding, or deletion errors. Every
    /// successful deletion is final even if a later candidate fails; retrying
    /// the same cutoff is safe.
    pub async fn prune_sealed_content_uploads(
        &self,
        sealed_before_unix_ms: u64,
    ) -> Result<ContentUploadMaintenanceReport> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let candidates = self.list_upload_states().await?;
        let mut report = ContentUploadMaintenanceReport::default();
        for candidate in candidates {
            report.scanned = report.scanned.saturating_add(1);
            if candidate.updated_at_unix_ms() >= sealed_before_unix_ms {
                continue;
            }

            let upload_id = candidate.upload_id();
            let _upload = self.lock_content_upload(upload_id).await;
            let current = match self.require_upload_state(upload_id).await {
                Ok(state) => state,
                Err(Error::ContentUploadNotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            if current.updated_at_unix_ms() < sealed_before_unix_ms
                && matches!(current.status(), UploadSessionStatus::Sealed(_))
            {
                self.delete_upload_state(upload_id).await?;
                report.pruned_sealed = report.pruned_sealed.saturating_add(1);
            }
        }
        Ok(report)
    }

    pub(super) const CONTENT_ACCESS_COMMIT_ATTEMPTS: usize = 8;
    pub(super) const CONTENT_LEASE_COMMIT_ATTEMPTS: usize = 8;
    pub(super) const CONTENT_PHYSICAL_HOLD_COMMIT_ATTEMPTS: usize = 8;

    /// Sets or clears the original-byte physical quota for a storage domain.
    ///
    /// Unique sealed content and unfinished upload reservations are tracked
    /// independently and summed for enforcement. Lowering the limit below the
    /// currently accounted total is rejected. The counter deliberately excludes
    /// framing, encryption, provider-version and replica overhead in v1.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentPhysicalQuotaExceeded`] when `limit` is below
    /// current use, or a storage/conflict/corruption error while protected
    /// accounting state is read or committed.
    pub async fn set_content_physical_quota(
        &self,
        storage_domain_id: StorageDomainId,
        limit: Option<u64>,
    ) -> Result<ContentPhysicalQuota> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.internal_bucket(CONTENT_CONTROL_BUCKET).await?;
        let _quota = self.lock_content_quota(storage_domain_id).await;
        let mut transaction = self.transaction(TransactionOptions::default());
        let current = self
            .read_content_physical_quota(&mut transaction, storage_domain_id)
            .await?;
        if let Some(limit) = limit {
            if current.accounted_bytes() > limit {
                return Err(Error::ContentPhysicalQuotaExceeded {
                    limit,
                    unique_content_bytes: current.unique_content_bytes(),
                    upload_reserved_bytes: current.upload_reserved_bytes(),
                    requested_bytes: 0,
                });
            }
        }
        let next = current.with_limit(limit);
        transaction.put_internal_bucket(
            CONTENT_CONTROL_BUCKET,
            content_physical_quota_key(storage_domain_id),
            next.encode(),
        )?;
        transaction.commit().await?;
        Ok(next)
    }

    /// Reads physical content-byte use and its optional limit.
    ///
    /// An unseen storage domain reports zero use and no limit without creating
    /// durable state.
    ///
    /// # Errors
    ///
    /// Returns a storage or protected-record format error.
    pub async fn content_physical_quota(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> Result<ContentPhysicalQuota> {
        self.ensure_open()?;
        self.internal_bucket(CONTENT_CONTROL_BUCKET).await?;
        let mut transaction = self.transaction(TransactionOptions::default());
        self.read_content_physical_quota(&mut transaction, storage_domain_id)
            .await
    }

    /// Returns the durability level recorded by newly sealed content.
    ///
    /// Persistent filesystem databases use their configured publish
    /// durability. In-memory, WASI, browser and object-store host backends
    /// currently report `Flush`; this is an observed backend result, not a
    /// replica-count or provider-retention promise.
    #[must_use]
    pub fn content_durability_mode(&self) -> DurabilityMode {
        self.content_durability()
    }

    /// Starts a bounded-memory upload for one immutable `ContentObject`.
    ///
    /// The upload is independent of key/value transactions and is not visible
    /// through [`open_content`](Self::open_content) until
    /// [`ContentUpload::seal`] publishes its descriptor. `options.chunk_bytes()`
    /// bounds retained unsealed payload memory; calls to `write` may use any
    /// input size.
    ///
    /// This storage-layer API does not create a higher-level File or consume an
    /// attachment token. Ordinary Blob values continue to use the key/value
    /// path.
    ///
    /// # Parameters
    ///
    /// - `options`: attachment scope, token lifetime, chunk bound, and optional
    ///   expected original length and `ContentId`. Chunk bounds outside 64 KiB
    ///   through 16 MiB and token lifetimes below one millisecond are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`], [`Error::ReadOnly`],
    /// [`Error::InvalidOptions`] for an invalid chunk bound, or
    /// [`Error::UnsupportedBackend`] when the selected host backend does not
    /// yet implement content objects. Backend failures may also be returned
    /// while creating the initial durable session record.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentUploadOptions, Db, DbOptions, OwnerScopeId,
    ///     StorageDomainId,
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
    ///     upload.write(b"immutable bytes").await?;
    ///     let sealed = upload.seal().await?;
    ///
    ///     let content = db
    ///         .open_content(scope.storage_domain_id(), sealed.content_id())
    ///         .await?;
    ///     assert_eq!(&*content.read_range(0, 9).await?, b"immutable");
    ///     Ok(())
    /// }
    /// ```
    pub async fn begin_content_upload(
        &self,
        options: ContentUploadOptions,
    ) -> Result<ContentUpload> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let options = options.validate()?;
        let upload_id = UploadId::generate()?;
        let upload_token = UploadToken::generate()?;
        let _upload = self.lock_content_upload(upload_id).await;
        let state = UploadSessionState::initial(upload_id, options, upload_token)?;
        self.write_upload_state(&state).await?;
        if let Err(error) = self
            .reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
            .await
        {
            let _ = self.discard_open_upload(&state).await;
            return Err(error);
        }
        Ok(ContentUpload::new(
            self.clone(),
            upload_id,
            options,
            Vec::with_capacity(options.chunk_bytes()),
            0,
            0,
            0,
        ))
    }

    /// Begins or resumes a durable upload under a caller-supplied [`UploadId`].
    ///
    /// This is the idempotent counterpart to [`Db::begin_content_upload`].
    /// Generate and persist `upload_id` before a remote or otherwise uncertain
    /// request. The first successful call creates the same bounded-memory
    /// sequential writer as `begin_content_upload`. An exact retry returns the
    /// current open writer at its durable original-byte length, or the exact
    /// prior [`SealedContent`] after sealing.
    ///
    /// An upload identity is permanently bound to its first options. Reusing it
    /// with a different attachment scope, token lifetime, chunk size, expected
    /// length, or expected `ContentId` fails without changing existing state.
    /// Concurrent append, seal, or abort operations remain serialized by the
    /// same upload lock; after the identity/options check this method delegates
    /// recovery to [`Db::resume_content_upload`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] for invalid options or an identity
    /// already bound to different options, [`Error::Closed`] or
    /// [`Error::ReadOnly`] when writes are unavailable, and typed backend,
    /// integrity, or recovery errors. An aborted identity is absent and may be
    /// started again; callers that require permanent request identity should not
    /// retry begin after a confirmed abort.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentUploadOptions, ContentUploadResume, Db, DbOptions,
    ///     OwnerScopeId, StorageDomainId, UploadId,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let upload_id = UploadId::new()?;
    ///     let options = ContentUploadOptions::new(
    ///         ContentAttachmentScope::new(
    ///             StorageDomainId::from_bytes([1; 16]),
    ///             OwnerScopeId::from_bytes([2; 16]),
    ///         ),
    ///         Duration::from_secs(60),
    ///     );
    ///     let first = db.begin_content_upload_with_id(upload_id, options).await?;
    ///     assert!(matches!(first, ContentUploadResume::Open(_)));
    ///     let retry = db.begin_content_upload_with_id(upload_id, options).await?;
    ///     assert!(matches!(retry, ContentUploadResume::Open(_)));
    ///     Ok(())
    /// }
    /// ```
    pub async fn begin_content_upload_with_id(
        &self,
        upload_id: UploadId,
        options: ContentUploadOptions,
    ) -> Result<ContentUploadResume> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let options = options.validate()?;
        let upload_guard = self.lock_content_upload(upload_id).await;
        let object = self.content_upload_state_object(upload_id)?;
        if let Some(bytes) = self.read_content_object(object).await? {
            let state = UploadSessionState::decode(&bytes, upload_id)?;
            if state.status() == UploadSessionStatus::Aborting {
                self.discard_open_upload(&state).await?;
            } else {
                if state.options() != options {
                    return Err(Error::invalid_options(format!(
                        "content upload {upload_id} is already bound to different options"
                    )));
                }
                if state.status() == UploadSessionStatus::Open {
                    self.reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
                        .await?;
                }
                drop(upload_guard);
                return self.resume_content_upload(upload_id).await;
            }
        }

        let upload_token = UploadToken::generate()?;
        let state = UploadSessionState::initial(upload_id, options, upload_token)?;
        self.write_upload_state(&state).await?;
        if let Err(error) = self
            .reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
            .await
        {
            let _ = self.discard_open_upload(&state).await;
            return Err(error);
        }
        Ok(ContentUploadResume::Open(ContentUpload::new(
            self.clone(),
            upload_id,
            options,
            Vec::with_capacity(options.chunk_bytes()),
            0,
            0,
            0,
        )))
    }

    /// Resumes durable upload state by [`UploadId`].
    ///
    /// Open state returns a writer positioned at its durable original-byte
    /// length. A partial chunk is reloaded and verified into a buffer no larger
    /// than the configured chunk bound. A session interrupted while issuing its
    /// attachment token completes seal recovery. Already sealed state returns
    /// the exact prior [`SealedContent`] instead of reopening a writer.
    ///
    /// A write publishes chunk bytes before advancing the session revision. If
    /// a crash leaves a newer partial frame than the session record, resume
    /// verifies that frame and keeps only the prefix named by the durable state.
    /// Callers should therefore continue from `ContentUpload::len()`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentUploadNotFound`] for an unknown or aborted
    /// identity, or a storage/format/integrity error when durable state or its
    /// partial chunk cannot be trusted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentUploadOptions, ContentUploadResume, Db, DbOptions,
    ///     OwnerScopeId, StorageDomainId,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let scope = ContentAttachmentScope::new(
    ///         StorageDomainId::from_bytes([1; 16]),
    ///         OwnerScopeId::from_bytes([2; 16]),
    ///     );
    ///     let mut first = db
    ///         .begin_content_upload(ContentUploadOptions::new(
    ///             scope,
    ///             Duration::from_secs(3600),
    ///         ))
    ///         .await?;
    ///     first.write(b"confirmed prefix").await?;
    ///     let upload_id = first.upload_id();
    ///     drop(first);
    ///
    ///     let mut resumed = match db.resume_content_upload(upload_id).await? {
    ///         ContentUploadResume::Open(upload) => upload,
    ///         ContentUploadResume::Sealed(sealed) => {
    ///             assert_eq!(sealed.len(), 16);
    ///             return Ok(());
    ///         }
    ///     };
    ///     assert_eq!(resumed.len(), 16);
    ///     resumed.write(b" and suffix").await?;
    ///     let sealed = resumed.seal().await?;
    ///     assert_eq!(db.seal_content_upload(upload_id).await?, sealed);
    ///     Ok(())
    /// }
    /// ```
    pub async fn resume_content_upload(&self, upload_id: UploadId) -> Result<ContentUploadResume> {
        self.ensure_open()?;
        let upload_guard = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        match state.status() {
            UploadSessionStatus::Sealed(sealed) => Ok(ContentUploadResume::Sealed(sealed)),
            UploadSessionStatus::Sealing(_) => {
                drop(upload_guard);
                self.seal_content_upload(upload_id)
                    .await
                    .map(ContentUploadResume::Sealed)
            }
            UploadSessionStatus::Open => {
                self.reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
                    .await?;
                let mut buffer = Vec::with_capacity(state.options().chunk_bytes());
                if state.partial_len() != 0 {
                    let frame = self
                        .read_content_chunk(upload_id, state.complete_chunks())
                        .await?
                        .ok_or_else(|| Error::Corruption {
                            message: format!(
                                "content upload {upload_id} is missing its partial chunk"
                            ),
                        })?;
                    let payload =
                        crate::content::decode_chunk(&frame, upload_id, state.complete_chunks())?;
                    let durable_len =
                        usize::try_from(state.partial_len()).map_err(|_| Error::InvalidFormat {
                            message: "content partial length exceeds usize".to_owned(),
                        })?;
                    let durable = payload.get(..durable_len).ok_or_else(|| Error::Corruption {
                        message: format!(
                            "content upload {upload_id} partial chunk is shorter than durable state"
                        ),
                    })?;
                    buffer.extend_from_slice(durable);
                }
                Ok(ContentUploadResume::Open(ContentUpload::new(
                    self.clone(),
                    upload_id,
                    state.options(),
                    buffer,
                    state.length(),
                    state.complete_chunks(),
                    state.revision(),
                )))
            }
            UploadSessionStatus::Aborting => {
                self.discard_open_upload(&state).await?;
                Err(Error::ContentUploadNotFound {
                    upload_id: upload_id.to_string(),
                })
            }
        }
    }

    /// Seals an upload idempotently by durable [`UploadId`].
    ///
    /// The complete SHA-256 is recomputed from verified durable chunks, so this
    /// works after process restart without serializing hash-library internals.
    /// Descriptor publication happens first, followed by a durable `sealing`
    /// checkpoint, one transactional token record, and the final `sealed`
    /// checkpoint. A retry after any crash observes those records and returns
    /// the same bearer token, expiry, identity, length, and durability result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentUploadNotFound`], a typed expected length/digest
    /// mismatch, or a storage/format/integrity error. An expectation mismatch
    /// aborts the session. Other failures before descriptor publication leave
    /// the upload open and resumable.
    pub async fn seal_content_upload(&self, upload_id: UploadId) -> Result<SealedContent> {
        self.seal_content_upload_at(upload_id, None).await
    }

    pub(crate) async fn seal_content_upload_at(
        &self,
        upload_id: UploadId,
        expected_revision: Option<u64>,
    ) -> Result<SealedContent> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let _upload = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        if let Some(expected_revision) = expected_revision {
            state.require_open_revision(expected_revision)?;
        }
        if state.status() == UploadSessionStatus::Aborting {
            return Err(Error::ContentUploadNotFound {
                upload_id: upload_id.to_string(),
            });
        }
        if let UploadSessionStatus::Sealed(sealed) = state.status() {
            let descriptor = self
                .read_content_descriptor(sealed.storage_domain_id(), sealed.content_id())
                .await?
                .ok_or_else(|| Error::ContentNotFound {
                    storage_domain_id: sealed.storage_domain_id().to_string(),
                    content_id: sealed.content_id().to_string(),
                })?;
            ContentDescriptor::decode(
                &descriptor,
                sealed.storage_domain_id(),
                sealed.content_id(),
            )?;
            return Ok(sealed);
        }
        let (sealing_state, sealed, reused) = self.prepare_upload_seal(&state).await?;

        if reused {
            self.cleanup_upload_chunks(&sealing_state).await?;
        }

        self.finalize_content_upload_quota(&sealing_state, sealed)
            .await?;

        self.ensure_upload_token_record(upload_id, sealed).await?;
        let sealed_state = sealing_state.into_sealed()?;
        self.write_upload_state(&sealed_state).await?;
        Ok(sealed)
    }

    async fn prepare_upload_seal(
        &self,
        state: &UploadSessionState,
    ) -> Result<(UploadSessionState, SealedContent, bool)> {
        let upload_id = state.upload_id();
        match state.status() {
            UploadSessionStatus::Sealed(_) => Err(Error::InvalidFormat {
                message: "sealed upload entered seal preparation".to_owned(),
            }),
            UploadSessionStatus::Sealing(sealed) => {
                let descriptor = self
                    .read_content_descriptor(sealed.storage_domain_id(), sealed.content_id())
                    .await?
                    .ok_or_else(|| Error::Corruption {
                        message: format!(
                            "sealing upload {upload_id} is missing content descriptor {}",
                            sealed.content_id()
                        ),
                    })?;
                let descriptor = ContentDescriptor::decode(
                    &descriptor,
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                )?;
                Ok((*state, sealed, descriptor.upload_id() != upload_id))
            }
            UploadSessionStatus::Open => self.prepare_open_upload_seal(state).await,
            UploadSessionStatus::Aborting => Err(Error::ContentUploadNotFound {
                upload_id: upload_id.to_string(),
            }),
        }
    }

    async fn prepare_open_upload_seal(
        &self,
        state: &UploadSessionState,
    ) -> Result<(UploadSessionState, SealedContent, bool)> {
        let upload_id = state.upload_id();
        let content_id = self.hash_upload_state(state).await?;
        if let Some(expected) = state.options().expected_length()
            && expected != state.length()
        {
            self.discard_open_upload(state).await?;
            return Err(Error::ContentLengthMismatch {
                expected,
                actual: state.length(),
            });
        }
        if let Some(expected) = state.options().expected_content_id()
            && expected != content_id
        {
            self.discard_open_upload(state).await?;
            return Err(Error::ContentDigestMismatch {
                expected: expected.to_string(),
                actual: content_id.to_string(),
            });
        }

        let expires_at = current_epoch_millis()?
            .checked_add(state.options().token_ttl_ms()?)
            .ok_or_else(|| Error::invalid_options("upload token expiry overflow"))?;
        let storage_domain_id = state.options().attachment_scope().storage_domain_id();
        let descriptor = ContentDescriptor::new(
            storage_domain_id,
            content_id,
            upload_id,
            state.length(),
            state.options().chunk_bytes(),
            state.chunk_count(),
        )?;
        let seal_guard = self.lock_content_seal().await;
        self.require_content_descriptor_publication_allowed(storage_domain_id, content_id)
            .await?;
        let reused = if let Some(existing) = self
            .read_content_descriptor(storage_domain_id, content_id)
            .await?
        {
            let existing = ContentDescriptor::decode(&existing, storage_domain_id, content_id)?;
            if existing.length() != state.length() {
                return Err(Error::Corruption {
                    message: format!(
                        "content descriptor {content_id} length {} differs from upload length {}",
                        existing.length(),
                        state.length()
                    ),
                });
            }
            existing.upload_id() != upload_id
        } else {
            self.write_content_descriptor(storage_domain_id, content_id, descriptor.encode())
                .await?;
            false
        };
        let sealing_state =
            (*state).into_sealing(content_id, expires_at, self.content_durability())?;
        self.write_upload_state(&sealing_state).await?;
        drop(seal_guard);
        let UploadSessionStatus::Sealing(sealed) = sealing_state.status() else {
            return Err(Error::InvalidFormat {
                message: "content upload did not enter sealing state".to_owned(),
            });
        };
        Ok((sealing_state, sealed, reused))
    }

    async fn ensure_upload_token_record(
        &self,
        upload_id: UploadId,
        sealed: SealedContent,
    ) -> Result<()> {
        self.internal_bucket(CONTENT_TOKEN_BUCKET).await?;
        self.internal_bucket(CONTENT_TOKEN_INDEX_BUCKET).await?;
        self.internal_bucket(CONTENT_CONTROL_BUCKET).await?;
        self.internal_bucket(CONTENT_LEASE_BUCKET).await?;
        let expected = UploadTokenRecord::available(upload_id, sealed);
        let key = upload_token_key(sealed.upload_token());
        let index = ContentTokenIndexRecord::for_token(sealed);
        let index_key = content_token_index_key(
            sealed.storage_domain_id(),
            sealed.content_id(),
            sealed.upload_token(),
        );
        let mut transaction = self.transaction(TransactionOptions {
            write_options: WriteOptions::new(sealed.durability()),
        });
        if let Some(bytes) = transaction
            .get_internal_bucket(CONTENT_TOKEN_BUCKET, &key)
            .await?
        {
            let existing = UploadTokenRecord::decode(&bytes, sealed.upload_token())?;
            if existing.attachment() != expected.attachment() {
                return Err(Error::Corruption {
                    message: format!("upload {upload_id} token claims changed during seal retry"),
                });
            }
            let indexed = transaction
                .get_internal_bucket(CONTENT_TOKEN_INDEX_BUCKET, &index_key)
                .await?;
            if existing.is_available() {
                let indexed = indexed.ok_or_else(|| Error::Corruption {
                    message: format!("upload {upload_id} is missing its token-authority index"),
                })?;
                ContentTokenIndexRecord::decode(
                    &indexed,
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    crate::content::upload_token_hash(sealed.upload_token()),
                )?;
            } else if indexed.is_some() {
                return Err(Error::Corruption {
                    message: format!("consumed upload {upload_id} retained token authority"),
                });
            }
            let control_key = crate::content::content_control_key(
                sealed.storage_domain_id(),
                sealed.content_id(),
            );
            let control = transaction
                .get_internal_bucket(CONTENT_CONTROL_BUCKET, &control_key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!("sealed upload {upload_id} is missing content control state"),
                })?;
            crate::content::ContentControlRecord::decode(
                &control,
                sealed.storage_domain_id(),
                sealed.content_id(),
            )?;
            return Ok(());
        }
        transaction.put_internal_bucket(CONTENT_TOKEN_BUCKET, key, expected.encode())?;
        transaction.put_internal_bucket(CONTENT_TOKEN_INDEX_BUCKET, index_key, index.encode())?;
        transaction
            .stage_content_activity(sealed.storage_domain_id(), sealed.content_id())
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn require_content_descriptor_publication_allowed(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<()> {
        if let Some(sweep) = self
            .content_reclaim_sweep(storage_domain_id, content_id)
            .await?
        {
            if sweep.reclaimed_at().is_none() {
                return Err(Error::ContentReclaimBlocked {
                    blocker: crate::ContentReclaimBlocker::SweepPrepared {
                        prepared_at_commit_seq: sweep.prepared_at().as_u64(),
                    },
                });
            }
        }
        Ok(())
    }

    /// Aborts durable upload state and schedules no content visibility.
    ///
    /// The session record is deleted before chunk cleanup. A crash during
    /// cleanup can therefore leave only unreachable staging chunks, never a
    /// resumable session that references missing bytes. Cleanup deletion is
    /// idempotent and may be retried by maintenance.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentUploadNotFound`] for an unknown identity,
    /// [`Error::ContentUploadSealed`] after seal, or a backend deletion error.
    pub async fn abort_content_upload(&self, upload_id: UploadId) -> Result<()> {
        self.abort_content_upload_at(upload_id, None).await
    }

    pub(crate) async fn abort_content_upload_at(
        &self,
        upload_id: UploadId,
        expected_revision: Option<u64>,
    ) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let _upload = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        if let Some(expected_revision) = expected_revision {
            state.require_open_revision(expected_revision)?;
        }
        if matches!(
            state.status(),
            UploadSessionStatus::Sealing(_) | UploadSessionStatus::Sealed(_)
        ) {
            return Err(Error::ContentUploadSealed {
                upload_id: upload_id.to_string(),
            });
        }
        self.discard_open_upload(&state).await
    }

    async fn hash_upload_state(&self, state: &UploadSessionState) -> Result<ContentId> {
        let mut hasher = Sha256::new();
        let expected_full_len = state.options().chunk_bytes();
        for index in 0..state.complete_chunks() {
            let frame = self
                .read_content_chunk(state.upload_id(), index)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content upload {} is missing complete chunk {index}",
                        state.upload_id()
                    ),
                })?;
            let payload = crate::content::decode_chunk(&frame, state.upload_id(), index)?;
            if payload.len() != expected_full_len {
                return Err(Error::Corruption {
                    message: format!(
                        "content upload {} complete chunk {index} has length {}, expected {expected_full_len}",
                        state.upload_id(),
                        payload.len()
                    ),
                });
            }
            hasher.update(payload);
        }
        if state.partial_len() != 0 {
            let index = state.complete_chunks();
            let frame = self
                .read_content_chunk(state.upload_id(), index)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content upload {} is missing partial chunk",
                        state.upload_id()
                    ),
                })?;
            let payload = crate::content::decode_chunk(&frame, state.upload_id(), index)?;
            let durable_len =
                usize::try_from(state.partial_len()).map_err(|_| Error::InvalidFormat {
                    message: "content partial length exceeds usize".to_owned(),
                })?;
            let durable = payload
                .get(..durable_len)
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content upload {} partial chunk is shorter than durable state",
                        state.upload_id()
                    ),
                })?;
            hasher.update(durable);
        }
        Ok(ContentId::from_sha256(hasher.finalize().into()))
    }

    async fn discard_open_upload(&self, state: &UploadSessionState) -> Result<()> {
        let aborting = match state.status() {
            UploadSessionStatus::Open => {
                let aborting = (*state).into_aborting()?;
                self.write_upload_state(&aborting).await?;
                aborting
            }
            UploadSessionStatus::Aborting => *state,
            UploadSessionStatus::Sealing(_) | UploadSessionStatus::Sealed(_) => {
                return Err(Error::ContentUploadSealed {
                    upload_id: state.upload_id().to_string(),
                });
            }
        };
        self.cleanup_upload_chunks(&aborting).await?;
        self.release_content_upload_reservation(
            aborting.options().attachment_scope().storage_domain_id(),
            aborting.upload_id(),
        )
        .await?;
        self.delete_upload_state(aborting.upload_id()).await?;
        Ok(())
    }

    async fn read_content_physical_quota(
        &self,
        transaction: &mut crate::Transaction,
        storage_domain_id: StorageDomainId,
    ) -> Result<ContentPhysicalQuota> {
        transaction
            .get_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                &content_physical_quota_key(storage_domain_id),
            )
            .await?
            .map(|bytes| ContentPhysicalQuota::decode(&bytes, storage_domain_id))
            .transpose()
            .map(|quota| {
                quota.unwrap_or_else(|| ContentPhysicalQuota::new(storage_domain_id, 0, 0, None))
            })
    }

    pub(crate) async fn reserve_content_upload_bytes(
        &self,
        state: &UploadSessionState,
        desired_reservation: u64,
    ) -> Result<()> {
        self.internal_bucket(CONTENT_CONTROL_BUCKET).await?;
        let storage_domain_id = state.options().attachment_scope().storage_domain_id();
        let _quota = self.lock_content_quota(storage_domain_id).await;
        let reservation_key = content_physical_reservation_key(state.upload_id());
        let mut transaction = self.transaction(TransactionOptions::default());
        let existing = transaction
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &reservation_key)
            .await?
            .map(|bytes| ContentPhysicalReservationRecord::decode(&bytes, state.upload_id()))
            .transpose()?;
        if existing.is_some_and(|reservation| reservation.storage_domain_id != storage_domain_id) {
            return Err(Error::InvalidFormat {
                message: "content upload physical reservation changed storage domain".to_owned(),
            });
        }
        let already_reserved = existing.map_or(0, |reservation| reservation.reserved_bytes);
        if existing.is_some() && desired_reservation <= already_reserved {
            return Ok(());
        }
        let additional = desired_reservation - already_reserved;
        let quota = self
            .read_content_physical_quota(&mut transaction, storage_domain_id)
            .await?;
        let reserved = quota
            .upload_reserved_bytes()
            .checked_add(additional)
            .ok_or_else(|| Error::InvalidOptions {
                message: "content physical reservation counter overflow".to_owned(),
            })?;
        let accounted = quota
            .unique_content_bytes()
            .checked_add(reserved)
            .ok_or_else(|| Error::InvalidOptions {
                message: "content physical accounting counter overflow".to_owned(),
            })?;
        if quota.limit().is_some_and(|limit| accounted > limit) {
            return Err(Error::ContentPhysicalQuotaExceeded {
                limit: quota.limit().unwrap_or(0),
                unique_content_bytes: quota.unique_content_bytes(),
                upload_reserved_bytes: quota.upload_reserved_bytes(),
                requested_bytes: additional,
            });
        }
        let next_quota = quota.with_counts(quota.unique_content_bytes(), reserved);
        transaction.put_internal_bucket(
            CONTENT_CONTROL_BUCKET,
            content_physical_quota_key(storage_domain_id),
            next_quota.encode(),
        )?;
        transaction.put_internal_bucket(
            CONTENT_CONTROL_BUCKET,
            reservation_key,
            ContentPhysicalReservationRecord {
                upload_id: state.upload_id(),
                storage_domain_id,
                reserved_bytes: desired_reservation,
            }
            .encode(),
        )?;
        transaction.commit().await?;
        Ok(())
    }

    async fn release_content_upload_reservation(
        &self,
        storage_domain_id: StorageDomainId,
        upload_id: UploadId,
    ) -> Result<()> {
        self.internal_bucket(CONTENT_CONTROL_BUCKET).await?;
        let _quota = self.lock_content_quota(storage_domain_id).await;
        let reservation_key = content_physical_reservation_key(upload_id);
        let mut transaction = self.transaction(TransactionOptions::default());
        let Some(bytes) = transaction
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &reservation_key)
            .await?
        else {
            return Ok(());
        };
        let reservation = ContentPhysicalReservationRecord::decode(&bytes, upload_id)?;
        if reservation.storage_domain_id != storage_domain_id {
            return Err(Error::Corruption {
                message: "content upload physical reservation changed storage domain".to_owned(),
            });
        }
        let quota = self
            .read_content_physical_quota(&mut transaction, reservation.storage_domain_id)
            .await?;
        let reserved = quota
            .upload_reserved_bytes()
            .checked_sub(reservation.reserved_bytes)
            .ok_or_else(|| Error::Corruption {
                message: "content physical reservation exceeds its domain counter".to_owned(),
            })?;
        transaction.put_internal_bucket(
            CONTENT_CONTROL_BUCKET,
            content_physical_quota_key(reservation.storage_domain_id),
            quota
                .with_counts(quota.unique_content_bytes(), reserved)
                .encode(),
        )?;
        transaction.delete_internal_bucket(CONTENT_CONTROL_BUCKET, reservation_key)?;
        transaction.commit().await?;
        Ok(())
    }

    async fn finalize_content_upload_quota(
        &self,
        state: &UploadSessionState,
        sealed: SealedContent,
    ) -> Result<()> {
        self.internal_bucket(CONTENT_CONTROL_BUCKET).await?;
        let storage_domain_id = sealed.storage_domain_id();
        let _quota = self.lock_content_quota(storage_domain_id).await;
        let reservation_key = content_physical_reservation_key(state.upload_id());
        let account_key = content_physical_account_key(storage_domain_id, sealed.content_id());
        let mut transaction = self.transaction(TransactionOptions::default());
        let account = transaction
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &account_key)
            .await?
            .map(|bytes| {
                ContentPhysicalAccountRecord::decode(&bytes, storage_domain_id, sealed.content_id())
            })
            .transpose()?;
        if account.is_some_and(|account| account.original_bytes != sealed.len()) {
            return Err(Error::Corruption {
                message: "content physical account length differs from descriptor".to_owned(),
            });
        }
        let Some(reservation_bytes) = transaction
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &reservation_key)
            .await?
        else {
            if account.is_some() {
                return Ok(());
            }
            return Err(Error::Corruption {
                message: "sealing content upload has no physical quota reservation".to_owned(),
            });
        };
        let reservation =
            ContentPhysicalReservationRecord::decode(&reservation_bytes, state.upload_id())?;
        if reservation.storage_domain_id != storage_domain_id
            || reservation.reserved_bytes < sealed.len()
        {
            return Err(Error::Corruption {
                message: "content physical reservation does not cover sealed bytes".to_owned(),
            });
        }
        let quota = self
            .read_content_physical_quota(&mut transaction, storage_domain_id)
            .await?;
        let reserved = quota
            .upload_reserved_bytes()
            .checked_sub(reservation.reserved_bytes)
            .ok_or_else(|| Error::Corruption {
                message: "content physical reservation exceeds its domain counter".to_owned(),
            })?;
        let unique = if account.is_some() {
            quota.unique_content_bytes()
        } else {
            quota
                .unique_content_bytes()
                .checked_add(sealed.len())
                .ok_or_else(|| Error::InvalidOptions {
                    message: "content physical unique-byte counter overflow".to_owned(),
                })?
        };
        let accounted = unique
            .checked_add(reserved)
            .ok_or_else(|| Error::InvalidOptions {
                message: "content physical accounting counter overflow".to_owned(),
            })?;
        if quota.limit().is_some_and(|limit| accounted > limit) {
            return Err(Error::ContentPhysicalQuotaExceeded {
                limit: quota.limit().unwrap_or(0),
                unique_content_bytes: quota.unique_content_bytes(),
                upload_reserved_bytes: quota.upload_reserved_bytes(),
                requested_bytes: 0,
            });
        }
        transaction.put_internal_bucket(
            CONTENT_CONTROL_BUCKET,
            content_physical_quota_key(storage_domain_id),
            quota.with_counts(unique, reserved).encode(),
        )?;
        if account.is_none() {
            transaction.put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                account_key,
                ContentPhysicalAccountRecord {
                    storage_domain_id,
                    content_id: sealed.content_id(),
                    original_bytes: sealed.len(),
                }
                .encode(),
            )?;
        }
        transaction.delete_internal_bucket(CONTENT_CONTROL_BUCKET, reservation_key)?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn stage_reclaimed_content_quota(
        &self,
        transaction: &mut crate::Transaction,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<()> {
        let account_key = content_physical_account_key(storage_domain_id, content_id);
        let Some(bytes) = transaction
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &account_key)
            .await?
        else {
            return Err(Error::Corruption {
                message: "reclaimed content has no physical-byte account".to_owned(),
            });
        };
        let account = ContentPhysicalAccountRecord::decode(&bytes, storage_domain_id, content_id)?;
        let quota = self
            .read_content_physical_quota(transaction, storage_domain_id)
            .await?;
        let unique = quota
            .unique_content_bytes()
            .checked_sub(account.original_bytes)
            .ok_or_else(|| Error::Corruption {
                message: "reclaimed content exceeds physical unique-byte counter".to_owned(),
            })?;
        transaction.put_internal_bucket(
            CONTENT_CONTROL_BUCKET,
            content_physical_quota_key(storage_domain_id),
            quota
                .with_counts(unique, quota.upload_reserved_bytes())
                .encode(),
        )?;
        transaction.delete_internal_bucket(CONTENT_CONTROL_BUCKET, account_key)?;
        Ok(())
    }

    async fn cleanup_upload_chunks(&self, state: &UploadSessionState) -> Result<()> {
        for index in 0..state.chunk_count() {
            self.delete_content_chunk(state.upload_id(), index).await?;
        }
        Ok(())
    }
}
