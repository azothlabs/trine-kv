use std::{path::PathBuf, sync::Arc, time::Duration};

use futures::lock::MutexGuard;
use sha2::{Digest, Sha256};

use crate::{
    content::{
        CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
        CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET, ContentAccessBarrier,
        ContentAccessBarrierId, ContentAccessBarrierRecord, ContentAccessCoordinateRecord,
        ContentAccessMode, ContentDescriptor, ContentHandle, ContentId, ContentLease,
        ContentLeaseId, ContentLeaseOptions, ContentLeaseRecord, ContentPhysicalHold,
        ContentPhysicalHoldId, ContentPhysicalHoldOptions, ContentPhysicalHoldOwnerId,
        ContentPhysicalHoldRecord, ContentPhysicalHoldRecordState, ContentQuarantine,
        ContentQuarantineRecord, ContentReaderDrainAttestation, ContentReaderDrainAttestationId,
        ContentReaderDrainAttestationOptions, ContentReaderDrainAttestationRecord,
        ContentTokenIndexRecord, ContentUpload, ContentUploadOptions, ContentUploadResume,
        SealedContent, StorageDomainId, UploadId, UploadSessionState, UploadSessionStatus,
        UploadToken, UploadTokenRecord, content_access_coordinate_key, content_lease_key,
        content_lease_prefix, content_physical_hold_key, content_quarantine_key,
        content_reader_drain_attestation_key, content_token_index_key, current_epoch_millis,
        duration_millis, upload_token_key,
    },
    error::{Error, Result},
    options::{DurabilityMode, HostStorageBackend, StorageMode, WriteOptions},
    storage::{
        StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind, StorageObjectReadBackend,
        StorageObjectWriteBackend,
    },
    transaction::TransactionOptions,
};

use super::Db;

impl Db {
    const CONTENT_ACCESS_COMMIT_ATTEMPTS: usize = 8;
    const CONTENT_LEASE_COMMIT_ATTEMPTS: usize = 8;
    const CONTENT_PHYSICAL_HOLD_COMMIT_ATTEMPTS: usize = 8;

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
        self.ensure_content_backend_supported()?;
        let options = options.validate()?;
        let upload_id = UploadId::generate()?;
        let upload_token = UploadToken::generate()?;
        let _upload = self.lock_content_upload(upload_id).await;
        let state = UploadSessionState::initial(upload_id, options, upload_token)?;
        self.write_upload_state(&state).await?;
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
        self.ensure_content_backend_supported()?;
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

    /// Returns the persisted content-access mode for one storage domain.
    ///
    /// The check reads the content backend directly instead of relying on this
    /// database handle's KV snapshot. A native or object-store read-only handle
    /// that was opened before the transition therefore observes the barrier on
    /// its next call without refreshing its ordinary KV view.
    ///
    /// An absent barrier returns [`ContentAccessMode::CompatibleUnleased`]. An
    /// active barrier returns [`ContentAccessMode::LeasedOnly`]. Malformed or
    /// identity-mismatched barrier bytes fail closed.
    pub async fn content_access_mode(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> Result<ContentAccessMode> {
        self.ensure_open()?;
        self.ensure_content_backend_supported()?;
        match self
            .read_content_access_barrier_record(storage_domain_id)
            .await?
        {
            Some(record) => Ok(ContentAccessMode::LeasedOnly {
                barrier_id: record.barrier_id,
            }),
            None => Ok(ContentAccessMode::CompatibleUnleased),
        }
    }

    /// Irreversibly requires durable leases for new content opens in a domain.
    ///
    /// Trine first publishes a small barrier through the content backend, where
    /// already-open stale read-only database handles can observe it directly.
    /// It then records the same identity and this method's final commit sequence
    /// in protected KV state. This fail-closed order means an interrupted call
    /// may reject unleased opens before the coordinate is available, but can
    /// never publish the coordinate before the barrier is effective. Retrying
    /// completes an interrupted coordinate publication.
    ///
    /// The transition is irreversible. If another identity already established
    /// the barrier, that existing identity is adopted and returned. The result
    /// fences new unleased opens; it does not prove that handles opened before
    /// the barrier have drained, and it does not authorize physical deletion.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] when the handle cannot publish the barrier,
    /// [`Error::InvalidFormat`] or [`Error::Corruption`] for malformed or
    /// mismatched protected state, [`Error::RuntimeBusy`] after repeated
    /// transaction conflicts, or the selected backend's write/durability error.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{
    ///     ContentAccessBarrierId, ContentAccessMode, Db, DbOptions, StorageDomainId,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let domain = StorageDomainId::from_bytes([1; 16]);
    ///     let barrier = db
    ///         .enforce_content_leased_only(domain, ContentAccessBarrierId::generate()?)
    ///         .await?;
    ///     assert_eq!(
    ///         db.content_access_mode(domain).await?,
    ///         ContentAccessMode::LeasedOnly {
    ///             barrier_id: barrier.barrier_id(),
    ///         }
    ///     );
    ///     Ok(())
    /// }
    /// ```
    pub async fn enforce_content_leased_only(
        &self,
        storage_domain_id: StorageDomainId,
        requested_id: ContentAccessBarrierId,
    ) -> Result<ContentAccessBarrier> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.ensure_content_backend_supported()?;
        let _access_guard = self.inner.content_access_lock.lock().await;
        let barrier = if let Some(existing) = self
            .read_content_access_barrier_record(storage_domain_id)
            .await?
        {
            existing
        } else {
            let requested = ContentAccessBarrierRecord {
                storage_domain_id,
                barrier_id: requested_id,
            };
            self.write_content_access_barrier_record(requested).await?;
            requested
        };
        self.bucket(CONTENT_CONTROL_BUCKET).await?;
        let key = content_access_coordinate_key(storage_domain_id);
        for _ in 0..Self::CONTENT_ACCESS_COMMIT_ATTEMPTS {
            let mut transaction = self.transaction(TransactionOptions::default());
            if let Some(bytes) = transaction.get_bucket(CONTENT_CONTROL_BUCKET, &key).await? {
                let coordinate = ContentAccessCoordinateRecord::decode(&bytes, storage_domain_id)?;
                if coordinate.barrier_id != barrier.barrier_id {
                    return Err(Error::Corruption {
                        message: "content access barrier differs from its protected coordinate"
                            .to_owned(),
                    });
                }
                return Ok(ContentAccessBarrier::new(
                    storage_domain_id,
                    coordinate.barrier_id,
                    coordinate.enforced_at,
                ));
            }
            transaction.put_bucket_with_commit_sequence(
                CONTENT_CONTROL_BUCKET,
                key.clone(),
                &ContentAccessCoordinateRecord::commit_prefix(
                    storage_domain_id,
                    barrier.barrier_id,
                ),
                &[],
            )?;
            match transaction.commit().await {
                Ok(commit) => {
                    return Ok(ContentAccessBarrier::new(
                        storage_domain_id,
                        barrier.barrier_id,
                        commit.read_version(),
                    ));
                }
                Err(Error::Conflict { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::runtime_busy(
            "content access-barrier coordinate did not converge after repeated conflicts",
        ))
    }

    /// Records a trusted deployment coordinator's pre-barrier reader-drain claim.
    ///
    /// `barrier` must be the durable value returned by
    /// [`Db::enforce_content_leased_only`]. Trine KV verifies its direct backend
    /// record and protected commit coordinate before atomically binding the
    /// attestation to that exact barrier. A retry with the same attestation id
    /// and options returns the original record and commit coordinate.
    ///
    /// This method does not inspect process supervisors, remote request streams,
    /// credential issuers, or object-store credentials. The trusted caller must
    /// retain the evidence committed by `options` and must not call this method
    /// until the selected [`ContentReaderDrainKind`](crate::ContentReaderDrainKind)
    /// is actually true. In particular, elapsed time alone is not reader-drain
    /// evidence. The returned record does not start grace or authorize physical
    /// deletion.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] for a read-only database, `InvalidOptions`
    /// when `barrier` does not name the active direct barrier or when an existing
    /// attestation has different exact claims, `Corruption` when protected
    /// barrier coordinates disagree, `RuntimeBusy` after repeated optimistic
    /// conflicts, or a storage/durability error.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{
    ///     ContentAccessBarrierId, ContentReaderDrainAttestationId,
    ///     ContentReaderDrainAttestationOptions, ContentReaderDrainCoordinatorId,
    ///     ContentReaderDrainEvidenceDigest, ContentReaderDrainKind, Db, DbOptions,
    ///     StorageDomainId,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let domain = StorageDomainId::from_bytes([1; 16]);
    ///     let barrier = db
    ///         .enforce_content_leased_only(domain, ContentAccessBarrierId::generate()?)
    ///         .await?;
    ///     // The host establishes and retains these canonical evidence bytes
    ///     // before it makes the trusted assertion.
    ///     let options = ContentReaderDrainAttestationOptions::new(
    ///         ContentReaderDrainKind::DomainBootstrap,
    ///         ContentReaderDrainCoordinatorId::from_bytes([2; 16]),
    ///         ContentReaderDrainEvidenceDigest::for_bytes(b"domain unused before barrier"),
    ///     );
    ///     let attestation = db
    ///         .attest_content_reader_drain(
    ///             barrier,
    ///             ContentReaderDrainAttestationId::generate()?,
    ///             options,
    ///         )
    ///         .await?;
    ///     assert_eq!(attestation.barrier_id(), barrier.barrier_id());
    ///     Ok(())
    /// }
    /// ```
    pub async fn attest_content_reader_drain(
        &self,
        barrier: ContentAccessBarrier,
        attestation_id: ContentReaderDrainAttestationId,
        options: ContentReaderDrainAttestationOptions,
    ) -> Result<ContentReaderDrainAttestation> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.ensure_content_backend_supported()?;
        let Some(direct_barrier) = self
            .read_content_access_barrier_record(barrier.storage_domain_id())
            .await?
        else {
            return Err(Error::invalid_options(
                "reader-drain attestation requires an active leased-only barrier",
            ));
        };
        if direct_barrier.barrier_id != barrier.barrier_id() {
            return Err(Error::invalid_options(
                "reader-drain attestation barrier differs from the active backend barrier",
            ));
        }

        self.bucket(CONTENT_CONTROL_BUCKET).await?;
        let access_key = content_access_coordinate_key(barrier.storage_domain_id());
        let attestation_key = content_reader_drain_attestation_key(barrier.storage_domain_id());
        for _ in 0..Self::CONTENT_ACCESS_COMMIT_ATTEMPTS {
            let mut transaction = self.transaction(TransactionOptions::default());
            let access_bytes = transaction
                .get_bucket(CONTENT_CONTROL_BUCKET, &access_key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: "reader-drain attestation barrier has no protected coordinate"
                        .to_owned(),
                })?;
            let access =
                ContentAccessCoordinateRecord::decode(&access_bytes, barrier.storage_domain_id())?;
            if access.barrier_id != barrier.barrier_id()
                || access.enforced_at != barrier.enforced_at()
            {
                return Err(Error::Corruption {
                    message: "reader-drain attestation barrier coordinates disagree".to_owned(),
                });
            }
            if let Some(bytes) = transaction
                .get_bucket(CONTENT_CONTROL_BUCKET, &attestation_key)
                .await?
            {
                let existing = ContentReaderDrainAttestationRecord::decode(
                    &bytes,
                    barrier.storage_domain_id(),
                )?;
                if existing.matches_request(barrier, attestation_id, options) {
                    return Ok(existing.into_public());
                }
                return Err(Error::invalid_options(format!(
                    "reader-drain attestation differs from existing identity {}",
                    existing.attestation_id
                )));
            }
            let requested = ContentReaderDrainAttestationRecord {
                storage_domain_id: barrier.storage_domain_id(),
                barrier_id: barrier.barrier_id(),
                attestation_id,
                options,
                barrier_enforced_at: barrier.enforced_at(),
                attested_at: crate::ReadVersion::from_u64(0),
            };
            transaction.put_bucket_with_commit_sequence(
                CONTENT_CONTROL_BUCKET,
                attestation_key.clone(),
                &requested.encode_prefix(),
                &[],
            )?;
            match transaction.commit().await {
                Ok(commit) => {
                    return Ok(ContentReaderDrainAttestationRecord {
                        attested_at: commit.read_version(),
                        ..requested
                    }
                    .into_public());
                }
                Err(Error::Conflict { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::runtime_busy(
            "content reader-drain attestation did not converge after repeated conflicts",
        ))
    }

    /// Reads the durable reader-drain attestation for one storage domain.
    ///
    /// `None` means no trusted coordinator claim has been recorded. The result
    /// is audit and future lifecycle input only: callers must still validate the
    /// active barrier and every content-specific reclaim condition before any
    /// future sweep. Read-only object-store handles may need their ordinary KV
    /// view refreshed before this protected record becomes visible.
    pub async fn content_reader_drain_attestation(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> Result<Option<ContentReaderDrainAttestation>> {
        self.ensure_open()?;
        self.ensure_content_backend_supported()?;
        if matches!(
            self.content_access_mode(storage_domain_id).await?,
            ContentAccessMode::CompatibleUnleased
        ) {
            return Ok(None);
        }
        let mut transaction = self.transaction(TransactionOptions::default());
        let key = content_reader_drain_attestation_key(storage_domain_id);
        transaction
            .get_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
            .map(|bytes| {
                ContentReaderDrainAttestationRecord::decode(&bytes, storage_domain_id)
                    .map(ContentReaderDrainAttestationRecord::into_public)
            })
            .transpose()
    }

    /// Reads the durable quarantine state for one exact content identity.
    ///
    /// `None` means the content is not currently quarantined. Attachment/token
    /// or physical-hold activity may remove a previous quarantine and return the
    /// content to Active state. A returned record is a read fence and recovery
    /// coordinate only; it does not start grace or authorize byte deletion.
    pub async fn content_quarantine(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Option<ContentQuarantine>> {
        self.ensure_open()?;
        self.ensure_content_backend_supported()?;
        let mut transaction = self.transaction(TransactionOptions::default());
        let key = content_quarantine_key(storage_domain_id, content_id);
        match transaction.get_bucket(CONTENT_CONTROL_BUCKET, &key).await {
            Ok(Some(bytes)) => {
                ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id)
                    .map(ContentQuarantineRecord::into_public)
                    .map(Some)
            }
            Ok(None) | Err(Error::BucketMissing { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Opens a sealed immutable `ContentObject` by cryptographic identity.
    ///
    /// The descriptor is read and validated once. The resulting handle returns
    /// original bytes through verified ranges and sequential streaming; it does
    /// not expose chunk paths or upload identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentLeaseRequired`] after this storage domain enters
    /// leased-only mode, [`Error::ContentNotFound`] when no sealed descriptor
    /// exists, [`Error::Closed`] for a closed database, or a
    /// storage/format/integrity error when protected state cannot be trusted.
    ///
    /// # Parameters
    ///
    /// - `storage_domain_id`: exact deduplication and physical-lifecycle domain.
    /// - `content_id`: cryptographic identity of the original bytes.
    pub async fn open_content(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<ContentHandle> {
        self.ensure_open()?;
        self.ensure_content_backend_supported()?;
        if let ContentAccessMode::LeasedOnly { barrier_id } =
            self.content_access_mode(storage_domain_id).await?
        {
            return Err(Error::ContentLeaseRequired { barrier_id });
        }
        self.open_content_unchecked(storage_domain_id, content_id)
            .await
    }

    async fn open_content_unchecked(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<ContentHandle> {
        let bytes = self
            .read_content_descriptor(storage_domain_id, content_id)
            .await?
            .ok_or_else(|| Error::ContentNotFound {
                storage_domain_id: storage_domain_id.to_string(),
                content_id: content_id.to_string(),
            })?;
        let descriptor = ContentDescriptor::decode(&bytes, storage_domain_id, content_id)?;
        Ok(ContentHandle::new(self.clone(), descriptor))
    }

    /// Opens sealed immutable content under a durable short-lived read lease.
    ///
    /// The descriptor is validated exactly as in [`Db::open_content`]. Before
    /// returning, Trine KV publishes a protected lease record bound to the exact
    /// `(storage_domain_id, content_id)`, a generated [`ContentLeaseId`], the
    /// opaque owner from `options`, and an explicit Unix-millisecond deadline.
    /// All clones of the returned [`ContentHandle`] share that lease deadline.
    /// Dropping them performs no asynchronous cleanup; the record simply becomes
    /// inactive at expiry.
    ///
    /// The owner has no authorization meaning inside Trine KV. A higher layer
    /// must authorize before this call and before
    /// [`ContentHandle::renew_lease`]. Existing [`Db::open_content`] remains the
    /// compatible unleased path and is not safe against future physical
    /// reclamation.
    ///
    /// # Parameters
    ///
    /// - `storage_domain_id`: exact physical lifecycle and deduplication domain.
    /// - `content_id`: immutable original-byte identity.
    /// - `options`: opaque owner and a lifetime of at least one millisecond.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentNotFound`] or descriptor integrity errors as the
    /// unleased open does, [`Error::ReadOnly`] when the database cannot publish
    /// a lease, [`Error::InvalidOptions`] for an invalid or overflowing
    /// lifetime, [`Error::ContentQuarantined`] when the exact content has entered
    /// durable quarantine, or a transaction/storage error when the lease record
    /// cannot be committed. Leased-only mode itself does not reject this method.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentLeaseOptions, ContentLeaseOwnerId,
    ///     ContentUploadOptions, Db, DbOptions, OwnerScopeId, StorageDomainId,
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
    ///     upload.write(b"leased bytes").await?;
    ///     let sealed = upload.seal().await?;
    ///
    ///     let handle = db
    ///         .open_content_leased(
    ///             domain,
    ///             sealed.content_id(),
    ///             ContentLeaseOptions::new(
    ///                 ContentLeaseOwnerId::from_bytes([3; 16]),
    ///                 Duration::from_secs(30),
    ///             ),
    ///         )
    ///         .await?;
    ///     assert_eq!(&*handle.read_range(0, u64::MAX).await?, b"leased bytes");
    ///     handle.renew_lease(Duration::from_secs(30)).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_content_leased(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        options: ContentLeaseOptions,
    ) -> Result<ContentHandle> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let ttl_ms = options.ttl_ms()?;
        self.ensure_content_backend_supported()?;
        let handle = self
            .open_content_unchecked(storage_domain_id, content_id)
            .await?;
        let lease_id = ContentLeaseId::generate()?;
        let expires_at_unix_ms = current_epoch_millis()?
            .checked_add(ttl_ms)
            .ok_or_else(|| Error::invalid_options("content lease expiry overflow"))?;
        let record = ContentLeaseRecord {
            lease_id,
            owner_id: options.owner_id(),
            storage_domain_id,
            content_id,
            expires_at_unix_ms,
        };
        self.bucket(CONTENT_LEASE_BUCKET).await?;
        let mut transaction = self.transaction(TransactionOptions::default());
        transaction.put_bucket(
            CONTENT_LEASE_BUCKET,
            content_lease_key(storage_domain_id, content_id, lease_id),
            record.encode(),
        )?;
        transaction
            .stage_content_read_activity(storage_domain_id, content_id)
            .await?;
        transaction.commit().await?;
        Ok(handle.with_lease(ContentLease::new(
            lease_id,
            options.owner_id(),
            expires_at_unix_ms,
        )))
    }

    pub(crate) async fn renew_content_lease(
        &self,
        handle: &ContentHandle,
        ttl: Duration,
    ) -> Result<u64> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let lease = handle.lease().ok_or_else(|| Error::ContentLeaseNotFound {
            lease_id: "unleased handle".to_owned(),
        })?;
        let ttl_ms = ContentLeaseOptions::new(lease.owner_id(), ttl).ttl_ms()?;
        let key = content_lease_key(handle.storage_domain_id(), handle.content_id(), lease.id());
        for _ in 0..Self::CONTENT_LEASE_COMMIT_ATTEMPTS {
            let mut transaction = self.transaction(TransactionOptions::default());
            let bytes = transaction
                .get_bucket(CONTENT_LEASE_BUCKET, &key)
                .await?
                .ok_or_else(|| Error::ContentLeaseNotFound {
                    lease_id: lease.id().to_string(),
                })?;
            let mut record = ContentLeaseRecord::decode(
                &bytes,
                handle.storage_domain_id(),
                handle.content_id(),
                lease.id(),
            )?;
            let now_unix_ms = current_epoch_millis()?;
            if record.owner_id != lease.owner_id() {
                return Err(Error::Corruption {
                    message: "content lease owner differs from its open handle".to_owned(),
                });
            }
            if now_unix_ms >= record.expires_at_unix_ms {
                return Err(Error::ContentLeaseExpired {
                    expired_at_unix_ms: record.expires_at_unix_ms,
                });
            }
            let requested_expiry = now_unix_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| Error::invalid_options("content lease expiry overflow"))?;
            let next_expiry = record.expires_at_unix_ms.max(requested_expiry);
            if next_expiry == record.expires_at_unix_ms {
                lease.publish_expiry(next_expiry);
                return Ok(next_expiry);
            }
            record.expires_at_unix_ms = next_expiry;
            transaction.put_bucket(CONTENT_LEASE_BUCKET, key.clone(), record.encode())?;
            transaction
                .stage_content_read_activity(handle.storage_domain_id(), handle.content_id())
                .await?;
            match transaction.commit().await {
                Ok(_) => {
                    lease.publish_expiry(next_expiry);
                    return Ok(next_expiry);
                }
                Err(Error::Conflict { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::runtime_busy(
            "content lease renewal did not converge after repeated conflicts",
        ))
    }

    /// Acquires a durable physical hold on sealed immutable content.
    ///
    /// Before returning, Trine KV validates the exact descriptor and atomically
    /// publishes both the hold and newer per-content activity. A reclaim-intent
    /// transaction racing acquisition must conflict; acquisition after intent
    /// returns the content to Active state. Migration, backup, repair, provider,
    /// and administrative workflows therefore share one physical fence.
    ///
    /// An expiring hold becomes inert at its exclusive Unix-millisecond
    /// deadline and cannot be revived. An until-released hold survives process
    /// restart and must be recovered with [`Db::resume_content_physical_hold`].
    /// Dropping the returned value performs no I/O. `hold_id` is supplied by
    /// the caller: retrying the exact same active identity returns its original
    /// durable record, closing the commit-before-response crash boundary.
    ///
    /// # Parameters
    ///
    /// - `storage_domain_id`: exact physical lifecycle and deduplication domain.
    /// - `content_id`: immutable original-byte identity to retain.
    /// - `hold_id`: stable caller-retained idempotency and recovery identity.
    /// - `options`: operational class, opaque owner, and expiring or explicit
    ///   release lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentNotFound`] for a missing descriptor,
    /// [`Error::ReadOnly`] when protected state cannot be written,
    /// [`Error::ContentPhysicalHoldOwnerMismatch`] when an existing identity
    /// belongs to another owner, [`Error::ContentPhysicalHoldExpired`] when an
    /// existing identity cannot be revived, [`Error::InvalidOptions`] when an
    /// existing identity names different class/lifetime semantics or the
    /// lifetime is invalid, or a storage/format/transaction error when
    /// acquisition cannot converge.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentPhysicalHoldId, ContentPhysicalHoldKind,
    ///     ContentPhysicalHoldOptions, ContentPhysicalHoldOwnerId, ContentUploadOptions, Db,
    ///     DbOptions, OwnerScopeId, StorageDomainId,
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
    ///     upload.write(b"backup bytes").await?;
    ///     let sealed = upload.seal().await?;
    ///     let hold = db
    ///         .acquire_content_physical_hold(
    ///             domain,
    ///             sealed.content_id(),
    ///             ContentPhysicalHoldId::generate()?,
    ///             ContentPhysicalHoldOptions::expiring(
    ///                 ContentPhysicalHoldKind::Backup,
    ///                 ContentPhysicalHoldOwnerId::from_bytes([3; 16]),
    ///                 Duration::from_secs(30),
    ///             ),
    ///         )
    ///         .await?;
    ///     hold.renew(Duration::from_secs(30)).await?;
    ///     hold.release().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn acquire_content_physical_hold(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        hold_id: ContentPhysicalHoldId,
        options: ContentPhysicalHoldOptions,
    ) -> Result<ContentPhysicalHold> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.ensure_content_backend_supported()?;
        let descriptor = self
            .read_content_descriptor(storage_domain_id, content_id)
            .await?
            .ok_or_else(|| Error::ContentNotFound {
                storage_domain_id: storage_domain_id.to_string(),
                content_id: content_id.to_string(),
            })?;
        ContentDescriptor::decode(&descriptor, storage_domain_id, content_id)?;
        let now_unix_ms = current_epoch_millis()?;
        let requested = ContentPhysicalHoldRecord {
            hold_id,
            owner_id: options.owner_id(),
            storage_domain_id,
            content_id,
            kind: options.kind(),
            expires_at_unix_ms: options.expires_at_unix_ms(now_unix_ms)?,
            state: ContentPhysicalHoldRecordState::Active,
        };
        self.bucket(CONTENT_PHYSICAL_HOLD_BUCKET).await?;
        let key = content_physical_hold_key(storage_domain_id, content_id, hold_id);
        for _ in 0..Self::CONTENT_PHYSICAL_HOLD_COMMIT_ATTEMPTS {
            let mut transaction = self.transaction(TransactionOptions::default());
            if let Some(bytes) = transaction
                .get_bucket(CONTENT_PHYSICAL_HOLD_BUCKET, &key)
                .await?
            {
                let existing = ContentPhysicalHoldRecord::decode(
                    &bytes,
                    storage_domain_id,
                    content_id,
                    hold_id,
                )?;
                if existing.owner_id != requested.owner_id {
                    return Err(Error::ContentPhysicalHoldOwnerMismatch);
                }
                if existing.is_released() {
                    return Err(Error::ContentPhysicalHoldNotFound {
                        hold_id: hold_id.to_string(),
                    });
                }
                if existing.kind != requested.kind
                    || (existing.expires_at_unix_ms == 0) != (requested.expires_at_unix_ms == 0)
                {
                    return Err(Error::invalid_options(
                        "content physical-hold identity already names different semantics",
                    ));
                }
                if !existing.is_active_at(now_unix_ms) {
                    return Err(Error::ContentPhysicalHoldExpired {
                        expired_at_unix_ms: existing.expires_at_unix_ms,
                    });
                }
                match transaction.commit().await {
                    Ok(_) => return Ok(ContentPhysicalHold::from_record(self.clone(), existing)),
                    Err(Error::Conflict { .. }) => continue,
                    Err(error) => return Err(error),
                }
            }
            transaction.put_bucket(
                CONTENT_PHYSICAL_HOLD_BUCKET,
                key.clone(),
                requested.encode(),
            )?;
            transaction
                .stage_content_activity(storage_domain_id, content_id)
                .await?;
            match transaction.commit().await {
                Ok(_) => return Ok(ContentPhysicalHold::from_record(self.clone(), requested)),
                Err(Error::Conflict { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::runtime_busy(
            "content physical-hold acquisition did not converge after repeated conflicts",
        ))
    }

    /// Resumes a durable physical hold after process or handle loss.
    ///
    /// This read-only operation validates the protected key/value identity and
    /// exact opaque owner. It does not extend expiry or reacquire a released
    /// hold. The caller must perform its higher-layer authorization before
    /// supplying `owner_id`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentPhysicalHoldNotFound`] when the exact record is
    /// absent, [`Error::ContentPhysicalHoldOwnerMismatch`] for a wrong owner,
    /// [`Error::ContentPhysicalHoldExpired`] for an expired record, or a
    /// storage/format/integrity error when protected state cannot be trusted.
    pub async fn resume_content_physical_hold(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        hold_id: ContentPhysicalHoldId,
        owner_id: ContentPhysicalHoldOwnerId,
    ) -> Result<ContentPhysicalHold> {
        self.ensure_open()?;
        let bucket = self.bucket(CONTENT_PHYSICAL_HOLD_BUCKET).await?;
        let key = content_physical_hold_key(storage_domain_id, content_id, hold_id);
        let bytes = bucket
            .get(&key)
            .await?
            .ok_or_else(|| Error::ContentPhysicalHoldNotFound {
                hold_id: hold_id.to_string(),
            })?;
        let record =
            ContentPhysicalHoldRecord::decode(&bytes, storage_domain_id, content_id, hold_id)?;
        if record.owner_id != owner_id {
            return Err(Error::ContentPhysicalHoldOwnerMismatch);
        }
        if record.is_released() {
            return Err(Error::ContentPhysicalHoldNotFound {
                hold_id: hold_id.to_string(),
            });
        }
        if !record.is_active_at(current_epoch_millis()?) {
            return Err(Error::ContentPhysicalHoldExpired {
                expired_at_unix_ms: record.expires_at_unix_ms,
            });
        }
        Ok(ContentPhysicalHold::from_record(self.clone(), record))
    }

    pub(crate) async fn renew_content_physical_hold(
        &self,
        hold: &ContentPhysicalHold,
        ttl: Duration,
    ) -> Result<u64> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if hold.is_released() {
            return Err(Error::ContentPhysicalHoldNotFound {
                hold_id: hold.id().to_string(),
            });
        }
        if hold.expires_at_unix_ms().is_none() {
            return Err(Error::invalid_options(
                "an until-released content physical hold cannot be renewed",
            ));
        }
        let ttl_ms = duration_millis(ttl, "content physical-hold renewal lifetime")?;
        let key = content_physical_hold_key(hold.storage_domain_id(), hold.content_id(), hold.id());
        for _ in 0..Self::CONTENT_PHYSICAL_HOLD_COMMIT_ATTEMPTS {
            let mut transaction = self.transaction(TransactionOptions::default());
            let bytes = transaction
                .get_bucket(CONTENT_PHYSICAL_HOLD_BUCKET, &key)
                .await?
                .ok_or_else(|| Error::ContentPhysicalHoldNotFound {
                    hold_id: hold.id().to_string(),
                })?;
            let mut record = ContentPhysicalHoldRecord::decode(
                &bytes,
                hold.storage_domain_id(),
                hold.content_id(),
                hold.id(),
            )?;
            if record.owner_id != hold.owner_id() {
                return Err(Error::ContentPhysicalHoldOwnerMismatch);
            }
            if record.is_released() {
                return Err(Error::ContentPhysicalHoldNotFound {
                    hold_id: hold.id().to_string(),
                });
            }
            let now_unix_ms = current_epoch_millis()?;
            if !record.is_active_at(now_unix_ms) {
                return Err(Error::ContentPhysicalHoldExpired {
                    expired_at_unix_ms: record.expires_at_unix_ms,
                });
            }
            if record.expires_at_unix_ms == 0 {
                return Err(Error::invalid_options(
                    "an until-released content physical hold cannot be renewed",
                ));
            }
            let requested_expiry = now_unix_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| Error::invalid_options("content physical-hold expiry overflow"))?;
            let next_expiry = record.expires_at_unix_ms.max(requested_expiry);
            if next_expiry == record.expires_at_unix_ms {
                hold.publish_expiry(next_expiry);
                return Ok(next_expiry);
            }
            record.expires_at_unix_ms = next_expiry;
            transaction.put_bucket(CONTENT_PHYSICAL_HOLD_BUCKET, key.clone(), record.encode())?;
            transaction
                .stage_content_activity(hold.storage_domain_id(), hold.content_id())
                .await?;
            match transaction.commit().await {
                Ok(_) => {
                    hold.publish_expiry(next_expiry);
                    return Ok(next_expiry);
                }
                Err(Error::Conflict { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::runtime_busy(
            "content physical-hold renewal did not converge after repeated conflicts",
        ))
    }

    pub(crate) async fn release_content_physical_hold(
        &self,
        hold: &ContentPhysicalHold,
    ) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if hold.is_released() {
            return Ok(());
        }
        let key = content_physical_hold_key(hold.storage_domain_id(), hold.content_id(), hold.id());
        for _ in 0..Self::CONTENT_PHYSICAL_HOLD_COMMIT_ATTEMPTS {
            let mut transaction = self.transaction(TransactionOptions::default());
            let Some(bytes) = transaction
                .get_bucket(CONTENT_PHYSICAL_HOLD_BUCKET, &key)
                .await?
            else {
                hold.publish_released();
                return Ok(());
            };
            let record = ContentPhysicalHoldRecord::decode(
                &bytes,
                hold.storage_domain_id(),
                hold.content_id(),
                hold.id(),
            )?;
            if record.owner_id != hold.owner_id() {
                return Err(Error::ContentPhysicalHoldOwnerMismatch);
            }
            if record.is_released() {
                hold.publish_released();
                return Ok(());
            }
            transaction.put_bucket(
                CONTENT_PHYSICAL_HOLD_BUCKET,
                key.clone(),
                record.released().encode(),
            )?;
            match transaction.commit().await {
                Ok(_) => {
                    hold.publish_released();
                    return Ok(());
                }
                Err(Error::Conflict { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::runtime_busy(
            "content physical-hold release did not converge after repeated conflicts",
        ))
    }

    // This is an exact conservative precheck for the future physical
    // reclamation path. It cannot authorize deletion without a per-content
    // quarantine/fence that prevents a new lease after this scan.
    #[allow(dead_code)]
    pub(crate) async fn content_has_active_lease(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<bool> {
        let now_unix_ms = current_epoch_millis()?;
        let prefix = content_lease_prefix(storage_domain_id, content_id);
        let bucket = self.bucket(CONTENT_LEASE_BUCKET).await?;
        for entry in bucket.prefix(prefix.clone()).await? {
            let entry = entry?;
            let lease_bytes = entry
                .key
                .get(prefix.len()..)
                .ok_or_else(|| Error::Corruption {
                    message: "content lease key is shorter than its content prefix".to_owned(),
                })?;
            let lease_id = ContentLeaseId::from_bytes(lease_bytes.try_into().map_err(|_| {
                Error::Corruption {
                    message: "content lease key has a malformed identity length".to_owned(),
                }
            })?)?;
            let record =
                ContentLeaseRecord::decode(&entry.value, storage_domain_id, content_id, lease_id)?;
            if now_unix_ms < record.expires_at_unix_ms {
                return Ok(true);
            }
        }
        Ok(false)
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
        if let UploadSessionStatus::Sealed(sealed) = state.status() {
            return Ok(sealed);
        }
        let (sealing_state, sealed, reused) = self.prepare_upload_seal(&state).await?;

        self.ensure_upload_token_record(upload_id, sealed).await?;
        let sealed_state = sealing_state.into_sealed()?;
        self.write_upload_state(&sealed_state).await?;
        if reused {
            self.cleanup_upload_chunks(&sealing_state).await;
        }
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
            UploadSessionStatus::Open => {
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
                let reused = if let Some(existing) = self
                    .read_content_descriptor(storage_domain_id, content_id)
                    .await?
                {
                    let existing =
                        ContentDescriptor::decode(&existing, storage_domain_id, content_id)?;
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
                    self.write_content_descriptor(
                        storage_domain_id,
                        content_id,
                        descriptor.encode(),
                    )
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
        }
    }

    async fn ensure_upload_token_record(
        &self,
        upload_id: UploadId,
        sealed: SealedContent,
    ) -> Result<()> {
        self.bucket(CONTENT_TOKEN_BUCKET).await?;
        self.bucket(CONTENT_TOKEN_INDEX_BUCKET).await?;
        self.bucket(CONTENT_CONTROL_BUCKET).await?;
        self.bucket(CONTENT_LEASE_BUCKET).await?;
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
        if let Some(bytes) = transaction.get_bucket(CONTENT_TOKEN_BUCKET, &key).await? {
            let existing = UploadTokenRecord::decode(&bytes, sealed.upload_token())?;
            if existing.attachment() != expected.attachment() {
                return Err(Error::Corruption {
                    message: format!("upload {upload_id} token claims changed during seal retry"),
                });
            }
            let indexed = transaction
                .get_bucket(CONTENT_TOKEN_INDEX_BUCKET, &index_key)
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
                .get_bucket(CONTENT_CONTROL_BUCKET, &control_key)
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
        transaction.put_bucket(CONTENT_TOKEN_BUCKET, key, expected.encode())?;
        transaction.put_bucket(CONTENT_TOKEN_INDEX_BUCKET, index_key, index.encode())?;
        transaction
            .stage_content_activity(sealed.storage_domain_id(), sealed.content_id())
            .await?;
        transaction.commit().await?;
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
        self.delete_upload_state(state.upload_id()).await?;
        self.cleanup_upload_chunks(state).await;
        Ok(())
    }

    async fn cleanup_upload_chunks(&self, state: &UploadSessionState) {
        for index in 0..state.chunk_count() {
            let _ = self.delete_content_chunk(state.upload_id(), index).await;
        }
    }

    pub(crate) async fn write_content_chunk(
        &self,
        upload_id: UploadId,
        index: u64,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_chunk_object(upload_id, index)?;
        self.write_content_object(object, bytes).await
    }

    pub(crate) async fn read_content_chunk(
        &self,
        upload_id: UploadId,
        index: u64,
    ) -> Result<Option<Arc<[u8]>>> {
        let object = self.content_chunk_object(upload_id, index)?;
        self.read_content_object(object).await
    }

    pub(crate) async fn delete_content_chunk(&self, upload_id: UploadId, index: u64) -> Result<()> {
        let object = self.content_chunk_object(upload_id, index)?;
        self.delete_content_object(object).await
    }

    pub(crate) async fn write_content_descriptor(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_descriptor_object(storage_domain_id, content_id)?;
        self.write_content_object(object, bytes).await
    }

    pub(crate) async fn read_content_descriptor(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Option<Arc<[u8]>>> {
        let object = self.content_descriptor_object(storage_domain_id, content_id)?;
        self.read_content_object(object).await
    }

    pub(crate) async fn read_content_access_barrier_record(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> Result<Option<ContentAccessBarrierRecord>> {
        let object = self.content_access_barrier_object(storage_domain_id)?;
        self.read_content_object(object)
            .await?
            .map(|bytes| ContentAccessBarrierRecord::decode(&bytes, storage_domain_id))
            .transpose()
    }

    #[cfg(test)]
    pub(crate) async fn write_content_access_barrier_bytes_for_test(
        &self,
        storage_domain_id: StorageDomainId,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_access_barrier_object(storage_domain_id)?;
        self.write_content_object(object, bytes).await
    }

    pub(crate) async fn write_content_access_barrier_record(
        &self,
        record: ContentAccessBarrierRecord,
    ) -> Result<()> {
        let object = self.content_access_barrier_object(record.storage_domain_id)?;
        self.write_content_object(object, record.encode()).await
    }

    pub(crate) async fn write_upload_state(&self, state: &UploadSessionState) -> Result<()> {
        let object = self.content_upload_state_object(state.upload_id())?;
        self.write_content_object(object, (*state).encode()?).await
    }

    pub(crate) async fn require_upload_state(
        &self,
        upload_id: UploadId,
    ) -> Result<UploadSessionState> {
        let object = self.content_upload_state_object(upload_id)?;
        let bytes = self.read_content_object(object).await?.ok_or_else(|| {
            Error::ContentUploadNotFound {
                upload_id: upload_id.to_string(),
            }
        })?;
        UploadSessionState::decode(&bytes, upload_id)
    }

    async fn delete_upload_state(&self, upload_id: UploadId) -> Result<()> {
        let object = self.content_upload_state_object(upload_id)?;
        self.delete_content_object(object).await
    }

    pub(crate) async fn lock_content_upload(&self, upload_id: UploadId) -> MutexGuard<'_, ()> {
        self.inner.content_upload_locks[upload_id.lock_shard()]
            .lock()
            .await
    }

    pub(crate) async fn lock_content_seal(&self) -> MutexGuard<'_, ()> {
        self.inner.content_seal_lock.lock().await
    }

    fn ensure_content_backend_supported(&self) -> Result<()> {
        match &self.inner.options.storage_mode {
            StorageMode::InMemory
            | StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. } | HostStorageBackend::ObjectStore,
            } => Ok(()),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    fn content_root(&self) -> Result<PathBuf> {
        let root = match &self.inner.options.storage_mode {
            StorageMode::InMemory => PathBuf::from("__trine_content_v1"),
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => self
                .persistent_path()
                .ok_or_else(|| Error::Corruption {
                    message: "persistent content backend has no database path".to_owned(),
                })?
                .join(".trine-content-v1"),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.object_store_db_path().join("content-v1"),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => {
                return Err(Error::unsupported_backend(
                    "browser content objects are not implemented in this prototype",
                ));
            }
        };
        Ok(root)
    }

    fn content_chunk_object(&self, upload_id: UploadId, index: u64) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("chunks")
            .join(upload_id.to_string())
            .join(format!("{index:020}.trinec"));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentChunk,
            path,
        ))
    }

    fn content_descriptor_object(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("domains")
            .join(hex_identifier(storage_domain_id.to_bytes()))
            .join("descriptors")
            .join("sha256")
            .join(format!("{}.trined", hex_identifier(content_id.digest())));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentDescriptor,
            path,
        ))
    }

    fn content_access_barrier_object(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("domains")
            .join(hex_identifier(storage_domain_id.to_bytes()))
            .join("access")
            .join("leased-only.trinebarrier");
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentAccessBarrier,
            path,
        ))
    }

    fn content_upload_state_object(&self, upload_id: UploadId) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("uploads")
            .join(format!("{upload_id}.trineu"));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentUpload,
            path,
        ))
    }

    async fn write_content_object(&self, object: StorageObjectId, bytes: Arc<[u8]>) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let durability = self.content_durability();
        match &self.inner.options.storage_mode {
            StorageMode::InMemory => {
                self.inner
                    .content_memory
                    .write_object(object, bytes, durability)
                    .await
            }
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => {
                self.inner
                    .native_storage
                    .write_object(object, bytes, durability)
                    .await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => {
                self.object_storage()?
                    .write_object(object, bytes, durability)
                    .await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    async fn read_content_object(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        self.ensure_open()?;
        match &self.inner.options.storage_mode {
            StorageMode::InMemory => self.inner.content_memory.read_object_bytes(object).await,
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => self.inner.native_storage.read_object_bytes(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.object_storage()?.read_object_bytes(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    async fn delete_content_object(&self, object: StorageObjectId) -> Result<()> {
        self.ensure_open()?;
        match &self.inner.options.storage_mode {
            StorageMode::InMemory => self.inner.content_memory.delete_object(object).await,
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => self.inner.native_storage.delete_object(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.object_storage()?.delete_object(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    fn content_durability(&self) -> DurabilityMode {
        match &self.inner.options.storage_mode {
            StorageMode::Persistent { .. } => self.filesystem_publish_durability(),
            StorageMode::InMemory
            | StorageMode::HostPersistent {
                backend:
                    HostStorageBackend::Wasi { .. }
                    | HostStorageBackend::Browser { .. }
                    | HostStorageBackend::ObjectStore,
            } => DurabilityMode::Flush,
        }
    }
}

fn hex_identifier<const N: usize>(bytes: [u8; N]) -> String {
    use std::fmt::Write;

    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}
