use super::{
    CONTENT_CONTROL_BUCKET, ContentAccessBarrier, ContentAccessBarrierId,
    ContentAccessBarrierRecord, ContentAccessCoordinateRecord, ContentAccessMode, ContentId,
    ContentQuarantine, ContentQuarantineRecord, ContentReaderDrainAttestation,
    ContentReaderDrainAttestationId, ContentReaderDrainAttestationOptions,
    ContentReaderDrainAttestationRecord, ContentReclaimGrace, ContentReclaimGraceRecord,
    ContentReclaimSweep, ContentReclaimSweepBackend, ContentReclaimSweepRecord,
    ContentReclaimSweepRecordState, ContentReclamationMode, Db, Error, HostStorageBackend, Result,
    StorageDomainId, StorageMode, TransactionOptions, content_access_coordinate_key,
    content_control_key, content_quarantine_key, content_reader_drain_attestation_key,
    content_reclaim_grace_key, content_reclaim_sweep_key,
};

impl Db {
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

    /// Reads the durable reclaim-grace scheduling record for exact content.
    ///
    /// `None` means grace has not been started or authoritative activity
    /// atomically revived the content. A returned deadline is not deletion
    /// authority: it is based on a host wall-clock observation and this API does
    /// not claim that elapsed real time survived clock jumps or restart.
    /// Malformed state or a grace record without its matching quarantine fails
    /// closed.
    pub async fn content_reclaim_grace(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Option<ContentReclaimGrace>> {
        self.ensure_open()?;
        let mut transaction = self.transaction(TransactionOptions::default());
        let grace_key = content_reclaim_grace_key(storage_domain_id, content_id);
        let Some(bytes) = transaction
            .get_bucket(CONTENT_CONTROL_BUCKET, &grace_key)
            .await?
        else {
            return Ok(None);
        };
        let grace = ContentReclaimGraceRecord::decode(&bytes, storage_domain_id, content_id)?;
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        let quarantine_bytes = transaction
            .get_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: "content reclaim grace exists without its quarantine fence".to_owned(),
            })?;
        let quarantine =
            ContentQuarantineRecord::decode(&quarantine_bytes, storage_domain_id, content_id)?;
        if !grace.matches_quarantine(quarantine) {
            return Err(Error::Corruption {
                message: "content reclaim grace differs from its quarantine fence".to_owned(),
            });
        }
        Ok(Some(grace.into_public()))
    }

    /// Reads durable physical-reclamation progress for exact content.
    ///
    /// `None` means no final sweep has been prepared. A value whose
    /// [`ContentReclaimSweep::reclaimed_at`] is `None` must be resumed from its
    /// protected manifest; it must not be revived or replaced in place.
    pub async fn content_reclaim_sweep(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Option<ContentReclaimSweep>> {
        self.ensure_open()?;
        let mut transaction = self.transaction(TransactionOptions::default());
        let key = content_reclaim_sweep_key(storage_domain_id, content_id);
        match transaction.get_bucket(CONTENT_CONTROL_BUCKET, &key).await {
            Ok(Some(bytes)) => {
                ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)
                    .map(ContentReclaimSweepRecord::into_public)
                    .map(Some)
            }
            Ok(None) | Err(Error::BucketMissing { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Resumes one Prepared filesystem sweep and records durable completion.
    ///
    /// Chunks are deleted before the descriptor. Every delete is idempotent;
    /// any error leaves the Prepared manifest intact so an explicit retry after
    /// repair or reopen continues the same work. The final transaction changes
    /// the record to Reclaimed only after all object deletions succeed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedBackend`] when reclamation is disabled or
    /// the database does not use the qualified native filesystem backend,
    /// [`Error::ReadOnly`] for a read-only handle, [`Error::ContentNotFound`]
    /// when no sweep exists, or a storage/conflict/corruption error. Errors
    /// never authorize skipping a remaining object.
    pub async fn resume_content_reclaim_sweep(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<ContentReclaimSweep> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.ensure_content_reclamation_supported()?;
        self.bucket(CONTENT_CONTROL_BUCKET).await?;
        let key = content_reclaim_sweep_key(storage_domain_id, content_id);
        let mut inspect = self.transaction(TransactionOptions::default());
        let bytes = inspect
            .get_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
            .ok_or_else(|| Error::ContentNotFound {
                storage_domain_id: storage_domain_id.to_string(),
                content_id: content_id.to_string(),
            })?;
        let sweep = ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)?;
        self.ensure_content_reclaim_sweep_backend(sweep.backend)?;
        if sweep.state == ContentReclaimSweepRecordState::Reclaimed {
            return Ok(sweep.into_public());
        }

        for index in 0..sweep.chunk_count {
            self.delete_content_chunk(sweep.upload_id, index).await?;
        }
        self.delete_content_descriptor(storage_domain_id, content_id)
            .await?;

        let _quota = self.lock_content_quota(storage_domain_id).await;
        let mut complete = self.transaction(TransactionOptions::default());
        let current_bytes = complete
            .get_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: "Prepared content sweep disappeared before completion".to_owned(),
            })?;
        let current =
            ContentReclaimSweepRecord::decode(&current_bytes, storage_domain_id, content_id)?;
        match current.resume_transition(sweep)? {
            crate::state_transition::DurableTransition::AlreadyApplied(reclaimed) => {
                return Ok(reclaimed.into_public());
            }
            crate::state_transition::DurableTransition::Apply(_) => {}
        }
        self.stage_reclaimed_content_quota(&mut complete, storage_domain_id, content_id)
            .await?;
        complete.delete_bucket(
            CONTENT_CONTROL_BUCKET,
            content_control_key(storage_domain_id, content_id),
        )?;
        complete.delete_bucket(
            CONTENT_CONTROL_BUCKET,
            content_quarantine_key(storage_domain_id, content_id),
        )?;
        complete.delete_bucket(
            CONTENT_CONTROL_BUCKET,
            content_reclaim_grace_key(storage_domain_id, content_id),
        )?;
        let reclaimed = sweep.reclaimed();
        complete.put_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            key.clone(),
            &reclaimed.encode_prefix(),
            &[],
        )?;
        let commit = match complete.commit().await {
            Ok(commit) => commit,
            Err(Error::Conflict { message }) => {
                let mut inspect = self.transaction(TransactionOptions::default());
                let current_bytes = inspect
                    .get_bucket(CONTENT_CONTROL_BUCKET, &key)
                    .await?
                    .ok_or_else(|| Error::Corruption {
                        message: "content reclaim sweep disappeared after a completion conflict"
                            .to_owned(),
                    })?;
                let current = ContentReclaimSweepRecord::decode(
                    &current_bytes,
                    storage_domain_id,
                    content_id,
                )?;
                return match current.resume_transition(sweep)? {
                    crate::state_transition::DurableTransition::AlreadyApplied(reclaimed) => {
                        Ok(reclaimed.into_public())
                    }
                    crate::state_transition::DurableTransition::Apply(_) => {
                        Err(Error::Conflict { message })
                    }
                };
            }
            Err(error) => return Err(error),
        };
        Ok(ContentReclaimSweepRecord {
            reclaimed_at: commit.read_version(),
            ..reclaimed
        }
        .into_public())
    }

    pub(crate) fn ensure_content_reclamation_supported(&self) -> Result<()> {
        self.content_reclaim_sweep_backend().map(|_| ())
    }

    pub(crate) fn content_reclaim_sweep_backend(&self) -> Result<ContentReclaimSweepBackend> {
        match (
            &self.inner.options.content_reclamation,
            &self.inner.options.storage_mode,
        ) {
            (ContentReclamationMode::QualifiedNativeFilesystem, StorageMode::Persistent { .. }) => {
                Ok(ContentReclaimSweepBackend::NativeFilesystem)
            }
            (
                ContentReclamationMode::QualifiedWasiFilesystem,
                StorageMode::HostPersistent {
                    backend: HostStorageBackend::Wasi { .. },
                },
            ) => Ok(ContentReclaimSweepBackend::WasiFilesystem),
            (
                ContentReclamationMode::QualifiedBrowserStorage,
                StorageMode::HostPersistent {
                    backend: HostStorageBackend::Browser { .. },
                },
            ) => Ok(ContentReclaimSweepBackend::BrowserStorage),
            (
                ContentReclamationMode::QualifiedObjectStore(qualification),
                StorageMode::HostPersistent {
                    backend: HostStorageBackend::ObjectStore,
                },
            ) => {
                if !qualification.matches_prefix(self.object_store_db_path()) {
                    return Err(Error::unsupported_backend(
                        "object-store reclamation qualification names a different database prefix",
                    ));
                }
                let client = self.object_storage()?.client();
                if !qualification.matches_client(&client) {
                    return Err(Error::unsupported_backend(
                        "object-store reclamation qualification belongs to a different client instance",
                    ));
                }
                Ok(ContentReclaimSweepBackend::ObjectStore {
                    evidence_digest: qualification.evidence_digest(),
                })
            }
            (ContentReclamationMode::Disabled, _) => Err(Error::unsupported_backend(
                "physical content reclamation is disabled for this database",
            )),
            (ContentReclamationMode::QualifiedNativeFilesystem, _) => {
                Err(Error::unsupported_backend(
                    "native filesystem reclamation qualification does not match this backend",
                ))
            }
            (ContentReclamationMode::QualifiedWasiFilesystem, _) => {
                Err(Error::unsupported_backend(
                    "WASI filesystem reclamation qualification does not match this backend",
                ))
            }
            (ContentReclamationMode::QualifiedBrowserStorage, _) => {
                Err(Error::unsupported_backend(
                    "browser storage reclamation qualification does not match this backend",
                ))
            }
            (ContentReclamationMode::QualifiedObjectStore(_), _) => {
                Err(Error::unsupported_backend(
                    "object-store reclamation qualification does not match this backend",
                ))
            }
        }
    }

    fn ensure_content_reclaim_sweep_backend(
        &self,
        prepared_backend: ContentReclaimSweepBackend,
    ) -> Result<()> {
        let configured = self.content_reclaim_sweep_backend()?;
        if configured != prepared_backend {
            return Err(Error::unsupported_backend(
                "Prepared content sweep provider evidence differs from the current qualification",
            ));
        }
        Ok(())
    }
}
