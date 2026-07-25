use super::{
    CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET, ContentAccessMode, ContentDescriptor,
    ContentHandle, ContentId, ContentLease, ContentLeaseId, ContentLeaseOptions,
    ContentLeaseRecord, ContentPhysicalHold, ContentPhysicalHoldId, ContentPhysicalHoldOptions,
    ContentPhysicalHoldOwnerId, ContentPhysicalHoldRecord, ContentPhysicalHoldRecordState, Db,
    Duration, Error, Result, StorageDomainId, TransactionOptions, content_lease_key,
    content_lease_prefix, content_physical_hold_key, current_epoch_millis, duration_millis,
};

impl Db {
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
    /// administrative, and asynchronous-processing workflows therefore share
    /// one physical fence.
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
}
