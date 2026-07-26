use super::super::{
    CONTENT_CONTROL_BUCKET, ContentId, ContentPhysicalAccountRecord, ContentPhysicalQuota,
    ContentPhysicalReservationRecord, Db, DurabilityMode, Error, Result, SealedContent,
    StorageDomainId, TransactionOptions, UploadId, UploadSessionState,
    content_physical_account_key, content_physical_quota_key, content_physical_reservation_key,
};

impl Db {
    pub(in crate::db::content) const CONTENT_ACCESS_COMMIT_ATTEMPTS: usize = 8;
    pub(in crate::db::content) const CONTENT_LEASE_COMMIT_ATTEMPTS: usize = 8;
    pub(in crate::db::content) const CONTENT_PHYSICAL_HOLD_COMMIT_ATTEMPTS: usize = 8;

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
        let _activity = self.inner.publish_barrier.begin_activity()?;
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
        if let Some(limit) = limit
            && current.accounted_bytes() > limit
        {
            return Err(Error::ContentPhysicalQuotaExceeded {
                limit,
                unique_content_bytes: current.unique_content_bytes(),
                upload_reserved_bytes: current.upload_reserved_bytes(),
                requested_bytes: 0,
            });
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
        let _activity = self.inner.publish_barrier.begin_activity()?;
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
    pub(super) async fn read_content_physical_quota(
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

    pub(super) async fn release_content_upload_reservation(
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

    pub(super) async fn finalize_content_upload_quota(
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

    pub(in crate::db::content) async fn stage_reclaimed_content_quota(
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
}
