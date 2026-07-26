use std::time::Duration;

use crate::{Error, Result, error::ContentReclaimBlocker, transaction::Transaction};

use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
    CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET, ContentAccessCoordinateRecord,
    ContentAccessMode, ContentAttachment, ContentAttachmentScope, ContentChangeId,
    ContentControlRecord, ContentDescriptor, ContentLeaseId, ContentLeaseRecord,
    ContentPhysicalHoldId, ContentPhysicalHoldRecord, ContentQuarantineRecord,
    ContentQuarantineStage, ContentReaderDrainAttestationRecord, ContentReclaimAuthorization,
    ContentReclaimClockAttestation, ContentReclaimGraceRecord, ContentReclaimGraceStage,
    ContentReclaimIntentStage, ContentReclaimSweepRecord, ContentReclaimSweepRecordState,
    ContentReclaimSweepStage, ContentTokenIndexRecord, StorageDomainId, UploadToken,
    UploadTokenRecord, content_access_coordinate_key, content_control_key, content_lease_prefix,
    content_physical_hold_prefix, content_prefix_range, content_quarantine_key,
    content_reader_drain_attestation_key, content_reclaim_grace_key, content_reclaim_sweep_key,
    content_token_index_key, content_token_index_prefix, current_epoch_millis, duration_millis,
    upload_token_key,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InactiveAuthorityPolicy {
    Retain,
    Prune,
}

impl Transaction {
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
        let bytes = match self.get_internal_bucket_sync(CONTENT_TOKEN_BUCKET, &key) {
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
        self.delete_internal_bucket(
            CONTENT_TOKEN_INDEX_BUCKET,
            content_token_index_key(
                attachment.scope().storage_domain_id(),
                attachment.content_id(),
                token,
            ),
        )?;
        self.put_internal_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.record_extension_claim(key, change_id.to_bytes());
        Ok(attachment)
    }

    pub(crate) fn stage_content_activity_sync(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let sweep_key = content_reclaim_sweep_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &sweep_key)? {
            let sweep = ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)?;
            match sweep.state {
                ContentReclaimSweepRecordState::Prepared => {
                    return Err(Error::ContentReclaimBlocked {
                        blocker: ContentReclaimBlocker::SweepPrepared {
                            prepared_at_commit_seq: sweep.prepared_at.as_u64(),
                        },
                    });
                }
                ContentReclaimSweepRecordState::Reclaimed => {
                    self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, sweep_key)?;
                }
            }
        }
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &key)? {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        let quarantine = self
            .get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &quarantine_key)?
            .map(|bytes| ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        let grace_key = content_reclaim_grace_key(storage_domain_id, content_id);
        let grace = self
            .get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &grace_key)?
            .map(|bytes| ContentReclaimGraceRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        if let Some(grace) = grace
            && !quarantine.is_some_and(|record| grace.matches_quarantine(record))
        {
            return Err(Error::Corruption {
                message: "content reclaim grace differs from its quarantine fence".to_owned(),
            });
        }
        if quarantine.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, quarantine_key)?;
        }
        if grace.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, grace_key)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
    }

    fn require_consistent_staged_token(
        &self,
        key: &[u8],
        change_id: ContentChangeId,
    ) -> Result<()> {
        if self
            .extension_claim(key)
            .is_some_and(|existing| existing != change_id.to_bytes())
        {
            return Err(Error::UploadTokenAlreadyConsumed);
        }
        Ok(())
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
        let bytes = match self.get_internal_bucket(CONTENT_TOKEN_BUCKET, &key).await {
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
        self.delete_internal_bucket(
            CONTENT_TOKEN_INDEX_BUCKET,
            content_token_index_key(
                attachment.scope().storage_domain_id(),
                attachment.content_id(),
                token,
            ),
        )?;
        self.put_internal_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.record_extension_claim(key, change_id.to_bytes());
        Ok(attachment)
    }

    pub(crate) async fn stage_content_activity(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let sweep_key = content_reclaim_sweep_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &sweep_key)
            .await?
        {
            let sweep = ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)?;
            match sweep.state {
                ContentReclaimSweepRecordState::Prepared => {
                    return Err(Error::ContentReclaimBlocked {
                        blocker: ContentReclaimBlocker::SweepPrepared {
                            prepared_at_commit_seq: sweep.prepared_at.as_u64(),
                        },
                    });
                }
                ContentReclaimSweepRecordState::Reclaimed => {
                    self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, sweep_key)?;
                }
            }
        }
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
        {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        let quarantine = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
            .map(|bytes| ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        let grace_key = content_reclaim_grace_key(storage_domain_id, content_id);
        let grace = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &grace_key)
            .await?
            .map(|bytes| ContentReclaimGraceRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        if let Some(grace) = grace
            && !quarantine.is_some_and(|record| grace.matches_quarantine(record))
        {
            return Err(Error::Corruption {
                message: "content reclaim grace differs from its quarantine fence".to_owned(),
            });
        }
        if quarantine.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, quarantine_key)?;
        }
        if grace.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, grace_key)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
    }

    pub(crate) async fn stage_content_read_activity(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let sweep_key = content_reclaim_sweep_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &sweep_key)
            .await?
        {
            let sweep = ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)?;
            return match sweep.state {
                ContentReclaimSweepRecordState::Prepared => Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::SweepPrepared {
                        prepared_at_commit_seq: sweep.prepared_at.as_u64(),
                    },
                }),
                ContentReclaimSweepRecordState::Reclaimed => Err(Error::ContentNotFound {
                    storage_domain_id: storage_domain_id.to_string(),
                    content_id: content_id.to_string(),
                }),
            };
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
        {
            let quarantine =
                ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id)?;
            return Err(Error::ContentQuarantined {
                quarantined_at: quarantine.quarantined_at,
            });
        }
        let grace_key = content_reclaim_grace_key(storage_domain_id, content_id);
        if self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &grace_key)
            .await?
            .is_some()
        {
            return Err(Error::Corruption {
                message: "content reclaim grace exists without its quarantine fence".to_owned(),
            });
        }
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
        {
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
        self.put_internal_bucket_with_commit_sequence(
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
        let (control_key, control) = self.require_reclaim_candidate(authorization).await?;

        self.require_no_active_content_token(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        self.require_no_active_content_lease(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        self.require_no_active_content_physical_hold(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        if control.matches_authorization(authorization) {
            let accepted_at = control.accepted_at().ok_or_else(|| Error::Corruption {
                message: "matching reclaim intent has no acceptance sequence".to_owned(),
            })?;
            return Ok(ContentReclaimIntentStage::Existing { accepted_at });
        }

        let intent = control.reclaim_intent(authorization);
        self.put_internal_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            control_key,
            &intent.encode_prefix(),
            &[],
        )?;
        Ok(ContentReclaimIntentStage::Staged)
    }

    async fn require_reclaim_candidate(
        &mut self,
        authorization: ContentReclaimAuthorization,
    ) -> Result<(Vec<u8>, ContentControlRecord)> {
        for bucket in [
            CONTENT_CONTROL_BUCKET,
            CONTENT_TOKEN_INDEX_BUCKET,
            CONTENT_LEASE_BUCKET,
            CONTENT_PHYSICAL_HOLD_BUCKET,
        ] {
            self.database().internal_bucket(bucket).await?;
        }
        self.require_coordinated_content_access(authorization.storage_domain_id())
            .await?;
        let descriptor = self
            .database()
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
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &control_key)
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
        Ok((control_key, control))
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
        self.database()
            .internal_bucket(CONTENT_CONTROL_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_TOKEN_INDEX_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_LEASE_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_PHYSICAL_HOLD_BUCKET)
            .await?;

        let access = self
            .require_coordinated_content_access(authorization.storage_domain_id())
            .await?;
        let drain = self
            .require_content_reader_drain_attestation(access)
            .await?;
        let intent_accepted_at = self
            .require_exact_content_reclaim_intent(authorization)
            .await?;

        self.require_no_active_content_token(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        self.require_no_active_content_lease(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        self.require_no_active_content_physical_hold(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;

        let quarantine_key = content_quarantine_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
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
        self.put_internal_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            quarantine_key,
            &requested.encode_prefix(),
            &[],
        )?;
        Ok(ContentQuarantineStage::Staged)
    }

    /// Stages a durable earliest-time scheduling record for quarantined content.
    ///
    /// The continuous quarantine, its original reclaim intent, leased-only
    /// barrier, reader-drain attestation, descriptor, activity, token, lease,
    /// and hold state are rechecked in this transaction. The higher layer must
    /// repeat its logical proof and reachability checks in the same transaction
    /// before this call. The authorization may be the original quarantine proof
    /// or a fresh proof verified at or after the durable quarantine coordinate;
    /// the latter recovers a quarantine-committed/grace-not-committed restart
    /// after the original short-lived proof expired.
    ///
    /// `observation_delay` must be at least one millisecond and is measured from
    /// the wall-clock observation made before commit. The resulting Unix
    /// deadline is a scheduling hint only: it may be closer than that delay when
    /// the commit becomes durable, does not authorize deletion, and this method
    /// deletes no descriptor or byte.
    /// Token/attachment or physical-hold activity removes both grace and
    /// quarantine while returning content control to Active atomically.
    ///
    /// # Returns
    ///
    /// [`ContentReclaimGraceStage::Staged`] means the write still needs this
    /// transaction to commit. `Existing` reports the original commit sequence
    /// for a retry with the same duration. After a possible lost response, use
    /// [`Db::content_reclaim_grace`](crate::Db::content_reclaim_grace) to
    /// discover the committed result; if no
    /// grace exists, a fresh higher-layer proof may safely retry this method
    /// while the original quarantine remains continuously durable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentReclaimBlocked`] when the quarantine or another
    /// required lifecycle coordinate is absent or stale. Invalid duration,
    /// deadline overflow, malformed protected state, and commit conflicts fail
    /// closed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentReclaimAuthorization, ContentReclaimGraceStage, Transaction,
    /// };
    ///
    /// async fn stage(
    ///     transaction: &mut Transaction,
    ///     authorization: ContentReclaimAuthorization,
    /// ) -> trine_kv::Result<()> {
    ///     // The caller has already rechecked logical state in `transaction`.
    ///     assert_eq!(
    ///         transaction
    ///             .stage_content_reclaim_grace(authorization, Duration::from_secs(60))
    ///             .await?,
    ///         ContentReclaimGraceStage::Staged,
    ///     );
    ///     Ok(())
    /// }
    /// ```
    pub async fn stage_content_reclaim_grace(
        &mut self,
        authorization: ContentReclaimAuthorization,
        observation_delay: Duration,
    ) -> Result<ContentReclaimGraceStage> {
        let requested_duration_ms = duration_millis(observation_delay, "content reclaim grace")?;
        let observed_at_unix_ms = current_epoch_millis()?;
        let not_before_unix_ms = observed_at_unix_ms
            .checked_add(requested_duration_ms)
            .ok_or_else(|| Error::invalid_options("content reclaim-grace deadline overflow"))?;
        if observed_at_unix_ms >= authorization.expires_at_unix_ms() {
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
                "content reclaim-grace verification sequence is invalid for this transaction",
            ));
        }
        self.database()
            .internal_bucket(CONTENT_CONTROL_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_TOKEN_INDEX_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_LEASE_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_PHYSICAL_HOLD_BUCKET)
            .await?;

        let access = self
            .require_coordinated_content_access(authorization.storage_domain_id())
            .await?;
        let drain = self
            .require_content_reader_drain_attestation(access)
            .await?;
        self.require_no_active_content_token(
            authorization,
            observed_at_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        self.require_no_active_content_lease(
            authorization,
            observed_at_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        self.require_no_active_content_physical_hold(
            authorization,
            observed_at_unix_ms,
            InactiveAuthorityPolicy::Retain,
        )
        .await?;
        let quarantine = self
            .require_continuous_content_quarantine_for_grace(authorization, access, drain)
            .await?;

        let key = content_reclaim_grace_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
        {
            let existing = ContentReclaimGraceRecord::decode(
                &bytes,
                authorization.storage_domain_id(),
                authorization.content_id(),
            )?;
            if existing.matches_quarantine(quarantine)
                && existing.requested_duration_ms == requested_duration_ms
            {
                return Ok(ContentReclaimGraceStage::Existing {
                    started_at: existing.started_at,
                });
            }
            return Err(Error::invalid_options(
                "existing content reclaim grace differs from this request",
            ));
        }

        let requested = ContentReclaimGraceRecord::requested(
            quarantine,
            requested_duration_ms,
            observed_at_unix_ms,
            not_before_unix_ms,
        );
        self.put_internal_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            key,
            &requested.encode_prefix(),
            &[],
        )?;
        Ok(ContentReclaimGraceStage::Staged)
    }

    /// Stages the irreversible, crash-resumable physical sweep fence.
    ///
    /// The higher layer must repeat its fresh exact logical-absence proof in
    /// this same transaction. `authorization.verified_at()` must be at or after
    /// the durable grace start. Trine KV then rechecks the exact leased-only,
    /// drain, quarantine, grace, clock-attestation, descriptor, activity,
    /// token, lease, hold, and enabled-backend coordinates before recording a
    /// Prepared manifest. This method deletes no bytes; after commit, call
    /// [`Db::resume_content_reclaim_sweep`](crate::Db::resume_content_reclaim_sweep).
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedBackend`] while reclamation is disabled or
    /// for an unqualified backend. Stale/expired proof, missing lifecycle
    /// state, active authority, malformed records, and optimistic conflicts all
    /// fail closed.
    #[allow(clippy::too_many_lines)] // The final gate intentionally keeps every protected recheck visible.
    pub async fn stage_content_reclaim_sweep(
        &mut self,
        authorization: ContentReclaimAuthorization,
        clock_attestation: ContentReclaimClockAttestation,
    ) -> Result<ContentReclaimSweepStage> {
        let sweep_backend = self.database().content_reclaim_sweep_backend()?;
        let now_unix_ms = clock_attestation.observed_at_unix_ms();
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
                "content reclaim-sweep verification sequence is invalid for this transaction",
            ));
        }
        if clock_attestation.storage_domain_id() != authorization.storage_domain_id()
            || clock_attestation.content_id() != authorization.content_id()
        {
            return Err(Error::invalid_options(
                "content reclaim-clock attestation names different content",
            ));
        }

        self.database()
            .internal_bucket(CONTENT_CONTROL_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_TOKEN_INDEX_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_LEASE_BUCKET)
            .await?;
        self.database()
            .internal_bucket(CONTENT_PHYSICAL_HOLD_BUCKET)
            .await?;

        let access = self
            .require_coordinated_content_access(authorization.storage_domain_id())
            .await?;
        let drain = self
            .require_content_reader_drain_attestation(access)
            .await?;
        let quarantine_key = content_quarantine_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let quarantine_bytes = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
            .ok_or(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::QuarantineRequired,
            })?;
        let quarantine = ContentQuarantineRecord::decode(
            &quarantine_bytes,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;
        if quarantine.barrier_id != access.barrier_id
            || quarantine.barrier_enforced_at != access.enforced_at
            || quarantine.drain_attestation_id != drain.attestation_id
        {
            return Err(Error::Corruption {
                message: "content reclaim-sweep quarantine differs from access coordinates"
                    .to_owned(),
            });
        }
        let grace_key = content_reclaim_grace_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let grace_bytes = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &grace_key)
            .await?
            .ok_or(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::QuarantineRequired,
            })?;
        let grace = ContentReclaimGraceRecord::decode(
            &grace_bytes,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;
        if !grace.matches_quarantine(quarantine) {
            return Err(Error::Corruption {
                message: "content reclaim-sweep grace differs from quarantine".to_owned(),
            });
        }
        if authorization.verified_at().as_u64() < grace.started_at.as_u64() {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded {
                    activity_at_commit_seq: grace.started_at.as_u64(),
                    verified_at_commit_seq: authorization.verified_at().as_u64(),
                },
            });
        }
        if clock_attestation.grace_started_at() != grace.started_at
            || clock_attestation.observed_at_unix_ms() < grace.not_before_unix_ms
        {
            return Err(Error::invalid_options(
                "content reclaim-clock attestation is not bound to completed grace",
            ));
        }

        let descriptor_bytes = self
            .database()
            .read_content_descriptor(
                authorization.storage_domain_id(),
                authorization.content_id(),
            )
            .await?
            .ok_or_else(|| Error::ContentNotFound {
                storage_domain_id: authorization.storage_domain_id().to_string(),
                content_id: authorization.content_id().to_string(),
            })?;
        let descriptor = ContentDescriptor::decode(
            &descriptor_bytes,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;
        let control_key = content_control_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let control_bytes = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &control_key)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: "reclaim-sweep content is missing physical control state".to_owned(),
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
        self.require_no_active_content_token(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Prune,
        )
        .await?;
        self.require_no_active_content_lease(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Prune,
        )
        .await?;
        self.require_no_active_content_physical_hold(
            authorization,
            now_unix_ms,
            InactiveAuthorityPolicy::Prune,
        )
        .await?;

        let sweep_key = content_reclaim_sweep_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &sweep_key)
            .await?
        {
            let existing = ContentReclaimSweepRecord::decode(
                &bytes,
                authorization.storage_domain_id(),
                authorization.content_id(),
            )?;
            if existing.matches_request(authorization, clock_attestation, sweep_backend) {
                return Ok(ContentReclaimSweepStage::Existing {
                    prepared_at: existing.prepared_at,
                });
            }
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::SweepPrepared {
                    prepared_at_commit_seq: existing.prepared_at.as_u64(),
                },
            });
        }
        let requested = ContentReclaimSweepRecord::prepared(
            authorization,
            quarantine,
            grace,
            clock_attestation,
            descriptor,
            sweep_backend,
        );
        self.put_internal_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            sweep_key,
            &requested.encode_prefix(),
            &[],
        )?;
        Ok(ContentReclaimSweepStage::Staged)
    }

    async fn require_continuous_content_quarantine_for_grace(
        &mut self,
        authorization: ContentReclaimAuthorization,
        access: ContentAccessCoordinateRecord,
        drain: ContentReaderDrainAttestationRecord,
    ) -> Result<ContentQuarantineRecord> {
        let descriptor = self
            .database()
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
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &control_key)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: "sealed content is missing its physical control record".to_owned(),
            })?;
        let control = ContentControlRecord::decode(
            &control_bytes,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;

        let quarantine_key = content_quarantine_key(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let Some(quarantine_bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
        else {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::QuarantineRequired,
            });
        };
        let quarantine = ContentQuarantineRecord::decode(
            &quarantine_bytes,
            authorization.storage_domain_id(),
            authorization.content_id(),
        )?;
        if !control.matches_quarantine(quarantine)
            || quarantine.barrier_id != access.barrier_id
            || quarantine.barrier_enforced_at != access.enforced_at
            || quarantine.drain_attestation_id != drain.attestation_id
        {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::QuarantineRequired,
            });
        }

        let exact_original = quarantine.matches_authorization(authorization);
        if !exact_original
            && authorization.verified_at().as_u64() < quarantine.quarantined_at.as_u64()
        {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded {
                    activity_at_commit_seq: quarantine.quarantined_at.as_u64(),
                    verified_at_commit_seq: authorization.verified_at().as_u64(),
                },
            });
        }
        let physical_activity = control.physical_activity_commit_seq();
        if physical_activity > authorization.verified_at().as_u64() {
            return Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded {
                    activity_at_commit_seq: physical_activity,
                    verified_at_commit_seq: authorization.verified_at().as_u64(),
                },
            });
        }
        Ok(quarantine)
    }

    async fn require_exact_content_reclaim_intent(
        &mut self,
        authorization: ContentReclaimAuthorization,
    ) -> Result<crate::ReadVersion> {
        let descriptor = self
            .database()
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
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &key)
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
        let barrier_id = match self
            .database()
            .content_access_mode(storage_domain_id)
            .await?
        {
            ContentAccessMode::CompatibleUnleased => {
                return Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::UnleasedAccessAllowed,
                });
            }
            ContentAccessMode::LeasedOnly { barrier_id } => barrier_id,
        };
        let access_key = content_access_coordinate_key(storage_domain_id);
        let Some(access_bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &access_key)
            .await?
        else {
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
        let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
        else {
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
        inactive_policy: InactiveAuthorityPolicy,
    ) -> Result<()> {
        let prefix = content_token_index_prefix(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let range = content_prefix_range(prefix.clone())?;
        let mut expired = Vec::new();
        for entry in self
            .range_internal_bucket(CONTENT_TOKEN_INDEX_BUCKET, range)
            .await?
        {
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
            expired.push((entry.key, hash));
        }
        if inactive_policy == InactiveAuthorityPolicy::Prune {
            for (key, hash) in expired {
                self.delete_internal_bucket(CONTENT_TOKEN_INDEX_BUCKET, key)?;
                self.delete_internal_bucket(CONTENT_TOKEN_BUCKET, hash.to_vec())?;
            }
        }
        Ok(())
    }

    async fn require_no_active_content_lease(
        &mut self,
        authorization: ContentReclaimAuthorization,
        now_unix_ms: u64,
        inactive_policy: InactiveAuthorityPolicy,
    ) -> Result<()> {
        let prefix = content_lease_prefix(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let range = content_prefix_range(prefix.clone())?;
        let mut expired = Vec::new();
        for entry in self
            .range_internal_bucket(CONTENT_LEASE_BUCKET, range)
            .await?
        {
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
            expired.push(entry.key);
        }
        if inactive_policy == InactiveAuthorityPolicy::Prune {
            for key in expired {
                self.delete_internal_bucket(CONTENT_LEASE_BUCKET, key)?;
            }
        }
        Ok(())
    }

    async fn require_no_active_content_physical_hold(
        &mut self,
        authorization: ContentReclaimAuthorization,
        now_unix_ms: u64,
        inactive_policy: InactiveAuthorityPolicy,
    ) -> Result<()> {
        let prefix = content_physical_hold_prefix(
            authorization.storage_domain_id(),
            authorization.content_id(),
        );
        let range = content_prefix_range(prefix.clone())?;
        let mut inactive = Vec::new();
        for entry in self
            .range_internal_bucket(CONTENT_PHYSICAL_HOLD_BUCKET, range)
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
            inactive.push(entry.key);
        }
        if inactive_policy == InactiveAuthorityPolicy::Prune {
            for key in inactive {
                self.delete_internal_bucket(CONTENT_PHYSICAL_HOLD_BUCKET, key)?;
            }
        }
        Ok(())
    }
}
