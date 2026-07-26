use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
    CONTENT_TOKEN_INDEX_BUCKET, ContentQuarantineRecord, ContentQuarantineStage,
    ContentReclaimAuthorization, ContentReclaimBlocker, Error, InactiveAuthorityPolicy, Result,
    Transaction, content_quarantine_key, current_epoch_millis,
};

impl Transaction {
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
}
