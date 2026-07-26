use super::{
    CONTENT_CONTROL_BUCKET, ContentReclaimAuthorization, ContentReclaimBlocker,
    ContentReclaimIntentStage, Error, InactiveAuthorityPolicy, Result, Transaction,
    current_epoch_millis,
};

impl Transaction {
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
}
