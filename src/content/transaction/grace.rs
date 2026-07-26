use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
    CONTENT_TOKEN_INDEX_BUCKET, ContentReclaimAuthorization, ContentReclaimBlocker,
    ContentReclaimGraceRecord, ContentReclaimGraceStage, Duration, Error, InactiveAuthorityPolicy,
    Result, Transaction, content_reclaim_grace_key, current_epoch_millis, duration_millis,
};

impl Transaction {
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
}
