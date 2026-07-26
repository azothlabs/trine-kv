use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
    CONTENT_TOKEN_INDEX_BUCKET, ContentControlRecord, ContentDescriptor, ContentQuarantineRecord,
    ContentReclaimAuthorization, ContentReclaimBlocker, ContentReclaimClockAttestation,
    ContentReclaimGraceRecord, ContentReclaimSweepRecord, ContentReclaimSweepStage, Error,
    InactiveAuthorityPolicy, Result, Transaction, content_control_key, content_quarantine_key,
    content_reclaim_grace_key, content_reclaim_sweep_key,
};

impl Transaction {
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
}
