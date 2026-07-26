use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
    CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET, ContentAccessCoordinateRecord,
    ContentAccessMode, ContentControlRecord, ContentDescriptor, ContentLeaseId, ContentLeaseRecord,
    ContentPhysicalHoldId, ContentPhysicalHoldRecord, ContentQuarantineRecord,
    ContentReaderDrainAttestationRecord, ContentReclaimAuthorization, ContentReclaimBlocker,
    ContentTokenIndexRecord, Error, InactiveAuthorityPolicy, Result, StorageDomainId, Transaction,
    content_access_coordinate_key, content_control_key, content_lease_prefix,
    content_physical_hold_prefix, content_prefix_range, content_quarantine_key,
    content_reader_drain_attestation_key, content_token_index_prefix,
};

impl Transaction {
    pub(super) async fn require_reclaim_candidate(
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

    pub(super) async fn require_continuous_content_quarantine_for_grace(
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

    pub(super) async fn require_exact_content_reclaim_intent(
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

    pub(super) async fn require_coordinated_content_access(
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

    pub(super) async fn require_content_reader_drain_attestation(
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

    pub(super) async fn require_no_active_content_token(
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

    pub(super) async fn require_no_active_content_lease(
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

    pub(super) async fn require_no_active_content_physical_hold(
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
