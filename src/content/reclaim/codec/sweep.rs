use super::{
    CONTENT_RECLAIM_SWEEP_MAGIC, CONTENT_RECLAIM_SWEEP_PREPARED, CONTENT_RECLAIM_SWEEP_RECLAIMED,
    ContentAccessBarrierId, ContentDescriptor, ContentId, ContentQuarantineRecord,
    ContentReaderDrainAttestationId, ContentReclaimAuthorization, ContentReclaimClockAttestation,
    ContentReclaimClockAttestationId, ContentReclaimClockCoordinatorId,
    ContentReclaimClockEvidenceDigest, ContentReclaimGraceRecord, ContentReclaimProofToken,
    ContentReclaimSweep, ContentReclaimSweepBackend, Error, ObjectStoreReclamationEvidenceDigest,
    Result, StorageDomainId, UploadId, array_at, decode_content_id,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentReclaimSweepRecordState {
    Prepared,
    Reclaimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentReclaimSweepRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) proof_token: ContentReclaimProofToken,
    pub(crate) verified_at: crate::ReadVersion,
    pub(crate) proof_expires_at_unix_ms: u64,
    pub(crate) quarantined_at: crate::ReadVersion,
    pub(crate) grace_started_at: crate::ReadVersion,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) barrier_enforced_at: crate::ReadVersion,
    pub(crate) drain_attestation_id: ContentReaderDrainAttestationId,
    pub(crate) clock_attestation: ContentReclaimClockAttestation,
    pub(crate) upload_id: UploadId,
    pub(crate) chunk_count: u64,
    pub(crate) backend: ContentReclaimSweepBackend,
    pub(crate) state: ContentReclaimSweepRecordState,
    pub(crate) prepared_at: crate::ReadVersion,
    pub(crate) reclaimed_at: crate::ReadVersion,
}

impl ContentReclaimSweepRecord {
    pub(crate) fn prepared(
        authorization: ContentReclaimAuthorization,
        quarantine: ContentQuarantineRecord,
        grace: ContentReclaimGraceRecord,
        clock_attestation: ContentReclaimClockAttestation,
        descriptor: ContentDescriptor,
        backend: ContentReclaimSweepBackend,
    ) -> Self {
        Self {
            storage_domain_id: authorization.storage_domain_id(),
            content_id: authorization.content_id(),
            proof_token: authorization.proof_token(),
            verified_at: authorization.verified_at(),
            proof_expires_at_unix_ms: authorization.expires_at_unix_ms(),
            quarantined_at: quarantine.quarantined_at,
            grace_started_at: grace.started_at,
            barrier_id: quarantine.barrier_id,
            barrier_enforced_at: quarantine.barrier_enforced_at,
            drain_attestation_id: quarantine.drain_attestation_id,
            clock_attestation,
            upload_id: descriptor.upload_id(),
            chunk_count: descriptor.chunk_count(),
            backend,
            state: ContentReclaimSweepRecordState::Prepared,
            prepared_at: crate::ReadVersion::from_u64(0),
            reclaimed_at: crate::ReadVersion::from_u64(0),
        }
    }

    pub(crate) const fn reclaimed(self) -> Self {
        Self {
            state: ContentReclaimSweepRecordState::Reclaimed,
            reclaimed_at: crate::ReadVersion::from_u64(0),
            ..self
        }
    }

    pub(crate) fn resume_transition(
        self,
        prepared: Self,
    ) -> Result<crate::state_transition::DurableTransition<Self>> {
        use crate::state_transition::DurableTransition;

        if prepared.state != ContentReclaimSweepRecordState::Prepared
            || prepared.reclaimed_at.as_u64() != 0
        {
            return Err(Error::Corruption {
                message: "content reclaim resume expected a Prepared durable record".to_owned(),
            });
        }
        if self == prepared {
            return Ok(DurableTransition::Apply(self));
        }
        if self.state == ContentReclaimSweepRecordState::Reclaimed
            && self.reclaimed_at.as_u64() >= self.prepared_at.as_u64()
            && (Self {
                state: ContentReclaimSweepRecordState::Prepared,
                reclaimed_at: crate::ReadVersion::from_u64(0),
                ..self
            }) == prepared
        {
            return Ok(DurableTransition::AlreadyApplied(self));
        }
        Err(Error::Corruption {
            message:
                "content reclaim sweep changed outside its legal Prepared-to-Reclaimed transition"
                    .to_owned(),
        })
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 318;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_RECLAIM_SWEEP_MAGIC);
        bytes.push(match self.state {
            ContentReclaimSweepRecordState::Prepared => CONTENT_RECLAIM_SWEEP_PREPARED,
            ContentReclaimSweepRecordState::Reclaimed => CONTENT_RECLAIM_SWEEP_RECLAIMED,
        });
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.proof_token.to_bytes());
        bytes.extend_from_slice(&self.verified_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.proof_expires_at_unix_ms.to_le_bytes());
        bytes.extend_from_slice(&self.quarantined_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.grace_started_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_enforced_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.drain_attestation_id.to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.attestation_id().to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.coordinator_id().to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.evidence_digest().to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.observed_at_unix_ms().to_le_bytes());
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        match self.backend {
            ContentReclaimSweepBackend::NativeFilesystem => {
                bytes.push(0);
                bytes.extend_from_slice(&[0_u8; 33]);
            }
            ContentReclaimSweepBackend::ObjectStore { evidence_digest } => {
                bytes.push(1);
                bytes.extend_from_slice(&evidence_digest.to_bytes());
            }
            ContentReclaimSweepBackend::WasiFilesystem => {
                bytes.push(2);
                bytes.extend_from_slice(&[0_u8; 33]);
            }
            ContentReclaimSweepBackend::BrowserStorage => {
                bytes.push(3);
                bytes.extend_from_slice(&[0_u8; 33]);
            }
        }
        bytes.extend_from_slice(&self.prepared_at.as_u64().to_be_bytes());
        bytes
    }

    #[allow(clippy::too_many_lines)] // Fixed-width protected record decoding keeps offsets together.
    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 326;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_RECLAIM_SWEEP_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content reclaim-sweep header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            9,
            "content reclaim-sweep storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 25, "content reclaim-sweep identity")?;
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content reclaim-sweep record differs from its protected key".to_owned(),
            });
        }
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            58,
            "content reclaim-sweep proof token",
        )?);
        let verified_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            107,
            "content reclaim-sweep verified sequence",
        )?));
        let proof_expires_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            115,
            "content reclaim-sweep proof expiry",
        )?);
        let quarantined_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            123,
            "content reclaim-sweep quarantine sequence",
        )?));
        let grace_started_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            131,
            "content reclaim-sweep grace sequence",
        )?));
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            139,
            "content reclaim-sweep barrier identity",
        )?)?;
        let barrier_enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            155,
            "content reclaim-sweep barrier sequence",
        )?));
        let drain_attestation_id = ContentReaderDrainAttestationId::from_bytes(array_at::<16>(
            bytes,
            163,
            "content reclaim-sweep drain identity",
        )?)?;
        let clock_attestation_id = ContentReclaimClockAttestationId::from_bytes(array_at::<16>(
            bytes,
            179,
            "content reclaim-sweep clock identity",
        )?)?;
        let clock_coordinator_id = ContentReclaimClockCoordinatorId::from_bytes(array_at::<16>(
            bytes,
            195,
            "content reclaim-sweep clock coordinator",
        )?);
        let clock_evidence_digest = ContentReclaimClockEvidenceDigest::from_bytes(array_at::<33>(
            bytes,
            211,
            "content reclaim-sweep clock evidence",
        )?)?;
        let clock_observed_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            244,
            "content reclaim-sweep clock observation",
        )?);
        let upload_id = UploadId::from_bytes(array_at::<16>(
            bytes,
            252,
            "content reclaim-sweep upload identity",
        )?);
        let chunk_count = u64::from_le_bytes(array_at::<8>(
            bytes,
            268,
            "content reclaim-sweep chunk count",
        )?);
        let backend = match bytes[276] {
            0 if bytes[277..310].iter().all(|byte| *byte == 0) => {
                ContentReclaimSweepBackend::NativeFilesystem
            }
            1 => ContentReclaimSweepBackend::ObjectStore {
                evidence_digest: ObjectStoreReclamationEvidenceDigest::from_bytes(array_at::<33>(
                    bytes,
                    277,
                    "content reclaim-sweep provider evidence",
                )?)?,
            },
            2 if bytes[277..310].iter().all(|byte| *byte == 0) => {
                ContentReclaimSweepBackend::WasiFilesystem
            }
            3 if bytes[277..310].iter().all(|byte| *byte == 0) => {
                ContentReclaimSweepBackend::BrowserStorage
            }
            _ => {
                return Err(Error::Corruption {
                    message: "content reclaim-sweep has invalid backend evidence".to_owned(),
                });
            }
        };
        let stored_prepared_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            310,
            "content reclaim-sweep prepared sequence",
        )?));
        let state_commit_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            318,
            "content reclaim-sweep state sequence",
        )?));
        let (state, prepared_at, reclaimed_at) = match bytes[8] {
            CONTENT_RECLAIM_SWEEP_PREPARED if stored_prepared_at.as_u64() == 0 => (
                ContentReclaimSweepRecordState::Prepared,
                state_commit_at,
                crate::ReadVersion::from_u64(0),
            ),
            CONTENT_RECLAIM_SWEEP_RECLAIMED
                if stored_prepared_at.as_u64() > 0
                    && state_commit_at.as_u64() >= stored_prepared_at.as_u64() =>
            {
                (
                    ContentReclaimSweepRecordState::Reclaimed,
                    stored_prepared_at,
                    state_commit_at,
                )
            }
            _ => {
                return Err(Error::Corruption {
                    message: "content reclaim-sweep has invalid state coordinates".to_owned(),
                });
            }
        };
        if verified_at.as_u64() < grace_started_at.as_u64()
            || proof_expires_at_unix_ms == 0
            || quarantined_at.as_u64() == 0
            || grace_started_at.as_u64() < quarantined_at.as_u64()
            || barrier_enforced_at.as_u64() == 0
            || clock_observed_at_unix_ms == 0
            || prepared_at.as_u64() < grace_started_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content reclaim-sweep has invalid protected coordinates".to_owned(),
            });
        }
        let clock_attestation = ContentReclaimClockAttestation {
            storage_domain_id,
            content_id,
            attestation_id: clock_attestation_id,
            coordinator_id: clock_coordinator_id,
            evidence_digest: clock_evidence_digest,
            grace_started_at,
            observed_at_unix_ms: clock_observed_at_unix_ms,
        };
        Ok(Self {
            storage_domain_id,
            content_id,
            proof_token,
            verified_at,
            proof_expires_at_unix_ms,
            quarantined_at,
            grace_started_at,
            barrier_id,
            barrier_enforced_at,
            drain_attestation_id,
            clock_attestation,
            upload_id,
            chunk_count,
            backend,
            state,
            prepared_at,
            reclaimed_at,
        })
    }

    pub(crate) fn matches_request(
        self,
        authorization: ContentReclaimAuthorization,
        clock_attestation: ContentReclaimClockAttestation,
        backend: ContentReclaimSweepBackend,
    ) -> bool {
        self.state == ContentReclaimSweepRecordState::Prepared
            && self.storage_domain_id == authorization.storage_domain_id()
            && self.content_id == authorization.content_id()
            && self.proof_token == authorization.proof_token()
            && self.verified_at == authorization.verified_at()
            && self.proof_expires_at_unix_ms == authorization.expires_at_unix_ms()
            && self.clock_attestation == clock_attestation
            && self.backend == backend
    }

    pub(crate) const fn into_public(self) -> ContentReclaimSweep {
        ContentReclaimSweep {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            backend: self.backend,
            prepared_at: self.prepared_at,
            reclaimed_at: match self.state {
                ContentReclaimSweepRecordState::Prepared => None,
                ContentReclaimSweepRecordState::Reclaimed => Some(self.reclaimed_at),
            },
        }
    }
}

pub(crate) fn content_reclaim_sweep_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16 + 33);
    key.extend_from_slice(b"sweep:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}
