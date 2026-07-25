use super::{
    Arc, CONTENT_ACCESS_BARRIER_MAGIC, CONTENT_ACCESS_COORDINATE_MAGIC, CONTENT_CONTROL_ACTIVE,
    CONTENT_CONTROL_MAGIC, CONTENT_CONTROL_RECLAIM_INTENT, CONTENT_QUARANTINE_MAGIC,
    CONTENT_READER_DRAIN_ATTESTATION_MAGIC, CONTENT_RECLAIM_GRACE_MAGIC,
    CONTENT_RECLAIM_PROOF_TOKEN_BYTES, CONTENT_RECLAIM_SWEEP_MAGIC, CONTENT_RECLAIM_SWEEP_PREPARED,
    CONTENT_RECLAIM_SWEEP_RECLAIMED, ContentAccessBarrier, ContentAccessBarrierId,
    ContentDescriptor, ContentId, ContentQuarantine, ContentReaderDrainAttestation,
    ContentReaderDrainAttestationId, ContentReaderDrainAttestationOptions,
    ContentReaderDrainCoordinatorId, ContentReaderDrainEvidenceDigest, ContentReaderDrainKind,
    ContentReclaimAuthorization, ContentReclaimClockAttestation, ContentReclaimClockAttestationId,
    ContentReclaimClockCoordinatorId, ContentReclaimClockEvidenceDigest, ContentReclaimGrace,
    ContentReclaimProofToken, ContentReclaimSweep, ContentReclaimSweepBackend, Error,
    ObjectStoreReclamationEvidenceDigest, Result, StorageDomainId, UploadId, array_at,
    decode_content_id,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentAccessBarrierRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
}

impl ContentAccessBarrierRecord {
    pub(crate) fn encode(self) -> Arc<[u8]> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16);
        bytes.extend_from_slice(CONTENT_ACCESS_BARRIER_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.into()
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_ACCESS_BARRIER_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content access-barrier header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content access-barrier storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content access-barrier identity",
        )?)?;
        if stored_domain != storage_domain_id {
            return Err(Error::Corruption {
                message: "content access-barrier record differs from its storage path".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentAccessCoordinateRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) enforced_at: crate::ReadVersion,
}

impl ContentAccessCoordinateRecord {
    pub(crate) fn commit_prefix(
        storage_domain_id: StorageDomainId,
        barrier_id: ContentAccessBarrierId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16);
        bytes.extend_from_slice(CONTENT_ACCESS_COORDINATE_MAGIC);
        bytes.extend_from_slice(&storage_domain_id.to_bytes());
        bytes.extend_from_slice(&barrier_id.to_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_ACCESS_COORDINATE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content access-coordinate header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content access-coordinate storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content access-coordinate barrier identity",
        )?)?;
        let enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            40,
            "content access-coordinate commit sequence",
        )?));
        if stored_domain != storage_domain_id || enforced_at.as_u64() == 0 {
            return Err(Error::Corruption {
                message: "content access-coordinate has invalid protected coordinates".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
            enforced_at,
        })
    }
}

pub(crate) fn content_access_coordinate_key(storage_domain_id: StorageDomainId) -> Vec<u8> {
    let mut key = Vec::with_capacity(7 + 16);
    key.extend_from_slice(b"access:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentReaderDrainAttestationRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) attestation_id: ContentReaderDrainAttestationId,
    pub(crate) options: ContentReaderDrainAttestationOptions,
    pub(crate) barrier_enforced_at: crate::ReadVersion,
    pub(crate) attested_at: crate::ReadVersion,
}

impl ContentReaderDrainAttestationRecord {
    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 16 + 16 + 1 + 16 + 33 + 8;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_READER_DRAIN_ATTESTATION_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.extend_from_slice(&self.attestation_id.to_bytes());
        bytes.push(self.options.kind().tag());
        bytes.extend_from_slice(&self.options.coordinator_id().to_bytes());
        bytes.extend_from_slice(&self.options.evidence_digest().to_bytes());
        bytes.extend_from_slice(&self.barrier_enforced_at.as_u64().to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 16 + 1 + 16 + 33 + 8 + 8;
        if bytes.len() != RECORD_LEN
            || bytes.get(..8) != Some(CONTENT_READER_DRAIN_ATTESTATION_MAGIC)
        {
            return Err(Error::InvalidFormat {
                message: "invalid content reader-drain attestation header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content reader-drain storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content reader-drain barrier identity",
        )?)?;
        let attestation_id = ContentReaderDrainAttestationId::from_bytes(array_at::<16>(
            bytes,
            40,
            "content reader-drain attestation identity",
        )?)?;
        let kind = ContentReaderDrainKind::from_tag(bytes[56])?;
        let coordinator_id = ContentReaderDrainCoordinatorId::from_bytes(array_at::<16>(
            bytes,
            57,
            "content reader-drain coordinator identity",
        )?);
        let evidence_digest = ContentReaderDrainEvidenceDigest::from_bytes(array_at::<33>(
            bytes,
            73,
            "content reader-drain evidence digest",
        )?)?;
        let barrier_enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            106,
            "content reader-drain barrier sequence",
        )?));
        let attested_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            114,
            "content reader-drain attestation sequence",
        )?));
        if stored_domain != storage_domain_id
            || barrier_enforced_at.as_u64() == 0
            || attested_at.as_u64() < barrier_enforced_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content reader-drain attestation has invalid protected coordinates"
                    .to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
            attestation_id,
            options: ContentReaderDrainAttestationOptions::new(
                kind,
                coordinator_id,
                evidence_digest,
            ),
            barrier_enforced_at,
            attested_at,
        })
    }

    pub(crate) fn matches_request(
        self,
        barrier: ContentAccessBarrier,
        attestation_id: ContentReaderDrainAttestationId,
        options: ContentReaderDrainAttestationOptions,
    ) -> bool {
        self.storage_domain_id == barrier.storage_domain_id()
            && self.barrier_id == barrier.barrier_id()
            && self.attestation_id == attestation_id
            && self.options == options
            && self.barrier_enforced_at == barrier.enforced_at()
    }

    pub(crate) const fn into_public(self) -> ContentReaderDrainAttestation {
        ContentReaderDrainAttestation {
            storage_domain_id: self.storage_domain_id,
            barrier_id: self.barrier_id,
            attestation_id: self.attestation_id,
            options: self.options,
            barrier_enforced_at: self.barrier_enforced_at,
            attested_at: self.attested_at,
        }
    }
}

pub(crate) fn content_reader_drain_attestation_key(storage_domain_id: StorageDomainId) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16);
    key.extend_from_slice(b"drain:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentQuarantineRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) proof_token: ContentReclaimProofToken,
    pub(crate) verified_at: crate::ReadVersion,
    pub(crate) proof_expires_at_unix_ms: u64,
    pub(crate) intent_accepted_at: crate::ReadVersion,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) barrier_enforced_at: crate::ReadVersion,
    pub(crate) drain_attestation_id: ContentReaderDrainAttestationId,
    pub(crate) quarantined_at: crate::ReadVersion,
}

impl ContentQuarantineRecord {
    pub(crate) fn requested(
        authorization: ContentReclaimAuthorization,
        intent_accepted_at: crate::ReadVersion,
        access: ContentAccessCoordinateRecord,
        drain: ContentReaderDrainAttestationRecord,
    ) -> Self {
        Self {
            storage_domain_id: authorization.storage_domain_id(),
            content_id: authorization.content_id(),
            proof_token: authorization.proof_token(),
            verified_at: authorization.verified_at(),
            proof_expires_at_unix_ms: authorization.expires_at_unix_ms(),
            intent_accepted_at,
            barrier_id: access.barrier_id,
            barrier_enforced_at: access.enforced_at,
            drain_attestation_id: drain.attestation_id,
            quarantined_at: crate::ReadVersion::from_u64(0),
        }
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 16 + 8 + 16;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_QUARANTINE_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.proof_token.to_bytes());
        bytes.extend_from_slice(&self.verified_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.proof_expires_at_unix_ms.to_le_bytes());
        bytes.extend_from_slice(&self.intent_accepted_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_enforced_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.drain_attestation_id.to_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 16 + 8 + 16 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_QUARANTINE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content quarantine header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content quarantine storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content quarantine identity")?;
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            57,
            "content quarantine proof token",
        )?);
        let verified_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            106,
            "content quarantine verified sequence",
        )?));
        let proof_expires_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            114,
            "content quarantine proof expiry",
        )?);
        let intent_accepted_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            122,
            "content quarantine intent sequence",
        )?));
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            130,
            "content quarantine barrier identity",
        )?)?;
        let barrier_enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            146,
            "content quarantine barrier sequence",
        )?));
        let drain_attestation_id = ContentReaderDrainAttestationId::from_bytes(array_at::<16>(
            bytes,
            154,
            "content quarantine drain attestation identity",
        )?)?;
        let quarantined_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            170,
            "content quarantine sequence",
        )?));
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content quarantine record differs from its protected key".to_owned(),
            });
        }
        if verified_at.as_u64() == 0
            || proof_expires_at_unix_ms == 0
            || intent_accepted_at.as_u64() == 0
            || barrier_enforced_at.as_u64() == 0
            || quarantined_at.as_u64() < intent_accepted_at.as_u64()
            || intent_accepted_at.as_u64() < barrier_enforced_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content quarantine has invalid protected coordinates".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            proof_token,
            verified_at,
            proof_expires_at_unix_ms,
            intent_accepted_at,
            barrier_id,
            barrier_enforced_at,
            drain_attestation_id,
            quarantined_at,
        })
    }

    pub(crate) fn matches_authorization(self, authorization: ContentReclaimAuthorization) -> bool {
        self.storage_domain_id == authorization.storage_domain_id()
            && self.content_id == authorization.content_id()
            && self.proof_token == authorization.proof_token()
            && self.verified_at == authorization.verified_at()
            && self.proof_expires_at_unix_ms == authorization.expires_at_unix_ms()
    }

    pub(crate) const fn into_public(self) -> ContentQuarantine {
        ContentQuarantine {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            proof_token: self.proof_token,
            verified_at: self.verified_at,
            proof_expires_at_unix_ms: self.proof_expires_at_unix_ms,
            intent_accepted_at: self.intent_accepted_at,
            barrier_id: self.barrier_id,
            barrier_enforced_at: self.barrier_enforced_at,
            drain_attestation_id: self.drain_attestation_id,
            quarantined_at: self.quarantined_at,
        }
    }
}

pub(crate) fn content_quarantine_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(11 + 16 + 33);
    key.extend_from_slice(b"quarantine:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentReclaimGraceRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) proof_token: ContentReclaimProofToken,
    pub(crate) quarantined_at: crate::ReadVersion,
    pub(crate) requested_duration_ms: u64,
    pub(crate) observed_at_unix_ms: u64,
    pub(crate) not_before_unix_ms: u64,
    pub(crate) started_at: crate::ReadVersion,
}

impl ContentReclaimGraceRecord {
    pub(crate) fn requested(
        quarantine: ContentQuarantineRecord,
        requested_duration_ms: u64,
        observed_at_unix_ms: u64,
        not_before_unix_ms: u64,
    ) -> Self {
        Self {
            storage_domain_id: quarantine.storage_domain_id,
            content_id: quarantine.content_id,
            proof_token: quarantine.proof_token,
            quarantined_at: quarantine.quarantined_at,
            requested_duration_ms,
            observed_at_unix_ms,
            not_before_unix_ms,
            started_at: crate::ReadVersion::from_u64(0),
        }
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 8;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_RECLAIM_GRACE_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.proof_token.to_bytes());
        bytes.extend_from_slice(&self.quarantined_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.requested_duration_ms.to_le_bytes());
        bytes.extend_from_slice(&self.observed_at_unix_ms.to_le_bytes());
        bytes.extend_from_slice(&self.not_before_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 8 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_RECLAIM_GRACE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content reclaim-grace header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content reclaim-grace storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content reclaim-grace identity")?;
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            57,
            "content reclaim-grace proof token",
        )?);
        let quarantined_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            106,
            "content reclaim-grace quarantine sequence",
        )?));
        let requested_duration_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 114, "content reclaim-grace duration")?);
        let observed_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            122,
            "content reclaim-grace clock observation",
        )?);
        let not_before_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            130,
            "content reclaim-grace not-before time",
        )?);
        let started_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            138,
            "content reclaim-grace commit sequence",
        )?));
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content reclaim-grace record differs from its protected key".to_owned(),
            });
        }
        if quarantined_at.as_u64() == 0
            || requested_duration_ms == 0
            || observed_at_unix_ms == 0
            || not_before_unix_ms
                != observed_at_unix_ms
                    .checked_add(requested_duration_ms)
                    .ok_or_else(|| Error::Corruption {
                        message: "content reclaim-grace deadline overflowed".to_owned(),
                    })?
            || started_at.as_u64() < quarantined_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content reclaim-grace has invalid protected coordinates".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            proof_token,
            quarantined_at,
            requested_duration_ms,
            observed_at_unix_ms,
            not_before_unix_ms,
            started_at,
        })
    }

    pub(crate) fn matches_quarantine(self, quarantine: ContentQuarantineRecord) -> bool {
        self.storage_domain_id == quarantine.storage_domain_id
            && self.content_id == quarantine.content_id
            && self.proof_token == quarantine.proof_token
            && self.quarantined_at == quarantine.quarantined_at
    }

    pub(crate) const fn into_public(self) -> ContentReclaimGrace {
        ContentReclaimGrace {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            proof_token: self.proof_token,
            quarantined_at: self.quarantined_at,
            requested_duration_ms: self.requested_duration_ms,
            observed_at_unix_ms: self.observed_at_unix_ms,
            not_before_unix_ms: self.not_before_unix_ms,
            started_at: self.started_at,
        }
    }
}

pub(crate) fn content_reclaim_grace_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16 + 33);
    key.extend_from_slice(b"grace:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentControlState {
    Active,
    ReclaimIntent {
        proof_token: ContentReclaimProofToken,
        verified_at: crate::ReadVersion,
        expires_at_unix_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentControlRecord {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    prior_activity_commit_seq: u64,
    state: ContentControlState,
    state_commit_seq: u64,
}

impl ContentControlRecord {
    pub(crate) const fn active(storage_domain_id: StorageDomainId, content_id: ContentId) -> Self {
        Self {
            storage_domain_id,
            content_id,
            prior_activity_commit_seq: 0,
            state: ContentControlState::Active,
            state_commit_seq: 0,
        }
    }

    pub(crate) fn reclaim_intent(self, authorization: ContentReclaimAuthorization) -> Self {
        Self {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            prior_activity_commit_seq: self.physical_activity_commit_seq(),
            state: ContentControlState::ReclaimIntent {
                proof_token: authorization.proof_token(),
                verified_at: authorization.verified_at(),
                expires_at_unix_ms: authorization.expires_at_unix_ms(),
            },
            state_commit_seq: 0,
        }
    }

    pub(crate) const fn physical_activity_commit_seq(self) -> u64 {
        match self.state {
            ContentControlState::Active => self.state_commit_seq,
            ContentControlState::ReclaimIntent { .. } => self.prior_activity_commit_seq,
        }
    }

    pub(crate) fn matches_authorization(self, authorization: ContentReclaimAuthorization) -> bool {
        self.storage_domain_id == authorization.storage_domain_id()
            && self.content_id == authorization.content_id()
            && matches!(
                self.state,
                ContentControlState::ReclaimIntent {
                    proof_token,
                    verified_at,
                    expires_at_unix_ms,
                } if proof_token.to_bytes() == authorization.proof_token().to_bytes()
                    && verified_at.as_u64() == authorization.verified_at().as_u64()
                    && expires_at_unix_ms == authorization.expires_at_unix_ms()
            )
    }

    pub(crate) fn matches_quarantine(self, quarantine: ContentQuarantineRecord) -> bool {
        self.storage_domain_id == quarantine.storage_domain_id
            && self.content_id == quarantine.content_id
            && self.accepted_at() == Some(quarantine.intent_accepted_at)
            && matches!(
                self.state,
                ContentControlState::ReclaimIntent {
                    proof_token,
                    verified_at,
                    expires_at_unix_ms,
                } if proof_token == quarantine.proof_token
                    && verified_at == quarantine.verified_at
                    && expires_at_unix_ms == quarantine.proof_expires_at_unix_ms
            )
    }

    pub(crate) const fn accepted_at(self) -> Option<crate::ReadVersion> {
        match self.state {
            ContentControlState::Active => None,
            ContentControlState::ReclaimIntent { .. } => {
                Some(crate::ReadVersion::from_u64(self.state_commit_seq))
            }
        }
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 33 + 8 + 1 + 49 + 8 + 8;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_CONTROL_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.prior_activity_commit_seq.to_be_bytes());
        match self.state {
            ContentControlState::Active => {
                bytes.push(CONTENT_CONTROL_ACTIVE);
                bytes.extend_from_slice(&[0_u8; CONTENT_RECLAIM_PROOF_TOKEN_BYTES]);
                bytes.extend_from_slice(&0_u64.to_be_bytes());
                bytes.extend_from_slice(&0_u64.to_le_bytes());
            }
            ContentControlState::ReclaimIntent {
                proof_token,
                verified_at,
                expires_at_unix_ms,
            } => {
                bytes.push(CONTENT_CONTROL_RECLAIM_INTENT);
                bytes.extend_from_slice(&proof_token.to_bytes());
                bytes.extend_from_slice(&verified_at.as_u64().to_be_bytes());
                bytes.extend_from_slice(&expires_at_unix_ms.to_le_bytes());
            }
        }
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 8 + 1 + 49 + 8 + 8 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_CONTROL_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content control record header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content control storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content control identity")?;
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content control record differs from its protected key".to_owned(),
            });
        }
        let prior_activity_commit_seq =
            u64::from_be_bytes(array_at::<8>(bytes, 57, "prior content activity")?);
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            66,
            "content reclaim proof token",
        )?);
        let verified_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            115,
            "content reclaim verified sequence",
        )?));
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 123, "content reclaim expiry")?);
        let state_commit_seq =
            u64::from_be_bytes(array_at::<8>(bytes, 131, "content control state sequence")?);
        let state = match bytes[65] {
            CONTENT_CONTROL_ACTIVE
                if prior_activity_commit_seq == 0
                    && proof_token.to_bytes() == [0_u8; 49]
                    && verified_at.as_u64() == 0
                    && expires_at_unix_ms == 0 =>
            {
                ContentControlState::Active
            }
            CONTENT_CONTROL_RECLAIM_INTENT
                if prior_activity_commit_seq > 0
                    && verified_at.as_u64() >= prior_activity_commit_seq
                    && expires_at_unix_ms > 0 =>
            {
                ContentControlState::ReclaimIntent {
                    proof_token,
                    verified_at,
                    expires_at_unix_ms,
                }
            }
            _ => {
                return Err(Error::Corruption {
                    message: "content control record has invalid lifecycle coordinates".to_owned(),
                });
            }
        };
        if state_commit_seq == 0 || state_commit_seq < prior_activity_commit_seq {
            return Err(Error::Corruption {
                message: "content control state sequence is invalid".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            prior_activity_commit_seq,
            state,
            state_commit_seq,
        })
    }
}

pub(crate) fn content_control_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 33);
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

pub(crate) fn content_prefix_range(prefix: Vec<u8>) -> Result<crate::KeyRange> {
    let mut end = prefix.clone();
    let position = end
        .iter()
        .rposition(|byte| *byte != u8::MAX)
        .ok_or_else(|| Error::Corruption {
            message: "protected content prefix has no finite successor".to_owned(),
        })?;
    end[position] = end[position].saturating_add(1);
    end.truncate(position + 1);
    Ok(crate::KeyRange::half_open(prefix, end))
}
