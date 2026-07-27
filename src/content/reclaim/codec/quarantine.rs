use super::{
    CONTENT_QUARANTINE_MAGIC, ContentAccessBarrierId, ContentAccessCoordinateRecord, ContentId,
    ContentQuarantine, ContentReaderDrainAttestationId, ContentReaderDrainAttestationRecord,
    ContentReclaimAuthorization, ContentReclaimProofToken, Error, Result, StorageDomainId,
    array_at, decode_content_id,
};

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
