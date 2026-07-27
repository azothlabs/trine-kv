use super::{
    CONTENT_READER_DRAIN_ATTESTATION_MAGIC, ContentAccessBarrier, ContentAccessBarrierId,
    ContentReaderDrainAttestation, ContentReaderDrainAttestationId,
    ContentReaderDrainAttestationOptions, ContentReaderDrainCoordinatorId,
    ContentReaderDrainEvidenceDigest, ContentReaderDrainKind, Error, Result, StorageDomainId,
    array_at,
};

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
