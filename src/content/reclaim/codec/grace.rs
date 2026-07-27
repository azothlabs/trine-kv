use super::{
    CONTENT_RECLAIM_GRACE_MAGIC, ContentId, ContentQuarantineRecord, ContentReclaimGrace,
    ContentReclaimProofToken, Error, Result, StorageDomainId, array_at, decode_content_id,
};

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
