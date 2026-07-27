use super::{
    CONTENT_CONTROL_ACTIVE, CONTENT_CONTROL_MAGIC, CONTENT_CONTROL_RECLAIM_INTENT,
    CONTENT_RECLAIM_PROOF_TOKEN_BYTES, ContentId, ContentQuarantineRecord,
    ContentReclaimAuthorization, ContentReclaimProofToken, Error, Result, StorageDomainId,
    array_at, decode_content_id,
};

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
