use super::{
    CONTENT_TOKEN_INDEX_MAGIC, ContentAttachment, ContentAttachmentScope, ContentChangeId,
    ContentHashAlgorithm, ContentId, Digest, DurabilityMode, Error, OwnerScopeId, Result,
    SealedContent, Sha256, StorageDomainId, UploadId, UploadToken, content_control_key, fmt,
};

pub(crate) const CONTENT_TOKEN_BUCKET: &str = "\u{1}trine-content-token\u{1}";
const TOKEN_RECORD_MAGIC: &[u8; 8] = b"TRNTOKN1";
const TOKEN_RECORD_LEN: usize = 8 + 32 + 16 + 16 + 16 + 33 + 8 + 8 + 1 + 1 + 16;
const TOKEN_AVAILABLE: u8 = 0;
const TOKEN_CONSUMED: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadTokenStatus {
    Available,
    Consumed(ContentChangeId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadTokenRecord {
    token_hash: [u8; 32],
    attachment: ContentAttachment,
    status: UploadTokenStatus,
}

impl UploadTokenRecord {
    pub(crate) fn available(upload_id: UploadId, sealed: SealedContent) -> Self {
        Self {
            token_hash: upload_token_hash(sealed.upload_token),
            attachment: ContentAttachment::new(
                upload_id,
                sealed.attachment_scope,
                sealed.content_id,
                sealed.length,
                sealed.token_expires_at_unix_ms,
                sealed.durability,
            ),
            status: UploadTokenStatus::Available,
        }
    }

    pub(crate) const fn attachment(self) -> ContentAttachment {
        self.attachment
    }

    pub(crate) const fn is_available(self) -> bool {
        matches!(self.status, UploadTokenStatus::Available)
    }

    pub(crate) fn consume(
        self,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
        now_unix_ms: u64,
    ) -> Result<Self> {
        if self.attachment.scope != expected_scope {
            return Err(Error::UploadTokenScopeMismatch);
        }
        match self.status {
            UploadTokenStatus::Consumed(existing) if existing == change_id => Ok(self),
            UploadTokenStatus::Consumed(_) => Err(Error::UploadTokenAlreadyConsumed),
            UploadTokenStatus::Available
                if now_unix_ms >= self.attachment.token_expires_at_unix_ms =>
            {
                Err(Error::UploadTokenExpired {
                    expired_at_unix_ms: self.attachment.token_expires_at_unix_ms,
                })
            }
            UploadTokenStatus::Available => Ok(Self {
                status: UploadTokenStatus::Consumed(change_id),
                ..self
            }),
        }
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(TOKEN_RECORD_LEN);
        bytes.extend_from_slice(TOKEN_RECORD_MAGIC);
        bytes.extend_from_slice(&self.token_hash);
        bytes.extend_from_slice(&self.attachment.upload_id.to_bytes());
        bytes.extend_from_slice(&self.attachment.scope.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.attachment.scope.owner_scope_id.to_bytes());
        self.attachment.content_id.encode_into(&mut bytes);
        bytes.extend_from_slice(&self.attachment.length.to_le_bytes());
        bytes.extend_from_slice(&self.attachment.token_expires_at_unix_ms.to_le_bytes());
        bytes.push(encode_durability(self.attachment.durability));
        match self.status {
            UploadTokenStatus::Available => {
                bytes.push(TOKEN_AVAILABLE);
                bytes.extend_from_slice(&[0_u8; 16]);
            }
            UploadTokenStatus::Consumed(change_id) => {
                bytes.push(TOKEN_CONSUMED);
                bytes.extend_from_slice(&change_id.to_bytes());
            }
        }
        debug_assert_eq!(bytes.len(), TOKEN_RECORD_LEN);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], token: UploadToken) -> Result<Self> {
        if bytes.len() != TOKEN_RECORD_LEN || bytes.get(..8) != Some(TOKEN_RECORD_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid upload token record header or length".to_owned(),
            });
        }
        let expected_hash = upload_token_hash(token);
        let token_hash = array_at::<32>(bytes, 8, "upload token hash")?;
        if token_hash != expected_hash {
            return Err(Error::InvalidFormat {
                message: "upload token record hash mismatch".to_owned(),
            });
        }
        let upload_id = UploadId(array_at::<16>(bytes, 40, "token upload id")?);
        let storage_domain_id = StorageDomainId(array_at::<16>(bytes, 56, "token storage domain")?);
        let owner_scope_id = OwnerScopeId(array_at::<16>(bytes, 72, "token owner scope")?);
        let content_id = decode_content_id(bytes, 88, "token content identity")?;
        let length = u64::from_le_bytes(array_at::<8>(bytes, 121, "token content length")?);
        let token_expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 129, "upload token expiry")?);
        let durability = decode_durability(bytes[137])?;
        let status = match bytes[138] {
            TOKEN_AVAILABLE => UploadTokenStatus::Available,
            TOKEN_CONSUMED => UploadTokenStatus::Consumed(ContentChangeId(array_at::<16>(
                bytes,
                139,
                "token consuming change",
            )?)),
            tag => {
                return Err(Error::UnsupportedFormat {
                    message: format!("unsupported upload token state tag {tag}"),
                });
            }
        };
        Ok(Self {
            token_hash,
            attachment: ContentAttachment::new(
                upload_id,
                ContentAttachmentScope::new(storage_domain_id, owner_scope_id),
                content_id,
                length,
                token_expires_at_unix_ms,
                durability,
            ),
            status,
        })
    }
}

pub(crate) fn upload_token_key(token: UploadToken) -> Vec<u8> {
    upload_token_hash(token).to_vec()
}

pub(crate) fn upload_token_hash(token: UploadToken) -> [u8; 32] {
    Sha256::digest(token.to_bytes()).into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentTokenIndexRecord {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    token_hash: [u8; 32],
    expires_at_unix_ms: u64,
}

impl ContentTokenIndexRecord {
    pub(crate) fn for_token(sealed: SealedContent) -> Self {
        Self {
            storage_domain_id: sealed.storage_domain_id(),
            content_id: sealed.content_id(),
            token_hash: upload_token_hash(sealed.upload_token()),
            expires_at_unix_ms: sealed.token_expires_at_unix_ms(),
        }
    }

    pub(crate) const fn expires_at_unix_ms(self) -> u64 {
        self.expires_at_unix_ms
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 32 + 8;
        let mut bytes = Vec::with_capacity(RECORD_LEN);
        bytes.extend_from_slice(CONTENT_TOKEN_INDEX_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.token_hash);
        bytes.extend_from_slice(&self.expires_at_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        token_hash: [u8; 32],
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 32 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_TOKEN_INDEX_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content token-index record header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content token-index storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content token-index identity")?;
        let stored_hash = array_at::<32>(bytes, 57, "content token-index hash")?;
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 89, "content token-index expiry")?);
        if stored_domain != storage_domain_id
            || stored_content != content_id
            || stored_hash != token_hash
        {
            return Err(Error::Corruption {
                message: "content token-index record differs from its protected key".to_owned(),
            });
        }
        if expires_at_unix_ms == 0 {
            return Err(Error::Corruption {
                message: "content token-index expiry cannot be zero".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            token_hash,
            expires_at_unix_ms,
        })
    }
}

pub(crate) fn content_token_index_prefix(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    content_control_key(storage_domain_id, content_id)
}

pub(crate) fn content_token_index_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    token: UploadToken,
) -> Vec<u8> {
    let mut key = content_token_index_prefix(storage_domain_id, content_id);
    key.extend_from_slice(&upload_token_hash(token));
    key
}

pub(super) fn encode_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.extend_from_slice(&[0_u8; 9]),
    }
}

pub(super) fn decode_optional_u64(
    bytes: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<Option<u64>> {
    match *bytes.get(offset).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} presence flag is truncated"),
    })? {
        0 => Ok(None),
        1 => Ok(Some(u64::from_le_bytes(array_at::<8>(
            bytes,
            offset + 1,
            field,
        )?))),
        tag => Err(Error::InvalidFormat {
            message: format!("{field} has invalid presence flag {tag}"),
        }),
    }
}

pub(super) fn encode_optional_content_id(bytes: &mut Vec<u8>, value: Option<ContentId>) {
    match value {
        Some(value) => {
            bytes.push(1);
            value.encode_into(bytes);
        }
        None => bytes.extend_from_slice(&[0_u8; 34]),
    }
}

pub(super) fn decode_optional_content_id(
    bytes: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<Option<ContentId>> {
    match *bytes.get(offset).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} presence flag is truncated"),
    })? {
        0 => Ok(None),
        1 => decode_content_id(bytes, offset + 1, field).map(Some),
        tag => Err(Error::InvalidFormat {
            message: format!("{field} has invalid presence flag {tag}"),
        }),
    }
}

pub(crate) const fn encode_durability(durability: DurabilityMode) -> u8 {
    match durability {
        DurabilityMode::Buffered => 0,
        DurabilityMode::Flush => 1,
        DurabilityMode::SyncData => 2,
        DurabilityMode::SyncAll => 3,
        DurabilityMode::SyncAllStrict => 4,
    }
}

pub(crate) fn decode_durability(tag: u8) -> Result<DurabilityMode> {
    match tag {
        0 => Ok(DurabilityMode::Buffered),
        1 => Ok(DurabilityMode::Flush),
        2 => Ok(DurabilityMode::SyncData),
        3 => Ok(DurabilityMode::SyncAll),
        4 => Ok(DurabilityMode::SyncAllStrict),
        _ => Err(Error::UnsupportedFormat {
            message: format!("unsupported content durability tag {tag}"),
        }),
    }
}

pub(super) fn decode_content_id(
    bytes: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<ContentId> {
    let tag = *bytes.get(offset).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} algorithm tag is truncated"),
    })?;
    let algorithm = ContentHashAlgorithm::from_tag(tag)?;
    let digest = array_at::<32>(bytes, offset + 1, field)?;
    Ok(ContentId { algorithm, digest })
}

pub(super) fn array_at<const N: usize>(
    bytes: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} offset overflow"),
    })?;
    bytes
        .get(offset..end)
        .ok_or_else(|| Error::InvalidFormat {
            message: format!("{field} is truncated"),
        })?
        .try_into()
        .map_err(|_| Error::InvalidFormat {
            message: format!("{field} has invalid length"),
        })
}

pub(super) fn digest_string(digest: [u8; 32]) -> String {
    let mut value = String::with_capacity(7 + digest.len() * 2);
    value.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

pub(super) fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
