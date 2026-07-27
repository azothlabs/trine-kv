use super::{
    Arc, ContentAttachmentScope, ContentId, ContentUploadInfo, ContentUploadOptions,
    ContentUploadState, DurabilityMode, Duration, Error, OwnerScopeId, Result, SealedContent,
    StorageDomainId, UPLOAD_STATE_ABORTING, UPLOAD_STATE_LEN, UPLOAD_STATE_MAGIC,
    UPLOAD_STATE_OPEN, UPLOAD_STATE_SEALED, UPLOAD_STATE_SEALING, UPLOAD_STATE_UPDATED_AT_OFFSET,
    UploadId, UploadIdRetirement, UploadToken, array_at, current_epoch_millis, decode_content_id,
    decode_durability, decode_optional_content_id, decode_optional_u64, encode_durability,
    encode_optional_content_id, encode_optional_u64,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadSessionStatus {
    Open,
    Sealing(SealedContent),
    Sealed(SealedContent),
    Aborting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadSessionState {
    upload_id: UploadId,
    revision: u64,
    options: ContentUploadOptions,
    length: u64,
    complete_chunks: u64,
    partial_len: u32,
    upload_token: UploadToken,
    status: UploadSessionStatus,
    updated_at_unix_ms: u64,
}

/// Storage action required to publish one validated upload-session revision.
///
/// The state object decides revision legality; storage adapters only translate
/// this plan into create-only or compare-and-swap operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadStatePublish {
    Create,
    Replace { previous_revision: u64 },
    AlreadyApplied,
}

impl UploadSessionState {
    pub(crate) fn initial(
        upload_id: UploadId,
        options: ContentUploadOptions,
        upload_token: UploadToken,
    ) -> Result<Self> {
        Self::open(upload_id, 0, options, 0, 0, 0, upload_token)
    }

    pub(crate) fn open(
        upload_id: UploadId,
        revision: u64,
        options: ContentUploadOptions,
        length: u64,
        complete_chunks: u64,
        partial_len: u32,
        upload_token: UploadToken,
    ) -> Result<Self> {
        let state = Self {
            upload_id,
            revision,
            options,
            length,
            complete_chunks,
            partial_len,
            upload_token,
            status: UploadSessionStatus::Open,
            updated_at_unix_ms: current_epoch_millis()?,
        };
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn into_sealing(
        self,
        content_id: ContentId,
        token_expires_at_unix_ms: u64,
        durability: DurabilityMode,
    ) -> Result<Self> {
        let sealed = SealedContent {
            attachment_scope: self.options.attachment_scope,
            content_id,
            length: self.length,
            upload_token: self.upload_token,
            token_expires_at_unix_ms,
            durability,
        };
        Ok(Self {
            revision: self
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?,
            status: UploadSessionStatus::Sealing(sealed),
            ..self
        })
    }

    pub(crate) fn into_sealed(self) -> Result<Self> {
        let UploadSessionStatus::Sealing(sealed) = self.status else {
            return Err(Error::InvalidFormat {
                message: "content upload can become sealed only from sealing state".to_owned(),
            });
        };
        Ok(Self {
            revision: self
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?,
            status: UploadSessionStatus::Sealed(sealed),
            ..self
        })
    }

    pub(crate) fn into_aborting(self) -> Result<Self> {
        if self.status != UploadSessionStatus::Open {
            return Err(Error::InvalidFormat {
                message: "only an open content upload can enter aborting state".to_owned(),
            });
        }
        Ok(Self {
            revision: self
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?,
            status: UploadSessionStatus::Aborting,
            ..self
        })
    }

    pub(crate) const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    pub(crate) const fn revision(self) -> u64 {
        self.revision
    }

    pub(crate) const fn status(self) -> UploadSessionStatus {
        self.status
    }

    pub(crate) const fn options(self) -> ContentUploadOptions {
        self.options
    }

    pub(crate) const fn length(self) -> u64 {
        self.length
    }

    pub(crate) const fn complete_chunks(self) -> u64 {
        self.complete_chunks
    }

    pub(crate) const fn partial_len(self) -> u32 {
        self.partial_len
    }

    pub(crate) const fn upload_token(self) -> UploadToken {
        self.upload_token
    }

    pub(crate) const fn updated_at_unix_ms(self) -> u64 {
        self.updated_at_unix_ms
    }

    pub(crate) const fn with_updated_at_unix_ms(self, updated_at_unix_ms: u64) -> Self {
        Self {
            updated_at_unix_ms,
            ..self
        }
    }

    pub(crate) fn logically_eq_ignoring_updated_at(&self, other: &Self) -> bool {
        self.upload_id == other.upload_id
            && self.revision == other.revision
            && self.options == other.options
            && self.length == other.length
            && self.complete_chunks == other.complete_chunks
            && self.partial_len == other.partial_len
            && self.upload_token == other.upload_token
            && self.status == other.status
    }

    pub(crate) fn plan_publish_against(
        &self,
        current: Option<&Self>,
    ) -> Result<UploadStatePublish> {
        let Some(current) = current else {
            return if self.revision == 0 {
                Ok(UploadStatePublish::Create)
            } else {
                Err(Error::ContentUploadConflict {
                    upload_id: self.upload_id.to_string(),
                    expected_revision: self.revision,
                    actual_revision: 0,
                })
            };
        };
        if current.logically_eq_ignoring_updated_at(self) {
            return Ok(UploadStatePublish::AlreadyApplied);
        }
        let next_revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::Corruption {
                message: "content upload revision overflow".to_owned(),
            })?;
        if self.revision != next_revision {
            return Err(Error::ContentUploadConflict {
                upload_id: self.upload_id.to_string(),
                expected_revision: self.revision,
                actual_revision: current.revision,
            });
        }
        Ok(UploadStatePublish::Replace {
            previous_revision: current.revision,
        })
    }

    pub(crate) fn require_retirement(self, retirement: UploadIdRetirement) -> Result<()> {
        let allowed = matches!(
            (retirement, self.status),
            (UploadIdRetirement::Aborted, UploadSessionStatus::Aborting)
                | (UploadIdRetirement::Sealed, UploadSessionStatus::Sealed(_))
        );
        if allowed {
            Ok(())
        } else {
            Err(Error::ContentUploadConflict {
                upload_id: self.upload_id.to_string(),
                expected_revision: self.revision,
                actual_revision: self.revision,
            })
        }
    }

    pub(crate) const fn maintenance_info(self) -> ContentUploadInfo {
        let state = match self.status {
            UploadSessionStatus::Open => ContentUploadState::Open,
            UploadSessionStatus::Sealing(_) => ContentUploadState::Sealing,
            UploadSessionStatus::Sealed(_) => ContentUploadState::Sealed,
            UploadSessionStatus::Aborting => ContentUploadState::Aborting,
        };
        ContentUploadInfo {
            upload_id: self.upload_id,
            state,
            updated_at_unix_ms: self.updated_at_unix_ms,
            length: self.length,
        }
    }

    pub(crate) const fn chunk_count(self) -> u64 {
        self.complete_chunks + if self.partial_len == 0 { 0 } else { 1 }
    }

    pub(crate) fn require_open_revision(self, expected_revision: u64) -> Result<()> {
        match self.status {
            UploadSessionStatus::Sealing(_) | UploadSessionStatus::Sealed(_) => {
                Err(Error::ContentUploadSealed {
                    upload_id: self.upload_id.to_string(),
                })
            }
            UploadSessionStatus::Aborting => Err(Error::ContentUploadNotFound {
                upload_id: self.upload_id.to_string(),
            }),
            UploadSessionStatus::Open if self.revision == expected_revision => Ok(()),
            UploadSessionStatus::Open => Err(Error::ContentUploadConflict {
                upload_id: self.upload_id.to_string(),
                expected_revision,
                actual_revision: self.revision,
            }),
        }
    }

    pub(crate) fn encode(self) -> Result<Arc<[u8]>> {
        let mut bytes = Vec::with_capacity(UPLOAD_STATE_LEN);
        bytes.extend_from_slice(UPLOAD_STATE_MAGIC);
        bytes.push(match self.status {
            UploadSessionStatus::Open => UPLOAD_STATE_OPEN,
            UploadSessionStatus::Sealing(_) => UPLOAD_STATE_SEALING,
            UploadSessionStatus::Sealed(_) => UPLOAD_STATE_SEALED,
            UploadSessionStatus::Aborting => UPLOAD_STATE_ABORTING,
        });
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.revision.to_le_bytes());
        let chunk_bytes = u32::try_from(self.options.chunk_bytes)
            .map_err(|_| Error::invalid_options("content chunk size exceeds u32"))?;
        bytes.extend_from_slice(&chunk_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.complete_chunks.to_le_bytes());
        bytes.extend_from_slice(&self.partial_len.to_le_bytes());
        encode_optional_u64(&mut bytes, self.options.expected_length);
        encode_optional_content_id(&mut bytes, self.options.expected_content_id);
        bytes.extend_from_slice(&self.options.attachment_scope.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.options.attachment_scope.owner_scope_id.to_bytes());
        bytes.extend_from_slice(&self.upload_token.secret());
        bytes.extend_from_slice(&self.options.token_ttl_ms()?.to_le_bytes());
        match self.status {
            UploadSessionStatus::Open | UploadSessionStatus::Aborting => {
                bytes.extend_from_slice(&0_u64.to_le_bytes());
                bytes.push(0);
                bytes.extend_from_slice(&[0_u8; 33]);
            }
            UploadSessionStatus::Sealing(sealed) | UploadSessionStatus::Sealed(sealed) => {
                bytes.extend_from_slice(&sealed.token_expires_at_unix_ms.to_le_bytes());
                bytes.push(encode_durability(sealed.durability));
                sealed.content_id.encode_into(&mut bytes);
            }
        }
        bytes.extend_from_slice(&self.updated_at_unix_ms.to_le_bytes());
        debug_assert_eq!(bytes.len(), UPLOAD_STATE_LEN);
        Ok(Arc::from(bytes))
    }

    pub(crate) fn decode(bytes: &[u8], expected_upload: UploadId) -> Result<Self> {
        if bytes.len() != UPLOAD_STATE_LEN || bytes.get(..8) != Some(UPLOAD_STATE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content upload state header or length".to_owned(),
            });
        }
        let status_tag = bytes[8];
        let upload_id = UploadId(array_at::<16>(bytes, 9, "content upload id")?);
        if upload_id != expected_upload {
            return Err(Error::InvalidFormat {
                message: format!("content upload identity mismatch for {expected_upload}"),
            });
        }
        let revision = u64::from_le_bytes(array_at::<8>(bytes, 25, "content upload revision")?);
        let chunk_bytes = usize::try_from(u32::from_le_bytes(array_at::<4>(
            bytes,
            33,
            "content upload chunk size",
        )?))
        .map_err(|_| Error::InvalidFormat {
            message: "content upload chunk size exceeds usize".to_owned(),
        })?;
        let length = u64::from_le_bytes(array_at::<8>(bytes, 37, "content upload length")?);
        let complete_chunks = u64::from_le_bytes(array_at::<8>(
            bytes,
            45,
            "content upload complete chunk count",
        )?);
        let partial_len =
            u32::from_le_bytes(array_at::<4>(bytes, 53, "content upload partial length")?);
        let expected_length = decode_optional_u64(bytes, 57, "content expected length")?;
        let expected_content_id =
            decode_optional_content_id(bytes, 66, "content expected identity")?;
        let storage_domain_id =
            StorageDomainId(array_at::<16>(bytes, 100, "content upload storage domain")?);
        let owner_scope_id =
            OwnerScopeId(array_at::<16>(bytes, 116, "content upload owner scope")?);
        let upload_token = UploadToken(array_at::<32>(bytes, 132, "content upload token")?);
        let token_ttl_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 164, "content upload token lifetime")?);
        let token_expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 172, "content upload token expiry")?);
        let durability = decode_durability(bytes[180])?;
        let options = ContentUploadOptions {
            attachment_scope: ContentAttachmentScope::new(storage_domain_id, owner_scope_id),
            token_ttl: Duration::from_millis(token_ttl_ms),
            chunk_bytes,
            expected_length,
            expected_content_id,
        }
        .validate()?;
        let status = decode_upload_session_status(
            bytes,
            status_tag,
            options,
            length,
            upload_token,
            token_expires_at_unix_ms,
            durability,
        )?;
        let updated_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            UPLOAD_STATE_UPDATED_AT_OFFSET,
            "content upload update time",
        )?);
        let state = Self {
            upload_id,
            revision,
            options,
            length,
            complete_chunks,
            partial_len,
            upload_token,
            status,
            updated_at_unix_ms,
        };
        state.validate()?;
        Ok(state)
    }

    pub(super) fn validate(self) -> Result<()> {
        if self.updated_at_unix_ms == 0 {
            return Err(Error::InvalidFormat {
                message: "content upload update time cannot be zero".to_owned(),
            });
        }
        let chunk_bytes =
            u64::try_from(self.options.chunk_bytes).map_err(|_| Error::InvalidFormat {
                message: "content upload chunk size exceeds u64".to_owned(),
            })?;
        if u64::from(self.partial_len) >= chunk_bytes && self.partial_len != 0 {
            return Err(Error::InvalidFormat {
                message: format!(
                    "content upload partial length {} is not below chunk size {chunk_bytes}",
                    self.partial_len
                ),
            });
        }
        let complete_bytes = self
            .complete_chunks
            .checked_mul(chunk_bytes)
            .ok_or_else(|| Error::InvalidFormat {
                message: "content upload complete length overflow".to_owned(),
            })?;
        let derived_length = complete_bytes
            .checked_add(u64::from(self.partial_len))
            .ok_or_else(|| Error::InvalidFormat {
                message: "content upload length overflow".to_owned(),
            })?;
        if derived_length != self.length {
            return Err(Error::InvalidFormat {
                message: format!(
                    "content upload length {} does not match durable chunks {derived_length}",
                    self.length
                ),
            });
        }
        match self.status {
            UploadSessionStatus::Open | UploadSessionStatus::Aborting => {}
            UploadSessionStatus::Sealing(sealed) | UploadSessionStatus::Sealed(sealed) => {
                if sealed.length != self.length
                    || sealed.attachment_scope != self.options.attachment_scope
                    || sealed.upload_token != self.upload_token
                    || sealed.token_expires_at_unix_ms == 0
                {
                    return Err(Error::InvalidFormat {
                        message: "sealed content claims differ from upload session".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }
}

fn decode_upload_session_status(
    bytes: &[u8],
    status_tag: u8,
    options: ContentUploadOptions,
    length: u64,
    upload_token: UploadToken,
    token_expires_at_unix_ms: u64,
    durability: DurabilityMode,
) -> Result<UploadSessionStatus> {
    match status_tag {
        UPLOAD_STATE_OPEN => Ok(UploadSessionStatus::Open),
        UPLOAD_STATE_ABORTING => Ok(UploadSessionStatus::Aborting),
        UPLOAD_STATE_SEALING | UPLOAD_STATE_SEALED => {
            let sealed = SealedContent {
                attachment_scope: options.attachment_scope,
                content_id: decode_content_id(bytes, 181, "sealed content identity")?,
                length,
                upload_token,
                token_expires_at_unix_ms,
                durability,
            };
            if status_tag == UPLOAD_STATE_SEALING {
                Ok(UploadSessionStatus::Sealing(sealed))
            } else {
                Ok(UploadSessionStatus::Sealed(sealed))
            }
        }
        _ => Err(Error::UnsupportedFormat {
            message: format!("unsupported content upload state tag {status_tag}"),
        }),
    }
}

#[cfg(test)]
mod transition_tests {
    use std::time::Duration;

    use super::{UploadIdRetirement, UploadSessionState, UploadSessionStatus, UploadStatePublish};
    use crate::{
        ContentAttachmentScope, ContentUploadOptions, Error, OwnerScopeId, StorageDomainId,
        UploadId, UploadToken,
    };

    fn initial_state() -> UploadSessionState {
        let options = ContentUploadOptions::new(
            ContentAttachmentScope::new(
                StorageDomainId::from_bytes([1; 16]),
                OwnerScopeId::from_bytes([2; 16]),
            ),
            Duration::from_mins(1),
        );
        let mut token = [4; 33];
        token[0] = 1;
        let state = UploadSessionState {
            upload_id: UploadId::from_bytes([3; 16]),
            revision: 0,
            options,
            length: 0,
            complete_chunks: 0,
            partial_len: 0,
            upload_token: UploadToken::from_bytes(token).expect("test token version is valid"),
            status: UploadSessionStatus::Open,
            updated_at_unix_ms: 1,
        };
        state.validate().expect("initial upload state is valid");
        state
    }

    #[test]
    fn upload_publish_transition_plans_create_retry_and_successor() {
        let initial = initial_state();
        assert_eq!(
            initial
                .plan_publish_against(None)
                .expect("revision zero creates"),
            UploadStatePublish::Create
        );
        assert_eq!(
            initial
                .plan_publish_against(Some(&initial.with_updated_at_unix_ms(7)))
                .expect("logical retry is idempotent"),
            UploadStatePublish::AlreadyApplied
        );

        let aborting = initial.into_aborting().expect("open may abort");
        assert_eq!(
            aborting
                .plan_publish_against(Some(&initial))
                .expect("one-step successor replaces"),
            UploadStatePublish::Replace {
                previous_revision: 0
            }
        );
    }

    #[test]
    fn upload_publish_transition_rejects_gap_and_invalid_retirement() {
        let initial = initial_state();
        let gap = UploadSessionState {
            revision: 2,
            updated_at_unix_ms: 2,
            ..initial
        };
        assert!(matches!(
            gap.plan_publish_against(Some(&initial)),
            Err(Error::ContentUploadConflict { .. })
        ));
        assert!(matches!(
            initial.require_retirement(UploadIdRetirement::Aborted),
            Err(Error::ContentUploadConflict { .. })
        ));
        initial
            .into_aborting()
            .expect("open may abort")
            .require_retirement(UploadIdRetirement::Aborted)
            .expect("aborting state may retire as aborted");
    }
}
