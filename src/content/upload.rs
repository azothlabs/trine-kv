use super::{
    Arc, CHUNK_HEADER_LEN, CHUNK_MAGIC, ContentAttachmentScope, ContentHashAlgorithm, ContentId,
    ContentUploadOptions, DESCRIPTOR_LEN, DESCRIPTOR_MAGIC, Db, Digest, DurabilityMode, Duration,
    Error, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES, OwnerScopeId, Result, SealedContent, Sha256,
    StorageDomainId, UPLOAD_ID_TOMBSTONE_LEN, UPLOAD_ID_TOMBSTONE_MAGIC, UPLOAD_STATE_ABORTING,
    UPLOAD_STATE_LEN, UPLOAD_STATE_MAGIC, UPLOAD_STATE_OPEN, UPLOAD_STATE_SEALED,
    UPLOAD_STATE_SEALING, UPLOAD_STATE_UPDATED_AT_OFFSET, UploadId, UploadToken, array_at,
    current_epoch_millis, decode_content_id, decode_durability, decode_optional_content_id,
    decode_optional_u64, digest_string, encode_durability, encode_optional_content_id,
    encode_optional_u64, fmt, mem,
};

/// Result of resuming durable state for an [`UploadId`].
///
/// A successful seal is remembered by upload identity. Callers therefore get
/// either a writable open session or the exact prior seal result; they never
/// reopen a sealed upload as writable state.
#[derive(Debug)]
pub enum ContentUploadResume {
    /// The upload remains open and may accept bytes at [`ContentUpload::len`].
    Open(ContentUpload),
    /// The upload was already sealed; this is the idempotent prior result.
    Sealed(SealedContent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadIdRetirement {
    Aborted,
    Sealed,
}

pub(crate) fn encode_upload_id_tombstone(
    upload_id: UploadId,
    retirement: UploadIdRetirement,
) -> Arc<[u8]> {
    let mut bytes = Vec::with_capacity(UPLOAD_ID_TOMBSTONE_LEN);
    bytes.extend_from_slice(UPLOAD_ID_TOMBSTONE_MAGIC);
    bytes.push(match retirement {
        UploadIdRetirement::Aborted => 0,
        UploadIdRetirement::Sealed => 1,
    });
    bytes.extend_from_slice(&upload_id.to_bytes());
    debug_assert_eq!(bytes.len(), UPLOAD_ID_TOMBSTONE_LEN);
    Arc::from(bytes)
}

pub(crate) fn decode_upload_id_tombstone(
    bytes: &[u8],
    expected: UploadId,
) -> Result<Option<UploadIdRetirement>> {
    if bytes.get(..8) != Some(UPLOAD_ID_TOMBSTONE_MAGIC) {
        return Ok(None);
    }
    if bytes.len() != UPLOAD_ID_TOMBSTONE_LEN {
        return Err(Error::InvalidFormat {
            message: "invalid retired upload-id marker length".to_owned(),
        });
    }
    let retirement = match bytes[8] {
        0 => UploadIdRetirement::Aborted,
        1 => UploadIdRetirement::Sealed,
        _ => {
            return Err(Error::InvalidFormat {
                message: "invalid retired upload-id marker state".to_owned(),
            });
        }
    };
    let encoded = UploadId(array_at::<16>(bytes, 9, "retired upload identity")?);
    if encoded != expected {
        return Err(Error::InvalidFormat {
            message: "retired upload-id marker identity mismatch".to_owned(),
        });
    }
    Ok(Some(retirement))
}

/// Durable lifecycle visible to content-upload maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentUploadState {
    /// The upload can still accept bytes.
    Open,
    /// Descriptor publication started and must be resumed rather than discarded.
    Sealing,
    /// The upload completed; its state remains only for idempotent retries.
    Sealed,
    /// Abort cleanup started and can be resumed.
    Aborting,
}

/// One durable upload discovered by the maintenance index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentUploadInfo {
    upload_id: UploadId,
    state: ContentUploadState,
    updated_at_unix_ms: u64,
    length: u64,
}

impl ContentUploadInfo {
    /// Returns the stable upload identity used by resume, abort, and seal APIs.
    #[must_use]
    pub const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    /// Returns the durable lifecycle observed while listing.
    #[must_use]
    pub const fn state(self) -> ContentUploadState {
        self.state
    }

    /// Returns the last durable update time in Unix milliseconds.
    #[must_use]
    pub const fn updated_at_unix_ms(self) -> u64 {
        self.updated_at_unix_ms
    }

    /// Returns the durable original-byte length.
    #[must_use]
    pub const fn len(self) -> u64 {
        self.length
    }

    /// Returns whether the durable original-byte length is zero.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }
}

/// Counts returned after one idempotent upload-maintenance pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentUploadMaintenanceReport {
    pub(crate) scanned: u64,
    pub(crate) aborted: u64,
    pub(crate) pruned_sealed: u64,
}

impl ContentUploadMaintenanceReport {
    /// Returns the number of durable upload states inspected.
    #[must_use]
    pub const fn scanned(self) -> u64 {
        self.scanned
    }

    /// Returns the number of inactive open or aborting uploads fully removed.
    #[must_use]
    pub const fn aborted(self) -> u64 {
        self.aborted
    }

    /// Returns the number of sealed idempotency states removed.
    #[must_use]
    pub const fn pruned_sealed(self) -> u64 {
        self.pruned_sealed
    }
}

impl ContentUploadResume {
    /// Returns the open upload, or `None` when it was already sealed.
    #[must_use]
    pub fn into_open(self) -> Option<ContentUpload> {
        match self {
            Self::Open(upload) => Some(upload),
            Self::Sealed(_) => None,
        }
    }

    /// Returns the prior seal result, or `None` while the upload remains open.
    #[must_use]
    pub const fn sealed(&self) -> Option<SealedContent> {
        match self {
            Self::Open(_) => None,
            Self::Sealed(sealed) => Some(*sealed),
        }
    }
}

/// In-progress sequential upload with memory bounded by its configured chunk.
///
/// Calls to [`write`](Self::write) may be any size and are split into fixed
/// chunks. `seal` consumes the upload and publishes the fixed-size descriptor
/// only after all chunks have been stored and the complete identity verified.
/// Dropping an unsealed upload never publishes content. Its durable
/// [`UploadId`] can be resumed or explicitly aborted later; cleanup of uploads
/// that are never resumed or aborted is a maintenance concern.
pub struct ContentUpload {
    db: Db,
    upload_id: UploadId,
    options: ContentUploadOptions,
    buffer: Vec<u8>,
    length: u64,
    complete_chunks: u64,
    revision: u64,
    failed: bool,
}

impl fmt::Debug for ContentUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentUpload")
            .field("upload_id", &self.upload_id)
            .field("chunk_bytes", &self.options.chunk_bytes)
            .field("buffered_bytes", &self.buffer.len())
            .field("length", &self.length)
            .field("complete_chunks", &self.complete_chunks)
            .field("revision", &self.revision)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl ContentUpload {
    pub(crate) fn new(
        db: Db,
        upload_id: UploadId,
        options: ContentUploadOptions,
        buffer: Vec<u8>,
        length: u64,
        complete_chunks: u64,
        revision: u64,
    ) -> Self {
        Self {
            db,
            upload_id,
            options,
            buffer,
            length,
            complete_chunks,
            revision,
            failed: false,
        }
    }

    /// Returns this temporary upload identity.
    #[must_use]
    pub const fn upload_id(&self) -> UploadId {
        self.upload_id
    }

    /// Returns the number of original bytes accepted so far.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.length
    }

    /// Returns whether no original bytes have been accepted.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns bytes currently retained before the next chunk write.
    ///
    /// This value never exceeds [`ContentUploadOptions::chunk_bytes`]. It is
    /// exposed so callers and benchmarks can verify the memory boundary.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Accepts more original bytes and stores each completed chunk.
    ///
    /// Empty writes are no-ops. If a storage write fails, this in-memory writer
    /// becomes unusable because the failure may have happened between the chunk
    /// and session writes. Resume or abort its [`UploadId`] through [`Db`] to
    /// recover from the last durable session revision.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] or [`Error::ReadOnly`] when the database cannot
    /// write, [`Error::InvalidOptions`] on length overflow, or a backend storage
    /// error while persisting a completed chunk.
    pub async fn write(&mut self, mut bytes: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let _activity = db.begin_activity()?;
        self.ensure_active()?;
        if bytes.is_empty() {
            return Ok(());
        }

        let _upload = db.lock_content_upload(self.upload_id).await;
        let durable = db.require_upload_state(self.upload_id).await?;
        durable.require_open_revision(self.revision)?;

        let incoming = u64::try_from(bytes.len())
            .map_err(|_| Error::invalid_options("content write length exceeds u64"))?;
        let next_length = self
            .length
            .checked_add(incoming)
            .ok_or_else(|| Error::invalid_options("content length overflow"))?;
        if let Some(expected) = self.options.expected_length()
            && next_length > expected
        {
            return Err(Error::ContentLengthMismatch {
                expected,
                actual: next_length,
            });
        }
        let desired_reservation = self.options.expected_length().unwrap_or(next_length);
        db.reserve_content_upload_bytes(&durable, desired_reservation)
            .await?;
        self.length = next_length;
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?;

        while !bytes.is_empty() {
            let available = self.options.chunk_bytes() - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.options.chunk_bytes()
                && let Err(error) = self.flush_full_chunk().await
            {
                self.failed = true;
                return Err(error);
            }
        }

        if !self.buffer.is_empty() {
            let frame = encode_chunk(self.upload_id, self.complete_chunks, &self.buffer)?;
            if let Err(error) = self
                .db
                .write_content_partial_chunk(
                    self.upload_id,
                    self.complete_chunks,
                    next_revision,
                    frame,
                )
                .await
            {
                self.failed = true;
                return Err(error);
            }
        }
        let state = UploadSessionState::open(
            self.upload_id,
            next_revision,
            self.options,
            self.length,
            self.complete_chunks,
            u32::try_from(self.buffer.len())
                .map_err(|_| Error::invalid_options("content partial chunk exceeds u32"))?,
            durable.upload_token(),
        )?;
        if let Err(error) = self.db.write_upload_state(&state).await {
            self.failed = true;
            return Err(error);
        }
        self.revision = next_revision;
        Ok(())
    }

    /// Seals this upload and publishes its immutable descriptor.
    ///
    /// Seal is the visibility boundary: chunk objects written before this call
    /// are not openable by `ContentId`. The returned length and identity describe
    /// original bytes, not framed storage bytes. Existing identical content is
    /// reused and the redundant upload chunks are removed.
    ///
    /// # Errors
    ///
    /// Returns a typed length or digest mismatch and aborts the session when an
    /// expectation fails,
    /// [`Error::InvalidOptions`] when the upload previously failed, or a storage
    /// error if the final chunk or descriptor cannot be made durable. A failed
    /// descriptor write does not publish the `ContentObject`. If descriptor
    /// publication succeeds but the sealed session write fails, retry
    /// [`Db::seal_content_upload`] with the same [`UploadId`].
    pub async fn seal(self) -> Result<SealedContent> {
        self.ensure_active()?;
        self.db
            .seal_content_upload_at(self.upload_id, Some(self.revision))
            .await
    }

    /// Aborts the upload and makes every staging chunk unreachable.
    ///
    /// No descriptor is published. The durable session enters `aborting`
    /// before chunk cleanup and ends as a permanent retired-ID marker, so a
    /// crash cannot reopen it as writable state.
    ///
    /// # Errors
    ///
    /// Returns a conflict if another writer advanced the session, or the
    /// backend error that prevented deletion of the durable session.
    pub async fn abort(self) -> Result<()> {
        self.ensure_active()?;
        self.db
            .abort_content_upload_at(self.upload_id, Some(self.revision))
            .await
    }

    async fn flush_full_chunk(&mut self) -> Result<()> {
        let payload = mem::replace(
            &mut self.buffer,
            Vec::with_capacity(self.options.chunk_bytes()),
        );
        let frame = encode_chunk(self.upload_id, self.complete_chunks, &payload)?;
        self.db
            .write_content_chunk(self.upload_id, self.complete_chunks, frame)
            .await?;
        self.complete_chunks = self
            .complete_chunks
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content chunk count overflow"))?;
        Ok(())
    }

    fn ensure_active(&self) -> Result<()> {
        if self.failed {
            Err(Error::invalid_options(
                "content upload previously failed and cannot continue",
            ))
        } else {
            Ok(())
        }
    }
}

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

    fn validate(self) -> Result<()> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentDescriptor {
    pub(super) storage_domain_id: StorageDomainId,
    pub(super) content_id: ContentId,
    pub(super) upload_id: UploadId,
    pub(super) length: u64,
    pub(super) chunk_bytes: u32,
    pub(super) chunk_count: u64,
}

impl ContentDescriptor {
    pub(crate) fn new(
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        upload_id: UploadId,
        length: u64,
        chunk_bytes: usize,
        chunk_count: u64,
    ) -> Result<Self> {
        let chunk_bytes = u32::try_from(chunk_bytes)
            .map_err(|_| Error::invalid_options("content chunk size exceeds u32"))?;
        Ok(Self {
            storage_domain_id,
            content_id,
            upload_id,
            length,
            chunk_bytes,
            chunk_count,
        })
    }

    pub(crate) const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    pub(crate) const fn content_id(self) -> ContentId {
        self.content_id
    }

    pub(crate) const fn length(self) -> u64 {
        self.length
    }

    pub(crate) const fn chunk_bytes(self) -> u32 {
        self.chunk_bytes
    }

    pub(crate) const fn chunk_count(self) -> u64 {
        self.chunk_count
    }

    pub(crate) fn encode(self) -> Arc<[u8]> {
        let mut bytes = Vec::with_capacity(DESCRIPTOR_LEN);
        bytes.extend_from_slice(DESCRIPTOR_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        self.content_id.encode_into(&mut bytes);
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        Arc::from(bytes)
    }

    pub(crate) fn decode(
        bytes: &[u8],
        expected_domain: StorageDomainId,
        expected_content: ContentId,
    ) -> Result<Self> {
        if bytes.len() != DESCRIPTOR_LEN || bytes.get(..8) != Some(DESCRIPTOR_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content descriptor header or length".to_owned(),
            });
        }
        let storage_domain_id = StorageDomainId(array_at::<16>(
            bytes,
            8,
            "content descriptor storage domain",
        )?);
        if storage_domain_id != expected_domain {
            return Err(Error::InvalidFormat {
                message: "content descriptor storage domain mismatch".to_owned(),
            });
        }
        let algorithm = ContentHashAlgorithm::from_tag(bytes[24])?;
        let digest = array_at::<32>(bytes, 25, "content descriptor digest")?;
        let content_id = ContentId { algorithm, digest };
        if content_id != expected_content {
            return Err(Error::ContentDigestMismatch {
                expected: expected_content.to_string(),
                actual: content_id.to_string(),
            });
        }
        let upload_id = UploadId(array_at::<16>(bytes, 57, "content descriptor upload id")?);
        let length = u64::from_le_bytes(array_at::<8>(bytes, 73, "content descriptor length")?);
        let chunk_bytes =
            u32::from_le_bytes(array_at::<4>(bytes, 81, "content descriptor chunk size")?);
        let chunk_count =
            u64::from_le_bytes(array_at::<8>(bytes, 85, "content descriptor chunk count")?);
        if !(MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&usize::try_from(chunk_bytes).map_err(
            |_| Error::InvalidFormat {
                message: "content descriptor chunk size exceeds usize".to_owned(),
            },
        )?) {
            return Err(Error::InvalidFormat {
                message: format!("invalid content descriptor chunk size {chunk_bytes}"),
            });
        }
        let expected_chunks = if length == 0 {
            0
        } else {
            length.div_ceil(u64::from(chunk_bytes))
        };
        if chunk_count != expected_chunks {
            return Err(Error::InvalidFormat {
                message: format!(
                    "content descriptor chunk count {chunk_count} does not match {expected_chunks}"
                ),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            upload_id,
            length,
            chunk_bytes,
            chunk_count,
        })
    }
}

fn encode_chunk(upload_id: UploadId, index: u64, payload: &[u8]) -> Result<Arc<[u8]>> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("content chunk payload exceeds u32"))?;
    let digest: [u8; 32] = Sha256::digest(payload).into();
    let mut bytes = Vec::with_capacity(CHUNK_HEADER_LEN + payload.len());
    bytes.extend_from_slice(CHUNK_MAGIC);
    bytes.extend_from_slice(&upload_id.bytes());
    bytes.extend_from_slice(&index.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&digest);
    bytes.extend_from_slice(payload);
    Ok(Arc::from(bytes))
}

pub(crate) fn decode_chunk(
    bytes: &[u8],
    expected_upload: UploadId,
    expected_index: u64,
) -> Result<&[u8]> {
    if bytes.len() < CHUNK_HEADER_LEN || bytes.get(..8) != Some(CHUNK_MAGIC) {
        return Err(Error::InvalidFormat {
            message: format!("invalid content chunk {expected_index} header"),
        });
    }
    let upload_id = UploadId(array_at::<16>(bytes, 8, "content chunk upload id")?);
    let index = u64::from_le_bytes(array_at::<8>(bytes, 24, "content chunk index")?);
    let payload_len = usize::try_from(u32::from_le_bytes(array_at::<4>(
        bytes,
        32,
        "content chunk payload length",
    )?))
    .map_err(|_| Error::InvalidFormat {
        message: "content chunk payload length exceeds usize".to_owned(),
    })?;
    let expected_digest = array_at::<32>(bytes, 36, "content chunk digest")?;
    if upload_id != expected_upload || index != expected_index {
        return Err(Error::InvalidFormat {
            message: format!("content chunk identity mismatch at index {expected_index}"),
        });
    }
    let payload = bytes
        .get(CHUNK_HEADER_LEN..)
        .ok_or_else(|| Error::InvalidFormat {
            message: format!("content chunk {expected_index} payload is missing"),
        })?;
    if payload.len() != payload_len {
        return Err(Error::InvalidFormat {
            message: format!(
                "content chunk {expected_index} length {} does not match {payload_len}",
                payload.len()
            ),
        });
    }
    let actual_digest: [u8; 32] = Sha256::digest(payload).into();
    if actual_digest != expected_digest {
        return Err(Error::ContentDigestMismatch {
            expected: digest_string(expected_digest),
            actual: digest_string(actual_digest),
        });
    }
    Ok(payload)
}
