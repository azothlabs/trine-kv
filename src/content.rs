//! Immutable, cryptographically identified content objects.
//!
//! This module is the storage-layer primitive used by higher layers that need
//! files or other large byte sequences. Content is accepted incrementally,
//! sealed by publishing a fixed-size descriptor after all chunks are durable,
//! and read through verified ranges or a sequential stream. Ordinary key/value
//! values do not enter this path automatically.

use std::{fmt, mem, sync::Arc, time::Duration};

use sha2::{Digest, Sha256};

use crate::{Db, DurabilityMode, Error, Result};

const CONTENT_ID_SHA256_TAG: u8 = 1;
const UPLOAD_TOKEN_VERSION: u8 = 1;
const DESCRIPTOR_MAGIC: &[u8; 8] = b"TRNCNTD2";
const CHUNK_MAGIC: &[u8; 8] = b"TRNCNTC1";
const UPLOAD_STATE_MAGIC: &[u8; 8] = b"TRNUPLD2";
const DESCRIPTOR_LEN: usize = 8 + 16 + 1 + 32 + 16 + 8 + 4 + 8;
const CHUNK_HEADER_LEN: usize = 8 + 16 + 8 + 4 + 32;
const UPLOAD_STATE_LEN: usize =
    8 + 1 + 16 + 8 + 4 + 8 + 8 + 4 + 1 + 8 + 1 + 33 + 16 + 16 + 32 + 8 + 8 + 1 + 33;
const UPLOAD_STATE_OPEN: u8 = 0;
const UPLOAD_STATE_SEALING: u8 = 1;
const UPLOAD_STATE_SEALED: u8 = 2;
const MIN_CHUNK_BYTES: usize = 64 * 1024;
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Opaque control-plane identity for one physical content boundary.
///
/// Deduplication, encryption, physical quota, and reclamation are scoped to
/// this identity. Trine KV compares and persists the bytes but does not parse
/// tenant or project semantics from them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageDomainId([u8; 16]);

impl StorageDomainId {
    /// Reconstructs an identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for StorageDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageDomainId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for StorageDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Opaque authenticated owner scope supplied by the database layer.
///
/// This identity can represent a project, tenant, or another authorization
/// boundary. Trine KV only requires exact equality when a token is consumed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerScopeId([u8; 16]);

impl OwnerScopeId {
    /// Reconstructs an identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for OwnerScopeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerScopeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Scope that an attachment token is issued to and later verified against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentAttachmentScope {
    storage_domain_id: StorageDomainId,
    owner_scope_id: OwnerScopeId,
}

impl ContentAttachmentScope {
    /// Creates an exact storage-domain and owner binding.
    #[must_use]
    pub const fn new(storage_domain_id: StorageDomainId, owner_scope_id: OwnerScopeId) -> Self {
        Self {
            storage_domain_id,
            owner_scope_id,
        }
    }

    /// Returns the physical content boundary.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the authenticated owner boundary.
    #[must_use]
    pub const fn owner_scope_id(self) -> OwnerScopeId {
        self.owner_scope_id
    }
}

/// Opaque bearer authority returned only after an upload is sealed.
///
/// The 32 random bytes are secret. Logging implementations deliberately redact
/// them; callers that persist or transmit a token should protect it like any
/// other short-lived capability.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadToken([u8; 32]);

impl UploadToken {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload token entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn secret(self) -> [u8; 32] {
        self.0
    }

    /// Decodes the versioned 33-byte bearer representation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the first byte names an
    /// unknown token format version.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        if bytes[0] != UPLOAD_TOKEN_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported upload token version {}", bytes[0]),
            });
        }
        let mut secret = [0_u8; 32];
        secret.copy_from_slice(&bytes[1..]);
        Ok(Self(secret))
    }

    /// Returns the versioned 33-byte bearer representation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = UPLOAD_TOKEN_VERSION;
        bytes[1..].copy_from_slice(&self.0);
        bytes
    }
}

impl fmt::Debug for UploadToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UploadToken([REDACTED])")
    }
}

/// Opaque database-layer change identity used for idempotent consumption.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentChangeId([u8; 16]);

impl ContentChangeId {
    /// Reconstructs a change identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentChangeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentChangeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Cryptographic algorithm carried by a [`ContentId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ContentHashAlgorithm {
    /// SHA-256 over the complete original byte sequence.
    Sha256,
}

impl ContentHashAlgorithm {
    const fn tag(self) -> u8 {
        match self {
            Self::Sha256 => CONTENT_ID_SHA256_TAG,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            CONTENT_ID_SHA256_TAG => Ok(Self::Sha256),
            _ => Err(Error::UnsupportedFormat {
                message: format!("unsupported content hash algorithm tag {tag}"),
            }),
        }
    }
}

/// Algorithm-tagged identity of one immutable original byte sequence.
///
/// Equality means byte identity under the carried cryptographic algorithm. It
/// does not identify a file name, owner, directory entry, or physical object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentId {
    algorithm: ContentHashAlgorithm,
    digest: [u8; 32],
}

impl ContentId {
    /// Decodes the portable 33-byte content identity.
    ///
    /// Byte zero selects the digest algorithm and the remaining 32 bytes carry
    /// its digest. Unknown algorithm tags fail closed so stored catalog values
    /// are never reinterpreted under a different hash scheme.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the algorithm tag is not
    /// supported by this build.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        let algorithm = ContentHashAlgorithm::from_tag(bytes[0])?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes[1..]);
        Ok(Self { algorithm, digest })
    }

    /// Returns the portable algorithm-tagged 33-byte content identity.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = self.algorithm.tag();
        bytes[1..].copy_from_slice(&self.digest);
        bytes
    }

    /// Computes the identity of an in-memory byte slice.
    ///
    /// Incremental uploads compute the same value without retaining all bytes;
    /// this convenience constructor is intended for expected-digest checks and
    /// small inputs.
    #[must_use]
    pub fn for_bytes(bytes: &[u8]) -> Self {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        Self {
            algorithm: ContentHashAlgorithm::Sha256,
            digest,
        }
    }

    /// Returns the hash algorithm carried by this identity.
    #[must_use]
    pub const fn algorithm(self) -> ContentHashAlgorithm {
        self.algorithm
    }

    /// Returns the 32-byte digest without its algorithm tag.
    #[must_use]
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub(crate) fn from_sha256(digest: [u8; 32]) -> Self {
        Self {
            algorithm: ContentHashAlgorithm::Sha256,
            digest,
        }
    }

    fn encode_into(self, bytes: &mut Vec<u8>) {
        bytes.push(self.algorithm.tag());
        bytes.extend_from_slice(&self.digest);
    }
}

impl fmt::Debug for ContentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ContentId({self})")
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("sha256:")?;
        write_hex(formatter, &self.digest)
    }
}

/// Globally random identity of one upload attempt.
///
/// Upload identity is temporary transfer state. It is deliberately distinct
/// from [`ContentId`], which is known only after the complete byte sequence has
/// been hashed or supplied and verified.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UploadId([u8; 16]);

impl UploadId {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload identity entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn bytes(self) -> [u8; 16] {
        self.0
    }

    /// Reconstructs an upload identity from its portable 16-byte form.
    ///
    /// Upload identities are not secrets. Persist this value when an upload
    /// must be resumed after the current process or database handle is gone.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable 16-byte representation.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    pub(crate) const fn lock_shard(self) -> usize {
        self.0[0] as usize
    }
}

impl fmt::Debug for UploadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "UploadId({self})")
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Configuration for a sequential content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentUploadOptions {
    attachment_scope: ContentAttachmentScope,
    token_ttl: Duration,
    chunk_bytes: usize,
    expected_length: Option<u64>,
    expected_content_id: Option<ContentId>,
}

impl ContentUploadOptions {
    /// Default chunk size used by uploads and sequential reads.
    pub const DEFAULT_CHUNK_BYTES: usize = 4 * 1024 * 1024;

    /// Creates options with an explicit attachment scope and token lifetime.
    ///
    /// The token lifetime starts when seal first reaches its durable sealing
    /// state, not when the upload begins. A zero or sub-millisecond lifetime is
    /// rejected by [`Db::begin_content_upload`](crate::Db::begin_content_upload).
    /// The default chunk bound is 4 MiB and no final identity is expected.
    #[must_use]
    pub const fn new(attachment_scope: ContentAttachmentScope, token_ttl: Duration) -> Self {
        Self {
            attachment_scope,
            token_ttl,
            chunk_bytes: Self::DEFAULT_CHUNK_BYTES,
            expected_length: None,
            expected_content_id: None,
        }
    }

    /// Sets the maximum unsealed payload bytes retained by the upload.
    ///
    /// Valid values are 64 KiB through 16 MiB, inclusive. The value also fixes
    /// chunk boundaries in the sealed descriptor. Invalid values are reported
    /// by [`Db::begin_content_upload`](crate::Db::begin_content_upload).
    #[must_use]
    pub const fn with_chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        self.chunk_bytes = chunk_bytes;
        self
    }

    /// Requires the final original byte length to equal `expected_length`.
    #[must_use]
    pub const fn with_expected_length(mut self, expected_length: u64) -> Self {
        self.expected_length = Some(expected_length);
        self
    }

    /// Requires the final complete digest to equal `expected_content_id`.
    #[must_use]
    pub const fn with_expected_content_id(mut self, expected_content_id: ContentId) -> Self {
        self.expected_content_id = Some(expected_content_id);
        self
    }

    /// Returns the configured chunk bound in bytes.
    #[must_use]
    pub const fn chunk_bytes(self) -> usize {
        self.chunk_bytes
    }

    /// Returns the scope that the sealed token will be bound to.
    #[must_use]
    pub const fn attachment_scope(self) -> ContentAttachmentScope {
        self.attachment_scope
    }

    /// Returns the requested retry lifetime starting at seal.
    #[must_use]
    pub const fn token_ttl(self) -> Duration {
        self.token_ttl
    }

    pub(crate) fn validate(self) -> Result<Self> {
        if !(MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&self.chunk_bytes) {
            return Err(Error::invalid_options(format!(
                "content chunk size {} is outside {MIN_CHUNK_BYTES}..={MAX_CHUNK_BYTES}",
                self.chunk_bytes
            )));
        }
        let ttl_ms = self.token_ttl.as_millis();
        if ttl_ms == 0 || ttl_ms > u128::from(u64::MAX) {
            return Err(Error::invalid_options(
                "upload token lifetime must be 1..=u64::MAX milliseconds",
            ));
        }
        Ok(self)
    }

    pub(crate) const fn expected_length(self) -> Option<u64> {
        self.expected_length
    }

    pub(crate) const fn expected_content_id(self) -> Option<ContentId> {
        self.expected_content_id
    }

    pub(crate) fn token_ttl_ms(self) -> Result<u64> {
        u64::try_from(self.token_ttl.as_millis()).map_err(|_| {
            Error::invalid_options("upload token lifetime milliseconds exceed u64::MAX")
        })
    }
}

/// Result of sealing one immutable content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedContent {
    attachment_scope: ContentAttachmentScope,
    content_id: ContentId,
    length: u64,
    upload_token: UploadToken,
    token_expires_at_unix_ms: u64,
    durability: DurabilityMode,
}

impl SealedContent {
    /// Returns the verified identity of the complete original bytes.
    #[must_use]
    pub const fn content_id(self) -> ContentId {
        self.content_id
    }

    /// Returns the storage domain in which this content was sealed.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.attachment_scope.storage_domain_id
    }

    /// Returns the exact domain and owner binding carried by the token.
    #[must_use]
    pub const fn attachment_scope(self) -> ContentAttachmentScope {
        self.attachment_scope
    }

    /// Returns the original byte length.
    #[must_use]
    pub const fn len(self) -> u64 {
        self.length
    }

    /// Returns whether the sealed content is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    /// Returns the short-lived bearer authority for atomic attachment.
    #[must_use]
    pub const fn upload_token(self) -> UploadToken {
        self.upload_token
    }

    /// Returns the token deadline as Unix epoch milliseconds.
    ///
    /// An available token is invalid once the current time is greater than or
    /// equal to this value. A token already consumed by its `ChangeId` remains an
    /// idempotency record rather than reusable authority.
    #[must_use]
    pub const fn token_expires_at_unix_ms(self) -> u64 {
        self.token_expires_at_unix_ms
    }

    /// Returns the durability result satisfied before token issue.
    #[must_use]
    pub const fn durability(self) -> DurabilityMode {
        self.durability
    }
}

/// Claims verified while staging one upload-token consumption.
///
/// Returning this value does not itself attach content. The token transition
/// and the caller's catalog writes become durable together only if the owning
/// [`crate::Transaction`] commits successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentAttachment {
    upload_id: UploadId,
    scope: ContentAttachmentScope,
    content_id: ContentId,
    length: u64,
    token_expires_at_unix_ms: u64,
    durability: DurabilityMode,
}

impl ContentAttachment {
    pub(crate) const fn new(
        upload_id: UploadId,
        scope: ContentAttachmentScope,
        content_id: ContentId,
        length: u64,
        token_expires_at_unix_ms: u64,
        durability: DurabilityMode,
    ) -> Self {
        Self {
            upload_id,
            scope,
            content_id,
            length,
            token_expires_at_unix_ms,
            durability,
        }
    }

    /// Returns the upload attempt that issued the token.
    #[must_use]
    pub const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    /// Returns the exact storage-domain and owner binding.
    #[must_use]
    pub const fn scope(self) -> ContentAttachmentScope {
        self.scope
    }

    /// Returns the immutable byte identity authorized for attachment.
    #[must_use]
    pub const fn content_id(self) -> ContentId {
        self.content_id
    }

    /// Returns the original byte length.
    #[must_use]
    pub const fn len(self) -> u64 {
        self.length
    }

    /// Returns whether the authorized original byte sequence is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    /// Returns the original token deadline as Unix epoch milliseconds.
    #[must_use]
    pub const fn token_expires_at_unix_ms(self) -> u64 {
        self.token_expires_at_unix_ms
    }

    /// Returns the durability result that was satisfied before token issue.
    #[must_use]
    pub const fn durability(self) -> DurabilityMode {
        self.durability
    }
}

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

fn upload_token_hash(token: UploadToken) -> [u8; 32] {
    Sha256::digest(token.to_bytes()).into()
}

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
        self.ensure_active()?;
        if bytes.is_empty() {
            return Ok(());
        }

        let db = self.db.clone();
        let _upload = db.lock_content_upload(self.upload_id).await;
        let durable = db.require_upload_state(self.upload_id).await?;
        durable.require_open_revision(self.revision)?;

        let incoming = u64::try_from(bytes.len())
            .map_err(|_| Error::invalid_options("content write length exceeds u64"))?;
        self.length = self
            .length
            .checked_add(incoming)
            .ok_or_else(|| Error::invalid_options("content length overflow"))?;

        while !bytes.is_empty() {
            let available = self.options.chunk_bytes - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.options.chunk_bytes {
                if let Err(error) = self.flush_full_chunk().await {
                    self.failed = true;
                    return Err(error);
                }
            }
        }

        if !self.buffer.is_empty() {
            let frame = encode_chunk(self.upload_id, self.complete_chunks, &self.buffer)?;
            if let Err(error) = self
                .db
                .write_content_chunk(self.upload_id, self.complete_chunks, frame)
                .await
            {
                self.failed = true;
                return Err(error);
            }
        }
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?;
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
    /// No descriptor is published. The durable session is deleted before
    /// best-effort chunk cleanup, so a crash cannot leave a resumable session
    /// that points at missing bytes.
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
            Vec::with_capacity(self.options.chunk_bytes),
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

/// Stable read handle for one sealed immutable `ContentObject`.
///
/// The handle fixes one descriptor when opened. This prototype has no physical
/// relocation, so the descriptor's upload identity and chunk boundaries remain
/// stable for the handle lifetime.
#[derive(Debug, Clone)]
pub struct ContentHandle {
    db: Db,
    descriptor: ContentDescriptor,
}

impl ContentHandle {
    pub(crate) fn new(db: Db, descriptor: ContentDescriptor) -> Self {
        Self { db, descriptor }
    }

    /// Returns the immutable content identity fixed by this handle.
    #[must_use]
    pub const fn content_id(&self) -> ContentId {
        self.descriptor.content_id
    }

    /// Returns the storage domain fixed by this handle.
    #[must_use]
    pub const fn storage_domain_id(&self) -> StorageDomainId {
        self.descriptor.storage_domain_id
    }

    /// Returns the original byte length.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.descriptor.length
    }

    /// Returns whether the original byte sequence is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.descriptor.length == 0
    }

    /// Reads a verified byte range using common half-open range semantics.
    ///
    /// `start > len()` returns [`Error::ContentRangeOutOfBounds`]. A zero-byte
    /// request or `start == len()` returns empty bytes. A request extending past
    /// EOF returns the verified suffix. Only chunks touched by the result are
    /// read, and each is verified before any of its payload is copied.
    ///
    /// # Errors
    ///
    /// Returns a typed range error, a storage error, or
    /// [`Error::ContentDigestMismatch`] / [`Error::Corruption`] if a chunk is
    /// missing, malformed, or tampered with.
    pub async fn read_range(&self, start: u64, length: u64) -> Result<Arc<[u8]>> {
        if start > self.descriptor.length {
            return Err(Error::ContentRangeOutOfBounds {
                start,
                length: self.descriptor.length,
            });
        }
        if length == 0 || start == self.descriptor.length {
            return Ok(Arc::from([]));
        }
        let end = start
            .checked_add(length)
            .unwrap_or(u64::MAX)
            .min(self.descriptor.length);
        let result_len = usize::try_from(end - start)
            .map_err(|_| Error::invalid_options("requested content range exceeds usize"))?;
        let mut result = Vec::with_capacity(result_len);
        let chunk_bytes = u64::from(self.descriptor.chunk_bytes);
        let mut position = start;
        while position < end {
            let chunk_index = position / chunk_bytes;
            let frame = self
                .db
                .read_content_chunk(self.descriptor.upload_id, chunk_index)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content {} is missing chunk {chunk_index}",
                        self.descriptor.content_id
                    ),
                })?;
            let payload = decode_chunk(&frame, self.descriptor.upload_id, chunk_index)?;
            let offset = usize::try_from(position % chunk_bytes)
                .map_err(|_| Error::invalid_options("content chunk offset exceeds usize"))?;
            let available = payload
                .len()
                .checked_sub(offset)
                .ok_or_else(|| Error::Corruption {
                    message: format!("content chunk {chunk_index} is shorter than its descriptor"),
                })?;
            let remaining = usize::try_from(end - position)
                .map_err(|_| Error::invalid_options("content range remainder exceeds usize"))?;
            let take = available.min(remaining);
            if take == 0 {
                return Err(Error::Corruption {
                    message: format!("content chunk {chunk_index} made no read progress"),
                });
            }
            result.extend_from_slice(&payload[offset..offset + take]);
            position =
                position
                    .checked_add(u64::try_from(take).map_err(|_| {
                        Error::invalid_options("content range progress exceeds u64")
                    })?)
                    .ok_or_else(|| Error::invalid_options("content range position overflow"))?;
        }
        Ok(Arc::from(result))
    }

    /// Creates a sequential stream starting at byte zero.
    #[must_use]
    pub fn stream(&self) -> ContentStream {
        ContentStream {
            handle: self.clone(),
            position: 0,
        }
    }

    /// Recomputes the complete `ContentId` through a bounded-memory stream.
    ///
    /// This additionally verifies each chunk frame. Success means both the
    /// per-chunk digests and the complete original-byte digest match.
    ///
    /// # Errors
    ///
    /// Returns a storage, format, or digest error at the first invalid chunk.
    pub async fn verify(&self) -> Result<()> {
        let mut stream = self.stream();
        let mut hasher = Sha256::new();
        while let Some(bytes) = stream.next().await? {
            hasher.update(&bytes);
        }
        let actual = ContentId::from_sha256(hasher.finalize().into());
        if actual != self.descriptor.content_id {
            return Err(Error::ContentDigestMismatch {
                expected: self.descriptor.content_id.to_string(),
                actual: actual.to_string(),
            });
        }
        Ok(())
    }
}

/// Sequential verified reader over one [`ContentHandle`].
#[derive(Debug, Clone)]
pub struct ContentStream {
    handle: ContentHandle,
    position: u64,
}

impl ContentStream {
    /// Returns the next verified chunk, or `None` at EOF.
    ///
    /// Each returned value is at most the upload's configured chunk size. The
    /// stream therefore does not allocate in proportion to total content size.
    ///
    /// # Errors
    ///
    /// Propagates the same storage, format, and integrity errors as
    /// [`ContentHandle::read_range`].
    pub async fn next(&mut self) -> Result<Option<Arc<[u8]>>> {
        if self.position == self.handle.len() {
            return Ok(None);
        }
        let remaining = self.handle.len() - self.position;
        let length = remaining.min(u64::from(self.handle.descriptor.chunk_bytes));
        let bytes = self.handle.read_range(self.position, length).await?;
        self.position =
            self.position
                .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                    Error::invalid_options("content stream chunk length exceeds u64")
                })?)
                .ok_or_else(|| Error::invalid_options("content stream position overflow"))?;
        Ok(Some(bytes))
    }

    /// Returns the next unread original-byte offset.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadSessionStatus {
    Open,
    Sealing(SealedContent),
    Sealed(SealedContent),
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
            UploadSessionStatus::Open => {
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
        let sealed_id = match status_tag {
            UPLOAD_STATE_OPEN => None,
            UPLOAD_STATE_SEALING | UPLOAD_STATE_SEALED => {
                Some(decode_content_id(bytes, 181, "sealed content identity")?)
            }
            _ => {
                return Err(Error::UnsupportedFormat {
                    message: format!("unsupported content upload state tag {status_tag}"),
                });
            }
        };
        let options = ContentUploadOptions {
            attachment_scope: ContentAttachmentScope::new(storage_domain_id, owner_scope_id),
            token_ttl: Duration::from_millis(token_ttl_ms),
            chunk_bytes,
            expected_length,
            expected_content_id,
        }
        .validate()?;
        let status = sealed_id.map_or(UploadSessionStatus::Open, |content_id| {
            let sealed = SealedContent {
                attachment_scope: options.attachment_scope,
                content_id,
                length,
                upload_token,
                token_expires_at_unix_ms,
                durability,
            };
            if status_tag == UPLOAD_STATE_SEALING {
                UploadSessionStatus::Sealing(sealed)
            } else {
                UploadSessionStatus::Sealed(sealed)
            }
        });
        let state = Self {
            upload_id,
            revision,
            options,
            length,
            complete_chunks,
            partial_len,
            upload_token,
            status,
        };
        state.validate()?;
        Ok(state)
    }

    fn validate(self) -> Result<()> {
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
            UploadSessionStatus::Open => {}
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentDescriptor {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    upload_id: UploadId,
    length: u64,
    chunk_bytes: u32,
    chunk_count: u64,
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

    pub(crate) const fn length(self) -> u64 {
        self.length
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

fn encode_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.extend_from_slice(&[0_u8; 9]),
    }
}

fn decode_optional_u64(bytes: &[u8], offset: usize, field: &'static str) -> Result<Option<u64>> {
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

fn encode_optional_content_id(bytes: &mut Vec<u8>, value: Option<ContentId>) {
    match value {
        Some(value) => {
            bytes.push(1);
            value.encode_into(bytes);
        }
        None => bytes.extend_from_slice(&[0_u8; 34]),
    }
}

fn decode_optional_content_id(
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

fn decode_content_id(bytes: &[u8], offset: usize, field: &'static str) -> Result<ContentId> {
    let tag = *bytes.get(offset).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} algorithm tag is truncated"),
    })?;
    let algorithm = ContentHashAlgorithm::from_tag(tag)?;
    let digest = array_at::<32>(bytes, offset + 1, field)?;
    Ok(ContentId { algorithm, digest })
}

fn array_at<const N: usize>(bytes: &[u8], offset: usize, field: &'static str) -> Result<[u8; N]> {
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

fn digest_string(digest: [u8; 32]) -> String {
    let mut value = String::with_capacity(7 + digest.len() * 2);
    value.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
