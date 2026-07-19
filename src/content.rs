//! Immutable, cryptographically identified content objects.
//!
//! This module is the storage-layer primitive used by higher layers that need
//! files or other large byte sequences. Content is accepted incrementally,
//! sealed by publishing a fixed-size descriptor after all chunks are durable,
//! and read through verified ranges or a sequential stream. Ordinary key/value
//! values do not enter this path automatically.

use std::{fmt, mem, sync::Arc};

use sha2::{Digest, Sha256};

use crate::{Db, Error, Result};

const CONTENT_ID_SHA256_TAG: u8 = 1;
const DESCRIPTOR_MAGIC: &[u8; 8] = b"TRNCNTD1";
const CHUNK_MAGIC: &[u8; 8] = b"TRNCNTC1";
const DESCRIPTOR_LEN: usize = 8 + 1 + 32 + 16 + 8 + 4 + 8;
const CHUNK_HEADER_LEN: usize = 8 + 16 + 8 + 4 + 32;
const MIN_CHUNK_BYTES: usize = 64 * 1024;
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

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

    fn from_sha256(digest: [u8; 32]) -> Self {
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
    chunk_bytes: usize,
    expected_length: Option<u64>,
    expected_content_id: Option<ContentId>,
}

impl ContentUploadOptions {
    /// Default chunk size used by uploads and sequential reads.
    pub const DEFAULT_CHUNK_BYTES: usize = 4 * 1024 * 1024;

    /// Creates options with a 4 MiB chunk bound and no expected identity.
    #[must_use]
    pub const fn new() -> Self {
        Self {
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

    pub(crate) fn validate(self) -> Result<Self> {
        if !(MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&self.chunk_bytes) {
            return Err(Error::invalid_options(format!(
                "content chunk size {} is outside {MIN_CHUNK_BYTES}..={MAX_CHUNK_BYTES}",
                self.chunk_bytes
            )));
        }
        Ok(self)
    }
}

impl Default for ContentUploadOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of sealing one immutable content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedContent {
    content_id: ContentId,
    length: u64,
}

impl SealedContent {
    /// Returns the verified identity of the complete original bytes.
    #[must_use]
    pub const fn content_id(self) -> ContentId {
        self.content_id
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
}

/// In-progress sequential upload with memory bounded by its configured chunk.
///
/// Calls to [`write`](Self::write) may be any size and are split into fixed
/// chunks. `seal` consumes the upload and publishes the fixed-size descriptor
/// only after all chunks have been stored and the complete identity verified.
/// Dropping an unsealed upload never publishes content; cleanup of abandoned
/// chunks is a maintenance concern.
pub struct ContentUpload {
    db: Db,
    upload_id: UploadId,
    options: ContentUploadOptions,
    hasher: Sha256,
    buffer: Vec<u8>,
    length: u64,
    chunk_count: u64,
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
            .field("chunk_count", &self.chunk_count)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl ContentUpload {
    pub(crate) fn new(db: Db, upload_id: UploadId, options: ContentUploadOptions) -> Self {
        Self {
            db,
            upload_id,
            options,
            hasher: Sha256::new(),
            buffer: Vec::with_capacity(options.chunk_bytes),
            length: 0,
            chunk_count: 0,
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
    /// Empty writes are no-ops. If a storage write fails, this upload becomes
    /// failed and cannot be sealed; start a new upload or abort this one.
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

        let incoming = u64::try_from(bytes.len())
            .map_err(|_| Error::invalid_options("content write length exceeds u64"))?;
        self.length = self
            .length
            .checked_add(incoming)
            .ok_or_else(|| Error::invalid_options("content length overflow"))?;
        self.hasher.update(bytes);

        while !bytes.is_empty() {
            let available = self.options.chunk_bytes - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.options.chunk_bytes {
                if let Err(error) = self.flush_chunk().await {
                    self.failed = true;
                    return Err(error);
                }
            }
        }
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
    /// Returns a typed length or digest mismatch when an expectation fails,
    /// [`Error::InvalidOptions`] when the upload previously failed, or a storage
    /// error if the final chunk or descriptor cannot be made durable. A failed
    /// descriptor write does not publish the `ContentObject`.
    pub async fn seal(mut self) -> Result<SealedContent> {
        self.ensure_active()?;
        if !self.buffer.is_empty() {
            if let Err(error) = self.flush_chunk().await {
                self.failed = true;
                return Err(error);
            }
        }

        let digest: [u8; 32] = self.hasher.clone().finalize().into();
        let content_id = ContentId::from_sha256(digest);
        if let Some(expected) = self.options.expected_length
            && expected != self.length
        {
            self.cleanup_chunks().await;
            return Err(Error::ContentLengthMismatch {
                expected,
                actual: self.length,
            });
        }
        if let Some(expected) = self.options.expected_content_id
            && expected != content_id
        {
            self.cleanup_chunks().await;
            return Err(Error::ContentDigestMismatch {
                expected: expected.to_string(),
                actual: content_id.to_string(),
            });
        }

        let descriptor = ContentDescriptor {
            content_id,
            upload_id: self.upload_id,
            length: self.length,
            chunk_bytes: u32::try_from(self.options.chunk_bytes)
                .map_err(|_| Error::invalid_options("content chunk size exceeds u32"))?,
            chunk_count: self.chunk_count,
        };
        let _seal = self.db.lock_content_seal().await;
        if let Some(existing) = self.db.read_content_descriptor(content_id).await? {
            let existing = ContentDescriptor::decode(&existing, content_id)?;
            self.cleanup_chunks().await;
            return Ok(existing.sealed());
        }
        self.db
            .write_content_descriptor(content_id, descriptor.encode())
            .await?;
        Ok(descriptor.sealed())
    }

    /// Aborts the upload and removes every chunk successfully written by it.
    ///
    /// No descriptor is ever published. Deletion is idempotent, so retrying
    /// cleanup after a partial failure is safe while the upload value remains
    /// available to the caller.
    ///
    /// # Errors
    ///
    /// Returns the first backend deletion error.
    pub async fn abort(mut self) -> Result<()> {
        self.buffer.clear();
        for index in 0..self.chunk_count {
            self.db.delete_content_chunk(self.upload_id, index).await?;
        }
        Ok(())
    }

    async fn flush_chunk(&mut self) -> Result<()> {
        let payload = mem::replace(
            &mut self.buffer,
            Vec::with_capacity(self.options.chunk_bytes),
        );
        let frame = encode_chunk(self.upload_id, self.chunk_count, &payload)?;
        self.db
            .write_content_chunk(self.upload_id, self.chunk_count, frame)
            .await?;
        self.chunk_count = self
            .chunk_count
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content chunk count overflow"))?;
        Ok(())
    }

    async fn cleanup_chunks(&self) {
        for index in 0..self.chunk_count {
            let _ = self.db.delete_content_chunk(self.upload_id, index).await;
        }
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
pub(crate) struct ContentDescriptor {
    content_id: ContentId,
    upload_id: UploadId,
    length: u64,
    chunk_bytes: u32,
    chunk_count: u64,
}

impl ContentDescriptor {
    fn encode(self) -> Arc<[u8]> {
        let mut bytes = Vec::with_capacity(DESCRIPTOR_LEN);
        bytes.extend_from_slice(DESCRIPTOR_MAGIC);
        self.content_id.encode_into(&mut bytes);
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        Arc::from(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], expected: ContentId) -> Result<Self> {
        if bytes.len() != DESCRIPTOR_LEN || bytes.get(..8) != Some(DESCRIPTOR_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content descriptor header or length".to_owned(),
            });
        }
        let algorithm = ContentHashAlgorithm::from_tag(bytes[8])?;
        let digest = array_at::<32>(bytes, 9, "content descriptor digest")?;
        let content_id = ContentId { algorithm, digest };
        if content_id != expected {
            return Err(Error::ContentDigestMismatch {
                expected: expected.to_string(),
                actual: content_id.to_string(),
            });
        }
        let upload_id = UploadId(array_at::<16>(bytes, 41, "content descriptor upload id")?);
        let length = u64::from_le_bytes(array_at::<8>(bytes, 57, "content descriptor length")?);
        let chunk_bytes =
            u32::from_le_bytes(array_at::<4>(bytes, 65, "content descriptor chunk size")?);
        let chunk_count =
            u64::from_le_bytes(array_at::<8>(bytes, 69, "content descriptor chunk count")?);
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
            content_id,
            upload_id,
            length,
            chunk_bytes,
            chunk_count,
        })
    }

    const fn sealed(self) -> SealedContent {
        SealedContent {
            content_id: self.content_id,
            length: self.length,
        }
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

fn decode_chunk(bytes: &[u8], expected_upload: UploadId, expected_index: u64) -> Result<&[u8]> {
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
