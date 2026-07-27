use super::{
    ContentAttachmentScope, ContentId, Duration, Error, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES, Result,
};

/// Configuration for a sequential content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentUploadOptions {
    pub(in crate::content) attachment_scope: ContentAttachmentScope,
    pub(in crate::content) token_ttl: Duration,
    pub(in crate::content) chunk_bytes: usize,
    pub(in crate::content) expected_length: Option<u64>,
    pub(in crate::content) expected_content_id: Option<ContentId>,
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
