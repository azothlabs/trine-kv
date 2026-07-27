use super::{
    ContentAttachmentScope, ContentId, DurabilityMode, StorageDomainId, UploadId, UploadToken,
};

/// Result of sealing one immutable content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedContent {
    pub(in crate::content) attachment_scope: ContentAttachmentScope,
    pub(in crate::content) content_id: ContentId,
    pub(in crate::content) length: u64,
    pub(in crate::content) upload_token: UploadToken,
    pub(in crate::content) token_expires_at_unix_ms: u64,
    pub(in crate::content) durability: DurabilityMode,
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
    pub(in crate::content) upload_id: UploadId,
    pub(in crate::content) scope: ContentAttachmentScope,
    pub(in crate::content) content_id: ContentId,
    pub(in crate::content) length: u64,
    pub(in crate::content) token_expires_at_unix_ms: u64,
    pub(in crate::content) durability: DurabilityMode,
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
