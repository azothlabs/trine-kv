use super::{OwnerScopeId, StorageDomainId};

/// Scope that an attachment token is issued to and later verified against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentAttachmentScope {
    pub(in crate::content) storage_domain_id: StorageDomainId,
    pub(in crate::content) owner_scope_id: OwnerScopeId,
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
