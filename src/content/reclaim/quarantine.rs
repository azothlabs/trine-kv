use super::{
    ContentAccessBarrierId, ContentId, ContentReaderDrainAttestationId, ContentReclaimProofToken,
    StorageDomainId,
};

/// Durable, exact-content quarantine coordinate.
///
/// The record binds one accepted reclaim intent, the leased-only barrier, and
/// the trusted reader-drain attestation used by the transition. Quarantine
/// prevents new leased opens but retains the descriptor and all content bytes.
/// Attachment authority or a physical hold may atomically return the content to
/// Active state before any future deletion protocol begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentQuarantine {
    pub(in crate::content::reclaim) storage_domain_id: StorageDomainId,
    pub(in crate::content::reclaim) content_id: ContentId,
    pub(in crate::content::reclaim) proof_token: ContentReclaimProofToken,
    pub(in crate::content::reclaim) verified_at: crate::ReadVersion,
    pub(in crate::content::reclaim) proof_expires_at_unix_ms: u64,
    pub(in crate::content::reclaim) intent_accepted_at: crate::ReadVersion,
    pub(in crate::content::reclaim) barrier_id: ContentAccessBarrierId,
    pub(in crate::content::reclaim) barrier_enforced_at: crate::ReadVersion,
    pub(in crate::content::reclaim) drain_attestation_id: ContentReaderDrainAttestationId,
    pub(in crate::content::reclaim) quarantined_at: crate::ReadVersion,
}

impl ContentQuarantine {
    /// Returns the physical lifecycle domain.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the exact immutable content identity.
    #[must_use]
    pub const fn content_id(self) -> ContentId {
        self.content_id
    }

    /// Returns the opaque higher-layer proof token accepted by the intent.
    #[must_use]
    pub const fn proof_token(self) -> ContentReclaimProofToken {
        self.proof_token
    }

    /// Returns the logical proof's stable verification coordinate.
    #[must_use]
    pub const fn verified_at(self) -> crate::ReadVersion {
        self.verified_at
    }

    /// Returns the logical proof's exclusive Unix-millisecond deadline.
    ///
    /// Expiry does not remove an already durable quarantine. A future sweep
    /// still needs a fresh logical proof and all physical checks.
    #[must_use]
    pub const fn proof_expires_at_unix_ms(self) -> u64 {
        self.proof_expires_at_unix_ms
    }

    /// Returns the commit sequence that accepted the matching reclaim intent.
    #[must_use]
    pub const fn intent_accepted_at(self) -> crate::ReadVersion {
        self.intent_accepted_at
    }

    /// Returns the irreversible leased-only barrier identity.
    #[must_use]
    pub const fn barrier_id(self) -> ContentAccessBarrierId {
        self.barrier_id
    }

    /// Returns the commit sequence that completed the leased-only barrier.
    #[must_use]
    pub const fn barrier_enforced_at(self) -> crate::ReadVersion {
        self.barrier_enforced_at
    }

    /// Returns the trusted coordinator attestation bound to the transition.
    #[must_use]
    pub const fn drain_attestation_id(self) -> ContentReaderDrainAttestationId {
        self.drain_attestation_id
    }

    /// Returns the commit sequence that established quarantine.
    #[must_use]
    pub const fn quarantined_at(self) -> crate::ReadVersion {
        self.quarantined_at
    }
}
