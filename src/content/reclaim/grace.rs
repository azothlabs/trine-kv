use super::{ContentId, ContentReclaimProofToken, StorageDomainId};

/// Durable scheduling boundary retained while exact content stays quarantined.
///
/// `not_before_unix_ms` is derived from the host wall clock when this record is
/// committed. Reaching it does not prove elapsed real time across clock jumps or
/// restarts and never authorizes deletion. A future delete protocol must obtain
/// separate clock, provider, replica, and fresh lifecycle evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReclaimGrace {
    pub(in crate::content::reclaim) storage_domain_id: StorageDomainId,
    pub(in crate::content::reclaim) content_id: ContentId,
    pub(in crate::content::reclaim) proof_token: ContentReclaimProofToken,
    pub(in crate::content::reclaim) quarantined_at: crate::ReadVersion,
    pub(in crate::content::reclaim) requested_duration_ms: u64,
    pub(in crate::content::reclaim) observed_at_unix_ms: u64,
    pub(in crate::content::reclaim) not_before_unix_ms: u64,
    pub(in crate::content::reclaim) started_at: crate::ReadVersion,
}

impl ContentReclaimGrace {
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

    /// Returns the proof token used by the matching quarantine transition.
    #[must_use]
    pub const fn proof_token(self) -> ContentReclaimProofToken {
        self.proof_token
    }

    /// Returns the commit sequence that established the retained quarantine.
    #[must_use]
    pub const fn quarantined_at(self) -> crate::ReadVersion {
        self.quarantined_at
    }

    /// Returns the requested wall-clock observation delay in milliseconds.
    ///
    /// The delay starts at [`Self::observed_at_unix_ms`], before the transaction
    /// commit identified by [`Self::started_at`]. It is not a minimum duration
    /// measured from durable visibility.
    #[must_use]
    pub const fn requested_duration_ms(self) -> u64 {
        self.requested_duration_ms
    }

    /// Returns the host Unix-millisecond observation used to start grace.
    #[must_use]
    pub const fn observed_at_unix_ms(self) -> u64 {
        self.observed_at_unix_ms
    }

    /// Returns the earliest Unix-millisecond scheduling observation.
    ///
    /// Passing this value is not deletion authority because a wall clock can
    /// jump and cannot prove elapsed time across restart by itself.
    #[must_use]
    pub const fn not_before_unix_ms(self) -> u64 {
        self.not_before_unix_ms
    }

    /// Returns the commit sequence that durably started grace.
    #[must_use]
    pub const fn started_at(self) -> crate::ReadVersion {
        self.started_at
    }
}
