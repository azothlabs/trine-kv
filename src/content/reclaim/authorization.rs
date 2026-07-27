use super::{CONTENT_RECLAIM_PROOF_TOKEN_BYTES, ContentId, StorageDomainId, fmt};

/// Opaque identity and digest bytes for one higher-layer reclaim proof.
///
/// Trine KV persists and compares this token but does not parse logical roots,
/// Policies, Versions, or proof-digest semantics. The higher layer must verify
/// those claims in the same optimistic transaction that stages reclaim intent.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentReclaimProofToken([u8; CONTENT_RECLAIM_PROOF_TOKEN_BYTES]);

impl ContentReclaimProofToken {
    /// Reconstructs an opaque proof token from its fixed portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; CONTENT_RECLAIM_PROOF_TOKEN_BYTES]) -> Self {
        Self(bytes)
    }

    /// Returns the fixed opaque proof bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; CONTENT_RECLAIM_PROOF_TOKEN_BYTES] {
        self.0
    }
}

impl fmt::Debug for ContentReclaimProofToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentReclaimProofToken([REDACTED])")
    }
}

/// Physical claims Trine KV checks before recording reclaim intent.
///
/// `verified_at` is the instance-local commit sequence `S` used by the
/// higher-layer exact absence proof. Trine KV rejects the request if later
/// durable content activity exists. `expires_at_unix_ms` is an exclusive
/// wall-clock deadline: a request at or after that millisecond is expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReclaimAuthorization {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    proof_token: ContentReclaimProofToken,
    verified_at: crate::ReadVersion,
    expires_at_unix_ms: u64,
}

impl ContentReclaimAuthorization {
    /// Creates physical reclaim claims supplied by a verified higher layer.
    #[must_use]
    pub const fn new(
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        proof_token: ContentReclaimProofToken,
        verified_at: crate::ReadVersion,
        expires_at_unix_ms: u64,
    ) -> Self {
        Self {
            storage_domain_id,
            content_id,
            proof_token,
            verified_at,
            expires_at_unix_ms,
        }
    }

    /// Returns the exact physical lifecycle domain.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the immutable byte identity being considered.
    #[must_use]
    pub const fn content_id(self) -> ContentId {
        self.content_id
    }

    /// Returns the opaque higher-layer proof identity and digest.
    #[must_use]
    pub const fn proof_token(self) -> ContentReclaimProofToken {
        self.proof_token
    }

    /// Returns the stable commit sequence checked by the higher layer.
    #[must_use]
    pub const fn verified_at(self) -> crate::ReadVersion {
        self.verified_at
    }

    /// Returns the exclusive proof deadline as Unix epoch milliseconds.
    #[must_use]
    pub const fn expires_at_unix_ms(self) -> u64 {
        self.expires_at_unix_ms
    }
}

/// Result of staging physical reclaim intent in an optimistic transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentReclaimIntentStage {
    /// This transaction staged a new or replacement intent record.
    Staged,
    /// The exact same intent was already durable at this commit sequence.
    Existing {
        /// Commit sequence that durably accepted the existing intent.
        accepted_at: crate::ReadVersion,
    },
}

/// Result of staging a crash-safe content quarantine in an optimistic transaction.
///
/// Quarantine blocks new leased reads through Trine KV but keeps every byte and
/// descriptor intact. It does not start grace and does not authorize physical
/// deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentQuarantineStage {
    /// This transaction staged a new quarantine record.
    Staged,
    /// The exact same quarantine was already durable.
    Existing {
        /// Commit sequence that durably established quarantine.
        quarantined_at: crate::ReadVersion,
    },
}

/// Result of staging a durable reclaim-grace scheduling record.
///
/// Grace keeps quarantine active and deletes nothing. Its wall-clock deadline
/// is an earliest scheduling observation only, not deletion authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentReclaimGraceStage {
    /// This transaction staged a new grace record.
    Staged,
    /// The exact same grace record was already durable.
    Existing {
        /// Commit sequence that durably started grace.
        started_at: crate::ReadVersion,
    },
}

/// Result of staging the final durable physical-reclamation fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentReclaimSweepStage {
    /// This transaction staged a new Prepared sweep.
    Staged,
    /// The exact same Prepared sweep was already durable.
    Existing {
        /// Commit sequence that established the irreversible worker fence.
        prepared_at: crate::ReadVersion,
    },
}
