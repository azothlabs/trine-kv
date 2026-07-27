use super::{
    CONTENT_RECLAIM_CLOCK_ATTESTATION_ID_VERSION, CONTENT_RECLAIM_CLOCK_EVIDENCE_DOMAIN,
    CONTENT_RECLAIM_CLOCK_EVIDENCE_SHA256_TAG, ContentId, ContentReclaimGrace, Digest, Error,
    ObjectStoreReclamationEvidenceDigest, Result, Sha256, StorageDomainId, fmt, write_hex,
};

/// Versioned identity of one trusted grace-clock attestation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentReclaimClockAttestationId([u8; 16]);

impl ContentReclaimClockAttestationId {
    /// Generates a new identity from operating-system entropy.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when secure entropy is unavailable.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::runtime_busy(format!(
                "content reclaim-clock attestation entropy: {error}"
            ))
        })?;
        bytes[0] = CONTENT_RECLAIM_CLOCK_ATTESTATION_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned portable identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] for an unknown identity version.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_RECLAIM_CLOCK_ATTESTATION_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content reclaim-clock attestation identity version {}",
                    bytes[0]
                ),
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the portable identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentReclaimClockAttestationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentReclaimClockAttestationId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Opaque identity of the authority that verified grace across clock/restart.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentReclaimClockCoordinatorId([u8; 16]);

impl ContentReclaimClockCoordinatorId {
    /// Reconstructs an opaque coordinator identity from portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the opaque coordinator bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentReclaimClockCoordinatorId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentReclaimClockCoordinatorId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Algorithm-tagged digest of externally retained clock/restart evidence.
///
/// The SHA-256 commitment is audit provenance, not a signature or an
/// independently verified time proof.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentReclaimClockEvidenceDigest([u8; 33]);

impl ContentReclaimClockEvidenceDigest {
    /// Hashes canonical evidence bytes into the v1 portable digest.
    #[must_use]
    pub fn for_bytes(evidence: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(CONTENT_RECLAIM_CLOCK_EVIDENCE_DOMAIN);
        hasher.update(evidence);
        let mut bytes = [0_u8; 33];
        bytes[0] = CONTENT_RECLAIM_CLOCK_EVIDENCE_SHA256_TAG;
        bytes[1..].copy_from_slice(&hasher.finalize());
        Self(bytes)
    }

    /// Decodes an algorithm-tagged portable evidence digest.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] for an unknown algorithm tag.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        if bytes[0] != CONTENT_RECLAIM_CLOCK_EVIDENCE_SHA256_TAG {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content reclaim-clock evidence digest algorithm {}",
                    bytes[0]
                ),
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the algorithm-tagged portable digest bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 33] {
        self.0
    }
}

impl fmt::Debug for ContentReclaimClockEvidenceDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentReclaimClockEvidenceDigest(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Trusted caller claim that one exact grace interval has safely elapsed.
///
/// Trine KV validates the grace binding and ordering but cannot verify an
/// external monotonic clock, supervisor restart, or time authority. The caller
/// must retain the canonical evidence named by [`Self::evidence_digest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReclaimClockAttestation {
    pub(in crate::content::reclaim) storage_domain_id: StorageDomainId,
    pub(in crate::content::reclaim) content_id: ContentId,
    pub(in crate::content::reclaim) attestation_id: ContentReclaimClockAttestationId,
    pub(in crate::content::reclaim) coordinator_id: ContentReclaimClockCoordinatorId,
    pub(in crate::content::reclaim) evidence_digest: ContentReclaimClockEvidenceDigest,
    pub(in crate::content::reclaim) grace_started_at: crate::ReadVersion,
    pub(in crate::content::reclaim) observed_at_unix_ms: u64,
}

/// Durable physical-reclamation progress for one exact content identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReclaimSweep {
    pub(in crate::content::reclaim) storage_domain_id: StorageDomainId,
    pub(in crate::content::reclaim) content_id: ContentId,
    pub(in crate::content::reclaim) backend: ContentReclaimSweepBackend,
    pub(in crate::content::reclaim) prepared_at: crate::ReadVersion,
    pub(in crate::content::reclaim) reclaimed_at: Option<crate::ReadVersion>,
}

/// Backend evidence durably bound to a physical content sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentReclaimSweepBackend {
    /// Independently qualified native filesystem deletion.
    NativeFilesystem,
    /// Independently qualified WASI preopened-filesystem deletion.
    WasiFilesystem,
    /// Independently qualified browser origin-private filesystem deletion.
    BrowserStorage,
    /// Qualified unversioned object-store deletion.
    ObjectStore {
        /// Digest of the external provider evidence used for qualification.
        evidence_digest: ObjectStoreReclamationEvidenceDigest,
    },
}

impl ContentReclaimSweep {
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

    /// Returns the backend and provider evidence fixed at Prepared.
    #[must_use]
    pub const fn backend(self) -> ContentReclaimSweepBackend {
        self.backend
    }

    /// Returns the commit sequence that established the irreversible fence.
    #[must_use]
    pub const fn prepared_at(self) -> crate::ReadVersion {
        self.prepared_at
    }

    /// Returns the durable completion sequence, or `None` while deletion must
    /// still be resumed from the stored manifest.
    #[must_use]
    pub const fn reclaimed_at(self) -> Option<crate::ReadVersion> {
        self.reclaimed_at
    }
}

impl ContentReclaimClockAttestation {
    /// Binds trusted external evidence to one durable grace record.
    ///
    /// `observed_at_unix_ms` is audit data supplied by the trusted caller. It
    /// must be at or after the grace record's scheduling deadline, but that
    /// comparison alone is not evidence; the caller is responsible for the
    /// external monotonic/restart guarantee committed by `evidence_digest`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when the observation precedes the
    /// grace deadline.
    pub fn new(
        grace: ContentReclaimGrace,
        attestation_id: ContentReclaimClockAttestationId,
        coordinator_id: ContentReclaimClockCoordinatorId,
        evidence_digest: ContentReclaimClockEvidenceDigest,
        observed_at_unix_ms: u64,
    ) -> Result<Self> {
        if observed_at_unix_ms < grace.not_before_unix_ms() {
            return Err(Error::invalid_options(
                "content reclaim-clock observation precedes durable grace deadline",
            ));
        }
        Ok(Self {
            storage_domain_id: grace.storage_domain_id(),
            content_id: grace.content_id(),
            attestation_id,
            coordinator_id,
            evidence_digest,
            grace_started_at: grace.started_at(),
            observed_at_unix_ms,
        })
    }

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

    /// Returns the caller-retained attestation identity.
    #[must_use]
    pub const fn attestation_id(self) -> ContentReclaimClockAttestationId {
        self.attestation_id
    }

    /// Returns the opaque coordinator identity.
    #[must_use]
    pub const fn coordinator_id(self) -> ContentReclaimClockCoordinatorId {
        self.coordinator_id
    }

    /// Returns the digest of externally retained evidence.
    #[must_use]
    pub const fn evidence_digest(self) -> ContentReclaimClockEvidenceDigest {
        self.evidence_digest
    }

    /// Returns the exact durable grace-start coordinate.
    #[must_use]
    pub const fn grace_started_at(self) -> crate::ReadVersion {
        self.grace_started_at
    }

    /// Returns the trusted caller's Unix-millisecond audit observation.
    #[must_use]
    pub const fn observed_at_unix_ms(self) -> u64 {
        self.observed_at_unix_ms
    }
}
