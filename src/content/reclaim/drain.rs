use super::{
    CONTENT_READER_DRAIN_ATTESTATION_ID_VERSION, CONTENT_READER_DRAIN_EVIDENCE_DOMAIN,
    CONTENT_READER_DRAIN_EVIDENCE_SHA256_TAG, ContentAccessBarrierId, Digest, Error, Result,
    Sha256, StorageDomainId, fmt, write_hex,
};

/// Versioned identity of one deployment-coordinator reader-drain attestation.
///
/// The identity makes a commit-before-response retry exact. It does not prove
/// the deployment claim by itself; the caller must retain the evidence named by
/// [`ContentReaderDrainEvidenceDigest`] and must not attest until every reader
/// that could have opened before the leased-only barrier has ended.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentReaderDrainAttestationId([u8; 16]);

impl ContentReaderDrainAttestationId {
    /// Generates a new identity from operating-system entropy.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when secure entropy is unavailable.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::runtime_busy(format!("content reader-drain attestation entropy: {error}"))
        })?;
        bytes[0] = CONTENT_READER_DRAIN_ATTESTATION_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned portable identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when byte zero names an unknown
    /// identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_READER_DRAIN_ATTESTATION_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content reader-drain attestation identity version {}",
                    bytes[0]
                ),
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the versioned portable identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentReaderDrainAttestationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentReaderDrainAttestationId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentReaderDrainAttestationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Opaque identity of the coordinator that verified reader drain.
///
/// Trine KV persists and compares these bytes but does not interpret a process,
/// service, credential issuer, tenant, Principal, or authorization model from
/// them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentReaderDrainCoordinatorId([u8; 16]);

impl ContentReaderDrainCoordinatorId {
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

impl fmt::Debug for ContentReaderDrainCoordinatorId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentReaderDrainCoordinatorId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Algorithm-tagged digest of deployment evidence retained outside Trine KV.
///
/// V1 uses SHA-256 over a domain separator and caller-supplied canonical
/// evidence bytes. The digest is an audit commitment, not a signature and not
/// an independently verified proof.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentReaderDrainEvidenceDigest([u8; 33]);

impl ContentReaderDrainEvidenceDigest {
    /// Hashes canonical deployment evidence into the v1 portable digest.
    ///
    /// The caller should include the deployment identity, barrier identity,
    /// stopped process set or retired credential epoch, and observation time in
    /// a stable encoding. Trine KV stores only the resulting commitment.
    #[must_use]
    pub fn for_bytes(evidence: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(CONTENT_READER_DRAIN_EVIDENCE_DOMAIN);
        hasher.update(evidence);
        let mut bytes = [0_u8; 33];
        bytes[0] = CONTENT_READER_DRAIN_EVIDENCE_SHA256_TAG;
        bytes[1..].copy_from_slice(&hasher.finalize());
        Self(bytes)
    }

    /// Decodes an algorithm-tagged portable evidence digest.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] for an unknown algorithm tag.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        if bytes[0] != CONTENT_READER_DRAIN_EVIDENCE_SHA256_TAG {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content reader-drain evidence digest algorithm {}",
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

impl fmt::Debug for ContentReaderDrainEvidenceDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentReaderDrainEvidenceDigest(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Deployment condition used to establish that pre-barrier readers ended.
///
/// Every variant is an assertion by a trusted deployment coordinator. Trine KV
/// records the category but cannot observe process supervisors, credential
/// issuers, gateways, or direct object-store credentials itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContentReaderDrainKind {
    /// The barrier was coordinated before the domain admitted its first read.
    DomainBootstrap,
    /// Every native process capable of a pre-barrier read was stopped and the
    /// admitted process set restarted under the leased-only boundary.
    NativeProcessSetRestarted,
    /// All pre-barrier remote sessions ended and their credential epoch was
    /// expired or revoked before this attestation.
    RemoteCredentialEpochRetired,
}

impl ContentReaderDrainKind {
    pub(in crate::content::reclaim) const fn tag(self) -> u8 {
        match self {
            Self::DomainBootstrap => 0,
            Self::NativeProcessSetRestarted => 1,
            Self::RemoteCredentialEpochRetired => 2,
        }
    }

    pub(in crate::content::reclaim) fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::DomainBootstrap),
            1 => Ok(Self::NativeProcessSetRestarted),
            2 => Ok(Self::RemoteCredentialEpochRetired),
            _ => Err(Error::UnsupportedFormat {
                message: format!("unsupported content reader-drain kind {tag}"),
            }),
        }
    }
}

/// Caller-supplied audit claims for one reader-drain attestation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReaderDrainAttestationOptions {
    kind: ContentReaderDrainKind,
    coordinator_id: ContentReaderDrainCoordinatorId,
    evidence_digest: ContentReaderDrainEvidenceDigest,
}

impl ContentReaderDrainAttestationOptions {
    /// Creates exact audit claims for a trusted coordinator's attestation.
    #[must_use]
    pub const fn new(
        kind: ContentReaderDrainKind,
        coordinator_id: ContentReaderDrainCoordinatorId,
        evidence_digest: ContentReaderDrainEvidenceDigest,
    ) -> Self {
        Self {
            kind,
            coordinator_id,
            evidence_digest,
        }
    }

    /// Returns the deployment condition asserted by the coordinator.
    #[must_use]
    pub const fn kind(self) -> ContentReaderDrainKind {
        self.kind
    }

    /// Returns the opaque coordinator identity.
    #[must_use]
    pub const fn coordinator_id(self) -> ContentReaderDrainCoordinatorId {
        self.coordinator_id
    }

    /// Returns the digest of externally retained evidence.
    #[must_use]
    pub const fn evidence_digest(self) -> ContentReaderDrainEvidenceDigest {
        self.evidence_digest
    }
}

/// Durable record that a trusted deployment coordinator attested reader drain.
///
/// This record is permanently bound to one leased-only barrier. It proves only
/// that Trine KV durably recorded the supplied claim; it does not independently
/// establish that external processes, sessions, or credentials ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReaderDrainAttestation {
    pub(in crate::content::reclaim) storage_domain_id: StorageDomainId,
    pub(in crate::content::reclaim) barrier_id: ContentAccessBarrierId,
    pub(in crate::content::reclaim) attestation_id: ContentReaderDrainAttestationId,
    pub(in crate::content::reclaim) options: ContentReaderDrainAttestationOptions,
    pub(in crate::content::reclaim) barrier_enforced_at: crate::ReadVersion,
    pub(in crate::content::reclaim) attested_at: crate::ReadVersion,
}

impl ContentReaderDrainAttestation {
    /// Returns the physical lifecycle domain covered by this attestation.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the irreversible leased-only barrier identity.
    #[must_use]
    pub const fn barrier_id(self) -> ContentAccessBarrierId {
        self.barrier_id
    }

    /// Returns the caller-retained idempotency identity.
    #[must_use]
    pub const fn attestation_id(self) -> ContentReaderDrainAttestationId {
        self.attestation_id
    }

    /// Returns the exact audit claims supplied by the coordinator.
    #[must_use]
    pub const fn options(self) -> ContentReaderDrainAttestationOptions {
        self.options
    }

    /// Returns the local commit coordinate that completed the barrier.
    #[must_use]
    pub const fn barrier_enforced_at(self) -> crate::ReadVersion {
        self.barrier_enforced_at
    }

    /// Returns the local commit coordinate that recorded the attestation.
    #[must_use]
    pub const fn attested_at(self) -> crate::ReadVersion {
        self.attested_at
    }
}
