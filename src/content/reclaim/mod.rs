use super::{
    Arc, CONTENT_ACCESS_BARRIER_ID_VERSION, CONTENT_ACCESS_BARRIER_MAGIC,
    CONTENT_ACCESS_COORDINATE_MAGIC, CONTENT_CONTROL_ACTIVE, CONTENT_CONTROL_MAGIC,
    CONTENT_CONTROL_RECLAIM_INTENT, CONTENT_QUARANTINE_MAGIC,
    CONTENT_READER_DRAIN_ATTESTATION_ID_VERSION, CONTENT_READER_DRAIN_ATTESTATION_MAGIC,
    CONTENT_READER_DRAIN_EVIDENCE_DOMAIN, CONTENT_READER_DRAIN_EVIDENCE_SHA256_TAG,
    CONTENT_RECLAIM_CLOCK_ATTESTATION_ID_VERSION, CONTENT_RECLAIM_CLOCK_EVIDENCE_DOMAIN,
    CONTENT_RECLAIM_CLOCK_EVIDENCE_SHA256_TAG, CONTENT_RECLAIM_GRACE_MAGIC,
    CONTENT_RECLAIM_PROOF_TOKEN_BYTES, CONTENT_RECLAIM_SWEEP_MAGIC, CONTENT_RECLAIM_SWEEP_PREPARED,
    CONTENT_RECLAIM_SWEEP_RECLAIMED, ContentDescriptor, ContentId, Digest, Error,
    ObjectStoreReclamationEvidenceDigest, Result, Sha256, StorageDomainId, UploadId, array_at,
    decode_content_id, fmt, write_hex,
};

/// Versioned identity of one irreversible leased-only content-access barrier.
///
/// The identity makes an interrupted barrier publication discoverable and
/// idempotent. It is scoped by the [`StorageDomainId`] passed to
/// [`Db::enforce_content_leased_only`](crate::Db::enforce_content_leased_only),
/// rather than being a database-wide authorization identity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentAccessBarrierId([u8; 16]);

impl ContentAccessBarrierId {
    /// Generates a new identity from operating-system entropy.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when secure entropy is unavailable.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::runtime_busy(format!("content access-barrier entropy: {error}"))
        })?;
        bytes[0] = CONTENT_ACCESS_BARRIER_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned portable identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when byte zero names an unknown
    /// identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_ACCESS_BARRIER_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content access-barrier identity version {}",
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

impl fmt::Debug for ContentAccessBarrierId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentAccessBarrierId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentAccessBarrierId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Persisted access policy for one physical storage domain.
///
/// `CompatibleUnleased` is the default for domains without a barrier.
/// `LeasedOnly` means every new ordinary
/// [`Db::open_content`](crate::Db::open_content) call fails with
/// [`Error::ContentLeaseRequired`]; callers must use
/// [`Db::open_content_leased`](crate::Db::open_content_leased). The transition
/// is irreversible because reverting it could race physical reclamation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContentAccessMode {
    /// Compatible mode in which ordinary unleased opens remain available.
    CompatibleUnleased,
    /// New content opens require durable read leases.
    LeasedOnly {
        /// Durable identity of the barrier that established this mode.
        barrier_id: ContentAccessBarrierId,
    },
}

/// Durable coordinate returned after leased-only access is fully recorded.
///
/// The backend barrier becomes visible before the protected commit coordinate
/// is published. Therefore this value proves that new unleased opens are
/// fenced and gives later lifecycle work a local ordering point. It does not
/// prove that handles opened before the barrier have drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentAccessBarrier {
    storage_domain_id: StorageDomainId,
    barrier_id: ContentAccessBarrierId,
    enforced_at: crate::ReadVersion,
}

impl ContentAccessBarrier {
    pub(crate) const fn new(
        storage_domain_id: StorageDomainId,
        barrier_id: ContentAccessBarrierId,
        enforced_at: crate::ReadVersion,
    ) -> Self {
        Self {
            storage_domain_id,
            barrier_id,
            enforced_at,
        }
    }

    /// Returns the physical lifecycle domain fenced by this barrier.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the durable barrier identity.
    #[must_use]
    pub const fn barrier_id(self) -> ContentAccessBarrierId {
        self.barrier_id
    }

    /// Returns the local commit sequence that recorded the barrier coordinate.
    ///
    /// This sequence is useful for ordering work in the same database instance;
    /// it is not a portable identity and does not prove reader drain.
    #[must_use]
    pub const fn enforced_at(self) -> crate::ReadVersion {
        self.enforced_at
    }
}

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
    const fn tag(self) -> u8 {
        match self {
            Self::DomainBootstrap => 0,
            Self::NativeProcessSetRestarted => 1,
            Self::RemoteCredentialEpochRetired => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
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
    storage_domain_id: StorageDomainId,
    barrier_id: ContentAccessBarrierId,
    attestation_id: ContentReaderDrainAttestationId,
    options: ContentReaderDrainAttestationOptions,
    barrier_enforced_at: crate::ReadVersion,
    attested_at: crate::ReadVersion,
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

/// Durable, exact-content quarantine coordinate.
///
/// The record binds one accepted reclaim intent, the leased-only barrier, and
/// the trusted reader-drain attestation used by the transition. Quarantine
/// prevents new leased opens but retains the descriptor and all content bytes.
/// Attachment authority or a physical hold may atomically return the content to
/// Active state before any future deletion protocol begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentQuarantine {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    proof_token: ContentReclaimProofToken,
    verified_at: crate::ReadVersion,
    proof_expires_at_unix_ms: u64,
    intent_accepted_at: crate::ReadVersion,
    barrier_id: ContentAccessBarrierId,
    barrier_enforced_at: crate::ReadVersion,
    drain_attestation_id: ContentReaderDrainAttestationId,
    quarantined_at: crate::ReadVersion,
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

/// Durable scheduling boundary retained while exact content stays quarantined.
///
/// `not_before_unix_ms` is derived from the host wall clock when this record is
/// committed. Reaching it does not prove elapsed real time across clock jumps or
/// restarts and never authorizes deletion. A future delete protocol must obtain
/// separate clock, provider, replica, and fresh lifecycle evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReclaimGrace {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    proof_token: ContentReclaimProofToken,
    quarantined_at: crate::ReadVersion,
    requested_duration_ms: u64,
    observed_at_unix_ms: u64,
    not_before_unix_ms: u64,
    started_at: crate::ReadVersion,
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
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    attestation_id: ContentReclaimClockAttestationId,
    coordinator_id: ContentReclaimClockCoordinatorId,
    evidence_digest: ContentReclaimClockEvidenceDigest,
    grace_started_at: crate::ReadVersion,
    observed_at_unix_ms: u64,
}

/// Durable physical-reclamation progress for one exact content identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentReclaimSweep {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    backend: ContentReclaimSweepBackend,
    prepared_at: crate::ReadVersion,
    reclaimed_at: Option<crate::ReadVersion>,
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

mod codec;
pub(crate) use codec::*;
