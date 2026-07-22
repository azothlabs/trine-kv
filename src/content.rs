//! Immutable, cryptographically identified content objects.
//!
//! This module is the storage-layer primitive used by higher layers that need
//! files or other large byte sequences. Content is accepted incrementally,
//! sealed by publishing a fixed-size descriptor after all chunks are durable,
//! and read through verified ranges or a sequential stream. Ordinary key/value
//! values do not enter this path automatically.

use std::{
    fmt, mem,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use sha2::{Digest, Sha256};

use crate::{Db, DurabilityMode, Error, Result};

const CONTENT_ID_SHA256_TAG: u8 = 1;
const UPLOAD_TOKEN_VERSION: u8 = 1;
const CONTENT_LEASE_ID_VERSION: u8 = 1;
const CONTENT_ACCESS_BARRIER_ID_VERSION: u8 = 1;
const CONTENT_READER_DRAIN_ATTESTATION_ID_VERSION: u8 = 1;
const CONTENT_READER_DRAIN_EVIDENCE_SHA256_TAG: u8 = 1;
const CONTENT_RECLAIM_CLOCK_ATTESTATION_ID_VERSION: u8 = 1;
const CONTENT_RECLAIM_CLOCK_EVIDENCE_SHA256_TAG: u8 = 1;
const CONTENT_PHYSICAL_HOLD_ID_VERSION: u8 = 1;
const DESCRIPTOR_MAGIC: &[u8; 8] = b"TRNCNTD2";
const CHUNK_MAGIC: &[u8; 8] = b"TRNCNTC1";
const UPLOAD_STATE_MAGIC: &[u8; 8] = b"TRNUPLD2";
const DESCRIPTOR_LEN: usize = 8 + 16 + 1 + 32 + 16 + 8 + 4 + 8;
const CHUNK_HEADER_LEN: usize = 8 + 16 + 8 + 4 + 32;
const UPLOAD_STATE_LEN: usize =
    8 + 1 + 16 + 8 + 4 + 8 + 8 + 4 + 1 + 8 + 1 + 33 + 16 + 16 + 32 + 8 + 8 + 1 + 33;
const UPLOAD_STATE_OPEN: u8 = 0;
const UPLOAD_STATE_SEALING: u8 = 1;
const UPLOAD_STATE_SEALED: u8 = 2;
const MIN_CHUNK_BYTES: usize = 64 * 1024;
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const CONTENT_LEASE_BUCKET: &str = "\u{1}trine-content-lease\u{1}";
pub(crate) const CONTENT_LEASE_MAGIC: &[u8; 8] = b"TRNCNLS1";
pub(crate) const CONTENT_PHYSICAL_HOLD_BUCKET: &str = "\u{1}trine-content-physical-hold\u{1}";
const CONTENT_PHYSICAL_HOLD_MAGIC: &[u8; 8] = b"TRNCPHL1";
pub(crate) const CONTENT_CONTROL_BUCKET: &str = "\u{1}trine-content-control\u{1}";
pub(crate) const CONTENT_TOKEN_INDEX_BUCKET: &str = "\u{1}trine-content-token-index\u{1}";
const CONTENT_CONTROL_MAGIC: &[u8; 8] = b"TRNCRCL1";
const CONTENT_TOKEN_INDEX_MAGIC: &[u8; 8] = b"TRNCTIX1";
const CONTENT_CONTROL_ACTIVE: u8 = 0;
const CONTENT_CONTROL_RECLAIM_INTENT: u8 = 1;
const CONTENT_RECLAIM_PROOF_TOKEN_BYTES: usize = 49;
const CONTENT_ACCESS_BARRIER_MAGIC: &[u8; 8] = b"TRNCABR1";
const CONTENT_ACCESS_COORDINATE_MAGIC: &[u8; 8] = b"TRNCACO1";
const CONTENT_READER_DRAIN_ATTESTATION_MAGIC: &[u8; 8] = b"TRNCRDA1";
const CONTENT_READER_DRAIN_EVIDENCE_DOMAIN: &[u8] = b"trine-content-reader-drain-evidence-v1";
const CONTENT_QUARANTINE_MAGIC: &[u8; 8] = b"TRNCQRT1";
const CONTENT_RECLAIM_GRACE_MAGIC: &[u8; 8] = b"TRNCRGR1";
const CONTENT_RECLAIM_SWEEP_MAGIC: &[u8; 8] = b"TRNCRSW1";
const CONTENT_RECLAIM_CLOCK_EVIDENCE_DOMAIN: &[u8] = b"trine-content-reclaim-clock-evidence-v1";
const CONTENT_RECLAIM_SWEEP_PREPARED: u8 = 0;
const CONTENT_RECLAIM_SWEEP_RECLAIMED: u8 = 1;

/// Opaque control-plane identity for one physical content boundary.
///
/// Deduplication, encryption, physical quota, and reclamation are scoped to
/// this identity. Trine KV compares and persists the bytes but does not parse
/// tenant or project semantics from them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageDomainId([u8; 16]);

impl StorageDomainId {
    /// Reconstructs an identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for StorageDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageDomainId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for StorageDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

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
/// `LeasedOnly` means every new ordinary [`Db::open_content`] call fails with
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
    prepared_at: crate::ReadVersion,
    reclaimed_at: Option<crate::ReadVersion>,
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

/// Opaque authenticated owner scope supplied by the database layer.
///
/// This identity can represent a project, tenant, or another authorization
/// boundary. Trine KV only requires exact equality when a token is consumed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerScopeId([u8; 16]);

impl OwnerScopeId {
    /// Reconstructs an identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for OwnerScopeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerScopeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Opaque higher-layer owner of one short-lived content read lease.
///
/// Trine KV persists and compares this identity but assigns it no Principal,
/// tenant, or Policy meaning. The caller must authorize before opening or
/// renewing a leased handle.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentLeaseOwnerId([u8; 16]);

impl ContentLeaseOwnerId {
    /// Reconstructs an opaque lease owner from portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the opaque owner bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentLeaseOwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentLeaseOwnerId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Generated identity of one durable short-lived content read lease.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentLeaseId([u8; 16]);

impl ContentLeaseId {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("content lease entropy: {error}")))?;
        bytes[0] = CONTENT_LEASE_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned lease identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the first byte carries an
    /// unknown identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_LEASE_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported content lease identity version {}", bytes[0]),
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

impl fmt::Debug for ContentLeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentLeaseId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentLeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Options for opening a sealed `ContentObject` under a short-lived read lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentLeaseOptions {
    owner_id: ContentLeaseOwnerId,
    ttl: Duration,
}

impl ContentLeaseOptions {
    /// Creates lease options for an already-authorized opaque owner.
    ///
    /// `ttl` is rounded down to whole milliseconds and must be at least one
    /// millisecond. The deadline is computed during leased-open acquisition,
    /// immediately before Trine KV stages the durable record.
    #[must_use]
    pub const fn new(owner_id: ContentLeaseOwnerId, ttl: Duration) -> Self {
        Self { owner_id, ttl }
    }

    /// Returns the opaque higher-layer owner identity.
    #[must_use]
    pub const fn owner_id(self) -> ContentLeaseOwnerId {
        self.owner_id
    }

    /// Returns the requested lease lifetime.
    #[must_use]
    pub const fn ttl(self) -> Duration {
        self.ttl
    }

    pub(crate) fn ttl_ms(self) -> Result<u64> {
        let millis = u64::try_from(self.ttl.as_millis()).map_err(|_| {
            Error::invalid_options("content lease lifetime milliseconds exceed u64::MAX")
        })?;
        if millis == 0 {
            return Err(Error::invalid_options(
                "content lease lifetime must be at least one millisecond",
            ));
        }
        Ok(millis)
    }
}

/// Classifies why physical content bytes must remain available.
///
/// The class is operational metadata, not authorization. Every class enters
/// the same reclaim fence; callers must not infer weaker protection from one
/// variant than another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ContentPhysicalHoldKind {
    /// A storage migration still reads or copies a representation.
    Migration,
    /// A backup workflow has not durably completed its copy.
    Backup,
    /// A repair workflow needs the current bytes or replicas.
    Repair,
    /// A storage provider operation still references provider-side objects.
    Provider,
    /// An explicit operator or compliance hold remains in force.
    Administrative,
    /// Durable asynchronous processing still depends on the source bytes.
    Processing,
}

impl ContentPhysicalHoldKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Migration => 1,
            Self::Backup => 2,
            Self::Repair => 3,
            Self::Provider => 4,
            Self::Administrative => 5,
            Self::Processing => 6,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(Self::Migration),
            2 => Ok(Self::Backup),
            3 => Ok(Self::Repair),
            4 => Ok(Self::Provider),
            5 => Ok(Self::Administrative),
            6 => Ok(Self::Processing),
            _ => Err(Error::UnsupportedFormat {
                message: format!("unsupported content physical-hold kind {tag}"),
            }),
        }
    }
}

impl fmt::Display for ContentPhysicalHoldKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Migration => "migration",
            Self::Backup => "backup",
            Self::Repair => "repair",
            Self::Provider => "provider",
            Self::Administrative => "administrative",
            Self::Processing => "processing",
        })
    }
}

/// Generated identity of one durable physical content hold.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentPhysicalHoldId([u8; 16]);

impl ContentPhysicalHoldId {
    /// Generates a new versioned hold identity from operating-system entropy.
    ///
    /// Callers should durably retain this identity before acquiring an
    /// until-released hold. Passing the same identity to acquisition after a
    /// lost response makes the operation idempotent and recoverable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when secure entropy is unavailable.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::runtime_busy(format!("content physical-hold entropy: {error}"))
        })?;
        bytes[0] = CONTENT_PHYSICAL_HOLD_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned portable hold identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when byte zero names an unknown
    /// identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_PHYSICAL_HOLD_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content physical-hold identity version {}",
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

impl fmt::Debug for ContentPhysicalHoldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentPhysicalHoldId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentPhysicalHoldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Opaque higher-layer owner of a physical content hold.
///
/// Trine KV compares this identity during resume, renewal, and release but does
/// not interpret it as a Principal, tenant, workflow, or Policy identity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentPhysicalHoldOwnerId([u8; 16]);

impl ContentPhysicalHoldOwnerId {
    /// Reconstructs an opaque owner from portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the opaque owner bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentPhysicalHoldOwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentPhysicalHoldOwnerId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentPhysicalHoldLifetime {
    Expiring(Duration),
    UntilReleased,
}

/// Options for acquiring one durable physical content hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentPhysicalHoldOptions {
    kind: ContentPhysicalHoldKind,
    owner_id: ContentPhysicalHoldOwnerId,
    lifetime: ContentPhysicalHoldLifetime,
}

impl ContentPhysicalHoldOptions {
    /// Creates an expiring hold for an already-authorized workflow.
    ///
    /// `ttl` is rounded down to whole milliseconds and must retain at least one
    /// millisecond. An expired hold is inert and cannot be renewed.
    #[must_use]
    pub const fn expiring(
        kind: ContentPhysicalHoldKind,
        owner_id: ContentPhysicalHoldOwnerId,
        ttl: Duration,
    ) -> Self {
        Self {
            kind,
            owner_id,
            lifetime: ContentPhysicalHoldLifetime::Expiring(ttl),
        }
    }

    /// Creates a hold that remains active until an explicit durable release.
    ///
    /// This form is appropriate only when the owner has a recovery path that
    /// can resume and release the hold after a process crash.
    #[must_use]
    pub const fn until_released(
        kind: ContentPhysicalHoldKind,
        owner_id: ContentPhysicalHoldOwnerId,
    ) -> Self {
        Self {
            kind,
            owner_id,
            lifetime: ContentPhysicalHoldLifetime::UntilReleased,
        }
    }

    /// Returns the operational hold class.
    #[must_use]
    pub const fn kind(self) -> ContentPhysicalHoldKind {
        self.kind
    }

    /// Returns the opaque higher-layer owner.
    #[must_use]
    pub const fn owner_id(self) -> ContentPhysicalHoldOwnerId {
        self.owner_id
    }

    /// Returns the requested expiring lifetime, or `None` for explicit release.
    #[must_use]
    pub const fn ttl(self) -> Option<Duration> {
        match self.lifetime {
            ContentPhysicalHoldLifetime::Expiring(ttl) => Some(ttl),
            ContentPhysicalHoldLifetime::UntilReleased => None,
        }
    }

    pub(crate) fn expires_at_unix_ms(self, now_unix_ms: u64) -> Result<u64> {
        let ContentPhysicalHoldLifetime::Expiring(ttl) = self.lifetime else {
            return Ok(0);
        };
        let ttl_ms = duration_millis(ttl, "content physical-hold lifetime")?;
        now_unix_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| Error::invalid_options("content physical-hold expiry overflow"))
    }
}

/// Scope that an attachment token is issued to and later verified against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentAttachmentScope {
    storage_domain_id: StorageDomainId,
    owner_scope_id: OwnerScopeId,
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

/// Opaque bearer authority returned only after an upload is sealed.
///
/// The 32 random bytes are secret. Logging implementations deliberately redact
/// them; callers that persist or transmit a token should protect it like any
/// other short-lived capability.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadToken([u8; 32]);

impl UploadToken {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload token entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn secret(self) -> [u8; 32] {
        self.0
    }

    /// Decodes the versioned 33-byte bearer representation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the first byte names an
    /// unknown token format version.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        if bytes[0] != UPLOAD_TOKEN_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported upload token version {}", bytes[0]),
            });
        }
        let mut secret = [0_u8; 32];
        secret.copy_from_slice(&bytes[1..]);
        Ok(Self(secret))
    }

    /// Returns the versioned 33-byte bearer representation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = UPLOAD_TOKEN_VERSION;
        bytes[1..].copy_from_slice(&self.0);
        bytes
    }
}

impl fmt::Debug for UploadToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UploadToken([REDACTED])")
    }
}

/// Opaque database-layer change identity used for idempotent consumption.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentChangeId([u8; 16]);

impl ContentChangeId {
    /// Reconstructs a change identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentChangeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentChangeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

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
    /// Decodes the portable 33-byte content identity.
    ///
    /// Byte zero selects the digest algorithm and the remaining 32 bytes carry
    /// its digest. Unknown algorithm tags fail closed so stored catalog values
    /// are never reinterpreted under a different hash scheme.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the algorithm tag is not
    /// supported by this build.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        let algorithm = ContentHashAlgorithm::from_tag(bytes[0])?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes[1..]);
        Ok(Self { algorithm, digest })
    }

    /// Returns the portable algorithm-tagged 33-byte content identity.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = self.algorithm.tag();
        bytes[1..].copy_from_slice(&self.digest);
        bytes
    }

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

    pub(crate) fn from_sha256(digest: [u8; 32]) -> Self {
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
    /// Creates a new random upload identity for caller-controlled idempotency.
    ///
    /// Generate and persist this identity before starting a transfer when an
    /// uncertain response must be retried through
    /// [`Db::begin_content_upload_with_id`](crate::Db::begin_content_upload_with_id).
    /// The identity is not secret and does not itself authorize content access.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when the operating system cannot provide
    /// cryptographic randomness. No durable state is created by this method.
    pub fn new() -> Result<Self> {
        Self::generate()
    }

    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload identity entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn bytes(self) -> [u8; 16] {
        self.0
    }

    /// Reconstructs an upload identity from its portable 16-byte form.
    ///
    /// Upload identities are not secrets. Persist this value when an upload
    /// must be resumed after the current process or database handle is gone.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable 16-byte representation.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    pub(crate) const fn lock_shard(self) -> usize {
        self.0[0] as usize
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
    attachment_scope: ContentAttachmentScope,
    token_ttl: Duration,
    chunk_bytes: usize,
    expected_length: Option<u64>,
    expected_content_id: Option<ContentId>,
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

/// Result of sealing one immutable content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedContent {
    attachment_scope: ContentAttachmentScope,
    content_id: ContentId,
    length: u64,
    upload_token: UploadToken,
    token_expires_at_unix_ms: u64,
    durability: DurabilityMode,
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
    upload_id: UploadId,
    scope: ContentAttachmentScope,
    content_id: ContentId,
    length: u64,
    token_expires_at_unix_ms: u64,
    durability: DurabilityMode,
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

pub(crate) const CONTENT_TOKEN_BUCKET: &str = "\u{1}trine-content-token\u{1}";
const TOKEN_RECORD_MAGIC: &[u8; 8] = b"TRNTOKN1";
const TOKEN_RECORD_LEN: usize = 8 + 32 + 16 + 16 + 16 + 33 + 8 + 8 + 1 + 1 + 16;
const TOKEN_AVAILABLE: u8 = 0;
const TOKEN_CONSUMED: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadTokenStatus {
    Available,
    Consumed(ContentChangeId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadTokenRecord {
    token_hash: [u8; 32],
    attachment: ContentAttachment,
    status: UploadTokenStatus,
}

impl UploadTokenRecord {
    pub(crate) fn available(upload_id: UploadId, sealed: SealedContent) -> Self {
        Self {
            token_hash: upload_token_hash(sealed.upload_token),
            attachment: ContentAttachment::new(
                upload_id,
                sealed.attachment_scope,
                sealed.content_id,
                sealed.length,
                sealed.token_expires_at_unix_ms,
                sealed.durability,
            ),
            status: UploadTokenStatus::Available,
        }
    }

    pub(crate) const fn attachment(self) -> ContentAttachment {
        self.attachment
    }

    pub(crate) const fn is_available(self) -> bool {
        matches!(self.status, UploadTokenStatus::Available)
    }

    pub(crate) fn consume(
        self,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
        now_unix_ms: u64,
    ) -> Result<Self> {
        if self.attachment.scope != expected_scope {
            return Err(Error::UploadTokenScopeMismatch);
        }
        match self.status {
            UploadTokenStatus::Consumed(existing) if existing == change_id => Ok(self),
            UploadTokenStatus::Consumed(_) => Err(Error::UploadTokenAlreadyConsumed),
            UploadTokenStatus::Available
                if now_unix_ms >= self.attachment.token_expires_at_unix_ms =>
            {
                Err(Error::UploadTokenExpired {
                    expired_at_unix_ms: self.attachment.token_expires_at_unix_ms,
                })
            }
            UploadTokenStatus::Available => Ok(Self {
                status: UploadTokenStatus::Consumed(change_id),
                ..self
            }),
        }
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(TOKEN_RECORD_LEN);
        bytes.extend_from_slice(TOKEN_RECORD_MAGIC);
        bytes.extend_from_slice(&self.token_hash);
        bytes.extend_from_slice(&self.attachment.upload_id.to_bytes());
        bytes.extend_from_slice(&self.attachment.scope.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.attachment.scope.owner_scope_id.to_bytes());
        self.attachment.content_id.encode_into(&mut bytes);
        bytes.extend_from_slice(&self.attachment.length.to_le_bytes());
        bytes.extend_from_slice(&self.attachment.token_expires_at_unix_ms.to_le_bytes());
        bytes.push(encode_durability(self.attachment.durability));
        match self.status {
            UploadTokenStatus::Available => {
                bytes.push(TOKEN_AVAILABLE);
                bytes.extend_from_slice(&[0_u8; 16]);
            }
            UploadTokenStatus::Consumed(change_id) => {
                bytes.push(TOKEN_CONSUMED);
                bytes.extend_from_slice(&change_id.to_bytes());
            }
        }
        debug_assert_eq!(bytes.len(), TOKEN_RECORD_LEN);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], token: UploadToken) -> Result<Self> {
        if bytes.len() != TOKEN_RECORD_LEN || bytes.get(..8) != Some(TOKEN_RECORD_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid upload token record header or length".to_owned(),
            });
        }
        let expected_hash = upload_token_hash(token);
        let token_hash = array_at::<32>(bytes, 8, "upload token hash")?;
        if token_hash != expected_hash {
            return Err(Error::InvalidFormat {
                message: "upload token record hash mismatch".to_owned(),
            });
        }
        let upload_id = UploadId(array_at::<16>(bytes, 40, "token upload id")?);
        let storage_domain_id = StorageDomainId(array_at::<16>(bytes, 56, "token storage domain")?);
        let owner_scope_id = OwnerScopeId(array_at::<16>(bytes, 72, "token owner scope")?);
        let content_id = decode_content_id(bytes, 88, "token content identity")?;
        let length = u64::from_le_bytes(array_at::<8>(bytes, 121, "token content length")?);
        let token_expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 129, "upload token expiry")?);
        let durability = decode_durability(bytes[137])?;
        let status = match bytes[138] {
            TOKEN_AVAILABLE => UploadTokenStatus::Available,
            TOKEN_CONSUMED => UploadTokenStatus::Consumed(ContentChangeId(array_at::<16>(
                bytes,
                139,
                "token consuming change",
            )?)),
            tag => {
                return Err(Error::UnsupportedFormat {
                    message: format!("unsupported upload token state tag {tag}"),
                });
            }
        };
        Ok(Self {
            token_hash,
            attachment: ContentAttachment::new(
                upload_id,
                ContentAttachmentScope::new(storage_domain_id, owner_scope_id),
                content_id,
                length,
                token_expires_at_unix_ms,
                durability,
            ),
            status,
        })
    }
}

pub(crate) fn upload_token_key(token: UploadToken) -> Vec<u8> {
    upload_token_hash(token).to_vec()
}

pub(crate) fn upload_token_hash(token: UploadToken) -> [u8; 32] {
    Sha256::digest(token.to_bytes()).into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentTokenIndexRecord {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    token_hash: [u8; 32],
    expires_at_unix_ms: u64,
}

impl ContentTokenIndexRecord {
    pub(crate) fn for_token(sealed: SealedContent) -> Self {
        Self {
            storage_domain_id: sealed.storage_domain_id(),
            content_id: sealed.content_id(),
            token_hash: upload_token_hash(sealed.upload_token()),
            expires_at_unix_ms: sealed.token_expires_at_unix_ms(),
        }
    }

    pub(crate) const fn expires_at_unix_ms(self) -> u64 {
        self.expires_at_unix_ms
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 32 + 8;
        let mut bytes = Vec::with_capacity(RECORD_LEN);
        bytes.extend_from_slice(CONTENT_TOKEN_INDEX_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.token_hash);
        bytes.extend_from_slice(&self.expires_at_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        token_hash: [u8; 32],
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 32 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_TOKEN_INDEX_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content token-index record header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content token-index storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content token-index identity")?;
        let stored_hash = array_at::<32>(bytes, 57, "content token-index hash")?;
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 89, "content token-index expiry")?);
        if stored_domain != storage_domain_id
            || stored_content != content_id
            || stored_hash != token_hash
        {
            return Err(Error::Corruption {
                message: "content token-index record differs from its protected key".to_owned(),
            });
        }
        if expires_at_unix_ms == 0 {
            return Err(Error::Corruption {
                message: "content token-index expiry cannot be zero".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            token_hash,
            expires_at_unix_ms,
        })
    }
}

pub(crate) fn content_token_index_prefix(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    content_control_key(storage_domain_id, content_id)
}

pub(crate) fn content_token_index_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    token: UploadToken,
) -> Vec<u8> {
    let mut key = content_token_index_prefix(storage_domain_id, content_id);
    key.extend_from_slice(&upload_token_hash(token));
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentAccessBarrierRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
}

impl ContentAccessBarrierRecord {
    pub(crate) fn encode(self) -> Arc<[u8]> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16);
        bytes.extend_from_slice(CONTENT_ACCESS_BARRIER_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.into()
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_ACCESS_BARRIER_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content access-barrier header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content access-barrier storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content access-barrier identity",
        )?)?;
        if stored_domain != storage_domain_id {
            return Err(Error::Corruption {
                message: "content access-barrier record differs from its storage path".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentAccessCoordinateRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) enforced_at: crate::ReadVersion,
}

impl ContentAccessCoordinateRecord {
    pub(crate) fn commit_prefix(
        storage_domain_id: StorageDomainId,
        barrier_id: ContentAccessBarrierId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16);
        bytes.extend_from_slice(CONTENT_ACCESS_COORDINATE_MAGIC);
        bytes.extend_from_slice(&storage_domain_id.to_bytes());
        bytes.extend_from_slice(&barrier_id.to_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_ACCESS_COORDINATE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content access-coordinate header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content access-coordinate storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content access-coordinate barrier identity",
        )?)?;
        let enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            40,
            "content access-coordinate commit sequence",
        )?));
        if stored_domain != storage_domain_id || enforced_at.as_u64() == 0 {
            return Err(Error::Corruption {
                message: "content access-coordinate has invalid protected coordinates".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
            enforced_at,
        })
    }
}

pub(crate) fn content_access_coordinate_key(storage_domain_id: StorageDomainId) -> Vec<u8> {
    let mut key = Vec::with_capacity(7 + 16);
    key.extend_from_slice(b"access:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentReaderDrainAttestationRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) attestation_id: ContentReaderDrainAttestationId,
    pub(crate) options: ContentReaderDrainAttestationOptions,
    pub(crate) barrier_enforced_at: crate::ReadVersion,
    pub(crate) attested_at: crate::ReadVersion,
}

impl ContentReaderDrainAttestationRecord {
    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 16 + 16 + 1 + 16 + 33 + 8;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_READER_DRAIN_ATTESTATION_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.extend_from_slice(&self.attestation_id.to_bytes());
        bytes.push(self.options.kind().tag());
        bytes.extend_from_slice(&self.options.coordinator_id().to_bytes());
        bytes.extend_from_slice(&self.options.evidence_digest().to_bytes());
        bytes.extend_from_slice(&self.barrier_enforced_at.as_u64().to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 16 + 1 + 16 + 33 + 8 + 8;
        if bytes.len() != RECORD_LEN
            || bytes.get(..8) != Some(CONTENT_READER_DRAIN_ATTESTATION_MAGIC)
        {
            return Err(Error::InvalidFormat {
                message: "invalid content reader-drain attestation header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content reader-drain storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content reader-drain barrier identity",
        )?)?;
        let attestation_id = ContentReaderDrainAttestationId::from_bytes(array_at::<16>(
            bytes,
            40,
            "content reader-drain attestation identity",
        )?)?;
        let kind = ContentReaderDrainKind::from_tag(bytes[56])?;
        let coordinator_id = ContentReaderDrainCoordinatorId::from_bytes(array_at::<16>(
            bytes,
            57,
            "content reader-drain coordinator identity",
        )?);
        let evidence_digest = ContentReaderDrainEvidenceDigest::from_bytes(array_at::<33>(
            bytes,
            73,
            "content reader-drain evidence digest",
        )?)?;
        let barrier_enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            106,
            "content reader-drain barrier sequence",
        )?));
        let attested_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            114,
            "content reader-drain attestation sequence",
        )?));
        if stored_domain != storage_domain_id
            || barrier_enforced_at.as_u64() == 0
            || attested_at.as_u64() < barrier_enforced_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content reader-drain attestation has invalid protected coordinates"
                    .to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
            attestation_id,
            options: ContentReaderDrainAttestationOptions::new(
                kind,
                coordinator_id,
                evidence_digest,
            ),
            barrier_enforced_at,
            attested_at,
        })
    }

    pub(crate) fn matches_request(
        self,
        barrier: ContentAccessBarrier,
        attestation_id: ContentReaderDrainAttestationId,
        options: ContentReaderDrainAttestationOptions,
    ) -> bool {
        self.storage_domain_id == barrier.storage_domain_id()
            && self.barrier_id == barrier.barrier_id()
            && self.attestation_id == attestation_id
            && self.options == options
            && self.barrier_enforced_at == barrier.enforced_at()
    }

    pub(crate) const fn into_public(self) -> ContentReaderDrainAttestation {
        ContentReaderDrainAttestation {
            storage_domain_id: self.storage_domain_id,
            barrier_id: self.barrier_id,
            attestation_id: self.attestation_id,
            options: self.options,
            barrier_enforced_at: self.barrier_enforced_at,
            attested_at: self.attested_at,
        }
    }
}

pub(crate) fn content_reader_drain_attestation_key(storage_domain_id: StorageDomainId) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16);
    key.extend_from_slice(b"drain:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentQuarantineRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) proof_token: ContentReclaimProofToken,
    pub(crate) verified_at: crate::ReadVersion,
    pub(crate) proof_expires_at_unix_ms: u64,
    pub(crate) intent_accepted_at: crate::ReadVersion,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) barrier_enforced_at: crate::ReadVersion,
    pub(crate) drain_attestation_id: ContentReaderDrainAttestationId,
    pub(crate) quarantined_at: crate::ReadVersion,
}

impl ContentQuarantineRecord {
    pub(crate) fn requested(
        authorization: ContentReclaimAuthorization,
        intent_accepted_at: crate::ReadVersion,
        access: ContentAccessCoordinateRecord,
        drain: ContentReaderDrainAttestationRecord,
    ) -> Self {
        Self {
            storage_domain_id: authorization.storage_domain_id(),
            content_id: authorization.content_id(),
            proof_token: authorization.proof_token(),
            verified_at: authorization.verified_at(),
            proof_expires_at_unix_ms: authorization.expires_at_unix_ms(),
            intent_accepted_at,
            barrier_id: access.barrier_id,
            barrier_enforced_at: access.enforced_at,
            drain_attestation_id: drain.attestation_id,
            quarantined_at: crate::ReadVersion::from_u64(0),
        }
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 16 + 8 + 16;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_QUARANTINE_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.proof_token.to_bytes());
        bytes.extend_from_slice(&self.verified_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.proof_expires_at_unix_ms.to_le_bytes());
        bytes.extend_from_slice(&self.intent_accepted_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_enforced_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.drain_attestation_id.to_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 16 + 8 + 16 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_QUARANTINE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content quarantine header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content quarantine storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content quarantine identity")?;
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            57,
            "content quarantine proof token",
        )?);
        let verified_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            106,
            "content quarantine verified sequence",
        )?));
        let proof_expires_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            114,
            "content quarantine proof expiry",
        )?);
        let intent_accepted_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            122,
            "content quarantine intent sequence",
        )?));
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            130,
            "content quarantine barrier identity",
        )?)?;
        let barrier_enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            146,
            "content quarantine barrier sequence",
        )?));
        let drain_attestation_id = ContentReaderDrainAttestationId::from_bytes(array_at::<16>(
            bytes,
            154,
            "content quarantine drain attestation identity",
        )?)?;
        let quarantined_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            170,
            "content quarantine sequence",
        )?));
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content quarantine record differs from its protected key".to_owned(),
            });
        }
        if verified_at.as_u64() == 0
            || proof_expires_at_unix_ms == 0
            || intent_accepted_at.as_u64() == 0
            || barrier_enforced_at.as_u64() == 0
            || quarantined_at.as_u64() < intent_accepted_at.as_u64()
            || intent_accepted_at.as_u64() < barrier_enforced_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content quarantine has invalid protected coordinates".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            proof_token,
            verified_at,
            proof_expires_at_unix_ms,
            intent_accepted_at,
            barrier_id,
            barrier_enforced_at,
            drain_attestation_id,
            quarantined_at,
        })
    }

    pub(crate) fn matches_authorization(self, authorization: ContentReclaimAuthorization) -> bool {
        self.storage_domain_id == authorization.storage_domain_id()
            && self.content_id == authorization.content_id()
            && self.proof_token == authorization.proof_token()
            && self.verified_at == authorization.verified_at()
            && self.proof_expires_at_unix_ms == authorization.expires_at_unix_ms()
    }

    pub(crate) const fn into_public(self) -> ContentQuarantine {
        ContentQuarantine {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            proof_token: self.proof_token,
            verified_at: self.verified_at,
            proof_expires_at_unix_ms: self.proof_expires_at_unix_ms,
            intent_accepted_at: self.intent_accepted_at,
            barrier_id: self.barrier_id,
            barrier_enforced_at: self.barrier_enforced_at,
            drain_attestation_id: self.drain_attestation_id,
            quarantined_at: self.quarantined_at,
        }
    }
}

pub(crate) fn content_quarantine_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(11 + 16 + 33);
    key.extend_from_slice(b"quarantine:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentReclaimGraceRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) proof_token: ContentReclaimProofToken,
    pub(crate) quarantined_at: crate::ReadVersion,
    pub(crate) requested_duration_ms: u64,
    pub(crate) observed_at_unix_ms: u64,
    pub(crate) not_before_unix_ms: u64,
    pub(crate) started_at: crate::ReadVersion,
}

impl ContentReclaimGraceRecord {
    pub(crate) fn requested(
        quarantine: ContentQuarantineRecord,
        requested_duration_ms: u64,
        observed_at_unix_ms: u64,
        not_before_unix_ms: u64,
    ) -> Self {
        Self {
            storage_domain_id: quarantine.storage_domain_id,
            content_id: quarantine.content_id,
            proof_token: quarantine.proof_token,
            quarantined_at: quarantine.quarantined_at,
            requested_duration_ms,
            observed_at_unix_ms,
            not_before_unix_ms,
            started_at: crate::ReadVersion::from_u64(0),
        }
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 8;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_RECLAIM_GRACE_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.proof_token.to_bytes());
        bytes.extend_from_slice(&self.quarantined_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.requested_duration_ms.to_le_bytes());
        bytes.extend_from_slice(&self.observed_at_unix_ms.to_le_bytes());
        bytes.extend_from_slice(&self.not_before_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 49 + 8 + 8 + 8 + 8 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_RECLAIM_GRACE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content reclaim-grace header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content reclaim-grace storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content reclaim-grace identity")?;
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            57,
            "content reclaim-grace proof token",
        )?);
        let quarantined_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            106,
            "content reclaim-grace quarantine sequence",
        )?));
        let requested_duration_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 114, "content reclaim-grace duration")?);
        let observed_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            122,
            "content reclaim-grace clock observation",
        )?);
        let not_before_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            130,
            "content reclaim-grace not-before time",
        )?);
        let started_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            138,
            "content reclaim-grace commit sequence",
        )?));
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content reclaim-grace record differs from its protected key".to_owned(),
            });
        }
        if quarantined_at.as_u64() == 0
            || requested_duration_ms == 0
            || observed_at_unix_ms == 0
            || not_before_unix_ms
                != observed_at_unix_ms
                    .checked_add(requested_duration_ms)
                    .ok_or_else(|| Error::Corruption {
                        message: "content reclaim-grace deadline overflowed".to_owned(),
                    })?
            || started_at.as_u64() < quarantined_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content reclaim-grace has invalid protected coordinates".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            proof_token,
            quarantined_at,
            requested_duration_ms,
            observed_at_unix_ms,
            not_before_unix_ms,
            started_at,
        })
    }

    pub(crate) fn matches_quarantine(self, quarantine: ContentQuarantineRecord) -> bool {
        self.storage_domain_id == quarantine.storage_domain_id
            && self.content_id == quarantine.content_id
            && self.proof_token == quarantine.proof_token
            && self.quarantined_at == quarantine.quarantined_at
    }

    pub(crate) const fn into_public(self) -> ContentReclaimGrace {
        ContentReclaimGrace {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            proof_token: self.proof_token,
            quarantined_at: self.quarantined_at,
            requested_duration_ms: self.requested_duration_ms,
            observed_at_unix_ms: self.observed_at_unix_ms,
            not_before_unix_ms: self.not_before_unix_ms,
            started_at: self.started_at,
        }
    }
}

pub(crate) fn content_reclaim_grace_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16 + 33);
    key.extend_from_slice(b"grace:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentReclaimSweepRecordState {
    Prepared,
    Reclaimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentReclaimSweepRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) proof_token: ContentReclaimProofToken,
    pub(crate) verified_at: crate::ReadVersion,
    pub(crate) proof_expires_at_unix_ms: u64,
    pub(crate) quarantined_at: crate::ReadVersion,
    pub(crate) grace_started_at: crate::ReadVersion,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) barrier_enforced_at: crate::ReadVersion,
    pub(crate) drain_attestation_id: ContentReaderDrainAttestationId,
    pub(crate) clock_attestation: ContentReclaimClockAttestation,
    pub(crate) upload_id: UploadId,
    pub(crate) chunk_count: u64,
    pub(crate) state: ContentReclaimSweepRecordState,
    pub(crate) prepared_at: crate::ReadVersion,
    pub(crate) reclaimed_at: crate::ReadVersion,
}

impl ContentReclaimSweepRecord {
    pub(crate) fn prepared(
        authorization: ContentReclaimAuthorization,
        quarantine: ContentQuarantineRecord,
        grace: ContentReclaimGraceRecord,
        clock_attestation: ContentReclaimClockAttestation,
        descriptor: ContentDescriptor,
    ) -> Self {
        Self {
            storage_domain_id: authorization.storage_domain_id(),
            content_id: authorization.content_id(),
            proof_token: authorization.proof_token(),
            verified_at: authorization.verified_at(),
            proof_expires_at_unix_ms: authorization.expires_at_unix_ms(),
            quarantined_at: quarantine.quarantined_at,
            grace_started_at: grace.started_at,
            barrier_id: quarantine.barrier_id,
            barrier_enforced_at: quarantine.barrier_enforced_at,
            drain_attestation_id: quarantine.drain_attestation_id,
            clock_attestation,
            upload_id: descriptor.upload_id(),
            chunk_count: descriptor.chunk_count(),
            state: ContentReclaimSweepRecordState::Prepared,
            prepared_at: crate::ReadVersion::from_u64(0),
            reclaimed_at: crate::ReadVersion::from_u64(0),
        }
    }

    pub(crate) const fn reclaimed(self) -> Self {
        Self {
            state: ContentReclaimSweepRecordState::Reclaimed,
            reclaimed_at: crate::ReadVersion::from_u64(0),
            ..self
        }
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize =
            8 + 1 + 16 + 33 + 49 + 8 + 8 + 8 + 8 + 16 + 8 + 16 + 16 + 16 + 33 + 8 + 16 + 8 + 8;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_RECLAIM_SWEEP_MAGIC);
        bytes.push(match self.state {
            ContentReclaimSweepRecordState::Prepared => CONTENT_RECLAIM_SWEEP_PREPARED,
            ContentReclaimSweepRecordState::Reclaimed => CONTENT_RECLAIM_SWEEP_RECLAIMED,
        });
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.proof_token.to_bytes());
        bytes.extend_from_slice(&self.verified_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.proof_expires_at_unix_ms.to_le_bytes());
        bytes.extend_from_slice(&self.quarantined_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.grace_started_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_enforced_at.as_u64().to_be_bytes());
        bytes.extend_from_slice(&self.drain_attestation_id.to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.attestation_id().to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.coordinator_id().to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.evidence_digest().to_bytes());
        bytes.extend_from_slice(&self.clock_attestation.observed_at_unix_ms().to_le_bytes());
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        bytes.extend_from_slice(&self.prepared_at.as_u64().to_be_bytes());
        bytes
    }

    #[allow(clippy::too_many_lines)] // Fixed-width protected record decoding keeps offsets together.
    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize =
            8 + 1 + 16 + 33 + 49 + 8 + 8 + 8 + 8 + 16 + 8 + 16 + 16 + 16 + 33 + 8 + 16 + 8 + 8 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_RECLAIM_SWEEP_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content reclaim-sweep header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            9,
            "content reclaim-sweep storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 25, "content reclaim-sweep identity")?;
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content reclaim-sweep record differs from its protected key".to_owned(),
            });
        }
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            58,
            "content reclaim-sweep proof token",
        )?);
        let verified_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            107,
            "content reclaim-sweep verified sequence",
        )?));
        let proof_expires_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            115,
            "content reclaim-sweep proof expiry",
        )?);
        let quarantined_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            123,
            "content reclaim-sweep quarantine sequence",
        )?));
        let grace_started_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            131,
            "content reclaim-sweep grace sequence",
        )?));
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            139,
            "content reclaim-sweep barrier identity",
        )?)?;
        let barrier_enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            155,
            "content reclaim-sweep barrier sequence",
        )?));
        let drain_attestation_id = ContentReaderDrainAttestationId::from_bytes(array_at::<16>(
            bytes,
            163,
            "content reclaim-sweep drain identity",
        )?)?;
        let clock_attestation_id = ContentReclaimClockAttestationId::from_bytes(array_at::<16>(
            bytes,
            179,
            "content reclaim-sweep clock identity",
        )?)?;
        let clock_coordinator_id = ContentReclaimClockCoordinatorId::from_bytes(array_at::<16>(
            bytes,
            195,
            "content reclaim-sweep clock coordinator",
        )?);
        let clock_evidence_digest = ContentReclaimClockEvidenceDigest::from_bytes(array_at::<33>(
            bytes,
            211,
            "content reclaim-sweep clock evidence",
        )?)?;
        let clock_observed_at_unix_ms = u64::from_le_bytes(array_at::<8>(
            bytes,
            244,
            "content reclaim-sweep clock observation",
        )?);
        let upload_id = UploadId::from_bytes(array_at::<16>(
            bytes,
            252,
            "content reclaim-sweep upload identity",
        )?);
        let chunk_count = u64::from_le_bytes(array_at::<8>(
            bytes,
            268,
            "content reclaim-sweep chunk count",
        )?);
        let stored_prepared_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            276,
            "content reclaim-sweep prepared sequence",
        )?));
        let state_commit_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            284,
            "content reclaim-sweep state sequence",
        )?));
        let (state, prepared_at, reclaimed_at) = match bytes[8] {
            CONTENT_RECLAIM_SWEEP_PREPARED if stored_prepared_at.as_u64() == 0 => (
                ContentReclaimSweepRecordState::Prepared,
                state_commit_at,
                crate::ReadVersion::from_u64(0),
            ),
            CONTENT_RECLAIM_SWEEP_RECLAIMED
                if stored_prepared_at.as_u64() > 0
                    && state_commit_at.as_u64() >= stored_prepared_at.as_u64() =>
            {
                (
                    ContentReclaimSweepRecordState::Reclaimed,
                    stored_prepared_at,
                    state_commit_at,
                )
            }
            _ => {
                return Err(Error::Corruption {
                    message: "content reclaim-sweep has invalid state coordinates".to_owned(),
                });
            }
        };
        if verified_at.as_u64() < grace_started_at.as_u64()
            || proof_expires_at_unix_ms == 0
            || quarantined_at.as_u64() == 0
            || grace_started_at.as_u64() < quarantined_at.as_u64()
            || barrier_enforced_at.as_u64() == 0
            || clock_observed_at_unix_ms == 0
            || prepared_at.as_u64() < grace_started_at.as_u64()
        {
            return Err(Error::Corruption {
                message: "content reclaim-sweep has invalid protected coordinates".to_owned(),
            });
        }
        let clock_attestation = ContentReclaimClockAttestation {
            storage_domain_id,
            content_id,
            attestation_id: clock_attestation_id,
            coordinator_id: clock_coordinator_id,
            evidence_digest: clock_evidence_digest,
            grace_started_at,
            observed_at_unix_ms: clock_observed_at_unix_ms,
        };
        Ok(Self {
            storage_domain_id,
            content_id,
            proof_token,
            verified_at,
            proof_expires_at_unix_ms,
            quarantined_at,
            grace_started_at,
            barrier_id,
            barrier_enforced_at,
            drain_attestation_id,
            clock_attestation,
            upload_id,
            chunk_count,
            state,
            prepared_at,
            reclaimed_at,
        })
    }

    pub(crate) fn matches_request(
        self,
        authorization: ContentReclaimAuthorization,
        clock_attestation: ContentReclaimClockAttestation,
    ) -> bool {
        self.state == ContentReclaimSweepRecordState::Prepared
            && self.storage_domain_id == authorization.storage_domain_id()
            && self.content_id == authorization.content_id()
            && self.proof_token == authorization.proof_token()
            && self.verified_at == authorization.verified_at()
            && self.proof_expires_at_unix_ms == authorization.expires_at_unix_ms()
            && self.clock_attestation == clock_attestation
    }

    pub(crate) const fn into_public(self) -> ContentReclaimSweep {
        ContentReclaimSweep {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            prepared_at: self.prepared_at,
            reclaimed_at: match self.state {
                ContentReclaimSweepRecordState::Prepared => None,
                ContentReclaimSweepRecordState::Reclaimed => Some(self.reclaimed_at),
            },
        }
    }
}

pub(crate) fn content_reclaim_sweep_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16 + 33);
    key.extend_from_slice(b"sweep:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentControlState {
    Active,
    ReclaimIntent {
        proof_token: ContentReclaimProofToken,
        verified_at: crate::ReadVersion,
        expires_at_unix_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentControlRecord {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    prior_activity_commit_seq: u64,
    state: ContentControlState,
    state_commit_seq: u64,
}

impl ContentControlRecord {
    pub(crate) const fn active(storage_domain_id: StorageDomainId, content_id: ContentId) -> Self {
        Self {
            storage_domain_id,
            content_id,
            prior_activity_commit_seq: 0,
            state: ContentControlState::Active,
            state_commit_seq: 0,
        }
    }

    pub(crate) fn reclaim_intent(self, authorization: ContentReclaimAuthorization) -> Self {
        Self {
            storage_domain_id: self.storage_domain_id,
            content_id: self.content_id,
            prior_activity_commit_seq: self.physical_activity_commit_seq(),
            state: ContentControlState::ReclaimIntent {
                proof_token: authorization.proof_token(),
                verified_at: authorization.verified_at(),
                expires_at_unix_ms: authorization.expires_at_unix_ms(),
            },
            state_commit_seq: 0,
        }
    }

    pub(crate) const fn physical_activity_commit_seq(self) -> u64 {
        match self.state {
            ContentControlState::Active => self.state_commit_seq,
            ContentControlState::ReclaimIntent { .. } => self.prior_activity_commit_seq,
        }
    }

    pub(crate) fn matches_authorization(self, authorization: ContentReclaimAuthorization) -> bool {
        self.storage_domain_id == authorization.storage_domain_id()
            && self.content_id == authorization.content_id()
            && matches!(
                self.state,
                ContentControlState::ReclaimIntent {
                    proof_token,
                    verified_at,
                    expires_at_unix_ms,
                } if proof_token.to_bytes() == authorization.proof_token().to_bytes()
                    && verified_at.as_u64() == authorization.verified_at().as_u64()
                    && expires_at_unix_ms == authorization.expires_at_unix_ms()
            )
    }

    pub(crate) fn matches_quarantine(self, quarantine: ContentQuarantineRecord) -> bool {
        self.storage_domain_id == quarantine.storage_domain_id
            && self.content_id == quarantine.content_id
            && self.accepted_at() == Some(quarantine.intent_accepted_at)
            && matches!(
                self.state,
                ContentControlState::ReclaimIntent {
                    proof_token,
                    verified_at,
                    expires_at_unix_ms,
                } if proof_token == quarantine.proof_token
                    && verified_at == quarantine.verified_at
                    && expires_at_unix_ms == quarantine.proof_expires_at_unix_ms
            )
    }

    pub(crate) const fn accepted_at(self) -> Option<crate::ReadVersion> {
        match self.state {
            ContentControlState::Active => None,
            ContentControlState::ReclaimIntent { .. } => {
                Some(crate::ReadVersion::from_u64(self.state_commit_seq))
            }
        }
    }

    pub(crate) fn encode_prefix(self) -> Vec<u8> {
        const PREFIX_LEN: usize = 8 + 16 + 33 + 8 + 1 + 49 + 8 + 8;
        let mut bytes = Vec::with_capacity(PREFIX_LEN);
        bytes.extend_from_slice(CONTENT_CONTROL_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.prior_activity_commit_seq.to_be_bytes());
        match self.state {
            ContentControlState::Active => {
                bytes.push(CONTENT_CONTROL_ACTIVE);
                bytes.extend_from_slice(&[0_u8; CONTENT_RECLAIM_PROOF_TOKEN_BYTES]);
                bytes.extend_from_slice(&0_u64.to_be_bytes());
                bytes.extend_from_slice(&0_u64.to_le_bytes());
            }
            ContentControlState::ReclaimIntent {
                proof_token,
                verified_at,
                expires_at_unix_ms,
            } => {
                bytes.push(CONTENT_CONTROL_RECLAIM_INTENT);
                bytes.extend_from_slice(&proof_token.to_bytes());
                bytes.extend_from_slice(&verified_at.as_u64().to_be_bytes());
                bytes.extend_from_slice(&expires_at_unix_ms.to_le_bytes());
            }
        }
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 33 + 8 + 1 + 49 + 8 + 8 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_CONTROL_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content control record header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content control storage domain",
        )?);
        let stored_content = decode_content_id(bytes, 24, "content control identity")?;
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::Corruption {
                message: "content control record differs from its protected key".to_owned(),
            });
        }
        let prior_activity_commit_seq =
            u64::from_be_bytes(array_at::<8>(bytes, 57, "prior content activity")?);
        let proof_token = ContentReclaimProofToken::from_bytes(array_at::<49>(
            bytes,
            66,
            "content reclaim proof token",
        )?);
        let verified_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            115,
            "content reclaim verified sequence",
        )?));
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 123, "content reclaim expiry")?);
        let state_commit_seq =
            u64::from_be_bytes(array_at::<8>(bytes, 131, "content control state sequence")?);
        let state = match bytes[65] {
            CONTENT_CONTROL_ACTIVE
                if prior_activity_commit_seq == 0
                    && proof_token.to_bytes() == [0_u8; 49]
                    && verified_at.as_u64() == 0
                    && expires_at_unix_ms == 0 =>
            {
                ContentControlState::Active
            }
            CONTENT_CONTROL_RECLAIM_INTENT
                if prior_activity_commit_seq > 0
                    && verified_at.as_u64() >= prior_activity_commit_seq
                    && expires_at_unix_ms > 0 =>
            {
                ContentControlState::ReclaimIntent {
                    proof_token,
                    verified_at,
                    expires_at_unix_ms,
                }
            }
            _ => {
                return Err(Error::Corruption {
                    message: "content control record has invalid lifecycle coordinates".to_owned(),
                });
            }
        };
        if state_commit_seq == 0 || state_commit_seq < prior_activity_commit_seq {
            return Err(Error::Corruption {
                message: "content control state sequence is invalid".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            prior_activity_commit_seq,
            state,
            state_commit_seq,
        })
    }
}

pub(crate) fn content_control_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 33);
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

pub(crate) fn content_prefix_range(prefix: Vec<u8>) -> Result<crate::KeyRange> {
    let mut end = prefix.clone();
    let position = end
        .iter()
        .rposition(|byte| *byte != u8::MAX)
        .ok_or_else(|| Error::Corruption {
            message: "protected content prefix has no finite successor".to_owned(),
        })?;
    end[position] = end[position].saturating_add(1);
    end.truncate(position + 1);
    Ok(crate::KeyRange::half_open(prefix, end))
}

/// Result of resuming durable state for an [`UploadId`].
///
/// A successful seal is remembered by upload identity. Callers therefore get
/// either a writable open session or the exact prior seal result; they never
/// reopen a sealed upload as writable state.
#[derive(Debug)]
pub enum ContentUploadResume {
    /// The upload remains open and may accept bytes at [`ContentUpload::len`].
    Open(ContentUpload),
    /// The upload was already sealed; this is the idempotent prior result.
    Sealed(SealedContent),
}

impl ContentUploadResume {
    /// Returns the open upload, or `None` when it was already sealed.
    #[must_use]
    pub fn into_open(self) -> Option<ContentUpload> {
        match self {
            Self::Open(upload) => Some(upload),
            Self::Sealed(_) => None,
        }
    }

    /// Returns the prior seal result, or `None` while the upload remains open.
    #[must_use]
    pub const fn sealed(&self) -> Option<SealedContent> {
        match self {
            Self::Open(_) => None,
            Self::Sealed(sealed) => Some(*sealed),
        }
    }
}

/// In-progress sequential upload with memory bounded by its configured chunk.
///
/// Calls to [`write`](Self::write) may be any size and are split into fixed
/// chunks. `seal` consumes the upload and publishes the fixed-size descriptor
/// only after all chunks have been stored and the complete identity verified.
/// Dropping an unsealed upload never publishes content. Its durable
/// [`UploadId`] can be resumed or explicitly aborted later; cleanup of uploads
/// that are never resumed or aborted is a maintenance concern.
pub struct ContentUpload {
    db: Db,
    upload_id: UploadId,
    options: ContentUploadOptions,
    buffer: Vec<u8>,
    length: u64,
    complete_chunks: u64,
    revision: u64,
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
            .field("complete_chunks", &self.complete_chunks)
            .field("revision", &self.revision)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl ContentUpload {
    pub(crate) fn new(
        db: Db,
        upload_id: UploadId,
        options: ContentUploadOptions,
        buffer: Vec<u8>,
        length: u64,
        complete_chunks: u64,
        revision: u64,
    ) -> Self {
        Self {
            db,
            upload_id,
            options,
            buffer,
            length,
            complete_chunks,
            revision,
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
    /// Empty writes are no-ops. If a storage write fails, this in-memory writer
    /// becomes unusable because the failure may have happened between the chunk
    /// and session writes. Resume or abort its [`UploadId`] through [`Db`] to
    /// recover from the last durable session revision.
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

        let db = self.db.clone();
        let _upload = db.lock_content_upload(self.upload_id).await;
        let durable = db.require_upload_state(self.upload_id).await?;
        durable.require_open_revision(self.revision)?;

        let incoming = u64::try_from(bytes.len())
            .map_err(|_| Error::invalid_options("content write length exceeds u64"))?;
        self.length = self
            .length
            .checked_add(incoming)
            .ok_or_else(|| Error::invalid_options("content length overflow"))?;

        while !bytes.is_empty() {
            let available = self.options.chunk_bytes - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.options.chunk_bytes {
                if let Err(error) = self.flush_full_chunk().await {
                    self.failed = true;
                    return Err(error);
                }
            }
        }

        if !self.buffer.is_empty() {
            let frame = encode_chunk(self.upload_id, self.complete_chunks, &self.buffer)?;
            if let Err(error) = self
                .db
                .write_content_chunk(self.upload_id, self.complete_chunks, frame)
                .await
            {
                self.failed = true;
                return Err(error);
            }
        }
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?;
        let state = UploadSessionState::open(
            self.upload_id,
            next_revision,
            self.options,
            self.length,
            self.complete_chunks,
            u32::try_from(self.buffer.len())
                .map_err(|_| Error::invalid_options("content partial chunk exceeds u32"))?,
            durable.upload_token(),
        )?;
        if let Err(error) = self.db.write_upload_state(&state).await {
            self.failed = true;
            return Err(error);
        }
        self.revision = next_revision;
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
    /// Returns a typed length or digest mismatch and aborts the session when an
    /// expectation fails,
    /// [`Error::InvalidOptions`] when the upload previously failed, or a storage
    /// error if the final chunk or descriptor cannot be made durable. A failed
    /// descriptor write does not publish the `ContentObject`. If descriptor
    /// publication succeeds but the sealed session write fails, retry
    /// [`Db::seal_content_upload`] with the same [`UploadId`].
    pub async fn seal(self) -> Result<SealedContent> {
        self.ensure_active()?;
        self.db
            .seal_content_upload_at(self.upload_id, Some(self.revision))
            .await
    }

    /// Aborts the upload and makes every staging chunk unreachable.
    ///
    /// No descriptor is published. The durable session is deleted before
    /// best-effort chunk cleanup, so a crash cannot leave a resumable session
    /// that points at missing bytes.
    ///
    /// # Errors
    ///
    /// Returns a conflict if another writer advanced the session, or the
    /// backend error that prevented deletion of the durable session.
    pub async fn abort(self) -> Result<()> {
        self.ensure_active()?;
        self.db
            .abort_content_upload_at(self.upload_id, Some(self.revision))
            .await
    }

    async fn flush_full_chunk(&mut self) -> Result<()> {
        let payload = mem::replace(
            &mut self.buffer,
            Vec::with_capacity(self.options.chunk_bytes),
        );
        let frame = encode_chunk(self.upload_id, self.complete_chunks, &payload)?;
        self.db
            .write_content_chunk(self.upload_id, self.complete_chunks, frame)
            .await?;
        self.complete_chunks = self
            .complete_chunks
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content chunk count overflow"))?;
        Ok(())
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

#[derive(Debug)]
pub(crate) struct ContentLease {
    id: ContentLeaseId,
    owner_id: ContentLeaseOwnerId,
    expires_at_unix_ms: AtomicU64,
}

impl ContentLease {
    pub(crate) const fn new(
        id: ContentLeaseId,
        owner_id: ContentLeaseOwnerId,
        expires_at_unix_ms: u64,
    ) -> Self {
        Self {
            id,
            owner_id,
            expires_at_unix_ms: AtomicU64::new(expires_at_unix_ms),
        }
    }

    pub(crate) const fn id(&self) -> ContentLeaseId {
        self.id
    }

    pub(crate) const fn owner_id(&self) -> ContentLeaseOwnerId {
        self.owner_id
    }

    pub(crate) fn expires_at_unix_ms(&self) -> u64 {
        self.expires_at_unix_ms.load(Ordering::Acquire)
    }

    pub(crate) fn publish_expiry(&self, expires_at_unix_ms: u64) {
        self.expires_at_unix_ms
            .store(expires_at_unix_ms, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentLeaseRecord {
    pub(crate) lease_id: ContentLeaseId,
    pub(crate) owner_id: ContentLeaseOwnerId,
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) expires_at_unix_ms: u64,
}

impl ContentLeaseRecord {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16 + 16 + 33 + 8);
        bytes.extend_from_slice(CONTENT_LEASE_MAGIC);
        bytes.extend_from_slice(&self.lease_id.to_bytes());
        bytes.extend_from_slice(&self.owner_id.to_bytes());
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.expires_at_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        lease_id: ContentLeaseId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 16 + 33 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_LEASE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content lease record header or length".to_owned(),
            });
        }
        let stored_lease_id =
            ContentLeaseId::from_bytes(array_at::<16>(bytes, 8, "content lease identity")?)?;
        let owner_id =
            ContentLeaseOwnerId::from_bytes(array_at::<16>(bytes, 24, "content lease owner")?);
        let stored_domain =
            StorageDomainId::from_bytes(array_at::<16>(bytes, 40, "content lease storage domain")?);
        let stored_content = decode_content_id(bytes, 56, "content lease content identity")?;
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 89, "content lease expiry")?);
        if stored_lease_id != lease_id
            || stored_domain != storage_domain_id
            || stored_content != content_id
        {
            return Err(Error::Corruption {
                message: "content lease record differs from its protected key".to_owned(),
            });
        }
        Ok(Self {
            lease_id,
            owner_id,
            storage_domain_id,
            content_id,
            expires_at_unix_ms,
        })
    }
}

pub(crate) fn content_lease_prefix(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 33);
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

pub(crate) fn content_lease_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    lease_id: ContentLeaseId,
) -> Vec<u8> {
    let mut key = content_lease_prefix(storage_domain_id, content_id);
    key.extend_from_slice(&lease_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentPhysicalHoldRecordState {
    Active,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentPhysicalHoldRecord {
    pub(crate) hold_id: ContentPhysicalHoldId,
    pub(crate) owner_id: ContentPhysicalHoldOwnerId,
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) kind: ContentPhysicalHoldKind,
    pub(crate) expires_at_unix_ms: u64,
    pub(crate) state: ContentPhysicalHoldRecordState,
}

impl ContentPhysicalHoldRecord {
    pub(crate) const fn is_active_at(self, now_unix_ms: u64) -> bool {
        matches!(self.state, ContentPhysicalHoldRecordState::Active)
            && (self.expires_at_unix_ms == 0 || now_unix_ms < self.expires_at_unix_ms)
    }

    pub(crate) const fn is_released(self) -> bool {
        matches!(self.state, ContentPhysicalHoldRecordState::Released)
    }

    pub(crate) const fn released(mut self) -> Self {
        self.state = ContentPhysicalHoldRecordState::Released;
        self
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16 + 16 + 33 + 1 + 1 + 8);
        bytes.extend_from_slice(CONTENT_PHYSICAL_HOLD_MAGIC);
        bytes.extend_from_slice(&self.hold_id.to_bytes());
        bytes.extend_from_slice(&self.owner_id.to_bytes());
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.push(self.kind.tag());
        bytes.push(match self.state {
            ContentPhysicalHoldRecordState::Active => 0,
            ContentPhysicalHoldRecordState::Released => 1,
        });
        bytes.extend_from_slice(&self.expires_at_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        hold_id: ContentPhysicalHoldId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 16 + 33 + 1 + 1 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_PHYSICAL_HOLD_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content physical-hold record header or length".to_owned(),
            });
        }
        let stored_hold_id = ContentPhysicalHoldId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content physical-hold identity",
        )?)?;
        let owner_id = ContentPhysicalHoldOwnerId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content physical-hold owner",
        )?);
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            40,
            "content physical-hold storage domain",
        )?);
        let stored_content =
            decode_content_id(bytes, 56, "content physical-hold content identity")?;
        let kind = ContentPhysicalHoldKind::from_tag(bytes[89])?;
        let state = match bytes[90] {
            0 => ContentPhysicalHoldRecordState::Active,
            1 => ContentPhysicalHoldRecordState::Released,
            state => {
                return Err(Error::UnsupportedFormat {
                    message: format!("unsupported content physical-hold state {state}"),
                });
            }
        };
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 91, "content physical-hold expiry")?);
        if stored_hold_id != hold_id
            || stored_domain != storage_domain_id
            || stored_content != content_id
        {
            return Err(Error::Corruption {
                message: "content physical-hold record differs from its protected key".to_owned(),
            });
        }
        Ok(Self {
            hold_id,
            owner_id,
            storage_domain_id,
            content_id,
            kind,
            expires_at_unix_ms,
            state,
        })
    }
}

pub(crate) fn content_physical_hold_prefix(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    content_lease_prefix(storage_domain_id, content_id)
}

pub(crate) fn content_physical_hold_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    hold_id: ContentPhysicalHoldId,
) -> Vec<u8> {
    let mut key = content_physical_hold_prefix(storage_domain_id, content_id);
    key.extend_from_slice(&hold_id.to_bytes());
    key
}

pub(crate) fn current_epoch_millis() -> Result<u64> {
    crate::platform::now_unix_millis()
}

pub(crate) fn duration_millis(duration: Duration, name: &str) -> Result<u64> {
    let millis = u64::try_from(duration.as_millis())
        .map_err(|_| Error::invalid_options(format!("{name} milliseconds exceed u64::MAX")))?;
    if millis == 0 {
        return Err(Error::invalid_options(format!(
            "{name} must be at least one millisecond"
        )));
    }
    Ok(millis)
}

#[derive(Debug)]
struct ContentPhysicalHoldState {
    expires_at_unix_ms: AtomicU64,
    released: AtomicBool,
}

/// Durable reason that one exact content identity must remain physically readable.
///
/// Clones share local release and expiry state, while the protected database
/// record is the cross-process source of truth. Dropping the value performs no
/// asynchronous I/O. Expiring holds become inert at their deadline; an
/// until-released hold must be resumed and explicitly released after a crash.
#[derive(Clone)]
pub struct ContentPhysicalHold {
    db: Db,
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    hold_id: ContentPhysicalHoldId,
    owner_id: ContentPhysicalHoldOwnerId,
    kind: ContentPhysicalHoldKind,
    state: Arc<ContentPhysicalHoldState>,
}

impl ContentPhysicalHold {
    pub(crate) fn from_record(db: Db, record: ContentPhysicalHoldRecord) -> Self {
        Self {
            db,
            storage_domain_id: record.storage_domain_id,
            content_id: record.content_id,
            hold_id: record.hold_id,
            owner_id: record.owner_id,
            kind: record.kind,
            state: Arc::new(ContentPhysicalHoldState {
                expires_at_unix_ms: AtomicU64::new(record.expires_at_unix_ms),
                released: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the exact physical lifecycle domain.
    #[must_use]
    pub const fn storage_domain_id(&self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the immutable byte identity protected by this hold.
    #[must_use]
    pub const fn content_id(&self) -> ContentId {
        self.content_id
    }

    /// Returns the durable hold identity.
    #[must_use]
    pub const fn id(&self) -> ContentPhysicalHoldId {
        self.hold_id
    }

    /// Returns the operational hold class.
    #[must_use]
    pub const fn kind(&self) -> ContentPhysicalHoldKind {
        self.kind
    }

    /// Returns the current exclusive deadline, or `None` for explicit release.
    ///
    /// All clones observe a successful renewal only after its durable commit.
    #[must_use]
    pub fn expires_at_unix_ms(&self) -> Option<u64> {
        match self.state.expires_at_unix_ms.load(Ordering::Acquire) {
            0 => None,
            expires_at_unix_ms => Some(expires_at_unix_ms),
        }
    }

    /// Returns whether this process has observed a successful explicit release.
    ///
    /// This local convenience flag is not the cross-process source of truth.
    #[must_use]
    pub fn is_released(&self) -> bool {
        self.state.released.load(Ordering::Acquire)
    }

    /// Renews an unexpired expiring hold for `ttl` from the current time.
    ///
    /// The caller must repeat higher-layer authorization first. Renewal cannot
    /// revive expiry and does not convert an until-released hold.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentPhysicalHoldNotFound`] after release or missing
    /// durable state, [`Error::ContentPhysicalHoldExpired`] after the deadline,
    /// [`Error::InvalidOptions`] for an until-released hold or invalid `ttl`,
    /// [`Error::ContentPhysicalHoldOwnerMismatch`] for wrong authority,
    /// [`Error::ReadOnly`] when protected state cannot be written, or a
    /// storage/conflict error when renewal cannot publish.
    pub async fn renew(&self, ttl: Duration) -> Result<u64> {
        self.db.renew_content_physical_hold(self, ttl).await
    }

    /// Durably releases this exact hold idempotently.
    ///
    /// All clones observe release after the durable Released tombstone commits.
    /// A missing record is accepted as an already-completed retry for recovery
    /// compatibility. Drop does not call this method.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentPhysicalHoldOwnerMismatch`] if the durable owner
    /// differs, [`Error::ReadOnly`] when protected state cannot be updated, or
    /// a storage/conflict error if release cannot converge.
    pub async fn release(&self) -> Result<()> {
        self.db.release_content_physical_hold(self).await
    }

    pub(crate) const fn owner_id(&self) -> ContentPhysicalHoldOwnerId {
        self.owner_id
    }

    pub(crate) fn publish_expiry(&self, expires_at_unix_ms: u64) {
        self.state
            .expires_at_unix_ms
            .store(expires_at_unix_ms, Ordering::Release);
    }

    pub(crate) fn publish_released(&self) {
        self.state.released.store(true, Ordering::Release);
    }
}

impl fmt::Debug for ContentPhysicalHold {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentPhysicalHold")
            .field("storage_domain_id", &self.storage_domain_id)
            .field("content_id", &self.content_id)
            .field("hold_id", &self.hold_id)
            .field("kind", &self.kind)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms())
            .field("released", &self.is_released())
            .finish_non_exhaustive()
    }
}

/// Stable read handle for one sealed immutable `ContentObject`.
///
/// The handle fixes one descriptor when opened. This prototype has no physical
/// relocation, so the descriptor's upload identity and chunk boundaries remain
/// stable for the handle lifetime. A handle opened with
/// [`Db::open_content_leased`](crate::Db::open_content_leased) additionally
/// carries one durable short-lived read lease shared by all of its clones.
#[derive(Debug, Clone)]
pub struct ContentHandle {
    db: Db,
    descriptor: ContentDescriptor,
    lease: Option<Arc<ContentLease>>,
}

impl ContentHandle {
    pub(crate) fn new(db: Db, descriptor: ContentDescriptor) -> Self {
        Self {
            db,
            descriptor,
            lease: None,
        }
    }

    pub(crate) fn with_lease(mut self, lease: ContentLease) -> Self {
        self.lease = Some(Arc::new(lease));
        self
    }

    pub(crate) fn lease(&self) -> Option<&Arc<ContentLease>> {
        self.lease.as_ref()
    }

    /// Returns the immutable content identity fixed by this handle.
    #[must_use]
    pub const fn content_id(&self) -> ContentId {
        self.descriptor.content_id
    }

    /// Returns the storage domain fixed by this handle.
    #[must_use]
    pub const fn storage_domain_id(&self) -> StorageDomainId {
        self.descriptor.storage_domain_id
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

    /// Returns the durable read lease identity, when this handle was opened
    /// through [`Db::open_content_leased`](crate::Db::open_content_leased).
    ///
    /// Ordinary [`Db::open_content`](crate::Db::open_content) handles return
    /// `None` and are not protected from future physical reclamation.
    #[must_use]
    pub fn lease_id(&self) -> Option<ContentLeaseId> {
        self.lease.as_deref().map(ContentLease::id)
    }

    /// Returns the current lease deadline as Unix epoch milliseconds.
    ///
    /// All clones share this value. `None` identifies an unleased handle.
    /// Renewal publishes durable state before this local deadline advances.
    #[must_use]
    pub fn lease_expires_at_unix_ms(&self) -> Option<u64> {
        self.lease.as_deref().map(ContentLease::expires_at_unix_ms)
    }

    /// Renews this handle's durable read lease for `ttl` from the current time.
    ///
    /// The caller must repeat its higher-layer authorization before calling.
    /// Trine KV validates only the opaque owner and exact content identity. All
    /// handle clones observe the new deadline after durable publication.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentLeaseNotFound`] for an unleased handle or missing
    /// durable record, [`Error::ContentLeaseExpired`] once the old deadline is
    /// reached, [`Error::InvalidOptions`] for a sub-millisecond or overflowing
    /// lifetime, [`Error::ContentQuarantined`] if quarantine won the race after
    /// the lease expired, or a storage/conflict error if renewal cannot publish.
    pub async fn renew_lease(&self, ttl: Duration) -> Result<u64> {
        self.db.renew_content_lease(self, ttl).await
    }

    fn ensure_lease_active(&self) -> Result<()> {
        let Some(lease) = self.lease.as_deref() else {
            return Ok(());
        };
        let now = current_epoch_millis()?;
        let expires_at_unix_ms = lease.expires_at_unix_ms();
        if now >= expires_at_unix_ms {
            return Err(Error::ContentLeaseExpired {
                expired_at_unix_ms: expires_at_unix_ms,
            });
        }
        Ok(())
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
        self.ensure_lease_active()?;
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
            self.ensure_lease_active()?;
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
pub(crate) enum UploadSessionStatus {
    Open,
    Sealing(SealedContent),
    Sealed(SealedContent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadSessionState {
    upload_id: UploadId,
    revision: u64,
    options: ContentUploadOptions,
    length: u64,
    complete_chunks: u64,
    partial_len: u32,
    upload_token: UploadToken,
    status: UploadSessionStatus,
}

impl UploadSessionState {
    pub(crate) fn initial(
        upload_id: UploadId,
        options: ContentUploadOptions,
        upload_token: UploadToken,
    ) -> Result<Self> {
        Self::open(upload_id, 0, options, 0, 0, 0, upload_token)
    }

    pub(crate) fn open(
        upload_id: UploadId,
        revision: u64,
        options: ContentUploadOptions,
        length: u64,
        complete_chunks: u64,
        partial_len: u32,
        upload_token: UploadToken,
    ) -> Result<Self> {
        let state = Self {
            upload_id,
            revision,
            options,
            length,
            complete_chunks,
            partial_len,
            upload_token,
            status: UploadSessionStatus::Open,
        };
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn into_sealing(
        self,
        content_id: ContentId,
        token_expires_at_unix_ms: u64,
        durability: DurabilityMode,
    ) -> Result<Self> {
        let sealed = SealedContent {
            attachment_scope: self.options.attachment_scope,
            content_id,
            length: self.length,
            upload_token: self.upload_token,
            token_expires_at_unix_ms,
            durability,
        };
        Ok(Self {
            revision: self
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?,
            status: UploadSessionStatus::Sealing(sealed),
            ..self
        })
    }

    pub(crate) fn into_sealed(self) -> Result<Self> {
        let UploadSessionStatus::Sealing(sealed) = self.status else {
            return Err(Error::InvalidFormat {
                message: "content upload can become sealed only from sealing state".to_owned(),
            });
        };
        Ok(Self {
            revision: self
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?,
            status: UploadSessionStatus::Sealed(sealed),
            ..self
        })
    }

    pub(crate) const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    pub(crate) const fn revision(self) -> u64 {
        self.revision
    }

    pub(crate) const fn status(self) -> UploadSessionStatus {
        self.status
    }

    pub(crate) const fn options(self) -> ContentUploadOptions {
        self.options
    }

    pub(crate) const fn length(self) -> u64 {
        self.length
    }

    pub(crate) const fn complete_chunks(self) -> u64 {
        self.complete_chunks
    }

    pub(crate) const fn partial_len(self) -> u32 {
        self.partial_len
    }

    pub(crate) const fn upload_token(self) -> UploadToken {
        self.upload_token
    }

    pub(crate) const fn chunk_count(self) -> u64 {
        self.complete_chunks + if self.partial_len == 0 { 0 } else { 1 }
    }

    pub(crate) fn require_open_revision(self, expected_revision: u64) -> Result<()> {
        match self.status {
            UploadSessionStatus::Sealing(_) | UploadSessionStatus::Sealed(_) => {
                Err(Error::ContentUploadSealed {
                    upload_id: self.upload_id.to_string(),
                })
            }
            UploadSessionStatus::Open if self.revision == expected_revision => Ok(()),
            UploadSessionStatus::Open => Err(Error::ContentUploadConflict {
                upload_id: self.upload_id.to_string(),
                expected_revision,
                actual_revision: self.revision,
            }),
        }
    }

    pub(crate) fn encode(self) -> Result<Arc<[u8]>> {
        let mut bytes = Vec::with_capacity(UPLOAD_STATE_LEN);
        bytes.extend_from_slice(UPLOAD_STATE_MAGIC);
        bytes.push(match self.status {
            UploadSessionStatus::Open => UPLOAD_STATE_OPEN,
            UploadSessionStatus::Sealing(_) => UPLOAD_STATE_SEALING,
            UploadSessionStatus::Sealed(_) => UPLOAD_STATE_SEALED,
        });
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.revision.to_le_bytes());
        let chunk_bytes = u32::try_from(self.options.chunk_bytes)
            .map_err(|_| Error::invalid_options("content chunk size exceeds u32"))?;
        bytes.extend_from_slice(&chunk_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.complete_chunks.to_le_bytes());
        bytes.extend_from_slice(&self.partial_len.to_le_bytes());
        encode_optional_u64(&mut bytes, self.options.expected_length);
        encode_optional_content_id(&mut bytes, self.options.expected_content_id);
        bytes.extend_from_slice(&self.options.attachment_scope.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.options.attachment_scope.owner_scope_id.to_bytes());
        bytes.extend_from_slice(&self.upload_token.secret());
        bytes.extend_from_slice(&self.options.token_ttl_ms()?.to_le_bytes());
        match self.status {
            UploadSessionStatus::Open => {
                bytes.extend_from_slice(&0_u64.to_le_bytes());
                bytes.push(0);
                bytes.extend_from_slice(&[0_u8; 33]);
            }
            UploadSessionStatus::Sealing(sealed) | UploadSessionStatus::Sealed(sealed) => {
                bytes.extend_from_slice(&sealed.token_expires_at_unix_ms.to_le_bytes());
                bytes.push(encode_durability(sealed.durability));
                sealed.content_id.encode_into(&mut bytes);
            }
        }
        debug_assert_eq!(bytes.len(), UPLOAD_STATE_LEN);
        Ok(Arc::from(bytes))
    }

    pub(crate) fn decode(bytes: &[u8], expected_upload: UploadId) -> Result<Self> {
        if bytes.len() != UPLOAD_STATE_LEN || bytes.get(..8) != Some(UPLOAD_STATE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content upload state header or length".to_owned(),
            });
        }
        let status_tag = bytes[8];
        let upload_id = UploadId(array_at::<16>(bytes, 9, "content upload id")?);
        if upload_id != expected_upload {
            return Err(Error::InvalidFormat {
                message: format!("content upload identity mismatch for {expected_upload}"),
            });
        }
        let revision = u64::from_le_bytes(array_at::<8>(bytes, 25, "content upload revision")?);
        let chunk_bytes = usize::try_from(u32::from_le_bytes(array_at::<4>(
            bytes,
            33,
            "content upload chunk size",
        )?))
        .map_err(|_| Error::InvalidFormat {
            message: "content upload chunk size exceeds usize".to_owned(),
        })?;
        let length = u64::from_le_bytes(array_at::<8>(bytes, 37, "content upload length")?);
        let complete_chunks = u64::from_le_bytes(array_at::<8>(
            bytes,
            45,
            "content upload complete chunk count",
        )?);
        let partial_len =
            u32::from_le_bytes(array_at::<4>(bytes, 53, "content upload partial length")?);
        let expected_length = decode_optional_u64(bytes, 57, "content expected length")?;
        let expected_content_id =
            decode_optional_content_id(bytes, 66, "content expected identity")?;
        let storage_domain_id =
            StorageDomainId(array_at::<16>(bytes, 100, "content upload storage domain")?);
        let owner_scope_id =
            OwnerScopeId(array_at::<16>(bytes, 116, "content upload owner scope")?);
        let upload_token = UploadToken(array_at::<32>(bytes, 132, "content upload token")?);
        let token_ttl_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 164, "content upload token lifetime")?);
        let token_expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 172, "content upload token expiry")?);
        let durability = decode_durability(bytes[180])?;
        let sealed_id = match status_tag {
            UPLOAD_STATE_OPEN => None,
            UPLOAD_STATE_SEALING | UPLOAD_STATE_SEALED => {
                Some(decode_content_id(bytes, 181, "sealed content identity")?)
            }
            _ => {
                return Err(Error::UnsupportedFormat {
                    message: format!("unsupported content upload state tag {status_tag}"),
                });
            }
        };
        let options = ContentUploadOptions {
            attachment_scope: ContentAttachmentScope::new(storage_domain_id, owner_scope_id),
            token_ttl: Duration::from_millis(token_ttl_ms),
            chunk_bytes,
            expected_length,
            expected_content_id,
        }
        .validate()?;
        let status = sealed_id.map_or(UploadSessionStatus::Open, |content_id| {
            let sealed = SealedContent {
                attachment_scope: options.attachment_scope,
                content_id,
                length,
                upload_token,
                token_expires_at_unix_ms,
                durability,
            };
            if status_tag == UPLOAD_STATE_SEALING {
                UploadSessionStatus::Sealing(sealed)
            } else {
                UploadSessionStatus::Sealed(sealed)
            }
        });
        let state = Self {
            upload_id,
            revision,
            options,
            length,
            complete_chunks,
            partial_len,
            upload_token,
            status,
        };
        state.validate()?;
        Ok(state)
    }

    fn validate(self) -> Result<()> {
        let chunk_bytes =
            u64::try_from(self.options.chunk_bytes).map_err(|_| Error::InvalidFormat {
                message: "content upload chunk size exceeds u64".to_owned(),
            })?;
        if u64::from(self.partial_len) >= chunk_bytes && self.partial_len != 0 {
            return Err(Error::InvalidFormat {
                message: format!(
                    "content upload partial length {} is not below chunk size {chunk_bytes}",
                    self.partial_len
                ),
            });
        }
        let complete_bytes = self
            .complete_chunks
            .checked_mul(chunk_bytes)
            .ok_or_else(|| Error::InvalidFormat {
                message: "content upload complete length overflow".to_owned(),
            })?;
        let derived_length = complete_bytes
            .checked_add(u64::from(self.partial_len))
            .ok_or_else(|| Error::InvalidFormat {
                message: "content upload length overflow".to_owned(),
            })?;
        if derived_length != self.length {
            return Err(Error::InvalidFormat {
                message: format!(
                    "content upload length {} does not match durable chunks {derived_length}",
                    self.length
                ),
            });
        }
        match self.status {
            UploadSessionStatus::Open => {}
            UploadSessionStatus::Sealing(sealed) | UploadSessionStatus::Sealed(sealed) => {
                if sealed.length != self.length
                    || sealed.attachment_scope != self.options.attachment_scope
                    || sealed.upload_token != self.upload_token
                    || sealed.token_expires_at_unix_ms == 0
                {
                    return Err(Error::InvalidFormat {
                        message: "sealed content claims differ from upload session".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentDescriptor {
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    upload_id: UploadId,
    length: u64,
    chunk_bytes: u32,
    chunk_count: u64,
}

impl ContentDescriptor {
    pub(crate) fn new(
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        upload_id: UploadId,
        length: u64,
        chunk_bytes: usize,
        chunk_count: u64,
    ) -> Result<Self> {
        let chunk_bytes = u32::try_from(chunk_bytes)
            .map_err(|_| Error::invalid_options("content chunk size exceeds u32"))?;
        Ok(Self {
            storage_domain_id,
            content_id,
            upload_id,
            length,
            chunk_bytes,
            chunk_count,
        })
    }

    pub(crate) const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    pub(crate) const fn length(self) -> u64 {
        self.length
    }

    pub(crate) const fn chunk_count(self) -> u64 {
        self.chunk_count
    }

    pub(crate) fn encode(self) -> Arc<[u8]> {
        let mut bytes = Vec::with_capacity(DESCRIPTOR_LEN);
        bytes.extend_from_slice(DESCRIPTOR_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        self.content_id.encode_into(&mut bytes);
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        Arc::from(bytes)
    }

    pub(crate) fn decode(
        bytes: &[u8],
        expected_domain: StorageDomainId,
        expected_content: ContentId,
    ) -> Result<Self> {
        if bytes.len() != DESCRIPTOR_LEN || bytes.get(..8) != Some(DESCRIPTOR_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content descriptor header or length".to_owned(),
            });
        }
        let storage_domain_id = StorageDomainId(array_at::<16>(
            bytes,
            8,
            "content descriptor storage domain",
        )?);
        if storage_domain_id != expected_domain {
            return Err(Error::InvalidFormat {
                message: "content descriptor storage domain mismatch".to_owned(),
            });
        }
        let algorithm = ContentHashAlgorithm::from_tag(bytes[24])?;
        let digest = array_at::<32>(bytes, 25, "content descriptor digest")?;
        let content_id = ContentId { algorithm, digest };
        if content_id != expected_content {
            return Err(Error::ContentDigestMismatch {
                expected: expected_content.to_string(),
                actual: content_id.to_string(),
            });
        }
        let upload_id = UploadId(array_at::<16>(bytes, 57, "content descriptor upload id")?);
        let length = u64::from_le_bytes(array_at::<8>(bytes, 73, "content descriptor length")?);
        let chunk_bytes =
            u32::from_le_bytes(array_at::<4>(bytes, 81, "content descriptor chunk size")?);
        let chunk_count =
            u64::from_le_bytes(array_at::<8>(bytes, 85, "content descriptor chunk count")?);
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
            storage_domain_id,
            content_id,
            upload_id,
            length,
            chunk_bytes,
            chunk_count,
        })
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

pub(crate) fn decode_chunk(
    bytes: &[u8],
    expected_upload: UploadId,
    expected_index: u64,
) -> Result<&[u8]> {
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

fn encode_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.extend_from_slice(&[0_u8; 9]),
    }
}

fn decode_optional_u64(bytes: &[u8], offset: usize, field: &'static str) -> Result<Option<u64>> {
    match *bytes.get(offset).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} presence flag is truncated"),
    })? {
        0 => Ok(None),
        1 => Ok(Some(u64::from_le_bytes(array_at::<8>(
            bytes,
            offset + 1,
            field,
        )?))),
        tag => Err(Error::InvalidFormat {
            message: format!("{field} has invalid presence flag {tag}"),
        }),
    }
}

fn encode_optional_content_id(bytes: &mut Vec<u8>, value: Option<ContentId>) {
    match value {
        Some(value) => {
            bytes.push(1);
            value.encode_into(bytes);
        }
        None => bytes.extend_from_slice(&[0_u8; 34]),
    }
}

fn decode_optional_content_id(
    bytes: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<Option<ContentId>> {
    match *bytes.get(offset).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} presence flag is truncated"),
    })? {
        0 => Ok(None),
        1 => decode_content_id(bytes, offset + 1, field).map(Some),
        tag => Err(Error::InvalidFormat {
            message: format!("{field} has invalid presence flag {tag}"),
        }),
    }
}

pub(crate) const fn encode_durability(durability: DurabilityMode) -> u8 {
    match durability {
        DurabilityMode::Buffered => 0,
        DurabilityMode::Flush => 1,
        DurabilityMode::SyncData => 2,
        DurabilityMode::SyncAll => 3,
        DurabilityMode::SyncAllStrict => 4,
    }
}

pub(crate) fn decode_durability(tag: u8) -> Result<DurabilityMode> {
    match tag {
        0 => Ok(DurabilityMode::Buffered),
        1 => Ok(DurabilityMode::Flush),
        2 => Ok(DurabilityMode::SyncData),
        3 => Ok(DurabilityMode::SyncAll),
        4 => Ok(DurabilityMode::SyncAllStrict),
        _ => Err(Error::UnsupportedFormat {
            message: format!("unsupported content durability tag {tag}"),
        }),
    }
}

fn decode_content_id(bytes: &[u8], offset: usize, field: &'static str) -> Result<ContentId> {
    let tag = *bytes.get(offset).ok_or_else(|| Error::InvalidFormat {
        message: format!("{field} algorithm tag is truncated"),
    })?;
    let algorithm = ContentHashAlgorithm::from_tag(tag)?;
    let digest = array_at::<32>(bytes, offset + 1, field)?;
    Ok(ContentId { algorithm, digest })
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
