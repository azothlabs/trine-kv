//! Public coordinates and authority values for physical content reclamation.
//!
//! The protocol advances through explicit access, drain, authorization,
//! quarantine, grace, and sweep states. Each state owns its public values while
//! durable record encoding remains an internal child module.

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

mod access;
mod authorization;
mod codec;
mod drain;
mod grace;
mod quarantine;
mod sweep;

pub use access::{ContentAccessBarrier, ContentAccessBarrierId, ContentAccessMode};
pub use authorization::{
    ContentQuarantineStage, ContentReclaimAuthorization, ContentReclaimGraceStage,
    ContentReclaimIntentStage, ContentReclaimProofToken, ContentReclaimSweepStage,
};
pub use drain::{
    ContentReaderDrainAttestation, ContentReaderDrainAttestationId,
    ContentReaderDrainAttestationOptions, ContentReaderDrainCoordinatorId,
    ContentReaderDrainEvidenceDigest, ContentReaderDrainKind,
};
pub use grace::ContentReclaimGrace;
pub use quarantine::ContentQuarantine;
pub use sweep::{
    ContentReclaimClockAttestation, ContentReclaimClockAttestationId,
    ContentReclaimClockCoordinatorId, ContentReclaimClockEvidenceDigest, ContentReclaimSweep,
    ContentReclaimSweepBackend,
};

pub(crate) use codec::*;
