//! Durable records for the content-reclamation lifecycle.
//!
//! Each child module owns one state transition's record and key encoding. This
//! façade keeps the protocol's internal API stable without coupling unrelated
//! transition implementations into one source file.

use super::{
    Arc, CONTENT_ACCESS_BARRIER_MAGIC, CONTENT_ACCESS_COORDINATE_MAGIC, CONTENT_CONTROL_ACTIVE,
    CONTENT_CONTROL_MAGIC, CONTENT_CONTROL_RECLAIM_INTENT, CONTENT_QUARANTINE_MAGIC,
    CONTENT_READER_DRAIN_ATTESTATION_MAGIC, CONTENT_RECLAIM_GRACE_MAGIC,
    CONTENT_RECLAIM_PROOF_TOKEN_BYTES, CONTENT_RECLAIM_SWEEP_MAGIC, CONTENT_RECLAIM_SWEEP_PREPARED,
    CONTENT_RECLAIM_SWEEP_RECLAIMED, ContentAccessBarrier, ContentAccessBarrierId,
    ContentDescriptor, ContentId, ContentQuarantine, ContentReaderDrainAttestation,
    ContentReaderDrainAttestationId, ContentReaderDrainAttestationOptions,
    ContentReaderDrainCoordinatorId, ContentReaderDrainEvidenceDigest, ContentReaderDrainKind,
    ContentReclaimAuthorization, ContentReclaimClockAttestation, ContentReclaimClockAttestationId,
    ContentReclaimClockCoordinatorId, ContentReclaimClockEvidenceDigest, ContentReclaimGrace,
    ContentReclaimProofToken, ContentReclaimSweep, ContentReclaimSweepBackend, Error,
    ObjectStoreReclamationEvidenceDigest, Result, StorageDomainId, UploadId, array_at,
    decode_content_id,
};

mod access;
mod attestation;
mod control;
mod grace;
mod quarantine;
mod sweep;

pub(crate) use access::*;
pub(crate) use attestation::*;
pub(crate) use control::*;
pub(crate) use grace::*;
pub(crate) use quarantine::*;
pub(crate) use sweep::*;
