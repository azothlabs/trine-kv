use std::time::Duration;

use crate::{Error, Result, error::ContentReclaimBlocker, transaction::Transaction};

use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
    CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET, ContentAccessCoordinateRecord,
    ContentAccessMode, ContentAttachment, ContentAttachmentScope, ContentChangeId,
    ContentControlRecord, ContentDescriptor, ContentLeaseId, ContentLeaseRecord,
    ContentPhysicalHoldId, ContentPhysicalHoldRecord, ContentQuarantineRecord,
    ContentQuarantineStage, ContentReaderDrainAttestationRecord, ContentReclaimAuthorization,
    ContentReclaimClockAttestation, ContentReclaimGraceRecord, ContentReclaimGraceStage,
    ContentReclaimIntentStage, ContentReclaimSweepRecord, ContentReclaimSweepRecordState,
    ContentReclaimSweepStage, ContentTokenIndexRecord, StorageDomainId, UploadToken,
    UploadTokenRecord, content_access_coordinate_key, content_control_key, content_lease_prefix,
    content_physical_hold_prefix, content_prefix_range, content_quarantine_key,
    content_reader_drain_attestation_key, content_reclaim_grace_key, content_reclaim_sweep_key,
    content_token_index_key, content_token_index_prefix, current_epoch_millis, duration_millis,
    upload_token_key,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InactiveAuthorityPolicy {
    Retain,
    Prune,
}

mod grace;
mod guards;
mod intent;
mod quarantine;
mod sweep;
mod token;
