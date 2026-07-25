use std::{
    hash::{BuildHasher, Hash},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use futures::lock::MutexGuard;
use sha2::Sha256;

use crate::{
    content::{
        CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
        CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET, ContentAccessBarrier,
        ContentAccessBarrierId, ContentAccessBarrierRecord, ContentAccessCoordinateRecord,
        ContentAccessMode, ContentDescriptor, ContentHandle, ContentId, ContentLease,
        ContentLeaseId, ContentLeaseOptions, ContentLeaseRecord, ContentPhysicalAccountRecord,
        ContentPhysicalHold, ContentPhysicalHoldId, ContentPhysicalHoldOptions,
        ContentPhysicalHoldOwnerId, ContentPhysicalHoldRecord, ContentPhysicalHoldRecordState,
        ContentPhysicalQuota, ContentPhysicalReservationRecord, ContentQuarantine,
        ContentQuarantineRecord, ContentReaderDrainAttestation, ContentReaderDrainAttestationId,
        ContentReaderDrainAttestationOptions, ContentReaderDrainAttestationRecord,
        ContentReclaimGrace, ContentReclaimGraceRecord, ContentReclaimSweep,
        ContentReclaimSweepBackend, ContentReclaimSweepRecord, ContentReclaimSweepRecordState,
        ContentTokenIndexRecord, ContentUpload, ContentUploadOptions, ContentUploadResume,
        SealedContent, StorageDomainId, UploadId, UploadSessionState, UploadSessionStatus,
        UploadToken, UploadTokenRecord, content_access_coordinate_key, content_control_key,
        content_lease_key, content_lease_prefix, content_physical_account_key,
        content_physical_hold_key, content_physical_quota_key, content_physical_reservation_key,
        content_quarantine_key, content_reader_drain_attestation_key, content_reclaim_grace_key,
        content_reclaim_sweep_key, content_token_index_key, current_epoch_millis, duration_millis,
        upload_token_key,
    },
    error::{Error, Result},
    options::{
        ContentReclamationMode, DurabilityMode, HostStorageBackend, StorageMode, WriteOptions,
    },
    storage::{
        StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind, StorageObjectReadBackend,
        StorageObjectWriteBackend,
    },
    transaction::TransactionOptions,
};

use super::Db;

fn initial_upload_reservation(state: &UploadSessionState) -> u64 {
    state
        .options()
        .expected_length()
        .unwrap_or_else(|| state.length())
}

fn content_lock_shard_index(
    hasher: &impl BuildHasher,
    id: &impl Hash,
    shard_count: usize,
) -> usize {
    debug_assert!(shard_count != 0);
    usize::try_from(hasher.hash_one(id) % shard_count as u64)
        .expect("content lock shard always fits usize")
}

mod lease_hold;
mod reclaim;
mod storage;
mod upload;

#[cfg(test)]
mod lock_shard_tests {
    use std::{
        collections::{BTreeSet, hash_map::RandomState},
        hash::{BuildHasherDefault, DefaultHasher},
    };

    use super::content_lock_shard_index;
    use crate::content::{StorageDomainId, UploadId};

    #[test]
    fn lock_sharding_hashes_the_complete_identifier() {
        let hasher = BuildHasherDefault::<DefaultHasher>::default();
        let upload_shards = (0_u8..=u8::MAX)
            .map(|suffix| {
                let mut bytes = [7_u8; 16];
                bytes[15] = suffix;
                content_lock_shard_index(&hasher, &UploadId::from_bytes(bytes), 256)
            })
            .collect::<BTreeSet<_>>();

        assert!(
            upload_shards.len() > 64,
            "IDs with one shared prefix must not collapse onto one lock"
        );
    }

    #[test]
    fn keyed_lock_sharding_is_stable_within_one_database() {
        let hasher = RandomState::new();
        let domain = StorageDomainId::from_bytes([19; 16]);

        assert_eq!(
            content_lock_shard_index(&hasher, &domain, 256),
            content_lock_shard_index(&hasher, &domain, 256)
        );
    }
}
