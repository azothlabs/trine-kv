use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::executor::block_on;

use crate::storage::{
    StorageObjectKind,
    fault_injection::{StorageFaultGuard, StorageFaultPoint},
};

use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_PHYSICAL_HOLD_BUCKET,
    CONTENT_TOKEN_INDEX_BUCKET, ContentAccessBarrierRecord, ContentLeaseRecord,
    content_control_key, content_lease_key, content_physical_hold_key, content_quarantine_key,
    content_reader_drain_attestation_key, content_reclaim_grace_key, content_reclaim_sweep_key,
    content_token_index_key,
};
use crate::{
    ContentAccessBarrier, ContentAccessBarrierId, ContentAccessMode, ContentAttachmentScope,
    ContentChangeId, ContentId, ContentLeaseOptions, ContentLeaseOwnerId, ContentPhysicalHoldId,
    ContentPhysicalHoldKind, ContentPhysicalHoldOptions, ContentPhysicalHoldOwnerId,
    ContentQuarantineStage, ContentReaderDrainAttestationId, ContentReaderDrainAttestationOptions,
    ContentReaderDrainCoordinatorId, ContentReaderDrainEvidenceDigest, ContentReaderDrainKind,
    ContentReclaimAuthorization, ContentReclaimBlocker, ContentReclaimClockAttestation,
    ContentReclaimClockAttestationId, ContentReclaimClockCoordinatorId,
    ContentReclaimClockEvidenceDigest, ContentReclaimGraceStage, ContentReclaimIntentStage,
    ContentReclaimProofToken, ContentReclaimSweepStage, ContentReclamationMode,
    ContentUploadOptions, ContentUploadResume, ContentUploadState, Db, DbOptions, ETag, Error,
    HostStorageBackend, InMemoryObjectStore, ObjectClient, ObjectFuture, ObjectMeta,
    ObjectStoreReclamationAttestation, ObjectStoreReclamationEvidenceDigest, ObjectVersion,
    OwnerScopeId, Precondition, PutIf, ReadVersion, SealedContent, StorageDomainId, StorageMode,
    TransactionOptions, UploadId, UploadToken, qualify_object_store_reclamation,
};

const TEST_CHUNK_BYTES: usize = 64 * 1024;
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn physical_content_reclamation_is_disabled_by_default() {
    assert_eq!(
        DbOptions::memory().content_reclamation,
        ContentReclamationMode::Disabled
    );
}

#[test]
fn wasi_storage_mode_does_not_alias_the_qualified_native_backend() {
    let options = DbOptions::wasi_persistent("wasi-content-reclaim-disabled")
        .with_content_reclamation(ContentReclamationMode::QualifiedNativeFilesystem);
    assert!(matches!(
        options.storage_mode,
        StorageMode::HostPersistent {
            backend: HostStorageBackend::Wasi { .. },
        }
    ));
    assert_eq!(
        options.content_reclamation,
        ContentReclamationMode::QualifiedNativeFilesystem
    );
}

#[test]
fn wasi_and_browser_reclamation_use_distinct_qualifications() {
    let wasi = DbOptions::wasi_persistent("wasi-content-reclaim-qualified")
        .with_content_reclamation(ContentReclamationMode::QualifiedWasiFilesystem);
    assert_eq!(
        wasi.content_reclamation,
        ContentReclamationMode::QualifiedWasiFilesystem
    );
    let browser = DbOptions::browser_persistent()
        .with_content_reclamation(ContentReclamationMode::QualifiedBrowserStorage);
    assert_eq!(
        browser.content_reclamation,
        ContentReclamationMode::QualifiedBrowserStorage
    );
}

fn test_scope() -> ContentAttachmentScope {
    ContentAttachmentScope::new(
        StorageDomainId::from_bytes([3_u8; 16]),
        OwnerScopeId::from_bytes([7_u8; 16]),
    )
}

fn test_upload_options() -> ContentUploadOptions {
    ContentUploadOptions::new(test_scope(), Duration::from_hours(1))
}

#[test]
fn empty_content_upload_has_a_durable_zero_byte_reservation_and_seals() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let upload = db
            .begin_content_upload(test_upload_options().with_expected_length(0))
            .await
            .expect("empty upload begins");
        let sealed = upload.seal().await.expect("empty upload seals");
        assert_eq!(sealed.content_id(), ContentId::for_bytes(b""));
        assert_eq!(sealed.len(), 0);
        let content = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("empty content opens");
        assert_eq!(content.len(), 0);
        assert!(
            content
                .read_range(0, 1)
                .await
                .expect("empty read")
                .is_empty()
        );
    });
}

#[test]
fn object_store_reclamation_qualification_is_not_transferable_between_clients() {
    block_on(async {
        let qualified_client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let other_client = Arc::new(InMemoryObjectStore::new());
        let qualification = qualify_object_store_reclamation(
            Arc::clone(&qualified_client),
            "client-bound-reclamation",
            ObjectStoreReclamationAttestation::new(
                ObjectStoreReclamationEvidenceDigest::for_bytes(b"client-bound evidence"),
            ),
        )
        .await
        .expect("first client qualifies");
        assert!(matches!(
            Db::open_object_store_at(
                other_client,
                "client-bound-reclamation",
                DbOptions::object_store().with_content_reclamation(
                    ContentReclamationMode::QualifiedObjectStore(qualification),
                ),
            )
            .await,
            Err(Error::UnsupportedBackend { .. })
        ));
    });
}

#[test]
fn caller_supplied_upload_id_binds_options_and_recovers_exact_state() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let upload_id = UploadId::new().expect("upload identity");
        let options = test_upload_options().with_expected_length(6);

        let ContentUploadResume::Open(mut first) = db
            .begin_content_upload_with_id(upload_id, options)
            .await
            .expect("first begin")
        else {
            panic!("first begin must be open");
        };
        first.write(b"abc").await.expect("write prefix");
        drop(first);

        let different = options.with_expected_length(7);
        assert!(matches!(
            db.begin_content_upload_with_id(upload_id, different).await,
            Err(Error::InvalidOptions { .. })
        ));

        let ContentUploadResume::Open(mut retry) = db
            .begin_content_upload_with_id(upload_id, options)
            .await
            .expect("exact retry")
        else {
            panic!("retry before seal must be open");
        };
        assert_eq!(retry.len(), 3);
        retry.write(b"def").await.expect("write suffix");
        let sealed = retry.seal().await.expect("seal");

        let ContentUploadResume::Sealed(recovered) = db
            .begin_content_upload_with_id(upload_id, options)
            .await
            .expect("sealed retry")
        else {
            panic!("retry after seal must recover sealed result");
        };
        assert_eq!(recovered, sealed);
    });
}

#[test]
fn upload_maintenance_reclaims_orphans_and_prunes_only_sealed_idempotency_state() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        let abandoned_id = UploadId::new().expect("abandoned upload identity");
        let ContentUploadResume::Open(mut abandoned) = db
            .begin_content_upload_with_id(
                abandoned_id,
                test_upload_options().with_expected_length(6),
            )
            .await
            .expect("abandoned upload begins")
        else {
            panic!("new upload is open");
        };
        abandoned
            .write(b"abc")
            .await
            .expect("partial bytes persist");
        drop(abandoned);

        let listed = db.list_content_uploads().await.expect("uploads list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].upload_id(), abandoned_id);
        assert_eq!(listed[0].state(), ContentUploadState::Open);
        assert_eq!(listed[0].len(), 3);
        assert!(listed[0].updated_at_unix_ms() > 0);
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reserved quota reads")
                .upload_reserved_bytes(),
            6
        );

        let reaped = db
            .reap_inactive_content_uploads(u64::MAX)
            .await
            .expect("inactive upload is reaped");
        assert_eq!(reaped.scanned(), 1);
        assert_eq!(reaped.aborted(), 1);
        assert!(
            db.list_content_uploads()
                .await
                .expect("uploads relist")
                .is_empty()
        );
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("released quota reads")
                .upload_reserved_bytes(),
            0
        );
        assert!(matches!(
            db.resume_content_upload(abandoned_id).await,
            Err(Error::ContentUploadNotFound { .. })
        ));

        let sealed_id = UploadId::new().expect("sealed upload identity");
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(sealed_id, test_upload_options().with_expected_length(3))
            .await
            .expect("sealed upload begins")
        else {
            panic!("new upload is open");
        };
        upload.write(b"xyz").await.expect("sealed bytes write");
        let sealed = upload.seal().await.expect("upload seals");
        let pruned = db
            .prune_sealed_content_uploads(u64::MAX)
            .await
            .expect("sealed state prunes");
        assert_eq!(pruned.pruned_sealed(), 1);
        assert!(matches!(
            db.resume_content_upload(sealed_id).await,
            Err(Error::ContentUploadNotFound { .. })
        ));
        let content = db
            .open_content(domain, sealed.content_id())
            .await
            .expect("content remains after idempotency state prune");
        assert_eq!(
            content
                .read_range(0, 3)
                .await
                .expect("content reads")
                .as_ref(),
            b"xyz"
        );
    });
}

#[test]
fn physical_quota_reserves_streams_reconciles_and_releases() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        let configured = db
            .set_content_physical_quota(domain, Some(8))
            .await
            .expect("physical quota configures");
        assert_eq!(configured.accounted_bytes(), 0);
        assert_eq!(configured.remaining(), Some(8));

        let upload_id = UploadId::new().expect("known upload identity");
        let ContentUploadResume::Open(known) = db
            .begin_content_upload_with_id(upload_id, test_upload_options().with_expected_length(6))
            .await
            .expect("known upload reserves")
        else {
            panic!("new known upload is open");
        };
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("known reservation reads")
                .upload_reserved_bytes(),
            6
        );
        assert!(matches!(
            db.begin_content_upload(test_upload_options().with_expected_length(3))
                .await,
            Err(Error::ContentPhysicalQuotaExceeded {
                limit: 8,
                requested_bytes: 3,
                ..
            })
        ));
        known.abort().await.expect("known reservation aborts");
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("released reservation reads")
                .accounted_bytes(),
            0
        );

        let mut streamed = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("unknown upload begins");
        streamed
            .write(b"abcde")
            .await
            .expect("stream reserves bytes");
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("stream reservation reads")
                .upload_reserved_bytes(),
            5
        );
        assert!(matches!(
            streamed.write(b"wxyz").await,
            Err(Error::ContentPhysicalQuotaExceeded {
                limit: 8,
                requested_bytes: 4,
                ..
            })
        ));
        let sealed = streamed.seal().await.expect("stream seals");
        assert_eq!(sealed.len(), 5);
        let reconciled = db
            .content_physical_quota(domain)
            .await
            .expect("sealed accounting reads");
        assert_eq!(reconciled.unique_content_bytes(), 5);
        assert_eq!(reconciled.upload_reserved_bytes(), 0);
        assert_eq!(reconciled.remaining(), Some(3));
        assert!(matches!(
            db.set_content_physical_quota(domain, Some(4)).await,
            Err(Error::ContentPhysicalQuotaExceeded {
                limit: 4,
                requested_bytes: 0,
                ..
            })
        ));
    });
}

#[test]
fn expected_upload_length_is_a_pre_write_physical_quota_boundary() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(1))
            .await
            .expect("physical quota configures");

        let mut upload = db
            .begin_content_upload(test_upload_options().with_expected_length(1))
            .await
            .expect("expected upload reserves");
        assert!(matches!(
            upload.write(b"too large").await,
            Err(Error::ContentLengthMismatch {
                expected: 1,
                actual: 9
            })
        ));
        assert_eq!(upload.len(), 0);
        assert_eq!(upload.buffered_bytes(), 0);
        let quota = db
            .content_physical_quota(domain)
            .await
            .expect("rejected write leaves accounting readable");
        assert_eq!(quota.upload_reserved_bytes(), 1);
        assert_eq!(quota.accounted_bytes(), 1);

        upload
            .write(b"x")
            .await
            .expect("writer remains usable at the declared boundary");
        let sealed = upload.seal().await.expect("bounded upload seals");
        assert_eq!(sealed.len(), 1);
        let quota = db
            .content_physical_quota(domain)
            .await
            .expect("sealed accounting reads");
        assert_eq!(quota.unique_content_bytes(), 1);
        assert_eq!(quota.upload_reserved_bytes(), 0);
    });
}

#[test]
fn physical_quota_concurrent_reservations_do_not_overcommit() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(10))
            .await
            .expect("physical quota configures");
        let options = test_upload_options().with_expected_length(6);
        let (first, second) = futures::join!(
            db.begin_content_upload(options),
            db.begin_content_upload(options)
        );
        let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());
        let rejections = usize::from(matches!(
            &first,
            Err(Error::ContentPhysicalQuotaExceeded { .. } | Error::Conflict { .. })
        )) + usize::from(matches!(
            &second,
            Err(Error::ContentPhysicalQuotaExceeded { .. } | Error::Conflict { .. })
        ));
        assert_eq!((successes, rejections), (1, 1));
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("concurrent accounting reads")
                .upload_reserved_bytes(),
            6
        );
        if let Ok(upload) = first {
            upload.abort().await.expect("first winner aborts");
        }
        if let Ok(upload) = second {
            upload.abort().await.expect("second winner aborts");
        }
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("released concurrent accounting reads")
                .accounted_bytes(),
            0
        );
    });
}

#[test]
fn physical_quota_reservation_survives_reopen_and_resume() {
    let path = temp_db_path("content-physical-quota-reopen");
    let upload_id = UploadId::new().expect("upload identity generates");
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database opens");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(32))
            .await
            .expect("physical quota configures");
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(upload_id, test_upload_options())
            .await
            .expect("unknown upload begins")
        else {
            panic!("new upload is open");
        };
        upload.write(b"durable").await.expect("prefix writes");
        drop(upload);
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reservation reads")
                .upload_reserved_bytes(),
            7
        );
        db.close().await.expect("database closes");
    });
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database reopens");
        let domain = test_scope().storage_domain_id();
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reopened reservation reads")
                .upload_reserved_bytes(),
            7
        );
        let ContentUploadResume::Open(upload) = db
            .resume_content_upload(upload_id)
            .await
            .expect("upload resumes")
        else {
            panic!("resumed upload is open");
        };
        assert_eq!(upload.len(), 7);
        upload.abort().await.expect("resumed upload aborts");
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reopened release reads")
                .accounted_bytes(),
            0
        );
        db.close().await.expect("database closes after release");
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn aborting_upload_retains_quota_until_chunk_cleanup_recovers() {
    let path = temp_db_path("content-physical-quota-abort-recovery");
    let upload_id = UploadId::new().expect("upload identity generates");
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database opens");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(32))
            .await
            .expect("physical quota configures");
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(upload_id, test_upload_options())
            .await
            .expect("upload begins")
        else {
            panic!("new upload is open");
        };
        upload
            .write(b"durable")
            .await
            .expect("partial chunk writes");
        let delete_fault = StorageFaultGuard::install(
            &path,
            StorageFaultPoint::ObjectDelete,
            Some(StorageObjectKind::ContentChunk),
            1,
        );
        assert!(matches!(upload.abort().await, Err(Error::Io(_))));
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("failed abort accounting reads")
                .upload_reserved_bytes(),
            7
        );
        drop(delete_fault);
        db.close().await.expect("database closes");
    });
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database reopens");
        assert!(matches!(
            db.resume_content_upload(upload_id).await,
            Err(Error::ContentUploadNotFound { .. })
        ));
        assert_eq!(
            db.content_physical_quota(test_scope().storage_domain_id())
                .await
                .expect("recovered abort accounting reads")
                .accounted_bytes(),
            0
        );
        db.close().await.expect("database closes after recovery");
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

fn test_hold_id(seed: u8) -> ContentPhysicalHoldId {
    let mut bytes = [seed; 16];
    bytes[0] = 1;
    ContentPhysicalHoldId::from_bytes(bytes).expect("test physical-hold id decodes")
}

fn test_access_barrier_id(seed: u8) -> ContentAccessBarrierId {
    let mut bytes = [seed; 16];
    bytes[0] = 1;
    ContentAccessBarrierId::from_bytes(bytes).expect("test access-barrier id decodes")
}

fn test_reader_drain_attestation_id(seed: u8) -> ContentReaderDrainAttestationId {
    let mut bytes = [seed; 16];
    bytes[0] = 1;
    ContentReaderDrainAttestationId::from_bytes(bytes)
        .expect("test reader-drain attestation id decodes")
}

fn test_reclaim_clock_attestation_id(seed: u8) -> ContentReclaimClockAttestationId {
    let mut bytes = [seed; 16];
    bytes[0] = 1;
    ContentReclaimClockAttestationId::from_bytes(bytes)
        .expect("test reclaim-clock attestation id decodes")
}

fn test_reader_drain_options(
    kind: ContentReaderDrainKind,
    seed: u8,
) -> ContentReaderDrainAttestationOptions {
    ContentReaderDrainAttestationOptions::new(
        kind,
        ContentReaderDrainCoordinatorId::from_bytes([seed; 16]),
        ContentReaderDrainEvidenceDigest::for_bytes(&[seed; 32]),
    )
}

async fn seal_content_without_access_barrier(db: &Db, bytes: &[u8]) -> SealedContent {
    let mut upload = db
        .begin_content_upload(test_upload_options())
        .await
        .expect("upload begins");
    upload.write(bytes).await.expect("content writes");
    upload.seal().await.expect("content seals")
}

async fn seal_reclaim_content(db: &Db, bytes: &[u8]) -> SealedContent {
    let sealed = seal_content_without_access_barrier(db, bytes).await;
    db.enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(2))
        .await
        .expect("leased-only access is enforced");
    sealed
}

async fn consume_reclaim_token(db: &Db, sealed: SealedContent, change: u8) {
    let mut transaction = db.transaction(TransactionOptions::default());
    transaction
        .consume_upload_token(
            sealed.upload_token(),
            test_scope(),
            ContentChangeId::from_bytes([change; 16]),
        )
        .await
        .expect("upload token consumption stages");
    transaction
        .commit()
        .await
        .expect("upload token consumption commits");
}

async fn attest_reclaim_reader_drain(db: &Db, domain: StorageDomainId, seed: u8) {
    let barrier = db
        .enforce_content_leased_only(domain, test_access_barrier_id(seed))
        .await
        .expect("leased-only barrier coordinate reads");
    db.attest_content_reader_drain(
        barrier,
        test_reader_drain_attestation_id(seed.saturating_add(1)),
        test_reader_drain_options(
            ContentReaderDrainKind::DomainBootstrap,
            seed.saturating_add(2),
        ),
    )
    .await
    .expect("reader drain attests");
}

async fn commit_reclaim_quarantine(
    db: &Db,
    sealed: SealedContent,
    proof_seed: u8,
) -> (ContentReclaimAuthorization, ReadVersion) {
    let mut intent = db.transaction(TransactionOptions::default());
    let authorization = reclaim_authorization(&intent, sealed, proof_seed);
    intent
        .stage_content_reclaim_intent(authorization)
        .await
        .expect("reclaim intent stages");
    intent.commit().await.expect("reclaim intent commits");
    let mut quarantine = db.transaction(TransactionOptions::default());
    quarantine
        .stage_content_quarantine(authorization)
        .await
        .expect("quarantine stages");
    let commit = quarantine.commit().await.expect("quarantine commits");
    (authorization, commit.read_version())
}

async fn assert_provider_hold_revives_grace(db: &Db, sealed: SealedContent) {
    let hold = db
        .acquire_content_physical_hold(
            sealed.storage_domain_id(),
            sealed.content_id(),
            test_hold_id(94),
            ContentPhysicalHoldOptions::until_released(
                ContentPhysicalHoldKind::Provider,
                ContentPhysicalHoldOwnerId::from_bytes([94; 16]),
            ),
        )
        .await
        .expect("provider hold atomically revives content");
    assert_eq!(
        db.content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("hold-revived grace status reads"),
        None
    );
    assert_eq!(
        db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("hold-revived quarantine status reads"),
        None
    );
    hold.release().await.expect("provider hold releases");
}

fn reclaim_authorization(
    transaction: &crate::Transaction,
    sealed: SealedContent,
    proof: u8,
) -> ContentReclaimAuthorization {
    ContentReclaimAuthorization::new(
        sealed.storage_domain_id(),
        sealed.content_id(),
        ContentReclaimProofToken::from_bytes([proof; 49]),
        transaction.read_version(),
        u64::MAX,
    )
}

#[test]
fn leased_only_barrier_fences_new_unleased_opens_but_not_old_handles() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"barrier bytes").await;
        assert_eq!(
            db.content_access_mode(sealed.storage_domain_id())
                .await
                .expect("compatible mode reads"),
            ContentAccessMode::CompatibleUnleased
        );
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("unleased content opens before barrier");

        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(3))
            .await
            .expect("leased-only barrier commits");
        assert!(barrier.enforced_at().as_u64() > 0);
        assert_eq!(
            db.content_access_mode(sealed.storage_domain_id())
                .await
                .expect("leased-only mode reads"),
            ContentAccessMode::LeasedOnly {
                barrier_id: barrier.barrier_id(),
            }
        );
        let repeated = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(4))
            .await
            .expect("existing barrier is adopted");
        assert_eq!(repeated, barrier);
        assert!(matches!(
            db.open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::ContentLeaseRequired { barrier_id })
                if barrier_id == barrier.barrier_id()
        ));
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("pre-barrier handle still reads")
                .as_ref(),
            b"barrier bytes"
        );
        let leased = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([5; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("leased content opens after barrier");
        assert!(leased.lease_id().is_some());
    });
}

#[test]
fn reader_drain_attestation_is_barrier_bound_idempotent_and_does_not_close_handles() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"attested drain bytes").await;
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier handle opens");
        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(13))
            .await
            .expect("leased-only barrier commits");
        let attestation_id = test_reader_drain_attestation_id(14);
        let options =
            test_reader_drain_options(ContentReaderDrainKind::NativeProcessSetRestarted, 15);
        let attestation = db
            .attest_content_reader_drain(barrier, attestation_id, options)
            .await
            .expect("trusted coordinator claim records");
        assert_eq!(attestation.storage_domain_id(), sealed.storage_domain_id());
        assert_eq!(attestation.barrier_id(), barrier.barrier_id());
        assert_eq!(attestation.attestation_id(), attestation_id);
        assert_eq!(attestation.options(), options);
        assert_eq!(attestation.barrier_enforced_at(), barrier.enforced_at());
        assert!(attestation.attested_at().as_u64() > barrier.enforced_at().as_u64());
        assert_eq!(
            db.content_reader_drain_attestation(sealed.storage_domain_id())
                .await
                .expect("attestation reads"),
            Some(attestation)
        );
        assert_eq!(
            db.attest_content_reader_drain(barrier, attestation_id, options)
                .await
                .expect("lost-response retry returns original"),
            attestation
        );
        assert!(matches!(
            db.attest_content_reader_drain(barrier, test_reader_drain_attestation_id(16), options,)
                .await,
            Err(Error::InvalidOptions { .. })
        ));
        assert!(matches!(
            db.attest_content_reader_drain(
                barrier,
                attestation_id,
                test_reader_drain_options(ContentReaderDrainKind::DomainBootstrap, 15),
            )
            .await,
            Err(Error::InvalidOptions { .. })
        ));

        // This intentionally dishonest coordinator call demonstrates the trust
        // boundary: persistence cannot terminate or observe an old handle.
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("old handle remains readable")
                .as_ref(),
            b"attested drain bytes"
        );
    });
}

#[test]
fn reader_drain_attestation_requires_an_active_exact_barrier_and_fails_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let domain = test_scope().storage_domain_id();
        assert_eq!(
            db.content_reader_drain_attestation(domain)
                .await
                .expect("compatible domain has no attestation"),
            None
        );
        let forged =
            ContentAccessBarrier::new(domain, test_access_barrier_id(17), ReadVersion::from_u64(1));
        assert!(matches!(
            db.attest_content_reader_drain(
                forged,
                test_reader_drain_attestation_id(18),
                test_reader_drain_options(ContentReaderDrainKind::DomainBootstrap, 19),
            )
            .await,
            Err(Error::InvalidOptions { .. })
        ));

        let barrier = db
            .enforce_content_leased_only(domain, test_access_barrier_id(20))
            .await
            .expect("leased-only barrier commits");
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_reader_drain_attestation_key(domain),
                b"malformed".to_vec(),
            )
            .expect("malformed attestation stages");
        damage
            .commit()
            .await
            .expect("malformed attestation commits");
        assert!(matches!(
            db.content_reader_drain_attestation(domain).await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.attest_content_reader_drain(
                barrier,
                test_reader_drain_attestation_id(21),
                test_reader_drain_options(ContentReaderDrainKind::DomainBootstrap, 22),
            )
            .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn reclaim_intent_requires_coordinated_leased_only_barrier() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"access-gated reclaim").await;
        consume_reclaim_token(&db, sealed, 40).await;
        let mut compatible = db.transaction(TransactionOptions::default());
        assert!(matches!(
            compatible
                .stage_content_reclaim_intent(reclaim_authorization(&compatible, sealed, 30))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::UnleasedAccessAllowed,
            })
        ));

        let barrier_id = test_access_barrier_id(6);
        db.write_content_access_barrier_record(ContentAccessBarrierRecord {
            storage_domain_id: sealed.storage_domain_id(),
            barrier_id,
        })
        .await
        .expect("backend barrier publishes without coordinate");
        let mut interrupted = db.transaction(TransactionOptions::default());
        assert!(matches!(
            interrupted
                .stage_content_reclaim_intent(reclaim_authorization(&interrupted, sealed, 31))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::LeasedOnlyBarrierUncoordinated {
                    barrier_id: blocked,
                },
            }) if blocked == barrier_id
        ));

        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(7))
            .await
            .expect("interrupted barrier coordinate resumes");
        assert_eq!(barrier.barrier_id(), barrier_id);
        let mut coordinated = db.transaction(TransactionOptions::default());
        coordinated
            .stage_content_reclaim_intent(reclaim_authorization(&coordinated, sealed, 32))
            .await
            .expect("coordinated leased-only barrier permits intent");
    });
}

#[test]
fn malformed_access_barrier_fails_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"malformed access barrier").await;
        db.write_content_access_barrier_bytes_for_test(
            sealed.storage_domain_id(),
            Arc::<[u8]>::from(b"malformed".as_slice()),
        )
        .await
        .expect("malformed barrier publishes for test");
        assert!(matches!(
            db.content_access_mode(sealed.storage_domain_id()).await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn stale_native_read_only_handle_observes_leased_only_barrier_without_refresh() {
    let path = temp_db_path("content-access-barrier-native-reader");
    block_on(async {
        let writer = Db::open(&path).await.expect("native writer opens");
        let sealed = seal_content_without_access_barrier(&writer, b"native stale reader").await;
        let reader = Db::open(DbOptions::persistent_read_only(&path))
            .await
            .expect("native read-only handle opens before barrier");
        writer
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(8))
            .await
            .expect("writer publishes barrier");

        assert!(matches!(
            reader
                .open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::ContentLeaseRequired { .. })
        ));
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn stale_object_store_reader_observes_leased_only_barrier_without_kv_refresh() {
    block_on(async {
        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let writer = Db::open_object_store_at(
            Arc::clone(&client),
            "content-access-barrier-object",
            DbOptions::object_store(),
        )
        .await
        .expect("object-store writer opens");
        let sealed = seal_content_without_access_barrier(&writer, b"object stale reader").await;
        let reader = Db::open_object_store_at(
            Arc::clone(&client),
            "content-access-barrier-object",
            DbOptions::object_store().read_only(),
        )
        .await
        .expect("stale object-store reader opens before barrier");
        writer
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(9))
            .await
            .expect("object-store barrier publishes");

        assert!(matches!(
            reader
                .open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::ContentLeaseRequired { .. })
        ));
    });
}

#[test]
fn reader_drain_attestation_survives_native_reopen() {
    let path = temp_db_path("content-reader-drain-native-reopen");
    let domain = test_scope().storage_domain_id();
    let recorded = block_on(async {
        let db = Db::open(&path).await.expect("native writer opens");
        let barrier = db
            .enforce_content_leased_only(domain, test_access_barrier_id(23))
            .await
            .expect("native barrier commits");
        db.attest_content_reader_drain(
            barrier,
            test_reader_drain_attestation_id(24),
            test_reader_drain_options(ContentReaderDrainKind::NativeProcessSetRestarted, 25),
        )
        .await
        .expect("native reader drain attests")
    });
    block_on(async {
        let reopened = Db::open(&path).await.expect("native writer reopens");
        assert_eq!(
            reopened
                .content_reader_drain_attestation(domain)
                .await
                .expect("native attestation reads after reopen"),
            Some(recorded)
        );
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn refreshed_object_store_reader_observes_remote_drain_attestation() {
    block_on(async {
        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let prefix = "content-reader-drain-object";
        let writer =
            Db::open_object_store_at(Arc::clone(&client), prefix, DbOptions::object_store())
                .await
                .expect("object-store writer opens");
        let reader = Db::open_object_store_at(
            Arc::clone(&client),
            prefix,
            DbOptions::object_store().read_only(),
        )
        .await
        .expect("object-store reader opens before attestation");
        let domain = test_scope().storage_domain_id();
        let barrier = writer
            .enforce_content_leased_only(domain, test_access_barrier_id(26))
            .await
            .expect("object-store barrier commits");
        let attestation = writer
            .attest_content_reader_drain(
                barrier,
                test_reader_drain_attestation_id(27),
                test_reader_drain_options(ContentReaderDrainKind::RemoteCredentialEpochRetired, 28),
            )
            .await
            .expect("remote reader drain attests");

        reader
            .refresh_object_store()
            .await
            .expect("reader refreshes protected KV state");
        assert_eq!(
            reader
                .content_reader_drain_attestation(domain)
                .await
                .expect("refreshed reader sees attestation"),
            Some(attestation)
        );
    });
}

#[test]
fn reclaim_intent_checks_token_and_is_durable_and_idempotent() {
    let path = temp_db_path("content-reclaim-intent");
    let authorization = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let sealed = seal_reclaim_content(&db, b"reclaim intent bytes").await;
        let mut blocked = db.transaction(TransactionOptions::default());
        let blocked_authorization = reclaim_authorization(&blocked, sealed, 31);
        assert!(matches!(
            blocked
                .stage_content_reclaim_intent(blocked_authorization)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::UploadToken { .. },
            })
        ));

        consume_reclaim_token(&db, sealed, 41).await;
        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 32);
        assert_eq!(
            accepted
                .stage_content_reclaim_intent(authorization)
                .await
                .expect("reclaim intent stages"),
            ContentReclaimIntentStage::Staged
        );
        accepted.commit().await.expect("reclaim intent commits");

        let mut repeated = db.transaction(TransactionOptions::default());
        assert!(matches!(
            repeated
                .stage_content_reclaim_intent(authorization)
                .await
                .expect("reclaim intent repeats"),
            ContentReclaimIntentStage::Existing { .. }
        ));
        authorization
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        let mut repeated = db.transaction(TransactionOptions::default());
        assert!(matches!(
            repeated
                .stage_content_reclaim_intent(authorization)
                .await
                .expect("durable reclaim intent reads"),
            ContentReclaimIntentStage::Existing { .. }
        ));
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn quarantine_requires_drain_and_exact_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"quarantine state bytes").await;
        consume_reclaim_token(&db, sealed, 71).await;

        let mut without_drain = db.transaction(TransactionOptions::default());
        let first_authorization = reclaim_authorization(&without_drain, sealed, 61);
        assert!(matches!(
            without_drain
                .stage_content_quarantine(first_authorization)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReaderDrainNotAttested { .. },
            })
        ));

        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 31).await;
        let mut without_intent = db.transaction(TransactionOptions::default());
        assert!(matches!(
            without_intent
                .stage_content_quarantine(reclaim_authorization(&without_intent, sealed, 62))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReclaimIntentRequired,
            })
        ));
    });
}

#[test]
fn quarantine_blocks_reads_is_idempotent_and_can_revive() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"quarantine state bytes").await;
        consume_reclaim_token(&db, sealed, 71).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 31).await;
        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 63);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");

        let mut quarantine = db.transaction(TransactionOptions::default());
        assert_eq!(
            quarantine
                .stage_content_quarantine(authorization)
                .await
                .expect("quarantine stages"),
            ContentQuarantineStage::Staged
        );
        let commit = quarantine.commit().await.expect("quarantine commits");
        let record = db
            .content_quarantine(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("quarantine reads")
            .expect("quarantine is durable");
        assert_eq!(record.quarantined_at(), commit.read_version());
        assert_eq!(record.proof_token(), authorization.proof_token());
        assert_eq!(record.verified_at(), authorization.verified_at());
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([72; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentQuarantined { quarantined_at })
                if quarantined_at == commit.read_version()
        ));

        let mut repeated = db.transaction(TransactionOptions::default());
        assert_eq!(
            repeated
                .stage_content_quarantine(authorization)
                .await
                .expect("quarantine retry reads durable state"),
            ContentQuarantineStage::Existing {
                quarantined_at: commit.read_version(),
            }
        );

        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(73),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Repair,
                    ContentPhysicalHoldOwnerId::from_bytes([73; 16]),
                ),
            )
            .await
            .expect("repair hold returns quarantined content to active");
        assert_eq!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("revived quarantine status reads"),
            None
        );
        db.open_content_leased(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentLeaseOptions::new(
                ContentLeaseOwnerId::from_bytes([74; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("leased read succeeds after revival");
        hold.release().await.expect("repair hold releases");
    });
}

#[test]
fn new_upload_authority_atomically_revives_quarantined_content() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"quarantine token revival";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 75).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 35).await;

        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 65);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");
        let mut quarantine = db.transaction(TransactionOptions::default());
        assert_eq!(
            quarantine
                .stage_content_quarantine(authorization)
                .await
                .expect("quarantine stages"),
            ContentQuarantineStage::Staged
        );
        quarantine.commit().await.expect("quarantine commits");

        let repeated_content = seal_content_without_access_barrier(&db, bytes).await;
        assert_eq!(repeated_content.content_id(), sealed.content_id());
        assert_eq!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("revived quarantine status reads"),
            None
        );
        let mut stale_retry = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale_retry.stage_content_quarantine(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReclaimIntentRequired,
            })
        ));
    });
}

#[test]
fn leased_open_racing_staged_quarantine_forces_transaction_conflict() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"quarantine lease race").await;
        consume_reclaim_token(&db, sealed, 75).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 34).await;
        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 64);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");

        let mut quarantine = db.transaction(TransactionOptions::default());
        quarantine
            .stage_content_quarantine(authorization)
            .await
            .expect("quarantine stages before lease");
        db.open_content_leased(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentLeaseOptions::new(
                ContentLeaseOwnerId::from_bytes([76; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("concurrent leased open commits first");
        assert!(matches!(
            quarantine.commit().await,
            Err(Error::Conflict { .. })
        ));
        assert_eq!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("failed quarantine remains absent"),
            None
        );
    });
}

#[test]
fn quarantine_survives_native_reopen_and_keeps_leased_reads_fenced() {
    let path = temp_db_path("content-quarantine-native-reopen");
    let (domain, content_id, quarantined_at) = block_on(async {
        let db = Db::open(&path).await.expect("native writer opens");
        let sealed = seal_reclaim_content(&db, b"durable quarantine bytes").await;
        consume_reclaim_token(&db, sealed, 77).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 37).await;
        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 65);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("native reclaim intent stages");
        intent
            .commit()
            .await
            .expect("native reclaim intent commits");
        let mut quarantine = db.transaction(TransactionOptions::default());
        quarantine
            .stage_content_quarantine(authorization)
            .await
            .expect("native quarantine stages");
        let commit = quarantine
            .commit()
            .await
            .expect("native quarantine commits");
        (
            sealed.storage_domain_id(),
            sealed.content_id(),
            commit.read_version(),
        )
    });

    block_on(async {
        let reopened = Db::open(&path).await.expect("native writer reopens");
        let quarantine = reopened
            .content_quarantine(domain, content_id)
            .await
            .expect("reopened quarantine reads")
            .expect("reopened quarantine exists");
        assert_eq!(quarantine.quarantined_at(), quarantined_at);
        assert!(matches!(
            reopened
                .open_content_leased(
                    domain,
                    content_id,
                    ContentLeaseOptions::new(
                        ContentLeaseOwnerId::from_bytes([78; 16]),
                        Duration::from_mins(1),
                    ),
                )
                .await,
            Err(Error::ContentQuarantined { quarantined_at: blocked })
                if blocked == quarantined_at
        ));
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn malformed_quarantine_fails_reads_and_revival_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"malformed quarantine bytes").await;
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_quarantine_key(sealed.storage_domain_id(), sealed.content_id()),
                b"malformed".to_vec(),
            )
            .expect("malformed quarantine stages");
        damage.commit().await.expect("malformed quarantine commits");

        assert!(matches!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([79; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(80),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Administrative,
                    ContentPhysicalHoldOwnerId::from_bytes([80; 16]),
                ),
            )
            .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn reclaim_grace_requires_quarantine_is_idempotent_and_keeps_bytes() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"reclaim grace retains bytes";
        let sealed = seal_content_without_access_barrier(&db, bytes).await;
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier handle opens");
        db.enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(40))
            .await
            .expect("leased-only barrier commits");
        consume_reclaim_token(&db, sealed, 91).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 40).await;

        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 71);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");
        let mut without_quarantine = db.transaction(TransactionOptions::default());
        assert!(matches!(
            without_quarantine
                .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::QuarantineRequired,
            })
        ));

        let mut quarantine = db.transaction(TransactionOptions::default());
        quarantine
            .stage_content_quarantine(authorization)
            .await
            .expect("quarantine stages");
        let quarantine_commit = quarantine.commit().await.expect("quarantine commits");
        let mut grace = db.transaction(TransactionOptions::default());
        assert_eq!(
            grace
                .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
                .await
                .expect("grace stages"),
            ContentReclaimGraceStage::Staged
        );
        let grace_commit = grace.commit().await.expect("grace commits");
        let record = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("grace reads")
            .expect("grace exists");
        assert_eq!(record.quarantined_at(), quarantine_commit.read_version());
        assert_eq!(record.started_at(), grace_commit.read_version());
        assert_eq!(record.requested_duration_ms(), 60_000);
        assert_eq!(
            record.not_before_unix_ms(),
            record.observed_at_unix_ms() + record.requested_duration_ms()
        );
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("old handle still reads because grace deletes nothing")
                .as_ref(),
            bytes
        );

        let mut repeated = db.transaction(TransactionOptions::default());
        assert_eq!(
            repeated
                .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
                .await
                .expect("grace retry reads existing record"),
            ContentReclaimGraceStage::Existing {
                started_at: grace_commit.read_version(),
            }
        );
        let mut different = db.transaction(TransactionOptions::default());
        assert!(matches!(
            different
                .stage_content_reclaim_grace(authorization, Duration::from_secs(61))
                .await,
            Err(Error::InvalidOptions { .. })
        ));
        assert_provider_hold_revives_grace(&db, sealed).await;
    });
}

#[test]
fn fresh_proof_recovers_quarantine_committed_before_grace() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(
            &db,
            b"fresh proof recovers continuously quarantined content",
        )
        .await;
        db.enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(42))
            .await
            .expect("leased-only barrier commits");
        consume_reclaim_token(&db, sealed, 92).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 42).await;

        let (original, quarantined_at) = commit_reclaim_quarantine(&db, sealed, 78).await;
        let durable_quarantine = db
            .content_quarantine(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("quarantine reads")
            .expect("quarantine is durable");
        assert_eq!(durable_quarantine.quarantined_at(), quarantined_at);

        let mut future = db.transaction(TransactionOptions::default());
        let future_authorization = ContentReclaimAuthorization::new(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentReclaimProofToken::from_bytes([80; 49]),
            ReadVersion::from_u64(future.read_version().as_u64().saturating_add(1)),
            u64::MAX,
        );
        assert!(matches!(
            future
                .stage_content_reclaim_grace(future_authorization, Duration::from_mins(1),)
                .await,
            Err(Error::InvalidOptions { .. })
        ));

        let mut recovery = db.transaction(TransactionOptions::default());
        let fresh = reclaim_authorization(&recovery, sealed, 79);
        assert_ne!(fresh.proof_token(), original.proof_token());
        assert!(fresh.verified_at().as_u64() >= quarantined_at.as_u64());
        assert_eq!(
            recovery
                .stage_content_reclaim_grace(fresh, Duration::from_mins(1))
                .await
                .expect("fresh proof stages grace over continuous quarantine"),
            ContentReclaimGraceStage::Staged
        );
        let committed = recovery.commit().await.expect("recovered grace commits");
        let grace = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("recovered grace reads")
            .expect("recovered grace exists");
        assert_eq!(grace.quarantined_at(), quarantined_at);
        assert_eq!(grace.proof_token(), original.proof_token());
        assert_eq!(grace.started_at(), committed.read_version());
    });
}

#[test]
fn reclaim_grace_survives_reopen_and_upload_activity_revives_content() {
    let path = temp_db_path("content-reclaim-grace-reopen");
    let (domain, content_id, recorded) = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let bytes = b"durable reclaim grace";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 92).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 43).await;
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 72).await;
        let mut grace = db.transaction(TransactionOptions::default());
        grace
            .stage_content_reclaim_grace(authorization, Duration::from_mins(2))
            .await
            .expect("native grace stages");
        grace.commit().await.expect("native grace commits");
        let record = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("native grace reads")
            .expect("native grace exists");
        (sealed.storage_domain_id(), sealed.content_id(), record)
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        assert_eq!(
            db.content_reclaim_grace(domain, content_id)
                .await
                .expect("reopened grace reads"),
            Some(recorded)
        );
        let repeated = seal_content_without_access_barrier(&db, b"durable reclaim grace").await;
        assert_eq!(repeated.content_id(), content_id);
        assert_eq!(
            db.content_reclaim_grace(domain, content_id)
                .await
                .expect("revived grace status reads"),
            None
        );
        assert_eq!(
            db.content_quarantine(domain, content_id)
                .await
                .expect("revived quarantine status reads"),
            None
        );
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
#[allow(clippy::too_many_lines)] // One reopen/fault/re-upload lifecycle must stay ordered.
fn qualified_filesystem_sweep_reclaims_after_reopen_and_allows_later_reupload() {
    let path = temp_db_path("content-reclaim-sweep-native-reopen");
    let options = || {
        DbOptions::new(&path)
            .with_content_reclamation(ContentReclamationMode::QualifiedNativeFilesystem)
    };
    let bytes = b"durable physical reclaim bytes";
    let (sealed, prepared_at) = block_on(async {
        let db = Db::open(options()).await.expect("native db opens");
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 97).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 55).await;
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 76).await;
        let mut grace_tx = db.transaction(TransactionOptions::default());
        grace_tx
            .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
            .await
            .expect("grace stages");
        grace_tx.commit().await.expect("grace commits");
        let grace = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("grace reads")
            .expect("grace is durable");
        assert!(matches!(
            ContentReclaimClockAttestation::new(
                grace,
                test_reclaim_clock_attestation_id(54),
                ContentReclaimClockCoordinatorId::from_bytes([55; 16]),
                ContentReclaimClockEvidenceDigest::for_bytes(b"premature clock claim"),
                grace.not_before_unix_ms() - 1,
            ),
            Err(Error::InvalidOptions { .. })
        ));
        let clock = ContentReclaimClockAttestation::new(
            grace,
            test_reclaim_clock_attestation_id(56),
            ContentReclaimClockCoordinatorId::from_bytes([57; 16]),
            ContentReclaimClockEvidenceDigest::for_bytes(b"trusted restart and monotonic evidence"),
            grace.not_before_unix_ms(),
        )
        .expect("trusted clock claim binds grace");
        let mut prepare = db.transaction(TransactionOptions::default());
        let expired_at_trusted_observation = ContentReclaimAuthorization::new(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentReclaimProofToken::from_bytes([78; 49]),
            prepare.read_version(),
            clock.observed_at_unix_ms(),
        );
        assert!(matches!(
            prepare
                .stage_content_reclaim_sweep(expired_at_trusted_observation, clock)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ProofExpired { .. },
            })
        ));
        let fresh = reclaim_authorization(&prepare, sealed, 77);
        assert_eq!(
            prepare
                .stage_content_reclaim_sweep(fresh, clock)
                .await
                .expect("final sweep stages"),
            ContentReclaimSweepStage::Staged
        );
        let commit = prepare.commit().await.expect("Prepared sweep commits");
        let sweep = db
            .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("Prepared sweep reads")
            .expect("Prepared sweep exists");
        assert_eq!(sweep.prepared_at(), commit.read_version());
        assert_eq!(sweep.reclaimed_at(), None);
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([58; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::SweepPrepared { .. },
            })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(61),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Backup,
                    ContentPhysicalHoldOwnerId::from_bytes([61; 16]),
                ),
            )
            .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::SweepPrepared { .. },
            })
        ));
        db.close().await.expect("native db closes");
        (sealed, commit.read_version())
    });

    block_on(async {
        let db = Db::open(options()).await.expect("native db reopens");
        let prepared = db
            .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("Prepared sweep survives reopen")
            .expect("Prepared sweep remains");
        assert_eq!(prepared.prepared_at(), prepared_at);
        let descriptor_fault = StorageFaultGuard::install(
            &path,
            StorageFaultPoint::ObjectDelete,
            Some(StorageObjectKind::ContentDescriptor),
            1,
        );
        assert!(matches!(
            db.resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::Io(_))
        ));
        assert!(
            db.content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("failed delete keeps sweep readable")
                .is_some_and(|sweep| sweep.reclaimed_at().is_none())
        );
        drop(descriptor_fault);
        db.close().await.expect("failed sweep db closes");
    });

    block_on(async {
        let db = Db::open(options())
            .await
            .expect("native db reopens after delete fault");
        let (first, second) =
            thread::scope(|scope| {
                let first = scope.spawn(|| {
                    block_on(db.resume_content_reclaim_sweep(
                        sealed.storage_domain_id(),
                        sealed.content_id(),
                    ))
                });
                let second = scope.spawn(|| {
                    block_on(db.resume_content_reclaim_sweep(
                        sealed.storage_domain_id(),
                        sealed.content_id(),
                    ))
                });
                (
                    first.join().expect("first sweep thread joins"),
                    second.join().expect("second sweep thread joins"),
                )
            });
        let first = first.expect("first concurrent sweep completes");
        let second = second.expect("second concurrent sweep is idempotent");
        assert!(first.reclaimed_at().is_some());
        assert_eq!(first, second);
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([59; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentNotFound { .. })
        ));
        assert_eq!(
            db.content_physical_quota(sealed.storage_domain_id())
                .await
                .expect("reclaimed physical accounting reads")
                .unique_content_bytes(),
            0
        );
        let replacement = seal_content_without_access_barrier(&db, bytes).await;
        assert_eq!(replacement.content_id(), sealed.content_id());
        assert_eq!(
            db.content_physical_quota(sealed.storage_domain_id())
                .await
                .expect("replacement physical accounting reads")
                .unique_content_bytes(),
            bytes.len() as u64
        );
        assert_eq!(
            db.content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("replacement clears tombstone"),
            None
        );
        let handle = db
            .open_content_leased(
                replacement.storage_domain_id(),
                replacement.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([60; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("replacement opens under a lease");
        assert_eq!(
            handle
                .read_range(0, u64::MAX)
                .await
                .expect("replacement bytes read")
                .as_ref(),
            bytes
        );
        db.close().await.expect("replacement db closes");
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[cfg(target_os = "wasi")]
#[test]
fn qualified_wasi_sweep_reclaims_after_process_reopen() {
    let path = PathBuf::from(format!(
        "wasi-content-reclaim-{}",
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let options = || {
        DbOptions::wasi_persistent(&path)
            .with_content_reclamation(ContentReclamationMode::QualifiedWasiFilesystem)
    };
    let sealed = block_on(async {
        let db = Db::open(options()).await.expect("WASI database opens");
        let sealed = seal_content_without_access_barrier(&db, b"WASI physical reclamation").await;
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier WASI handle opens");
        db.enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(130))
            .await
            .expect("WASI leased-only barrier commits");
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("pre-barrier WASI handle remains valid")
                .as_ref(),
            b"WASI physical reclamation"
        );
        drop(old);
        consume_reclaim_token(&db, sealed, 131).await;
        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(130))
            .await
            .expect("WASI barrier coordinate reads");
        db.attest_content_reader_drain(
            barrier,
            test_reader_drain_attestation_id(131),
            test_reader_drain_options(ContentReaderDrainKind::NativeProcessSetRestarted, 132),
        )
        .await
        .expect("WASI process-set drain attests");
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 133).await;
        let mut grace_tx = db.transaction(TransactionOptions::default());
        grace_tx
            .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
            .await
            .expect("WASI grace stages");
        grace_tx.commit().await.expect("WASI grace commits");
        let grace = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("WASI grace reads")
            .expect("WASI grace exists");
        let clock = ContentReclaimClockAttestation::new(
            grace,
            test_reclaim_clock_attestation_id(134),
            ContentReclaimClockCoordinatorId::from_bytes([135; 16]),
            ContentReclaimClockEvidenceDigest::for_bytes(b"WASI restart clock evidence"),
            grace.not_before_unix_ms(),
        )
        .expect("WASI clock binds grace");
        let mut prepare = db.transaction(TransactionOptions::default());
        let fresh = reclaim_authorization(&prepare, sealed, 136);
        assert_eq!(
            prepare
                .stage_content_reclaim_sweep(fresh, clock)
                .await
                .expect("WASI sweep stages"),
            ContentReclaimSweepStage::Staged
        );
        prepare.commit().await.expect("WASI Prepared commits");
        db.close().await.expect("WASI database closes at Prepared");
        sealed
    });

    block_on(async {
        let reopened = Db::open(options())
            .await
            .expect("WASI database reopens at Prepared");
        let sweep = reopened
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("WASI sweep resumes");
        assert!(sweep.reclaimed_at().is_some());
        assert!(matches!(
            reopened
                .open_content_leased(
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    ContentLeaseOptions::new(
                        ContentLeaseOwnerId::from_bytes([137; 16]),
                        Duration::from_secs(60),
                    ),
                )
                .await,
            Err(Error::ContentNotFound { .. })
        ));
        reopened
            .close()
            .await
            .expect("reclaimed WASI database closes");
    });
    // wasm32-wasip1's std directory removal may report DirectoryNotEmpty for
    // nested host-preopened paths even after every database handle closes. The
    // test namespace is unique and the host harness owns final fixture cleanup.
    let _ = std::fs::remove_dir_all(path);
}

#[cfg(not(feature = "s3"))]
#[test]
fn qualified_object_store_sweep_binds_evidence_and_recovers_partial_delete() {
    block_on(qualified_object_store_sweep_impl());
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread")]
async fn qualified_object_store_sweep_binds_evidence_and_recovers_partial_delete() {
    qualified_object_store_sweep_impl().await;
}

#[allow(clippy::too_many_lines)] // Provider evidence, reopen, fault, and retry form one safety proof.
async fn qualified_object_store_sweep_impl() {
    let prefix = "content-reclaim-sweep-object";
    let client = Arc::new(MeasuredClient::new());
    let probe_client: Arc<dyn ObjectClient> = client.clone();
    let evidence = ObjectStoreReclamationEvidenceDigest::for_bytes(
        b"test unversioned unlocked exclusive namespace revision 1",
    );
    let qualification = qualify_object_store_reclamation(
        Arc::clone(&probe_client),
        prefix,
        ObjectStoreReclamationAttestation::new(evidence),
    )
    .await
    .expect("object-store delete contract qualifies");
    let options = || {
        DbOptions::object_store().with_content_reclamation(
            ContentReclamationMode::QualifiedObjectStore(qualification.clone()),
        )
    };

    let db = Db::open_object_store_at(client.clone(), prefix, options())
        .await
        .expect("qualified object database opens");
    let sealed = seal_reclaim_content(&db, b"qualified object reclaim bytes").await;
    consume_reclaim_token(&db, sealed, 111).await;
    attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 112).await;
    let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 113).await;
    let mut grace_tx = db.transaction(TransactionOptions::default());
    grace_tx
        .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
        .await
        .expect("object grace stages");
    grace_tx.commit().await.expect("object grace commits");
    let grace = db
        .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("object grace reads")
        .expect("object grace exists");
    let clock = ContentReclaimClockAttestation::new(
        grace,
        test_reclaim_clock_attestation_id(114),
        ContentReclaimClockCoordinatorId::from_bytes([115; 16]),
        ContentReclaimClockEvidenceDigest::for_bytes(b"object clock evidence"),
        grace.not_before_unix_ms(),
    )
    .expect("object clock binds grace");
    let mut prepare = db.transaction(TransactionOptions::default());
    let fresh = reclaim_authorization(&prepare, sealed, 116);
    assert_eq!(
        prepare
            .stage_content_reclaim_sweep(fresh, clock)
            .await
            .expect("object sweep stages"),
        ContentReclaimSweepStage::Staged
    );
    prepare.commit().await.expect("object Prepared commits");
    let prepared = db
        .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("object Prepared reads")
        .expect("object Prepared exists");
    assert!(prepared.reclaimed_at().is_none());
    db.close().await.expect("object database closes");
    drop(db);

    let changed = qualify_object_store_reclamation(
        Arc::clone(&probe_client),
        prefix,
        ObjectStoreReclamationAttestation::new(ObjectStoreReclamationEvidenceDigest::for_bytes(
            b"test unversioned unlocked exclusive namespace revision 2",
        )),
    )
    .await
    .expect("changed evidence independently probes");
    let mismatched = Db::open_object_store_at(
        client.clone(),
        prefix,
        DbOptions::object_store()
            .with_content_reclamation(ContentReclamationMode::QualifiedObjectStore(changed)),
    )
    .await
    .expect("database opens with changed runtime evidence");
    assert!(matches!(
        mismatched
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await,
        Err(Error::UnsupportedBackend { .. })
    ));
    mismatched
        .close()
        .await
        .expect("mismatched database closes");
    drop(mismatched);

    let resumed = Db::open_object_store_at(client.clone(), prefix, options())
        .await
        .expect("database reopens with original evidence");
    client
        .report_provider_version
        .store(true, Ordering::Release);
    assert!(matches!(
        resumed
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await,
        Err(Error::UnsupportedBackend { .. })
    ));
    client
        .report_provider_version
        .store(false, Ordering::Release);
    client
        .fail_content_descriptor_delete_once
        .store(true, Ordering::Release);
    assert!(matches!(
        resumed
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await,
        Err(Error::Io(_))
    ));
    assert!(
        resumed
            .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("failed object sweep remains readable")
            .is_some_and(|sweep| sweep.reclaimed_at().is_none())
    );
    let reclaimed = resumed
        .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("object sweep resumes after injected delete failure");
    assert!(reclaimed.reclaimed_at().is_some());
    resumed.close().await.expect("reclaimed database closes");
    drop(resumed);
    assert!(
        client
            .list(prefix)
            .await
            .expect("reclaimed object prefix lists")
            .iter()
            .all(|meta| {
                !meta.key.contains("/content-v1/chunks/") && !meta.key.contains("/descriptors/")
            })
    );
    drop(probe_client);
    drop(client);
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires audited S3-compatible credentials and makes billable requests"]
#[allow(clippy::too_many_lines)] // Full provider qualification and reclaim lifecycle stays auditable.
async fn s3_live_qualified_content_reclamation() {
    use crate::s3::{ObjectStoreClient, S3ClientOptions};

    let Ok(bucket) = std::env::var("TRINE_S3_BUCKET") else {
        eprintln!("skipping live reclamation: TRINE_S3_BUCKET is not set");
        return;
    };
    let Ok(evidence) = std::env::var("TRINE_S3_RECLAMATION_EVIDENCE") else {
        eprintln!(
            "skipping live reclamation: set TRINE_S3_RECLAMATION_EVIDENCE only after auditing the exact provider namespace"
        );
        return;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".to_owned());
    let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    let allow_http = std::env::var("TRINE_S3_ALLOW_HTTP").is_ok_and(|value| value == "1");
    let client: Arc<dyn ObjectClient> = Arc::new(
        ObjectStoreClient::s3_with_options(
            bucket,
            region,
            S3ClientOptions {
                endpoint,
                allow_http,
            },
        )
        .expect("live S3-compatible client builds"),
    );
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let prefix = format!("trine-kv-it/content-reclaim/{}-{nonce}", std::process::id());
    let qualification = qualify_object_store_reclamation(
        Arc::clone(&client),
        &prefix,
        ObjectStoreReclamationAttestation::new(ObjectStoreReclamationEvidenceDigest::for_bytes(
            evidence.as_bytes(),
        )),
    )
    .await
    .expect("live provider reclamation contract qualifies");
    let options = || {
        DbOptions::object_store().with_content_reclamation(
            ContentReclamationMode::QualifiedObjectStore(qualification.clone()),
        )
    };

    let db = Db::open_object_store_at(Arc::clone(&client), &prefix, options())
        .await
        .expect("live qualified database opens");
    let sealed = seal_reclaim_content(&db, b"live provider physical reclaim bytes").await;
    consume_reclaim_token(&db, sealed, 121).await;
    attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 122).await;
    let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 123).await;
    let mut grace_tx = db.transaction(TransactionOptions::default());
    grace_tx
        .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
        .await
        .expect("live provider grace stages");
    grace_tx
        .commit()
        .await
        .expect("live provider grace commits");
    let grace = db
        .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("live provider grace reads")
        .expect("live provider grace exists");
    let clock = ContentReclaimClockAttestation::new(
        grace,
        test_reclaim_clock_attestation_id(124),
        ContentReclaimClockCoordinatorId::from_bytes([125; 16]),
        ContentReclaimClockEvidenceDigest::for_bytes(b"live provider monotonic clock evidence"),
        grace.not_before_unix_ms(),
    )
    .expect("live provider clock binds grace");
    let mut prepare = db.transaction(TransactionOptions::default());
    let fresh = reclaim_authorization(&prepare, sealed, 126);
    assert_eq!(
        prepare
            .stage_content_reclaim_sweep(fresh, clock)
            .await
            .expect("live provider sweep stages"),
        ContentReclaimSweepStage::Staged
    );
    prepare
        .commit()
        .await
        .expect("live provider Prepared commits");
    db.close().await.expect("live provider closes at Prepared");

    let reopened = Db::open_object_store_at(Arc::clone(&client), &prefix, options())
        .await
        .expect("live provider reopens at Prepared");
    let reclaimed = reopened
        .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("live provider sweep resumes");
    assert!(reclaimed.reclaimed_at().is_some());
    assert!(matches!(
        reopened
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([127; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
        Err(Error::ContentNotFound { .. })
    ));
    reopened
        .close()
        .await
        .expect("live provider database closes");

    let remaining = client.list(&prefix).await.expect("live prefix lists");
    assert!(
        remaining.iter().all(|meta| {
            !meta.key.contains("/content-v1/chunks/")
                && !meta.key.contains("/content-v1/descriptors/")
        }),
        "reclaimed content bytes remain in the provider namespace"
    );
    for meta in remaining {
        client
            .delete(&meta.key)
            .await
            .expect("live fixture object deletes");
    }
}

#[test]
fn refreshed_object_store_reader_observes_grace_without_content_delete() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let prefix = "content-reclaim-grace-object";
        let writer = Db::open_object_store_at(
            client.clone(),
            prefix,
            DbOptions::object_store()
                .with_content_reclamation(ContentReclamationMode::QualifiedNativeFilesystem),
        )
        .await
        .expect("object-store writer opens");
        let reader = Db::open_object_store_at(
            client.clone(),
            prefix,
            DbOptions::object_store().read_only(),
        )
        .await
        .expect("object-store reader opens");
        let bytes = b"object-store reclaim grace";
        let sealed = seal_content_without_access_barrier(&writer, bytes).await;
        consume_reclaim_token(&writer, sealed, 96).await;
        reader
            .refresh_object_store()
            .await
            .expect("reader refreshes sealed state");
        let old = reader
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier object-store handle opens");
        writer
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(52))
            .await
            .expect("object-store barrier commits");
        attest_reclaim_reader_drain(&writer, sealed.storage_domain_id(), 52).await;
        let (authorization, _) = commit_reclaim_quarantine(&writer, sealed, 75).await;

        client.reset_counts();
        let mut grace = writer.transaction(TransactionOptions::default());
        grace
            .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
            .await
            .expect("object-store grace stages");
        grace.commit().await.expect("object-store grace commits");
        assert_eq!(client.counts().content_delete, 0);

        let grace_record = writer
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("object-store grace reads")
            .expect("object-store grace exists");
        let clock = ContentReclaimClockAttestation::new(
            grace_record,
            test_reclaim_clock_attestation_id(61),
            ContentReclaimClockCoordinatorId::from_bytes([62; 16]),
            ContentReclaimClockEvidenceDigest::for_bytes(b"object store must stay fail closed"),
            grace_record.not_before_unix_ms(),
        )
        .expect("clock claim is well formed");
        let mut unsupported = writer.transaction(TransactionOptions::default());
        let fresh = reclaim_authorization(&unsupported, sealed, 78);
        assert!(matches!(
            unsupported.stage_content_reclaim_sweep(fresh, clock).await,
            Err(Error::UnsupportedBackend { .. })
        ));
        assert_eq!(client.counts().content_delete, 0);

        reader
            .refresh_object_store()
            .await
            .expect("reader refreshes grace state");
        assert_eq!(
            reader
                .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("refreshed reader sees grace"),
            writer
                .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("writer sees grace")
        );
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("old object-store handle still reads")
                .as_ref(),
            bytes
        );
    });
}

#[test]
fn upload_activity_racing_staged_reclaim_grace_forces_conflict() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"reclaim grace upload race";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 95).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 49).await;
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 74).await;
        let mut grace = db.transaction(TransactionOptions::default());
        grace
            .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
            .await
            .expect("grace stages before upload activity");

        let repeated = seal_content_without_access_barrier(&db, bytes).await;
        assert_eq!(repeated.content_id(), sealed.content_id());
        assert!(matches!(grace.commit().await, Err(Error::Conflict { .. })));
        assert!(
            db.content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("raced grace remains absent")
                .is_none()
        );
        assert!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("upload revival removes quarantine")
                .is_none()
        );
    });
}

#[test]
fn malformed_reclaim_grace_blocks_query_and_revival() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"malformed reclaim grace";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 93).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 46).await;
        commit_reclaim_quarantine(&db, sealed, 73).await;

        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_reclaim_grace_key(sealed.storage_domain_id(), sealed.content_id()),
                b"damaged reclaim grace".to_vec(),
            )
            .expect("malformed grace stages");
        damage.commit().await.expect("malformed grace commits");
        assert!(matches!(
            db.content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        let mut unsafe_caller = db.transaction(TransactionOptions::default());
        assert!(matches!(
            unsafe_caller
                .stage_content_activity(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        unsafe_caller
            .commit()
            .await
            .expect("committing after validation error writes no lifecycle change");
        assert!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("failed revival retained quarantine")
                .is_some()
        );
        let mut upload = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("revival upload begins");
        upload.write(bytes).await.expect("revival bytes write");
        assert!(matches!(
            upload.seal().await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("quarantine remains readable")
                .is_some()
        );
    });
}

#[test]
fn malformed_reclaim_sweep_blocks_query_and_authoritative_activity() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"malformed reclaim sweep").await;
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_reclaim_sweep_key(sealed.storage_domain_id(), sealed.content_id()),
                b"damaged reclaim sweep".to_vec(),
            )
            .expect("malformed sweep stages");
        damage.commit().await.expect("malformed sweep commits");

        assert!(matches!(
            db.content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        let mut activity = db.transaction(TransactionOptions::default());
        assert!(matches!(
            activity
                .stage_content_activity(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        activity
            .commit()
            .await
            .expect("failed validation stages no lifecycle writes");
    });
}

#[test]
fn reclaim_intent_is_blocked_by_lease_and_later_lease_supersedes_it() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"leased reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 42).await;
        let handle = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([12; 16]),
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("leased content opens");
        let mut blocked = db.transaction(TransactionOptions::default());
        assert!(matches!(
            blocked
                .stage_content_reclaim_intent(reclaim_authorization(&blocked, sealed, 33))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReadLease { .. },
            })
        ));
        thread::sleep(Duration::from_millis(10));

        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 34);
        accepted
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("expired lease permits intent");
        accepted.commit().await.expect("reclaim intent commits");

        let newer = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([13; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("new lease cancels intent");
        assert!(newer.lease_id().is_some());
        assert!(handle.lease_id().is_some());
        let mut stale = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale.stage_content_reclaim_intent(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded { .. },
            })
        ));
    });
}

#[test]
fn concurrent_lease_conflicts_with_staged_reclaim_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"concurrent reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 43).await;
        let mut reclaim = db.transaction(TransactionOptions::default());
        reclaim
            .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 35))
            .await
            .expect("reclaim intent stages");

        db.open_content_leased(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentLeaseOptions::new(
                ContentLeaseOwnerId::from_bytes([14; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("concurrent lease commits");
        assert!(matches!(
            reclaim.commit().await,
            Err(Error::Conflict { .. })
        ));
    });
}

#[test]
fn new_upload_authority_supersedes_reclaim_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"repeated content bytes").await;
        consume_reclaim_token(&db, sealed, 45).await;
        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 38);
        accepted
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        accepted.commit().await.expect("reclaim intent commits");

        let newer = seal_reclaim_content(&db, b"repeated content bytes").await;
        assert_eq!(newer.content_id(), sealed.content_id());
        let mut stale = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale.stage_content_reclaim_intent(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded { .. },
            })
        ));
    });
}

#[test]
fn malformed_control_and_expired_proof_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"malformed reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 44).await;
        let mut expired = db.transaction(TransactionOptions::default());
        let expired_authorization = ContentReclaimAuthorization::new(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentReclaimProofToken::from_bytes([36; 49]),
            expired.read_version(),
            1,
        );
        assert!(matches!(
            expired
                .stage_content_reclaim_intent(expired_authorization)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ProofExpired { .. },
            })
        ));

        let token_damaged = seal_reclaim_content(&db, b"malformed token index bytes").await;
        let mut damage_token = db.transaction(TransactionOptions::default());
        damage_token
            .put_internal_bucket(
                CONTENT_TOKEN_INDEX_BUCKET,
                content_token_index_key(
                    token_damaged.storage_domain_id(),
                    token_damaged.content_id(),
                    token_damaged.upload_token(),
                ),
                b"malformed".to_vec(),
            )
            .expect("token-index damage stages");
        damage_token
            .commit()
            .await
            .expect("token-index damage commits");
        let mut token_reclaim = db.transaction(TransactionOptions::default());
        assert!(matches!(
            token_reclaim
                .stage_content_reclaim_intent(reclaim_authorization(
                    &token_reclaim,
                    token_damaged,
                    39,
                ))
                .await,
            Err(Error::InvalidFormat { .. })
        ));

        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_control_key(sealed.storage_domain_id(), sealed.content_id()),
                b"malformed".to_vec(),
            )
            .expect("damage stages");
        damage.commit().await.expect("damage commits");
        let mut reclaim = db.transaction(TransactionOptions::default());
        assert!(matches!(
            reclaim
                .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 37))
                .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn physical_hold_blocks_intent_release_allows_it_and_later_hold_supersedes() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"physical hold reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 46).await;
        let owner = ContentPhysicalHoldOwnerId::from_bytes([17; 16]);
        let hold_id = test_hold_id(22);
        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::until_released(ContentPhysicalHoldKind::Backup, owner),
            )
            .await
            .expect("backup hold acquires");
        let repeated = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::until_released(ContentPhysicalHoldKind::Backup, owner),
            )
            .await
            .expect("lost-response acquisition retries exactly");
        assert_eq!(repeated.id(), hold.id());
        assert_eq!(repeated.expires_at_unix_ms(), None);
        let clone = hold.clone();
        let mut blocked = db.transaction(TransactionOptions::default());
        assert!(matches!(
            blocked
                .stage_content_reclaim_intent(reclaim_authorization(&blocked, sealed, 40))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::PhysicalHold {
                    kind: ContentPhysicalHoldKind::Backup,
                    expires_at_unix_ms: None,
                    ..
                },
            })
        ));

        hold.release().await.expect("backup hold releases");
        assert!(clone.is_released());
        clone.release().await.expect("release retry is idempotent");
        assert!(matches!(
            db.resume_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                owner,
            )
            .await,
            Err(Error::ContentPhysicalHoldNotFound { .. })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::until_released(ContentPhysicalHoldKind::Backup, owner,),
            )
            .await,
            Err(Error::ContentPhysicalHoldNotFound { .. })
        ));
        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 41);
        accepted
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("released hold permits intent");
        accepted.commit().await.expect("reclaim intent commits");

        let migration = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(23),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Migration,
                    owner,
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("later migration hold acquires");
        assert!(migration.expires_at_unix_ms().is_some());
        let mut stale = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale.stage_content_reclaim_intent(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded { .. },
            })
        ));
    });
}

#[test]
fn every_physical_hold_kind_enters_the_same_reclaim_fence() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        for (index, kind) in [
            ContentPhysicalHoldKind::Migration,
            ContentPhysicalHoldKind::Backup,
            ContentPhysicalHoldKind::Repair,
            ContentPhysicalHoldKind::Provider,
            ContentPhysicalHoldKind::Administrative,
            ContentPhysicalHoldKind::Processing,
            ContentPhysicalHoldKind::Offline,
        ]
        .into_iter()
        .enumerate()
        {
            let label = format!("physical hold class {kind}");
            let sealed = seal_reclaim_content(&db, label.as_bytes()).await;
            let change = u8::try_from(60 + index).expect("test change fits");
            consume_reclaim_token(&db, sealed, change).await;
            let hold = db
                .acquire_content_physical_hold(
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    test_hold_id(change),
                    ContentPhysicalHoldOptions::until_released(
                        kind,
                        ContentPhysicalHoldOwnerId::from_bytes([change; 16]),
                    ),
                )
                .await
                .expect("physical hold class acquires");
            let mut blocked = db.transaction(TransactionOptions::default());
            let result = blocked
                .stage_content_reclaim_intent(reclaim_authorization(
                    &blocked,
                    sealed,
                    change.saturating_add(10),
                ))
                .await;
            assert!(matches!(
                result,
                Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::PhysicalHold {
                        kind: blocked_kind,
                        ..
                    },
                }) if blocked_kind == kind
            ));
            hold.release().await.expect("physical hold class releases");
        }
    });
}

#[test]
fn concurrent_physical_hold_conflicts_with_staged_reclaim_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"concurrent physical hold bytes").await;
        consume_reclaim_token(&db, sealed, 47).await;
        let mut reclaim = db.transaction(TransactionOptions::default());
        reclaim
            .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 42))
            .await
            .expect("reclaim intent stages");

        db.acquire_content_physical_hold(
            sealed.storage_domain_id(),
            sealed.content_id(),
            test_hold_id(24),
            ContentPhysicalHoldOptions::expiring(
                ContentPhysicalHoldKind::Repair,
                ContentPhysicalHoldOwnerId::from_bytes([18; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("concurrent repair hold commits");
        assert!(matches!(
            reclaim.commit().await,
            Err(Error::Conflict { .. })
        ));
    });
}

#[test]
fn physical_hold_renewal_expiry_and_owner_checks_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"expiring physical hold bytes").await;
        consume_reclaim_token(&db, sealed, 48).await;
        let owner = ContentPhysicalHoldOwnerId::from_bytes([19; 16]);
        let hold_id = test_hold_id(25);
        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Provider,
                    owner,
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("provider hold acquires");
        let before = hold.expires_at_unix_ms().expect("expiry exists");
        let renewed = hold
            .renew(Duration::from_mins(2))
            .await
            .expect("provider hold renews");
        assert!(renewed >= before);
        assert_eq!(hold.expires_at_unix_ms(), Some(renewed));
        assert!(matches!(
            db.resume_content_physical_hold(
                hold.storage_domain_id(),
                hold.content_id(),
                hold.id(),
                ContentPhysicalHoldOwnerId::from_bytes([20; 16]),
            )
            .await,
            Err(Error::ContentPhysicalHoldOwnerMismatch)
        ));

        let short = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(26),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Provider,
                    owner,
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("short provider hold acquires");
        thread::sleep(Duration::from_millis(10));
        assert!(matches!(
            short.renew(Duration::from_mins(1)).await,
            Err(Error::ContentPhysicalHoldExpired { .. })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                short.storage_domain_id(),
                short.content_id(),
                short.id(),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Provider,
                    owner,
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentPhysicalHoldExpired { .. })
        ));
        assert!(matches!(
            db.resume_content_physical_hold(
                short.storage_domain_id(),
                short.content_id(),
                short.id(),
                owner,
            )
            .await,
            Err(Error::ContentPhysicalHoldExpired { .. })
        ));
    });
}

#[test]
fn physical_hold_survives_native_reopen_and_malformed_state_blocks_intent() {
    let path = temp_db_path("content-physical-hold-native");
    let (sealed, hold_id, owner) = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let sealed = seal_reclaim_content(&db, b"durable physical hold bytes").await;
        consume_reclaim_token(&db, sealed, 49).await;
        let owner = ContentPhysicalHoldOwnerId::from_bytes([21; 16]);
        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(27),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Administrative,
                    owner,
                ),
            )
            .await
            .expect("administrative hold acquires");
        (sealed, hold.id(), owner)
    });

    block_on(async {
        let db = Db::open(DbOptions::persistent_read_only(&path))
            .await
            .expect("native read-only db reopens");
        let hold = db
            .resume_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                owner,
            )
            .await
            .expect("administrative hold resumes");
        assert_eq!(hold.kind(), ContentPhysicalHoldKind::Administrative);
        assert_eq!(hold.expires_at_unix_ms(), None);
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native writable db reopens");
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_PHYSICAL_HOLD_BUCKET,
                content_physical_hold_key(
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    ContentPhysicalHoldId::from_bytes([1; 16]).expect("test hold id decodes"),
                ),
                b"malformed".to_vec(),
            )
            .expect("hold damage stages");
        damage.commit().await.expect("hold damage commits");
        let mut reclaim = db.transaction(TransactionOptions::default());
        assert!(matches!(
            reclaim
                .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 43))
                .await,
            Err(Error::InvalidFormat { .. })
        ));
    });

    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn lifecycle_vacuum_removes_expired_authority_and_preserves_active_indefinite_holds() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(ContentUploadOptions::new(
                test_scope(),
                Duration::from_millis(2),
            ))
            .await
            .expect("short-token upload begins");
        upload.write(b"vacuum").await.expect("content writes");
        let sealed = upload.seal().await.expect("content seals");
        let lease = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([71; 16]),
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("short lease opens");
        let expiring = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(71),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Repair,
                    ContentPhysicalHoldOwnerId::from_bytes([72; 16]),
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("short hold acquires");
        let indefinite_owner = ContentPhysicalHoldOwnerId::from_bytes([73; 16]);
        let indefinite = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(72),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Administrative,
                    indefinite_owner,
                ),
            )
            .await
            .expect("indefinite hold acquires");

        thread::sleep(Duration::from_millis(10));
        let cutoff = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_millis(),
        )
        .expect("current time fits u64");
        let report = db
            .vacuum_content_lifecycle(cutoff)
            .await
            .expect("lifecycle vacuum commits");
        assert_eq!(report.scanned(), 4);
        assert_eq!(report.expired_tokens_removed(), 1);
        assert_eq!(report.expired_leases_removed(), 1);
        assert_eq!(report.inactive_holds_removed(), 1);
        assert!(
            !db.content_has_active_lease(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("lease index reads")
        );
        assert!(matches!(
            db.resume_content_physical_hold(
                expiring.storage_domain_id(),
                expiring.content_id(),
                expiring.id(),
                expiring.owner_id(),
            )
            .await,
            Err(Error::ContentPhysicalHoldNotFound { .. })
        ));
        db.resume_content_physical_hold(
            indefinite.storage_domain_id(),
            indefinite.content_id(),
            indefinite.id(),
            indefinite_owner,
        )
        .await
        .expect("active indefinite hold remains");
        drop(lease);
    });
}

#[test]
fn content_id_portable_bytes_round_trip_and_reject_unknown_algorithms() {
    let id = ContentId::for_bytes(b"portable content identity");
    assert_eq!(
        ContentId::from_bytes(id.to_bytes()).expect("identity decodes"),
        id
    );

    let mut unknown = id.to_bytes();
    unknown[0] = u8::MAX;
    assert!(matches!(
        ContentId::from_bytes(unknown),
        Err(Error::UnsupportedFormat { .. })
    ));
}

#[test]
fn leased_content_open_clone_renew_and_expiry_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("upload begins");
        upload.write(b"leased bytes").await.expect("bytes write");
        let sealed = upload.seal().await.expect("upload seals");
        let owner = ContentLeaseOwnerId::from_bytes([9_u8; 16]);
        let handle = db
            .open_content_leased(
                test_scope().storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(owner, Duration::from_mins(1)),
            )
            .await
            .expect("leased content opens");
        let clone = handle.clone();
        let lease_id = handle.lease_id().expect("lease identity exists");
        assert_eq!(clone.lease_id(), Some(lease_id));
        assert_eq!(
            handle
                .read_range(0, u64::MAX)
                .await
                .expect("leased range reads")
                .as_ref(),
            b"leased bytes"
        );
        assert!(
            db.content_has_active_lease(test_scope().storage_domain_id(), sealed.content_id())
                .await
                .expect("active lease probes")
        );

        let before = handle.lease_expires_at_unix_ms().expect("deadline exists");
        let renewed = clone
            .renew_lease(Duration::from_mins(2))
            .await
            .expect("lease renews");
        assert!(renewed >= before);
        assert_eq!(handle.lease_expires_at_unix_ms(), Some(renewed));

        let lease = handle.lease().expect("private lease exists");
        let expired = ContentLeaseRecord {
            lease_id,
            owner_id: owner,
            storage_domain_id: handle.storage_domain_id(),
            content_id: handle.content_id(),
            expires_at_unix_ms: 0,
        };
        let mut expire = db.transaction(TransactionOptions::default());
        expire
            .put_internal_bucket(
                CONTENT_LEASE_BUCKET,
                content_lease_key(handle.storage_domain_id(), handle.content_id(), lease_id),
                expired.encode(),
            )
            .expect("expired record stages");
        expire.commit().await.expect("expired record commits");
        lease.publish_expiry(0);
        assert!(matches!(
            handle.read_range(0, 1).await,
            Err(Error::ContentLeaseExpired { .. })
        ));
        assert!(matches!(
            clone.renew_lease(Duration::from_mins(1)).await,
            Err(Error::ContentLeaseExpired { .. })
        ));
        assert!(
            !db.content_has_active_lease(test_scope().storage_domain_id(), sealed.content_id())
                .await
                .expect("expired lease probes inactive")
        );

        let unleased = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("ordinary content opens");
        assert!(matches!(
            unleased.renew_lease(Duration::from_secs(1)).await,
            Err(Error::ContentLeaseNotFound { .. })
        ));
        assert!(matches!(
            db.open_content_leased(
                test_scope().storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(owner, Duration::ZERO),
            )
            .await,
            Err(Error::InvalidOptions { .. })
        ));
    });
}

#[test]
fn durable_content_lease_survives_reopen_and_malformed_state_fails_closed() {
    let path = temp_db_path("content-lease-native");
    let (domain, content_id, lease_id) = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("upload begins");
        upload
            .write(b"persistent lease")
            .await
            .expect("bytes write");
        let sealed = upload.seal().await.expect("upload seals");
        let domain = test_scope().storage_domain_id();
        let handle = db
            .open_content_leased(
                domain,
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([10_u8; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("leased content opens");
        (
            domain,
            sealed.content_id(),
            handle.lease_id().expect("lease exists"),
        )
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        assert!(
            db.content_has_active_lease(domain, content_id)
                .await
                .expect("durable lease reopens")
        );
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_LEASE_BUCKET,
                content_lease_key(domain, content_id, lease_id),
                b"malformed".to_vec(),
            )
            .expect("damage stages");
        damage.commit().await.expect("damage commits");
        assert!(matches!(
            db.content_has_active_lease(domain, content_id).await,
            Err(Error::InvalidFormat { .. })
        ));
    });

    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn token_expiry_overflow_does_not_publish_content() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(ContentUploadOptions::new(
                test_scope(),
                Duration::from_millis(u64::MAX),
            ))
            .await
            .expect("upload begins before seal fixes the expiry epoch");
        upload.write(b"unpublished").await.expect("bytes write");
        let upload_id = upload.upload_id();
        let content_id = ContentId::for_bytes(b"unpublished");

        assert!(matches!(
            upload.seal().await,
            Err(Error::InvalidOptions { .. })
        ));
        assert!(matches!(
            db.open_content(test_scope().storage_domain_id(), content_id)
                .await,
            Err(Error::ContentNotFound { .. })
        ));
        assert!(
            db.resume_content_upload(upload_id)
                .await
                .expect("upload remains resumable")
                .into_open()
                .is_some()
        );
    });
}

#[test]
fn memory_content_ranges_stream_and_expectations_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut bytes = vec![0_u8; TEST_CHUNK_BYTES * 2 + 37];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::try_from(index % 251).expect("pattern is within u8");
        }

        let mut upload = db
            .begin_content_upload(
                test_upload_options()
                    .with_chunk_bytes(TEST_CHUNK_BYTES)
                    .with_expected_length(bytes.len() as u64)
                    .with_expected_content_id(ContentId::for_bytes(&bytes)),
            )
            .await
            .expect("upload begins");
        for piece in bytes.chunks(7_919) {
            upload.write(piece).await.expect("piece writes");
            assert!(upload.buffered_bytes() <= TEST_CHUNK_BYTES);
        }
        let sealed = upload.seal().await.expect("upload seals");
        assert!(matches!(
            db.open_content(
                StorageDomainId::from_bytes([88_u8; 16]),
                sealed.content_id(),
            )
            .await,
            Err(Error::ContentNotFound { .. })
        ));
        let handle = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("sealed content opens");

        let start = TEST_CHUNK_BYTES as u64 - 11;
        let start_index = usize::try_from(start).expect("test range start fits usize");
        assert_eq!(
            handle
                .read_range(start, 29)
                .await
                .expect("cross-chunk range reads")
                .as_ref(),
            &bytes[start_index..start_index + 29]
        );
        assert!(
            handle
                .read_range(handle.len(), 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(handle.read_range(0, 0).await.unwrap().is_empty());
        assert!(matches!(
            handle.read_range(handle.len() + 1, 1).await,
            Err(Error::ContentRangeOutOfBounds { .. })
        ));
        handle.verify().await.expect("complete digest verifies");

        let mut stream = handle.stream();
        let mut streamed = 0_usize;
        while let Some(piece) = stream.next().await.expect("stream chunk reads") {
            assert!(piece.len() <= TEST_CHUNK_BYTES);
            streamed += piece.len();
        }
        assert_eq!(streamed, bytes.len());

        let mut mismatch = db
            .begin_content_upload(
                test_upload_options()
                    .with_chunk_bytes(TEST_CHUNK_BYTES)
                    .with_expected_length(1),
            )
            .await
            .expect("mismatch upload begins");
        let mismatch_id = mismatch.upload_id();
        assert!(matches!(
            mismatch.write(b"two").await,
            Err(Error::ContentLengthMismatch {
                expected: 1,
                actual: 3
            })
        ));
        assert_eq!(mismatch.len(), 0);
        let resumed = db
            .resume_content_upload(mismatch_id)
            .await
            .expect("rejected write leaves resumable upload state")
            .into_open()
            .expect("rejected write leaves the upload open");
        assert_eq!(resumed.len(), 0);
        drop(resumed);
        mismatch
            .write(b"x")
            .await
            .expect("valid replacement byte writes");
        mismatch.seal().await.expect("exact-length upload seals");
    });
}

#[test]
fn native_upload_resumes_across_reopen_and_remembers_seal() {
    let path = temp_db_path("content-resume-native");
    let mut bytes = vec![0_u8; TEST_CHUNK_BYTES * 2 + 37];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::try_from(index % 239).expect("pattern is within u8");
    }
    let first_len = TEST_CHUNK_BYTES + 123;

    let upload_id = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload
            .write(&bytes[..first_len])
            .await
            .expect("durable prefix writes");
        assert_eq!(upload.len(), first_len as u64);
        assert_eq!(upload.buffered_bytes(), 123);
        let upload_id = upload.upload_id();
        drop(upload);
        drop(db);
        upload_id
    });

    let sealed = block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        let mut resumed = match db
            .resume_content_upload(upload_id)
            .await
            .expect("upload resumes")
        {
            ContentUploadResume::Open(upload) => upload,
            ContentUploadResume::Sealed(_) => panic!("upload unexpectedly sealed"),
        };
        assert_eq!(resumed.len(), first_len as u64);
        assert_eq!(resumed.buffered_bytes(), 123);
        resumed
            .write(&bytes[first_len..])
            .await
            .expect("remaining bytes write");
        let sealed = resumed.seal().await.expect("resumed upload seals");
        assert_eq!(sealed.content_id(), ContentId::for_bytes(&bytes));
        drop(db);
        sealed
    });

    block_on(async {
        let db = Db::open(&path).await.expect("sealed db reopens");
        let remembered = db
            .resume_content_upload(upload_id)
            .await
            .expect("sealed upload state reopens")
            .sealed()
            .expect("seal result is remembered");
        assert_eq!(remembered, sealed);
        assert_eq!(
            db.seal_content_upload(upload_id)
                .await
                .expect("seal retry is idempotent"),
            sealed
        );
        let handle = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("resumed content opens");
        handle.verify().await.expect("resumed content verifies");
        let mut transaction = db.transaction(TransactionOptions::default());
        let attached = transaction
            .consume_upload_token(
                sealed.upload_token(),
                test_scope(),
                ContentChangeId::from_bytes([21_u8; 16]),
            )
            .await
            .expect("reopened token stages consumption");
        assert_eq!(attached.content_id(), sealed.content_id());
        transaction.put(b"native:attached", b"yes");
        transaction
            .commit()
            .await
            .expect("reopened token and marker commit");
        drop(handle);
        drop(db);
    });

    std::fs::remove_dir_all(&path).expect("test database removes");
}

#[test]
fn stale_upload_writer_conflicts_with_newer_revision() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut first = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        let upload_id = first.upload_id();
        let mut stale = db
            .resume_content_upload(upload_id)
            .await
            .expect("second writer resumes")
            .into_open()
            .expect("upload is open");

        first.write(b"winner").await.expect("first writer advances");
        assert!(matches!(
            stale.write(b"stale").await,
            Err(Error::ContentUploadConflict {
                expected_revision: 0,
                actual_revision: 1,
                ..
            })
        ));
        first.abort().await.expect("winning session aborts");
    });
}

#[test]
fn upload_token_rejects_wrong_scope_expiry_and_invalid_bearer() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload.write(b"attach me").await.expect("content writes");
        let sealed = upload.seal().await.expect("content seals");
        let token = sealed.upload_token();
        let change_a = ContentChangeId::from_bytes([11_u8; 16]);
        let before_expiry = sealed.token_expires_at_unix_ms() - 1;

        let wrong_scope = ContentAttachmentScope::new(
            test_scope().storage_domain_id(),
            OwnerScopeId::from_bytes([99_u8; 16]),
        );
        let mut wrong = db.transaction(TransactionOptions::default());
        assert!(matches!(
            wrong
                .consume_upload_token_at(token, wrong_scope, change_a, before_expiry)
                .await,
            Err(Error::UploadTokenScopeMismatch)
        ));

        let mut expired = db.transaction(TransactionOptions::default());
        assert!(matches!(
            expired
                .consume_upload_token_at(
                    token,
                    test_scope(),
                    change_a,
                    sealed.token_expires_at_unix_ms(),
                )
                .await,
            Err(Error::UploadTokenExpired { .. })
        ));

        let mut invalid_bytes = token.to_bytes();
        invalid_bytes[32] ^= 0xff;
        let invalid_token = UploadToken::from_bytes(invalid_bytes).expect("version remains valid");
        let mut invalid = db.transaction(TransactionOptions::default());
        assert!(matches!(
            invalid
                .consume_upload_token_at(invalid_token, test_scope(), change_a, before_expiry,)
                .await,
            Err(Error::UploadTokenInvalid)
        ));
    });
}

#[test]
fn upload_token_consumption_is_idempotent_and_atomic_with_catalog_writes() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload.write(b"attach me").await.expect("content writes");
        let sealed = upload.seal().await.expect("content seals");
        let token = sealed.upload_token();
        let change_a = ContentChangeId::from_bytes([11_u8; 16]);
        let change_b = ContentChangeId::from_bytes([12_u8; 16]);
        let before_expiry = sealed.token_expires_at_unix_ms() - 1;

        let mut abandoned = db.transaction(TransactionOptions::default());
        let claims = abandoned
            .consume_upload_token_at(token, test_scope(), change_a, before_expiry)
            .await
            .expect("available token stages");
        assert_eq!(claims.content_id(), sealed.content_id());
        abandoned.put(b"catalog:abandoned", b"must not commit");
        drop(abandoned);
        assert_eq!(
            db.get(b"catalog:abandoned").await.expect("marker reads"),
            None
        );

        let mut committed = db.transaction(TransactionOptions::default());
        committed
            .consume_upload_token_at(token, test_scope(), change_a, before_expiry)
            .await
            .expect("dropped transaction left token available");
        committed.put(b"catalog:file", b"visible");
        committed.commit().await.expect("token and catalog commit");
        assert_eq!(
            db.get(b"catalog:file").await.expect("catalog reads"),
            Some(b"visible".to_vec())
        );

        let mut idempotent = db.transaction(TransactionOptions::default());
        let repeated = idempotent
            .consume_upload_token_at(
                token,
                test_scope(),
                change_a,
                sealed.token_expires_at_unix_ms() + 1,
            )
            .await
            .expect("same committed ChangeId remains idempotent after expiry");
        assert_eq!(repeated, claims);
        idempotent.commit().await.expect("idempotent retry commits");

        let mut sync_retry = db.transaction(TransactionOptions::default());
        assert_eq!(
            sync_retry
                .consume_upload_token_sync(token, test_scope(), change_a)
                .expect("sync API returns the same committed claims"),
            claims
        );
        sync_retry
            .commit_sync()
            .expect("sync idempotent retry commits");

        let mut reused = db.transaction(TransactionOptions::default());
        assert!(matches!(
            reused
                .consume_upload_token_at(token, test_scope(), change_b, before_expiry)
                .await,
            Err(Error::UploadTokenAlreadyConsumed)
        ));
    });
}

#[test]
fn competing_token_consumptions_commit_with_exactly_one_catalog_write() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let change_a = ContentChangeId::from_bytes([11_u8; 16]);
        let change_b = ContentChangeId::from_bytes([12_u8; 16]);
        let mut second_upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("second upload begins");
        second_upload
            .write(b"conflict content")
            .await
            .expect("second content writes");
        let second = second_upload.seal().await.expect("second content seals");
        let second_before_expiry = second.token_expires_at_unix_ms() - 1;
        let mut loser = db.transaction(TransactionOptions::default());
        loser
            .consume_upload_token_at(
                second.upload_token(),
                test_scope(),
                change_a,
                second_before_expiry,
            )
            .await
            .expect("loser stages token");
        loser.put(b"catalog:loser", b"must not commit");
        let mut winner = db.transaction(TransactionOptions::default());
        winner
            .consume_upload_token_at(
                second.upload_token(),
                test_scope(),
                change_b,
                second_before_expiry,
            )
            .await
            .expect("winner stages same available token");
        winner.put(b"catalog:winner", b"committed");
        winner.commit().await.expect("winner commits first");
        assert!(matches!(loser.commit().await, Err(Error::Conflict { .. })));
        assert_eq!(db.get(b"catalog:loser").await.expect("loser reads"), None);
        assert_eq!(
            db.get(b"catalog:winner").await.expect("winner reads"),
            Some(b"committed".to_vec())
        );
    });
}

#[test]
fn native_content_exceeds_ordinary_value_limit_and_reopens() {
    let path = temp_db_path("content-large-native");
    let content_id = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let one_mib = 1024 * 1024;
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(one_mib))
            .await
            .expect("large upload begins");
        let mut piece = vec![0_u8; one_mib];
        for part in 0_u8..65 {
            piece.fill(part);
            upload.write(&piece).await.expect("large piece writes");
            assert_eq!(upload.buffered_bytes(), 0);
        }
        assert_eq!(upload.len(), 65 * one_mib as u64);
        let sealed = upload.seal().await.expect("large upload seals");
        drop(db);
        sealed.content_id()
    });

    block_on(async {
        let reopened = Db::open(&path).await.expect("native db reopens");
        let handle = reopened
            .open_content(test_scope().storage_domain_id(), content_id)
            .await
            .expect("large content reopens");
        assert_eq!(handle.len(), 65 * 1024 * 1024);
        let boundary = handle
            .read_range(1024 * 1024 - 8, 16)
            .await
            .expect("native boundary range reads");
        assert_eq!(&boundary[..8], &[0_u8; 8]);
        assert_eq!(&boundary[8..], &[1_u8; 8]);
        handle.verify().await.expect("reopened content verifies");
        drop(handle);
        drop(reopened);
    });

    std::fs::remove_dir_all(&path).expect("test database removes");
}

#[test]
fn object_store_counts_requests_detects_tampering_and_hides_failed_seal() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db =
            Db::open_object_store_at(client.clone(), "content-probe", DbOptions::object_store())
                .await
                .expect("object db opens");
        client.reset_counts();

        let bytes = vec![7_u8; TEST_CHUNK_BYTES * 2 + 17];
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("object upload begins");
        upload.write(&bytes).await.expect("object bytes write");
        let sealed = upload.seal().await.expect("object upload seals");
        let after_seal = client.counts();
        assert_eq!(
            after_seal.put, 8,
            "content state, chunks, descriptor, and upload state use ordinary immutable writes"
        );
        assert_eq!(
            after_seal.put_if, 12,
            "six database commits each create one immutable WAL segment and CAS the durable head"
        );

        let handle = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("object content opens");
        let after_open = client.counts();
        assert_eq!(
            after_open.head - after_seal.head,
            2,
            "open checks the domain access barrier before the descriptor"
        );
        assert_eq!(after_open.get_range - after_seal.get_range, 1);

        let range = handle
            .read_range(TEST_CHUNK_BYTES as u64 - 5, 10)
            .await
            .expect("object boundary range reads");
        assert_eq!(range.as_ref(), &[7_u8; 10]);
        let after_range = client.counts();
        assert_eq!(after_range.head - after_open.head, 2);
        assert_eq!(after_range.get_range - after_open.get_range, 2);
        assert_eq!(after_range.get, 0, "content reads avoid whole-object GET");

        let chunk = client
            .inner
            .list("content-probe/content-v1/chunks")
            .await
            .expect("chunks list")
            .into_iter()
            .next()
            .expect("at least one chunk");
        let mut corrupted = client
            .inner
            .get(&chunk.key)
            .await
            .expect("chunk reads")
            .expect("chunk exists")
            .to_vec();
        let last = corrupted.last_mut().expect("chunk has payload");
        *last ^= 0xff;
        client
            .inner
            .put(&chunk.key, Arc::from(corrupted))
            .await
            .expect("corrupt chunk replaces");
        assert!(matches!(
            handle.read_range(0, 1).await,
            Err(Error::ContentDigestMismatch { .. })
        ));

        drop(handle);
        drop(db);

        let failed_client = Arc::new(MeasuredClient::new());
        let failed_db = Db::open_object_store_at(
            failed_client.clone(),
            "failed-content-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("failure db opens");
        failed_client
            .fail_descriptors
            .store(true, Ordering::Release);
        let expected = ContentId::for_bytes(b"not published");
        let mut failed_upload = failed_db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("failed upload begins");
        failed_upload
            .write(b"not published")
            .await
            .expect("failed upload chunk buffers");
        assert!(failed_upload.seal().await.is_err());
        assert!(matches!(
            failed_db
                .open_content(test_scope().storage_domain_id(), expected)
                .await,
            Err(Error::ContentNotFound { .. })
        ));
        drop(failed_db);
    });
}

#[test]
fn seal_retry_repairs_descriptor_session_crash_window() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db = Db::open_object_store_at(
            client.clone(),
            "seal-retry-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("object db opens");
        let bytes = vec![11_u8; TEST_CHUNK_BYTES + 31];
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload.write(&bytes).await.expect("bytes write");
        let upload_id = upload.upload_id();
        let content_id = ContentId::for_bytes(&bytes);

        client
            .fail_sealing_upload_states
            .store(true, Ordering::Release);
        assert!(upload.seal().await.is_err());
        db.open_content(test_scope().storage_domain_id(), content_id)
            .await
            .expect("published descriptor is visible");

        client
            .fail_sealing_upload_states
            .store(false, Ordering::Release);
        let repaired = db
            .seal_content_upload(upload_id)
            .await
            .expect("seal retry completes session state");
        assert_eq!(repaired.content_id(), content_id);
        assert_eq!(repaired.len(), bytes.len() as u64);
        assert_eq!(
            db.resume_content_upload(upload_id)
                .await
                .expect("repaired session resumes")
                .sealed(),
            Some(repaired)
        );
    });
}

#[test]
fn resume_repairs_token_issued_before_sealed_session_state() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db = Db::open_object_store_at(
            client.clone(),
            "token-seal-retry-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("object db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload
            .write(b"token crash window")
            .await
            .expect("bytes write");
        let upload_id = upload.upload_id();

        client
            .fail_sealed_upload_states
            .store(true, Ordering::Release);
        assert!(upload.seal().await.is_err());
        client
            .fail_sealed_upload_states
            .store(false, Ordering::Release);

        let sealed = db
            .resume_content_upload(upload_id)
            .await
            .expect("resume completes sealing state")
            .sealed()
            .expect("session is sealed after recovery");
        let mut transaction = db.transaction(TransactionOptions::default());
        let claims = transaction
            .consume_upload_token(
                sealed.upload_token(),
                test_scope(),
                ContentChangeId::from_bytes([31_u8; 16]),
            )
            .await
            .expect("recovered token stages consumption");
        assert_eq!(claims.content_id(), sealed.content_id());
        transaction.put(b"token:recovered", b"yes");
        transaction
            .commit()
            .await
            .expect("recovered token commits atomically");
    });
}

#[test]
fn resume_ignores_chunk_bytes_not_confirmed_by_session_revision() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db = Db::open_object_store_at(
            client.clone(),
            "progress-retry-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("object db opens");
        let bytes = vec![19_u8; TEST_CHUNK_BYTES + 29];
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        let upload_id = upload.upload_id();

        client
            .fail_open_upload_states
            .store(true, Ordering::Release);
        assert!(upload.write(&bytes).await.is_err());
        client
            .fail_open_upload_states
            .store(false, Ordering::Release);

        let mut resumed = db
            .resume_content_upload(upload_id)
            .await
            .expect("upload resumes from old revision")
            .into_open()
            .expect("session remains open");
        assert_eq!(resumed.len(), 0);
        assert_eq!(resumed.buffered_bytes(), 0);
        resumed
            .write(&bytes)
            .await
            .expect("unconfirmed chunks are overwritten");
        let sealed = resumed.seal().await.expect("retried upload seals");
        assert_eq!(sealed.content_id(), ContentId::for_bytes(&bytes));
        db.open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("retried content opens")
            .verify()
            .await
            .expect("retried content verifies");
    });
}

#[derive(Debug, Clone, Copy, Default)]
struct RequestCounts {
    get: usize,
    get_range: usize,
    put: usize,
    put_if: usize,
    head: usize,
    content_delete: usize,
}

#[derive(Debug)]
struct MeasuredClient {
    inner: Arc<InMemoryObjectStore>,
    get: AtomicUsize,
    get_range: AtomicUsize,
    put: AtomicUsize,
    put_if: AtomicUsize,
    head: AtomicUsize,
    content_delete: AtomicUsize,
    fail_descriptors: AtomicBool,
    fail_open_upload_states: AtomicBool,
    fail_sealing_upload_states: AtomicBool,
    fail_sealed_upload_states: AtomicBool,
    fail_content_descriptor_delete_once: AtomicBool,
    report_provider_version: AtomicBool,
}

impl MeasuredClient {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryObjectStore::new()),
            get: AtomicUsize::new(0),
            get_range: AtomicUsize::new(0),
            put: AtomicUsize::new(0),
            put_if: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            content_delete: AtomicUsize::new(0),
            fail_descriptors: AtomicBool::new(false),
            fail_open_upload_states: AtomicBool::new(false),
            fail_sealing_upload_states: AtomicBool::new(false),
            fail_sealed_upload_states: AtomicBool::new(false),
            fail_content_descriptor_delete_once: AtomicBool::new(false),
            report_provider_version: AtomicBool::new(false),
        }
    }

    fn reset_counts(&self) {
        self.get.store(0, Ordering::Relaxed);
        self.get_range.store(0, Ordering::Relaxed);
        self.put.store(0, Ordering::Relaxed);
        self.put_if.store(0, Ordering::Relaxed);
        self.head.store(0, Ordering::Relaxed);
        self.content_delete.store(0, Ordering::Relaxed);
    }

    fn counts(&self) -> RequestCounts {
        RequestCounts {
            get: self.get.load(Ordering::Relaxed),
            get_range: self.get_range.load(Ordering::Relaxed),
            put: self.put.load(Ordering::Relaxed),
            put_if: self.put_if.load(Ordering::Relaxed),
            head: self.head.load(Ordering::Relaxed),
            content_delete: self.content_delete.load(Ordering::Relaxed),
        }
    }
}

impl ObjectClient for MeasuredClient {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        self.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key)
    }

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>> {
        self.get_range.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(key, offset, len)
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        self.put.fetch_add(1, Ordering::Relaxed);
        if self.fail_descriptors.load(Ordering::Acquire) && key.contains("/descriptors/") {
            return Box::pin(async move {
                Err(Error::Io(io::Error::other(
                    "injected content descriptor publish failure",
                )))
            });
        }
        if self.fail_sealing_upload_states.load(Ordering::Acquire)
            && key.contains("/uploads/")
            && bytes.get(8) == Some(&1)
        {
            return Box::pin(async move {
                Err(Error::Io(io::Error::other(
                    "injected sealing upload state failure",
                )))
            });
        }
        if self.fail_sealed_upload_states.load(Ordering::Acquire)
            && key.contains("/uploads/")
            && bytes.get(8) == Some(&2)
        {
            return Box::pin(async move {
                Err(Error::Io(io::Error::other(
                    "injected sealed upload state failure",
                )))
            });
        }
        if self.fail_open_upload_states.load(Ordering::Acquire)
            && key.contains("/uploads/")
            && bytes.get(8) == Some(&0)
        {
            return Box::pin(async move {
                Err(Error::Io(io::Error::other(
                    "injected open upload state failure",
                )))
            });
        }
        self.inner.put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        if key.contains("/content-v1/")
            && (key.contains("/descriptors/") || key.contains("/chunks/"))
        {
            self.content_delete.fetch_add(1, Ordering::Relaxed);
        }
        if key.contains("/content-v1/")
            && key.contains("/descriptors/")
            && self
                .fail_content_descriptor_delete_once
                .swap(false, Ordering::AcqRel)
        {
            return Box::pin(async move {
                Err(Error::Io(io::Error::other(
                    "injected object content descriptor delete failure",
                )))
            });
        }
        self.inner.delete(key)
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        self.head.fetch_add(1, Ordering::Relaxed);
        let key = key.to_owned();
        Box::pin(async move {
            self.inner.head(&key).await.map(|meta| {
                meta.map(|mut meta| {
                    if self.report_provider_version.load(Ordering::Acquire) {
                        meta.version = Some(ObjectVersion::new("provider-version-after-prepare"));
                    }
                    meta
                })
            })
        })
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        self.put_if.fetch_add(1, Ordering::Relaxed);
        self.inner.put_if(key, bytes, precondition)
    }
}

fn temp_db_path(label: &str) -> PathBuf {
    let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("trine-kv-{label}-{}-{id}", std::process::id()))
}
