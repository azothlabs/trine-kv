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
    CONTENT_TOKEN_INDEX_BUCKET, ContentAccessBarrierRecord, ContentLeaseRecord, UploadSessionState,
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
    HostStorageBackend, InMemoryObjectStore, ObjectClient, ObjectFuture, ObjectListPage,
    ObjectMeta, ObjectStoreReclamationAttestation, ObjectStoreReclamationEvidenceDigest,
    ObjectVersion, OwnerScopeId, Precondition, PutIf, ReadVersion, SealedContent, StorageDomainId,
    StorageMode, TransactionOptions, UploadId, UploadToken, qualify_object_store_reclamation,
};

const TEST_CHUNK_BYTES: usize = 64 * 1024;
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_scope() -> ContentAttachmentScope {
    ContentAttachmentScope::new(
        StorageDomainId::from_bytes([3_u8; 16]),
        OwnerScopeId::from_bytes([7_u8; 16]),
    )
}

fn test_upload_options() -> ContentUploadOptions {
    ContentUploadOptions::new(test_scope(), Duration::from_hours(1))
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

fn temp_db_path(label: &str) -> PathBuf {
    let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("trine-kv-{label}-{}-{id}", std::process::id()))
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

    fn get_range<'op>(
        &'op self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>> {
        self.get_range.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(key, offset, len, expected_etag)
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

    fn list_page<'op>(
        &'op self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage> {
        self.inner.list_page(prefix, after, limit)
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
        self.inner.put_if(key, bytes, precondition)
    }
}

mod access_coordination;
mod authority;
mod lifecycle;
mod reclaim;
mod sweep;
mod upload;
