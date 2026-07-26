use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::{
    ETag, InMemoryObjectStore, ObjectClient, ObjectFuture, ObjectListPage, ObjectMeta,
    ObjectStoreBackend, ObjectStoreReclamationAttestation, ObjectStoreReclamationEvidenceDigest,
    ObjectVersion, Precondition, PutIf, canonical_object_key, canonical_object_prefix,
    qualify_object_store_reclamation,
};
use crate::error::{Error, Result};
use crate::options::DurabilityMode;
use crate::storage::{
    StorageObjectDeleteBackend, StorageObjectId, StorageObjectListBackend,
    StorageObjectReadBackend, StorageObjectWriteBackend, StorageReadBackend, StorageReadObject,
};

fn bytes(data: &[u8]) -> Arc<[u8]> {
    Arc::from(data)
}

#[test]
fn object_keys_have_one_cross_platform_canonical_form() {
    assert_eq!(
        canonical_object_prefix(r"tenant\db//./tables").expect("Windows separators normalize"),
        "tenant/db/tables"
    );
    assert_eq!(
        canonical_object_prefix("/tenant/db/tables").expect("Unix separators normalize"),
        "tenant/db/tables"
    );
    assert_eq!(
        canonical_object_key(Path::new(r"\tenant\db\MANIFEST"))
            .expect("absolute Windows-style key normalizes"),
        "tenant/db/MANIFEST"
    );
    assert!(matches!(
        canonical_object_prefix("tenant/../other"),
        Err(Error::InvalidOptions { .. })
    ));
    assert!(matches!(
        canonical_object_prefix("tenant/\0/other"),
        Err(Error::InvalidOptions { .. })
    ));
}

/// Drives an [`ObjectFuture`] to completion. The in-memory store never
/// yields, so a single poll with a no-op waker suffices.
fn block_on<T>(future: ObjectFuture<'_, T>) -> Result<T> {
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWaker));
    let mut context = Context::from_waker(&waker);
    let mut future = future;
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("in-memory object store future should be ready immediately"),
    }
}

#[derive(Debug, Default)]
struct OversizedHeadClient {
    get_calls: AtomicU64,
}

impl ObjectClient for OversizedHeadClient {
    fn get<'op>(&'op self, _key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        Box::pin(async move {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Some(bytes(b"unreachable")))
        })
    }

    fn get_range<'op>(
        &'op self,
        _key: &str,
        _offset: u64,
        _len: u64,
        _expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>> {
        Box::pin(async move { Err(Error::invalid_options("unexpected range read")) })
    }

    fn put<'op>(&'op self, _key: &str, _bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        Box::pin(async move { Err(Error::invalid_options("unexpected put")) })
    }

    fn delete<'op>(&'op self, _key: &str) -> ObjectFuture<'op, ()> {
        Box::pin(async move { Err(Error::invalid_options("unexpected delete")) })
    }

    fn list<'op>(&'op self, _prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        Box::pin(async move { Err(Error::invalid_options("unexpected list")) })
    }

    fn list_page<'op>(
        &'op self,
        _prefix: &str,
        _after: Option<&str>,
        _limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage> {
        Box::pin(async move { Err(Error::invalid_options("unexpected list page")) })
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        let key = key.to_owned();
        Box::pin(async move {
            Ok(Some(ObjectMeta {
                key,
                size: u64::MAX,
                etag: ETag::new("oversized"),
                version: None,
            }))
        })
    }

    fn put_if<'op>(
        &'op self,
        _key: &str,
        _bytes: Arc<[u8]>,
        _precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        Box::pin(async move { Err(Error::invalid_options("unexpected put_if")) })
    }
}

#[derive(Debug, Default)]
struct ShortRangeClient {
    get_calls: AtomicU64,
    range_calls: AtomicU64,
}

impl ObjectClient for ShortRangeClient {
    fn get<'op>(&'op self, _key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        Box::pin(async move {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Some(bytes(b"unreachable")))
        })
    }

    fn get_range<'op>(
        &'op self,
        _key: &str,
        _offset: u64,
        _len: u64,
        _expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>> {
        Box::pin(async move {
            self.range_calls.fetch_add(1, Ordering::Relaxed);
            Ok(bytes(b"abc"))
        })
    }

    fn put<'op>(&'op self, _key: &str, _bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        Box::pin(async move { Err(Error::invalid_options("unexpected put")) })
    }

    fn delete<'op>(&'op self, _key: &str) -> ObjectFuture<'op, ()> {
        Box::pin(async move { Err(Error::invalid_options("unexpected delete")) })
    }

    fn list<'op>(&'op self, _prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        Box::pin(async move { Err(Error::invalid_options("unexpected list")) })
    }

    fn list_page<'op>(
        &'op self,
        _prefix: &str,
        _after: Option<&str>,
        _limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage> {
        Box::pin(async move { Err(Error::invalid_options("unexpected list page")) })
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        let key = key.to_owned();
        Box::pin(async move {
            Ok(Some(ObjectMeta {
                key,
                size: 5,
                etag: ETag::new("short-range"),
                version: None,
            }))
        })
    }

    fn put_if<'op>(
        &'op self,
        _key: &str,
        _bytes: Arc<[u8]>,
        _precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        Box::pin(async move { Err(Error::invalid_options("unexpected put_if")) })
    }
}

#[derive(Debug)]
struct ReclamationProbeClient {
    inner: InMemoryObjectStore,
    report_version: bool,
    retain_on_delete: bool,
    hide_head: bool,
}

impl ReclamationProbeClient {
    fn new(report_version: bool, retain_on_delete: bool, hide_head: bool) -> Self {
        Self {
            inner: InMemoryObjectStore::new(),
            report_version,
            retain_on_delete,
            hide_head,
        }
    }

    fn decorate_meta(&self, mut meta: ObjectMeta) -> ObjectMeta {
        if self.report_version {
            meta.version = Some(ObjectVersion::new("provider-version-1"));
        }
        meta
    }
}

impl ObjectClient for ReclamationProbeClient {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        self.inner.get(key)
    }

    fn get_range<'op>(
        &'op self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>> {
        self.inner.get_range(key, offset, len, expected_etag)
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        self.inner.put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        if self.retain_on_delete {
            Box::pin(async { Ok(()) })
        } else {
            self.inner.delete(key)
        }
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        let prefix = prefix.to_owned();
        Box::pin(async move {
            self.inner.list(&prefix).await.map(|metas| {
                metas
                    .into_iter()
                    .map(|meta| self.decorate_meta(meta))
                    .collect()
            })
        })
    }

    fn list_page<'op>(
        &'op self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage> {
        let prefix = prefix.to_owned();
        let after = after.map(str::to_owned);
        Box::pin(async move {
            self.inner
                .list_page(&prefix, after.as_deref(), limit)
                .await
                .map(|mut page| {
                    page.objects = page
                        .objects
                        .into_iter()
                        .map(|meta| self.decorate_meta(meta))
                        .collect();
                    page
                })
        })
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        let key = key.to_owned();
        Box::pin(async move {
            if self.hide_head {
                return Ok(None);
            }
            self.inner
                .head(&key)
                .await
                .map(|meta| meta.map(|meta| self.decorate_meta(meta)))
        })
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        self.inner.put_if(key, bytes, precondition)
    }
}

#[test]
fn put_then_get_roundtrips_and_overwrite_changes_etag() {
    let store = InMemoryObjectStore::new();
    let first = block_on(store.put("k", bytes(b"hello"))).unwrap();
    assert_eq!(
        block_on(store.get("k")).unwrap().as_deref(),
        Some(b"hello".as_slice())
    );
    let second = block_on(store.put("k", bytes(b"world"))).unwrap();
    assert_ne!(first, second, "overwrite mints a new ETag");
    assert_eq!(
        block_on(store.get("k")).unwrap().as_deref(),
        Some(b"world".as_slice())
    );
}

#[test]
fn reclamation_qualification_requires_unversioned_observable_delete() {
    let evidence = ObjectStoreReclamationAttestation::new(
        ObjectStoreReclamationEvidenceDigest::for_bytes(b"test provider evidence"),
    );
    let qualified_client: Arc<dyn ObjectClient> =
        Arc::new(ReclamationProbeClient::new(false, false, false));
    let qualification = block_on(Box::pin(qualify_object_store_reclamation(
        Arc::clone(&qualified_client),
        "qualified-prefix",
        evidence,
    )))
    .expect("unversioned strong delete qualifies");
    assert_eq!(qualification.evidence_digest(), evidence.evidence_digest());
    assert!(qualification.matches_prefix(Path::new("qualified-prefix")));
    assert!(!qualification.matches_prefix(Path::new("different-prefix")));
    assert!(qualification.matches_client(&qualified_client));
    let different_client: Arc<dyn ObjectClient> =
        Arc::new(ReclamationProbeClient::new(false, false, false));
    assert!(!qualification.matches_client(&different_client));

    let versioned_client: Arc<dyn ObjectClient> =
        Arc::new(ReclamationProbeClient::new(true, false, false));
    assert!(matches!(
        block_on(Box::pin(qualify_object_store_reclamation(
            versioned_client,
            "versioned-prefix",
            evidence,
        ))),
        Err(Error::Corruption { .. })
    ));

    let sticky_client: Arc<dyn ObjectClient> =
        Arc::new(ReclamationProbeClient::new(false, true, false));
    assert!(matches!(
        block_on(Box::pin(qualify_object_store_reclamation(
            sticky_client,
            "sticky-prefix",
            evidence,
        ))),
        Err(Error::Corruption { .. })
    ));
}

#[test]
fn verified_delete_does_not_trust_head_absence_alone() {
    let concrete = Arc::new(ReclamationProbeClient::new(false, false, true));
    block_on(concrete.put("db/content-object", bytes(b"still present")))
        .expect("seed hidden object");
    let client: Arc<dyn ObjectClient> = concrete.clone();
    let backend = ObjectStoreBackend::new(client);
    let object = StorageObjectId::native_file(
        crate::storage::StorageObjectKind::ContentChunk,
        "db/content-object",
    );

    assert!(matches!(
        block_on(Box::pin(backend.delete_unversioned_object_verified(object))),
        Err(Error::Corruption { .. })
    ));
    assert_eq!(
        block_on(concrete.get("db/content-object"))
            .expect("hidden object reads")
            .as_deref(),
        Some(b"still present".as_slice())
    );
}

#[test]
fn get_absent_is_none_and_range_reads_a_window() {
    let store = InMemoryObjectStore::new();
    assert!(block_on(store.get("missing")).unwrap().is_none());
    let etag = block_on(store.put("k", bytes(b"abcdef"))).unwrap();
    assert_eq!(
        block_on(store.get_range("k", 2, 3, &etag))
            .unwrap()
            .as_ref(),
        b"cde"
    );
    // Absent key and out-of-bounds range are both errors.
    assert!(matches!(
        block_on(store.get_range("missing", 0, 1, &etag)),
        Err(Error::ObjectVersionChanged { .. })
    ));
    assert!(block_on(store.get_range("k", 4, 10, &etag)).is_err());

    block_on(store.put("k", bytes(b"replacement"))).expect("overwrite changes ETag");
    assert!(matches!(
        block_on(store.get_range("k", 0, 3, &etag)),
        Err(Error::ObjectVersionChanged { .. })
    ));
}

#[test]
fn delete_is_idempotent() {
    let store = InMemoryObjectStore::new();
    block_on(store.put("k", bytes(b"x"))).unwrap();
    block_on(store.delete("k")).unwrap();
    assert!(block_on(store.get("k")).unwrap().is_none());
    // Deleting an absent key still succeeds.
    block_on(store.delete("k")).unwrap();
}

#[test]
fn list_returns_prefix_matches_in_key_order() {
    let store = InMemoryObjectStore::new();
    block_on(store.put("wal/2", bytes(b"b"))).unwrap();
    block_on(store.put("wal/1", bytes(b"aa"))).unwrap();
    block_on(store.put("table/9", bytes(b"c"))).unwrap();
    let listed = block_on(store.list("wal/")).unwrap();
    let keys: Vec<&str> = listed.iter().map(|meta| meta.key.as_str()).collect();
    assert_eq!(keys, ["wal/1", "wal/2"], "prefix-filtered, key-ordered");
    assert_eq!(listed[0].size, 2);
    assert_eq!(listed[1].size, 1);
}

#[test]
fn list_page_uses_exclusive_continuation_without_duplicates() {
    let store = InMemoryObjectStore::new();
    for key in ["wal/1", "wal/2", "wal/3"] {
        block_on(store.put(key, bytes(key.as_bytes()))).unwrap();
    }
    let first = block_on(store.list_page("wal/", None, 2)).unwrap();
    assert_eq!(
        first
            .objects
            .iter()
            .map(|meta| meta.key.as_str())
            .collect::<Vec<_>>(),
        ["wal/1", "wal/2"]
    );
    let second = block_on(store.list_page("wal/", first.next_after.as_deref(), 2)).unwrap();
    assert_eq!(
        second
            .objects
            .iter()
            .map(|meta| meta.key.as_str())
            .collect::<Vec<_>>(),
        ["wal/3"]
    );
    assert!(second.next_after.is_none());
}

#[test]
fn head_returns_metadata_without_bytes_and_none_when_absent() {
    let store = InMemoryObjectStore::new();
    assert!(block_on(store.head("k")).unwrap().is_none());
    let etag = block_on(store.put("k", bytes(b"hello"))).unwrap();
    let meta = block_on(store.head("k")).unwrap().expect("present");
    assert_eq!(meta.key, "k");
    assert_eq!(meta.size, 5);
    assert_eq!(meta.etag, etag);
}

#[test]
fn put_if_none_match_creates_only_when_absent() {
    let store = InMemoryObjectStore::new();
    let created = block_on(store.put_if("k", bytes(b"v1"), Precondition::IfNoneMatch)).unwrap();
    let etag = match created {
        PutIf::Stored { etag } => etag,
        PutIf::PreconditionFailed { .. } => panic!("create should succeed when absent"),
    };
    // A second create is refused and reports the current ETag.
    match block_on(store.put_if("k", bytes(b"v2"), Precondition::IfNoneMatch)).unwrap() {
        PutIf::PreconditionFailed { current } => assert_eq!(current, Some(etag)),
        PutIf::Stored { .. } => panic!("create should fail when present"),
    }
    assert_eq!(
        block_on(store.get("k")).unwrap().as_deref(),
        Some(b"v1".as_slice()),
        "refused create left the object unchanged"
    );
}

#[test]
fn put_if_match_is_a_compare_and_swap() {
    let store = InMemoryObjectStore::new();
    let v1 = block_on(store.put("k", bytes(b"v1"))).unwrap();

    // CAS with the current ETag wins and advances the ETag.
    let v2 = match block_on(store.put_if("k", bytes(b"v2"), Precondition::IfMatch(v1.clone())))
        .unwrap()
    {
        PutIf::Stored { etag } => etag,
        PutIf::PreconditionFailed { .. } => panic!("CAS with current ETag should win"),
    };
    assert_ne!(v1, v2);

    // A second CAS with the now-stale ETag loses and reports the current one
    // — this is the manifest-commit retry signal.
    match block_on(store.put_if("k", bytes(b"v3"), Precondition::IfMatch(v1))).unwrap() {
        PutIf::PreconditionFailed { current } => assert_eq!(current, Some(v2)),
        PutIf::Stored { .. } => panic!("CAS with stale ETag should lose"),
    }
    assert_eq!(
        block_on(store.get("k")).unwrap().as_deref(),
        Some(b"v2".as_slice()),
        "the losing CAS left v2 in place"
    );
}

#[test]
fn put_if_match_on_absent_object_fails() {
    let store = InMemoryObjectStore::new();
    let phantom = ETag(Arc::from("etag-phantom"));
    match block_on(store.put_if("k", bytes(b"v"), Precondition::IfMatch(phantom))).unwrap() {
        PutIf::PreconditionFailed { current } => assert_eq!(current, None),
        PutIf::Stored { .. } => panic!("IfMatch cannot match a missing object"),
    }
}

#[test]
fn object_store_backend_round_trips_an_object() {
    use crate::storage::StorageObjectKind;

    let backend = ObjectStoreBackend::new(Arc::new(InMemoryObjectStore::new()));
    let id = StorageObjectId::native_file(StorageObjectKind::ContentChunk, "/db/0001.trinec");

    block_on(backend.write_object(id.clone(), bytes(b"hello world"), DurabilityMode::Flush))
        .unwrap();

    // Whole-object read.
    assert_eq!(
        block_on(backend.read_object_bytes(id.clone()))
            .unwrap()
            .as_deref(),
        Some(b"hello world".as_slice())
    );

    // Random-access read via the ranged read handle.
    let object = block_on(backend.open_read(id.clone())).unwrap();
    assert_eq!(block_on(StorageReadObject::len(&object)).unwrap(), 11);
    let mut window = [0_u8; 5];
    block_on(StorageReadObject::read_exact_at(&object, 6, &mut window)).unwrap();
    assert_eq!(&window, b"world");

    // Delete, then it is gone.
    block_on(backend.delete_object(id.clone())).unwrap();
    assert!(block_on(backend.read_object_bytes(id)).unwrap().is_none());
}

#[test]
fn immutable_table_object_cannot_be_overwritten_or_deleted() {
    use crate::storage::StorageObjectKind;

    let client = Arc::new(InMemoryObjectStore::new());
    let backend = ObjectStoreBackend::new(client.clone());
    let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/0001.trinet");

    block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
        .expect("initial immutable write");
    block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
        .expect("identical retry is idempotent");
    let error =
        block_on(backend.write_object(id.clone(), bytes(b"different"), DurabilityMode::Flush))
            .expect_err("different bytes cannot replace immutable object");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(
        block_on(backend.delete_object(id.clone())).is_err(),
        "table deletion requires a reader-retirement protocol"
    );
    assert_eq!(
        block_on(client.get("db/0001.trinet")).unwrap().as_deref(),
        Some(b"first".as_slice())
    );
}

#[test]
fn immutable_content_chunk_rejects_conflicting_retry() {
    let backend = ObjectStoreBackend::new(Arc::new(InMemoryObjectStore::new()));
    let id = StorageObjectId::native_file(
        crate::storage::StorageObjectKind::ContentChunk,
        "/db/content/upload/partial-0001-0007.trinec",
    );

    block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
        .expect("initial immutable chunk write");
    block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
        .expect("identical chunk retry is idempotent");
    assert!(matches!(
        block_on(backend.write_object(id, bytes(b"stale"), DurabilityMode::Flush)),
        Err(Error::Corruption { .. })
    ));
}

#[test]
fn object_store_backend_rejects_oversized_head_before_get() {
    use crate::storage::StorageObjectKind;

    let client = Arc::new(OversizedHeadClient::default());
    let backend = ObjectStoreBackend::new(client.clone());
    let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/huge.trinet");

    let error = block_on(backend.read_object_bytes(id.clone())).expect_err("oversized head fails");
    assert!(error.to_string().contains("exceeds maximum"));
    assert_eq!(
        client.get_calls.load(Ordering::Relaxed),
        0,
        "oversized object should fail on HEAD before GET"
    );

    let error = block_on(backend.open_read(id)).expect_err("oversized head fails");
    assert!(error.to_string().contains("exceeds maximum"));
    assert_eq!(
        client.get_calls.load(Ordering::Relaxed),
        0,
        "open_read should also fail on HEAD before GET"
    );
}

#[test]
fn object_store_backend_rejects_short_range_without_whole_get() {
    use crate::storage::StorageObjectKind;

    let client = Arc::new(ShortRangeClient::default());
    let backend = ObjectStoreBackend::new(client.clone());
    let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/short.trinet");

    let error = block_on(backend.read_object_bytes(id)).expect_err("short range fails");
    assert!(error.to_string().contains("declared length"));
    assert_eq!(
        client.get_calls.load(Ordering::Relaxed),
        0,
        "bounded range reads should not fall back to whole-object GET"
    );

    let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/short.trinet");
    let object = block_on(backend.open_read(id)).expect("HEAD-only open succeeds");
    assert_eq!(
        client.range_calls.load(Ordering::Relaxed),
        1,
        "open itself performs no additional range request"
    );
    let mut out = [0_u8; 5];
    let error = block_on(StorageReadObject::read_exact_at(&object, 0, &mut out))
        .expect_err("short ranged handle read fails");
    assert!(error.to_string().contains("requested length"));
    assert_eq!(client.range_calls.load(Ordering::Relaxed), 2);
}

#[test]
fn object_store_backend_lists_direct_children_by_extension() {
    use crate::storage::{StorageObjectKind, StorageObjectListRequest};

    let backend = ObjectStoreBackend::new(Arc::new(InMemoryObjectStore::new()));
    let write = |key: &'static str| {
        block_on(backend.write_object(
            StorageObjectId::native_file(StorageObjectKind::Table, key),
            bytes(b"x"),
            DurabilityMode::Flush,
        ))
        .unwrap();
    };
    write("/db/0002.trinet");
    write("/db/0001.trinet");
    write("/db/MANIFEST"); // wrong extension
    write("/db/sub/9999.trinet"); // not a direct child of /db

    let listed = block_on(
        backend.list_objects(
            StorageObjectListRequest::native_file(StorageObjectKind::Table, "/db")
                .with_file_extension("trinet"),
        ),
    )
    .unwrap();
    let paths: Vec<String> = listed
        .iter()
        .map(|id| id.path().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        paths,
        ["db/0001.trinet", "db/0002.trinet"],
        "only direct .trinet children, in key order"
    );
}
