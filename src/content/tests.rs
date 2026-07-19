use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use futures::executor::block_on;

use crate::{
    ContentId, ContentUploadOptions, Db, DbOptions, ETag, Error, InMemoryObjectStore, ObjectClient,
    ObjectFuture, ObjectMeta, Precondition, PutIf,
};

const TEST_CHUNK_BYTES: usize = 64 * 1024;
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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
                ContentUploadOptions::new()
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
        let handle = db
            .open_content(sealed.content_id())
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
                ContentUploadOptions::new()
                    .with_chunk_bytes(TEST_CHUNK_BYTES)
                    .with_expected_length(1),
            )
            .await
            .expect("mismatch upload begins");
        mismatch.write(b"two").await.expect("mismatch bytes write");
        assert!(matches!(
            mismatch.seal().await,
            Err(Error::ContentLengthMismatch {
                expected: 1,
                actual: 3
            })
        ));
    });
}

#[test]
fn native_content_exceeds_ordinary_value_limit_and_reopens() {
    let path = temp_db_path("content-large-native");
    let content_id = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let one_mib = 1024 * 1024;
        let mut upload = db
            .begin_content_upload(ContentUploadOptions::new().with_chunk_bytes(one_mib))
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
            .open_content(content_id)
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
            .begin_content_upload(ContentUploadOptions::new().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("object upload begins");
        upload.write(&bytes).await.expect("object bytes write");
        let sealed = upload.seal().await.expect("object upload seals");
        let after_seal = client.counts();
        assert_eq!(after_seal.put, 4, "three chunks plus one descriptor");

        let handle = db
            .open_content(sealed.content_id())
            .await
            .expect("object content opens");
        let after_open = client.counts();
        assert_eq!(after_open.head - after_seal.head, 1);
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
            .begin_content_upload(ContentUploadOptions::new().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("failed upload begins");
        failed_upload
            .write(b"not published")
            .await
            .expect("failed upload chunk buffers");
        assert!(failed_upload.seal().await.is_err());
        assert!(matches!(
            failed_db.open_content(expected).await,
            Err(Error::ContentNotFound { .. })
        ));
        drop(failed_db);
    });
}

#[derive(Debug, Clone, Copy, Default)]
struct RequestCounts {
    get: usize,
    get_range: usize,
    put: usize,
    head: usize,
}

#[derive(Debug)]
struct MeasuredClient {
    inner: Arc<InMemoryObjectStore>,
    get: AtomicUsize,
    get_range: AtomicUsize,
    put: AtomicUsize,
    head: AtomicUsize,
    fail_descriptors: AtomicBool,
}

impl MeasuredClient {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryObjectStore::new()),
            get: AtomicUsize::new(0),
            get_range: AtomicUsize::new(0),
            put: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            fail_descriptors: AtomicBool::new(false),
        }
    }

    fn reset_counts(&self) {
        self.get.store(0, Ordering::Relaxed);
        self.get_range.store(0, Ordering::Relaxed);
        self.put.store(0, Ordering::Relaxed);
        self.head.store(0, Ordering::Relaxed);
    }

    fn counts(&self) -> RequestCounts {
        RequestCounts {
            get: self.get.load(Ordering::Relaxed),
            get_range: self.get_range.load(Ordering::Relaxed),
            put: self.put.load(Ordering::Relaxed),
            head: self.head.load(Ordering::Relaxed),
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
        self.inner.put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        self.inner.delete(key)
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        self.head.fetch_add(1, Ordering::Relaxed);
        self.inner.head(key)
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

fn temp_db_path(label: &str) -> PathBuf {
    let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("trine-kv-{label}-{}-{id}", std::process::id()))
}
