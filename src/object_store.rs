//! Provider-agnostic object-store client and an in-memory fake.
//!
//! This is the seam the (planned) object-storage backend is written against —
//! see `docs/object-storage-backend.md`. Real providers (S3 and compatible)
//! implement [`ObjectClient`]; the [`InMemoryObjectStore`] here reproduces the
//! semantics that matter — whole-object `put`/`get`, range reads, listing by
//! prefix, idempotent delete, and **conditional writes with `ETags`** — so the
//! backend's harder pieces (segmented WAL, manifest CAS, writer-lease fencing)
//! can be built and tested deterministically with no cloud dependency.
//!
//! ETag/conditional-write semantics mirror object stores: every store assigns a
//! fresh `ETag`, `IfNoneMatch` creates only when absent, and `IfMatch` stores only
//! when the current `ETag` matches. A failed precondition reports the current
//! `ETag` so a compare-and-swap caller (the manifest commit) can retry.
//!
//! This trait is the public "bring your own object store" seam: implement
//! [`ObjectClient`] for your provider (S3 and compatible) and open a database
//! with [`crate::Db::open_object_store`]. The crate's manifest commit and remote
//! WAL head rely on `put_if` providing a real conditional write
//! (compare-and-swap); a backend that cannot honor `If-None-Match` /
//! `If-Match` is unsafe for concurrent writers. After a successful conditional
//! write, later `get`/`head` calls for that same key must observe that version
//! or a newer one. Recovery and read-only refresh follow the lease/head and
//! manifest keys directly; object listing is used for cleanup, so eventually
//! consistent listings may delay garbage collection but must not define
//! committed state.

use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::error::{Error, Result};
use crate::options::DurabilityMode;
use crate::storage::{
    StorageCapabilities, StorageFuture, StorageObjectDeleteBackend, StorageObjectId,
    StorageObjectListBackend, StorageObjectListRequest, StorageObjectReadBackend,
    StorageObjectWriteBackend, StorageReadBackend, StorageReadFuture, StorageReadObject,
    ensure_whole_object_read_len,
};

/// Boxed future returned by [`ObjectClient`] methods. Mirrors the storage
/// layer's `StorageFuture`: object stores are used through `dyn`, so the async
/// methods return a boxed future rather than `async fn`. The `Send` bound is
/// dropped only on the single-threaded wasm target, matching `StorageFuture`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type ObjectFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + 'op>>;

/// See the wasm variant above.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub type ObjectFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'op>>;

/// An opaque entity tag identifying a specific stored version of an object. A
/// new value is minted on every store, so an unchanged `ETag` means the object
/// has not been overwritten since it was observed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ETag(Arc<str>);

impl ETag {
    /// Wraps a provider's entity-tag string (e.g. an S3 `ETag` response header).
    #[must_use]
    pub fn new(tag: impl Into<Arc<str>>) -> Self {
        Self(tag.into())
    }

    /// The underlying entity-tag string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Precondition for a conditional write ([`ObjectClient::put_if`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// Store only if the object does not exist (create). `If-None-Match: *`.
    IfNoneMatch,
    /// Store only if the object exists and its current `ETag` equals this one
    /// (compare-and-swap). `If-Match: <etag>`.
    IfMatch(ETag),
}

/// Outcome of a conditional write. A failed precondition is **not** an error:
/// it is the expected, retryable result of losing a compare-and-swap race, and
/// carries the current `ETag` (or `None` when the object is absent) so the caller
/// can re-read and retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutIf {
    /// The write was applied; the object now has this `ETag`.
    Stored {
        /// The new entity tag of the stored object.
        etag: ETag,
    },
    /// The precondition did not hold; the object was left unchanged.
    PreconditionFailed {
        /// The object's current entity tag, or `None` if it does not exist.
        current: Option<ETag>,
    },
}

/// Metadata for one object returned by [`ObjectClient::list`] / [`ObjectClient::head`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// Current entity tag.
    pub etag: ETag,
}

/// A flat key/value object store: keys are strings, values are immutable byte
/// blobs with an `ETag`. All methods are async (real providers are network I/O);
/// the in-memory fake completes synchronously.
///
/// Contract the backend relies on:
/// - `put` always stores and returns a fresh `ETag` (overwrites bump the `ETag`).
/// - `get` returns `None` for an absent key; `get_range` errors for an absent
///   key or an out-of-bounds range.
/// - After `put`, or after a `put_if` returns [`PutIf::Stored`], later `get` and
///   `head` calls for the same key observe that object version or a newer one.
/// - `delete` is idempotent (deleting an absent key succeeds).
/// - `list` returns objects whose key starts with the prefix, in key order.
///   Listing drives orphan cleanup only; recovery and read-only refresh do not
///   infer committed state from a listing result.
/// - `head` returns an object's metadata (size + `ETag`) without its bytes, or
///   `None` when the key is absent (like S3 `HEAD`).
/// - `put_if` applies the write only when the precondition holds, otherwise
///   reports [`PutIf::PreconditionFailed`] with the current `ETag`.
///
/// Writable object-store opens run a small same-key contract probe against the
/// storage and WAL clients before taking ownership. The probe catches common
/// unsafe adapters early, such as unconditional `put_if`, stale `head`, or stale
/// `get` after a successful conditional write. It is still the implementor's
/// responsibility to provide these semantics for all keys after open.
///
/// # WAL durability sink and split tiers
///
/// A database opened with [`Db::open_object_store_at`](crate::Db::open_object_store_at)
/// writes everything through one client — `SSTable` segments, blobs, the
/// manifest CAS, the writer lease, and the **write-ahead log** — and a commit is
/// acknowledged only after its WAL bytes and WAL head are durable.
///
/// A database opened with
/// [`Db::open_object_store_with_wal_at`](crate::Db::open_object_store_with_wal_at)
/// splits that responsibility: the storage client stores bulk objects and the
/// manifest, while the WAL client stores the writer lease, remote WAL head, and
/// WAL segments. The WAL client is then the commit-latency and commit-durability
/// sink; the storage client remains the long-term table/blob tier.
///
/// Because `Arc<C>` is itself an `ObjectClient`, one client can be **shared
/// across many open databases**. A higher layer (e.g. a multi-tenant service)
/// can either provide an explicit WAL client through the split-tier open API or
/// supply a custom shared client that recognizes WAL writes with
/// [`is_wal_object_key`](crate::is_wal_object_key) and coalesces them across
/// databases. In both forms, the client handling WAL keys must provide the
/// conditional-write and same-key visibility guarantees described above.
pub trait ObjectClient: Send + Sync {
    /// Reads the whole object, or `None` when the key is absent.
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>>;

    /// Reads `len` bytes starting at `offset`; errors for an absent key or an
    /// out-of-bounds range.
    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>>;

    /// Stores the object unconditionally, returning its new `ETag`.
    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag>;

    /// Deletes the object (idempotent: deleting an absent key succeeds).
    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()>;

    /// Lists objects whose key starts with `prefix`, in key order.
    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>>;

    /// Returns the object's metadata (size + `ETag`) without its bytes, or `None`
    /// when the key is absent (like S3 `HEAD`).
    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>>;

    /// Conditional write (compare-and-swap): stores `bytes` only if `precondition`
    /// holds, otherwise reports [`PutIf::PreconditionFailed`] with the current
    /// `ETag`. This is the manifest commit point; it must be a real CAS.
    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf>;
}

static OBJECT_CLIENT_CONTRACT_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) async fn verify_object_client_contract(
    client: &Arc<dyn ObjectClient>,
    db_path: &Path,
    role: &str,
) -> Result<()> {
    let key = object_client_contract_probe_key(db_path, role)?;
    let result = verify_object_client_contract_at_key(client, &key).await;
    let cleanup = client.delete(&key).await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
    }
}

async fn verify_object_client_contract_at_key(
    client: &Arc<dyn ObjectClient>,
    key: &str,
) -> Result<()> {
    client.delete(key).await?;

    let first = Arc::<[u8]>::from(b"trine-object-client-contract:first".as_slice());
    let second = Arc::<[u8]>::from(b"trine-object-client-contract:second".as_slice());
    let first_etag = client.put(key, Arc::clone(&first)).await?;
    verify_object_client_observed_bytes(client, key, &first, &first_etag, "put").await?;

    match client
        .put_if(key, Arc::clone(&second), Precondition::IfNoneMatch)
        .await?
    {
        PutIf::PreconditionFailed { current } if current.as_ref() == Some(&first_etag) => {}
        PutIf::PreconditionFailed { current } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} returned wrong IfNoneMatch ETag: {current:?}"
                ),
            });
        }
        PutIf::Stored { .. } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} stored despite IfNoneMatch on an existing object"
                ),
            });
        }
    }

    let mismatched = ETag::new("trine-object-client-contract-mismatch");
    match client
        .put_if(key, Arc::clone(&second), Precondition::IfMatch(mismatched))
        .await?
    {
        PutIf::PreconditionFailed { .. } => {}
        PutIf::Stored { .. } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} stored despite a mismatched IfMatch ETag"
                ),
            });
        }
    }

    let second_etag = match client
        .put_if(
            key,
            Arc::clone(&second),
            Precondition::IfMatch(first_etag.clone()),
        )
        .await?
    {
        PutIf::Stored { etag } => etag,
        PutIf::PreconditionFailed { current } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} rejected a matching IfMatch ETag: {current:?}"
                ),
            });
        }
    };
    if second_etag == first_etag {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} reused an ETag after overwriting bytes"
            ),
        });
    }
    verify_object_client_observed_bytes(client, key, &second, &second_etag, "put_if").await
}

async fn verify_object_client_observed_bytes(
    client: &Arc<dyn ObjectClient>,
    key: &str,
    expected: &Arc<[u8]>,
    expected_etag: &ETag,
    operation: &str,
) -> Result<()> {
    let head = client.head(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object client contract probe for {key} lost head after {operation}"),
    })?;
    if &head.etag != expected_etag || head.size != expected.len() as u64 {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} observed stale head after {operation}"
            ),
        });
    }
    let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object client contract probe for {key} lost bytes after {operation}"),
    })?;
    if bytes.as_ref() != expected.as_ref() {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} observed stale bytes after {operation}"
            ),
        });
    }
    Ok(())
}

fn object_client_contract_probe_key(db_path: &Path, role: &str) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Corruption {
            message: format!("system clock is before UNIX_EPOCH: {error}"),
        })?;
    let counter = OBJECT_CLIENT_CONTRACT_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(db_path
        .join(format!(
            ".trine-object-client-contract-{role}-{}-{counter}",
            now.as_nanos()
        ))
        .to_string_lossy()
        .into_owned())
}

/// One stored object: its bytes and current `ETag`.
#[derive(Debug, Clone)]
struct StoredObject {
    bytes: Arc<[u8]>,
    etag: ETag,
}

/// An in-memory [`ObjectClient`] with real `ETag` and conditional-write
/// semantics, for building and testing the object-storage backend without a
/// cloud dependency.
#[derive(Debug, Default)]
pub struct InMemoryObjectStore {
    objects: Mutex<BTreeMap<String, StoredObject>>,
    next_etag: AtomicU64,
}

impl InMemoryObjectStore {
    /// Creates an empty in-memory object store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn mint_etag(&self) -> ETag {
        let value = self.next_etag.fetch_add(1, Ordering::Relaxed);
        ETag(Arc::from(format!("etag-{value}")))
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<String, StoredObject>>> {
        self.objects.lock().map_err(|_| Error::Corruption {
            message: "in-memory object store lock poisoned".to_owned(),
        })
    }

    fn get_inner(&self, key: &str) -> Result<Option<Arc<[u8]>>> {
        Ok(self
            .lock()?
            .get(key)
            .map(|object| Arc::clone(&object.bytes)))
    }

    fn get_range_inner(&self, key: &str, offset: u64, len: u64) -> Result<Arc<[u8]>> {
        let objects = self.lock()?;
        let object = objects.get(key).ok_or_else(|| Error::Corruption {
            message: format!("object {key} not found for range read"),
        })?;
        let offset = usize::try_from(offset)
            .map_err(|_| Error::invalid_options("object range offset overflow"))?;
        let len = usize::try_from(len)
            .map_err(|_| Error::invalid_options("object range length overflow"))?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::invalid_options("object range end overflow"))?;
        let slice = object
            .bytes
            .get(offset..end)
            .ok_or_else(|| Error::Corruption {
                message: format!("object {key} short read for range {offset}..{end}"),
            })?;
        Ok(Arc::from(slice))
    }

    fn put_inner(&self, key: &str, bytes: Arc<[u8]>) -> Result<ETag> {
        let etag = self.mint_etag();
        self.lock()?.insert(
            key.to_owned(),
            StoredObject {
                bytes,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    fn delete_inner(&self, key: &str) -> Result<()> {
        self.lock()?.remove(key);
        Ok(())
    }

    fn list_inner(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let objects = self.lock()?;
        Ok(objects
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, object)| ObjectMeta {
                key: key.clone(),
                size: object.bytes.len() as u64,
                etag: object.etag.clone(),
            })
            .collect())
    }

    fn head_inner(&self, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.lock()?.get(key).map(|object| ObjectMeta {
            key: key.to_owned(),
            size: object.bytes.len() as u64,
            etag: object.etag.clone(),
        }))
    }

    fn put_if_inner(
        &self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: &Precondition,
    ) -> Result<PutIf> {
        let mut objects = self.lock()?;
        let current = objects.get(key).map(|object| object.etag.clone());
        let allowed = match (precondition, &current) {
            (Precondition::IfNoneMatch, None) => true,
            (Precondition::IfMatch(expected), Some(actual)) => expected == actual,
            (Precondition::IfNoneMatch, Some(_)) | (Precondition::IfMatch(_), None) => false,
        };
        if !allowed {
            return Ok(PutIf::PreconditionFailed { current });
        }
        let etag = self.mint_etag();
        objects.insert(
            key.to_owned(),
            StoredObject {
                bytes,
                etag: etag.clone(),
            },
        );
        Ok(PutIf::Stored { etag })
    }
}

impl ObjectClient for InMemoryObjectStore {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        let key = key.to_owned();
        Box::pin(async move { self.get_inner(&key) })
    }

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>> {
        let key = key.to_owned();
        Box::pin(async move { self.get_range_inner(&key, offset, len) })
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        let key = key.to_owned();
        Box::pin(async move { self.put_inner(&key, bytes) })
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        let key = key.to_owned();
        Box::pin(async move { self.delete_inner(&key) })
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        let prefix = prefix.to_owned();
        Box::pin(async move { self.list_inner(&prefix) })
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        let key = key.to_owned();
        Box::pin(async move { self.head_inner(&key) })
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        let key = key.to_owned();
        Box::pin(async move { self.put_if_inner(&key, bytes, &precondition) })
    }
}

/// A shared [`ObjectClient`] is itself an `ObjectClient`, so several components
/// (e.g. the manifest store and the byte backend) can share one client by
/// holding `Arc<C>` clones.
impl<C: ObjectClient + ?Sized> ObjectClient for Arc<C> {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        (**self).get(key)
    }

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>> {
        (**self).get_range(key, offset, len)
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        (**self).put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        (**self).delete(key)
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        (**self).list(prefix)
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        (**self).head(key)
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        (**self).put_if(key, bytes, precondition)
    }
}

/// An object-storage **byte** backend: `SSTable` and blob object IO over an
/// [`ObjectClient`].
///
/// It implements the async `Storage*Backend` byte traits the already-generic
/// table/blob async helpers are written against, so flush, compaction, and reads
/// work over object storage. The WAL, the manifest CAS, and the writer lease are
/// deliberately **not** here — those are the object-storage durability
/// substrate's job (manifest CAS lives in [`crate::manifest::ObjectManifestStore`]).
///
/// A [`StorageObjectId`]'s path is used directly as the object key, so keys are
/// consistent across read / write / list / delete (the open path joins file
/// names under the database's key prefix, mirroring the filesystem layout).
#[derive(Clone)]
pub(crate) struct ObjectStoreBackend {
    client: Arc<dyn ObjectClient>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreBackend")
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    pub(crate) fn new(client: Arc<dyn ObjectClient>) -> Self {
        Self { client }
    }

    pub(crate) fn client(&self) -> Arc<dyn ObjectClient> {
        Arc::clone(&self.client)
    }

    fn object_key(object: &StorageObjectId) -> String {
        object.path().to_string_lossy().into_owned()
    }
}

/// A whole-object read handle: the bytes are fetched eagerly on `open_read`
/// (object stores favour whole-object GETs) and random-access reads are served
/// from the in-memory buffer — mirroring `MemoryStorageObject`.
#[derive(Debug, Clone)]
pub(crate) struct ObjectStoreReadObject {
    object: StorageObjectId,
    bytes: Arc<[u8]>,
}

impl ObjectStoreReadObject {
    fn read_exact_at_offset(&self, offset: usize, out: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(out.len())
            .ok_or_else(|| Error::invalid_options("object read offset overflow"))?;
        let source = self
            .bytes
            .get(offset..end)
            .ok_or_else(|| Error::Corruption {
                message: format!("object {} short read", self.object.path().display()),
            })?;
        out.copy_from_slice(source);
        Ok(())
    }
}

impl StorageReadObject for ObjectStoreReadObject {
    fn object(&self) -> &StorageObjectId {
        &self.object
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        let len = self.bytes.len();
        Box::pin(async move {
            u64::try_from(len).map_err(|_| Error::invalid_options("object length overflow"))
        })
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        Box::pin(async move { self.read_exact_at_offset(offset, bytes) })
    }
}

impl StorageReadBackend for ObjectStoreBackend {
    type ReadObject = ObjectStoreReadObject;

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::object_store()
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        Box::pin(async move {
            let key = Self::object_key(&object);
            let meta = self
                .client
                .head(&key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!("referenced object {key} cannot be opened"),
                })?;
            let bytes =
                read_object_bytes_by_meta(self.client.as_ref(), &key, &object, &meta).await?;
            Ok(ObjectStoreReadObject { object, bytes })
        })
    }
}

impl StorageObjectReadBackend for ObjectStoreBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        Box::pin(async move {
            let key = Self::object_key(&object);
            let Some(meta) = self.client.head(&key).await? else {
                return Ok(None);
            };
            read_object_bytes_by_meta(self.client.as_ref(), &key, &object, &meta)
                .await
                .map(Some)
        })
    }
}

impl StorageObjectWriteBackend for ObjectStoreBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        _durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        // A PUT is durable once the store acknowledges it, so durability hints do
        // not apply (there is no separate flush/fsync step).
        Box::pin(async move {
            self.client.put(&Self::object_key(&object), bytes).await?;
            Ok(())
        })
    }
}

impl StorageObjectDeleteBackend for ObjectStoreBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
        Box::pin(async move { self.client.delete(&Self::object_key(&object)).await })
    }
}

impl StorageObjectListBackend for ObjectStoreBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>> {
        Box::pin(async move {
            let root = request.root().to_path_buf();
            let kind = request.kind();
            let extension = request.file_extension();
            // Prefix-list under the database root, then keep only the direct
            // children matching the requested extension — mirroring the
            // filesystem backend's non-recursive, extension-filtered listing.
            let prefix = root.to_string_lossy().into_owned();
            let mut objects: Vec<StorageObjectId> = self
                .client
                .list(&prefix)
                .await?
                .into_iter()
                .map(|meta| PathBuf::from(meta.key))
                .filter(|path| path.parent() == Some(root.as_path()))
                .filter(|path| path_matches_extension(path, extension))
                .map(|path| StorageObjectId::native_file(kind, path))
                .collect();
            objects.sort_unstable();
            Ok(objects)
        })
    }
}

fn path_matches_extension(path: &Path, expected: Option<&str>) -> bool {
    expected.is_none_or(|expected| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
    })
}

fn ensure_object_meta_read_len(object: &StorageObjectId, meta: &ObjectMeta) -> Result<()> {
    let len = usize::try_from(meta.size).map_err(|_| Error::Corruption {
        message: format!("object {} length exceeds usize", object.path().display()),
    })?;
    ensure_whole_object_read_len(object, len)
}

async fn read_object_bytes_by_meta(
    client: &dyn ObjectClient,
    key: &str,
    object: &StorageObjectId,
    meta: &ObjectMeta,
) -> Result<Arc<[u8]>> {
    ensure_object_meta_read_len(object, meta)?;
    let expected_len = usize::try_from(meta.size).map_err(|_| Error::Corruption {
        message: format!("object {} length exceeds usize", object.path().display()),
    })?;
    if expected_len == 0 {
        return Ok(Arc::from([]));
    }
    let bytes = client.get_range(key, 0, meta.size).await?;
    if bytes.len() != expected_len {
        return Err(Error::Corruption {
            message: format!(
                "object {} range read returned {} bytes for declared length {expected_len}",
                object.path().display(),
                bytes.len()
            ),
        });
    }
    ensure_whole_object_read_len(object, bytes.len())?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn bytes(data: &[u8]) -> Arc<[u8]> {
        Arc::from(data)
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

        fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
            let key = key.to_owned();
            Box::pin(async move {
                Ok(Some(ObjectMeta {
                    key,
                    size: u64::MAX,
                    etag: ETag::new("oversized"),
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
        ) -> ObjectFuture<'op, Arc<[u8]>> {
            Box::pin(async move { Ok(bytes(b"abc")) })
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

        fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
            let key = key.to_owned();
            Box::pin(async move {
                Ok(Some(ObjectMeta {
                    key,
                    size: 5,
                    etag: ETag::new("short-range"),
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
    fn get_absent_is_none_and_range_reads_a_window() {
        let store = InMemoryObjectStore::new();
        assert!(block_on(store.get("missing")).unwrap().is_none());
        block_on(store.put("k", bytes(b"abcdef"))).unwrap();
        assert_eq!(
            block_on(store.get_range("k", 2, 3)).unwrap().as_ref(),
            b"cde"
        );
        // Absent key and out-of-bounds range are both errors.
        assert!(block_on(store.get_range("missing", 0, 1)).is_err());
        assert!(block_on(store.get_range("k", 4, 10)).is_err());
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
        let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/0001.trinet");

        block_on(backend.write_object(id.clone(), bytes(b"hello world"), DurabilityMode::Flush))
            .unwrap();

        // Whole-object read.
        assert_eq!(
            block_on(backend.read_object_bytes(id.clone()))
                .unwrap()
                .as_deref(),
            Some(b"hello world".as_slice())
        );

        // Random-access read via the eager read handle.
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
    fn object_store_backend_rejects_oversized_head_before_get() {
        use crate::storage::StorageObjectKind;

        let client = Arc::new(OversizedHeadClient::default());
        let backend = ObjectStoreBackend::new(client.clone());
        let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/huge.trinet");

        let error =
            block_on(backend.read_object_bytes(id.clone())).expect_err("oversized head fails");
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
            ["/db/0001.trinet", "/db/0002.trinet"],
            "only direct .trinet children, in key order"
        );
    }
}
