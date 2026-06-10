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
//! The trait and fake are not yet consumed (the object-storage backend that
//! uses them is a later slice), so the surface is allowed to be dead for now.
#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::error::{Error, Result};

/// Boxed future returned by [`ObjectClient`] methods. Mirrors the storage
/// layer's `StorageFuture`: object stores are used through `dyn`, so the async
/// methods return a boxed future rather than `async fn`. The `Send` bound is
/// dropped only on the single-threaded wasm target, matching `StorageFuture`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) type ObjectFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + 'op>>;

/// See the wasm variant above.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type ObjectFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'op>>;

/// An opaque entity tag identifying a specific stored version of an object. A
/// new value is minted on every store, so an unchanged `ETag` means the object
/// has not been overwritten since it was observed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ETag(Arc<str>);

impl ETag {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Precondition for a conditional write ([`ObjectClient::put_if`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Precondition {
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
pub(crate) enum PutIf {
    /// The write was applied; the object now has this `ETag`.
    Stored { etag: ETag },
    /// The precondition did not hold; the object was left unchanged.
    PreconditionFailed { current: Option<ETag> },
}

/// Metadata for one object returned by [`ObjectClient::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectMeta {
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) etag: ETag,
}

/// A flat key/value object store: keys are strings, values are immutable byte
/// blobs with an `ETag`. All methods are async (real providers are network I/O);
/// the in-memory fake completes synchronously.
///
/// Contract the backend relies on:
/// - `put` always stores and returns a fresh `ETag` (overwrites bump the `ETag`).
/// - `get` returns `None` for an absent key; `get_range` errors for an absent
///   key or an out-of-bounds range.
/// - `delete` is idempotent (deleting an absent key succeeds).
/// - `list` returns objects whose key starts with the prefix, in key order.
/// - `put_if` applies the write only when the precondition holds, otherwise
///   reports [`PutIf::PreconditionFailed`] with the current `ETag`.
pub(crate) trait ObjectClient: Send + Sync {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>>;

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>>;

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag>;

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()>;

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>>;

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf>;
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
pub(crate) struct InMemoryObjectStore {
    objects: Mutex<BTreeMap<String, StoredObject>>,
    next_etag: AtomicU64,
}

impl InMemoryObjectStore {
    pub(crate) fn new() -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
