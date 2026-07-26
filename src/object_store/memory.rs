use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use super::{ETag, ObjectClient, ObjectFuture, ObjectListPage, ObjectMeta, Precondition, PutIf};
use crate::error::{Error, Result};

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

    fn get_range_inner(
        &self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> Result<Arc<[u8]>> {
        let objects = self.lock()?;
        let object = objects
            .get(key)
            .ok_or_else(|| Error::object_version_changed(key))?;
        if &object.etag != expected_etag {
            return Err(Error::object_version_changed(key));
        }
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
                version: None,
            })
            .collect())
    }

    fn head_inner(&self, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.lock()?.get(key).map(|object| ObjectMeta {
            key: key.to_owned(),
            size: object.bytes.len() as u64,
            etag: object.etag.clone(),
            version: None,
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

    fn get_range<'op>(
        &'op self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>> {
        let key = key.to_owned();
        let expected_etag = expected_etag.clone();
        Box::pin(async move { self.get_range_inner(&key, offset, len, &expected_etag) })
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

    fn list_page<'op>(
        &'op self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage> {
        let prefix = prefix.to_owned();
        let after = after.map(str::to_owned);
        Box::pin(async move {
            if limit == 0 {
                return Err(Error::invalid_options(
                    "object listing page limit must be non-zero",
                ));
            }
            let take = limit
                .checked_add(1)
                .ok_or_else(|| Error::invalid_options("object listing page limit overflow"))?;
            let objects = self.lock()?;
            let start = after.map_or_else(|| Bound::Included(prefix.clone()), Bound::Excluded);
            let mut page = objects
                .range((start, Bound::Unbounded))
                .take_while(|(key, _)| key.starts_with(&prefix))
                .take(take)
                .map(|(key, object)| ObjectMeta {
                    key: key.clone(),
                    size: object.bytes.len() as u64,
                    etag: object.etag.clone(),
                    version: None,
                })
                .collect::<Vec<_>>();
            let has_more = page.len() > limit;
            if has_more {
                page.pop();
            }
            let next_after =
                has_more.then(|| page.last().expect("non-empty bounded page").key.clone());
            Ok(ObjectListPage {
                objects: page,
                next_after,
            })
        })
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
