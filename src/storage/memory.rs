//! In-memory implementation of the object read/write/list contract.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
};

use super::{
    BlockingStorageObjectReadBackend, BlockingStorageReadBackend, BlockingStorageReadObject,
    StorageCapabilities, StorageFuture, StorageObjectDeleteBackend, StorageObjectId,
    StorageObjectListBackend, StorageObjectListPage, StorageObjectListRequest,
    StorageObjectReadBackend, StorageObjectWriteBackend, StorageReadBackend, StorageReadFuture,
    StorageReadObject, paginate_storage_objects, poll_ready_storage_future, usize_to_u64,
};

#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub(crate) struct MemoryStorageBackend {
    objects: Arc<Mutex<BTreeMap<StorageObjectId, Arc<[u8]>>>>,
}

#[allow(dead_code)]
impl MemoryStorageBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert_read_object(
        &self,
        object: StorageObjectId,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<()> {
        let mut objects = self.lock_objects()?;
        objects.insert(object, bytes.into());
        Ok(())
    }

    fn object_bytes(&self, object: &StorageObjectId) -> Result<Arc<[u8]>> {
        self.optional_object_bytes(object)?
            .ok_or_else(|| Error::Corruption {
                message: format!(
                    "referenced memory {} {} cannot be opened",
                    object.kind().as_str(),
                    object.path().display()
                ),
            })
    }

    fn optional_object_bytes(&self, object: &StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        let objects = self.lock_objects()?;
        Ok(objects.get(object).cloned())
    }

    fn lock_objects(&self) -> Result<MutexGuard<'_, BTreeMap<StorageObjectId, Arc<[u8]>>>> {
        self.objects.lock().map_err(|_| Error::Corruption {
            message: "memory storage registry lock poisoned".to_owned(),
        })
    }
}

impl StorageReadBackend for MemoryStorageBackend {
    type ReadObject = MemoryStorageObject;

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::memory_read()
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        Box::pin(async move {
            let bytes = self.object_bytes(&object)?;
            Ok(MemoryStorageObject { object, bytes })
        })
    }
}

impl BlockingStorageReadBackend for MemoryStorageBackend {
    fn open_read_blocking(&self, object: StorageObjectId) -> Result<Self::ReadObject> {
        poll_ready_storage_future(self.open_read(object))
    }
}

impl StorageObjectReadBackend for MemoryStorageBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        Box::pin(async move { self.optional_object_bytes(&object) })
    }
}

impl BlockingStorageObjectReadBackend for MemoryStorageBackend {
    fn read_object_bytes_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        poll_ready_storage_future(self.read_object_bytes(object))
    }
}

impl StorageObjectWriteBackend for MemoryStorageBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        _durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        Box::pin(async move { self.insert_read_object(object, bytes) })
    }
}

impl StorageObjectDeleteBackend for MemoryStorageBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            self.lock_objects()?.remove(&object);
            Ok(())
        })
    }
}

impl StorageObjectListBackend for MemoryStorageBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>> {
        Box::pin(async move {
            let objects = self.lock_objects()?;
            let mut listed = objects
                .keys()
                .filter(|object| object.kind() == request.kind())
                .filter(|object| object.path().parent() == Some(request.root()))
                .filter(|object| {
                    request.file_extension().is_none_or(|extension| {
                        object.path().extension().and_then(|value| value.to_str())
                            == Some(extension)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            listed.sort_unstable();
            Ok(listed)
        })
    }

    fn list_objects_page(
        &self,
        request: StorageObjectListRequest,
        after: Option<&str>,
        limit: usize,
    ) -> StorageFuture<'_, StorageObjectListPage> {
        let after = after.map(str::to_owned);
        Box::pin(async move {
            paginate_storage_objects(self.list_objects(request).await?, after.as_deref(), limit)
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct MemoryStorageObject {
    object: StorageObjectId,
    bytes: Arc<[u8]>,
}

impl MemoryStorageObject {
    fn len_from_memory(&self) -> Result<u64> {
        usize_to_u64(self.bytes.len(), "memory storage object length")
    }

    fn read_exact_at_offset(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| Error::invalid_options("memory storage object read offset overflow"))?;
        let source = self
            .bytes
            .get(offset..end)
            .ok_or_else(|| Error::Corruption {
                message: format!(
                    "referenced memory {} {} short read",
                    self.object.kind().as_str(),
                    self.object.path().display()
                ),
            })?;
        bytes.copy_from_slice(source);
        Ok(())
    }
}

impl StorageReadObject for MemoryStorageObject {
    fn object(&self) -> &StorageObjectId {
        &self.object
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        Box::pin(async move { self.len_from_memory() })
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        Box::pin(async move { self.read_exact_at_offset(offset, bytes) })
    }
}

impl BlockingStorageReadObject for MemoryStorageObject {
    fn len_blocking(&self) -> Result<u64> {
        poll_ready_storage_future(StorageReadObject::len(self))
    }

    fn read_exact_at_blocking(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        poll_ready_storage_future(StorageReadObject::read_exact_at(self, offset, bytes))
    }
}
