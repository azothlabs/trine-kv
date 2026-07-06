use super::{
    Arc, BlockingStorageAppendBackend, BlockingStorageAppendObject,
    BlockingStorageDirectoryCreateBackend, BlockingStorageDirectoryListBackend,
    BlockingStorageDirectorySyncBackend, BlockingStorageManifestPublishBackend,
    BlockingStorageManifestReadBackend, BlockingStorageObjectDeleteBackend,
    BlockingStorageObjectListBackend, BlockingStorageObjectReadBackend,
    BlockingStorageObjectWriteBackend, BlockingStorageReadBackend, BlockingStorageReadObject,
    BlockingStorageWalRewriteBackend, BlockingStorageWriterLeaseBackend, DurabilityMode, Error,
    NativeFileAppendObject, NativeFileBackend, NativeFileObject, NativeFileWriterLease,
    ObjectStoreBackend, ObjectStoreReadObject, Result, StorageAppendBackend, StorageAppendObject,
    StorageCapabilities, StorageDirectoryCreateBackend, StorageDirectoryFile, StorageDirectoryId,
    StorageDirectoryListBackend, StorageDirectorySyncBackend, StorageFuture,
    StorageManifestPublishBackend, StorageManifestReadBackend, StorageObjectDeleteBackend,
    StorageObjectId, StorageObjectListBackend, StorageObjectListRequest, StorageObjectReadBackend,
    StorageObjectWriteBackend, StorageReadBackend, StorageReadFuture, StorageReadObject,
    StorageWalRewriteBackend, StorageWriterLeaseBackend,
};

/// The storage backend the engine binds to, dispatched as an enum so the
/// database can select a backend at runtime without threading a generic `<B>`
/// through every engine type. It implements every `Storage*Backend` trait by
/// delegating to the active variant, so the existing backend-generic helpers
/// (`fn ...<B: Storage*Backend>(backend: &B, ...)`) accept `&StorageBackend`
/// unchanged.
///
/// Today it wraps only the native filesystem backend; the object-storage
/// backend joins as a second variant (see `docs/object-storage-backend.md`).
/// The browser/wasm backend keeps its own dedicated path and is not routed
/// through here.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum StorageBackend {
    Native(NativeFileBackend),
    /// Object storage (async-only). Only the byte ops (read/write/delete/list)
    /// are supported; append, WAL rewrite, writer lease, directory ops, and
    /// manifest publish return `unsupported` because object-store databases
    /// drive remote WAL and manifest ownership through the durability substrate
    /// and `ObjectManifestStore`, not this byte backend.
    ObjectStore(ObjectStoreBackend),
}

/// A read handle opened from a [`StorageBackend`], dispatched to the active
/// variant's concrete read object.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum BackendReadObject {
    Native(NativeFileObject),
    ObjectStore(ObjectStoreReadObject),
}

/// An append handle opened from a [`StorageBackend`].
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum BackendAppendObject {
    Native(NativeFileAppendObject),
}

/// A writer lease acquired from a [`StorageBackend`]; held for its lifetime and
/// released on drop.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum BackendWriterLease {
    Native(NativeFileWriterLease),
}

/// Error for a synchronous/blocking call against the async-only object-store
/// backend.
fn object_store_sync_unsupported() -> Error {
    Error::unsupported_backend("object-store backend is async-only")
}

impl StorageReadBackend for StorageBackend {
    type ReadObject = BackendReadObject;

    fn capabilities(&self) -> StorageCapabilities {
        match self {
            StorageBackend::Native(backend) => backend.capabilities(),
            StorageBackend::ObjectStore(backend) => backend.capabilities(),
        }
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        match self {
            StorageBackend::Native(backend) => {
                Box::pin(
                    async move { Ok(BackendReadObject::Native(backend.open_read(object).await?)) },
                )
            }
            StorageBackend::ObjectStore(backend) => Box::pin(async move {
                Ok(BackendReadObject::ObjectStore(
                    backend.open_read(object).await?,
                ))
            }),
        }
    }
}

impl BlockingStorageReadBackend for StorageBackend {
    fn open_read_blocking(&self, object: StorageObjectId) -> Result<Self::ReadObject> {
        match self {
            StorageBackend::Native(backend) => Ok(BackendReadObject::Native(
                backend.open_read_blocking(object)?,
            )),
            StorageBackend::ObjectStore(_) => Err(object_store_sync_unsupported()),
        }
    }
}

impl StorageObjectReadBackend for StorageBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        match self {
            StorageBackend::Native(backend) => backend.read_object_bytes(object),
            StorageBackend::ObjectStore(backend) => backend.read_object_bytes(object),
        }
    }
}

impl BlockingStorageObjectReadBackend for StorageBackend {
    fn read_object_bytes_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        match self {
            StorageBackend::Native(backend) => backend.read_object_bytes_blocking(object),
            StorageBackend::ObjectStore(_) => Err(object_store_sync_unsupported()),
        }
    }
}

impl StorageAppendBackend for StorageBackend {
    type AppendObject = BackendAppendObject;

    fn open_append(&self, object: StorageObjectId) -> StorageFuture<'_, Self::AppendObject> {
        match self {
            StorageBackend::Native(backend) => Box::pin(async move {
                Ok(BackendAppendObject::Native(
                    backend.open_append(object).await?,
                ))
            }),
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store backend has no appendable objects",
                ))
            }),
        }
    }
}

impl BlockingStorageAppendBackend for StorageBackend {}

impl StorageWalRewriteBackend for StorageBackend {
    fn rewrite_wal(
        &self,
        object: StorageObjectId,
        temporary_object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        match self {
            StorageBackend::Native(backend) => {
                backend.rewrite_wal(object, temporary_object, bytes, durability)
            }
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store WAL rewrite is managed by the durability substrate",
                ))
            }),
        }
    }
}

impl BlockingStorageWalRewriteBackend for StorageBackend {}

impl StorageWriterLeaseBackend for StorageBackend {
    type WriterLease = BackendWriterLease;

    fn acquire_writer_lease(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Self::WriterLease> {
        match self {
            StorageBackend::Native(backend) => Box::pin(async move {
                Ok(BackendWriterLease::Native(
                    backend.acquire_writer_lease(object).await?,
                ))
            }),
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store writer lease is held by the durability substrate, not this backend",
                ))
            }),
        }
    }
}

impl BlockingStorageWriterLeaseBackend for StorageBackend {}

impl StorageDirectoryCreateBackend for StorageBackend {
    fn create_directory_all(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()> {
        match self {
            StorageBackend::Native(backend) => backend.create_directory_all(directory),
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store backend has no directories",
                ))
            }),
        }
    }
}

impl BlockingStorageDirectoryCreateBackend for StorageBackend {}

impl StorageDirectoryListBackend for StorageBackend {
    fn list_directory_files(
        &self,
        directory: StorageDirectoryId,
    ) -> StorageFuture<'_, Vec<StorageDirectoryFile>> {
        match self {
            StorageBackend::Native(backend) => backend.list_directory_files(directory),
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store backend has no directories",
                ))
            }),
        }
    }
}

impl BlockingStorageDirectoryListBackend for StorageBackend {}

impl StorageDirectorySyncBackend for StorageBackend {
    fn sync_directory_after_renames(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()> {
        match self {
            StorageBackend::Native(backend) => backend.sync_directory_after_renames(directory),
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store backend has no directories",
                ))
            }),
        }
    }
}

impl BlockingStorageDirectorySyncBackend for StorageBackend {}

impl StorageManifestReadBackend for StorageBackend {
    fn read_current_manifest(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        match self {
            StorageBackend::Native(backend) => backend.read_current_manifest(object),
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store manifest is read via ObjectManifestStore",
                ))
            }),
        }
    }
}

impl BlockingStorageManifestReadBackend for StorageBackend {}

impl StorageManifestPublishBackend for StorageBackend {
    fn publish_manifest(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        match self {
            StorageBackend::Native(backend) => backend.publish_manifest(object, bytes, durability),
            StorageBackend::ObjectStore(_) => Box::pin(async move {
                Err(Error::unsupported_backend(
                    "object-store manifest is published via ObjectManifestStore CAS",
                ))
            }),
        }
    }
}

impl BlockingStorageManifestPublishBackend for StorageBackend {}

impl StorageObjectWriteBackend for StorageBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        match self {
            StorageBackend::Native(backend) => backend.write_object(object, bytes, durability),
            StorageBackend::ObjectStore(backend) => backend.write_object(object, bytes, durability),
        }
    }
}

impl BlockingStorageObjectWriteBackend for StorageBackend {}

impl StorageObjectDeleteBackend for StorageBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
        match self {
            StorageBackend::Native(backend) => backend.delete_object(object),
            StorageBackend::ObjectStore(backend) => backend.delete_object(object),
        }
    }
}

impl BlockingStorageObjectDeleteBackend for StorageBackend {}

impl StorageObjectListBackend for StorageBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>> {
        match self {
            StorageBackend::Native(backend) => backend.list_objects(request),
            StorageBackend::ObjectStore(backend) => backend.list_objects(request),
        }
    }
}

impl BlockingStorageObjectListBackend for StorageBackend {}

impl StorageReadObject for BackendReadObject {
    fn object(&self) -> &StorageObjectId {
        match self {
            BackendReadObject::Native(object) => object.object(),
            BackendReadObject::ObjectStore(object) => object.object(),
        }
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        match self {
            BackendReadObject::Native(object) => object.len(),
            BackendReadObject::ObjectStore(object) => object.len(),
        }
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        match self {
            BackendReadObject::Native(object) => object.read_exact_at(offset, bytes),
            BackendReadObject::ObjectStore(object) => object.read_exact_at(offset, bytes),
        }
    }
}

impl BlockingStorageReadObject for BackendReadObject {
    fn len_blocking(&self) -> Result<u64> {
        match self {
            BackendReadObject::Native(object) => object.len_blocking(),
            BackendReadObject::ObjectStore(_) => Err(object_store_sync_unsupported()),
        }
    }

    fn read_exact_at_blocking(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        match self {
            BackendReadObject::Native(object) => object.read_exact_at_blocking(offset, bytes),
            BackendReadObject::ObjectStore(_) => Err(object_store_sync_unsupported()),
        }
    }
}

impl StorageAppendObject for BackendAppendObject {
    fn append<'op>(
        &'op mut self,
        bytes: &'op [u8],
        durability: DurabilityMode,
    ) -> StorageFuture<'op, ()> {
        match self {
            BackendAppendObject::Native(object) => object.append(bytes, durability),
        }
    }

    fn persist(&mut self, durability: DurabilityMode) -> StorageFuture<'_, ()> {
        match self {
            BackendAppendObject::Native(object) => object.persist(durability),
        }
    }
}

impl BlockingStorageAppendObject for BackendAppendObject {}
