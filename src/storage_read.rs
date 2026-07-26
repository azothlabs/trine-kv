//! Backend-neutral random-read capability used by database read paths.
//!
//! This adapter intentionally exposes only random reads. Database ownership,
//! WAL appends, publication, deletion, listing, and writer leases stay behind
//! their dedicated backend boundaries.

use crate::{
    object_store::{ObjectStoreBackend, ObjectStoreReadObject},
    storage::{
        MemoryStorageBackend, MemoryStorageObject, NativeFileBackend, NativeFileObject,
        StorageCapabilities, StorageObjectId, StorageReadBackend, StorageReadFuture,
        StorageReadObject,
    },
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::storage::{BrowserStorageBackend, BrowserStorageObject};

/// Cloneable random-read access for every supported database backend.
#[derive(Debug, Clone)]
pub(crate) enum ReadBackend {
    Memory(MemoryStorageBackend),
    Filesystem(NativeFileBackend),
    ObjectStore(ObjectStoreBackend),
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Browser(BrowserStorageBackend),
}

/// Open object returned by [`ReadBackend`].
#[derive(Debug)]
pub(crate) enum ReadObject {
    Memory(MemoryStorageObject),
    Filesystem(NativeFileObject),
    ObjectStore(ObjectStoreReadObject),
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Browser(BrowserStorageObject),
}

impl ReadBackend {
    pub(crate) fn filesystem(&self) -> Option<&NativeFileBackend> {
        match self {
            Self::Filesystem(backend) => Some(backend),
            _ => None,
        }
    }
}

impl StorageReadBackend for ReadBackend {
    type ReadObject = ReadObject;

    fn capabilities(&self) -> StorageCapabilities {
        match self {
            Self::Memory(backend) => backend.capabilities(),
            Self::Filesystem(backend) => backend.capabilities(),
            Self::ObjectStore(backend) => backend.capabilities(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(backend) => backend.capabilities(),
        }
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        match self {
            Self::Memory(backend) => {
                Box::pin(async move { Ok(ReadObject::Memory(backend.open_read(object).await?)) })
            }
            Self::Filesystem(backend) => {
                Box::pin(
                    async move { Ok(ReadObject::Filesystem(backend.open_read(object).await?)) },
                )
            }
            Self::ObjectStore(backend) => {
                Box::pin(
                    async move { Ok(ReadObject::ObjectStore(backend.open_read(object).await?)) },
                )
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(backend) => {
                Box::pin(async move { Ok(ReadObject::Browser(backend.open_read(object).await?)) })
            }
        }
    }
}

impl StorageReadObject for ReadObject {
    fn object(&self) -> &StorageObjectId {
        match self {
            Self::Memory(object) => object.object(),
            Self::Filesystem(object) => object.object(),
            Self::ObjectStore(object) => object.object(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(object) => object.object(),
        }
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        match self {
            Self::Memory(object) => object.len(),
            Self::Filesystem(object) => object.len(),
            Self::ObjectStore(object) => object.len(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(object) => object.len(),
        }
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        match self {
            Self::Memory(object) => object.read_exact_at(offset, bytes),
            Self::Filesystem(object) => object.read_exact_at(offset, bytes),
            Self::ObjectStore(object) => object.read_exact_at(offset, bytes),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(object) => object.read_exact_at(offset, bytes),
        }
    }
}
