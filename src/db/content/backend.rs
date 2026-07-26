use std::sync::Arc;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::error::Error;
use crate::{
    DurabilityMode,
    error::Result,
    object_store::ObjectStoreBackend,
    storage::{
        MemoryStorageBackend, NativeFileBackend, StorageObjectDeleteBackend, StorageObjectId,
        StorageObjectListBackend, StorageObjectListRequest, StorageObjectReadBackend,
        StorageObjectWriteBackend,
    },
};

use super::Db;

/// Narrow storage adapter for immutable-content objects.
///
/// Platform selection happens once when the adapter is created. Content state
/// machines call one semantic operation and do not duplicate native, memory,
/// object-store, and browser dispatch.
#[derive(Debug, Clone)]
pub(super) enum ContentObjectBackend {
    Memory(MemoryStorageBackend),
    Native(NativeFileBackend),
    ObjectStore(ObjectStoreBackend),
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Browser(crate::storage::BrowserStorageBackend),
}

impl ContentObjectBackend {
    pub(super) fn for_db(db: &Db) -> Result<Self> {
        match &db.inner.options.storage_mode {
            crate::StorageMode::InMemory => Ok(Self::Memory(db.inner.content_memory.clone())),
            crate::StorageMode::Persistent { .. }
            | crate::StorageMode::HostPersistent {
                backend: crate::HostStorageBackend::Wasi { .. },
            } => Ok(Self::Native(db.inner.native_storage.clone())),
            crate::StorageMode::HostPersistent {
                backend: crate::HostStorageBackend::ObjectStore,
            } => Ok(Self::ObjectStore(db.object_storage()?)),
            crate::StorageMode::HostPersistent {
                backend: crate::HostStorageBackend::Browser { .. },
            } => {
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                {
                    Ok(Self::Browser(db.browser_storage()?))
                }
                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                {
                    Err(Error::unsupported_backend(
                        "browser content objects require wasm32-unknown-unknown",
                    ))
                }
            }
        }
    }

    pub(super) const fn requires_lease_fence(&self) -> bool {
        matches!(self, Self::ObjectStore(_))
    }

    pub(super) async fn read(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        match self {
            Self::Memory(backend) => backend.read_object_bytes(object).await,
            Self::Native(backend) => backend.read_object_bytes(object).await,
            Self::ObjectStore(backend) => backend.read_object_bytes(object).await,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(backend) => backend.read_object_bytes(object).await,
        }
    }

    pub(super) async fn write(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.write_object(object, bytes, durability).await,
            Self::Native(backend) => backend.write_object(object, bytes, durability).await,
            Self::ObjectStore(backend) => backend.write_object(object, bytes, durability).await,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(backend) => backend.write_object(object, bytes, durability).await,
        }
    }

    pub(super) async fn delete(
        &self,
        object: StorageObjectId,
        durability: DurabilityMode,
    ) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.delete_object(object).await,
            Self::Native(backend) => backend.delete_object_durable(object, durability).await,
            Self::ObjectStore(backend) => backend.delete_unversioned_object_verified(object).await,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(backend) => backend.delete_object(object).await,
        }
    }

    pub(super) async fn list(
        &self,
        request: StorageObjectListRequest,
    ) -> Result<Vec<StorageObjectId>> {
        match self {
            Self::Memory(backend) => backend.list_objects(request).await,
            Self::Native(backend) => backend.list_objects(request).await,
            Self::ObjectStore(backend) => backend.list_objects(request).await,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser(backend) => backend.list_objects(request).await,
        }
    }
}
