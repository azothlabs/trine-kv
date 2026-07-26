use std::path::Path;

#[cfg(feature = "platform-io")]
use crate::error::Result;
use crate::{
    object_store::ObjectStoreBackend,
    storage::{MemoryStorageBackend, NativeFileBackend},
    storage_read::ReadBackend,
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::{
    storage::{BrowserStorageBackend, BrowserWriterLease},
    wal::BrowserWalFrontDoor,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use std::sync::Mutex;

/// The storage resources owned by one open database.
///
/// Each variant contains exactly the resources valid for that backend. This
/// prevents non-filesystem databases from carrying an unused native backend and
/// prevents independently optional object-store or browser resources from
/// forming invalid combinations.
#[derive(Debug)]
pub(in crate::db) enum DatabaseStorage {
    Memory {
        content: MemoryStorageBackend,
    },
    Filesystem {
        files: NativeFileBackend,
        root: std::path::PathBuf,
    },
    ObjectStore {
        objects: ObjectStoreBackend,
        wal_objects: ObjectStoreBackend,
        prefix: std::path::PathBuf,
    },
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Browser {
        files: BrowserStorageBackend,
        root: std::path::PathBuf,
        writer_lease: Mutex<Option<BrowserWriterLease>>,
        wal: Option<BrowserWalFrontDoor>,
    },
}

#[derive(Clone, Copy)]
pub(in crate::db) struct MemoryResources<'storage> {
    pub(in crate::db) content: &'storage MemoryStorageBackend,
}

#[derive(Clone, Copy)]
pub(in crate::db) struct FilesystemResources<'storage> {
    pub(in crate::db) files: &'storage NativeFileBackend,
    pub(in crate::db) root: &'storage Path,
}

#[derive(Clone, Copy)]
pub(in crate::db) struct ObjectStoreResources<'storage> {
    pub(in crate::db) objects: &'storage ObjectStoreBackend,
    pub(in crate::db) wal_objects: &'storage ObjectStoreBackend,
    pub(in crate::db) prefix: &'storage Path,
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Clone, Copy)]
pub(in crate::db) struct BrowserResources<'storage> {
    pub(in crate::db) files: &'storage BrowserStorageBackend,
    pub(in crate::db) root: &'storage Path,
    pub(in crate::db) wal: Option<&'storage BrowserWalFrontDoor>,
}

#[derive(Clone, Copy)]
pub(in crate::db) enum DatabaseStorageRef<'storage> {
    Memory(MemoryResources<'storage>),
    Filesystem(FilesystemResources<'storage>),
    ObjectStore(ObjectStoreResources<'storage>),
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Browser(BrowserResources<'storage>),
}

impl DatabaseStorage {
    pub(in crate::db) fn memory() -> Self {
        Self::Memory {
            content: MemoryStorageBackend::new(),
        }
    }

    pub(in crate::db) const fn filesystem(
        files: NativeFileBackend,
        root: std::path::PathBuf,
    ) -> Self {
        Self::Filesystem { files, root }
    }

    pub(in crate::db) fn object_store(
        objects: ObjectStoreBackend,
        wal_objects: ObjectStoreBackend,
        prefix: std::path::PathBuf,
    ) -> Self {
        Self::ObjectStore {
            objects,
            wal_objects,
            prefix,
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) fn browser(
        files: BrowserStorageBackend,
        root: std::path::PathBuf,
        writer_lease: Option<BrowserWriterLease>,
        wal: Option<BrowserWalFrontDoor>,
    ) -> Self {
        Self::Browser {
            files,
            root,
            writer_lease: Mutex::new(writer_lease),
            wal,
        }
    }

    pub(in crate::db) fn resources(&self) -> DatabaseStorageRef<'_> {
        match self {
            Self::Memory { content } => DatabaseStorageRef::Memory(MemoryResources { content }),
            Self::Filesystem { files, root } => {
                DatabaseStorageRef::Filesystem(FilesystemResources { files, root })
            }
            Self::ObjectStore {
                objects,
                wal_objects,
                prefix,
            } => DatabaseStorageRef::ObjectStore(ObjectStoreResources {
                objects,
                wal_objects,
                prefix,
            }),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser {
                files, root, wal, ..
            } => DatabaseStorageRef::Browser(BrowserResources {
                files,
                root,
                wal: wal.as_ref(),
            }),
        }
    }

    pub(in crate::db) fn read_backend(&self) -> ReadBackend {
        match self.resources() {
            DatabaseStorageRef::Memory(resources) => ReadBackend::Memory(resources.content.clone()),
            DatabaseStorageRef::Filesystem(resources) => {
                ReadBackend::Filesystem(resources.files.clone())
            }
            DatabaseStorageRef::ObjectStore(resources) => {
                ReadBackend::ObjectStore(resources.objects.clone())
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            DatabaseStorageRef::Browser(resources) => ReadBackend::Browser(resources.files.clone()),
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) fn release_writer_lease(&self) {
        if let Self::Browser { writer_lease, .. } = self {
            let Ok(mut lease) = writer_lease.lock() else {
                return;
            };
            let _ = lease.take();
        }
    }

    #[cfg(feature = "platform-io")]
    pub(in crate::db) fn close_platform_io(&self) -> Result<()> {
        match self {
            Self::Filesystem { files, .. } => files.close_platform_io(),
            Self::Memory { .. } | Self::ObjectStore { .. } => Ok(()),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser { .. } => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{super::DbInner, DatabaseStorage};

    #[test]
    fn db_inner_storage_field_has_one_exhaustive_backend_type() {
        fn storage_of(inner: &DbInner) -> &DatabaseStorage {
            &inner.storage
        }

        let _ = storage_of;
    }
}
