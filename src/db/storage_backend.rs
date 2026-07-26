use std::path::Path;

use crate::{
    error::{Error, Result},
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
    },
    ObjectStore {
        objects: ObjectStoreBackend,
        wal_objects: ObjectStoreBackend,
        prefix: std::path::PathBuf,
    },
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Browser {
        files: BrowserStorageBackend,
        writer_lease: Mutex<Option<BrowserWriterLease>>,
        wal: Option<BrowserWalFrontDoor>,
    },
}

impl DatabaseStorage {
    pub(in crate::db) fn memory() -> Self {
        Self::Memory {
            content: MemoryStorageBackend::new(),
        }
    }

    pub(in crate::db) const fn filesystem(files: NativeFileBackend) -> Self {
        Self::Filesystem { files }
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
        writer_lease: Option<BrowserWriterLease>,
        wal: Option<BrowserWalFrontDoor>,
    ) -> Self {
        Self::Browser {
            files,
            writer_lease: Mutex::new(writer_lease),
            wal,
        }
    }

    pub(in crate::db) fn memory_content(&self) -> Result<MemoryStorageBackend> {
        match self {
            Self::Memory { content } => Ok(content.clone()),
            _ => Err(Error::Corruption {
                message: "in-memory database is missing its content backend".to_owned(),
            }),
        }
    }

    pub(in crate::db) fn read_backend(&self) -> ReadBackend {
        match self {
            Self::Memory { content } => ReadBackend::Memory(content.clone()),
            Self::Filesystem { files } => ReadBackend::Filesystem(files.clone()),
            Self::ObjectStore { objects, .. } => ReadBackend::ObjectStore(objects.clone()),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser { files, .. } => ReadBackend::Browser(files.clone()),
        }
    }

    pub(in crate::db) fn filesystem_files(&self) -> Result<&NativeFileBackend> {
        match self {
            Self::Filesystem { files } => Ok(files),
            _ => Err(Error::Corruption {
                message: "filesystem database is missing its native storage backend".to_owned(),
            }),
        }
    }

    pub(in crate::db) const fn filesystem_files_if_present(&self) -> Option<&NativeFileBackend> {
        match self {
            Self::Filesystem { files } => Some(files),
            _ => None,
        }
    }

    pub(in crate::db) fn filesystem_files_cloned(&self) -> Result<NativeFileBackend> {
        self.filesystem_files().cloned()
    }

    pub(in crate::db) fn object_store_objects(&self) -> Result<ObjectStoreBackend> {
        match self {
            Self::ObjectStore { objects, .. } => Ok(objects.clone()),
            _ => Err(Error::Corruption {
                message: "object-store database is missing its data backend".to_owned(),
            }),
        }
    }

    pub(in crate::db) fn object_store_wal_objects(&self) -> Result<ObjectStoreBackend> {
        match self {
            Self::ObjectStore { wal_objects, .. } => Ok(wal_objects.clone()),
            _ => Err(Error::Corruption {
                message: "object-store database is missing its WAL backend".to_owned(),
            }),
        }
    }

    pub(in crate::db) fn object_store_prefix(&self) -> Result<&Path> {
        match self {
            Self::ObjectStore { prefix, .. } => Ok(prefix),
            _ => Err(Error::Corruption {
                message: "object-store database is missing its key prefix".to_owned(),
            }),
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) fn browser_files(&self) -> Result<BrowserStorageBackend> {
        match self {
            Self::Browser { files, .. } => Ok(files.clone()),
            _ => Err(Error::Corruption {
                message: "browser database is missing its OPFS backend".to_owned(),
            }),
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) fn browser_wal(&self) -> Option<&BrowserWalFrontDoor> {
        match self {
            Self::Browser { wal, .. } => wal.as_ref(),
            _ => None,
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
            Self::Filesystem { files } => files.close_platform_io(),
            Self::Memory { .. } | Self::ObjectStore { .. } => Ok(()),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::Browser { .. } => Ok(()),
        }
    }
}
