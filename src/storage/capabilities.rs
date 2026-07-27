//! Backend capability declarations and durability qualification.

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageCapability {
    Volatile,
    Persistent,
    RandomRead,
    ObjectRead,
    ObjectListing,
    ObjectWrite,
    ObjectDelete,
    Append,
    AtomicWalRewrite,
    DirectoryCreate,
    DirectoryListing,
    DirectorySync,
    AtomicManifestPublish,
    WriterLease,
    Flush,
    StrictDataSync,
    StrictMetadataSync,
    BackgroundThreads,
    AsyncTasks,
    BlockingAdapter,
    PlatformAsyncIo,
    CooperativeTasks,
}

impl StorageCapability {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Volatile => "volatile storage",
            Self::Persistent => "persistent storage",
            Self::RandomRead => "random read",
            Self::ObjectRead => "object read",
            Self::ObjectListing => "object listing",
            Self::ObjectWrite => "object write",
            Self::ObjectDelete => "object delete",
            Self::Append => "append",
            Self::AtomicWalRewrite => "atomic WAL rewrite",
            Self::DirectoryCreate => "directory create",
            Self::DirectoryListing => "directory listing",
            Self::DirectorySync => "directory sync",
            Self::AtomicManifestPublish => "atomic manifest publish",
            Self::WriterLease => "writer lease",
            Self::Flush => "flush",
            Self::StrictDataSync => "strict data sync",
            Self::StrictMetadataSync => "strict metadata sync",
            Self::BackgroundThreads => "background threads",
            Self::AsyncTasks => "async tasks",
            Self::BlockingAdapter => "sync storage adapter",
            Self::PlatformAsyncIo => "platform async I/O",
            Self::CooperativeTasks => "cooperative tasks",
        }
    }

    const fn bit(self) -> u32 {
        match self {
            Self::Volatile => 1 << 0,
            Self::Persistent => 1 << 1,
            Self::RandomRead => 1 << 2,
            Self::ObjectRead => 1 << 3,
            Self::ObjectListing => 1 << 4,
            Self::ObjectWrite => 1 << 5,
            Self::ObjectDelete => 1 << 6,
            Self::Append => 1 << 7,
            Self::AtomicWalRewrite => 1 << 8,
            Self::DirectoryCreate => 1 << 9,
            Self::DirectoryListing => 1 << 10,
            Self::DirectorySync => 1 << 11,
            Self::AtomicManifestPublish => 1 << 12,
            Self::WriterLease => 1 << 13,
            Self::Flush => 1 << 14,
            Self::StrictDataSync => 1 << 15,
            Self::StrictMetadataSync => 1 << 16,
            Self::BackgroundThreads => 1 << 17,
            Self::AsyncTasks => 1 << 18,
            Self::BlockingAdapter => 1 << 19,
            Self::PlatformAsyncIo => 1 << 20,
            Self::CooperativeTasks => 1 << 21,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageCapabilities {
    bits: u32,
}

impl StorageCapabilities {
    pub(crate) const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub(crate) const fn native_file_read() -> Self {
        Self::empty()
            .with(StorageCapability::Persistent)
            .with(StorageCapability::RandomRead)
            .with(StorageCapability::ObjectRead)
            .with(StorageCapability::ObjectListing)
            .with(StorageCapability::DirectoryListing)
    }

    pub(crate) const fn native_file() -> Self {
        let capabilities = Self::native_file_read()
            .with(StorageCapability::ObjectWrite)
            .with(StorageCapability::ObjectDelete)
            .with(StorageCapability::Append)
            .with(StorageCapability::AtomicWalRewrite)
            .with(StorageCapability::DirectoryCreate)
            .with(StorageCapability::AtomicManifestPublish)
            .with(StorageCapability::WriterLease)
            .with(StorageCapability::Flush);

        #[cfg(target_os = "wasi")]
        {
            capabilities
        }

        #[cfg(not(target_os = "wasi"))]
        {
            capabilities
                .with(StorageCapability::DirectorySync)
                .with(StorageCapability::StrictDataSync)
                .with(StorageCapability::StrictMetadataSync)
        }
    }

    pub(crate) const fn memory_read() -> Self {
        Self::empty()
            .with(StorageCapability::Volatile)
            .with(StorageCapability::RandomRead)
            .with(StorageCapability::ObjectRead)
    }

    /// Byte-level capabilities of an object-storage backend: whole-object
    /// get/put/delete + prefix listing. Append, atomic rename/WAL-rewrite, and
    /// filesystem locking are deliberately absent — those diverge and are handled
    /// by the object-storage durability substrate (immutable WAL segments +
    /// manifest CAS + lease/head object), not this byte backend.
    pub(crate) const fn object_store() -> Self {
        Self::empty()
            .with(StorageCapability::Persistent)
            .with(StorageCapability::RandomRead)
            .with(StorageCapability::ObjectRead)
            .with(StorageCapability::ObjectWrite)
            .with(StorageCapability::ObjectDelete)
            .with(StorageCapability::ObjectListing)
    }

    pub(crate) const fn with(self, capability: StorageCapability) -> Self {
        Self {
            bits: self.bits | capability.bit(),
        }
    }

    pub(crate) const fn supports(self, capability: StorageCapability) -> bool {
        self.bits & capability.bit() != 0
    }

    pub(crate) fn require(self, capability: StorageCapability) -> Result<()> {
        if self.supports(capability) {
            Ok(())
        } else {
            Err(Error::unsupported_backend(capability.as_str()))
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn supports_durability(self, durability: DurabilityMode) -> bool {
        match durability {
            DurabilityMode::Buffered => true,
            DurabilityMode::Flush => self.supports(StorageCapability::Flush),
            DurabilityMode::SyncData => self.supports(StorageCapability::StrictDataSync),
            DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
                self.supports(StorageCapability::StrictDataSync)
                    && self.supports(StorageCapability::StrictMetadataSync)
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn require_durability(self, durability: DurabilityMode) -> Result<()> {
        if self.supports_durability(durability) {
            Ok(())
        } else {
            Err(Error::unsupported_durability(durability))
        }
    }
}
