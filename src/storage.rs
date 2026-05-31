use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    block::BlockReadSource,
    durability::sync_parent_dir_after_rename,
    error::{Error, Result},
    options::DurabilityMode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum StorageObjectKind {
    Blob,
    Manifest,
    Table,
    Wal,
    WriterLease,
}

impl StorageObjectKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Manifest => "manifest",
            Self::Table => "table",
            Self::Wal => "WAL",
            Self::WriterLease => "writer lease",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StorageObjectId {
    kind: StorageObjectKind,
    path: PathBuf,
}

impl StorageObjectId {
    pub(crate) fn native_file(kind: StorageObjectKind, path: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            path: path.into(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn memory(kind: StorageObjectKind, name: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            path: name.into(),
        }
    }

    pub(crate) const fn kind(&self) -> StorageObjectKind {
        self.kind
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageObjectListRequest {
    kind: StorageObjectKind,
    root: PathBuf,
    file_extension: Option<&'static str>,
}

impl StorageObjectListRequest {
    pub(crate) fn native_file(kind: StorageObjectKind, root: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            root: root.into(),
            file_extension: None,
        }
    }

    pub(crate) fn with_file_extension(mut self, file_extension: &'static str) -> Self {
        self.file_extension = Some(file_extension);
        self
    }

    pub(crate) const fn kind(&self) -> StorageObjectKind {
        self.kind
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    const fn file_extension(&self) -> Option<&'static str> {
        self.file_extension
    }
}

pub(crate) type StorageFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'op>>;
pub(crate) type StorageReadFuture<'op, T> = StorageFuture<'op, T>;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageCapability {
    Volatile,
    Persistent,
    RandomRead,
    ObjectListing,
    ObjectWrite,
    ObjectDelete,
    Append,
    AtomicManifestPublish,
    WriterLease,
    Flush,
    StrictDataSync,
    StrictMetadataSync,
    BackgroundThreads,
    AsyncTasks,
    CooperativeTasks,
}

impl StorageCapability {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Volatile => "volatile storage",
            Self::Persistent => "persistent storage",
            Self::RandomRead => "random read",
            Self::ObjectListing => "object listing",
            Self::ObjectWrite => "object write",
            Self::ObjectDelete => "object delete",
            Self::Append => "append",
            Self::AtomicManifestPublish => "atomic manifest publish",
            Self::WriterLease => "writer lease",
            Self::Flush => "flush",
            Self::StrictDataSync => "strict data sync",
            Self::StrictMetadataSync => "strict metadata sync",
            Self::BackgroundThreads => "background threads",
            Self::AsyncTasks => "async tasks",
            Self::CooperativeTasks => "cooperative tasks",
        }
    }

    const fn bit(self) -> u16 {
        match self {
            Self::Volatile => 1 << 0,
            Self::Persistent => 1 << 1,
            Self::RandomRead => 1 << 2,
            Self::ObjectListing => 1 << 3,
            Self::ObjectWrite => 1 << 4,
            Self::ObjectDelete => 1 << 5,
            Self::Append => 1 << 6,
            Self::AtomicManifestPublish => 1 << 7,
            Self::WriterLease => 1 << 8,
            Self::Flush => 1 << 9,
            Self::StrictDataSync => 1 << 10,
            Self::StrictMetadataSync => 1 << 11,
            Self::BackgroundThreads => 1 << 12,
            Self::AsyncTasks => 1 << 13,
            Self::CooperativeTasks => 1 << 14,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageCapabilities {
    bits: u16,
}

impl StorageCapabilities {
    pub(crate) const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub(crate) const fn native_file_read() -> Self {
        Self::empty()
            .with(StorageCapability::Persistent)
            .with(StorageCapability::RandomRead)
            .with(StorageCapability::ObjectListing)
    }

    pub(crate) const fn native_file() -> Self {
        Self::native_file_read()
            .with(StorageCapability::ObjectWrite)
            .with(StorageCapability::ObjectDelete)
            .with(StorageCapability::Append)
            .with(StorageCapability::AtomicManifestPublish)
            .with(StorageCapability::WriterLease)
            .with(StorageCapability::Flush)
            .with(StorageCapability::StrictDataSync)
            .with(StorageCapability::StrictMetadataSync)
    }

    pub(crate) const fn memory_read() -> Self {
        Self::empty()
            .with(StorageCapability::Volatile)
            .with(StorageCapability::RandomRead)
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
            DurabilityMode::SyncAll => {
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

pub(crate) trait StorageReadObject: Send + Sync {
    #[allow(dead_code)]
    fn object(&self) -> &StorageObjectId;

    fn len(&self) -> StorageReadFuture<'_, u64>;

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()>;
}

pub(crate) trait BlockingStorageReadObject: StorageReadObject {
    fn len_blocking(&self) -> Result<u64>;

    fn read_exact_at_blocking(&self, offset: usize, bytes: &mut [u8]) -> Result<()>;
}

pub(crate) trait StorageReadBackend: Send + Sync {
    type ReadObject: StorageReadObject;

    fn capabilities(&self) -> StorageCapabilities;

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject>;
}

pub(crate) trait BlockingStorageReadBackend: StorageReadBackend
where
    Self::ReadObject: BlockingStorageReadObject,
{
    fn open_read_blocking(&self, object: StorageObjectId) -> Result<Self::ReadObject>;
}

pub(crate) trait StorageAppendObject: Send {
    fn append<'op>(
        &'op mut self,
        bytes: &'op [u8],
        durability: DurabilityMode,
    ) -> StorageFuture<'op, ()>;

    fn persist(&mut self, durability: DurabilityMode) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageAppendObject: StorageAppendObject {
    fn append_blocking(&mut self, bytes: &[u8], durability: DurabilityMode) -> Result<()>;

    fn persist_blocking(&mut self, durability: DurabilityMode) -> Result<()>;
}

pub(crate) trait StorageAppendBackend: StorageReadBackend {
    type AppendObject: StorageAppendObject;

    fn open_append(&self, object: StorageObjectId) -> StorageFuture<'_, Self::AppendObject>;
}

pub(crate) trait BlockingStorageAppendBackend: StorageAppendBackend
where
    Self::AppendObject: BlockingStorageAppendObject,
{
    fn open_append_blocking(&self, object: StorageObjectId) -> Result<Self::AppendObject>;
}

pub(crate) trait StorageWriterLeaseBackend: StorageReadBackend {
    type WriterLease: Send;

    fn acquire_writer_lease(&self, object: StorageObjectId)
    -> StorageFuture<'_, Self::WriterLease>;
}

pub(crate) trait BlockingStorageWriterLeaseBackend: StorageWriterLeaseBackend {
    fn acquire_writer_lease_blocking(&self, object: StorageObjectId) -> Result<Self::WriterLease>;
}

pub(crate) trait StorageManifestReadBackend: StorageReadBackend {
    fn read_current_manifest(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Option<Arc<[u8]>>>;
}

pub(crate) trait BlockingStorageManifestReadBackend: StorageManifestReadBackend {
    fn read_current_manifest_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>>;
}

pub(crate) trait StorageManifestPublishBackend: StorageReadBackend {
    fn publish_manifest(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageManifestPublishBackend:
    StorageManifestPublishBackend
{
    fn publish_manifest_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()>;
}

pub(crate) trait StorageObjectWriteBackend: StorageReadBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageObjectWriteBackend: StorageObjectWriteBackend {
    fn write_object_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()>;
}

pub(crate) trait StorageObjectDeleteBackend: StorageReadBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageObjectDeleteBackend: StorageObjectDeleteBackend {
    fn delete_object_blocking(&self, object: StorageObjectId) -> Result<()>;
}

pub(crate) trait StorageObjectListBackend: StorageReadBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>>;
}

pub(crate) trait BlockingStorageObjectListBackend: StorageObjectListBackend {
    fn list_objects_blocking(
        &self,
        request: StorageObjectListRequest,
    ) -> Result<Vec<StorageObjectId>>;
}

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
        let objects = self.lock_objects()?;
        objects
            .get(object)
            .cloned()
            .ok_or_else(|| Error::Corruption {
                message: format!(
                    "referenced memory {} {} cannot be opened",
                    object.kind().as_str(),
                    object.path().display()
                ),
            })
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

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct NativeFileBackend;

impl NativeFileBackend {
    pub(crate) const fn new() -> Self {
        Self
    }
}

impl StorageReadBackend for NativeFileBackend {
    type ReadObject = NativeFileObject;

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::native_file()
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        Box::pin(async move { NativeFileObject::open(object) })
    }
}

impl BlockingStorageReadBackend for NativeFileBackend {
    fn open_read_blocking(&self, object: StorageObjectId) -> Result<Self::ReadObject> {
        poll_ready_storage_future(self.open_read(object))
    }
}

impl StorageAppendBackend for NativeFileBackend {
    type AppendObject = NativeFileAppendObject;

    fn open_append(&self, object: StorageObjectId) -> StorageFuture<'_, Self::AppendObject> {
        Box::pin(async move { NativeFileAppendObject::open(&object) })
    }
}

impl BlockingStorageAppendBackend for NativeFileBackend {
    fn open_append_blocking(&self, object: StorageObjectId) -> Result<Self::AppendObject> {
        poll_ready_storage_future(self.open_append(object))
    }
}

impl StorageWriterLeaseBackend for NativeFileBackend {
    type WriterLease = NativeFileWriterLease;

    fn acquire_writer_lease(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Self::WriterLease> {
        Box::pin(async move { NativeFileWriterLease::acquire(object) })
    }
}

impl BlockingStorageWriterLeaseBackend for NativeFileBackend {
    fn acquire_writer_lease_blocking(&self, object: StorageObjectId) -> Result<Self::WriterLease> {
        poll_ready_storage_future(self.acquire_writer_lease(object))
    }
}

impl StorageManifestReadBackend for NativeFileBackend {
    fn read_current_manifest(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        Box::pin(async move { read_current_manifest_from_native_file(&object) })
    }
}

impl BlockingStorageManifestReadBackend for NativeFileBackend {
    fn read_current_manifest_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        poll_ready_storage_future(self.read_current_manifest(object))
    }
}

impl StorageManifestPublishBackend for NativeFileBackend {
    fn publish_manifest(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        Box::pin(async move { publish_manifest_to_native_file(&object, &bytes, durability) })
    }
}

impl BlockingStorageManifestPublishBackend for NativeFileBackend {
    fn publish_manifest_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        poll_ready_storage_future(self.publish_manifest(object, bytes, durability))
    }
}

impl StorageObjectWriteBackend for NativeFileBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        Box::pin(async move { write_native_file_object(&object, &bytes, durability) })
    }
}

impl BlockingStorageObjectWriteBackend for NativeFileBackend {
    fn write_object_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        poll_ready_storage_future(self.write_object(object, bytes, durability))
    }
}

impl StorageObjectDeleteBackend for NativeFileBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
        Box::pin(async move { delete_native_file_object(&object) })
    }
}

impl BlockingStorageObjectDeleteBackend for NativeFileBackend {
    fn delete_object_blocking(&self, object: StorageObjectId) -> Result<()> {
        poll_ready_storage_future(self.delete_object(object))
    }
}

impl StorageObjectListBackend for NativeFileBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>> {
        Box::pin(async move { list_native_file_objects(&request) })
    }
}

impl BlockingStorageObjectListBackend for NativeFileBackend {
    fn list_objects_blocking(
        &self,
        request: StorageObjectListRequest,
    ) -> Result<Vec<StorageObjectId>> {
        poll_ready_storage_future(self.list_objects(request))
    }
}

#[derive(Debug)]
pub(crate) struct NativeFileObject {
    object: StorageObjectId,
    file: Mutex<File>,
}

impl NativeFileObject {
    fn open(object: StorageObjectId) -> Result<Self> {
        let file = open_native_file(&object)?;
        Ok(Self {
            object,
            file: Mutex::new(file),
        })
    }

    fn len_from_native_file(&self) -> Result<u64> {
        let file = self.lock_file()?;
        Ok(file.metadata()?.len())
    }

    fn read_exact_at_offset(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        let mut file = self.lock_file()?;
        read_exact_at_native_file(&mut file, offset, bytes)
    }

    fn lock_file(&self) -> Result<MutexGuard<'_, File>> {
        self.file.lock().map_err(|_| Error::Corruption {
            message: format!(
                "referenced {} {} handle lock poisoned",
                self.object.kind().as_str(),
                self.object.path().display()
            ),
        })
    }
}

impl StorageReadObject for NativeFileObject {
    fn object(&self) -> &StorageObjectId {
        &self.object
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        Box::pin(async move { self.len_from_native_file() })
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        Box::pin(async move { self.read_exact_at_offset(offset, bytes) })
    }
}

impl BlockingStorageReadObject for NativeFileObject {
    fn len_blocking(&self) -> Result<u64> {
        poll_ready_storage_future(StorageReadObject::len(self))
    }

    fn read_exact_at_blocking(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        poll_ready_storage_future(StorageReadObject::read_exact_at(self, offset, bytes))
    }
}

#[derive(Debug)]
pub(crate) struct NativeFileAppendObject {
    file: File,
}

impl NativeFileAppendObject {
    fn open(object: &StorageObjectId) -> Result<Self> {
        let file = open_native_append_file(object)?;
        Ok(Self { file })
    }
}

impl StorageAppendObject for NativeFileAppendObject {
    fn append<'op>(
        &'op mut self,
        bytes: &'op [u8],
        durability: DurabilityMode,
    ) -> StorageFuture<'op, ()> {
        Box::pin(async move { append_native_file_object(&mut self.file, bytes, durability) })
    }

    fn persist(&mut self, durability: DurabilityMode) -> StorageFuture<'_, ()> {
        Box::pin(async move { persist_native_append_file(&mut self.file, durability) })
    }
}

impl BlockingStorageAppendObject for NativeFileAppendObject {
    fn append_blocking(&mut self, bytes: &[u8], durability: DurabilityMode) -> Result<()> {
        poll_ready_storage_future(StorageAppendObject::append(self, bytes, durability))
    }

    fn persist_blocking(&mut self, durability: DurabilityMode) -> Result<()> {
        poll_ready_storage_future(StorageAppendObject::persist(self, durability))
    }
}

#[derive(Debug)]
pub(crate) struct NativeFileWriterLease {
    object: StorageObjectId,
    owner: String,
    file: Option<File>,
}

impl NativeFileWriterLease {
    fn acquire(object: StorageObjectId) -> Result<Self> {
        let mut file = acquire_native_file_writer_lease(&object)?;
        let owner = writer_lease_owner_text();
        if let Err(error) = write_native_file_writer_lease_owner(&mut file, &owner) {
            let _ = fs::remove_file(object.path());
            return Err(error);
        }

        Ok(Self {
            object,
            owner,
            file: Some(file),
        })
    }
}

impl Drop for NativeFileWriterLease {
    fn drop(&mut self) {
        let should_remove = fs::read_to_string(self.object.path())
            .is_ok_and(|contents| contents.as_str() == self.owner.as_str());
        drop(self.file.take());
        if should_remove {
            let _ = fs::remove_file(self.object.path());
        }
    }
}

pub(crate) struct StorageReadSource<'src, H> {
    object: &'src H,
}

impl<'src, H> StorageReadSource<'src, H> {
    pub(crate) const fn new(object: &'src H) -> Self {
        Self { object }
    }
}

impl<H> BlockReadSource for StorageReadSource<'_, H>
where
    H: BlockingStorageReadObject,
{
    fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        self.object.read_exact_at_blocking(offset, bytes)
    }
}

pub(crate) struct NativeFileReadSource<'src, H> {
    object: StorageObjectId,
    cached: Option<&'src H>,
}

impl<'src, H> NativeFileReadSource<'src, H> {
    pub(crate) fn new(object: StorageObjectId, cached: Option<&'src H>) -> Self {
        Self { object, cached }
    }
}

impl<H> BlockReadSource for NativeFileReadSource<'_, H>
where
    H: BlockingStorageReadObject,
{
    fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        if let Some(cached) = self.cached {
            return cached.read_exact_at_blocking(offset, bytes);
        }

        read_exact_from_native_file(&self.object, offset, bytes)
    }
}

fn read_exact_from_native_file(
    object: &StorageObjectId,
    offset: usize,
    bytes: &mut [u8],
) -> Result<()> {
    let file = NativeFileBackend::new().open_read_blocking(object.clone())?;
    file.read_exact_at_blocking(offset, bytes)
}

fn open_native_file(object: &StorageObjectId) -> Result<File> {
    File::open(object.path()).map_err(|error| Error::Corruption {
        message: format!(
            "referenced {} {} cannot be opened: {error}",
            object.kind().as_str(),
            object.path().display()
        ),
    })
}

fn read_exact_at_native_file(file: &mut File, offset: usize, bytes: &mut [u8]) -> Result<()> {
    file.seek(SeekFrom::Start(usize_to_u64(
        offset,
        "storage object read offset",
    )?))?;
    file.read_exact(bytes)?;
    Ok(())
}

fn open_native_append_file(object: &StorageObjectId) -> Result<File> {
    if object.kind() != StorageObjectKind::Wal {
        return Err(Error::invalid_options(
            "append storage objects must use WAL object kind",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::Append)?;

    if let Some(parent) = object.path().parent() {
        fs::create_dir_all(parent)?;
    }

    OpenOptions::new()
        .create(true)
        .append(true)
        .open(object.path())
        .map_err(Error::from)
}

fn append_native_file_object(
    file: &mut File,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::Append)?;
    capabilities.require_durability(durability)?;

    file.write_all(bytes)?;
    persist_native_append_file(file, durability)
}

fn persist_native_append_file(file: &mut File, durability: DurabilityMode) -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require_durability(durability)?;

    match durability {
        DurabilityMode::Buffered => Ok(()),
        DurabilityMode::Flush => {
            file.flush()?;
            Ok(())
        }
        DurabilityMode::SyncData => {
            file.sync_data()?;
            Ok(())
        }
        DurabilityMode::SyncAll => {
            file.sync_all()?;
            Ok(())
        }
    }
}

fn acquire_native_file_writer_lease(object: &StorageObjectId) -> Result<File> {
    if object.kind() != StorageObjectKind::WriterLease {
        return Err(Error::invalid_options(
            "writer lease requires a writer lease storage object",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::WriterLease)?;

    if let Some(parent) = object.path().parent() {
        fs::create_dir_all(parent)?;
    }

    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(object.path())
    {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(Error::Corruption {
            message: format!("database lock is already held: {}", object.path().display()),
        }),
        Err(error) => Err(Error::Io(error)),
    }
}

fn writer_lease_owner_text() -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("pid={}\nnonce={nonce}\n", std::process::id())
}

fn write_native_file_writer_lease_owner(file: &mut File, owner: &str) -> Result<()> {
    file.write_all(owner.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn read_current_manifest_from_native_file(object: &StorageObjectId) -> Result<Option<Arc<[u8]>>> {
    if object.kind() != StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "current manifest read requires a manifest storage object",
        ));
    }

    match fs::read(object.path()) {
        Ok(bytes) => Ok(Some(Arc::from(bytes))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn list_native_file_objects(request: &StorageObjectListRequest) -> Result<Vec<StorageObjectId>> {
    let mut objects = Vec::new();
    for entry in fs::read_dir(request.root())? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let path = entry.path();
        if !native_file_matches_list_request(request, &path) {
            continue;
        }

        objects.push(StorageObjectId::native_file(request.kind(), path));
    }
    objects.sort_unstable();
    Ok(objects)
}

fn native_file_matches_list_request(request: &StorageObjectListRequest, path: &Path) -> bool {
    request.file_extension().is_none_or(|expected| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
    })
}

fn write_native_file_object(
    object: &StorageObjectId,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    if object.kind() == StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "manifest storage objects must use manifest publish",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::ObjectWrite)?;
    capabilities.require_durability(durability)?;

    let path = object.path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    fs::rename(tmp_path, path)?;

    Ok(())
}

fn delete_native_file_object(object: &StorageObjectId) -> Result<()> {
    if object.kind() == StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "manifest storage objects must use manifest publish",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::ObjectDelete)?;

    match fs::remove_file(object.path()) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

fn publish_manifest_to_native_file(
    object: &StorageObjectId,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    if object.kind() != StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "manifest publish requires a manifest storage object",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::AtomicManifestPublish)?;
    capabilities.require_durability(durability)?;

    let path = object.path();
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    fs::rename(tmp_path, path)?;
    if durability == DurabilityMode::SyncAll {
        sync_parent_dir_after_rename(path)?;
    }

    Ok(())
}

fn sync_native_file_for_durability(file: &File, durability: DurabilityMode) -> Result<()> {
    match durability {
        DurabilityMode::Buffered => Ok(()),
        DurabilityMode::Flush | DurabilityMode::SyncData => {
            file.sync_data()?;
            Ok(())
        }
        DurabilityMode::SyncAll => {
            file.sync_all()?;
            Ok(())
        }
    }
}

fn poll_ready_storage_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => Err(Error::unsupported_backend(
            "runtime for pending storage future",
        )),
    }
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn native_file_backend_exposes_async_read_shape() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-async-storage-read-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"abcdef").expect("test file writes");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::RandomRead)
            .expect("native-file backend supports random reads");
        let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
        let object = poll_ready_storage_future(backend.open_read(object_id)).expect("object opens");
        assert_eq!(
            StorageReadObject::object(&object).kind(),
            StorageObjectKind::Table
        );
        assert_eq!(
            poll_ready_storage_future(StorageReadObject::len(&object)).expect("length reads"),
            6
        );

        let mut bytes = [0_u8; 3];
        poll_ready_storage_future(StorageReadObject::read_exact_at(&object, 2, &mut bytes))
            .expect("range reads");
        assert_eq!(&bytes, b"cde");

        std::fs::remove_file(path).expect("test file removes");
    }

    #[test]
    fn native_file_backend_publishes_manifest_with_capabilities() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-manifest-publish-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");

        let backend = NativeFileBackend::new();
        let capabilities = backend.capabilities();
        capabilities
            .require(StorageCapability::AtomicManifestPublish)
            .expect("native-file backend supports manifest publish");
        capabilities
            .require_durability(DurabilityMode::SyncAll)
            .expect("native-file backend supports strict publish sync");

        let object =
            StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
        backend
            .publish_manifest_blocking(
                object.clone(),
                Arc::from(&b"first"[..]),
                DurabilityMode::SyncAll,
            )
            .expect("manifest publishes");
        assert_eq!(
            std::fs::read(object.path()).expect("manifest reads"),
            b"first"
        );

        backend
            .publish_manifest_blocking(
                object.clone(),
                Arc::from(&b"second"[..]),
                DurabilityMode::SyncAll,
            )
            .expect("manifest republishes");
        assert_eq!(
            std::fs::read(object.path()).expect("manifest reads"),
            b"second"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_reads_current_manifest() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-manifest-read-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");

        let backend = NativeFileBackend::new();
        let object =
            StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
        assert!(
            backend
                .read_current_manifest_blocking(object.clone())
                .expect("missing manifest read succeeds")
                .is_none()
        );

        backend
            .publish_manifest_blocking(
                object.clone(),
                Arc::from(&b"manifest bytes"[..]),
                DurabilityMode::SyncAll,
            )
            .expect("manifest publishes");
        let bytes = backend
            .read_current_manifest_blocking(object)
            .expect("manifest reads")
            .expect("manifest exists");
        assert_eq!(&*bytes, b"manifest bytes");

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_lists_matching_file_objects() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-list-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        std::fs::write(root.join("table-00000000000000000001.trinet"), b"table")
            .expect("table file writes");
        std::fs::write(root.join("table-00000000000000000002.TRINET"), b"table")
            .expect("uppercase table file writes");
        std::fs::write(root.join("MANIFEST"), b"manifest").expect("manifest file writes");
        std::fs::create_dir(root.join("table-00000000000000000003.trinet"))
            .expect("table-shaped dir creates");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::ObjectListing)
            .expect("native-file backend supports object listing");
        let request = StorageObjectListRequest::native_file(StorageObjectKind::Table, &root)
            .with_file_extension("trinet");
        let objects = backend
            .list_objects_blocking(request)
            .expect("objects list");
        assert!(
            objects
                .iter()
                .all(|object| object.kind() == StorageObjectKind::Table)
        );
        let mut names = objects
            .iter()
            .map(|object| {
                object
                    .path()
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("listed path has utf-8 file name")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![
                "table-00000000000000000001.trinet".to_owned(),
                "table-00000000000000000002.TRINET".to_owned(),
            ]
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_writes_table_object() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-write-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let path = root.join("table-00000000000000000007.trinet");
        let object = StorageObjectId::native_file(StorageObjectKind::Table, &path);

        let backend = NativeFileBackend::new();
        let capabilities = backend.capabilities();
        capabilities
            .require(StorageCapability::ObjectWrite)
            .expect("native-file backend supports object writes");
        capabilities
            .require_durability(DurabilityMode::SyncAll)
            .expect("native-file backend supports strict object sync");
        backend
            .write_object_blocking(
                object.clone(),
                Arc::from(&b"table bytes"[..]),
                DurabilityMode::SyncAll,
            )
            .expect("table object writes");

        assert_eq!(
            std::fs::read(object.path()).expect("table object reads"),
            b"table bytes"
        );
        assert!(
            !path.with_extension("tmp").exists(),
            "successful table write should leave only the final object"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_writes_blob_object() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-write-blob-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let path = root.join("blob-00000000000000000007.trineb");
        let object = StorageObjectId::native_file(StorageObjectKind::Blob, &path);

        let backend = NativeFileBackend::new();
        let capabilities = backend.capabilities();
        capabilities
            .require(StorageCapability::ObjectWrite)
            .expect("native-file backend supports object writes");
        backend
            .write_object_blocking(
                object.clone(),
                Arc::from(&b"blob bytes"[..]),
                DurabilityMode::SyncAll,
            )
            .expect("blob object writes");

        assert_eq!(
            std::fs::read(object.path()).expect("blob object reads"),
            b"blob bytes"
        );
        assert!(
            !path.with_extension("tmp").exists(),
            "successful blob write should leave only the final object"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_object_write_rejects_manifest_objects() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-write-manifest-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let object =
            StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));

        let backend = NativeFileBackend::new();
        let error = backend
            .write_object_blocking(object, Arc::from(&b"manifest"[..]), DurabilityMode::SyncAll)
            .expect_err("manifest objects use manifest publish");
        assert!(matches!(error, Error::InvalidOptions { .. }));
    }

    #[test]
    fn native_file_backend_deletes_table_and_blob_objects() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-delete-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        let table_path = root.join("table-00000000000000000007.trinet");
        let blob_path = root.join("blob-00000000000000000007.trineb");
        std::fs::write(&table_path, b"table").expect("table object writes");
        std::fs::write(&blob_path, b"blob").expect("blob object writes");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::ObjectDelete)
            .expect("native-file backend supports object deletes");
        backend
            .delete_object_blocking(StorageObjectId::native_file(
                StorageObjectKind::Table,
                &table_path,
            ))
            .expect("table object deletes");
        backend
            .delete_object_blocking(StorageObjectId::native_file(
                StorageObjectKind::Blob,
                &blob_path,
            ))
            .expect("blob object deletes");
        backend
            .delete_object_blocking(StorageObjectId::native_file(
                StorageObjectKind::Blob,
                &blob_path,
            ))
            .expect("missing object delete is idempotent");

        assert!(!table_path.exists(), "table object should be deleted");
        assert!(!blob_path.exists(), "blob object should be deleted");

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_object_delete_rejects_manifest_objects() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-delete-manifest-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let object =
            StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));

        let backend = NativeFileBackend::new();
        let error = backend
            .delete_object_blocking(object)
            .expect_err("manifest objects use manifest publish");
        assert!(matches!(error, Error::InvalidOptions { .. }));
    }

    #[test]
    fn native_file_backend_appends_wal_object_with_capabilities() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-append-wal-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let object = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::Append)
            .expect("native-file backend supports append");
        let mut append = backend
            .open_append_blocking(object.clone())
            .expect("WAL append object opens");

        append
            .append_blocking(b"first", DurabilityMode::Buffered)
            .expect("first WAL bytes append");
        append
            .append_blocking(b"second", DurabilityMode::Flush)
            .expect("second WAL bytes append");
        append
            .persist_blocking(DurabilityMode::SyncData)
            .expect("WAL append object persists");

        assert_eq!(
            std::fs::read(object.path()).expect("WAL object reads"),
            b"firstsecond"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_append_rejects_non_wal_objects() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-append-table-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let object =
            StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));

        let error = NativeFileBackend::new()
            .open_append_blocking(object)
            .expect_err("only WAL objects use append");
        assert!(matches!(error, Error::InvalidOptions { .. }));
    }

    #[test]
    fn native_file_backend_acquires_and_releases_writer_lease() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-writer-lease-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let object =
            StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::WriterLease)
            .expect("native-file backend supports writer leases");
        let lease = backend
            .acquire_writer_lease_blocking(object.clone())
            .expect("writer lease acquires");
        assert!(object.path().exists(), "writer lease marker should exist");

        let error = backend
            .acquire_writer_lease_blocking(object.clone())
            .expect_err("existing writer lease fails closed");
        assert!(error.to_string().contains("database lock is already held"));

        drop(lease);
        assert!(
            !object.path().exists(),
            "dropping owned writer lease should remove marker"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_writer_lease_does_not_remove_changed_marker() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-writer-lease-changed-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let object =
            StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
        let lease = NativeFileBackend::new()
            .acquire_writer_lease_blocking(object.clone())
            .expect("writer lease acquires");
        std::fs::write(object.path(), b"pid=other\nnonce=other\n").expect("lease marker changes");

        drop(lease);

        assert_eq!(
            std::fs::read(object.path()).expect("changed lease marker remains"),
            b"pid=other\nnonce=other\n"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn memory_storage_backend_exposes_async_read_shape() {
        let backend = MemoryStorageBackend::new();
        let capabilities = backend.capabilities();
        assert!(capabilities.supports(StorageCapability::Volatile));
        assert!(capabilities.supports(StorageCapability::RandomRead));
        assert!(!capabilities.supports(StorageCapability::Persistent));
        assert!(matches!(
            capabilities.require(StorageCapability::Persistent),
            Err(Error::UnsupportedBackend {
                feature: "persistent storage"
            })
        ));

        let object_id = StorageObjectId::memory(StorageObjectKind::Table, "table-7");
        backend
            .insert_read_object(object_id.clone(), Vec::from(&b"abcdef"[..]))
            .expect("memory object inserts");

        let object =
            poll_ready_storage_future(backend.open_read(object_id)).expect("memory object opens");
        assert_eq!(
            StorageReadObject::object(&object).kind(),
            StorageObjectKind::Table
        );
        assert_eq!(
            poll_ready_storage_future(StorageReadObject::len(&object)).expect("length reads"),
            6
        );

        let mut bytes = [0_u8; 3];
        poll_ready_storage_future(StorageReadObject::read_exact_at(&object, 1, &mut bytes))
            .expect("range reads");
        assert_eq!(&bytes, b"bcd");
    }

    #[test]
    fn storage_capabilities_report_unsupported_backend_and_durability() {
        let read_only = StorageCapabilities::native_file_read();
        assert!(read_only.supports(StorageCapability::Persistent));
        assert!(read_only.supports(StorageCapability::RandomRead));
        assert!(read_only.supports(StorageCapability::ObjectListing));
        assert!(!read_only.supports(StorageCapability::ObjectWrite));
        assert!(!read_only.supports(StorageCapability::ObjectDelete));
        assert!(!read_only.supports(StorageCapability::Append));
        assert!(!read_only.supports(StorageCapability::WriterLease));
        assert!(matches!(
            read_only.require(StorageCapability::Append),
            Err(Error::UnsupportedBackend { feature: "append" })
        ));
        assert!(read_only.supports_durability(DurabilityMode::Buffered));
        assert!(matches!(
            read_only.require_durability(DurabilityMode::SyncAll),
            Err(Error::UnsupportedDurability {
                requested: DurabilityMode::SyncAll
            })
        ));

        let strict = StorageCapabilities::empty()
            .with(StorageCapability::Flush)
            .with(StorageCapability::StrictDataSync)
            .with(StorageCapability::StrictMetadataSync);
        assert!(strict.supports_durability(DurabilityMode::SyncAll));
    }
}
