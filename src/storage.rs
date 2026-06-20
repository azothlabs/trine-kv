use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    block::BlockReadSource,
    durability::{sync_dir_after_renames, sync_parent_dir_after_rename},
    error::{Error, Result},
    io::{
        BlockingAdapterIoDriver, InlineIoDriver, IoAppendObject, IoCompletion, IoDriver,
        IoDriverInfo, IoReadObject,
    },
    limits,
    object_store::{ObjectStoreBackend, ObjectStoreReadObject},
    options::DurabilityMode,
    runtime::Runtime,
    stats::{PlatformIoOperationStats, StorageOperationMetric, StorageOperationStats},
};
use bytes::Bytes;

#[cfg(feature = "platform-io")]
use crate::io::{PlatformIoDriver, PlatformIoOperation, PlatformIoTaskClass};
#[cfg(feature = "platform-io")]
use crate::stats::PlatformIoClassCounters;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum StorageObjectKind {
    Blob,
    Manifest,
    RecoveryReport,
    Table,
    Temporary,
    Wal,
    WriterLease,
}

impl StorageObjectKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Manifest => "manifest",
            Self::RecoveryReport => "recovery report",
            Self::Table => "table",
            Self::Temporary => "temporary",
            Self::Wal => "WAL",
            Self::WriterLease => "writer lease",
        }
    }
}

pub(crate) fn max_whole_object_read_bytes(kind: StorageObjectKind) -> usize {
    match kind {
        StorageObjectKind::Blob => limits::MAX_WHOLE_BLOB_DECODE_BYTES,
        StorageObjectKind::Manifest => limits::MAX_MANIFEST_PAYLOAD_BYTES + 14,
        StorageObjectKind::RecoveryReport => limits::MAX_MANIFEST_PAYLOAD_BYTES,
        StorageObjectKind::Table => 14 + limits::MAX_WHOLE_TABLE_DECODE_BYTES,
        StorageObjectKind::Temporary => limits::MAX_WHOLE_TABLE_DECODE_BYTES,
        StorageObjectKind::Wal => limits::MAX_WAL_FRAME_PAYLOAD_BYTES * 16,
        StorageObjectKind::WriterLease => 64 * 1024,
    }
}

pub(crate) fn ensure_whole_object_read_len(object: &StorageObjectId, len: usize) -> Result<()> {
    let max = max_whole_object_read_bytes(object.kind());
    if len <= max {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "{} object {} length {len} exceeds maximum {max}",
            object.kind().as_str(),
            object.path().display()
        ),
    })
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StorageDirectoryId {
    path: PathBuf,
}

impl StorageDirectoryId {
    pub(crate) fn native_file(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub(crate) fn native_file_parent_of(path: &Path) -> Option<Self> {
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Self::native_file)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StorageDirectoryFile {
    path: PathBuf,
    byte_len: Option<u64>,
}

impl StorageDirectoryFile {
    #[allow(dead_code)]
    pub(crate) fn native_file(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            byte_len: None,
        }
    }

    pub(crate) fn native_file_with_len(path: impl Into<PathBuf>, byte_len: u64) -> Self {
        Self {
            path: path.into(),
            byte_len: Some(byte_len),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) const fn byte_len(&self) -> Option<u64> {
        self.byte_len
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

    pub(crate) const fn file_extension(&self) -> Option<&'static str> {
        self.file_extension
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) type StorageFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + 'op>>;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type StorageFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'op>>;

pub(crate) type StorageReadFuture<'op, T> = StorageFuture<'op, T>;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) trait StorageThreadBound {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> StorageThreadBound for T {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) trait StorageThreadBound: Send {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send + ?Sized> StorageThreadBound for T {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) trait StorageSharedBound {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> StorageSharedBound for T {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) trait StorageSharedBound: Send + Sync {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send + Sync + ?Sized> StorageSharedBound for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageReadBuffer {
    offset: usize,
    bytes: Bytes,
}

impl StorageReadBuffer {
    fn new(offset: usize, bytes: Bytes) -> Self {
        Self { offset, bytes }
    }

    /// Wraps an owned byte vector as a read completion. Used by block decode
    /// fallbacks that read into a heap buffer before handing it to the decoder.
    pub(crate) fn from_vec(offset: usize, bytes: Vec<u8>) -> Self {
        Self::new(offset, Bytes::from(bytes))
    }

    pub(crate) const fn offset(&self) -> usize {
        self.offset
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub(crate) fn into_arc_bytes(self) -> Arc<[u8]> {
        Arc::from(self.bytes.as_ref())
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

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
        Self::native_file_read()
            .with(StorageCapability::ObjectWrite)
            .with(StorageCapability::ObjectDelete)
            .with(StorageCapability::Append)
            .with(StorageCapability::AtomicWalRewrite)
            .with(StorageCapability::DirectoryCreate)
            .with(StorageCapability::DirectorySync)
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

pub(crate) trait StorageReadObject: StorageSharedBound {
    #[allow(dead_code)]
    fn object(&self) -> &StorageObjectId;

    fn len(&self) -> StorageReadFuture<'_, u64>;

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()>;

    fn read_exact_at_owned(
        &self,
        offset: usize,
        len: usize,
    ) -> StorageReadFuture<'_, StorageReadBuffer> {
        Box::pin(async move {
            let mut bytes = allocate_read_buffer(len)?;
            self.read_exact_at(offset, &mut bytes).await?;
            Ok(StorageReadBuffer::from_vec(offset, bytes))
        })
    }
}

pub(crate) trait BlockingStorageReadObject: StorageReadObject {
    fn len_blocking(&self) -> Result<u64>;

    fn read_exact_at_blocking(&self, offset: usize, bytes: &mut [u8]) -> Result<()>;

    fn read_exact_at_owned_blocking(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        poll_ready_storage_future(StorageReadObject::read_exact_at_owned(self, offset, len))
    }
}

pub(crate) trait StorageReadBackend: StorageSharedBound {
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

pub(crate) trait StorageObjectReadBackend: StorageReadBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>>;
}

pub(crate) trait BlockingStorageObjectReadBackend: StorageObjectReadBackend {
    fn read_object_bytes_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>>;
}

pub(crate) trait StorageAppendObject: StorageThreadBound {
    fn append<'op>(
        &'op mut self,
        bytes: &'op [u8],
        durability: DurabilityMode,
    ) -> StorageFuture<'op, ()>;

    fn persist(&mut self, durability: DurabilityMode) -> StorageFuture<'_, ()>;
}

#[allow(dead_code)]
pub(crate) trait BlockingStorageAppendObject: StorageAppendObject {
    fn append_blocking(&mut self, bytes: &[u8], durability: DurabilityMode) -> Result<()> {
        poll_ready_storage_future(StorageAppendObject::append(self, bytes, durability))
    }

    fn persist_blocking(&mut self, durability: DurabilityMode) -> Result<()> {
        poll_ready_storage_future(StorageAppendObject::persist(self, durability))
    }
}

pub(crate) trait StorageAppendBackend: StorageReadBackend {
    type AppendObject: StorageAppendObject;

    fn open_append(&self, object: StorageObjectId) -> StorageFuture<'_, Self::AppendObject>;
}

pub(crate) trait BlockingStorageAppendBackend: StorageAppendBackend
where
    Self::AppendObject: BlockingStorageAppendObject,
{
    fn open_append_blocking(&self, object: StorageObjectId) -> Result<Self::AppendObject> {
        poll_ready_storage_future(self.open_append(object))
    }
}

pub(crate) trait StorageWalRewriteBackend: StorageReadBackend {
    fn rewrite_wal(
        &self,
        object: StorageObjectId,
        temporary_object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()>;
}

#[allow(dead_code)]
pub(crate) trait BlockingStorageWalRewriteBackend: StorageWalRewriteBackend {
    fn rewrite_wal_blocking(
        &self,
        object: StorageObjectId,
        temporary_object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        poll_ready_storage_future(self.rewrite_wal(object, temporary_object, bytes, durability))
    }
}

pub(crate) trait StorageWriterLeaseBackend: StorageReadBackend {
    type WriterLease: StorageThreadBound;

    fn acquire_writer_lease(&self, object: StorageObjectId)
    -> StorageFuture<'_, Self::WriterLease>;
}

pub(crate) trait BlockingStorageWriterLeaseBackend: StorageWriterLeaseBackend {
    fn acquire_writer_lease_blocking(&self, object: StorageObjectId) -> Result<Self::WriterLease> {
        poll_ready_storage_future(self.acquire_writer_lease(object))
    }
}

pub(crate) trait StorageDirectoryCreateBackend: StorageReadBackend {
    fn create_directory_all(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageDirectoryCreateBackend:
    StorageDirectoryCreateBackend
{
    fn create_directory_all_blocking(&self, directory: StorageDirectoryId) -> Result<()> {
        poll_ready_storage_future(self.create_directory_all(directory))
    }
}

pub(crate) trait StorageDirectoryListBackend: StorageReadBackend {
    fn list_directory_files(
        &self,
        directory: StorageDirectoryId,
    ) -> StorageFuture<'_, Vec<StorageDirectoryFile>>;
}

pub(crate) trait BlockingStorageDirectoryListBackend: StorageDirectoryListBackend {
    fn list_directory_files_blocking(
        &self,
        directory: StorageDirectoryId,
    ) -> Result<Vec<StorageDirectoryFile>> {
        poll_ready_storage_future(self.list_directory_files(directory))
    }
}

pub(crate) trait StorageDirectorySyncBackend: StorageReadBackend {
    fn sync_directory_after_renames(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageDirectorySyncBackend: StorageDirectorySyncBackend {
    fn sync_directory_after_renames_blocking(&self, directory: StorageDirectoryId) -> Result<()> {
        poll_ready_storage_future(self.sync_directory_after_renames(directory))
    }
}

pub(crate) trait StorageManifestReadBackend: StorageReadBackend {
    fn read_current_manifest(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Option<Arc<[u8]>>>;
}

pub(crate) trait BlockingStorageManifestReadBackend: StorageManifestReadBackend {
    fn read_current_manifest_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        poll_ready_storage_future(self.read_current_manifest(object))
    }
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
    ) -> Result<()> {
        poll_ready_storage_future(self.publish_manifest(object, bytes, durability))
    }
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
    ) -> Result<()> {
        poll_ready_storage_future(self.write_object(object, bytes, durability))
    }
}

pub(crate) trait StorageObjectDeleteBackend: StorageReadBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageObjectDeleteBackend: StorageObjectDeleteBackend {
    fn delete_object_blocking(&self, object: StorageObjectId) -> Result<()> {
        poll_ready_storage_future(self.delete_object(object))
    }
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
    ) -> Result<Vec<StorageObjectId>> {
        poll_ready_storage_future(self.list_objects(request))
    }
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

#[derive(Debug, Clone)]
pub(crate) struct NativeFileBackend {
    runtime: Option<Runtime>,
    #[cfg(feature = "platform-io")]
    platform_io: Option<PlatformIoDriver>,
    metrics: Arc<NativeFileStorageMetrics>,
}

impl NativeFileBackend {
    pub(crate) fn new() -> Self {
        Self {
            runtime: None,
            #[cfg(feature = "platform-io")]
            platform_io: None,
            metrics: Arc::new(NativeFileStorageMetrics::default()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_runtime(runtime: Runtime) -> Self {
        #[cfg(feature = "platform-io")]
        let platform_io = runtime
            .capabilities()
            .platform_io_driver()
            .then(PlatformIoDriver::new);
        Self {
            runtime: Some(runtime),
            #[cfg(feature = "platform-io")]
            platform_io,
            metrics: Arc::new(NativeFileStorageMetrics::default()),
        }
    }

    pub(crate) fn stats(&self) -> NativeFileStorageStats {
        let uses_platform_io_driver = self.uses_platform_io_driver();
        let uses_platform_async_io = self.supports_platform_async_io();
        let blocking_adapter_stats = self
            .runtime
            .as_ref()
            .and_then(Runtime::blocking_adapter_stats)
            .unwrap_or_default();
        NativeFileStorageStats {
            uses_blocking_adapter: !uses_platform_io_driver
                && self
                    .runtime
                    .as_ref()
                    .is_some_and(|runtime| runtime.capabilities().blocking_adapter()),
            uses_platform_io_driver,
            uses_platform_async_io,
            blocking_adapter_tasks: self.metrics.blocking_adapter_tasks(),
            blocking_adapter_queue_capacity: blocking_adapter_stats.queue_capacity,
            blocking_adapter_queued_tasks: blocking_adapter_stats.queued_tasks,
            blocking_adapter_submitted_tasks: blocking_adapter_stats.submitted_tasks,
            blocking_adapter_completed_tasks: blocking_adapter_stats.completed_tasks,
            blocking_adapter_rejected_tasks: blocking_adapter_stats.rejected_tasks,
            blocking_adapter_total_runtime_micros: blocking_adapter_stats.total_runtime_micros,
            platform_async_io_tasks: self.metrics.platform_async_io_tasks(),
            platform_thread_pool_managed_async_tasks: self
                .metrics
                .platform_thread_pool_managed_async_tasks(),
            platform_blocking_fallback_tasks: self.metrics.platform_blocking_fallback_tasks(),
            inline_tasks: self.metrics.inline_tasks(),
            operations: self.metrics.operation_stats(),
            platform_io_operations: self.metrics.platform_io_operation_stats(),
        }
    }

    fn io_driver_info(&self) -> IoDriverInfo {
        #[cfg(feature = "platform-io")]
        if self.platform_io.is_some() {
            return PlatformIoDriver::info();
        }

        if self
            .runtime
            .as_ref()
            .is_some_and(|runtime| runtime.capabilities().blocking_adapter())
        {
            IoDriverInfo::blocking_adapter()
        } else {
            InlineIoDriver.info()
        }
    }

    fn uses_platform_io_driver(&self) -> bool {
        #[cfg(feature = "platform-io")]
        {
            self.platform_io.is_some()
        }
        #[cfg(not(feature = "platform-io"))]
        {
            let _ = self.runtime.is_some();
            false
        }
    }

    fn supports_platform_async_io(&self) -> bool {
        self.uses_platform_io_driver()
            && self
                .runtime
                .as_ref()
                .is_some_and(|runtime| runtime.capabilities().platform_async_io())
    }

    fn run_owned_storage_task<T>(
        &self,
        operation: StorageOperation,
        task: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> StorageFuture<'_, T>
    where
        T: Send + 'static,
    {
        if let Some(runtime) = self.runtime.clone() {
            if runtime.capabilities().blocking_adapter() {
                self.metrics.record_blocking_adapter_task();
                return record_timed_storage_future(
                    Arc::clone(&self.metrics),
                    operation,
                    Box::pin(async move { runtime.spawn_blocking_result(task)?.await }),
                );
            }
        }

        self.metrics.record_inline_task();
        record_timed_storage_future(
            Arc::clone(&self.metrics),
            operation,
            Box::pin(async move { task() }),
        )
    }
}

impl Default for NativeFileBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
struct NativeFileStorageMetrics {
    blocking_adapter: AtomicU64,
    platform_async_io: AtomicU64,
    platform_thread_pool_managed_async: AtomicU64,
    platform_blocking_fallback: AtomicU64,
    inline: AtomicU64,
    operations: NativeFileStorageOperationMetrics,
    #[cfg(feature = "platform-io")]
    platform_io_operations: NativeFilePlatformIoOperationMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageOperation {
    OpenRead,
    Len,
    ReadExactAt,
    ReadExactAtOwned,
    ReadObjectBytes,
    OpenAppend,
    Append,
    Persist,
    RewriteWal,
    AcquireWriterLease,
    CreateDirectoryAll,
    ListDirectoryFiles,
    SyncDirectoryAfterRenames,
    ReadCurrentManifest,
    PublishManifest,
    WriteObject,
    DeleteObject,
    ListObjects,
}

#[derive(Debug, Default)]
struct NativeFileStorageOperationMetrics {
    open_read: NativeFileStorageOperationMetric,
    len: NativeFileStorageOperationMetric,
    read_exact_at: NativeFileStorageOperationMetric,
    read_exact_at_owned: NativeFileStorageOperationMetric,
    read_object_bytes: NativeFileStorageOperationMetric,
    open_append: NativeFileStorageOperationMetric,
    append: NativeFileStorageOperationMetric,
    persist: NativeFileStorageOperationMetric,
    rewrite_wal: NativeFileStorageOperationMetric,
    acquire_writer_lease: NativeFileStorageOperationMetric,
    create_directory_all: NativeFileStorageOperationMetric,
    list_directory_files: NativeFileStorageOperationMetric,
    sync_directory_after_renames: NativeFileStorageOperationMetric,
    read_current_manifest: NativeFileStorageOperationMetric,
    publish_manifest: NativeFileStorageOperationMetric,
    write_object: NativeFileStorageOperationMetric,
    delete_object: NativeFileStorageOperationMetric,
    list_objects: NativeFileStorageOperationMetric,
}

#[derive(Debug, Default)]
struct NativeFileStorageOperationMetric {
    requests: AtomicU64,
    total_latency_micros: AtomicU64,
}

impl NativeFileStorageMetrics {
    fn record_blocking_adapter_task(&self) {
        self.blocking_adapter.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "platform-io")]
    fn record_platform_io_task(&self, class: PlatformIoTaskClass) {
        match class {
            PlatformIoTaskClass::TruePlatformAsync
            | PlatformIoTaskClass::PlatformNativeAsyncButPartial => {
                self.platform_async_io.fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::ThreadPoolManagedAsync => {
                self.platform_thread_pool_managed_async
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::BlockingFallback => {
                self.platform_blocking_fallback
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::Unsupported => {}
        }
    }

    #[cfg(feature = "platform-io")]
    fn record_platform_io_operation(
        &self,
        operation: PlatformIoOperation,
        class: PlatformIoTaskClass,
    ) {
        self.record_platform_io_task(class);
        self.platform_io_operations
            .metric(operation)
            .record_class(class);
    }

    fn record_inline_task(&self) {
        self.inline.fetch_add(1, Ordering::Relaxed);
    }

    fn record_operation(&self, operation: StorageOperation, latency: Duration) {
        self.operations
            .metric(operation)
            .record(duration_to_micros_saturating(latency));
    }

    fn blocking_adapter_tasks(&self) -> u64 {
        self.blocking_adapter.load(Ordering::Acquire)
    }

    fn platform_async_io_tasks(&self) -> u64 {
        self.platform_async_io.load(Ordering::Acquire)
    }

    fn platform_thread_pool_managed_async_tasks(&self) -> u64 {
        self.platform_thread_pool_managed_async
            .load(Ordering::Acquire)
    }

    fn platform_blocking_fallback_tasks(&self) -> u64 {
        self.platform_blocking_fallback.load(Ordering::Acquire)
    }

    fn inline_tasks(&self) -> u64 {
        self.inline.load(Ordering::Acquire)
    }

    fn operation_stats(&self) -> StorageOperationStats {
        self.operations.snapshot()
    }

    fn platform_io_operation_stats(&self) -> PlatformIoOperationStats {
        #[cfg(feature = "platform-io")]
        {
            self.platform_io_operations.snapshot()
        }
        #[cfg(not(feature = "platform-io"))]
        {
            let _ = self.inline_tasks();
            PlatformIoOperationStats::default()
        }
    }
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Default)]
struct NativeFilePlatformIoOperationMetrics {
    length_lookup: NativeFilePlatformIoClassMetrics,
    random_read: NativeFilePlatformIoClassMetrics,
    whole_object_read: NativeFilePlatformIoClassMetrics,
    temp_write_rename_publish: NativeFilePlatformIoClassMetrics,
    append_open: NativeFilePlatformIoClassMetrics,
    append: NativeFilePlatformIoClassMetrics,
    persist: NativeFilePlatformIoClassMetrics,
    wal_rewrite: NativeFilePlatformIoClassMetrics,
    delete: NativeFilePlatformIoClassMetrics,
    directory_create: NativeFilePlatformIoClassMetrics,
    directory_sync: NativeFilePlatformIoClassMetrics,
    directory_listing: NativeFilePlatformIoClassMetrics,
    writer_lease: NativeFilePlatformIoClassMetrics,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Default)]
struct NativeFilePlatformIoClassMetrics {
    true_platform_async: AtomicU64,
    platform_native_async_but_partial: AtomicU64,
    thread_pool_managed_async: AtomicU64,
    blocking_fallback: AtomicU64,
    unsupported: AtomicU64,
}

#[cfg(feature = "platform-io")]
impl NativeFilePlatformIoOperationMetrics {
    fn metric(&self, operation: PlatformIoOperation) -> &NativeFilePlatformIoClassMetrics {
        match operation {
            PlatformIoOperation::LengthLookup => &self.length_lookup,
            PlatformIoOperation::OwnedRandomRead => &self.random_read,
            PlatformIoOperation::OptionalWholeObjectRead => &self.whole_object_read,
            PlatformIoOperation::TempWriteRenamePublish => &self.temp_write_rename_publish,
            PlatformIoOperation::AppendObjectOpen => &self.append_open,
            PlatformIoOperation::Append => &self.append,
            PlatformIoOperation::Persist => &self.persist,
            PlatformIoOperation::WalRewrite => &self.wal_rewrite,
            PlatformIoOperation::ObjectDelete => &self.delete,
            PlatformIoOperation::DirectoryCreate => &self.directory_create,
            PlatformIoOperation::DirectorySync => &self.directory_sync,
            PlatformIoOperation::DirectoryListing => &self.directory_listing,
            PlatformIoOperation::WriterLeaseAcquire => &self.writer_lease,
        }
    }

    fn snapshot(&self) -> PlatformIoOperationStats {
        PlatformIoOperationStats {
            length_lookup: self.length_lookup.snapshot(),
            random_read: self.random_read.snapshot(),
            whole_object_read: self.whole_object_read.snapshot(),
            temp_write_rename_publish: self.temp_write_rename_publish.snapshot(),
            append_open: self.append_open.snapshot(),
            append: self.append.snapshot(),
            persist: self.persist.snapshot(),
            wal_rewrite: self.wal_rewrite.snapshot(),
            delete: self.delete.snapshot(),
            directory_create: self.directory_create.snapshot(),
            directory_sync: self.directory_sync.snapshot(),
            directory_listing: self.directory_listing.snapshot(),
            writer_lease: self.writer_lease.snapshot(),
        }
    }
}

#[cfg(feature = "platform-io")]
impl NativeFilePlatformIoClassMetrics {
    fn record_class(&self, class: PlatformIoTaskClass) {
        match class {
            PlatformIoTaskClass::TruePlatformAsync => {
                self.true_platform_async.fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::PlatformNativeAsyncButPartial => {
                self.platform_native_async_but_partial
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::ThreadPoolManagedAsync => {
                self.thread_pool_managed_async
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::BlockingFallback => {
                self.blocking_fallback.fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::Unsupported => {
                self.unsupported.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> PlatformIoClassCounters {
        PlatformIoClassCounters {
            true_platform_async: self.true_platform_async.load(Ordering::Acquire),
            platform_native_async_but_partial: self
                .platform_native_async_but_partial
                .load(Ordering::Acquire),
            thread_pool_managed_async: self.thread_pool_managed_async.load(Ordering::Acquire),
            blocking_fallback: self.blocking_fallback.load(Ordering::Acquire),
            unsupported: self.unsupported.load(Ordering::Acquire),
        }
    }
}

impl NativeFileStorageOperationMetrics {
    fn metric(&self, operation: StorageOperation) -> &NativeFileStorageOperationMetric {
        match operation {
            StorageOperation::OpenRead => &self.open_read,
            StorageOperation::Len => &self.len,
            StorageOperation::ReadExactAt => &self.read_exact_at,
            StorageOperation::ReadExactAtOwned => &self.read_exact_at_owned,
            StorageOperation::ReadObjectBytes => &self.read_object_bytes,
            StorageOperation::OpenAppend => &self.open_append,
            StorageOperation::Append => &self.append,
            StorageOperation::Persist => &self.persist,
            StorageOperation::RewriteWal => &self.rewrite_wal,
            StorageOperation::AcquireWriterLease => &self.acquire_writer_lease,
            StorageOperation::CreateDirectoryAll => &self.create_directory_all,
            StorageOperation::ListDirectoryFiles => &self.list_directory_files,
            StorageOperation::SyncDirectoryAfterRenames => &self.sync_directory_after_renames,
            StorageOperation::ReadCurrentManifest => &self.read_current_manifest,
            StorageOperation::PublishManifest => &self.publish_manifest,
            StorageOperation::WriteObject => &self.write_object,
            StorageOperation::DeleteObject => &self.delete_object,
            StorageOperation::ListObjects => &self.list_objects,
        }
    }

    fn snapshot(&self) -> StorageOperationStats {
        StorageOperationStats {
            open_read: self.open_read.snapshot(),
            len: self.len.snapshot(),
            read_exact_at: self.read_exact_at.snapshot(),
            read_exact_at_owned: self.read_exact_at_owned.snapshot(),
            read_object_bytes: self.read_object_bytes.snapshot(),
            open_append: self.open_append.snapshot(),
            append: self.append.snapshot(),
            persist: self.persist.snapshot(),
            rewrite_wal: self.rewrite_wal.snapshot(),
            acquire_writer_lease: self.acquire_writer_lease.snapshot(),
            create_directory_all: self.create_directory_all.snapshot(),
            list_directory_files: self.list_directory_files.snapshot(),
            sync_directory_after_renames: self.sync_directory_after_renames.snapshot(),
            read_current_manifest: self.read_current_manifest.snapshot(),
            publish_manifest: self.publish_manifest.snapshot(),
            write_object: self.write_object.snapshot(),
            delete_object: self.delete_object.snapshot(),
            list_objects: self.list_objects.snapshot(),
        }
    }
}

impl NativeFileStorageOperationMetric {
    fn record(&self, latency_micros: u64) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_micros
            .fetch_add(latency_micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StorageOperationMetric {
        StorageOperationMetric {
            requests: self.requests.load(Ordering::Acquire),
            total_latency_micros: self.total_latency_micros.load(Ordering::Acquire),
        }
    }
}

#[cfg(feature = "platform-io")]
fn record_platform_io_task(
    metrics: &NativeFileStorageMetrics,
    _driver: &PlatformIoDriver,
    operation: PlatformIoOperation,
) {
    metrics.record_platform_io_operation(operation, PlatformIoDriver::task_class(operation));
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct NativeFileStorageStats {
    pub(crate) uses_blocking_adapter: bool,
    pub(crate) uses_platform_io_driver: bool,
    pub(crate) uses_platform_async_io: bool,
    pub(crate) blocking_adapter_tasks: u64,
    pub(crate) blocking_adapter_queue_capacity: usize,
    pub(crate) blocking_adapter_queued_tasks: usize,
    pub(crate) blocking_adapter_submitted_tasks: u64,
    pub(crate) blocking_adapter_completed_tasks: u64,
    pub(crate) blocking_adapter_rejected_tasks: u64,
    pub(crate) blocking_adapter_total_runtime_micros: u64,
    pub(crate) platform_async_io_tasks: u64,
    pub(crate) platform_thread_pool_managed_async_tasks: u64,
    pub(crate) platform_blocking_fallback_tasks: u64,
    pub(crate) inline_tasks: u64,
    pub(crate) operations: StorageOperationStats,
    pub(crate) platform_io_operations: PlatformIoOperationStats,
}

fn record_timed_storage_result<T>(
    metrics: &NativeFileStorageMetrics,
    operation: StorageOperation,
    task: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    let result = task();
    metrics.record_operation(operation, started.elapsed());
    result
}

fn record_timed_storage_future<'op, T>(
    metrics: Arc<NativeFileStorageMetrics>,
    operation: StorageOperation,
    future: StorageFuture<'op, T>,
) -> StorageFuture<'op, T>
where
    T: 'op,
{
    Box::pin(async move {
        let started = Instant::now();
        let result = future.await;
        metrics.record_operation(operation, started.elapsed());
        result
    })
}

fn duration_to_micros_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl StorageReadBackend for NativeFileBackend {
    type ReadObject = NativeFileObject;

    fn capabilities(&self) -> StorageCapabilities {
        let capabilities = StorageCapabilities::native_file();
        if self.uses_platform_io_driver() {
            let capabilities = capabilities
                .with(StorageCapability::AsyncTasks)
                .with(StorageCapability::BlockingAdapter)
                .with(StorageCapability::BackgroundThreads);
            if self.supports_platform_async_io() {
                capabilities.with(StorageCapability::PlatformAsyncIo)
            } else {
                capabilities
            }
        } else if self.io_driver_info().kind().is_blocking_adapter() {
            capabilities
                .with(StorageCapability::AsyncTasks)
                .with(StorageCapability::BlockingAdapter)
                .with(StorageCapability::BackgroundThreads)
        } else {
            capabilities
        }
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        let runtime = self.runtime.clone();
        #[cfg(feature = "platform-io")]
        let platform_io = self.platform_io.clone();
        let metrics = Arc::clone(&self.metrics);
        let object_metrics = Arc::clone(&metrics);
        record_timed_storage_future(
            metrics,
            StorageOperation::OpenRead,
            Box::pin(async move {
                NativeFileObject::open(
                    object,
                    runtime,
                    #[cfg(feature = "platform-io")]
                    platform_io,
                    object_metrics,
                )
            }),
        )
    }
}

impl BlockingStorageReadBackend for NativeFileBackend {
    fn open_read_blocking(&self, object: StorageObjectId) -> Result<Self::ReadObject> {
        let metrics = Arc::clone(&self.metrics);
        record_timed_storage_result(self.metrics.as_ref(), StorageOperation::OpenRead, || {
            NativeFileObject::open(
                object,
                self.runtime.clone(),
                #[cfg(feature = "platform-io")]
                self.platform_io.clone(),
                metrics,
            )
        })
    }
}

impl StorageObjectReadBackend for NativeFileBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::ReadObjectBytes,
                Box::pin(async move {
                    require_native_file_object_read()?;
                    let max_bytes = max_whole_object_read_bytes(object.kind());
                    let completion =
                        driver.submit_read_optional_path(object.path().to_path_buf(), max_bytes)?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::OptionalWholeObjectRead,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::ReadObjectBytes, move || {
            read_native_file_object_bytes(&object)
        })
    }
}

impl BlockingStorageObjectReadBackend for NativeFileBackend {
    fn read_object_bytes_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::ReadObjectBytes,
            || read_native_file_object_bytes(&object),
        )
    }
}

impl StorageAppendBackend for NativeFileBackend {
    type AppendObject = NativeFileAppendObject;

    fn open_append(&self, object: StorageObjectId) -> StorageFuture<'_, Self::AppendObject> {
        let runtime = self.runtime.clone();
        #[cfg(feature = "platform-io")]
        let platform_io = self.platform_io.clone();
        let metrics = Arc::clone(&self.metrics);
        #[cfg(feature = "platform-io")]
        if let Some(driver) = platform_io.clone() {
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::OpenAppend,
                Box::pin(async move {
                    require_native_file_append(&object)?;
                    let completion = driver.submit_open_append_path(object.path().to_path_buf())?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::AppendObjectOpen,
                    );
                    completion.await?;
                    Ok(NativeFileAppendObject::open_platform(
                        object,
                        runtime,
                        platform_io,
                        task_metrics,
                    ))
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::OpenAppend, move || {
            NativeFileAppendObject::open(
                &object,
                runtime,
                #[cfg(feature = "platform-io")]
                platform_io,
                metrics,
            )
        })
    }
}

impl BlockingStorageAppendBackend for NativeFileBackend {
    fn open_append_blocking(&self, object: StorageObjectId) -> Result<Self::AppendObject> {
        let metrics = Arc::clone(&self.metrics);
        record_timed_storage_result(self.metrics.as_ref(), StorageOperation::OpenAppend, || {
            NativeFileAppendObject::open(
                &object,
                self.runtime.clone(),
                #[cfg(feature = "platform-io")]
                self.platform_io.clone(),
                metrics,
            )
        })
    }
}

impl StorageWalRewriteBackend for NativeFileBackend {
    fn rewrite_wal(
        &self,
        object: StorageObjectId,
        temporary_object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::RewriteWal,
                Box::pin(async move {
                    let (path, tmp_path) =
                        prepare_native_file_wal_rewrite(&object, &temporary_object, durability)?;
                    let completion = driver.submit_write_temp_rename_path(
                        path, tmp_path, bytes, durability, true, true,
                    )?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::WalRewrite,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::RewriteWal, move || {
            rewrite_native_file_wal(&object, &temporary_object, &bytes, durability)
        })
    }
}

impl BlockingStorageWalRewriteBackend for NativeFileBackend {
    fn rewrite_wal_blocking(
        &self,
        object: StorageObjectId,
        temporary_object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        record_timed_storage_result(self.metrics.as_ref(), StorageOperation::RewriteWal, || {
            rewrite_native_file_wal(&object, &temporary_object, &bytes, durability)
        })
    }
}

impl StorageWriterLeaseBackend for NativeFileBackend {
    type WriterLease = NativeFileWriterLease;

    fn acquire_writer_lease(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Self::WriterLease> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::AcquireWriterLease,
                Box::pin(async move {
                    require_native_file_writer_lease(&object)?;
                    let owner = writer_lease_owner_text();
                    let completion = driver.submit_acquire_writer_lease_path(
                        object.path().to_path_buf(),
                        Arc::from(owner.as_bytes()),
                    )?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::WriterLeaseAcquire,
                    );
                    let file = completion.await?;
                    Ok(NativeFileWriterLease::from_locked_file(object, owner, file))
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::AcquireWriterLease, move || {
            NativeFileWriterLease::acquire(object)
        })
    }
}

impl BlockingStorageWriterLeaseBackend for NativeFileBackend {
    fn acquire_writer_lease_blocking(&self, object: StorageObjectId) -> Result<Self::WriterLease> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            return record_timed_storage_result(
                self.metrics.as_ref(),
                StorageOperation::AcquireWriterLease,
                || {
                    require_native_file_writer_lease(&object)?;
                    let owner = writer_lease_owner_text();
                    let completion = driver.submit_acquire_writer_lease_path(
                        object.path().to_path_buf(),
                        Arc::from(owner.as_bytes()),
                    )?;
                    record_platform_io_task(
                        self.metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::WriterLeaseAcquire,
                    );
                    let file = wait_for_platform_io(completion)?;
                    Ok(NativeFileWriterLease::from_locked_file(object, owner, file))
                },
            );
        }

        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::AcquireWriterLease,
            || NativeFileWriterLease::acquire(object),
        )
    }
}

impl StorageDirectoryCreateBackend for NativeFileBackend {
    fn create_directory_all(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::CreateDirectoryAll,
                Box::pin(async move {
                    require_native_file_directory_create()?;
                    let completion =
                        driver.submit_create_dir_all_path(directory.path().to_path_buf())?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::DirectoryCreate,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::CreateDirectoryAll, move || {
            create_native_file_directory_all(&directory)
        })
    }
}

impl BlockingStorageDirectoryCreateBackend for NativeFileBackend {
    fn create_directory_all_blocking(&self, directory: StorageDirectoryId) -> Result<()> {
        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::CreateDirectoryAll,
            || create_native_file_directory_all(&directory),
        )
    }
}

impl StorageDirectoryListBackend for NativeFileBackend {
    fn list_directory_files(
        &self,
        directory: StorageDirectoryId,
    ) -> StorageFuture<'_, Vec<StorageDirectoryFile>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::ListDirectoryFiles,
                Box::pin(async move {
                    require_native_file_directory_listing()?;
                    let completion =
                        driver.submit_list_file_paths_path(directory.path().to_path_buf())?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::DirectoryListing,
                    );
                    let paths = completion.await?;
                    Ok(paths
                        .into_iter()
                        .map(StorageDirectoryFile::native_file)
                        .collect())
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::ListDirectoryFiles, move || {
            list_native_file_directory_files(&directory)
        })
    }
}

impl BlockingStorageDirectoryListBackend for NativeFileBackend {
    fn list_directory_files_blocking(
        &self,
        directory: StorageDirectoryId,
    ) -> Result<Vec<StorageDirectoryFile>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            return record_timed_storage_result(
                self.metrics.as_ref(),
                StorageOperation::ListDirectoryFiles,
                || {
                    require_native_file_directory_listing()?;
                    let completion =
                        driver.submit_list_file_paths_path(directory.path().to_path_buf())?;
                    record_platform_io_task(
                        self.metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::DirectoryListing,
                    );
                    let paths = wait_for_platform_io(completion)?;
                    Ok(paths
                        .into_iter()
                        .map(StorageDirectoryFile::native_file)
                        .collect())
                },
            );
        }

        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::ListDirectoryFiles,
            || list_native_file_directory_files(&directory),
        )
    }
}

impl StorageDirectorySyncBackend for NativeFileBackend {
    fn sync_directory_after_renames(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::SyncDirectoryAfterRenames,
                Box::pin(async move {
                    require_native_file_directory_sync()?;
                    let completion = driver.submit_sync_dir_path(directory.path().to_path_buf())?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::DirectorySync,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::SyncDirectoryAfterRenames, move || {
            sync_native_file_directory_after_renames(&directory)
        })
    }
}

impl BlockingStorageDirectorySyncBackend for NativeFileBackend {
    fn sync_directory_after_renames_blocking(&self, directory: StorageDirectoryId) -> Result<()> {
        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::SyncDirectoryAfterRenames,
            || sync_native_file_directory_after_renames(&directory),
        )
    }
}

impl StorageManifestReadBackend for NativeFileBackend {
    fn read_current_manifest(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::ReadCurrentManifest,
                Box::pin(async move {
                    require_native_file_manifest_read(&object)?;
                    let max_bytes = max_whole_object_read_bytes(object.kind());
                    let completion =
                        driver.submit_read_optional_path(object.path().to_path_buf(), max_bytes)?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::OptionalWholeObjectRead,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::ReadCurrentManifest, move || {
            read_current_manifest_from_native_file(&object)
        })
    }
}

impl BlockingStorageManifestReadBackend for NativeFileBackend {
    fn read_current_manifest_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::ReadCurrentManifest,
            || read_current_manifest_from_native_file(&object),
        )
    }
}

impl StorageManifestPublishBackend for NativeFileBackend {
    fn publish_manifest(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::PublishManifest,
                Box::pin(async move {
                    let (path, tmp_path) =
                        prepare_native_file_manifest_publish(&object, durability)?;
                    let completion = driver.submit_write_temp_rename_path(
                        path, tmp_path, bytes, durability, false, true,
                    )?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::TempWriteRenamePublish,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::PublishManifest, move || {
            publish_manifest_to_native_file(&object, &bytes, durability)
        })
    }
}

impl BlockingStorageManifestPublishBackend for NativeFileBackend {
    fn publish_manifest_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::PublishManifest,
            || publish_manifest_to_native_file(&object, &bytes, durability),
        )
    }
}

impl StorageObjectWriteBackend for NativeFileBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::WriteObject,
                Box::pin(async move {
                    let (path, tmp_path) = prepare_native_file_object_write(&object, durability)?;
                    let completion = driver.submit_write_temp_rename_path(
                        path, tmp_path, bytes, durability, true, false,
                    )?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::TempWriteRenamePublish,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::WriteObject, move || {
            write_native_file_object(&object, &bytes, durability)
        })
    }
}

impl BlockingStorageObjectWriteBackend for NativeFileBackend {
    fn write_object_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        record_timed_storage_result(self.metrics.as_ref(), StorageOperation::WriteObject, || {
            write_native_file_object(&object, &bytes, durability)
        })
    }
}

impl StorageObjectDeleteBackend for NativeFileBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::DeleteObject,
                Box::pin(async move {
                    require_native_file_object_delete(&object)?;
                    let completion = driver.submit_delete_path(object.path().to_path_buf())?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::ObjectDelete,
                    );
                    completion.await
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::DeleteObject, move || {
            delete_native_file_object(&object)
        })
    }
}

impl BlockingStorageObjectDeleteBackend for NativeFileBackend {
    fn delete_object_blocking(&self, object: StorageObjectId) -> Result<()> {
        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::DeleteObject,
            || delete_native_file_object(&object),
        )
    }
}

impl StorageObjectListBackend for NativeFileBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            let metrics = Arc::clone(&self.metrics);
            let task_metrics = Arc::clone(&metrics);
            return record_timed_storage_future(
                metrics,
                StorageOperation::ListObjects,
                Box::pin(async move {
                    require_native_file_object_listing()?;
                    let completion =
                        driver.submit_list_file_paths_path(request.root().to_path_buf())?;
                    record_platform_io_task(
                        task_metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::DirectoryListing,
                    );
                    let paths = completion.await?;
                    Ok(native_file_objects_from_paths(&request, paths))
                }),
            );
        }

        self.run_owned_storage_task(StorageOperation::ListObjects, move || {
            list_native_file_objects(&request)
        })
    }
}

impl BlockingStorageObjectListBackend for NativeFileBackend {
    fn list_objects_blocking(
        &self,
        request: StorageObjectListRequest,
    ) -> Result<Vec<StorageObjectId>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = self.platform_io.clone() {
            return record_timed_storage_result(
                self.metrics.as_ref(),
                StorageOperation::ListObjects,
                || {
                    require_native_file_object_listing()?;
                    let completion =
                        driver.submit_list_file_paths_path(request.root().to_path_buf())?;
                    record_platform_io_task(
                        self.metrics.as_ref(),
                        &driver,
                        PlatformIoOperation::DirectoryListing,
                    );
                    let paths = wait_for_platform_io(completion)?;
                    Ok(native_file_objects_from_paths(&request, paths))
                },
            );
        }

        record_timed_storage_result(self.metrics.as_ref(), StorageOperation::ListObjects, || {
            list_native_file_objects(&request)
        })
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(dead_code)]
mod browser_persistent_storage {
    use std::{
        cell::RefCell,
        io,
        path::{Component, Path},
        rc::Rc,
        sync::Arc,
    };

    use futures::{StreamExt, channel::oneshot};
    use js_sys::{Function, Promise, Reflect};
    use opfs::{
        CreateWritableOptions, DirectoryEntry, DirectoryHandle as _, FileHandle as _,
        FileSystemRemoveOptions, GetDirectoryHandleOptions, GetFileHandleOptions,
        WritableFileStream as _,
        persistent::{self, DirectoryHandle, FileHandle},
    };
    use wasm_bindgen::{JsCast, JsValue};

    use super::{
        DurabilityMode, Error, Result, StorageAppendBackend, StorageAppendObject,
        StorageCapabilities, StorageCapability, StorageDirectoryCreateBackend,
        StorageDirectoryFile, StorageDirectoryId, StorageDirectoryListBackend, StorageFuture,
        StorageManifestPublishBackend, StorageManifestReadBackend, StorageObjectDeleteBackend,
        StorageObjectId, StorageObjectKind, StorageObjectListBackend, StorageObjectListRequest,
        StorageObjectReadBackend, StorageObjectWriteBackend, StorageReadBackend, StorageReadFuture,
        StorageReadObject, StorageWalRewriteBackend, StorageWriterLeaseBackend,
        native_file_objects_from_paths, usize_to_u64,
    };

    #[derive(Debug, Clone)]
    pub(crate) struct BrowserStorageBackend {
        root: DirectoryHandle,
    }

    impl BrowserStorageBackend {
        pub(crate) async fn new() -> Result<Self> {
            let root = persistent::app_specific_dir()
                .await
                .map_err(|error| map_opfs_error(&error))?;
            Ok(Self { root })
        }

        pub(crate) fn from_root(root: DirectoryHandle) -> Self {
            Self { root }
        }

        fn capabilities_for_browser() -> StorageCapabilities {
            let capabilities = StorageCapabilities::empty()
                .with(StorageCapability::Persistent)
                .with(StorageCapability::RandomRead)
                .with(StorageCapability::ObjectRead)
                .with(StorageCapability::ObjectListing)
                .with(StorageCapability::ObjectWrite)
                .with(StorageCapability::ObjectDelete)
                .with(StorageCapability::Append)
                .with(StorageCapability::AtomicWalRewrite)
                .with(StorageCapability::DirectoryCreate)
                .with(StorageCapability::DirectoryListing)
                .with(StorageCapability::AtomicManifestPublish)
                .with(StorageCapability::Flush)
                .with(StorageCapability::AsyncTasks)
                .with(StorageCapability::CooperativeTasks);
            if browser_web_locks_available() {
                capabilities.with(StorageCapability::WriterLease)
            } else {
                capabilities
            }
        }

        async fn directory_from_segments(
            &self,
            segments: &[String],
            create: bool,
        ) -> Result<Option<DirectoryHandle>> {
            let mut directory = self.root.clone();
            let options = GetDirectoryHandleOptions { create };
            for segment in segments {
                directory = match directory
                    .get_directory_handle_with_options(segment, &options)
                    .await
                {
                    Ok(directory) => directory,
                    Err(error) if !create && is_opfs_not_found(&error) => return Ok(None),
                    Err(error) => return Err(map_opfs_error(&error)),
                };
            }
            Ok(Some(directory))
        }

        async fn directory_handle(
            &self,
            path: &Path,
            create: bool,
        ) -> Result<Option<DirectoryHandle>> {
            let segments = opfs_path_segments(path)?;
            self.directory_from_segments(&segments, create).await
        }

        async fn parent_directory_and_name(
            &self,
            path: &Path,
            create: bool,
        ) -> Result<Option<(DirectoryHandle, String)>> {
            let mut segments = opfs_path_segments(path)?;
            let name = segments.pop().ok_or_else(|| {
                Error::invalid_options("browser persistent object path must include a file name")
            })?;
            let Some(directory) = self.directory_from_segments(&segments, create).await? else {
                return Ok(None);
            };
            Ok(Some((directory, name)))
        }

        async fn file_handle(&self, path: &Path, create: bool) -> Result<Option<FileHandle>> {
            let Some((directory, name)) = self.parent_directory_and_name(path, create).await?
            else {
                return Ok(None);
            };
            let options = GetFileHandleOptions { create };
            match directory
                .get_file_handle_with_options(&name, &options)
                .await
            {
                Ok(file) => Ok(Some(file)),
                Err(error) if !create && is_opfs_not_found(&error) => Ok(None),
                Err(error) => Err(map_opfs_error(&error)),
            }
        }

        async fn read_object_bytes_inner(
            &self,
            object: &StorageObjectId,
        ) -> Result<Option<Arc<[u8]>>> {
            Self::capabilities_for_browser().require(StorageCapability::ObjectRead)?;
            let Some(file) = self.file_handle(object.path(), false).await? else {
                return Ok(None);
            };
            let bytes = file.read().await.map_err(|error| map_opfs_error(&error))?;
            Ok(Some(Arc::from(bytes)))
        }

        async fn write_object_bytes(&self, object: &StorageObjectId, bytes: &[u8]) -> Result<()> {
            let Some((directory, name)) =
                self.parent_directory_and_name(object.path(), true).await?
            else {
                return Err(Error::invalid_options(
                    "browser persistent object path parent cannot be opened",
                ));
            };
            let options = GetFileHandleOptions { create: true };
            let mut file = directory
                .get_file_handle_with_options(&name, &options)
                .await
                .map_err(|error| map_opfs_error(&error))?;
            let write_options = CreateWritableOptions {
                keep_existing_data: false,
            };
            let mut stream = file
                .create_writable_with_options(&write_options)
                .await
                .map_err(|error| map_opfs_error(&error))?;
            stream
                .write_at_cursor_pos(bytes)
                .await
                .map_err(|error| map_opfs_error(&error))?;
            stream
                .close()
                .await
                .map_err(|error| map_opfs_error(&error))?;
            Ok(())
        }
    }

    impl StorageReadBackend for BrowserStorageBackend {
        type ReadObject = BrowserStorageObject;

        fn capabilities(&self) -> StorageCapabilities {
            Self::capabilities_for_browser()
        }

        fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
            Box::pin(async move {
                let Some(file) = self.file_handle(object.path(), false).await? else {
                    return Err(Error::Corruption {
                        message: format!(
                            "referenced browser persistent {} {} cannot be opened",
                            object.kind().as_str(),
                            object.path().display()
                        ),
                    });
                };
                Ok(BrowserStorageObject { object, file })
            })
        }
    }

    impl StorageObjectReadBackend for BrowserStorageBackend {
        fn read_object_bytes(
            &self,
            object: StorageObjectId,
        ) -> StorageFuture<'_, Option<Arc<[u8]>>> {
            Box::pin(async move { self.read_object_bytes_inner(&object).await })
        }
    }

    impl StorageDirectoryCreateBackend for BrowserStorageBackend {
        fn create_directory_all(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()> {
            Box::pin(async move {
                Self::capabilities_for_browser().require(StorageCapability::DirectoryCreate)?;
                self.directory_handle(directory.path(), true).await?;
                Ok(())
            })
        }
    }

    impl StorageDirectoryListBackend for BrowserStorageBackend {
        fn list_directory_files(
            &self,
            directory_id: StorageDirectoryId,
        ) -> StorageFuture<'_, Vec<StorageDirectoryFile>> {
            Box::pin(async move {
                Self::capabilities_for_browser().require(StorageCapability::DirectoryListing)?;
                let Some(directory) = self.directory_handle(directory_id.path(), false).await?
                else {
                    return Ok(Vec::new());
                };
                let mut files = Vec::new();
                let mut entries = directory
                    .entries()
                    .await
                    .map_err(|error| map_opfs_error(&error))?;
                while let Some(entry) = entries.next().await {
                    let (name, entry) = entry.map_err(|error| map_opfs_error(&error))?;
                    if matches!(entry, DirectoryEntry::File(_)) {
                        files.push(StorageDirectoryFile::native_file(
                            directory_id.path().join(name),
                        ));
                    }
                }
                files.sort_unstable();
                Ok(files)
            })
        }
    }

    impl StorageManifestReadBackend for BrowserStorageBackend {
        fn read_current_manifest(
            &self,
            object: StorageObjectId,
        ) -> StorageFuture<'_, Option<Arc<[u8]>>> {
            Box::pin(async move {
                require_browser_manifest_object(&object)?;
                self.read_object_bytes_inner(&object).await
            })
        }
    }

    impl StorageManifestPublishBackend for BrowserStorageBackend {
        fn publish_manifest(
            &self,
            object: StorageObjectId,
            bytes: Arc<[u8]>,
            durability: DurabilityMode,
        ) -> StorageFuture<'_, ()> {
            Box::pin(async move {
                require_browser_manifest_object(&object)?;
                Self::capabilities_for_browser()
                    .require(StorageCapability::AtomicManifestPublish)?;
                require_browser_durability(durability)?;
                self.write_object_bytes(&object, &bytes).await
            })
        }
    }

    impl StorageObjectWriteBackend for BrowserStorageBackend {
        fn write_object(
            &self,
            object: StorageObjectId,
            bytes: Arc<[u8]>,
            durability: DurabilityMode,
        ) -> StorageFuture<'_, ()> {
            Box::pin(async move {
                require_browser_object_write(&object)?;
                Self::capabilities_for_browser().require(StorageCapability::ObjectWrite)?;
                require_browser_durability(durability)?;
                self.write_object_bytes(&object, &bytes).await
            })
        }
    }

    impl StorageObjectDeleteBackend for BrowserStorageBackend {
        fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
            Box::pin(async move {
                require_browser_object_delete(&object)?;
                let Some((mut directory, name)) =
                    self.parent_directory_and_name(object.path(), false).await?
                else {
                    return Ok(());
                };
                let options = FileSystemRemoveOptions { recursive: false };
                match directory.remove_entry_with_options(&name, &options).await {
                    Ok(()) => Ok(()),
                    Err(error) if is_opfs_not_found(&error) => Ok(()),
                    Err(error) => Err(map_opfs_error(&error)),
                }
            })
        }
    }

    impl StorageObjectListBackend for BrowserStorageBackend {
        fn list_objects(
            &self,
            request: StorageObjectListRequest,
        ) -> StorageFuture<'_, Vec<StorageObjectId>> {
            Box::pin(async move {
                Self::capabilities_for_browser().require(StorageCapability::ObjectListing)?;
                let Some(directory) = self.directory_handle(request.root(), false).await? else {
                    return Ok(Vec::new());
                };
                let mut paths = Vec::new();
                let mut entries = directory
                    .entries()
                    .await
                    .map_err(|error| map_opfs_error(&error))?;
                while let Some(entry) = entries.next().await {
                    let (name, entry) = entry.map_err(|error| map_opfs_error(&error))?;
                    if matches!(entry, DirectoryEntry::File(_)) {
                        paths.push(request.root().join(name));
                    }
                }
                Ok(native_file_objects_from_paths(&request, paths))
            })
        }
    }

    impl StorageAppendBackend for BrowserStorageBackend {
        type AppendObject = BrowserAppendObject;

        fn open_append(&self, object: StorageObjectId) -> StorageFuture<'_, Self::AppendObject> {
            Box::pin(async move {
                require_browser_wal_object(&object)?;
                Self::capabilities_for_browser().require(StorageCapability::Append)?;
                Ok(BrowserAppendObject {
                    backend: self.clone(),
                    object,
                })
            })
        }
    }

    impl StorageWalRewriteBackend for BrowserStorageBackend {
        fn rewrite_wal(
            &self,
            object: StorageObjectId,
            temporary_object: StorageObjectId,
            bytes: Arc<[u8]>,
            durability: DurabilityMode,
        ) -> StorageFuture<'_, ()> {
            Box::pin(async move {
                prepare_browser_wal_rewrite(&object, &temporary_object, durability)?;
                self.write_object_bytes(&temporary_object, &bytes).await?;
                self.write_object_bytes(&object, &bytes).await?;
                self.delete_object(temporary_object).await
            })
        }
    }

    impl StorageWriterLeaseBackend for BrowserStorageBackend {
        type WriterLease = BrowserWriterLease;

        fn acquire_writer_lease(
            &self,
            object: StorageObjectId,
        ) -> StorageFuture<'_, Self::WriterLease> {
            Box::pin(async move { acquire_browser_writer_lease(object).await })
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct BrowserStorageObject {
        object: StorageObjectId,
        file: FileHandle,
    }

    impl StorageReadObject for BrowserStorageObject {
        fn object(&self) -> &StorageObjectId {
            &self.object
        }

        fn len(&self) -> StorageReadFuture<'_, u64> {
            Box::pin(async move {
                let len = self
                    .file
                    .size()
                    .await
                    .map_err(|error| map_opfs_error(&error))?;
                usize_to_u64(len, "browser persistent object length")
            })
        }

        fn read_exact_at<'op>(
            &'op self,
            offset: usize,
            bytes: &'op mut [u8],
        ) -> StorageReadFuture<'op, ()> {
            Box::pin(async move {
                let end = offset.checked_add(bytes.len()).ok_or_else(|| {
                    Error::invalid_options("browser persistent object read offset overflow")
                })?;
                let read = self
                    .file
                    .read_range(offset..end)
                    .await
                    .map_err(|error| map_opfs_error(&error))?;
                if read.len() != bytes.len() {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "referenced browser persistent {} {} short read",
                            self.object.kind().as_str(),
                            self.object.path().display()
                        ),
                    )));
                }
                bytes.copy_from_slice(&read);
                Ok(())
            })
        }
    }

    pub(crate) struct BrowserAppendObject {
        backend: BrowserStorageBackend,
        object: StorageObjectId,
    }

    impl StorageAppendObject for BrowserAppendObject {
        fn append<'op>(
            &'op mut self,
            bytes: &'op [u8],
            durability: DurabilityMode,
        ) -> StorageFuture<'op, ()> {
            Box::pin(async move {
                require_browser_wal_object(&self.object)?;
                require_browser_durability(durability)?;
                let mut existing = self
                    .backend
                    .read_object_bytes_inner(&self.object)
                    .await?
                    .map_or_else(Vec::new, |bytes| bytes.as_ref().to_vec());
                existing.extend_from_slice(bytes);
                self.backend
                    .write_object_bytes(&self.object, &existing)
                    .await
            })
        }

        fn persist(&mut self, durability: DurabilityMode) -> StorageFuture<'_, ()> {
            Box::pin(async move { require_browser_durability(durability) })
        }
    }

    pub(crate) struct BrowserWriterLease {
        release: Rc<RefCell<Option<Function>>>,
        _request: Promise,
        _callback: wasm_bindgen::closure::Closure<dyn FnMut(JsValue) -> Promise>,
    }

    impl std::fmt::Debug for BrowserWriterLease {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("BrowserWriterLease").finish_non_exhaustive()
        }
    }

    impl Drop for BrowserWriterLease {
        fn drop(&mut self) {
            if let Some(resolve) = self.release.borrow_mut().take() {
                let _ = resolve.call1(&JsValue::UNDEFINED, &JsValue::UNDEFINED);
            }
        }
    }

    fn require_browser_manifest_object(object: &StorageObjectId) -> Result<()> {
        if object.kind() != StorageObjectKind::Manifest {
            return Err(Error::invalid_options(
                "manifest operation requires a manifest storage object",
            ));
        }
        Ok(())
    }

    fn require_browser_object_write(object: &StorageObjectId) -> Result<()> {
        match object.kind() {
            StorageObjectKind::Manifest => Err(Error::invalid_options(
                "manifest storage objects must use manifest publish",
            )),
            StorageObjectKind::Temporary => Err(Error::invalid_options(
                "temporary storage objects must use their owning publish operation",
            )),
            StorageObjectKind::Blob
            | StorageObjectKind::RecoveryReport
            | StorageObjectKind::Table
            | StorageObjectKind::Wal
            | StorageObjectKind::WriterLease => Ok(()),
        }
    }

    fn require_browser_wal_object(object: &StorageObjectId) -> Result<()> {
        if object.kind() != StorageObjectKind::Wal {
            return Err(Error::invalid_options(
                "WAL operation requires a WAL storage object",
            ));
        }
        Ok(())
    }

    fn prepare_browser_wal_rewrite(
        object: &StorageObjectId,
        temporary_object: &StorageObjectId,
        durability: DurabilityMode,
    ) -> Result<()> {
        require_browser_wal_object(object)?;
        require_browser_wal_object(temporary_object)?;
        BrowserStorageBackend::capabilities_for_browser()
            .require(StorageCapability::AtomicWalRewrite)?;
        require_browser_durability(durability)?;
        if object.path() == temporary_object.path() {
            return Err(Error::invalid_options(
                "WAL rewrite temporary object must differ from final object",
            ));
        }
        if object.path().parent() != temporary_object.path().parent() {
            return Err(Error::invalid_options(
                "WAL rewrite temporary object must share the final object's parent directory",
            ));
        }
        Ok(())
    }

    fn require_browser_writer_lease_object(object: &StorageObjectId) -> Result<()> {
        if object.kind() != StorageObjectKind::WriterLease {
            return Err(Error::invalid_options(
                "writer lease requires a writer lease storage object",
            ));
        }
        BrowserStorageBackend::capabilities_for_browser().require(StorageCapability::WriterLease)
    }

    fn require_browser_durability(durability: DurabilityMode) -> Result<()> {
        match durability {
            DurabilityMode::Buffered | DurabilityMode::Flush => Ok(()),
            DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
                Err(Error::unsupported_durability(durability))
            }
        }
    }

    async fn acquire_browser_writer_lease(object: StorageObjectId) -> Result<BrowserWriterLease> {
        require_browser_writer_lease_object(&object)?;
        let locks = browser_lock_manager()?;
        let request = Reflect::get(&locks, &JsValue::from_str("request"))
            .map_err(|error| map_js_value_error(&error, "read browser lock request function"))?
            .dyn_into::<Function>()
            .map_err(|_| {
                Error::unsupported_backend("browser persistent writer lease request function")
            })?;
        let options = js_sys::Object::new();
        Reflect::set(&options, &JsValue::from_str("ifAvailable"), &JsValue::TRUE).map_err(
            |error| map_js_value_error(&error, "configure browser writer lease options"),
        )?;

        let release = Rc::new(RefCell::new(None));
        let release_for_callback = Rc::clone(&release);
        let (sender, receiver) = oneshot::channel();
        let sender = Rc::new(RefCell::new(Some(sender)));
        let sender_for_callback = Rc::clone(&sender);
        let callback = wasm_bindgen::closure::Closure::<dyn FnMut(JsValue) -> Promise>::new(
            move |lock: JsValue| {
                if lock.is_null() || lock.is_undefined() {
                    if let Some(sender) = sender_for_callback.borrow_mut().take() {
                        let _ = sender.send(false);
                    }
                    return Promise::resolve(&JsValue::UNDEFINED);
                }

                let release_for_promise = Rc::clone(&release_for_callback);
                let pending = Promise::new(&mut |resolve, _reject| {
                    *release_for_promise.borrow_mut() = Some(resolve);
                });
                if let Some(sender) = sender_for_callback.borrow_mut().take() {
                    let _ = sender.send(true);
                }
                pending
            },
        );

        let request_promise = request
            .call3(
                &locks,
                &JsValue::from_str(&browser_writer_lease_name(&object)),
                &options,
                callback.as_ref(),
            )
            .map_err(|error| map_js_value_error(&error, "request browser writer lease"))?
            .dyn_into::<Promise>()
            .map_err(|_| Error::unsupported_backend("browser persistent writer lease promise"))?;
        let acquired = receiver
            .await
            .map_err(|_| Error::unsupported_backend("browser persistent writer lease callback"))?;
        if !acquired {
            return Err(Error::runtime_busy(
                "browser persistent writer lease is already held",
            ));
        }

        Ok(BrowserWriterLease {
            release,
            _request: request_promise,
            _callback: callback,
        })
    }

    fn browser_lock_manager() -> Result<JsValue> {
        let navigator = Reflect::get(&js_sys::global(), &JsValue::from_str("navigator"))
            .map_err(|error| map_js_value_error(&error, "read browser navigator"))?;
        if navigator.is_null() || navigator.is_undefined() {
            return Err(Error::unsupported_backend("browser navigator"));
        }
        let locks = Reflect::get(&navigator, &JsValue::from_str("locks"))
            .map_err(|error| map_js_value_error(&error, "read browser lock manager"))?;
        if locks.is_null() || locks.is_undefined() {
            return Err(Error::unsupported_backend(
                "browser persistent writer lease",
            ));
        }
        Ok(locks)
    }

    fn browser_web_locks_available() -> bool {
        browser_lock_manager().is_ok()
    }

    fn browser_writer_lease_name(object: &StorageObjectId) -> String {
        format!("trine-kv:{}", object.path().display())
    }

    fn require_browser_object_delete(object: &StorageObjectId) -> Result<()> {
        if object.kind() == StorageObjectKind::Manifest {
            return Err(Error::invalid_options(
                "manifest storage objects must use manifest publish",
            ));
        }
        Ok(())
    }

    fn opfs_path_segments(path: &Path) -> Result<Vec<String>> {
        let mut segments = Vec::new();
        for component in path.components() {
            match component {
                Component::Normal(segment) => {
                    let segment = segment.to_str().ok_or_else(|| {
                        Error::invalid_options("browser persistent path must be valid UTF-8")
                    })?;
                    if segment.is_empty() {
                        return Err(Error::invalid_options(
                            "browser persistent path segment must be non-empty",
                        ));
                    }
                    segments.push(segment.to_owned());
                }
                Component::CurDir | Component::RootDir => {}
                Component::ParentDir | Component::Prefix(_) => {
                    return Err(Error::invalid_options(
                        "browser persistent path cannot contain parent or prefix components",
                    ));
                }
            }
        }
        Ok(segments)
    }

    fn map_opfs_error(error: &persistent::Error) -> Error {
        let message = opfs_error_property(error, "message")
            .or_else(|| opfs_error_property(error, "name"))
            .unwrap_or_else(|| format!("{error:?}"));
        Error::Io(io::Error::other(format!(
            "browser persistent storage operation failed: {message}"
        )))
    }

    fn is_opfs_not_found(error: &persistent::Error) -> bool {
        opfs_error_property(error, "name").is_some_and(|name| name == "NotFoundError")
            || format!("{error:?}").contains("NotFoundError")
    }

    fn opfs_error_property(error: &persistent::Error, property: &str) -> Option<String> {
        js_sys::Reflect::get(error, &JsValue::from_str(property))
            .ok()
            .and_then(|value| value.as_string())
    }

    fn map_js_value_error(error: &JsValue, action: &'static str) -> Error {
        let message = js_value_property(error, "message")
            .or_else(|| js_value_property(error, "name"))
            .or_else(|| error.as_string())
            .unwrap_or_else(|| format!("{error:?}"));
        Error::Io(io::Error::other(format!(
            "browser persistent storage failed to {action}: {message}"
        )))
    }

    fn js_value_property(value: &JsValue, property: &str) -> Option<String> {
        Reflect::get(value, &JsValue::from_str(property))
            .ok()
            .and_then(|value| value.as_string())
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(unused_imports)]
pub(crate) use browser_persistent_storage::{BrowserStorageBackend, BrowserWriterLease};

#[derive(Debug)]
pub(crate) struct NativeFileObject {
    object: StorageObjectId,
    file: Arc<Mutex<File>>,
    runtime: Option<Runtime>,
    #[cfg(feature = "platform-io")]
    platform_io: Option<PlatformIoDriver>,
    metrics: Arc<NativeFileStorageMetrics>,
}

impl NativeFileObject {
    fn open(
        object: StorageObjectId,
        runtime: Option<Runtime>,
        #[cfg(feature = "platform-io")] platform_io: Option<PlatformIoDriver>,
        metrics: Arc<NativeFileStorageMetrics>,
    ) -> Result<Self> {
        let file = open_native_file(&object)?;
        Ok(Self {
            object,
            file: Arc::new(Mutex::new(file)),
            runtime,
            #[cfg(feature = "platform-io")]
            platform_io,
            metrics,
        })
    }

    fn read_exact_at_offset(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        read_exact_at_native_file_handle(self.file.as_ref(), &self.object, offset, bytes)
    }

    fn read_exact_at_offset_owned(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        read_exact_at_native_file_handle_owned(self.file.as_ref(), &self.object, offset, len)
    }
}

impl IoReadObject for NativeFileObject {
    fn len_io(&self) -> Result<IoCompletion<u64>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            return driver.submit_len_path(self.object.path().to_path_buf());
        }

        let object = self.object.clone();
        let file = Arc::clone(&self.file);
        InlineIoDriver.submit_len(move || len_native_file_handle(file.as_ref(), &object))
    }

    fn read_exact_at_owned_io(
        &self,
        offset: usize,
        len: usize,
    ) -> Result<IoCompletion<StorageReadBuffer>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            return driver.submit_read_exact_at_owned_path(
                self.object.path().to_path_buf(),
                offset,
                len,
            );
        }

        let object = self.object.clone();
        let file = Arc::clone(&self.file);
        InlineIoDriver.submit_read_exact_at_owned(move || {
            read_exact_at_native_file_handle_owned(file.as_ref(), &object, offset, len)
        })
    }
}

impl StorageReadObject for NativeFileObject {
    fn object(&self) -> &StorageObjectId {
        &self.object
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            record_platform_io_task(
                self.metrics.as_ref(),
                driver,
                PlatformIoOperation::LengthLookup,
            );
        }

        record_timed_storage_future(
            Arc::clone(&self.metrics),
            StorageOperation::Len,
            Box::pin(async move { self.len_io()?.await }),
        )
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        record_timed_storage_future(
            Arc::clone(&self.metrics),
            StorageOperation::ReadExactAt,
            Box::pin(async move { self.read_exact_at_offset(offset, bytes) }),
        )
    }

    fn read_exact_at_owned(
        &self,
        offset: usize,
        len: usize,
    ) -> StorageReadFuture<'_, StorageReadBuffer> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            record_platform_io_task(
                self.metrics.as_ref(),
                driver,
                PlatformIoOperation::OwnedRandomRead,
            );
            return record_timed_storage_future(
                Arc::clone(&self.metrics),
                StorageOperation::ReadExactAtOwned,
                Box::pin(async move { self.read_exact_at_owned_io(offset, len)?.await }),
            );
        }

        if let Some(runtime) = self.runtime.clone() {
            if runtime.capabilities().blocking_adapter() {
                let object = self.object.clone();
                self.metrics.record_blocking_adapter_task();
                return record_timed_storage_future(
                    Arc::clone(&self.metrics),
                    StorageOperation::ReadExactAtOwned,
                    Box::pin(async move {
                        BlockingAdapterIoDriver::new(runtime)
                            .submit_read_exact_at_owned(move || {
                                read_exact_at_native_file_owned(&object, offset, len)
                            })?
                            .await
                    }),
                );
            }
        }
        self.metrics.record_inline_task();
        record_timed_storage_future(
            Arc::clone(&self.metrics),
            StorageOperation::ReadExactAtOwned,
            Box::pin(async move { self.read_exact_at_owned_io(offset, len)?.await }),
        )
    }
}

impl BlockingStorageReadObject for NativeFileObject {
    fn len_blocking(&self) -> Result<u64> {
        record_timed_storage_result(self.metrics.as_ref(), StorageOperation::Len, || {
            len_native_file_handle(self.file.as_ref(), &self.object)
        })
    }

    fn read_exact_at_blocking(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        poll_ready_storage_future(StorageReadObject::read_exact_at(self, offset, bytes))
    }

    fn read_exact_at_owned_blocking(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        record_timed_storage_result(
            self.metrics.as_ref(),
            StorageOperation::ReadExactAtOwned,
            || self.read_exact_at_offset_owned(offset, len),
        )
    }
}

#[derive(Debug)]
pub(crate) struct NativeFileAppendObject {
    object: StorageObjectId,
    file: Option<Arc<Mutex<File>>>,
    runtime: Option<Runtime>,
    #[cfg(feature = "platform-io")]
    platform_io: Option<PlatformIoDriver>,
    metrics: Arc<NativeFileStorageMetrics>,
}

impl NativeFileAppendObject {
    fn open(
        object: &StorageObjectId,
        runtime: Option<Runtime>,
        #[cfg(feature = "platform-io")] platform_io: Option<PlatformIoDriver>,
        metrics: Arc<NativeFileStorageMetrics>,
    ) -> Result<Self> {
        let file = open_native_append_file(object)?;
        Ok(Self {
            object: object.clone(),
            file: Some(Arc::new(Mutex::new(file))),
            runtime,
            #[cfg(feature = "platform-io")]
            platform_io,
            metrics,
        })
    }

    #[cfg(feature = "platform-io")]
    fn open_platform(
        object: StorageObjectId,
        runtime: Option<Runtime>,
        platform_io: Option<PlatformIoDriver>,
        metrics: Arc<NativeFileStorageMetrics>,
    ) -> Self {
        Self {
            object,
            file: None,
            runtime,
            platform_io,
            metrics,
        }
    }

    #[allow(dead_code)]
    fn append_to_file(&mut self, bytes: &[u8], durability: DurabilityMode) -> Result<()> {
        let mut file = self.lock_or_open_file()?;
        append_native_file_object(&mut file, bytes, durability)
    }

    #[allow(dead_code)]
    fn persist_file(&mut self, durability: DurabilityMode) -> Result<()> {
        let mut file = self.lock_or_open_file()?;
        persist_native_append_file(&mut file, durability)
    }

    fn file_handle(&self) -> Result<Arc<Mutex<File>>> {
        self.file
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| Error::runtime_busy("platform append object has no local file handle"))
    }

    #[allow(dead_code)]
    fn lock_or_open_file(&mut self) -> Result<MutexGuard<'_, File>> {
        if self.file.is_none() {
            self.file = Some(Arc::new(Mutex::new(open_native_append_file(&self.object)?)));
        }
        let file = self
            .file
            .as_ref()
            .expect("append file handle is initialized");
        lock_native_append_file(file.as_ref(), &self.object)
    }
}

impl IoAppendObject for NativeFileAppendObject {
    fn append_io(&self, bytes: Arc<[u8]>, durability: DurabilityMode) -> Result<IoCompletion<()>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            return driver.submit_append_path(self.object.path().to_path_buf(), bytes, durability);
        }

        let object = self.object.clone();
        let file = self.file_handle()?;
        InlineIoDriver.submit_append(move || {
            let mut file = lock_native_append_file(file.as_ref(), &object)?;
            append_native_file_object(&mut file, bytes.as_ref(), durability)
        })
    }

    fn persist_io(&self, durability: DurabilityMode) -> Result<IoCompletion<()>> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            return driver.submit_persist_path(self.object.path().to_path_buf(), durability);
        }

        let object = self.object.clone();
        let file = self.file_handle()?;
        InlineIoDriver.submit_sync(move || {
            let mut file = lock_native_append_file(file.as_ref(), &object)?;
            persist_native_append_file(&mut file, durability)
        })
    }
}

impl StorageAppendObject for NativeFileAppendObject {
    fn append<'op>(
        &'op mut self,
        bytes: &'op [u8],
        durability: DurabilityMode,
    ) -> StorageFuture<'op, ()> {
        let bytes: Arc<[u8]> = Arc::from(bytes);
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            record_platform_io_task(self.metrics.as_ref(), driver, PlatformIoOperation::Append);
            return record_timed_storage_future(
                Arc::clone(&self.metrics),
                StorageOperation::Append,
                Box::pin(async move { self.append_io(bytes, durability)?.await }),
            );
        }

        if let Some(runtime) = self.runtime.clone() {
            if runtime.capabilities().blocking_adapter() {
                let object = self.object.clone();
                let file = match self.file_handle() {
                    Ok(file) => file,
                    Err(error) => return Box::pin(async move { Err(error) }),
                };
                self.metrics.record_blocking_adapter_task();
                return record_timed_storage_future(
                    Arc::clone(&self.metrics),
                    StorageOperation::Append,
                    Box::pin(async move {
                        BlockingAdapterIoDriver::new(runtime)
                            .submit_append(move || {
                                let mut file = lock_native_append_file(file.as_ref(), &object)?;
                                append_native_file_object(&mut file, bytes.as_ref(), durability)
                            })?
                            .await
                    }),
                );
            }
        }

        self.metrics.record_inline_task();
        record_timed_storage_future(
            Arc::clone(&self.metrics),
            StorageOperation::Append,
            Box::pin(async move { self.append_io(bytes, durability)?.await }),
        )
    }

    fn persist(&mut self, durability: DurabilityMode) -> StorageFuture<'_, ()> {
        #[cfg(feature = "platform-io")]
        if let Some(driver) = &self.platform_io {
            record_platform_io_task(self.metrics.as_ref(), driver, PlatformIoOperation::Persist);
            return record_timed_storage_future(
                Arc::clone(&self.metrics),
                StorageOperation::Persist,
                Box::pin(async move { self.persist_io(durability)?.await }),
            );
        }

        if let Some(runtime) = self.runtime.clone() {
            if runtime.capabilities().blocking_adapter() {
                let object = self.object.clone();
                let file = match self.file_handle() {
                    Ok(file) => file,
                    Err(error) => return Box::pin(async move { Err(error) }),
                };
                self.metrics.record_blocking_adapter_task();
                return record_timed_storage_future(
                    Arc::clone(&self.metrics),
                    StorageOperation::Persist,
                    Box::pin(async move {
                        BlockingAdapterIoDriver::new(runtime)
                            .submit_sync(move || {
                                let mut file = lock_native_append_file(file.as_ref(), &object)?;
                                persist_native_append_file(&mut file, durability)
                            })?
                            .await
                    }),
                );
            }
        }

        self.metrics.record_inline_task();
        record_timed_storage_future(
            Arc::clone(&self.metrics),
            StorageOperation::Persist,
            Box::pin(async move { self.persist_io(durability)?.await }),
        )
    }
}

impl BlockingStorageAppendObject for NativeFileAppendObject {
    fn append_blocking(&mut self, bytes: &[u8], durability: DurabilityMode) -> Result<()> {
        let started = Instant::now();
        let result = self.append_to_file(bytes, durability);
        self.metrics
            .record_operation(StorageOperation::Append, started.elapsed());
        result
    }

    fn persist_blocking(&mut self, durability: DurabilityMode) -> Result<()> {
        let started = Instant::now();
        let result = self.persist_file(durability);
        self.metrics
            .record_operation(StorageOperation::Persist, started.elapsed());
        result
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
        let owner = writer_lease_owner_text();
        let mut file = acquire_native_file_writer_lease(&object)?;
        write_native_file_writer_lease_owner(&mut file, &owner)?;

        Ok(Self {
            object,
            owner,
            file: Some(file),
        })
    }

    #[cfg(feature = "platform-io")]
    fn from_locked_file(object: StorageObjectId, owner: String, file: File) -> Self {
        Self {
            object,
            owner,
            file: Some(file),
        }
    }
}

impl Drop for NativeFileWriterLease {
    fn drop(&mut self) {
        let should_clear = fs::read_to_string(self.object.path())
            .is_ok_and(|contents| contents.as_str() == self.owner.as_str());
        if should_clear {
            if let Some(file) = self.file.as_mut() {
                let _ = clear_native_file_writer_lease_owner(file);
            }
        }
        if let Some(file) = self.file.as_ref() {
            let _ = fs4::fs_std::FileExt::unlock(file);
        }
        let _ = self.file.take();
    }
}

#[cfg(feature = "platform-io")]
fn wait_for_platform_io<T>(completion: IoCompletion<T>) -> Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut completion = std::pin::pin!(completion);
    loop {
        match completion.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(1)),
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

    fn read_exact_at_owned(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        self.object.read_exact_at_owned_blocking(offset, len)
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

    fn read_exact_at_owned(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        if let Some(cached) = self.cached {
            return cached.read_exact_at_owned_blocking(offset, len);
        }

        read_exact_at_native_file_owned(&self.object, offset, len)
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

fn read_exact_at_native_file_owned(
    object: &StorageObjectId,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    let mut bytes = allocate_read_buffer(len)?;
    read_exact_from_native_file(object, offset, &mut bytes)?;
    Ok(StorageReadBuffer::from_vec(offset, bytes))
}

fn lock_native_read_file<'file>(
    file: &'file Mutex<File>,
    object: &StorageObjectId,
) -> Result<MutexGuard<'file, File>> {
    file.lock().map_err(|_| Error::Corruption {
        message: format!(
            "referenced {} {} handle lock poisoned",
            object.kind().as_str(),
            object.path().display()
        ),
    })
}

fn len_native_file_handle(file: &Mutex<File>, object: &StorageObjectId) -> Result<u64> {
    let file = lock_native_read_file(file, object)?;
    Ok(file.metadata()?.len())
}

fn read_exact_at_native_file_handle(
    file: &Mutex<File>,
    object: &StorageObjectId,
    offset: usize,
    bytes: &mut [u8],
) -> Result<()> {
    let mut file = lock_native_read_file(file, object)?;
    read_exact_at_native_file(&mut file, offset, bytes)
}

fn read_exact_at_native_file_handle_owned(
    file: &Mutex<File>,
    object: &StorageObjectId,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    let mut bytes = allocate_read_buffer(len)?;
    read_exact_at_native_file_handle(file, object, offset, &mut bytes)?;
    Ok(StorageReadBuffer::from_vec(offset, bytes))
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

fn require_native_file_object_read() -> Result<()> {
    let capabilities = StorageCapabilities::native_file_read();
    capabilities.require(StorageCapability::ObjectRead)
}

fn require_native_file_object_listing() -> Result<()> {
    let capabilities = StorageCapabilities::native_file_read();
    capabilities.require(StorageCapability::ObjectListing)
}

fn require_native_file_directory_listing() -> Result<()> {
    let capabilities = StorageCapabilities::native_file_read();
    capabilities.require(StorageCapability::DirectoryListing)
}

fn require_native_file_append(object: &StorageObjectId) -> Result<()> {
    if object.kind() != StorageObjectKind::Wal {
        return Err(Error::invalid_options(
            "append storage objects must use WAL object kind",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::Append)
}

fn require_native_file_manifest_read(object: &StorageObjectId) -> Result<()> {
    if object.kind() != StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "current manifest read requires a manifest storage object",
        ));
    }
    Ok(())
}

fn prepare_native_file_manifest_publish(
    object: &StorageObjectId,
    durability: DurabilityMode,
) -> Result<(PathBuf, PathBuf)> {
    if object.kind() != StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "manifest publish requires a manifest storage object",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::AtomicManifestPublish)?;
    capabilities.require_durability(durability)?;

    let path = object.path().to_path_buf();
    let tmp_path = path.with_extension("tmp");
    Ok((path, tmp_path))
}

fn prepare_native_file_object_write(
    object: &StorageObjectId,
    durability: DurabilityMode,
) -> Result<(PathBuf, PathBuf)> {
    match object.kind() {
        StorageObjectKind::Manifest => {
            return Err(Error::invalid_options(
                "manifest storage objects must use manifest publish",
            ));
        }
        StorageObjectKind::Temporary => {
            return Err(Error::invalid_options(
                "temporary storage objects must use their owning publish operation",
            ));
        }
        StorageObjectKind::Blob
        | StorageObjectKind::RecoveryReport
        | StorageObjectKind::Table
        | StorageObjectKind::Wal
        | StorageObjectKind::WriterLease => {}
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::ObjectWrite)?;
    capabilities.require_durability(durability)?;

    let path = object.path().to_path_buf();
    let tmp_path = path.with_extension("tmp");
    Ok((path, tmp_path))
}

fn require_native_file_object_delete(object: &StorageObjectId) -> Result<()> {
    if object.kind() == StorageObjectKind::Manifest {
        return Err(Error::invalid_options(
            "manifest storage objects must use manifest publish",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::ObjectDelete)
}

fn prepare_native_file_wal_rewrite(
    object: &StorageObjectId,
    temporary_object: &StorageObjectId,
    durability: DurabilityMode,
) -> Result<(PathBuf, PathBuf)> {
    if object.kind() != StorageObjectKind::Wal || temporary_object.kind() != StorageObjectKind::Wal
    {
        return Err(Error::invalid_options(
            "WAL rewrite requires WAL storage objects",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::AtomicWalRewrite)?;
    capabilities.require_durability(durability)?;

    let path = object.path().to_path_buf();
    let tmp_path = temporary_object.path().to_path_buf();
    if path == tmp_path {
        return Err(Error::invalid_options(
            "WAL rewrite temporary object must differ from final object",
        ));
    }
    if path.parent() != tmp_path.parent() {
        return Err(Error::invalid_options(
            "WAL rewrite temporary object must share the final object's parent directory",
        ));
    }

    Ok((path, tmp_path))
}

fn require_native_file_writer_lease(object: &StorageObjectId) -> Result<()> {
    if object.kind() != StorageObjectKind::WriterLease {
        return Err(Error::invalid_options(
            "writer lease requires a writer lease storage object",
        ));
    }

    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::WriterLease)
}

fn require_native_file_directory_create() -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::DirectoryCreate)
}

fn require_native_file_directory_sync() -> Result<()> {
    let capabilities = StorageCapabilities::native_file();
    capabilities.require(StorageCapability::DirectorySync)?;
    capabilities.require(StorageCapability::StrictMetadataSync)
}

fn read_native_file_object_bytes(object: &StorageObjectId) -> Result<Option<Arc<[u8]>>> {
    require_native_file_object_read()?;

    let file = match File::open(object.path()) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::Io(error)),
    };
    let object = NativeFileObject {
        object: object.clone(),
        file: Arc::new(Mutex::new(file)),
        runtime: None,
        #[cfg(feature = "platform-io")]
        platform_io: None,
        metrics: Arc::new(NativeFileStorageMetrics::default()),
    };
    let len = u64_to_usize(object.len_blocking()?, "storage object length")?;
    ensure_whole_object_read_len(&object.object, len)?;
    let buffer = object.read_exact_at_owned_blocking(0, len)?;
    debug_assert_eq!(buffer.offset(), 0);
    debug_assert_eq!(buffer.len(), len);
    debug_assert_eq!(buffer.is_empty(), len == 0);
    Ok(Some(buffer.into_arc_bytes()))
}

fn open_native_append_file(object: &StorageObjectId) -> Result<File> {
    require_native_file_append(object)?;

    if let Some(parent) = object.path().parent() {
        fs::create_dir_all(parent)?;
    }

    OpenOptions::new()
        .create(true)
        .append(true)
        .open(object.path())
        .map_err(Error::from)
}

fn lock_native_append_file<'file>(
    file: &'file Mutex<File>,
    object: &StorageObjectId,
) -> Result<MutexGuard<'file, File>> {
    file.lock().map_err(|_| Error::Corruption {
        message: format!(
            "referenced {} {} append handle lock poisoned",
            object.kind().as_str(),
            object.path().display()
        ),
    })
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
        DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
            crate::durability::sync_file_for_durability(file, durability)
        }
    }
}

fn rewrite_native_file_wal(
    object: &StorageObjectId,
    temporary_object: &StorageObjectId,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    let (path, tmp_path) = prepare_native_file_wal_rewrite(object, temporary_object, durability)?;
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    fs::rename(&tmp_path, &path)?;
    if durability == DurabilityMode::SyncAll {
        sync_native_file_parent_directory_after_rename(&path)?;
    }

    Ok(())
}

fn acquire_native_file_writer_lease(object: &StorageObjectId) -> Result<File> {
    require_native_file_writer_lease(object)?;

    if let Some(parent) = object.path().parent() {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(object.path())
        .map_err(Error::Io)?;
    if fs4::fs_std::FileExt::try_lock_exclusive(&file).map_err(Error::Io)? {
        Ok(file)
    } else {
        Err(Error::Corruption {
            message: format!("database lock is already held: {}", object.path().display()),
        })
    }
}

fn writer_lease_owner_text() -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("pid={}\nnonce={nonce}\n", std::process::id())
}

fn write_native_file_writer_lease_owner(file: &mut File, owner: &str) -> Result<()> {
    // The exclusive lease is the OS file lock held by this open handle. The
    // owner text is only a release-time guard and diagnostic aid, so it does not
    // need a storage sync.
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(owner.as_bytes())?;
    file.flush()?;
    Ok(())
}

fn clear_native_file_writer_lease_owner(file: &mut File) -> Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.flush()?;
    Ok(())
}

fn create_native_file_directory_all(directory: &StorageDirectoryId) -> Result<()> {
    require_native_file_directory_create()?;

    fs::create_dir_all(directory.path()).map_err(Error::from)
}

fn list_native_file_directory_files(
    directory: &StorageDirectoryId,
) -> Result<Vec<StorageDirectoryFile>> {
    require_native_file_directory_listing()?;

    let mut files = Vec::new();
    for entry in fs::read_dir(directory.path())? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        files.push(StorageDirectoryFile::native_file_with_len(
            entry.path(),
            metadata.len(),
        ));
    }

    files.sort_unstable();
    Ok(files)
}

fn sync_native_file_directory_after_renames(directory: &StorageDirectoryId) -> Result<()> {
    require_native_file_directory_sync()?;

    sync_dir_after_renames(directory.path())
}

fn sync_native_file_parent_directory_after_rename(path: &Path) -> Result<()> {
    require_native_file_directory_sync()?;

    sync_parent_dir_after_rename(path)
}

fn read_current_manifest_from_native_file(object: &StorageObjectId) -> Result<Option<Arc<[u8]>>> {
    require_native_file_manifest_read(object)?;

    match fs::read(object.path()) {
        Ok(bytes) => Ok(Some(Arc::from(bytes))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn list_native_file_objects(request: &StorageObjectListRequest) -> Result<Vec<StorageObjectId>> {
    require_native_file_object_listing()?;

    let mut paths = Vec::new();
    for entry in fs::read_dir(request.root())? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        paths.push(entry.path());
    }
    Ok(native_file_objects_from_paths(request, paths))
}

fn native_file_objects_from_paths(
    request: &StorageObjectListRequest,
    paths: Vec<PathBuf>,
) -> Vec<StorageObjectId> {
    let mut objects = paths
        .into_iter()
        .filter(|path| native_file_matches_list_request(request, path))
        .map(|path| StorageObjectId::native_file(request.kind(), path))
        .collect::<Vec<_>>();
    objects.sort_unstable();
    objects
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
    let (path, tmp_path) = prepare_native_file_object_write(object, durability)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    fs::rename(&tmp_path, &path)?;

    Ok(())
}

fn delete_native_file_object(object: &StorageObjectId) -> Result<()> {
    require_native_file_object_delete(object)?;

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
    let (path, tmp_path) = prepare_native_file_manifest_publish(object, durability)?;
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        sync_native_file_for_durability(&file, durability)?;
    }
    fs::rename(&tmp_path, &path)?;
    if durability == DurabilityMode::SyncAll {
        sync_native_file_parent_directory_after_rename(&path)?;
    }

    Ok(())
}

fn sync_native_file_for_durability(file: &File, durability: DurabilityMode) -> Result<()> {
    match durability {
        DurabilityMode::Buffered => Ok(()),
        // This path treats `Flush` as a data sync (it publishes a file whose
        // bytes must be on disk before the rename is made durable).
        DurabilityMode::Flush | DurabilityMode::SyncData => {
            crate::durability::sync_file_for_durability(file, DurabilityMode::SyncData)
        }
        DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict => {
            crate::durability::sync_file_for_durability(file, durability)
        }
    }
}

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

#[cfg(test)]
mod storage_backend_tests {
    use super::*;

    #[test]
    fn native_variant_delegates_backend_traits() {
        let backend = StorageBackend::Native(NativeFileBackend::new());
        let direct = NativeFileBackend::new();
        // Capability reporting must match the wrapped backend exactly: the enum
        // is a transparent dispatcher, not a policy layer.
        assert_eq!(
            backend
                .capabilities()
                .supports(StorageCapability::Persistent),
            direct
                .capabilities()
                .supports(StorageCapability::Persistent)
        );
        assert_eq!(
            backend
                .capabilities()
                .supports(StorageCapability::ObjectWrite),
            direct
                .capabilities()
                .supports(StorageCapability::ObjectWrite)
        );
    }

    #[test]
    fn object_store_variant_dispatches_byte_ops_and_rejects_the_rest() {
        use crate::object_store::InMemoryObjectStore;

        let backend = StorageBackend::ObjectStore(ObjectStoreBackend::new(Arc::new(
            InMemoryObjectStore::new(),
        )));
        let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/0001.trinet");

        // Byte ops dispatch to the object-store backend.
        poll_ready_storage_future(backend.write_object(
            id.clone(),
            Arc::from(b"hi".as_slice()),
            DurabilityMode::Flush,
        ))
        .expect("write");
        assert_eq!(
            poll_ready_storage_future(backend.read_object_bytes(id.clone()))
                .expect("read")
                .as_deref(),
            Some(b"hi".as_slice())
        );
        assert!(
            backend
                .capabilities()
                .supports(StorageCapability::ObjectWrite)
        );
        assert!(!backend.capabilities().supports(StorageCapability::Append));

        // Non-byte ops are unsupported here: object-store DBs are async-only and
        // drive WAL/manifest ownership outside this byte backend.
        assert!(
            poll_ready_storage_future(
                backend.create_directory_all(StorageDirectoryId::native_file("/db"))
            )
            .is_err()
        );
        assert!(
            backend.open_read_blocking(id).is_err(),
            "object store is async-only"
        );
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

fn allocate_read_buffer(len: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| Error::invalid_options("storage read length exceeds addressable memory"))?;
    bytes.resize(len, 0);
    Ok(bytes)
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

fn u64_to_usize(value: u64, field: &'static str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| Error::invalid_options(format!("{field} exceeds usize::MAX")))
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(dead_code)]
mod browser_storage_bound_check {
    use std::{cell::Cell, rc::Rc};

    use super::{StorageObjectId, StorageObjectKind, StorageReadFuture, StorageReadObject};

    struct LocalReadObject {
        object: StorageObjectId,
        byte: Rc<Cell<u8>>,
    }

    impl LocalReadObject {
        fn new() -> Self {
            Self {
                object: StorageObjectId::memory(StorageObjectKind::Temporary, "local"),
                byte: Rc::new(Cell::new(0)),
            }
        }
    }

    impl StorageReadObject for LocalReadObject {
        fn object(&self) -> &StorageObjectId {
            &self.object
        }

        fn len(&self) -> StorageReadFuture<'_, u64> {
            let byte = Rc::clone(&self.byte);
            Box::pin(async move { Ok(u64::from(byte.get())) })
        }

        fn read_exact_at<'op>(
            &'op self,
            _offset: usize,
            bytes: &'op mut [u8],
        ) -> StorageReadFuture<'op, ()> {
            let byte = Rc::clone(&self.byte);
            Box::pin(async move {
                bytes.fill(byte.get());
                Ok(())
            })
        }
    }

    fn accepts_thread_local_read_object() -> LocalReadObject {
        LocalReadObject::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{Arc, mpsc},
        task::{Context, Poll, Wake, Waker},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::runtime::{Runtime, RuntimeOptions};

    struct ThreadWaker {
        thread: thread::Thread,
    }

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.thread.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.thread.unpark();
        }
    }

    fn test_waker() -> Waker {
        Waker::from(Arc::new(ThreadWaker {
            thread: thread::current(),
        }))
    }

    fn block_on_test_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
        let waker = test_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(result) => return result,
                Poll::Pending => thread::park_timeout(Duration::from_secs(1)),
            }
        }
    }

    fn hold_runtime_blocking_worker(runtime: &Runtime) -> mpsc::Sender<()> {
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        runtime
            .spawn_blocking(move || {
                started_tx.send(()).expect("report blocking worker start");
                release_rx.recv().expect("wait for release");
            })
            .expect("spawn worker holder");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker holder starts");
        release_tx
    }

    fn complete_after_blocking_worker_release<T>(
        runtime: &Runtime,
        mut future: StorageFuture<'_, T>,
        pending_message: &str,
    ) -> Result<T> {
        let release = hold_runtime_blocking_worker(runtime);
        let waker = test_waker();
        let mut context = Context::from_waker(&waker);
        assert!(
            matches!(future.as_mut().poll(&mut context), Poll::Pending),
            "{pending_message}"
        );

        release.send(()).expect("release blocking worker");
        block_on_test_future(future)
    }

    fn temp_storage_root(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn storage_read_buffer_from_vec_reuses_vec_allocation() {
        let bytes = b"owned read bytes".to_vec();
        let expected_ptr = bytes.as_ptr();

        let buffer = StorageReadBuffer::from_vec(7, bytes);

        assert_eq!(buffer.offset(), 7);
        assert_eq!(buffer.as_slice(), b"owned read bytes");
        assert_eq!(buffer.as_slice().as_ptr(), expected_ptr);
    }

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

        let owned =
            poll_ready_storage_future(StorageReadObject::read_exact_at_owned(&object, 1, 4))
                .expect("owned range reads");
        assert_eq!(owned.offset(), 1);
        assert_eq!(owned.len(), 4);
        assert!(!owned.is_empty());
        assert_eq!(&*owned.into_bytes(), b"bcde");

        let owned_blocking = object
            .read_exact_at_owned_blocking(3, 2)
            .expect("blocking owned range reads");
        assert_eq!(owned_blocking.offset(), 3);
        assert_eq!(&*owned_blocking.into_bytes(), b"de");

        std::fs::remove_file(path).expect("test file removes");
    }

    #[test]
    fn native_file_read_io_completion_returns_owned_buffer() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-completion-storage-read-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"abcdef").expect("test file writes");

        let backend = NativeFileBackend::new();
        let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
        let object = backend
            .open_read_blocking(object_id)
            .expect("completion object opens");
        let completion = object
            .read_exact_at_owned_io(2, 3)
            .expect("completion read submits");
        assert!(completion.is_finished().expect("completion state reads"));
        let buffer = poll_ready_storage_future(completion).expect("completion read finishes");

        assert_eq!(buffer.offset(), 2);
        assert_eq!(&*buffer.into_bytes(), b"cde");

        std::fs::remove_file(path).expect("test file removes");
    }

    #[test]
    fn native_file_append_io_completion_writes_and_persists() {
        let root = temp_storage_root("trine-kv-completion-append-storage");
        let object = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
        let backend = NativeFileBackend::new();
        let append = backend
            .open_append_blocking(object.clone())
            .expect("completion append object opens");

        let append_completion = append
            .append_io(Arc::from(&b"wal bytes"[..]), DurabilityMode::Buffered)
            .expect("completion append submits");
        assert!(
            append_completion
                .is_finished()
                .expect("append completion state reads")
        );
        poll_ready_storage_future(append_completion).expect("completion append finishes");

        let persist_completion = append
            .persist_io(DurabilityMode::Buffered)
            .expect("completion persist submits");
        poll_ready_storage_future(persist_completion).expect("completion persist finishes");

        assert_eq!(
            std::fs::read(object.path()).expect("WAL object reads"),
            b"wal bytes"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn runtime_enabled_native_file_object_mutations_use_blocking_adapter() {
        let root = temp_storage_root("trine-kv-runtime-object-mutations");
        let path = root.join("table-00000000000000000011.trinet");
        let object = StorageObjectId::native_file(StorageObjectKind::Table, &path);
        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
        let backend = NativeFileBackend::with_runtime(runtime.clone());

        complete_after_blocking_worker_release(
            &runtime,
            backend.write_object(
                object.clone(),
                Arc::from(&b"table bytes"[..]),
                DurabilityMode::Buffered,
            ),
            "object write should wait behind the occupied blocking worker",
        )
        .expect("runtime object write completes");
        assert_eq!(
            std::fs::read(object.path()).expect("table object reads"),
            b"table bytes"
        );

        complete_after_blocking_worker_release(
            &runtime,
            backend.delete_object(object.clone()),
            "object delete should wait behind the occupied blocking worker",
        )
        .expect("runtime object delete completes");
        assert!(!object.path().exists(), "table object should be deleted");

        let blocking_path = root.join("table-00000000000000000012.trinet");
        let blocking_object =
            StorageObjectId::native_file(StorageObjectKind::Table, &blocking_path);
        let release = hold_runtime_blocking_worker(&runtime);
        backend
            .write_object_blocking(
                blocking_object.clone(),
                Arc::from(&b"blocking table"[..]),
                DurabilityMode::Buffered,
            )
            .expect("blocking object write stays direct");
        backend
            .delete_object_blocking(blocking_object)
            .expect("blocking object delete stays direct");
        release.send(()).expect("release blocking worker");

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn runtime_enabled_native_file_directory_and_lease_ops_use_blocking_adapter() {
        let root = temp_storage_root("trine-kv-runtime-directory-storage");
        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
        let backend = NativeFileBackend::with_runtime(runtime.clone());
        let directory = StorageDirectoryId::native_file(&root);

        complete_after_blocking_worker_release(
            &runtime,
            backend.create_directory_all(directory.clone()),
            "directory create should wait behind the occupied blocking worker",
        )
        .expect("runtime directory create completes");
        assert!(root.is_dir(), "runtime directory create should create root");

        let lease_object =
            StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
        let lease = complete_after_blocking_worker_release(
            &runtime,
            backend.acquire_writer_lease(lease_object.clone()),
            "writer lease acquire should wait behind the occupied blocking worker",
        )
        .expect("runtime writer lease acquire completes");
        assert!(
            lease_object.path().exists(),
            "runtime writer lease should create marker"
        );
        drop(lease);
        assert!(
            lease_object.path().exists(),
            "dropping runtime writer lease should keep the lock file inode"
        );
        assert!(
            std::fs::read(lease_object.path())
                .expect("runtime writer lease marker reads")
                .is_empty(),
            "dropping runtime writer lease should clear owner text"
        );

        let listed_path = root.join("directory-file.tmp");
        std::fs::write(&listed_path, b"listed").expect("directory file writes");
        let files = complete_after_blocking_worker_release(
            &runtime,
            backend.list_directory_files(directory.clone()),
            "directory listing should wait behind the occupied blocking worker",
        )
        .expect("runtime directory listing completes");
        assert!(
            files.iter().any(|file| file.path() == listed_path),
            "directory listing should include the written file"
        );

        let sync_tmp = root.join("sync.tmp");
        let sync_published = root.join("sync.trinet");
        std::fs::write(&sync_tmp, b"sync").expect("sync temp file writes");
        std::fs::rename(&sync_tmp, &sync_published).expect("sync temp file renames");
        complete_after_blocking_worker_release(
            &runtime,
            backend.sync_directory_after_renames(directory),
            "directory sync should wait behind the occupied blocking worker",
        )
        .expect("runtime directory sync completes");

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn runtime_enabled_native_file_manifest_wal_and_listing_use_blocking_adapter() {
        let root = temp_storage_root("trine-kv-runtime-metadata-storage");
        std::fs::create_dir_all(&root).expect("test dir creates");
        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
        let backend = NativeFileBackend::with_runtime(runtime.clone());

        let manifest =
            StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
        complete_after_blocking_worker_release(
            &runtime,
            backend.publish_manifest(
                manifest.clone(),
                Arc::from(&b"manifest"[..]),
                DurabilityMode::Buffered,
            ),
            "manifest publish should wait behind the occupied blocking worker",
        )
        .expect("runtime manifest publish completes");
        let manifest_bytes = complete_after_blocking_worker_release(
            &runtime,
            backend.read_current_manifest(manifest.clone()),
            "manifest read should wait behind the occupied blocking worker",
        )
        .expect("runtime manifest read completes")
        .expect("manifest exists");
        assert_eq!(&*manifest_bytes, b"manifest");

        let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
        let wal_tmp =
            StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal.tmp"));
        std::fs::write(wal.path(), b"old wal").expect("old WAL writes");
        complete_after_blocking_worker_release(
            &runtime,
            backend.rewrite_wal(
                wal.clone(),
                wal_tmp.clone(),
                Arc::from(&b"new wal"[..]),
                DurabilityMode::Buffered,
            ),
            "WAL rewrite should wait behind the occupied blocking worker",
        )
        .expect("runtime WAL rewrite completes");
        assert_eq!(std::fs::read(wal.path()).expect("WAL reads"), b"new wal");
        assert!(
            !wal_tmp.path().exists(),
            "runtime WAL rewrite should remove the temporary object"
        );

        let table_path = root.join("table-00000000000000000021.trinet");
        std::fs::write(&table_path, b"table").expect("table file writes");
        let objects = complete_after_blocking_worker_release(
            &runtime,
            backend.list_objects(
                StorageObjectListRequest::native_file(StorageObjectKind::Table, &root)
                    .with_file_extension("trinet"),
            ),
            "object listing should wait behind the occupied blocking worker",
        )
        .expect("runtime object listing completes");
        assert_eq!(
            objects,
            vec![StorageObjectId::native_file(
                StorageObjectKind::Table,
                &table_path
            )]
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn runtime_enabled_native_file_append_operations_use_blocking_adapter() {
        let root = temp_storage_root("trine-kv-runtime-append-storage");
        let object = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
        let backend = NativeFileBackend::with_runtime(runtime.clone());

        let mut append = complete_after_blocking_worker_release(
            &runtime,
            backend.open_append(object.clone()),
            "append open should wait behind the occupied blocking worker",
        )
        .expect("runtime append object opens");
        complete_after_blocking_worker_release(
            &runtime,
            StorageAppendObject::append(&mut append, b"first", DurabilityMode::Buffered),
            "append write should wait behind the occupied blocking worker",
        )
        .expect("runtime append write completes");
        complete_after_blocking_worker_release(
            &runtime,
            StorageAppendObject::persist(&mut append, DurabilityMode::Flush),
            "append persist should wait behind the occupied blocking worker",
        )
        .expect("runtime append persist completes");
        assert_eq!(
            std::fs::read(object.path()).expect("WAL object reads"),
            b"first"
        );

        let release = hold_runtime_blocking_worker(&runtime);
        append
            .append_blocking(b"second", DurabilityMode::Buffered)
            .expect("blocking append stays direct");
        append
            .persist_blocking(DurabilityMode::Buffered)
            .expect("blocking append persist stays direct");
        release.send(()).expect("release blocking worker");
        assert_eq!(
            std::fs::read(object.path()).expect("WAL object reads"),
            b"firstsecond"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[cfg(all(
        feature = "platform-io",
        feature = "platform-io-native",
        target_os = "linux"
    ))]
    #[test]
    fn platform_io_native_file_read_and_append_use_platform_driver() {
        let root = temp_storage_root("trine-kv-platform-io-storage");
        std::fs::create_dir_all(&root).expect("test dir creates");
        let table =
            StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));
        std::fs::write(table.path(), b"abcdef").expect("table file writes");

        let runtime = Runtime::new(RuntimeOptions::platform_io());
        let backend = NativeFileBackend::with_runtime(runtime);
        let capabilities = backend.capabilities();
        assert!(capabilities.supports(StorageCapability::PlatformAsyncIo));

        let object = backend
            .open_read_blocking(table)
            .expect("platform I/O read object opens");
        let object_len = block_on_test_future(object.len()).expect("platform I/O len completes");
        assert_eq!(object_len, 6);
        let buffer = block_on_test_future(object.read_exact_at_owned(2, 3))
            .expect("platform I/O read completes");
        assert_eq!(buffer.offset(), 2);
        assert_eq!(&*buffer.into_bytes(), b"cde");

        let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
        let mut append = block_on_test_future(backend.open_append(wal.clone()))
            .expect("platform I/O append object opens");
        block_on_test_future(StorageAppendObject::append(
            &mut append,
            b"first",
            DurabilityMode::Buffered,
        ))
        .expect("platform I/O append completes");
        block_on_test_future(StorageAppendObject::persist(
            &mut append,
            DurabilityMode::Buffered,
        ))
        .expect("platform I/O persist completes");
        assert_eq!(
            std::fs::read(wal.path()).expect("WAL object reads"),
            b"first"
        );

        let stats = backend.stats();
        assert_platform_task_accounting(&stats, 5, 0);
        assert!(
            stats
                .platform_io_operations
                .length_lookup
                .true_platform_async
                > 0,
            "Linux platform length lookup should report true platform async"
        );
        assert!(
            stats.platform_io_operations.random_read.true_platform_async > 0,
            "Linux platform random reads should report true platform async"
        );
        assert!(
            stats.platform_io_operations.append_open.true_platform_async > 0,
            "Linux platform append open should report true platform async"
        );
        assert!(
            stats.platform_io_operations.append.true_platform_async > 0,
            "Linux platform append should report true platform async"
        );
        assert!(
            stats.platform_io_operations.persist.true_platform_async > 0,
            "Linux platform persist should report true platform async"
        );
        assert!(
            stats
                .platform_io_operations
                .total()
                .uses_true_platform_async(),
            "Linux platform diagnostics should aggregate true platform async work"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[cfg(all(
        feature = "platform-io",
        feature = "platform-io-native",
        target_os = "linux"
    ))]
    fn assert_platform_io_listing_managed_async(
        backend: &NativeFileBackend,
        directory: StorageDirectoryId,
        table: &StorageObjectId,
    ) {
        let list_request =
            StorageObjectListRequest::native_file(StorageObjectKind::Table, directory.path())
                .with_file_extension("trinet");
        let listed_objects = block_on_test_future(backend.list_objects(list_request.clone()))
            .expect("platform I/O object listing completes");
        assert_eq!(listed_objects, vec![table.clone()]);

        let listed_files = block_on_test_future(backend.list_directory_files(directory.clone()))
            .expect("platform I/O directory listing completes");
        assert!(
            listed_files.iter().any(|file| file.path() == table.path()),
            "directory listing should include the table"
        );
        assert_eq!(
            backend
                .list_objects_blocking(list_request)
                .expect("blocking object listing fallback completes"),
            vec![table.clone()]
        );
        assert!(
            backend
                .list_directory_files_blocking(directory)
                .expect("blocking directory listing fallback completes")
                .iter()
                .any(|file| file.path() == table.path()),
            "blocking directory listing should include the table"
        );
    }

    #[cfg(all(
        feature = "platform-io",
        feature = "platform-io-native",
        target_os = "linux"
    ))]
    fn assert_platform_task_accounting(
        stats: &NativeFileStorageStats,
        min_driver_tasks: u64,
        min_blocking_fallback_tasks: u64,
    ) {
        assert!(stats.uses_platform_io_driver);
        assert!(stats.uses_platform_async_io);
        assert_eq!(stats.blocking_adapter_tasks, 0);

        let driver_tasks = stats
            .platform_async_io_tasks
            .saturating_add(stats.platform_thread_pool_managed_async_tasks);
        assert!(
            driver_tasks >= min_driver_tasks,
            "platform driver task count {driver_tasks} should be at least {min_driver_tasks}"
        );
        assert!(
            stats.platform_blocking_fallback_tasks >= min_blocking_fallback_tasks,
            "platform blocking fallback count {} should be at least {}",
            stats.platform_blocking_fallback_tasks,
            min_blocking_fallback_tasks
        );

        assert!(
            stats.platform_async_io_tasks > 0,
            "Linux platform backend should account true async work"
        );
    }

    #[cfg(all(
        feature = "platform-io",
        feature = "platform-io-native",
        target_os = "linux"
    ))]
    #[test]
    fn platform_io_native_file_management_ops_use_platform_driver() {
        let root = temp_storage_root("trine-kv-platform-io-management");
        let runtime = Runtime::new(RuntimeOptions::platform_io());
        let backend = NativeFileBackend::with_runtime(runtime);
        let directory = StorageDirectoryId::native_file(&root);

        block_on_test_future(backend.create_directory_all(directory.clone()))
            .expect("platform I/O directory create completes");
        assert!(root.is_dir(), "directory create should create root");

        let table = StorageObjectId::native_file(
            StorageObjectKind::Table,
            root.join("table-00000000000000000033.trinet"),
        );
        block_on_test_future(backend.write_object(
            table.clone(),
            Arc::from(&b"table bytes"[..]),
            DurabilityMode::Buffered,
        ))
        .expect("platform I/O object write completes");
        let table_bytes = block_on_test_future(backend.read_object_bytes(table.clone()))
            .expect("platform I/O object read completes")
            .expect("table object exists");
        assert_eq!(&*table_bytes, b"table bytes");

        let manifest =
            StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
        block_on_test_future(backend.publish_manifest(
            manifest.clone(),
            Arc::from(&b"manifest bytes"[..]),
            DurabilityMode::SyncAll,
        ))
        .expect("platform I/O manifest publish completes");
        let manifest_bytes = block_on_test_future(backend.read_current_manifest(manifest.clone()))
            .expect("platform I/O manifest read completes")
            .expect("manifest exists");
        assert_eq!(&*manifest_bytes, b"manifest bytes");

        let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
        let wal_tmp =
            StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal.tmp"));
        std::fs::write(wal.path(), b"old wal").expect("old WAL writes");
        block_on_test_future(backend.rewrite_wal(
            wal.clone(),
            wal_tmp.clone(),
            Arc::from(&b"new wal"[..]),
            DurabilityMode::SyncAll,
        ))
        .expect("platform I/O WAL rewrite completes");
        assert_eq!(
            std::fs::read(wal.path()).expect("WAL object reads"),
            b"new wal"
        );
        assert!(
            !wal_tmp.path().exists(),
            "WAL rewrite should remove the temporary object"
        );

        let lease_object =
            StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
        let lease = block_on_test_future(backend.acquire_writer_lease(lease_object.clone()))
            .expect("platform I/O writer lease acquires");
        assert!(
            lease_object.path().exists(),
            "writer lease marker should exist"
        );
        drop(lease);
        assert!(
            lease_object.path().exists(),
            "dropping writer lease should keep the lock file inode"
        );
        assert!(
            std::fs::read(lease_object.path())
                .expect("platform writer lease marker reads")
                .is_empty(),
            "dropping writer lease should clear owner text"
        );

        assert_platform_io_listing_managed_async(&backend, directory.clone(), &table);

        block_on_test_future(backend.sync_directory_after_renames(directory))
            .expect("platform I/O directory sync completes");
        block_on_test_future(backend.delete_object(table.clone()))
            .expect("platform I/O object delete completes");
        assert!(!table.path().exists(), "table object should be deleted");

        let stats = backend.stats();
        assert_platform_task_accounting(&stats, 11, 0);
        assert_linux_platform_management_counters(&stats);

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[cfg(all(
        feature = "platform-io",
        feature = "platform-io-native",
        target_os = "linux"
    ))]
    fn assert_linux_platform_management_counters(stats: &NativeFileStorageStats) {
        assert!(
            stats
                .platform_io_operations
                .temp_write_rename_publish
                .true_platform_async
                > 0,
            "Linux platform publish writes should report true platform async"
        );
        assert!(
            stats.platform_io_operations.wal_rewrite.true_platform_async > 0,
            "Linux platform WAL rewrite should report true platform async"
        );
        assert!(
            stats
                .platform_io_operations
                .whole_object_read
                .true_platform_async
                > 0,
            "Linux platform whole-object reads should report true platform async"
        );
        assert!(
            stats.platform_io_operations.delete.true_platform_async > 0,
            "Linux platform deletes should report true platform async"
        );
        assert!(
            stats
                .platform_io_operations
                .directory_create
                .true_platform_async
                > 0,
            "Linux platform directory create should report true platform async"
        );
        assert!(
            stats
                .platform_io_operations
                .writer_lease
                .true_platform_async
                > 0,
            "Linux platform writer lease should report true platform async"
        );
        assert!(
            stats
                .platform_io_operations
                .total()
                .uses_true_platform_async(),
            "Linux platform management diagnostics should aggregate true platform async work"
        );
        assert!(
            stats
                .platform_io_operations
                .directory_listing
                .thread_pool_managed_async
                > 0,
            "directory listing should report thread-pool managed async"
        );
        assert!(
            stats
                .platform_io_operations
                .directory_sync
                .true_platform_async
                > 0,
            "Linux platform directory sync should report true platform async"
        );
    }

    #[cfg(all(
        feature = "platform-io",
        feature = "platform-io-native",
        any(
            windows,
            target_os = "macos",
            target_os = "freebsd",
            target_os = "illumos",
            target_os = "solaris"
        )
    ))]
    #[test]
    fn platform_io_partial_native_storage_ops_use_platform_driver() {
        let root = temp_storage_root("trine-kv-platform-io-partial-native-storage");
        std::fs::create_dir_all(&root).expect("test dir creates");
        let table =
            StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));
        std::fs::write(table.path(), b"abcdef").expect("table file writes");

        let runtime = Runtime::new(RuntimeOptions::platform_io());
        let backend = NativeFileBackend::with_runtime(runtime);
        let capabilities = backend.capabilities();
        assert!(capabilities.supports(StorageCapability::PlatformAsyncIo));
        assert!(capabilities.supports(StorageCapability::BlockingAdapter));

        let object = backend
            .open_read_blocking(table)
            .expect("read object opens with platform driver fallback");
        let buffer = block_on_test_future(object.read_exact_at_owned(2, 3))
            .expect("read completes through platform driver fallback");
        assert_eq!(buffer.offset(), 2);
        assert_eq!(&*buffer.into_bytes(), b"cde");

        let stats = backend.stats();
        assert!(!stats.uses_blocking_adapter);
        assert!(stats.uses_platform_io_driver);
        assert!(stats.uses_platform_async_io);
        assert!(
            stats.platform_async_io_tasks > 0,
            "partial native platform work should count as platform async I/O"
        );
        assert_eq!(
            stats.platform_thread_pool_managed_async_tasks, 0,
            "single random read should not be counted as thread-pool managed async"
        );
        assert_eq!(stats.platform_blocking_fallback_tasks, 0);
        assert_eq!(stats.blocking_adapter_tasks, 0);
        assert!(
            stats
                .platform_io_operations
                .random_read
                .platform_native_async_but_partial
                > 0,
            "platform random read should report partial native async"
        );
        let platform_total = stats.platform_io_operations.total();
        assert!(
            platform_total.uses_non_true_platform_async(),
            "non-Linux platform diagnostics should aggregate non-true-platform work"
        );
        assert!(
            !platform_total.uses_true_platform_async(),
            "partial native diagnostics should not report whole-operation true async work"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[cfg(all(
        feature = "platform-io",
        not(feature = "platform-io-native"),
        any(unix, windows)
    ))]
    #[test]
    fn platform_io_threadpool_storage_ops_use_platform_driver() {
        let root = temp_storage_root("trine-kv-platform-io-threadpool-storage");
        std::fs::create_dir_all(&root).expect("test dir creates");
        let table =
            StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));
        std::fs::write(table.path(), b"abcdef").expect("table file writes");

        let runtime = Runtime::new(RuntimeOptions::platform_io());
        let backend = NativeFileBackend::with_runtime(runtime);
        let capabilities = backend.capabilities();
        assert!(capabilities.supports(StorageCapability::PlatformAsyncIo));
        assert!(capabilities.supports(StorageCapability::BlockingAdapter));

        let object = backend
            .open_read_blocking(table)
            .expect("read object opens with platform driver fallback");
        let buffer = block_on_test_future(object.read_exact_at_owned(2, 3))
            .expect("read completes through platform driver fallback");
        assert_eq!(buffer.offset(), 2);
        assert_eq!(&*buffer.into_bytes(), b"cde");

        let stats = backend.stats();
        assert!(!stats.uses_blocking_adapter);
        assert!(stats.uses_platform_io_driver);
        assert!(stats.uses_platform_async_io);
        assert_eq!(stats.platform_async_io_tasks, 0);
        assert!(
            stats.platform_thread_pool_managed_async_tasks > 0,
            "platform driver should account thread-pool managed async tasks"
        );
        assert_eq!(stats.platform_blocking_fallback_tasks, 0);
        assert_eq!(stats.blocking_adapter_tasks, 0);
        assert!(
            stats
                .platform_io_operations
                .random_read
                .thread_pool_managed_async
                > 0,
            "platform random read should report thread-pool managed async"
        );
        let platform_total = stats.platform_io_operations.total();
        assert!(
            platform_total.uses_non_true_platform_async(),
            "thread-pool diagnostics should aggregate non-true-platform work"
        );
        assert!(
            !platform_total.uses_true_platform_async(),
            "thread-pool diagnostics should not report true platform async work"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn inline_runtime_native_file_mutations_remain_ready() {
        let root = temp_storage_root("trine-kv-inline-runtime-mutations");
        let runtime = Runtime::new(RuntimeOptions::inline());
        let backend = NativeFileBackend::with_runtime(runtime);
        let table = StorageObjectId::native_file(
            StorageObjectKind::Table,
            root.join("table-00000000000000000031.trinet"),
        );
        poll_ready_storage_future(backend.write_object(
            table.clone(),
            Arc::from(&b"inline table"[..]),
            DurabilityMode::Buffered,
        ))
        .expect("inline runtime object write is ready");
        assert_eq!(
            std::fs::read(table.path()).expect("table object reads"),
            b"inline table"
        );

        let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
        let mut append =
            poll_ready_storage_future(backend.open_append(wal.clone())).expect("WAL opens");
        poll_ready_storage_future(StorageAppendObject::append(
            &mut append,
            b"inline wal",
            DurabilityMode::Buffered,
        ))
        .expect("inline runtime append is ready");
        poll_ready_storage_future(StorageAppendObject::persist(
            &mut append,
            DurabilityMode::Buffered,
        ))
        .expect("inline runtime append persist is ready");
        assert_eq!(
            std::fs::read(wal.path()).expect("WAL object reads"),
            b"inline wal"
        );

        poll_ready_storage_future(backend.delete_object(table))
            .expect("inline runtime object delete is ready");

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn runtime_enabled_native_file_owned_read_uses_blocking_adapter() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-runtime-storage-read-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"abcdef").expect("test file writes");

        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 2);
        let release = hold_runtime_blocking_worker(&runtime);
        let backend = NativeFileBackend::with_runtime(runtime);
        let capabilities = backend.capabilities();
        assert!(capabilities.supports(StorageCapability::AsyncTasks));
        assert!(capabilities.supports(StorageCapability::BlockingAdapter));
        assert!(capabilities.supports(StorageCapability::BackgroundThreads));
        assert!(!capabilities.supports(StorageCapability::PlatformAsyncIo));

        let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
        let object = poll_ready_storage_future(backend.open_read(object_id))
            .expect("runtime-backed object opens");
        let mut read = StorageReadObject::read_exact_at_owned(&object, 1, 4);
        let waker = test_waker();
        let mut context = Context::from_waker(&waker);
        assert!(
            matches!(read.as_mut().poll(&mut context), Poll::Pending),
            "owned read should wait behind the occupied blocking worker"
        );

        release.send(()).expect("release blocking worker");
        let buffer = block_on_test_future(read).expect("runtime owned read completes");
        assert_eq!(buffer.offset(), 1);
        assert_eq!(&*buffer.into_bytes(), b"bcde");
        let stats = backend.stats();
        assert!(stats.uses_blocking_adapter);
        assert!(!stats.uses_platform_async_io);
        assert_eq!(stats.blocking_adapter_tasks, 1);
        assert_eq!(stats.inline_tasks, 0);

        std::fs::remove_file(path).expect("test file removes");
    }

    #[test]
    fn runtime_enabled_native_file_object_read_uses_blocking_adapter() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-runtime-object-read-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"whole object").expect("test file writes");

        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 2);
        let release = hold_runtime_blocking_worker(&runtime);
        let backend = NativeFileBackend::with_runtime(runtime);
        let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);

        let mut read = backend.read_object_bytes(object_id);
        let waker = test_waker();
        let mut context = Context::from_waker(&waker);
        assert!(
            matches!(read.as_mut().poll(&mut context), Poll::Pending),
            "whole-object read should wait behind the occupied blocking worker"
        );

        release.send(()).expect("release blocking worker");
        let bytes = block_on_test_future(read)
            .expect("runtime object read completes")
            .expect("object exists");
        assert_eq!(&*bytes, b"whole object");
        let stats = backend.stats();
        assert!(stats.uses_blocking_adapter);
        assert_eq!(stats.blocking_adapter_tasks, 1);
        assert_eq!(stats.blocking_adapter_queue_capacity, 2);
        assert!(
            (1..=2).contains(&stats.blocking_adapter_submitted_tasks),
            "submitted blocking tasks should include the read and may include the worker holder"
        );
        assert!(
            (1..=2).contains(&stats.blocking_adapter_completed_tasks),
            "completed blocking tasks should include the read and may include the worker holder"
        );
        assert_eq!(stats.operations.read_object_bytes.requests, 1);
        assert!(stats.operations.read_object_bytes.total_latency_micros > 0);
        assert_eq!(stats.inline_tasks, 0);

        std::fs::remove_file(path).expect("test file removes");
    }

    #[test]
    fn inline_runtime_native_file_owned_read_remains_ready() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-inline-runtime-storage-read-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::write(&path, b"abcdef").expect("test file writes");

        let runtime = Runtime::new(RuntimeOptions::inline());
        let backend = NativeFileBackend::with_runtime(runtime);
        let capabilities = backend.capabilities();
        assert!(!capabilities.supports(StorageCapability::AsyncTasks));
        assert!(!capabilities.supports(StorageCapability::BlockingAdapter));
        assert!(!capabilities.supports(StorageCapability::PlatformAsyncIo));

        let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
        let object = poll_ready_storage_future(backend.open_read(object_id))
            .expect("inline runtime object opens");
        let buffer =
            poll_ready_storage_future(StorageReadObject::read_exact_at_owned(&object, 2, 3))
                .expect("inline runtime owned read is ready");

        assert_eq!(buffer.offset(), 2);
        assert_eq!(&*buffer.into_bytes(), b"cde");
        let stats = backend.stats();
        assert!(!stats.uses_blocking_adapter);
        assert!(!stats.uses_platform_async_io);
        assert_eq!(stats.blocking_adapter_tasks, 0);
        assert_eq!(stats.inline_tasks, 1);

        std::fs::remove_file(path).expect("test file removes");
    }

    #[test]
    fn native_file_backend_reads_optional_object_bytes() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-object-read-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        let path = root.join("trine.wal");
        std::fs::write(&path, b"wal bytes").expect("WAL writes");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::ObjectRead)
            .expect("native-file backend supports whole-object reads");
        let object = StorageObjectId::native_file(StorageObjectKind::Wal, &path);
        let bytes = backend
            .read_object_bytes_blocking(object)
            .expect("object read succeeds")
            .expect("object exists");
        assert_eq!(&*bytes, b"wal bytes");

        let missing =
            StorageObjectId::native_file(StorageObjectKind::Wal, root.join("missing.wal"));
        assert!(
            backend
                .read_object_bytes_blocking(missing)
                .expect("missing object read succeeds")
                .is_none()
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
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
    fn native_file_backend_writes_recovery_report_object() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-write-recovery-report-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let path = root.join("RECOVERY_REPORT");
        let tmp_path = path.with_extension("tmp");
        let object = StorageObjectId::native_file(StorageObjectKind::RecoveryReport, &path);

        NativeFileBackend::new()
            .write_object_blocking(
                object.clone(),
                Arc::from(&b"recovery report"[..]),
                DurabilityMode::SyncAll,
            )
            .expect("recovery report object writes");

        assert_eq!(
            std::fs::read(object.path()).expect("recovery report object reads"),
            b"recovery report"
        );
        assert!(
            !tmp_path.exists(),
            "successful recovery report write should leave only the final object"
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
    fn native_file_backend_rewrites_wal_with_explicit_temporary_object() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-wal-rewrite-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        let wal_path = root.join("trine.wal");
        let tmp_path = root.join("trine.wal.tmp");
        std::fs::write(&wal_path, b"old wal").expect("old WAL writes");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::AtomicWalRewrite)
            .expect("native-file backend supports WAL rewrite");
        backend
            .rewrite_wal_blocking(
                StorageObjectId::native_file(StorageObjectKind::Wal, &wal_path),
                StorageObjectId::native_file(StorageObjectKind::Wal, &tmp_path),
                Arc::from(&b"new wal"[..]),
                DurabilityMode::SyncAll,
            )
            .expect("WAL rewrites");

        assert_eq!(std::fs::read(&wal_path).expect("WAL reads"), b"new wal");
        assert!(
            !tmp_path.exists(),
            "successful WAL rewrite should remove the explicit temporary object"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_wal_rewrite_rejects_non_wal_objects() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-wal-rewrite-kind-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let backend = NativeFileBackend::new();
        let error = backend
            .rewrite_wal_blocking(
                StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet")),
                StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal.tmp")),
                Arc::from(&b"bytes"[..]),
                DurabilityMode::SyncAll,
            )
            .expect_err("WAL rewrite only accepts WAL objects");
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
            object.path().exists(),
            "dropping owned writer lease should keep the lock file inode"
        );
        assert!(
            std::fs::read(object.path())
                .expect("writer lease marker reads")
                .is_empty(),
            "dropping owned writer lease should clear owner text"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_recovers_stale_writer_lease_marker() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-writer-lease-stale-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        let object =
            StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
        std::fs::write(object.path(), b"pid=stale\nnonce=stale\n")
            .expect("stale writer lease marker writes");

        let lease = NativeFileBackend::new()
            .acquire_writer_lease_blocking(object.clone())
            .expect("stale writer lease marker does not block OS lock acquire");
        let marker = std::fs::read_to_string(object.path()).expect("lease marker reads");
        assert_ne!(
            marker, "pid=stale\nnonce=stale\n",
            "acquiring over a stale marker should publish the new owner"
        );

        let error = NativeFileBackend::new()
            .acquire_writer_lease_blocking(object.clone())
            .expect_err("live OS writer lease still blocks a second writer");
        assert!(error.to_string().contains("database lock is already held"));

        drop(lease);
        assert!(
            object.path().exists(),
            "dropping recovered writer lease should keep the lock file inode"
        );
        assert!(
            std::fs::read(object.path())
                .expect("recovered writer lease marker reads")
                .is_empty(),
            "dropping recovered writer lease should clear owner text"
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
        let mut lease = NativeFileBackend::new()
            .acquire_writer_lease_blocking(object.clone())
            .expect("writer lease acquires");
        let file = lease
            .file
            .as_mut()
            .expect("native writer lease owns a file");
        file.set_len(0).expect("lease marker truncates");
        file.seek(SeekFrom::Start(0)).expect("lease marker seeks");
        file.write_all(b"pid=other\nnonce=other\n")
            .expect("lease marker changes");
        file.flush().expect("lease marker flushes");

        drop(lease);

        assert_eq!(
            std::fs::read(object.path()).expect("changed lease marker remains"),
            b"pid=other\nnonce=other\n"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_creates_directory_tree() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-directory-create-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        let nested = root.join("db").join("nested");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::DirectoryCreate)
            .expect("native-file backend supports directory create");
        backend
            .create_directory_all_blocking(StorageDirectoryId::native_file(&nested))
            .expect("directory tree creates");

        assert!(nested.is_dir(), "nested directory should exist");

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_lists_directory_files() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-directory-list-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        std::fs::write(root.join("MANIFEST.tmp"), b"manifest").expect("manifest tmp writes");
        std::fs::write(root.join("trine.wal.tmp"), b"wal").expect("wal tmp writes");
        std::fs::create_dir(root.join("nested")).expect("nested dir creates");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::DirectoryListing)
            .expect("native-file backend supports directory listing");
        let files = backend
            .list_directory_files_blocking(StorageDirectoryId::native_file(&root))
            .expect("directory files list");
        let names = files
            .iter()
            .map(|file| {
                file.path()
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("file name is UTF-8")
            })
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["MANIFEST.tmp", "trine.wal.tmp"]);
        let lengths = files
            .iter()
            .map(|file| file.byte_len().expect("native listing records byte length"))
            .collect::<Vec<_>>();
        assert_eq!(lengths, vec![8, 3]);

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn native_file_backend_syncs_directory_after_renames() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-storage-directory-sync-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");

        let tmp_path = root.join("value.tmp");
        let published_path = root.join("value.trinet");
        std::fs::write(&tmp_path, b"published").expect("temp file writes");
        std::fs::rename(&tmp_path, &published_path).expect("file renames");

        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::DirectorySync)
            .expect("native-file backend supports directory sync");
        backend
            .sync_directory_after_renames_blocking(StorageDirectoryId::native_file(&root))
            .expect("directory sync succeeds");

        let parent = StorageDirectoryId::native_file_parent_of(&published_path)
            .expect("published path has parent directory");
        assert_eq!(parent.path(), root.as_path());
        assert_eq!(
            std::fs::read(&published_path).expect("published file reads"),
            b"published"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn memory_storage_backend_exposes_async_read_shape() {
        let backend = MemoryStorageBackend::new();
        let capabilities = backend.capabilities();
        assert!(capabilities.supports(StorageCapability::Volatile));
        assert!(capabilities.supports(StorageCapability::RandomRead));
        assert!(capabilities.supports(StorageCapability::ObjectRead));
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

        let owned =
            poll_ready_storage_future(StorageReadObject::read_exact_at_owned(&object, 2, 3))
                .expect("owned range reads");
        assert_eq!(owned.offset(), 2);
        assert_eq!(owned.len(), 3);
        assert!(!owned.is_empty());
        assert_eq!(&*owned.into_bytes(), b"cde");

        let owned_blocking = object
            .read_exact_at_owned_blocking(0, 0)
            .expect("empty owned range reads");
        assert_eq!(owned_blocking.offset(), 0);
        assert_eq!(owned_blocking.len(), 0);
        assert!(owned_blocking.is_empty());
        assert_eq!(&*owned_blocking.into_bytes(), b"");

        let full = backend
            .read_object_bytes_blocking(StorageObjectId::memory(
                StorageObjectKind::Table,
                "table-7",
            ))
            .expect("memory object read succeeds")
            .expect("memory object exists");
        assert_eq!(&*full, b"abcdef");
        assert!(
            backend
                .read_object_bytes_blocking(StorageObjectId::memory(
                    StorageObjectKind::Table,
                    "missing-table",
                ))
                .expect("missing memory object read succeeds")
                .is_none()
        );
    }

    #[test]
    fn storage_capabilities_report_unsupported_backend_and_durability() {
        let read_only = StorageCapabilities::native_file_read();
        assert!(read_only.supports(StorageCapability::Persistent));
        assert!(read_only.supports(StorageCapability::RandomRead));
        assert!(read_only.supports(StorageCapability::ObjectRead));
        assert!(read_only.supports(StorageCapability::ObjectListing));
        assert!(read_only.supports(StorageCapability::DirectoryListing));
        assert!(!read_only.supports(StorageCapability::ObjectWrite));
        assert!(!read_only.supports(StorageCapability::ObjectDelete));
        assert!(!read_only.supports(StorageCapability::Append));
        assert!(!read_only.supports(StorageCapability::AtomicWalRewrite));
        assert!(!read_only.supports(StorageCapability::DirectoryCreate));
        assert!(!read_only.supports(StorageCapability::DirectorySync));
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
