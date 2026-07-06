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
    durability::{
        requires_parent_dir_sync_after_rename, sync_dir_after_renames, sync_parent_dir_after_rename,
    },
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

mod backend;
mod metrics;
mod native_file;

#[allow(unused_imports)]
pub(crate) use backend::*;
pub(crate) use metrics::NativeFileStorageStats;
use metrics::*;
pub(crate) use native_file::*;
#[cfg(test)]
mod backend_tests;
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
mod tests;
