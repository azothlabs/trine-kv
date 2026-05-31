use std::{
    fs::File,
    future::Future,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};

use crate::{
    block::BlockReadSource,
    error::{Error, Result},
    options::DurabilityMode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StorageObjectKind {
    Table,
}

impl StorageObjectKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    pub(crate) const fn kind(&self) -> StorageObjectKind {
        self.kind
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

pub(crate) type StorageReadFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'op>>;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageCapability {
    Volatile,
    Persistent,
    RandomRead,
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
            Self::Append => 1 << 3,
            Self::AtomicManifestPublish => 1 << 4,
            Self::WriterLease => 1 << 5,
            Self::Flush => 1 << 6,
            Self::StrictDataSync => 1 << 7,
            Self::StrictMetadataSync => 1 << 8,
            Self::BackgroundThreads => 1 << 9,
            Self::AsyncTasks => 1 << 10,
            Self::CooperativeTasks => 1 << 11,
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
        StorageCapabilities::native_file_read()
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

fn poll_ready_storage_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => Err(Error::unsupported_backend(
            "runtime for pending native-file storage future",
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
    fn storage_capabilities_report_unsupported_backend_and_durability() {
        let read_only = StorageCapabilities::native_file_read();
        assert!(read_only.supports(StorageCapability::Persistent));
        assert!(read_only.supports(StorageCapability::RandomRead));
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
