use super::super::poll_ready_storage_future;
#[cfg(feature = "platform-io")]
use super::super::{Context, Future, Poll, Waker};
use super::{
    Arc, BlockReadSource, BlockingAdapterIoDriver, BlockingStorageAppendObject,
    BlockingStorageReadObject, DurabilityMode, Error, File, InlineIoDriver, Instant,
    IoAppendObject, IoCompletion, IoDriver, IoReadObject, Mutex, MutexGuard,
    NativeFileStorageMetrics, Result, Runtime, StorageAppendObject, StorageFuture, StorageObjectId,
    StorageOperation, StorageReadBuffer, StorageReadFuture, StorageReadObject,
    acquire_native_file_writer_lease, append_native_file_object,
    clear_native_file_writer_lease_owner, fs, len_native_file_handle, lock_native_append_file,
    open_native_append_file, open_native_file, persist_native_append_file,
    read_exact_at_native_file_handle, read_exact_at_native_file_handle_owned,
    read_exact_at_native_file_owned, read_exact_from_native_file, record_timed_storage_future,
    record_timed_storage_result, write_native_file_writer_lease_owner, writer_lease_owner_text,
};
#[cfg(feature = "platform-io")]
use super::{PlatformIoDriver, PlatformIoOperation, record_platform_io_task};

#[derive(Debug)]
pub(crate) struct NativeFileObject {
    pub(in crate::storage) object: StorageObjectId,
    pub(in crate::storage) file: Arc<Mutex<File>>,
    pub(in crate::storage) runtime: Option<Runtime>,
    #[cfg(feature = "platform-io")]
    pub(in crate::storage) platform_io: Option<PlatformIoDriver>,
    pub(in crate::storage) metrics: Arc<NativeFileStorageMetrics>,
}

impl NativeFileObject {
    pub(in crate::storage) fn open(
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
    pub(in crate::storage) fn open(
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
    pub(in crate::storage) fn open_platform(
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
    pub(in crate::storage) file: Option<File>,
}

impl NativeFileWriterLease {
    pub(in crate::storage) fn acquire(object: StorageObjectId) -> Result<Self> {
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
    pub(in crate::storage) fn from_locked_file(
        object: StorageObjectId,
        owner: String,
        file: File,
    ) -> Self {
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
        #[cfg(any(unix, windows))]
        {
            if let Some(file) = self.file.as_ref() {
                let _ = fs4::fs_std::FileExt::unlock(file);
            }
        }
        let _ = self.file.take();
    }
}

#[cfg(feature = "platform-io")]
pub(in crate::storage) fn wait_for_platform_io<T>(completion: IoCompletion<T>) -> Result<T> {
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
