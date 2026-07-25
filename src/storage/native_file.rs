use super::{
    Arc, BlockReadSource, BlockingAdapterIoDriver, BlockingStorageAppendBackend,
    BlockingStorageAppendObject, BlockingStorageDirectoryCreateBackend,
    BlockingStorageDirectoryListBackend, BlockingStorageDirectorySyncBackend,
    BlockingStorageManifestPublishBackend, BlockingStorageManifestReadBackend,
    BlockingStorageObjectDeleteBackend, BlockingStorageObjectListBackend,
    BlockingStorageObjectReadBackend, BlockingStorageObjectWriteBackend,
    BlockingStorageReadBackend, BlockingStorageReadObject, BlockingStorageWalRewriteBackend,
    BlockingStorageWriterLeaseBackend, DurabilityMode, Error, File, InlineIoDriver, Instant,
    IoAppendObject, IoCompletion, IoDriver, IoReadObject, Mutex, MutexGuard, NativeFileBackend,
    NativeFileStorageMetrics, OpenOptions, Path, PathBuf, Read, Result, Runtime, Seek, SeekFrom,
    StorageAppendBackend, StorageAppendObject, StorageCapabilities, StorageCapability,
    StorageDirectoryCreateBackend, StorageDirectoryFile, StorageDirectoryId,
    StorageDirectoryListBackend, StorageDirectorySyncBackend, StorageFuture,
    StorageManifestPublishBackend, StorageManifestReadBackend, StorageObjectDeleteBackend,
    StorageObjectId, StorageObjectKind, StorageObjectListBackend, StorageObjectListRequest,
    StorageObjectReadBackend, StorageObjectWriteBackend, StorageOperation, StorageReadBackend,
    StorageReadBuffer, StorageReadFuture, StorageReadObject, StorageWalRewriteBackend,
    StorageWriterLeaseBackend, SystemTime, UNIX_EPOCH, Write, allocate_read_buffer,
    ensure_whole_object_read_len, fs, io, record_timed_storage_future, record_timed_storage_result,
    requires_parent_dir_sync_after_rename, sync_dir_after_renames, sync_parent_dir_after_rename,
    u64_to_usize, usize_to_u64,
};
#[cfg(feature = "platform-io")]
use super::{
    PlatformIoDriver, PlatformIoOperation, max_whole_object_read_bytes, record_platform_io_task,
};

mod backend_impls;
mod helpers;
mod objects;

pub(in crate::storage) use helpers::*;
#[cfg(feature = "platform-io")]
pub(in crate::storage) use objects::wait_for_platform_io;
pub(crate) use objects::{
    NativeFileAppendObject, NativeFileObject, NativeFileReadSource, NativeFileWriterLease,
    StorageReadSource,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(dead_code)]
mod browser_persistent_storage {
    use std::{
        cell::RefCell,
        io,
        path::{Component, Path, PathBuf},
        rc::Rc,
        sync::Arc,
    };

    use futures::{StreamExt, channel::oneshot};
    use js_sys::{Function, Promise, Reflect};
    use opfs::{
        CreateWritableOptions, DirectoryEntry, DirectoryHandle as _, FileHandle as _,
        FileSystemRemoveOptions, GetDirectoryHandleOptions as OpfsGetDirectoryHandleOptions,
        GetFileHandleOptions as OpfsGetFileHandleOptions, WritableFileStream as _,
        persistent::{self, DirectoryHandle, FileHandle},
    };
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        DedicatedWorkerGlobalScope, FileSystemDirectoryHandle, FileSystemFileHandle,
        FileSystemGetDirectoryOptions as WebGetDirectoryOptions,
        FileSystemGetFileOptions as WebGetFileOptions, FileSystemReadWriteOptions,
        FileSystemSyncAccessHandle,
    };

    use super::{
        DurabilityMode, Error, Result, StorageAppendBackend, StorageAppendObject,
        StorageCapabilities, StorageCapability, StorageDirectoryCreateBackend,
        StorageDirectoryFile, StorageDirectoryId, StorageDirectoryListBackend, StorageFuture,
        StorageManifestPublishBackend, StorageManifestReadBackend, StorageObjectDeleteBackend,
        StorageObjectId, StorageObjectKind, StorageObjectListBackend, StorageObjectListRequest,
        StorageObjectReadBackend, StorageObjectWriteBackend, StorageReadBackend, StorageReadFuture,
        StorageReadObject, StorageWalRewriteBackend, StorageWriterLeaseBackend,
        allocate_read_buffer, ensure_whole_object_read_len, native_file_objects_from_paths,
        usize_to_u64,
    };

    #[derive(Debug, Clone)]
    pub(crate) struct BrowserStorageBackend {
        root: DirectoryHandle,
        web_root: FileSystemDirectoryHandle,
    }

    impl BrowserStorageBackend {
        pub(crate) async fn new() -> Result<Self> {
            let (root, web_root) = browser_opfs_roots().await?;
            Ok(Self { root, web_root })
        }

        pub(crate) fn normalize_namespace_path(path: &Path) -> Result<PathBuf> {
            let segments = opfs_path_segments(path)?;
            let mut normalized = PathBuf::new();
            for segment in segments {
                normalized.push(segment);
            }
            Ok(normalized)
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
            let options = OpfsGetDirectoryHandleOptions { create };
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
            let options = OpfsGetFileHandleOptions { create };
            match directory
                .get_file_handle_with_options(&name, &options)
                .await
            {
                Ok(file) => Ok(Some(file)),
                Err(error) if !create && is_opfs_not_found(&error) => Ok(None),
                Err(error) => Err(map_opfs_error(&error)),
            }
        }

        async fn web_directory_from_segments(
            &self,
            segments: &[String],
            create: bool,
        ) -> Result<Option<FileSystemDirectoryHandle>> {
            let mut directory = self.web_root.clone();
            let options = WebGetDirectoryOptions::new();
            options.set_create(create);
            for segment in segments {
                let promise = directory.get_directory_handle_with_options(segment, &options);
                directory = match JsFuture::from(promise).await {
                    Ok(directory) => directory
                        .dyn_into::<FileSystemDirectoryHandle>()
                        .map_err(|_| Error::unsupported_backend("browser OPFS directory handle"))?,
                    Err(error) if !create && is_opfs_not_found(&error) => return Ok(None),
                    Err(error) => {
                        return Err(map_js_value_error(&error, "open browser OPFS directory"));
                    }
                };
            }
            Ok(Some(directory))
        }

        async fn web_parent_directory_and_name(
            &self,
            path: &Path,
            create: bool,
        ) -> Result<Option<(FileSystemDirectoryHandle, String)>> {
            let mut segments = opfs_path_segments(path)?;
            let name = segments.pop().ok_or_else(|| {
                Error::invalid_options("browser persistent object path must include a file name")
            })?;
            let Some(directory) = self.web_directory_from_segments(&segments, create).await? else {
                return Ok(None);
            };
            Ok(Some((directory, name)))
        }

        async fn web_file_handle(
            &self,
            path: &Path,
            create: bool,
        ) -> Result<Option<FileSystemFileHandle>> {
            let Some((directory, name)) = self.web_parent_directory_and_name(path, create).await?
            else {
                return Ok(None);
            };
            let options = WebGetFileOptions::new();
            options.set_create(create);
            let promise = directory.get_file_handle_with_options(&name, &options);
            match JsFuture::from(promise).await {
                Ok(file) => file
                    .dyn_into::<FileSystemFileHandle>()
                    .map(Some)
                    .map_err(|_| Error::unsupported_backend("browser OPFS file handle")),
                Err(error) if !create && is_opfs_not_found(&error) => Ok(None),
                Err(error) => Err(map_js_value_error(&error, "open browser OPFS file")),
            }
        }

        async fn read_object_bytes_inner(
            &self,
            object: &StorageObjectId,
        ) -> Result<Option<Arc<[u8]>>> {
            Self::capabilities_for_browser().require(StorageCapability::ObjectRead)?;
            if browser_uses_sync_access_handles()? {
                return self.read_object_bytes_sync(object).await;
            }
            let Some(file) = self.file_handle(object.path(), false).await? else {
                return Ok(None);
            };
            let len = file.size().await.map_err(|error| map_opfs_error(&error))?;
            ensure_whole_object_read_len(object, len)?;
            let bytes = file.read().await.map_err(|error| map_opfs_error(&error))?;
            ensure_whole_object_read_len(object, bytes.len())?;
            Ok(Some(Arc::from(bytes)))
        }

        async fn read_object_bytes_sync(
            &self,
            object: &StorageObjectId,
        ) -> Result<Option<Arc<[u8]>>> {
            let Some(file) = self.web_file_handle(object.path(), false).await? else {
                return Ok(None);
            };
            let handle = BrowserSyncAccessHandle::open(&file, Some(object.clone())).await?;
            let len = handle.len()?;
            ensure_whole_object_read_len(object, len)?;
            let mut bytes = allocate_read_buffer(len)?;
            handle.read_exact_at(0, &mut bytes)?;
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
            let options = OpfsGetFileHandleOptions { create: true };
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

        async fn write_object_bytes_with_durability(
            &self,
            object: &StorageObjectId,
            bytes: &[u8],
            durability: DurabilityMode,
        ) -> Result<()> {
            if browser_uses_sync_access_handles()? {
                self.write_object_bytes_sync(object, bytes, durability)
                    .await
            } else {
                self.write_object_bytes(object, bytes).await
            }
        }

        async fn write_object_bytes_sync(
            &self,
            object: &StorageObjectId,
            bytes: &[u8],
            durability: DurabilityMode,
        ) -> Result<()> {
            let Some(file) = self.web_file_handle(object.path(), true).await? else {
                return Err(Error::invalid_options(
                    "browser persistent object path parent cannot be opened",
                ));
            };
            let handle = BrowserSyncAccessHandle::open(&file, Some(object.clone())).await?;
            handle.truncate(0)?;
            handle.write_all_at(0, bytes)?;
            handle.truncate(bytes.len())?;
            handle.flush_if_needed(durability)?;
            Ok(())
        }

        async fn append_object_bytes(&self, object: &StorageObjectId, bytes: &[u8]) -> Result<()> {
            require_browser_wal_object(object)?;
            let Some((directory, name)) =
                self.parent_directory_and_name(object.path(), true).await?
            else {
                return Err(Error::invalid_options(
                    "browser persistent WAL parent cannot be opened",
                ));
            };
            let options = OpfsGetFileHandleOptions { create: true };
            let mut file = directory
                .get_file_handle_with_options(&name, &options)
                .await
                .map_err(|error| map_opfs_error(&error))?;
            let len = file.size().await.map_err(|error| map_opfs_error(&error))?;
            let next_len = len.checked_add(bytes.len()).ok_or_else(|| {
                Error::invalid_options("browser persistent WAL append length overflow")
            })?;
            ensure_whole_object_read_len(object, next_len)?;
            let write_options = CreateWritableOptions {
                keep_existing_data: true,
            };
            let mut stream = file
                .create_writable_with_options(&write_options)
                .await
                .map_err(|error| map_opfs_error(&error))?;
            stream
                .seek(len)
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

        async fn append_object_bytes_with_durability(
            &self,
            object: &StorageObjectId,
            bytes: &[u8],
            durability: DurabilityMode,
        ) -> Result<()> {
            if browser_uses_sync_access_handles()? {
                self.append_object_bytes_sync(object, bytes, durability)
                    .await
            } else {
                self.append_object_bytes(object, bytes).await
            }
        }

        async fn append_object_bytes_sync(
            &self,
            object: &StorageObjectId,
            bytes: &[u8],
            durability: DurabilityMode,
        ) -> Result<()> {
            require_browser_wal_object(object)?;
            let Some(file) = self.web_file_handle(object.path(), true).await? else {
                return Err(Error::invalid_options(
                    "browser persistent WAL parent cannot be opened",
                ));
            };
            let handle = BrowserSyncAccessHandle::open(&file, Some(object.clone())).await?;
            let len = handle.len()?;
            let next_len = len.checked_add(bytes.len()).ok_or_else(|| {
                Error::invalid_options("browser persistent WAL append length overflow")
            })?;
            ensure_whole_object_read_len(object, next_len)?;
            handle.write_all_at(len, bytes)?;
            handle.flush_if_needed(durability)?;
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
                let file = if browser_uses_sync_access_handles()? {
                    let Some(file) = self.web_file_handle(object.path(), false).await? else {
                        return Err(referenced_browser_object_missing(&object));
                    };
                    BrowserStorageObjectFile::Sync(file)
                } else {
                    let Some(file) = self.file_handle(object.path(), false).await? else {
                        return Err(referenced_browser_object_missing(&object));
                    };
                    BrowserStorageObjectFile::Async(file)
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
                // FileSystemWritableFileStream stages changes and exposes them
                // only when close succeeds. A SyncAccessHandle truncates the
                // live file in place, so it must never implement an atomic
                // publication point even when the worker supports it.
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
                self.write_object_bytes_with_durability(&object, &bytes, durability)
                    .await
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
                // Both writes use WritableFileStream's close-commit boundary;
                // direct SyncAccessHandle truncation is not crash-atomic.
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
        file: BrowserStorageObjectFile,
    }

    #[derive(Debug, Clone)]
    enum BrowserStorageObjectFile {
        Async(FileHandle),
        Sync(FileSystemFileHandle),
    }

    impl StorageReadObject for BrowserStorageObject {
        fn object(&self) -> &StorageObjectId {
            &self.object
        }

        fn len(&self) -> StorageReadFuture<'_, u64> {
            Box::pin(async move {
                match &self.file {
                    BrowserStorageObjectFile::Async(file) => {
                        let len = file.size().await.map_err(|error| map_opfs_error(&error))?;
                        usize_to_u64(len, "browser persistent object length")
                    }
                    BrowserStorageObjectFile::Sync(file) => {
                        let handle =
                            BrowserSyncAccessHandle::open(file, Some(self.object.clone())).await?;
                        usize_to_u64(handle.len()?, "browser persistent object length")
                    }
                }
            })
        }

        fn read_exact_at<'op>(
            &'op self,
            offset: usize,
            bytes: &'op mut [u8],
        ) -> StorageReadFuture<'op, ()> {
            Box::pin(async move {
                match &self.file {
                    BrowserStorageObjectFile::Async(file) => {
                        let end = offset.checked_add(bytes.len()).ok_or_else(|| {
                            Error::invalid_options("browser persistent object read offset overflow")
                        })?;
                        let read = file
                            .read_range(offset..end)
                            .await
                            .map_err(|error| map_opfs_error(&error))?;
                        if read.len() != bytes.len() {
                            return Err(short_browser_read(&self.object));
                        }
                        bytes.copy_from_slice(&read);
                    }
                    BrowserStorageObjectFile::Sync(file) => {
                        let handle =
                            BrowserSyncAccessHandle::open(file, Some(self.object.clone())).await?;
                        handle.read_exact_at(offset, bytes)?;
                    }
                }
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
                self.backend
                    .append_object_bytes_with_durability(&self.object, bytes, durability)
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

    fn referenced_browser_object_missing(object: &StorageObjectId) -> Error {
        Error::Corruption {
            message: format!(
                "referenced browser persistent {} {} cannot be opened",
                object.kind().as_str(),
                object.path().display()
            ),
        }
    }

    fn short_browser_read(object: &StorageObjectId) -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "referenced browser persistent {} {} short read",
                object.kind().as_str(),
                object.path().display()
            ),
        ))
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
            | StorageObjectKind::ContentAccessBarrier
            | StorageObjectKind::ContentChunk
            | StorageObjectKind::ContentDescriptor
            | StorageObjectKind::ContentUpload
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

    async fn browser_opfs_roots() -> Result<(DirectoryHandle, FileSystemDirectoryHandle)> {
        let navigator = Reflect::get(&js_sys::global(), &JsValue::from_str("navigator"))
            .map_err(|error| map_js_value_error(&error, "read browser navigator"))?;
        if navigator.is_null() || navigator.is_undefined() {
            return Err(Error::unsupported_backend("browser navigator"));
        }

        let storage = Reflect::get(&navigator, &JsValue::from_str("storage"))
            .map_err(|error| map_js_value_error(&error, "read browser storage manager"))?;
        if storage.is_null() || storage.is_undefined() {
            return Err(Error::unsupported_backend("browser storage manager"));
        }

        let get_directory = Reflect::get(&storage, &JsValue::from_str("getDirectory"))
            .map_err(|error| map_js_value_error(&error, "read browser OPFS root function"))?
            .dyn_into::<Function>()
            .map_err(|_| Error::unsupported_backend("browser OPFS root function"))?;
        let promise = get_directory
            .call0(&storage)
            .map_err(|error| map_js_value_error(&error, "request browser OPFS root"))?
            .dyn_into::<Promise>()
            .map_err(|_| Error::unsupported_backend("browser OPFS root promise"))?;
        let root = JsFuture::from(promise)
            .await
            .map_err(|error| map_js_value_error(&error, "await browser OPFS root"))?
            .dyn_into::<FileSystemDirectoryHandle>()
            .map_err(|_| Error::unsupported_backend("browser OPFS root handle"))?;
        Ok((DirectoryHandle::from(root.clone()), root))
    }

    fn browser_web_locks_available() -> bool {
        browser_lock_manager().is_ok()
    }

    fn browser_uses_sync_access_handles() -> Result<bool> {
        if !browser_dedicated_worker_global_available() {
            return Ok(false);
        }
        if browser_sync_access_handle_api_available() {
            return Ok(true);
        }
        Err(Error::unsupported_backend(
            "browser OPFS sync access handle in DedicatedWorker",
        ))
    }

    fn browser_dedicated_worker_global_available() -> bool {
        js_sys::global()
            .dyn_ref::<DedicatedWorkerGlobalScope>()
            .is_some()
    }

    fn browser_sync_access_handle_api_available() -> bool {
        let Ok(file_handle_ctor) = Reflect::get(
            &js_sys::global(),
            &JsValue::from_str("FileSystemFileHandle"),
        ) else {
            return false;
        };
        if file_handle_ctor.is_null() || file_handle_ctor.is_undefined() {
            return false;
        }
        let Ok(prototype) = Reflect::get(&file_handle_ctor, &JsValue::from_str("prototype")) else {
            return false;
        };
        let Ok(create_sync_access_handle) =
            Reflect::get(&prototype, &JsValue::from_str("createSyncAccessHandle"))
        else {
            return false;
        };
        create_sync_access_handle.dyn_ref::<Function>().is_some()
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

    struct BrowserSyncAccessHandle {
        object: Option<StorageObjectId>,
        handle: FileSystemSyncAccessHandle,
    }

    impl BrowserSyncAccessHandle {
        async fn open(
            file: &FileSystemFileHandle,
            object: Option<StorageObjectId>,
        ) -> Result<Self> {
            let handle = JsFuture::from(file.create_sync_access_handle())
                .await
                .map_err(|error| map_sync_access_open_error(&error))?
                .dyn_into::<FileSystemSyncAccessHandle>()
                .map_err(|_| Error::unsupported_backend("browser OPFS sync access handle"))?;
            Ok(Self { object, handle })
        }

        fn len(&self) -> Result<usize> {
            js_number_to_usize(
                self.handle
                    .get_size()
                    .map_err(|error| map_js_value_error(&error, "read browser OPFS file size"))?,
                "browser OPFS file size",
            )
        }

        fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
            let options = FileSystemReadWriteOptions::new();
            options.set_at(usize_to_js_number(offset, "browser OPFS read offset")?);
            let read = self
                .handle
                .read_with_u8_array_and_options(bytes, &options)
                .map_err(|error| map_js_value_error(&error, "read browser OPFS sync handle"))?;
            let read = js_number_to_usize(read, "browser OPFS read length")?;
            if read == bytes.len() {
                return Ok(());
            }
            if let Some(object) = &self.object {
                return Err(short_browser_read(object));
            }
            Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "browser OPFS sync handle short read",
            )))
        }

        fn write_all_at(&self, offset: usize, bytes: &[u8]) -> Result<()> {
            let options = FileSystemReadWriteOptions::new();
            options.set_at(usize_to_js_number(offset, "browser OPFS write offset")?);
            let written = self
                .handle
                .write_with_u8_array_and_options(bytes, &options)
                .map_err(|error| map_js_value_error(&error, "write browser OPFS sync handle"))?;
            let written = js_number_to_usize(written, "browser OPFS write length")?;
            if written == bytes.len() {
                return Ok(());
            }
            Err(Error::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "browser OPFS sync handle short write",
            )))
        }

        fn truncate(&self, len: usize) -> Result<()> {
            self.handle
                .truncate_with_f64(usize_to_js_number(len, "browser OPFS truncate length")?)
                .map_err(|error| map_js_value_error(&error, "truncate browser OPFS sync handle"))
        }

        fn flush_if_needed(&self, durability: DurabilityMode) -> Result<()> {
            require_browser_durability(durability)?;
            if durability == DurabilityMode::Flush {
                self.handle.flush().map_err(|error| {
                    map_js_value_error(&error, "flush browser OPFS sync handle")
                })?;
            }
            Ok(())
        }
    }

    impl Drop for BrowserSyncAccessHandle {
        fn drop(&mut self) {
            self.handle.close();
        }
    }

    fn map_sync_access_open_error(error: &JsValue) -> Error {
        let name = js_value_property(error, "name");
        if matches!(
            name.as_deref(),
            Some("InvalidStateError" | "NoModificationAllowedError")
        ) {
            return Error::runtime_busy("browser OPFS sync access handle is already open");
        }
        if matches!(
            name.as_deref(),
            Some("NotAllowedError" | "NotSupportedError" | "SecurityError" | "TypeError")
        ) {
            return Error::unsupported_backend("browser OPFS sync access handle");
        }
        map_js_value_error(error, "open browser OPFS sync access handle")
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn js_number_to_usize(value: f64, context: &'static str) -> Result<usize> {
        if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > usize::MAX as f64 {
            return Err(Error::Io(io::Error::other(format!(
                "{context} was out of range"
            ))));
        }
        Ok(value as usize)
    }

    #[allow(clippy::cast_precision_loss)]
    fn usize_to_js_number(value: usize, context: &'static str) -> Result<f64> {
        const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
        let value = u64::try_from(value).map_err(|_| {
            Error::invalid_options(format!("{context} exceeds JavaScript safe integer range"))
        })?;
        if value > MAX_SAFE_INTEGER {
            return Err(Error::invalid_options(format!(
                "{context} exceeds JavaScript safe integer range"
            )));
        }
        Ok(value as f64)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(unused_imports)]
pub(crate) use browser_persistent_storage::{BrowserStorageBackend, BrowserWriterLease};
