use super::*;

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
                        path, tmp_path, bytes, durability, true, true,
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
