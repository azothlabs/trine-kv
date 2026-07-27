use super::{
    Arc, BTreeMap, DEFAULT_BUCKET_NAME, Db, DbInnerParts, DbOptions, Error, LsmTree, ManifestStore,
    NativeFileBackend, NativeInnerParts, PersistentOpenParts, Result, Runtime, StorageMode,
    WalFrontDoor, acquire_persistent_process_lock, acquire_persistent_process_lock_async,
    buckets_from_manifest, buckets_from_manifest_async, create_storage_directory_all,
    create_storage_directory_all_async, ensure_default_bucket_in_manifest,
    ensure_default_bucket_in_manifest_async, ensure_default_bucket_loaded,
    list_persistent_directory_files, list_persistent_directory_files_async, manifest,
    persistent_path_from_options, recovery, repair_safe_temporary_files_for_open,
    repair_safe_temporary_files_for_open_from_directory_files_async,
    run_persistent_recovery_checks, run_persistent_recovery_checks_from_directory_files_async,
    validate_options, wal,
};

impl Db {
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    pub(in crate::db) fn memory_sync(mut options: DbOptions) -> Result<Self> {
        options.storage_mode = StorageMode::InMemory;
        validate_options(&options)?;
        let runtime = Runtime::new(options.runtime);
        let default_bucket = Arc::new(LsmTree::new(
            options.default_bucket_options.clone(),
            Vec::new(),
        )?);
        let mut buckets = BTreeMap::new();
        buckets.insert(DEFAULT_BUCKET_NAME.to_owned(), default_bucket);

        Ok(Self::from_inner_parts(DbInnerParts::memory(
            options, buckets, runtime,
        )))
    }

    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    pub(in crate::db) fn open_persistent_with_options(options: DbOptions) -> Result<Self> {
        validate_options(&options)?;
        let runtime = Runtime::new(options.runtime);
        let native_storage = NativeFileBackend::with_runtime(runtime.clone())?;
        let Some(path) = persistent_path_from_options(&options) else {
            return Err(Error::invalid_options("persistent open requires a path"));
        };
        let db_path_for_cleanup = path.to_path_buf();
        let path = db_path_for_cleanup.as_path();

        if path.exists() {
            if !path.is_dir() {
                return Err(Error::invalid_options("database path is not a directory"));
            }
        } else if options.create_if_missing && !options.read_only {
            create_storage_directory_all(&native_storage, path)?;
        } else {
            return Err(Error::invalid_options("database path does not exist"));
        }

        let (process_lock, directory_files) =
            Self::prepare_persistent_lock_and_directory_files(&native_storage, path, &options)?;

        let manifest_path = manifest::manifest_path(path);
        let mut manifest = ManifestStore::open_or_create_with_backend(
            manifest_path,
            options.create_if_missing && !options.read_only,
            native_storage.clone(),
        )?;
        ensure_default_bucket_in_manifest(&mut manifest, &options)?;
        let replay_floor = manifest.state().wal_replay_floor();
        let mut buckets = buckets_from_manifest(&native_storage, path, manifest.state())?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;
        run_persistent_recovery_checks(&native_storage, path, manifest.state(), &directory_files)?;

        let wal_paths = wal::discover_wal_paths_from_directory_entries(
            directory_files.iter().map(|file| file.path().to_path_buf()),
        )?;
        let wal_streams = if options.read_only
            && wal::discovered_wal_paths_are_empty_with_backend(
                &native_storage,
                &wal_paths,
                &directory_files,
            )? {
            Vec::new()
        } else {
            wal::read_recovery_streams_after_paths_with_backend(
                &native_storage,
                &wal_paths,
                replay_floor,
            )?
        };
        let batches = wal::merge_batch_streams_by_sequence(wal_streams)?;
        let wal = if options.read_only {
            None
        } else {
            Some(WalFrontDoor::open_sharded_with_discovered_paths(
                &native_storage,
                path,
                options
                    .wal_shards
                    .resolve(&options.storage_mode, options.durability),
                wal_paths,
            )?)
        };

        Self::from_persistent_open_parts(PersistentOpenParts {
            options,
            runtime,
            native_storage,
            process_lock,
            buckets,
            manifest,
            wal,
            batches,
            replay_floor,
            db_path_for_cleanup,
        })
    }

    pub(in crate::db) async fn open_persistent_with_options_async(
        options: DbOptions,
    ) -> Result<Self> {
        Self::open_persistent_with_options_async_inner(options, true).await
    }

    pub(in crate::db) async fn open_persistent_with_options_async_inner(
        options: DbOptions,
        require_wait_support: bool,
    ) -> Result<Self> {
        validate_options(&options)?;
        if require_wait_support && !options.runtime.capabilities().blocking_adapter() {
            return Err(Error::unsupported("runtime sync adapter"));
        }

        let runtime = Runtime::new(options.runtime);
        let native_storage = NativeFileBackend::with_runtime(runtime.clone())?;
        let Some(path) = persistent_path_from_options(&options) else {
            return Err(Error::invalid_options("persistent open requires a path"));
        };
        let db_path_for_cleanup = path.to_path_buf();
        let path = db_path_for_cleanup.as_path();

        if path.exists() {
            if !path.is_dir() {
                return Err(Error::invalid_options("database path is not a directory"));
            }
        } else if options.create_if_missing && !options.read_only {
            create_storage_directory_all_async(&native_storage, path).await?;
        } else {
            return Err(Error::invalid_options("database path does not exist"));
        }

        let (process_lock, directory_files) =
            Self::prepare_persistent_lock_and_directory_files_async(
                &native_storage,
                path,
                &options,
            )
            .await?;

        let manifest_path = manifest::manifest_path(path);
        let mut manifest = ManifestStore::open_or_create_with_backend_async(
            manifest_path,
            options.create_if_missing && !options.read_only,
            native_storage.clone(),
        )
        .await?;
        ensure_default_bucket_in_manifest_async(&mut manifest, &options).await?;
        let replay_floor = manifest.state().wal_replay_floor();
        let mut buckets =
            buckets_from_manifest_async(&native_storage, path, manifest.state(), false).await?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;
        run_persistent_recovery_checks_from_directory_files_async(
            &native_storage,
            path,
            manifest.state(),
            &directory_files,
        )
        .await?;

        let wal_paths = wal::discover_wal_paths_from_directory_entries(
            directory_files.iter().map(|file| file.path().to_path_buf()),
        )?;
        let wal_streams = if options.read_only
            && wal::discovered_wal_paths_are_empty_with_backend_async(
                &native_storage,
                &wal_paths,
                &directory_files,
            )
            .await?
        {
            Vec::new()
        } else {
            wal::read_recovery_streams_after_paths_with_backend_async(
                &native_storage,
                &wal_paths,
                replay_floor,
            )
            .await?
        };
        let batches = wal::merge_batch_streams_by_sequence(wal_streams)?;
        let wal = if options.read_only {
            None
        } else {
            Some(WalFrontDoor::open_sharded_with_discovered_paths(
                &native_storage,
                path,
                options
                    .wal_shards
                    .resolve(&options.storage_mode, options.durability),
                wal_paths,
            )?)
        };

        Self::from_persistent_open_parts(PersistentOpenParts {
            options,
            runtime,
            native_storage,
            process_lock,
            buckets,
            manifest,
            wal,
            batches,
            replay_floor,
            db_path_for_cleanup,
        })
    }

    fn prepare_persistent_lock_and_directory_files(
        native_storage: &NativeFileBackend,
        path: &std::path::Path,
        options: &DbOptions,
    ) -> Result<(
        Option<recovery::ProcessLock>,
        Vec<crate::storage::StorageDirectoryFile>,
    )> {
        #[cfg(target_os = "wasi")]
        {
            let directory_files = list_persistent_directory_files(native_storage, path)?;
            repair_safe_temporary_files_for_open(native_storage, path, options, &directory_files)?;
            let directory_files = list_persistent_directory_files(native_storage, path)?;
            let process_lock = acquire_persistent_process_lock(native_storage, path, options)?;
            Ok((process_lock, directory_files))
        }

        #[cfg(not(target_os = "wasi"))]
        {
            let process_lock = acquire_persistent_process_lock(native_storage, path, options)?;
            let directory_files = list_persistent_directory_files(native_storage, path)?;
            repair_safe_temporary_files_for_open(native_storage, path, options, &directory_files)?;
            Ok((process_lock, directory_files))
        }
    }

    async fn prepare_persistent_lock_and_directory_files_async(
        native_storage: &NativeFileBackend,
        path: &std::path::Path,
        options: &DbOptions,
    ) -> Result<(
        Option<recovery::ProcessLock>,
        Vec<crate::storage::StorageDirectoryFile>,
    )> {
        #[cfg(target_os = "wasi")]
        {
            let directory_files =
                list_persistent_directory_files_async(native_storage, path).await?;
            repair_safe_temporary_files_for_open_from_directory_files_async(
                native_storage,
                path,
                options,
                &directory_files,
            )
            .await?;
            let directory_files =
                list_persistent_directory_files_async(native_storage, path).await?;
            let process_lock =
                acquire_persistent_process_lock_async(native_storage, path, options).await?;
            Ok((process_lock, directory_files))
        }

        #[cfg(not(target_os = "wasi"))]
        {
            let process_lock =
                acquire_persistent_process_lock_async(native_storage, path, options).await?;
            let directory_files =
                list_persistent_directory_files_async(native_storage, path).await?;
            repair_safe_temporary_files_for_open_from_directory_files_async(
                native_storage,
                path,
                options,
                &directory_files,
            )
            .await?;
            Ok((process_lock, directory_files))
        }
    }

    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    pub(in crate::db) fn from_persistent_open_parts(parts: PersistentOpenParts) -> Result<Self> {
        let PersistentOpenParts {
            options,
            runtime,
            native_storage,
            process_lock,
            buckets,
            manifest,
            wal,
            batches,
            replay_floor,
            db_path_for_cleanup,
        } = parts;
        let db = Self::from_inner_parts(DbInnerParts::native(
            options,
            buckets,
            NativeInnerParts {
                manifest,
                storage: native_storage,
                root: db_path_for_cleanup.clone(),
                wal,
                process_lock,
            },
            runtime,
        ));
        db.replay_wal_batches(batches, replay_floor)?;
        if !db.inner.options.read_only {
            db.cleanup_pending_obsolete_blob_files(&db_path_for_cleanup)?;
        }
        db.start_background_workers()?;

        Ok(db)
    }
}
