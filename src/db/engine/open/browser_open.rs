#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use super::{
    BrowserInnerParts, BrowserStorageBackend, BrowserWalFrontDoor, DbInnerParts,
    HostStorageBackend, ManifestStore, Runtime, StorageMode, StorageObjectId, StorageObjectKind,
    StorageWriterLeaseBackend, buckets_from_manifest_async,
    ensure_default_bucket_in_manifest_async, ensure_default_bucket_loaded, manifest, recovery,
    run_persistent_recovery_checks_async, wal,
};
use super::{Db, DbOptions, Error, Result};

impl Db {
    #[allow(clippy::unused_async)]
    pub(in crate::db) async fn open_browser_persistent_with_options_async(
        options: DbOptions,
    ) -> Result<Self> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Self::open_browser_persistent_with_options_async_inner(options).await
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            drop(options);
            Err(Error::unsupported_backend(
                "browser persistent storage backend",
            ))
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(clippy::arc_with_non_send_sync, clippy::too_many_lines)]
    pub(in crate::db) async fn open_browser_persistent_with_options_async_inner(
        mut options: DbOptions,
    ) -> Result<Self> {
        Self::validate_browser_persistent_options(&options)?;
        let storage = BrowserStorageBackend::new().await?;
        let db_path = options
            .storage_mode
            .browser_path()
            .ok_or_else(|| Error::invalid_options("browser persistent open requires a path"))?;
        let db_path = BrowserStorageBackend::normalize_namespace_path(db_path)?;
        if let StorageMode::HostPersistent {
            backend: HostStorageBackend::Browser { path },
        } = &mut options.storage_mode
        {
            path.clone_from(&db_path);
        }
        let manifest_path = manifest::manifest_path(&db_path);
        let writer_lease = if options.read_only {
            None
        } else {
            storage
                .acquire_writer_lease(StorageObjectId::native_file(
                    StorageObjectKind::WriterLease,
                    db_path.join(recovery::PROCESS_LOCK_FILE_NAME),
                ))
                .await
                .map(Some)?
        };
        if options.read_only {
            recovery::fail_on_safe_temporary_files_with_backend_async(&storage, &db_path).await?;
        } else {
            recovery::repair_safe_temporary_files_with_backend_async(
                &storage,
                &db_path,
                options.fail_on_corruption,
            )
            .await?;
        }

        let mut manifest = ManifestStore::open_or_create_with_browser_backend_async(
            manifest_path,
            options.create_if_missing && !options.read_only,
            storage.clone(),
        )
        .await?;
        ensure_default_bucket_in_manifest_async(&mut manifest, &options).await?;
        let replay_floor = manifest.state().wal_replay_floor();

        run_persistent_recovery_checks_async(&storage, &db_path, manifest.state()).await?;
        let mut buckets =
            buckets_from_manifest_async(&storage, &db_path, manifest.state(), true).await?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;

        let wal_streams =
            wal::read_recovery_streams_after_with_backend_async(&storage, &db_path, replay_floor)
                .await?;
        let batches = wal::merge_batch_streams_by_sequence(wal_streams)?;
        let browser_wal = if options.read_only {
            None
        } else {
            Some(
                BrowserWalFrontDoor::open_sharded_with_backend(
                    &storage,
                    &db_path,
                    options
                        .wal_shards
                        .resolve(&options.storage_mode, options.durability),
                )
                .await?,
            )
        };
        let runtime = Runtime::new(options.runtime);
        let db = Self::from_inner_parts(DbInnerParts::browser(
            options,
            buckets,
            BrowserInnerParts {
                manifest,
                storage,
                root: db_path,
                writer_lease,
                wal: browser_wal,
            },
            runtime,
        ));
        db.replay_wal_batches(batches, replay_floor)?;
        Ok(db)
    }
}
