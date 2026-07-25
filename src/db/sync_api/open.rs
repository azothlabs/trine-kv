use super::{
    Arc, AtomicBool, AtomicU64, AtomicUsize, BTreeMap, BlobReadMetrics, CancellationToken,
    CommitTracker, DEFAULT_BUCKET_NAME, Db, DbInner, DbOptions, DurabilityMode,
    DurabilitySubstrate, Error, FilesystemSubstrate, HostStorageBackend, IntoOpenOptions, LsmTree,
    MaintenanceCoordinator, ManifestStore, Mutex, NativeFileBackend, ObjectClient,
    ObjectClientTrustMode, ObjectLeaseState, ObjectStoreBackend, ObjectStoreSubstrate,
    ObjectWriterLease, PathBuf, PersistentOpenParts, PublishBarrier, Result, Runtime, RwLock,
    ScanWasteMetrics, Sequence, SnapshotTracker, StorageMode, WalFrontDoor,
    acquire_persistent_process_lock, acquire_persistent_process_lock_async, buckets_from_manifest,
    buckets_from_manifest_async, cache, create_storage_directory_all,
    create_storage_directory_all_async, ensure_default_bucket_in_manifest,
    ensure_default_bucket_in_manifest_async, ensure_default_bucket_loaded,
    list_persistent_directory_files, list_persistent_directory_files_async, manifest,
    object_store_committed_wal_batches, persistent_path_from_options, recovery,
    repair_safe_temporary_files_for_open,
    repair_safe_temporary_files_for_open_from_directory_files_async,
    run_persistent_recovery_checks, run_persistent_recovery_checks_from_directory_files_async,
    runtime, validate_common_options, validate_options, verify_object_client_contract_for_open,
    wal,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::db::open_helpers::run_persistent_recovery_checks_async;
use crate::object_store::{canonical_object_key, canonical_object_prefix};
use crate::options::ContentReclamationMode;
use crate::storage::MemoryStorageBackend;
use crate::substrate::object_store_wal_batches_after_replay_floor;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::{
    storage::{
        BrowserStorageBackend, BrowserWriterLease, StorageObjectId, StorageObjectKind,
        StorageWriterLeaseBackend,
    },
    wal::BrowserWalFrontDoor,
};
use std::collections::hash_map::RandomState;
#[derive(Debug)]
struct DbInnerParts {
    options: DbOptions,
    buckets: BTreeMap<String, Arc<LsmTree>>,
    manifest: Option<ManifestStore>,
    substrate: DurabilitySubstrate,
    native_storage: NativeFileBackend,
    object_storage: Option<ObjectStoreBackend>,
    object_wal_storage: Option<ObjectStoreBackend>,
    object_storage_prefix: PathBuf,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_storage: Option<BrowserStorageBackend>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_writer_lease: Option<BrowserWriterLease>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_wal: Option<BrowserWalFrontDoor>,
    runtime: Runtime,
}

#[derive(Debug)]
struct ObjectStoreInnerParts {
    substrate: DurabilitySubstrate,
    storage: ObjectStoreBackend,
    wal_storage: ObjectStoreBackend,
    prefix: PathBuf,
}

impl DbInnerParts {
    fn base(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        manifest: Option<ManifestStore>,
        substrate: DurabilitySubstrate,
        native_storage: NativeFileBackend,
        runtime: Runtime,
    ) -> Self {
        Self {
            options,
            buckets,
            manifest,
            substrate,
            native_storage,
            object_storage: None,
            object_wal_storage: None,
            object_storage_prefix: PathBuf::new(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            browser_storage: None,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            browser_writer_lease: None,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            browser_wal: None,
            runtime,
        }
    }

    fn memory(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        runtime: Runtime,
    ) -> Self {
        Self::base(
            options,
            buckets,
            None,
            DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None)),
            NativeFileBackend::new(),
            runtime,
        )
    }

    fn native(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        manifest: ManifestStore,
        native_storage: NativeFileBackend,
        wal: Option<WalFrontDoor>,
        process_lock: Option<recovery::ProcessLock>,
        runtime: Runtime,
    ) -> Self {
        Self::base(
            options,
            buckets,
            Some(manifest),
            DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(wal, process_lock)),
            native_storage,
            runtime,
        )
    }

    fn object_store(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        manifest: ManifestStore,
        backend: ObjectStoreInnerParts,
        runtime: Runtime,
    ) -> Self {
        Self {
            object_storage: Some(backend.storage),
            object_wal_storage: Some(backend.wal_storage),
            object_storage_prefix: backend.prefix,
            ..Self::base(
                options,
                buckets,
                Some(manifest),
                backend.substrate,
                NativeFileBackend::new(),
                runtime,
            )
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    fn browser(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        manifest: ManifestStore,
        storage: BrowserStorageBackend,
        writer_lease: Option<BrowserWriterLease>,
        wal: Option<BrowserWalFrontDoor>,
        runtime: Runtime,
    ) -> Self {
        Self {
            browser_storage: Some(storage),
            browser_writer_lease: writer_lease,
            browser_wal: wal,
            ..Self::base(
                options,
                buckets,
                Some(manifest),
                DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None)),
                NativeFileBackend::new(),
                runtime,
            )
        }
    }
}

impl Db {
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    fn from_inner_parts(parts: DbInnerParts) -> Self {
        let DbInnerParts {
            options,
            buckets,
            manifest,
            substrate,
            native_storage,
            object_storage,
            object_wal_storage,
            object_storage_prefix,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            browser_storage,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            browser_writer_lease,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            browser_wal,
            runtime,
        } = parts;
        let block_cache_bytes = options.block_cache_bytes;

        Self {
            inner: Arc::new(DbInner {
                options,
                user_handles: AtomicUsize::new(1),
                commit_tracker: CommitTracker::new(Sequence::ZERO),
                closed: AtomicBool::new(false),
                publish_barrier: PublishBarrier::new(),
                object_wal_commit_order: Mutex::new(()),
                memtable_publish_lock: Mutex::new(()),
                buckets: RwLock::new(buckets),
                snapshots: Arc::new(SnapshotTracker::default()),
                checkpoints: Mutex::new(BTreeMap::new()),
                pending_obsolete_tables: Mutex::new(Vec::new()),
                manifest: manifest.map(Mutex::new),
                substrate,
                block_cache: Arc::new(cache::BlockCache::new(block_cache_bytes)),
                compaction_runs: AtomicU64::new(0),
                compaction_input_tables: AtomicU64::new(0),
                compaction_output_tables: AtomicU64::new(0),
                compaction_input_bytes: AtomicU64::new(0),
                compaction_output_bytes: AtomicU64::new(0),
                compaction_level_stats: Mutex::new(BTreeMap::new()),
                compaction_trigger_stats: Mutex::new(BTreeMap::new()),
                compaction_skip_stats: Mutex::new(BTreeMap::new()),
                blob_gc_runs: AtomicU64::new(0),
                blob_gc_input_bytes: AtomicU64::new(0),
                blob_gc_output_bytes: AtomicU64::new(0),
                blob_gc_discarded_bytes: AtomicU64::new(0),
                blob_reads: Arc::new(BlobReadMetrics::default()),
                scan_waste: Arc::new(ScanWasteMetrics::default()),
                maintenance_cooperative_yields: AtomicU64::new(0),
                maintenance_budget_exhaustions: AtomicU64::new(0),
                native_storage,
                content_memory: MemoryStorageBackend::new(),
                content_seal_lock: futures::lock::Mutex::new(()),
                content_access_lock: futures::lock::Mutex::new(()),
                content_lock_hasher: RandomState::new(),
                content_quota_locks: std::array::from_fn(|_| futures::lock::Mutex::new(())),
                content_upload_locks: std::array::from_fn(|_| futures::lock::Mutex::new(())),
                object_storage,
                object_wal_storage,
                object_storage_prefix,
                object_manifest_async_lock: futures::lock::Mutex::new(()),
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_storage,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_writer_lease: Mutex::new(browser_writer_lease),
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_wal,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_manifest_async_lock: futures::lock::Mutex::new(()),
                runtime,
                runtime_shutdown: CancellationToken::new(),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
                background_workers: Mutex::new(Vec::new()),
            }),
            counts_as_user_handle: true,
        }
    }

    /// Opens a database synchronously.
    ///
    /// The `options` argument accepts either [`DbOptions`] or any path-like
    /// value such as `&str`, `Path`, or `PathBuf`. Path-like input is converted
    /// with [`DbOptions::new`], so it opens a persistent native-filesystem
    /// database and creates it when missing. Use [`DbOptions::memory`] for a
    /// temporary in-memory database.
    ///
    /// Opening performs startup recovery checks before returning. Persistent
    /// opens acquire the writer lease when the selected backend supports it,
    /// load the current manifest, replay accepted WAL records, and rebuild the
    /// in-memory bucket state. Browser persistence is only available through
    /// the async [`Db::open`] path on browser WASM targets.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] for invalid paths or option
    /// combinations, [`Error::UnsupportedBackend`] when the selected host
    /// backend is unavailable on this target, [`Error::Corruption`] when
    /// recovery detects unsafe durable state, or [`Error::Io`] for storage
    /// failures.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// let persistent = Db::open_sync("target/doc-example-open-sync")?;
    /// persistent.put_sync(b"k", b"v")?;
    ///
    /// let memory = Db::open_sync(DbOptions::memory())?;
    /// memory.put_sync(b"session", b"only in memory")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_sync(options: impl IntoOpenOptions) -> Result<Self> {
        let options = options.into_open_options();
        match &options.storage_mode {
            StorageMode::InMemory => Self::memory_sync(options),
            StorageMode::Persistent { .. } => Self::open_persistent_with_options(options),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => Self::open_wasi_persistent_with_options(options),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser persistent storage backend",
            )),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => Err(Error::unsupported_backend(
                "object-store databases are async-only; use the async open with a client",
            )),
        }
    }

    pub(in crate::db) fn open_wasi_persistent_with_options(options: DbOptions) -> Result<Self> {
        let StorageMode::HostPersistent {
            backend: HostStorageBackend::Wasi { path },
        } = &options.storage_mode
        else {
            return Err(Error::invalid_options(
                "WASI persistent open requires a path",
            ));
        };
        let path = path.clone();
        if path.as_os_str().is_empty() {
            return Err(Error::invalid_options(
                "WASI persistent path must be non-empty",
            ));
        }

        #[cfg(target_os = "wasi")]
        {
            Self::validate_wasi_persistent_options(&options)?;
            Self::open_persistent_with_options(options)
        }

        #[cfg(not(target_os = "wasi"))]
        {
            let _ = path;
            drop(options);
            Err(Error::unsupported_backend(
                "WASI persistent storage backend",
            ))
        }
    }

    #[cfg_attr(not(target_os = "wasi"), allow(clippy::unused_async))]
    pub(in crate::db) async fn open_wasi_persistent_with_options_async(
        options: DbOptions,
    ) -> Result<Self> {
        let StorageMode::HostPersistent {
            backend: HostStorageBackend::Wasi { path },
        } = &options.storage_mode
        else {
            return Err(Error::invalid_options(
                "WASI persistent open requires a path",
            ));
        };
        let path = path.clone();
        if path.as_os_str().is_empty() {
            return Err(Error::invalid_options(
                "WASI persistent path must be non-empty",
            ));
        }

        #[cfg(target_os = "wasi")]
        {
            Self::validate_wasi_persistent_options(&options)?;
            Self::open_persistent_with_options_async_inner(options, false).await
        }

        #[cfg(not(target_os = "wasi"))]
        {
            let _ = path;
            drop(options);
            Err(Error::unsupported_backend(
                "WASI persistent storage backend",
            ))
        }
    }

    #[cfg(target_os = "wasi")]
    pub(in crate::db) fn validate_wasi_persistent_options(options: &DbOptions) -> Result<()> {
        if options.runtime.mode != runtime::RuntimeMode::Inline {
            return Err(Error::invalid_options(
                "WASI persistent backend requires inline runtime",
            ));
        }
        if options.background_worker_count != 0 {
            return Err(Error::invalid_options(
                "WASI persistent backend does not support background workers yet",
            ));
        }
        if matches!(
            options.durability,
            DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
        ) {
            return Err(Error::unsupported_durability(options.durability));
        }
        if options
            .wal_shards
            .resolve(&options.storage_mode, options.durability)
            != 1
        {
            return Err(Error::invalid_options(
                "WASI persistent backend supports one WAL lane",
            ));
        }
        Ok(())
    }

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
            manifest,
            storage,
            writer_lease,
            browser_wal,
            runtime,
        ));
        db.replay_wal_batches(batches, replay_floor)?;
        Ok(db)
    }

    /// Opens an object-storage database at the bucket root (async-only).
    ///
    /// Equivalent to [`Db::open_object_store_at`] with an empty key prefix. Use
    /// the prefixed form to host several databases in one bucket.
    ///
    /// # Errors
    ///
    /// See [`Db::open_object_store_at`].
    pub async fn open_object_store(
        client: Arc<dyn ObjectClient>,
        options: DbOptions,
    ) -> Result<Self> {
        Self::open_object_store_at(client, "", options).await
    }

    /// Opens an object-storage database whose write-ahead log uses a separate
    /// object client.
    ///
    /// `storage_client` stores table/blob objects and the manifest CAS.
    /// `wal_client` stores the writer lease, remote WAL head, and WAL segment
    /// bytes. A confirmed durable write is acknowledged only after the WAL tier
    /// stores the frame and publishes the WAL head. Flush later copies covered
    /// memtable data into the storage tier's table objects and advances the
    /// storage-tier manifest replay floor.
    ///
    /// Use this when object storage is the shared bulk tier but commit latency
    /// should be bounded by a lower-latency durable WAL tier. The two clients may
    /// point at different buckets, services, or adapters; they share the same
    /// key prefix string so recovery can address the corresponding objects in
    /// each tier.
    ///
    /// # Parameters
    ///
    /// - `storage_client`: object client for manifest, tables, blobs, and bulk
    ///   cleanup.
    /// - `wal_client`: object client for the writer lease and remote WAL
    ///   objects. It must provide real conditional writes and same-key
    ///   read-after-write visibility.
    /// - `options`: must be [`DbOptions::object_store`], optionally marked
    ///   read-only.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when `options` is not object-store mode,
    /// [`Error::Fenced`] when another writer owns a newer WAL-tier epoch, or
    /// storage/`ObjectClient` errors from either tier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use trine_kv::{Db, DbOptions, InMemoryObjectStore};
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let storage = Arc::new(InMemoryObjectStore::new());
    ///     let wal = Arc::new(InMemoryObjectStore::new());
    ///     let db = Db::open_object_store_with_wal(storage, wal, DbOptions::object_store()).await?;
    ///
    ///     db.put(b"k", b"v").await?;
    ///     assert_eq!(db.get(b"k").await?, Some(b"v".to_vec()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_object_store_with_wal(
        storage_client: Arc<dyn ObjectClient>,
        wal_client: Arc<dyn ObjectClient>,
        options: DbOptions,
    ) -> Result<Self> {
        Self::open_object_store_with_wal_at(storage_client, wal_client, "", options).await
    }

    /// Opens an object-storage database under a key `prefix` (async-only).
    ///
    /// Provide any [`ObjectClient`] implementation (S3 and compatible, or the
    /// in-memory fake) and object-store options ([`DbOptions::object_store`]).
    /// All of the database's object keys live under `prefix`
    /// (`<prefix>/MANIFEST`, `<prefix>/LOCK`, `<prefix>/table-*.trinet`, …), so
    /// several databases can share one bucket. An empty `prefix` uses the bucket
    /// root.
    ///
    /// The byte IO, the manifest (conditional-PUT CAS), the writer lease, and
    /// the remote WAL all ride `client`. Durable writes publish WAL frames into
    /// the current remote segment before they become visible; flush later
    /// converts covered memtables into table objects, advances the manifest
    /// replay floor, and cleans old WAL objects.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when `options` is not object-store mode,
    /// or storage/`ObjectClient` errors from reading the manifest and acquiring
    /// the writer lease.
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    #[allow(clippy::too_many_lines)]
    pub async fn open_object_store_at(
        client: Arc<dyn ObjectClient>,
        prefix: impl Into<String>,
        options: DbOptions,
    ) -> Result<Self> {
        Self::open_object_store_with_wal_at(Arc::clone(&client), client, prefix, options).await
    }

    /// Opens an object-storage database under `prefix` with an explicit WAL tier.
    ///
    /// This is the prefixed form of [`Db::open_object_store_with_wal`]. The
    /// prefix is applied independently to both clients: storage keys such as
    /// `<prefix>/MANIFEST` live in `storage_client`, while WAL-tier keys such as
    /// `<prefix>/LOCK` and `<prefix>/wal-...` live in `wal_client`.
    ///
    /// A read-only open reads the storage manifest from `storage_client` and the
    /// remote WAL head/segment from `wal_client`, then serves a point-in-time
    /// view until [`Db::refresh_object_store`] is called.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when `options` is not object-store mode,
    /// [`Error::UnsupportedBackend`] when a reclamation capability belongs to a
    /// different storage client or prefix, or any error returned by the storage
    /// or WAL client while opening, recovering, or acquiring the writer lease.
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    #[allow(clippy::too_many_lines)]
    pub async fn open_object_store_with_wal_at(
        storage_client: Arc<dyn ObjectClient>,
        wal_client: Arc<dyn ObjectClient>,
        prefix: impl Into<String>,
        options: DbOptions,
    ) -> Result<Self> {
        if !options.storage_mode.is_object_store_persistent() {
            return Err(Error::invalid_options(
                "object-store open requires the object-store storage mode",
            ));
        }
        validate_common_options(&options)?;
        if matches!(
            options.durability,
            DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
        ) {
            return Err(Error::unsupported_durability(options.durability));
        }

        let prefix = canonical_object_prefix(&prefix.into())?;
        let db_path = PathBuf::from(prefix);
        if let ContentReclamationMode::QualifiedObjectStore(qualification) =
            &options.content_reclamation
        {
            if !qualification.matches_prefix(&db_path) {
                return Err(Error::unsupported_backend(
                    "object-store reclamation qualification names a different database prefix",
                ));
            }
            if !qualification.matches_client(&storage_client) {
                return Err(Error::unsupported_backend(
                    "object-store reclamation qualification belongs to a different client instance",
                ));
            }
        }
        let backend = ObjectStoreBackend::new(Arc::clone(&storage_client));
        let wal_backend = ObjectStoreBackend::new(Arc::clone(&wal_client));
        // The default trust mode assumes adapter qualification happened outside
        // open (CI/startup/health check). VerifyOnOpen is the explicit
        // fail-closed mode for deployments that want this open call to probe.
        if !options.read_only
            && matches!(
                options.object_client_trust,
                ObjectClientTrustMode::VerifyOnOpen
            )
        {
            verify_object_client_contract_for_open(&storage_client, &db_path, "storage").await?;
            if !Arc::ptr_eq(&storage_client, &wal_client) {
                verify_object_client_contract_for_open(&wal_client, &db_path, "wal").await?;
            }
        }
        let manifest_key = canonical_object_key(&manifest::manifest_path(&db_path))?;

        // Acquire the writer lease before the manifest, so its fencing epoch is
        // known and can be stamped into every publish.
        let lease_key = canonical_object_key(&db_path.join(recovery::PROCESS_LOCK_FILE_NAME))?;
        let (substrate, remote_wal_state) = if options.read_only {
            let remote_wal_state =
                ObjectWriterLease::read_current(Arc::clone(&wal_client), lease_key)
                    .await?
                    .unwrap_or_else(ObjectLeaseState::empty);
            (
                DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None)),
                remote_wal_state,
            )
        } else {
            let lease = ObjectWriterLease::acquire(Arc::clone(&wal_client), lease_key).await?;
            let remote_wal_state = lease.lease_state();
            (
                DurabilitySubstrate::ObjectStore(ObjectStoreSubstrate::new(
                    lease,
                    db_path.clone(),
                )?),
                remote_wal_state,
            )
        };
        let writer_epoch = substrate.object_fencing_epoch().unwrap_or(0);

        let mut manifest = ManifestStore::open_object_store_async(
            Arc::clone(&storage_client),
            manifest_key,
            writer_epoch,
        )
        .await?;
        ensure_default_bucket_in_manifest_async(&mut manifest, &options).await?;
        // Stamp our fencing epoch into the manifest now (writable opens), so a
        // just-displaced prior owner is fenced before we issue our first flush —
        // not only after it. Fails with `Error::Fenced` if a newer owner exists.
        if !options.read_only {
            manifest.claim_object_epoch_async().await?;
        }

        let mut buckets =
            buckets_from_manifest_async(&backend, &db_path, manifest.state(), true).await?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;
        let replay_floor = manifest.state().wal_replay_floor();
        let wal_batches = object_store_wal_batches_after_replay_floor(
            Arc::clone(&wal_client),
            &db_path,
            &remote_wal_state,
            replay_floor,
        )
        .await?;
        let batches = object_store_committed_wal_batches(
            wal_batches,
            replay_floor,
            remote_wal_state.committed_sequence,
        )?;

        let runtime = Runtime::new(options.runtime);
        let db = Self::from_inner_parts(DbInnerParts::object_store(
            options,
            buckets,
            manifest,
            ObjectStoreInnerParts {
                substrate,
                storage: backend,
                wal_storage: wal_backend,
                prefix: db_path,
            },
            runtime,
        ));
        db.replay_wal_batches(batches, replay_floor)?;
        Ok(db)
    }

    #[cfg_attr(
        not(all(target_arch = "wasm32", target_os = "unknown")),
        allow(dead_code)
    )]
    pub(in crate::db) fn validate_browser_persistent_options(options: &DbOptions) -> Result<()> {
        if !matches!(
            options.storage_mode,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. }
            }
        ) {
            return Err(Error::invalid_options(
                "browser persistent open requires browser backend",
            ));
        }
        if options.read_only && options.create_if_missing {
            return Err(Error::invalid_options(
                "browser read-only open cannot create missing storage",
            ));
        }
        if matches!(
            options.durability,
            DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
        ) {
            return Err(Error::unsupported_durability(options.durability));
        }
        if options.runtime.mode != runtime::RuntimeMode::Inline {
            return Err(Error::invalid_options(
                "browser persistent backend requires inline runtime",
            ));
        }
        if options.background_worker_count != 0 {
            return Err(Error::invalid_options(
                "browser persistent backend does not support background workers yet",
            ));
        }
        validate_common_options(options)
    }

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
        let native_storage = NativeFileBackend::with_runtime(runtime.clone());
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
        let native_storage = NativeFileBackend::with_runtime(runtime.clone());
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
            manifest,
            native_storage,
            wal,
            process_lock,
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
