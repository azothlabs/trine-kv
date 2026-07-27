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
use std::sync::atomic::AtomicU8;

use crate::db::FatalWriteStopReason;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::db::open_helpers::run_persistent_recovery_checks_async;
use crate::db::storage_backend::DatabaseStorage;
use crate::object_store::{canonical_object_key, canonical_object_prefix};
use crate::options::ContentReclamationMode;
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

mod browser_open;
mod browser_options;
mod native;
mod object_store;

#[derive(Debug)]
struct DbInnerParts {
    options: DbOptions,
    buckets: BTreeMap<String, Arc<LsmTree>>,
    manifest: Option<ManifestStore>,
    substrate: DurabilitySubstrate,
    storage: DatabaseStorage,
    runtime: Runtime,
}

#[derive(Debug)]
struct ObjectStoreInnerParts {
    substrate: DurabilitySubstrate,
    storage: ObjectStoreBackend,
    wal_storage: ObjectStoreBackend,
    prefix: PathBuf,
}

#[derive(Debug)]
struct NativeInnerParts {
    manifest: ManifestStore,
    storage: NativeFileBackend,
    root: PathBuf,
    wal: Option<WalFrontDoor>,
    process_lock: Option<recovery::ProcessLock>,
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug)]
struct BrowserInnerParts {
    manifest: ManifestStore,
    storage: BrowserStorageBackend,
    root: PathBuf,
    writer_lease: Option<BrowserWriterLease>,
    wal: Option<BrowserWalFrontDoor>,
}

impl DbInnerParts {
    fn base(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        manifest: Option<ManifestStore>,
        substrate: DurabilitySubstrate,
        storage: DatabaseStorage,
        runtime: Runtime,
    ) -> Self {
        Self {
            options,
            buckets,
            manifest,
            substrate,
            storage,
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
            DatabaseStorage::memory(),
            runtime,
        )
    }

    fn native(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        backend: NativeInnerParts,
        runtime: Runtime,
    ) -> Self {
        Self::base(
            options,
            buckets,
            Some(backend.manifest),
            DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(
                backend.wal,
                backend.process_lock,
            )),
            DatabaseStorage::filesystem(backend.storage, backend.root),
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
        Self::base(
            options,
            buckets,
            Some(manifest),
            backend.substrate,
            DatabaseStorage::object_store(backend.storage, backend.wal_storage, backend.prefix),
            runtime,
        )
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    fn browser(
        options: DbOptions,
        buckets: BTreeMap<String, Arc<LsmTree>>,
        backend: BrowserInnerParts,
        runtime: Runtime,
    ) -> Self {
        Self::base(
            options,
            buckets,
            Some(backend.manifest),
            DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None)),
            DatabaseStorage::browser(
                backend.storage,
                backend.root,
                backend.writer_lease,
                backend.wal,
            ),
            runtime,
        )
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
            storage,
            runtime,
        } = parts;
        let block_cache_bytes = options.block_cache_bytes;

        Self {
            inner: Arc::new(DbInner {
                options,
                user_handles: AtomicUsize::new(1),
                commit_tracker: CommitTracker::new(Sequence::ZERO),
                closed: AtomicBool::new(false),
                fatal_write_stop_reason: AtomicU8::new(FatalWriteStopReason::NONE),
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
                manifest_sync_adapter_tasks: AtomicU64::new(0),
                storage,
                content_seal_lock: futures::lock::Mutex::new(()),
                content_access_lock: futures::lock::Mutex::new(()),
                content_lock_hasher: RandomState::new(),
                content_quota_locks: std::array::from_fn(|_| futures::lock::Mutex::new(())),
                content_upload_locks: std::array::from_fn(|_| futures::lock::Mutex::new(())),
                object_manifest_async_lock: futures::lock::Mutex::new(()),
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
    /// backend is unavailable on this target, [`Error::LeaseUnavailable`] when
    /// another writable handle owns the persistent namespace,
    /// [`Error::Corruption`] when recovery detects unsafe durable state, or
    /// [`Error::Io`] for storage failures.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// # let path = std::env::temp_dir().join(format!("trine-doc-open-{}", std::process::id()));
    /// # let _ = std::fs::remove_dir_all(&path);
    /// let persistent = Db::open_sync(&path)?;
    /// persistent.put_sync(b"k", b"v")?;
    ///
    /// let memory = Db::open_sync(DbOptions::memory())?;
    /// memory.put_sync(b"session", b"only in memory")?;
    /// # persistent.close_sync();
    /// # drop(persistent);
    /// # std::fs::remove_dir_all(path)?;
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
}
