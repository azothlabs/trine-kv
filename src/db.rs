use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::{
        Arc, Condvar, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
};

use crate::{
    blob::{self, ValueRef},
    cache, compaction, durability,
    error::{Error, Result},
    iterator::{Direction, Iter, ScanSelector},
    keyspace::{Keyspace, KeyspaceName},
    lsm::{
        CompactionInput as LsmCompactionInput, CompactionOutput as LsmCompactionOutput,
        FlushInput as LsmFlushInput, LsmTree,
    },
    manifest::{self, ManifestState, ManifestStore},
    options::{
        DbOptions, DurabilityMode, FailOnCorruptionPolicy, FilterPolicy, KeyspaceOptions,
        PrefixFilterPolicy, StorageMode,
    },
    recovery,
    snapshot::{Snapshot, SnapshotTracker},
    stats::{DbStats, LevelStats},
    table::{self, Table},
    transaction::{Transaction, TransactionOptions},
    types::{KeyRange, Sequence},
    wal::{self, WalWriter},
    write_batch::BatchOperation,
};

mod commit;

#[derive(Debug, Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

#[derive(Debug)]
pub(crate) struct DbInner {
    options: DbOptions,
    last_sequence: AtomicU64,
    closed: AtomicBool,
    writer: Mutex<()>,
    process_lock: Mutex<Option<recovery::ProcessLock>>,
    keyspaces: RwLock<BTreeMap<String, Arc<LsmTree>>>,
    snapshots: Arc<SnapshotTracker>,
    pending_obsolete_table_ids: Mutex<BTreeSet<table::TableId>>,
    manifest: Option<Mutex<ManifestStore>>,
    wal: Option<Mutex<WalWriter>>,
    block_cache: Arc<cache::BlockCache>,
    compaction_runs: AtomicU64,
    compaction_input_tables: AtomicU64,
    compaction_output_tables: AtomicU64,
    compaction_input_bytes: AtomicU64,
    compaction_output_bytes: AtomicU64,
    maintenance: Arc<MaintenanceCoordinator>,
}

struct NamedFlushInput {
    keyspace: String,
    tree: Arc<LsmTree>,
    input: LsmFlushInput,
}

struct NamedCompactionInput {
    keyspace: String,
    tree: Arc<LsmTree>,
    input: LsmCompactionInput,
}

struct NamedCompactionOutput {
    keyspace: String,
    output: LsmCompactionOutput,
}

#[derive(Debug)]
struct MaintenanceCoordinator {
    state: Mutex<MaintenanceState>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct MaintenanceState {
    requested: bool,
    shutdown: bool,
    last_error: Option<String>,
}

impl MaintenanceCoordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(MaintenanceState::default()),
            wake: Condvar::new(),
        }
    }

    fn request(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.requested = true;
            self.wake.notify_one();
        }
    }

    fn wait_for_request(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        while !state.requested && !state.shutdown {
            let Ok(next_state) = self.wake.wait(state) else {
                return false;
            };
            state = next_state;
        }
        if state.shutdown {
            return false;
        }
        state.requested = false;
        true
    }

    fn record_error(&self, error: &Error) {
        if let Ok(mut state) = self.state.lock() {
            state.last_error = Some(error.to_string());
        }
    }

    fn take_error(&self) -> Option<String> {
        self.state
            .lock()
            .ok()
            .and_then(|mut state| state.last_error.take())
    }

    fn shutdown(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.shutdown = true;
            self.wake.notify_all();
        }
    }
}

fn record_maintenance_success(_maintenance: &MaintenanceCoordinator) {
    // A later successful maintenance pass must not hide a failure that no
    // caller has observed yet. `take_error` is the only path that clears it.
}

impl Drop for DbInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.maintenance.shutdown();
        let _ = cleanup_pending_obsolete_table_files(
            persistent_path_from_options(&self.options),
            &self.snapshots,
            &self.pending_obsolete_table_ids,
        );
    }
}

impl Db {
    pub fn open(options: DbOptions) -> Result<Self> {
        match options.storage_mode {
            StorageMode::InMemory => Self::memory(options),
            StorageMode::Persistent { .. } => Self::open_persistent_with_options(options),
        }
    }

    pub fn open_memory() -> Result<Self> {
        Self::memory(DbOptions::memory())
    }

    pub fn open_persistent(path: impl Into<std::path::PathBuf>) -> Result<Self> {
        Self::open(DbOptions::persistent(path))
    }

    pub fn open_read_only(path: impl Into<std::path::PathBuf>) -> Result<Self> {
        Self::open(DbOptions::persistent_read_only(path))
    }

    pub fn memory(mut options: DbOptions) -> Result<Self> {
        options.storage_mode = StorageMode::InMemory;
        validate_options(&options)?;
        let block_cache_bytes = options.block_cache_bytes;

        Ok(Self {
            inner: Arc::new(DbInner {
                options,
                last_sequence: AtomicU64::new(Sequence::ZERO.get()),
                closed: AtomicBool::new(false),
                writer: Mutex::new(()),
                process_lock: Mutex::new(None),
                keyspaces: RwLock::new(BTreeMap::new()),
                snapshots: Arc::new(SnapshotTracker::default()),
                pending_obsolete_table_ids: Mutex::new(BTreeSet::new()),
                manifest: None,
                wal: None,
                block_cache: Arc::new(cache::BlockCache::new(block_cache_bytes)),
                compaction_runs: AtomicU64::new(0),
                compaction_input_tables: AtomicU64::new(0),
                compaction_output_tables: AtomicU64::new(0),
                compaction_input_bytes: AtomicU64::new(0),
                compaction_output_bytes: AtomicU64::new(0),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
            }),
        })
    }

    fn open_persistent_with_options(options: DbOptions) -> Result<Self> {
        validate_options(&options)?;
        let block_cache_bytes = options.block_cache_bytes;
        let StorageMode::Persistent { path } = &options.storage_mode else {
            return Err(Error::invalid_options("persistent open requires a path"));
        };

        if path.exists() {
            if !path.is_dir() {
                return Err(Error::invalid_options("database path is not a directory"));
            }
        } else if options.create_if_missing && !options.read_only {
            wal::ensure_parent_dir(path)?;
        } else {
            return Err(Error::invalid_options("database path does not exist"));
        }

        let process_lock = if options.read_only {
            None
        } else {
            Some(recovery::ProcessLock::acquire(path)?)
        };

        if options.read_only {
            recovery::repair_safe_temporary_files(path, FailOnCorruptionPolicy::FailClosed)?;
        } else {
            recovery::repair_safe_temporary_files(path, options.fail_on_corruption)?;
        }

        let manifest_path = manifest::manifest_path(path);
        let manifest = ManifestStore::open_or_create(
            manifest_path,
            options.create_if_missing && !options.read_only,
        )?;
        let replay_floor = manifest.state().wal_replay_floor();
        let keyspaces = keyspaces_from_manifest(path, manifest.state())?;
        recovery::fail_on_unreferenced_storage_files(
            path,
            &referenced_table_file_ids(manifest.state()),
            &referenced_blob_file_ids_from_manifest(manifest.state()),
        )?;

        let wal_path = wal::wal_path(path);
        let batches = wal::read_batches_after(&wal_path, replay_floor)?;
        let wal = if options.read_only {
            None
        } else {
            Some(Mutex::new(WalWriter::open_append(&wal_path)?))
        };

        let db = Self {
            inner: Arc::new(DbInner {
                options,
                last_sequence: AtomicU64::new(Sequence::ZERO.get()),
                closed: AtomicBool::new(false),
                writer: Mutex::new(()),
                process_lock: Mutex::new(process_lock),
                keyspaces: RwLock::new(keyspaces),
                snapshots: Arc::new(SnapshotTracker::default()),
                pending_obsolete_table_ids: Mutex::new(BTreeSet::new()),
                manifest: Some(Mutex::new(manifest)),
                wal,
                block_cache: Arc::new(cache::BlockCache::new(block_cache_bytes)),
                compaction_runs: AtomicU64::new(0),
                compaction_input_tables: AtomicU64::new(0),
                compaction_output_tables: AtomicU64::new(0),
                compaction_input_bytes: AtomicU64::new(0),
                compaction_output_bytes: AtomicU64::new(0),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
            }),
        };
        db.replay_wal_batches(batches, replay_floor)?;
        db.start_background_workers()?;

        Ok(db)
    }

    pub fn keyspace(
        &self,
        name: impl Into<KeyspaceName>,
        options: KeyspaceOptions,
    ) -> Result<Keyspace> {
        self.ensure_open()?;

        let name = name.into();
        if name.as_str().is_empty() {
            return Err(Error::invalid_options("keyspace name cannot be empty"));
        }

        validate_keyspace_options(&options)?;

        if let Some(existing_options) = self.existing_keyspace_options(name.as_str())? {
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing keyspace options do not match requested options",
                ));
            }
            return Ok(Keyspace::new(self.clone(), name, existing_options));
        }

        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        self.persist_keyspace_creation(name.as_str(), &options)?;

        let keyspace_options = {
            let mut keyspaces = self
                .inner
                .keyspaces
                .write()
                .map_err(|_| lock_poisoned("keyspace registry"))?;

            if let Some(state) = keyspaces.get(name.as_str()) {
                if state.options != options {
                    return Err(Error::invalid_options(
                        "existing keyspace options do not match requested options",
                    ));
                }
                state.options.clone()
            } else {
                let keyspace_options = options.clone();
                keyspaces.insert(
                    name.as_str().to_owned(),
                    Arc::new(LsmTree::new(options, Vec::new())?),
                );
                keyspace_options
            }
        };

        Ok(Keyspace::new(self.clone(), name, keyspace_options))
    }

    pub fn persist(&self, mode: DurabilityMode) -> Result<()> {
        self.ensure_open()?;

        match self.inner.options.storage_mode {
            StorageMode::InMemory => Ok(()),
            StorageMode::Persistent { .. } => {
                if let Some(wal) = &self.inner.wal {
                    wal.lock()
                        .map_err(|_| lock_poisoned("WAL writer"))?
                        .persist(mode)?;
                }
                Ok(())
            }
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;

        let StorageMode::Persistent { path } = &self.inner.options.storage_mode else {
            return Ok(());
        };
        let db_path = path.clone();
        let flush_sequence = self.last_committed_sequence();

        let should_compact = {
            // Flush holds the writer coordinator while it freezes active
            // memtables, writes tables, and advances the WAL replay floor. That
            // gives the manifest edit and in-memory table list one clear
            // cutover point relative to commits.
            let _writer = self
                .inner
                .writer
                .lock()
                .map_err(|_| lock_poisoned("writer coordinator"))?;
            self.flush_memtables_locked(&db_path, Some(flush_sequence))?
        };

        if should_compact {
            self.compact_range(KeyRange::all())?;
        }
        self.cleanup_pending_obsolete_table_files(&db_path)?;

        Ok(())
    }

    // Keep the public shape aligned with the accepted v1 protocol:
    // `Db::compact_range(range) -> Result<()>`.
    #[allow(clippy::needless_pass_by_value)]
    pub fn compact_range(&self, range: KeyRange) -> Result<()> {
        self.take_background_maintenance_error()?;
        self.compact_range_internal(range)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn compact_range_internal(&self, range: KeyRange) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let StorageMode::Persistent { path } = &self.inner.options.storage_mode else {
            return Ok(());
        };
        let db_path = path.clone();

        // Compaction holds the writer coordinator while it chooses inputs,
        // writes replacement tables, and publishes the manifest edit. Readers
        // keep using the old Arc<Table> handles until the in-memory list is
        // swapped after publish.
        let _writer = self
            .inner
            .writer
            .lock()
            .map_err(|_| lock_poisoned("writer coordinator"))?;
        let oldest_active_snapshot = self.oldest_active_snapshot_sequence();
        let compaction_inputs = self.collect_compaction_inputs(&range, oldest_active_snapshot)?;
        if compaction_inputs.is_empty() {
            return Ok(());
        }

        let obsolete_table_ids = compaction_inputs
            .iter()
            .flat_map(|input| input.input.input_table_ids.iter().copied())
            .collect::<Vec<_>>();
        let mut written_tables = Vec::with_capacity(compaction_inputs.len());
        let mut written_table_ids = Vec::new();
        let mut next_table_id = self.next_table_id()?;
        for input in &compaction_inputs {
            let payloads = input.tree.build_compaction_table_payloads(
                &input.input,
                &range,
                oldest_active_snapshot,
                self.inner.options.target_table_bytes,
            )?;
            let mut output_tables = Vec::with_capacity(payloads.len());
            for payload in payloads {
                let table_id = next_table_id;
                next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                    message: "table id counter overflow".to_owned(),
                })?;
                let table_path = table::table_path(&db_path, table_id);
                written_table_ids.push(table_id);
                let table = match table::write_table(
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &input.input.table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                ) {
                    Ok(table) => table,
                    Err(error) => {
                        let _ = remove_storage_files(&db_path, &written_table_ids);
                        return Err(error);
                    }
                };
                output_tables.push(Arc::new(table));
            }
            written_tables.push(NamedCompactionOutput {
                keyspace: input.keyspace.clone(),
                output: LsmCompactionOutput {
                    input_table_ids: input.input.input_table_ids.clone(),
                    tables: output_tables,
                },
            });
        }

        if !written_table_ids.is_empty() {
            if let Err(error) = durability::sync_dir_after_renames(&db_path) {
                let _ = remove_storage_files(&db_path, &written_table_ids);
                return Err(error);
            }
        }

        if let Err(error) = self.publish_compacted_tables(&written_tables) {
            let _ = remove_storage_files(&db_path, &written_table_ids);
            return Err(error);
        }

        self.install_compacted_tables(written_tables)?;
        self.record_compaction_stats(
            &db_path,
            compaction_inputs.len(),
            &obsolete_table_ids,
            &written_table_ids,
        );
        self.retire_obsolete_table_files(&db_path, &obsolete_table_ids)?;
        self.remove_unreferenced_blob_files(&db_path)?;

        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.inner
            .snapshots
            .pinned_snapshot(self.last_committed_sequence())
    }

    #[must_use]
    pub fn transaction(&self, options: TransactionOptions) -> Transaction {
        Transaction::new(self.clone(), self.last_committed_sequence(), options)
    }

    #[must_use]
    pub fn stats(&self) -> DbStats {
        let mut stats = DbStats {
            active_snapshots: self.inner.snapshots.active_count(),
            compaction_runs: self.inner.compaction_runs.load(Ordering::Acquire),
            compaction_input_tables: self.inner.compaction_input_tables.load(Ordering::Acquire),
            compaction_output_tables: self.inner.compaction_output_tables.load(Ordering::Acquire),
            compaction_input_bytes: self.inner.compaction_input_bytes.load(Ordering::Acquire),
            compaction_output_bytes: self.inner.compaction_output_bytes.load(Ordering::Acquire),
            ..DbStats::default()
        };
        let cache_stats = self.inner.block_cache.stats();
        stats.block_cache_hits = cache_stats.hits;
        stats.block_cache_misses = cache_stats.misses;

        let persistent_path = self.persistent_path();
        let mut level_stats = BTreeMap::<u32, LevelStats>::new();
        let mut live_blob_bytes_by_file = BTreeMap::<u64, u64>::new();

        let Ok(keyspaces) = self.inner.keyspaces.read() else {
            return stats;
        };
        stats.live_keyspaces = keyspaces.len();

        for state in keyspaces.values() {
            if let Ok(memtable_bytes) = state.memtable_bytes() {
                stats.memtable_bytes = stats.memtable_bytes.saturating_add(memtable_bytes);
            }
            if let Ok(immutable_memtables) = state.immutable_memtable_count() {
                stats.immutable_memtables = stats
                    .immutable_memtables
                    .saturating_add(immutable_memtables);
            }
            let Ok(version) = state.current_version() else {
                continue;
            };

            for (level_state, tables) in version.level_table_handles() {
                let level = level_state.get();
                let level_entry = level_stats.entry(level).or_insert(LevelStats {
                    level,
                    tables: 0,
                    bytes: 0,
                });
                for table in tables {
                    let properties = table.properties();
                    let table_bytes = persistent_path
                        .map_or(0, |db_path| table_file_bytes(db_path, properties.id));
                    stats.total_tables += 1;
                    stats.table_bytes = stats.table_bytes.saturating_add(table_bytes);
                    if properties.level == table::TableLevel::ZERO {
                        stats.l0_tables += 1;
                    }
                    level_entry.tables += 1;
                    level_entry.bytes = level_entry.bytes.saturating_add(table_bytes);

                    if let Ok(records) = table.point_records() {
                        for record in records {
                            if let Some(ValueRef::Blob { file_id, len, .. }) = record.value {
                                live_blob_bytes_by_file
                                    .entry(file_id)
                                    .and_modify(|bytes| *bytes = bytes.saturating_add(len))
                                    .or_insert(len);
                            }
                        }
                    }
                }
            }
        }

        stats.level_tables = level_stats.into_values().collect();
        stats.live_blob_files = live_blob_bytes_by_file.len();
        stats.live_blob_bytes = live_blob_bytes_by_file.values().copied().sum();
        if let Some(db_path) = persistent_path {
            add_obsolete_blob_stats(db_path, &live_blob_bytes_by_file, &mut stats);
        }

        stats
    }

    #[must_use]
    pub fn options(&self) -> &DbOptions {
        &self.inner.options
    }

    #[must_use]
    pub fn last_committed_sequence(&self) -> Sequence {
        Sequence::new(self.inner.last_sequence.load(Ordering::Acquire))
    }

    fn oldest_active_snapshot_sequence(&self) -> Sequence {
        self.inner
            .snapshots
            .oldest_active_or(self.last_committed_sequence())
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.maintenance.shutdown();
        // The directory lock is released only after the writer coordinator is
        // idle. Otherwise a second process could open while this one is still
        // publishing files for a commit, flush, or compaction.
        let Ok(_writer) = self.inner.writer.lock() else {
            return;
        };
        if let Some(db_path) = self.persistent_path().map(Path::to_path_buf) {
            let _ = self.cleanup_pending_obsolete_table_files(&db_path);
        }
        if let Ok(mut process_lock) = self.inner.process_lock.lock() {
            process_lock.take();
        }
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    fn start_background_workers(&self) -> Result<()> {
        if !self.background_workers_enabled() {
            return Ok(());
        }

        for worker_index in 0..self.inner.options.background_worker_count {
            let inner = Arc::downgrade(&self.inner);
            let maintenance = Arc::clone(&self.inner.maintenance);
            thread::Builder::new()
                .name(format!("trine-kv-maintenance-{worker_index}"))
                .spawn(move || background_worker_loop(&inner, &maintenance))
                .map_err(Error::Io)?;
        }
        self.request_background_maintenance();

        Ok(())
    }

    fn background_workers_enabled(&self) -> bool {
        !self.inner.options.read_only
            && self.inner.options.background_worker_count != 0
            && matches!(
                self.inner.options.storage_mode,
                StorageMode::Persistent { .. }
            )
    }

    fn request_background_maintenance(&self) {
        if self.background_workers_enabled() {
            self.inner.maintenance.request();
        }
    }

    fn take_background_maintenance_error(&self) -> Result<()> {
        if let Some(error) = self.inner.maintenance.take_error() {
            Err(Error::Corruption {
                message: format!("background maintenance failed: {error}"),
            })
        } else {
            Ok(())
        }
    }

    fn run_background_maintenance(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Ok(());
        }

        let StorageMode::Persistent { path } = &self.inner.options.storage_mode else {
            return Ok(());
        };
        let db_path = path.clone();
        let should_compact = {
            let _writer = self
                .inner
                .writer
                .lock()
                .map_err(|_| lock_poisoned("writer coordinator"))?;
            let flushed_needs_compaction = if self.has_immutable_memtables()? {
                self.flush_memtables_locked(&db_path, None)?
            } else {
                false
            };
            flushed_needs_compaction || self.l0_pressure_exceeded()?
        };

        if should_compact {
            self.compact_range_internal(KeyRange::all())?;
        }

        Ok(())
    }

    pub(crate) fn get_at(
        &self,
        keyspace: &str,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        self.get_at_with_pin_state(keyspace, key, read_sequence, false)
    }

    pub(crate) fn get_at_with_pin_state(
        &self,
        keyspace: &str,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<Vec<u8>>> {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        let state = self.keyspace_state(keyspace)?;
        state.read_visible_point(
            key,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
        )
    }

    pub(crate) fn range_at(
        &self,
        keyspace: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.keyspace_state(keyspace)?;
        let selector = ScanSelector::Range(range.clone());
        let scan = state.scan(&selector, direction, Some(&self.inner.block_cache))?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(Iter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            scan.range_tombstones,
            scan.sources,
        ))
    }

    pub(crate) fn prefix_at(
        &self,
        keyspace: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.keyspace_state(keyspace)?;
        let selector = ScanSelector::Prefix(prefix.to_vec());
        let scan = state.scan(&selector, direction, Some(&self.inner.block_cache))?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(Iter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            scan.range_tombstones,
            scan.sources,
        ))
    }

    fn keyspace_state(&self, keyspace: &str) -> Result<Arc<LsmTree>> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        keyspaces
            .get(keyspace)
            .cloned()
            .ok_or_else(|| Error::KeyspaceMissing {
                name: keyspace.to_owned(),
            })
    }

    fn existing_keyspace_options(&self, keyspace: &str) -> Result<Option<KeyspaceOptions>> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        Ok(keyspaces.get(keyspace).map(|state| state.options.clone()))
    }

    fn persistent_path(&self) -> Option<&Path> {
        match &self.inner.options.storage_mode {
            StorageMode::Persistent { path } => Some(path.as_path()),
            StorageMode::InMemory => None,
        }
    }

    fn persist_keyspace_creation(&self, name: &str, options: &KeyspaceOptions) -> Result<()> {
        if let Some(manifest) = &self.inner.manifest {
            // Manifest I/O happens outside the keyspace registry lock. Two
            // racing creators are serialized by the manifest lock, and the
            // second identical request becomes a no-op.
            manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .create_keyspace(name.to_owned(), options.clone())?;
        }

        Ok(())
    }

    fn resolve_batch_keyspaces(&self, operations: &[BatchOperation]) -> Result<Vec<Arc<LsmTree>>> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut states = Vec::with_capacity(operations.len());

        for operation in operations {
            let state = keyspaces
                .get(operation.keyspace())
                .cloned()
                .ok_or_else(|| Error::KeyspaceMissing {
                    name: operation.keyspace().to_owned(),
                })?;
            states.push(state);
        }

        Ok(states)
    }

    fn flush_immutable_memtables_for_write_locked(&self, db_path: &Path) -> Result<()> {
        let flush_inputs = self.collect_pressure_flush_inputs()?;
        if !flush_inputs.is_empty() && self.write_flush_inputs(db_path, &flush_inputs)? {
            self.request_background_maintenance();
        }

        Ok(())
    }

    fn freeze_large_active_memtables_after_commit_locked(
        &self,
        sequence: Sequence,
        states: &[Arc<LsmTree>],
    ) -> Result<bool> {
        let threshold = usize_to_u64_saturating(self.inner.options.write_buffer_bytes);
        let mut frozen_count = 0_usize;

        for state in states {
            if state.active_memtable_bytes()? >= threshold
                && state.freeze_active_memtable(sequence)?
            {
                frozen_count += 1;
            }
        }

        Ok(frozen_count != 0)
    }

    fn has_immutable_memtables(&self) -> Result<bool> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        for state in keyspaces.values() {
            if state.has_immutable_memtables()? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn freeze_all_active_memtables(&self, freeze_sequence: Sequence) -> Result<usize> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut frozen_count = 0;

        for state in keyspaces.values() {
            if state.freeze_active_memtable(freeze_sequence)? {
                frozen_count += 1;
            }
        }

        Ok(frozen_count)
    }

    fn flush_memtables_locked(
        &self,
        db_path: &Path,
        freeze_active_at: Option<Sequence>,
    ) -> Result<bool> {
        if let Some(sequence) = freeze_active_at {
            self.freeze_all_active_memtables(sequence)?;
        }

        let flush_inputs = self.collect_flush_inputs()?;
        self.write_flush_inputs(db_path, &flush_inputs)
    }

    fn write_flush_inputs(&self, db_path: &Path, flush_inputs: &[NamedFlushInput]) -> Result<bool> {
        if flush_inputs.is_empty() {
            return Ok(false);
        }
        let flush_sequence = flush_inputs
            .iter()
            .map(|input| input.input.freeze_sequence)
            .max()
            .expect("non-empty flush input list has a max sequence");

        let mut written_tables = Vec::with_capacity(flush_inputs.len());
        let mut written_table_ids = Vec::with_capacity(flush_inputs.len());
        for input in flush_inputs {
            let table_path = table::table_path(db_path, input.input.table_id);
            written_table_ids.push(input.input.table_id);
            let table = match table::write_table(
                &table_path,
                input.input.table_id,
                input.input.table_level,
                &input.input.table_options,
                &input.input.point_records,
                &input.input.range_tombstones,
            ) {
                Ok(table) => table,
                Err(error) => {
                    let _ = remove_storage_files(db_path, &written_table_ids);
                    return Err(error);
                }
            };
            written_tables.push((input.keyspace.clone(), Arc::new(table)));
        }

        if let Err(error) = durability::sync_dir_after_renames(db_path) {
            let _ = remove_storage_files(db_path, &written_table_ids);
            return Err(error);
        }

        if let Err(error) = self.publish_flushed_tables(&written_tables, flush_sequence) {
            let _ = remove_storage_files(db_path, &written_table_ids);
            return Err(error);
        }
        Self::install_flushed_tables(flush_inputs, written_tables)?;
        self.rewrite_wal_after_replay_floor(db_path, flush_sequence)?;
        self.l0_pressure_exceeded()
    }

    fn collect_pressure_flush_inputs(&self) -> Result<Vec<NamedFlushInput>> {
        let max_immutable_memtables = self.inner.options.max_immutable_memtables;
        let mut next_table_id = self.next_table_id()?;
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in keyspaces.iter() {
            if state.immutable_memtable_count()? < max_immutable_memtables {
                continue;
            }
            for input in state.prepare_flush_inputs(&mut next_table_id)? {
                inputs.push(NamedFlushInput {
                    keyspace: name.clone(),
                    tree: Arc::clone(state),
                    input,
                });
            }
        }

        Ok(inputs)
    }

    fn collect_flush_inputs(&self) -> Result<Vec<NamedFlushInput>> {
        let mut next_table_id = self.next_table_id()?;
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in keyspaces.iter() {
            for input in state.prepare_flush_inputs(&mut next_table_id)? {
                inputs.push(NamedFlushInput {
                    keyspace: name.clone(),
                    tree: Arc::clone(state),
                    input,
                });
            }
        }

        Ok(inputs)
    }

    fn collect_compaction_inputs(
        &self,
        range: &KeyRange,
        oldest_active_snapshot: Sequence,
    ) -> Result<Vec<NamedCompactionInput>> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();
        let compaction_options = compaction_options(&self.inner.options);

        for (name, state) in keyspaces.iter() {
            let Some(input) =
                state.plan_compaction(name, range, oldest_active_snapshot, compaction_options)?
            else {
                continue;
            };
            inputs.push(NamedCompactionInput {
                keyspace: name.clone(),
                tree: Arc::clone(state),
                input,
            });
        }

        Ok(inputs)
    }

    fn next_table_id(&self) -> Result<table::TableId> {
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .next_table_id()
    }

    fn publish_flushed_tables(
        &self,
        tables: &[(String, Arc<Table>)],
        flush_sequence: Sequence,
    ) -> Result<()> {
        let edits = tables
            .iter()
            .map(|(keyspace, table)| (keyspace.clone(), table.properties().clone()))
            .collect::<Vec<_>>();
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .add_tables(edits, flush_sequence)
    }

    fn publish_compacted_tables(&self, outputs: &[NamedCompactionOutput]) -> Result<()> {
        let edits = outputs
            .iter()
            .map(|output| {
                (
                    output.keyspace.clone(),
                    output.output.input_table_ids.clone(),
                    output
                        .output
                        .tables
                        .iter()
                        .map(|table| table.properties().clone())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .replace_tables_batch(edits)
    }

    fn rewrite_wal_after_replay_floor(&self, db_path: &Path, replay_floor: Sequence) -> Result<()> {
        let Some(wal) = &self.inner.wal else {
            return Ok(());
        };

        let wal_path = wal::wal_path(db_path);
        let mut writer = wal.lock().map_err(|_| lock_poisoned("WAL writer"))?;
        writer.persist(DurabilityMode::SyncAll)?;
        wal::rewrite_batches_after(&wal_path, replay_floor)?;
        writer.reopen_append(&wal_path)
    }

    fn install_flushed_tables(
        inputs: &[NamedFlushInput],
        tables: Vec<(String, Arc<Table>)>,
    ) -> Result<()> {
        for (input, (keyspace, table)) in inputs.iter().zip(tables) {
            debug_assert_eq!(input.keyspace, keyspace);
            input.tree.install_flush(&input.input, table)?;
        }

        Ok(())
    }

    fn install_compacted_tables(&self, outputs: Vec<NamedCompactionOutput>) -> Result<()> {
        for output in outputs {
            let state = self.keyspace_state(&output.keyspace)?;
            state.install_compaction(output.output)?;
        }

        Ok(())
    }

    fn live_blob_file_ids(&self) -> Result<BTreeSet<u64>> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        referenced_blob_file_ids(&keyspaces)
    }

    fn remove_unreferenced_blob_files(&self, db_path: &Path) -> Result<()> {
        // This pass runs after manifest publish and the in-memory table switch.
        // A snapshot or short read pin may still hold an older Arc<Table>, so
        // skip deletion when any pin exists; a later compaction can retry.
        if self.inner.snapshots.active_count() != 0 {
            return Ok(());
        }

        let live_file_ids = self.live_blob_file_ids()?;
        for file_id in blob::list_blob_file_ids(db_path)? {
            if live_file_ids.contains(&file_id) {
                continue;
            }

            match fs::remove_file(blob::blob_path(db_path, file_id)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::Io(error)),
            }
        }

        Ok(())
    }

    fn retire_obsolete_table_files(
        &self,
        db_path: &Path,
        table_ids: &[table::TableId],
    ) -> Result<()> {
        {
            let mut pending = self
                .inner
                .pending_obsolete_table_ids
                .lock()
                .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
            pending.extend(table_ids.iter().copied());
        }

        self.cleanup_pending_obsolete_table_files(db_path)
    }

    fn cleanup_pending_obsolete_table_files(&self, db_path: &Path) -> Result<()> {
        cleanup_pending_obsolete_table_files(
            Some(db_path),
            &self.inner.snapshots,
            &self.inner.pending_obsolete_table_ids,
        )
    }

    fn l0_pressure_exceeded(&self) -> Result<bool> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        for state in keyspaces.values() {
            if state.l0_table_count()? > self.inner.options.max_l0_files {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn record_compaction_stats(
        &self,
        db_path: &Path,
        runs: usize,
        input_table_ids: &[table::TableId],
        output_table_ids: &[table::TableId],
    ) {
        let input_bytes = input_table_ids
            .iter()
            .map(|table_id| table_file_bytes(db_path, *table_id))
            .sum::<u64>();
        let output_bytes = output_table_ids
            .iter()
            .map(|table_id| table_file_bytes(db_path, *table_id))
            .sum::<u64>();

        self.inner
            .compaction_runs
            .fetch_add(usize_to_u64_saturating(runs), Ordering::AcqRel);
        self.inner.compaction_input_tables.fetch_add(
            usize_to_u64_saturating(input_table_ids.len()),
            Ordering::AcqRel,
        );
        self.inner.compaction_output_tables.fetch_add(
            usize_to_u64_saturating(output_table_ids.len()),
            Ordering::AcqRel,
        );
        self.inner
            .compaction_input_bytes
            .fetch_add(input_bytes, Ordering::AcqRel);
        self.inner
            .compaction_output_bytes
            .fetch_add(output_bytes, Ordering::AcqRel);
    }
}

fn validate_options(options: &DbOptions) -> Result<()> {
    if options.write_buffer_bytes == 0 {
        return Err(Error::invalid_options("write buffer must be non-zero"));
    }
    if options.max_immutable_memtables == 0 {
        return Err(Error::invalid_options(
            "max immutable memtables must be non-zero",
        ));
    }
    if options.target_table_bytes == 0 {
        return Err(Error::invalid_options("target table size must be non-zero"));
    }
    if options.level_size_multiplier < 2 {
        return Err(Error::invalid_options("level size multiplier must be >= 2"));
    }
    if options.max_l0_files == 0 {
        return Err(Error::invalid_options("max L0 files must be non-zero"));
    }

    Ok(())
}

fn background_worker_loop(inner: &Weak<DbInner>, maintenance: &MaintenanceCoordinator) {
    while maintenance.wait_for_request() {
        let Some(inner) = inner.upgrade() else {
            break;
        };
        if inner.closed.load(Ordering::Acquire) {
            break;
        }

        let db = Db { inner };
        match db.run_background_maintenance() {
            Ok(()) => record_maintenance_success(maintenance),
            Err(Error::Closed) => break,
            Err(error) => maintenance.record_error(&error),
        }
    }
}

fn keyspaces_from_manifest(
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<BTreeMap<String, Arc<LsmTree>>> {
    let mut keyspaces = BTreeMap::new();

    for (name, options) in manifest.keyspaces() {
        validate_keyspace_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let table = table::read_table(&table_path)?;
            if table.properties() != properties {
                return Err(Error::Corruption {
                    message: format!(
                        "manifest properties do not match table {}",
                        properties.id.get()
                    ),
                });
            }
            tables.push(Arc::new(table));
        }

        keyspaces.insert(
            name.clone(),
            Arc::new(LsmTree::new(options.clone(), tables)?),
        );
    }

    Ok(keyspaces)
}

fn table_file_bytes(db_path: &Path, table_id: table::TableId) -> u64 {
    fs::metadata(table::table_path(db_path, table_id)).map_or(0, |metadata| metadata.len())
}

fn add_obsolete_blob_stats(
    db_path: &Path,
    live_blob_bytes_by_file: &BTreeMap<u64, u64>,
    stats: &mut DbStats,
) {
    let Ok(blob_file_ids) = blob::list_blob_file_ids(db_path) else {
        return;
    };

    for file_id in blob_file_ids {
        if live_blob_bytes_by_file.contains_key(&file_id) {
            continue;
        }
        stats.obsolete_blob_files += 1;
        let bytes =
            fs::metadata(blob::blob_path(db_path, file_id)).map_or(0, |metadata| metadata.len());
        stats.obsolete_blob_bytes = stats.obsolete_blob_bytes.saturating_add(bytes);
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

fn referenced_table_file_ids(manifest: &ManifestState) -> BTreeSet<table::TableId> {
    manifest
        .tables()
        .values()
        .flat_map(|tables| tables.iter().map(|properties| properties.id))
        .collect()
}

fn referenced_blob_file_ids_from_manifest(manifest: &ManifestState) -> BTreeSet<u64> {
    manifest
        .tables()
        .values()
        .flat_map(|tables| {
            tables
                .iter()
                .flat_map(|properties| properties.blob_file_ids.iter().copied())
        })
        .collect()
}

fn referenced_blob_file_ids(keyspaces: &BTreeMap<String, Arc<LsmTree>>) -> Result<BTreeSet<u64>> {
    let mut file_ids = BTreeSet::new();

    for state in keyspaces.values() {
        for table in state.tables_snapshot()? {
            file_ids.extend(table.blob_file_ids());
        }
    }

    Ok(file_ids)
}

fn validate_keyspace_options(options: &KeyspaceOptions) -> Result<()> {
    if options.block_bytes == 0 {
        return Err(Error::invalid_options("block size must be non-zero"));
    }
    if matches!(
        options.filter_policy,
        FilterPolicy::Bloom { bits_per_key: 0 }
    ) {
        return Err(Error::invalid_options(
            "bits_per_key must be non-zero for Bloom filters",
        ));
    }
    if matches!(
        options.prefix_filter_policy,
        PrefixFilterPolicy::Bloom { bits_per_prefix: 0 }
    ) {
        return Err(Error::invalid_options(
            "bits_per_prefix must be non-zero for Bloom filters",
        ));
    }
    if options.blob_threshold_bytes == 0 {
        return Err(Error::invalid_options("blob threshold must be non-zero"));
    }

    Ok(())
}

fn compaction_options(options: &DbOptions) -> compaction::CompactionOptions {
    compaction::CompactionOptions {
        target_table_bytes: usize_to_u64_saturating(options.target_table_bytes),
        level_size_multiplier: usize_to_u64_saturating(options.level_size_multiplier),
        max_l0_files: options.max_l0_files,
    }
}

fn validate_batch_len(len: usize) -> Result<()> {
    if len > u32::MAX as usize {
        return Err(Error::InvalidOptions {
            message: "write batch operation count exceeds u32::MAX".to_owned(),
        });
    }

    Ok(())
}

fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}

fn persistent_path_from_options(options: &DbOptions) -> Option<&Path> {
    match &options.storage_mode {
        StorageMode::Persistent { path } => Some(path.as_path()),
        StorageMode::InMemory => None,
    }
}

fn cleanup_pending_obsolete_table_files(
    db_path: Option<&Path>,
    snapshots: &SnapshotTracker,
    pending_table_ids: &Mutex<BTreeSet<table::TableId>>,
) -> Result<()> {
    let Some(db_path) = db_path else {
        return Ok(());
    };
    if snapshots.active_count() != 0 {
        return Ok(());
    }

    let table_ids = {
        let pending = pending_table_ids
            .lock()
            .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
        if pending.is_empty() {
            return Ok(());
        }
        pending.iter().copied().collect::<Vec<_>>()
    };

    remove_table_files(db_path, &table_ids)?;

    let mut pending = pending_table_ids
        .lock()
        .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
    for table_id in table_ids {
        pending.remove(&table_id);
    }

    Ok(())
}

fn remove_table_files(db_path: &Path, table_ids: &[table::TableId]) -> Result<()> {
    for table_id in table_ids {
        match fs::remove_file(table::table_path(db_path, *table_id)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Io(error)),
        }
    }

    Ok(())
}

fn remove_blob_files(db_path: &Path, table_ids: &[table::TableId]) -> Result<()> {
    for table_id in table_ids {
        match fs::remove_file(blob::blob_path(db_path, table_id.get())) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Io(error)),
        }
    }

    Ok(())
}

fn remove_storage_files(db_path: &Path, table_ids: &[table::TableId]) -> Result<()> {
    // A table write uses the table id as the blob file id for large values.
    // Before manifest publish succeeds, both files are unpublished output and
    // can be removed together after a failed flush or compaction attempt.
    remove_table_files(db_path, table_ids)?;
    remove_blob_files(db_path, table_ids)
}

#[cfg(test)]
mod tests {
    use super::{Error, MaintenanceCoordinator, record_maintenance_success};

    #[test]
    fn maintenance_success_does_not_clear_unreported_error() {
        let coordinator = MaintenanceCoordinator::new();
        coordinator.record_error(&Error::Corruption {
            message: "publish failed".to_string(),
        });

        record_maintenance_success(&coordinator);

        let error = coordinator
            .take_error()
            .expect("unreported background error remains visible");
        assert!(error.contains("publish failed"));
        assert!(coordinator.take_error().is_none());
    }
}
