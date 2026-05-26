use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    ops::Bound,
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
    internal_key::{InternalKey, ValueKind},
    iterator::{
        Direction, Iter, RecordGroup, RecordSource, ScanRangeTombstone, ScanSelector,
        prefix_successor,
    },
    keyspace::{Keyspace, KeyspaceName},
    lsm::{ImmutableMemtable, LsmTree, RangeTombstone},
    manifest::{self, ManifestState, ManifestStore},
    memtable::Memtable,
    options::{
        DbOptions, DurabilityMode, FailOnCorruptionPolicy, FilterPolicy, KeyspaceOptions,
        PrefixFilterPolicy, StorageMode,
    },
    range_tombstone::{self, RangeTombstoneIndex},
    recovery,
    snapshot::{Snapshot, SnapshotTracker},
    stats::{DbStats, LevelStats},
    table::{self, Table, TablePointCursor, TableRangeTombstone},
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

struct FlushInput {
    keyspace: String,
    memtable: Arc<Memtable>,
    freeze_sequence: Sequence,
    table_id: table::TableId,
    table_level: table::TableLevel,
    table_options: table::TableWriteOptions,
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    range_tombstones: Vec<TableRangeTombstone>,
}

struct CompactionInput {
    keyspace: String,
    table_level: table::TableLevel,
    table_options: table::TableWriteOptions,
    input_table_ids: Vec<table::TableId>,
    input_tables: Vec<Arc<Table>>,
    index_search_policy: crate::options::IndexSearchPolicy,
}

struct CompactionOutput {
    keyspace: String,
    input_table_ids: Vec<table::TableId>,
    tables: Vec<Arc<Table>>,
}

#[derive(Debug, Default)]
struct CompactionChunk {
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    estimated_bytes: u64,
}

struct CompactionTablePayload {
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    range_tombstones: Vec<TableRangeTombstone>,
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
                    Arc::new(LsmTree::new(options, Vec::new())),
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
            .flat_map(|input| input.input_table_ids.iter().copied())
            .collect::<Vec<_>>();
        let mut written_tables = Vec::with_capacity(compaction_inputs.len());
        let mut written_table_ids = Vec::new();
        let mut next_table_id = self.next_table_id()?;
        for input in &compaction_inputs {
            let payloads = build_compaction_table_payloads(
                input,
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
                    input.table_level,
                    &input.table_options,
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
            written_tables.push(CompactionOutput {
                keyspace: input.keyspace.clone(),
                input_table_ids: input.input_table_ids.clone(),
                tables: output_tables,
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
        remove_table_files(&db_path, &obsolete_table_ids)?;
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
            let active_memtable = state
                .active_memtable
                .read()
                .map(|memtable| Arc::clone(&memtable));
            if let Ok(active_memtable) = active_memtable {
                if let Ok(entries) = active_memtable.read_entries() {
                    stats.memtable_bytes = stats
                        .memtable_bytes
                        .saturating_add(memtable_entry_bytes(&entries));
                }
            }
            if let Ok(tombstones) = state.range_tombstones.read() {
                stats.memtable_bytes = stats
                    .memtable_bytes
                    .saturating_add(memtable_tombstone_bytes(&tombstones));
            }
            if let Ok(immutable_memtables) = state.immutable_memtables.read() {
                stats.immutable_memtables = stats
                    .immutable_memtables
                    .saturating_add(immutable_memtables.len());
                for immutable in immutable_memtables.iter() {
                    if let Ok(entries) = immutable.memtable.read_entries() {
                        stats.memtable_bytes = stats
                            .memtable_bytes
                            .saturating_add(memtable_entry_bytes(&entries));
                    }
                    stats.memtable_bytes = stats
                        .memtable_bytes
                        .saturating_add(memtable_tombstone_bytes(&immutable.range_tombstones));
                }
            }
            let Ok(tables) = state.tables.read() else {
                continue;
            };

            for table in tables.iter() {
                let properties = table.properties();
                let level = properties.level.get();
                let table_bytes =
                    persistent_path.map_or(0, |db_path| table_file_bytes(db_path, properties.id));
                stats.total_tables += 1;
                stats.table_bytes = stats.table_bytes.saturating_add(table_bytes);
                if properties.level == table::TableLevel::ZERO {
                    stats.l0_tables += 1;
                }
                let level_entry = level_stats.entry(level).or_insert(LevelStats {
                    level,
                    tables: 0,
                    bytes: 0,
                });
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
        let sources = scan_sources(&state, &selector, direction, Some(&self.inner.block_cache))?;
        let range_tombstones = scan_range_tombstones(&state, &selector)?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(Iter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            range_tombstones,
            sources,
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
        let sources = scan_sources(&state, &selector, direction, Some(&self.inner.block_cache))?;
        let range_tombstones = scan_range_tombstones(&state, &selector)?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(Iter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            range_tombstones,
            sources,
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
        if self.immutable_memtable_pressure_reached()?
            && self.flush_memtables_locked(db_path, None)?
        {
            self.request_background_maintenance();
        }

        Ok(())
    }

    fn freeze_large_active_memtables_after_commit_locked(
        &self,
        sequence: Sequence,
    ) -> Result<bool> {
        let StorageMode::Persistent { .. } = self.inner.options.storage_mode else {
            return Ok(false);
        };

        if self.active_write_buffer_reached()? {
            return self
                .freeze_all_active_memtables(sequence)
                .map(|frozen_count| frozen_count != 0);
        }

        Ok(false)
    }

    fn active_write_buffer_reached(&self) -> Result<bool> {
        let threshold = usize_to_u64_saturating(self.inner.options.write_buffer_bytes);
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        for state in keyspaces.values() {
            if active_memtable_bytes(state)? >= threshold {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn immutable_memtable_pressure_reached(&self) -> Result<bool> {
        let max_immutable_memtables = self.inner.options.max_immutable_memtables;
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        for state in keyspaces.values() {
            let immutable_memtables = state
                .immutable_memtables
                .read()
                .map_err(|_| lock_poisoned("immutable memtable queue"))?;
            if immutable_memtables.len() >= max_immutable_memtables {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn has_immutable_memtables(&self) -> Result<bool> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        for state in keyspaces.values() {
            let immutable_memtables = state
                .immutable_memtables
                .read()
                .map_err(|_| lock_poisoned("immutable memtable queue"))?;
            if !immutable_memtables.is_empty() {
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
            if freeze_active_memtable(state, freeze_sequence)? {
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
        if flush_inputs.is_empty() {
            return Ok(false);
        }
        let flush_sequence = flush_inputs
            .iter()
            .map(|input| input.freeze_sequence)
            .max()
            .expect("non-empty flush input list has a max sequence");

        let mut written_tables = Vec::with_capacity(flush_inputs.len());
        let mut written_table_ids = Vec::with_capacity(flush_inputs.len());
        for input in &flush_inputs {
            let table_path = table::table_path(db_path, input.table_id);
            written_table_ids.push(input.table_id);
            let table = match table::write_table(
                &table_path,
                input.table_id,
                input.table_level,
                &input.table_options,
                &input.point_records,
                &input.range_tombstones,
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
        self.install_flushed_tables(&flush_inputs, written_tables)?;
        self.rewrite_wal_after_replay_floor(db_path, flush_sequence)?;
        self.l0_pressure_exceeded()
    }

    fn collect_flush_inputs(&self) -> Result<Vec<FlushInput>> {
        let mut next_table_id = self.next_table_id()?;
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in keyspaces.iter() {
            let immutable_memtables = state
                .immutable_memtables
                .read()
                .map_err(|_| lock_poisoned("immutable memtable queue"))?
                .clone();

            for immutable in immutable_memtables {
                let point_records = {
                    let entries = immutable
                        .memtable
                        .read_entries()
                        .map_err(|_| lock_poisoned("memtable entries"))?;
                    entries
                        .iter()
                        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
                        .collect::<Vec<_>>()
                };
                let range_tombstones = immutable
                    .range_tombstones
                    .iter()
                    .map(|tombstone| TableRangeTombstone {
                        range: tombstone.range.clone(),
                        sequence: tombstone.sequence,
                        batch_index: tombstone.batch_index,
                    })
                    .collect::<Vec<_>>();

                if point_records.is_empty() && range_tombstones.is_empty() {
                    continue;
                }

                inputs.push(FlushInput {
                    keyspace: name.clone(),
                    memtable: Arc::clone(&immutable.memtable),
                    freeze_sequence: immutable.freeze_sequence,
                    table_id: next_table_id,
                    table_level: table::TableLevel::ZERO,
                    table_options: table_write_options(&state.options),
                    point_records,
                    range_tombstones,
                });
                next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                    message: "table id counter overflow".to_owned(),
                })?;
            }
        }

        Ok(inputs)
    }

    fn collect_compaction_inputs(
        &self,
        range: &KeyRange,
        oldest_active_snapshot: Sequence,
    ) -> Result<Vec<CompactionInput>> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();
        let compaction_options = compaction_options(&self.inner.options);

        for (name, state) in keyspaces.iter() {
            let (candidate_tables, table_level) = {
                let tables = state
                    .tables
                    .read()
                    .map_err(|_| lock_poisoned("table list"))?;
                let plan_tables = tables
                    .iter()
                    .map(|table| {
                        compaction::CompactionTable::from_properties_with_bytes(
                            table.properties(),
                            table.estimated_file_bytes(),
                        )
                    })
                    .collect::<Vec<_>>();
                let Some(plan) = compaction::plan_compaction(
                    name,
                    &plan_tables,
                    range,
                    oldest_active_snapshot,
                    compaction_options,
                )?
                else {
                    continue;
                };
                let input_table_ids = plan.input_tables.iter().copied().collect::<BTreeSet<_>>();
                let candidate_tables = tables
                    .iter()
                    .filter(|table| input_table_ids.contains(&table.properties().id))
                    .cloned()
                    .collect::<Vec<_>>();
                (candidate_tables, plan.output_level)
            };

            let input_table_ids = candidate_tables
                .iter()
                .map(|table| table.properties().id)
                .collect::<Vec<_>>();

            inputs.push(CompactionInput {
                keyspace: name.clone(),
                table_level,
                table_options: table_write_options(&state.options),
                input_table_ids,
                input_tables: candidate_tables,
                index_search_policy: state.options.index_search_policy,
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

    fn publish_compacted_tables(&self, outputs: &[CompactionOutput]) -> Result<()> {
        let edits = outputs
            .iter()
            .map(|output| {
                (
                    output.keyspace.clone(),
                    output.input_table_ids.clone(),
                    output
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
        &self,
        inputs: &[FlushInput],
        tables: Vec<(String, Arc<Table>)>,
    ) -> Result<()> {
        for (input, (keyspace, table)) in inputs.iter().zip(tables) {
            debug_assert_eq!(input.keyspace, keyspace);
            let state = self.keyspace_state(&keyspace)?;
            {
                let mut tables = state
                    .tables
                    .write()
                    .map_err(|_| lock_poisoned("table list"))?;
                tables.push(table);
                LsmTree::sort_tables_for_reads(&mut tables);
            }
            let mut immutable_memtables = state
                .immutable_memtables
                .write()
                .map_err(|_| lock_poisoned("immutable memtable queue"))?;
            let Some(position) = immutable_memtables.iter().position(|immutable| {
                immutable.freeze_sequence == input.freeze_sequence
                    && Arc::ptr_eq(&immutable.memtable, &input.memtable)
            }) else {
                return Err(Error::Corruption {
                    message: "flushed immutable memtable is no longer queued".to_owned(),
                });
            };
            immutable_memtables.remove(position);
        }

        Ok(())
    }

    fn install_compacted_tables(&self, outputs: Vec<CompactionOutput>) -> Result<()> {
        for output in outputs {
            let state = self.keyspace_state(&output.keyspace)?;
            let mut tables = state
                .tables
                .write()
                .map_err(|_| lock_poisoned("table list"))?;
            tables.retain(|table| !output.input_table_ids.contains(&table.properties().id));
            for table in output.tables {
                tables.push(table);
            }
            LsmTree::sort_tables_for_reads(&mut tables);
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

    fn l0_pressure_exceeded(&self) -> Result<bool> {
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;

        for state in keyspaces.values() {
            let tables = state
                .tables
                .read()
                .map_err(|_| lock_poisoned("table list"))?;
            let l0_count = tables
                .iter()
                .filter(|table| table.properties().level == table::TableLevel::ZERO)
                .count();
            if l0_count > self.inner.options.max_l0_files {
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
            Arc::new(LsmTree::new(options.clone(), tables)),
        );
    }

    Ok(keyspaces)
}

fn active_memtable_bytes(state: &LsmTree) -> Result<u64> {
    let active_memtable = state
        .active_memtable
        .read()
        .map_err(|_| lock_poisoned("active memtable"))?
        .clone();
    let entries = active_memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let entry_bytes = memtable_entry_bytes(&entries);
    drop(entries);

    let range_tombstones = state
        .range_tombstones
        .read()
        .map_err(|_| lock_poisoned("range tombstones"))?;
    Ok(entry_bytes.saturating_add(memtable_tombstone_bytes(&range_tombstones)))
}

fn freeze_active_memtable(state: &LsmTree, freeze_sequence: Sequence) -> Result<bool> {
    // Lock order is active pointer -> active tombstones -> immutable queue.
    // Readers follow the same active-before-tombstone path, so a freeze cannot
    // leave a reader seeing the new active memtable without the frozen data.
    let mut active_memtable = state
        .active_memtable
        .write()
        .map_err(|_| lock_poisoned("active memtable"))?;
    let active = Arc::clone(&active_memtable);
    let entries_empty = {
        let entries = active
            .read_entries()
            .map_err(|_| lock_poisoned("memtable entries"))?;
        entries.is_empty()
    };
    let mut range_tombstones = state
        .range_tombstones
        .write()
        .map_err(|_| lock_poisoned("range tombstones"))?;

    if entries_empty && range_tombstones.is_empty() {
        return Ok(false);
    }

    let immutable = ImmutableMemtable {
        memtable: active,
        range_tombstones: Arc::new(range_tombstones.clone()),
        freeze_sequence,
    };
    state
        .immutable_memtables
        .write()
        .map_err(|_| lock_poisoned("immutable memtable queue"))?
        .push(immutable);

    *active_memtable = Arc::new(Memtable::default());
    range_tombstones.clear();

    Ok(true)
}

fn memtable_entry_bytes(entries: &BTreeMap<InternalKey, Option<ValueRef>>) -> u64 {
    entries
        .iter()
        .map(|(internal_key, value)| {
            let value_len = value.as_ref().map_or(0, ValueRef::len);
            usize_to_u64_saturating(internal_key.user_key().len())
                .saturating_add(value_len)
                .saturating_add(16)
        })
        .sum()
}

fn memtable_tombstone_bytes(tombstones: &[RangeTombstone]) -> u64 {
    tombstones
        .iter()
        .map(|tombstone| {
            key_range_bytes(&tombstone.range)
                .saturating_add(usize_to_u64_saturating(std::mem::size_of::<Sequence>()))
                .saturating_add(usize_to_u64_saturating(std::mem::size_of::<u32>()))
        })
        .sum()
}

fn key_range_bytes(range: &KeyRange) -> u64 {
    bound_bytes(&range.start).saturating_add(bound_bytes(&range.end))
}

fn bound_bytes(bound: &Bound<Vec<u8>>) -> u64 {
    match bound {
        Bound::Included(bytes) | Bound::Excluded(bytes) => usize_to_u64_saturating(bytes.len()),
        Bound::Unbounded => 0,
    }
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
        let tables = state
            .tables
            .read()
            .map_err(|_| lock_poisoned("table list"))?;
        for table in tables.iter() {
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

fn table_write_options(options: &KeyspaceOptions) -> table::TableWriteOptions {
    table::TableWriteOptions {
        codec: options.compression.codec_id(),
        block_bytes: options.block_bytes,
        filter_policy: options.filter_policy,
        prefix_extractor: options.prefix_extractor.clone(),
        prefix_filter_policy: options.prefix_filter_policy,
        blob_threshold_bytes: options.blob_threshold_bytes,
    }
}

fn compaction_options(options: &DbOptions) -> compaction::CompactionOptions {
    compaction::CompactionOptions {
        target_table_bytes: usize_to_u64_saturating(options.target_table_bytes),
        level_size_multiplier: usize_to_u64_saturating(options.level_size_multiplier),
        max_l0_files: options.max_l0_files,
    }
}

fn build_compaction_table_payloads(
    input: &CompactionInput,
    range: &KeyRange,
    oldest_active_snapshot: Sequence,
    target_table_bytes: usize,
) -> Result<Vec<CompactionTablePayload>> {
    let mut sources = input
        .input_tables
        .iter()
        .cloned()
        .map(|table| {
            CompactionSource::new(table.point_cursor(
                ScanSelector::Range(range.clone()),
                input.table_options.prefix_extractor.clone(),
                Direction::Forward,
                input.index_search_policy,
                None,
            ))
        })
        .collect::<Vec<_>>();
    let range_tombstones = collect_compaction_range_tombstones(input, range)?;
    let mut tombstone_has_remaining_put = vec![false; range_tombstones.len()];
    let mut chunks = Vec::new();
    let mut current_chunk = CompactionChunk::default();
    let target_table_bytes = usize_to_u64_saturating(target_table_bytes).max(1);

    while let Some(user_key) = next_compaction_user_key(&mut sources)? {
        let mut records = Vec::new();
        for source in &mut sources {
            if source.current_key()? == Some(user_key.as_slice()) {
                let group = source
                    .take_current_group()?
                    .expect("source current key must have a current group");
                records.push(group.first);
                records.extend(group.rest);
            }
        }

        let records = compact_point_record_group(records, oldest_active_snapshot);
        if records.is_empty() {
            continue;
        }
        mark_tombstones_covering_records(
            &range_tombstones,
            &mut tombstone_has_remaining_put,
            &records,
        );
        push_compaction_records_to_chunks(
            &mut chunks,
            &mut current_chunk,
            records,
            target_table_bytes,
        );
    }

    if !current_chunk.point_records.is_empty() {
        chunks.push(current_chunk);
    }

    let range_tombstones = cleanup_range_tombstones_by_coverage(
        range_tombstones,
        tombstone_has_remaining_put,
        range_is_all(range),
    );
    Ok(compaction_payloads_from_chunks(
        chunks,
        &range_tombstones,
        range_is_all(range),
        target_table_bytes,
    ))
}

fn collect_compaction_range_tombstones(
    input: &CompactionInput,
    range: &KeyRange,
) -> Result<Vec<TableRangeTombstone>> {
    let mut tombstones = Vec::new();
    for table in &input.input_tables {
        tombstones.extend(table.range_tombstones_overlapping_range(range)?);
    }
    range_tombstone::sort_tombstones(&mut tombstones);
    Ok(tombstones)
}

#[derive(Debug)]
struct CompactionSource {
    cursor: TablePointCursor,
    current: Option<RecordGroup>,
}

impl CompactionSource {
    fn new(cursor: TablePointCursor) -> Self {
        Self {
            cursor,
            current: None,
        }
    }

    fn current_key(&mut self) -> Result<Option<&[u8]>> {
        self.ensure_current()?;
        Ok(self.current.as_ref().map(|group| group.user_key.as_slice()))
    }

    fn take_current_group(&mut self) -> Result<Option<RecordGroup>> {
        self.ensure_current()?;
        Ok(self.current.take())
    }

    fn ensure_current(&mut self) -> Result<()> {
        if self.current.is_none() {
            self.current = self.cursor.next_group()?;
        }
        Ok(())
    }
}

fn next_compaction_user_key(sources: &mut [CompactionSource]) -> Result<Option<Vec<u8>>> {
    let mut selected: Option<Vec<u8>> = None;
    for source in sources {
        let Some(user_key) = source.current_key()? else {
            continue;
        };
        if selected
            .as_ref()
            .is_none_or(|selected| user_key < selected.as_slice())
        {
            selected = Some(user_key.to_vec());
        }
    }
    Ok(selected)
}

fn compact_point_record_group(
    records: Vec<(InternalKey, Option<ValueRef>)>,
    oldest_active_snapshot: Sequence,
) -> Vec<(InternalKey, Option<ValueRef>)> {
    let compacted = compact_point_records(records, oldest_active_snapshot);
    cleanup_point_tombstones(&compacted)
}

fn push_compaction_records_to_chunks(
    chunks: &mut Vec<CompactionChunk>,
    current_chunk: &mut CompactionChunk,
    records: Vec<(InternalKey, Option<ValueRef>)>,
    target_table_bytes: u64,
) {
    let record_bytes = records.iter().map(compaction_record_bytes).sum::<u64>();
    if !current_chunk.point_records.is_empty()
        && current_chunk.estimated_bytes.saturating_add(record_bytes) > target_table_bytes
    {
        chunks.push(std::mem::take(current_chunk));
    }

    current_chunk.estimated_bytes = current_chunk.estimated_bytes.saturating_add(record_bytes);
    current_chunk.point_records.extend(records);
}

fn mark_tombstones_covering_records(
    tombstones: &[TableRangeTombstone],
    tombstone_has_remaining_put: &mut [bool],
    records: &[(InternalKey, Option<ValueRef>)],
) {
    for (internal_key, _) in records {
        if !matches!(internal_key.kind(), ValueKind::Put) {
            continue;
        }
        for (index, tombstone) in tombstones.iter().enumerate() {
            if internal_key.sequence() <= tombstone.sequence
                && key_is_in_range(internal_key.user_key(), &tombstone.range)
            {
                tombstone_has_remaining_put[index] = true;
            }
        }
    }
}

fn cleanup_range_tombstones_by_coverage(
    range_tombstones: Vec<TableRangeTombstone>,
    tombstone_has_remaining_put: Vec<bool>,
    full_keyspace_compaction: bool,
) -> Vec<TableRangeTombstone> {
    if !full_keyspace_compaction {
        return range_tombstones;
    }

    range_tombstones
        .into_iter()
        .zip(tombstone_has_remaining_put)
        .filter_map(|(tombstone, keep)| keep.then_some(tombstone))
        .collect()
}

fn compaction_payloads_from_chunks(
    chunks: Vec<CompactionChunk>,
    range_tombstones: &[TableRangeTombstone],
    full_keyspace_compaction: bool,
    target_table_bytes: u64,
) -> Vec<CompactionTablePayload> {
    let mut payloads = Vec::with_capacity(chunks.len());
    let mut assigned_tombstones = vec![false; range_tombstones.len()];

    for chunk in chunks {
        let Some(span) = chunk_range(&chunk.point_records) else {
            continue;
        };
        let mut chunk_tombstones = Vec::new();
        for (index, tombstone) in range_tombstones.iter().enumerate() {
            if let Some(tombstone) =
                tombstone_for_output_span(tombstone, &span, full_keyspace_compaction)
            {
                assigned_tombstones[index] = true;
                chunk_tombstones.push(tombstone);
            }
        }

        payloads.push(CompactionTablePayload {
            point_records: chunk.point_records,
            range_tombstones: chunk_tombstones,
        });
    }

    let mut tombstone_only = Vec::new();
    let mut tombstone_only_bytes = 0_u64;
    for (index, tombstone) in range_tombstones.iter().enumerate() {
        if assigned_tombstones[index] {
            continue;
        }
        let tombstone_bytes = range_tombstone_bytes(tombstone);
        if !tombstone_only.is_empty()
            && tombstone_only_bytes.saturating_add(tombstone_bytes) > target_table_bytes
        {
            payloads.push(CompactionTablePayload {
                point_records: Vec::new(),
                range_tombstones: std::mem::take(&mut tombstone_only),
            });
            tombstone_only_bytes = 0;
        }
        tombstone_only.push(tombstone.clone());
        tombstone_only_bytes = tombstone_only_bytes.saturating_add(tombstone_bytes);
    }
    if !tombstone_only.is_empty() {
        payloads.push(CompactionTablePayload {
            point_records: Vec::new(),
            range_tombstones: tombstone_only,
        });
    }

    payloads
}

fn tombstone_for_output_span(
    tombstone: &TableRangeTombstone,
    span: &KeyRange,
    full_keyspace_compaction: bool,
) -> Option<TableRangeTombstone> {
    if full_keyspace_compaction {
        range_tombstone::range_intersection(&tombstone.range, span).map(|range| {
            TableRangeTombstone {
                range,
                sequence: tombstone.sequence,
                batch_index: tombstone.batch_index,
            }
        })
    } else if range_tombstone::ranges_overlap(&tombstone.range, span) {
        Some(tombstone.clone())
    } else {
        None
    }
}

fn chunk_range(point_records: &[(InternalKey, Option<ValueRef>)]) -> Option<KeyRange> {
    let smallest = point_records.first()?.0.user_key();
    let largest = point_records.last()?.0.user_key();
    Some(range_tombstone::range_from_inclusive_span(
        smallest, largest,
    ))
}

fn compaction_record_bytes(record: &(InternalKey, Option<ValueRef>)) -> u64 {
    usize_to_u64_saturating(record.0.user_key().len())
        .saturating_add(record.1.as_ref().map_or(0, ValueRef::len))
        .saturating_add(32)
}

fn range_tombstone_bytes(tombstone: &TableRangeTombstone) -> u64 {
    key_range_bytes(&tombstone.range)
        .saturating_add(usize_to_u64_saturating(std::mem::size_of::<Sequence>()))
        .saturating_add(usize_to_u64_saturating(std::mem::size_of::<u32>()))
}

fn compact_point_records(
    mut point_records: Vec<(InternalKey, Option<ValueRef>)>,
    oldest_active_snapshot: Sequence,
) -> Vec<(InternalKey, Option<ValueRef>)> {
    point_records.sort_by(|left, right| left.0.cmp(&right.0));

    let mut compacted = Vec::with_capacity(point_records.len());
    let mut current_user_key: Option<Vec<u8>> = None;
    let mut kept_floor_version = false;

    for record in point_records {
        if current_user_key.as_deref() != Some(record.0.user_key()) {
            current_user_key = Some(record.0.user_key().to_vec());
            kept_floor_version = false;
        }

        // Keep all versions newer than the oldest active snapshot, because a
        // later reader may still need them. At or below the oldest active
        // snapshot, only the newest record for that user key can be observed.
        if record.0.sequence() > oldest_active_snapshot {
            compacted.push(record);
        } else if !kept_floor_version {
            compacted.push(record);
            kept_floor_version = true;
        }
    }

    compacted
}

fn cleanup_point_tombstones(
    point_records: &[(InternalKey, Option<ValueRef>)],
) -> Vec<(InternalKey, Option<ValueRef>)> {
    let mut compacted = Vec::with_capacity(point_records.len());
    let mut index = 0;

    while index < point_records.len() {
        let user_key = point_records[index].0.user_key();
        let group_end = point_records[index..]
            .partition_point(|(internal_key, _)| internal_key.user_key() == user_key)
            + index;

        for record_index in index..group_end {
            let (internal_key, _) = &point_records[record_index];
            if matches!(internal_key.kind(), ValueKind::PointDelete)
                && !has_older_point_record(point_records, record_index, group_end)
            {
                continue;
            }
            compacted.push(point_records[record_index].clone());
        }

        index = group_end;
    }

    compacted
}

fn has_older_point_record(
    point_records: &[(InternalKey, Option<ValueRef>)],
    tombstone_index: usize,
    group_end: usize,
) -> bool {
    let tombstone_sequence = point_records[tombstone_index].0.sequence();
    point_records[tombstone_index + 1..group_end]
        .iter()
        .any(|(internal_key, _)| internal_key.sequence() <= tombstone_sequence)
}

#[cfg(test)]
fn cleanup_range_tombstones(
    range_tombstones: Vec<TableRangeTombstone>,
    point_records: &[(InternalKey, Option<ValueRef>)],
    full_keyspace_compaction: bool,
) -> Vec<TableRangeTombstone> {
    // Partial compaction cannot prove there is no older covered data just
    // outside its input tables. Keep range tombstones there and only clean them
    // when the whole keyspace participates in this compaction pass.
    if !full_keyspace_compaction {
        return range_tombstones;
    }

    range_tombstones
        .into_iter()
        .filter(|tombstone| range_tombstone_covers_remaining_put(tombstone, point_records))
        .collect()
}

#[cfg(test)]
fn range_tombstone_covers_remaining_put(
    tombstone: &TableRangeTombstone,
    point_records: &[(InternalKey, Option<ValueRef>)],
) -> bool {
    point_records.iter().any(|(internal_key, _)| {
        matches!(internal_key.kind(), ValueKind::Put)
            && internal_key.sequence() <= tombstone.sequence
            && key_is_in_range(internal_key.user_key(), &tombstone.range)
    })
}

fn validate_batch_len(len: usize) -> Result<()> {
    if len > u32::MAX as usize {
        return Err(Error::InvalidOptions {
            message: "write batch operation count exceeds u32::MAX".to_owned(),
        });
    }

    Ok(())
}

fn scan_sources(
    state: &LsmTree,
    selector: &ScanSelector,
    direction: Direction,
    block_cache: Option<&Arc<cache::BlockCache>>,
) -> Result<Vec<RecordSource>> {
    let active_memtable = state
        .active_memtable
        .read()
        .map_err(|_| lock_poisoned("active memtable"))?
        .clone();

    let mut sources = vec![RecordSource::memtable(
        active_memtable,
        selector.clone(),
        direction,
    )];
    let immutable_memtables = state
        .immutable_memtables
        .read()
        .map_err(|_| lock_poisoned("immutable memtable queue"))?
        .clone();
    sources.extend(
        immutable_memtables.into_iter().map(|immutable| {
            RecordSource::memtable(immutable.memtable, selector.clone(), direction)
        }),
    );

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        if let Some(prefix) = selector.prefix() {
            if !table.may_contain_prefix(prefix, &state.options.prefix_extractor) {
                continue;
            }
        }

        let cursor = Arc::clone(table).point_cursor(
            selector.clone(),
            state.options.prefix_extractor.clone(),
            direction,
            state.options.index_search_policy,
            block_cache.cloned(),
        );
        sources.push(RecordSource::table(cursor));
    }

    Ok(sources)
}

fn collect_point_key_records(
    state: &LsmTree,
    key: &[u8],
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let active_memtable = state
        .active_memtable
        .read()
        .map_err(|_| lock_poisoned("active memtable"))?
        .clone();
    let entries = active_memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let mut records = entries
        .iter()
        .filter(|(internal_key, _)| internal_key.user_key() == key)
        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
        .collect::<Vec<_>>();
    drop(entries);
    let immutable_memtables = state
        .immutable_memtables
        .read()
        .map_err(|_| lock_poisoned("immutable memtable queue"))?
        .clone();
    for immutable in immutable_memtables {
        let entries = immutable
            .memtable
            .read_entries()
            .map_err(|_| lock_poisoned("memtable entries"))?;
        records.extend(
            entries
                .iter()
                .filter(|(internal_key, _)| internal_key.user_key() == key)
                .map(|(internal_key, value)| (internal_key.clone(), value.clone())),
        );
    }

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        if !table.may_contain_key(key) {
            continue;
        }
        records.extend(
            table
                .point_records_for_key_with_cache(
                    key,
                    state.options.index_search_policy,
                    block_cache,
                )?
                .into_iter()
                .map(|record| (record.internal_key, record.value)),
        );
    }
    records.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(records)
}

fn collect_range_point_records(
    state: &LsmTree,
    range: &KeyRange,
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let active_memtable = state
        .active_memtable
        .read()
        .map_err(|_| lock_poisoned("active memtable"))?
        .clone();
    let entries = active_memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let mut records = entries
        .iter()
        .filter(|(internal_key, _)| key_is_in_range(internal_key.user_key(), range))
        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
        .collect::<Vec<_>>();
    drop(entries);
    let immutable_memtables = state
        .immutable_memtables
        .read()
        .map_err(|_| lock_poisoned("immutable memtable queue"))?
        .clone();
    for immutable in immutable_memtables {
        let entries = immutable
            .memtable
            .read_entries()
            .map_err(|_| lock_poisoned("memtable entries"))?;
        records.extend(
            entries
                .iter()
                .filter(|(internal_key, _)| key_is_in_range(internal_key.user_key(), range))
                .map(|(internal_key, value)| (internal_key.clone(), value.clone())),
        );
    }

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        records.extend(
            table
                .point_records_in_range_with_cache(
                    range,
                    state.options.index_search_policy,
                    block_cache,
                )?
                .into_iter()
                .map(|record| (record.internal_key, record.value)),
        );
    }
    records.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(records)
}

fn scan_range_tombstones(
    state: &LsmTree,
    selector: &ScanSelector,
) -> Result<Vec<ScanRangeTombstone>> {
    let range = selector_query_range(selector);
    let memtable_tombstones = state.memtable_range_tombstones()?;
    let mut tombstones = memtable_tombstones
        .overlapping_range(&range)
        .cloned()
        .collect::<Vec<_>>();

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        tombstones.extend(
            table
                .range_tombstones_overlapping_range(&range)?
                .into_iter()
                .map(|tombstone| RangeTombstone {
                    range: tombstone.range,
                    sequence: tombstone.sequence,
                    batch_index: tombstone.batch_index,
                }),
        );
    }

    Ok(RangeTombstoneIndex::new(tombstones)
        .all()
        .iter()
        .cloned()
        .map(|tombstone| {
            ScanRangeTombstone::new(tombstone.range, tombstone.sequence, tombstone.batch_index)
        })
        .collect())
}

fn selector_query_range(selector: &ScanSelector) -> KeyRange {
    match selector {
        ScanSelector::Range(range) => range.clone(),
        ScanSelector::Prefix(prefix) => KeyRange {
            start: Bound::Included(prefix.clone()),
            end: prefix_successor(prefix).map_or(Bound::Unbounded, Bound::Excluded),
        },
    }
}

fn point_key_modified_after(state: &LsmTree, key: &[u8], read_sequence: Sequence) -> Result<bool> {
    // A point read is invalidated by either a newer point record for that user
    // key or a newer range tombstone covering it.
    for (internal_key, _) in collect_point_key_records(state, key, None)? {
        if internal_key.sequence() > read_sequence {
            return Ok(true);
        }
    }

    range_tombstone_modified_after_key(state, key, read_sequence)
}

fn key_range_modified_after(
    state: &LsmTree,
    range: &KeyRange,
    read_sequence: Sequence,
) -> Result<bool> {
    // A range read is invalidated by any newer point record inside the range or
    // any newer range tombstone whose bounds overlap the range read.
    for (internal_key, _) in collect_range_point_records(state, range, None)? {
        if internal_key.sequence() > read_sequence {
            return Ok(true);
        }
    }

    range_tombstone_modified_after_range(state, range, read_sequence)
}

fn key_is_before_start(key: &[u8], start: &Bound<Vec<u8>>) -> bool {
    match start {
        Bound::Included(start) => key < start.as_slice(),
        Bound::Excluded(start) => key <= start.as_slice(),
        Bound::Unbounded => false,
    }
}

fn key_is_after_end(key: &[u8], end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Included(end) => key > end.as_slice(),
        Bound::Excluded(end) => key >= end.as_slice(),
        Bound::Unbounded => false,
    }
}

fn key_is_in_range(key: &[u8], range: &KeyRange) -> bool {
    !key_is_before_start(key, &range.start) && !key_is_after_end(key, &range.end)
}

fn range_is_all(range: &KeyRange) -> bool {
    matches!(
        (&range.start, &range.end),
        (Bound::Unbounded, Bound::Unbounded)
    )
}

fn range_tombstone_modified_after_key(
    state: &LsmTree,
    key: &[u8],
    read_sequence: Sequence,
) -> Result<bool> {
    let memtable_tombstones = state.memtable_range_tombstones()?;
    if memtable_tombstones
        .covering_key(key)
        .any(|tombstone| tombstone.sequence > read_sequence)
    {
        return Ok(true);
    }

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        let tombstones = table.range_tombstones()?;
        if tombstones
            .covering_key(key)
            .any(|tombstone| tombstone.sequence > read_sequence)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn range_tombstone_modified_after_range(
    state: &LsmTree,
    range: &KeyRange,
    read_sequence: Sequence,
) -> Result<bool> {
    let memtable_tombstones = state.memtable_range_tombstones()?;
    if memtable_tombstones
        .overlapping_range(range)
        .any(|tombstone| tombstone.sequence > read_sequence)
    {
        return Ok(true);
    }

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        if table
            .range_tombstones_overlapping_range(range)?
            .into_iter()
            .any(|tombstone| tombstone.sequence > read_sequence)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn apply_memtable_operation(
    state: &LsmTree,
    operation: BatchOperation,
    sequence: Sequence,
    batch_index: u32,
) -> Result<()> {
    let active_memtable = state
        .active_memtable
        .read()
        .map_err(|_| lock_poisoned("active memtable"))?
        .clone();
    let mut entries = active_memtable
        .write_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;

    match operation {
        BatchOperation::Insert { key, value, .. } => {
            entries.insert(
                InternalKey::new(key, sequence, ValueKind::Put, batch_index),
                Some(ValueRef::Inline(value)),
            );
        }
        BatchOperation::Remove { key, .. } => {
            entries.insert(
                InternalKey::new(key, sequence, ValueKind::PointDelete, batch_index),
                None,
            );
        }
        BatchOperation::RemoveRange { range, .. } => {
            // Range tombstones live beside point records for now. Drop the
            // point-record lock before taking the tombstone lock so readers and
            // writers keep one simple lock order.
            drop(entries);
            let mut range_tombstones = state
                .range_tombstones
                .write()
                .map_err(|_| lock_poisoned("range tombstones"))?;
            range_tombstone::insert_sorted(
                &mut range_tombstones,
                RangeTombstone {
                    range,
                    sequence,
                    batch_index,
                },
            );
        }
    }

    Ok(())
}

fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
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
    use super::{
        Error, InternalKey, MaintenanceCoordinator, TableRangeTombstone, ValueKind, ValueRef,
        cleanup_point_tombstones, cleanup_range_tombstones, compact_point_records,
        record_maintenance_success,
    };
    use crate::types::{KeyRange, Sequence};

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

    #[test]
    fn compaction_keeps_newer_versions_and_snapshot_floor() {
        let compacted = compact_point_records(
            vec![
                record("a", 1),
                record("a", 3),
                record("a", 2),
                record("b", 1),
                record("b", 2),
            ],
            Sequence::new(2),
        );

        assert_eq!(
            record_sequences(&compacted),
            vec![("a", 3), ("a", 2), ("b", 2)]
        );
    }

    #[test]
    fn compaction_without_old_snapshot_keeps_only_newest_record_per_key() {
        let compacted = compact_point_records(
            vec![
                record("a", 1),
                record("a", 4),
                record("a", 3),
                tombstone("b", 2),
                record("b", 1),
            ],
            Sequence::new(4),
        );

        assert_eq!(record_sequences(&compacted), vec![("a", 4), ("b", 2)]);
        assert!(matches!(compacted[1].0.kind(), ValueKind::PointDelete));
    }

    #[test]
    fn point_tombstone_cleanup_drops_delete_after_older_records_are_removed() {
        let compacted =
            compact_point_records(vec![tombstone("a", 2), record("a", 1)], Sequence::new(2));

        assert!(cleanup_point_tombstones(&compacted).is_empty());
    }

    #[test]
    fn point_tombstone_cleanup_keeps_delete_while_older_record_remains() {
        let compacted =
            compact_point_records(vec![tombstone("a", 3), record("a", 1)], Sequence::new(1));

        assert_eq!(
            record_sequences(&cleanup_point_tombstones(&compacted)),
            vec![("a", 3), ("a", 1)]
        );
    }

    #[test]
    fn range_tombstone_cleanup_keeps_tombstone_covering_remaining_put() {
        let tombstones =
            cleanup_range_tombstones(vec![range_tombstone("a", "c", 2)], &[record("b", 1)], true);

        assert_eq!(tombstones.len(), 1);
    }

    #[test]
    fn range_tombstone_cleanup_drops_tombstone_without_remaining_put() {
        let tombstones = cleanup_range_tombstones(
            vec![range_tombstone("a", "c", 2)],
            &[record("b", 3), record("z", 1)],
            true,
        );

        assert!(tombstones.is_empty());
    }

    #[test]
    fn range_tombstone_cleanup_keeps_tombstone_for_partial_compaction() {
        let tombstones = cleanup_range_tombstones(vec![range_tombstone("a", "c", 2)], &[], false);

        assert_eq!(tombstones.len(), 1);
    }

    fn record(key: &str, sequence: u64) -> (InternalKey, Option<ValueRef>) {
        (
            InternalKey::new(key, Sequence::new(sequence), ValueKind::Put, 0),
            Some(ValueRef::Inline(format!("{key}-{sequence}").into_bytes())),
        )
    }

    fn tombstone(key: &str, sequence: u64) -> (InternalKey, Option<ValueRef>) {
        (
            InternalKey::new(key, Sequence::new(sequence), ValueKind::PointDelete, 0),
            None,
        )
    }

    fn range_tombstone(start: &str, end: &str, sequence: u64) -> TableRangeTombstone {
        TableRangeTombstone {
            range: KeyRange::half_open(start.as_bytes(), end.as_bytes()),
            sequence: Sequence::new(sequence),
            batch_index: 0,
        }
    }

    fn record_sequences(records: &[(InternalKey, Option<ValueRef>)]) -> Vec<(&str, u64)> {
        records
            .iter()
            .map(|(internal_key, _)| {
                (
                    std::str::from_utf8(internal_key.user_key()).expect("test key is UTF-8"),
                    internal_key.sequence().get(),
                )
            })
            .collect()
    }
}
