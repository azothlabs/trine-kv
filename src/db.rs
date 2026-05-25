use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, BTreeSet},
    fs,
    ops::Bound,
    path::Path,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::{
    blob::{self, ValueRef},
    cache, compaction,
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    iterator::{Direction, Iter},
    keyspace::{Keyspace, KeyspaceName},
    manifest::{self, ManifestState, ManifestStore},
    options::{DbOptions, DurabilityMode, FailOnCorruptionPolicy, KeyspaceOptions, StorageMode},
    recovery,
    snapshot::{Snapshot, SnapshotTracker},
    stats::{DbStats, LevelStats},
    table::{self, Table, TableRangeTombstone},
    transaction::{Transaction, TransactionOptions},
    types::{KeyRange, KeyValue, Sequence},
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
    keyspaces: RwLock<BTreeMap<String, Arc<KeyspaceState>>>,
    snapshots: Arc<SnapshotTracker>,
    manifest: Option<Mutex<ManifestStore>>,
    wal: Option<Mutex<WalWriter>>,
    block_cache: cache::BlockCache,
    compaction_runs: AtomicU64,
    compaction_input_tables: AtomicU64,
    compaction_output_tables: AtomicU64,
    compaction_input_bytes: AtomicU64,
    compaction_output_bytes: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct KeyspaceState {
    options: KeyspaceOptions,
    entries: RwLock<BTreeMap<InternalKey, Option<ValueRef>>>,
    range_tombstones: RwLock<Vec<RangeTombstone>>,
    tables: RwLock<Vec<Arc<Table>>>,
}

impl KeyspaceState {
    fn new(options: KeyspaceOptions, mut tables: Vec<Arc<Table>>) -> Self {
        sort_tables_for_reads(&mut tables);
        Self {
            options,
            entries: RwLock::new(BTreeMap::new()),
            range_tombstones: RwLock::new(Vec::new()),
            tables: RwLock::new(tables),
        }
    }
}

#[derive(Debug, Clone)]
struct RangeTombstone {
    range: KeyRange,
    sequence: Sequence,
    batch_index: u32,
}

impl RangeTombstone {
    fn covers_visible_point(
        &self,
        key: &[u8],
        point_sequence: Sequence,
        point_batch_index: u32,
        read_sequence: Sequence,
    ) -> bool {
        if self.sequence > read_sequence || !key_is_in_range(key, &self.range) {
            return false;
        }

        self.sequence > point_sequence
            || (self.sequence == point_sequence && self.batch_index > point_batch_index)
    }
}

struct FlushInput {
    keyspace: String,
    table_id: table::TableId,
    table_level: table::TableLevel,
    table_options: table::TableWriteOptions,
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    range_tombstones: Vec<TableRangeTombstone>,
}

struct CompactionInput {
    keyspace: String,
    table_id: table::TableId,
    table_level: table::TableLevel,
    table_options: table::TableWriteOptions,
    input_table_ids: Vec<table::TableId>,
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    range_tombstones: Vec<TableRangeTombstone>,
}

struct CompactionOutput {
    keyspace: String,
    input_table_ids: Vec<table::TableId>,
    table: Option<Arc<Table>>,
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
                block_cache: cache::BlockCache::new(block_cache_bytes),
                compaction_runs: AtomicU64::new(0),
                compaction_input_tables: AtomicU64::new(0),
                compaction_output_tables: AtomicU64::new(0),
                compaction_input_bytes: AtomicU64::new(0),
                compaction_output_bytes: AtomicU64::new(0),
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
            &referenced_blob_file_ids(&keyspaces)?,
        )?;

        let wal_path = wal::wal_path(path);
        let batches = wal::read_batches(&wal_path)?;
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
                block_cache: cache::BlockCache::new(block_cache_bytes),
                compaction_runs: AtomicU64::new(0),
                compaction_input_tables: AtomicU64::new(0),
                compaction_output_tables: AtomicU64::new(0),
                compaction_input_bytes: AtomicU64::new(0),
                compaction_output_bytes: AtomicU64::new(0),
            }),
        };
        db.replay_wal_batches(batches, replay_floor)?;

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
                    Arc::new(KeyspaceState::new(options, Vec::new())),
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

        let StorageMode::Persistent { path } = &self.inner.options.storage_mode else {
            return Ok(());
        };
        let db_path = path.clone();
        let flush_sequence = self.last_committed_sequence();

        let should_compact = {
            // Flush holds the writer coordinator while it copies and clears
            // memtables. That gives the manifest edit and the in-memory table
            // list one clear cutover point relative to commits.
            let _writer = self
                .inner
                .writer
                .lock()
                .map_err(|_| lock_poisoned("writer coordinator"))?;
            let flush_inputs = self.collect_flush_inputs()?;
            if flush_inputs.is_empty() {
                return Ok(());
            }

            let mut written_tables = Vec::with_capacity(flush_inputs.len());
            let mut written_table_ids = Vec::with_capacity(flush_inputs.len());
            for input in &flush_inputs {
                let table_path = table::table_path(&db_path, input.table_id);
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
                        let _ = remove_storage_files(&db_path, &written_table_ids);
                        return Err(error);
                    }
                };
                written_tables.push((input.keyspace.clone(), Arc::new(table)));
            }

            if let Err(error) = self.publish_flushed_tables(&written_tables, flush_sequence) {
                let _ = remove_storage_files(&db_path, &written_table_ids);
                return Err(error);
            }
            self.install_flushed_tables(&flush_inputs, written_tables)?;
            self.l0_pressure_exceeded()?
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
        let mut written_table_ids = Vec::with_capacity(compaction_inputs.len());
        for input in &compaction_inputs {
            let table = if input.point_records.is_empty() && input.range_tombstones.is_empty() {
                None
            } else {
                let table_path = table::table_path(&db_path, input.table_id);
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
                        let _ = remove_storage_files(&db_path, &written_table_ids);
                        return Err(error);
                    }
                };
                Some(Arc::new(table))
            };
            written_tables.push(CompactionOutput {
                keyspace: input.keyspace.clone(),
                input_table_ids: input.input_table_ids.clone(),
                table,
            });
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
            if let Ok(entries) = state.entries.read() {
                stats.memtable_bytes = stats
                    .memtable_bytes
                    .saturating_add(memtable_entry_bytes(&entries));
            }
            if let Ok(tombstones) = state.range_tombstones.read() {
                stats.memtable_bytes = stats
                    .memtable_bytes
                    .saturating_add(memtable_tombstone_bytes(&tombstones));
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

                for record in table.point_records() {
                    if let Some(ValueRef::Blob { file_id, len, .. }) = &record.value {
                        live_blob_bytes_by_file
                            .entry(*file_id)
                            .and_modify(|bytes| *bytes = bytes.saturating_add(*len))
                            .or_insert(*len);
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

    pub(crate) fn get_at(
        &self,
        keyspace: &str,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        self.ensure_open()?;
        let _read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.keyspace_state(keyspace)?;
        Ok(collect_visible_point(
            &state,
            key,
            read_sequence,
            self.persistent_path(),
            Some(&self.inner.block_cache),
        )?
        .into_iter()
        .next()
        .map(|item| item.value))
    }

    pub(crate) fn range_at(
        &self,
        keyspace: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let _read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.keyspace_state(keyspace)?;
        let items = collect_visible_range(
            &state,
            range,
            read_sequence,
            self.persistent_path(),
            Some(&self.inner.block_cache),
        )?;

        Ok(Iter::from_items(items, direction))
    }

    pub(crate) fn prefix_at(
        &self,
        keyspace: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let _read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.keyspace_state(keyspace)?;
        let mut items = collect_visible_prefix(
            &state,
            prefix,
            read_sequence,
            self.persistent_path(),
            Some(&self.inner.block_cache),
        )?;
        items.retain(|item| item.key.starts_with(prefix));

        Ok(Iter::from_items(items, direction))
    }

    fn keyspace_state(&self, keyspace: &str) -> Result<Arc<KeyspaceState>> {
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

    fn resolve_batch_keyspaces(
        &self,
        operations: &[BatchOperation],
    ) -> Result<Vec<Arc<KeyspaceState>>> {
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

    fn collect_flush_inputs(&self) -> Result<Vec<FlushInput>> {
        let mut next_table_id = self.next_table_id()?;
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in keyspaces.iter() {
            let point_records = {
                let entries = state
                    .entries
                    .read()
                    .map_err(|_| lock_poisoned("memtable entries"))?;
                entries
                    .iter()
                    .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
                    .collect::<Vec<_>>()
            };
            let range_tombstones = {
                let tombstones = state
                    .range_tombstones
                    .read()
                    .map_err(|_| lock_poisoned("range tombstones"))?;
                tombstones
                    .iter()
                    .map(|tombstone| TableRangeTombstone {
                        range: tombstone.range.clone(),
                        sequence: tombstone.sequence,
                        batch_index: tombstone.batch_index,
                    })
                    .collect::<Vec<_>>()
            };

            if point_records.is_empty() && range_tombstones.is_empty() {
                continue;
            }

            inputs.push(FlushInput {
                keyspace: name.clone(),
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

        Ok(inputs)
    }

    fn collect_compaction_inputs(
        &self,
        range: &KeyRange,
        oldest_active_snapshot: Sequence,
    ) -> Result<Vec<CompactionInput>> {
        let mut next_table_id = self.next_table_id()?;
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in keyspaces.iter() {
            let (candidate_tables, table_level) = {
                let tables = state
                    .tables
                    .read()
                    .map_err(|_| lock_poisoned("table list"))?;
                let plan_tables = tables
                    .iter()
                    .map(|table| compaction::CompactionTable::from_properties(table.properties()))
                    .collect::<Vec<_>>();
                let Some(plan) =
                    compaction::plan_compaction(name, &plan_tables, range, oldest_active_snapshot)?
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

            let mut input_table_ids = Vec::with_capacity(candidate_tables.len());
            let mut point_records = Vec::new();
            let mut range_tombstones = Vec::new();
            for table in &candidate_tables {
                input_table_ids.push(table.properties().id);
                point_records.extend(
                    table
                        .point_records()
                        .iter()
                        .map(|record| (record.internal_key.clone(), record.value.clone())),
                );
                range_tombstones.extend(table.range_tombstones().iter().cloned());
            }
            let point_records = compact_point_records(point_records, oldest_active_snapshot);
            let point_records = cleanup_point_tombstones(&point_records);
            let range_tombstones =
                cleanup_range_tombstones(range_tombstones, &point_records, range_is_all(range));

            inputs.push(CompactionInput {
                keyspace: name.clone(),
                table_id: next_table_id,
                table_level,
                table_options: table_write_options(&state.options),
                input_table_ids,
                point_records,
                range_tombstones,
            });
            next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                message: "table id counter overflow".to_owned(),
            })?;
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
                        .table
                        .as_ref()
                        .map(|table| table.properties().clone()),
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

    fn install_flushed_tables(
        &self,
        inputs: &[FlushInput],
        tables: Vec<(String, Arc<Table>)>,
    ) -> Result<()> {
        for (input, (keyspace, table)) in inputs.iter().zip(tables) {
            debug_assert_eq!(input.keyspace, keyspace);
            let state = self.keyspace_state(&keyspace)?;
            let mut tables = state
                .tables
                .write()
                .map_err(|_| lock_poisoned("table list"))?;
            tables.push(table);
            sort_tables_for_reads(&mut tables);
            state
                .entries
                .write()
                .map_err(|_| lock_poisoned("memtable entries"))?
                .clear();
            state
                .range_tombstones
                .write()
                .map_err(|_| lock_poisoned("range tombstones"))?
                .clear();
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
            if let Some(table) = output.table {
                tables.push(table);
            }
            sort_tables_for_reads(&mut tables);
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

fn keyspaces_from_manifest(
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<BTreeMap<String, Arc<KeyspaceState>>> {
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
            validate_table_blob_refs(db_path, &table)?;
            tables.push(Arc::new(table));
        }

        keyspaces.insert(
            name.clone(),
            Arc::new(KeyspaceState::new(options.clone(), tables)),
        );
    }

    Ok(keyspaces)
}

fn validate_table_blob_refs(db_path: &Path, table: &Table) -> Result<()> {
    for record in table.point_records() {
        if let Some(value @ ValueRef::Blob { .. }) = record.value.as_ref() {
            crate::blob::read_value(db_path, value)?;
        }
    }

    Ok(())
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

fn sort_tables_for_reads(tables: &mut [Arc<Table>]) {
    // Keep table handles in level order. Reads still merge candidate records
    // defensively, but this invariant gives optimized point reads and
    // compaction picking one stable rule to share.
    tables.sort_by(compare_tables_for_reads);
}

fn compare_tables_for_reads(left: &Arc<Table>, right: &Arc<Table>) -> CmpOrdering {
    let left = left.properties();
    let right = right.properties();
    left.level
        .cmp(&right.level)
        .then_with(|| right.largest_sequence.cmp(&left.largest_sequence))
        .then_with(|| right.id.cmp(&left.id))
}

fn referenced_table_file_ids(manifest: &ManifestState) -> BTreeSet<table::TableId> {
    manifest
        .tables()
        .values()
        .flat_map(|tables| tables.iter().map(|properties| properties.id))
        .collect()
}

fn referenced_blob_file_ids(
    keyspaces: &BTreeMap<String, Arc<KeyspaceState>>,
) -> Result<BTreeSet<u64>> {
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
    if options.blob_threshold_bytes == 0 {
        return Err(Error::invalid_options("blob threshold must be non-zero"));
    }

    Ok(())
}

fn table_write_options(options: &KeyspaceOptions) -> table::TableWriteOptions {
    table::TableWriteOptions {
        codec: options.compression.codec_id(),
        filter_policy: options.filter_policy,
        prefix_extractor: options.prefix_extractor.clone(),
        prefix_filter_policy: options.prefix_filter_policy,
        blob_threshold_bytes: options.blob_threshold_bytes,
    }
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

fn collect_point_key_records(
    state: &KeyspaceState,
    key: &[u8],
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let entries = state
        .entries
        .read()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let mut records = entries
        .iter()
        .filter(|(internal_key, _)| internal_key.user_key() == key)
        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
        .collect::<Vec<_>>();
    drop(entries);

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
                )
                .into_iter()
                .map(|record| (record.internal_key.clone(), record.value.clone())),
        );
    }
    records.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(records)
}

fn collect_range_point_records(
    state: &KeyspaceState,
    range: &KeyRange,
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let entries = state
        .entries
        .read()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let mut records = entries
        .iter()
        .filter(|(internal_key, _)| key_is_in_range(internal_key.user_key(), range))
        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
        .collect::<Vec<_>>();
    drop(entries);

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
                )
                .into_iter()
                .map(|record| (record.internal_key.clone(), record.value.clone())),
        );
    }
    records.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(records)
}

fn collect_prefix_point_records(
    state: &KeyspaceState,
    prefix: &[u8],
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let entries = state
        .entries
        .read()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let mut records = entries
        .iter()
        .filter(|(internal_key, _)| internal_key.user_key().starts_with(prefix))
        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
        .collect::<Vec<_>>();
    drop(entries);

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        if !table.may_contain_prefix(prefix, &state.options.prefix_extractor) {
            continue;
        }
        records.extend(
            table
                .point_records_with_prefix_with_cache(
                    prefix,
                    &state.options.prefix_extractor,
                    state.options.index_search_policy,
                    block_cache,
                )
                .into_iter()
                .map(|record| (record.internal_key.clone(), record.value.clone())),
        );
    }
    records.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(records)
}

fn collect_range_tombstones(state: &KeyspaceState) -> Result<Vec<RangeTombstone>> {
    let range_tombstones = state
        .range_tombstones
        .read()
        .map_err(|_| lock_poisoned("range tombstones"))?;
    let mut tombstones = range_tombstones.clone();
    drop(range_tombstones);

    let tables = state
        .tables
        .read()
        .map_err(|_| lock_poisoned("table list"))?;
    for table in tables.iter() {
        tombstones.extend(
            table
                .range_tombstones()
                .iter()
                .map(|tombstone| RangeTombstone {
                    range: tombstone.range.clone(),
                    sequence: tombstone.sequence,
                    batch_index: tombstone.batch_index,
                }),
        );
    }

    Ok(tombstones)
}

fn point_key_modified_after(
    state: &KeyspaceState,
    key: &[u8],
    read_sequence: Sequence,
) -> Result<bool> {
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
    state: &KeyspaceState,
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

// This scan is deliberately small-scope: it applies the same user-visible MVCC
// rule that table readers and merge iterators must later share. The first
// visible internal record for a user key decides whether that key is returned.
fn collect_visible_range(
    state: &KeyspaceState,
    range: &KeyRange,
    read_sequence: Sequence,
    db_path: Option<&Path>,
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<KeyValue>> {
    let point_records = collect_range_point_records(state, range, block_cache)?;
    let range_tombstones = collect_range_tombstones(state)?;
    collect_visible_records(
        &point_records,
        &range_tombstones,
        range,
        read_sequence,
        db_path,
    )
}

fn collect_visible_point(
    state: &KeyspaceState,
    key: &[u8],
    read_sequence: Sequence,
    db_path: Option<&Path>,
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<KeyValue>> {
    let point_records = collect_point_key_records(state, key, block_cache)?;
    let range_tombstones = collect_range_tombstones(state)?;
    let point_range = KeyRange {
        start: Bound::Included(key.to_vec()),
        end: Bound::Included(key.to_vec()),
    };
    collect_visible_records(
        &point_records,
        &range_tombstones,
        &point_range,
        read_sequence,
        db_path,
    )
}

fn collect_visible_prefix(
    state: &KeyspaceState,
    prefix: &[u8],
    read_sequence: Sequence,
    db_path: Option<&Path>,
    block_cache: Option<&cache::BlockCache>,
) -> Result<Vec<KeyValue>> {
    let point_records = collect_prefix_point_records(state, prefix, block_cache)?;
    let range_tombstones = collect_range_tombstones(state)?;
    collect_visible_records(
        &point_records,
        &range_tombstones,
        &KeyRange::all(),
        read_sequence,
        db_path,
    )
}

// Prefix filters may remove table point records from the input set, but they
// never remove range tombstones. This helper is the single MVCC visibility path
// for normal range scans and prefix scans after table selection is finished.
fn collect_visible_records(
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[RangeTombstone],
    range: &KeyRange,
    read_sequence: Sequence,
    db_path: Option<&Path>,
) -> Result<Vec<KeyValue>> {
    let mut items = Vec::new();
    let mut decided_user_key: Option<Vec<u8>> = None;

    for (internal_key, value) in point_records {
        let user_key = internal_key.user_key();

        // Internal keys are sorted by user key ascending, then newest visible
        // version first. Once a visible record decides a user key, older
        // versions for that same key cannot change the scan result.
        if decided_user_key.as_deref() == Some(user_key) {
            continue;
        }
        if key_is_before_start(user_key, &range.start) {
            continue;
        }
        if key_is_after_end(user_key, &range.end) {
            break;
        }
        if internal_key.sequence() > read_sequence {
            continue;
        }

        match internal_key.kind() {
            ValueKind::Put => {
                if !range_tombstones_cover(
                    range_tombstones,
                    user_key,
                    internal_key.sequence(),
                    internal_key.batch_index(),
                    read_sequence,
                ) {
                    items.push(KeyValue::new(
                        user_key.to_vec(),
                        value_bytes(value.as_ref(), db_path)?,
                    ));
                }
                decided_user_key = Some(user_key.to_vec());
            }
            ValueKind::PointDelete => {
                decided_user_key = Some(user_key.to_vec());
            }
            ValueKind::RangeDelete => {}
        }
    }

    Ok(items)
}

fn value_bytes(value: Option<&ValueRef>, db_path: Option<&Path>) -> Result<Vec<u8>> {
    let value = value.ok_or_else(|| Error::Corruption {
        message: "put record is missing value bytes".to_owned(),
    })?;

    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::Blob { .. } => {
            let db_path = db_path.ok_or_else(|| Error::Corruption {
                message: "in-memory database cannot read blob value references".to_owned(),
            })?;
            crate::blob::read_value(db_path, value)
        }
    }
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

fn range_tombstones_cover(
    range_tombstones: &[RangeTombstone],
    key: &[u8],
    point_sequence: Sequence,
    point_batch_index: u32,
    read_sequence: Sequence,
) -> bool {
    range_tombstones.iter().any(|tombstone| {
        tombstone.covers_visible_point(key, point_sequence, point_batch_index, read_sequence)
    })
}

fn range_tombstone_modified_after_key(
    state: &KeyspaceState,
    key: &[u8],
    read_sequence: Sequence,
) -> Result<bool> {
    let range_tombstones = collect_range_tombstones(state)?;

    Ok(range_tombstones.iter().any(|tombstone| {
        tombstone.sequence > read_sequence && key_is_in_range(key, &tombstone.range)
    }))
}

fn range_tombstone_modified_after_range(
    state: &KeyspaceState,
    range: &KeyRange,
    read_sequence: Sequence,
) -> Result<bool> {
    let range_tombstones = collect_range_tombstones(state)?;

    Ok(range_tombstones.iter().any(|tombstone| {
        tombstone.sequence > read_sequence && ranges_overlap(range, &tombstone.range)
    }))
}

fn ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    !range_ends_before_start(&left.end, &right.start)
        && !range_ends_before_start(&right.end, &left.start)
}

fn range_ends_before_start(end: &Bound<Vec<u8>>, start: &Bound<Vec<u8>>) -> bool {
    match (end, start) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Excluded(end), Bound::Included(start) | Bound::Excluded(start)) => {
            end.as_slice() <= start.as_slice()
        }
        (Bound::Included(end), Bound::Included(start)) => end.as_slice() < start.as_slice(),
        (Bound::Included(end), Bound::Excluded(start)) => end.as_slice() <= start.as_slice(),
    }
}

fn apply_memtable_operation(
    state: &KeyspaceState,
    operation: BatchOperation,
    sequence: Sequence,
    batch_index: u32,
) -> Result<()> {
    let mut entries = state
        .entries
        .write()
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
            range_tombstones.push(RangeTombstone {
                range,
                sequence,
                batch_index,
            });
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
        InternalKey, TableRangeTombstone, ValueKind, ValueRef, cleanup_point_tombstones,
        cleanup_range_tombstones, compact_point_records,
    };
    use crate::types::{KeyRange, Sequence};

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
