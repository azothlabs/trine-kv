use std::{
    collections::BTreeMap,
    fs,
    ops::Bound,
    path::Path,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::{
    blob::ValueRef,
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    iterator::{Direction, Iter},
    keyspace::{Keyspace, KeyspaceName},
    manifest::{self, ManifestState, ManifestStore},
    options::{DbOptions, DurabilityMode, KeyspaceOptions, StorageMode},
    snapshot::Snapshot,
    stats::DbStats,
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
    keyspaces: RwLock<BTreeMap<String, Arc<KeyspaceState>>>,
    manifest: Option<Mutex<ManifestStore>>,
    wal: Option<Mutex<WalWriter>>,
}

#[derive(Debug)]
pub(crate) struct KeyspaceState {
    options: KeyspaceOptions,
    entries: RwLock<BTreeMap<InternalKey, Option<ValueRef>>>,
    range_tombstones: RwLock<Vec<RangeTombstone>>,
    tables: RwLock<Vec<Arc<Table>>>,
}

impl KeyspaceState {
    fn new(options: KeyspaceOptions, tables: Vec<Arc<Table>>) -> Self {
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
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    range_tombstones: Vec<TableRangeTombstone>,
}

struct CompactionInput {
    keyspace: String,
    table_id: table::TableId,
    input_table_ids: Vec<table::TableId>,
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    range_tombstones: Vec<TableRangeTombstone>,
}

struct CompactionOutput {
    keyspace: String,
    input_table_ids: Vec<table::TableId>,
    table: Arc<Table>,
}

impl Db {
    pub fn open(options: DbOptions) -> Result<Self> {
        match options.storage_mode {
            StorageMode::InMemory => Self::memory(options),
            StorageMode::Persistent { .. } => Self::open_persistent(options),
        }
    }

    pub fn memory(mut options: DbOptions) -> Result<Self> {
        options.storage_mode = StorageMode::InMemory;
        validate_options(&options)?;

        Ok(Self {
            inner: Arc::new(DbInner {
                options,
                last_sequence: AtomicU64::new(Sequence::ZERO.get()),
                closed: AtomicBool::new(false),
                writer: Mutex::new(()),
                keyspaces: RwLock::new(BTreeMap::new()),
                manifest: None,
                wal: None,
            }),
        })
    }

    fn open_persistent(options: DbOptions) -> Result<Self> {
        validate_options(&options)?;
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

        let manifest_path = manifest::manifest_path(path);
        let manifest = ManifestStore::open_or_create(
            manifest_path,
            options.create_if_missing && !options.read_only,
        )?;
        let replay_floor = manifest.state().wal_replay_floor();
        let keyspaces = keyspaces_from_manifest(path, manifest.state())?;

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
                keyspaces: RwLock::new(keyspaces),
                manifest: Some(Mutex::new(manifest)),
                wal,
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

        // Flush holds the writer coordinator while it copies and clears
        // memtables. That gives the manifest edit and the in-memory table list
        // one clear cutover point relative to commits.
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
        for input in &flush_inputs {
            let table_path = table::table_path(&db_path, input.table_id);
            let table = table::write_table(
                &table_path,
                input.table_id,
                &input.point_records,
                &input.range_tombstones,
            )?;
            written_tables.push((input.keyspace.clone(), Arc::new(table)));
        }

        self.publish_flushed_tables(&written_tables, flush_sequence)?;
        self.install_flushed_tables(&flush_inputs, written_tables)?;

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

        // The first compaction slice preserves every internal version and
        // tombstone. That keeps old snapshots valid while replacing several
        // flushed table files with one new file per keyspace.
        let _writer = self
            .inner
            .writer
            .lock()
            .map_err(|_| lock_poisoned("writer coordinator"))?;
        let compaction_inputs = self.collect_compaction_inputs(&range)?;
        if compaction_inputs.is_empty() {
            return Ok(());
        }

        let obsolete_table_ids = compaction_inputs
            .iter()
            .flat_map(|input| input.input_table_ids.iter().copied())
            .collect::<Vec<_>>();
        let mut written_tables = Vec::with_capacity(compaction_inputs.len());
        for input in &compaction_inputs {
            let table_path = table::table_path(&db_path, input.table_id);
            let table = table::write_table(
                &table_path,
                input.table_id,
                &input.point_records,
                &input.range_tombstones,
            )?;
            written_tables.push(CompactionOutput {
                keyspace: input.keyspace.clone(),
                input_table_ids: input.input_table_ids.clone(),
                table: Arc::new(table),
            });
        }

        let written_table_ids = written_tables
            .iter()
            .map(|output| output.table.properties().id)
            .collect::<Vec<_>>();
        if let Err(error) = self.publish_compacted_tables(&written_tables) {
            let _ = remove_table_files(&db_path, &written_table_ids);
            return Err(error);
        }

        self.install_compacted_tables(written_tables)?;
        remove_table_files(&db_path, &obsolete_table_ids)?;

        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.last_committed_sequence())
    }

    #[must_use]
    pub fn transaction(&self, options: TransactionOptions) -> Transaction {
        Transaction::new(self.clone(), self.last_committed_sequence(), options)
    }

    #[must_use]
    pub fn stats(&self) -> DbStats {
        let live_keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_or(0, |keyspaces| keyspaces.len());

        DbStats {
            live_keyspaces,
            ..DbStats::default()
        }
    }

    #[must_use]
    pub fn options(&self) -> &DbOptions {
        &self.inner.options
    }

    #[must_use]
    pub fn last_committed_sequence(&self) -> Sequence {
        Sequence::new(self.inner.last_sequence.load(Ordering::Acquire))
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
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

        let state = self.keyspace_state(keyspace)?;
        let point_range = KeyRange {
            start: Bound::Included(key.to_vec()),
            end: Bound::Included(key.to_vec()),
        };
        Ok(collect_visible_range(&state, &point_range, read_sequence)?
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

        let state = self.keyspace_state(keyspace)?;
        let items = collect_visible_range(&state, range, read_sequence)?;

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

        let state = self.keyspace_state(keyspace)?;
        let mut items = collect_visible_range(&state, &KeyRange::all(), read_sequence)?;
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
                point_records,
                range_tombstones,
            });
            next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                message: "table id counter overflow".to_owned(),
            })?;
        }

        Ok(inputs)
    }

    fn collect_compaction_inputs(&self, range: &KeyRange) -> Result<Vec<CompactionInput>> {
        let mut next_table_id = self.next_table_id()?;
        let keyspaces = self
            .inner
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned("keyspace registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in keyspaces.iter() {
            let candidate_tables = {
                let tables = state
                    .tables
                    .read()
                    .map_err(|_| lock_poisoned("table list"))?;
                tables
                    .iter()
                    .filter(|table| table_overlaps_range(table, range))
                    .cloned()
                    .collect::<Vec<_>>()
            };

            if candidate_tables.len() < 2 {
                continue;
            }

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

            inputs.push(CompactionInput {
                keyspace: name.clone(),
                table_id: next_table_id,
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
                    output.table.properties().clone(),
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
            state
                .tables
                .write()
                .map_err(|_| lock_poisoned("table list"))?
                .push(table);
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
            tables.push(output.table);
        }

        Ok(())
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
            tables.push(Arc::new(table));
        }

        keyspaces.insert(
            name.clone(),
            Arc::new(KeyspaceState::new(options.clone(), tables)),
        );
    }

    Ok(keyspaces)
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

fn validate_batch_len(len: usize) -> Result<()> {
    if len > u32::MAX as usize {
        return Err(Error::InvalidOptions {
            message: "write batch operation count exceeds u32::MAX".to_owned(),
        });
    }

    Ok(())
}

fn collect_point_records(state: &KeyspaceState) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let entries = state
        .entries
        .read()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let mut records = entries
        .iter()
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
                .point_records()
                .iter()
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
    for (internal_key, _) in collect_point_records(state)? {
        match internal_key.user_key().cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Greater => break,
            std::cmp::Ordering::Equal => {
                if internal_key.sequence() > read_sequence {
                    return Ok(true);
                }
            }
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
    for (internal_key, _) in collect_point_records(state)? {
        let user_key = internal_key.user_key();
        if key_is_before_start(user_key, &range.start) {
            continue;
        }
        if key_is_after_end(user_key, &range.end) {
            break;
        }
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
) -> Result<Vec<KeyValue>> {
    let point_records = collect_point_records(state)?;
    let range_tombstones = collect_range_tombstones(state)?;
    let mut items = Vec::new();
    let mut decided_user_key: Option<Vec<u8>> = None;

    for (internal_key, value) in &point_records {
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
                    &range_tombstones,
                    user_key,
                    internal_key.sequence(),
                    internal_key.batch_index(),
                    read_sequence,
                ) {
                    items.push(KeyValue::new(
                        user_key.to_vec(),
                        inline_value(value.as_ref())?.to_vec(),
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

fn inline_value(value: Option<&ValueRef>) -> Result<&[u8]> {
    value
        .and_then(ValueRef::inline_bytes)
        .ok_or_else(|| Error::unsupported("blob values are not implemented yet"))
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

fn table_overlaps_range(table: &Table, range: &KeyRange) -> bool {
    if range_is_all(range) {
        return true;
    }

    table
        .point_records()
        .iter()
        .any(|record| key_is_in_range(record.internal_key.user_key(), range))
        || table
            .range_tombstones()
            .iter()
            .any(|tombstone| ranges_overlap(&tombstone.range, range))
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
