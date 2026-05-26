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
    bucket::{Bucket, BucketName, DEFAULT_BUCKET_NAME},
    cache, compaction, durability,
    error::{Error, Result},
    iterator::{Direction, Iter, LazyIter, ScanSelector},
    lsm::{
        CompactionInput as LsmCompactionInput, CompactionOutput as LsmCompactionOutput,
        CompactionTablePayload as LsmCompactionTablePayload, FlushInput as LsmFlushInput, LsmTree,
    },
    manifest::{self, ManifestState, ManifestStore},
    options::{
        BlobLevelMergePolicy, BucketOptions, DbOptions, DurabilityMode, FailOnCorruptionPolicy,
        FilterPolicy, PrefixFilterPolicy, StorageMode, WriteOptions,
    },
    recovery,
    snapshot::{Snapshot, SnapshotTracker},
    stats::{BlobReadMetrics, DbStats, LevelStats},
    table::{self, Table},
    transaction::{Transaction, TransactionOptions},
    types::{CommitInfo, KeyRange, Sequence, Value},
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
    buckets: RwLock<BTreeMap<String, Arc<LsmTree>>>,
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
    blob_gc_runs: AtomicU64,
    blob_gc_input_bytes: AtomicU64,
    blob_gc_output_bytes: AtomicU64,
    blob_gc_discarded_bytes: AtomicU64,
    blob_reads: Arc<BlobReadMetrics>,
    maintenance: Arc<MaintenanceCoordinator>,
}

struct NamedFlushInput {
    bucket: String,
    tree: Arc<LsmTree>,
    input: LsmFlushInput,
}

struct NamedCompactionInput {
    bucket: String,
    tree: Arc<LsmTree>,
    input: LsmCompactionInput,
}

struct NamedCompactionOutput {
    bucket: String,
    output: LsmCompactionOutput,
}

struct PendingCompactionOutputs {
    outputs: Vec<NamedCompactionOutput>,
    written_table_ids: Vec<table::TableId>,
}

struct BlobGcCandidate {
    file_id: u64,
    total_bytes: u64,
    live_bytes: u64,
}

struct BlobGcRewriteTable {
    bucket: String,
    input_table_id: table::TableId,
    output_table_id: table::TableId,
    level: table::TableLevel,
    options: table::TableWriteOptions,
    point_records: Vec<table::TablePointRecord>,
    range_tombstones: Vec<table::TableRangeTombstone>,
}

struct BlobGcRewriteRecord {
    internal_key: crate::internal_key::InternalKey,
    value: Vec<u8>,
    compression: crate::codec::CodecId,
    table_index: usize,
    record_index: usize,
}

struct BlobGcRewritePlan {
    candidates: Vec<BlobGcCandidate>,
    new_blob_file_id: u64,
    tables: Vec<BlobGcRewriteTable>,
    records: Vec<BlobGcRewriteRecord>,
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
        let _ = cleanup_pending_obsolete_blob_files(
            persistent_path_from_options(&self.options),
            &self.snapshots,
            self.manifest.as_ref(),
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
        let default_bucket = Arc::new(LsmTree::new(
            options.default_bucket_options.clone(),
            Vec::new(),
        )?);
        let mut buckets = BTreeMap::new();
        buckets.insert(DEFAULT_BUCKET_NAME.to_owned(), default_bucket);

        Ok(Self {
            inner: Arc::new(DbInner {
                options,
                last_sequence: AtomicU64::new(Sequence::ZERO.get()),
                closed: AtomicBool::new(false),
                writer: Mutex::new(()),
                process_lock: Mutex::new(None),
                buckets: RwLock::new(buckets),
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
                blob_gc_runs: AtomicU64::new(0),
                blob_gc_input_bytes: AtomicU64::new(0),
                blob_gc_output_bytes: AtomicU64::new(0),
                blob_gc_discarded_bytes: AtomicU64::new(0),
                blob_reads: Arc::new(BlobReadMetrics::default()),
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
        let db_path_for_cleanup = path.clone();

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
        let mut manifest = ManifestStore::open_or_create(
            manifest_path,
            options.create_if_missing && !options.read_only,
        )?;
        ensure_default_bucket_in_manifest(&mut manifest, &options)?;
        let replay_floor = manifest.state().wal_replay_floor();
        let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest.state());
        let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest.state());
        let mut buckets = buckets_from_manifest(path, manifest.state())?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;
        recovery::fail_on_missing_referenced_blob_files(path, &referenced_blob_ids)?;
        recovery::fail_on_invalid_referenced_blob_files(path, manifest.state())?;
        recovery::fail_on_unreferenced_storage_files(
            path,
            &referenced_table_file_ids(manifest.state()),
            &allowed_blob_ids,
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
                buckets: RwLock::new(buckets),
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
                blob_gc_runs: AtomicU64::new(0),
                blob_gc_input_bytes: AtomicU64::new(0),
                blob_gc_output_bytes: AtomicU64::new(0),
                blob_gc_discarded_bytes: AtomicU64::new(0),
                blob_reads: Arc::new(BlobReadMetrics::default()),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
            }),
        };
        db.replay_wal_batches(batches, replay_floor)?;
        if !db.inner.options.read_only {
            db.cleanup_pending_obsolete_blob_files(&db_path_for_cleanup)?;
        }
        db.start_background_workers()?;

        Ok(db)
    }

    /// Returns a handle for the built-in default bucket.
    ///
    /// Direct helpers such as `Db::put` and `Db::get` use this bucket without
    /// requiring callers to open it explicitly.
    pub fn default_bucket(&self) -> Result<Bucket> {
        let options = self
            .existing_bucket_options(DEFAULT_BUCKET_NAME)?
            .ok_or_else(|| Error::BucketMissing {
                name: DEFAULT_BUCKET_NAME.to_owned(),
            })?;
        Ok(Bucket::new(
            self.clone(),
            BucketName::new(DEFAULT_BUCKET_NAME),
            options,
        ))
    }

    /// Returns an existing named bucket or creates it with default bucket
    /// options.
    ///
    /// The built-in default bucket is reserved for direct `Db` helpers and
    /// `Db::default_bucket`; using `"default"` as a named bucket returns an
    /// error.
    pub fn bucket(&self, name: impl Into<BucketName>) -> Result<Bucket> {
        self.bucket_with_options(name, BucketOptions::default())
    }

    /// Returns an existing named bucket or creates it with explicit options.
    ///
    /// Bucket options are fixed after creation. Calling this for an existing
    /// named bucket with different options returns an error. The built-in
    /// default bucket is configured through `DbOptions::default_bucket_options`.
    pub fn bucket_with_options(
        &self,
        name: impl Into<BucketName>,
        options: BucketOptions,
    ) -> Result<Bucket> {
        self.ensure_open()?;

        let name = name.into();
        if name.as_str().is_empty() {
            return Err(Error::invalid_options("bucket name cannot be empty"));
        }
        if name.as_str() == DEFAULT_BUCKET_NAME {
            return Err(Error::invalid_options(
                "default bucket is accessed through Db helpers",
            ));
        }

        validate_bucket_options(&options)?;

        if let Some(existing_options) = self.existing_bucket_options(name.as_str())? {
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(self.clone(), name, existing_options));
        }

        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        self.persist_bucket_creation(name.as_str(), &options)?;

        let bucket_options = {
            let mut buckets = self
                .inner
                .buckets
                .write()
                .map_err(|_| lock_poisoned("bucket registry"))?;

            if let Some(state) = buckets.get(name.as_str()) {
                if state.options != options {
                    return Err(Error::invalid_options(
                        "existing bucket options do not match requested options",
                    ));
                }
                state.options.clone()
            } else {
                let bucket_options = options.clone();
                buckets.insert(
                    name.as_str().to_owned(),
                    Arc::new(LsmTree::new(options, Vec::new())?),
                );
                bucket_options
            }
        };

        Ok(Bucket::new(self.clone(), name, bucket_options))
    }

    /// Reads the newest committed value for `key` from the default bucket.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_sequence(DEFAULT_BUCKET_NAME, key, self.last_committed_sequence())
    }

    /// Reads `key` from the default bucket at the sequence pinned by
    /// `snapshot`.
    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_with_pin_state(
            DEFAULT_BUCKET_NAME,
            key,
            snapshot.read_sequence(),
            snapshot.is_pinned(),
        )
    }

    /// Writes one key/value pair to the default bucket using default write
    /// options.
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options(key, value, WriteOptions::default())
            .map(|_| ())
    }

    /// Writes one key/value pair to the default bucket and returns commit
    /// information.
    pub fn put_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.put(key, value);
        self.write(batch, options)
    }

    /// Adds a point delete for one default-bucket key using default write
    /// options.
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options(key, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a point delete for one default-bucket key and returns commit
    /// information.
    pub fn delete_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete(key);
        self.write(batch, options)
    }

    /// Adds a range delete to the default bucket using default write options.
    pub fn delete_range(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options(range, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a range delete to the default bucket and returns commit
    /// information.
    pub fn delete_range_with_options(
        &self,
        range: KeyRange,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete_range(range);
        self.write(batch, options)
    }

    /// Returns a forward iterator over default-bucket rows in `range`.
    pub fn range(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket iterator whose blob values are read on
    /// demand.
    pub fn range_lazy(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket iterator over `range` at `snapshot`.
    pub fn range_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward value-lazy default-bucket iterator at `snapshot`.
    pub fn range_lazy_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a reverse iterator over default-bucket rows in `range`.
    pub fn range_reverse(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket iterator whose blob values are read on
    /// demand.
    pub fn range_lazy_reverse(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket iterator over `range` at `snapshot`.
    pub fn range_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse value-lazy default-bucket iterator at `snapshot`.
    pub fn range_lazy_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a forward iterator over default-bucket rows whose keys begin
    /// with `prefix`.
    pub fn prefix(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket prefix iterator whose blob values are
    /// read on demand.
    pub fn prefix_lazy(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket prefix iterator at `snapshot`.
    pub fn prefix_at(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward value-lazy default-bucket prefix iterator at
    /// `snapshot`.
    pub fn prefix_lazy_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a reverse iterator over default-bucket rows whose keys begin
    /// with `prefix`.
    pub fn prefix_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket prefix iterator whose blob values are
    /// read on demand.
    pub fn prefix_lazy_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket prefix iterator at `snapshot`.
    pub fn prefix_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse value-lazy default-bucket prefix iterator at
    /// `snapshot`.
    pub fn prefix_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
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
        self.cleanup_pending_obsolete_blob_files(&db_path)?;

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

        let PendingCompactionOutputs {
            outputs: written_tables,
            written_table_ids,
        } = self.build_compaction_outputs(
            &db_path,
            &range,
            oldest_active_snapshot,
            &compaction_inputs,
        )?;

        let output_table_ids = written_tables
            .iter()
            .flat_map(|output| {
                output
                    .output
                    .tables
                    .iter()
                    .map(|table| table.properties().id)
            })
            .collect::<BTreeSet<_>>();
        let input_table_ids_for_stats = compaction_inputs
            .iter()
            .flat_map(|input| input.input.input_table_ids.iter().copied())
            .collect::<Vec<_>>();
        // A direct table move keeps the input file alive under the same id, so
        // cleanup must use only ids that disappeared from the published output.
        let obsolete_table_ids = compaction_inputs
            .iter()
            .flat_map(|input| input.input.input_table_ids.iter().copied())
            .filter(|table_id| !output_table_ids.contains(table_id))
            .collect::<Vec<_>>();
        let output_table_ids_for_stats = output_table_ids.iter().copied().collect::<Vec<_>>();
        let obsolete_blob_ids =
            self.obsolete_blob_ids_for_compaction(&compaction_inputs, &written_tables)?;

        if let Err(error) = self.validate_compacted_tables(&written_tables) {
            let _ = remove_storage_files(&db_path, &written_table_ids);
            if is_level_layout_compaction_error(&error) {
                return Ok(());
            }
            return Err(error);
        }

        if !written_table_ids.is_empty() {
            if let Err(error) = durability::sync_dir_after_renames(&db_path) {
                let _ = remove_storage_files(&db_path, &written_table_ids);
                return Err(error);
            }
        }

        if let Err(error) = self.publish_compacted_tables(&written_tables, &obsolete_blob_ids) {
            let _ = remove_storage_files(&db_path, &written_table_ids);
            return Err(error);
        }

        self.install_compacted_tables(written_tables)?;
        self.record_compaction_stats(
            &db_path,
            compaction_inputs.len(),
            &input_table_ids_for_stats,
            &output_table_ids_for_stats,
        );
        self.retire_obsolete_table_files(&db_path, &obsolete_table_ids)?;
        self.cleanup_pending_obsolete_blob_files(&db_path)?;
        if self.inner.options.blob_gc_enabled {
            self.run_blob_gc_once_locked(&db_path)?;
        }

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
            blob_gc_runs: self.inner.blob_gc_runs.load(Ordering::Acquire),
            blob_gc_input_bytes: self.inner.blob_gc_input_bytes.load(Ordering::Acquire),
            blob_gc_output_bytes: self.inner.blob_gc_output_bytes.load(Ordering::Acquire),
            blob_gc_discarded_bytes: self.inner.blob_gc_discarded_bytes.load(Ordering::Acquire),
            ..DbStats::default()
        };
        let (blob_read_count, blob_read_bytes) = self.inner.blob_reads.snapshot();
        stats.blob_read_count = blob_read_count;
        stats.blob_read_bytes = blob_read_bytes;
        let cache_stats = self.inner.block_cache.stats();
        stats.block_cache_hits = cache_stats.hits;
        stats.block_cache_misses = cache_stats.misses;

        let persistent_path = self.persistent_path();
        let mut level_stats = BTreeMap::<u32, LevelStats>::new();
        let mut live_blob_bytes_by_file = BTreeMap::<u64, u64>::new();

        let Ok(buckets) = self.inner.buckets.read() else {
            return stats;
        };
        stats.live_buckets = buckets.len();

        for state in buckets.values() {
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
                    stats.filters.saturating_add_assign(table.filter_stats());
                    stats
                        .read_path
                        .saturating_add_assign(table.read_path_stats());
                    stats.total_tables += 1;
                    stats.table_bytes = stats.table_bytes.saturating_add(table_bytes);
                    if properties.level == table::TableLevel::ZERO {
                        stats.l0_tables += 1;
                    }
                    level_entry.tables += 1;
                    level_entry.bytes = level_entry.bytes.saturating_add(table_bytes);

                    for reference in &properties.blob_references {
                        live_blob_bytes_by_file
                            .entry(reference.file_id)
                            .and_modify(|bytes| {
                                *bytes = bytes.saturating_add(reference.referenced_bytes);
                            })
                            .or_insert(reference.referenced_bytes);
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
            let _ = self.cleanup_pending_obsolete_blob_files(&db_path);
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

    pub(crate) fn get_at_sequence(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        self.get_at_with_pin_state(bucket, key, read_sequence, false)
    }

    pub(crate) fn get_at_with_pin_state(
        &self,
        bucket: &str,
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

        let state = self.bucket_state(bucket)?;
        state.read_visible_point(
            key,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
            Some(self.inner.blob_reads.as_ref()),
        )
    }

    pub(crate) fn range_at_sequence(
        &self,
        bucket: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Range(range.clone());
        let scan = state.scan(&selector, direction, Some(&self.inner.block_cache))?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(Iter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            Some(Arc::clone(&self.inner.blob_reads)),
            scan.range_tombstones,
            scan.sources,
        ))
    }

    pub(crate) fn range_lazy_at_sequence(
        &self,
        bucket: &str,
        range: &KeyRange,
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Range(range.clone());
        let scan = state.scan(&selector, direction, Some(&self.inner.block_cache))?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(LazyIter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            Some(Arc::clone(&self.inner.blob_reads)),
            scan.range_tombstones,
            scan.sources,
        ))
    }

    pub(crate) fn prefix_at_sequence(
        &self,
        bucket: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<Iter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Prefix(prefix.to_vec());
        let scan = state.scan(&selector, direction, Some(&self.inner.block_cache))?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(Iter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            Some(Arc::clone(&self.inner.blob_reads)),
            scan.range_tombstones,
            scan.sources,
        ))
    }

    pub(crate) fn prefix_lazy_at_sequence(
        &self,
        bucket: &str,
        prefix: &[u8],
        read_sequence: Sequence,
        direction: Direction,
    ) -> Result<LazyIter> {
        self.ensure_open()?;
        let read_pin = self.inner.snapshots.pinned_snapshot(read_sequence);

        let state = self.bucket_state(bucket)?;
        let selector = ScanSelector::Prefix(prefix.to_vec());
        let scan = state.scan(&selector, direction, Some(&self.inner.block_cache))?;
        let db_path = self.persistent_path().map(Path::to_path_buf);

        Ok(LazyIter::from_sources(
            direction,
            read_sequence,
            read_pin,
            db_path,
            Some(Arc::clone(&self.inner.blob_reads)),
            scan.range_tombstones,
            scan.sources,
        ))
    }

    fn bucket_state(&self, bucket: &str) -> Result<Arc<LsmTree>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        buckets
            .get(bucket)
            .cloned()
            .ok_or_else(|| Error::BucketMissing {
                name: bucket.to_owned(),
            })
    }

    fn existing_bucket_options(&self, bucket: &str) -> Result<Option<BucketOptions>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        Ok(buckets.get(bucket).map(|state| state.options.clone()))
    }

    fn persistent_path(&self) -> Option<&Path> {
        match &self.inner.options.storage_mode {
            StorageMode::Persistent { path } => Some(path.as_path()),
            StorageMode::InMemory => None,
        }
    }

    fn persist_bucket_creation(&self, name: &str, options: &BucketOptions) -> Result<()> {
        if let Some(manifest) = &self.inner.manifest {
            // Manifest I/O happens outside the bucket registry lock. Two
            // racing creators are serialized by the manifest lock, and the
            // second identical request becomes a no-op.
            manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .create_bucket(name.to_owned(), options.clone())?;
        }

        Ok(())
    }

    fn resolve_batch_buckets(&self, operations: &[BatchOperation]) -> Result<Vec<Arc<LsmTree>>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut states = Vec::with_capacity(operations.len());

        for operation in operations {
            let state =
                buckets
                    .get(operation.bucket())
                    .cloned()
                    .ok_or_else(|| Error::BucketMissing {
                        name: operation.bucket().to_owned(),
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
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        for state in buckets.values() {
            if state.has_immutable_memtables()? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn freeze_all_active_memtables(&self, freeze_sequence: Sequence) -> Result<usize> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut frozen_count = 0;

        for state in buckets.values() {
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
            written_tables.push((input.bucket.clone(), Arc::new(table)));
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
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in buckets.iter() {
            if state.immutable_memtable_count()? < max_immutable_memtables {
                continue;
            }
            for input in state.prepare_flush_inputs(&mut next_table_id)? {
                inputs.push(NamedFlushInput {
                    bucket: name.clone(),
                    tree: Arc::clone(state),
                    input,
                });
            }
        }

        Ok(inputs)
    }

    fn collect_flush_inputs(&self) -> Result<Vec<NamedFlushInput>> {
        let mut next_table_id = self.next_table_id()?;
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut inputs = Vec::new();

        for (name, state) in buckets.iter() {
            for input in state.prepare_flush_inputs(&mut next_table_id)? {
                inputs.push(NamedFlushInput {
                    bucket: name.clone(),
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
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut inputs = Vec::new();
        let compaction_options = compaction_options(&self.inner.options);

        for (name, state) in buckets.iter() {
            let Some(input) =
                state.plan_compaction(name, range, oldest_active_snapshot, compaction_options)?
            else {
                continue;
            };
            inputs.push(NamedCompactionInput {
                bucket: name.clone(),
                tree: Arc::clone(state),
                input,
            });
        }

        Ok(inputs)
    }

    fn build_compaction_outputs(
        &self,
        db_path: &Path,
        range: &KeyRange,
        oldest_active_snapshot: Sequence,
        compaction_inputs: &[NamedCompactionInput],
    ) -> Result<PendingCompactionOutputs> {
        let mut outputs = Vec::with_capacity(compaction_inputs.len());
        let mut written_table_ids = Vec::new();
        let mut next_table_id = self.next_table_id()?;

        for input in compaction_inputs {
            let force_rewrite_trivial =
                input.tree.options.blob_level_merge_policy == BlobLevelMergePolicy::Always;
            if input.input.trivial_move && !force_rewrite_trivial {
                outputs.push(NamedCompactionOutput {
                    bucket: input.bucket.clone(),
                    output: LsmCompactionOutput {
                        input_table_ids: input.input.input_table_ids.clone(),
                        tables: vec![input.input.moved_table()?],
                    },
                });
                continue;
            }

            let payloads = match input.tree.build_compaction_table_payloads(
                &input.input,
                range,
                oldest_active_snapshot,
                self.inner.options.target_table_bytes,
            ) {
                Ok(payloads) => payloads,
                Err(error) => {
                    let _ = remove_storage_files(db_path, &written_table_ids);
                    return Err(error);
                }
            };
            let mut table_options = input.input.table_options.clone();
            table_options.rewrite_blob_indexes = should_rewrite_blob_indexes_for_compaction(
                &input.input,
                &payloads,
                input.tree.options.blob_level_merge_policy,
            );
            let mut output_tables = Vec::with_capacity(payloads.len());
            for payload in payloads {
                let table_id = next_table_id;
                next_table_id = if let Some(table_id) = next_table_id.next() {
                    table_id
                } else {
                    let _ = remove_storage_files(db_path, &written_table_ids);
                    return Err(Error::Corruption {
                        message: "table id counter overflow".to_owned(),
                    });
                };

                let table_path = table::table_path(db_path, table_id);
                written_table_ids.push(table_id);
                let table = match table::write_table(
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                ) {
                    Ok(table) => table,
                    Err(error) => {
                        let _ = remove_storage_files(db_path, &written_table_ids);
                        return Err(error);
                    }
                };
                output_tables.push(Arc::new(table));
            }
            outputs.push(NamedCompactionOutput {
                bucket: input.bucket.clone(),
                output: LsmCompactionOutput {
                    input_table_ids: input.input.input_table_ids.clone(),
                    tables: output_tables,
                },
            });
        }

        Ok(PendingCompactionOutputs {
            outputs,
            written_table_ids,
        })
    }

    fn run_blob_gc_once_locked(&self, db_path: &Path) -> Result<()> {
        let Some(plan) = self.build_blob_gc_rewrite_plan(db_path)? else {
            return Ok(());
        };

        let input_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes)
        });
        let discarded_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes.saturating_sub(candidate.live_bytes))
        });
        let obsolete_blob_ids = plan
            .candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<Vec<_>>();

        let header = blob::BlobFileHeader::new(
            plan.new_blob_file_id,
            self.last_committed_sequence(),
            1,
            crate::codec::CodecId::None,
        );
        let blob_records = blob_gc_blob_records(&plan.records);

        let written_table_ids = plan
            .tables
            .iter()
            .map(|table| table.output_table_id)
            .collect::<Vec<_>>();
        let obsolete_table_ids = plan
            .tables
            .iter()
            .map(|table| table.input_table_id)
            .collect::<Vec<_>>();
        let indexes =
            match blob::write_blob_file(db_path, plan.new_blob_file_id, header, &blob_records) {
                Ok(indexes) => indexes,
                Err(error) => {
                    let _ = remove_storage_files(db_path, &written_table_ids);
                    return Err(error);
                }
            };

        let mut tables = plan.tables;
        let output_bytes = match apply_blob_gc_indexes(&mut tables, plan.records, indexes) {
            Ok(output_bytes) => output_bytes,
            Err(error) => {
                let _ = remove_storage_files(db_path, &written_table_ids);
                return Err(error);
            }
        };
        let outputs = match write_blob_gc_replacement_tables(db_path, tables) {
            Ok(outputs) => outputs,
            Err(error) => {
                let _ = remove_storage_files(db_path, &written_table_ids);
                return Err(error);
            }
        };

        if let Err(error) = durability::sync_dir_after_renames(db_path) {
            let _ = remove_storage_files(db_path, &written_table_ids);
            return Err(error);
        }

        if let Err(error) = self.publish_compacted_tables(&outputs, &obsolete_blob_ids) {
            let _ = remove_storage_files(db_path, &written_table_ids);
            return Err(error);
        }

        self.install_compacted_tables(outputs)?;
        self.retire_obsolete_table_files(db_path, &obsolete_table_ids)?;
        self.inner.blob_gc_runs.fetch_add(1, Ordering::AcqRel);
        self.inner
            .blob_gc_input_bytes
            .fetch_add(input_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_output_bytes
            .fetch_add(output_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_discarded_bytes
            .fetch_add(discarded_bytes, Ordering::AcqRel);
        self.cleanup_pending_obsolete_blob_files(db_path)
    }

    fn build_blob_gc_rewrite_plan(&self, db_path: &Path) -> Result<Option<BlobGcRewritePlan>> {
        let candidates = self.choose_blob_gc_candidates(db_path)?;
        if candidates.is_empty() {
            return Ok(None);
        }
        let candidate_file_ids = candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<BTreeSet<_>>();

        let mut next_table_id = self.next_table_id()?;
        let new_blob_file_id = next_table_id.get();
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut tables = Vec::new();
        let mut rewrite_records = Vec::new();

        for (bucket, tree) in buckets.iter() {
            for table in tree.tables_snapshot()? {
                if !table
                    .blob_file_ids()
                    .iter()
                    .any(|file_id| candidate_file_ids.contains(file_id))
                {
                    continue;
                }
                let output_table_id = next_table_id;
                next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                    message: "table id counter overflow".to_owned(),
                })?;

                let table_index = tables.len();
                let point_records = table.point_records()?;
                for (record_index, point_record) in point_records.iter().enumerate() {
                    let Some(ValueRef::BlobIndex(index)) = point_record.value.as_ref() else {
                        continue;
                    };
                    if !candidate_file_ids.contains(&index.file_id) {
                        continue;
                    }
                    let blob_record = blob::read_record_for_index(
                        db_path,
                        index,
                        Some(&point_record.internal_key),
                    )?;
                    rewrite_records.push(BlobGcRewriteRecord {
                        internal_key: point_record.internal_key.clone(),
                        value: blob_record.record.value.clone(),
                        compression: blob_record.record.compression,
                        table_index,
                        record_index,
                    });
                }

                tables.push(BlobGcRewriteTable {
                    bucket: bucket.clone(),
                    input_table_id: table.properties().id,
                    output_table_id,
                    level: table.properties().level,
                    options: blob_gc_table_write_options(&tree.options),
                    point_records,
                    range_tombstones: table.range_tombstones()?.all().to_vec(),
                });
            }
        }
        drop(buckets);

        if rewrite_records.is_empty() {
            return Ok(None);
        }
        rewrite_records.sort_by(|left, right| left.internal_key.cmp(&right.internal_key));

        Ok(Some(BlobGcRewritePlan {
            candidates,
            new_blob_file_id,
            tables,
            records: rewrite_records,
        }))
    }

    fn choose_blob_gc_candidates(&self, db_path: &Path) -> Result<Vec<BlobGcCandidate>> {
        let live_bytes_by_file = self.live_blob_bytes_by_file()?;
        let mut candidates = Vec::new();

        for (file_id, live_bytes) in live_bytes_by_file {
            let properties = blob::read_blob_file_properties(db_path, file_id)?;
            let total_bytes = properties.encoded_bytes;
            if total_bytes < self.inner.options.blob_gc_min_file_bytes {
                continue;
            }
            let discardable_bytes = total_bytes.saturating_sub(live_bytes);
            if discardable_bytes == 0
                || !self
                    .inner
                    .options
                    .blob_gc_discardable_ratio
                    .should_collect(discardable_bytes, total_bytes)
            {
                continue;
            }

            candidates.push(BlobGcCandidate {
                file_id,
                total_bytes,
                live_bytes,
            });
        }
        candidates.sort_by(|left, right| {
            let left_discardable = left.total_bytes.saturating_sub(left.live_bytes);
            let right_discardable = right.total_bytes.saturating_sub(right.live_bytes);
            right_discardable.cmp(&left_discardable)
        });

        Ok(candidates)
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
            .map(|(bucket, table)| (bucket.clone(), table.properties().clone()))
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

    fn publish_compacted_tables(
        &self,
        outputs: &[NamedCompactionOutput],
        obsolete_blob_ids: &[u64],
    ) -> Result<()> {
        let edits = outputs
            .iter()
            .map(|output| {
                (
                    output.bucket.clone(),
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
            .replace_tables_batch_and_mark_blob_deletions(
                edits,
                obsolete_blob_ids.to_vec(),
                self.last_committed_sequence(),
            )
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
        for (input, (bucket, table)) in inputs.iter().zip(tables) {
            debug_assert_eq!(input.bucket, bucket);
            input.tree.install_flush(&input.input, table)?;
        }

        Ok(())
    }

    fn install_compacted_tables(&self, outputs: Vec<NamedCompactionOutput>) -> Result<()> {
        for output in outputs {
            let state = self.bucket_state(&output.bucket)?;
            state.install_compaction(output.output)?;
        }

        Ok(())
    }

    fn validate_compacted_tables(&self, outputs: &[NamedCompactionOutput]) -> Result<()> {
        for output in outputs {
            let state = self.bucket_state(&output.bucket)?;
            state.validate_compaction(&output.output)?;
        }

        Ok(())
    }

    fn live_blob_bytes_by_file(&self) -> Result<BTreeMap<u64, u64>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut live_blob_bytes_by_file = BTreeMap::<u64, u64>::new();

        for state in buckets.values() {
            for table in state.tables_snapshot()? {
                for reference in table.properties().blob_references() {
                    live_blob_bytes_by_file
                        .entry(reference.file_id)
                        .and_modify(|bytes| {
                            *bytes = bytes.saturating_add(reference.referenced_bytes);
                        })
                        .or_insert(reference.referenced_bytes);
                }
            }
        }

        Ok(live_blob_bytes_by_file)
    }

    fn cleanup_pending_obsolete_blob_files(&self, db_path: &Path) -> Result<()> {
        cleanup_pending_obsolete_blob_files(
            Some(db_path),
            &self.inner.snapshots,
            self.inner.manifest.as_ref(),
        )
    }

    fn obsolete_blob_ids_for_compaction(
        &self,
        inputs: &[NamedCompactionInput],
        outputs: &[NamedCompactionOutput],
    ) -> Result<Vec<u64>> {
        let input_table_ids = inputs
            .iter()
            .flat_map(|input| input.input.input_table_ids.iter().copied())
            .collect::<BTreeSet<_>>();
        let input_blob_ids = inputs
            .iter()
            .flat_map(|input| {
                input
                    .input
                    .input_tables
                    .iter()
                    .flat_map(|table| table.blob_file_ids())
            })
            .collect::<BTreeSet<_>>();
        let output_blob_ids = outputs
            .iter()
            .flat_map(|output| {
                output
                    .output
                    .tables
                    .iter()
                    .flat_map(|table| table.blob_file_ids())
            })
            .collect::<BTreeSet<_>>();

        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut outside_blob_ids = BTreeSet::new();
        for state in buckets.values() {
            for table in state.tables_snapshot()? {
                if input_table_ids.contains(&table.properties().id) {
                    continue;
                }
                outside_blob_ids.extend(table.blob_file_ids());
            }
        }

        Ok(input_blob_ids
            .difference(&output_blob_ids)
            .copied()
            .filter(|file_id| !outside_blob_ids.contains(file_id))
            .collect())
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
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        for state in buckets.values() {
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
    validate_bucket_options(&options.default_bucket_options)?;
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
    let blob_gc_ratio = options.blob_gc_discardable_ratio.millionths();
    if blob_gc_ratio == 0 || blob_gc_ratio > 1_000_000 {
        return Err(Error::invalid_options(
            "blob GC discardable ratio must be in (0.0, 1.0]",
        ));
    }
    if options.blob_gc_enabled && options.blob_gc_min_file_bytes == 0 {
        return Err(Error::invalid_options(
            "blob GC minimum file size must be non-zero",
        ));
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

fn buckets_from_manifest(
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<BTreeMap<String, Arc<LsmTree>>> {
    let mut buckets = BTreeMap::new();

    for (name, options) in manifest.buckets() {
        validate_bucket_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let table = table::read_table(&table_path)?.with_manifest_properties(properties)?;
            tables.push(Arc::new(table));
        }

        buckets.insert(
            name.clone(),
            Arc::new(LsmTree::new(options.clone(), tables)?),
        );
    }

    Ok(buckets)
}

fn ensure_default_bucket_in_manifest(
    manifest: &mut ManifestStore,
    options: &DbOptions,
) -> Result<()> {
    if manifest.state().buckets().contains_key(DEFAULT_BUCKET_NAME) || options.read_only {
        return Ok(());
    }

    manifest.create_bucket(
        DEFAULT_BUCKET_NAME.to_owned(),
        options.default_bucket_options.clone(),
    )
}

fn ensure_default_bucket_loaded(
    buckets: &mut BTreeMap<String, Arc<LsmTree>>,
    options: &DbOptions,
) -> Result<()> {
    if buckets.contains_key(DEFAULT_BUCKET_NAME) {
        return Ok(());
    }

    // Read-only opens cannot publish a missing manifest entry, but the public
    // API still treats the default bucket as always present.
    buckets.insert(
        DEFAULT_BUCKET_NAME.to_owned(),
        Arc::new(LsmTree::new(
            options.default_bucket_options.clone(),
            Vec::new(),
        )?),
    );
    Ok(())
}

fn table_file_bytes(db_path: &Path, table_id: table::TableId) -> u64 {
    fs::metadata(table::table_path(db_path, table_id)).map_or(0, |metadata| metadata.len())
}

fn add_obsolete_blob_stats(
    db_path: &Path,
    live_blob_bytes_by_file: &BTreeMap<u64, u64>,
    stats: &mut DbStats,
) {
    for (file_id, live_bytes) in live_blob_bytes_by_file {
        let Ok(properties) = blob::read_blob_file_properties(db_path, *file_id) else {
            continue;
        };
        if properties.encoded_bytes > *live_bytes {
            stats.stale_blob_files = stats.stale_blob_files.saturating_add(1);
            stats.stale_blob_bytes = stats
                .stale_blob_bytes
                .saturating_add(properties.encoded_bytes - *live_bytes);
        }
    }

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
        stats.stale_blob_files = stats.stale_blob_files.saturating_add(1);
        stats.stale_blob_bytes = stats.stale_blob_bytes.saturating_add(bytes);
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

fn allowed_blob_file_ids_from_manifest(manifest: &ManifestState) -> BTreeSet<u64> {
    let mut file_ids = referenced_blob_file_ids_from_manifest(manifest);
    file_ids.extend(manifest.pending_blob_deletions().keys().copied());
    file_ids
}

fn should_rewrite_blob_indexes_for_compaction(
    input: &LsmCompactionInput,
    payloads: &[LsmCompactionTablePayload],
    policy: BlobLevelMergePolicy,
) -> bool {
    match policy {
        BlobLevelMergePolicy::Disabled => false,
        BlobLevelMergePolicy::Always => payloads_have_blob_references(payloads),
        BlobLevelMergePolicy::Auto => {
            let output_bytes = payload_blob_bytes_by_file(payloads);
            if output_bytes.is_empty() {
                return false;
            }
            if output_bytes.len() > 1 {
                return true;
            }

            let input_bytes = input_blob_bytes_by_file(input);
            input_bytes.iter().any(|(file_id, input_bytes)| {
                let output_bytes = output_bytes.get(file_id).copied().unwrap_or(0);
                *input_bytes > output_bytes
            })
        }
    }
}

fn payloads_have_blob_references(payloads: &[LsmCompactionTablePayload]) -> bool {
    payloads.iter().any(|payload| {
        payload
            .point_records
            .iter()
            .any(|(_, value)| matches!(value, Some(ValueRef::BlobIndex(_) | ValueRef::Blob { .. })))
    })
}

fn input_blob_bytes_by_file(input: &LsmCompactionInput) -> BTreeMap<u64, u64> {
    let mut bytes_by_file = BTreeMap::new();
    for table in &input.input_tables {
        for reference in table.properties().blob_references() {
            bytes_by_file
                .entry(reference.file_id)
                .and_modify(|bytes: &mut u64| {
                    *bytes = bytes.saturating_add(reference.referenced_bytes);
                })
                .or_insert(reference.referenced_bytes);
        }
    }
    bytes_by_file
}

fn payload_blob_bytes_by_file(payloads: &[LsmCompactionTablePayload]) -> BTreeMap<u64, u64> {
    let mut bytes_by_file = BTreeMap::new();
    for payload in payloads {
        for (_, value) in &payload.point_records {
            let Some((file_id, referenced_bytes)) = blob_reference_bytes(value.as_ref()) else {
                continue;
            };
            bytes_by_file
                .entry(file_id)
                .and_modify(|bytes: &mut u64| {
                    *bytes = bytes.saturating_add(referenced_bytes);
                })
                .or_insert(referenced_bytes);
        }
    }
    bytes_by_file
}

fn blob_reference_bytes(value: Option<&ValueRef>) -> Option<(u64, u64)> {
    match value {
        Some(ValueRef::BlobIndex(index)) => Some((index.file_id, index.encoded_len)),
        Some(ValueRef::Blob { file_id, len, .. }) => Some((*file_id, *len)),
        Some(ValueRef::Inline(_)) | None => None,
    }
}

fn blob_gc_table_write_options(options: &BucketOptions) -> table::TableWriteOptions {
    table::TableWriteOptions {
        codec: options.compression.codec_id(),
        block_bytes: options.block_bytes,
        filter_policy: options.filter_policy,
        prefix_extractor: options.prefix_extractor.clone(),
        prefix_filter_policy: options.prefix_filter_policy,
        blob_threshold_bytes: usize::MAX,
        rewrite_blob_indexes: false,
    }
}

fn blob_gc_blob_records(records: &[BlobGcRewriteRecord]) -> Vec<blob::BlobRecord> {
    records
        .iter()
        .map(|record| blob::BlobRecord {
            internal_key: record.internal_key.clone(),
            value: record.value.clone(),
            compression: record.compression,
        })
        .collect()
}

fn apply_blob_gc_indexes(
    tables: &mut [BlobGcRewriteTable],
    records: Vec<BlobGcRewriteRecord>,
    indexes: Vec<blob::BlobIndex>,
) -> Result<u64> {
    if records.len() != indexes.len() {
        return Err(Error::Corruption {
            message: "blob GC rewrite record count does not match blob indexes".to_owned(),
        });
    }

    let output_bytes = indexes.iter().fold(0_u64, |bytes, index| {
        bytes.saturating_add(index.encoded_len)
    });
    for (rewrite, index) in records.into_iter().zip(indexes) {
        let record = tables
            .get_mut(rewrite.table_index)
            .and_then(|table| table.point_records.get_mut(rewrite.record_index))
            .ok_or_else(|| Error::Corruption {
                message: "blob GC rewrite record position is invalid".to_owned(),
            })?;
        record.value = Some(ValueRef::BlobIndex(index));
    }

    Ok(output_bytes)
}

fn write_blob_gc_replacement_tables(
    db_path: &Path,
    tables: Vec<BlobGcRewriteTable>,
) -> Result<Vec<NamedCompactionOutput>> {
    let mut outputs = Vec::with_capacity(tables.len());
    for rewrite_table in tables {
        let table_path = table::table_path(db_path, rewrite_table.output_table_id);
        let point_records = rewrite_table
            .point_records
            .iter()
            .map(|record| (record.internal_key.clone(), record.value.clone()))
            .collect::<Vec<_>>();
        let table = Arc::new(table::write_table(
            &table_path,
            rewrite_table.output_table_id,
            rewrite_table.level,
            &rewrite_table.options,
            &point_records,
            &rewrite_table.range_tombstones,
        )?);

        outputs.push(NamedCompactionOutput {
            bucket: rewrite_table.bucket,
            output: LsmCompactionOutput {
                input_table_ids: vec![rewrite_table.input_table_id],
                tables: vec![table],
            },
        });
    }

    Ok(outputs)
}

fn validate_bucket_options(options: &BucketOptions) -> Result<()> {
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

fn is_level_layout_compaction_error(error: &Error) -> bool {
    let Error::Corruption { message } = error else {
        return false;
    };
    message.contains("has overlapping tables")
        || message.contains("unbounded table mixed with other tables")
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

fn cleanup_pending_obsolete_blob_files(
    db_path: Option<&Path>,
    snapshots: &SnapshotTracker,
    manifest: Option<&Mutex<ManifestStore>>,
) -> Result<()> {
    let Some(db_path) = db_path else {
        return Ok(());
    };
    if snapshots.active_count() != 0 {
        return Ok(());
    }
    let manifest = manifest.ok_or_else(|| Error::Corruption {
        message: "persistent database is missing manifest store".to_owned(),
    })?;

    let pending_file_ids = {
        let manifest = manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?;
        let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest.state());
        // Manifest metadata is the deletion authority. A pending entry that is
        // still referenced is inconsistent, so leave it on disk instead of
        // risking a read-visible blob file.
        manifest
            .state()
            .pending_blob_deletions()
            .keys()
            .copied()
            .filter(|file_id| !referenced_blob_ids.contains(file_id))
            .collect::<Vec<_>>()
    };
    if pending_file_ids.is_empty() {
        return Ok(());
    }

    for file_id in &pending_file_ids {
        match fs::remove_file(blob::blob_path(db_path, *file_id)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Io(error)),
        }
    }

    manifest
        .lock()
        .map_err(|_| lock_poisoned("manifest store"))?
        .clear_pending_blob_deletions(&pending_file_ids)
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
