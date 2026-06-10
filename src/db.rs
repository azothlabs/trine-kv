use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    ops::Bound,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{
    blob::{self, ValueRef},
    bucket::{Bucket, BucketName, BucketReader, DEFAULT_BUCKET_NAME},
    cache, compaction,
    error::{Error, Result},
    iterator::{Direction, Iter, LazyIter, ScanSelector, ScanSourceInput},
    lsm::{
        AsyncPointReadIo, CompactionInput as LsmCompactionInput,
        CompactionOutput as LsmCompactionOutput,
        CompactionTablePayload as LsmCompactionTablePayload, FlushInput as LsmFlushInput,
        LsmPointReadSnapshot, LsmTree,
    },
    manifest::{self, ManifestState, ManifestStore},
    object_store::{ObjectClient, ObjectStoreBackend},
    options::{
        BlobLevelMergePolicy, BucketOptions, DbOptions, DurabilityMode, FailOnCorruptionPolicy,
        FilterPolicy, HostStorageBackend, PrefixFilterPolicy, StorageMode, WriteOptions,
    },
    point_value::PointValue,
    recovery,
    runtime::{self, CancellationToken, Runtime, RuntimeTask},
    snapshot::{Snapshot, SnapshotTracker},
    stats::{BlobReadMetrics, DbStats, LevelStats},
    storage::{
        BlockingStorageDirectoryCreateBackend, BlockingStorageDirectoryListBackend,
        BlockingStorageDirectorySyncBackend, BlockingStorageObjectDeleteBackend,
        BlockingStorageReadBackend, BlockingStorageReadObject, NativeFileBackend,
        StorageCapability, StorageDirectoryCreateBackend, StorageDirectoryFile, StorageDirectoryId,
        StorageDirectoryListBackend, StorageManifestReadBackend, StorageObjectId,
        StorageObjectKind, StorageObjectListBackend, StorageObjectReadBackend, StorageReadBackend,
    },
    substrate::{
        DurabilitySubstrate, FilesystemSubstrate, ObjectStoreSubstrate, ObjectWriterLease,
    },
    table::{self, Table},
    transaction::{Transaction, TransactionOptions},
    types::{CommitInfo, KeyRange, Sequence, Value},
    wal::{self, WalBatch, WalFrontDoor},
    write_batch::BatchOperation,
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::{
    storage::{
        BrowserStorageBackend, BrowserWriterLease, StorageObjectDeleteBackend,
        StorageWriterLeaseBackend,
    },
    wal::BrowserWalFrontDoor,
};

mod commit;

/// Database handle for reads, writes, snapshots, buckets, and maintenance.
#[derive(Debug)]
pub struct Db {
    inner: Arc<DbInner>,
    counts_as_user_handle: bool,
}

/// Converts common open inputs into `DbOptions`.
pub trait IntoOpenOptions {
    /// Converts this value into database open options.
    fn into_open_options(self) -> DbOptions;
}

impl IntoOpenOptions for DbOptions {
    fn into_open_options(self) -> DbOptions {
        self
    }
}

impl<P> IntoOpenOptions for P
where
    P: AsRef<Path>,
{
    fn into_open_options(self) -> DbOptions {
        DbOptions::new(self.as_ref())
    }
}

/// Cooperative maintenance work budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceBudget {
    max_flush_inputs: usize,
    max_compaction_inputs: usize,
}

impl MaintenanceBudget {
    /// Default number of immutable memtables to flush per maintenance call.
    pub const DEFAULT_MAX_FLUSH_INPUTS: usize = 1;
    /// Default number of compaction inputs to process per maintenance call.
    pub const DEFAULT_MAX_COMPACTION_INPUTS: usize = 1;

    /// Creates a maintenance budget, treating zero limits as one.
    #[must_use]
    pub const fn new(max_flush_inputs: usize, max_compaction_inputs: usize) -> Self {
        let max_flush_inputs = if max_flush_inputs == 0 {
            1
        } else {
            max_flush_inputs
        };
        let max_compaction_inputs = if max_compaction_inputs == 0 {
            1
        } else {
            max_compaction_inputs
        };
        Self {
            max_flush_inputs,
            max_compaction_inputs,
        }
    }

    /// Creates the default one-unit maintenance budget.
    #[must_use]
    pub const fn single_unit() -> Self {
        Self::new(
            Self::DEFAULT_MAX_FLUSH_INPUTS,
            Self::DEFAULT_MAX_COMPACTION_INPUTS,
        )
    }

    /// Creates a budget that does not intentionally limit maintenance inputs.
    #[must_use]
    pub const fn unbounded() -> Self {
        Self::new(usize::MAX, usize::MAX)
    }

    /// Returns the maximum number of flush inputs.
    #[must_use]
    pub const fn max_flush_inputs(self) -> usize {
        self.max_flush_inputs
    }

    /// Returns the maximum number of compaction inputs.
    #[must_use]
    pub const fn max_compaction_inputs(self) -> usize {
        self.max_compaction_inputs
    }

    fn flush_input_limit(self) -> usize {
        self.max_flush_inputs.max(1)
    }

    fn compaction_input_limit(self) -> usize {
        self.max_compaction_inputs.max(1)
    }
}

impl Default for MaintenanceBudget {
    fn default() -> Self {
        Self::single_unit()
    }
}

/// Result of a cooperative maintenance call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceOutcome {
    /// Number of flushes completed.
    pub flushes: usize,
    /// Number of compactions completed.
    pub compactions: usize,
    /// Whether the supplied budget stopped more available work.
    pub budget_exhausted: bool,
    /// Whether maintenance was already running elsewhere.
    pub busy: bool,
}

impl MaintenanceOutcome {
    /// Returns `true` when at least one flush or compaction completed.
    #[must_use]
    pub const fn made_progress(self) -> bool {
        self.flushes != 0 || self.compactions != 0
    }

    /// Returns whether the supplied budget stopped more available work.
    #[must_use]
    pub const fn budget_exhausted(self) -> bool {
        self.budget_exhausted
    }

    /// Returns whether maintenance was already running elsewhere.
    #[must_use]
    pub const fn busy(self) -> bool {
        self.busy
    }

    fn busy_outcome() -> Self {
        Self {
            busy: true,
            ..Self::default()
        }
    }

    fn add_assign(&mut self, other: Self) {
        self.flushes = self.flushes.saturating_add(other.flushes);
        self.compactions = self.compactions.saturating_add(other.compactions);
        self.budget_exhausted |= other.budget_exhausted;
        self.busy |= other.busy;
    }
}

#[derive(Debug)]
pub(crate) struct DbInner {
    options: DbOptions,
    user_handles: AtomicUsize,
    commit_tracker: CommitTracker,
    closed: AtomicBool,
    publish_barrier: PublishBarrier,
    memtable_publish_lock: Mutex<()>,
    buckets: RwLock<BTreeMap<String, Arc<LsmTree>>>,
    snapshots: Arc<SnapshotTracker>,
    pending_obsolete_table_ids: Mutex<BTreeSet<table::TableId>>,
    manifest: Option<Mutex<ManifestStore>>,
    // The write-ahead log + single-writer lease behind the Band 3 durability
    // substrate (see `src/substrate.rs`). The native/WASI persistent path holds a
    // filesystem WAL + LOCK lease here; in-memory and the browser path hold an
    // inert substrate (browser durability still rides the `browser_*` fields).
    substrate: DurabilitySubstrate,
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
    maintenance_cooperative_yields: AtomicU64,
    maintenance_budget_exhaustions: AtomicU64,
    native_storage: NativeFileBackend,
    /// Object-storage byte backend for object-store databases (async-only),
    /// mirroring `browser_storage`. `None` for every other backend; when set,
    /// `native_storage` is an unused default and `substrate` is `ObjectStore`.
    object_storage: Option<ObjectStoreBackend>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_storage: Option<BrowserStorageBackend>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(dead_code)]
    browser_writer_lease: Option<BrowserWriterLease>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_wal: Option<BrowserWalFrontDoor>,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    browser_manifest_async_lock: futures::lock::Mutex<()>,
    runtime: Runtime,
    runtime_shutdown: CancellationToken,
    maintenance: Arc<MaintenanceCoordinator>,
    background_workers: Mutex<Vec<RuntimeTask>>,
}

#[derive(Debug)]
struct PersistentOpenParts {
    options: DbOptions,
    runtime: Runtime,
    native_storage: NativeFileBackend,
    process_lock: Option<recovery::ProcessLock>,
    buckets: BTreeMap<String, Arc<LsmTree>>,
    manifest: ManifestStore,
    wal: Option<WalFrontDoor>,
    batches: Vec<WalBatch>,
    replay_floor: Sequence,
    db_path_for_cleanup: PathBuf,
}

#[derive(Debug)]
pub(super) struct CommitTracker {
    last_reserved_sequence: AtomicU64,
    visible_sequence: AtomicU64,
    skipped_slots: AtomicU64,
    slots: Mutex<BTreeMap<u64, CommitSlotState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CommitSlot {
    sequence: Sequence,
}

#[derive(Debug)]
pub(super) struct PublishBarrier {
    lock: Mutex<()>,
}

#[derive(Debug)]
pub(super) struct PublishBarrierGuard<'barrier> {
    _guard: MutexGuard<'barrier, ()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitSlotState {
    Open,
    Visible,
    Skipped,
}

impl CommitTracker {
    fn new(visible_sequence: Sequence) -> Self {
        Self {
            last_reserved_sequence: AtomicU64::new(visible_sequence.get()),
            visible_sequence: AtomicU64::new(visible_sequence.get()),
            skipped_slots: AtomicU64::new(0),
            slots: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    fn visible_sequence(&self) -> Sequence {
        Sequence::new(self.visible_sequence.load(Ordering::Acquire))
    }

    fn reset_visible_boundary(&self, visible_sequence: Sequence) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        slots.clear();
        self.visible_sequence
            .store(visible_sequence.get(), Ordering::Release);
        self.last_reserved_sequence
            .store(visible_sequence.get(), Ordering::Release);
        self.skipped_slots.store(0, Ordering::Release);
        Ok(())
    }

    fn last_reserved_sequence(&self) -> Sequence {
        Sequence::new(self.last_reserved_sequence.load(Ordering::Acquire))
    }

    fn open_slot_count(&self) -> usize {
        self.slots
            .lock()
            .map(|slots| {
                slots
                    .values()
                    .filter(|state| **state == CommitSlotState::Open)
                    .count()
            })
            .unwrap_or(0)
    }

    fn skipped_slot_count(&self) -> u64 {
        self.skipped_slots.load(Ordering::Acquire)
    }

    pub(super) fn reserve_slot(&self) -> Result<CommitSlot> {
        let reserved = self
            .last_reserved_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Error::Corruption {
                message: "sequence counter overflow".to_owned(),
            })?
            .checked_add(1)
            .ok_or_else(|| Error::Corruption {
                message: "sequence counter overflow".to_owned(),
            })?;
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        if slots.insert(reserved, CommitSlotState::Open).is_some() {
            return Err(Error::Corruption {
                message: format!("commit slot {reserved} was reserved twice"),
            });
        }
        Ok(CommitSlot {
            sequence: Sequence::new(reserved),
        })
    }

    pub(super) fn mark_visible(&self, slot: CommitSlot) -> Result<()> {
        self.mark_terminal(slot, CommitSlotState::Visible)
    }

    pub(super) fn mark_skipped(&self, slot: CommitSlot) -> Result<()> {
        self.mark_terminal(slot, CommitSlotState::Skipped)
    }

    fn mark_terminal(&self, slot: CommitSlot, terminal_state: CommitSlotState) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        let state = slots
            .get_mut(&slot.sequence.get())
            .ok_or_else(|| Error::Corruption {
                message: format!("commit slot {} is missing", slot.sequence.get()),
            })?;
        match *state {
            CommitSlotState::Open => {
                *state = terminal_state;
                if terminal_state == CommitSlotState::Skipped {
                    self.skipped_slots.fetch_add(1, Ordering::AcqRel);
                }
                self.advance_visible_sequence(&mut slots);
                Ok(())
            }
            CommitSlotState::Visible | CommitSlotState::Skipped => Err(Error::Corruption {
                message: format!("commit slot {} is already terminal", slot.sequence.get()),
            }),
        }
    }

    fn advance_visible_sequence(&self, slots: &mut BTreeMap<u64, CommitSlotState>) {
        let mut visible = self.visible_sequence.load(Ordering::Acquire);
        while let Some(next) = visible.checked_add(1) {
            match slots.get(&next).copied() {
                Some(CommitSlotState::Visible | CommitSlotState::Skipped) => {
                    slots.remove(&next);
                    visible = next;
                    self.visible_sequence.store(visible, Ordering::Release);
                }
                Some(CommitSlotState::Open) | None => break,
            }
        }
    }
}

impl PublishBarrier {
    fn new() -> Self {
        Self {
            lock: Mutex::new(()),
        }
    }

    pub(super) fn enter(&self) -> Result<PublishBarrierGuard<'_>> {
        self.lock
            .lock()
            .map(|guard| PublishBarrierGuard { _guard: guard })
            .map_err(|_| lock_poisoned("publish barrier"))
    }
}

impl CommitSlot {
    #[must_use]
    pub(super) const fn sequence(self) -> Sequence {
        self.sequence
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MaintenanceRequest {
    flush: bool,
    compaction: bool,
}

impl MaintenanceRequest {
    #[must_use]
    const fn any(self) -> bool {
        self.flush || self.compaction
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WritePressure {
    flush: bool,
    compaction: bool,
}

impl WritePressure {
    #[must_use]
    const fn none(self) -> bool {
        !self.flush && !self.compaction
    }

    #[must_use]
    const fn request(self) -> MaintenanceRequest {
        MaintenanceRequest {
            flush: self.flush,
            compaction: self.compaction,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactionReservation {
    bucket: String,
    range: KeyRange,
}

#[derive(Debug)]
struct MaintenanceCoordinator {
    state: Mutex<MaintenanceState>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct MaintenanceState {
    flush_requests: usize,
    compaction_requests: usize,
    active_flushes: usize,
    active_compactions: Vec<CompactionReservation>,
    progress: u64,
    shutdown: bool,
    last_error: Option<String>,
}

#[derive(Debug)]
struct MaintenanceFlushGuard {
    coordinator: Arc<MaintenanceCoordinator>,
}

#[derive(Debug)]
struct MaintenanceCompactionGuard {
    coordinator: Arc<MaintenanceCoordinator>,
    reservations: Vec<CompactionReservation>,
}

impl MaintenanceCoordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(MaintenanceState::default()),
            wake: Condvar::new(),
        }
    }

    fn request(&self, request: MaintenanceRequest) {
        if !request.any() {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if request.flush {
                state.flush_requests = state.flush_requests.saturating_add(1);
            }
            if request.compaction {
                state.compaction_requests = state.compaction_requests.saturating_add(1);
            }
            self.wake.notify_all();
        }
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    fn wait_for_request(&self) -> Option<MaintenanceRequest> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        while state.flush_requests == 0 && state.compaction_requests == 0 && !state.shutdown {
            let Ok(next_state) = self.wake.wait(state) else {
                return None;
            };
            state = next_state;
        }
        if state.shutdown {
            return None;
        }
        let request = MaintenanceRequest {
            flush: state.flush_requests != 0,
            compaction: state.compaction_requests != 0,
        };
        state.flush_requests = 0;
        state.compaction_requests = 0;
        self.wake.notify_all();
        Some(request)
    }

    fn progress(&self) -> u64 {
        self.state.lock().map_or(0, |state| state.progress)
    }

    fn wait_for_progress(&self, observed_progress: u64, timeout: Duration) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        while state.progress == observed_progress && !state.shutdown && state.last_error.is_none() {
            let Ok((next_state, wait_result)) = self.wake.wait_timeout(state, timeout) else {
                return false;
            };
            state = next_state;
            if wait_result.timed_out() {
                break;
            }
        }
        state.progress != observed_progress || state.shutdown || state.last_error.is_some()
    }

    fn wait_until_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while state.active_flushes != 0 || !state.active_compactions.is_empty() {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    fn wait_until_flush_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while (state.flush_requests != 0 || state.active_flushes != 0)
            && !state.shutdown
            && state.last_error.is_none()
        {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    fn wait_until_compaction_idle(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while (state.compaction_requests != 0 || !state.active_compactions.is_empty())
            && !state.shutdown
            && state.last_error.is_none()
        {
            let Ok(next_state) = self.wake.wait(state) else {
                return;
            };
            state = next_state;
        }
    }

    fn has_pending_compaction(&self) -> bool {
        self.state.lock().is_ok_and(|state| {
            state.compaction_requests != 0 || !state.active_compactions.is_empty()
        })
    }

    fn try_start_flush(self: &Arc<Self>) -> Option<MaintenanceFlushGuard> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        if state.shutdown || state.active_flushes != 0 {
            return None;
        }
        state.active_flushes = 1;
        Some(MaintenanceFlushGuard {
            coordinator: Arc::clone(self),
        })
    }

    fn reserve_compactions(
        self: &Arc<Self>,
        candidates: Vec<CompactionReservation>,
    ) -> Option<MaintenanceCompactionGuard> {
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        if state.shutdown {
            return None;
        }

        let mut reservations = Vec::new();
        for candidate in candidates {
            if state
                .active_compactions
                .iter()
                .any(|active| compaction_reservations_conflict(active, &candidate))
            {
                continue;
            }
            state.active_compactions.push(candidate.clone());
            reservations.push(candidate);
        }

        if reservations.is_empty() {
            return None;
        }

        Some(MaintenanceCompactionGuard {
            coordinator: Arc::clone(self),
            reservations,
        })
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    fn record_error(&self, error: &Error) {
        if let Ok(mut state) = self.state.lock() {
            state.last_error = Some(error.to_string());
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
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

    fn finish_flush(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active_flushes = state.active_flushes.saturating_sub(1);
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }

    fn finish_compactions(&self, reservations: &[CompactionReservation]) {
        if let Ok(mut state) = self.state.lock() {
            state
                .active_compactions
                .retain(|active| !reservations.iter().any(|finished| finished == active));
            state.progress = state.progress.saturating_add(1);
            self.wake.notify_all();
        }
    }
}

impl Drop for MaintenanceFlushGuard {
    fn drop(&mut self) {
        self.coordinator.finish_flush();
    }
}

impl Drop for MaintenanceCompactionGuard {
    fn drop(&mut self) {
        self.coordinator.finish_compactions(&self.reservations);
    }
}

impl MaintenanceCompactionGuard {
    fn contains(&self, bucket: &str, range: &KeyRange) -> bool {
        self.reservations
            .iter()
            .any(|reservation| reservation.bucket == bucket && reservation.range == *range)
    }
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
fn record_maintenance_success(_maintenance: &MaintenanceCoordinator) {
    // A later successful maintenance pass must not hide a failure that no
    // caller has observed yet. `take_error` is the only path that clears it.
}

fn compaction_reservations_conflict(
    left: &CompactionReservation,
    right: &CompactionReservation,
) -> bool {
    left.bucket == right.bucket && key_ranges_overlap(&left.range, &right.range)
}

fn key_ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    !range_end_is_before_start(&left.end, &right.start)
        && !range_end_is_before_start(&right.end, &left.start)
}

fn range_end_is_before_start(end: &Bound<Vec<u8>>, start: &Bound<Vec<u8>>) -> bool {
    match (end, start) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(end), Bound::Included(start)) => end < start,
        (Bound::Included(end), Bound::Excluded(start))
        | (Bound::Excluded(end), Bound::Included(start) | Bound::Excluded(start)) => end <= start,
    }
}

fn shutdown_background_workers(
    maintenance: &Arc<MaintenanceCoordinator>,
    runtime_shutdown: &CancellationToken,
    workers: &Mutex<Vec<RuntimeTask>>,
) {
    runtime_shutdown.cancel();
    maintenance.shutdown();
    let workers = workers
        .lock()
        .map(|mut workers| std::mem::take(&mut *workers))
        .unwrap_or_default();

    for worker in workers {
        if worker.is_current_thread() {
            continue;
        }
        let _ = worker.join();
    }
    maintenance.wait_until_idle();
}

impl Drop for DbInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        shutdown_background_workers(
            &self.maintenance,
            &self.runtime_shutdown,
            &self.background_workers,
        );
        let _ = cleanup_pending_obsolete_table_files(
            &self.native_storage,
            persistent_path_from_options(&self.options),
            &self.snapshots,
            &self.pending_obsolete_table_ids,
        );
        let _ = cleanup_pending_obsolete_blob_files(
            &self.native_storage,
            persistent_path_from_options(&self.options),
            &self.snapshots,
            self.manifest.as_ref(),
        );
    }
}

impl Clone for Db {
    fn clone(&self) -> Self {
        if self.counts_as_user_handle {
            self.inner.user_handles.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            inner: Arc::clone(&self.inner),
            counts_as_user_handle: self.counts_as_user_handle,
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.counts_as_user_handle {
            return;
        }
        if self.inner.user_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.closed.store(true, Ordering::Release);
            shutdown_background_workers(
                &self.inner.maintenance,
                &self.inner.runtime_shutdown,
                &self.inner.background_workers,
            );
        }
    }
}

impl Db {
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
                backend: HostStorageBackend::Browser,
            } => Err(Error::unsupported_backend(
                HostStorageBackend::Browser.as_str(),
            )),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => Err(Error::unsupported_backend(
                "object-store databases are async-only; use the async open with a client",
            )),
        }
    }

    fn open_wasi_persistent_with_options(options: DbOptions) -> Result<Self> {
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
    async fn open_wasi_persistent_with_options_async(options: DbOptions) -> Result<Self> {
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
    fn validate_wasi_persistent_options(options: &DbOptions) -> Result<()> {
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
            DurabilityMode::SyncData | DurabilityMode::SyncAll
        ) {
            return Err(Error::unsupported_durability(options.durability));
        }
        Ok(())
    }

    #[allow(clippy::unused_async)]
    async fn open_browser_persistent_with_options_async(options: DbOptions) -> Result<Self> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Self::open_browser_persistent_with_options_async_inner(options).await
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            drop(options);
            Err(Error::unsupported_backend(
                HostStorageBackend::Browser.as_str(),
            ))
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(clippy::arc_with_non_send_sync)]
    async fn open_browser_persistent_with_options_async_inner(options: DbOptions) -> Result<Self> {
        Self::validate_browser_persistent_options(&options)?;
        let storage = BrowserStorageBackend::new().await?;
        let db_path = Path::new("");
        let manifest_path = manifest::manifest_path(db_path);
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
            recovery::fail_on_safe_temporary_files_with_backend_async(&storage, db_path).await?;
        } else {
            recovery::repair_safe_temporary_files_with_backend_async(
                &storage,
                db_path,
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

        run_persistent_recovery_checks_async(&storage, db_path, manifest.state()).await?;
        let mut buckets =
            buckets_from_manifest_async(&storage, db_path, manifest.state(), true).await?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;

        let wal_streams =
            wal::read_recovery_streams_after_with_backend_async(&storage, db_path, replay_floor)
                .await?;
        let batches = wal::merge_batch_streams_by_sequence(wal_streams)?;
        let browser_wal = if options.read_only {
            None
        } else {
            Some(
                BrowserWalFrontDoor::open_sharded_with_backend(
                    &storage,
                    db_path,
                    wal::DEFAULT_WAL_SHARD_COUNT,
                )
                .await?,
            )
        };
        let block_cache_bytes = options.block_cache_bytes;
        let runtime = Runtime::new(options.runtime);
        let db = Self {
            inner: Arc::new(DbInner {
                options,
                user_handles: AtomicUsize::new(1),
                commit_tracker: CommitTracker::new(Sequence::ZERO),
                closed: AtomicBool::new(false),
                publish_barrier: PublishBarrier::new(),
                memtable_publish_lock: Mutex::new(()),
                buckets: RwLock::new(buckets),
                snapshots: Arc::new(SnapshotTracker::default()),
                pending_obsolete_table_ids: Mutex::new(BTreeSet::new()),
                manifest: Some(Mutex::new(manifest)),
                // Browser durability rides the `browser_*` fields; the substrate
                // is inert here.
                substrate: DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None)),
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
                maintenance_cooperative_yields: AtomicU64::new(0),
                maintenance_budget_exhaustions: AtomicU64::new(0),
                native_storage: NativeFileBackend::new(),
                object_storage: None,
                browser_storage: Some(storage),
                browser_writer_lease: writer_lease,
                browser_wal,
                browser_manifest_async_lock: futures::lock::Mutex::new(()),
                runtime,
                runtime_shutdown: CancellationToken::new(),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
                background_workers: Mutex::new(Vec::new()),
            }),
            counts_as_user_handle: true,
        };
        db.replay_wal_batches(batches, replay_floor)?;
        Ok(db)
    }

    /// Open an object-storage database (async-only). Internal until a public
    /// object-store client API lands; tested with the in-memory `ObjectClient`.
    ///
    /// The byte IO, the manifest (CAS publish), and the writer lease all ride
    /// `client`. There is no WAL — a commit is durable once its memtable is
    /// flushed to objects and the manifest CAS publishes them — so the recovered
    /// visible sequence is the maximum `largest_sequence` across the manifest's
    /// tables.
    #[allow(dead_code)]
    pub(crate) async fn open_object_store_async(
        client: Arc<dyn ObjectClient>,
        options: DbOptions,
    ) -> Result<Self> {
        if !options.storage_mode.is_object_store_persistent() {
            return Err(Error::invalid_options(
                "object-store open requires the object-store storage mode",
            ));
        }
        validate_common_options(&options)?;

        let backend = ObjectStoreBackend::new(Arc::clone(&client));
        let db_path = Path::new("");
        let manifest_key = manifest::manifest_path(db_path)
            .to_string_lossy()
            .into_owned();

        let mut manifest =
            ManifestStore::open_object_store_async(Arc::clone(&client), manifest_key).await?;
        ensure_default_bucket_in_manifest_async(&mut manifest, &options).await?;

        let mut buckets =
            buckets_from_manifest_async(&backend, db_path, manifest.state(), true).await?;
        ensure_default_bucket_loaded(&mut buckets, &options)?;

        // No WAL: the durable visible boundary is the newest sequence already
        // flushed into the manifest's tables.
        let visible_sequence = manifest
            .state()
            .tables()
            .values()
            .flatten()
            .map(|properties| properties.largest_sequence)
            .max()
            .unwrap_or(Sequence::ZERO);

        let substrate = if options.read_only {
            DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None))
        } else {
            let lease_key = db_path
                .join(recovery::PROCESS_LOCK_FILE_NAME)
                .to_string_lossy()
                .into_owned();
            let lease = ObjectWriterLease::acquire(Arc::clone(&client), lease_key).await?;
            DurabilitySubstrate::ObjectStore(ObjectStoreSubstrate::new(lease))
        };

        let block_cache_bytes = options.block_cache_bytes;
        let runtime = Runtime::new(options.runtime);
        Ok(Self {
            inner: Arc::new(DbInner {
                options,
                user_handles: AtomicUsize::new(1),
                commit_tracker: CommitTracker::new(visible_sequence),
                closed: AtomicBool::new(false),
                publish_barrier: PublishBarrier::new(),
                memtable_publish_lock: Mutex::new(()),
                buckets: RwLock::new(buckets),
                snapshots: Arc::new(SnapshotTracker::default()),
                pending_obsolete_table_ids: Mutex::new(BTreeSet::new()),
                manifest: Some(Mutex::new(manifest)),
                substrate,
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
                maintenance_cooperative_yields: AtomicU64::new(0),
                maintenance_budget_exhaustions: AtomicU64::new(0),
                native_storage: NativeFileBackend::new(),
                object_storage: Some(backend),
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_storage: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_writer_lease: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_wal: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_manifest_async_lock: futures::lock::Mutex::new(()),
                runtime,
                runtime_shutdown: CancellationToken::new(),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
                background_workers: Mutex::new(Vec::new()),
            }),
            counts_as_user_handle: true,
        })
    }

    #[cfg_attr(
        not(all(target_arch = "wasm32", target_os = "unknown")),
        allow(dead_code)
    )]
    fn validate_browser_persistent_options(options: &DbOptions) -> Result<()> {
        if !matches!(
            options.storage_mode,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser
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
            DurabilityMode::SyncData | DurabilityMode::SyncAll
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
    fn memory_sync(mut options: DbOptions) -> Result<Self> {
        options.storage_mode = StorageMode::InMemory;
        validate_options(&options)?;
        let block_cache_bytes = options.block_cache_bytes;
        let runtime = Runtime::new(options.runtime);
        let default_bucket = Arc::new(LsmTree::new(
            options.default_bucket_options.clone(),
            Vec::new(),
        )?);
        let mut buckets = BTreeMap::new();
        buckets.insert(DEFAULT_BUCKET_NAME.to_owned(), default_bucket);

        Ok(Self {
            inner: Arc::new(DbInner {
                options,
                user_handles: AtomicUsize::new(1),
                commit_tracker: CommitTracker::new(Sequence::ZERO),
                closed: AtomicBool::new(false),
                publish_barrier: PublishBarrier::new(),
                memtable_publish_lock: Mutex::new(()),
                buckets: RwLock::new(buckets),
                snapshots: Arc::new(SnapshotTracker::default()),
                pending_obsolete_table_ids: Mutex::new(BTreeSet::new()),
                manifest: None,
                // In-memory: no WAL, no lease.
                substrate: DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None)),
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
                maintenance_cooperative_yields: AtomicU64::new(0),
                maintenance_budget_exhaustions: AtomicU64::new(0),
                native_storage: NativeFileBackend::new(),
                object_storage: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_storage: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_writer_lease: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_wal: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_manifest_async_lock: futures::lock::Mutex::new(()),
                runtime,
                runtime_shutdown: CancellationToken::new(),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
                background_workers: Mutex::new(Vec::new()),
            }),
            counts_as_user_handle: true,
        })
    }

    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    fn open_persistent_with_options(options: DbOptions) -> Result<Self> {
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

        let process_lock = acquire_persistent_process_lock(&native_storage, path, &options)?;
        let directory_files = list_persistent_directory_files(&native_storage, path)?;
        repair_safe_temporary_files_for_open(&native_storage, path, &options, &directory_files)?;

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
                wal::DEFAULT_WAL_SHARD_COUNT,
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

    async fn open_persistent_with_options_async(options: DbOptions) -> Result<Self> {
        Self::open_persistent_with_options_async_inner(options, true).await
    }

    async fn open_persistent_with_options_async_inner(
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

        let process_lock =
            acquire_persistent_process_lock_async(&native_storage, path, &options).await?;
        let directory_files = list_persistent_directory_files_async(&native_storage, path).await?;
        repair_safe_temporary_files_for_open_from_directory_files_async(
            &native_storage,
            path,
            &options,
            &directory_files,
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
                wal::DEFAULT_WAL_SHARD_COUNT,
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

    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        allow(clippy::arc_with_non_send_sync)
    )]
    fn from_persistent_open_parts(parts: PersistentOpenParts) -> Result<Self> {
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
        let block_cache_bytes = options.block_cache_bytes;

        let db = Self {
            inner: Arc::new(DbInner {
                options,
                user_handles: AtomicUsize::new(1),
                commit_tracker: CommitTracker::new(Sequence::ZERO),
                closed: AtomicBool::new(false),
                publish_barrier: PublishBarrier::new(),
                memtable_publish_lock: Mutex::new(()),
                buckets: RwLock::new(buckets),
                snapshots: Arc::new(SnapshotTracker::default()),
                pending_obsolete_table_ids: Mutex::new(BTreeSet::new()),
                manifest: Some(Mutex::new(manifest)),
                substrate: DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(
                    wal,
                    process_lock,
                )),
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
                maintenance_cooperative_yields: AtomicU64::new(0),
                maintenance_budget_exhaustions: AtomicU64::new(0),
                native_storage,
                object_storage: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_storage: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_writer_lease: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_wal: None,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                browser_manifest_async_lock: futures::lock::Mutex::new(()),
                runtime,
                runtime_shutdown: CancellationToken::new(),
                maintenance: Arc::new(MaintenanceCoordinator::new()),
                background_workers: Mutex::new(Vec::new()),
            }),
            counts_as_user_handle: true,
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
    /// Direct helpers such as `Db::put_sync` and `Db::get_sync` use this bucket without
    /// requiring callers to open it explicitly.
    pub fn default_bucket_sync(&self) -> Result<Bucket> {
        let state = self.bucket_state(DEFAULT_BUCKET_NAME)?;
        let options = state.options.clone();
        Ok(Bucket::new(
            self.clone(),
            BucketName::new(DEFAULT_BUCKET_NAME),
            options,
            state,
        ))
    }

    /// Returns an existing named bucket or creates it with default
    /// `BucketOptions`.
    ///
    /// The built-in default bucket is reserved for direct `Db` helpers and
    /// `Db::default_bucket_sync`; using `"default"` as a named bucket returns an
    /// error.
    pub fn bucket_sync(&self, name: impl Into<BucketName>) -> Result<Bucket> {
        self.bucket_with_options_sync(name, BucketOptions::default())
    }

    /// Returns an existing named bucket or creates it with explicit options.
    ///
    /// Bucket options are fixed after creation. Calling this for an existing
    /// named bucket with different options returns an error. The built-in
    /// default bucket is configured through `DbOptions::default_bucket_options`.
    pub fn bucket_with_options_sync(
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

        if let Some(existing_state) = self.bucket_state_if_exists(name.as_str())? {
            let existing_options = existing_state.options.clone();
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(
                self.clone(),
                name,
                existing_options,
                existing_state,
            ));
        }

        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent bucket creation requires async API",
            ));
        }

        self.persist_bucket_creation(name.as_str(), &options)?;

        let (bucket_options, state) = {
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
                (state.options.clone(), Arc::clone(state))
            } else {
                let bucket_options = options.clone();
                let state = Arc::new(LsmTree::new(options, Vec::new())?);
                buckets.insert(name.as_str().to_owned(), Arc::clone(&state));
                (bucket_options, state)
            }
        };

        Ok(Bucket::new(self.clone(), name, bucket_options, state))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn bucket_with_options_browser_async(
        &self,
        name: BucketName,
        options: BucketOptions,
    ) -> Result<Bucket> {
        self.ensure_open()?;

        if name.as_str().is_empty() {
            return Err(Error::invalid_options("bucket name cannot be empty"));
        }
        if name.as_str() == DEFAULT_BUCKET_NAME {
            return Err(Error::invalid_options(
                "default bucket is accessed through Db helpers",
            ));
        }

        validate_bucket_options(&options)?;

        if let Some(existing_state) = self.bucket_state_if_exists(name.as_str())? {
            let existing_options = existing_state.options.clone();
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(
                self.clone(),
                name,
                existing_options,
                existing_state,
            ));
        }

        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
        if let Some(existing_state) = self.bucket_state_if_exists(name.as_str())? {
            let existing_options = existing_state.options.clone();
            if existing_options != options {
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            return Ok(Bucket::new(
                self.clone(),
                name,
                existing_options,
                existing_state,
            ));
        }

        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "browser persistent database is missing manifest store".to_owned(),
            })?;
        let prepared_publish = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            manifest.prepare_create_bucket_publish(name.as_str().to_owned(), options.clone())?
        };
        if let Some(prepared_publish) = prepared_publish {
            prepared_publish.publish_async().await?;
            manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .install_prepared_publish(prepared_publish)?;
        }

        let (bucket_options, state) = {
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
                (state.options.clone(), Arc::clone(state))
            } else {
                let bucket_options = options.clone();
                let state = Arc::new(LsmTree::new(options, Vec::new())?);
                buckets.insert(name.as_str().to_owned(), Arc::clone(&state));
                (bucket_options, state)
            }
        };

        Ok(Bucket::new(self.clone(), name, bucket_options, state))
    }

    /// Reads the newest committed value for `key` from the default bucket.
    ///
    /// The `key` parameter is compared as raw bytes. The returned value is an
    /// owned `Vec<u8>` so callers can keep it after the database handle or
    /// iterator state changes. `Ok(None)` means no visible value exists at the
    /// newest committed sequence, either because the key was never written or
    /// because the newest visible record is a delete.
    ///
    /// This method searches the active memtable, immutable memtables, and table
    /// files in newest-to-oldest MVCC order. Large values stored in blob files
    /// are read before the method returns.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes in the built-in default bucket.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, plus storage or
    /// format errors encountered while reading tables or blob files.
    pub fn get_sync(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_sequence(DEFAULT_BUCKET_NAME, key, self.last_committed_sequence())
    }

    /// Reads many newest committed values from the default bucket.
    ///
    /// The returned vector has exactly one entry for each input key, in input
    /// order. `Ok(None)` at a position means that key has no value visible at
    /// the read sequence captured for this batch, either because it was never
    /// written or because its newest visible record is a delete. Duplicate
    /// input keys produce duplicate result entries; this method does not
    /// reorder or deduplicate keys.
    ///
    /// A batch captures one committed read sequence and one set of point-read
    /// sources before reading the first key. That gives all keys a consistent
    /// view and avoids rebuilding the default bucket read state for each key.
    /// Large blob-backed values are read before the method returns.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes in the built-in default bucket. The slice may
    ///   be empty; an empty input returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, plus storage or
    /// format errors encountered while reading tables or blob files. Any such
    /// error fails the whole batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// # fn main() -> trine_kv::Result<()> {
    /// let db = Db::open_sync(DbOptions::memory())?;
    /// db.put_sync(b"a", b"one")?;
    /// db.put_sync(b"b", b"two")?;
    ///
    /// let keys = [b"a".as_slice(), b"missing".as_slice(), b"b".as_slice()];
    /// let values = db.get_many_sync(&keys)?;
    /// assert_eq!(values, vec![Some(b"one".to_vec()), None, Some(b"two".to_vec())]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_many_sync<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        self.default_bucket_sync()?.get_many_sync(keys)
    }

    /// Reads `key` from the default bucket at the sequence pinned by `snapshot`.
    ///
    /// This is the repeatable-read form of [`Db::get_sync`]. Later commits do
    /// not affect the result because visibility is capped at
    /// `snapshot.read_sequence()`.
    ///
    /// # Parameters
    ///
    /// - `snapshot`: snapshot whose sequence defines read visibility.
    /// - `key`: user key bytes in the built-in default bucket.
    pub fn get_at_sync(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_with_pin_state(
            DEFAULT_BUCKET_NAME,
            key,
            snapshot.read_sequence(),
            snapshot.is_pinned(),
        )
    }

    /// Writes one key/value pair to the default bucket using default write options.
    ///
    /// The write is assigned the next commit sequence, appended to the WAL for
    /// persistent databases, inserted into the active memtable, and then made
    /// visible to future reads. The method returns after the configured default
    /// durability has been requested.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes. Empty keys are allowed unless the bucket
    ///   options reject them.
    /// - `value`: value bytes to store. Values at or above the bucket's blob
    ///   threshold may be written to blob files.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] for read-only handles, [`Error::Closed`] for
    /// closed handles, [`Error::InvalidOptions`] for invalid key/options, or
    /// storage errors from WAL/blob writes.
    pub fn put_sync(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options_sync(key, value, WriteOptions::default())
            .map(|_| ())
    }

    /// Writes one key/value pair to the default bucket and returns commit information.
    ///
    /// This is the explicit-options form of [`Db::put_sync`]. Use it when a
    /// specific write needs a different durability level than the database
    /// default.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes.
    /// - `value`: value bytes to store.
    /// - `options`: per-write durability options.
    ///
    /// # Returns
    ///
    /// Returns [`CommitInfo`] containing the sequence assigned to this commit.
    pub fn put_with_options_sync(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.put(key, value);
        self.write_sync(batch, options)
    }

    /// Adds a point delete for one default-bucket key using default write options.
    ///
    /// A point delete creates a new committed record that hides older values for
    /// the same key at later read sequences. Existing snapshots may still see an
    /// older value if their read sequence is before the delete.
    pub fn delete_sync(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options_sync(key, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a point delete for one default-bucket key and returns commit
    /// information.
    pub fn delete_with_options_sync(
        &self,
        key: impl Into<Vec<u8>>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete(key);
        self.write_sync(batch, options)
    }

    /// Adds a range delete to the default bucket using default write options.
    ///
    /// A range delete hides keys in `range` for later read sequences. It is
    /// committed atomically like a point write and participates in transaction
    /// conflict checks. Existing snapshots can continue to see earlier values.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range to hide. Bounds follow `std::ops::Bound`
    ///   semantics through [`KeyRange`].
    pub fn delete_range_sync(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options_sync(range, WriteOptions::default())
            .map(|_| ())
    }

    /// Adds a range delete to the default bucket and returns commit
    /// information.
    pub fn delete_range_with_options_sync(
        &self,
        range: KeyRange,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete_range(range);
        self.write_sync(batch, options)
    }

    /// Returns a forward iterator over default-bucket rows in `range`.
    ///
    /// The iterator yields owned [`crate::KeyValue`] rows in ascending byte
    /// order. Each row is the newest value visible at the sequence captured
    /// when the iterator is created. Point deletes and covering range deletes
    /// are skipped.
    ///
    /// The returned iterator may read table blocks and blob files as iteration
    /// advances. Use [`Db::range_lazy_sync`] when callers want keys first and
    /// large values only on demand.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range to scan.
    pub fn range_sync(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket iterator whose blob values are read on demand.
    ///
    /// This has the same visibility and ordering rules as [`Db::range_sync`],
    /// but yields [`crate::LazyKeyValue`] rows. Inline values are already
    /// available; blob-backed values are read only when
    /// [`crate::LazyValue::read_sync`] or [`crate::LazyValue::into_value_sync`]
    /// is called.
    pub fn range_lazy_sync(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket iterator over `range` at `snapshot`.
    pub fn range_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward value-lazy default-bucket iterator at `snapshot`.
    pub fn range_lazy_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a reverse iterator over default-bucket rows in `range`.
    pub fn range_reverse_sync(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket iterator whose blob values are read on
    /// demand.
    pub fn range_lazy_reverse_sync(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket iterator over `range` at `snapshot`.
    pub fn range_reverse_at_sync(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse value-lazy default-bucket iterator at `snapshot`.
    pub fn range_lazy_reverse_at_sync(
        &self,
        snapshot: &Snapshot,
        range: &KeyRange,
    ) -> Result<LazyIter> {
        self.range_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a forward iterator over default-bucket rows whose keys begin with `prefix`.
    ///
    /// Prefix scans use raw byte-prefix matching over user keys. The bucket's
    /// configured [`crate::PrefixExtractor`] may let Trine skip table or block
    /// reads, but it does not change which keys are returned.
    ///
    /// # Parameters
    ///
    /// - `prefix`: byte prefix that returned keys must start with.
    pub fn prefix_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
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
    pub fn prefix_lazy_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
    }

    /// Returns a forward default-bucket prefix iterator at `snapshot`.
    pub fn prefix_at_sync(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
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
    pub fn prefix_lazy_at_sync(
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
    pub fn prefix_reverse_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
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
    pub fn prefix_lazy_reverse_sync(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
    }

    /// Returns a reverse default-bucket prefix iterator at `snapshot`.
    pub fn prefix_reverse_at_sync(
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
    pub fn prefix_lazy_reverse_at_sync(
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

    /// Persists pending WAL bytes according to `mode`.
    ///
    /// This function does not flush memtables into table files. It asks the WAL
    /// storage backend to push already accepted WAL bytes to the durability
    /// level represented by `mode`. In-memory databases have no durable WAL, so
    /// this is a no-op.
    ///
    /// Use this when writes were committed with a weaker durability mode and
    /// the application later reaches a checkpoint where those commits should be
    /// made stronger. Backends may reject durability modes that they cannot
    /// honestly provide.
    ///
    /// # Parameters
    ///
    /// - `mode`: durability level to request for pending WAL bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the database is closed,
    /// [`Error::UnsupportedDurability`] if the backend cannot provide `mode`,
    /// [`Error::UnsupportedBackend`] for unsupported host backends, or
    /// [`Error::Io`] for storage failures.
    pub fn persist_sync(&self, mode: DurabilityMode) -> Result<()> {
        self.ensure_open()?;

        if self.inner.options.storage_mode.is_wasi_persistent()
            && matches!(mode, DurabilityMode::SyncData | DurabilityMode::SyncAll)
        {
            return Err(Error::unsupported_durability(mode));
        }

        match &self.inner.options.storage_mode {
            StorageMode::InMemory => Ok(()),
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => {
                self.inner.substrate.persist_wal(mode)?;
                Ok(())
            }
            StorageMode::HostPersistent { backend } => {
                Err(Error::unsupported_backend(backend.as_str()))
            }
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    fn browser_storage(&self) -> Result<BrowserStorageBackend> {
        self.inner
            .browser_storage
            .clone()
            .ok_or_else(|| Error::Corruption {
                message: "browser persistent database is missing storage backend".to_owned(),
            })
    }

    fn object_storage(&self) -> Result<ObjectStoreBackend> {
        self.inner
            .object_storage
            .clone()
            .ok_or_else(|| Error::Corruption {
                message: "object-store database is missing storage backend".to_owned(),
            })
    }

    /// Flush all immutable memtables to objects and publish them via the manifest
    /// CAS (the WAL-less durability point for object storage).
    ///
    /// NOTE: this holds the manifest mutex across the async CAS publish, so the
    /// returned future is not `Send` (fine for the current async-only,
    /// single-writer object-store path; a prepare/publish/install split — like
    /// the browser backend — is the Send-hardening follow-up).
    async fn flush_object_store_async(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let db_path = Path::new("");
        let target_sequence = self.freeze_public_flush_target()?;
        while self.has_immutable_memtables_at_or_below(target_sequence)? {
            let Some(_flush_guard) = self.inner.maintenance.try_start_flush() else {
                return Err(Error::runtime_busy("object-store flush is already active"));
            };
            let (flush_inputs, _budget_exhausted) =
                self.collect_flush_inputs_with_budget(MaintenanceBudget::unbounded())?;
            if flush_inputs.is_empty() {
                break;
            }
            self.write_flush_inputs_object_store_async(db_path, &flush_inputs)
                .await?;
        }
        Ok(())
    }

    async fn write_flush_inputs_object_store_async(
        &self,
        db_path: &Path,
        flush_inputs: &[NamedFlushInput],
    ) -> Result<()> {
        if flush_inputs.is_empty() {
            return Ok(());
        }
        let flush_sequence = flush_inputs
            .iter()
            .map(|input| input.input.freeze_sequence)
            .max()
            .expect("non-empty flush input list has a max sequence");
        let backend = self.object_storage()?;
        let mut written_tables = Vec::with_capacity(flush_inputs.len());
        for input in flush_inputs {
            let table_path = table::table_path(db_path, input.input.table_id);
            // A write failure leaves the freshly-PUT objects unreferenced by the
            // manifest; they are reclaimed by orphan-object GC (2c-5).
            let table = table::write_table_with_backend_async(
                &backend,
                &table_path,
                input.input.table_id,
                input.input.table_level,
                &input.input.table_options,
                &input.input.point_records,
                &input.input.range_tombstones,
                DurabilityMode::Flush,
            )
            .await?;
            written_tables.push((input.bucket.clone(), Arc::new(table)));
        }

        let _publish = self.inner.publish_barrier.enter()?;
        self.publish_flushed_tables_object_store_async(&written_tables, flush_sequence)
            .await?;
        Self::install_flushed_tables(flush_inputs, written_tables)?;
        // No WAL to rewrite: object-store databases are WAL-less.
        Ok(())
    }

    // Holds the manifest mutex across the async CAS publish (single-writer,
    // async-only object store). The Send-safe prepare/publish/install split — like
    // the browser backend — is the documented hardening follow-up.
    #[allow(clippy::await_holding_lock)]
    async fn publish_flushed_tables_object_store_async(
        &self,
        tables: &[(String, Arc<Table>)],
        flush_sequence: Sequence,
    ) -> Result<()> {
        let edits = tables
            .iter()
            .map(|(bucket, table)| (bucket.clone(), table.properties().clone()))
            .collect::<Vec<_>>();
        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "object-store database is missing manifest store".to_owned(),
            })?;
        let mut manifest = manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?;
        manifest.add_tables_async(edits, flush_sequence).await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn run_owned_browser_task<T>(
        label: &'static str,
        task: impl std::future::Future<Output = Result<T>> + 'static,
    ) -> Result<T>
    where
        T: 'static,
    {
        let (sender, receiver) = futures::channel::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = sender.send(task.await);
        });
        receiver.await.map_err(|_| Error::runtime_busy(label))?
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    async fn run_native_blocking_task<T>(
        &self,
        task: impl FnOnce(Db) -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let db = self.clone();
        self.inner
            .runtime
            .spawn_blocking_result(move || task(db))?
            .await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn persist_browser_async(&self, mode: DurabilityMode) -> Result<()> {
        self.ensure_open()?;
        if matches!(mode, DurabilityMode::SyncData | DurabilityMode::SyncAll) {
            return Err(Error::unsupported_durability(mode));
        }
        let Some(wal) = &self.inner.browser_wal else {
            return Ok(());
        };
        let storage = self.browser_storage()?;
        wal.persist(&storage, Path::new(""), mode).await
    }

    /// Flushes committed memtable data to persistent table files.
    ///
    /// Flush freezes the currently committed in-memory data up to a stable
    /// sequence, writes immutable memtables to level-0 table files, publishes
    /// the updated manifest, and then removes flushed immutable memtables from
    /// the read path. Readers keep seeing a consistent snapshot while this
    /// happens.
    ///
    /// In-memory databases have no table files, so this returns successfully
    /// without doing storage work. Read-only handles reject flush because it can
    /// publish new durable metadata.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] for read-only handles, [`Error::Closed`] for
    /// closed handles, [`Error::UnsupportedBackend`] when the selected backend
    /// requires the async maintenance path, or storage/recovery errors from the
    /// flush and manifest publish steps.
    pub fn flush_sync(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent flush requires async maintenance",
            ));
        }
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store flush requires the async API",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        let target_sequence = self.freeze_public_flush_target()?;
        let mut should_compact = false;

        while self.has_immutable_memtables_at_or_below(target_sequence)? {
            self.take_background_maintenance_error()?;
            if self.run_flush_once(&db_path, false)? {
                should_compact |= self.l0_pressure_exceeded()?;
                continue;
            }

            self.request_background_flush();
            self.record_cooperative_maintenance_yield();
            self.inner.maintenance.wait_until_flush_idle();
        }

        if should_compact
            || self.l0_pressure_exceeded()?
            || self.foreground_l0_overlap_pressure_exceeded()?
        {
            self.run_compaction_barrier(&db_path, &KeyRange::all(), true)?;
        }
        self.cleanup_pending_obsolete_table_files(&db_path)?;
        self.cleanup_pending_obsolete_blob_files(&db_path)?;
        self.take_background_maintenance_error()?;

        Ok(())
    }

    // Keep the public shape aligned with the accepted v1 protocol:
    // `Db::compact_range_sync(range) -> Result<()>`.
    /// Compacts table files that overlap `range`.
    ///
    /// Compaction rewrites overlapping table files into lower levels according
    /// to the current bucket options, drops overwritten point versions and
    /// covered range-deleted data that are no longer visible to active
    /// snapshots, and publishes a new manifest. It does not change the
    /// caller-visible result of reads; it changes the on-disk layout and future
    /// read cost.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range whose overlapping table files should be
    ///   considered for compaction. Use [`KeyRange::all`] to compact the whole
    ///   keyspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReadOnly`] for read-only handles, [`Error::Closed`] for
    /// closed handles, [`Error::UnsupportedBackend`] when this target requires
    /// async maintenance, or storage/format errors from reading, rewriting, or
    /// publishing table metadata.
    #[allow(clippy::needless_pass_by_value)]
    pub fn compact_range_sync(&self, range: KeyRange) -> Result<()> {
        self.take_background_maintenance_error()?;
        self.compact_range_internal(range)
    }

    /// Compacts table files that overlap `range` within `budget`.
    ///
    /// This is the cooperative form of [`Db::compact_range_sync`]. It performs
    /// only the amount of compaction admitted by `budget` and reports whether
    /// more eligible work remains.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range whose overlapping table files should be
    ///   considered.
    /// - `budget`: maximum flush and compaction inputs to process during this
    ///   call. Zero limits are treated as one by [`MaintenanceBudget::new`].
    #[allow(clippy::needless_pass_by_value)]
    pub fn compact_range_with_budget_sync(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.compact_range_with_budget_internal(range, budget)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn compact_range_internal(&self, range: KeyRange) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent compaction requires async maintenance",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        self.run_compaction_barrier(&db_path, &range, false)?;

        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    fn compact_range_with_budget_internal(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent compaction requires async maintenance",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(MaintenanceOutcome::default());
        };
        let db_path = path.to_path_buf();
        self.run_compaction_once_with_budget(&db_path, &range, false, budget)
    }

    /// Runs cooperative flush and compaction work within `budget`.
    ///
    /// This method lets applications do foreground maintenance in small pieces.
    /// It first tries flush work, then compaction work, and returns a
    /// [`MaintenanceOutcome`] describing completed work, budget exhaustion, or
    /// contention with another maintenance worker.
    ///
    /// # Parameters
    ///
    /// - `budget`: limits how many flush and compaction inputs this call may
    ///   process.
    ///
    /// # Errors
    ///
    /// Returns the same categories as [`Db::flush_sync`] and
    /// [`Db::compact_range_sync`].
    pub fn run_maintenance_with_budget_sync(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent maintenance requires async maintenance",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(MaintenanceOutcome::default());
        };
        let db_path = path.to_path_buf();
        let mut outcome = MaintenanceOutcome::default();
        let mut should_compact = self.l0_pressure_exceeded()?;

        if self.has_immutable_memtables()? {
            let (flush_should_compact, flush_outcome) =
                self.run_flush_once_with_budget(&db_path, false, budget)?;
            should_compact |= flush_should_compact;
            outcome.add_assign(flush_outcome);
        }

        if should_compact {
            let compaction_outcome =
                self.run_compaction_once_with_budget(&db_path, &KeyRange::all(), true, budget)?;
            outcome.add_assign(compaction_outcome);
        }

        if outcome.made_progress() {
            self.cleanup_pending_obsolete_table_files(&db_path)?;
            self.cleanup_pending_obsolete_blob_files(&db_path)?;
        }
        self.take_background_maintenance_error()?;
        Ok(outcome)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn flush_browser_async(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;

        let db_path = Path::new("");
        let target_sequence = self.freeze_public_flush_target()?;
        let mut should_compact = false;

        while self.has_immutable_memtables_at_or_below(target_sequence)? {
            self.take_background_maintenance_error()?;
            let (flush_should_compact, outcome) = self
                .run_flush_once_with_budget_browser_async(
                    db_path,
                    false,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                return Err(Error::runtime_busy(
                    "browser persistent flush is already active",
                ));
            }
            should_compact |= flush_should_compact;
            if outcome.flushes == 0 {
                break;
            }
        }

        if should_compact
            || self.l0_pressure_exceeded()?
            || self.foreground_l0_overlap_pressure_exceeded()?
        {
            let outcome = self
                .run_compaction_once_with_budget_browser_async(
                    db_path,
                    &KeyRange::all(),
                    true,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                return Err(Error::runtime_busy(
                    "browser persistent compaction is already active",
                ));
            }
        }
        self.cleanup_pending_obsolete_table_files_browser_async(db_path)
            .await?;
        self.cleanup_pending_obsolete_blob_files_browser_async(db_path)
            .await?;
        self.take_background_maintenance_error()?;

        Ok(())
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn compact_range_browser_async(&self, range: KeyRange) -> Result<()> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let outcome = self
            .run_compaction_once_with_budget_browser_async(
                Path::new(""),
                &range,
                false,
                MaintenanceBudget::unbounded(),
            )
            .await?;
        if outcome.busy {
            return Err(Error::runtime_busy(
                "browser persistent compaction is already active",
            ));
        }
        Ok(())
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn compact_range_with_budget_browser_async(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        self.run_compaction_once_with_budget_browser_async(Path::new(""), &range, false, budget)
            .await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn run_maintenance_with_budget_browser_async(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.take_background_maintenance_error()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        let db_path = Path::new("");
        let mut outcome = MaintenanceOutcome::default();
        let mut should_compact = self.l0_pressure_exceeded()?;

        if self.has_immutable_memtables()? {
            let (flush_should_compact, flush_outcome) = self
                .run_flush_once_with_budget_browser_async(db_path, false, budget)
                .await?;
            should_compact |= flush_should_compact;
            outcome.add_assign(flush_outcome);
        }

        if should_compact {
            let compaction_outcome = self
                .run_compaction_once_with_budget_browser_async(
                    db_path,
                    &KeyRange::all(),
                    true,
                    budget,
                )
                .await?;
            outcome.add_assign(compaction_outcome);
        }

        if outcome.made_progress() {
            self.cleanup_pending_obsolete_table_files_browser_async(db_path)
                .await?;
            self.cleanup_pending_obsolete_blob_files_browser_async(db_path)
                .await?;
        }
        self.take_background_maintenance_error()?;
        Ok(outcome)
    }

    /// Creates a snapshot at the newest visible committed sequence.
    ///
    /// Reads through the returned [`Snapshot`] see the database as of the
    /// sequence captured here, even if later writes commit before those reads
    /// run. The snapshot keeps old versions needed by its reads alive until all
    /// clones are dropped.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.inner
            .snapshots
            .pinned_snapshot(self.last_committed_sequence())
    }

    /// Creates an optimistic transaction over the newest visible sequence.
    ///
    /// The transaction records point and range reads against this sequence and
    /// stages writes in memory. Commit succeeds only if no later committed write
    /// conflicts with the recorded read set.
    ///
    /// # Parameters
    ///
    /// - `options`: commit options used when the transaction writes are
    ///   accepted.
    #[must_use]
    pub fn transaction(&self, options: TransactionOptions) -> Transaction {
        Transaction::new(self.clone(), self.last_committed_sequence(), options)
    }

    /// Returns a point-in-time copy of live database statistics.
    #[must_use]
    pub fn stats(&self) -> DbStats {
        let mut stats = DbStats {
            active_snapshots: self.inner.snapshots.active_count(),
            compaction_runs: self.inner.compaction_runs.load(Ordering::Acquire),
            compaction_input_tables: self.inner.compaction_input_tables.load(Ordering::Acquire),
            compaction_output_tables: self.inner.compaction_output_tables.load(Ordering::Acquire),
            compaction_input_bytes: self.inner.compaction_input_bytes.load(Ordering::Acquire),
            compaction_output_bytes: self.inner.compaction_output_bytes.load(Ordering::Acquire),
            commit_sequences_allocated: self.inner.commit_tracker.last_reserved_sequence().get(),
            commit_visible_sequence: self.inner.commit_tracker.visible_sequence().get(),
            commit_open_slots: self.inner.commit_tracker.open_slot_count(),
            commit_skipped_slots: self.inner.commit_tracker.skipped_slot_count(),
            blob_gc_runs: self.inner.blob_gc_runs.load(Ordering::Acquire),
            blob_gc_input_bytes: self.inner.blob_gc_input_bytes.load(Ordering::Acquire),
            blob_gc_output_bytes: self.inner.blob_gc_output_bytes.load(Ordering::Acquire),
            blob_gc_discarded_bytes: self.inner.blob_gc_discarded_bytes.load(Ordering::Acquire),
            maintenance_cooperative_yields: self
                .inner
                .maintenance_cooperative_yields
                .load(Ordering::Acquire),
            maintenance_budget_exhaustions: self
                .inner
                .maintenance_budget_exhaustions
                .load(Ordering::Acquire),
            ..DbStats::default()
        };
        self.add_wal_stats(&mut stats);
        self.add_storage_runtime_stats(&mut stats);
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
            stats.immutable_memtables = stats
                .immutable_memtables
                .saturating_add(state.immutable_memtable_count());
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
                    let table_bytes = persistent_path.map_or(0, |db_path| {
                        table_file_bytes(&self.inner.native_storage, db_path, properties.id)
                    });
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
            add_obsolete_blob_stats(
                &self.inner.native_storage,
                db_path,
                &live_blob_bytes_by_file,
                &mut stats,
            );
        }

        stats
    }

    fn add_wal_stats(&self, stats: &mut DbStats) {
        if let Some(wal_stats) = self.inner.substrate.wal_stats() {
            stats.wal_shards = wal_stats.shards;
            stats.wal_open_shards = wal_stats.open_shards;
            stats.wal_queue_capacity = wal_stats.queue_capacity;
            stats.wal_records_accepted = wal_stats.records_accepted;
            stats.wal_bytes_accepted = wal_stats.bytes_accepted;
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if let Some(wal) = &self.inner.browser_wal {
            let wal_stats = wal.stats();
            stats.wal_shards = wal_stats.shards;
            stats.wal_open_shards = wal_stats.open_shards;
            stats.wal_queue_capacity = wal_stats.queue_capacity;
            stats.wal_records_accepted = wal_stats.records_accepted;
            stats.wal_bytes_accepted = wal_stats.bytes_accepted;
        }
    }

    fn add_storage_runtime_stats(&self, stats: &mut DbStats) {
        let storage_stats = self.inner.native_storage.stats();
        stats.storage_uses_sync_adapter = storage_stats.uses_blocking_adapter;
        stats.storage_uses_platform_async_io = storage_stats.uses_platform_async_io;
        stats.storage_sync_adapter_tasks = storage_stats.blocking_adapter_tasks;
        stats.storage_sync_adapter_queue_capacity = storage_stats.blocking_adapter_queue_capacity;
        stats.storage_sync_adapter_queued_tasks = storage_stats.blocking_adapter_queued_tasks;
        stats.storage_sync_adapter_submitted_tasks = storage_stats.blocking_adapter_submitted_tasks;
        stats.storage_sync_adapter_completed_tasks = storage_stats.blocking_adapter_completed_tasks;
        stats.storage_sync_adapter_rejected_tasks = storage_stats.blocking_adapter_rejected_tasks;
        stats.storage_sync_adapter_total_runtime_micros =
            storage_stats.blocking_adapter_total_runtime_micros;
        stats.storage_platform_async_io_tasks = storage_stats.platform_async_io_tasks;
        stats.storage_platform_backend_fallback_tasks =
            storage_stats.platform_backend_fallback_tasks;
        stats.storage_platform_sync_fallback_tasks = storage_stats.platform_blocking_fallback_tasks;
        stats.storage_inline_tasks = storage_stats.inline_tasks;
        stats.storage_operations = storage_stats.operations;
    }

    /// Returns the options used to open this database handle.
    #[must_use]
    pub fn options(&self) -> &DbOptions {
        &self.inner.options
    }

    /// Returns the newest commit sequence visible to readers.
    #[must_use]
    pub fn last_committed_sequence(&self) -> Sequence {
        self.inner.commit_tracker.visible_sequence()
    }

    fn oldest_active_snapshot_sequence(&self) -> Sequence {
        self.inner
            .snapshots
            .oldest_active_or(self.last_committed_sequence())
    }

    /// Closes this handle synchronously and stops background workers.
    pub fn close_sync(&self) {
        self.inner.closed.store(true, Ordering::Release);
        shutdown_background_workers(
            &self.inner.maintenance,
            &self.inner.runtime_shutdown,
            &self.inner.background_workers,
        );
        // The directory lock is released only after the publish barrier is
        // idle. Otherwise a second process could open while this one is still
        // publishing files for a commit, flush, or compaction.
        let Ok(_publish) = self.inner.publish_barrier.enter() else {
            return;
        };
        if let Some(db_path) = self.persistent_path().map(Path::to_path_buf) {
            let _ = self.cleanup_pending_obsolete_table_files(&db_path);
            let _ = self.cleanup_pending_obsolete_blob_files(&db_path);
        }
        self.inner.substrate.release_writer_lease();
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

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Err(Error::unsupported_backend(
                "browser persistent background workers",
            ))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            for worker_index in 0..self.inner.options.background_worker_count {
                let inner = Arc::downgrade(&self.inner);
                let maintenance = Arc::clone(&self.inner.maintenance);
                let runtime_shutdown = self.inner.runtime_shutdown.clone();
                let worker = self.inner.runtime.spawn_background(
                    format!("trine-kv-maintenance-{worker_index}"),
                    move || background_worker_loop(&inner, &maintenance, &runtime_shutdown),
                )?;
                self.inner
                    .background_workers
                    .lock()
                    .map_err(|_| lock_poisoned("background worker registry"))?
                    .push(worker);
            }
            self.request_background_maintenance();

            Ok(())
        }
    }

    fn background_workers_enabled(&self) -> bool {
        !self.inner.options.read_only
            && self.inner.options.background_worker_count != 0
            && self.inner.runtime.capabilities().background_threads()
            && self.inner.options.storage_mode.persistent_path().is_some()
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    fn request_background_maintenance(&self) {
        if self.background_workers_enabled() {
            self.inner.maintenance.request(MaintenanceRequest {
                flush: true,
                compaction: true,
            });
        }
    }

    fn request_background_flush(&self) {
        if self.background_workers_enabled() {
            self.inner.maintenance.request(MaintenanceRequest {
                flush: true,
                compaction: false,
            });
        }
    }

    fn request_background_compaction(&self) {
        if self.background_workers_enabled() {
            self.inner.maintenance.request(MaintenanceRequest {
                flush: false,
                compaction: true,
            });
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

    fn record_cooperative_maintenance_yield(&self) {
        self.inner
            .maintenance_cooperative_yields
            .fetch_add(1, Ordering::AcqRel);
    }

    fn record_maintenance_budget_exhaustion(&self) {
        self.inner
            .maintenance_budget_exhaustions
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    fn run_background_maintenance(&self, request: MaintenanceRequest) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Ok(());
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        let mut should_compact = request.compaction || self.l0_pressure_exceeded()?;

        if request.flush && self.has_immutable_memtables()? {
            let (flush_should_compact, _) =
                self.run_flush_once_with_budget(&db_path, false, MaintenanceBudget::single_unit())?;
            should_compact |= flush_should_compact;
        }

        if should_compact {
            self.run_compaction_once_with_budget(
                &db_path,
                &KeyRange::all(),
                true,
                MaintenanceBudget::single_unit(),
            )?;
        }
        if self.has_immutable_memtables()? {
            self.request_background_flush();
        }
        if self.l0_pressure_exceeded()? {
            self.request_background_compaction();
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

    pub(crate) async fn get_at_sequence_async(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        self.get_at_with_pin_state_async(bucket, key, read_sequence, false)
            .await
    }

    pub(crate) fn get_at_with_pin_state(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<Vec<u8>>> {
        let state = self.bucket_state(bucket)?;
        self.get_at_state_with_pin_state(&state, key, read_sequence, read_pin_held)
    }

    pub(crate) async fn get_at_with_pin_state_async(
        &self,
        bucket: &str,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<Vec<u8>>> {
        let state = self.bucket_state(bucket)?;
        self.get_at_state_with_pin_state_async(&state, key, read_sequence, read_pin_held)
            .await
    }

    pub(crate) fn get_at_state_with_pin_state(
        &self,
        state: &LsmTree,
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

        state.read_visible_point(
            key,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
            Some(self.inner.blob_reads.as_ref()),
        )
    }

    pub(crate) async fn get_at_state_with_pin_state_async(
        &self,
        state: &LsmTree,
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

        state
            .read_visible_point_async(
                &self.inner.native_storage,
                key,
                read_sequence,
                self.persistent_path(),
                Some(self.inner.block_cache.as_ref()),
                Some(self.inner.blob_reads.as_ref()),
            )
            .await
    }

    pub(crate) fn get_value_at_state_snapshot_with_pin_state(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<PointValue>> {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state.read_visible_point_value_in_snapshot(
            read_snapshot,
            key,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
            Some(self.inner.blob_reads.as_ref()),
        )
    }

    pub(crate) fn get_values_at_state_snapshot_with_pin_state<K>(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        keys: &[K],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Vec<Option<PointValue>>>
    where
        K: AsRef<[u8]>,
    {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state.read_visible_point_values_in_snapshot(
            read_snapshot,
            keys,
            read_sequence,
            self.persistent_path(),
            Some(self.inner.block_cache.as_ref()),
            Some(self.inner.blob_reads.as_ref()),
        )
    }

    pub(crate) async fn get_value_at_state_snapshot_with_pin_state_async(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        key: &[u8],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Option<PointValue>> {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state
            .read_visible_point_value_in_snapshot_async(
                read_snapshot,
                key,
                read_sequence,
                AsyncPointReadIo::new(
                    &self.inner.native_storage,
                    self.persistent_path(),
                    Some(self.inner.block_cache.as_ref()),
                    Some(self.inner.blob_reads.as_ref()),
                ),
            )
            .await
    }

    pub(crate) async fn get_values_at_state_snapshot_with_pin_state_async<K>(
        &self,
        state: &LsmTree,
        read_snapshot: &LsmPointReadSnapshot,
        keys: &[K],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<Vec<Option<PointValue>>>
    where
        K: AsRef<[u8]>,
    {
        self.ensure_open()?;
        let _read_pin = if read_pin_held {
            None
        } else {
            Some(self.inner.snapshots.pinned_snapshot(read_sequence))
        };

        state
            .read_visible_point_values_in_snapshot_async(
                read_snapshot,
                keys,
                read_sequence,
                AsyncPointReadIo::new(
                    &self.inner.native_storage,
                    self.persistent_path(),
                    Some(self.inner.block_cache.as_ref()),
                    Some(self.inner.blob_reads.as_ref()),
                ),
            )
            .await
    }

    pub(crate) fn reader_for_state<'snapshot>(
        &self,
        state: &Arc<LsmTree>,
        snapshot: &'snapshot Snapshot,
    ) -> Result<BucketReader<'snapshot>> {
        self.reader_for_state_at_sequence(state, snapshot.read_sequence(), snapshot.is_pinned())
    }

    pub(crate) fn reader_for_state_at_sequence<'snapshot>(
        &self,
        state: &Arc<LsmTree>,
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<BucketReader<'snapshot>> {
        self.ensure_open()?;
        let read_pin =
            (!read_pin_held).then(|| self.inner.snapshots.pinned_snapshot(read_sequence));
        let read_snapshot = state.point_read_snapshot(read_sequence)?;
        Ok(BucketReader::new(
            self.clone(),
            Arc::clone(state),
            read_snapshot,
            read_sequence,
            read_pin,
        ))
    }

    pub(crate) fn reader_for_state_keys_at_sequence<'snapshot, K>(
        &self,
        state: &Arc<LsmTree>,
        keys: &[K],
        read_sequence: Sequence,
        read_pin_held: bool,
    ) -> Result<BucketReader<'snapshot>>
    where
        K: AsRef<[u8]>,
    {
        self.ensure_open()?;
        let read_pin =
            (!read_pin_held).then(|| self.inner.snapshots.pinned_snapshot(read_sequence));
        let read_snapshot = state.point_read_snapshot_for_keys(keys, read_sequence)?;
        Ok(BucketReader::new(
            self.clone(),
            Arc::clone(state),
            read_snapshot,
            read_sequence,
            read_pin,
        ))
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
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn range_at_sequence_async(
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
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
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
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn range_lazy_at_sequence_async(
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
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
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
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn prefix_at_sequence_async(
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
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(Iter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
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
        let scan = state.scan(
            &selector,
            direction,
            read_sequence,
            Some(&self.inner.block_cache),
        )?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    pub(crate) async fn prefix_lazy_at_sequence_async(
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
        let scan = state
            .scan_async(
                &selector,
                direction,
                read_sequence,
                Some(&self.inner.block_cache),
            )
            .await?;
        let db_path = self.persistent_path().map(Path::to_path_buf);
        let native_storage = db_path.as_ref().map(|_| self.inner.native_storage.clone());

        Ok(LazyIter::from_sources(
            direction,
            ScanSourceInput {
                read_sequence,
                read_pin,
                db_path,
                native_storage,
                blob_reads: Some(Arc::clone(&self.inner.blob_reads)),
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        ))
    }

    fn bucket_state(&self, bucket: &str) -> Result<Arc<LsmTree>> {
        self.bucket_state_if_exists(bucket)?
            .ok_or_else(|| Error::BucketMissing {
                name: bucket.to_owned(),
            })
    }

    fn bucket_state_if_exists(&self, bucket: &str) -> Result<Option<Arc<LsmTree>>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        Ok(buckets.get(bucket).cloned())
    }

    fn persistent_path(&self) -> Option<&Path> {
        self.inner.options.storage_mode.persistent_path()
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

    fn apply_write_backpressure(&self) -> Result<()> {
        if self.inner.options.storage_mode.is_browser_persistent() {
            let pressure = self.write_pressure()?;
            return if pressure.none() {
                Ok(())
            } else {
                Err(Error::runtime_busy(
                    "browser persistent write pressure requires async maintenance",
                ))
            };
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();

        loop {
            self.take_background_maintenance_error()?;
            let pressure = self.write_pressure()?;
            if pressure.none() {
                return Ok(());
            }

            self.inner.maintenance.request(pressure.request());
            if self.background_workers_enabled() {
                let progress = self.inner.maintenance.progress();
                self.record_cooperative_maintenance_yield();
                if self
                    .inner
                    .maintenance
                    .wait_for_progress(progress, Duration::from_millis(20))
                {
                    continue;
                }
                self.record_maintenance_budget_exhaustion();
            }

            self.run_maintenance_for_pressure(&db_path, pressure)?;
        }
    }

    fn write_pressure(&self) -> Result<WritePressure> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut pressure = WritePressure::default();

        for state in buckets.values() {
            if state.immutable_memtable_count() >= self.inner.options.max_immutable_memtables {
                pressure.flush = true;
            }
            if state.l0_table_count()? > self.inner.options.max_l0_files {
                pressure.compaction = true;
            }
        }

        Ok(pressure)
    }

    fn run_maintenance_for_pressure(&self, db_path: &Path, pressure: WritePressure) -> Result<()> {
        let mut should_compact = pressure.compaction;
        if pressure.flush {
            should_compact |= self.run_pressure_flush_once(db_path)?;
        }
        if should_compact {
            self.run_compaction_once_with_budget(
                db_path,
                &KeyRange::all(),
                true,
                MaintenanceBudget::single_unit(),
            )?;
        }

        Ok(())
    }

    fn run_pressure_flush_once(&self, db_path: &Path) -> Result<bool> {
        let (should_compact, _) =
            self.run_pressure_flush_once_with_budget(db_path, MaintenanceBudget::unbounded())?;
        Ok(should_compact)
    }

    fn run_pressure_flush_once_with_budget(
        &self,
        db_path: &Path,
        budget: MaintenanceBudget,
    ) -> Result<(bool, MaintenanceOutcome)> {
        let Some(_flush_guard) = self.inner.maintenance.try_start_flush() else {
            return Ok((false, MaintenanceOutcome::busy_outcome()));
        };

        let (flush_inputs, budget_exhausted) =
            self.collect_pressure_flush_inputs_with_budget(budget)?;
        let flush_count = flush_inputs.len();
        let should_compact = self.write_flush_inputs(db_path, &flush_inputs)?;
        let outcome = MaintenanceOutcome {
            flushes: flush_count,
            budget_exhausted: budget_exhausted && flush_count != 0,
            ..MaintenanceOutcome::default()
        };
        if outcome.budget_exhausted {
            self.record_maintenance_budget_exhaustion();
        }
        Ok((should_compact, outcome))
    }

    fn run_flush_once(&self, db_path: &Path, freeze_active: bool) -> Result<bool> {
        let (should_compact, _) = self.run_flush_once_with_budget(
            db_path,
            freeze_active,
            MaintenanceBudget::unbounded(),
        )?;
        Ok(should_compact)
    }

    fn run_flush_once_with_budget(
        &self,
        db_path: &Path,
        freeze_active: bool,
        budget: MaintenanceBudget,
    ) -> Result<(bool, MaintenanceOutcome)> {
        let Some(_flush_guard) = self.inner.maintenance.try_start_flush() else {
            return Ok((false, MaintenanceOutcome::busy_outcome()));
        };

        if freeze_active {
            let _memtable_publish = self
                .inner
                .memtable_publish_lock
                .lock()
                .map_err(|_| lock_poisoned("memtable publish lock"))?;
            let _publish = self.inner.publish_barrier.enter()?;
            self.freeze_all_active_memtables(self.last_committed_sequence())?;
        }

        let (flush_inputs, budget_exhausted) = self.collect_flush_inputs_with_budget(budget)?;
        let flush_count = flush_inputs.len();
        let should_compact = self.write_flush_inputs(db_path, &flush_inputs)?;
        let outcome = MaintenanceOutcome {
            flushes: flush_count,
            budget_exhausted: budget_exhausted && flush_count != 0,
            ..MaintenanceOutcome::default()
        };
        if outcome.budget_exhausted {
            self.record_maintenance_budget_exhaustion();
        }
        Ok((should_compact, outcome))
    }

    fn freeze_large_active_memtables_after_commit(
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

    fn freeze_public_flush_target(&self) -> Result<Sequence> {
        // Lock order is memtable publish -> publish barrier. This keeps
        // concurrent writers from adding higher-sequence records between the
        // public flush boundary and the active-memtable freeze.
        let _memtable_publish = self
            .inner
            .memtable_publish_lock
            .lock()
            .map_err(|_| lock_poisoned("memtable publish lock"))?;
        let _publish = self.inner.publish_barrier.enter()?;
        let target_sequence = self.last_committed_sequence();
        self.freeze_all_active_memtables(target_sequence)?;

        Ok(target_sequence)
    }

    fn has_immutable_memtables(&self) -> Result<bool> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        for state in buckets.values() {
            if state.has_immutable_memtables() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn has_immutable_memtables_at_or_below(&self, max_sequence: Sequence) -> Result<bool> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        for state in buckets.values() {
            if state.has_immutable_memtables_at_or_below(max_sequence)? {
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
            let table = match table::write_table_with_backend(
                &self.inner.native_storage,
                &table_path,
                input.input.table_id,
                input.input.table_level,
                &input.input.table_options,
                &input.input.point_records,
                &input.input.range_tombstones,
            ) {
                Ok(table) => table,
                Err(error) => {
                    let _ = remove_storage_files(
                        &self.inner.native_storage,
                        db_path,
                        &written_table_ids,
                    );
                    return Err(error);
                }
            };
            written_tables.push((input.bucket.clone(), Arc::new(table)));
        }

        if let Err(error) =
            sync_storage_directory_after_renames(&self.inner.native_storage, db_path)
        {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            return Err(error);
        }

        {
            let _publish = self.inner.publish_barrier.enter()?;
            if let Err(error) = self.publish_flushed_tables(&written_tables, flush_sequence) {
                let _ =
                    remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
                return Err(error);
            }
            Self::install_flushed_tables(flush_inputs, written_tables)?;
            self.rewrite_wal_after_replay_floor(flush_sequence)?;
        }
        self.l0_pressure_exceeded()
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn run_flush_once_with_budget_browser_async(
        &self,
        db_path: &Path,
        freeze_active: bool,
        budget: MaintenanceBudget,
    ) -> Result<(bool, MaintenanceOutcome)> {
        let Some(_flush_guard) = self.inner.maintenance.try_start_flush() else {
            return Ok((false, MaintenanceOutcome::busy_outcome()));
        };

        if freeze_active {
            let _memtable_publish = self
                .inner
                .memtable_publish_lock
                .lock()
                .map_err(|_| lock_poisoned("memtable publish lock"))?;
            let _publish = self.inner.publish_barrier.enter()?;
            self.freeze_all_active_memtables(self.last_committed_sequence())?;
        }

        let (flush_inputs, budget_exhausted) = self.collect_flush_inputs_with_budget(budget)?;
        let flush_count = flush_inputs.len();
        let should_compact = self
            .write_flush_inputs_browser_async(db_path, &flush_inputs)
            .await?;
        let outcome = MaintenanceOutcome {
            flushes: flush_count,
            budget_exhausted: budget_exhausted && flush_count != 0,
            ..MaintenanceOutcome::default()
        };
        if outcome.budget_exhausted {
            self.record_maintenance_budget_exhaustion();
        }
        Ok((should_compact, outcome))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn write_flush_inputs_browser_async(
        &self,
        db_path: &Path,
        flush_inputs: &[NamedFlushInput],
    ) -> Result<bool> {
        if flush_inputs.is_empty() {
            return Ok(false);
        }

        let flush_sequence = flush_inputs
            .iter()
            .map(|input| input.input.freeze_sequence)
            .max()
            .expect("non-empty flush input list has a max sequence");
        let storage = self.browser_storage()?;
        let mut written_tables = Vec::with_capacity(flush_inputs.len());
        let mut written_table_ids = Vec::with_capacity(flush_inputs.len());

        for input in flush_inputs {
            let table_path = table::table_path(db_path, input.input.table_id);
            written_table_ids.push(input.input.table_id);
            let table = match table::write_table_with_backend_async(
                &storage,
                &table_path,
                input.input.table_id,
                input.input.table_level,
                &input.input.table_options,
                &input.input.point_records,
                &input.input.range_tombstones,
                DurabilityMode::Flush,
            )
            .await
            {
                Ok(table) => table,
                Err(error) => {
                    let _ = self
                        .remove_storage_files_browser_async(db_path, &written_table_ids)
                        .await;
                    return Err(error);
                }
            };
            written_tables.push((input.bucket.clone(), Arc::new(table)));
        }

        if let Err(error) = self
            .publish_flushed_tables_browser_async(&written_tables, flush_sequence)
            .await
        {
            let _ = self
                .remove_storage_files_browser_async(db_path, &written_table_ids)
                .await;
            return Err(error);
        }
        Self::install_flushed_tables(flush_inputs, written_tables)?;
        self.rewrite_wal_after_replay_floor_browser_async(db_path, flush_sequence)
            .await?;
        self.l0_pressure_exceeded()
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn publish_flushed_tables_browser_async(
        &self,
        tables: &[(String, Arc<Table>)],
        flush_sequence: Sequence,
    ) -> Result<()> {
        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
        let edits = tables
            .iter()
            .map(|(bucket, table)| (bucket.clone(), table.properties().clone()))
            .collect::<Vec<_>>();
        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?;
        let prepared = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            manifest.prepare_add_tables_publish(edits, flush_sequence)?
        };
        prepared.publish_async().await?;
        manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_prepared_publish(prepared)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn rewrite_wal_after_replay_floor_browser_async(
        &self,
        db_path: &Path,
        replay_floor: Sequence,
    ) -> Result<()> {
        let Some(wal) = &self.inner.browser_wal else {
            return Ok(());
        };
        let storage = self.browser_storage()?;
        wal.rewrite_after_replay_floor(&storage, db_path, replay_floor)
            .await
    }

    fn collect_pressure_flush_inputs_with_budget(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<(Vec<NamedFlushInput>, bool)> {
        let max_immutable_memtables = self.inner.options.max_immutable_memtables;
        let mut next_table_id = self.next_table_id()?;
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut inputs = Vec::new();
        let limit = budget.flush_input_limit();
        let mut budget_exhausted = false;

        for (name, state) in buckets.iter() {
            if state.immutable_memtable_count() < max_immutable_memtables {
                continue;
            }
            for input in state.prepare_flush_inputs(&mut next_table_id)? {
                if inputs.len() >= limit {
                    budget_exhausted = true;
                    break;
                }
                inputs.push(NamedFlushInput {
                    bucket: name.clone(),
                    tree: Arc::clone(state),
                    input,
                });
            }
            if budget_exhausted {
                break;
            }
        }

        Ok((inputs, budget_exhausted))
    }

    fn collect_flush_inputs_with_budget(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<(Vec<NamedFlushInput>, bool)> {
        let mut next_table_id = self.next_table_id()?;
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut inputs = Vec::new();
        let limit = budget.flush_input_limit();
        let mut budget_exhausted = false;

        for (name, state) in buckets.iter() {
            for input in state.prepare_flush_inputs(&mut next_table_id)? {
                if inputs.len() >= limit {
                    budget_exhausted = true;
                    break;
                }
                inputs.push(NamedFlushInput {
                    bucket: name.clone(),
                    tree: Arc::clone(state),
                    input,
                });
            }
            if budget_exhausted {
                break;
            }
        }

        Ok((inputs, budget_exhausted))
    }

    fn collect_compaction_inputs(
        &self,
        range: &KeyRange,
        oldest_active_snapshot: Sequence,
        local_l0_compaction: bool,
    ) -> Result<Vec<NamedCompactionInput>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut inputs = Vec::new();
        let compaction_options = compaction_options(&self.inner.options, local_l0_compaction);

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

    fn run_compaction_barrier(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
    ) -> Result<()> {
        loop {
            self.take_background_maintenance_error()?;
            let outcome = self.run_compaction_once_with_budget(
                db_path,
                range,
                local_l0_compaction,
                MaintenanceBudget::unbounded(),
            )?;
            if outcome.compactions != 0 || !outcome.busy {
                return Ok(());
            }
            if !self.inner.maintenance.has_pending_compaction() {
                return Ok(());
            }
            self.request_background_compaction();
            self.record_cooperative_maintenance_yield();
            self.inner.maintenance.wait_until_compaction_idle();
            self.take_background_maintenance_error()?;
        }
    }

    fn run_compaction_once_with_budget(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        let oldest_active_snapshot = self.oldest_active_snapshot_sequence();
        let compaction_inputs =
            self.collect_compaction_inputs(range, oldest_active_snapshot, local_l0_compaction)?;
        if compaction_inputs.is_empty() {
            return Ok(MaintenanceOutcome::default());
        }

        let reservations = compaction_inputs
            .iter()
            .map(|input| CompactionReservation {
                bucket: input.bucket.clone(),
                range: input.input.compaction_range.clone(),
            })
            .collect::<Vec<_>>();
        let Some(compaction_guard) = self.inner.maintenance.reserve_compactions(reservations)
        else {
            return Ok(MaintenanceOutcome::busy_outcome());
        };
        let mut compaction_inputs = compaction_inputs
            .into_iter()
            .filter(|input| compaction_guard.contains(&input.bucket, &input.input.compaction_range))
            .collect::<Vec<_>>();
        if compaction_inputs.is_empty() {
            return Ok(MaintenanceOutcome::busy_outcome());
        }
        let limit = budget.compaction_input_limit();
        let budget_exhausted = compaction_inputs.len() > limit;
        compaction_inputs.truncate(limit);
        if compaction_inputs.is_empty() {
            return Ok(MaintenanceOutcome::default());
        }

        let PendingCompactionOutputs {
            outputs: written_tables,
            written_table_ids,
        } = self.build_compaction_outputs(db_path, oldest_active_snapshot, &compaction_inputs)?;

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

        if !written_table_ids.is_empty() {
            if let Err(error) =
                sync_storage_directory_after_renames(&self.inner.native_storage, db_path)
            {
                let _ =
                    remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
                return Err(error);
            }
        }

        let _publish = self.inner.publish_barrier.enter()?;
        if let Err(error) = self.validate_compacted_tables(&written_tables) {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            if is_level_layout_compaction_error(&error) {
                return Ok(MaintenanceOutcome::default());
            }
            return Err(error);
        }
        if let Err(error) = self.publish_compacted_tables(&written_tables, &obsolete_blob_ids) {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            return Err(error);
        }

        self.install_compacted_tables(written_tables)?;
        self.record_compaction_stats(
            db_path,
            compaction_inputs.len(),
            &input_table_ids_for_stats,
            &output_table_ids_for_stats,
        );
        self.retire_obsolete_table_files(db_path, &obsolete_table_ids)?;
        self.cleanup_pending_obsolete_blob_files(db_path)?;
        if self.inner.options.blob_gc_enabled {
            self.run_blob_gc_once_locked(db_path)?;
        }

        let outcome = MaintenanceOutcome {
            compactions: compaction_inputs.len(),
            budget_exhausted,
            ..MaintenanceOutcome::default()
        };
        if outcome.budget_exhausted {
            self.record_maintenance_budget_exhaustion();
        }
        Ok(outcome)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(clippy::too_many_lines)]
    async fn run_compaction_once_with_budget_browser_async(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        let oldest_active_snapshot = self.oldest_active_snapshot_sequence();
        let compaction_inputs =
            self.collect_compaction_inputs(range, oldest_active_snapshot, local_l0_compaction)?;
        if compaction_inputs.is_empty() {
            return Ok(MaintenanceOutcome::default());
        }

        let reservations = compaction_inputs
            .iter()
            .map(|input| CompactionReservation {
                bucket: input.bucket.clone(),
                range: input.input.compaction_range.clone(),
            })
            .collect::<Vec<_>>();
        let Some(compaction_guard) = self.inner.maintenance.reserve_compactions(reservations)
        else {
            return Ok(MaintenanceOutcome::busy_outcome());
        };
        let mut compaction_inputs = compaction_inputs
            .into_iter()
            .filter(|input| compaction_guard.contains(&input.bucket, &input.input.compaction_range))
            .collect::<Vec<_>>();
        if compaction_inputs.is_empty() {
            return Ok(MaintenanceOutcome::busy_outcome());
        }
        let limit = budget.compaction_input_limit();
        let budget_exhausted = compaction_inputs.len() > limit;
        compaction_inputs.truncate(limit);
        if compaction_inputs.is_empty() {
            return Ok(MaintenanceOutcome::default());
        }

        let PendingCompactionOutputs {
            outputs: written_tables,
            written_table_ids,
        } = self
            .build_compaction_outputs_browser_async(
                db_path,
                oldest_active_snapshot,
                &compaction_inputs,
            )
            .await?;

        let output_tables = written_tables
            .iter()
            .flat_map(|output| output.output.tables.iter().cloned())
            .collect::<Vec<_>>();
        let input_tables = compaction_inputs
            .iter()
            .flat_map(|input| input.input.input_tables.iter().cloned())
            .collect::<Vec<_>>();
        let output_table_ids = output_tables
            .iter()
            .map(|table| table.properties().id)
            .collect::<BTreeSet<_>>();
        let obsolete_table_ids = compaction_inputs
            .iter()
            .flat_map(|input| input.input.input_table_ids.iter().copied())
            .filter(|table_id| !output_table_ids.contains(table_id))
            .collect::<Vec<_>>();
        let obsolete_blob_ids =
            self.obsolete_blob_ids_for_compaction(&compaction_inputs, &written_tables)?;

        if let Err(error) = self.validate_compacted_tables(&written_tables) {
            let _ = self
                .remove_storage_files_browser_async(db_path, &written_table_ids)
                .await;
            if is_level_layout_compaction_error(&error) {
                return Ok(MaintenanceOutcome::default());
            }
            return Err(error);
        }
        if let Err(error) = self
            .publish_compacted_tables_browser_async(&written_tables, &obsolete_blob_ids)
            .await
        {
            let _ = self
                .remove_storage_files_browser_async(db_path, &written_table_ids)
                .await;
            return Err(error);
        }

        self.install_compacted_tables(written_tables)?;
        self.record_compaction_stats_from_tables(
            compaction_inputs.len(),
            &input_tables,
            &output_tables,
        );
        self.retire_obsolete_table_files_browser_async(db_path, &obsolete_table_ids)
            .await?;
        self.cleanup_pending_obsolete_blob_files_browser_async(db_path)
            .await?;
        if self.inner.options.blob_gc_enabled {
            self.run_blob_gc_once_browser_async(db_path).await?;
        }

        let outcome = MaintenanceOutcome {
            compactions: compaction_inputs.len(),
            budget_exhausted,
            ..MaintenanceOutcome::default()
        };
        if outcome.budget_exhausted {
            self.record_maintenance_budget_exhaustion();
        }
        Ok(outcome)
    }

    fn build_compaction_outputs(
        &self,
        db_path: &Path,
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
                &input.input.compaction_range,
                oldest_active_snapshot,
                self.inner.options.target_table_bytes,
            ) {
                Ok(payloads) => payloads,
                Err(error) => {
                    let _ = remove_storage_files(
                        &self.inner.native_storage,
                        db_path,
                        &written_table_ids,
                    );
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
                    let _ = remove_storage_files(
                        &self.inner.native_storage,
                        db_path,
                        &written_table_ids,
                    );
                    return Err(Error::Corruption {
                        message: "table id counter overflow".to_owned(),
                    });
                };

                let table_path = table::table_path(db_path, table_id);
                written_table_ids.push(table_id);
                let table = match table::write_table_with_backend(
                    &self.inner.native_storage,
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                ) {
                    Ok(table) => table,
                    Err(error) => {
                        let _ = remove_storage_files(
                            &self.inner.native_storage,
                            db_path,
                            &written_table_ids,
                        );
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn build_compaction_outputs_browser_async(
        &self,
        db_path: &Path,
        oldest_active_snapshot: Sequence,
        compaction_inputs: &[NamedCompactionInput],
    ) -> Result<PendingCompactionOutputs> {
        let storage = self.browser_storage()?;
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
                &input.input.compaction_range,
                oldest_active_snapshot,
                self.inner.options.target_table_bytes,
            ) {
                Ok(payloads) => payloads,
                Err(error) => {
                    let _ = self
                        .remove_storage_files_browser_async(db_path, &written_table_ids)
                        .await;
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
                    let _ = self
                        .remove_storage_files_browser_async(db_path, &written_table_ids)
                        .await;
                    return Err(Error::Corruption {
                        message: "table id counter overflow".to_owned(),
                    });
                };

                let table_path = table::table_path(db_path, table_id);
                written_table_ids.push(table_id);
                let table = match table::write_table_with_backend_async(
                    &storage,
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                    DurabilityMode::Flush,
                )
                .await
                {
                    Ok(table) => table,
                    Err(error) => {
                        let _ = self
                            .remove_storage_files_browser_async(db_path, &written_table_ids)
                            .await;
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
        let indexes = match blob::write_blob_file_with_backend(
            &self.inner.native_storage,
            db_path,
            plan.new_blob_file_id,
            header,
            &blob_records,
        ) {
            Ok(indexes) => indexes,
            Err(error) => {
                let _ =
                    remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
                return Err(error);
            }
        };

        let mut tables = plan.tables;
        let output_bytes = match apply_blob_gc_indexes(&mut tables, plan.records, indexes) {
            Ok(output_bytes) => output_bytes,
            Err(error) => {
                let _ =
                    remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
                return Err(error);
            }
        };
        let outputs =
            match write_blob_gc_replacement_tables(&self.inner.native_storage, db_path, tables) {
                Ok(outputs) => outputs,
                Err(error) => {
                    let _ = remove_storage_files(
                        &self.inner.native_storage,
                        db_path,
                        &written_table_ids,
                    );
                    return Err(error);
                }
            };

        if let Err(error) =
            sync_storage_directory_after_renames(&self.inner.native_storage, db_path)
        {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            return Err(error);
        }

        if let Err(error) = self.publish_compacted_tables(&outputs, &obsolete_blob_ids) {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(clippy::too_many_lines)]
    async fn run_blob_gc_once_browser_async(&self, db_path: &Path) -> Result<()> {
        let Some(plan) = self
            .build_blob_gc_rewrite_plan_browser_async(db_path)
            .await?
        else {
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
        let storage = self.browser_storage()?;
        let indexes = match blob::write_blob_file_with_backend_async(
            &storage,
            db_path,
            plan.new_blob_file_id,
            header,
            &blob_records,
            DurabilityMode::Flush,
        )
        .await
        {
            Ok(indexes) => indexes,
            Err(error) => {
                let _ = self
                    .remove_storage_files_browser_async(db_path, &written_table_ids)
                    .await;
                return Err(error);
            }
        };

        let mut tables = plan.tables;
        let output_bytes = match apply_blob_gc_indexes(&mut tables, plan.records, indexes) {
            Ok(output_bytes) => output_bytes,
            Err(error) => {
                let _ = self
                    .remove_storage_files_browser_async(db_path, &written_table_ids)
                    .await;
                return Err(error);
            }
        };
        let outputs = match self
            .write_blob_gc_replacement_tables_browser_async(db_path, tables)
            .await
        {
            Ok(outputs) => outputs,
            Err(error) => {
                let _ = self
                    .remove_storage_files_browser_async(db_path, &written_table_ids)
                    .await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .publish_compacted_tables_browser_async(&outputs, &obsolete_blob_ids)
            .await
        {
            let _ = self
                .remove_storage_files_browser_async(db_path, &written_table_ids)
                .await;
            return Err(error);
        }

        self.install_compacted_tables(outputs)?;
        self.retire_obsolete_table_files_browser_async(db_path, &obsolete_table_ids)
            .await?;
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
        self.cleanup_pending_obsolete_blob_files_browser_async(db_path)
            .await
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
                    let blob_record = blob::read_record_for_index_with_backend(
                        &self.inner.native_storage,
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn build_blob_gc_rewrite_plan_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<Option<BlobGcRewritePlan>> {
        let candidates = self
            .choose_blob_gc_candidates_browser_async(db_path)
            .await?;
        if candidates.is_empty() {
            return Ok(None);
        }
        let candidate_file_ids = candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<BTreeSet<_>>();

        let storage = self.browser_storage()?;
        let mut next_table_id = self.next_table_id()?;
        let new_blob_file_id = next_table_id.get();
        let mut tables = Vec::new();
        let mut rewrite_sources = Vec::new();
        {
            let buckets = self
                .inner
                .buckets
                .read()
                .map_err(|_| lock_poisoned("bucket registry"))?;

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
                        rewrite_sources.push((
                            point_record.internal_key.clone(),
                            *index,
                            table_index,
                            record_index,
                        ));
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
        }

        if rewrite_sources.is_empty() {
            return Ok(None);
        }

        let mut rewrite_records = Vec::with_capacity(rewrite_sources.len());
        for (internal_key, index, table_index, record_index) in rewrite_sources {
            let blob_record = blob::read_record_for_index_with_backend_async(
                &storage,
                db_path,
                &index,
                Some(&internal_key),
            )
            .await?;
            rewrite_records.push(BlobGcRewriteRecord {
                internal_key,
                value: blob_record.record.value.clone(),
                compression: blob_record.record.compression,
                table_index,
                record_index,
            });
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
            let properties = blob::read_blob_file_properties_with_backend(
                &self.inner.native_storage,
                db_path,
                file_id,
            )?;
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn choose_blob_gc_candidates_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<Vec<BlobGcCandidate>> {
        let live_bytes_by_file = self.live_blob_bytes_by_file()?;
        let storage = self.browser_storage()?;
        let mut candidates = Vec::new();

        for (file_id, live_bytes) in live_bytes_by_file {
            let properties =
                blob::read_blob_file_properties_with_backend_async(&storage, db_path, file_id)
                    .await?;
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn write_blob_gc_replacement_tables_browser_async(
        &self,
        db_path: &Path,
        tables: Vec<BlobGcRewriteTable>,
    ) -> Result<Vec<NamedCompactionOutput>> {
        let storage = self.browser_storage()?;
        let mut outputs = Vec::with_capacity(tables.len());
        for rewrite_table in tables {
            let table_path = table::table_path(db_path, rewrite_table.output_table_id);
            let point_records = rewrite_table
                .point_records
                .iter()
                .map(|record| (record.internal_key.clone(), record.value.clone()))
                .collect::<Vec<_>>();
            let table = Arc::new(
                table::write_table_with_backend_async(
                    &storage,
                    &table_path,
                    rewrite_table.output_table_id,
                    rewrite_table.level,
                    &rewrite_table.options,
                    &point_records,
                    &rewrite_table.range_tombstones,
                    DurabilityMode::Flush,
                )
                .await?,
            );

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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn publish_compacted_tables_browser_async(
        &self,
        outputs: &[NamedCompactionOutput],
        obsolete_blob_ids: &[u64],
    ) -> Result<()> {
        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
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
        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?;
        let prepared = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            manifest.prepare_replace_tables_batch_publish(
                edits,
                obsolete_blob_ids.to_vec(),
                self.last_committed_sequence(),
            )?
        };
        prepared.publish_async().await?;
        manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_prepared_publish(prepared)
    }

    fn rewrite_wal_after_replay_floor(&self, replay_floor: Sequence) -> Result<()> {
        self.inner
            .substrate
            .rewrite_wal_after_replay_floor(replay_floor)
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
            &self.inner.native_storage,
            Some(db_path),
            &self.inner.snapshots,
            self.inner.manifest.as_ref(),
        )
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn cleanup_pending_obsolete_blob_files_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        if self.inner.snapshots.active_count() != 0 {
            return Ok(());
        }

        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?;
        let pending_file_ids = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest.state());
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

        let storage = self.browser_storage()?;
        for file_id in &pending_file_ids {
            storage
                .delete_object(StorageObjectId::native_file(
                    StorageObjectKind::Blob,
                    blob::blob_path(db_path, *file_id),
                ))
                .await?;
        }

        let prepared = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            manifest.prepare_clear_pending_blob_deletions_publish(&pending_file_ids)
        };
        let Some(prepared) = prepared else {
            return Ok(());
        };
        prepared.publish_async().await?;
        manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_prepared_publish(prepared)
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn retire_obsolete_table_files_browser_async(
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

        self.cleanup_pending_obsolete_table_files_browser_async(db_path)
            .await
    }

    fn cleanup_pending_obsolete_table_files(&self, db_path: &Path) -> Result<()> {
        cleanup_pending_obsolete_table_files(
            &self.inner.native_storage,
            Some(db_path),
            &self.inner.snapshots,
            &self.inner.pending_obsolete_table_ids,
        )
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn cleanup_pending_obsolete_table_files_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        if self.inner.snapshots.active_count() != 0 {
            return Ok(());
        }

        let table_ids = {
            let pending = self
                .inner
                .pending_obsolete_table_ids
                .lock()
                .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
            if pending.is_empty() {
                return Ok(());
            }
            pending.iter().copied().collect::<Vec<_>>()
        };

        let storage = self.browser_storage()?;
        for table_id in &table_ids {
            storage
                .delete_object(StorageObjectId::native_file(
                    StorageObjectKind::Table,
                    table::table_path(db_path, *table_id),
                ))
                .await?;
        }

        let mut pending = self
            .inner
            .pending_obsolete_table_ids
            .lock()
            .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
        for table_id in table_ids {
            pending.remove(&table_id);
        }

        Ok(())
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn remove_storage_files_browser_async(
        &self,
        db_path: &Path,
        table_ids: &[table::TableId],
    ) -> Result<()> {
        let storage = self.browser_storage()?;
        for table_id in table_ids {
            storage
                .delete_object(StorageObjectId::native_file(
                    StorageObjectKind::Table,
                    table::table_path(db_path, *table_id),
                ))
                .await?;
            storage
                .delete_object(StorageObjectId::native_file(
                    StorageObjectKind::Blob,
                    blob::blob_path(db_path, table_id.get()),
                ))
                .await?;
        }

        Ok(())
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

    fn foreground_l0_overlap_pressure_exceeded(&self) -> Result<bool> {
        if self.background_workers_enabled() {
            return Ok(false);
        }

        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        for state in buckets.values() {
            // Overlapping L0 files force point reads to test newer misses before
            // reaching older hits. When background workers are disabled, public
            // flush is also the foreground maintenance boundary, so close that
            // overlap before read-heavy work starts.
            if state.l0_has_overlapping_tables()? {
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
            .map(|table_id| table_file_bytes(&self.inner.native_storage, db_path, *table_id))
            .sum::<u64>();
        let output_bytes = output_table_ids
            .iter()
            .map(|table_id| table_file_bytes(&self.inner.native_storage, db_path, *table_id))
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    fn record_compaction_stats_from_tables(
        &self,
        runs: usize,
        input_tables: &[Arc<Table>],
        output_tables: &[Arc<Table>],
    ) {
        let input_bytes = input_tables
            .iter()
            .map(|table| table.estimated_file_bytes())
            .sum::<u64>();
        let output_bytes = output_tables
            .iter()
            .map(|table| table.estimated_file_bytes())
            .sum::<u64>();

        self.inner
            .compaction_runs
            .fetch_add(usize_to_u64_saturating(runs), Ordering::AcqRel);
        self.inner.compaction_input_tables.fetch_add(
            usize_to_u64_saturating(input_tables.len()),
            Ordering::AcqRel,
        );
        self.inner.compaction_output_tables.fetch_add(
            usize_to_u64_saturating(output_tables.len()),
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

/// Primary async database API. Synchronous callers can use the explicit
/// `*_sync` adapters above.
#[allow(clippy::unused_async)]
impl Db {
    /// Opens a database asynchronously.
    ///
    /// This has the same input conversion and recovery behavior as
    /// [`Db::open_sync`], but persistent storage work is driven through the
    /// configured runtime. On browser WASM targets this is the required entry
    /// point for browser persistence.
    ///
    /// # Parameters
    ///
    /// - `options`: either [`DbOptions`] or a path-like value converted through
    ///   [`DbOptions::new`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trine_kv::{Db, DbOptions};
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     db.put(b"k", b"v").await?;
    ///     assert_eq!(db.get(b"k").await?, Some(b"v".to_vec()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn open(options: impl IntoOpenOptions) -> Result<Self> {
        let options = options.into_open_options();
        match &options.storage_mode {
            StorageMode::InMemory => Self::memory_sync(options),
            StorageMode::Persistent { .. } => {
                Self::open_persistent_with_options_async(options).await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser,
            } => Self::open_browser_persistent_with_options_async(options).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => Self::open_wasi_persistent_with_options_async(options).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => Err(Error::unsupported_backend(
                "object-store databases must be opened with an object-store client",
            )),
        }
    }

    /// Returns a handle for the built-in default bucket.
    ///
    /// Direct `Db` methods such as [`Db::put`] and [`Db::get`] use this bucket
    /// internally. Use the handle when code wants to pass a bucket-bound API to
    /// another component or create a [`crate::BucketReader`].
    pub async fn default_bucket(&self) -> Result<Bucket> {
        self.default_bucket_sync()
    }

    /// Returns an existing named bucket or creates it with default options.
    ///
    /// Bucket creation is durable for persistent databases: Trine publishes the
    /// bucket metadata before returning the handle. If the bucket already
    /// exists, the existing options are reused.
    ///
    /// # Parameters
    ///
    /// - `name`: non-empty bucket name. The reserved default bucket name must
    ///   be accessed with [`Db::default_bucket`].
    pub async fn bucket(&self, name: impl Into<BucketName>) -> Result<Bucket> {
        self.bucket_with_options(name, BucketOptions::default())
            .await
    }

    /// Returns an existing named bucket or creates it with explicit options.
    ///
    /// Bucket options are fixed at creation. Calling this for an existing
    /// bucket with different options returns [`Error::InvalidOptions`] instead
    /// of silently changing the bucket's storage behavior.
    ///
    /// # Parameters
    ///
    /// - `name`: non-empty bucket name.
    /// - `options`: compression, filter, prefix, blob, and block settings used
    ///   if the bucket is created.
    pub async fn bucket_with_options(
        &self,
        name: impl Into<BucketName>,
        options: BucketOptions,
    ) -> Result<Bucket> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            return self
                .bucket_with_options_browser_async(name.into(), options)
                .await;
        }

        self.bucket_with_options_sync(name, options)
    }

    /// Reads the newest committed value for `key` from the default bucket.
    ///
    /// This is the async form of [`Db::get_sync`]. It returns owned value bytes,
    /// or `Ok(None)` when no value is visible at the newest committed sequence.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes in the built-in default bucket.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_sequence_async(DEFAULT_BUCKET_NAME, key, self.last_committed_sequence())
            .await
    }

    /// Reads many newest committed values from the default bucket.
    ///
    /// This is the async form of [`Db::get_many_sync`]. It preserves input
    /// order, returns `None` for missing or deleted keys, and fails the whole
    /// batch on storage or format errors. The batch captures one committed read
    /// sequence and one set of point-read sources before reading the first key,
    /// so all returned values share one consistent view of the default bucket.
    ///
    /// # Parameters
    ///
    /// - `keys`: user key bytes in the built-in default bucket. Empty input
    ///   returns an empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] if the handle is closed, plus storage or
    /// format errors encountered while reading tables or blob files. Any such
    /// error fails the whole batch.
    pub async fn get_many<K>(&self, keys: &[K]) -> Result<Vec<Option<Value>>>
    where
        K: AsRef<[u8]>,
    {
        self.default_bucket().await?.get_many(keys).await
    }

    /// Reads `key` from the default bucket at the sequence pinned by `snapshot`.
    pub async fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<Value>> {
        self.get_at_with_pin_state_async(
            DEFAULT_BUCKET_NAME,
            key,
            snapshot.read_sequence(),
            snapshot.is_pinned(),
        )
        .await
    }

    /// Writes one key/value pair to the default bucket using default write options.
    ///
    /// This is the async form of [`Db::put_sync`]. The write is appended to the
    /// WAL for persistent databases, added to the memtable, and made visible to
    /// later reads once the commit sequence is published.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes.
    /// - `value`: value bytes to store.
    pub async fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.put_with_options(key, value, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Writes one key/value pair to the default bucket and returns commit information.
    ///
    /// This is the async explicit-options form of [`Db::put`]. Use it when one
    /// write needs different durability than the database default.
    ///
    /// # Parameters
    ///
    /// - `key`: user key bytes.
    /// - `value`: value bytes to store.
    /// - `options`: per-write durability options.
    pub async fn put_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.put(key, value);
        self.write(batch, options).await
    }

    /// Adds a point delete for one default-bucket key using default write options.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.delete_with_options(key, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Adds a point delete for one default-bucket key and returns commit information.
    pub async fn delete_with_options(
        &self,
        key: impl Into<Vec<u8>>,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete(key);
        self.write(batch, options).await
    }

    /// Adds a range delete to the default bucket using default write options.
    pub async fn delete_range(&self, range: KeyRange) -> Result<()> {
        self.delete_range_with_options(range, WriteOptions::default())
            .await
            .map(|_| ())
    }

    /// Adds a range delete to the default bucket and returns commit information.
    pub async fn delete_range_with_options(
        &self,
        range: KeyRange,
        options: WriteOptions,
    ) -> Result<CommitInfo> {
        let mut batch = crate::WriteBatch::new();
        batch.delete_range(range);
        self.write(batch, options).await
    }

    /// Returns a forward iterator over default-bucket rows in `range`.
    ///
    /// This is the async form of [`Db::range_sync`]. The returned iterator is
    /// positioned over the newest committed sequence captured when this method
    /// runs.
    ///
    /// # Parameters
    ///
    /// - `range`: user-key range to scan.
    pub async fn range(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket iterator whose blob values are read on demand.
    pub async fn range_lazy(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket iterator over `range` at `snapshot`.
    pub async fn range_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward value-lazy default-bucket iterator at `snapshot`.
    pub async fn range_lazy_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a reverse iterator over default-bucket rows in `range`.
    pub async fn range_reverse(&self, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket iterator whose blob values are read on demand.
    pub async fn range_lazy_reverse(&self, range: &KeyRange) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket iterator over `range` at `snapshot`.
    pub async fn range_reverse_at(&self, snapshot: &Snapshot, range: &KeyRange) -> Result<Iter> {
        self.range_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse value-lazy default-bucket iterator at `snapshot`.
    pub async fn range_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        range: &KeyRange,
    ) -> Result<LazyIter> {
        self.range_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            range,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a forward iterator over default-bucket rows whose keys begin with `prefix`.
    pub async fn prefix(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket prefix iterator whose blob values are read on demand.
    pub async fn prefix_lazy(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_at(&self, snapshot: &Snapshot, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a forward value-lazy default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_lazy_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Forward,
        )
        .await
    }

    /// Returns a reverse iterator over default-bucket rows whose keys begin with `prefix`.
    pub async fn prefix_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket prefix iterator whose blob values are read on demand.
    pub async fn prefix_lazy_reverse(&self, prefix: impl Into<Vec<u8>>) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            self.last_committed_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        let prefix = prefix.into();
        self.prefix_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Returns a reverse value-lazy default-bucket prefix iterator at `snapshot`.
    pub async fn prefix_lazy_reverse_at(
        &self,
        snapshot: &Snapshot,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        let prefix = prefix.into();
        self.prefix_lazy_at_sequence_async(
            DEFAULT_BUCKET_NAME,
            &prefix,
            snapshot.read_sequence(),
            Direction::Reverse,
        )
        .await
    }

    /// Persists pending WAL bytes according to `mode`.
    ///
    /// This is the async form of [`Db::persist_sync`]. Native persistent
    /// databases run blocking persistence work through the configured runtime;
    /// browser persistent databases use the browser storage path on supported
    /// targets.
    ///
    /// # Parameters
    ///
    /// - `mode`: durability level to request for pending WAL bytes.
    pub async fn persist(&self, mode: DurabilityMode) -> Result<()> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if matches!(
            self.inner.options.storage_mode,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser
            }
        ) {
            return self.persist_browser_async(mode).await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self
                .run_native_blocking_task(move |db| db.persist_sync(mode))
                .await;
        }

        self.persist_sync(mode)
    }

    /// Flushes committed memtable data to persistent table files.
    ///
    /// This is the async form of [`Db::flush_sync`]. It can be used by async
    /// applications to force committed memtable data into table files without
    /// blocking the caller's executor thread on native persistent storage.
    pub async fn flush(&self) -> Result<()> {
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return self.flush_object_store_async().await;
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent flush task was cancelled",
                async move { db.flush_browser_async().await },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self.run_native_blocking_task(|db| db.flush_sync()).await;
        }

        self.flush_sync()
    }

    /// Compacts table files that overlap `range`.
    pub async fn compact_range(&self, range: KeyRange) -> Result<()> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent compaction task was cancelled",
                async move { db.compact_range_browser_async(range).await },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self
                .run_native_blocking_task(move |db| db.compact_range_sync(range))
                .await;
        }

        self.compact_range_sync(range)
    }

    /// Compacts table files that overlap `range` within `budget`.
    pub async fn compact_range_with_budget(
        &self,
        range: KeyRange,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent compaction task was cancelled",
                async move {
                    db.compact_range_with_budget_browser_async(range, budget)
                        .await
                },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self
                .run_native_blocking_task(move |db| {
                    db.compact_range_with_budget_sync(range, budget)
                })
                .await;
        }

        self.compact_range_with_budget_sync(range, budget)
    }

    /// Runs cooperative flush and compaction work within `budget`.
    pub async fn run_maintenance_with_budget(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.inner.options.storage_mode.is_browser_persistent() {
            let db = self.clone();
            return Self::run_owned_browser_task(
                "browser persistent maintenance task was cancelled",
                async move { db.run_maintenance_with_budget_browser_async(budget).await },
            )
            .await;
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self
                .run_native_blocking_task(move |db| db.run_maintenance_with_budget_sync(budget))
                .await;
        }

        self.run_maintenance_with_budget_sync(budget)
    }

    /// Closes this handle asynchronously and stops background workers.
    pub async fn close(&self) -> Result<()> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.persistent_path().is_some() {
            return self
                .run_native_blocking_task(|db| {
                    db.close_sync();
                    Ok(())
                })
                .await;
        }

        self.close_sync();
        Ok(())
    }
}

fn validate_options(options: &DbOptions) -> Result<()> {
    if let StorageMode::HostPersistent {
        backend: HostStorageBackend::Browser,
    } = &options.storage_mode
    {
        return Err(Error::unsupported_backend(
            HostStorageBackend::Browser.as_str(),
        ));
    }
    validate_common_options(options)
}

fn validate_common_options(options: &DbOptions) -> Result<()> {
    runtime::validate_runtime_options(
        options.runtime,
        &options.storage_mode,
        options.read_only,
        options.background_worker_count,
    )?;
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

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
fn background_worker_loop(
    inner: &Weak<DbInner>,
    maintenance: &MaintenanceCoordinator,
    runtime_shutdown: &CancellationToken,
) {
    while let Some(request) = maintenance.wait_for_request() {
        if runtime_shutdown.is_cancelled() {
            break;
        }
        let Some(inner) = inner.upgrade() else {
            break;
        };
        if inner.closed.load(Ordering::Acquire) || runtime_shutdown.is_cancelled() {
            break;
        }

        let db = Db {
            inner,
            counts_as_user_handle: false,
        };
        match db.run_background_maintenance(request) {
            Ok(()) => record_maintenance_success(maintenance),
            Err(Error::Closed) => break,
            Err(error) => maintenance.record_error(&error),
        }
    }
}

fn acquire_persistent_process_lock(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
) -> Result<Option<recovery::ProcessLock>> {
    if options.read_only {
        return Ok(None);
    }
    recovery::ProcessLock::acquire_with_backend(backend, db_path).map(Some)
}

async fn acquire_persistent_process_lock_async(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
) -> Result<Option<recovery::ProcessLock>> {
    if options.read_only {
        return Ok(None);
    }
    recovery::ProcessLock::acquire_with_backend_async(backend, db_path)
        .await
        .map(Some)
}

fn list_persistent_directory_files(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<StorageDirectoryFile>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    backend.list_directory_files_blocking(StorageDirectoryId::native_file(db_path))
}

async fn list_persistent_directory_files_async(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<StorageDirectoryFile>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    backend
        .list_directory_files(StorageDirectoryId::native_file(db_path))
        .await
}

fn repair_safe_temporary_files_for_open(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let policy = if options.read_only {
        FailOnCorruptionPolicy::FailClosed
    } else {
        options.fail_on_corruption
    };
    recovery::repair_safe_temporary_files_from_directory_files_with_backend(
        backend,
        db_path,
        policy,
        directory_files,
    )?;
    Ok(())
}

async fn repair_safe_temporary_files_for_open_from_directory_files_async(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let policy = if options.read_only {
        FailOnCorruptionPolicy::FailClosed
    } else {
        options.fail_on_corruption
    };
    recovery::repair_safe_temporary_files_from_directory_files_with_backend_async(
        backend,
        db_path,
        policy,
        directory_files,
    )
    .await?;
    Ok(())
}

fn run_persistent_recovery_checks(
    backend: &NativeFileBackend,
    db_path: &Path,
    manifest: &ManifestState,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend(
        backend,
        db_path,
        &referenced_blob_ids,
    )?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend(backend, db_path, manifest)?;
    recovery::fail_on_unreferenced_storage_files_from_directory_files(
        db_path,
        directory_files,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
}

async fn run_persistent_recovery_checks_from_directory_files_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
    directory_files: &[StorageDirectoryFile],
) -> Result<()>
where
    B: StorageReadBackend + StorageObjectReadBackend,
{
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend_async(
        backend,
        db_path,
        &referenced_blob_ids,
    )
    .await?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend_async(backend, db_path, manifest)
        .await?;
    recovery::fail_on_unreferenced_storage_files_from_directory_files(
        db_path,
        directory_files,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
async fn run_persistent_recovery_checks_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<()>
where
    B: StorageReadBackend
        + StorageObjectReadBackend
        + StorageObjectListBackend
        + StorageDirectoryListBackend,
{
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend_async(
        backend,
        db_path,
        &referenced_blob_ids,
    )
    .await?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend_async(backend, db_path, manifest)
        .await?;
    recovery::fail_on_unreferenced_storage_files_with_backend_async(
        backend,
        db_path,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
    .await
}

fn buckets_from_manifest(
    backend: &NativeFileBackend,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<BTreeMap<String, Arc<LsmTree>>> {
    let mut buckets = BTreeMap::new();

    for (name, options) in manifest.buckets() {
        validate_bucket_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let table = table::read_table_with_backend(backend, &table_path)?
                .with_manifest_properties(properties)?;
            tables.push(Arc::new(table));
        }

        buckets.insert(
            name.clone(),
            Arc::new(LsmTree::new(options.clone(), tables)?),
        );
    }

    Ok(buckets)
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
async fn buckets_from_manifest_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
    inline_blob_values: bool,
) -> Result<BTreeMap<String, Arc<LsmTree>>>
where
    B: StorageReadBackend + StorageObjectReadBackend,
{
    let mut buckets = BTreeMap::new();

    for (name, options) in manifest.buckets() {
        validate_bucket_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let mut table = table::read_table_with_backend_async(backend, &table_path)
                .await?
                .with_manifest_properties(properties)?;
            if inline_blob_values {
                table =
                    table::inline_blob_values_with_backend_async(backend, db_path, table).await?;
            }
            tables.push(Arc::new(table));
        }

        buckets.insert(
            name.clone(),
            Arc::new(LsmTree::new(options.clone(), tables)?),
        );
    }

    Ok(buckets)
}

#[allow(dead_code)]
async fn read_manifest_or_empty_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<ManifestState>
where
    B: StorageManifestReadBackend,
{
    match manifest::read_manifest_with_backend_async(backend, path).await {
        Ok(manifest) => Ok(manifest),
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            Ok(ManifestState::empty())
        }
        Err(error) => Err(error),
    }
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

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
async fn ensure_default_bucket_in_manifest_async(
    manifest: &mut ManifestStore,
    options: &DbOptions,
) -> Result<()> {
    if manifest.state().buckets().contains_key(DEFAULT_BUCKET_NAME) || options.read_only {
        return Ok(());
    }

    manifest
        .create_bucket_async(
            DEFAULT_BUCKET_NAME.to_owned(),
            options.default_bucket_options.clone(),
        )
        .await
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

fn table_file_bytes(backend: &NativeFileBackend, db_path: &Path, table_id: table::TableId) -> u64 {
    storage_object_file_bytes(
        backend,
        StorageObjectKind::Table,
        &table::table_path(db_path, table_id),
    )
}

fn add_obsolete_blob_stats(
    backend: &NativeFileBackend,
    db_path: &Path,
    live_blob_bytes_by_file: &BTreeMap<u64, u64>,
    stats: &mut DbStats,
) {
    for (file_id, live_bytes) in live_blob_bytes_by_file {
        let Ok(properties) =
            blob::read_blob_file_properties_with_backend(backend, db_path, *file_id)
        else {
            continue;
        };
        if properties.encoded_bytes > *live_bytes {
            stats.stale_blob_files = stats.stale_blob_files.saturating_add(1);
            stats.stale_blob_bytes = stats
                .stale_blob_bytes
                .saturating_add(properties.encoded_bytes - *live_bytes);
        }
    }

    let Ok(blob_file_ids) = blob::list_blob_file_ids_with_backend(backend, db_path) else {
        return;
    };

    for file_id in blob_file_ids {
        if live_blob_bytes_by_file.contains_key(&file_id) {
            continue;
        }
        stats.obsolete_blob_files += 1;
        let bytes = storage_object_file_bytes(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, file_id),
        );
        stats.obsolete_blob_bytes = stats.obsolete_blob_bytes.saturating_add(bytes);
        stats.stale_blob_files = stats.stale_blob_files.saturating_add(1);
        stats.stale_blob_bytes = stats.stale_blob_bytes.saturating_add(bytes);
    }
}

fn storage_object_file_bytes(
    backend: &NativeFileBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> u64 {
    if backend
        .capabilities()
        .require(StorageCapability::RandomRead)
        .is_err()
    {
        return 0;
    }

    let object_id = StorageObjectId::native_file(kind, path);
    let Ok(object) = backend.open_read_blocking(object_id) else {
        return 0;
    };

    object.len_blocking().unwrap_or(0)
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
    backend: &NativeFileBackend,
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
        let table = Arc::new(table::write_table_with_backend(
            backend,
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

fn compaction_options(
    options: &DbOptions,
    local_l0_compaction: bool,
) -> compaction::CompactionOptions {
    compaction::CompactionOptions {
        target_table_bytes: usize_to_u64_saturating(options.target_table_bytes),
        level_size_multiplier: usize_to_u64_saturating(options.level_size_multiplier),
        max_l0_files: options.max_l0_files,
        local_l0_compaction,
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
    options.storage_mode.persistent_path()
}

fn cleanup_pending_obsolete_table_files(
    backend: &NativeFileBackend,
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

    remove_table_files(backend, db_path, &table_ids)?;

    let mut pending = pending_table_ids
        .lock()
        .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
    for table_id in table_ids {
        pending.remove(&table_id);
    }

    Ok(())
}

fn cleanup_pending_obsolete_blob_files(
    backend: &NativeFileBackend,
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
        delete_storage_object(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, *file_id),
        )?;
    }

    manifest
        .lock()
        .map_err(|_| lock_poisoned("manifest store"))?
        .clear_pending_blob_deletions(&pending_file_ids)
}

fn remove_table_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Table,
            &table::table_path(db_path, *table_id),
        )?;
    }

    Ok(())
}

fn remove_blob_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, table_id.get()),
        )?;
    }

    Ok(())
}

fn delete_storage_object(
    backend: &NativeFileBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)?;
    backend.delete_object_blocking(StorageObjectId::native_file(kind, path))
}

fn sync_storage_directory_after_renames(backend: &NativeFileBackend, path: &Path) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectorySync)?;
    backend.sync_directory_after_renames_blocking(StorageDirectoryId::native_file(path))
}

fn create_storage_directory_all(backend: &NativeFileBackend, path: &Path) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryCreate)?;
    backend.create_directory_all_blocking(StorageDirectoryId::native_file(path))
}

async fn create_storage_directory_all_async(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryCreate)?;
    backend
        .create_directory_all(StorageDirectoryId::native_file(path))
        .await
}

fn remove_storage_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    // A table write uses the table id as the blob file id for large values.
    // Before manifest publish succeeds, both files are unpublished output and
    // can be removed together after a failed flush or compaction attempt.
    remove_table_files(backend, db_path, table_ids)?;
    remove_blob_files(backend, db_path, table_ids)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex, mpsc},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    use std::{
        future::Future,
        task::{Context, Poll, Wake, Waker},
    };

    use super::{
        CompactionReservation, Db, Error, MaintenanceCoordinator, compaction_reservations_conflict,
        record_maintenance_success, shutdown_background_workers,
    };
    use crate::{
        bucket::DEFAULT_BUCKET_NAME,
        options::{BucketOptions, DbOptions},
        runtime::CancellationToken,
        storage::{StorageCapability, StorageReadBackend},
        types::KeyRange,
    };

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    struct ThreadWaker {
        thread: thread::Thread,
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.thread.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.thread.unpark();
        }
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn block_on_test_future<T>(future: impl Future<Output = crate::Result<T>>) -> crate::Result<T> {
        let waker = Waker::from(Arc::new(ThreadWaker {
            thread: thread::current(),
        }));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(result) => return result,
                Poll::Pending => thread::park_timeout(Duration::from_secs(1)),
            }
        }
    }

    #[test]
    fn object_store_database_persists_across_reopen() {
        use crate::object_store::{InMemoryObjectStore, ObjectClient};

        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

        {
            let db = block_on_test_future(Db::open_object_store_async(
                Arc::clone(&client),
                DbOptions::object_store(),
            ))
            .expect("open object-store database");
            db.put_sync(b"alpha", b"one").expect("put alpha");
            db.put_sync(b"beta", b"two").expect("put beta");
            assert_eq!(
                db.get_sync(b"alpha").expect("get alpha").as_deref(),
                Some(b"one".as_slice())
            );
            // Durable flush: memtables -> SSTable objects + manifest CAS publish.
            block_on_test_future(db.flush()).expect("flush to object storage");
        }

        // Reopen from the same object store. There is no WAL: recovery comes from
        // the manifest (read via CAS store) + the flushed SSTable objects.
        let db = block_on_test_future(Db::open_object_store_async(
            client,
            DbOptions::object_store(),
        ))
        .expect("reopen object-store database");
        assert_eq!(
            db.get_sync(b"alpha")
                .expect("get alpha after reopen")
                .as_deref(),
            Some(b"one".as_slice())
        );
        assert_eq!(
            db.get_sync(b"beta")
                .expect("get beta after reopen")
                .as_deref(),
            Some(b"two".as_slice())
        );
    }

    #[test]
    fn object_store_flush_is_rejected_synchronously() {
        use crate::object_store::{InMemoryObjectStore, ObjectClient};

        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let db = block_on_test_future(Db::open_object_store_async(
            client,
            DbOptions::object_store(),
        ))
        .expect("open object-store database");
        assert!(
            db.flush_sync().is_err(),
            "object-store flush must require the async API"
        );
    }

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
    fn background_shutdown_cancels_runtime_token() {
        let maintenance = Arc::new(MaintenanceCoordinator::new());
        let runtime_shutdown = CancellationToken::new();
        let workers = Mutex::new(Vec::new());

        shutdown_background_workers(&maintenance, &runtime_shutdown, &workers);

        assert!(runtime_shutdown.is_cancelled());
    }

    #[test]
    fn persistent_open_attaches_runtime_enabled_native_storage_backend() {
        let path = temp_db_path("persistent-runtime-native-storage");
        let mut options = DbOptions::persistent(&path);
        options.background_worker_count = 0;
        options.default_bucket_options =
            options.default_bucket_options.with_blob_threshold_bytes(4);
        let db = Db::open_sync(options).expect("persistent db opens");

        let capabilities = db.inner.native_storage.capabilities();
        assert!(capabilities.supports(StorageCapability::AsyncTasks));
        assert!(capabilities.supports(StorageCapability::BlockingAdapter));
        assert!(capabilities.supports(StorageCapability::BackgroundThreads));
        assert!(!capabilities.supports(StorageCapability::PlatformAsyncIo));

        let value = b"value-stored-through-blob".to_vec();
        db.put_sync(b"key", value.clone()).expect("write");
        db.flush_sync()
            .expect("flush through db-owned native storage");
        assert_eq!(
            db.get_sync(b"key").expect("read after flush"),
            Some(value.clone())
        );
        let stats = db.stats();
        assert_eq!(stats.live_blob_files, 1);
        assert!(stats.live_blob_bytes >= value.len() as u64);
        assert!(stats.storage_uses_sync_adapter);
        assert!(!stats.storage_uses_platform_async_io);
        assert_eq!(stats.storage_sync_adapter_queue_capacity, 1024);
        assert!(stats.storage_sync_adapter_submitted_tasks >= stats.storage_sync_adapter_tasks);
        assert!(stats.storage_operations.open_append.requests > 0);
        assert!(stats.storage_operations.write_object.requests > 0);
        assert_eq!(stats.storage_inline_tasks, 0);

        drop(db);
        fs::remove_dir_all(path).expect("cleanup test db");
    }

    #[test]
    fn native_read_only_open_skips_writer_lease_and_rejects_writes() {
        let path = temp_db_path("native-read-only-open");
        let mut writable_options = DbOptions::persistent(&path);
        writable_options.background_worker_count = 0;
        {
            let db = Db::open_sync(writable_options.clone()).expect("persistent db opens");
            db.put_sync(b"key", b"value").expect("write succeeds");
            db.flush_sync().expect("flush succeeds");
        }

        let db = Db::open_sync(writable_options.read_only()).expect("read-only db opens");

        assert!(db.options().read_only);
        assert!(!db.inner.substrate.wal_is_present());
        assert_eq!(
            db.get_sync(b"key").expect("read-only read succeeds"),
            Some(b"value".to_vec())
        );
        assert!(matches!(
            db.put_sync(b"other", b"value"),
            Err(Error::ReadOnly)
        ));
        assert_eq!(db.stats().storage_operations.read_object_bytes.requests, 0);
        assert_eq!(
            db.stats().storage_operations.acquire_writer_lease.requests,
            0
        );

        drop(db);
        fs::remove_dir_all(path).expect("cleanup test db");
    }

    #[test]
    fn native_read_only_open_replays_non_empty_wal() {
        let path = temp_db_path("native-read-only-open-wal-replay");
        let mut writable_options = DbOptions::persistent(&path);
        writable_options.background_worker_count = 0;
        {
            let db = Db::open_sync(writable_options.clone()).expect("persistent db opens");
            db.put_sync(b"key", b"value").expect("write succeeds");
        }

        let db = Db::open_sync(writable_options.read_only()).expect("read-only db opens");

        assert_eq!(
            db.get_sync(b"key").expect("read-only WAL read succeeds"),
            Some(b"value".to_vec())
        );
        assert!(
            db.stats().storage_operations.read_object_bytes.requests > 0,
            "read-only open must read non-empty WAL shards"
        );

        drop(db);
        fs::remove_dir_all(path).expect("cleanup test db");
    }

    #[cfg(not(target_os = "wasi"))]
    #[test]
    fn wasi_persistent_backend_requires_wasi_target() {
        let path = temp_db_path("wasi-persistent-host-unsupported");
        let options = DbOptions::wasi_persistent(&path);
        assert_eq!(options.runtime.mode, crate::runtime::RuntimeMode::Inline);
        assert_eq!(options.background_worker_count, 0);

        let wasi_error = Db::open_sync(options).expect_err("WASI backend requires WASI target");
        assert!(matches!(wasi_error, Error::UnsupportedBackend { .. }));
        assert!(wasi_error.to_string().contains("WASI persistent"));
    }

    #[cfg(not(target_os = "wasi"))]
    #[test]
    fn wasi_persistent_open_async_requires_wasi_target() {
        let path = temp_db_path("wasi-persistent-async-host-unsupported");
        let error = block_on_test_future(Db::open(DbOptions::wasi_persistent(&path)))
            .expect_err("WASI async open requires WASI target");

        assert!(matches!(error, Error::UnsupportedBackend { .. }));
        assert!(error.to_string().contains("WASI persistent"));
    }

    #[cfg(target_os = "wasi")]
    #[test]
    fn wasi_persistent_backend_uses_host_filesystem() {
        let path = temp_db_path("wasi-persistent-host");
        let db = Db::open_sync(DbOptions::wasi_persistent(&path)).expect("WASI db opens");
        db.put_sync(b"key", b"value").expect("WASI write succeeds");
        db.flush_sync().expect("WASI flush succeeds");
        drop(db);

        let db = Db::open_sync(DbOptions::wasi_persistent_read_only(&path))
            .expect("WASI read-only db reopens");
        assert_eq!(
            db.get_sync(b"key").expect("WASI read succeeds"),
            Some(b"value".to_vec())
        );
        drop(db);

        fs::remove_dir_all(path).expect("cleanup WASI test db");
    }

    #[cfg(target_os = "wasi")]
    #[test]
    fn wasi_persistent_open_async_uses_host_filesystem() {
        let path = temp_db_path("wasi-persistent-async-host");
        let db = block_on_test_future(Db::open(DbOptions::wasi_persistent(&path)))
            .expect("WASI async db opens");
        db.put_sync(b"key", b"value").expect("WASI write succeeds");
        db.flush_sync().expect("WASI flush succeeds");
        drop(db);

        let db = block_on_test_future(Db::open(DbOptions::wasi_persistent_read_only(&path)))
            .expect("WASI async read-only db reopens");
        assert_eq!(
            db.get_sync(b"key").expect("WASI read succeeds"),
            Some(b"value".to_vec())
        );
        drop(db);

        fs::remove_dir_all(path).expect("cleanup WASI async test db");
    }

    #[test]
    fn browser_persistent_backend_is_explicitly_unsupported() {
        let options = DbOptions::browser_persistent();
        assert_eq!(options.runtime.mode, crate::runtime::RuntimeMode::Inline);
        assert_eq!(options.background_worker_count, 0);

        let browser_error =
            Db::open_sync(options).expect_err("browser backend is not wired for sync open");
        assert!(matches!(browser_error, Error::UnsupportedBackend { .. }));
        assert!(browser_error.to_string().contains("browser persistent"));
    }

    #[test]
    fn browser_persistent_read_only_options_disable_creation() {
        let options = DbOptions::browser_persistent_read_only();
        assert!(options.read_only);
        assert!(!options.create_if_missing);
        assert_eq!(options.runtime.mode, crate::runtime::RuntimeMode::Inline);
        assert_eq!(options.background_worker_count, 0);
    }

    #[test]
    fn get_many_sync_preserves_order_missing_deletes_and_duplicates() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        db.put_sync(b"a", b"one").expect("first write");
        db.put_sync(b"b", b"two").expect("second write");
        db.delete_sync(b"deleted").expect("delete writes");

        let keys = [
            b"b".as_slice(),
            b"missing".as_slice(),
            b"a".as_slice(),
            b"b".as_slice(),
            b"deleted".as_slice(),
        ];
        let values = db.get_many_sync(&keys).expect("batch reads");

        assert_eq!(
            values,
            vec![
                Some(b"two".to_vec()),
                None,
                Some(b"one".to_vec()),
                Some(b"two".to_vec()),
                None,
            ]
        );
    }

    #[test]
    fn bucket_get_many_sync_reads_named_bucket_only() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        db.put_sync(b"same", b"default").expect("default write");
        let users = db.bucket_sync("users").expect("named bucket opens");
        users.put_sync(b"same", b"named").expect("named write");

        let keys = [b"same".as_slice(), b"missing".as_slice()];
        let values = users.get_many_sync(&keys).expect("named batch reads");

        assert_eq!(values, vec![Some(b"named".to_vec()), None]);
        assert_eq!(
            db.get_many_sync(&keys).expect("default batch reads"),
            vec![Some(b"default".to_vec()), None]
        );
    }

    #[test]
    fn bucket_reader_get_many_sync_keeps_snapshot_view() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        let bucket = db.default_bucket_sync().expect("default bucket opens");
        bucket.put_sync(b"a", b"one").expect("first write");
        bucket.put_sync(b"b", b"two").expect("second write");
        let snapshot = db.snapshot();
        let reader = bucket.reader(&snapshot).expect("reader opens");

        bucket.put_sync(b"a", b"new").expect("new write");
        bucket.put_sync(b"c", b"three").expect("third write");

        let keys = [b"a".as_slice(), b"c".as_slice(), b"b".as_slice()];
        let values = reader
            .get_many_owned_sync(&keys)
            .expect("snapshot batch reads");

        assert_eq!(
            values,
            vec![Some(b"one".to_vec()), None, Some(b"two".to_vec())]
        );
        assert_eq!(
            bucket.get_many_sync(&keys).expect("current batch reads"),
            vec![
                Some(b"new".to_vec()),
                Some(b"three".to_vec()),
                Some(b"two".to_vec()),
            ]
        );
    }

    #[test]
    fn get_many_async_preserves_order() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        db.put_sync(b"a", b"one").expect("first write");
        db.put_sync(b"b", b"two").expect("second write");

        let keys = [b"b".as_slice(), b"missing".as_slice(), b"a".as_slice()];
        let values = block_on_test_future(db.get_many(&keys)).expect("async batch reads");

        assert_eq!(
            values,
            vec![Some(b"two".to_vec()), None, Some(b"one".to_vec())]
        );
    }

    #[test]
    fn get_many_sync_groups_persistent_keys_by_data_block() {
        let path = temp_db_path("get-many-block-grouping");
        let options = DbOptions::persistent(&path).with_default_bucket_options(BucketOptions {
            block_bytes: 4096,
            ..BucketOptions::default()
        });
        let db = Db::open_sync(options).expect("persistent db opens");
        for index in 0..8 {
            let key = format!("key-{index:02}");
            let value = format!("value-{index:02}");
            db.put_sync(key.as_bytes(), value.as_bytes())
                .expect("write key");
        }
        db.flush_sync().expect("flush table");

        let before = db.stats();
        let keys = [
            b"key-01".as_slice(),
            b"key-02".as_slice(),
            b"key-03".as_slice(),
            b"key-01".as_slice(),
        ];
        let values = db.get_many_sync(&keys).expect("batch reads");
        let after = db.stats();

        assert_eq!(
            values,
            vec![
                Some(b"value-01".to_vec()),
                Some(b"value-02".to_vec()),
                Some(b"value-03".to_vec()),
                Some(b"value-01".to_vec()),
            ]
        );
        assert_eq!(
            after
                .read_path
                .point_data_block_reads
                .saturating_sub(before.read_path.point_data_block_reads),
            1,
            "batch keys in one data block should share the block read"
        );

        drop(db);
        fs::remove_dir_all(path).expect("cleanup test db");
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    #[test]
    fn browser_persistent_open_async_requires_browser_target() {
        let error = block_on_test_future(Db::open(DbOptions::browser_persistent_read_only()))
            .expect_err("browser async open requires browser target");
        assert!(matches!(error, Error::UnsupportedBackend { .. }));
        assert!(error.to_string().contains("browser persistent"));
    }

    #[test]
    fn compaction_reservation_conflicts_are_bucket_and_range_scoped() {
        let base = reservation("default", KeyRange::half_open(b"a", b"c"));

        assert!(compaction_reservations_conflict(
            &base,
            &reservation("default", KeyRange::half_open(b"b", b"d"))
        ));
        assert!(!compaction_reservations_conflict(
            &base,
            &reservation("default", KeyRange::half_open(b"c", b"e"))
        ));
        assert!(!compaction_reservations_conflict(
            &base,
            &reservation("other", KeyRange::half_open(b"b", b"d"))
        ));
    }

    #[test]
    fn maintenance_coordinator_allows_non_overlapping_compactions() {
        let coordinator = Arc::new(MaintenanceCoordinator::new());
        let first = coordinator
            .reserve_compactions(vec![reservation(
                "default",
                KeyRange::half_open(b"a", b"c"),
            )])
            .expect("first compaction reserves");
        let second = coordinator
            .reserve_compactions(vec![
                reservation("default", KeyRange::half_open(b"b", b"d")),
                reservation("default", KeyRange::half_open(b"c", b"e")),
                reservation("other", KeyRange::half_open(b"b", b"d")),
            ])
            .expect("non-overlapping compactions reserve");

        assert!(!second.contains("default", &KeyRange::half_open(b"b", b"d")));
        assert!(second.contains("default", &KeyRange::half_open(b"c", b"e")));
        assert!(second.contains("other", &KeyRange::half_open(b"b", b"d")));

        drop(first);
        drop(second);
        let third = coordinator
            .reserve_compactions(vec![reservation(
                "default",
                KeyRange::half_open(b"b", b"d"),
            )])
            .expect("released range can reserve again");
        assert!(third.contains("default", &KeyRange::half_open(b"b", b"d")));
    }

    #[test]
    fn flush_waits_for_existing_flush_guard() {
        let path = temp_db_path("flush-waits-for-existing-guard");
        let mut options = DbOptions::persistent(&path);
        options.background_worker_count = 0;
        let db = Db::open_sync(options).expect("open db");
        db.put_sync(b"key", b"value").expect("write");

        let flush_guard = db
            .inner
            .maintenance
            .try_start_flush()
            .expect("test holds flush guard");
        let thread_db = db.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).expect("report flush thread start");
            done_tx
                .send(thread_db.flush_sync())
                .expect("send flush result");
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("flush thread starts");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "public flush must wait while another flush guard is active"
        );

        drop(flush_guard);
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("flush finishes after guard release")
            .expect("flush succeeds");
        handle.join().expect("flush thread joins");

        let stats = db.stats();
        assert_eq!(stats.memtable_bytes, 0);
        assert_eq!(stats.immutable_memtables, 0);
        assert!(stats.total_tables > 0);

        drop(db);
        fs::remove_dir_all(path).expect("cleanup test db");
    }

    #[test]
    fn flush_returns_after_default_background_flush_publishes_tables() {
        let path = temp_db_path("flush-default-background-publishes");
        let mut options = DbOptions::persistent(&path);
        options.write_buffer_bytes = 128;
        let db = Db::open_sync(options).expect("open db");

        for index in 0..128_u32 {
            let key = format!("key-{index:04}");
            db.put_sync(key.as_bytes(), [b'x'; 96]).expect("write");
        }

        db.flush_sync().expect("public flush");
        let stats = db.stats();
        assert_eq!(stats.memtable_bytes, 0);
        assert_eq!(stats.immutable_memtables, 0);
        assert!(stats.total_tables > 0);

        drop(db);
        fs::remove_dir_all(path).expect("cleanup test db");
    }

    #[test]
    fn compact_range_is_not_silent_best_effort() {
        let path = temp_db_path("compact-range-waits-for-guard");
        let mut options = DbOptions::persistent(&path);
        options.background_worker_count = 0;
        let db = Db::open_sync(options).expect("open db");
        db.put_sync(b"a1", b"one").expect("write first");
        db.flush_sync().expect("flush first table");
        db.put_sync(b"a2", b"two").expect("write second");
        db.flush_sync().expect("flush second table");

        let compaction_guard = db
            .inner
            .maintenance
            .reserve_compactions(vec![reservation(DEFAULT_BUCKET_NAME, KeyRange::all())])
            .expect("test holds compaction reservation");
        let thread_db = db.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).expect("report compaction thread start");
            done_tx
                .send(thread_db.compact_range_sync(KeyRange::all()))
                .expect("send compaction result");
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("compaction thread starts");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "public compact_range must wait while its range is reserved"
        );

        drop(compaction_guard);
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("compaction finishes after guard release")
            .expect("compaction succeeds");
        handle.join().expect("compaction thread joins");
        assert!(db.stats().compaction_runs > 0);
        assert!(db.stats().maintenance_cooperative_yields > 0);

        drop(db);
        fs::remove_dir_all(path).expect("cleanup test db");
    }

    fn reservation(bucket: &str, range: KeyRange) -> CompactionReservation {
        CompactionReservation {
            bucket: bucket.to_owned(),
            range,
        }
    }

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after UNIX epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("trine-kv-{name}-{}-{nonce}", std::process::id()))
    }
}
