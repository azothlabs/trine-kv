use std::{
    collections::BTreeMap,
    ops::Bound,
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
}

impl KeyspaceState {
    fn new(options: KeyspaceOptions) -> Self {
        Self {
            options,
            entries: RwLock::new(BTreeMap::new()),
            range_tombstones: RwLock::new(Vec::new()),
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
        let keyspaces = keyspaces_from_manifest(manifest.state())?;

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
                    Arc::new(KeyspaceState::new(options)),
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
        Err(Error::unsupported("memtable flush is not implemented yet"))
    }

    pub fn compact_range(&self, _range: KeyRange) -> Result<()> {
        self.ensure_open()?;
        Err(Error::unsupported("compaction is not implemented yet"))
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
        let entries = state
            .entries
            .read()
            .map_err(|_| lock_poisoned("memtable entries"))?;

        for (internal_key, value) in entries.iter() {
            match internal_key.user_key().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {}
            }

            if internal_key.sequence() > read_sequence {
                continue;
            }

            return match internal_key.kind() {
                ValueKind::Put => {
                    if range_tombstone_covers(
                        &state,
                        internal_key.user_key(),
                        internal_key.sequence(),
                        internal_key.batch_index(),
                        read_sequence,
                    )? {
                        return Ok(None);
                    }

                    inline_value(value.as_ref()).map(|bytes| Some(bytes.to_vec()))
                }
                ValueKind::PointDelete => Ok(None),
                ValueKind::RangeDelete => continue,
            };
        }

        Ok(None)
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
    manifest: &ManifestState,
) -> Result<BTreeMap<String, Arc<KeyspaceState>>> {
    let mut keyspaces = BTreeMap::new();

    for (name, options) in manifest.keyspaces() {
        validate_keyspace_options(options)?;
        keyspaces.insert(name.clone(), Arc::new(KeyspaceState::new(options.clone())));
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

fn point_key_modified_after(
    state: &KeyspaceState,
    key: &[u8],
    read_sequence: Sequence,
) -> Result<bool> {
    // A point read is invalidated by either a newer point record for that user
    // key or a newer range tombstone covering it.
    let entries = state
        .entries
        .read()
        .map_err(|_| lock_poisoned("memtable entries"))?;

    for (internal_key, _) in entries.iter() {
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
    drop(entries);

    range_tombstone_modified_after_key(state, key, read_sequence)
}

fn key_range_modified_after(
    state: &KeyspaceState,
    range: &KeyRange,
    read_sequence: Sequence,
) -> Result<bool> {
    // A range read is invalidated by any newer point record inside the range or
    // any newer range tombstone whose bounds overlap the range read.
    let entries = state
        .entries
        .read()
        .map_err(|_| lock_poisoned("memtable entries"))?;

    for (internal_key, _) in entries.iter() {
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
    drop(entries);

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
    let entries = state
        .entries
        .read()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let mut items = Vec::new();
    let mut decided_user_key: Option<Vec<u8>> = None;

    for (internal_key, value) in entries.iter() {
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
                if !range_tombstone_covers(
                    state,
                    user_key,
                    internal_key.sequence(),
                    internal_key.batch_index(),
                    read_sequence,
                )? {
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

fn range_tombstone_covers(
    state: &KeyspaceState,
    key: &[u8],
    point_sequence: Sequence,
    point_batch_index: u32,
    read_sequence: Sequence,
) -> Result<bool> {
    let range_tombstones = state
        .range_tombstones
        .read()
        .map_err(|_| lock_poisoned("range tombstones"))?;

    Ok(range_tombstones.iter().any(|tombstone| {
        tombstone.covers_visible_point(key, point_sequence, point_batch_index, read_sequence)
    }))
}

fn range_tombstone_modified_after_key(
    state: &KeyspaceState,
    key: &[u8],
    read_sequence: Sequence,
) -> Result<bool> {
    let range_tombstones = state
        .range_tombstones
        .read()
        .map_err(|_| lock_poisoned("range tombstones"))?;

    Ok(range_tombstones.iter().any(|tombstone| {
        tombstone.sequence > read_sequence && key_is_in_range(key, &tombstone.range)
    }))
}

fn range_tombstone_modified_after_range(
    state: &KeyspaceState,
    range: &KeyRange,
    read_sequence: Sequence,
) -> Result<bool> {
    let range_tombstones = state
        .range_tombstones
        .read()
        .map_err(|_| lock_poisoned("range tombstones"))?;

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
