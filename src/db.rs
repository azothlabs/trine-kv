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
    options::{DbOptions, KeyspaceOptions, StorageMode, WriteOptions},
    snapshot::Snapshot,
    stats::DbStats,
    transaction::{Transaction, TransactionOptions},
    types::{CommitInfo, KeyRange, KeyValue, Sequence},
    write_batch::{BatchOperation, WriteBatch},
};

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
            StorageMode::Persistent { .. } => {
                Err(Error::unsupported("persistent open is not implemented yet"))
            }
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
            }),
        })
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

        let keyspace_options = {
            let mut keyspaces = self
                .inner
                .keyspaces
                .write()
                .map_err(|_| lock_poisoned("keyspace registry"))?;

            if let Some(state) = keyspaces.get(name.as_str()) {
                // Opening an existing keyspace with different options would
                // hide a manifest-level decision behind a harmless-looking
                // handle lookup. Keep that explicit from the start.
                if state.options != options {
                    return Err(Error::invalid_options(
                        "existing keyspace options do not match requested options",
                    ));
                }
                state.options.clone()
            } else {
                if self.inner.options.read_only {
                    return Err(Error::ReadOnly);
                }

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

    pub fn persist(&self, _mode: crate::options::DurabilityMode) -> Result<()> {
        self.ensure_open()?;

        match self.inner.options.storage_mode {
            StorageMode::InMemory => Ok(()),
            StorageMode::Persistent { .. } => Err(Error::unsupported(
                "persistent durability is not implemented yet",
            )),
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

    pub fn write(&self, batch: WriteBatch, _options: WriteOptions) -> Result<CommitInfo> {
        self.ensure_open()?;

        let operations = batch.into_operations();
        if operations.is_empty() {
            return Ok(CommitInfo::new(self.last_committed_sequence()));
        }

        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }

        // Check every batch-wide precondition before taking the writer lock or
        // touching memtables, so a rejected batch cannot leave partial state.
        validate_batch_len(operations.len())?;

        // The writer lock serializes commit sequence assignment and memtable
        // updates. Reads only take keyspace/table read locks and do not enter
        // this coordinator.
        let _writer = self
            .inner
            .writer
            .lock()
            .map_err(|_| lock_poisoned("writer coordinator"))?;
        let sequence = self
            .last_committed_sequence()
            .next()
            .ok_or_else(|| Error::Corruption {
                message: "sequence counter overflow".to_owned(),
            })?;
        let states = self.resolve_batch_keyspaces(&operations)?;

        let indexed_operations = operations
            .into_iter()
            .zip(states)
            .enumerate()
            .map(|(batch_index, (operation, state))| {
                let batch_index = u32::try_from(batch_index).map_err(|_| {
                    Error::invalid_options("write batch operation count exceeds u32::MAX")
                })?;
                Ok((batch_index, operation, state))
            })
            .collect::<Result<Vec<_>>>()?;

        for (batch_index, operation, state) in indexed_operations {
            apply_memtable_operation(&state, operation, sequence, batch_index)?;
        }

        self.inner
            .last_sequence
            .store(sequence.get(), Ordering::Release);
        Ok(CommitInfo::new(sequence))
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
