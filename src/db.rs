use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::{
    blob::ValueRef,
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    keyspace::{Keyspace, KeyspaceName},
    options::{DbOptions, KeyspaceOptions, StorageMode, WriteOptions},
    snapshot::Snapshot,
    stats::DbStats,
    transaction::{Transaction, TransactionOptions},
    types::{CommitInfo, KeyRange, Sequence},
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
}

impl KeyspaceState {
    fn new(options: KeyspaceOptions) -> Self {
        Self {
            options,
            entries: RwLock::new(BTreeMap::new()),
        }
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
                drop(options);
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

        for (batch_index, (operation, state)) in operations.into_iter().zip(states).enumerate() {
            let batch_index = u32::try_from(batch_index).map_err(|_| Error::InvalidOptions {
                message: "write batch operation count exceeds u32::MAX".to_owned(),
            })?;

            apply_point_operation(&state, operation, sequence, batch_index)?;
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
                ValueKind::Put => match value.as_ref().and_then(ValueRef::inline_bytes) {
                    Some(bytes) => Ok(Some(bytes.to_vec())),
                    None => Err(Error::unsupported("blob values are not implemented yet")),
                },
                ValueKind::PointDelete => Ok(None),
                ValueKind::RangeDelete => continue,
            };
        }

        Ok(None)
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
            if matches!(operation, BatchOperation::RemoveRange { .. }) {
                return Err(Error::unsupported("range deletes are not implemented yet"));
            }

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

fn apply_point_operation(
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
        BatchOperation::RemoveRange { .. } => {
            return Err(Error::unsupported("range deletes are not implemented yet"));
        }
    }

    Ok(())
}

fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}
