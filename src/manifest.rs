use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    codec::CodecId,
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    limits,
    object_store::{ETag, ObjectClient, Precondition, PutIf},
    options::{
        BlobLevelMergePolicy, BucketOptions, CompressionProfile, DurabilityMode, FilterDepthCurve,
        FilterPolicy, IndexSearchPolicy, PrefixFilterPolicy,
    },
    prefix::PrefixExtractor,
    storage::{
        BlockingStorageManifestPublishBackend, BlockingStorageManifestReadBackend,
        NativeFileBackend, StorageManifestPublishBackend, StorageManifestReadBackend,
        StorageObjectId, StorageObjectKind,
    },
    table::{TableBlobReference, TableId, TableLevel, TableProperties},
    types::Sequence,
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::storage::BrowserStorageBackend;

pub const MANIFEST_FILE_NAME: &str = "MANIFEST";
const MANIFEST_MAGIC: u32 = 0x5452_4d46;
// Reset to 1 for the first published storage contract — the crate has no users
// yet, so each incompatible pre-1.0 layout gets one exact integer version.
// Version 2 adds durable file-id and bucket-generation high-water marks.
const MANIFEST_VERSION: u16 = 2;
const HEADER_LEN: usize = 14;
// The lower bound for one table entry: fixed fields plus two empty byte fields.
// Decoding uses this to reject impossible counts before reserving memory.
const MIN_TABLE_PROPERTY_BYTES: usize = 45;
const MANIFEST_EDIT_MAX_ATTEMPTS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    wal_replay_floor: Sequence,
    buckets: BTreeMap<String, BucketOptions>,
    bucket_generations: BTreeMap<String, u64>,
    tables: BTreeMap<String, Vec<TableProperties>>,
    pending_blob_deletions: BTreeMap<u64, Sequence>,
    checkpoints: BTreeMap<String, Sequence>,
    /// Exclusive high-water mark for every table/blob file identifier that has
    /// been durably reserved. It advances before the corresponding objects are
    /// written, so a crash may leave a gap but can never cause identifier reuse.
    next_file_id: u64,
    /// Exclusive high-water mark for logical bucket incarnations. Dropping and
    /// recreating the same name consumes a new generation permanently.
    next_bucket_generation: u64,
    /// The fencing epoch of the writer that last published this manifest. Used
    /// only by the object-store backend (the filesystem backend uses a `LOCK`
    /// file for mutual exclusion and always leaves this 0); a publish observing a
    /// higher epoch than the publisher holds is fenced. See
    /// [`ObjectManifestStore`].
    writer_epoch: u64,
}

impl ManifestState {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            wal_replay_floor: Sequence::ZERO,
            buckets: BTreeMap::new(),
            bucket_generations: BTreeMap::new(),
            tables: BTreeMap::new(),
            pending_blob_deletions: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            next_file_id: 1,
            next_bucket_generation: 1,
            writer_epoch: 0,
        }
    }

    #[must_use]
    pub const fn wal_replay_floor(&self) -> Sequence {
        self.wal_replay_floor
    }

    #[must_use]
    pub fn buckets(&self) -> &BTreeMap<String, BucketOptions> {
        &self.buckets
    }

    pub(crate) fn bucket_generation(&self, name: &str) -> Option<u64> {
        self.bucket_generations.get(name).copied()
    }

    #[must_use]
    pub fn tables(&self) -> &BTreeMap<String, Vec<TableProperties>> {
        &self.tables
    }

    #[must_use]
    pub fn pending_blob_deletions(&self) -> &BTreeMap<u64, Sequence> {
        &self.pending_blob_deletions
    }

    #[must_use]
    pub fn checkpoints(&self) -> &BTreeMap<String, Sequence> {
        &self.checkpoints
    }

    fn derived_next_file_id(&self) -> Result<u64> {
        let highest = self
            .tables
            .values()
            .flat_map(|tables| {
                tables.iter().flat_map(|properties| {
                    std::iter::once(properties.id.get())
                        .chain(properties.blob_file_ids().iter().copied())
                })
            })
            .chain(self.pending_blob_deletions.keys().copied())
            .max()
            .unwrap_or(0);

        highest.checked_add(1).ok_or_else(|| Error::Corruption {
            message: "table id counter overflow".to_owned(),
        })
    }

    fn validate_next_file_id(&self) -> Result<()> {
        let minimum = self.derived_next_file_id()?;
        if self.next_file_id < minimum {
            return Err(Error::Corruption {
                message: format!(
                    "manifest file-id high-water mark {} is below live-object minimum {minimum}",
                    self.next_file_id
                ),
            });
        }
        Ok(())
    }

    fn validate_bucket_generations(&self) -> Result<()> {
        if self.bucket_generations.len() != self.buckets.len()
            || self
                .buckets
                .keys()
                .any(|name| !self.bucket_generations.contains_key(name))
        {
            return Err(Error::Corruption {
                message: "manifest bucket-generation map does not match bucket map".to_owned(),
            });
        }
        let mut generations = BTreeSet::new();
        let highest = self.bucket_generations.values().copied().max().unwrap_or(0);
        if self
            .bucket_generations
            .values()
            .any(|generation| *generation == 0 || !generations.insert(*generation))
            || self.next_bucket_generation <= highest
        {
            return Err(Error::Corruption {
                message:
                    "manifest bucket generations are zero, duplicated, or above their high-water mark"
                        .to_owned(),
            });
        }
        if self.tables.len() != self.buckets.len()
            || self
                .buckets
                .keys()
                .any(|name| !self.tables.contains_key(name))
        {
            return Err(Error::Corruption {
                message: "manifest table-bucket map does not match bucket map".to_owned(),
            });
        }
        let mut table_ids = BTreeSet::new();
        for properties in self.tables.values().flatten() {
            if properties.id.get() == 0 || !table_ids.insert(properties.id.get()) {
                return Err(Error::Corruption {
                    message: format!(
                        "manifest contains invalid or duplicate table id {}",
                        properties.id.get()
                    ),
                });
            }
            for file_id in properties.blob_file_ids() {
                if *file_id == 0 {
                    return Err(Error::Corruption {
                        message: "manifest contains zero live blob file id".to_owned(),
                    });
                }
            }
        }
        for file_id in self.pending_blob_deletions.keys() {
            if *file_id == 0 {
                return Err(Error::Corruption {
                    message: "manifest contains zero pending blob file id".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn insert_new_bucket(&mut self, name: String, options: BucketOptions) -> Result<()> {
        let generation = self.next_bucket_generation;
        self.next_bucket_generation =
            generation.checked_add(1).ok_or_else(|| Error::Corruption {
                message: "bucket-generation high-water mark overflow".to_owned(),
            })?;
        self.bucket_generations.insert(name.clone(), generation);
        self.buckets.insert(name.clone(), options);
        self.tables.entry(name).or_default();
        Ok(())
    }

    fn remove_bucket(&mut self, name: &str) {
        self.buckets.remove(name);
        self.bucket_generations.remove(name);
        self.tables.remove(name);
    }
}

/// A durably committed, single-use range of table/blob file identifiers.
///
/// The manifest high-water mark has already advanced when this value is
/// returned. Dropping unused identifiers is safe; they intentionally become
/// permanent gaps rather than being recycled after failures.
#[derive(Debug)]
pub(crate) struct FileIdReservation {
    next: u64,
    end_exclusive: u64,
}

impl FileIdReservation {
    fn new(start: u64, end_exclusive: u64) -> Self {
        Self {
            next: start,
            end_exclusive,
        }
    }

    pub(crate) fn next_table_id(&mut self) -> Result<TableId> {
        let id = self.next_file_id()?;
        Ok(TableId(id))
    }

    pub(crate) fn next_file_id(&mut self) -> Result<u64> {
        if self.next >= self.end_exclusive {
            return Err(Error::Corruption {
                message: "file-id reservation exhausted".to_owned(),
            });
        }
        let id = self.next;
        self.next = self.next.checked_add(1).ok_or_else(|| Error::Corruption {
            message: "file-id reservation counter overflow".to_owned(),
        })?;
        Ok(id)
    }
}

impl Default for ManifestState {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug)]
pub struct ManifestStore {
    path: PathBuf,
    state: ManifestState,
    storage: ManifestStoreBackend,
}

#[derive(Debug, Clone)]
enum ManifestStoreBackend {
    Native(NativeFileBackend),
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Browser(BrowserStorageBackend),
    /// Object storage: publishing is a conditional-PUT CAS via
    /// [`ObjectManifestStore`] (async only). Constructed by
    /// `ManifestStore::open_object_store_async`.
    ObjectStore(ObjectManifestStore<Arc<dyn ObjectClient>>),
}

/// Outcome of attempting to publish a new manifest state at the durable cutover
/// point.
///
/// Filesystem publish is temp-write + atomic `rename`: it cannot lose a race, so
/// it always reports [`PublishOutcome::Published`]. The conflict-aware variant
/// exists for the object-storage substrate, whose publish is a conditional-write
/// CAS that can lose to another writer; it then reports
/// [`PublishOutcome::Conflict`] carrying the manifest that is now current, so the
/// caller can rebase its edit onto the winner and retry.
#[derive(Debug)]
pub(crate) enum PublishOutcome {
    /// The new state is now the durable manifest.
    Published,
    /// The namespace points at the new manifest, but syncing the parent
    /// directory failed. Referenced objects must be retained and the database
    /// handle must stop accepting work.
    PublishedDurabilityUnknown {
        /// Durability confirmation failure returned to the caller.
        error: Error,
    },
    /// Another writer published first. `current` is the winning manifest state,
    /// so the caller can rebase its edit and retry. Constructed by
    /// [`ObjectManifestStore::try_publish`] (object-storage CAS); the filesystem
    /// path never produces it.
    Conflict {
        // Read by the object-storage rebase-and-retry loop (wired in a later 2c
        // slice); `published_or_err` and the filesystem path ignore it.
        #[allow(dead_code)]
        current: ManifestState,
    },
}

impl PublishOutcome {
    /// Collapse to `Result<()>`, mapping a lost CAS race to a conflict error.
    ///
    /// Used by publish sites that do not (yet) implement rebase-and-retry. On the
    /// filesystem path this is always `Ok(())` because publish never conflicts;
    /// the object-storage substrate (slice 2c) drives [`ObjectManifestStore`] in
    /// an actual retry loop instead of collapsing the conflict to an error.
    fn published_or_err(self) -> Result<()> {
        match self {
            Self::Published => Ok(()),
            Self::PublishedDurabilityUnknown { error } => Err(error),
            Self::Conflict { .. } => Err(Error::Conflict {
                message: "manifest publish lost a concurrent CAS race".to_owned(),
            }),
        }
    }
}

/// Conflict-aware manifest publishing over an object store — the durable commit
/// point for the object-storage substrate (slice 2c).
///
/// The manifest object is the single source of truth. Publishing a new state is
/// a conditional PUT ([`ObjectClient::put_if`]): `If-None-Match` to create the
/// first manifest, `If-Match <etag>` to advance an existing one. Losing the CAS
/// means a concurrent writer published first; rather than clobber them,
/// [`Self::try_publish`] refreshes the cached state + `ETag` from the store and
/// reports [`PublishOutcome::Conflict`] carrying the winning state, so the caller
/// can rebase its edit and retry. This is where the conflict-aware result
/// introduced in slice 2b ① is finally constructed.
///
/// Unlike [`ManifestStore`], the manifest *state machine* (validation, edit
/// shapes) is not duplicated here: this owns only the read / encode / CAS-publish
/// of `ManifestState` bytes. A later slice wires it into the open path + the
/// object-storage substrate as the manifest backend.
// Held by `ManifestStoreBackend::ObjectStore`; the object-store open path that
// constructs it lands in 2c-4c. `Debug` is hand-written because the backend uses
// it over `Arc<dyn ObjectClient>`, which is not `Debug`.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct ObjectManifestStore<C: ObjectClient> {
    client: C,
    key: String,
    /// `ETag` of the manifest object we last observed, or `None` when it does
    /// not exist yet (the first publish creates it with `If-None-Match`).
    etag: Option<ETag>,
    state: ManifestState,
    /// This writer's fencing epoch (from the writer lease). Stamped into every
    /// publish; a publish observing a higher epoch in the current manifest is
    /// fenced.
    writer_epoch: u64,
}

impl<C: ObjectClient> std::fmt::Debug for ObjectManifestStore<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectManifestStore")
            .field("key", &self.key)
            .field("etag", &self.etag)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
impl<C: ObjectClient> ObjectManifestStore<C> {
    /// Open by reading the current manifest object (if any) and its `ETag`,
    /// holding `writer_epoch` (this writer's fencing token) for publishes.
    pub(crate) async fn open(client: C, key: impl Into<String>, writer_epoch: u64) -> Result<Self> {
        let key = key.into();
        let (state, etag) = Self::read_current(&client, &key).await?;
        Ok(Self {
            client,
            key,
            etag,
            state,
            writer_epoch,
        })
    }

    /// The most recently observed manifest state (after `open` or a publish).
    pub(crate) fn state(&self) -> &ManifestState {
        &self.state
    }

    async fn read_current(client: &C, key: &str) -> Result<(ManifestState, Option<ETag>)> {
        match client.head(key).await? {
            None => Ok((ManifestState::empty(), None)),
            Some(meta) => {
                let max_manifest_bytes = limits::MAX_MANIFEST_PAYLOAD_BYTES
                    .checked_add(HEADER_LEN)
                    .ok_or_else(|| Error::Corruption {
                        message: "manifest size limit overflow".to_owned(),
                    })?;
                if meta.size > max_manifest_bytes as u64 {
                    return Err(Error::Corruption {
                        message: format!(
                            "manifest object {key} length {} exceeds maximum {}",
                            meta.size, max_manifest_bytes
                        ),
                    });
                }
                let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
                    message: format!("manifest object {key} vanished between head and get"),
                })?;
                limits::ensure_corruption_len(
                    bytes.len(),
                    max_manifest_bytes,
                    "manifest object length",
                )?;
                Ok((decode_manifest(&bytes)?, Some(meta.etag)))
            }
        }
    }

    /// Attempt one conditional publish of `next`.
    ///
    /// On success, advance the cached state + `ETag` and return
    /// [`PublishOutcome::Published`]. On a lost CAS, refresh the cached state +
    /// `ETag` from the store (so the caller sees the winning state) and return
    /// [`PublishOutcome::Conflict`] without advancing past it.
    pub(crate) async fn try_publish(&mut self, mut next: ManifestState) -> Result<PublishOutcome> {
        // Fencing: if the manifest we are about to overwrite was last published by
        // a writer holding a higher epoch than ours, a newer owner has taken over
        // and we must stop — never retry into a clobber.
        if self.state.writer_epoch > self.writer_epoch {
            return Err(Error::Fenced {
                held_epoch: self.writer_epoch,
                current_epoch: self.state.writer_epoch,
            });
        }
        // Stamp our epoch so a stale prior owner is fenced on its next publish.
        next.writer_epoch = self.writer_epoch;
        let bytes = encode_manifest_bytes(&next)?;
        let precondition = match &self.etag {
            Some(etag) => Precondition::IfMatch(etag.clone()),
            None => Precondition::IfNoneMatch,
        };
        let base = self.state.clone();
        let publish = self.client.put_if(&self.key, bytes, precondition).await;
        match publish {
            Ok(PutIf::Stored { etag }) => {
                self.state = next;
                self.etag = Some(etag);
                Ok(PublishOutcome::Published)
            }
            Ok(PutIf::PreconditionFailed { .. }) => {
                let (current, etag) = Self::read_current(&self.client, &self.key).await?;
                self.state = current.clone();
                self.etag = etag;
                Ok(PublishOutcome::Conflict { current })
            }
            Err(error) => {
                // A conditional PUT can reach durable storage even when its
                // response is lost. Reconcile against the exact intended state;
                // if another edit already followed ours, return Conflict and
                // let the idempotent edit rebase onto that newer state.
                if let Ok((current, etag)) = Self::read_current(&self.client, &self.key).await {
                    self.state = current.clone();
                    self.etag = etag;
                    if current == next {
                        return Ok(PublishOutcome::Published);
                    }
                    if current == base {
                        return Err(error);
                    }
                    return Ok(PublishOutcome::Conflict { current });
                }
                Err(error)
            }
        }
    }

    /// Apply a manifest edit and CAS-publish it, retrying against the winning
    /// state on conflict. `edit` returns the next state, or `None` for a no-op.
    /// This owns `self`, so the caller can run it without holding any external
    /// lock across the await (the Send-safe path for the database's manifest).
    async fn commit_edit(
        &mut self,
        edit: impl Fn(&ManifestState) -> Result<Option<ManifestState>>,
    ) -> Result<()> {
        for _ in 0..MANIFEST_EDIT_MAX_ATTEMPTS {
            let Some(next_state) = edit(&self.state)? else {
                return Ok(());
            };
            match self.try_publish(next_state).await? {
                PublishOutcome::Published => return Ok(()),
                PublishOutcome::PublishedDurabilityUnknown { error } => return Err(error),
                // `try_publish` refreshed `self.state` to the winner; rebase.
                PublishOutcome::Conflict { .. } => {}
            }
        }
        Err(Error::Conflict {
            message: format!(
                "manifest edit exceeded {MANIFEST_EDIT_MAX_ATTEMPTS} concurrent CAS retries"
            ),
        })
    }

    pub(crate) async fn reserve_file_ids(&mut self, count: usize) -> Result<FileIdReservation> {
        let count = u64::try_from(count)
            .map_err(|_| Error::invalid_options("file-id reservation count exceeds u64::MAX"))?;
        for _ in 0..MANIFEST_EDIT_MAX_ATTEMPTS {
            self.state.validate_next_file_id()?;
            let start = self.state.next_file_id;
            let end_exclusive = start.checked_add(count).ok_or_else(|| Error::Corruption {
                message: "file-id high-water mark overflow".to_owned(),
            })?;
            if count == 0 {
                return Ok(FileIdReservation::new(start, end_exclusive));
            }
            let mut next_state = self.state.clone();
            next_state.next_file_id = end_exclusive;
            match self.try_publish(next_state).await? {
                PublishOutcome::Published => {
                    return Ok(FileIdReservation::new(start, end_exclusive));
                }
                PublishOutcome::PublishedDurabilityUnknown { error } => return Err(error),
                PublishOutcome::Conflict { .. } => {}
            }
        }
        Err(Error::Conflict {
            message: format!(
                "file-id reservation exceeded {MANIFEST_EDIT_MAX_ATTEMPTS} concurrent CAS retries"
            ),
        })
    }

    /// Force-publish the current state so this writer's fencing epoch is stamped
    /// into the manifest immediately — closing the window where a just-displaced
    /// prior owner could still publish before our first real edit. Fence-checked
    /// (errors [`Error::Fenced`] if a newer owner already published).
    pub(crate) async fn claim_writer_epoch(&mut self) -> Result<()> {
        self.commit_edit(|state| Ok(Some(state.clone()))).await
    }

    /// Create a bucket (idempotent), CAS-published with rebase-retry.
    pub(crate) async fn create_bucket(
        &mut self,
        name: String,
        options: BucketOptions,
    ) -> Result<()> {
        self.commit_edit(|state| {
            if let Some(existing) = state.buckets.get(&name) {
                if existing == &options {
                    return Ok(None);
                }
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            let mut next_state = state.clone();
            next_state.insert_new_bucket(name.clone(), options.clone())?;
            Ok(Some(next_state))
        })
        .await
    }

    /// Drop a bucket and its table list, CAS-published with rebase-retry. The
    /// bucket's now-unreferenced immutable table and blob objects are retained
    /// until remote reader retirement can be proven.
    pub(crate) async fn drop_bucket(&mut self, name: String) -> Result<()> {
        self.commit_edit(|state| {
            if !state.buckets.contains_key(&name) {
                return Err(Error::invalid_options(
                    "cannot drop a bucket that does not exist",
                ));
            }
            let mut next_state = state.clone();
            next_state.remove_bucket(&name);
            Ok(Some(next_state))
        })
        .await
    }

    /// Create a named checkpoint pin, CAS-published with rebase-retry.
    pub(crate) async fn create_checkpoint(
        &mut self,
        name: String,
        sequence: Sequence,
    ) -> Result<()> {
        self.commit_edit(|state| {
            let mut next_state = state.clone();
            if next_state
                .checkpoints
                .insert(name.clone(), sequence)
                .is_some()
            {
                return Err(Error::CheckpointAlreadyExists { name: name.clone() });
            }
            Ok(Some(next_state))
        })
        .await
    }

    /// Delete a named checkpoint pin, CAS-published with rebase-retry.
    pub(crate) async fn delete_checkpoint(&mut self, name: String) -> Result<()> {
        self.commit_edit(|state| {
            let mut next_state = state.clone();
            if next_state.checkpoints.remove(&name).is_none() {
                return Err(Error::CheckpointNotFound { name: name.clone() });
            }
            Ok(Some(next_state))
        })
        .await
    }

    /// Add flushed tables to their buckets, CAS-published with rebase-retry.
    pub(crate) async fn add_tables(
        &mut self,
        tables: Vec<(String, TableProperties)>,
        wal_replay_floor: Sequence,
    ) -> Result<()> {
        self.commit_edit(|state| {
            for (bucket, _) in &tables {
                if !state.buckets.contains_key(bucket) {
                    return Err(Error::Corruption {
                        message: format!("table references missing bucket: {bucket}"),
                    });
                }
            }
            let mut next_state = state.clone();
            for (bucket, properties) in &tables {
                let bucket_tables = next_state.tables.entry(bucket.clone()).or_default();
                if let Some(existing) = bucket_tables
                    .iter()
                    .find(|existing| existing.id == properties.id)
                {
                    if existing != properties {
                        return Err(Error::Corruption {
                            message: format!(
                                "table id {} has conflicting manifest properties",
                                properties.id.get()
                            ),
                        });
                    }
                } else {
                    bucket_tables.push(properties.clone());
                }
            }
            next_state.wal_replay_floor = wal_replay_floor;
            Ok((next_state != *state).then_some(next_state))
        })
        .await
    }

    /// Replace compaction-input tables with their outputs and mark obsolete blob
    /// files pending deletion, CAS-published with rebase-retry.
    pub(crate) async fn replace_tables_batch_and_mark_blob_deletions(
        &mut self,
        replacements: Vec<(String, Vec<TableId>, Vec<TableProperties>)>,
        pending_blob_deletions: Vec<u64>,
        pending_deletion_sequence: Sequence,
    ) -> Result<()> {
        self.commit_edit(|state| {
            for (bucket, removed_table_ids, replacement_tables) in &replacements {
                let tables = state.tables.get(bucket).ok_or_else(|| Error::Corruption {
                    message: format!("compaction references missing bucket: {bucket}"),
                })?;
                let present_inputs = removed_table_ids
                    .iter()
                    .filter(|table_id| tables.iter().any(|table| table.id == **table_id))
                    .count();
                let outputs_match = replacement_tables.iter().all(|replacement| {
                    tables
                        .iter()
                        .any(|table| table.id == replacement.id && table == replacement)
                });
                if present_inputs == 0 && outputs_match {
                    continue;
                }
                if present_inputs != removed_table_ids.len() {
                    return Err(Error::Corruption {
                        message: format!(
                            "compaction manifest edit for bucket {bucket} is partially applied"
                        ),
                    });
                }
            }

            let mut next_state = state.clone();
            for (bucket, removed_table_ids, replacement_tables) in &replacements {
                let tables =
                    next_state
                        .tables
                        .get_mut(bucket)
                        .ok_or_else(|| Error::Corruption {
                            message: format!("manifest is missing table list for bucket: {bucket}"),
                        })?;
                let already_applied = removed_table_ids
                    .iter()
                    .all(|table_id| !tables.iter().any(|table| table.id == *table_id))
                    && replacement_tables.iter().all(|replacement| {
                        tables
                            .iter()
                            .any(|table| table.id == replacement.id && table == replacement)
                    });
                if already_applied {
                    continue;
                }
                tables.retain(|properties| !removed_table_ids.contains(&properties.id));
                for replacement in replacement_tables {
                    if let Some(existing) =
                        tables.iter().find(|existing| existing.id == replacement.id)
                    {
                        if existing != replacement {
                            return Err(Error::Corruption {
                                message: format!(
                                    "replacement table id {} has conflicting properties",
                                    replacement.id.get()
                                ),
                            });
                        }
                    } else {
                        tables.push(replacement.clone());
                    }
                }
            }
            for file_id in &pending_blob_deletions {
                next_state
                    .pending_blob_deletions
                    .entry(*file_id)
                    .or_insert(pending_deletion_sequence);
            }
            Ok((next_state != *state).then_some(next_state))
        })
        .await
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub(crate) struct PreparedManifestPublish {
    path: PathBuf,
    storage: ManifestStoreBackend,
    base_state: ManifestState,
    next_state: ManifestState,
}

mod format;
mod store;

pub(crate) use format::*;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl PreparedManifestPublish {
    pub(crate) async fn publish_async(&self) -> Result<()> {
        let outcome = match &self.storage {
            ManifestStoreBackend::Native(native_storage) => {
                publish_manifest_with_backend_async(
                    native_storage,
                    &self.path,
                    &self.next_state,
                    native_manifest_publish_durability(),
                )
                .await?
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            ManifestStoreBackend::Browser(storage) => {
                publish_manifest_with_backend_async(
                    storage,
                    &self.path,
                    &self.next_state,
                    DurabilityMode::Flush,
                )
                .await?
            }
            ManifestStoreBackend::ObjectStore(_) => {
                return Err(Error::unsupported_backend(
                    "object-store manifest publish does not use prepared publish",
                ));
            }
        };
        outcome.published_or_err()
    }
}

#[must_use]
pub fn manifest_path(db_path: &Path) -> PathBuf {
    db_path.join(MANIFEST_FILE_NAME)
}

#[cfg(test)]
mod tests;
