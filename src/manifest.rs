use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    codec::CodecId,
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    object_store::{ETag, ObjectClient, Precondition, PutIf},
    options::{
        BlobLevelMergePolicy, BucketOptions, CompressionProfile, DurabilityMode, FilterPolicy,
        IndexSearchPolicy, PrefixFilterPolicy,
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
const MANIFEST_VERSION: u16 = 8;
const MIN_SUPPORTED_MANIFEST_VERSION: u16 = 8;
const HEADER_LEN: usize = 14;
// The lower bound for one table entry: fixed fields plus two empty byte fields.
// Decoding uses this to reject impossible counts before reserving memory.
const MIN_TABLE_PROPERTY_BYTES: usize = 45;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    wal_replay_floor: Sequence,
    buckets: BTreeMap<String, BucketOptions>,
    tables: BTreeMap<String, Vec<TableProperties>>,
    pending_blob_deletions: BTreeMap<u64, Sequence>,
}

impl ManifestState {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            wal_replay_floor: Sequence::ZERO,
            buckets: BTreeMap::new(),
            tables: BTreeMap::new(),
            pending_blob_deletions: BTreeMap::new(),
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

    #[must_use]
    pub fn tables(&self) -> &BTreeMap<String, Vec<TableProperties>> {
        &self.tables
    }

    #[must_use]
    pub fn pending_blob_deletions(&self) -> &BTreeMap<u64, Sequence> {
        &self.pending_blob_deletions
    }

    pub fn next_table_id(&self) -> Result<TableId> {
        let highest = self
            .tables
            .values()
            .flat_map(|tables| tables.iter().map(|properties| properties.id.get()))
            .max()
            .unwrap_or(0);

        highest
            .checked_add(1)
            .map(TableId)
            .ok_or_else(|| Error::Corruption {
                message: "table id counter overflow".to_owned(),
            })
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
    /// Open by reading the current manifest object (if any) and its `ETag`.
    pub(crate) async fn open(client: C, key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        let (state, etag) = Self::read_current(&client, &key).await?;
        Ok(Self {
            client,
            key,
            etag,
            state,
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
                let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
                    message: format!("manifest object {key} vanished between head and get"),
                })?;
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
    pub(crate) async fn try_publish(&mut self, next: ManifestState) -> Result<PublishOutcome> {
        let bytes = encode_manifest_bytes(&next)?;
        let precondition = match &self.etag {
            Some(etag) => Precondition::IfMatch(etag.clone()),
            None => Precondition::IfNoneMatch,
        };
        match self.client.put_if(&self.key, bytes, precondition).await? {
            PutIf::Stored { etag } => {
                self.state = next;
                self.etag = Some(etag);
                Ok(PublishOutcome::Published)
            }
            PutIf::PreconditionFailed { .. } => {
                let (current, etag) = Self::read_current(&self.client, &self.key).await?;
                self.state = current.clone();
                self.etag = etag;
                Ok(PublishOutcome::Conflict { current })
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
        loop {
            let Some(next_state) = edit(&self.state)? else {
                return Ok(());
            };
            match self.try_publish(next_state).await? {
                PublishOutcome::Published => return Ok(()),
                // `try_publish` refreshed `self.state` to the winner; rebase.
                PublishOutcome::Conflict { .. } => {}
            }
        }
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
            next_state.buckets.insert(name.clone(), options.clone());
            next_state.tables.entry(name.clone()).or_default();
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
                next_state
                    .tables
                    .entry(bucket.clone())
                    .or_default()
                    .push(properties.clone());
            }
            next_state.wal_replay_floor = wal_replay_floor;
            Ok(Some(next_state))
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

impl ManifestStore {
    #[cfg(test)]
    pub(crate) fn open_or_create(
        path: impl Into<PathBuf>,
        create_if_missing: bool,
    ) -> Result<Self> {
        Self::open_or_create_with_backend(path, create_if_missing, NativeFileBackend::new())
    }

    pub(crate) fn open_or_create_with_backend(
        path: impl Into<PathBuf>,
        create_if_missing: bool,
        native_storage: NativeFileBackend,
    ) -> Result<Self> {
        let path = path.into();
        let state = if let Some(bytes) = read_manifest_bytes_with_backend(&native_storage, &path)? {
            decode_manifest(&bytes)?
        } else if create_if_missing {
            let state = ManifestState::empty();
            publish_manifest_with_backend(&native_storage, &path, &state)?.published_or_err()?;
            state
        } else {
            ManifestState::empty()
        };

        Ok(Self {
            path,
            state,
            storage: ManifestStoreBackend::Native(native_storage),
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn open_or_create_with_backend_async(
        path: impl Into<PathBuf>,
        create_if_missing: bool,
        native_storage: NativeFileBackend,
    ) -> Result<Self> {
        let path = path.into();
        let state = if let Some(bytes) =
            read_manifest_bytes_with_backend_async(&native_storage, &path).await?
        {
            decode_manifest(&bytes)?
        } else if create_if_missing {
            let state = ManifestState::empty();
            publish_manifest_with_backend_async(
                &native_storage,
                &path,
                &state,
                DurabilityMode::SyncAll,
            )
            .await?
            .published_or_err()?;
            state
        } else {
            ManifestState::empty()
        };

        Ok(Self {
            path,
            state,
            storage: ManifestStoreBackend::Native(native_storage),
        })
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) async fn open_or_create_with_browser_backend_async(
        path: impl Into<PathBuf>,
        create_if_missing: bool,
        storage: BrowserStorageBackend,
    ) -> Result<Self> {
        let path = path.into();
        let state =
            if let Some(bytes) = read_manifest_bytes_with_backend_async(&storage, &path).await? {
                decode_manifest(&bytes)?
            } else if create_if_missing {
                let state = ManifestState::empty();
                publish_manifest_with_backend_async(&storage, &path, &state, DurabilityMode::Flush)
                    .await?
                    .published_or_err()?;
                state
            } else {
                ManifestState::empty()
            };

        Ok(Self {
            path,
            state,
            storage: ManifestStoreBackend::Browser(storage),
        })
    }

    /// Open a manifest backed by object storage (async only).
    ///
    /// Reads the current manifest object (if any) and its `ETag` via
    /// [`ObjectManifestStore`]; subsequent publishes are conditional-PUT CAS, and
    /// the mutating `*_async` methods retry against the winning state on conflict.
    #[allow(dead_code)] // constructed by the object-store open path in 2c-4c
    pub(crate) async fn open_object_store_async(
        client: Arc<dyn ObjectClient>,
        key: impl Into<String>,
    ) -> Result<Self> {
        let object = ObjectManifestStore::open(client, key).await?;
        Ok(Self {
            // Unused for the object store (the key lives in `ObjectManifestStore`);
            // publishing never touches `self.path` on this backend.
            path: PathBuf::new(),
            state: object.state().clone(),
            storage: ManifestStoreBackend::ObjectStore(object),
        })
    }

    /// Clone the object-store manifest handle so a caller can run a CAS publish
    /// on the owned clone without holding the database's manifest mutex across
    /// the await (then `install_object_manifest` writes the result back). Errors
    /// for non-object-store backends.
    pub(crate) fn clone_object_manifest(
        &self,
    ) -> Result<ObjectManifestStore<Arc<dyn ObjectClient>>> {
        match &self.storage {
            ManifestStoreBackend::ObjectStore(object) => Ok(object.clone()),
            ManifestStoreBackend::Native(_) => Err(Error::unsupported_backend(
                "manifest backend is not object storage",
            )),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            ManifestStoreBackend::Browser(_) => Err(Error::unsupported_backend(
                "manifest backend is not object storage",
            )),
        }
    }

    /// Write back an object-store manifest handle after a CAS publish, syncing
    /// the cached state. Errors for non-object-store backends.
    pub(crate) fn install_object_manifest(
        &mut self,
        object: ObjectManifestStore<Arc<dyn ObjectClient>>,
    ) -> Result<()> {
        match &mut self.storage {
            ManifestStoreBackend::ObjectStore(slot) => {
                self.state = object.state().clone();
                *slot = object;
                Ok(())
            }
            ManifestStoreBackend::Native(_) => Err(Error::unsupported_backend(
                "manifest backend is not object storage",
            )),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            ManifestStoreBackend::Browser(_) => Err(Error::unsupported_backend(
                "manifest backend is not object storage",
            )),
        }
    }

    #[must_use]
    pub const fn state(&self) -> &ManifestState {
        &self.state
    }

    pub fn create_bucket(&mut self, name: String, options: BucketOptions) -> Result<()> {
        if let Some(existing) = self.state.buckets.get(&name) {
            if existing == &options {
                return Ok(());
            }
            return Err(Error::invalid_options(
                "existing bucket options do not match requested options",
            ));
        }

        let mut next_state = self.state.clone();
        next_state.buckets.insert(name.clone(), options);
        next_state.tables.entry(name).or_default();
        self.publish_next_state(next_state)?.published_or_err()
    }

    #[allow(dead_code)]
    pub(crate) async fn create_bucket_async(
        &mut self,
        name: String,
        options: BucketOptions,
    ) -> Result<()> {
        self.commit_edit_async(|state| {
            if let Some(existing) = state.buckets.get(&name) {
                if existing == &options {
                    return Ok(None);
                }
                return Err(Error::invalid_options(
                    "existing bucket options do not match requested options",
                ));
            }
            let mut next_state = state.clone();
            next_state.buckets.insert(name.clone(), options.clone());
            next_state.tables.entry(name.clone()).or_default();
            Ok(Some(next_state))
        })
        .await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn prepare_create_bucket_publish(
        &self,
        name: String,
        options: BucketOptions,
    ) -> Result<Option<PreparedManifestPublish>> {
        if let Some(existing) = self.state.buckets.get(&name) {
            if existing == &options {
                return Ok(None);
            }
            return Err(Error::invalid_options(
                "existing bucket options do not match requested options",
            ));
        }

        let mut next_state = self.state.clone();
        next_state.buckets.insert(name.clone(), options);
        next_state.tables.entry(name).or_default();
        Ok(Some(PreparedManifestPublish {
            path: self.path.clone(),
            storage: self.storage.clone(),
            base_state: self.state.clone(),
            next_state,
        }))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn install_prepared_publish(
        &mut self,
        prepared: PreparedManifestPublish,
    ) -> Result<()> {
        if self.state != prepared.base_state {
            return Err(Error::Corruption {
                message: "manifest changed while async publish was pending".to_owned(),
            });
        }
        self.state = prepared.next_state;
        Ok(())
    }

    pub fn next_table_id(&self) -> Result<TableId> {
        self.state.next_table_id()
    }

    pub fn add_tables(
        &mut self,
        tables: Vec<(String, TableProperties)>,
        wal_replay_floor: Sequence,
    ) -> Result<()> {
        for (bucket, _) in &tables {
            if !self.state.buckets.contains_key(bucket) {
                return Err(Error::Corruption {
                    message: format!("table references missing bucket: {bucket}"),
                });
            }
        }

        let mut next_state = self.state.clone();
        for (bucket, properties) in tables {
            next_state
                .tables
                .entry(bucket)
                .or_default()
                .push(properties);
        }
        next_state.wal_replay_floor = wal_replay_floor;
        self.publish_next_state(next_state)?.published_or_err()
    }

    #[allow(dead_code)]
    pub(crate) async fn add_tables_async(
        &mut self,
        tables: Vec<(String, TableProperties)>,
        wal_replay_floor: Sequence,
    ) -> Result<()> {
        self.commit_edit_async(|state| {
            for (bucket, _) in &tables {
                if !state.buckets.contains_key(bucket) {
                    return Err(Error::Corruption {
                        message: format!("table references missing bucket: {bucket}"),
                    });
                }
            }
            let mut next_state = state.clone();
            for (bucket, properties) in &tables {
                next_state
                    .tables
                    .entry(bucket.clone())
                    .or_default()
                    .push(properties.clone());
            }
            next_state.wal_replay_floor = wal_replay_floor;
            Ok(Some(next_state))
        })
        .await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(dead_code)]
    pub(crate) fn prepare_add_tables_publish(
        &self,
        tables: Vec<(String, TableProperties)>,
        wal_replay_floor: Sequence,
    ) -> Result<PreparedManifestPublish> {
        for (bucket, _) in &tables {
            if !self.state.buckets.contains_key(bucket) {
                return Err(Error::Corruption {
                    message: format!("table references missing bucket: {bucket}"),
                });
            }
        }

        let mut next_state = self.state.clone();
        for (bucket, properties) in tables {
            next_state
                .tables
                .entry(bucket)
                .or_default()
                .push(properties);
        }
        next_state.wal_replay_floor = wal_replay_floor;
        Ok(PreparedManifestPublish {
            path: self.path.clone(),
            storage: self.storage.clone(),
            base_state: self.state.clone(),
            next_state,
        })
    }

    #[cfg(test)]
    pub(crate) fn replace_tables(
        &mut self,
        bucket: &str,
        removed_table_ids: &[TableId],
        replacement: TableProperties,
    ) -> Result<()> {
        self.replace_tables_batch(vec![(
            bucket.to_owned(),
            removed_table_ids.to_vec(),
            vec![replacement],
        )])
    }

    #[cfg(test)]
    fn replace_tables_batch(
        &mut self,
        replacements: Vec<(String, Vec<TableId>, Vec<TableProperties>)>,
    ) -> Result<()> {
        self.replace_tables_batch_and_mark_blob_deletions(replacements, Vec::new(), Sequence::ZERO)
    }

    pub fn replace_tables_batch_and_mark_blob_deletions(
        &mut self,
        replacements: Vec<(String, Vec<TableId>, Vec<TableProperties>)>,
        pending_blob_deletions: Vec<u64>,
        pending_deletion_sequence: Sequence,
    ) -> Result<()> {
        // Validate the whole batch before changing in-memory manifest state.
        // That keeps multi-bucket compaction from publishing a partial edit.
        for (bucket, removed_table_ids, _) in &replacements {
            if !self.state.buckets.contains_key(bucket) {
                return Err(Error::Corruption {
                    message: format!("compaction references missing bucket: {bucket}"),
                });
            }

            let tables = self
                .state
                .tables
                .get(bucket)
                .ok_or_else(|| Error::Corruption {
                    message: format!("manifest is missing table list for bucket: {bucket}"),
                })?;
            for table_id in removed_table_ids {
                if !tables.iter().any(|properties| properties.id == *table_id) {
                    return Err(Error::Corruption {
                        message: format!("compaction input table is missing: {}", table_id.get()),
                    });
                }
            }
        }

        let mut next_state = self.state.clone();
        for (bucket, removed_table_ids, replacements) in replacements {
            let tables = next_state
                .tables
                .get_mut(&bucket)
                .ok_or_else(|| Error::Corruption {
                    message: format!("manifest is missing table list for bucket: {bucket}"),
                })?;
            tables.retain(|properties| !removed_table_ids.contains(&properties.id));
            for replacement in replacements {
                tables.push(replacement);
            }
        }
        for file_id in pending_blob_deletions {
            next_state
                .pending_blob_deletions
                .entry(file_id)
                .or_insert(pending_deletion_sequence);
        }

        self.publish_next_state(next_state)?.published_or_err()
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn prepare_replace_tables_batch_publish(
        &self,
        replacements: Vec<(String, Vec<TableId>, Vec<TableProperties>)>,
        pending_blob_deletions: Vec<u64>,
        pending_deletion_sequence: Sequence,
    ) -> Result<PreparedManifestPublish> {
        for (bucket, removed_table_ids, _) in &replacements {
            if !self.state.buckets.contains_key(bucket) {
                return Err(Error::Corruption {
                    message: format!("compaction references missing bucket: {bucket}"),
                });
            }

            let tables = self
                .state
                .tables
                .get(bucket)
                .ok_or_else(|| Error::Corruption {
                    message: format!("manifest is missing table list for bucket: {bucket}"),
                })?;
            for table_id in removed_table_ids {
                if !tables.iter().any(|properties| properties.id == *table_id) {
                    return Err(Error::Corruption {
                        message: format!("compaction input table is missing: {}", table_id.get()),
                    });
                }
            }
        }

        let mut next_state = self.state.clone();
        for (bucket, removed_table_ids, replacements) in replacements {
            let tables = next_state
                .tables
                .get_mut(&bucket)
                .ok_or_else(|| Error::Corruption {
                    message: format!("manifest is missing table list for bucket: {bucket}"),
                })?;
            tables.retain(|properties| !removed_table_ids.contains(&properties.id));
            for replacement in replacements {
                tables.push(replacement);
            }
        }
        for file_id in pending_blob_deletions {
            next_state
                .pending_blob_deletions
                .entry(file_id)
                .or_insert(pending_deletion_sequence);
        }

        Ok(PreparedManifestPublish {
            path: self.path.clone(),
            storage: self.storage.clone(),
            base_state: self.state.clone(),
            next_state,
        })
    }

    pub fn clear_pending_blob_deletions(&mut self, file_ids: &[u64]) -> Result<()> {
        if file_ids.is_empty() {
            return Ok(());
        }

        let mut next_state = self.state.clone();
        for file_id in file_ids {
            next_state.pending_blob_deletions.remove(file_id);
        }
        self.publish_next_state(next_state)?.published_or_err()
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn prepare_clear_pending_blob_deletions_publish(
        &self,
        file_ids: &[u64],
    ) -> Option<PreparedManifestPublish> {
        if file_ids.is_empty() {
            return None;
        }

        let mut next_state = self.state.clone();
        for file_id in file_ids {
            next_state.pending_blob_deletions.remove(file_id);
        }
        Some(PreparedManifestPublish {
            path: self.path.clone(),
            storage: self.storage.clone(),
            base_state: self.state.clone(),
            next_state,
        })
    }

    fn publish_next_state(&mut self, next_state: ManifestState) -> Result<PublishOutcome> {
        // Manifest publish is the durable cutover point. Keep the in-memory
        // state unchanged until storage publish succeeds, so a failed create,
        // flush, or compaction cannot make later operations believe an edit was
        // committed when the durable manifest never advanced.
        let outcome = match &self.storage {
            ManifestStoreBackend::Native(native_storage) => {
                publish_manifest_with_backend(native_storage, &self.path, &next_state)?
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            ManifestStoreBackend::Browser(_) => {
                return Err(Error::unsupported_backend(
                    "browser manifest publish requires async API",
                ));
            }
            ManifestStoreBackend::ObjectStore(_) => {
                return Err(Error::unsupported_backend(
                    "object-store manifest publish requires async API",
                ));
            }
        };
        // Only advance once the durable manifest actually cut over. A lost CAS
        // race (object-storage substrate) leaves the state untouched so the
        // caller can rebase onto the winner and retry.
        if matches!(outcome, PublishOutcome::Published) {
            self.state = next_state;
        }
        Ok(outcome)
    }

    async fn publish_next_state_async(
        &mut self,
        next_state: ManifestState,
    ) -> Result<PublishOutcome> {
        match &mut self.storage {
            ManifestStoreBackend::Native(native_storage) => {
                let outcome = publish_manifest_with_backend_async(
                    native_storage,
                    &self.path,
                    &next_state,
                    DurabilityMode::SyncAll,
                )
                .await?;
                if matches!(outcome, PublishOutcome::Published) {
                    self.state = next_state;
                }
                Ok(outcome)
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            ManifestStoreBackend::Browser(storage) => {
                let outcome = publish_manifest_with_backend_async(
                    storage,
                    &self.path,
                    &next_state,
                    DurabilityMode::Flush,
                )
                .await?;
                if matches!(outcome, PublishOutcome::Published) {
                    self.state = next_state;
                }
                Ok(outcome)
            }
            ManifestStoreBackend::ObjectStore(object) => {
                // Delegate to the conflict-aware CAS primitive; after it returns,
                // its cached state is authoritative (the published state on
                // `Published`, or the winning state on `Conflict`).
                let outcome = object.try_publish(next_state).await?;
                self.state = object.state().clone();
                Ok(outcome)
            }
        }
    }

    /// Apply a manifest edit and publish it, retrying on a lost object-store CAS.
    ///
    /// `edit` receives the current state and returns the next state, or `None`
    /// when the edit is already satisfied (a no-op — e.g. creating a bucket that
    /// already exists). On an object-store conflict, `publish_next_state_async`
    /// has already refreshed `self.state` to the winning manifest, so the loop
    /// re-runs `edit` to rebase its validation + mutation onto it and retries.
    /// The filesystem/memory publish never conflicts, so the loop runs once.
    async fn commit_edit_async(
        &mut self,
        edit: impl Fn(&ManifestState) -> Result<Option<ManifestState>>,
    ) -> Result<()> {
        loop {
            let Some(next_state) = edit(&self.state)? else {
                return Ok(());
            };
            match self.publish_next_state_async(next_state).await? {
                PublishOutcome::Published => return Ok(()),
                PublishOutcome::Conflict { .. } => {}
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl PreparedManifestPublish {
    pub(crate) async fn publish_async(&self) -> Result<()> {
        let outcome = match &self.storage {
            ManifestStoreBackend::Native(native_storage) => {
                publish_manifest_with_backend_async(
                    native_storage,
                    &self.path,
                    &self.next_state,
                    DurabilityMode::SyncAll,
                )
                .await?
            }
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
pub(crate) fn read_manifest(path: &Path) -> Result<ManifestState> {
    let bytes = read_manifest_bytes(path)?.ok_or_else(|| {
        Error::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("manifest {} not found", path.display()),
        ))
    })?;
    decode_manifest(&bytes)
}

#[allow(dead_code)]
pub(crate) async fn read_manifest_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<ManifestState>
where
    B: StorageManifestReadBackend,
{
    let bytes = read_manifest_bytes_with_backend_async(backend, path)
        .await?
        .ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("manifest {} not found", path.display()),
            ))
        })?;
    decode_manifest(&bytes)
}

#[cfg(test)]
fn read_manifest_bytes(path: &Path) -> Result<Option<Arc<[u8]>>> {
    let backend = NativeFileBackend::new();
    read_manifest_bytes_with_backend(&backend, path)
}

fn read_manifest_bytes_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<Option<Arc<[u8]>>> {
    let object = manifest_storage_object(path);
    backend.read_current_manifest_blocking(object)
}

async fn read_manifest_bytes_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<Option<Arc<[u8]>>>
where
    B: StorageManifestReadBackend,
{
    let object = manifest_storage_object(path);
    backend.read_current_manifest(object).await
}

fn publish_manifest_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    state: &ManifestState,
) -> Result<PublishOutcome> {
    let bytes = encode_manifest_bytes(state)?;
    let object = manifest_storage_object(path);
    backend.publish_manifest_blocking(object, bytes, DurabilityMode::SyncAll)?;
    // Temp-write + atomic rename cannot lose a CAS race, so the filesystem
    // manifest always advances.
    Ok(PublishOutcome::Published)
}

async fn publish_manifest_with_backend_async<B>(
    backend: &B,
    path: &Path,
    state: &ManifestState,
    durability: DurabilityMode,
) -> Result<PublishOutcome>
where
    B: StorageManifestPublishBackend,
{
    let bytes = encode_manifest_bytes(state)?;
    let object = manifest_storage_object(path);
    backend.publish_manifest(object, bytes, durability).await?;
    Ok(PublishOutcome::Published)
}

fn encode_manifest_bytes(state: &ManifestState) -> Result<Arc<[u8]>> {
    let payload = encode_state(state)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("manifest payload exceeds u32::MAX"))?;
    let payload_checksum = checksum(&payload);
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());

    bytes.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    bytes.extend_from_slice(&payload);

    Ok(bytes.into())
}

fn manifest_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Manifest, path)
}

fn encode_state(state: &ManifestState) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let bucket_count = u32::try_from(state.buckets.len())
        .map_err(|_| Error::invalid_options("too many buckets for manifest"))?;

    put_u64(&mut bytes, state.wal_replay_floor.get());
    put_u32(&mut bytes, bucket_count);
    for (name, options) in &state.buckets {
        put_bytes(&mut bytes, name.as_bytes())?;
        put_bucket_options(&mut bytes, options)?;
    }
    put_tables(&mut bytes, &state.tables)?;
    put_pending_blob_deletions(&mut bytes, &state.pending_blob_deletions)?;

    Ok(bytes)
}

fn decode_manifest(bytes: &[u8]) -> Result<ManifestState> {
    if bytes.len() < HEADER_LEN {
        return Err(invalid_manifest("short header"));
    }

    let magic = read_u32_at(bytes, 0)?;
    let version = read_u16_at(bytes, 4)?;
    let payload_len = read_u32_at(bytes, 6)? as usize;
    let payload_checksum = read_u32_at(bytes, 10)?;
    if magic != MANIFEST_MAGIC {
        return Err(Error::Corruption {
            message: "manifest magic mismatch".to_owned(),
        });
    }
    if !(MIN_SUPPORTED_MANIFEST_VERSION..=MANIFEST_VERSION).contains(&version) {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported manifest version {version}"),
        });
    }
    if bytes.len() != HEADER_LEN + payload_len {
        return Err(Error::Corruption {
            message: "manifest length mismatch".to_owned(),
        });
    }

    let payload = &bytes[HEADER_LEN..];
    if checksum(payload) != payload_checksum {
        return Err(Error::Corruption {
            message: "manifest checksum mismatch".to_owned(),
        });
    }

    decode_state(payload, version)
}

fn decode_state(payload: &[u8], version: u16) -> Result<ManifestState> {
    let mut cursor = Cursor::new(payload);
    let wal_replay_floor = Sequence::new(cursor.read_u64()?);
    let bucket_count = cursor.read_u32()? as usize;
    let mut buckets = BTreeMap::new();

    for _ in 0..bucket_count {
        let name =
            String::from_utf8(cursor.read_bytes()?.to_vec()).map_err(|_| Error::InvalidFormat {
                message: "manifest bucket name is not valid UTF-8".to_owned(),
            })?;
        let options = cursor.read_bucket_options(version)?;
        buckets.insert(name, options);
    }
    let tables = cursor.read_tables()?;
    let pending_blob_deletions = if version >= 5 {
        cursor.read_pending_blob_deletions()?
    } else {
        BTreeMap::new()
    };

    if !cursor.is_finished() {
        return Err(invalid_manifest("trailing payload bytes"));
    }

    Ok(ManifestState {
        wal_replay_floor,
        buckets,
        tables,
        pending_blob_deletions,
    })
}

fn put_bucket_options(bytes: &mut Vec<u8>, options: &BucketOptions) -> Result<()> {
    put_bool(bytes, options.allow_empty_keys);
    put_compression_profile(bytes, options.compression);
    put_usize(bytes, options.block_bytes)?;
    put_filter_policy(bytes, options.filter_policy);
    put_prefix_extractor(bytes, &options.prefix_extractor)?;
    put_prefix_filter_policy(bytes, options.prefix_filter_policy);
    put_index_search_policy(bytes, options.index_search_policy);
    put_usize(bytes, options.blob_threshold_bytes)?;
    put_blob_level_merge_policy(bytes, options.blob_level_merge_policy);
    Ok(())
}

fn put_bool(bytes: &mut Vec<u8>, value: bool) {
    put_u8(bytes, u8::from(value));
}

fn put_compression_profile(bytes: &mut Vec<u8>, value: CompressionProfile) {
    put_u8(
        bytes,
        match value {
            CompressionProfile::None => 0,
            CompressionProfile::Fast => 1,
        },
    );
}

fn put_filter_policy(bytes: &mut Vec<u8>, value: FilterPolicy) {
    match value {
        FilterPolicy::Disabled => put_u8(bytes, 0),
        FilterPolicy::Bloom { bits_per_key } => {
            put_u8(bytes, 1);
            put_u8(bytes, bits_per_key);
        }
    }
}

fn put_prefix_extractor(bytes: &mut Vec<u8>, value: &PrefixExtractor) -> Result<()> {
    match value {
        PrefixExtractor::FixedLen(len) => {
            put_u8(bytes, 0);
            put_usize(bytes, *len)?;
        }
        PrefixExtractor::Separator(separator) => {
            put_u8(bytes, 1);
            put_u8(bytes, *separator);
        }
        PrefixExtractor::Custom(name) => {
            put_u8(bytes, 2);
            put_bytes(bytes, name.as_bytes())?;
        }
        PrefixExtractor::Disabled => put_u8(bytes, 3),
    }
    Ok(())
}

fn put_prefix_filter_policy(bytes: &mut Vec<u8>, value: PrefixFilterPolicy) {
    match value {
        PrefixFilterPolicy::Disabled => put_u8(bytes, 0),
        PrefixFilterPolicy::Bloom { bits_per_prefix } => {
            put_u8(bytes, 1);
            put_u8(bytes, bits_per_prefix);
        }
    }
}

fn put_index_search_policy(bytes: &mut Vec<u8>, value: IndexSearchPolicy) {
    put_u8(
        bytes,
        match value {
            IndexSearchPolicy::Linear => 0,
            IndexSearchPolicy::Binary => 1,
            IndexSearchPolicy::Auto => 4,
        },
    );
}

fn put_blob_level_merge_policy(bytes: &mut Vec<u8>, value: BlobLevelMergePolicy) {
    put_u8(
        bytes,
        match value {
            BlobLevelMergePolicy::Disabled => 0,
            BlobLevelMergePolicy::Auto => 1,
            BlobLevelMergePolicy::Always => 2,
        },
    );
}

fn put_tables(bytes: &mut Vec<u8>, tables: &BTreeMap<String, Vec<TableProperties>>) -> Result<()> {
    let table_bucket_count = u32::try_from(tables.len())
        .map_err(|_| Error::invalid_options("too many table buckets for manifest"))?;
    put_u32(bytes, table_bucket_count);

    for (bucket, table_list) in tables {
        put_bytes(bytes, bucket.as_bytes())?;
        let table_count = u32::try_from(table_list.len())
            .map_err(|_| Error::invalid_options("too many tables for manifest bucket"))?;
        put_u32(bytes, table_count);
        for properties in table_list {
            put_table_properties(bytes, properties)?;
        }
    }

    Ok(())
}

fn put_pending_blob_deletions(
    bytes: &mut Vec<u8>,
    pending_blob_deletions: &BTreeMap<u64, Sequence>,
) -> Result<()> {
    let count = u32::try_from(pending_blob_deletions.len())
        .map_err(|_| Error::invalid_options("too many pending blob deletions for manifest"))?;
    put_u32(bytes, count);
    for (file_id, sequence) in pending_blob_deletions {
        put_u64(bytes, *file_id);
        put_u64(bytes, sequence.get());
    }
    Ok(())
}

fn put_table_properties(bytes: &mut Vec<u8>, properties: &TableProperties) -> Result<()> {
    put_u64(bytes, properties.id.get());
    put_u32(bytes, properties.level.get());
    put_bytes(bytes, &properties.smallest_user_key)?;
    put_bytes(bytes, &properties.largest_user_key)?;
    put_u64(bytes, properties.smallest_sequence.get());
    put_u64(bytes, properties.largest_sequence.get());
    put_codec(bytes, properties.codec);
    put_u32(
        bytes,
        u32::try_from(properties.blob_file_ids.len())
            .map_err(|_| Error::invalid_options("too many blob file ids for table properties"))?,
    );
    for file_id in &properties.blob_file_ids {
        put_u64(bytes, *file_id);
    }
    put_u32(
        bytes,
        u32::try_from(properties.blob_references.len())
            .map_err(|_| Error::invalid_options("too many blob references for table properties"))?,
    );
    for reference in &properties.blob_references {
        put_u64(bytes, reference.file_id);
        put_u64(bytes, reference.referenced_bytes);
        put_u64(bytes, reference.referenced_record_count);
        put_internal_key(bytes, &reference.smallest_internal_key)?;
        put_internal_key(bytes, &reference.largest_internal_key)?;
    }
    Ok(())
}

fn put_internal_key(bytes: &mut Vec<u8>, internal_key: &InternalKey) -> Result<()> {
    put_bytes(bytes, internal_key.user_key())?;
    put_u64(bytes, internal_key.sequence().get());
    put_u8(
        bytes,
        match internal_key.kind() {
            ValueKind::Put => 1,
            ValueKind::PointDelete => 2,
            ValueKind::RangeDelete => 3,
        },
    );
    put_u32(bytes, internal_key.batch_index());
    Ok(())
}

fn put_codec(bytes: &mut Vec<u8>, codec: CodecId) {
    put_u8(
        bytes,
        match codec {
            CodecId::None => 0,
            CodecId::FastLz4Block => 1,
        },
    );
}

fn put_usize(bytes: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u64::try_from(value)
        .map_err(|_| Error::invalid_options("manifest usize field exceeds u64::MAX"))?;
    put_u64(bytes, value);
    Ok(())
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("manifest byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| invalid_manifest("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_manifest("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn checksum(bytes: &[u8]) -> u32 {
    crate::checksum::crc32c(bytes)
}

fn invalid_manifest(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid manifest: {message}"),
    }
}

struct Cursor<'payload> {
    payload: &'payload [u8],
    offset: usize,
}

impl<'payload> Cursor<'payload> {
    const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_manifest("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidFormat {
                message: format!("invalid manifest bool {value}"),
            }),
        }
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32_at(self.payload, self.offset)?;
        self.offset += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let value = self
            .payload
            .get(self.offset..self.offset + 8)
            .ok_or_else(|| invalid_manifest("short u64"))?;
        self.offset += 8;
        Ok(u64::from_le_bytes([
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ]))
    }

    fn read_usize(&mut self) -> Result<usize> {
        usize::try_from(self.read_u64()?).map_err(|_| Error::UnsupportedFormat {
            message: "manifest usize field does not fit this platform".to_owned(),
        })
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let value = self
            .payload
            .get(self.offset..self.offset + len)
            .ok_or_else(|| invalid_manifest("short bytes"))?;
        self.offset += len;
        Ok(value)
    }

    fn read_bucket_options(&mut self, version: u16) -> Result<BucketOptions> {
        Ok(BucketOptions {
            allow_empty_keys: self.read_bool()?,
            compression: self.read_compression_profile()?,
            block_bytes: self.read_usize()?,
            filter_policy: self.read_filter_policy()?,
            prefix_extractor: self.read_prefix_extractor()?,
            prefix_filter_policy: self.read_prefix_filter_policy()?,
            index_search_policy: self.read_index_search_policy()?,
            blob_threshold_bytes: self.read_usize()?,
            blob_level_merge_policy: if version >= 7 {
                self.read_blob_level_merge_policy()?
            } else if version >= 6 {
                if self.read_bool()? {
                    BlobLevelMergePolicy::Always
                } else {
                    BlobLevelMergePolicy::Auto
                }
            } else {
                BlobLevelMergePolicy::Auto
            },
        })
    }

    fn read_tables(&mut self) -> Result<BTreeMap<String, Vec<TableProperties>>> {
        let table_bucket_count = self.read_u32()? as usize;
        let mut tables = BTreeMap::new();

        for _ in 0..table_bucket_count {
            let bucket = String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| {
                Error::InvalidFormat {
                    message: "manifest table bucket is not valid UTF-8".to_owned(),
                }
            })?;
            let table_count = self.read_u32()? as usize;
            if table_count > self.remaining_len() / MIN_TABLE_PROPERTY_BYTES {
                return Err(invalid_manifest("table count exceeds payload bytes"));
            }
            let mut table_list = Vec::with_capacity(table_count);
            for _ in 0..table_count {
                table_list.push(self.read_table_properties()?);
            }
            tables.insert(bucket, table_list);
        }

        Ok(tables)
    }

    fn read_pending_blob_deletions(&mut self) -> Result<BTreeMap<u64, Sequence>> {
        let pending_count = self.read_u32()? as usize;
        if pending_count > self.remaining_len() / 16 {
            return Err(invalid_manifest(
                "pending blob deletion count exceeds payload bytes",
            ));
        }

        let mut pending = BTreeMap::new();
        let mut previous = None;
        for _ in 0..pending_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_manifest("pending blob deletions are not sorted"));
            }
            let sequence = Sequence::new(self.read_u64()?);
            pending.insert(file_id, sequence);
            previous = Some(file_id);
        }
        Ok(pending)
    }

    fn read_table_properties(&mut self) -> Result<TableProperties> {
        Ok(TableProperties {
            id: TableId(self.read_u64()?),
            level: TableLevel(self.read_u32()?),
            smallest_user_key: self.read_bytes()?.to_vec(),
            largest_user_key: self.read_bytes()?.to_vec(),
            smallest_sequence: Sequence::new(self.read_u64()?),
            largest_sequence: Sequence::new(self.read_u64()?),
            codec: self.read_codec()?,
            blob_file_ids: self.read_blob_file_ids()?,
            blob_references: self.read_blob_references()?,
        })
    }

    fn read_blob_file_ids(&mut self) -> Result<Vec<u64>> {
        let file_id_count = self.read_u32()? as usize;
        if file_id_count > self.remaining_len() / 8 {
            return Err(invalid_manifest("blob file id count exceeds payload bytes"));
        }
        let mut file_ids = Vec::with_capacity(file_id_count);
        let mut previous = None;
        for _ in 0..file_id_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_manifest("blob file ids are not sorted"));
            }
            file_ids.push(file_id);
            previous = Some(file_id);
        }
        Ok(file_ids)
    }

    fn read_blob_references(&mut self) -> Result<Vec<TableBlobReference>> {
        let reference_count = self.read_u32()? as usize;
        if reference_count > self.remaining_len() / 58 {
            return Err(invalid_manifest(
                "blob reference count exceeds payload bytes",
            ));
        }

        let mut references = Vec::with_capacity(reference_count);
        let mut previous = None;
        for _ in 0..reference_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_manifest("blob references are not sorted"));
            }
            let referenced_bytes = self.read_u64()?;
            let referenced_record_count = self.read_u64()?;
            let smallest_internal_key = self.read_internal_key()?;
            let largest_internal_key = self.read_internal_key()?;
            if smallest_internal_key > largest_internal_key {
                return Err(invalid_manifest("blob reference key bounds are invalid"));
            }
            references.push(TableBlobReference {
                file_id,
                referenced_bytes,
                referenced_record_count,
                smallest_internal_key,
                largest_internal_key,
            });
            previous = Some(file_id);
        }
        Ok(references)
    }

    fn read_internal_key(&mut self) -> Result<InternalKey> {
        let user_key = self.read_bytes()?.to_vec();
        let sequence = Sequence::new(self.read_u64()?);
        let kind = self.read_value_kind()?;
        let batch_index = self.read_u32()?;
        Ok(InternalKey::new(user_key, sequence, kind, batch_index))
    }

    fn read_value_kind(&mut self) -> Result<ValueKind> {
        match self.read_u8()? {
            1 => Ok(ValueKind::Put),
            2 => Ok(ValueKind::PointDelete),
            3 => Ok(ValueKind::RangeDelete),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest internal value kind {tag}"),
            }),
        }
    }

    fn read_compression_profile(&mut self) -> Result<CompressionProfile> {
        match self.read_u8()? {
            0 => Ok(CompressionProfile::None),
            1 => Ok(CompressionProfile::Fast),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest compression profile {tag}"),
            }),
        }
    }

    fn read_filter_policy(&mut self) -> Result<FilterPolicy> {
        match self.read_u8()? {
            0 => Ok(FilterPolicy::Disabled),
            1 => Ok(FilterPolicy::Bloom {
                bits_per_key: self.read_u8()?,
            }),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest filter policy {tag}"),
            }),
        }
    }

    fn read_prefix_extractor(&mut self) -> Result<PrefixExtractor> {
        match self.read_u8()? {
            0 => Ok(PrefixExtractor::FixedLen(self.read_usize()?)),
            1 => Ok(PrefixExtractor::Separator(self.read_u8()?)),
            2 => {
                let name = String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| {
                    Error::InvalidFormat {
                        message: "manifest custom prefix extractor is not UTF-8".to_owned(),
                    }
                })?;
                Ok(PrefixExtractor::Custom(name))
            }
            3 => Ok(PrefixExtractor::Disabled),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest prefix extractor {tag}"),
            }),
        }
    }

    fn read_prefix_filter_policy(&mut self) -> Result<PrefixFilterPolicy> {
        match self.read_u8()? {
            0 => Ok(PrefixFilterPolicy::Disabled),
            1 => Ok(PrefixFilterPolicy::Bloom {
                bits_per_prefix: self.read_u8()?,
            }),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest prefix filter policy {tag}"),
            }),
        }
    }

    fn read_index_search_policy(&mut self) -> Result<IndexSearchPolicy> {
        match self.read_u8()? {
            0 => Ok(IndexSearchPolicy::Linear),
            1 => Ok(IndexSearchPolicy::Binary),
            2..=4 => Ok(IndexSearchPolicy::Auto),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest index search policy {tag}"),
            }),
        }
    }

    fn read_blob_level_merge_policy(&mut self) -> Result<BlobLevelMergePolicy> {
        match self.read_u8()? {
            0 => Ok(BlobLevelMergePolicy::Disabled),
            1 => Ok(BlobLevelMergePolicy::Auto),
            2 => Ok(BlobLevelMergePolicy::Always),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest blob level merge policy {tag}"),
            }),
        }
    }

    fn read_codec(&mut self) -> Result<CodecId> {
        match self.read_u8()? {
            0 => Ok(CodecId::None),
            1 => Ok(CodecId::FastLz4Block),
            tag => Err(Error::UnsupportedFormat {
                message: format!("unknown manifest table codec {tag}"),
            }),
        }
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }

    const fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        future::Future,
        path::PathBuf,
        task::{Context, Poll, Waker},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{MANIFEST_VERSION, ManifestStore, decode_state, manifest_path};
    use crate::{
        options::{
            BlobLevelMergePolicy, BucketOptions, CompressionProfile, FilterPolicy,
            IndexSearchPolicy, PrefixFilterPolicy,
        },
        prefix::PrefixExtractor,
        storage::NativeFileBackend,
    };

    #[test]
    fn manifest_decode_rejects_table_count_before_large_allocation() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_u64.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&u32::MAX.to_le_bytes());

        let error = decode_state(&payload, MANIFEST_VERSION)
            .expect_err("impossible table count should fail");
        assert!(
            error
                .to_string()
                .contains("table count exceeds payload bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn manifest_decode_accepts_previous_version_without_pending_blob_deletions() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_u64.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());

        let state = decode_state(&payload, 4).expect("v4 manifest decodes");
        assert!(state.buckets().is_empty());
        assert!(state.tables().is_empty());
        assert!(state.pending_blob_deletions().is_empty());
    }

    #[test]
    fn manifest_decode_v5_bucket_options_default_blob_level_merge_policy() {
        let mut payload = Vec::new();
        super::put_u64(&mut payload, 0);
        super::put_u32(&mut payload, 1);
        super::put_bytes(&mut payload, b"users").expect("bucket name encodes");
        super::put_bool(&mut payload, true);
        super::put_compression_profile(&mut payload, CompressionProfile::Fast);
        super::put_usize(&mut payload, 4096).expect("block size encodes");
        super::put_filter_policy(&mut payload, FilterPolicy::Bloom { bits_per_key: 12 });
        super::put_prefix_extractor(&mut payload, &PrefixExtractor::Separator(b':'))
            .expect("prefix extractor encodes");
        super::put_prefix_filter_policy(
            &mut payload,
            PrefixFilterPolicy::Bloom { bits_per_prefix: 8 },
        );
        super::put_index_search_policy(&mut payload, IndexSearchPolicy::Binary);
        super::put_usize(&mut payload, 128 * 1024).expect("threshold encodes");
        super::put_u32(&mut payload, 0);
        super::put_u32(&mut payload, 0);

        let state = decode_state(&payload, 5).expect("v5 manifest decodes");
        let options = state.buckets().get("users").expect("bucket options exist");
        assert_eq!(options.blob_level_merge_policy, BlobLevelMergePolicy::Auto);
        assert_eq!(options.blob_threshold_bytes, 128 * 1024);
    }

    #[test]
    fn manifest_decode_v6_bool_bucket_options_as_policy() {
        let mut payload = Vec::new();
        super::put_u64(&mut payload, 0);
        super::put_u32(&mut payload, 1);
        super::put_bytes(&mut payload, b"users").expect("bucket name encodes");
        super::put_bool(&mut payload, true);
        super::put_compression_profile(&mut payload, CompressionProfile::Fast);
        super::put_usize(&mut payload, 4096).expect("block size encodes");
        super::put_filter_policy(&mut payload, FilterPolicy::Disabled);
        super::put_prefix_extractor(&mut payload, &PrefixExtractor::Disabled)
            .expect("prefix extractor encodes");
        super::put_prefix_filter_policy(&mut payload, PrefixFilterPolicy::Disabled);
        super::put_index_search_policy(&mut payload, IndexSearchPolicy::Auto);
        super::put_usize(&mut payload, 128 * 1024).expect("threshold encodes");
        super::put_bool(&mut payload, true);
        super::put_u32(&mut payload, 0);
        super::put_u32(&mut payload, 0);

        let state = decode_state(&payload, 6).expect("v6 manifest decodes");
        let options = state.buckets().get("users").expect("bucket options exist");
        assert_eq!(
            options.blob_level_merge_policy,
            BlobLevelMergePolicy::Always
        );
    }

    #[test]
    fn manifest_decode_legacy_search_policy_tags_as_auto() {
        let mut payload = Vec::new();
        super::put_u64(&mut payload, 0);
        super::put_u32(&mut payload, 1);
        super::put_bytes(&mut payload, b"users").expect("bucket name encodes");
        super::put_bool(&mut payload, true);
        super::put_compression_profile(&mut payload, CompressionProfile::Fast);
        super::put_usize(&mut payload, 4096).expect("block size encodes");
        super::put_filter_policy(&mut payload, FilterPolicy::Disabled);
        super::put_prefix_extractor(&mut payload, &PrefixExtractor::Disabled)
            .expect("prefix extractor encodes");
        super::put_prefix_filter_policy(&mut payload, PrefixFilterPolicy::Disabled);
        super::put_u8(&mut payload, 2);
        super::put_usize(&mut payload, 128 * 1024).expect("threshold encodes");
        super::put_blob_level_merge_policy(&mut payload, BlobLevelMergePolicy::Auto);
        super::put_u32(&mut payload, 0);
        super::put_u32(&mut payload, 0);

        let state = decode_state(&payload, 7).expect("v7 manifest decodes");
        let options = state.buckets().get("users").expect("bucket options exist");
        assert_eq!(options.index_search_policy, IndexSearchPolicy::Auto);
    }

    #[test]
    fn manifest_state_stays_put_when_publish_fails() {
        let dir = temp_manifest_dir("publish-fails");
        fs::create_dir_all(&dir).expect("create manifest test dir");
        let path = manifest_path(&dir);
        let mut store = ManifestStore::open_or_create(path, true).expect("manifest opens");

        fs::remove_dir_all(&dir).expect("remove manifest parent to force publish failure");
        let error = store
            .create_bucket("users".to_owned(), BucketOptions::default())
            .expect_err("publish should fail");
        assert!(
            error.to_string().contains("io error"),
            "unexpected error: {error}"
        );
        assert!(
            !store.state().buckets().contains_key("users"),
            "failed publish must not advance in-memory manifest state"
        );
    }

    #[test]
    fn async_manifest_open_create_and_bucket_publish_round_trip() {
        let dir = temp_manifest_dir("async-round-trip");
        fs::create_dir_all(&dir).expect("create manifest test dir");
        let path = manifest_path(&dir);
        let mut store = poll_ready(ManifestStore::open_or_create_with_backend_async(
            path.clone(),
            true,
            NativeFileBackend::new(),
        ))
        .expect("manifest opens through async helper");

        poll_ready(store.create_bucket_async("users".to_owned(), BucketOptions::default()))
            .expect("bucket publishes through async helper");

        let reopened = poll_ready(ManifestStore::open_or_create_with_backend_async(
            path,
            false,
            NativeFileBackend::new(),
        ))
        .expect("manifest reopens through async helper");
        assert!(reopened.state().buckets().contains_key("users"));
        assert!(reopened.state().tables().contains_key("users"));
    }

    #[test]
    fn async_manifest_publish_failure_does_not_advance_state() {
        let dir = temp_manifest_dir("async-publish-fails");
        fs::create_dir_all(&dir).expect("create manifest test dir");
        let path = manifest_path(&dir);
        let mut store = poll_ready(ManifestStore::open_or_create_with_backend_async(
            path,
            true,
            NativeFileBackend::new(),
        ))
        .expect("manifest opens through async helper");

        fs::remove_dir_all(&dir).expect("remove manifest parent to force publish failure");
        let error =
            poll_ready(store.create_bucket_async("users".to_owned(), BucketOptions::default()))
                .expect_err("publish should fail");
        assert!(
            error.to_string().contains("io error"),
            "unexpected error: {error}"
        );
        assert!(
            !store.state().buckets().contains_key("users"),
            "failed publish must not advance in-memory manifest state"
        );
    }

    fn object_manifest_state(floor: u64) -> super::ManifestState {
        let mut state = super::ManifestState::empty();
        state.wal_replay_floor = crate::types::Sequence::new(floor);
        state
    }

    #[test]
    fn object_manifest_creates_then_advances_via_cas() {
        use crate::object_store::InMemoryObjectStore;

        let store = std::sync::Arc::new(InMemoryObjectStore::new());
        let mut manifest =
            poll_ready(super::ObjectManifestStore::open(store, "MANIFEST")).expect("open empty");
        assert_eq!(
            manifest.state().wal_replay_floor(),
            crate::types::Sequence::ZERO,
            "absent manifest opens empty"
        );

        // First publish creates the object (If-None-Match).
        assert!(matches!(
            poll_ready(manifest.try_publish(object_manifest_state(5))).expect("create"),
            super::PublishOutcome::Published
        ));
        assert_eq!(
            manifest.state().wal_replay_floor(),
            crate::types::Sequence::new(5)
        );

        // Second publish advances the existing object (If-Match).
        assert!(matches!(
            poll_ready(manifest.try_publish(object_manifest_state(9))).expect("advance"),
            super::PublishOutcome::Published
        ));
        assert_eq!(
            manifest.state().wal_replay_floor(),
            crate::types::Sequence::new(9)
        );
    }

    #[test]
    fn object_manifest_reports_conflict_then_rebases() {
        use crate::object_store::InMemoryObjectStore;

        let store = std::sync::Arc::new(InMemoryObjectStore::new());
        // Two writers that both observed the (empty) manifest.
        let mut writer_a = poll_ready(super::ObjectManifestStore::open(
            std::sync::Arc::clone(&store),
            "MANIFEST",
        ))
        .expect("open A");
        let mut writer_b = poll_ready(super::ObjectManifestStore::open(
            std::sync::Arc::clone(&store),
            "MANIFEST",
        ))
        .expect("open B");

        // A wins the create race.
        assert!(matches!(
            poll_ready(writer_a.try_publish(object_manifest_state(5))).expect("A creates"),
            super::PublishOutcome::Published
        ));

        // B's create loses the CAS; it learns A's winning state and does not
        // advance past it.
        match poll_ready(writer_b.try_publish(object_manifest_state(7))).expect("B conflicts") {
            super::PublishOutcome::Conflict { current } => {
                assert_eq!(current.wal_replay_floor(), crate::types::Sequence::new(5));
            }
            super::PublishOutcome::Published => panic!("B must lose the create race"),
        }
        assert_eq!(
            writer_b.state().wal_replay_floor(),
            crate::types::Sequence::new(5),
            "B refreshed to the winning state, ready to rebase"
        );

        // After rebasing onto A's state, B's If-Match publish now succeeds.
        assert!(matches!(
            poll_ready(writer_b.try_publish(object_manifest_state(7))).expect("B retries"),
            super::PublishOutcome::Published
        ));
        assert_eq!(
            writer_b.state().wal_replay_floor(),
            crate::types::Sequence::new(7)
        );
    }

    #[test]
    fn object_store_manifest_create_bucket_rebases_on_conflict() {
        use crate::object_store::{InMemoryObjectStore, ObjectClient};

        let client: std::sync::Arc<dyn ObjectClient> =
            std::sync::Arc::new(InMemoryObjectStore::new());
        let mut writer_a = poll_ready(ManifestStore::open_object_store_async(
            std::sync::Arc::clone(&client),
            "MANIFEST",
        ))
        .expect("open A");
        let mut writer_b = poll_ready(ManifestStore::open_object_store_async(
            std::sync::Arc::clone(&client),
            "MANIFEST",
        ))
        .expect("open B");

        // A creates "alpha" (first publish = If-None-Match create).
        poll_ready(writer_a.create_bucket_async("alpha".to_owned(), BucketOptions::default()))
            .expect("A creates alpha");
        assert!(writer_a.state().buckets().contains_key("alpha"));

        // B still believes the manifest is empty; creating "beta" loses the
        // initial CAS, rebases onto A's winning state (which has alpha), re-runs
        // the edit, and retries — ending with both buckets.
        poll_ready(writer_b.create_bucket_async("beta".to_owned(), BucketOptions::default()))
            .expect("B creates beta after rebase");
        assert!(
            writer_b.state().buckets().contains_key("alpha"),
            "B rebased onto A's winning state"
        );
        assert!(writer_b.state().buckets().contains_key("beta"));

        // A fresh open sees both — the durable manifest holds the merged result.
        let reopened =
            poll_ready(ManifestStore::open_object_store_async(client, "MANIFEST")).expect("reopen");
        assert!(reopened.state().buckets().contains_key("alpha"));
        assert!(reopened.state().buckets().contains_key("beta"));
    }

    #[test]
    fn object_store_manifest_create_bucket_is_idempotent() {
        use crate::object_store::{InMemoryObjectStore, ObjectClient};

        let client: std::sync::Arc<dyn ObjectClient> =
            std::sync::Arc::new(InMemoryObjectStore::new());
        let mut manifest =
            poll_ready(ManifestStore::open_object_store_async(client, "MANIFEST")).expect("open");
        poll_ready(manifest.create_bucket_async("alpha".to_owned(), BucketOptions::default()))
            .expect("create");
        // Re-creating with identical options is a no-op (the edit returns None).
        poll_ready(manifest.create_bucket_async("alpha".to_owned(), BucketOptions::default()))
            .expect("idempotent re-create");
        assert_eq!(manifest.state().buckets().len(), 1);
    }

    #[test]
    fn object_store_manifest_rejects_sync_publish() {
        use crate::object_store::{InMemoryObjectStore, ObjectClient};

        let client: std::sync::Arc<dyn ObjectClient> =
            std::sync::Arc::new(InMemoryObjectStore::new());
        let mut manifest =
            poll_ready(ManifestStore::open_object_store_async(client, "MANIFEST")).expect("open");
        // The sync API is unsupported for object storage (it cannot await a CAS).
        let error = manifest
            .create_bucket("alpha".to_owned(), BucketOptions::default())
            .expect_err("sync create must be rejected");
        assert!(
            error.to_string().contains("async API"),
            "unexpected error: {error}"
        );
    }

    fn poll_ready<T>(future: impl Future<Output = crate::Result<T>>) -> crate::Result<T> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("manifest storage future unexpectedly pending"),
        }
    }

    fn temp_manifest_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "trine-kv-manifest-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
