#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use super::BrowserStorageBackend;
use super::{
    Arc, BucketOptions, DurabilityMode, Error, ManifestState, ManifestStore, ManifestStoreBackend,
    NativeFileBackend, ObjectClient, ObjectManifestStore, PathBuf, PreparedManifestPublish,
    PublishOutcome, Result, Sequence, TableId, TableProperties, decode_manifest,
    publish_manifest_with_backend, publish_manifest_with_backend_async,
    read_manifest_bytes_with_backend, read_manifest_bytes_with_backend_async,
};

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
        writer_epoch: u64,
    ) -> Result<Self> {
        let object = ObjectManifestStore::open(client, key, writer_epoch).await?;
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

    /// Claim this writer's fencing epoch by force-publishing the current
    /// object-store manifest (stamping the epoch immediately on open, so a
    /// displaced prior owner is fenced before our first edit). Errors
    /// [`Error::Fenced`] if a newer owner already exists, or for non-object
    /// backends.
    pub(crate) async fn claim_object_epoch_async(&mut self) -> Result<()> {
        let mut object = self.clone_object_manifest()?;
        object.claim_writer_epoch().await?;
        self.install_object_manifest(object)
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

    /// Removes a bucket and its table list, marking the bucket's blob files for
    /// deletion at `pending_deletion_sequence`. The caller retires the bucket's
    /// table files separately (they are refcount-guarded). Errors if the bucket
    /// does not exist.
    pub fn drop_bucket(
        &mut self,
        name: &str,
        pending_blob_deletions: Vec<u64>,
        pending_deletion_sequence: Sequence,
    ) -> Result<()> {
        if !self.state.buckets.contains_key(name) {
            return Err(Error::invalid_options(
                "cannot drop a bucket that does not exist",
            ));
        }
        let mut next_state = self.state.clone();
        next_state.buckets.remove(name);
        next_state.tables.remove(name);
        for file_id in pending_blob_deletions {
            next_state
                .pending_blob_deletions
                .entry(file_id)
                .or_insert(pending_deletion_sequence);
        }
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

    pub(crate) fn create_checkpoint(&mut self, name: String, sequence: Sequence) -> Result<()> {
        let mut next_state = self.state.clone();
        if next_state
            .checkpoints
            .insert(name.clone(), sequence)
            .is_some()
        {
            return Err(Error::CheckpointAlreadyExists { name });
        }
        self.publish_next_state(next_state)?.published_or_err()
    }

    #[allow(dead_code)]
    pub(crate) async fn create_checkpoint_async(
        &mut self,
        name: String,
        sequence: Sequence,
    ) -> Result<()> {
        self.commit_edit_async(|state| {
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

    pub(crate) fn delete_checkpoint(&mut self, name: String) -> Result<()> {
        let mut next_state = self.state.clone();
        if next_state.checkpoints.remove(&name).is_none() {
            return Err(Error::CheckpointNotFound { name });
        }
        self.publish_next_state(next_state)?.published_or_err()
    }

    #[allow(dead_code)]
    pub(crate) async fn delete_checkpoint_async(&mut self, name: String) -> Result<()> {
        self.commit_edit_async(|state| {
            let mut next_state = state.clone();
            if next_state.checkpoints.remove(&name).is_none() {
                return Err(Error::CheckpointNotFound { name: name.clone() });
            }
            Ok(Some(next_state))
        })
        .await
    }

    pub(crate) fn checkpoint_sequence(&self, name: &str) -> Option<Sequence> {
        self.state.checkpoints.get(name).copied()
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
    pub(crate) fn prepare_drop_bucket_publish(
        &self,
        name: &str,
        pending_blob_deletions: Vec<u64>,
        pending_deletion_sequence: Sequence,
    ) -> Result<Option<PreparedManifestPublish>> {
        if !self.state.buckets.contains_key(name) {
            return Err(Error::invalid_options(
                "cannot drop a bucket that does not exist",
            ));
        }
        let mut next_state = self.state.clone();
        next_state.buckets.remove(name);
        next_state.tables.remove(name);
        for file_id in pending_blob_deletions {
            next_state
                .pending_blob_deletions
                .entry(file_id)
                .or_insert(pending_deletion_sequence);
        }
        Ok(Some(PreparedManifestPublish {
            path: self.path.clone(),
            storage: self.storage.clone(),
            base_state: self.state.clone(),
            next_state,
        }))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn prepare_create_checkpoint_publish(
        &self,
        name: String,
        sequence: Sequence,
    ) -> Result<PreparedManifestPublish> {
        let mut next_state = self.state.clone();
        if next_state
            .checkpoints
            .insert(name.clone(), sequence)
            .is_some()
        {
            return Err(Error::CheckpointAlreadyExists { name });
        }
        Ok(PreparedManifestPublish {
            path: self.path.clone(),
            storage: self.storage.clone(),
            base_state: self.state.clone(),
            next_state,
        })
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn prepare_delete_checkpoint_publish(
        &self,
        name: String,
    ) -> Result<PreparedManifestPublish> {
        let mut next_state = self.state.clone();
        if next_state.checkpoints.remove(&name).is_none() {
            return Err(Error::CheckpointNotFound { name });
        }
        Ok(PreparedManifestPublish {
            path: self.path.clone(),
            storage: self.storage.clone(),
            base_state: self.state.clone(),
            next_state,
        })
    }

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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
