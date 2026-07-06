use super::{
    Arc, BlobLevelMergePolicy, CompactionReservation, Db, DurabilityMode, Error,
    HostStorageBackend, KeyRange, LsmCompactionOutput, MaintenanceBudget, MaintenanceOutcome,
    NamedCompactionInput, NamedCompactionOutput, NamedFlushInput, ObjectClient, ObjectStoreBackend,
    Path, PendingCompactionOutputs, Result, Sequence, StorageMode, StorageObjectDeleteBackend,
    StorageObjectId, StorageObjectKind, Table, blob, compaction_trigger_stat_deltas,
    is_level_layout_compaction_error, lock_poisoned, referenced_blob_file_ids_from_manifest,
    referenced_table_file_ids, should_rewrite_blob_indexes_for_compaction, table,
};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use super::{Ordering, shutdown_background_workers};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::{ReadVersion, storage::BrowserStorageBackend};

impl Db {
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
            && matches!(
                mode,
                DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
            )
        {
            return Err(Error::unsupported_durability(mode));
        }

        match &self.inner.options.storage_mode {
            StorageMode::InMemory => Ok(()),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.inner.substrate.persist_wal(mode),
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
    pub(in crate::db) fn browser_storage(&self) -> Result<BrowserStorageBackend> {
        self.inner
            .browser_storage
            .clone()
            .ok_or_else(|| Error::Corruption {
                message: "browser persistent database is missing storage backend".to_owned(),
            })
    }

    pub(in crate::db) fn object_storage(&self) -> Result<ObjectStoreBackend> {
        self.inner
            .object_storage
            .clone()
            .ok_or_else(|| Error::Corruption {
                message: "object-store database is missing storage backend".to_owned(),
            })
    }

    pub(in crate::db) fn object_wal_storage(&self) -> Result<ObjectStoreBackend> {
        self.inner
            .object_wal_storage
            .clone()
            .ok_or_else(|| Error::Corruption {
                message: "object-store database is missing WAL backend".to_owned(),
            })
    }

    /// The key prefix (as a `db_path`) under which this object-store database's
    /// keys live; empty for a bucket-root database.
    pub(in crate::db) fn object_store_db_path(&self) -> &Path {
        &self.inner.object_storage_prefix
    }

    /// Flush all immutable memtables to objects, publish them via the manifest
    /// CAS, then clean remote WAL objects covered by the new replay floor.
    ///
    /// Object-store manifest changes use a checkout / mutate / publish /
    /// install sequence. The async CAS runs on an owned manifest clone while
    /// the async manifest lock serializes publishers; the std manifest mutex is
    /// only held for short checkout and install steps.
    pub(in crate::db) async fn flush_object_store_async(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let db_path = self.object_store_db_path();
        let target_sequence = self.freeze_public_flush_target()?;
        while self.has_immutable_memtables_at_or_below(target_sequence)? {
            let outcome = self
                .run_flush_once_with_budget_object_store_async(
                    db_path,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                return Err(Error::runtime_busy("object-store flush is already active"));
            }
            if outcome.flushes == 0 {
                break;
            }
        }
        Ok(())
    }

    pub(in crate::db) async fn flush_object_store_with_budget_async(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let db_path = self.object_store_db_path();
        if !self.has_immutable_memtables()? {
            return Ok(MaintenanceOutcome::default());
        }
        self.run_flush_once_with_budget_object_store_async(db_path, budget)
            .await
    }

    pub(in crate::db) async fn run_flush_once_with_budget_object_store_async(
        &self,
        db_path: &Path,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        let Some(_flush_guard) = self.inner.maintenance.try_start_flush() else {
            return Ok(MaintenanceOutcome::busy_outcome());
        };

        let (flush_inputs, budget_exhausted) = self.collect_flush_inputs_with_budget(budget)?;
        let flush_count = flush_inputs.len();
        self.write_flush_inputs_object_store_async(db_path, &flush_inputs)
            .await?;
        let outcome = MaintenanceOutcome {
            flushes: flush_count,
            budget_exhausted: budget_exhausted && flush_count != 0,
            ..MaintenanceOutcome::default()
        };
        if outcome.budget_exhausted {
            self.record_maintenance_budget_exhaustion();
        }
        Ok(outcome)
    }

    pub(in crate::db) async fn write_flush_inputs_object_store_async(
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

        // No publish barrier here: object-store manifest publishes are serialized
        // by `object_manifest_async_lock` (held across the CAS await, Send-safe),
        // and `close` is a no-op for object storage, so there is no publish-vs-close
        // race to guard. Holding the barrier's std guard across the await would
        // make this future `!Send`.
        self.publish_flushed_tables_object_store_async(&written_tables, flush_sequence)
            .await?;
        Self::install_flushed_tables(flush_inputs, written_tables)
            .map_err(|error| self.close_after_durable_publish_error("flush", &error))?;
        self.inner
            .substrate
            .rewrite_wal_after_replay_floor_async(flush_sequence)
            .await?;
        Ok(())
    }

    pub(in crate::db) async fn publish_flushed_tables_object_store_async(
        &self,
        tables: &[(String, Arc<Table>)],
        flush_sequence: Sequence,
    ) -> Result<()> {
        let edits = tables
            .iter()
            .map(|(bucket, table)| (bucket.clone(), table.properties().clone()))
            .collect::<Vec<_>>();
        self.publish_object_manifest_add_tables(edits, flush_sequence)
            .await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn create_checkpoint_browser_async(
        &self,
        name: &str,
    ) -> Result<ReadVersion> {
        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
        let sequence = self.last_committed_sequence();
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
            manifest.prepare_create_checkpoint_publish(name.to_owned(), sequence)?
        };
        prepared_publish.publish_async().await?;
        self.install_prepared_manifest_after_durable_publish(
            "checkpoint creation",
            manifest,
            prepared_publish,
        )?;
        Ok(ReadVersion::from_sequence(sequence))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn delete_checkpoint_browser_async(&self, name: &str) -> Result<()> {
        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
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
            manifest.prepare_delete_checkpoint_publish(name.to_owned())?
        };
        prepared_publish.publish_async().await?;
        self.install_prepared_manifest_after_durable_publish(
            "checkpoint deletion",
            manifest,
            prepared_publish,
        )
    }

    /// Object-store manifest publishes run Send-safely as a checkout / mutate /
    /// write-back: serialize via the async lock (held by the caller for the whole
    /// sequence), clone the manifest handle out under a brief std lock, run the
    /// awaiting CAS on the owned clone, then write it back. The std mutex is never
    /// held across the await.
    pub(in crate::db) async fn checkout_object_manifest(
        &self,
    ) -> Result<(
        crate::manifest::ObjectManifestStore<Arc<dyn ObjectClient>>,
        futures::lock::MutexGuard<'_, ()>,
    )> {
        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "object-store database is missing manifest store".to_owned(),
            })?;
        let serialize = self.inner.object_manifest_async_lock.lock().await;
        let object = manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .clone_object_manifest()?;
        Ok((object, serialize))
    }

    pub(in crate::db) async fn publish_object_manifest_create_bucket(
        &self,
        name: String,
        options: crate::BucketOptions,
    ) -> Result<()> {
        let (mut object, _serialize) = self.checkout_object_manifest().await?;
        object.create_bucket(name, options).await?;
        self.install_object_manifest_after_durable_publish("bucket creation", object)
    }

    pub(in crate::db) async fn publish_object_manifest_drop_bucket(
        &self,
        name: String,
    ) -> Result<()> {
        let (mut object, _serialize) = self.checkout_object_manifest().await?;
        object.drop_bucket(name).await?;
        self.install_object_manifest_after_durable_publish("bucket drop", object)
    }

    pub(in crate::db) async fn publish_object_manifest_create_checkpoint(
        &self,
        name: String,
        sequence: Sequence,
    ) -> Result<()> {
        let (mut object, _serialize) = self.checkout_object_manifest().await?;
        object.create_checkpoint(name, sequence).await?;
        self.install_object_manifest_after_durable_publish("checkpoint creation", object)
    }

    pub(in crate::db) async fn publish_object_manifest_delete_checkpoint(
        &self,
        name: String,
    ) -> Result<()> {
        let (mut object, _serialize) = self.checkout_object_manifest().await?;
        object.delete_checkpoint(name).await?;
        self.install_object_manifest_after_durable_publish("checkpoint deletion", object)
    }

    pub(in crate::db) async fn publish_object_manifest_add_tables(
        &self,
        edits: Vec<(String, table::TableProperties)>,
        flush_sequence: Sequence,
    ) -> Result<()> {
        let (mut object, _serialize) = self.checkout_object_manifest().await?;
        object.add_tables(edits, flush_sequence).await?;
        self.install_object_manifest_after_durable_publish("flush", object)
    }

    pub(in crate::db) async fn publish_object_manifest_replace_tables(
        &self,
        edits: Vec<(String, Vec<table::TableId>, Vec<table::TableProperties>)>,
        obsolete_blob_ids: Vec<u64>,
        pending_deletion_sequence: Sequence,
    ) -> Result<()> {
        let (mut object, _serialize) = self.checkout_object_manifest().await?;
        object
            .replace_tables_batch_and_mark_blob_deletions(
                edits,
                obsolete_blob_ids,
                pending_deletion_sequence,
            )
            .await?;
        self.install_object_manifest_after_durable_publish("compaction", object)
    }

    pub(in crate::db) fn install_object_manifest(
        &self,
        object: crate::manifest::ObjectManifestStore<Arc<dyn ObjectClient>>,
    ) -> Result<()> {
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "object-store database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_object_manifest(object)
    }

    pub(in crate::db) fn install_object_manifest_after_durable_publish(
        &self,
        operation: &'static str,
        object: crate::manifest::ObjectManifestStore<Arc<dyn ObjectClient>>,
    ) -> Result<()> {
        self.install_object_manifest(object)
            .map_err(|error| self.close_after_durable_publish_error(operation, &error))
    }

    /// Delete object-store table/blob objects the published manifest does not
    /// reference — orphans left by a flush that wrote objects but failed before
    /// the manifest CAS published them. Returns the number of objects removed.
    ///
    /// Serialized with flush via the maintenance flush guard so it never removes
    /// an object an in-flight flush just wrote (cross-process safety comes from
    /// the single-writer lease). The manifest mutex is held only to snapshot the
    /// referenced ids, not across the deletes.
    pub(in crate::db) async fn cleanup_object_store_orphans_async(&self) -> Result<usize> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let Some(_flush_guard) = self.inner.maintenance.try_start_flush() else {
            return Err(Error::runtime_busy(
                "object-store orphan GC cannot run during a flush",
            ));
        };
        let backend = self.object_storage()?;
        let db_path = self.object_store_db_path();

        let (referenced_tables, referenced_blobs) = {
            let manifest = self
                .inner
                .manifest
                .as_ref()
                .ok_or_else(|| Error::Corruption {
                    message: "object-store database is missing manifest store".to_owned(),
                })?;
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            (
                referenced_table_file_ids(manifest.state()),
                referenced_blob_file_ids_from_manifest(manifest.state()),
            )
        };

        let mut deleted = 0_usize;
        for table_id in table::list_table_file_ids_with_backend_async(&backend, db_path).await? {
            if !referenced_tables.contains(&table_id) {
                backend
                    .delete_object(StorageObjectId::native_file(
                        StorageObjectKind::Table,
                        table::table_path(db_path, table_id),
                    ))
                    .await?;
                deleted += 1;
            }
        }
        for file_id in blob::list_blob_file_ids_with_backend_async(&backend, db_path).await? {
            if !referenced_blobs.contains(&file_id) {
                backend
                    .delete_object(StorageObjectId::native_file(
                        StorageObjectKind::Blob,
                        blob::blob_path(db_path, file_id),
                    ))
                    .await?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Compact object-store tables overlapping `range` once. Mirrors the browser
    /// async compaction, but publishes the table replacement via the object-store
    /// manifest CAS and leaves the now-unreferenced input table objects + obsolete
    /// blobs to orphan GC (which is snapshot-safe), rather than deleting inline.
    #[allow(clippy::too_many_lines)] // faithful mirror of the browser compaction orchestration
    pub(in crate::db) async fn run_compaction_once_object_store_async(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        let oldest_active_snapshot = self.oldest_retained_sequence();
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
            written_table_ids: _,
        } = self
            .build_compaction_outputs_object_store_async(
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
        let trigger_stats = compaction_trigger_stat_deltas(&compaction_inputs, &written_tables);
        let obsolete_blob_ids =
            self.obsolete_blob_ids_for_compaction(&compaction_inputs, &written_tables)?;

        match self.validate_compacted_tables(&written_tables) {
            Ok(()) => {}
            // Output objects left unreferenced are reclaimed by orphan GC.
            Err(error) if is_level_layout_compaction_error(&error) => {
                return Ok(MaintenanceOutcome::default());
            }
            Err(error) => return Err(error),
        }

        let edits = written_tables
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
        let pending_deletion_sequence = self.last_committed_sequence();
        self.publish_object_manifest_replace_tables(
            edits,
            obsolete_blob_ids,
            pending_deletion_sequence,
        )
        .await?;

        self.install_compacted_tables(written_tables)
            .map_err(|error| self.close_after_durable_publish_error("compaction", &error))?;
        self.record_compaction_stats_from_tables(
            compaction_inputs.len(),
            &input_tables,
            &output_tables,
            &trigger_stats,
        );
        // The replaced input table objects and obsolete blob objects are now
        // unreferenced by the manifest; orphan GC reclaims them snapshot-safely.

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

    /// Build compaction output table objects (object-store backend). On error the
    /// freshly-written objects are left for orphan GC. Mirrors the browser version.
    pub(in crate::db) async fn build_compaction_outputs_object_store_async(
        &self,
        db_path: &Path,
        oldest_active_snapshot: Sequence,
        compaction_inputs: &[NamedCompactionInput],
    ) -> Result<PendingCompactionOutputs> {
        let backend = self.object_storage()?;
        let mut outputs = Vec::with_capacity(compaction_inputs.len());
        let mut written_table_ids = Vec::new();
        let mut next_table_id = self.next_table_id()?;

        for input in compaction_inputs {
            let force_rewrite_trivial =
                input.tree.options.blob_level_merge_policy == BlobLevelMergePolicy::Always;
            if input.input.trivial_move && !force_rewrite_trivial {
                outputs.push(NamedCompactionOutput {
                    bucket: input.bucket.clone(),
                    trigger: Some(input.input.trigger),
                    output: LsmCompactionOutput {
                        input_table_ids: input.input.input_table_ids.clone(),
                        tables: vec![input.input.moved_table()?],
                    },
                });
                continue;
            }

            let payloads = input.tree.build_compaction_table_payloads(
                &input.input,
                &input.input.compaction_range,
                oldest_active_snapshot,
                self.inner.options.target_table_bytes,
            )?;
            let mut table_options = input.input.table_options.clone();
            table_options.rewrite_blob_indexes = should_rewrite_blob_indexes_for_compaction(
                &input.input,
                &payloads,
                input.tree.options.blob_level_merge_policy,
            );
            let mut output_tables = Vec::with_capacity(payloads.len());
            for payload in payloads {
                let table_id = next_table_id;
                next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                    message: "table id counter overflow".to_owned(),
                })?;
                let table_path = table::table_path(db_path, table_id);
                written_table_ids.push(table_id);
                let table = table::write_table_with_backend_async(
                    &backend,
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                    DurabilityMode::Flush,
                )
                .await?;
                output_tables.push(Arc::new(table));
            }
            outputs.push(NamedCompactionOutput {
                bucket: input.bucket.clone(),
                trigger: Some(input.input.trigger),
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
    pub(in crate::db) async fn run_owned_browser_task<T>(
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn persist_browser_async(&self, mode: DurabilityMode) -> Result<()> {
        self.ensure_open()?;
        if matches!(
            mode,
            DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
        ) {
            return Err(Error::unsupported_durability(mode));
        }
        let Some(wal) = &self.inner.browser_wal else {
            return Ok(());
        };
        let storage = self.browser_storage()?;
        wal.persist(&storage, Path::new(""), mode).await
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn persist_native_async(&self, mode: DurabilityMode) -> Result<()> {
        self.ensure_open()?;

        if self.inner.options.storage_mode.is_wasi_persistent()
            && matches!(
                mode,
                DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
            )
        {
            return Err(Error::unsupported_durability(mode));
        }

        match &self.inner.options.storage_mode {
            StorageMode::InMemory => Ok(()),
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. } | HostStorageBackend::ObjectStore,
            } => self.inner.substrate.persist_wal_async(mode).await,
            StorageMode::HostPersistent { backend } => {
                Err(Error::unsupported_backend(backend.as_str()))
            }
        }
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

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn flush_native_async(&self) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;
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
            let (flush_should_compact, outcome) = self
                .run_flush_once_with_budget_native_async(
                    &db_path,
                    false,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
            if outcome.busy {
                self.request_background_flush();
                self.record_cooperative_maintenance_yield();
                self.inner.maintenance.wait_until_flush_idle();
                continue;
            }
            should_compact |= flush_should_compact;
        }

        if should_compact
            || self.l0_pressure_exceeded()?
            || self.foreground_l0_overlap_pressure_exceeded()?
        {
            self.run_compaction_barrier_native_async(&db_path, &KeyRange::all(), true)
                .await?;
        }
        self.cleanup_pending_obsolete_table_files_native_async(&db_path)
            .await?;
        self.cleanup_pending_obsolete_blob_files_native_async(&db_path)
            .await?;
        self.take_background_maintenance_error()?;

        Ok(())
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn close_native_async(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::Release);
        shutdown_background_workers(
            &self.inner.maintenance,
            &self.inner.runtime_shutdown,
            &self.inner.background_workers,
        );
        self.inner.publish_barrier.close()?;
        if let Some(db_path) = self.persistent_path().map(Path::to_path_buf) {
            self.cleanup_pending_obsolete_table_files_native_async(&db_path)
                .await?;
            self.cleanup_pending_obsolete_blob_files_native_async(&db_path)
                .await?;
        }
        super::super::release_browser_writer_lease(&self.inner);
        self.inner.substrate.release_writer_lease();
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
    pub(in crate::db) fn compact_range_internal(&self, range: KeyRange) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent compaction requires async maintenance",
            ));
        }
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store compaction requires the async API",
            ));
        }

        let Some(path) = self.persistent_path() else {
            return Ok(());
        };
        let db_path = path.to_path_buf();
        self.run_compaction_barrier(&db_path, &range, false)?;
        // Run cleanup after the compaction call returns so its input table
        // handles are dropped; obsolete files are then uniquely owned by the
        // cleanup queue and reclaimed without waiting for a later pass.
        self.cleanup_pending_obsolete_table_files(&db_path)?;

        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(in crate::db) fn compact_range_with_budget_internal(
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
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store compaction requires the async API",
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
    /// - `budget`: carries separate limits for flush inputs and compaction
    ///   inputs. A call may consume work from both limits; if flushing reports
    ///   busy or budget exhaustion, this call skips the follow-up compaction
    ///   pass.
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
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return Err(Error::unsupported_backend(
                "object-store maintenance requires the async API",
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
    pub(in crate::db) async fn flush_browser_async(&self) -> Result<()> {
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
    pub(in crate::db) async fn compact_range_browser_async(&self, range: KeyRange) -> Result<()> {
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
    pub(in crate::db) async fn compact_range_with_budget_browser_async(
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
    pub(in crate::db) async fn run_maintenance_with_budget_browser_async(
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
}
