#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use super::NamedCompactionOutput;
use super::{
    Arc, Db, Error, KeyRange, LsmTree, MaintenanceBudget, MaintenanceOutcome, NamedFlushInput,
    Path, Result, Sequence, Table, WritePressure, lock_poisoned, remove_storage_files,
    remove_storage_files_async, table, usize_to_u64_saturating,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::DurabilityMode;
use crate::db::DatabaseStorageRef;

impl Db {
    pub(in crate::db) fn apply_write_backpressure(&self) -> Result<()> {
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

            // Snapshot progress before attempting foreground maintenance. A
            // background worker can win the maintenance guard and complete
            // between the foreground busy result and our request below. If we
            // sampled afterwards, that real progress would be lost and this
            // writer could wait until timeout for a change that already
            // happened.
            let progress = self.inner.maintenance.progress();
            let outcome = self.run_maintenance_for_pressure(&db_path, pressure)?;
            if outcome.made_progress() {
                continue;
            }

            if self.background_workers_enabled() {
                self.inner.maintenance.request(pressure.request());
                self.record_cooperative_maintenance_yield();
                if self
                    .inner
                    .maintenance
                    .wait_for_progress(progress, Self::background_maintenance_progress_wait())
                {
                    continue;
                }
                // The worker can clear pressure without publishing another
                // progress transition after the timed wait observes its final
                // state. Recheck the actual invariant before reporting
                // backpressure failure.
                self.take_background_maintenance_error()?;
                if self.write_pressure()?.none() {
                    return Ok(());
                }
            }
            self.record_maintenance_budget_exhaustion();
            return Err(Error::runtime_busy(
                "write pressure could not make maintenance progress",
            ));
        }
    }

    pub(in crate::db) fn write_pressure(&self) -> Result<WritePressure> {
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

    pub(in crate::db) fn run_maintenance_for_pressure(
        &self,
        db_path: &Path,
        pressure: WritePressure,
    ) -> Result<MaintenanceOutcome> {
        let mut should_compact = pressure.compaction;
        let mut outcome = MaintenanceOutcome::default();
        if pressure.flush {
            let (flush_should_compact, flush_outcome) =
                self.run_pressure_flush_once_with_budget(db_path, MaintenanceBudget::unbounded())?;
            should_compact |= flush_should_compact;
            outcome.flushes += flush_outcome.flushes;
            outcome.budget_exhausted |= flush_outcome.budget_exhausted;
            outcome.busy |= flush_outcome.busy;
        }
        if should_compact {
            let compaction_outcome = self.run_compaction_once_with_budget(
                db_path,
                &KeyRange::all(),
                true,
                MaintenanceBudget::single_unit(),
            )?;
            outcome.compactions += compaction_outcome.compactions;
            outcome.budget_exhausted |= compaction_outcome.budget_exhausted;
            outcome.busy |= compaction_outcome.busy;
        }

        Ok(outcome)
    }

    pub(in crate::db) fn run_pressure_flush_once_with_budget(
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

    pub(in crate::db) fn run_flush_once(
        &self,
        db_path: &Path,
        freeze_active: bool,
    ) -> Result<bool> {
        let (should_compact, _) = self.run_flush_once_with_budget(
            db_path,
            freeze_active,
            MaintenanceBudget::unbounded(),
        )?;
        Ok(should_compact)
    }

    pub(in crate::db) fn run_flush_once_with_budget(
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

    pub(in crate::db) fn freeze_large_active_memtables_after_commit(
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

        Ok(frozen_count != 0
            && states.iter().any(|state| {
                state.immutable_memtable_count() >= self.background_flush_request_threshold()
            }))
    }

    pub(in crate::db) fn freeze_public_flush_target(&self) -> Result<Sequence> {
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

    pub(in crate::db) fn has_immutable_memtables(&self) -> Result<bool> {
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

    pub(in crate::db) fn has_immutable_memtables_at_or_below(
        &self,
        max_sequence: Sequence,
    ) -> Result<bool> {
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

    pub(in crate::db) fn freeze_all_active_memtables(
        &self,
        freeze_sequence: Sequence,
    ) -> Result<usize> {
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

    pub(in crate::db) fn write_flush_inputs(
        &self,
        db_path: &Path,
        flush_inputs: &[NamedFlushInput],
    ) -> Result<bool> {
        if flush_inputs.is_empty() {
            return Ok(false);
        }
        let DatabaseStorageRef::Filesystem(resources) = self.inner.storage.resources() else {
            return Err(Error::unsupported_backend(
                "synchronous flush requires filesystem storage",
            ));
        };
        let storage = resources.files;
        let mut file_ids = self.reserve_file_ids(flush_inputs.len())?;

        let mut written_tables = Vec::with_capacity(flush_inputs.len());
        let mut written_table_ids = Vec::with_capacity(flush_inputs.len());
        for input in flush_inputs {
            let table_id = file_ids.next_table_id()?;
            let table_path = table::table_path(db_path, table_id);
            written_table_ids.push(table_id);
            let table = match table::write_table_with_backend_with_durability(
                storage,
                &table_path,
                table_id,
                input.input.table_level,
                &input.input.table_options,
                &input.input.point_records,
                &input.input.range_tombstones,
                self.filesystem_publish_durability(),
            ) {
                Ok(table) => table,
                Err(error) => {
                    let _ = remove_storage_files(storage, db_path, &written_table_ids);
                    return Err(error);
                }
            };
            written_tables.push((input.bucket.clone(), Arc::new(table)));
        }

        if let Err(error) = self.sync_filesystem_directory_after_renames(db_path) {
            let _ = remove_storage_files(storage, db_path, &written_table_ids);
            return Err(error);
        }

        {
            let _memtable_publish = self
                .inner
                .memtable_publish_lock
                .lock()
                .map_err(|_| lock_poisoned("memtable publish lock"))?;
            let _publish = self.inner.publish_barrier.enter()?;
            let replay_floor = self.replay_floor_after_flush(flush_inputs)?;
            if let Err(error) = self.publish_flushed_tables(&written_tables, replay_floor) {
                let error = self.close_after_manifest_durability_failure("flush", error);
                if !self.closed_after_durable_publish_error() {
                    let _ = remove_storage_files(storage, db_path, &written_table_ids);
                }
                return Err(error);
            }
            Self::install_flushed_tables(flush_inputs, written_tables)
                .map_err(|error| self.close_after_durable_publish_error("flush", &error))?;
            self.rewrite_wal_after_replay_floor(replay_floor)?;
        }
        self.l0_pressure_exceeded()
    }

    pub(in crate::db) async fn run_flush_once_with_budget_host_async(
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
            .write_flush_inputs_host_async(db_path, &flush_inputs)
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

    pub(in crate::db) async fn write_flush_inputs_host_async(
        &self,
        db_path: &Path,
        flush_inputs: &[NamedFlushInput],
    ) -> Result<bool> {
        if flush_inputs.is_empty() {
            return Ok(false);
        }
        let mut file_ids = self.reserve_file_ids_host_async(flush_inputs.len()).await?;

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let storage = match self.inner.storage.resources() {
            DatabaseStorageRef::Filesystem(resources) => resources.files.clone(),
            _ => {
                return Err(Error::unsupported_backend(
                    "native async flush requires filesystem storage",
                ));
            }
        };
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let storage = match self.inner.storage.resources() {
            DatabaseStorageRef::Browser(resources) => resources.files.clone(),
            _ => {
                return Err(Error::unsupported_backend(
                    "browser flush requires browser storage",
                ));
            }
        };
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let durability = self.filesystem_publish_durability();
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let durability = DurabilityMode::Flush;
        let mut written_tables = Vec::with_capacity(flush_inputs.len());
        let mut written_table_ids = Vec::with_capacity(flush_inputs.len());

        for input in flush_inputs {
            let table_id = file_ids.next_table_id()?;
            let table_path = table::table_path(db_path, table_id);
            written_table_ids.push(table_id);
            let table = match table::write_table_with_backend_async(
                &storage,
                &table_path,
                table_id,
                input.input.table_level,
                &input.input.table_options,
                &input.input.point_records,
                &input.input.range_tombstones,
                durability,
            )
            .await
            {
                Ok(table) => table,
                Err(error) => {
                    let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                    return Err(error);
                }
            };
            written_tables.push((input.bucket.clone(), Arc::new(table)));
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if let Err(error) = self
            .sync_filesystem_directory_after_renames_async(db_path)
            .await
        {
            let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
            return Err(error);
        }

        let _publish_activity = match self.inner.publish_barrier.begin_activity() {
            Ok(activity) => activity,
            Err(error) => {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                return Err(error);
            }
        };
        let replay_floor = self.replay_floor_after_flush_serialized(flush_inputs)?;

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let publish_result = self
            .publish_flushed_tables_native_async(&written_tables, replay_floor)
            .await;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let publish_result = self
            .publish_flushed_tables_browser_async(&written_tables, replay_floor)
            .await;
        if let Err(error) = publish_result {
            let error = self.close_after_manifest_durability_failure("flush", error);
            if !self.closed_after_durable_publish_error() {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
            }
            return Err(error);
        }

        Self::install_flushed_tables(flush_inputs, written_tables)
            .map_err(|error| self.close_after_durable_publish_error("flush", &error))?;
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        self.rewrite_wal_after_replay_floor_async(replay_floor)
            .await?;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        self.rewrite_wal_after_replay_floor_browser_async(db_path, replay_floor)
            .await?;
        self.l0_pressure_exceeded()
    }

    pub(in crate::db) fn replay_floor_after_flush_serialized(
        &self,
        flush_inputs: &[NamedFlushInput],
    ) -> Result<Sequence> {
        let _memtable_publish = self
            .inner
            .memtable_publish_lock
            .lock()
            .map_err(|_| lock_poisoned("memtable publish lock"))?;
        self.replay_floor_after_flush(flush_inputs)
    }

    fn replay_floor_after_flush(&self, flush_inputs: &[NamedFlushInput]) -> Result<Sequence> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut oldest_unflushed = None;

        for state in buckets.values() {
            let flushed_memtables = flush_inputs
                .iter()
                .filter(|input| Arc::ptr_eq(&input.tree, state))
                .map(|input| Arc::clone(&input.input.memtable))
                .collect::<Vec<_>>();
            if let Some(sequence) = state.oldest_unflushed_sequence_excluding(&flushed_memtables)? {
                oldest_unflushed = Some(
                    oldest_unflushed.map_or(sequence, |current: Sequence| current.min(sequence)),
                );
            }
        }

        Ok(oldest_unflushed.map_or_else(
            || self.last_committed_sequence(),
            |sequence| Sequence::new(sequence.get().saturating_sub(1)),
        ))
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn publish_flushed_tables_native_async(
        &self,
        tables: &[(String, Arc<Table>)],
        flush_sequence: Sequence,
    ) -> Result<()> {
        let edits = tables
            .iter()
            .map(|(bucket, table)| (bucket.clone(), table.properties().clone()))
            .collect::<Vec<_>>();
        self.edit_host_manifest_async(move |manifest| manifest.add_tables(edits, flush_sequence))
            .await
            .map_err(|error| self.close_after_manifest_durability_failure("flush", error))
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn publish_compacted_tables_native_async(
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
        let obsolete_blob_ids = obsolete_blob_ids.to_vec();
        let pending_deletion_sequence = self.last_committed_sequence();
        self.edit_host_manifest_async(move |manifest| {
            manifest.replace_tables_batch_and_mark_blob_deletions(
                edits,
                obsolete_blob_ids,
                pending_deletion_sequence,
            )
        })
        .await
        .map_err(|error| self.close_after_manifest_durability_failure("compaction", error))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn publish_flushed_tables_browser_async(
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
        prepared
            .publish_async()
            .await
            .map_err(|error| self.close_after_manifest_durability_failure("flush", error))?;
        self.install_prepared_manifest_after_durable_publish("flush", manifest, prepared)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn rewrite_wal_after_replay_floor_browser_async(
        &self,
        db_path: &Path,
        replay_floor: Sequence,
    ) -> Result<()> {
        let DatabaseStorageRef::Browser(resources) = self.inner.storage.resources() else {
            return Err(Error::unsupported_backend(
                "browser WAL rewrite requires browser storage",
            ));
        };
        let Some(wal) = resources.wal else {
            return Ok(());
        };
        wal.rewrite_after_replay_floor(resources.files, db_path, replay_floor)
            .await
    }

    pub(in crate::db) fn collect_pressure_flush_inputs_with_budget(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<(Vec<NamedFlushInput>, bool)> {
        let max_immutable_memtables = self.inner.options.max_immutable_memtables;
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
            for input in state.prepare_flush_inputs()? {
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

    pub(in crate::db) fn collect_flush_inputs_with_budget(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<(Vec<NamedFlushInput>, bool)> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut inputs = Vec::new();
        let limit = budget.flush_input_limit();
        let mut budget_exhausted = false;

        for (name, state) in buckets.iter() {
            for input in state.prepare_flush_inputs()? {
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
}
