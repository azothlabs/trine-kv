use super::{
    Arc, BlobLevelMergePolicy, CompactionReservation, Db, Error, KeyRange, LsmCompactionOutput,
    MaintenanceBudget, MaintenanceCompactionGuard, MaintenanceOutcome, NamedCompactionInput,
    NamedCompactionOutput, Path, PendingCompactionOutputs, Result, Sequence, compaction_options,
    compaction_trigger_stat_deltas, is_level_layout_compaction_error, lock_poisoned,
    remove_storage_files, remove_storage_files_async, should_rewrite_blob_indexes_for_compaction,
    table,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::DurabilityMode;

struct PreparedCompactionRun {
    oldest_active_snapshot: Sequence,
    inputs: Vec<NamedCompactionInput>,
    guard: MaintenanceCompactionGuard,
    _snapshot_guard: crate::snapshot::CompactionSnapshotGuard,
    budget_exhausted: bool,
}

enum CompactionRunPreparation {
    Ready(PreparedCompactionRun),
    Outcome(MaintenanceOutcome),
}

pub(in crate::db) struct PreparedCompactionRewrite {
    pub(in crate::db) payloads: Vec<crate::lsm::CompactionTablePayload>,
    pub(in crate::db) table_options: table::TableWriteOptions,
}

impl Db {
    pub(in crate::db) fn prepare_compaction_rewrites(
        &self,
        oldest_active_snapshot: Sequence,
        compaction_inputs: &[NamedCompactionInput],
    ) -> Result<Vec<Option<PreparedCompactionRewrite>>> {
        compaction_inputs
            .iter()
            .map(|input| {
                let force_rewrite_trivial =
                    input.tree.options.blob_level_merge_policy == BlobLevelMergePolicy::Always;
                if input.input.trivial_move && !force_rewrite_trivial {
                    return Ok(None);
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
                Ok(Some(PreparedCompactionRewrite {
                    payloads,
                    table_options,
                }))
            })
            .collect()
    }

    pub(in crate::db) fn collect_compaction_inputs(
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
            let result =
                state.plan_compaction(name, range, oldest_active_snapshot, compaction_options)?;
            if let Some(skip) = result.skip {
                self.record_compaction_skip(skip);
            }
            let Some(input) = result.input else {
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

    pub(in crate::db) fn run_compaction_barrier(
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

    pub(in crate::db) fn run_compaction_once_with_budget(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        let PreparedCompactionRun {
            oldest_active_snapshot,
            inputs: compaction_inputs,
            guard: compaction_guard,
            _snapshot_guard,
            budget_exhausted,
        } = match self.prepare_compaction_run(range, local_l0_compaction, budget)? {
            CompactionRunPreparation::Ready(run) => run,
            CompactionRunPreparation::Outcome(outcome) => return Ok(outcome),
        };

        let PendingCompactionOutputs {
            outputs: written_tables,
            written_table_ids,
        } = self.build_compaction_outputs(db_path, oldest_active_snapshot, &compaction_inputs)?;

        let input_tables_for_stats = compaction_inputs
            .iter()
            .flat_map(|input| input.input.input_tables.iter().cloned())
            .collect::<Vec<_>>();
        let output_tables_for_stats = written_tables
            .iter()
            .flat_map(|output| output.output.tables.iter().cloned())
            .collect::<Vec<_>>();
        let trigger_stats = compaction_trigger_stat_deltas(&compaction_inputs, &written_tables);
        let obsolete_blob_ids =
            self.obsolete_blob_ids_for_compaction(&compaction_inputs, &written_tables)?;

        if !written_table_ids.is_empty()
            && let Err(error) = self.sync_filesystem_directory_after_renames(db_path)
        {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            return Err(error);
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
            let error = self.close_after_manifest_durability_failure("compaction", error);
            if !self.closed_after_durable_publish_error() {
                let _ =
                    remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            }
            return Err(error);
        }

        let obsolete_tables = self
            .install_compacted_tables(written_tables)
            .map_err(|error| self.close_after_durable_publish_error("compaction", &error))?;
        self.record_compaction_stats_from_tables(
            compaction_inputs.len(),
            &input_tables_for_stats,
            &output_tables_for_stats,
            &trigger_stats,
        );
        self.retire_obsolete_table_files(db_path, obsolete_tables)?;
        self.delete_pending_obsolete_blob_files(db_path)?;
        drop(compaction_guard);
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

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn run_compaction_barrier_native_async(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
    ) -> Result<()> {
        loop {
            self.take_background_maintenance_error()?;
            let outcome = self
                .run_compaction_once_with_budget_host_async(
                    db_path,
                    range,
                    local_l0_compaction,
                    MaintenanceBudget::unbounded(),
                )
                .await?;
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

    #[allow(clippy::too_many_lines)] // compaction keeps one linear publish/install transaction
    pub(in crate::db) async fn run_compaction_once_with_budget_host_async(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceOutcome> {
        let PreparedCompactionRun {
            oldest_active_snapshot,
            inputs: compaction_inputs,
            guard: compaction_guard,
            _snapshot_guard,
            budget_exhausted,
        } = match self.prepare_compaction_run(range, local_l0_compaction, budget)? {
            CompactionRunPreparation::Ready(run) => run,
            CompactionRunPreparation::Outcome(outcome) => return Ok(outcome),
        };

        let pending_outputs = self
            .build_compaction_outputs_host_async(
                db_path,
                oldest_active_snapshot,
                &compaction_inputs,
            )
            .await?;
        let PendingCompactionOutputs {
            outputs: written_tables,
            written_table_ids,
        } = pending_outputs;

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

        if !written_table_ids.is_empty()
            && let Err(error) = self.sync_compaction_directory_host_async(db_path).await
        {
            let _ = self
                .remove_storage_files_host_async(db_path, &written_table_ids)
                .await;
            return Err(error);
        }

        if let Err(error) = self.validate_compacted_tables(&written_tables) {
            let _ = self
                .remove_storage_files_host_async(db_path, &written_table_ids)
                .await;
            if is_level_layout_compaction_error(&error) {
                return Ok(MaintenanceOutcome::default());
            }
            return Err(error);
        }
        let _publish_activity = match self.inner.publish_barrier.begin_activity() {
            Ok(activity) => activity,
            Err(error) => {
                let _ = self
                    .remove_storage_files_host_async(db_path, &written_table_ids)
                    .await;
                return Err(error);
            }
        };

        let publish_result = self
            .publish_compacted_tables_host_async(&written_tables, &obsolete_blob_ids)
            .await;
        if let Err(error) = publish_result {
            let error = self.close_after_manifest_durability_failure("compaction", error);
            if !self.closed_after_durable_publish_error() {
                let _ = self
                    .remove_storage_files_host_async(db_path, &written_table_ids)
                    .await;
            }
            return Err(error);
        }

        let compaction_count = compaction_inputs.len();
        let obsolete_tables = self
            .install_compacted_tables(written_tables)
            .map_err(|error| self.close_after_durable_publish_error("compaction", &error))?;
        self.record_compaction_stats_from_tables(
            compaction_count,
            &input_tables,
            &output_tables,
            &trigger_stats,
        );
        // Release plan-held table references before asynchronous retirement.
        drop(input_tables);
        drop(compaction_inputs);
        self.retire_obsolete_table_files_host_async(db_path, obsolete_tables)
            .await?;
        self.delete_pending_obsolete_blob_files_host_async(db_path)
            .await?;
        drop(compaction_guard);
        if self.inner.options.blob_gc_enabled {
            self.run_blob_gc_once_host_async(db_path).await?;
        }

        let outcome = MaintenanceOutcome {
            compactions: compaction_count,
            budget_exhausted,
            ..MaintenanceOutcome::default()
        };
        if outcome.budget_exhausted {
            self.record_maintenance_budget_exhaustion();
        }
        Ok(outcome)
    }

    fn prepare_compaction_run(
        &self,
        range: &KeyRange,
        local_l0_compaction: bool,
        budget: MaintenanceBudget,
    ) -> Result<CompactionRunPreparation> {
        let latest = self.last_committed_sequence();
        let retained_floor = self.retained_floor_without_active_snapshots(latest);
        let (oldest_active_snapshot, snapshot_guard) =
            self.inner.snapshots.begin_compaction(retained_floor);
        let inputs =
            self.collect_compaction_inputs(range, oldest_active_snapshot, local_l0_compaction)?;
        if inputs.is_empty() {
            return Ok(CompactionRunPreparation::Outcome(
                MaintenanceOutcome::default(),
            ));
        }
        let reservations = inputs
            .iter()
            .map(|input| CompactionReservation {
                bucket: input.bucket.clone(),
                range: input.input.compaction_range.clone(),
            })
            .collect::<Vec<_>>();
        let Some(guard) = self.inner.maintenance.reserve_compactions(reservations) else {
            return Ok(CompactionRunPreparation::Outcome(
                MaintenanceOutcome::busy_outcome(),
            ));
        };
        let mut inputs = inputs
            .into_iter()
            .filter(|input| guard.contains(&input.bucket, &input.input.compaction_range))
            .collect::<Vec<_>>();
        if inputs.is_empty() {
            return Ok(CompactionRunPreparation::Outcome(
                MaintenanceOutcome::busy_outcome(),
            ));
        }
        let limit = budget.compaction_input_limit();
        let budget_exhausted = inputs.len() > limit;
        inputs.truncate(limit);
        if !Self::compaction_inputs_are_current(&inputs)? {
            return Ok(CompactionRunPreparation::Outcome(
                MaintenanceOutcome::busy_outcome(),
            ));
        }
        Ok(CompactionRunPreparation::Ready(PreparedCompactionRun {
            oldest_active_snapshot,
            inputs,
            guard,
            _snapshot_guard: snapshot_guard,
            budget_exhausted,
        }))
    }

    async fn build_compaction_outputs_host_async(
        &self,
        db_path: &Path,
        oldest_active_snapshot: Sequence,
        inputs: &[NamedCompactionInput],
    ) -> Result<PendingCompactionOutputs> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        return self
            .build_compaction_outputs_native_async(db_path, oldest_active_snapshot, inputs)
            .await;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        return self
            .build_compaction_outputs_browser_async(db_path, oldest_active_snapshot, inputs)
            .await;
    }

    pub(in crate::db) async fn remove_storage_files_host_async(
        &self,
        db_path: &Path,
        table_ids: &[table::TableId],
    ) -> Result<()> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let storage = self.inner.native_storage.clone();
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let storage = self.browser_storage()?;
        remove_storage_files_async(&storage, db_path, table_ids).await
    }

    pub(in crate::db) async fn sync_compaction_directory_host_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        return self
            .sync_filesystem_directory_after_renames_async(db_path)
            .await;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            let _ = db_path;
            futures::future::ready(Ok(())).await
        }
    }

    pub(in crate::db) async fn publish_compacted_tables_host_async(
        &self,
        outputs: &[NamedCompactionOutput],
        obsolete_blob_ids: &[u64],
    ) -> Result<()> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        return self
            .publish_compacted_tables_native_async(outputs, obsolete_blob_ids)
            .await;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        return self
            .publish_compacted_tables_browser_async(outputs, obsolete_blob_ids)
            .await;
    }

    pub(in crate::db) async fn retire_obsolete_table_files_host_async(
        &self,
        db_path: &Path,
        obsolete_tables: Vec<Arc<crate::table::Table>>,
    ) -> Result<()> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        return self
            .retire_obsolete_table_files_native_async(db_path, obsolete_tables)
            .await;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        return self
            .retire_obsolete_table_files_browser_async(db_path, obsolete_tables)
            .await;
    }

    pub(in crate::db) async fn delete_pending_obsolete_blob_files_host_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let _ = self
            .delete_pending_obsolete_blob_files_native_async(db_path)
            .await?;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let _ = self
            .delete_pending_obsolete_blob_files_browser_async(db_path)
            .await?;
        Ok(())
    }

    pub(in crate::db) fn build_compaction_outputs(
        &self,
        db_path: &Path,
        oldest_active_snapshot: Sequence,
        compaction_inputs: &[NamedCompactionInput],
    ) -> Result<PendingCompactionOutputs> {
        let rewrites =
            self.prepare_compaction_rewrites(oldest_active_snapshot, compaction_inputs)?;
        let output_table_count = rewrites
            .iter()
            .flatten()
            .try_fold(0usize, |count, rewrite| {
                count
                    .checked_add(rewrite.payloads.len())
                    .ok_or_else(|| Error::Corruption {
                        message: "compaction output table count overflow".to_owned(),
                    })
            })?;
        let mut file_ids = self.reserve_file_ids(output_table_count)?;
        let mut outputs = Vec::with_capacity(compaction_inputs.len());
        let mut written_table_ids = Vec::new();

        for (input, rewrite) in compaction_inputs.iter().zip(rewrites) {
            let Some(rewrite) = rewrite else {
                outputs.push(NamedCompactionOutput {
                    bucket: input.bucket.clone(),
                    trigger: Some(input.input.trigger),
                    output: LsmCompactionOutput {
                        input_table_ids: input.input.input_table_ids.clone(),
                        tables: vec![input.input.moved_table()?],
                    },
                });
                continue;
            };
            let mut output_tables = Vec::with_capacity(rewrite.payloads.len());
            for payload in rewrite.payloads {
                let table_id = file_ids.next_table_id()?;

                let table_path = table::table_path(db_path, table_id);
                written_table_ids.push(table_id);
                let table = match table::write_table_with_backend_with_durability(
                    &self.inner.native_storage,
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &rewrite.table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                    self.filesystem_publish_durability(),
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

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn build_compaction_outputs_native_async(
        &self,
        db_path: &Path,
        oldest_active_snapshot: Sequence,
        compaction_inputs: &[NamedCompactionInput],
    ) -> Result<PendingCompactionOutputs> {
        let storage = self.inner.native_storage.clone();
        let rewrites =
            self.prepare_compaction_rewrites(oldest_active_snapshot, compaction_inputs)?;
        let output_table_count = rewrites
            .iter()
            .flatten()
            .try_fold(0usize, |count, rewrite| {
                count
                    .checked_add(rewrite.payloads.len())
                    .ok_or_else(|| Error::Corruption {
                        message: "compaction output table count overflow".to_owned(),
                    })
            })?;
        let mut file_ids = self.reserve_file_ids_host_async(output_table_count).await?;
        let mut outputs = Vec::with_capacity(compaction_inputs.len());
        let mut written_table_ids = Vec::new();

        for (input, rewrite) in compaction_inputs.iter().zip(rewrites) {
            let Some(rewrite) = rewrite else {
                outputs.push(NamedCompactionOutput {
                    bucket: input.bucket.clone(),
                    trigger: Some(input.input.trigger),
                    output: LsmCompactionOutput {
                        input_table_ids: input.input.input_table_ids.clone(),
                        tables: vec![input.input.moved_table()?],
                    },
                });
                continue;
            };
            let mut output_tables = Vec::with_capacity(rewrite.payloads.len());
            for payload in rewrite.payloads {
                let table_id = file_ids.next_table_id()?;

                let table_path = table::table_path(db_path, table_id);
                written_table_ids.push(table_id);
                let table = match table::write_table_with_backend_async(
                    &storage,
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &rewrite.table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                    self.filesystem_publish_durability(),
                )
                .await
                {
                    Ok(table) => table,
                    Err(error) => {
                        let _ =
                            remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                        return Err(error);
                    }
                };
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
    pub(in crate::db) async fn build_compaction_outputs_browser_async(
        &self,
        db_path: &Path,
        oldest_active_snapshot: Sequence,
        compaction_inputs: &[NamedCompactionInput],
    ) -> Result<PendingCompactionOutputs> {
        let storage = self.browser_storage()?;
        let rewrites =
            self.prepare_compaction_rewrites(oldest_active_snapshot, compaction_inputs)?;
        let output_table_count = rewrites
            .iter()
            .flatten()
            .try_fold(0usize, |count, rewrite| {
                count
                    .checked_add(rewrite.payloads.len())
                    .ok_or_else(|| Error::Corruption {
                        message: "compaction output table count overflow".to_owned(),
                    })
            })?;
        let mut file_ids = self.reserve_file_ids_host_async(output_table_count).await?;
        let mut outputs = Vec::with_capacity(compaction_inputs.len());
        let mut written_table_ids = Vec::new();

        for (input, rewrite) in compaction_inputs.iter().zip(rewrites) {
            let Some(rewrite) = rewrite else {
                outputs.push(NamedCompactionOutput {
                    bucket: input.bucket.clone(),
                    trigger: Some(input.input.trigger),
                    output: LsmCompactionOutput {
                        input_table_ids: input.input.input_table_ids.clone(),
                        tables: vec![input.input.moved_table()?],
                    },
                });
                continue;
            };
            let mut output_tables = Vec::with_capacity(rewrite.payloads.len());
            for payload in rewrite.payloads {
                let table_id = file_ids.next_table_id()?;

                let table_path = table::table_path(db_path, table_id);
                written_table_ids.push(table_id);
                let table = match table::write_table_with_backend_async(
                    &storage,
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &rewrite.table_options,
                    &payload.point_records,
                    &payload.range_tombstones,
                    DurabilityMode::Flush,
                )
                .await
                {
                    Ok(table) => table,
                    Err(error) => {
                        let _ =
                            remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                        return Err(error);
                    }
                };
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
}
