#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use super::remove_storage_files_async;
use super::{
    Arc, BlobLevelMergePolicy, CompactionReservation, Db, Error, KeyRange, LsmCompactionOutput,
    MaintenanceBudget, MaintenanceOutcome, NamedCompactionInput, NamedCompactionOutput, Path,
    PendingCompactionOutputs, Result, Sequence, compaction_options, compaction_trigger_stat_deltas,
    is_level_layout_compaction_error, lock_poisoned, remove_storage_files,
    should_rewrite_blob_indexes_for_compaction, table,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::DurabilityMode;

impl Db {
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
        if !Self::compaction_inputs_are_current(&compaction_inputs)? {
            return Ok(MaintenanceOutcome::busy_outcome());
        }

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

        if !written_table_ids.is_empty() {
            if let Err(error) = self.sync_filesystem_directory_after_renames(db_path) {
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
    #[allow(clippy::too_many_lines)]
    pub(in crate::db) async fn run_compaction_barrier_native_async(
        &self,
        db_path: &Path,
        range: &KeyRange,
        local_l0_compaction: bool,
    ) -> Result<()> {
        loop {
            self.take_background_maintenance_error()?;
            let outcome = self
                .run_compaction_once_with_budget_native_async(
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

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    #[allow(clippy::too_many_lines)]
    pub(in crate::db) async fn run_compaction_once_with_budget_native_async(
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
        if !Self::compaction_inputs_are_current(&compaction_inputs)? {
            return Ok(MaintenanceOutcome::busy_outcome());
        }

        let PendingCompactionOutputs {
            outputs: written_tables,
            written_table_ids,
        } = self
            .build_compaction_outputs_native_async(
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

        let storage = self.inner.native_storage.clone();
        if !written_table_ids.is_empty() {
            if let Err(error) = self
                .sync_filesystem_directory_after_renames_async(db_path)
                .await
            {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                return Err(error);
            }
        }

        if let Err(error) = self.validate_compacted_tables(&written_tables) {
            let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
            if is_level_layout_compaction_error(&error) {
                return Ok(MaintenanceOutcome::default());
            }
            return Err(error);
        }
        let _publish_activity = match self.inner.publish_barrier.begin_activity() {
            Ok(activity) => activity,
            Err(error) => {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .publish_compacted_tables_native_async(&written_tables, &obsolete_blob_ids)
            .await
        {
            if !self.closed_after_durable_publish_error() {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
            }
            return Err(error);
        }

        let obsolete_tables = self
            .install_compacted_tables(written_tables)
            .map_err(|error| self.close_after_durable_publish_error("compaction", &error))?;
        self.record_compaction_stats_from_tables(
            compaction_inputs.len(),
            &input_tables,
            &output_tables,
            &trigger_stats,
        );
        self.retire_obsolete_table_files_native_async(db_path, obsolete_tables)
            .await?;
        self.delete_pending_obsolete_blob_files_native_async(db_path)
            .await?;
        drop(compaction_guard);
        if self.inner.options.blob_gc_enabled {
            self.run_blob_gc_once_native_async(db_path).await?;
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
    pub(in crate::db) async fn run_compaction_once_with_budget_browser_async(
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
        if !Self::compaction_inputs_are_current(&compaction_inputs)? {
            return Ok(MaintenanceOutcome::busy_outcome());
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
        let trigger_stats = compaction_trigger_stat_deltas(&compaction_inputs, &written_tables);
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
            if !self.closed_after_durable_publish_error() {
                let _ = self
                    .remove_storage_files_browser_async(db_path, &written_table_ids)
                    .await;
            }
            return Err(error);
        }

        let obsolete_tables = self
            .install_compacted_tables(written_tables)
            .map_err(|error| self.close_after_durable_publish_error("compaction", &error))?;
        self.record_compaction_stats_from_tables(
            compaction_inputs.len(),
            &input_tables,
            &output_tables,
            &trigger_stats,
        );
        self.retire_obsolete_table_files_browser_async(db_path, obsolete_tables)
            .await?;
        self.delete_pending_obsolete_blob_files_browser_async(db_path)
            .await?;
        drop(compaction_guard);
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

    pub(in crate::db) fn build_compaction_outputs(
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
                    trigger: Some(input.input.trigger),
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
                let table = match table::write_table_with_backend_with_durability(
                    &self.inner.native_storage,
                    &table_path,
                    table_id,
                    input.input.table_level,
                    &table_options,
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

            let payloads = match input.tree.build_compaction_table_payloads(
                &input.input,
                &input.input.compaction_range,
                oldest_active_snapshot,
                self.inner.options.target_table_bytes,
            ) {
                Ok(payloads) => payloads,
                Err(error) => {
                    let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
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
                    let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
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
