use super::*;

impl Db {
    pub(in crate::db) fn next_table_id(&self) -> Result<table::TableId> {
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .next_table_id()
    }

    pub(in crate::db) fn publish_flushed_tables(
        &self,
        tables: &[(String, Arc<Table>)],
        flush_sequence: Sequence,
    ) -> Result<()> {
        let edits = tables
            .iter()
            .map(|(bucket, table)| (bucket.clone(), table.properties().clone()))
            .collect::<Vec<_>>();
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .add_tables(edits, flush_sequence)
    }

    pub(in crate::db) fn publish_compacted_tables(
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
        self.inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .replace_tables_batch_and_mark_blob_deletions(
                edits,
                obsolete_blob_ids.to_vec(),
                self.last_committed_sequence(),
            )
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn publish_compacted_tables_browser_async(
        &self,
        outputs: &[NamedCompactionOutput],
        obsolete_blob_ids: &[u64],
    ) -> Result<()> {
        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
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
            manifest.prepare_replace_tables_batch_publish(
                edits,
                obsolete_blob_ids.to_vec(),
                self.last_committed_sequence(),
            )?
        };
        prepared.publish_async().await?;
        manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_prepared_publish(prepared)
    }

    pub(in crate::db) fn rewrite_wal_after_replay_floor(
        &self,
        replay_floor: Sequence,
    ) -> Result<()> {
        self.inner
            .substrate
            .rewrite_wal_after_replay_floor(replay_floor)
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(in crate::db) async fn rewrite_wal_after_replay_floor_async(
        &self,
        replay_floor: Sequence,
    ) -> Result<()> {
        self.inner
            .substrate
            .rewrite_wal_after_replay_floor_async(replay_floor)
            .await
    }

    pub(in crate::db) fn install_flushed_tables(
        inputs: &[NamedFlushInput],
        tables: Vec<(String, Arc<Table>)>,
    ) -> Result<()> {
        for (input, (bucket, table)) in inputs.iter().zip(tables) {
            debug_assert_eq!(input.bucket, bucket);
            input.tree.install_flush(&input.input, table)?;
        }

        Ok(())
    }

    /// Installs each compaction output and returns the obsolete input table
    /// handles whose files are now eligible for liveness-gated deletion. Callers
    /// that manage table files through the pending-obsolete queue pass the result
    /// to `retire_obsolete_table_files`; object-store mode ignores it and relies
    /// on snapshot-safe orphan GC instead.
    pub(in crate::db) fn install_compacted_tables(
        &self,
        outputs: Vec<NamedCompactionOutput>,
    ) -> Result<Vec<Arc<Table>>> {
        let mut obsolete = Vec::new();
        for output in outputs {
            let state = self.bucket_state(&output.bucket)?;
            obsolete.extend(state.install_compaction(output.output)?);
        }

        Ok(obsolete)
    }

    pub(in crate::db) fn validate_compacted_tables(
        &self,
        outputs: &[NamedCompactionOutput],
    ) -> Result<()> {
        for output in outputs {
            let state = self.bucket_state(&output.bucket)?;
            state.validate_compaction(&output.output)?;
        }

        Ok(())
    }

    pub(in crate::db) fn live_blob_bytes_by_file(&self) -> Result<BTreeMap<u64, u64>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut live_blob_bytes_by_file = BTreeMap::<u64, u64>::new();

        for state in buckets.values() {
            for table in state.tables_snapshot()? {
                for reference in table.properties().blob_references() {
                    live_blob_bytes_by_file
                        .entry(reference.file_id)
                        .and_modify(|bytes| {
                            *bytes = bytes.saturating_add(reference.referenced_bytes);
                        })
                        .or_insert(reference.referenced_bytes);
                }
            }
        }

        Ok(live_blob_bytes_by_file)
    }

    pub(in crate::db) fn cleanup_pending_obsolete_blob_files(&self, db_path: &Path) -> Result<()> {
        cleanup_pending_obsolete_blob_files(
            &self.inner.native_storage,
            Some(db_path),
            &self.inner.snapshots,
            self.inner.manifest.as_ref(),
        )
    }

    pub(in crate::db) fn delete_pending_obsolete_blob_files(&self, db_path: &Path) -> Result<()> {
        let _ = delete_pending_obsolete_blob_files(
            &self.inner.native_storage,
            Some(db_path),
            &self.inner.snapshots,
            self.inner.manifest.as_ref(),
        )?;
        Ok(())
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn cleanup_pending_obsolete_blob_files_native_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        let (manifest, pending_file_ids) = self
            .delete_pending_obsolete_blob_files_native_async(db_path)
            .await?;
        if pending_file_ids.is_empty() {
            return Ok(());
        }

        let prepared = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            manifest.prepare_clear_pending_blob_deletions_publish(&pending_file_ids)
        };
        let Some(prepared) = prepared else {
            return Ok(());
        };
        prepared.publish_async().await?;
        manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_prepared_publish(prepared)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn delete_pending_obsolete_blob_files_native_async(
        &self,
        db_path: &Path,
    ) -> Result<(&Mutex<ManifestStore>, Vec<u64>)> {
        if self.inner.snapshots.active_count() != 0 {
            return Ok((
                self.inner
                    .manifest
                    .as_ref()
                    .ok_or_else(|| Error::Corruption {
                        message: "persistent database is missing manifest store".to_owned(),
                    })?,
                Vec::new(),
            ));
        }
        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?;

        let pending_file_ids = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest.state());
            manifest
                .state()
                .pending_blob_deletions()
                .keys()
                .copied()
                .filter(|file_id| !referenced_blob_ids.contains(file_id))
                .collect::<Vec<_>>()
        };
        if pending_file_ids.is_empty() {
            return Ok((manifest, Vec::new()));
        }

        let storage = self.inner.native_storage.clone();
        for file_id in &pending_file_ids {
            delete_storage_object_async(
                &storage,
                StorageObjectKind::Blob,
                &blob::blob_path(db_path, *file_id),
            )
            .await?;
        }

        Ok((manifest, pending_file_ids))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn cleanup_pending_obsolete_blob_files_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        let _manifest_publish = self.inner.browser_manifest_async_lock.lock().await;
        let (manifest, pending_file_ids) = self
            .delete_pending_obsolete_blob_files_browser_async(db_path)
            .await?;
        if pending_file_ids.is_empty() {
            return Ok(());
        }

        let prepared = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            manifest.prepare_clear_pending_blob_deletions_publish(&pending_file_ids)
        };
        let Some(prepared) = prepared else {
            return Ok(());
        };
        prepared.publish_async().await?;
        manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?
            .install_prepared_publish(prepared)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn delete_pending_obsolete_blob_files_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<(&Mutex<ManifestStore>, Vec<u64>)> {
        if self.inner.snapshots.active_count() != 0 {
            return Ok((
                self.inner
                    .manifest
                    .as_ref()
                    .ok_or_else(|| Error::Corruption {
                        message: "persistent database is missing manifest store".to_owned(),
                    })?,
                Vec::new(),
            ));
        }

        let manifest = self
            .inner
            .manifest
            .as_ref()
            .ok_or_else(|| Error::Corruption {
                message: "persistent database is missing manifest store".to_owned(),
            })?;
        let pending_file_ids = {
            let manifest = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?;
            let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest.state());
            manifest
                .state()
                .pending_blob_deletions()
                .keys()
                .copied()
                .filter(|file_id| !referenced_blob_ids.contains(file_id))
                .collect::<Vec<_>>()
        };
        if pending_file_ids.is_empty() {
            return Ok((manifest, Vec::new()));
        }

        let storage = self.browser_storage()?;
        for file_id in &pending_file_ids {
            storage
                .delete_object(StorageObjectId::native_file(
                    StorageObjectKind::Blob,
                    blob::blob_path(db_path, *file_id),
                ))
                .await?;
        }

        Ok((manifest, pending_file_ids))
    }

    pub(in crate::db) fn obsolete_blob_ids_for_compaction(
        &self,
        inputs: &[NamedCompactionInput],
        outputs: &[NamedCompactionOutput],
    ) -> Result<Vec<u64>> {
        let input_table_ids = inputs
            .iter()
            .flat_map(|input| input.input.input_table_ids.iter().copied())
            .collect::<BTreeSet<_>>();
        let input_blob_ids = inputs
            .iter()
            .flat_map(|input| {
                input
                    .input
                    .input_tables
                    .iter()
                    .flat_map(|table| table.blob_file_ids())
            })
            .collect::<BTreeSet<_>>();
        let output_blob_ids = outputs
            .iter()
            .flat_map(|output| {
                output
                    .output
                    .tables
                    .iter()
                    .flat_map(|table| table.blob_file_ids())
            })
            .collect::<BTreeSet<_>>();

        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut outside_blob_ids = BTreeSet::new();
        for state in buckets.values() {
            for table in state.tables_snapshot()? {
                if input_table_ids.contains(&table.properties().id) {
                    continue;
                }
                outside_blob_ids.extend(table.blob_file_ids());
            }
        }

        Ok(input_blob_ids
            .difference(&output_blob_ids)
            .copied()
            .filter(|file_id| !outside_blob_ids.contains(file_id))
            .collect())
    }

    pub(in crate::db) fn retire_obsolete_table_files(
        &self,
        db_path: &Path,
        obsolete_tables: Vec<Arc<Table>>,
    ) -> Result<()> {
        self.enqueue_obsolete_tables(obsolete_tables)?;
        self.cleanup_pending_obsolete_table_files(db_path)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn retire_obsolete_table_files_native_async(
        &self,
        db_path: &Path,
        obsolete_tables: Vec<Arc<Table>>,
    ) -> Result<()> {
        self.enqueue_obsolete_tables(obsolete_tables)?;
        self.cleanup_pending_obsolete_table_files_native_async(db_path)
            .await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn retire_obsolete_table_files_browser_async(
        &self,
        db_path: &Path,
        obsolete_tables: Vec<Arc<Table>>,
    ) -> Result<()> {
        self.enqueue_obsolete_tables(obsolete_tables)?;
        self.cleanup_pending_obsolete_table_files_browser_async(db_path)
            .await
    }

    pub(in crate::db) fn enqueue_obsolete_tables(
        &self,
        obsolete_tables: Vec<Arc<Table>>,
    ) -> Result<()> {
        if obsolete_tables.is_empty() {
            return Ok(());
        }
        self.inner
            .pending_obsolete_tables
            .lock()
            .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?
            .extend(obsolete_tables);
        Ok(())
    }

    pub(in crate::db) fn requeue_obsolete_tables(
        &self,
        obsolete_tables: Vec<Arc<Table>>,
    ) -> Result<()> {
        self.inner
            .pending_obsolete_tables
            .lock()
            .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?
            .extend(obsolete_tables);
        Ok(())
    }

    pub(in crate::db) fn cleanup_pending_obsolete_table_files(&self, db_path: &Path) -> Result<()> {
        cleanup_pending_obsolete_table_files(
            &self.inner.native_storage,
            Some(db_path),
            &self.inner.pending_obsolete_tables,
        )
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn cleanup_pending_obsolete_table_files_native_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        let deletable = take_deletable_obsolete_tables(&self.inner.pending_obsolete_tables)?;
        if deletable.is_empty() {
            return Ok(());
        }

        let storage = self.inner.native_storage.clone();
        let mut result = Ok(());
        let mut remaining = Vec::new();
        for table in deletable {
            if result.is_ok() {
                match delete_storage_object_async(
                    &storage,
                    StorageObjectKind::Table,
                    &table::table_path(db_path, table.properties().id),
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        result = Err(error);
                        remaining.push(table);
                    }
                }
            } else {
                remaining.push(table);
            }
        }
        self.requeue_obsolete_tables(remaining)?;
        result
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn cleanup_pending_obsolete_table_files_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<()> {
        let deletable = take_deletable_obsolete_tables(&self.inner.pending_obsolete_tables)?;
        if deletable.is_empty() {
            return Ok(());
        }

        let storage = self.browser_storage()?;
        let mut result = Ok(());
        let mut remaining = Vec::new();
        for table in deletable {
            if result.is_ok() {
                match storage
                    .delete_object(StorageObjectId::native_file(
                        StorageObjectKind::Table,
                        table::table_path(db_path, table.properties().id),
                    ))
                    .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        result = Err(error);
                        remaining.push(table);
                    }
                }
            } else {
                remaining.push(table);
            }
        }
        self.requeue_obsolete_tables(remaining)?;
        result
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn remove_storage_files_browser_async(
        &self,
        db_path: &Path,
        table_ids: &[table::TableId],
    ) -> Result<()> {
        let storage = self.browser_storage()?;
        for table_id in table_ids {
            storage
                .delete_object(StorageObjectId::native_file(
                    StorageObjectKind::Table,
                    table::table_path(db_path, *table_id),
                ))
                .await?;
            storage
                .delete_object(StorageObjectId::native_file(
                    StorageObjectKind::Blob,
                    blob::blob_path(db_path, table_id.get()),
                ))
                .await?;
        }

        Ok(())
    }

    pub(in crate::db) fn l0_pressure_exceeded(&self) -> Result<bool> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        for state in buckets.values() {
            if state.l0_table_count()? > self.inner.options.max_l0_files {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(in crate::db) fn foreground_l0_overlap_pressure_exceeded(&self) -> Result<bool> {
        if self.background_workers_enabled() {
            return Ok(false);
        }

        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        for state in buckets.values() {
            // Overlapping L0 files force point reads to test newer misses before
            // reaching older hits. When background workers are disabled, public
            // flush is also the foreground maintenance boundary, so close that
            // overlap before read-heavy work starts.
            if state.l0_has_overlapping_tables()? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(in crate::db) fn record_compaction_stats_from_tables(
        &self,
        runs: usize,
        input_tables: &[Arc<Table>],
        output_tables: &[Arc<Table>],
        trigger_stats: &[CompactionTriggerStats],
    ) {
        let input_bytes = input_tables
            .iter()
            .map(|table| table.estimated_file_bytes())
            .sum::<u64>();
        let output_bytes = output_tables
            .iter()
            .map(|table| table.estimated_file_bytes())
            .sum::<u64>();

        self.inner
            .compaction_runs
            .fetch_add(usize_to_u64_saturating(runs), Ordering::AcqRel);
        self.inner.compaction_input_tables.fetch_add(
            usize_to_u64_saturating(input_tables.len()),
            Ordering::AcqRel,
        );
        self.inner.compaction_output_tables.fetch_add(
            usize_to_u64_saturating(output_tables.len()),
            Ordering::AcqRel,
        );
        self.inner
            .compaction_input_bytes
            .fetch_add(input_bytes, Ordering::AcqRel);
        self.inner
            .compaction_output_bytes
            .fetch_add(output_bytes, Ordering::AcqRel);
        self.record_compaction_level_stats(input_tables, output_tables);
        self.record_compaction_trigger_stats(trigger_stats);
    }

    pub(in crate::db) fn record_compaction_level_stats(
        &self,
        input_tables: &[Arc<Table>],
        output_tables: &[Arc<Table>],
    ) {
        let Ok(mut levels) = self.inner.compaction_level_stats.lock() else {
            return;
        };
        for table in input_tables {
            let properties = table.properties();
            let entry =
                levels
                    .entry(properties.level.get())
                    .or_insert_with(|| CompactionLevelStats {
                        level: properties.level.get(),
                        ..CompactionLevelStats::default()
                    });
            entry.input_tables = entry.input_tables.saturating_add(1);
            entry.input_bytes = entry
                .input_bytes
                .saturating_add(table.estimated_file_bytes());
        }
        for table in output_tables {
            let properties = table.properties();
            let entry =
                levels
                    .entry(properties.level.get())
                    .or_insert_with(|| CompactionLevelStats {
                        level: properties.level.get(),
                        ..CompactionLevelStats::default()
                    });
            entry.output_tables = entry.output_tables.saturating_add(1);
            entry.output_bytes = entry
                .output_bytes
                .saturating_add(table.estimated_file_bytes());
        }
    }

    pub(in crate::db) fn record_compaction_trigger_stats(&self, deltas: &[CompactionTriggerStats]) {
        let Ok(mut triggers) = self.inner.compaction_trigger_stats.lock() else {
            return;
        };
        for delta in deltas {
            let entry = triggers
                .entry(delta.trigger)
                .or_insert_with(|| CompactionTriggerStats {
                    trigger: delta.trigger,
                    runs: 0,
                    input_tables: 0,
                    output_tables: 0,
                    input_bytes: 0,
                    output_bytes: 0,
                });
            entry.runs = entry.runs.saturating_add(delta.runs);
            entry.input_tables = entry.input_tables.saturating_add(delta.input_tables);
            entry.output_tables = entry.output_tables.saturating_add(delta.output_tables);
            entry.input_bytes = entry.input_bytes.saturating_add(delta.input_bytes);
            entry.output_bytes = entry.output_bytes.saturating_add(delta.output_bytes);
        }
    }

    pub(in crate::db) fn record_compaction_skip(&self, skip: CompactionSkip) {
        let Ok(mut skips) = self.inner.compaction_skip_stats.lock() else {
            return;
        };
        let entry = skips.entry(skip).or_insert(CompactionSkipStats {
            skip,
            occurrences: 0,
        });
        entry.occurrences = entry.occurrences.saturating_add(1);
    }
}
