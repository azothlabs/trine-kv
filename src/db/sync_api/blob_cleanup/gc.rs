#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use super::remove_storage_files_async;
use super::{
    Arc, BTreeSet, BlobGcCandidate, BlobGcRewritePlan, BlobGcRewriteRecord, BlobGcRewriteTable, Db,
    Error, LsmCompactionOutput, NamedCompactionOutput, Ordering, Path, Result, ValueRef,
    apply_blob_gc_indexes, blob, blob_gc_blob_records, blob_gc_table_write_options, lock_poisoned,
    remove_storage_files, table, write_blob_gc_replacement_tables,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::DurabilityMode;

impl Db {
    pub(in crate::db) fn run_blob_gc_once_locked(&self, db_path: &Path) -> Result<()> {
        let Some(plan) = self.build_blob_gc_rewrite_plan(db_path)? else {
            return Ok(());
        };

        let input_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes)
        });
        let discarded_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes.saturating_sub(candidate.live_bytes))
        });
        let obsolete_blob_ids = plan
            .candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<Vec<_>>();

        let header = blob::BlobFileHeader::new(
            plan.new_blob_file_id,
            self.last_committed_sequence(),
            1,
            crate::codec::CodecId::None,
        );
        let blob_records = blob_gc_blob_records(&plan.records);

        let written_table_ids = plan
            .tables
            .iter()
            .map(|table| table.output_table_id)
            .collect::<Vec<_>>();
        let indexes = match blob::write_blob_file_with_backend(
            &self.inner.native_storage,
            db_path,
            plan.new_blob_file_id,
            header,
            &blob_records,
        ) {
            Ok(indexes) => indexes,
            Err(error) => {
                let _ =
                    remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
                return Err(error);
            }
        };

        let mut tables = plan.tables;
        let output_bytes = match apply_blob_gc_indexes(&mut tables, plan.records, indexes) {
            Ok(output_bytes) => output_bytes,
            Err(error) => {
                let _ =
                    remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
                return Err(error);
            }
        };
        let outputs =
            match write_blob_gc_replacement_tables(&self.inner.native_storage, db_path, tables) {
                Ok(outputs) => outputs,
                Err(error) => {
                    let _ = remove_storage_files(
                        &self.inner.native_storage,
                        db_path,
                        &written_table_ids,
                    );
                    return Err(error);
                }
            };

        if let Err(error) = self.sync_filesystem_directory_after_renames(db_path) {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            return Err(error);
        }

        if let Err(error) = self.publish_compacted_tables(&outputs, &obsolete_blob_ids) {
            let _ = remove_storage_files(&self.inner.native_storage, db_path, &written_table_ids);
            return Err(error);
        }

        let obsolete_tables = self
            .install_compacted_tables(outputs)
            .map_err(|error| self.close_after_durable_publish_error("blob GC", &error))?;
        self.retire_obsolete_table_files(db_path, obsolete_tables)?;
        self.inner.blob_gc_runs.fetch_add(1, Ordering::AcqRel);
        self.inner
            .blob_gc_input_bytes
            .fetch_add(input_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_output_bytes
            .fetch_add(output_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_discarded_bytes
            .fetch_add(discarded_bytes, Ordering::AcqRel);
        self.delete_pending_obsolete_blob_files(db_path)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    #[allow(clippy::too_many_lines)]
    pub(in crate::db) async fn run_blob_gc_once_native_async(&self, db_path: &Path) -> Result<()> {
        let Some(plan) = self
            .build_blob_gc_rewrite_plan_native_async(db_path)
            .await?
        else {
            return Ok(());
        };

        let input_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes)
        });
        let discarded_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes.saturating_sub(candidate.live_bytes))
        });
        let obsolete_blob_ids = plan
            .candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<Vec<_>>();

        let header = blob::BlobFileHeader::new(
            plan.new_blob_file_id,
            self.last_committed_sequence(),
            1,
            crate::codec::CodecId::None,
        );
        let blob_records = blob_gc_blob_records(&plan.records);
        let written_table_ids = plan
            .tables
            .iter()
            .map(|table| table.output_table_id)
            .collect::<Vec<_>>();
        let storage = self.inner.native_storage.clone();
        let indexes = match blob::write_blob_file_with_backend_async(
            &storage,
            db_path,
            plan.new_blob_file_id,
            header,
            &blob_records,
            self.filesystem_publish_durability(),
        )
        .await
        {
            Ok(indexes) => indexes,
            Err(error) => {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                return Err(error);
            }
        };

        let mut tables = plan.tables;
        let output_bytes = match apply_blob_gc_indexes(&mut tables, plan.records, indexes) {
            Ok(output_bytes) => output_bytes,
            Err(error) => {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                return Err(error);
            }
        };
        let outputs = match self
            .write_blob_gc_replacement_tables_native_async(db_path, tables)
            .await
        {
            Ok(outputs) => outputs,
            Err(error) => {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .sync_filesystem_directory_after_renames_async(db_path)
            .await
        {
            let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
            return Err(error);
        }

        if let Err(error) = self
            .publish_compacted_tables_native_async(&outputs, &obsolete_blob_ids)
            .await
        {
            if !self.closed_after_durable_publish_error() {
                let _ = remove_storage_files_async(&storage, db_path, &written_table_ids).await;
            }
            return Err(error);
        }

        let obsolete_tables = self
            .install_compacted_tables(outputs)
            .map_err(|error| self.close_after_durable_publish_error("blob GC", &error))?;
        self.retire_obsolete_table_files_native_async(db_path, obsolete_tables)
            .await?;
        self.inner.blob_gc_runs.fetch_add(1, Ordering::AcqRel);
        self.inner
            .blob_gc_input_bytes
            .fetch_add(input_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_output_bytes
            .fetch_add(output_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_discarded_bytes
            .fetch_add(discarded_bytes, Ordering::AcqRel);
        let _ = self
            .delete_pending_obsolete_blob_files_native_async(db_path)
            .await?;
        Ok(())
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    #[allow(clippy::too_many_lines)]
    pub(in crate::db) async fn run_blob_gc_once_browser_async(&self, db_path: &Path) -> Result<()> {
        let Some(plan) = self
            .build_blob_gc_rewrite_plan_browser_async(db_path)
            .await?
        else {
            return Ok(());
        };

        let input_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes)
        });
        let discarded_bytes = plan.candidates.iter().fold(0_u64, |bytes, candidate| {
            bytes.saturating_add(candidate.total_bytes.saturating_sub(candidate.live_bytes))
        });
        let obsolete_blob_ids = plan
            .candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<Vec<_>>();

        let header = blob::BlobFileHeader::new(
            plan.new_blob_file_id,
            self.last_committed_sequence(),
            1,
            crate::codec::CodecId::None,
        );
        let blob_records = blob_gc_blob_records(&plan.records);
        let written_table_ids = plan
            .tables
            .iter()
            .map(|table| table.output_table_id)
            .collect::<Vec<_>>();
        let storage = self.browser_storage()?;
        let indexes = match blob::write_blob_file_with_backend_async(
            &storage,
            db_path,
            plan.new_blob_file_id,
            header,
            &blob_records,
            DurabilityMode::Flush,
        )
        .await
        {
            Ok(indexes) => indexes,
            Err(error) => {
                let _ = self
                    .remove_storage_files_browser_async(db_path, &written_table_ids)
                    .await;
                return Err(error);
            }
        };

        let mut tables = plan.tables;
        let output_bytes = match apply_blob_gc_indexes(&mut tables, plan.records, indexes) {
            Ok(output_bytes) => output_bytes,
            Err(error) => {
                let _ = self
                    .remove_storage_files_browser_async(db_path, &written_table_ids)
                    .await;
                return Err(error);
            }
        };
        let outputs = match self
            .write_blob_gc_replacement_tables_browser_async(db_path, tables)
            .await
        {
            Ok(outputs) => outputs,
            Err(error) => {
                let _ = self
                    .remove_storage_files_browser_async(db_path, &written_table_ids)
                    .await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .publish_compacted_tables_browser_async(&outputs, &obsolete_blob_ids)
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
            .install_compacted_tables(outputs)
            .map_err(|error| self.close_after_durable_publish_error("blob GC", &error))?;
        self.retire_obsolete_table_files_browser_async(db_path, obsolete_tables)
            .await?;
        self.inner.blob_gc_runs.fetch_add(1, Ordering::AcqRel);
        self.inner
            .blob_gc_input_bytes
            .fetch_add(input_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_output_bytes
            .fetch_add(output_bytes, Ordering::AcqRel);
        self.inner
            .blob_gc_discarded_bytes
            .fetch_add(discarded_bytes, Ordering::AcqRel);
        let _ = self
            .delete_pending_obsolete_blob_files_browser_async(db_path)
            .await?;
        Ok(())
    }

    pub(in crate::db) fn build_blob_gc_rewrite_plan(
        &self,
        db_path: &Path,
    ) -> Result<Option<BlobGcRewritePlan>> {
        let candidates = self.choose_blob_gc_candidates(db_path)?;
        if candidates.is_empty() {
            return Ok(None);
        }
        let candidate_file_ids = candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<BTreeSet<_>>();

        let mut next_table_id = self.next_table_id()?;
        let new_blob_file_id = next_table_id.get();
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut tables = Vec::new();
        let mut rewrite_records = Vec::new();

        for (bucket, tree) in buckets.iter() {
            for table in tree.tables_snapshot()? {
                if !table
                    .blob_file_ids()
                    .iter()
                    .any(|file_id| candidate_file_ids.contains(file_id))
                {
                    continue;
                }
                let output_table_id = next_table_id;
                next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                    message: "table id counter overflow".to_owned(),
                })?;

                let table_index = tables.len();
                let point_records = table.point_records()?;
                for (record_index, point_record) in point_records.iter().enumerate() {
                    let Some(ValueRef::BlobIndex(index)) = point_record.value.as_ref() else {
                        continue;
                    };
                    if !candidate_file_ids.contains(&index.file_id) {
                        continue;
                    }
                    let blob_record = blob::read_record_for_index_with_backend(
                        &self.inner.native_storage,
                        db_path,
                        index,
                        Some(&point_record.internal_key),
                    )?;
                    rewrite_records.push(BlobGcRewriteRecord {
                        internal_key: point_record.internal_key.clone(),
                        value: blob_record.record.value.clone(),
                        compression: blob_record.record.compression,
                        table_index,
                        record_index,
                    });
                }

                tables.push(BlobGcRewriteTable {
                    bucket: bucket.clone(),
                    input_table_id: table.properties().id,
                    output_table_id,
                    level: table.properties().level,
                    options: blob_gc_table_write_options(&tree.options),
                    point_records,
                    range_tombstones: table.range_tombstones()?.all().to_vec(),
                });
            }
        }
        drop(buckets);

        if rewrite_records.is_empty() {
            return Ok(None);
        }
        rewrite_records.sort_by(|left, right| left.internal_key.cmp(&right.internal_key));

        Ok(Some(BlobGcRewritePlan {
            candidates,
            new_blob_file_id,
            tables,
            records: rewrite_records,
        }))
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn build_blob_gc_rewrite_plan_native_async(
        &self,
        db_path: &Path,
    ) -> Result<Option<BlobGcRewritePlan>> {
        let candidates = self.choose_blob_gc_candidates_native_async(db_path).await?;
        if candidates.is_empty() {
            return Ok(None);
        }
        let candidate_file_ids = candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<BTreeSet<_>>();

        let storage = self.inner.native_storage.clone();
        let mut next_table_id = self.next_table_id()?;
        let new_blob_file_id = next_table_id.get();
        let mut tables = Vec::new();
        let mut rewrite_sources = Vec::new();
        {
            let buckets = self
                .inner
                .buckets
                .read()
                .map_err(|_| lock_poisoned("bucket registry"))?;

            for (bucket, tree) in buckets.iter() {
                for table in tree.tables_snapshot()? {
                    if !table
                        .blob_file_ids()
                        .iter()
                        .any(|file_id| candidate_file_ids.contains(file_id))
                    {
                        continue;
                    }
                    let output_table_id = next_table_id;
                    next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                        message: "table id counter overflow".to_owned(),
                    })?;

                    let table_index = tables.len();
                    let point_records = table.point_records()?;
                    for (record_index, point_record) in point_records.iter().enumerate() {
                        let Some(ValueRef::BlobIndex(index)) = point_record.value.as_ref() else {
                            continue;
                        };
                        if !candidate_file_ids.contains(&index.file_id) {
                            continue;
                        }
                        rewrite_sources.push((
                            point_record.internal_key.clone(),
                            *index,
                            table_index,
                            record_index,
                        ));
                    }

                    tables.push(BlobGcRewriteTable {
                        bucket: bucket.clone(),
                        input_table_id: table.properties().id,
                        output_table_id,
                        level: table.properties().level,
                        options: blob_gc_table_write_options(&tree.options),
                        point_records,
                        range_tombstones: table.range_tombstones()?.all().to_vec(),
                    });
                }
            }
        }

        if rewrite_sources.is_empty() {
            return Ok(None);
        }

        let mut rewrite_records = Vec::with_capacity(rewrite_sources.len());
        for (internal_key, index, table_index, record_index) in rewrite_sources {
            let blob_record = blob::read_record_for_index_with_backend_async(
                &storage,
                db_path,
                &index,
                Some(&internal_key),
            )
            .await?;
            rewrite_records.push(BlobGcRewriteRecord {
                internal_key,
                value: blob_record.record.value.clone(),
                compression: blob_record.record.compression,
                table_index,
                record_index,
            });
        }
        rewrite_records.sort_by(|left, right| left.internal_key.cmp(&right.internal_key));

        Ok(Some(BlobGcRewritePlan {
            candidates,
            new_blob_file_id,
            tables,
            records: rewrite_records,
        }))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn build_blob_gc_rewrite_plan_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<Option<BlobGcRewritePlan>> {
        let candidates = self
            .choose_blob_gc_candidates_browser_async(db_path)
            .await?;
        if candidates.is_empty() {
            return Ok(None);
        }
        let candidate_file_ids = candidates
            .iter()
            .map(|candidate| candidate.file_id)
            .collect::<BTreeSet<_>>();

        let storage = self.browser_storage()?;
        let mut next_table_id = self.next_table_id()?;
        let new_blob_file_id = next_table_id.get();
        let mut tables = Vec::new();
        let mut rewrite_sources = Vec::new();
        {
            let buckets = self
                .inner
                .buckets
                .read()
                .map_err(|_| lock_poisoned("bucket registry"))?;

            for (bucket, tree) in buckets.iter() {
                for table in tree.tables_snapshot()? {
                    if !table
                        .blob_file_ids()
                        .iter()
                        .any(|file_id| candidate_file_ids.contains(file_id))
                    {
                        continue;
                    }
                    let output_table_id = next_table_id;
                    next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                        message: "table id counter overflow".to_owned(),
                    })?;

                    let table_index = tables.len();
                    let point_records = table.point_records()?;
                    for (record_index, point_record) in point_records.iter().enumerate() {
                        let Some(ValueRef::BlobIndex(index)) = point_record.value.as_ref() else {
                            continue;
                        };
                        if !candidate_file_ids.contains(&index.file_id) {
                            continue;
                        }
                        rewrite_sources.push((
                            point_record.internal_key.clone(),
                            *index,
                            table_index,
                            record_index,
                        ));
                    }

                    tables.push(BlobGcRewriteTable {
                        bucket: bucket.clone(),
                        input_table_id: table.properties().id,
                        output_table_id,
                        level: table.properties().level,
                        options: blob_gc_table_write_options(&tree.options),
                        point_records,
                        range_tombstones: table.range_tombstones()?.all().to_vec(),
                    });
                }
            }
        }

        if rewrite_sources.is_empty() {
            return Ok(None);
        }

        let mut rewrite_records = Vec::with_capacity(rewrite_sources.len());
        for (internal_key, index, table_index, record_index) in rewrite_sources {
            let blob_record = blob::read_record_for_index_with_backend_async(
                &storage,
                db_path,
                &index,
                Some(&internal_key),
            )
            .await?;
            rewrite_records.push(BlobGcRewriteRecord {
                internal_key,
                value: blob_record.record.value.clone(),
                compression: blob_record.record.compression,
                table_index,
                record_index,
            });
        }
        rewrite_records.sort_by(|left, right| left.internal_key.cmp(&right.internal_key));

        Ok(Some(BlobGcRewritePlan {
            candidates,
            new_blob_file_id,
            tables,
            records: rewrite_records,
        }))
    }

    pub(in crate::db) fn choose_blob_gc_candidates(
        &self,
        db_path: &Path,
    ) -> Result<Vec<BlobGcCandidate>> {
        let live_bytes_by_file = self.live_blob_bytes_by_file()?;
        let mut candidates = Vec::new();

        for (file_id, live_bytes) in live_bytes_by_file {
            let properties = blob::read_blob_file_properties_with_backend(
                &self.inner.native_storage,
                db_path,
                file_id,
            )?;
            let total_bytes = properties.encoded_bytes;
            if total_bytes < self.inner.options.blob_gc_min_file_bytes {
                continue;
            }
            let discardable_bytes = total_bytes.saturating_sub(live_bytes);
            if discardable_bytes == 0
                || !self
                    .inner
                    .options
                    .blob_gc_discardable_ratio
                    .should_collect(discardable_bytes, total_bytes)
            {
                continue;
            }

            candidates.push(BlobGcCandidate {
                file_id,
                total_bytes,
                live_bytes,
            });
        }
        candidates.sort_by(|left, right| {
            let left_discardable = left.total_bytes.saturating_sub(left.live_bytes);
            let right_discardable = right.total_bytes.saturating_sub(right.live_bytes);
            right_discardable.cmp(&left_discardable)
        });

        Ok(candidates)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn choose_blob_gc_candidates_native_async(
        &self,
        db_path: &Path,
    ) -> Result<Vec<BlobGcCandidate>> {
        let live_bytes_by_file = self.live_blob_bytes_by_file()?;
        let storage = self.inner.native_storage.clone();
        let mut candidates = Vec::new();

        for (file_id, live_bytes) in live_bytes_by_file {
            let properties =
                blob::read_blob_file_properties_with_backend_async(&storage, db_path, file_id)
                    .await?;
            let total_bytes = properties.encoded_bytes;
            if total_bytes < self.inner.options.blob_gc_min_file_bytes {
                continue;
            }
            let discardable_bytes = total_bytes.saturating_sub(live_bytes);
            if discardable_bytes == 0
                || !self
                    .inner
                    .options
                    .blob_gc_discardable_ratio
                    .should_collect(discardable_bytes, total_bytes)
            {
                continue;
            }

            candidates.push(BlobGcCandidate {
                file_id,
                total_bytes,
                live_bytes,
            });
        }
        candidates.sort_by(|left, right| {
            let left_discardable = left.total_bytes.saturating_sub(left.live_bytes);
            let right_discardable = right.total_bytes.saturating_sub(right.live_bytes);
            right_discardable.cmp(&left_discardable)
        });

        Ok(candidates)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn choose_blob_gc_candidates_browser_async(
        &self,
        db_path: &Path,
    ) -> Result<Vec<BlobGcCandidate>> {
        let live_bytes_by_file = self.live_blob_bytes_by_file()?;
        let storage = self.browser_storage()?;
        let mut candidates = Vec::new();

        for (file_id, live_bytes) in live_bytes_by_file {
            let properties =
                blob::read_blob_file_properties_with_backend_async(&storage, db_path, file_id)
                    .await?;
            let total_bytes = properties.encoded_bytes;
            if total_bytes < self.inner.options.blob_gc_min_file_bytes {
                continue;
            }
            let discardable_bytes = total_bytes.saturating_sub(live_bytes);
            if discardable_bytes == 0
                || !self
                    .inner
                    .options
                    .blob_gc_discardable_ratio
                    .should_collect(discardable_bytes, total_bytes)
            {
                continue;
            }

            candidates.push(BlobGcCandidate {
                file_id,
                total_bytes,
                live_bytes,
            });
        }
        candidates.sort_by(|left, right| {
            let left_discardable = left.total_bytes.saturating_sub(left.live_bytes);
            let right_discardable = right.total_bytes.saturating_sub(right.live_bytes);
            right_discardable.cmp(&left_discardable)
        });

        Ok(candidates)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(in crate::db) async fn write_blob_gc_replacement_tables_native_async(
        &self,
        db_path: &Path,
        tables: Vec<BlobGcRewriteTable>,
    ) -> Result<Vec<NamedCompactionOutput>> {
        let storage = self.inner.native_storage.clone();
        let mut outputs = Vec::with_capacity(tables.len());
        for rewrite_table in tables {
            let table_path = table::table_path(db_path, rewrite_table.output_table_id);
            let point_records = rewrite_table
                .point_records
                .iter()
                .map(|record| (record.internal_key.clone(), record.value.clone()))
                .collect::<Vec<_>>();
            let table = Arc::new(
                table::write_table_with_backend_async(
                    &storage,
                    &table_path,
                    rewrite_table.output_table_id,
                    rewrite_table.level,
                    &rewrite_table.options,
                    &point_records,
                    &rewrite_table.range_tombstones,
                    self.filesystem_publish_durability(),
                )
                .await?,
            );

            outputs.push(NamedCompactionOutput {
                bucket: rewrite_table.bucket,
                trigger: None,
                output: LsmCompactionOutput {
                    input_table_ids: vec![rewrite_table.input_table_id],
                    tables: vec![table],
                },
            });
        }

        Ok(outputs)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(in crate::db) async fn write_blob_gc_replacement_tables_browser_async(
        &self,
        db_path: &Path,
        tables: Vec<BlobGcRewriteTable>,
    ) -> Result<Vec<NamedCompactionOutput>> {
        let storage = self.browser_storage()?;
        let mut outputs = Vec::with_capacity(tables.len());
        for rewrite_table in tables {
            let table_path = table::table_path(db_path, rewrite_table.output_table_id);
            let point_records = rewrite_table
                .point_records
                .iter()
                .map(|record| (record.internal_key.clone(), record.value.clone()))
                .collect::<Vec<_>>();
            let table = Arc::new(
                table::write_table_with_backend_async(
                    &storage,
                    &table_path,
                    rewrite_table.output_table_id,
                    rewrite_table.level,
                    &rewrite_table.options,
                    &point_records,
                    &rewrite_table.range_tombstones,
                    DurabilityMode::Flush,
                )
                .await?,
            );

            outputs.push(NamedCompactionOutput {
                bucket: rewrite_table.bucket,
                trigger: None,
                output: LsmCompactionOutput {
                    input_table_ids: vec![rewrite_table.input_table_id],
                    tables: vec![table],
                },
            });
        }

        Ok(outputs)
    }
}
