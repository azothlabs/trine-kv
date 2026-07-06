use super::*;

impl Table {
    #[must_use]
    pub(crate) const fn properties(&self) -> &TableProperties {
        &self.properties
    }

    /// Resident Bloom filter bytes held in memory for this table: the table-level
    /// filter (pinned shallow levels only) plus any resident data-block filters.
    ///
    /// This is the layered (Monkey-style) allocation memory metric. It measures
    /// filter memory actually held in RAM, the quantity that matters under a
    /// memory budget; lazily loaded deep filters that are not resident report
    /// zero because they are not currently consuming memory.
    pub(crate) fn resident_filter_bytes(&self) -> u64 {
        let point = self
            .point_key_filter
            .as_ref()
            .map_or(0, |filter| usize_to_u64_saturating(filter.bytes().len()));
        let prefix = self
            .prefix_filter
            .as_ref()
            .map_or(0, |filter| usize_to_u64_saturating(filter.bytes().len()));
        let block_level = self.data_blocks.as_ref().map_or(0, |blocks| {
            blocks.iter().map(TableDataBlock::filter_bytes).sum()
        });
        point.saturating_add(prefix).saturating_add(block_level)
    }

    pub(crate) fn point_records(&self) -> Result<Vec<TablePointRecord>> {
        self.all_point_records()
    }

    pub(crate) fn range_tombstones(&self) -> Result<Arc<RangeTombstoneIndex<TableRangeTombstone>>> {
        if let Ok(guard) = self.range_tombstones.read() {
            if let Some(tombstones) = guard.as_ref() {
                return Ok(Arc::clone(tombstones));
            }
        }

        let tombstones = RangeTombstoneIndex::new(self.load_range_tombstones()?);
        let tombstones = Arc::new(tombstones);
        let mut guard = self
            .range_tombstones
            .write()
            .map_err(|_| Error::Corruption {
                message: "table range tombstone cache lock poisoned".to_owned(),
            })?;
        let cached = guard.get_or_insert_with(|| Arc::clone(&tombstones));
        Ok(Arc::clone(cached))
    }

    pub(crate) async fn range_tombstones_async(
        &self,
    ) -> Result<Arc<RangeTombstoneIndex<TableRangeTombstone>>> {
        if let Ok(guard) = self.range_tombstones.read() {
            if let Some(tombstones) = guard.as_ref() {
                return Ok(Arc::clone(tombstones));
            }
        }

        let tombstones = RangeTombstoneIndex::new(self.load_range_tombstones_async().await?);
        let tombstones = Arc::new(tombstones);
        let mut guard = self
            .range_tombstones
            .write()
            .map_err(|_| Error::Corruption {
                message: "table range tombstone cache lock poisoned".to_owned(),
            })?;
        let cached = guard.get_or_insert_with(|| Arc::clone(&tombstones));
        Ok(Arc::clone(cached))
    }

    pub(crate) fn range_tombstone_covers_visible_point(
        &self,
        key: &[u8],
        point_sequence: Sequence,
        point_batch_index: u32,
        read_sequence: Sequence,
    ) -> Result<bool> {
        if !self.may_have_range_tombstones {
            return Ok(false);
        }

        let tombstones = self.range_tombstones()?;
        Ok(tombstones.covering_key(key).any(|tombstone| {
            tombstone.sequence <= read_sequence
                && (tombstone.sequence > point_sequence
                    || (tombstone.sequence == point_sequence
                        && tombstone.batch_index > point_batch_index))
        }))
    }

    pub(crate) async fn range_tombstone_covers_visible_point_async(
        &self,
        key: &[u8],
        point_sequence: Sequence,
        point_batch_index: u32,
        read_sequence: Sequence,
    ) -> Result<bool> {
        if !self.may_have_range_tombstones {
            return Ok(false);
        }

        let tombstones = self.range_tombstones_async().await?;
        Ok(tombstones.covering_key(key).any(|tombstone| {
            tombstone.sequence <= read_sequence
                && (tombstone.sequence > point_sequence
                    || (tombstone.sequence == point_sequence
                        && tombstone.batch_index > point_batch_index))
        }))
    }

    pub(crate) fn range_tombstones_overlapping_range(
        &self,
        range: &KeyRange,
    ) -> Result<Vec<TableRangeTombstone>> {
        if !self.may_have_range_tombstones {
            return Ok(Vec::new());
        }

        let tombstones = self.range_tombstones()?;
        Ok(tombstones.overlapping_range(range).cloned().collect())
    }

    pub(crate) async fn range_tombstones_overlapping_range_async(
        &self,
        range: &KeyRange,
    ) -> Result<Vec<TableRangeTombstone>> {
        if !self.may_have_range_tombstones {
            return Ok(Vec::new());
        }

        let tombstones = self.range_tombstones_async().await?;
        Ok(tombstones.overlapping_range(range).cloned().collect())
    }

    pub(crate) fn blob_file_ids(&self) -> Vec<u64> {
        self.properties.blob_file_ids.clone()
    }

    pub(crate) const fn may_have_range_tombstones(&self) -> bool {
        self.may_have_range_tombstones
    }

    pub(crate) fn estimated_file_bytes(&self) -> u64 {
        usize_to_u64_saturating(HEADER_LEN.saturating_add(self.payload_len))
    }

    pub(crate) fn filter_stats(&self) -> FilterStats {
        self.filter_stats.snapshot()
    }

    pub(crate) fn read_path_stats(&self) -> ReadPathStats {
        self.read_path_stats.snapshot()
    }

    pub(crate) fn record_point_table_probe(&self) {
        self.read_path_stats
            .record_point_table_probe(self.properties.level);
    }

    pub(crate) fn record_range_table_probe(&self) {
        self.read_path_stats
            .record_range_table_probe(self.properties.level);
    }

    pub(crate) fn record_range_tombstone_table_probe(&self) {
        self.read_path_stats.record_range_tombstone_table_probe();
    }

    pub(crate) fn record_prefix_table_probe(&self) {
        self.read_path_stats.record_prefix_table_probe();
    }

    pub(crate) fn record_prefix_tombstone_table_probe(&self) {
        self.read_path_stats.record_prefix_tombstone_table_probe();
    }

    pub(crate) fn record_prefix_filter_miss(&self) {
        self.read_path_stats.record_prefix_filter_miss();
    }

    pub(crate) fn with_manifest_properties(mut self, manifest: &TableProperties) -> Result<Self> {
        // Table files keep their original creation level. A manifest entry owns
        // the live level after a direct table move, but every other property
        // still has to match the file before recovery can trust it.
        let mut expected = self.properties.clone();
        expected.level = manifest.level;
        if &expected != manifest {
            return Err(Error::Corruption {
                message: format!(
                    "manifest properties do not match table {}",
                    manifest.id.get()
                ),
            });
        }
        self.properties.level = manifest.level;
        Ok(self)
    }

    pub(crate) fn clone_with_level(&self, level: TableLevel) -> Self {
        let mut properties = self.properties.clone();
        properties.level = level;
        Self {
            path: self.path.clone(),
            file: self.file.as_ref().map(Arc::clone),
            payload_len: self.payload_len,
            footer: self.footer.clone(),
            properties,
            point_records: self.point_records.clone(),
            data_blocks: self.data_blocks.clone(),
            data_block_count: self.data_block_count,
            index_partitions: self.index_partitions.clone(),
            index_partition_cache: Arc::clone(&self.index_partition_cache),
            range_tombstones: Arc::clone(&self.range_tombstones),
            may_have_range_tombstones: self.may_have_range_tombstones,
            point_key_filter: self.point_key_filter.clone(),
            prefix_filter: self.prefix_filter.clone(),
            filter_stats: Arc::clone(&self.filter_stats),
            read_path_stats: Arc::clone(&self.read_path_stats),
        }
    }

    #[must_use]
    pub(crate) fn has_key_bounds(&self) -> bool {
        !(self.data_block_count == 0
            && self.properties.smallest_user_key.is_empty()
            && self.properties.largest_user_key.is_empty())
    }

    #[cfg(test)]
    pub(crate) fn point_records_for_key(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
    ) -> Result<Vec<TablePointRecord>> {
        self.point_records_for_key_with_cache(key, policy, None)
    }

    pub(crate) fn point_records_for_key_with_cache(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Vec<TablePointRecord>> {
        let Some(start) = self.first_block_for_key(key, policy, block_cache)? else {
            return Ok(Vec::new());
        };

        let mut records = Vec::new();
        let mut block_index = start;
        while block_index < self.data_block_count {
            self.read_path_stats.record_point_block_metadata_probe();
            let decision = self.with_data_block_metadata(block_index, block_cache, |block| {
                if block.smallest_internal_key.user_key() > key {
                    return Ok(PointBlockDecision::Done);
                }
                let had_filter = block.point_key_filter.is_some();
                if !self.block_point_filter_allows(block, key) {
                    if had_filter {
                        self.read_path_stats.record_point_filter_miss();
                    }
                    return Ok(PointBlockDecision::Skip);
                }
                Ok(PointBlockDecision::Read { had_filter })
            })?;
            let PointBlockDecision::Read { had_filter } = decision else {
                if decision == PointBlockDecision::Done {
                    break;
                }
                block_index += 1;
                continue;
            };
            self.read_path_stats.record_point_data_block_read();
            let block = self.load_data_block(block_index, block_cache)?;
            let block_records = data_block_point_records_for_key(&block, key, policy)?;
            if had_filter && block_records.is_empty() {
                self.filter_stats.record_block_point_false_positive();
            }
            records.extend(block_records);
            block_index += 1;
        }
        if self.point_key_filter.is_some() && records.is_empty() {
            self.filter_stats.record_table_point_false_positive();
        }
        Ok(records)
    }

    #[cfg(test)]
    pub(crate) fn newest_visible_point_record_for_key_with_cache(
        &self,
        key: &[u8],
        read_sequence: Sequence,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<TablePointRecord>> {
        let Some(start) = self.first_block_for_key(key, policy, block_cache)? else {
            return Ok(None);
        };

        let mut saw_point_key = false;
        let mut block_index = start;
        while block_index < self.data_block_count {
            self.read_path_stats.record_point_block_metadata_probe();
            let decision = self.with_data_block_metadata(block_index, block_cache, |block| {
                if block.smallest_internal_key.user_key() > key {
                    return Ok(PointBlockDecision::Done);
                }
                let had_filter = block.point_key_filter.is_some();
                if !self.block_point_filter_allows(block, key) {
                    if had_filter {
                        self.read_path_stats.record_point_filter_miss();
                    }
                    return Ok(PointBlockDecision::Skip);
                }
                Ok(PointBlockDecision::Read { had_filter })
            })?;
            let PointBlockDecision::Read { had_filter } = decision else {
                if decision == PointBlockDecision::Done {
                    break;
                }
                block_index += 1;
                continue;
            };
            self.read_path_stats.record_point_data_block_read();
            let block = self.load_data_block(block_index, block_cache)?;
            let (block_has_key, record) =
                data_block_newest_visible_point_record_for_key(&block, key, read_sequence, policy)?;
            if !block_has_key {
                if had_filter {
                    self.filter_stats.record_block_point_false_positive();
                }
                block_index += 1;
                continue;
            }
            saw_point_key = true;
            if let Some(record) = record {
                return Ok(Some(record));
            }
            block_index += 1;
        }

        if self.point_key_filter.is_some() && !saw_point_key {
            self.filter_stats.record_table_point_false_positive();
        }
        Ok(None)
    }

    pub(crate) fn newest_visible_point_value_record_for_key_with_cache(
        &self,
        key: &[u8],
        read_sequence: Sequence,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<TablePointValueRecord>> {
        let Some(start) = self.first_block_for_key(key, policy, block_cache)? else {
            return Ok(None);
        };

        let mut saw_point_key = false;
        let mut block_index = start;
        while block_index < self.data_block_count {
            self.read_path_stats.record_point_block_metadata_probe();
            let decision = self.with_data_block_metadata(block_index, block_cache, |block| {
                if block.smallest_internal_key.user_key() > key {
                    return Ok(PointBlockDecision::Done);
                }
                let had_filter = block.point_key_filter.is_some();
                if !self.block_point_filter_allows(block, key) {
                    if had_filter {
                        self.read_path_stats.record_point_filter_miss();
                    }
                    return Ok(PointBlockDecision::Skip);
                }
                Ok(PointBlockDecision::Read { had_filter })
            })?;
            let PointBlockDecision::Read { had_filter } = decision else {
                if decision == PointBlockDecision::Done {
                    break;
                }
                block_index += 1;
                continue;
            };
            self.read_path_stats.record_point_data_block_read();
            let block = self.load_data_block(block_index, block_cache)?;
            let (block_has_key, record) = data_block_newest_visible_point_value_record_for_key(
                &block,
                key,
                read_sequence,
                policy,
            )?;
            if !block_has_key {
                if had_filter {
                    self.filter_stats.record_block_point_false_positive();
                }
                block_index += 1;
                continue;
            }
            saw_point_key = true;
            if let Some(record) = record {
                return Ok(Some(record));
            }
            block_index += 1;
        }

        if self.point_key_filter.is_some() && !saw_point_key {
            self.filter_stats.record_table_point_false_positive();
        }
        Ok(None)
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn newest_visible_point_value_records_for_keys_with_cache(
        &self,
        keys: &[&[u8]],
        read_sequence: Sequence,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Vec<Option<TablePointValueRecord>>> {
        let mut records = Vec::with_capacity(keys.len());
        records.resize_with(keys.len(), || None);
        let mut scans = Vec::new();
        let mut saw_point_key = vec![false; keys.len()];

        for (key_index, key) in keys.iter().enumerate() {
            if let Some(block_index) = self.first_block_for_key(key, policy, block_cache)? {
                scans.push(PointBatchScan {
                    key_index,
                    block_index,
                });
            }
        }

        while !scans.is_empty() {
            scans.sort_unstable_by_key(|scan| scan.block_index);
            let mut next_scans = Vec::new();
            let mut scan_index = 0;
            while scan_index < scans.len() {
                let block_index = scans[scan_index].block_index;
                let group_start = scan_index;
                while scan_index < scans.len() && scans[scan_index].block_index == block_index {
                    scan_index += 1;
                }

                self.read_path_stats.record_point_block_metadata_probe();
                let mut reads = Vec::new();
                self.with_data_block_metadata(block_index, block_cache, |block| {
                    for scan in &scans[group_start..scan_index] {
                        let key = keys[scan.key_index];
                        if block.smallest_internal_key.user_key() > key {
                            continue;
                        }
                        let had_filter = block.point_key_filter.is_some();
                        if !self.block_point_filter_allows(block, key) {
                            if had_filter {
                                self.read_path_stats.record_point_filter_miss();
                            }
                            if let Some(block_index) =
                                next_point_batch_block(scan.block_index, self.data_block_count)
                            {
                                next_scans.push(PointBatchScan {
                                    key_index: scan.key_index,
                                    block_index,
                                });
                            }
                            continue;
                        }
                        reads.push(PointBatchRead {
                            key_index: scan.key_index,
                            block_index: scan.block_index,
                            had_filter,
                        });
                    }
                    Ok(())
                })?;

                if reads.is_empty() {
                    continue;
                }

                self.read_path_stats.record_point_data_block_read();
                let block = self.load_data_block(block_index, block_cache)?;
                for read in reads {
                    let key = keys[read.key_index];
                    let (block_has_key, record) =
                        data_block_newest_visible_point_value_record_for_key(
                            &block,
                            key,
                            read_sequence,
                            policy,
                        )?;
                    if !block_has_key {
                        if read.had_filter {
                            self.filter_stats.record_block_point_false_positive();
                        }
                        if let Some(block_index) =
                            next_point_batch_block(read.block_index, self.data_block_count)
                        {
                            next_scans.push(PointBatchScan {
                                key_index: read.key_index,
                                block_index,
                            });
                        }
                        continue;
                    }
                    saw_point_key[read.key_index] = true;
                    if let Some(record) = record {
                        records[read.key_index] = Some(record);
                    } else if let Some(block_index) =
                        next_point_batch_block(read.block_index, self.data_block_count)
                    {
                        next_scans.push(PointBatchScan {
                            key_index: read.key_index,
                            block_index,
                        });
                    }
                }
            }
            scans = next_scans;
        }

        if self.point_key_filter.is_some() {
            for saw_point_key in saw_point_key {
                if !saw_point_key {
                    self.filter_stats.record_table_point_false_positive();
                }
            }
        }

        Ok(records)
    }

    pub(crate) async fn newest_visible_point_value_record_for_key_with_cache_async(
        &self,
        key: &[u8],
        read_sequence: Sequence,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<TablePointValueRecord>> {
        let Some(start) = self
            .first_block_for_key_async(key, policy, block_cache)
            .await?
        else {
            return Ok(None);
        };

        let mut saw_point_key = false;
        let mut block_index = start;
        while block_index < self.data_block_count {
            self.read_path_stats.record_point_block_metadata_probe();
            let decision = self
                .with_data_block_metadata_async(block_index, block_cache, |block| {
                    if block.smallest_internal_key.user_key() > key {
                        return Ok(PointBlockDecision::Done);
                    }
                    let had_filter = block.point_key_filter.is_some();
                    if !self.block_point_filter_allows(block, key) {
                        if had_filter {
                            self.read_path_stats.record_point_filter_miss();
                        }
                        return Ok(PointBlockDecision::Skip);
                    }
                    Ok(PointBlockDecision::Read { had_filter })
                })
                .await?;
            let PointBlockDecision::Read { had_filter } = decision else {
                if decision == PointBlockDecision::Done {
                    break;
                }
                block_index += 1;
                continue;
            };
            self.read_path_stats.record_point_data_block_read();
            let block = self.load_data_block_async(block_index, block_cache).await?;
            let (block_has_key, record) = data_block_newest_visible_point_value_record_for_key(
                &block,
                key,
                read_sequence,
                policy,
            )?;
            if !block_has_key {
                if had_filter {
                    self.filter_stats.record_block_point_false_positive();
                }
                block_index += 1;
                continue;
            }
            saw_point_key = true;
            if let Some(record) = record {
                return Ok(Some(record));
            }
            block_index += 1;
        }

        if self.point_key_filter.is_some() && !saw_point_key {
            self.filter_stats.record_table_point_false_positive();
        }
        Ok(None)
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn newest_visible_point_value_records_for_keys_with_cache_async(
        &self,
        keys: &[&[u8]],
        read_sequence: Sequence,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Vec<Option<TablePointValueRecord>>> {
        let mut records = Vec::with_capacity(keys.len());
        records.resize_with(keys.len(), || None);
        let mut scans = Vec::new();
        let mut saw_point_key = vec![false; keys.len()];

        for (key_index, key) in keys.iter().enumerate() {
            if let Some(block_index) = self
                .first_block_for_key_async(key, policy, block_cache)
                .await?
            {
                scans.push(PointBatchScan {
                    key_index,
                    block_index,
                });
            }
        }

        while !scans.is_empty() {
            scans.sort_unstable_by_key(|scan| scan.block_index);
            let mut next_scans = Vec::new();
            let mut scan_index = 0;
            while scan_index < scans.len() {
                let block_index = scans[scan_index].block_index;
                let group_start = scan_index;
                while scan_index < scans.len() && scans[scan_index].block_index == block_index {
                    scan_index += 1;
                }

                self.read_path_stats.record_point_block_metadata_probe();
                let mut reads = Vec::new();
                self.with_data_block_metadata_async(block_index, block_cache, |block| {
                    for scan in &scans[group_start..scan_index] {
                        let key = keys[scan.key_index];
                        if block.smallest_internal_key.user_key() > key {
                            continue;
                        }
                        let had_filter = block.point_key_filter.is_some();
                        if !self.block_point_filter_allows(block, key) {
                            if had_filter {
                                self.read_path_stats.record_point_filter_miss();
                            }
                            if let Some(block_index) =
                                next_point_batch_block(scan.block_index, self.data_block_count)
                            {
                                next_scans.push(PointBatchScan {
                                    key_index: scan.key_index,
                                    block_index,
                                });
                            }
                            continue;
                        }
                        reads.push(PointBatchRead {
                            key_index: scan.key_index,
                            block_index: scan.block_index,
                            had_filter,
                        });
                    }
                    Ok(())
                })
                .await?;

                if reads.is_empty() {
                    continue;
                }

                self.read_path_stats.record_point_data_block_read();
                let block = self.load_data_block_async(block_index, block_cache).await?;
                for read in reads {
                    let key = keys[read.key_index];
                    let (block_has_key, record) =
                        data_block_newest_visible_point_value_record_for_key(
                            &block,
                            key,
                            read_sequence,
                            policy,
                        )?;
                    if !block_has_key {
                        if read.had_filter {
                            self.filter_stats.record_block_point_false_positive();
                        }
                        if let Some(block_index) =
                            next_point_batch_block(read.block_index, self.data_block_count)
                        {
                            next_scans.push(PointBatchScan {
                                key_index: read.key_index,
                                block_index,
                            });
                        }
                        continue;
                    }
                    saw_point_key[read.key_index] = true;
                    if let Some(record) = record {
                        records[read.key_index] = Some(record);
                    } else if let Some(block_index) =
                        next_point_batch_block(read.block_index, self.data_block_count)
                    {
                        next_scans.push(PointBatchScan {
                            key_index: read.key_index,
                            block_index,
                        });
                    }
                }
            }
            scans = next_scans;
        }

        if self.point_key_filter.is_some() {
            for saw_point_key in saw_point_key {
                if !saw_point_key {
                    self.filter_stats.record_table_point_false_positive();
                }
            }
        }

        Ok(records)
    }

    #[cfg(test)]
    pub(crate) fn point_records_in_range(
        &self,
        range: &KeyRange,
        policy: IndexSearchPolicy,
    ) -> Result<Vec<TablePointRecord>> {
        self.point_records_in_range_with_cache(range, policy, None)
    }

    pub(crate) fn point_records_in_range_with_cache(
        &self,
        range: &KeyRange,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Vec<TablePointRecord>> {
        let Some(start) = self.first_block_for_range(range, policy, block_cache)? else {
            return Ok(Vec::new());
        };

        let mut records = Vec::new();
        let mut block_index = start;
        while block_index < self.data_block_count {
            let decision = self.with_data_block_metadata(block_index, block_cache, |block| {
                if key_is_after_end(block.smallest_internal_key.user_key(), &range.end) {
                    return Ok(RangeBlockDecision::Done);
                }
                if !block.overlaps_range(range) {
                    return Ok(RangeBlockDecision::Skip);
                }
                Ok(RangeBlockDecision::Read)
            })?;
            match decision {
                RangeBlockDecision::Done => break,
                RangeBlockDecision::Skip => {
                    block_index += 1;
                    continue;
                }
                RangeBlockDecision::Read => {}
            }
            let block = self.load_data_block(block_index, block_cache)?;
            records.extend(data_block_point_records_in_range(&block, range, policy)?);
            block_index += 1;
        }
        Ok(records)
    }

    #[cfg(test)]
    pub(crate) fn point_records_with_prefix(
        &self,
        prefix: &[u8],
        extractor: &PrefixExtractor,
        policy: IndexSearchPolicy,
    ) -> Result<Vec<TablePointRecord>> {
        self.point_records_with_prefix_with_cache(prefix, extractor, policy, None)
    }

    #[cfg(test)]
    pub(crate) fn point_records_with_prefix_with_cache(
        &self,
        prefix: &[u8],
        extractor: &PrefixExtractor,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Vec<TablePointRecord>> {
        let Some(start) = self.first_block_for_prefix(prefix, policy, block_cache)? else {
            return Ok(Vec::new());
        };

        let mut records = Vec::new();
        let mut block_index = start;
        while block_index < self.data_block_count {
            self.read_path_stats.record_prefix_block_metadata_probe();
            let decision = self.with_data_block_metadata(block_index, block_cache, |block| {
                if !block.prefix_bounds_may_overlap(prefix) {
                    return Ok(PrefixBlockDecision::Done);
                }
                let (allowed, had_filter) =
                    self.block_prefix_filter_allows(block, prefix, extractor);
                if !allowed {
                    if had_filter {
                        self.read_path_stats.record_prefix_filter_miss();
                    }
                    return Ok(PrefixBlockDecision::Skip);
                }
                Ok(PrefixBlockDecision::Read { had_filter })
            })?;
            let PrefixBlockDecision::Read { had_filter } = decision else {
                if decision == PrefixBlockDecision::Done {
                    break;
                }
                block_index += 1;
                continue;
            };
            self.read_path_stats.record_prefix_data_block_read();
            let block = self.load_data_block(block_index, block_cache)?;
            let block_records = data_block_point_records_with_prefix(&block, prefix, policy)?;
            if had_filter && block_records.is_empty() {
                self.filter_stats.record_block_prefix_false_positive();
            }
            records.extend(block_records);
            block_index += 1;
        }
        Ok(records)
    }

    #[must_use]
    pub(crate) fn may_contain_key(&self, key: &[u8]) -> bool {
        if !self.key_bounds_may_contain_key(key) {
            return false;
        }
        let Some(filter) = &self.point_key_filter else {
            return true;
        };
        let allowed = filter.may_contain_key(key);
        self.filter_stats.record_table_point(allowed);
        if !allowed {
            self.read_path_stats.record_point_filter_miss();
        }
        allowed
    }

    #[must_use]
    pub(crate) fn key_bounds_may_contain_key(&self, key: &[u8]) -> bool {
        if !self.has_key_bounds() {
            return self.may_have_range_tombstones;
        }
        self.properties.smallest_user_key.as_slice() <= key
            && key <= self.properties.largest_user_key.as_slice()
    }

    #[must_use]
    pub(crate) fn key_bounds_overlap_range(&self, range: &KeyRange) -> bool {
        if !self.has_key_bounds() {
            return self.may_have_range_tombstones;
        }
        !key_is_after_end(self.properties.smallest_user_key.as_slice(), &range.end)
            && !key_is_before_start(self.properties.largest_user_key.as_slice(), &range.start)
    }

    #[must_use]
    pub(crate) fn may_contain_prefix(&self, prefix: &[u8], extractor: &PrefixExtractor) -> bool {
        let Some(allowed) = self.table_prefix_filter_result(prefix, extractor) else {
            return true;
        };
        self.filter_stats.record_table_prefix(allowed);
        allowed
    }

    pub(super) fn table_prefix_filter_result(
        &self,
        prefix: &[u8],
        extractor: &PrefixExtractor,
    ) -> Option<bool> {
        let filter = self.prefix_filter.as_ref()?;
        if filter.extractor() != extractor {
            return None;
        }
        let filter_prefix = extractor.query_filter_prefix(prefix)?;
        Some(filter.may_contain_prefix(filter_prefix))
    }

    pub(super) fn block_point_filter_allows(&self, block: &TableDataBlock, key: &[u8]) -> bool {
        if !block.key_bounds_may_contain(key) {
            return false;
        }
        let Some(allowed) = block.point_filter_result(key) else {
            return true;
        };
        self.filter_stats.record_block_point(allowed);
        allowed
    }

    pub(super) fn block_prefix_filter_allows(
        &self,
        block: &TableDataBlock,
        prefix: &[u8],
        extractor: &PrefixExtractor,
    ) -> (bool, bool) {
        let Some(allowed) = block.prefix_filter_result(prefix, extractor) else {
            return (true, false);
        };
        self.filter_stats.record_block_prefix(allowed);
        (allowed, true)
    }
}
