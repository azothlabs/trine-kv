use super::{
    Arc, BlockCache, BlockCacheKey, CacheKind, DataBlockIndexEntry, DecodedDataBlock, Direction,
    Error, INDEX_PARTITION_TARGET_ENTRIES, IndexPartitionEntry, IndexSearchPolicy, KeyRange,
    PrefixExtractor, Result, ScanSelector, Table, TableDataBlock, TablePointCursor,
    TablePointRecord, TableRangeTombstone, TableSection, decode_range_tombstone_block,
    invalid_table, key_is_after_end, key_is_before_start, read_data_block_from_file,
    read_data_block_from_file_async, read_index_partition_from_file_async,
    read_index_partition_from_source, read_single_block_section_from_file_shared,
    read_single_block_section_from_file_shared_async, search, should_pin_read_metadata,
    table_read_source, validate_block_codec, validate_sorted_point_records,
};

impl Table {
    pub(super) fn all_point_records(&self) -> Result<Vec<TablePointRecord>> {
        if let Some(records) = &self.point_records {
            return Ok(records.clone());
        }

        let mut records = Vec::new();
        for block_index in 0..self.data_block_count {
            let block = self.load_data_block(block_index, None)?;
            records.extend(block.records_owned()?);
        }
        validate_sorted_point_records(&records)?;
        Ok(records)
    }

    pub(super) fn load_range_tombstones(&self) -> Result<Vec<TableRangeTombstone>> {
        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table range tombstones are not loaded".to_owned(),
            });
        };
        let block = read_single_block_section_from_file_shared(
            path,
            self.file.as_deref(),
            self.payload_len,
            self.footer.range_tombstones,
        )?;
        let codec = block.codec();
        validate_block_codec(codec, self.properties.codec, TableSection::RangeTombstones)?;
        decode_range_tombstone_block(block.payload())
    }

    pub(super) async fn load_range_tombstones_async(&self) -> Result<Vec<TableRangeTombstone>> {
        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table range tombstones are not loaded".to_owned(),
            });
        };
        let block = read_single_block_section_from_file_shared_async(
            path,
            self.file.as_deref(),
            self.payload_len,
            self.footer.range_tombstones,
        )
        .await?;
        let codec = block.codec();
        validate_block_codec(codec, self.properties.codec, TableSection::RangeTombstones)?;
        decode_range_tombstone_block(block.payload())
    }

    pub(super) fn with_data_block_metadata<R>(
        &self,
        block_index: usize,
        block_cache: Option<&BlockCache>,
        read: impl FnOnce(&TableDataBlock) -> Result<R>,
    ) -> Result<R> {
        if let Some(blocks) = &self.data_blocks {
            let block = blocks
                .get(block_index)
                .ok_or_else(|| invalid_table("data block index outside table"))?;
            return read(block);
        }

        let partition_index = self.index_partition_index_for_block(block_index)?;
        let partition = self.load_index_partition(partition_index, block_cache)?;
        let partition_entry = self
            .index_partitions
            .get(partition_index)
            .ok_or_else(|| invalid_table("index partition outside table"))?;
        let local_index = block_index
            .checked_sub(partition_entry.first_data_block_index)
            .ok_or_else(|| invalid_table("data block index before partition"))?;
        let block = partition
            .get(local_index)
            .ok_or_else(|| invalid_table("data block index outside partition"))?;
        read(block)
    }

    pub(super) async fn with_data_block_metadata_async<R>(
        &self,
        block_index: usize,
        block_cache: Option<&BlockCache>,
        read: impl FnOnce(&TableDataBlock) -> Result<R>,
    ) -> Result<R> {
        if let Some(blocks) = &self.data_blocks {
            let block = blocks
                .get(block_index)
                .ok_or_else(|| invalid_table("data block index outside table"))?;
            return read(block);
        }

        let partition_index = self.index_partition_index_for_block(block_index)?;
        let partition = self
            .load_index_partition_async(partition_index, block_cache)
            .await?;
        let partition_entry = self
            .index_partitions
            .get(partition_index)
            .ok_or_else(|| invalid_table("index partition outside table"))?;
        let local_index = block_index
            .checked_sub(partition_entry.first_data_block_index)
            .ok_or_else(|| invalid_table("data block index before partition"))?;
        let block = partition
            .get(local_index)
            .ok_or_else(|| invalid_table("data block index outside partition"))?;
        read(block)
    }

    pub(super) fn load_index_partition(
        &self,
        partition_index: usize,
        block_cache: Option<&BlockCache>,
    ) -> Result<Arc<Vec<TableDataBlock>>> {
        if let Ok(guard) = self.index_partition_cache.read() {
            if let Some(partition) = guard.get(&partition_index) {
                return Ok(Arc::clone(partition));
            }
        }

        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table index partition has no file path".to_owned(),
            });
        };
        let load_partition = || {
            let partition_entry = self
                .index_partitions
                .get(partition_index)
                .ok_or_else(|| invalid_table("index partition outside table"))?;
            let source = table_read_source(path, self.file.as_deref());
            read_index_partition_from_source(
                &source,
                self.payload_len,
                self.footer.data_blocks,
                self.properties.codec,
                partition_entry,
            )
        };
        let partition = if should_pin_read_metadata(self.properties.level) {
            Arc::new(load_partition()?)
        } else if let Some(block_cache) = block_cache {
            let key = BlockCacheKey::with_kind(
                CacheKind::IndexBlock,
                self.properties.id,
                partition_index,
            );
            block_cache.get_or_insert_index_partition_with(key, load_partition)?
        } else {
            Arc::new(load_partition()?)
        };

        if should_pin_read_metadata(self.properties.level) {
            let mut guard = self
                .index_partition_cache
                .write()
                .map_err(|_| Error::Corruption {
                    message: "table index partition cache lock poisoned".to_owned(),
                })?;
            let cached = guard
                .entry(partition_index)
                .or_insert_with(|| Arc::clone(&partition));
            return Ok(Arc::clone(cached));
        }

        Ok(partition)
    }

    pub(super) async fn load_index_partition_async(
        &self,
        partition_index: usize,
        block_cache: Option<&BlockCache>,
    ) -> Result<Arc<Vec<TableDataBlock>>> {
        if let Ok(guard) = self.index_partition_cache.read() {
            if let Some(partition) = guard.get(&partition_index) {
                return Ok(Arc::clone(partition));
            }
        }

        let partition = if should_pin_read_metadata(self.properties.level) {
            Arc::new(self.read_index_partition_async(partition_index).await?)
        } else if let Some(block_cache) = block_cache {
            let key = BlockCacheKey::with_kind(
                CacheKind::IndexBlock,
                self.properties.id,
                partition_index,
            );
            block_cache
                .get_or_insert_index_partition_with_async(key, || async {
                    self.read_index_partition_async(partition_index).await
                })
                .await?
        } else {
            Arc::new(self.read_index_partition_async(partition_index).await?)
        };

        if should_pin_read_metadata(self.properties.level) {
            let mut guard = self
                .index_partition_cache
                .write()
                .map_err(|_| Error::Corruption {
                    message: "table index partition cache lock poisoned".to_owned(),
                })?;
            let cached = guard
                .entry(partition_index)
                .or_insert_with(|| Arc::clone(&partition));
            return Ok(Arc::clone(cached));
        }

        Ok(partition)
    }

    pub(super) async fn read_index_partition_async(
        &self,
        partition_index: usize,
    ) -> Result<Vec<TableDataBlock>> {
        let partition_entry = self
            .index_partitions
            .get(partition_index)
            .ok_or_else(|| invalid_table("index partition outside table"))?;
        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table index partition has no file path".to_owned(),
            });
        };
        read_index_partition_from_file_async(
            path,
            self.file.as_deref(),
            self.payload_len,
            self.footer.data_blocks,
            self.properties.codec,
            partition_entry,
        )
        .await
    }

    pub(super) fn index_partition_index_for_block(&self, block_index: usize) -> Result<usize> {
        if block_index >= self.data_block_count {
            return Err(invalid_table("data block index outside table"));
        }
        let expected = block_index / INDEX_PARTITION_TARGET_ENTRIES;
        if self.partition_contains_block(expected, block_index) {
            return Ok(expected);
        }
        let upper = self
            .index_partitions
            .partition_point(|partition| partition.first_data_block_index <= block_index);
        let Some(index) = upper.checked_sub(1) else {
            return Err(invalid_table("data block index has no partition"));
        };
        self.partition_contains_block(index, block_index)
            .then_some(index)
            .ok_or_else(|| invalid_table("data block index has no partition"))
    }

    pub(super) fn partition_contains_block(
        &self,
        partition_index: usize,
        block_index: usize,
    ) -> bool {
        self.index_partitions
            .get(partition_index)
            .is_some_and(|partition| {
                let end = partition
                    .first_data_block_index
                    .saturating_add(partition.data_block_count);
                partition.first_data_block_index <= block_index && block_index < end
            })
    }

    pub(super) fn load_data_block(
        &self,
        block_index: usize,
        block_cache: Option<&BlockCache>,
    ) -> Result<Arc<DecodedDataBlock>> {
        if let Some(records) = &self.point_records {
            let records = self.with_data_block_metadata(block_index, None, |block| {
                records
                    .get(block.record_range.clone())
                    .ok_or_else(|| invalid_table("data block record range outside table"))
                    .map(<[_]>::to_vec)
            })?;
            return DecodedDataBlock::from_records(&records).map(Arc::new);
        }

        let entry = self.with_data_block_metadata(block_index, block_cache, |block| {
            Ok(DataBlockIndexEntry {
                smallest_internal_key: block.smallest_internal_key.clone(),
                largest_internal_key: block.largest_internal_key.clone(),
                block: block.block,
                point_key_filter: block.point_key_filter.clone(),
                prefix_filter: block.prefix_filter.clone(),
            })
        })?;
        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table data block has no file path".to_owned(),
            });
        };
        let key = BlockCacheKey::new(self.properties.id, block_index);
        if let Some(block_cache) = block_cache {
            let path = path.clone();
            let file = self.file.as_ref().map(Arc::clone);
            let payload_len = self.payload_len;
            let codec = self.properties.codec;
            block_cache.get_or_insert_data_block_with(key, move || {
                read_data_block_from_file(&path, file.as_deref(), payload_len, codec, &entry)
            })
        } else {
            read_data_block_from_file(
                path,
                self.file.as_deref(),
                self.payload_len,
                self.properties.codec,
                &entry,
            )
            .map(Arc::new)
        }
    }

    pub(super) async fn load_data_block_async(
        &self,
        block_index: usize,
        block_cache: Option<&BlockCache>,
    ) -> Result<Arc<DecodedDataBlock>> {
        if let Some(records) = &self.point_records {
            let records = self.with_data_block_metadata(block_index, None, |block| {
                records
                    .get(block.record_range.clone())
                    .ok_or_else(|| invalid_table("data block record range outside table"))
                    .map(<[_]>::to_vec)
            })?;
            return DecodedDataBlock::from_records(&records).map(Arc::new);
        }

        let entry = self
            .with_data_block_metadata_async(block_index, block_cache, |block| {
                Ok(DataBlockIndexEntry {
                    smallest_internal_key: block.smallest_internal_key.clone(),
                    largest_internal_key: block.largest_internal_key.clone(),
                    block: block.block,
                    point_key_filter: block.point_key_filter.clone(),
                    prefix_filter: block.prefix_filter.clone(),
                })
            })
            .await?;
        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table data block has no file path".to_owned(),
            });
        };
        let key = BlockCacheKey::new(self.properties.id, block_index);
        if let Some(block_cache) = block_cache {
            let path = path.clone();
            let file = self.file.as_ref().map(Arc::clone);
            let payload_len = self.payload_len;
            let codec = self.properties.codec;
            block_cache
                .get_or_insert_data_block_with_async(key, move || async move {
                    read_data_block_from_file_async(
                        &path,
                        file.as_deref(),
                        payload_len,
                        codec,
                        &entry,
                    )
                    .await
                })
                .await
        } else {
            read_data_block_from_file_async(
                path,
                self.file.as_deref(),
                self.payload_len,
                self.properties.codec,
                &entry,
            )
            .await
            .map(Arc::new)
        }
    }

    pub(super) fn first_block_for_key(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        self.first_block_matching_key(key, policy, block_cache)
    }

    pub(super) async fn first_block_for_key_async(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        self.first_block_matching_key_async(key, policy, block_cache)
            .await
    }

    pub(super) fn first_block_for_range(
        &self,
        range: &KeyRange,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        self.first_block_matching_range_start(range, policy, block_cache)
    }

    #[cfg(test)]
    pub(super) fn first_block_for_prefix(
        &self,
        prefix: &[u8],
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        self.first_block_matching_key(prefix, policy, block_cache)
    }

    pub(crate) fn point_cursor(
        self: Arc<Self>,
        selector: ScanSelector,
        prefix_extractor: PrefixExtractor,
        direction: Direction,
        policy: IndexSearchPolicy,
        block_cache: Option<Arc<BlockCache>>,
    ) -> TablePointCursor {
        TablePointCursor::new(
            self,
            selector,
            prefix_extractor,
            direction,
            policy,
            block_cache,
        )
    }

    pub(super) fn first_block_candidate_for_key(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
    ) -> Option<usize> {
        if let Some(blocks) = &self.data_blocks {
            let index = search::partition_point_by(blocks.len(), policy, |index| {
                blocks[index].largest_internal_key.user_key() < key
            });
            return (index < blocks.len()).then_some(index);
        }
        let partition_index =
            search::partition_point_by(self.index_partitions.len(), policy, |index| {
                self.index_partitions[index].largest_internal_key.user_key() < key
            });
        self.index_partitions
            .get(partition_index)
            .map(|partition| partition.first_data_block_index)
    }

    pub(super) fn first_block_matching_key(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        if let Some(blocks) = &self.data_blocks {
            let index = search::partition_point_by(blocks.len(), policy, |index| {
                blocks[index].largest_internal_key.user_key() < key
            });
            return Ok((index < blocks.len()).then_some(index));
        }
        let Some((partition_index, partition_entry)) =
            self.first_partition_matching_key(key, policy)
        else {
            return Ok(None);
        };
        self.read_path_stats.record_point_index_partition_probe();
        let partition = self.load_index_partition(partition_index, block_cache)?;
        let local_index = search::partition_point_by(partition.len(), policy, |index| {
            partition[index].largest_internal_key.user_key() < key
        });
        Ok((local_index < partition.len())
            .then_some(partition_entry.first_data_block_index + local_index))
    }

    pub(super) async fn first_block_matching_key_async(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        if let Some(blocks) = &self.data_blocks {
            let index = search::partition_point_by(blocks.len(), policy, |index| {
                blocks[index].largest_internal_key.user_key() < key
            });
            return Ok((index < blocks.len()).then_some(index));
        }
        let Some((partition_index, partition_entry)) =
            self.first_partition_matching_key(key, policy)
        else {
            return Ok(None);
        };
        self.read_path_stats.record_point_index_partition_probe();
        let partition = self
            .load_index_partition_async(partition_index, block_cache)
            .await?;
        let local_index = search::partition_point_by(partition.len(), policy, |index| {
            partition[index].largest_internal_key.user_key() < key
        });
        Ok((local_index < partition.len())
            .then_some(partition_entry.first_data_block_index + local_index))
    }

    pub(super) fn first_partition_matching_key(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
    ) -> Option<(usize, &IndexPartitionEntry)> {
        let partition_index =
            search::partition_point_by(self.index_partitions.len(), policy, |index| {
                self.index_partitions[index].largest_internal_key.user_key() < key
            });
        self.index_partitions
            .get(partition_index)
            .map(|partition| (partition_index, partition))
    }

    pub(super) fn first_block_candidate_for_range(
        &self,
        range: &KeyRange,
        policy: IndexSearchPolicy,
    ) -> Option<usize> {
        if let Some(blocks) = &self.data_blocks {
            let index = search::partition_point_by(blocks.len(), policy, |index| {
                key_is_before_start(blocks[index].largest_internal_key.user_key(), &range.start)
            });
            return (index < blocks.len()).then_some(index);
        }
        let partition_index =
            search::partition_point_by(self.index_partitions.len(), policy, |index| {
                key_is_before_start(
                    self.index_partitions[index].largest_internal_key.user_key(),
                    &range.start,
                )
            });
        self.index_partitions
            .get(partition_index)
            .map(|partition| partition.first_data_block_index)
    }

    pub(super) fn first_block_matching_range_start(
        &self,
        range: &KeyRange,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        if let Some(blocks) = &self.data_blocks {
            let index = search::partition_point_by(blocks.len(), policy, |index| {
                key_is_before_start(blocks[index].largest_internal_key.user_key(), &range.start)
            });
            return Ok((index < blocks.len()).then_some(index));
        }
        let partition_index =
            search::partition_point_by(self.index_partitions.len(), policy, |index| {
                key_is_before_start(
                    self.index_partitions[index].largest_internal_key.user_key(),
                    &range.start,
                )
            });
        let Some(partition_entry) = self.index_partitions.get(partition_index) else {
            return Ok(None);
        };
        let partition = self.load_index_partition(partition_index, block_cache)?;
        let local_index = search::partition_point_by(partition.len(), policy, |index| {
            key_is_before_start(
                partition[index].largest_internal_key.user_key(),
                &range.start,
            )
        });
        Ok((local_index < partition.len())
            .then_some(partition_entry.first_data_block_index + local_index))
    }

    pub(super) fn last_block_candidate_for_range(
        &self,
        range: &KeyRange,
        policy: IndexSearchPolicy,
    ) -> Option<usize> {
        if let Some(blocks) = &self.data_blocks {
            let upper = search::partition_point_by(blocks.len(), policy, |index| {
                !key_is_after_end(blocks[index].smallest_internal_key.user_key(), &range.end)
            });
            return upper.checked_sub(1);
        }
        let upper = search::partition_point_by(self.index_partitions.len(), policy, |index| {
            !key_is_after_end(
                self.index_partitions[index]
                    .smallest_internal_key
                    .user_key(),
                &range.end,
            )
        });
        upper.checked_sub(1).and_then(|partition_index| {
            let partition = &self.index_partitions[partition_index];
            partition
                .first_data_block_index
                .checked_add(partition.data_block_count)
                .and_then(|end| end.checked_sub(1))
        })
    }
}
