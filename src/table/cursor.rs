use super::*;

#[derive(Debug, Clone)]
pub(crate) struct TablePointCursor {
    table: Arc<Table>,
    selector: ScanSelector,
    prefix_extractor: PrefixExtractor,
    direction: Direction,
    policy: IndexSearchPolicy,
    block_cache: Option<Arc<BlockCache>>,
    block_index: Option<usize>,
    record_index: usize,
    current_block: Option<(usize, Arc<DecodedDataBlock>)>,
    current_block_had_prefix_filter: bool,
    current_block_matched_selector: bool,
    pending: Option<ScanRecord>,
    exhausted: bool,
}

impl TablePointCursor {
    pub(super) fn new(
        table: Arc<Table>,
        selector: ScanSelector,
        prefix_extractor: PrefixExtractor,
        direction: Direction,
        policy: IndexSearchPolicy,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Self {
        let block_index = match direction {
            Direction::Forward => first_block_for_selector(&table, &selector, policy),
            Direction::Reverse => last_block_for_selector(&table, &selector, policy),
        };
        Self {
            table,
            selector,
            prefix_extractor,
            direction,
            policy,
            block_cache,
            block_index,
            record_index: 0,
            current_block: None,
            current_block_had_prefix_filter: false,
            current_block_matched_selector: false,
            pending: None,
            exhausted: false,
        }
    }

    pub(crate) fn next_group(&mut self) -> Result<Option<RecordGroup>> {
        let first = if let Some(record) = self.pending.take() {
            record
        } else {
            let Some(record) = self.next_record()? else {
                return Ok(None);
            };
            record
        };
        let user_key = first.0.user_key().to_vec();
        let mut rest = Vec::new();

        while let Some(record) = self.next_record()? {
            if record.0.user_key() == user_key.as_slice() {
                rest.push(record);
            } else {
                self.pending = Some(record);
                break;
            }
        }
        let (first, rest) = sort_group_records(first, rest);

        Ok(Some(RecordGroup {
            user_key,
            first,
            rest,
        }))
    }

    pub(crate) async fn next_group_async(&mut self) -> Result<Option<RecordGroup>> {
        let first = if let Some(record) = self.pending.take() {
            record
        } else {
            let Some(record) = self.next_record_async().await? else {
                return Ok(None);
            };
            record
        };
        let user_key = first.0.user_key().to_vec();
        let mut rest = Vec::new();

        while let Some(record) = self.next_record_async().await? {
            if record.0.user_key() == user_key.as_slice() {
                rest.push(record);
            } else {
                self.pending = Some(record);
                break;
            }
        }
        let (first, rest) = sort_group_records(first, rest);

        Ok(Some(RecordGroup {
            user_key,
            first,
            rest,
        }))
    }

    pub(super) fn next_record(&mut self) -> Result<Option<ScanRecord>> {
        match self.direction {
            Direction::Forward => self.next_record_forward(),
            Direction::Reverse => self.next_record_reverse(),
        }
    }

    async fn next_record_async(&mut self) -> Result<Option<ScanRecord>> {
        match self.direction {
            Direction::Forward => self.next_record_forward_async().await,
            Direction::Reverse => self.next_record_reverse_async().await,
        }
    }

    pub(super) fn next_record_forward(&mut self) -> Result<Option<ScanRecord>> {
        while !self.exhausted {
            let Some(block_index) = self.block_index else {
                return Ok(None);
            };
            if !self.current_block_is(block_index) {
                match self.forward_block_state(block_index)? {
                    CursorBlockState::Scan { had_prefix_filter } => {
                        self.prepare_current_block_scan(had_prefix_filter);
                    }
                    CursorBlockState::Skip => {
                        self.move_to_next_block();
                        continue;
                    }
                    CursorBlockState::Done => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            while self.record_index < self.current_block_len(block_index)? {
                let record = self.current_block_record(block_index, self.record_index)?;
                self.record_index += 1;

                match self
                    .selector
                    .forward_key_state(record.internal_key.user_key())
                {
                    ForwardKeyState::Before => {}
                    ForwardKeyState::Match => {
                        self.current_block_matched_selector = true;
                        return Ok(Some((record.internal_key, record.value)));
                    }
                    ForwardKeyState::After => {
                        self.finish_current_block_scan();
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.move_to_next_block();
        }

        Ok(None)
    }

    async fn next_record_forward_async(&mut self) -> Result<Option<ScanRecord>> {
        while !self.exhausted {
            let Some(block_index) = self.block_index else {
                return Ok(None);
            };
            if !self.current_block_is(block_index) {
                match self.forward_block_state_async(block_index).await? {
                    CursorBlockState::Scan { had_prefix_filter } => {
                        self.prepare_current_block_scan(had_prefix_filter);
                    }
                    CursorBlockState::Skip => {
                        self.move_to_next_block();
                        continue;
                    }
                    CursorBlockState::Done => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            while self.record_index < self.current_block_len_async(block_index).await? {
                let record = self
                    .current_block_record_async(block_index, self.record_index)
                    .await?;
                self.record_index += 1;

                match self
                    .selector
                    .forward_key_state(record.internal_key.user_key())
                {
                    ForwardKeyState::Before => {}
                    ForwardKeyState::Match => {
                        self.current_block_matched_selector = true;
                        return Ok(Some((record.internal_key, record.value)));
                    }
                    ForwardKeyState::After => {
                        self.finish_current_block_scan();
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.move_to_next_block();
        }

        Ok(None)
    }

    pub(super) fn next_record_reverse(&mut self) -> Result<Option<ScanRecord>> {
        while !self.exhausted {
            let Some(block_index) = self.block_index else {
                return Ok(None);
            };
            if !self.current_block_is(block_index) {
                match self.reverse_block_state(block_index)? {
                    CursorBlockState::Scan { had_prefix_filter } => {
                        self.prepare_current_block_scan(had_prefix_filter);
                    }
                    CursorBlockState::Skip => {
                        self.move_to_previous_block();
                        continue;
                    }
                    CursorBlockState::Done => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.ensure_current_block(block_index)?;
            while self.record_index > 0 {
                self.record_index -= 1;
                let record = self.current_block_record(block_index, self.record_index)?;

                match self
                    .selector
                    .reverse_key_state(record.internal_key.user_key())
                {
                    ReverseKeyState::Above => {}
                    ReverseKeyState::Match => {
                        self.current_block_matched_selector = true;
                        return Ok(Some((record.internal_key, record.value)));
                    }
                    ReverseKeyState::Below => {
                        self.finish_current_block_scan();
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.move_to_previous_block();
        }

        Ok(None)
    }

    async fn next_record_reverse_async(&mut self) -> Result<Option<ScanRecord>> {
        while !self.exhausted {
            let Some(block_index) = self.block_index else {
                return Ok(None);
            };
            if !self.current_block_is(block_index) {
                match self.reverse_block_state_async(block_index).await? {
                    CursorBlockState::Scan { had_prefix_filter } => {
                        self.prepare_current_block_scan(had_prefix_filter);
                    }
                    CursorBlockState::Skip => {
                        self.move_to_previous_block();
                        continue;
                    }
                    CursorBlockState::Done => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.ensure_current_block_async(block_index).await?;
            while self.record_index > 0 {
                self.record_index -= 1;
                let record = self
                    .current_block_record_async(block_index, self.record_index)
                    .await?;

                match self
                    .selector
                    .reverse_key_state(record.internal_key.user_key())
                {
                    ReverseKeyState::Above => {}
                    ReverseKeyState::Match => {
                        self.current_block_matched_selector = true;
                        return Ok(Some((record.internal_key, record.value)));
                    }
                    ReverseKeyState::Below => {
                        self.finish_current_block_scan();
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.move_to_previous_block();
        }

        Ok(None)
    }

    pub(super) fn forward_block_state(&self, block_index: usize) -> Result<CursorBlockState> {
        if self.selector.prefix().is_some() {
            self.table
                .read_path_stats
                .record_prefix_block_metadata_probe();
        }
        self.table
            .with_data_block_metadata(
                block_index,
                self.block_cache.as_deref(),
                |block| match &self.selector {
                    ScanSelector::Range(range) => {
                        if key_is_after_end(block.smallest_internal_key.user_key(), &range.end) {
                            Ok(CursorBlockState::Done)
                        } else if block.overlaps_range(range) {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            Ok(CursorBlockState::Skip)
                        }
                    }
                    ScanSelector::Prefix(prefix) => {
                        if !block.prefix_bounds_may_overlap(prefix) {
                            if block.largest_internal_key.user_key() < prefix.as_slice() {
                                Ok(CursorBlockState::Skip)
                            } else {
                                Ok(CursorBlockState::Done)
                            }
                        } else if self
                            .current_block
                            .as_ref()
                            .is_some_and(|(current_index, _)| *current_index == block_index)
                        {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            let (allowed, had_filter) = self.table.block_prefix_filter_allows(
                                block,
                                prefix,
                                &self.prefix_extractor,
                            );
                            if allowed {
                                Ok(CursorBlockState::Scan {
                                    had_prefix_filter: had_filter,
                                })
                            } else {
                                self.table.read_path_stats.record_prefix_filter_miss();
                                Ok(CursorBlockState::Skip)
                            }
                        }
                    }
                },
            )
    }

    async fn forward_block_state_async(&self, block_index: usize) -> Result<CursorBlockState> {
        if self.selector.prefix().is_some() {
            self.table
                .read_path_stats
                .record_prefix_block_metadata_probe();
        }
        self.table
            .with_data_block_metadata_async(block_index, self.block_cache.as_deref(), |block| {
                match &self.selector {
                    ScanSelector::Range(range) => {
                        if key_is_after_end(block.smallest_internal_key.user_key(), &range.end) {
                            Ok(CursorBlockState::Done)
                        } else if block.overlaps_range(range) {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            Ok(CursorBlockState::Skip)
                        }
                    }
                    ScanSelector::Prefix(prefix) => {
                        if !block.prefix_bounds_may_overlap(prefix) {
                            if block.largest_internal_key.user_key() < prefix.as_slice() {
                                Ok(CursorBlockState::Skip)
                            } else {
                                Ok(CursorBlockState::Done)
                            }
                        } else if self
                            .current_block
                            .as_ref()
                            .is_some_and(|(current_index, _)| *current_index == block_index)
                        {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            let (allowed, had_filter) = self.table.block_prefix_filter_allows(
                                block,
                                prefix,
                                &self.prefix_extractor,
                            );
                            if allowed {
                                Ok(CursorBlockState::Scan {
                                    had_prefix_filter: had_filter,
                                })
                            } else {
                                self.table.read_path_stats.record_prefix_filter_miss();
                                Ok(CursorBlockState::Skip)
                            }
                        }
                    }
                }
            })
            .await
    }

    pub(super) fn reverse_block_state(&self, block_index: usize) -> Result<CursorBlockState> {
        if self.selector.prefix().is_some() {
            self.table
                .read_path_stats
                .record_prefix_block_metadata_probe();
        }
        self.table
            .with_data_block_metadata(
                block_index,
                self.block_cache.as_deref(),
                |block| match &self.selector {
                    ScanSelector::Range(range) => {
                        if key_is_before_start(block.largest_internal_key.user_key(), &range.start)
                        {
                            Ok(CursorBlockState::Done)
                        } else if block.overlaps_range(range) {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            Ok(CursorBlockState::Skip)
                        }
                    }
                    ScanSelector::Prefix(prefix) => {
                        if block.largest_internal_key.user_key() < prefix.as_slice() {
                            Ok(CursorBlockState::Done)
                        } else if !block.prefix_bounds_may_overlap(prefix) {
                            Ok(CursorBlockState::Skip)
                        } else if self
                            .current_block
                            .as_ref()
                            .is_some_and(|(current_index, _)| *current_index == block_index)
                        {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            let (allowed, had_filter) = self.table.block_prefix_filter_allows(
                                block,
                                prefix,
                                &self.prefix_extractor,
                            );
                            if allowed {
                                Ok(CursorBlockState::Scan {
                                    had_prefix_filter: had_filter,
                                })
                            } else {
                                self.table.read_path_stats.record_prefix_filter_miss();
                                Ok(CursorBlockState::Skip)
                            }
                        }
                    }
                },
            )
    }

    async fn reverse_block_state_async(&self, block_index: usize) -> Result<CursorBlockState> {
        if self.selector.prefix().is_some() {
            self.table
                .read_path_stats
                .record_prefix_block_metadata_probe();
        }
        self.table
            .with_data_block_metadata_async(block_index, self.block_cache.as_deref(), |block| {
                match &self.selector {
                    ScanSelector::Range(range) => {
                        if key_is_before_start(block.largest_internal_key.user_key(), &range.start)
                        {
                            Ok(CursorBlockState::Done)
                        } else if block.overlaps_range(range) {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            Ok(CursorBlockState::Skip)
                        }
                    }
                    ScanSelector::Prefix(prefix) => {
                        if block.largest_internal_key.user_key() < prefix.as_slice() {
                            Ok(CursorBlockState::Done)
                        } else if !block.prefix_bounds_may_overlap(prefix) {
                            Ok(CursorBlockState::Skip)
                        } else if self
                            .current_block
                            .as_ref()
                            .is_some_and(|(current_index, _)| *current_index == block_index)
                        {
                            Ok(CursorBlockState::Scan {
                                had_prefix_filter: false,
                            })
                        } else {
                            let (allowed, had_filter) = self.table.block_prefix_filter_allows(
                                block,
                                prefix,
                                &self.prefix_extractor,
                            );
                            if allowed {
                                Ok(CursorBlockState::Scan {
                                    had_prefix_filter: had_filter,
                                })
                            } else {
                                self.table.read_path_stats.record_prefix_filter_miss();
                                Ok(CursorBlockState::Skip)
                            }
                        }
                    }
                }
            })
            .await
    }

    pub(super) fn move_to_next_block(&mut self) {
        let Some(block_index) = self.block_index else {
            self.exhausted = true;
            return;
        };
        self.finish_current_block_scan();
        let next = block_index + 1;
        self.block_index = (next < self.table.data_block_count).then_some(next);
        self.record_index = 0;
        self.current_block = None;
    }

    pub(super) fn move_to_previous_block(&mut self) {
        let Some(block_index) = self.block_index else {
            self.exhausted = true;
            return;
        };
        self.finish_current_block_scan();
        self.block_index = block_index.checked_sub(1);
        self.record_index = 0;
        self.current_block = None;
    }

    pub(super) fn prepare_current_block_scan(&mut self, had_prefix_filter: bool) {
        self.current_block_had_prefix_filter = had_prefix_filter;
        self.current_block_matched_selector = false;
    }

    pub(super) fn finish_current_block_scan(&mut self) {
        if self.selector.prefix().is_some()
            && self.current_block_had_prefix_filter
            && !self.current_block_matched_selector
        {
            self.table.filter_stats.record_block_prefix_false_positive();
        }
        self.current_block_had_prefix_filter = false;
        self.current_block_matched_selector = false;
    }

    pub(super) fn ensure_current_block(&mut self, block_index: usize) -> Result<()> {
        if self.current_block_is(block_index) {
            return Ok(());
        }

        if self.selector.prefix().is_some() {
            self.table.read_path_stats.record_prefix_data_block_read();
        }
        let block = self
            .table
            .load_data_block(block_index, self.block_cache.as_deref())?;
        self.record_index = match self.direction {
            Direction::Forward => {
                first_record_index_for_decoded_block(&block, &self.selector, self.policy)?
            }
            Direction::Reverse => block.record_count(),
        };
        self.current_block = Some((block_index, block));
        Ok(())
    }

    async fn ensure_current_block_async(&mut self, block_index: usize) -> Result<()> {
        if self.current_block_is(block_index) {
            return Ok(());
        }

        if self.selector.prefix().is_some() {
            self.table.read_path_stats.record_prefix_data_block_read();
        }
        let block = self
            .table
            .load_data_block_async(block_index, self.block_cache.as_deref())
            .await?;
        self.record_index = match self.direction {
            Direction::Forward => {
                first_record_index_for_decoded_block(&block, &self.selector, self.policy)?
            }
            Direction::Reverse => block.record_count(),
        };
        self.current_block = Some((block_index, block));
        Ok(())
    }

    pub(super) fn current_block_len(&mut self, block_index: usize) -> Result<usize> {
        self.ensure_current_block(block_index)?;
        Ok(self
            .current_block
            .as_ref()
            .map_or(0, |(_, block)| block.record_count()))
    }

    async fn current_block_len_async(&mut self, block_index: usize) -> Result<usize> {
        self.ensure_current_block_async(block_index).await?;
        Ok(self
            .current_block
            .as_ref()
            .map_or(0, |(_, block)| block.record_count()))
    }

    pub(super) fn current_block_is(&self, block_index: usize) -> bool {
        self.current_block
            .as_ref()
            .is_some_and(|(current_index, _)| *current_index == block_index)
    }

    pub(super) fn current_block_record(
        &mut self,
        block_index: usize,
        record_index: usize,
    ) -> Result<TablePointRecord> {
        self.ensure_current_block(block_index)?;
        let (_, block) = self
            .current_block
            .as_ref()
            .ok_or_else(|| invalid_table("cursor record index outside data block"))?;
        block.record_owned(record_index)
    }

    async fn current_block_record_async(
        &mut self,
        block_index: usize,
        record_index: usize,
    ) -> Result<TablePointRecord> {
        self.ensure_current_block_async(block_index).await?;
        let (_, block) = self
            .current_block
            .as_ref()
            .ok_or_else(|| invalid_table("cursor record index outside data block"))?;
        block.record_owned(record_index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PointBlockDecision {
    Done,
    Skip,
    Read { had_filter: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PointBatchScan {
    pub(super) key_index: usize,
    pub(super) block_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PointBatchRead {
    pub(super) key_index: usize,
    pub(super) block_index: usize,
    pub(super) had_filter: bool,
}

pub(super) fn next_point_batch_block(block_index: usize, data_block_count: usize) -> Option<usize> {
    let next = block_index.checked_add(1)?;
    (next < data_block_count).then_some(next)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeBlockDecision {
    Done,
    Skip,
    Read,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PrefixBlockDecision {
    Done,
    Skip,
    Read { had_filter: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CursorBlockState {
    Scan { had_prefix_filter: bool },
    Skip,
    Done,
}

pub(super) fn first_block_for_selector(
    table: &Table,
    selector: &ScanSelector,
    policy: IndexSearchPolicy,
) -> Option<usize> {
    match selector {
        ScanSelector::Range(range) => table.first_block_candidate_for_range(range, policy),
        ScanSelector::Prefix(prefix) => table.first_block_candidate_for_key(prefix, policy),
    }
}

pub(super) fn last_block_for_selector(
    table: &Table,
    selector: &ScanSelector,
    policy: IndexSearchPolicy,
) -> Option<usize> {
    match selector {
        ScanSelector::Range(range) => table.last_block_candidate_for_range(range, policy),
        ScanSelector::Prefix(prefix) => {
            let end = prefix_successor(prefix).map_or(Bound::Unbounded, Bound::Excluded);
            let range = KeyRange {
                start: Bound::Included(prefix.clone()),
                end,
            };
            table.last_block_candidate_for_range(&range, policy)
        }
    }
}

pub(super) fn first_record_index_for_decoded_block(
    block: &DecodedDataBlock,
    selector: &ScanSelector,
    policy: IndexSearchPolicy,
) -> Result<usize> {
    match selector {
        ScanSelector::Range(range) => {
            data_block_restart_index_for_bound(block, &range.start, policy)
        }
        ScanSelector::Prefix(prefix) => data_block_restart_index_for_key(block, prefix, policy),
    }
}

pub(super) fn data_block_point_records_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    policy: IndexSearchPolicy,
) -> Result<Vec<TablePointRecord>> {
    let range = data_block_point_record_range_for_key(block, key, policy)?;
    range
        .map(|record_index| block.record_owned(record_index))
        .collect()
}

#[cfg(test)]
pub(super) fn data_block_newest_visible_point_record_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    read_sequence: Sequence,
    _policy: IndexSearchPolicy,
) -> Result<(bool, Option<TablePointRecord>)> {
    for entry in block
        .point_lookup_index
        .matching_entries(user_key_hash(key))
    {
        let start = u32_to_usize(entry.start_record);
        let end = u32_to_usize(entry.end_record);
        let first_record = block.record_view(start)?;
        if first_record.user_key != key {
            continue;
        }
        if first_record.sequence <= read_sequence {
            return Ok((true, Some(first_record.to_owned())));
        }
        for record_index in start + 1..end {
            let record = block.record_view(record_index)?;
            if record.sequence <= read_sequence {
                return Ok((true, Some(record.to_owned())));
            }
        }
        return Ok((true, None));
    }
    Ok((false, None))
}

pub(super) fn data_block_newest_visible_point_value_record_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    read_sequence: Sequence,
    _policy: IndexSearchPolicy,
) -> Result<(bool, Option<TablePointValueRecord>)> {
    for entry in block
        .point_lookup_index
        .matching_entries(user_key_hash(key))
    {
        let start = u32_to_usize(entry.start_record);
        let end = u32_to_usize(entry.end_record);
        let first_record = block.record_view(start)?;
        if first_record.user_key != key {
            continue;
        }
        if first_record.sequence <= read_sequence {
            return Ok((true, Some(block.point_value_record(start)?)));
        }
        for record_index in start + 1..end {
            let record = block.record_view(record_index)?;
            if record.sequence <= read_sequence {
                return Ok((true, Some(block.point_value_record(record_index)?)));
            }
        }
        return Ok((true, None));
    }
    Ok((false, None))
}

pub(super) fn data_block_point_records_in_range(
    block: &DecodedDataBlock,
    range: &KeyRange,
    policy: IndexSearchPolicy,
) -> Result<Vec<TablePointRecord>> {
    let start = data_block_restart_index_for_bound(block, &range.start, policy)?;
    let mut records = Vec::new();
    for record_index in start..block.record_count() {
        let record = block.record_view(record_index)?;
        if key_is_before_start(record.user_key, &range.start) {
            continue;
        }
        if key_is_after_end(record.user_key, &range.end) {
            break;
        }
        records.push(record.to_owned());
    }
    Ok(records)
}

#[cfg(test)]
pub(super) fn data_block_point_records_with_prefix(
    block: &DecodedDataBlock,
    prefix: &[u8],
    policy: IndexSearchPolicy,
) -> Result<Vec<TablePointRecord>> {
    let start = data_block_restart_index_for_key(block, prefix, policy)?;
    let mut records = Vec::new();
    for record_index in start..block.record_count() {
        let record = block.record_view(record_index)?;
        if record.user_key < prefix {
            continue;
        }
        if !record.user_key.starts_with(prefix) {
            break;
        }
        records.push(record.to_owned());
    }
    Ok(records)
}

pub(super) fn data_block_restart_index_for_bound(
    block: &DecodedDataBlock,
    bound: &Bound<Vec<u8>>,
    policy: IndexSearchPolicy,
) -> Result<usize> {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => {
            data_block_restart_index_for_key(block, key, policy)
        }
        Bound::Unbounded => Ok(0),
    }
}

pub(super) fn data_block_restart_index_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    policy: IndexSearchPolicy,
) -> Result<usize> {
    let upper = match policy {
        IndexSearchPolicy::Linear => data_block_linear_restart_partition_point(block, key)?,
        IndexSearchPolicy::Auto if block.restart_indices.len() <= 8 => {
            data_block_linear_restart_partition_point(block, key)?
        }
        IndexSearchPolicy::Auto | IndexSearchPolicy::Binary => {
            data_block_binary_restart_partition_point(block, key)?
        }
    };
    if upper == 0 {
        Ok(0)
    } else {
        Ok(u32_to_usize(block.restart_indices[upper - 1]))
    }
}

pub(super) fn data_block_linear_restart_partition_point(
    block: &DecodedDataBlock,
    key: &[u8],
) -> Result<usize> {
    for (index, restart_index) in block.restart_indices.iter().enumerate() {
        let record = block.record_view(u32_to_usize(*restart_index))?;
        if record.user_key > key {
            return Ok(index);
        }
    }
    Ok(block.restart_indices.len())
}

pub(super) fn data_block_binary_restart_partition_point(
    block: &DecodedDataBlock,
    key: &[u8],
) -> Result<usize> {
    let mut left = 0;
    let mut right = block.restart_indices.len();
    while left < right {
        let mid = left + (right - left) / 2;
        let record = block.record_view(u32_to_usize(block.restart_indices[mid]))?;
        if record.user_key <= key {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    Ok(left)
}

pub(super) fn data_block_point_record_range_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    _policy: IndexSearchPolicy,
) -> Result<Range<usize>> {
    for entry in block
        .point_lookup_index
        .matching_entries(user_key_hash(key))
    {
        let first_record = block.record_view(u32_to_usize(entry.start_record))?;
        if first_record.user_key == key {
            return Ok(u32_to_usize(entry.start_record)..u32_to_usize(entry.end_record));
        }
    }
    Ok(0..0)
}
