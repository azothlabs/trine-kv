use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    ops::{Bound, Range},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    blob::ValueRef,
    cache::{BlockCache, BlockCacheKey},
    codec::{self, CodecId},
    error::{Error, Result},
    filter::{PointKeyFilter, PrefixFilter},
    internal_key::{InternalKey, ValueKind},
    iterator::{
        Direction, ForwardKeyState, RecordGroup, ReverseKeyState, ScanRecord, ScanSelector,
        prefix_successor, sort_group_records,
    },
    options::{FilterPolicy, IndexSearchPolicy, PrefixFilterPolicy},
    prefix::PrefixExtractor,
    range_tombstone::{self, RangeTombstoneIndex, RangeTombstoneLike},
    search,
    stats::FilterStats,
    types::{KeyRange, Sequence},
};

pub const TABLE_FILE_EXTENSION: &str = "trinet";
const TABLE_MAGIC: u32 = 0x5452_5442;
const TABLE_VERSION: u16 = 3;
const HEADER_LEN: usize = 14;
const FOOTER_MAGIC: u32 = 0x5452_5446;
const FOOTER_LEN: usize = 90;
const BLOCK_HEADER_LEN: usize = 13;
const DATA_BLOCK_RESTART_INTERVAL: usize = 16;

const VALUE_KIND_PUT: u8 = 1;
const VALUE_KIND_POINT_DELETE: u8 = 2;
const VALUE_KIND_RANGE_DELETE: u8 = 3;

const VALUE_NONE: u8 = 0;
const VALUE_INLINE: u8 = 1;
const VALUE_BLOB: u8 = 2;

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

const PREFIX_FILTER_ABSENT: u8 = 0;
const PREFIX_FILTER_PRESENT: u8 = 1;

const POINT_KEY_FILTER_ABSENT: u8 = 0;
const POINT_KEY_FILTER_PRESENT: u8 = 1;

const PREFIX_EXTRACTOR_DISABLED: u8 = 0;
const PREFIX_EXTRACTOR_FIXED_LEN: u8 = 1;
const PREFIX_EXTRACTOR_SEPARATOR: u8 = 2;
const PREFIX_EXTRACTOR_CUSTOM: u8 = 3;

// These are on-disk lower bounds. Decoders use them to reject impossible
// record counts before reserving memory; real entries may be larger because
// keys, values, and filters carry byte fields.
const MIN_INTERNAL_KEY_BYTES: usize = 17;
const MIN_VALUE_REF_BYTES: usize = 1;
const MIN_DATA_RECORD_BYTES: usize = MIN_INTERNAL_KEY_BYTES + MIN_VALUE_REF_BYTES;
const MIN_INDEX_ENTRY_BYTES: usize = MIN_INTERNAL_KEY_BYTES * 2 + 16 + 1 + 1;
const MIN_RANGE_TOMBSTONE_BYTES: usize = 14;
const RESTART_POINT_BYTES: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

impl TableId {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableLevel(pub u32);

impl TableLevel {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableSection {
    DataBlocks,
    RangeTombstones,
    Filters,
    Indexes,
    Properties,
    Footer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableProperties {
    pub id: TableId,
    pub level: TableLevel,
    pub smallest_user_key: Vec<u8>,
    pub largest_user_key: Vec<u8>,
    pub smallest_sequence: Sequence,
    pub largest_sequence: Sequence,
    pub codec: CodecId,
    pub(crate) blob_file_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableWriteOptions {
    pub(crate) codec: CodecId,
    pub(crate) block_bytes: usize,
    pub(crate) filter_policy: FilterPolicy,
    pub(crate) prefix_extractor: PrefixExtractor,
    pub(crate) prefix_filter_policy: PrefixFilterPolicy,
    pub(crate) blob_threshold_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TablePointRecord {
    pub(crate) internal_key: InternalKey,
    pub(crate) value: Option<ValueRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SectionHandle {
    offset: u64,
    len: u64,
}

impl SectionHandle {
    fn from_span(start: usize, end: usize) -> Result<Self> {
        Ok(Self {
            offset: usize_to_u64(start, "section offset")?,
            len: usize_to_u64(end.saturating_sub(start), "section length")?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockHandle {
    offset: u64,
    len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableFooter {
    data_blocks: SectionHandle,
    range_tombstones: SectionHandle,
    filters: SectionHandle,
    indexes: SectionHandle,
    properties: SectionHandle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DataBlockIndexEntry {
    smallest_internal_key: InternalKey,
    largest_internal_key: InternalKey,
    block: BlockHandle,
    point_key_filter: Option<PointKeyFilter>,
    prefix_filter: Option<PrefixFilter>,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedDataBlock {
    records: Vec<TablePointRecord>,
    restart_indices: Vec<usize>,
    point_lookup_index: HashMap<Vec<u8>, Range<usize>>,
}

impl DecodedDataBlock {
    fn new(records: Vec<TablePointRecord>, restart_indices: Vec<usize>) -> Self {
        let point_lookup_index = build_data_block_point_lookup_index(&records);
        Self {
            records,
            restart_indices,
            point_lookup_index,
        }
    }

    pub(crate) fn estimated_bytes(&self) -> u64 {
        let records = self.records.iter().fold(0_u64, |bytes, record| {
            bytes
                .saturating_add(usize_to_u64_saturating(
                    record.internal_key.user_key().len(),
                ))
                .saturating_add(record.value.as_ref().map_or(0, ValueRef::len))
                .saturating_add(32)
        });
        let restarts = usize_to_u64_saturating(
            self.restart_indices
                .len()
                .saturating_mul(RESTART_POINT_BYTES),
        );
        let point_index = self
            .point_lookup_index
            .iter()
            .fold(0_u64, |bytes, (key, _)| {
                bytes
                    .saturating_add(usize_to_u64_saturating(key.len()))
                    .saturating_add(48)
            });
        records
            .saturating_add(restarts)
            .saturating_add(point_index)
            .max(1)
    }

    #[cfg(test)]
    pub(crate) fn empty_for_cache_test() -> Self {
        Self::new(Vec::new(), Vec::new())
    }
}

#[derive(Debug, Default)]
struct TableFilterStats {
    table_point_hits: AtomicU64,
    table_point_misses: AtomicU64,
    table_point_false_positives: AtomicU64,
    table_prefix_hits: AtomicU64,
    table_prefix_misses: AtomicU64,
    table_prefix_false_positives: AtomicU64,
    block_point_hits: AtomicU64,
    block_point_misses: AtomicU64,
    block_point_false_positives: AtomicU64,
    block_prefix_hits: AtomicU64,
    block_prefix_misses: AtomicU64,
    block_prefix_false_positives: AtomicU64,
}

impl TableFilterStats {
    fn snapshot(&self) -> FilterStats {
        FilterStats {
            table_point_hits: self.table_point_hits.load(Ordering::Acquire),
            table_point_misses: self.table_point_misses.load(Ordering::Acquire),
            table_point_false_positives: self.table_point_false_positives.load(Ordering::Acquire),
            table_prefix_hits: self.table_prefix_hits.load(Ordering::Acquire),
            table_prefix_misses: self.table_prefix_misses.load(Ordering::Acquire),
            table_prefix_false_positives: self.table_prefix_false_positives.load(Ordering::Acquire),
            block_point_hits: self.block_point_hits.load(Ordering::Acquire),
            block_point_misses: self.block_point_misses.load(Ordering::Acquire),
            block_point_false_positives: self.block_point_false_positives.load(Ordering::Acquire),
            block_prefix_hits: self.block_prefix_hits.load(Ordering::Acquire),
            block_prefix_misses: self.block_prefix_misses.load(Ordering::Acquire),
            block_prefix_false_positives: self.block_prefix_false_positives.load(Ordering::Acquire),
        }
    }

    fn record_table_point(&self, allowed: bool) {
        record_filter_result(&self.table_point_hits, &self.table_point_misses, allowed);
    }

    fn record_table_prefix(&self, allowed: bool) {
        record_filter_result(&self.table_prefix_hits, &self.table_prefix_misses, allowed);
    }

    fn record_block_point(&self, allowed: bool) {
        record_filter_result(&self.block_point_hits, &self.block_point_misses, allowed);
    }

    fn record_block_prefix(&self, allowed: bool) {
        record_filter_result(&self.block_prefix_hits, &self.block_prefix_misses, allowed);
    }

    fn record_table_point_false_positive(&self) {
        self.table_point_false_positives
            .fetch_add(1, Ordering::AcqRel);
    }

    fn record_block_point_false_positive(&self) {
        self.block_point_false_positives
            .fetch_add(1, Ordering::AcqRel);
    }

    fn record_block_prefix_false_positive(&self) {
        self.block_prefix_false_positives
            .fetch_add(1, Ordering::AcqRel);
    }
}

fn record_filter_result(hits: &AtomicU64, misses: &AtomicU64, allowed: bool) {
    if allowed {
        hits.fetch_add(1, Ordering::AcqRel);
    } else {
        misses.fetch_add(1, Ordering::AcqRel);
    }
}

// Loaded tables keep one sorted record array. Each data block stores only the
// record range it owns plus restart positions inside that range, so point and
// range reads can jump near the target without duplicating all records.
#[derive(Debug, Clone)]
struct TableDataBlock {
    smallest_internal_key: InternalKey,
    largest_internal_key: InternalKey,
    block: BlockHandle,
    record_range: Range<usize>,
    restart_indices: Vec<usize>,
    point_key_filter: Option<PointKeyFilter>,
    prefix_filter: Option<PrefixFilter>,
}

impl TableDataBlock {
    fn from_record_range(
        point_records: &[TablePointRecord],
        record_range: Range<usize>,
        restart_indices: Vec<usize>,
        point_key_filter: Option<PointKeyFilter>,
        prefix_filter: Option<PrefixFilter>,
    ) -> Result<Self> {
        Self::from_record_range_and_block(
            point_records,
            record_range,
            restart_indices,
            BlockHandle { offset: 0, len: 0 },
            point_key_filter,
            prefix_filter,
        )
    }

    fn from_record_range_and_block(
        point_records: &[TablePointRecord],
        record_range: Range<usize>,
        restart_indices: Vec<usize>,
        block: BlockHandle,
        point_key_filter: Option<PointKeyFilter>,
        prefix_filter: Option<PrefixFilter>,
    ) -> Result<Self> {
        let records = point_records
            .get(record_range.clone())
            .ok_or_else(|| invalid_table("data block record range outside table"))?;
        if records.is_empty() {
            return Err(invalid_table("empty data block"));
        }
        if restart_indices.first().copied() != Some(record_range.start) {
            return Err(invalid_table(
                "data block first restart is not first record",
            ));
        }
        for restart_index in &restart_indices {
            if !record_range.contains(restart_index) {
                return Err(invalid_table("data block restart outside record range"));
            }
        }
        let first = records
            .first()
            .ok_or_else(|| invalid_table("empty data block"))?;
        let last = records
            .last()
            .ok_or_else(|| invalid_table("empty data block"))?;

        Ok(Self {
            smallest_internal_key: first.internal_key.clone(),
            largest_internal_key: last.internal_key.clone(),
            block,
            record_range,
            restart_indices,
            point_key_filter,
            prefix_filter,
        })
    }

    fn from_index_entry(entry: DataBlockIndexEntry) -> Result<Self> {
        if entry.smallest_internal_key > entry.largest_internal_key {
            return Err(Error::Corruption {
                message: "data block index key bounds are inverted".to_owned(),
            });
        }

        Ok(Self {
            smallest_internal_key: entry.smallest_internal_key,
            largest_internal_key: entry.largest_internal_key,
            block: entry.block,
            record_range: 0..0,
            restart_indices: Vec::new(),
            point_key_filter: entry.point_key_filter,
            prefix_filter: entry.prefix_filter,
        })
    }

    fn overlaps_range(&self, range: &KeyRange) -> bool {
        !key_is_after_end(self.smallest_internal_key.user_key(), &range.end)
            && !key_is_before_start(self.largest_internal_key.user_key(), &range.start)
    }

    fn key_bounds_may_contain(&self, key: &[u8]) -> bool {
        self.smallest_internal_key.user_key() <= key && key <= self.largest_internal_key.user_key()
    }

    fn point_filter_result(&self, key: &[u8]) -> Option<bool> {
        self.point_key_filter
            .as_ref()
            .map(|filter| filter.may_contain_key(key))
    }

    fn prefix_filter_result(&self, prefix: &[u8], extractor: &PrefixExtractor) -> Option<bool> {
        let filter = self.prefix_filter.as_ref()?;
        if filter.extractor() != extractor {
            return None;
        }
        let filter_prefix = extractor.query_filter_prefix(prefix)?;
        Some(filter.may_contain_prefix(filter_prefix))
    }

    fn prefix_bounds_may_overlap(&self, prefix: &[u8]) -> bool {
        self.largest_internal_key.user_key() >= prefix
            && (self.smallest_internal_key.user_key().starts_with(prefix)
                || self.smallest_internal_key.user_key() <= prefix)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRangeTombstone {
    pub(crate) range: KeyRange,
    pub(crate) sequence: Sequence,
    pub(crate) batch_index: u32,
}

impl RangeTombstoneLike for TableRangeTombstone {
    fn range(&self) -> &KeyRange {
        &self.range
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Table {
    path: Option<PathBuf>,
    file: Option<Arc<File>>,
    payload_len: usize,
    footer: TableFooter,
    properties: TableProperties,
    point_records: Option<Vec<TablePointRecord>>,
    data_blocks: Vec<TableDataBlock>,
    range_tombstones: Arc<RwLock<Option<Arc<RangeTombstoneIndex<TableRangeTombstone>>>>>,
    point_key_filter: Option<PointKeyFilter>,
    prefix_filter: Option<PrefixFilter>,
    filter_stats: Arc<TableFilterStats>,
}

impl Table {
    #[must_use]
    pub(crate) const fn properties(&self) -> &TableProperties {
        &self.properties
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

    pub(crate) fn range_tombstone_covers_visible_point(
        &self,
        key: &[u8],
        point_sequence: Sequence,
        point_batch_index: u32,
        read_sequence: Sequence,
    ) -> Result<bool> {
        let tombstones = self.range_tombstones()?;
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
        let tombstones = self.range_tombstones()?;
        Ok(tombstones.overlapping_range(range).cloned().collect())
    }

    pub(crate) fn blob_file_ids(&self) -> Vec<u64> {
        self.properties.blob_file_ids.clone()
    }

    pub(crate) fn estimated_file_bytes(&self) -> u64 {
        usize_to_u64_saturating(HEADER_LEN.saturating_add(self.payload_len))
    }

    pub(crate) fn filter_stats(&self) -> FilterStats {
        self.filter_stats.snapshot()
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
            range_tombstones: Arc::clone(&self.range_tombstones),
            point_key_filter: self.point_key_filter.clone(),
            prefix_filter: self.prefix_filter.clone(),
            filter_stats: Arc::clone(&self.filter_stats),
        }
    }

    #[must_use]
    pub(crate) fn has_key_bounds(&self) -> bool {
        !(self.data_blocks.is_empty()
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
        let Some(start) = self.first_block_for_key(key, policy) else {
            return Ok(Vec::new());
        };

        let mut records = Vec::new();
        for (offset, block) in self.data_blocks[start..].iter().enumerate() {
            if block.smallest_internal_key.user_key() > key {
                break;
            }
            let had_filter = block.point_key_filter.is_some();
            if !self.block_point_filter_allows(block, key) {
                continue;
            }
            let block = self.load_data_block(start + offset, block_cache)?;
            let block_records = data_block_point_records_for_key(&block, key, policy);
            if had_filter && block_records.is_empty() {
                self.filter_stats.record_block_point_false_positive();
            }
            records.extend(block_records);
        }
        if self.point_key_filter.is_some() && records.is_empty() {
            self.filter_stats.record_table_point_false_positive();
        }
        Ok(records)
    }

    pub(crate) fn newest_visible_point_record_for_key_with_cache(
        &self,
        key: &[u8],
        read_sequence: Sequence,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<TablePointRecord>> {
        let Some(start) = self.first_block_for_key(key, policy) else {
            return Ok(None);
        };

        let mut saw_point_key = false;
        for (offset, block) in self.data_blocks[start..].iter().enumerate() {
            if block.smallest_internal_key.user_key() > key {
                break;
            }
            let had_filter = block.point_key_filter.is_some();
            if !self.block_point_filter_allows(block, key) {
                continue;
            }
            let block = self.load_data_block(start + offset, block_cache)?;
            let block_has_key = data_block_has_point_key(&block, key, policy);
            if !block_has_key {
                if had_filter {
                    self.filter_stats.record_block_point_false_positive();
                }
                continue;
            }
            saw_point_key = true;
            if let Some(record) =
                data_block_newest_visible_point_record_for_key(&block, key, read_sequence, policy)
            {
                return Ok(Some(record));
            }
        }

        if self.point_key_filter.is_some() && !saw_point_key {
            self.filter_stats.record_table_point_false_positive();
        }
        Ok(None)
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
        let Some(start) = self.first_block_for_range(range, policy) else {
            return Ok(Vec::new());
        };

        let mut records = Vec::new();
        for (offset, block) in self.data_blocks[start..].iter().enumerate() {
            if key_is_after_end(block.smallest_internal_key.user_key(), &range.end) {
                break;
            }
            if !block.overlaps_range(range) {
                continue;
            }
            let block = self.load_data_block(start + offset, block_cache)?;
            records.extend(data_block_point_records_in_range(&block, range, policy));
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
        let Some(start) = self.first_block_for_prefix(prefix, policy) else {
            return Ok(Vec::new());
        };

        let mut records = Vec::new();
        for (offset, block) in self.data_blocks[start..].iter().enumerate() {
            if !block.prefix_bounds_may_overlap(prefix) {
                break;
            }
            let (allowed, had_filter) = self.block_prefix_filter_allows(block, prefix, extractor);
            if !allowed {
                continue;
            }
            let block = self.load_data_block(start + offset, block_cache)?;
            let block_records = data_block_point_records_with_prefix(&block, prefix, policy);
            if had_filter && block_records.is_empty() {
                self.filter_stats.record_block_prefix_false_positive();
            }
            records.extend(block_records);
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
        allowed
    }

    #[must_use]
    pub(crate) fn key_bounds_may_contain_key(&self, key: &[u8]) -> bool {
        self.has_key_bounds()
            && self.properties.smallest_user_key.as_slice() <= key
            && key <= self.properties.largest_user_key.as_slice()
    }

    #[must_use]
    pub(crate) fn key_bounds_overlap_range(&self, range: &KeyRange) -> bool {
        self.has_key_bounds()
            && !key_is_after_end(self.properties.smallest_user_key.as_slice(), &range.end)
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

    fn table_prefix_filter_result(
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

    fn block_point_filter_allows(&self, block: &TableDataBlock, key: &[u8]) -> bool {
        if !block.key_bounds_may_contain(key) {
            return false;
        }
        let Some(allowed) = block.point_filter_result(key) else {
            return true;
        };
        self.filter_stats.record_block_point(allowed);
        allowed
    }

    fn block_prefix_filter_allows(
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

    fn all_point_records(&self) -> Result<Vec<TablePointRecord>> {
        if let Some(records) = &self.point_records {
            return Ok(records.clone());
        }

        let mut records = Vec::new();
        for block_index in 0..self.data_blocks.len() {
            let block = self.load_data_block(block_index, None)?;
            records.extend(block.records.iter().cloned());
        }
        validate_sorted_point_records(&records)?;
        Ok(records)
    }

    fn load_range_tombstones(&self) -> Result<Vec<TableRangeTombstone>> {
        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table range tombstones are not loaded".to_owned(),
            });
        };
        let (codec, payload) = read_single_block_section_from_file(
            path,
            self.file.as_deref(),
            self.payload_len,
            self.footer.range_tombstones,
        )?;
        validate_block_codec(codec, self.properties.codec, TableSection::RangeTombstones)?;
        decode_range_tombstone_block(&payload)
    }

    fn load_data_block(
        &self,
        block_index: usize,
        block_cache: Option<&BlockCache>,
    ) -> Result<Arc<DecodedDataBlock>> {
        if let Some(records) = &self.point_records {
            let block = self
                .data_blocks
                .get(block_index)
                .ok_or_else(|| invalid_table("data block index outside table"))?;
            let records = records
                .get(block.record_range.clone())
                .ok_or_else(|| invalid_table("data block record range outside table"))?
                .to_vec();
            let restart_indices = block
                .restart_indices
                .iter()
                .map(|index| index.saturating_sub(block.record_range.start))
                .collect();
            return Ok(Arc::new(DecodedDataBlock::new(records, restart_indices)));
        }

        let block = self
            .data_blocks
            .get(block_index)
            .ok_or_else(|| invalid_table("data block index outside table"))?;
        let Some(path) = &self.path else {
            return Err(Error::Corruption {
                message: "table data block has no file path".to_owned(),
            });
        };
        let key = BlockCacheKey::new(self.properties.id, block_index);
        let entry = DataBlockIndexEntry {
            smallest_internal_key: block.smallest_internal_key.clone(),
            largest_internal_key: block.largest_internal_key.clone(),
            block: block.block,
            point_key_filter: block.point_key_filter.clone(),
            prefix_filter: block.prefix_filter.clone(),
        };
        if let Some(block_cache) = block_cache {
            let path = path.clone();
            let file = self.file.as_ref().map(Arc::clone);
            let payload_len = self.payload_len;
            let codec = self.properties.codec;
            block_cache.get_or_insert_with(key, move || {
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

    fn first_block_for_key(&self, key: &[u8], policy: IndexSearchPolicy) -> Option<usize> {
        let index = search::partition_point_by(self.data_blocks.len(), policy, |index| {
            self.data_blocks[index].largest_internal_key.user_key() < key
        });
        (index < self.data_blocks.len()).then_some(index)
    }

    fn first_block_for_range(&self, range: &KeyRange, policy: IndexSearchPolicy) -> Option<usize> {
        let index = search::partition_point_by(self.data_blocks.len(), policy, |index| {
            key_is_before_start(
                self.data_blocks[index].largest_internal_key.user_key(),
                &range.start,
            )
        });
        (index < self.data_blocks.len()).then_some(index)
    }

    fn first_block_for_prefix(&self, prefix: &[u8], policy: IndexSearchPolicy) -> Option<usize> {
        let index = search::partition_point_by(self.data_blocks.len(), policy, |index| {
            self.data_blocks[index].largest_internal_key.user_key() < prefix
        });
        (index < self.data_blocks.len()).then_some(index)
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

    fn last_block_for_range(&self, range: &KeyRange, policy: IndexSearchPolicy) -> Option<usize> {
        let upper = search::partition_point_by(self.data_blocks.len(), policy, |index| {
            !key_is_after_end(
                self.data_blocks[index].smallest_internal_key.user_key(),
                &range.end,
            )
        });
        upper.checked_sub(1)
    }

    fn last_block_for_prefix(&self, prefix: &[u8], policy: IndexSearchPolicy) -> Option<usize> {
        let end = prefix_successor(prefix).map_or(Bound::Unbounded, Bound::Excluded);
        let range = KeyRange {
            start: Bound::Included(prefix.to_vec()),
            end,
        };
        self.last_block_for_range(&range, policy)
    }
}

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
    pending: Option<ScanRecord>,
    exhausted: bool,
}

impl TablePointCursor {
    fn new(
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

    fn next_record(&mut self) -> Result<Option<ScanRecord>> {
        match self.direction {
            Direction::Forward => self.next_record_forward(),
            Direction::Reverse => self.next_record_reverse(),
        }
    }

    fn next_record_forward(&mut self) -> Result<Option<ScanRecord>> {
        while !self.exhausted {
            let Some(block_index) = self.block_index else {
                return Ok(None);
            };
            match self.forward_block_state(block_index) {
                CursorBlockState::Scan => {}
                CursorBlockState::Skip => {
                    self.move_to_next_block();
                    continue;
                }
                CursorBlockState::Done => {
                    self.exhausted = true;
                    return Ok(None);
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
                        return Ok(Some((record.internal_key, record.value)));
                    }
                    ForwardKeyState::After => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.move_to_next_block();
        }

        Ok(None)
    }

    fn next_record_reverse(&mut self) -> Result<Option<ScanRecord>> {
        while !self.exhausted {
            let Some(block_index) = self.block_index else {
                return Ok(None);
            };
            match self.reverse_block_state(block_index) {
                CursorBlockState::Scan => {}
                CursorBlockState::Skip => {
                    self.move_to_previous_block();
                    continue;
                }
                CursorBlockState::Done => {
                    self.exhausted = true;
                    return Ok(None);
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
                        return Ok(Some((record.internal_key, record.value)));
                    }
                    ReverseKeyState::Below => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            self.move_to_previous_block();
        }

        Ok(None)
    }

    fn forward_block_state(&self, block_index: usize) -> CursorBlockState {
        let block = &self.table.data_blocks[block_index];
        match &self.selector {
            ScanSelector::Range(range) => {
                if key_is_after_end(block.smallest_internal_key.user_key(), &range.end) {
                    CursorBlockState::Done
                } else if block.overlaps_range(range) {
                    CursorBlockState::Scan
                } else {
                    CursorBlockState::Skip
                }
            }
            ScanSelector::Prefix(prefix) => {
                if !block.prefix_bounds_may_overlap(prefix) {
                    if block.largest_internal_key.user_key() < prefix.as_slice() {
                        CursorBlockState::Skip
                    } else {
                        CursorBlockState::Done
                    }
                } else if self
                    .current_block
                    .as_ref()
                    .is_some_and(|(current_index, _)| *current_index == block_index)
                {
                    CursorBlockState::Scan
                } else {
                    let (allowed, _) = self.table.block_prefix_filter_allows(
                        block,
                        prefix,
                        &self.prefix_extractor,
                    );
                    if allowed {
                        CursorBlockState::Scan
                    } else {
                        CursorBlockState::Skip
                    }
                }
            }
        }
    }

    fn reverse_block_state(&self, block_index: usize) -> CursorBlockState {
        let block = &self.table.data_blocks[block_index];
        match &self.selector {
            ScanSelector::Range(range) => {
                if key_is_before_start(block.largest_internal_key.user_key(), &range.start) {
                    CursorBlockState::Done
                } else if block.overlaps_range(range) {
                    CursorBlockState::Scan
                } else {
                    CursorBlockState::Skip
                }
            }
            ScanSelector::Prefix(prefix) => {
                if block.largest_internal_key.user_key() < prefix.as_slice() {
                    CursorBlockState::Done
                } else if !block.prefix_bounds_may_overlap(prefix) {
                    CursorBlockState::Skip
                } else if self
                    .current_block
                    .as_ref()
                    .is_some_and(|(current_index, _)| *current_index == block_index)
                {
                    CursorBlockState::Scan
                } else {
                    let (allowed, _) = self.table.block_prefix_filter_allows(
                        block,
                        prefix,
                        &self.prefix_extractor,
                    );
                    if allowed {
                        CursorBlockState::Scan
                    } else {
                        CursorBlockState::Skip
                    }
                }
            }
        }
    }

    fn move_to_next_block(&mut self) {
        let Some(block_index) = self.block_index else {
            self.exhausted = true;
            return;
        };
        let next = block_index + 1;
        self.block_index = (next < self.table.data_blocks.len()).then_some(next);
        self.record_index = 0;
        self.current_block = None;
    }

    fn move_to_previous_block(&mut self) {
        let Some(block_index) = self.block_index else {
            self.exhausted = true;
            return;
        };
        self.block_index = block_index.checked_sub(1);
        self.record_index = 0;
        self.current_block = None;
    }

    fn ensure_current_block(&mut self, block_index: usize) -> Result<()> {
        if self
            .current_block
            .as_ref()
            .is_some_and(|(current_index, _)| *current_index == block_index)
        {
            return Ok(());
        }

        let block = self
            .table
            .load_data_block(block_index, self.block_cache.as_deref())?;
        if let ScanSelector::Prefix(prefix) = &self.selector {
            let table_block = &self.table.data_blocks[block_index];
            if table_block
                .prefix_filter_result(prefix, &self.prefix_extractor)
                .is_some()
                && !data_block_has_prefix(&block, prefix, self.policy)
            {
                self.table.filter_stats.record_block_prefix_false_positive();
            }
        }
        self.record_index = match self.direction {
            Direction::Forward => {
                first_record_index_for_decoded_block(&block, &self.selector, self.policy)
            }
            Direction::Reverse => block.records.len(),
        };
        self.current_block = Some((block_index, block));
        Ok(())
    }

    fn current_block_len(&mut self, block_index: usize) -> Result<usize> {
        self.ensure_current_block(block_index)?;
        Ok(self
            .current_block
            .as_ref()
            .map_or(0, |(_, block)| block.records.len()))
    }

    fn current_block_record(
        &mut self,
        block_index: usize,
        record_index: usize,
    ) -> Result<TablePointRecord> {
        self.ensure_current_block(block_index)?;
        self.current_block
            .as_ref()
            .and_then(|(_, block)| block.records.get(record_index))
            .cloned()
            .ok_or_else(|| invalid_table("cursor record index outside data block"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorBlockState {
    Scan,
    Skip,
    Done,
}

fn first_block_for_selector(
    table: &Table,
    selector: &ScanSelector,
    policy: IndexSearchPolicy,
) -> Option<usize> {
    match selector {
        ScanSelector::Range(range) => table.first_block_for_range(range, policy),
        ScanSelector::Prefix(prefix) => table.first_block_for_prefix(prefix, policy),
    }
}

fn last_block_for_selector(
    table: &Table,
    selector: &ScanSelector,
    policy: IndexSearchPolicy,
) -> Option<usize> {
    match selector {
        ScanSelector::Range(range) => table.last_block_for_range(range, policy),
        ScanSelector::Prefix(prefix) => table.last_block_for_prefix(prefix, policy),
    }
}

fn first_record_index_for_decoded_block(
    block: &DecodedDataBlock,
    selector: &ScanSelector,
    policy: IndexSearchPolicy,
) -> usize {
    match selector {
        ScanSelector::Range(range) => {
            data_block_restart_index_for_bound(block, &range.start, policy)
        }
        ScanSelector::Prefix(prefix) => data_block_restart_index_for_key(block, prefix, policy),
    }
}

fn data_block_point_records_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    policy: IndexSearchPolicy,
) -> Vec<TablePointRecord> {
    let range = data_block_point_record_range_for_key(block, key, policy);
    block.records[range].to_vec()
}

fn data_block_newest_visible_point_record_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    read_sequence: Sequence,
    policy: IndexSearchPolicy,
) -> Option<TablePointRecord> {
    let range = data_block_point_record_range_for_key(block, key, policy);
    block.records[range]
        .iter()
        .find(move |record| {
            record.internal_key.user_key() == key && record.internal_key.sequence() <= read_sequence
        })
        .cloned()
}

fn data_block_has_point_key(
    block: &DecodedDataBlock,
    key: &[u8],
    policy: IndexSearchPolicy,
) -> bool {
    !data_block_point_record_range_for_key(block, key, policy).is_empty()
}

fn data_block_point_records_in_range(
    block: &DecodedDataBlock,
    range: &KeyRange,
    policy: IndexSearchPolicy,
) -> Vec<TablePointRecord> {
    let start = data_block_restart_index_for_bound(block, &range.start, policy);
    block.records[start..]
        .iter()
        .skip_while(move |record| key_is_before_start(record.internal_key.user_key(), &range.start))
        .take_while(move |record| !key_is_after_end(record.internal_key.user_key(), &range.end))
        .cloned()
        .collect()
}

#[cfg(test)]
fn data_block_point_records_with_prefix(
    block: &DecodedDataBlock,
    prefix: &[u8],
    policy: IndexSearchPolicy,
) -> Vec<TablePointRecord> {
    let start = data_block_restart_index_for_key(block, prefix, policy);
    block.records[start..]
        .iter()
        .skip_while(move |record| record.internal_key.user_key() < prefix)
        .take_while(move |record| record.internal_key.user_key().starts_with(prefix))
        .cloned()
        .collect()
}

fn data_block_has_prefix(
    block: &DecodedDataBlock,
    prefix: &[u8],
    policy: IndexSearchPolicy,
) -> bool {
    let start = data_block_restart_index_for_key(block, prefix, policy);
    block.records[start..]
        .iter()
        .find(|record| record.internal_key.user_key() >= prefix)
        .is_some_and(|record| record.internal_key.user_key().starts_with(prefix))
}

fn data_block_restart_index_for_bound(
    block: &DecodedDataBlock,
    bound: &Bound<Vec<u8>>,
    policy: IndexSearchPolicy,
) -> usize {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => {
            data_block_restart_index_for_key(block, key, policy)
        }
        Bound::Unbounded => 0,
    }
}

fn data_block_restart_index_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    policy: IndexSearchPolicy,
) -> usize {
    let upper = search::partition_point_by(block.restart_indices.len(), policy, |index| {
        block.records[block.restart_indices[index]]
            .internal_key
            .user_key()
            <= key
    });
    if upper == 0 {
        0
    } else {
        block.restart_indices[upper - 1]
    }
}

fn data_block_point_record_range_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    policy: IndexSearchPolicy,
) -> Range<usize> {
    if let Some(range) = block.point_lookup_index.get(key) {
        return range.clone();
    }

    let start = data_block_first_record_index_for_key(block, key, policy);
    let end = block.records[start..]
        .iter()
        .take_while(|record| record.internal_key.user_key() == key)
        .count()
        .saturating_add(start);
    start..end
}

fn data_block_first_record_index_for_key(
    block: &DecodedDataBlock,
    key: &[u8],
    policy: IndexSearchPolicy,
) -> usize {
    search::partition_point_by(block.records.len(), policy, |index| {
        block.records[index].internal_key.user_key() < key
    })
}

fn build_data_block_point_lookup_index(
    records: &[TablePointRecord],
) -> HashMap<Vec<u8>, Range<usize>> {
    let mut index = HashMap::new();
    let mut start = 0;
    while start < records.len() {
        let key = records[start].internal_key.user_key();
        let mut end = start + 1;
        while end < records.len() && records[end].internal_key.user_key() == key {
            end += 1;
        }
        index.insert(key.to_vec(), start..end);
        start = end;
    }
    index
}

#[must_use]
pub fn table_path(db_path: &Path, table_id: TableId) -> PathBuf {
    db_path.join(format!(
        "table-{id:020}.{TABLE_FILE_EXTENSION}",
        id = table_id.get()
    ))
}

pub(crate) fn list_table_file_ids(db_path: &Path) -> Result<BTreeSet<TableId>> {
    let mut table_ids = BTreeSet::new();

    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let path = entry.path();
        let has_table_extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(TABLE_FILE_EXTENSION));
        if !has_table_extension {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Some(table_id) = stem.strip_prefix("table-") else {
            continue;
        };
        let table_id = table_id
            .parse::<u64>()
            .map(TableId)
            .map_err(|_| Error::Corruption {
                message: format!("invalid table file name: {}", path.display()),
            })?;
        table_ids.insert(table_id);
    }

    Ok(table_ids)
}

pub(crate) fn write_table(
    path: &Path,
    table_id: TableId,
    level: TableLevel,
    options: &TableWriteOptions,
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[TableRangeTombstone],
) -> Result<Table> {
    if point_records.is_empty() && range_tombstones.is_empty() {
        return Err(Error::invalid_options("cannot write an empty table"));
    }

    // The caller batches the parent-directory sync after one or more table
    // writes and before publishing the manifest. That keeps table/blob file
    // names durable without forcing one directory sync per output file.
    let mut point_records = point_records.to_vec();
    point_records.sort_by(|left, right| left.0.cmp(&right.0));
    let db_path = path
        .parent()
        .ok_or_else(|| Error::invalid_options("table path has no parent"))?;
    let point_records = crate::blob::write_large_values(
        db_path,
        table_id.get(),
        options.blob_threshold_bytes,
        &point_records,
    )?
    .into_iter()
    .map(|(internal_key, value)| TablePointRecord {
        internal_key,
        value,
    })
    .collect::<Vec<_>>();
    let data_blocks = build_data_blocks(&point_records, options)?;

    let table = Table {
        path: None,
        file: None,
        payload_len: 0,
        footer: empty_footer(),
        properties: table_properties(
            table_id,
            level,
            options.codec,
            &point_records,
            range_tombstones,
        ),
        point_key_filter: build_point_key_filter(options, &point_records),
        prefix_filter: build_prefix_filter(options, &point_records),
        filter_stats: Arc::new(TableFilterStats::default()),
        point_records: Some(point_records),
        data_blocks,
        range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
            range_tombstones.to_vec(),
        ))))),
    };
    let payload = encode_table(&table)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("table payload exceeds u32::MAX"))?;
    let payload_checksum = checksum(&payload);
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());

    bytes.extend_from_slice(&TABLE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&TABLE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    bytes.extend_from_slice(&payload);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(tmp_path, path)?;

    read_table(path)
}

pub(crate) fn read_table(path: &Path) -> Result<Table> {
    let mut file = File::open(path).map_err(|error| Error::Corruption {
        message: format!(
            "referenced table {} cannot be opened: {error}",
            path.display()
        ),
    })?;
    let mut header = [0_u8; HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} header cannot be read: {error}",
                path.display()
            ),
        })?;
    let magic = read_u32_at(&header, 0)?;
    let version = read_u16_at(&header, 4)?;
    let payload_len = read_u32_at(&header, 6)? as usize;
    if magic != TABLE_MAGIC {
        return Err(Error::Corruption {
            message: "table magic mismatch".to_owned(),
        });
    }
    if version != TABLE_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported table version {version}"),
        });
    }
    let file_len = file.metadata()?.len();
    let expected_len = usize_to_u64(
        HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| invalid_table("table length overflow"))?,
        "table file length",
    )?;
    if file_len != expected_len {
        return Err(Error::Corruption {
            message: "table length mismatch".to_owned(),
        });
    }

    let footer = read_footer_from_file(&mut file, payload_len)?;
    validate_footer_sections_by_len(payload_len, &footer)?;

    let (properties_codec, properties_payload) =
        read_single_block_section_from_file(path, Some(&file), payload_len, footer.properties)?;
    let properties = decode_properties_block(&properties_payload)?;
    validate_block_codec(properties_codec, properties.codec, TableSection::Properties)?;

    let (index_codec, index_payload) =
        read_single_block_section_from_file(path, Some(&file), payload_len, footer.indexes)?;
    validate_block_codec(index_codec, properties.codec, TableSection::Indexes)?;
    let index_entries = decode_index_block(&index_payload)?;
    validate_data_index_covers_section(&index_entries, footer.data_blocks)?;
    let data_blocks = index_entries
        .into_iter()
        .map(TableDataBlock::from_index_entry)
        .collect::<Result<Vec<_>>>()?;

    let (filter_codec, filter_payload) =
        read_single_block_section_from_file(path, Some(&file), payload_len, footer.filters)?;
    validate_block_codec(filter_codec, properties.codec, TableSection::Filters)?;
    let (point_key_filter, prefix_filter) = decode_filter_block(&filter_payload)?;

    Ok(Table {
        path: Some(path.to_path_buf()),
        file: Some(Arc::new(file)),
        payload_len,
        footer,
        properties,
        point_records: None,
        data_blocks,
        range_tombstones: Arc::new(RwLock::new(None)),
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
    })
}

fn table_properties(
    table_id: TableId,
    level: TableLevel,
    codec: CodecId,
    point_records: &[TablePointRecord],
    range_tombstones: &[TableRangeTombstone],
) -> TableProperties {
    let mut smallest_sequence: Option<Sequence> = None;
    let mut largest_sequence: Option<Sequence> = None;

    for sequence in point_records
        .iter()
        .map(|record| record.internal_key.sequence())
        .chain(range_tombstones.iter().map(|tombstone| tombstone.sequence))
    {
        smallest_sequence =
            Some(smallest_sequence.map_or(sequence, |current| std::cmp::min(current, sequence)));
        largest_sequence =
            Some(largest_sequence.map_or(sequence, |current| std::cmp::max(current, sequence)));
    }

    let blob_file_ids = point_records
        .iter()
        .filter_map(|record| match record.value.as_ref() {
            Some(ValueRef::Blob { file_id, .. }) => Some(*file_id),
            Some(ValueRef::Inline(_)) | None => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let (smallest_user_key, largest_user_key) = table_key_bounds(point_records, range_tombstones);

    TableProperties {
        id: table_id,
        level,
        smallest_user_key,
        largest_user_key,
        smallest_sequence: smallest_sequence.unwrap_or(Sequence::ZERO),
        largest_sequence: largest_sequence.unwrap_or(Sequence::ZERO),
        codec,
        blob_file_ids,
    }
}

fn table_key_bounds(
    point_records: &[TablePointRecord],
    range_tombstones: &[TableRangeTombstone],
) -> (Vec<u8>, Vec<u8>) {
    let mut smallest = point_records
        .first()
        .map(|record| record.internal_key.user_key().to_vec());
    let mut largest = point_records
        .last()
        .map(|record| record.internal_key.user_key().to_vec());

    for tombstone in range_tombstones {
        let (Some(start), Some(end)) = (
            finite_bound_bytes(&tombstone.range.start),
            finite_bound_bytes(&tombstone.range.end),
        ) else {
            continue;
        };
        update_smallest(&mut smallest, start);
        update_largest(&mut largest, end);
    }

    match (smallest, largest) {
        (Some(smallest), Some(largest)) => (smallest, largest),
        _ => (Vec::new(), Vec::new()),
    }
}

fn finite_bound_bytes(bound: &Bound<Vec<u8>>) -> Option<Vec<u8>> {
    match bound {
        Bound::Included(bytes) | Bound::Excluded(bytes) => Some(bytes.clone()),
        Bound::Unbounded => None,
    }
}

fn update_smallest(current: &mut Option<Vec<u8>>, candidate: Vec<u8>) {
    if current
        .as_ref()
        .is_none_or(|current| candidate.as_slice() < current.as_slice())
    {
        *current = Some(candidate);
    }
}

fn update_largest(current: &mut Option<Vec<u8>>, candidate: Vec<u8>) {
    if current
        .as_ref()
        .is_none_or(|current| candidate.as_slice() > current.as_slice())
    {
        *current = Some(candidate);
    }
}

fn build_prefix_filter(
    options: &TableWriteOptions,
    point_records: &[TablePointRecord],
) -> Option<PrefixFilter> {
    match options.prefix_filter_policy {
        PrefixFilterPolicy::Disabled => None,
        PrefixFilterPolicy::Bloom { bits_per_prefix } => PrefixFilter::from_keys(
            options.prefix_extractor.clone(),
            point_records
                .iter()
                .map(|record| record.internal_key.user_key()),
            bits_per_prefix,
        ),
    }
}

fn build_point_key_filter(
    options: &TableWriteOptions,
    point_records: &[TablePointRecord],
) -> Option<PointKeyFilter> {
    match options.filter_policy {
        FilterPolicy::Disabled => None,
        FilterPolicy::Bloom { bits_per_key } => Some(PointKeyFilter::from_keys(
            point_records
                .iter()
                .map(|record| record.internal_key.user_key()),
            bits_per_key,
        )),
    }
}

fn build_data_blocks(
    point_records: &[TablePointRecord],
    options: &TableWriteOptions,
) -> Result<Vec<TableDataBlock>> {
    let mut data_blocks = Vec::new();
    let mut block_start = 0;

    while block_start < point_records.len() {
        let mut block_end = block_start;
        let mut estimated_len = 0_usize;
        while block_end < point_records.len() {
            let next_len = point_record_encoded_len(&point_records[block_end]);
            if block_end > block_start && estimated_len + next_len > options.block_bytes {
                break;
            }
            estimated_len += next_len;
            block_end += 1;
        }

        let restart_indices = (block_start..block_end)
            .step_by(DATA_BLOCK_RESTART_INTERVAL)
            .collect::<Vec<_>>();
        let records = &point_records[block_start..block_end];
        data_blocks.push(TableDataBlock::from_record_range(
            point_records,
            block_start..block_end,
            restart_indices,
            build_point_key_filter(options, records),
            build_prefix_filter(options, records),
        )?);
        block_start = block_end;
    }

    Ok(data_blocks)
}

fn encode_table(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let codec = table.properties.codec;
    let point_records = table
        .point_records
        .as_deref()
        .ok_or_else(|| invalid_table("cannot encode table without point records"))?;
    let (data_blocks, index_entries) =
        append_data_blocks(&mut bytes, codec, point_records, &table.data_blocks)?;
    let range_tombstones =
        append_single_block_section(&mut bytes, codec, &encode_range_tombstone_block(table)?)?;
    let filters = append_single_block_section(&mut bytes, codec, &encode_filter_block(table)?)?;
    let indexes =
        append_single_block_section(&mut bytes, codec, &encode_index_block(&index_entries)?)?;
    let properties = append_single_block_section(
        &mut bytes,
        codec,
        &encode_properties_block(&table.properties)?,
    )?;
    put_footer(
        &mut bytes,
        &TableFooter {
            data_blocks,
            range_tombstones,
            filters,
            indexes,
            properties,
        },
    );

    Ok(bytes)
}

#[cfg(test)]
fn decode_table(bytes: &[u8]) -> Result<Table> {
    if bytes.len() < HEADER_LEN {
        return Err(invalid_table("short header"));
    }

    let magic = read_u32_at(bytes, 0)?;
    let version = read_u16_at(bytes, 4)?;
    let payload_len = read_u32_at(bytes, 6)? as usize;
    let payload_checksum = read_u32_at(bytes, 10)?;
    if magic != TABLE_MAGIC {
        return Err(Error::Corruption {
            message: "table magic mismatch".to_owned(),
        });
    }
    if version != TABLE_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported table version {version}"),
        });
    }
    if bytes.len() != HEADER_LEN + payload_len {
        return Err(Error::Corruption {
            message: "table length mismatch".to_owned(),
        });
    }

    let payload = &bytes[HEADER_LEN..];
    if checksum(payload) != payload_checksum {
        return Err(Error::Corruption {
            message: "table checksum mismatch".to_owned(),
        });
    }

    let footer = read_footer(payload)?;
    validate_footer_sections(payload, &footer)?;

    let (properties_codec, properties_payload) =
        read_single_block_section(payload, footer.properties)?;
    let properties = decode_properties_block(&properties_payload)?;
    validate_block_codec(properties_codec, properties.codec, TableSection::Properties)?;

    let (index_codec, index_payload) = read_single_block_section(payload, footer.indexes)?;
    validate_block_codec(index_codec, properties.codec, TableSection::Indexes)?;
    let index_entries = decode_index_block(&index_payload)?;
    validate_data_index_covers_section(&index_entries, footer.data_blocks)?;

    let (point_records, data_blocks) =
        decode_test_point_records_and_blocks(payload, &properties, &index_entries)?;

    let (tombstone_codec, tombstone_payload) =
        read_single_block_section(payload, footer.range_tombstones)?;
    validate_block_codec(
        tombstone_codec,
        properties.codec,
        TableSection::RangeTombstones,
    )?;
    let range_tombstones = decode_range_tombstone_block(&tombstone_payload)?;
    let (filter_codec, filter_payload) = read_single_block_section(payload, footer.filters)?;
    validate_block_codec(filter_codec, properties.codec, TableSection::Filters)?;
    let (point_key_filter, prefix_filter) = decode_filter_block(&filter_payload)?;
    if properties
        != table_properties(
            properties.id,
            properties.level,
            properties.codec,
            &point_records,
            &range_tombstones,
        )
    {
        return Err(Error::Corruption {
            message: "table properties do not match encoded records".to_owned(),
        });
    }

    Ok(Table {
        path: None,
        file: None,
        payload_len: payload.len(),
        footer,
        properties,
        point_records: Some(point_records),
        data_blocks,
        range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
            range_tombstones,
        ))))),
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
    })
}

#[cfg(test)]
fn decode_test_point_records_and_blocks(
    payload: &[u8],
    properties: &TableProperties,
    index_entries: &[DataBlockIndexEntry],
) -> Result<(Vec<TablePointRecord>, Vec<TableDataBlock>)> {
    let mut point_records = Vec::new();
    let mut data_blocks = Vec::new();
    for entry in index_entries {
        let (block_codec, block_payload) = read_checked_block(payload, entry.block)?;
        validate_block_codec(block_codec, properties.codec, TableSection::DataBlocks)?;
        let decoded_block = decode_data_block(&block_payload)?;
        validate_data_block_entry(entry, &decoded_block.records)?;
        validate_data_block_filters(entry, &decoded_block.records)?;
        let record_start = point_records.len();
        point_records.extend(decoded_block.records);
        let record_end = point_records.len();
        data_blocks.push(TableDataBlock::from_record_range_and_block(
            &point_records,
            record_start..record_end,
            decoded_block
                .restart_indices
                .into_iter()
                .map(|index| record_start + index)
                .collect(),
            entry.block,
            entry.point_key_filter.clone(),
            entry.prefix_filter.clone(),
        )?);
    }
    validate_sorted_point_records(&point_records)?;
    Ok((point_records, data_blocks))
}

fn append_data_blocks(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    point_records: &[TablePointRecord],
    data_blocks: &[TableDataBlock],
) -> Result<(SectionHandle, Vec<DataBlockIndexEntry>)> {
    let section_start = bytes.len();
    let mut index_entries = Vec::new();

    for data_block in data_blocks {
        let records = point_records
            .get(data_block.record_range.clone())
            .ok_or_else(|| invalid_table("data block record range outside table"))?;
        let block_payload = encode_data_block(records)?;
        let block = append_checked_block(bytes, codec, &block_payload)?;
        index_entries.push(DataBlockIndexEntry {
            smallest_internal_key: data_block.smallest_internal_key.clone(),
            largest_internal_key: data_block.largest_internal_key.clone(),
            block,
            point_key_filter: data_block.point_key_filter.clone(),
            prefix_filter: data_block.prefix_filter.clone(),
        });
    }

    Ok((
        SectionHandle::from_span(section_start, bytes.len())?,
        index_entries,
    ))
}

fn append_single_block_section(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    block_payload: &[u8],
) -> Result<SectionHandle> {
    let section_start = bytes.len();
    append_checked_block(bytes, codec, block_payload)?;
    SectionHandle::from_span(section_start, bytes.len())
}

fn append_checked_block(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    block_payload: &[u8],
) -> Result<BlockHandle> {
    let section_start = bytes.len();
    let encoded = codec::encode_block(codec, block_payload)?;
    put_codec(bytes, codec);
    put_u32(
        bytes,
        usize_to_u32(block_payload.len(), "block payload length")?,
    );
    put_u32(bytes, usize_to_u32(encoded.len(), "encoded block length")?);
    put_u32(bytes, checksum(&encoded));
    bytes.extend_from_slice(&encoded);

    Ok(BlockHandle {
        offset: usize_to_u64(section_start, "block offset")?,
        len: usize_to_u64(bytes.len() - section_start, "block length")?,
    })
}

fn encode_data_block(records: &[TablePointRecord]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut restart_offsets = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(records.len(), "data block record count")?,
    );

    for (index, record) in records.iter().enumerate() {
        if index % DATA_BLOCK_RESTART_INTERVAL == 0 {
            restart_offsets.push(usize_to_u32(bytes.len(), "data block restart offset")?);
        }
        put_internal_key(&mut bytes, &record.internal_key)?;
        put_value_ref(&mut bytes, record.value.as_ref())?;
    }

    put_u32(
        &mut bytes,
        usize_to_u32(restart_offsets.len(), "data block restart count")?,
    );
    for restart_offset in restart_offsets {
        put_u32(&mut bytes, restart_offset);
    }

    Ok(bytes)
}

fn encode_range_tombstone_block(table: &Table) -> Result<Vec<u8>> {
    let range_tombstones = table.range_tombstones()?;
    let mut bytes = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(
            range_tombstones.all().len(),
            "range tombstone block record count",
        )?,
    );
    for tombstone in range_tombstones.all() {
        put_bound(&mut bytes, &tombstone.range.start)?;
        put_bound(&mut bytes, &tombstone.range.end)?;
        put_u64(&mut bytes, tombstone.sequence.get());
        put_u32(&mut bytes, tombstone.batch_index);
    }
    Ok(bytes)
}

fn encode_filter_block(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_point_key_filter(&mut bytes, table.point_key_filter.as_ref())?;
    put_prefix_filter(&mut bytes, table.prefix_filter.as_ref())?;
    Ok(bytes)
}

fn encode_index_block(index_entries: &[DataBlockIndexEntry]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(index_entries.len(), "data block index entry count")?,
    );
    for entry in index_entries {
        put_internal_key(&mut bytes, &entry.smallest_internal_key)?;
        put_internal_key(&mut bytes, &entry.largest_internal_key)?;
        put_u64(&mut bytes, entry.block.offset);
        put_u64(&mut bytes, entry.block.len);
        put_point_key_filter(&mut bytes, entry.point_key_filter.as_ref())?;
        put_prefix_filter(&mut bytes, entry.prefix_filter.as_ref())?;
    }
    Ok(bytes)
}

fn put_point_key_filter(bytes: &mut Vec<u8>, filter: Option<&PointKeyFilter>) -> Result<()> {
    match filter {
        None => put_u8(bytes, POINT_KEY_FILTER_ABSENT),
        Some(filter) => {
            put_u8(bytes, POINT_KEY_FILTER_PRESENT);
            put_u64(bytes, filter.bit_count());
            put_u8(bytes, filter.hash_count());
            put_bytes(bytes, filter.bytes())?;
        }
    }

    Ok(())
}

fn put_prefix_filter(bytes: &mut Vec<u8>, filter: Option<&PrefixFilter>) -> Result<()> {
    match filter {
        None => put_u8(bytes, PREFIX_FILTER_ABSENT),
        Some(filter) => {
            put_u8(bytes, PREFIX_FILTER_PRESENT);
            put_prefix_extractor(bytes, filter.extractor())?;
            put_u64(bytes, filter.bit_count());
            put_u8(bytes, filter.hash_count());
            put_bytes(bytes, filter.bytes())?;
        }
    }

    Ok(())
}

fn encode_properties_block(properties: &TableProperties) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_properties(&mut bytes, properties)?;
    Ok(bytes)
}

#[cfg(test)]
fn read_footer(payload: &[u8]) -> Result<TableFooter> {
    if payload.len() < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let footer_start = payload.len() - FOOTER_LEN;
    read_footer_bytes(&payload[footer_start..])
}

fn read_footer_from_file(file: &mut File, payload_len: usize) -> Result<TableFooter> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let footer_start = HEADER_LEN
        .checked_add(payload_len - FOOTER_LEN)
        .ok_or_else(|| invalid_table("footer offset overflow"))?;
    file.seek(SeekFrom::Start(usize_to_u64(
        footer_start,
        "footer file offset",
    )?))?;
    let mut footer = [0_u8; FOOTER_LEN];
    file.read_exact(&mut footer)?;
    read_footer_bytes(&footer)
}

fn read_footer_bytes(footer: &[u8]) -> Result<TableFooter> {
    if footer.len() != FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let stored_checksum = read_u32_at(footer, FOOTER_LEN - 4)?;
    if checksum(&footer[..FOOTER_LEN - 4]) != stored_checksum {
        return Err(Error::Corruption {
            message: "table footer checksum mismatch".to_owned(),
        });
    }

    let mut cursor = Cursor::new(footer);
    let magic = cursor.read_u32()?;
    let version = cursor.read_u16()?;
    if magic != FOOTER_MAGIC {
        return Err(Error::Corruption {
            message: "table footer magic mismatch".to_owned(),
        });
    }
    if version != TABLE_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported table footer version {version}"),
        });
    }

    let footer = TableFooter {
        data_blocks: cursor.read_section_handle()?,
        range_tombstones: cursor.read_section_handle()?,
        filters: cursor.read_section_handle()?,
        indexes: cursor.read_section_handle()?,
        properties: cursor.read_section_handle()?,
    };
    let _footer_checksum = cursor.read_u32()?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing footer bytes"));
    }

    Ok(footer)
}

fn put_footer(bytes: &mut Vec<u8>, footer: &TableFooter) {
    let mut footer_bytes = Vec::with_capacity(FOOTER_LEN);
    put_u32(&mut footer_bytes, FOOTER_MAGIC);
    put_u16(&mut footer_bytes, TABLE_VERSION);
    put_section_handle(&mut footer_bytes, footer.data_blocks);
    put_section_handle(&mut footer_bytes, footer.range_tombstones);
    put_section_handle(&mut footer_bytes, footer.filters);
    put_section_handle(&mut footer_bytes, footer.indexes);
    put_section_handle(&mut footer_bytes, footer.properties);
    let footer_checksum = checksum(&footer_bytes);
    put_u32(&mut footer_bytes, footer_checksum);
    debug_assert_eq!(footer_bytes.len(), FOOTER_LEN);
    bytes.extend_from_slice(&footer_bytes);
}

const fn empty_footer() -> TableFooter {
    let empty = SectionHandle { offset: 0, len: 0 };
    TableFooter {
        data_blocks: empty,
        range_tombstones: empty,
        filters: empty,
        indexes: empty,
        properties: empty,
    }
}

#[cfg(test)]
fn validate_footer_sections(payload: &[u8], footer: &TableFooter) -> Result<()> {
    validate_footer_sections_by_len(payload.len(), footer)
}

fn validate_footer_sections_by_len(payload_len: usize, footer: &TableFooter) -> Result<()> {
    let footer_start = payload_len - FOOTER_LEN;
    let mut expected_start = 0_usize;
    for section in [
        footer.data_blocks,
        footer.range_tombstones,
        footer.filters,
        footer.indexes,
        footer.properties,
    ] {
        let (section_start, section_end) = section_bounds(section)?;
        if section_start != expected_start || section_end > footer_start {
            return Err(Error::Corruption {
                message: "table section layout is inconsistent".to_owned(),
            });
        }
        expected_start = section_end;
    }
    if expected_start != footer_start {
        return Err(Error::Corruption {
            message: "table footer does not cover all section bytes".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
fn read_single_block_section(payload: &[u8], section: SectionHandle) -> Result<(CodecId, Vec<u8>)> {
    let (_, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty single-block section"));
    }
    let block = BlockHandle {
        offset: section.offset,
        len: section.len,
    };
    let (_, block_end) = block_bounds(block)?;
    if block_end != section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    read_checked_block(payload, block)
}

fn read_single_block_section_from_file(
    path: &Path,
    file: Option<&File>,
    payload_len: usize,
    section: SectionHandle,
) -> Result<(CodecId, Vec<u8>)> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let (_, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty single-block section"));
    }
    if section_end > payload_len - FOOTER_LEN {
        return Err(Error::Corruption {
            message: "table section layout is inconsistent".to_owned(),
        });
    }
    let block = BlockHandle {
        offset: section.offset,
        len: section.len,
    };
    let (_, block_end) = block_bounds(block)?;
    if block_end != section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    read_checked_block_from_file(path, file, payload_len, block)
}

#[cfg(test)]
fn read_checked_block(payload: &[u8], block: BlockHandle) -> Result<(CodecId, Vec<u8>)> {
    let (start, end) = block_bounds(block)?;
    let block_bytes = payload
        .get(start..end)
        .ok_or_else(|| invalid_table("block outside table payload"))?;
    read_checked_block_bytes(block_bytes)
}

fn read_checked_block_from_file(
    path: &Path,
    file: Option<&File>,
    payload_len: usize,
    block: BlockHandle,
) -> Result<(CodecId, Vec<u8>)> {
    let (start, end) = block_bounds(block)?;
    if end > payload_len {
        return Err(invalid_table("block outside table payload"));
    }

    let file_offset = HEADER_LEN
        .checked_add(start)
        .ok_or_else(|| invalid_table("block file offset overflow"))?;
    let mut file = table_file_for_block_read(path, file)?;
    file.seek(SeekFrom::Start(usize_to_u64(
        file_offset,
        "block file offset",
    )?))?;
    let mut block_bytes = vec![0_u8; end - start];
    file.read_exact(&mut block_bytes)?;
    read_checked_block_bytes(&block_bytes)
}

fn read_data_block_from_file(
    path: &Path,
    file: Option<&File>,
    payload_len: usize,
    expected_codec: CodecId,
    entry: &DataBlockIndexEntry,
) -> Result<DecodedDataBlock> {
    let (actual_codec, payload) =
        read_checked_block_from_file(path, file, payload_len, entry.block)?;
    validate_block_codec(actual_codec, expected_codec, TableSection::DataBlocks)?;
    let decoded = decode_data_block(&payload)?;
    validate_data_block_entry(entry, &decoded.records)?;
    validate_data_block_filters(entry, &decoded.records)?;
    Ok(decoded)
}

fn table_file_for_block_read(path: &Path, file: Option<&File>) -> Result<File> {
    if let Some(file) = file {
        return file.try_clone().map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} handle cannot be cloned: {error}",
                path.display()
            ),
        });
    }

    File::open(path).map_err(|error| Error::Corruption {
        message: format!(
            "referenced table {} cannot be opened: {error}",
            path.display()
        ),
    })
}

fn read_checked_block_bytes(block_bytes: &[u8]) -> Result<(CodecId, Vec<u8>)> {
    if block_bytes.len() < BLOCK_HEADER_LEN {
        return Err(invalid_table("short block header"));
    }

    let codec = codec_from_tag(block_bytes[0])?;
    let uncompressed_len = read_u32_at(block_bytes, 1)? as usize;
    let encoded_len = read_u32_at(block_bytes, 5)? as usize;
    let expected_checksum = read_u32_at(block_bytes, 9)?;
    if block_bytes.len() != BLOCK_HEADER_LEN + encoded_len {
        return Err(Error::Corruption {
            message: "block length mismatch".to_owned(),
        });
    }

    let encoded = &block_bytes[BLOCK_HEADER_LEN..];
    if checksum(encoded) != expected_checksum {
        return Err(Error::Corruption {
            message: "block checksum mismatch".to_owned(),
        });
    }

    Ok((
        codec,
        codec::decode_block(codec, encoded, uncompressed_len)?,
    ))
}

fn validate_block_codec(actual: CodecId, expected: CodecId, section: TableSection) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "table {section:?} block codec {} does not match table codec {}",
            actual.as_str(),
            expected.as_str()
        ),
    })
}

fn decode_properties_block(bytes: &[u8]) -> Result<TableProperties> {
    let mut cursor = Cursor::new(bytes);
    let properties = cursor.read_properties()?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing properties block bytes"));
    }
    Ok(properties)
}

fn decode_index_block(bytes: &[u8]) -> Result<Vec<DataBlockIndexEntry>> {
    let mut cursor = Cursor::new(bytes);
    let entry_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        entry_count,
        cursor.remaining_len(),
        MIN_INDEX_ENTRY_BYTES,
        "index entry count exceeds block bytes",
    )?;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(DataBlockIndexEntry {
            smallest_internal_key: cursor.read_internal_key()?,
            largest_internal_key: cursor.read_internal_key()?,
            block: BlockHandle {
                offset: cursor.read_u64()?,
                len: cursor.read_u64()?,
            },
            point_key_filter: read_point_key_filter(&mut cursor)?,
            prefix_filter: read_prefix_filter(&mut cursor)?,
        });
    }
    if !cursor.is_finished() {
        return Err(invalid_table("trailing index block bytes"));
    }
    Ok(entries)
}

fn decode_data_block(bytes: &[u8]) -> Result<DecodedDataBlock> {
    let mut cursor = Cursor::new(bytes);
    let record_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        record_count,
        cursor.remaining_len(),
        MIN_DATA_RECORD_BYTES,
        "data record count exceeds block bytes",
    )?;
    let mut records = Vec::with_capacity(record_count);
    let mut record_offsets = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        record_offsets.push(cursor.offset);
        records.push(TablePointRecord {
            internal_key: cursor.read_internal_key()?,
            value: cursor.read_value_ref()?,
        });
    }
    let restart_indices = decode_restart_points(&mut cursor, &record_offsets)?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing data block bytes"));
    }
    Ok(DecodedDataBlock::new(records, restart_indices))
}

fn decode_range_tombstone_block(bytes: &[u8]) -> Result<Vec<TableRangeTombstone>> {
    let mut cursor = Cursor::new(bytes);
    let tombstone_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        tombstone_count,
        cursor.remaining_len(),
        MIN_RANGE_TOMBSTONE_BYTES,
        "range tombstone count exceeds block bytes",
    )?;
    let mut range_tombstones = Vec::with_capacity(tombstone_count);
    for _ in 0..tombstone_count {
        let start = cursor.read_bound()?;
        let end = cursor.read_bound()?;
        range_tombstones.push(TableRangeTombstone {
            range: KeyRange { start, end },
            sequence: Sequence::new(cursor.read_u64()?),
            batch_index: cursor.read_u32()?,
        });
    }
    if !cursor.is_finished() {
        return Err(invalid_table("trailing range tombstone block bytes"));
    }
    range_tombstone::sort_tombstones(&mut range_tombstones);
    Ok(range_tombstones)
}

fn decode_filter_block(bytes: &[u8]) -> Result<(Option<PointKeyFilter>, Option<PrefixFilter>)> {
    let mut cursor = Cursor::new(bytes);
    let point_key_filter = read_point_key_filter(&mut cursor)?;
    let prefix_filter = read_prefix_filter(&mut cursor)?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing filter block bytes"));
    }
    Ok((point_key_filter, prefix_filter))
}

fn read_point_key_filter(cursor: &mut Cursor<'_>) -> Result<Option<PointKeyFilter>> {
    match cursor.read_u8()? {
        POINT_KEY_FILTER_ABSENT => Ok(None),
        POINT_KEY_FILTER_PRESENT => {
            let bit_count = cursor.read_u64()?;
            let hash_count = cursor.read_u8()?;
            let bytes = cursor.read_bytes()?.to_vec();
            Ok(Some(PointKeyFilter::from_parts(
                bit_count, hash_count, bytes,
            )?))
        }
        tag => Err(Error::InvalidFormat {
            message: format!("unknown table point-key filter tag {tag}"),
        }),
    }
}

fn read_prefix_filter(cursor: &mut Cursor<'_>) -> Result<Option<PrefixFilter>> {
    match cursor.read_u8()? {
        PREFIX_FILTER_ABSENT => Ok(None),
        PREFIX_FILTER_PRESENT => {
            let extractor = cursor.read_prefix_extractor()?;
            let bit_count = cursor.read_u64()?;
            let hash_count = cursor.read_u8()?;
            let bytes = cursor.read_bytes()?.to_vec();
            Ok(Some(PrefixFilter::from_parts(
                extractor, bit_count, hash_count, bytes,
            )?))
        }
        tag => Err(Error::InvalidFormat {
            message: format!("unknown table prefix filter tag {tag}"),
        }),
    }
}

fn decode_restart_points(cursor: &mut Cursor<'_>, record_offsets: &[usize]) -> Result<Vec<usize>> {
    // The on-disk restart list stores byte offsets. Convert them to record
    // indexes once during table open, and reject offsets that do not land
    // exactly on a decoded record boundary.
    let records_end = cursor.offset;
    let restart_count = cursor.read_u32()? as usize;
    if record_offsets.is_empty() {
        if restart_count == 0 {
            return Ok(Vec::new());
        }
        return Err(invalid_table("empty data block has restart points"));
    }
    if restart_count == 0 {
        return Err(invalid_table("data block is missing restart points"));
    }
    ensure_count_fits_remaining(
        restart_count,
        cursor.remaining_len(),
        RESTART_POINT_BYTES,
        "data block restart count exceeds block bytes",
    )?;

    let mut restart_indices = Vec::with_capacity(restart_count);
    let mut previous_restart = None;
    for _ in 0..restart_count {
        let restart = cursor.read_u32()? as usize;
        if restart >= records_end {
            return Err(invalid_table("data block restart outside record area"));
        }
        if previous_restart.is_some_and(|previous| restart <= previous) {
            return Err(invalid_table("data block restart points are not sorted"));
        }
        let record_index = record_offsets
            .binary_search(&restart)
            .map_err(|_| invalid_table("data block restart is not a record start"))?;
        restart_indices.push(record_index);
        previous_restart = Some(restart);
    }
    if restart_indices.first().copied() != Some(0) {
        return Err(invalid_table(
            "data block first restart is not first record",
        ));
    }

    Ok(restart_indices)
}

fn validate_data_index_covers_section(
    index_entries: &[DataBlockIndexEntry],
    data_blocks: SectionHandle,
) -> Result<()> {
    let (section_start, section_end) = section_bounds(data_blocks)?;
    if index_entries.is_empty() {
        if section_start == section_end {
            return Ok(());
        }
        return Err(Error::Corruption {
            message: "data block section is not indexed".to_owned(),
        });
    }

    let mut expected_start = section_start;
    let mut previous_largest = None;
    for entry in index_entries {
        let (block_start, block_end) = block_bounds(entry.block)?;
        if block_start != expected_start || block_end > section_end {
            return Err(Error::Corruption {
                message: "data block index does not cover section bytes".to_owned(),
            });
        }
        if entry.smallest_internal_key > entry.largest_internal_key {
            return Err(Error::Corruption {
                message: "data block index key bounds are inverted".to_owned(),
            });
        }
        if previous_largest
            .as_ref()
            .is_some_and(|previous| previous >= &entry.smallest_internal_key)
        {
            return Err(Error::Corruption {
                message: "data block index entries are not sorted".to_owned(),
            });
        }
        expected_start = block_end;
        previous_largest = Some(entry.largest_internal_key.clone());
    }

    if expected_start != section_end {
        return Err(Error::Corruption {
            message: "data block index leaves section bytes unread".to_owned(),
        });
    }

    Ok(())
}

fn validate_data_block_entry(
    entry: &DataBlockIndexEntry,
    records: &[TablePointRecord],
) -> Result<()> {
    let Some(first) = records.first() else {
        return Err(Error::Corruption {
            message: "data block index points to an empty block".to_owned(),
        });
    };
    let last = records
        .last()
        .expect("non-empty data block has last record");
    if first.internal_key != entry.smallest_internal_key
        || last.internal_key != entry.largest_internal_key
    {
        return Err(Error::Corruption {
            message: "data block index key bounds do not match block records".to_owned(),
        });
    }

    validate_sorted_point_records(records)
}

fn validate_data_block_filters(
    entry: &DataBlockIndexEntry,
    records: &[TablePointRecord],
) -> Result<()> {
    // Index-level filters can only remove data-block candidates if every key in
    // the block remains represented. Rejecting false negatives keeps filters
    // advisory instead of letting them decide MVCC visibility.
    for record in records {
        let user_key = record.internal_key.user_key();
        if entry
            .point_key_filter
            .as_ref()
            .is_some_and(|filter| !filter.may_contain_key(user_key))
        {
            return Err(Error::Corruption {
                message: "data block point-key filter misses a block key".to_owned(),
            });
        }

        if let Some(filter) = &entry.prefix_filter {
            if filter
                .extractor()
                .extract(user_key)
                .is_some_and(|prefix| !filter.may_contain_prefix(prefix))
            {
                return Err(Error::Corruption {
                    message: "data block prefix filter misses a block prefix".to_owned(),
                });
            }
        }
    }

    Ok(())
}

fn validate_sorted_point_records(point_records: &[TablePointRecord]) -> Result<()> {
    for pair in point_records.windows(2) {
        if pair[0].internal_key >= pair[1].internal_key {
            return Err(Error::Corruption {
                message: "table point records are not sorted by internal key".to_owned(),
            });
        }
    }

    Ok(())
}

fn put_properties(bytes: &mut Vec<u8>, properties: &TableProperties) -> Result<()> {
    put_u64(bytes, properties.id.get());
    put_u32(bytes, properties.level.get());
    put_bytes(bytes, &properties.smallest_user_key)?;
    put_bytes(bytes, &properties.largest_user_key)?;
    put_u64(bytes, properties.smallest_sequence.get());
    put_u64(bytes, properties.largest_sequence.get());
    put_codec(bytes, properties.codec);
    put_u32(
        bytes,
        usize_to_u32(
            properties.blob_file_ids.len(),
            "properties blob file id count",
        )?,
    );
    for file_id in &properties.blob_file_ids {
        put_u64(bytes, *file_id);
    }
    Ok(())
}

fn put_internal_key(bytes: &mut Vec<u8>, internal_key: &InternalKey) -> Result<()> {
    put_bytes(bytes, internal_key.user_key())?;
    put_u64(bytes, internal_key.sequence().get());
    put_value_kind(bytes, internal_key.kind());
    put_u32(bytes, internal_key.batch_index());
    Ok(())
}

fn put_value_kind(bytes: &mut Vec<u8>, value_kind: ValueKind) {
    put_u8(
        bytes,
        match value_kind {
            ValueKind::Put => VALUE_KIND_PUT,
            ValueKind::PointDelete => VALUE_KIND_POINT_DELETE,
            ValueKind::RangeDelete => VALUE_KIND_RANGE_DELETE,
        },
    );
}

fn put_value_ref(bytes: &mut Vec<u8>, value: Option<&ValueRef>) -> Result<()> {
    match value {
        None => put_u8(bytes, VALUE_NONE),
        Some(ValueRef::Inline(inline)) => {
            put_u8(bytes, VALUE_INLINE);
            put_bytes(bytes, inline)?;
        }
        Some(ValueRef::Blob {
            file_id,
            offset,
            len,
            checksum,
        }) => {
            put_u8(bytes, VALUE_BLOB);
            put_u64(bytes, *file_id);
            put_u64(bytes, *offset);
            put_u64(bytes, *len);
            put_u32(bytes, *checksum);
        }
    }
    Ok(())
}

fn put_codec(bytes: &mut Vec<u8>, codec: CodecId) {
    put_u8(
        bytes,
        match codec {
            CodecId::None => 0,
            CodecId::FastLz4Block => 1,
        },
    );
}

fn codec_from_tag(tag: u8) -> Result<CodecId> {
    match tag {
        0 => Ok(CodecId::None),
        1 => Ok(CodecId::FastLz4Block),
        tag => Err(Error::UnsupportedFormat {
            message: format!("unknown table codec {tag}"),
        }),
    }
}

fn put_bound(bytes: &mut Vec<u8>, bound: &Bound<Vec<u8>>) -> Result<()> {
    match bound {
        Bound::Unbounded => put_u8(bytes, BOUND_UNBOUNDED),
        Bound::Included(value) => {
            put_u8(bytes, BOUND_INCLUDED);
            put_bytes(bytes, value)?;
        }
        Bound::Excluded(value) => {
            put_u8(bytes, BOUND_EXCLUDED);
            put_bytes(bytes, value)?;
        }
    }
    Ok(())
}

fn put_prefix_extractor(bytes: &mut Vec<u8>, extractor: &PrefixExtractor) -> Result<()> {
    match extractor {
        PrefixExtractor::Disabled => put_u8(bytes, PREFIX_EXTRACTOR_DISABLED),
        PrefixExtractor::FixedLen(len) => {
            put_u8(bytes, PREFIX_EXTRACTOR_FIXED_LEN);
            put_u64(
                bytes,
                u64::try_from(*len).map_err(|_| {
                    Error::invalid_options("prefix extractor length exceeds u64::MAX")
                })?,
            );
        }
        PrefixExtractor::Separator(separator) => {
            put_u8(bytes, PREFIX_EXTRACTOR_SEPARATOR);
            put_u8(bytes, *separator);
        }
        PrefixExtractor::Custom(name) => {
            put_u8(bytes, PREFIX_EXTRACTOR_CUSTOM);
            put_bytes(bytes, name.as_bytes())?;
        }
    }
    Ok(())
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("table byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

fn put_section_handle(bytes: &mut Vec<u8>, handle: SectionHandle) {
    put_u64(bytes, handle.offset);
    put_u64(bytes, handle.len);
}

fn point_record_encoded_len(record: &TablePointRecord) -> usize {
    internal_key_encoded_len(&record.internal_key) + value_ref_encoded_len(record.value.as_ref())
}

fn internal_key_encoded_len(internal_key: &InternalKey) -> usize {
    4 + internal_key.user_key().len() + 8 + 1 + 4
}

fn value_ref_encoded_len(value: Option<&ValueRef>) -> usize {
    match value {
        None => 1,
        Some(ValueRef::Inline(bytes)) => 1 + 4 + bytes.len(),
        Some(ValueRef::Blob { .. }) => 1 + 8 + 8 + 8 + 4,
    }
}

fn key_is_before_start(key: &[u8], start: &Bound<Vec<u8>>) -> bool {
    match start {
        Bound::Included(start) => key < start.as_slice(),
        Bound::Excluded(start) => key <= start.as_slice(),
        Bound::Unbounded => false,
    }
}

fn key_is_after_end(key: &[u8], end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Included(end) => key > end.as_slice(),
        Bound::Excluded(end) => key >= end.as_slice(),
        Bound::Unbounded => false,
    }
}

fn section_bounds(handle: SectionHandle) -> Result<(usize, usize)> {
    bounds(handle.offset, handle.len)
}

fn block_bounds(handle: BlockHandle) -> Result<(usize, usize)> {
    bounds(handle.offset, handle.len)
}

fn bounds(offset: u64, len: u64) -> Result<(usize, usize)> {
    let start = usize::try_from(offset).map_err(|_| invalid_table("offset exceeds usize"))?;
    let len = usize::try_from(len).map_err(|_| invalid_table("length exceeds usize"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| invalid_table("offset plus length overflows usize"))?;
    Ok((start, end))
}

fn usize_to_u32(value: usize, field: &'static str) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u32::MAX")))
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| invalid_table("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_table("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn invalid_table(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid table: {message}"),
    }
}

fn ensure_count_fits_remaining(
    count: usize,
    remaining: usize,
    min_item_bytes: usize,
    message: &'static str,
) -> Result<()> {
    debug_assert!(min_item_bytes > 0);
    if count > remaining / min_item_bytes {
        return Err(invalid_table(message));
    }
    Ok(())
}

struct Cursor<'payload> {
    payload: &'payload [u8],
    offset: usize,
}

impl<'payload> Cursor<'payload> {
    const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_table("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let value = read_u16_at(self.payload, self.offset)?;
        self.offset += 2;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32_at(self.payload, self.offset)?;
        self.offset += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let value = self
            .payload
            .get(self.offset..self.offset + 8)
            .ok_or_else(|| invalid_table("short u64"))?;
        self.offset += 8;
        Ok(u64::from_le_bytes([
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ]))
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let value = self
            .payload
            .get(self.offset..self.offset + len)
            .ok_or_else(|| invalid_table("short bytes"))?;
        self.offset += len;
        Ok(value)
    }

    fn read_properties(&mut self) -> Result<TableProperties> {
        Ok(TableProperties {
            id: TableId(self.read_u64()?),
            level: TableLevel(self.read_u32()?),
            smallest_user_key: self.read_bytes()?.to_vec(),
            largest_user_key: self.read_bytes()?.to_vec(),
            smallest_sequence: Sequence::new(self.read_u64()?),
            largest_sequence: Sequence::new(self.read_u64()?),
            codec: self.read_codec()?,
            blob_file_ids: self.read_blob_file_ids()?,
        })
    }

    fn read_internal_key(&mut self) -> Result<InternalKey> {
        let user_key = self.read_bytes()?.to_vec();
        let sequence = Sequence::new(self.read_u64()?);
        let kind = self.read_value_kind()?;
        let batch_index = self.read_u32()?;
        Ok(InternalKey::new(user_key, sequence, kind, batch_index))
    }

    fn read_value_kind(&mut self) -> Result<ValueKind> {
        match self.read_u8()? {
            VALUE_KIND_PUT => Ok(ValueKind::Put),
            VALUE_KIND_POINT_DELETE => Ok(ValueKind::PointDelete),
            VALUE_KIND_RANGE_DELETE => Ok(ValueKind::RangeDelete),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown table value kind {tag}"),
            }),
        }
    }

    fn read_value_ref(&mut self) -> Result<Option<ValueRef>> {
        match self.read_u8()? {
            VALUE_NONE => Ok(None),
            VALUE_INLINE => Ok(Some(ValueRef::Inline(self.read_bytes()?.to_vec()))),
            VALUE_BLOB => Ok(Some(ValueRef::Blob {
                file_id: self.read_u64()?,
                offset: self.read_u64()?,
                len: self.read_u64()?,
                checksum: self.read_u32()?,
            })),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown table value reference {tag}"),
            }),
        }
    }

    fn read_codec(&mut self) -> Result<CodecId> {
        codec_from_tag(self.read_u8()?)
    }

    fn read_blob_file_ids(&mut self) -> Result<Vec<u64>> {
        let file_id_count = self.read_u32()? as usize;
        ensure_count_fits_remaining(
            file_id_count,
            self.remaining_len(),
            8,
            "properties blob file id count exceeds block bytes",
        )?;
        let mut file_ids = Vec::with_capacity(file_id_count);
        let mut previous = None;
        for _ in 0..file_id_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_table("properties blob file ids are not sorted"));
            }
            file_ids.push(file_id);
            previous = Some(file_id);
        }
        Ok(file_ids)
    }

    fn read_section_handle(&mut self) -> Result<SectionHandle> {
        Ok(SectionHandle {
            offset: self.read_u64()?,
            len: self.read_u64()?,
        })
    }

    fn read_prefix_extractor(&mut self) -> Result<PrefixExtractor> {
        match self.read_u8()? {
            PREFIX_EXTRACTOR_DISABLED => Ok(PrefixExtractor::Disabled),
            PREFIX_EXTRACTOR_FIXED_LEN => {
                let len = usize::try_from(self.read_u64()?).map_err(|_| Error::InvalidFormat {
                    message: "prefix extractor length exceeds usize".to_owned(),
                })?;
                Ok(PrefixExtractor::FixedLen(len))
            }
            PREFIX_EXTRACTOR_SEPARATOR => Ok(PrefixExtractor::Separator(self.read_u8()?)),
            PREFIX_EXTRACTOR_CUSTOM => {
                let name = String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| {
                    Error::InvalidFormat {
                        message: "prefix extractor custom name is not utf-8".to_owned(),
                    }
                })?;
                Ok(PrefixExtractor::Custom(name))
            }
            tag => Err(Error::InvalidFormat {
                message: format!("unknown table prefix extractor tag {tag}"),
            }),
        }
    }

    fn read_bound(&mut self) -> Result<Bound<Vec<u8>>> {
        match self.read_u8()? {
            BOUND_UNBOUNDED => Ok(Bound::Unbounded),
            BOUND_INCLUDED => Ok(Bound::Included(self.read_bytes()?.to_vec())),
            BOUND_EXCLUDED => Ok(Bound::Excluded(self.read_bytes()?.to_vec())),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown table range bound tag {tag}"),
            }),
        }
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }

    const fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::PointKeyFilter;
    use crate::options::KeyspaceOptions;

    #[test]
    fn checked_block_index_round_trips_multiple_data_blocks() {
        let table = table_with_records(160, CodecId::None);
        let payload = encode_table(&table).expect("table encodes");
        let footer = read_footer(&payload).expect("footer reads");
        let (_, index_payload) =
            read_single_block_section(&payload, footer.indexes).expect("index block reads");
        let index_entries = decode_index_block(&index_payload).expect("index decodes");
        assert!(
            index_entries.len() > 1,
            "test table should span multiple data blocks"
        );

        let decoded = decode_table(&table_file_bytes(&payload)).expect("table decodes");
        assert_eq!(decoded.properties(), table.properties());
        assert_eq!(
            decoded.point_records().expect("decoded records load"),
            table.point_records().expect("source records load")
        );
    }

    #[test]
    fn fast_lz4_block_index_round_trips() {
        let table = table_with_records(160, CodecId::FastLz4Block);
        let payload = encode_table(&table).expect("table encodes");
        let decoded = decode_table(&table_file_bytes(&payload)).expect("table decodes");
        assert_eq!(decoded.properties(), table.properties());
        assert_eq!(
            decoded.point_records().expect("decoded records load"),
            table.point_records().expect("source records load")
        );
    }

    #[test]
    fn block_candidates_use_index_bounds_and_restart_keys() {
        let table = table_with_records(160, CodecId::None);
        assert!(
            table.data_blocks.len() > 1,
            "test table should span multiple data blocks"
        );

        let point_keys = table
            .point_records_for_key(b"key-127", IndexSearchPolicy::Binary)
            .expect("point records load")
            .into_iter()
            .map(|record| record.internal_key.user_key().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(point_keys, vec![b"key-127".to_vec()]);
        assert!(
            table
                .point_records_for_key(b"missing", IndexSearchPolicy::Binary)
                .expect("missing point probe loads")
                .is_empty()
        );

        let range_keys = table
            .point_records_in_range(
                &KeyRange::half_open(b"key-020", b"key-030"),
                IndexSearchPolicy::Binary,
            )
            .expect("range records load")
            .into_iter()
            .map(|record| record.internal_key.user_key().to_vec())
            .collect::<Vec<_>>();
        let expected_range = (20..30)
            .map(|index| format!("key-{index:03}").into_bytes())
            .collect::<Vec<_>>();
        assert_eq!(range_keys, expected_range);

        let prefix_keys = table
            .point_records_with_prefix(
                b"key-12",
                &PrefixExtractor::Disabled,
                IndexSearchPolicy::Binary,
            )
            .expect("prefix records load")
            .into_iter()
            .map(|record| record.internal_key.user_key().to_vec())
            .collect::<Vec<_>>();
        let expected_prefix = (120..130)
            .map(|index| format!("key-{index:03}").into_bytes())
            .collect::<Vec<_>>();
        assert_eq!(prefix_keys, expected_prefix);
    }

    #[test]
    fn data_block_point_lookup_uses_hash_index() {
        let block = DecodedDataBlock::new(
            vec![
                test_point_record(b"a", 10, b"a1"),
                test_point_record(b"target", 9, b"newer"),
                test_point_record(b"target", 7, b"older"),
                test_point_record(b"z", 1, b"z1"),
            ],
            vec![0],
        );
        assert_eq!(
            block.point_lookup_index.get(b"target".as_slice()),
            Some(&(1..3))
        );

        let records =
            data_block_point_records_for_key(&block, b"target", IndexSearchPolicy::Binary);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].internal_key.sequence(), Sequence::new(9));
        assert_eq!(records[1].internal_key.sequence(), Sequence::new(7));

        let visible = data_block_newest_visible_point_record_for_key(
            &block,
            b"target",
            Sequence::new(8),
            IndexSearchPolicy::Binary,
        )
        .expect("older target version is visible");
        assert_eq!(visible.internal_key.sequence(), Sequence::new(7));

        assert!(
            data_block_point_records_for_key(&block, b"missing", IndexSearchPolicy::Binary)
                .is_empty()
        );
    }

    #[test]
    fn block_read_uses_cached_file_handle() {
        let mut payload = Vec::new();
        let block = append_checked_block(&mut payload, CodecId::None, b"cached block")
            .expect("checked block appends");
        let path = std::env::temp_dir().join(format!(
            "trine-kv-cached-table-handle-{}-{}.trinet",
            std::process::id(),
            table_time_suffix()
        ));
        let mut file_bytes = vec![0_u8; HEADER_LEN];
        file_bytes.extend_from_slice(&payload);
        std::fs::write(&path, file_bytes).expect("test table file writes");
        let file = std::fs::File::open(&path).expect("test table file opens");
        let missing_path = path.with_extension("missing");

        let (codec, decoded) =
            read_checked_block_from_file(&missing_path, Some(&file), payload.len(), block)
                .expect("cached file handle supplies block bytes");

        assert_eq!(codec, CodecId::None);
        assert_eq!(decoded, b"cached block");
        std::fs::remove_file(path).expect("test table file removes");
    }

    #[test]
    fn search_policies_keep_table_candidate_results_stable() {
        let table = table_with_filters(160, CodecId::None);
        let expected_range = (20..30)
            .map(|index| format!("key-{index:03}").into_bytes())
            .collect::<Vec<_>>();
        let expected_prefix = (120..130)
            .map(|index| format!("key-{index:03}").into_bytes())
            .collect::<Vec<_>>();

        for policy in [
            IndexSearchPolicy::Linear,
            IndexSearchPolicy::Binary,
            IndexSearchPolicy::Auto,
            IndexSearchPolicy::Eytzinger,
            IndexSearchPolicy::GallopingWithHint,
        ] {
            let point_keys = table
                .point_records_for_key(b"key-127", policy)
                .expect("point records load")
                .into_iter()
                .map(|record| record.internal_key.user_key().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(point_keys, vec![b"key-127".to_vec()]);

            let range_keys = table
                .point_records_in_range(&KeyRange::half_open(b"key-020", b"key-030"), policy)
                .expect("range records load")
                .into_iter()
                .map(|record| record.internal_key.user_key().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(range_keys, expected_range, "policy {policy:?}");

            let prefix_keys = table
                .point_records_with_prefix(b"key-12", &PrefixExtractor::FixedLen(6), policy)
                .expect("prefix records load")
                .into_iter()
                .map(|record| record.internal_key.user_key().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(prefix_keys, expected_prefix, "policy {policy:?}");
        }
    }

    #[test]
    fn configured_block_bytes_controls_data_block_count() {
        let mut small_blocks = test_table_options(CodecId::None, false);
        small_blocks.block_bytes = 256;
        let mut large_blocks = test_table_options(CodecId::None, false);
        large_blocks.block_bytes = 4096;

        let small_table = table_with_options(160, &small_blocks);
        let large_table = table_with_options(160, &large_blocks);

        assert!(
            small_table.data_blocks.len() > large_table.data_blocks.len(),
            "smaller configured blocks should split records into more data blocks"
        );
    }

    #[test]
    fn partitioned_filters_round_trip_through_index_entries() {
        let table = table_with_filters(160, CodecId::None);
        let payload = encode_table(&table).expect("table encodes");
        let footer = read_footer(&payload).expect("footer reads");
        let (_, index_payload) =
            read_single_block_section(&payload, footer.indexes).expect("index block reads");
        let index_entries = decode_index_block(&index_payload).expect("index decodes");
        assert!(
            index_entries.len() > 1,
            "test table should span multiple data blocks"
        );
        assert!(
            index_entries
                .iter()
                .all(|entry| entry.point_key_filter.is_some())
        );
        assert!(
            index_entries
                .iter()
                .all(|entry| entry.prefix_filter.is_some())
        );

        let first_entry = index_entries.first().expect("index has first entry");
        let point_filter = first_entry
            .point_key_filter
            .as_ref()
            .expect("point filter exists");
        assert!(point_filter.may_contain_key(first_entry.smallest_internal_key.user_key()));
        let missing = point_filter_miss(point_filter);
        assert!(!point_filter.may_contain_key(&missing));

        let decoded = decode_table(&table_file_bytes(&payload)).expect("table decodes");
        let prefix_keys = decoded
            .point_records_with_prefix(
                b"key-12",
                &PrefixExtractor::FixedLen(6),
                IndexSearchPolicy::Binary,
            )
            .expect("prefix records load")
            .into_iter()
            .map(|record| record.internal_key.user_key().to_vec())
            .collect::<Vec<_>>();
        let expected_prefix = (120..130)
            .map(|index| format!("key-{index:03}").into_bytes())
            .collect::<Vec<_>>();
        assert_eq!(prefix_keys, expected_prefix);
    }

    #[test]
    fn data_block_filter_false_negative_fails_closed() {
        let table = table_with_filters(32, CodecId::None);
        let block = table.data_blocks.first().expect("test table has a block");
        let point_records = table.point_records.as_ref().expect("test records loaded");
        let records = &point_records[block.record_range.clone()];
        let entry = DataBlockIndexEntry {
            smallest_internal_key: block.smallest_internal_key.clone(),
            largest_internal_key: block.largest_internal_key.clone(),
            block: BlockHandle { offset: 0, len: 0 },
            point_key_filter: Some(
                PointKeyFilter::from_parts(1, 1, vec![0]).expect("test filter decodes"),
            ),
            prefix_filter: None,
        };

        let error = validate_data_block_filters(&entry, records)
            .expect_err("missing block key should fail");
        assert!(matches!(error, Error::Corruption { .. }));
    }

    #[test]
    fn prefix_block_filter_false_negative_fails_closed() {
        let table = table_with_filters(32, CodecId::None);
        let block = table.data_blocks.first().expect("test table has a block");
        let point_records = table.point_records.as_ref().expect("test records loaded");
        let records = &point_records[block.record_range.clone()];
        let entry = DataBlockIndexEntry {
            smallest_internal_key: block.smallest_internal_key.clone(),
            largest_internal_key: block.largest_internal_key.clone(),
            block: BlockHandle { offset: 0, len: 0 },
            point_key_filter: None,
            prefix_filter: Some(
                PrefixFilter::from_parts(PrefixExtractor::FixedLen(6), 1, 1, vec![0])
                    .expect("test filter decodes"),
            ),
        };

        let error = validate_data_block_filters(&entry, records)
            .expect_err("missing block prefix should fail");
        assert!(matches!(error, Error::Corruption { .. }));
    }

    #[test]
    fn point_key_filter_round_trips_and_rejects_missing_keys() {
        let mut table = table_with_records(8, CodecId::None);
        let point_records = table.point_records().expect("test records load");
        table.point_key_filter = Some(PointKeyFilter::from_keys(
            point_records
                .iter()
                .map(|record| record.internal_key.user_key()),
            10,
        ));
        let payload = encode_table(&table).expect("table encodes");
        let decoded = decode_table(&table_file_bytes(&payload)).expect("table decodes");

        assert!(decoded.may_contain_key(b"key-003"));
        let missing = point_filter_miss(decoded.point_key_filter.as_ref().expect("filter exists"));
        assert!(!decoded.may_contain_key(&missing));
    }

    #[test]
    fn unknown_data_block_codec_fails_closed() {
        let table = table_with_records(4, CodecId::None);
        let mut payload = encode_table(&table).expect("table encodes");
        payload[0] = u8::MAX;

        let error =
            decode_table(&table_file_bytes(&payload)).expect_err("unknown block codec fails");
        assert!(matches!(error, Error::UnsupportedFormat { .. }));
    }

    #[test]
    fn table_decode_rejects_index_entry_count_before_large_allocation() {
        let error = decode_index_block(&count_block(u32::MAX))
            .expect_err("impossible index count should fail");
        assert_invalid_table_message(&error, "index entry count exceeds block bytes");
    }

    #[test]
    fn table_decode_rejects_data_record_count_before_large_allocation() {
        let error = decode_data_block(&count_block(u32::MAX))
            .expect_err("impossible data record count should fail");
        assert_invalid_table_message(&error, "data record count exceeds block bytes");
    }

    #[test]
    fn table_decode_rejects_restart_count_before_large_allocation() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, 1);
        put_internal_key(
            &mut bytes,
            &InternalKey::new(Vec::new(), Sequence::new(1), ValueKind::Put, 0),
        )
        .expect("internal key encodes");
        put_value_ref(&mut bytes, None).expect("value reference encodes");
        put_u32(&mut bytes, u32::MAX);

        let error = decode_data_block(&bytes).expect_err("impossible restart count should fail");
        assert_invalid_table_message(&error, "data block restart count exceeds block bytes");
    }

    #[test]
    fn table_decode_rejects_range_tombstone_count_before_large_allocation() {
        let error = decode_range_tombstone_block(&count_block(u32::MAX))
            .expect_err("impossible tombstone count should fail");
        assert_invalid_table_message(&error, "range tombstone count exceeds block bytes");
    }

    #[test]
    fn table_decode_rejects_malformed_bloom_filters() {
        let mut point_bytes = Vec::new();
        put_u8(&mut point_bytes, POINT_KEY_FILTER_PRESENT);
        put_u64(&mut point_bytes, 16);
        put_u8(&mut point_bytes, 1);
        put_bytes(&mut point_bytes, &[0]).expect("bitset encodes");
        let error =
            decode_filter_block(&point_bytes).expect_err("short point-key bitset should fail");
        assert!(error.to_string().contains("byte length"));

        let mut prefix_bytes = Vec::new();
        put_u8(&mut prefix_bytes, POINT_KEY_FILTER_ABSENT);
        put_u8(&mut prefix_bytes, PREFIX_FILTER_PRESENT);
        put_u8(&mut prefix_bytes, PREFIX_EXTRACTOR_DISABLED);
        put_u64(&mut prefix_bytes, 16);
        put_u8(&mut prefix_bytes, 0);
        put_bytes(&mut prefix_bytes, &[0, 0]).expect("bitset encodes");
        let error =
            decode_filter_block(&prefix_bytes).expect_err("invalid prefix hash count should fail");
        assert!(error.to_string().contains("hash count"));
    }

    fn table_with_records(count: usize, codec: CodecId) -> Table {
        let options = test_table_options(codec, false);
        table_with_options(count, &options)
    }

    fn table_with_filters(count: usize, codec: CodecId) -> Table {
        let options = test_table_options(codec, true);
        table_with_options(count, &options)
    }

    fn table_with_options(count: usize, options: &TableWriteOptions) -> Table {
        let point_records = (0..count)
            .map(|index| TablePointRecord {
                internal_key: InternalKey::new(
                    format!("key-{index:03}").into_bytes(),
                    Sequence::new(u64::try_from(index + 1).expect("test sequence fits u64")),
                    ValueKind::Put,
                    0,
                ),
                value: Some(ValueRef::Inline(format!("value-{index:03}").into_bytes())),
            })
            .collect::<Vec<_>>();
        let data_blocks = build_data_blocks(&point_records, options).expect("test blocks build");
        Table {
            path: None,
            file: None,
            payload_len: 0,
            footer: empty_footer(),
            properties: table_properties(
                TableId(7),
                TableLevel::ZERO,
                options.codec,
                &point_records,
                &[],
            ),
            point_key_filter: build_point_key_filter(options, &point_records),
            prefix_filter: build_prefix_filter(options, &point_records),
            filter_stats: Arc::new(TableFilterStats::default()),
            point_records: Some(point_records),
            data_blocks,
            range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
                Vec::new(),
            ))))),
        }
    }

    fn test_point_record(key: &[u8], sequence: u64, value: &[u8]) -> TablePointRecord {
        TablePointRecord {
            internal_key: InternalKey::new(
                key.to_vec(),
                Sequence::new(sequence),
                ValueKind::Put,
                0,
            ),
            value: Some(ValueRef::Inline(value.to_vec())),
        }
    }

    fn table_time_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is after epoch")
            .as_nanos()
    }

    const fn test_table_options(codec: CodecId, filters_enabled: bool) -> TableWriteOptions {
        TableWriteOptions {
            codec,
            block_bytes: 1024,
            filter_policy: if filters_enabled {
                FilterPolicy::Bloom { bits_per_key: 10 }
            } else {
                FilterPolicy::Disabled
            },
            prefix_extractor: if filters_enabled {
                PrefixExtractor::FixedLen(6)
            } else {
                PrefixExtractor::Disabled
            },
            prefix_filter_policy: if filters_enabled {
                PrefixFilterPolicy::Bloom {
                    bits_per_prefix: 10,
                }
            } else {
                PrefixFilterPolicy::Disabled
            },
            blob_threshold_bytes: KeyspaceOptions::DEFAULT_BLOB_THRESHOLD_BYTES,
        }
    }

    fn table_file_bytes(payload: &[u8]) -> Vec<u8> {
        let payload_len = u32::try_from(payload.len()).expect("test payload fits u32");
        let payload_checksum = checksum(payload);
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
        bytes.extend_from_slice(&TABLE_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&TABLE_VERSION.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&payload_checksum.to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn count_block(count: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, count);
        bytes
    }

    fn assert_invalid_table_message(error: &Error, expected: &str) {
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    fn point_filter_miss(filter: &PointKeyFilter) -> Vec<u8> {
        for index in 0..10_000 {
            let key = format!("missing-{index:05}").into_bytes();
            if !filter.may_contain_key(&key) {
                return key;
            }
        }
        panic!("test filter should have at least one definite miss");
    }
}
