use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    ops::{Bound, Range},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::{
    blob::{BlobIndex, ValueRef},
    block::{BlockHandle, BlockManager, BlockReadSource, block_bounds, checksum},
    cache::{BlockCache, BlockCacheKey, CacheKind},
    codec::CodecId,
    error::{Error, Result},
    filter::{PointKeyFilter, PrefixFilter},
    internal_key::{InternalKey, ValueKind},
    iterator::{
        Direction, ForwardKeyState, RecordGroup, ReverseKeyState, ScanRecord, ScanSelector,
        prefix_successor, sort_group_records,
    },
    options::{DurabilityMode, FilterPolicy, IndexSearchPolicy, PrefixFilterPolicy},
    point_value::PointValueSource,
    prefix::PrefixExtractor,
    range_tombstone::{self, RangeTombstoneIndex, RangeTombstoneLike},
    search,
    stats::{FilterStats, ReadPathStats},
    storage::{
        BlockingStorageObjectListBackend, BlockingStorageObjectWriteBackend,
        BlockingStorageReadBackend, BlockingStorageReadObject, NativeFileBackend, NativeFileObject,
        NativeFileReadSource, StorageCapability, StorageObjectId, StorageObjectKind,
        StorageObjectListRequest, StorageReadBackend, StorageReadSource,
    },
    types::{KeyRange, Sequence},
};

pub const TABLE_FILE_EXTENSION: &str = "trinet";
const TABLE_MAGIC: u32 = 0x5452_5442;
const TABLE_VERSION: u16 = 5;
const HEADER_LEN: usize = 14;
const FOOTER_MAGIC: u32 = 0x5452_5446;
const FOOTER_LEN: usize = 90;
const DATA_BLOCK_RESTART_INTERVAL: usize = 16;
const INDEX_PARTITION_TARGET_ENTRIES: usize = 128;
const PINNED_READ_METADATA_MAX_LEVEL: u32 = 1;

const VALUE_KIND_PUT: u8 = 1;
const VALUE_KIND_POINT_DELETE: u8 = 2;
const VALUE_KIND_RANGE_DELETE: u8 = 3;

const VALUE_NONE: u8 = 0;
const VALUE_INLINE: u8 = 1;
const VALUE_BLOB: u8 = 2;
const VALUE_BLOB_INDEX: u8 = 3;

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
const TABLE_STAT_SHARDS: usize = 32;

static NEXT_TABLE_STAT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TABLE_STAT_SHARD: usize =
        NEXT_TABLE_STAT_SHARD.fetch_add(1, Ordering::Relaxed) % TABLE_STAT_SHARDS;
}

// These are on-disk lower bounds. Decoders use them to reject impossible
// record counts before reserving memory; real entries may be larger because
// keys, values, and filters carry byte fields.
const MIN_INTERNAL_KEY_BYTES: usize = 17;
const MIN_VALUE_REF_BYTES: usize = 1;
const MIN_DATA_RECORD_BYTES: usize = MIN_INTERNAL_KEY_BYTES + MIN_VALUE_REF_BYTES;
const MIN_DATA_BLOCK_HASH_ENTRY_BYTES: usize = 16;
const MIN_INDEX_ENTRY_BYTES: usize = MIN_INTERNAL_KEY_BYTES * 2 + 16 + 1 + 1;
const MIN_INDEX_PARTITION_ENTRY_BYTES: usize = MIN_INTERNAL_KEY_BYTES * 2 + 16 + 8;
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
pub struct TableBlobReference {
    pub file_id: u64,
    pub referenced_bytes: u64,
    pub referenced_record_count: u64,
    pub smallest_internal_key: InternalKey,
    pub largest_internal_key: InternalKey,
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
    pub(crate) blob_references: Vec<TableBlobReference>,
}

impl TableProperties {
    #[must_use]
    pub fn blob_file_ids(&self) -> &[u64] {
        &self.blob_file_ids
    }

    #[must_use]
    pub fn blob_references(&self) -> &[TableBlobReference] {
        &self.blob_references
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableWriteOptions {
    pub(crate) codec: CodecId,
    pub(crate) block_bytes: usize,
    pub(crate) filter_policy: FilterPolicy,
    pub(crate) prefix_extractor: PrefixExtractor,
    pub(crate) prefix_filter_policy: PrefixFilterPolicy,
    pub(crate) blob_threshold_bytes: usize,
    pub(crate) rewrite_blob_indexes: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TablePointRecord {
    pub(crate) internal_key: InternalKey,
    pub(crate) value: Option<ValueRef>,
}

#[derive(Debug)]
pub(crate) struct TablePointValueRecord {
    pub(crate) internal_key: InternalKey,
    pub(crate) value: Option<PointValueSource>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexPartitionEntry {
    smallest_internal_key: InternalKey,
    largest_internal_key: InternalKey,
    block: BlockHandle,
    first_data_block_index: usize,
    data_block_count: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedDataBlock {
    bytes: Arc<[u8]>,
    record_headers: Box<[DataBlockRecordHeader]>,
    restart_indices: Box<[u32]>,
    point_lookup_index: DataBlockPointLookupIndex,
}

impl DecodedDataBlock {
    fn from_records(records: &[TablePointRecord]) -> Result<Self> {
        decode_data_block(encode_data_block(records)?)
    }

    pub(crate) fn estimated_bytes(&self) -> u64 {
        let bytes = usize_to_u64_saturating(self.bytes.len());
        let record_headers = usize_to_u64_saturating(
            self.record_headers
                .len()
                .saturating_mul(size_of::<DataBlockRecordHeader>()),
        );
        let restarts =
            usize_to_u64_saturating(self.restart_indices.len().saturating_mul(size_of::<u32>()));
        bytes
            .saturating_add(record_headers)
            .saturating_add(restarts)
            .saturating_add(self.point_lookup_index.estimated_bytes())
            .max(1)
    }

    fn record_count(&self) -> usize {
        self.record_headers.len()
    }

    fn record_view(&self, index: usize) -> Result<DataBlockRecordView<'_>> {
        self.record_headers
            .get(index)
            .ok_or_else(|| invalid_table("record index outside data block"))?
            .view(&self.bytes)
    }

    fn record_owned(&self, index: usize) -> Result<TablePointRecord> {
        self.record_view(index).map(DataBlockRecordView::to_owned)
    }

    fn point_value_record(&self, index: usize) -> Result<TablePointValueRecord> {
        let header = self
            .record_headers
            .get(index)
            .ok_or_else(|| invalid_table("record index outside data block"))?;
        let record = header.view(&self.bytes)?;
        let value = match header.value {
            Some(ValueRefHeader::Inline { offset, len }) => Some(PointValueSource::from_shared(
                Arc::clone(&self.bytes),
                inline_value_range(offset, len, self.bytes.len())?,
            )?),
            Some(value) => Some(PointValueSource::from_value_ref(
                value.view(&self.bytes)?.to_owned(),
            )),
            None => None,
        };

        Ok(TablePointValueRecord {
            internal_key: InternalKey::new(
                record.user_key.to_vec(),
                record.sequence,
                record.kind,
                record.batch_index,
            ),
            value,
        })
    }

    fn records_owned(&self) -> Result<Vec<TablePointRecord>> {
        (0..self.record_count())
            .map(|index| self.record_owned(index))
            .collect()
    }

    #[cfg(test)]
    fn restart_indices_with_base(&self, base: usize) -> Vec<usize> {
        self.restart_indices
            .iter()
            .map(|index| base.saturating_add(u32_to_usize(*index)))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn empty_for_cache_test() -> Self {
        Self {
            bytes: Arc::from([]),
            record_headers: Box::default(),
            restart_indices: Box::default(),
            point_lookup_index: DataBlockPointLookupIndex::empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DataBlockPointLookupIndex {
    entries: Box<[BlockHashEntry]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockHashEntry {
    key_hash: u64,
    start_record: u32,
    end_record: u32,
}

impl DataBlockPointLookupIndex {
    #[cfg(test)]
    fn empty() -> Self {
        Self {
            entries: Box::default(),
        }
    }

    fn from_records(records: &[TablePointRecord]) -> Result<Self> {
        let mut entries = Vec::new();
        let mut start = 0;
        while start < records.len() {
            let key = records[start].internal_key.user_key();
            let mut end = start + 1;
            while end < records.len() && records[end].internal_key.user_key() == key {
                end += 1;
            }
            entries.push(BlockHashEntry {
                key_hash: user_key_hash(key),
                start_record: usize_to_u32(start, "data block hash range start")?,
                end_record: usize_to_u32(end, "data block hash range end")?,
            });
            start = end;
        }
        Ok(Self::from_entries(entries))
    }

    fn from_entries(mut entries: Vec<BlockHashEntry>) -> Self {
        entries.sort_unstable_by_key(|entry| (entry.key_hash, entry.start_record));
        Self {
            entries: entries.into_boxed_slice(),
        }
    }

    fn matching_entries(&self, key_hash: u64) -> &[BlockHashEntry] {
        let start = self
            .entries
            .partition_point(|entry| entry.key_hash < key_hash);
        let end = start + self.entries[start..].partition_point(|entry| entry.key_hash == key_hash);
        &self.entries[start..end]
    }

    fn estimated_bytes(&self) -> u64 {
        usize_to_u64_saturating(
            self.entries
                .len()
                .saturating_mul(size_of::<BlockHashEntry>()),
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct DataBlockRecordHeader {
    record_offset: u32,
    record_end: u32,
    user_key_offset: u32,
    user_key_len: u32,
    sequence: Sequence,
    kind: ValueKind,
    batch_index: u32,
    value: Option<ValueRefHeader>,
}

impl DataBlockRecordHeader {
    fn view<'block>(&self, bytes: &'block [u8]) -> Result<DataBlockRecordView<'block>> {
        let record_start = u32_to_usize(self.record_offset);
        let record_end = u32_to_usize(self.record_end);
        if record_start >= record_end || record_end > bytes.len() {
            return Err(invalid_table("record header points outside data block"));
        }
        let user_key_end = self
            .user_key_offset
            .checked_add(self.user_key_len)
            .ok_or_else(|| invalid_table("record user key length overflows"))?;
        let user_key = bytes
            .get(u32_to_usize(self.user_key_offset)..u32_to_usize(user_key_end))
            .ok_or_else(|| invalid_table("record user key points outside data block"))?;
        let value = self.value.map(|value| value.view(bytes)).transpose()?;
        Ok(DataBlockRecordView {
            user_key,
            sequence: self.sequence,
            kind: self.kind,
            batch_index: self.batch_index,
            value,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum ValueRefHeader {
    Inline {
        offset: u32,
        len: u32,
    },
    BlobIndex(BlobIndex),
    Blob {
        file_id: u64,
        offset: u64,
        len: u64,
        checksum: u32,
    },
}

impl ValueRefHeader {
    fn view<'block>(&self, bytes: &'block [u8]) -> Result<ValueRefView<'block>> {
        match *self {
            Self::Inline { offset, len } => {
                let start = u32_to_usize(offset);
                let end = start
                    .checked_add(u32_to_usize(len))
                    .ok_or_else(|| invalid_table("inline value length overflows"))?;
                let bytes = bytes
                    .get(start..end)
                    .ok_or_else(|| invalid_table("inline value points outside data block"))?;
                Ok(ValueRefView::Inline(bytes))
            }
            Self::BlobIndex(index) => Ok(ValueRefView::BlobIndex(index)),
            Self::Blob {
                file_id,
                offset,
                len,
                checksum,
            } => Ok(ValueRefView::Blob {
                file_id,
                offset,
                len,
                checksum,
            }),
        }
    }
}

fn inline_value_range(offset: u32, len: u32, block_len: usize) -> Result<Range<usize>> {
    let start = u32_to_usize(offset);
    let end = start
        .checked_add(u32_to_usize(len))
        .ok_or_else(|| invalid_table("inline value length overflows"))?;
    if end > block_len {
        return Err(invalid_table("inline value points outside data block"));
    }
    Ok(start..end)
}

#[derive(Debug, Clone, Copy)]
struct DataBlockRecordView<'block> {
    user_key: &'block [u8],
    sequence: Sequence,
    kind: ValueKind,
    batch_index: u32,
    value: Option<ValueRefView<'block>>,
}

impl DataBlockRecordView<'_> {
    fn to_owned(self) -> TablePointRecord {
        TablePointRecord {
            internal_key: InternalKey::new(
                self.user_key.to_vec(),
                self.sequence,
                self.kind,
                self.batch_index,
            ),
            value: self.value.map(ValueRefView::to_owned),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ValueRefView<'block> {
    Inline(&'block [u8]),
    BlobIndex(BlobIndex),
    Blob {
        file_id: u64,
        offset: u64,
        len: u64,
        checksum: u32,
    },
}

impl ValueRefView<'_> {
    fn to_owned(self) -> ValueRef {
        match self {
            Self::Inline(bytes) => ValueRef::Inline(bytes.to_vec()),
            Self::BlobIndex(index) => ValueRef::BlobIndex(index),
            Self::Blob {
                file_id,
                offset,
                len,
                checksum,
            } => ValueRef::Blob {
                file_id,
                offset,
                len,
                checksum,
            },
        }
    }
}

#[derive(Debug)]
struct TableFilterStats {
    shards: Box<[TableFilterStatsShard; TABLE_STAT_SHARDS]>,
}

#[derive(Debug)]
struct TableReadPathStats {
    shards: Box<[TableReadPathStatsShard; TABLE_STAT_SHARDS]>,
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct TableReadPathStatsShard {
    table_probes: AtomicU64,
    index_partition_probes: AtomicU64,
    block_metadata_probes: AtomicU64,
    data_block_reads: AtomicU64,
    filter_misses: AtomicU64,
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct TableFilterStatsShard {
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

impl Default for TableReadPathStats {
    fn default() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| TableReadPathStatsShard::default())),
        }
    }
}

impl Default for TableFilterStats {
    fn default() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| TableFilterStatsShard::default())),
        }
    }
}

impl TableReadPathStats {
    fn snapshot(&self) -> ReadPathStats {
        let mut stats = ReadPathStats::default();
        for shard in self.shards.iter() {
            stats.point_table_probes = stats
                .point_table_probes
                .saturating_add(shard.table_probes.load(Ordering::Acquire));
            stats.point_index_partition_probes = stats
                .point_index_partition_probes
                .saturating_add(shard.index_partition_probes.load(Ordering::Acquire));
            stats.point_block_metadata_probes = stats
                .point_block_metadata_probes
                .saturating_add(shard.block_metadata_probes.load(Ordering::Acquire));
            stats.point_data_block_reads = stats
                .point_data_block_reads
                .saturating_add(shard.data_block_reads.load(Ordering::Acquire));
            stats.point_filter_misses = stats
                .point_filter_misses
                .saturating_add(shard.filter_misses.load(Ordering::Acquire));
        }
        stats
    }

    fn record_point_table_probe(&self) {
        self.shard().table_probes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_point_index_partition_probe(&self) {
        self.shard()
            .index_partition_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_point_block_metadata_probe(&self) {
        self.shard()
            .block_metadata_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_point_data_block_read(&self) {
        self.shard()
            .data_block_reads
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_point_filter_miss(&self) {
        self.shard().filter_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn shard(&self) -> &TableReadPathStatsShard {
        &self.shards[table_stat_shard_index()]
    }
}

impl TableFilterStats {
    fn snapshot(&self) -> FilterStats {
        let mut stats = FilterStats::default();
        for shard in self.shards.iter() {
            stats.table_point_hits = stats
                .table_point_hits
                .saturating_add(shard.table_point_hits.load(Ordering::Acquire));
            stats.table_point_misses = stats
                .table_point_misses
                .saturating_add(shard.table_point_misses.load(Ordering::Acquire));
            stats.table_point_false_positives = stats
                .table_point_false_positives
                .saturating_add(shard.table_point_false_positives.load(Ordering::Acquire));
            stats.table_prefix_hits = stats
                .table_prefix_hits
                .saturating_add(shard.table_prefix_hits.load(Ordering::Acquire));
            stats.table_prefix_misses = stats
                .table_prefix_misses
                .saturating_add(shard.table_prefix_misses.load(Ordering::Acquire));
            stats.table_prefix_false_positives = stats
                .table_prefix_false_positives
                .saturating_add(shard.table_prefix_false_positives.load(Ordering::Acquire));
            stats.block_point_hits = stats
                .block_point_hits
                .saturating_add(shard.block_point_hits.load(Ordering::Acquire));
            stats.block_point_misses = stats
                .block_point_misses
                .saturating_add(shard.block_point_misses.load(Ordering::Acquire));
            stats.block_point_false_positives = stats
                .block_point_false_positives
                .saturating_add(shard.block_point_false_positives.load(Ordering::Acquire));
            stats.block_prefix_hits = stats
                .block_prefix_hits
                .saturating_add(shard.block_prefix_hits.load(Ordering::Acquire));
            stats.block_prefix_misses = stats
                .block_prefix_misses
                .saturating_add(shard.block_prefix_misses.load(Ordering::Acquire));
            stats.block_prefix_false_positives = stats
                .block_prefix_false_positives
                .saturating_add(shard.block_prefix_false_positives.load(Ordering::Acquire));
        }
        stats
    }

    fn record_table_point(&self, allowed: bool) {
        let shard = self.shard();
        record_filter_result(&shard.table_point_hits, &shard.table_point_misses, allowed);
    }

    fn record_table_prefix(&self, allowed: bool) {
        let shard = self.shard();
        record_filter_result(
            &shard.table_prefix_hits,
            &shard.table_prefix_misses,
            allowed,
        );
    }

    fn record_block_point(&self, allowed: bool) {
        let shard = self.shard();
        record_filter_result(&shard.block_point_hits, &shard.block_point_misses, allowed);
    }

    fn record_block_prefix(&self, allowed: bool) {
        let shard = self.shard();
        record_filter_result(
            &shard.block_prefix_hits,
            &shard.block_prefix_misses,
            allowed,
        );
    }

    fn record_table_point_false_positive(&self) {
        self.shard()
            .table_point_false_positives
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_block_point_false_positive(&self) {
        self.shard()
            .block_point_false_positives
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_block_prefix_false_positive(&self) {
        self.shard()
            .block_prefix_false_positives
            .fetch_add(1, Ordering::Relaxed);
    }

    fn shard(&self) -> &TableFilterStatsShard {
        &self.shards[table_stat_shard_index()]
    }
}

fn table_stat_shard_index() -> usize {
    TABLE_STAT_SHARD.with(|index| *index)
}

fn record_filter_result(hits: &AtomicU64, misses: &AtomicU64, allowed: bool) {
    if allowed {
        hits.fetch_add(1, Ordering::Relaxed);
    } else {
        misses.fetch_add(1, Ordering::Relaxed);
    }
}

// Loaded in-memory tables keep one sorted record array. Persistent table
// blocks keep only key bounds and file handles here; the data-block cache owns
// the compact block bytes after a block is read.
#[derive(Debug, Clone)]
pub(crate) struct TableDataBlock {
    smallest_internal_key: InternalKey,
    largest_internal_key: InternalKey,
    block: BlockHandle,
    record_range: Range<usize>,
    point_key_filter: Option<PointKeyFilter>,
    prefix_filter: Option<PrefixFilter>,
}

impl TableDataBlock {
    fn from_record_range(
        point_records: &[TablePointRecord],
        record_range: Range<usize>,
        restart_indices: &[usize],
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
        restart_indices: &[usize],
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
        for restart_index in restart_indices {
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

    pub(crate) fn estimated_bytes(&self) -> u64 {
        usize_to_u64_saturating(size_of::<Self>())
            .saturating_add(usize_to_u64_saturating(
                self.smallest_internal_key.user_key().len(),
            ))
            .saturating_add(usize_to_u64_saturating(
                self.largest_internal_key.user_key().len(),
            ))
            .saturating_add(self.filter_bytes())
    }

    fn filter_bytes(&self) -> u64 {
        let point = self
            .point_key_filter
            .as_ref()
            .map_or(0, |filter| usize_to_u64_saturating(filter.bytes().len()));
        let prefix = self
            .prefix_filter
            .as_ref()
            .map_or(0, |filter| usize_to_u64_saturating(filter.bytes().len()));
        point.saturating_add(prefix)
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
    file: Option<Arc<NativeFileObject>>,
    payload_len: usize,
    footer: TableFooter,
    properties: TableProperties,
    point_records: Option<Vec<TablePointRecord>>,
    data_blocks: Option<Vec<TableDataBlock>>,
    data_block_count: usize,
    index_partitions: Vec<IndexPartitionEntry>,
    index_partition_cache: Arc<RwLock<BTreeMap<usize, Arc<Vec<TableDataBlock>>>>>,
    range_tombstones: Arc<RwLock<Option<Arc<RangeTombstoneIndex<TableRangeTombstone>>>>>,
    may_have_range_tombstones: bool,
    point_key_filter: Option<PointKeyFilter>,
    prefix_filter: Option<PrefixFilter>,
    filter_stats: Arc<TableFilterStats>,
    read_path_stats: Arc<TableReadPathStats>,
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
        self.read_path_stats.record_point_table_probe();
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
            let decision = self.with_data_block_metadata(block_index, block_cache, |block| {
                if !block.prefix_bounds_may_overlap(prefix) {
                    return Ok(PrefixBlockDecision::Done);
                }
                let (allowed, had_filter) =
                    self.block_prefix_filter_allows(block, prefix, extractor);
                if !allowed {
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
        for block_index in 0..self.data_block_count {
            let block = self.load_data_block(block_index, None)?;
            records.extend(block.records_owned()?);
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

    fn with_data_block_metadata<R>(
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

    fn load_index_partition(
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

    fn index_partition_index_for_block(&self, block_index: usize) -> Result<usize> {
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

    fn partition_contains_block(&self, partition_index: usize, block_index: usize) -> bool {
        self.index_partitions
            .get(partition_index)
            .is_some_and(|partition| {
                let end = partition
                    .first_data_block_index
                    .saturating_add(partition.data_block_count);
                partition.first_data_block_index <= block_index && block_index < end
            })
    }

    fn load_data_block(
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

    fn first_block_for_key(
        &self,
        key: &[u8],
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        self.first_block_matching_key(key, policy, block_cache)
    }

    fn first_block_for_range(
        &self,
        range: &KeyRange,
        policy: IndexSearchPolicy,
        block_cache: Option<&BlockCache>,
    ) -> Result<Option<usize>> {
        self.first_block_matching_range_start(range, policy, block_cache)
    }

    #[cfg(test)]
    fn first_block_for_prefix(
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

    fn first_block_candidate_for_key(
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

    fn first_block_matching_key(
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

    fn first_partition_matching_key(
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

    fn first_block_candidate_for_range(
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

    fn first_block_matching_range_start(
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

    fn last_block_candidate_for_range(
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
            match self.forward_block_state(block_index)? {
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
            match self.reverse_block_state(block_index)? {
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

    fn forward_block_state(&self, block_index: usize) -> Result<CursorBlockState> {
        self.table
            .with_data_block_metadata(
                block_index,
                self.block_cache.as_deref(),
                |block| match &self.selector {
                    ScanSelector::Range(range) => {
                        if key_is_after_end(block.smallest_internal_key.user_key(), &range.end) {
                            Ok(CursorBlockState::Done)
                        } else if block.overlaps_range(range) {
                            Ok(CursorBlockState::Scan)
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
                            Ok(CursorBlockState::Scan)
                        } else {
                            let (allowed, _) = self.table.block_prefix_filter_allows(
                                block,
                                prefix,
                                &self.prefix_extractor,
                            );
                            if allowed {
                                Ok(CursorBlockState::Scan)
                            } else {
                                Ok(CursorBlockState::Skip)
                            }
                        }
                    }
                },
            )
    }

    fn reverse_block_state(&self, block_index: usize) -> Result<CursorBlockState> {
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
                            Ok(CursorBlockState::Scan)
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
                            Ok(CursorBlockState::Scan)
                        } else {
                            let (allowed, _) = self.table.block_prefix_filter_allows(
                                block,
                                prefix,
                                &self.prefix_extractor,
                            );
                            if allowed {
                                Ok(CursorBlockState::Scan)
                            } else {
                                Ok(CursorBlockState::Skip)
                            }
                        }
                    }
                },
            )
    }

    fn move_to_next_block(&mut self) {
        let Some(block_index) = self.block_index else {
            self.exhausted = true;
            return;
        };
        let next = block_index + 1;
        self.block_index = (next < self.table.data_block_count).then_some(next);
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
            if self.table.with_data_block_metadata(
                block_index,
                self.block_cache.as_deref(),
                |table_block| {
                    Ok(table_block
                        .prefix_filter_result(prefix, &self.prefix_extractor)
                        .is_some())
                },
            )? && !data_block_has_prefix(&block, prefix, self.policy)?
            {
                self.table.filter_stats.record_block_prefix_false_positive();
            }
        }
        self.record_index = match self.direction {
            Direction::Forward => {
                first_record_index_for_decoded_block(&block, &self.selector, self.policy)?
            }
            Direction::Reverse => block.record_count(),
        };
        self.current_block = Some((block_index, block));
        Ok(())
    }

    fn current_block_len(&mut self, block_index: usize) -> Result<usize> {
        self.ensure_current_block(block_index)?;
        Ok(self
            .current_block
            .as_ref()
            .map_or(0, |(_, block)| block.record_count()))
    }

    fn current_block_record(
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PointBlockDecision {
    Done,
    Skip,
    Read { had_filter: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeBlockDecision {
    Done,
    Skip,
    Read,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefixBlockDecision {
    Done,
    Skip,
    Read { had_filter: bool },
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
        ScanSelector::Range(range) => table.first_block_candidate_for_range(range, policy),
        ScanSelector::Prefix(prefix) => table.first_block_candidate_for_key(prefix, policy),
    }
}

fn last_block_for_selector(
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

fn first_record_index_for_decoded_block(
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

fn data_block_point_records_for_key(
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
fn data_block_newest_visible_point_record_for_key(
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

fn data_block_newest_visible_point_value_record_for_key(
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

fn data_block_point_records_in_range(
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
fn data_block_point_records_with_prefix(
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

fn data_block_has_prefix(
    block: &DecodedDataBlock,
    prefix: &[u8],
    policy: IndexSearchPolicy,
) -> Result<bool> {
    let start = data_block_restart_index_for_key(block, prefix, policy)?;
    for record_index in start..block.record_count() {
        let record = block.record_view(record_index)?;
        if record.user_key >= prefix {
            return Ok(record.user_key.starts_with(prefix));
        }
    }
    Ok(false)
}

fn data_block_restart_index_for_bound(
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

fn data_block_restart_index_for_key(
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

fn data_block_linear_restart_partition_point(
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

fn data_block_binary_restart_partition_point(
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

fn data_block_point_record_range_for_key(
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

#[must_use]
pub fn table_path(db_path: &Path, table_id: TableId) -> PathBuf {
    db_path.join(format!(
        "table-{id:020}.{TABLE_FILE_EXTENSION}",
        id = table_id.get()
    ))
}

pub(crate) fn list_table_file_ids(db_path: &Path) -> Result<BTreeSet<TableId>> {
    let backend = table_storage_backend();
    backend
        .capabilities()
        .require(StorageCapability::ObjectListing)?;
    let request = StorageObjectListRequest::native_file(StorageObjectKind::Table, db_path)
        .with_file_extension(TABLE_FILE_EXTENSION);
    let mut table_ids = BTreeSet::new();

    for object in backend.list_objects_blocking(request)? {
        let path = object.path();
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
    let point_records = if options.rewrite_blob_indexes {
        // Level Merge keeps the same MVCC records but gives retained large
        // values a fresh blob layout beside the output table.
        crate::blob::inline_blob_values(db_path, &point_records)?
    } else {
        point_records
    };
    let point_records = crate::blob::write_large_values(
        db_path,
        table_id.get(),
        options.blob_threshold_bytes,
        CodecId::None,
        &point_records,
    )?
    .into_iter()
    .map(|(internal_key, value)| TablePointRecord {
        internal_key,
        value,
    })
    .collect::<Vec<_>>();
    let data_blocks = build_data_blocks(&point_records, options)?;
    let (point_key_filter, prefix_filter) = if should_pin_read_metadata(level) {
        (
            build_point_key_filter(options, &point_records),
            build_prefix_filter(options, &point_records),
        )
    } else {
        (None, None)
    };

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
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
        read_path_stats: Arc::new(TableReadPathStats::default()),
        point_records: Some(point_records),
        data_block_count: data_blocks.len(),
        index_partitions: index_partitions_for_loaded_blocks(&data_blocks),
        index_partition_cache: Arc::new(RwLock::new(BTreeMap::new())),
        data_blocks: Some(data_blocks),
        range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
            range_tombstones.to_vec(),
        ))))),
        may_have_range_tombstones: !range_tombstones.is_empty(),
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

    let backend = table_storage_backend();
    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    let object = table_storage_object(path);
    backend.write_object_blocking(
        object,
        Arc::from(bytes.into_boxed_slice()),
        DurabilityMode::SyncAll,
    )?;

    read_table(path)
}

pub(crate) fn read_table(path: &Path) -> Result<Table> {
    let backend = table_storage_backend();
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    let object = table_storage_object(path);
    let table_file = Arc::new(backend.open_read_blocking(object)?);
    let source = StorageReadSource::new(table_file.as_ref());

    let header = read_table_header(path, &source)?;
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
    let file_len = table_file.len_blocking()?;
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

    let footer = read_footer_from_source(&source, payload_len)?;
    validate_footer_sections_by_len(payload_len, &footer)?;

    let (properties_codec, properties_payload) =
        read_single_block_section_from_source(&source, payload_len, footer.properties)?;
    let properties = decode_properties_block(&properties_payload)?;
    validate_block_codec(properties_codec, properties.codec, TableSection::Properties)?;

    let (top_level_block, index_codec, index_payload) =
        read_first_block_in_section_from_source(&source, payload_len, footer.indexes)?;
    validate_index_top_level_codec(index_codec, properties.codec)?;
    let index_partitions = decode_index_top_level(&index_payload)?;
    let data_block_count =
        validate_index_top_level(top_level_block, &index_partitions, footer.indexes)?;
    let (point_key_filter, prefix_filter) =
        read_pinned_table_filters(&source, payload_len, footer.filters, &properties)?;
    let index_partition_cache = Arc::new(RwLock::new(read_pinned_index_partitions(
        &source,
        payload_len,
        footer.data_blocks,
        &index_partitions,
        properties.codec,
        properties.level,
    )?));
    Ok(Table {
        path: Some(path.to_path_buf()),
        file: Some(table_file),
        payload_len,
        footer,
        properties,
        point_records: None,
        data_blocks: None,
        data_block_count,
        index_partitions,
        index_partition_cache,
        range_tombstones: Arc::new(RwLock::new(None)),
        may_have_range_tombstones: true,
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
        read_path_stats: Arc::new(TableReadPathStats::default()),
    })
}

fn read_table_header(path: &Path, source: &impl BlockReadSource) -> Result<[u8; HEADER_LEN]> {
    let mut header = [0_u8; HEADER_LEN];
    source
        .read_exact_at(0, &mut header)
        .map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} header cannot be read: {error}",
                path.display()
            ),
        })?;
    Ok(header)
}

fn table_read_source<'src>(
    path: &Path,
    file: Option<&'src NativeFileObject>,
) -> NativeFileReadSource<'src, NativeFileObject> {
    NativeFileReadSource::new(table_storage_object(path), file)
}

fn table_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Table, path)
}

fn table_storage_backend() -> NativeFileBackend {
    NativeFileBackend::new()
}

fn read_pinned_table_filters(
    source: &impl BlockReadSource,
    payload_len: usize,
    filter_section: SectionHandle,
    properties: &TableProperties,
) -> Result<(Option<PointKeyFilter>, Option<PrefixFilter>)> {
    if !should_pin_read_metadata(properties.level) {
        return Ok((None, None));
    }

    let (filter_codec, filter_payload) =
        read_single_block_section_from_source(source, payload_len, filter_section)?;
    validate_block_codec(filter_codec, properties.codec, TableSection::Filters)?;
    decode_filter_block(&filter_payload)
}

fn read_pinned_index_partitions(
    source: &impl BlockReadSource,
    payload_len: usize,
    data_blocks_section: SectionHandle,
    partitions: &[IndexPartitionEntry],
    expected_codec: CodecId,
    level: TableLevel,
) -> Result<BTreeMap<usize, Arc<Vec<TableDataBlock>>>> {
    let mut pinned = BTreeMap::new();
    if !should_pin_read_metadata(level) {
        return Ok(pinned);
    }

    for (partition_index, partition) in partitions.iter().enumerate() {
        let entries = read_index_partition_from_source(
            source,
            payload_len,
            data_blocks_section,
            expected_codec,
            partition,
        )?;
        pinned.insert(partition_index, Arc::new(entries));
    }
    Ok(pinned)
}

fn read_index_partition_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    data_blocks_section: SectionHandle,
    expected_codec: CodecId,
    partition: &IndexPartitionEntry,
) -> Result<Vec<TableDataBlock>> {
    let (codec, payload) = read_checked_block_from_source(source, payload_len, partition.block)?;
    validate_block_codec(codec, expected_codec, TableSection::Indexes)?;
    let entries = decode_index_block(&payload)?;
    validate_index_partition(partition, &entries, data_blocks_section)?;
    entries
        .into_iter()
        .map(TableDataBlock::from_index_entry)
        .collect()
}

const fn should_pin_read_metadata(level: TableLevel) -> bool {
    level.get() <= PINNED_READ_METADATA_MAX_LEVEL
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

    let blob_references = table_blob_references(point_records);
    let blob_file_ids = blob_references
        .iter()
        .map(|reference| reference.file_id)
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
        blob_references,
    }
}

fn table_blob_references(point_records: &[TablePointRecord]) -> Vec<TableBlobReference> {
    let mut references = BTreeMap::<u64, TableBlobReference>::new();

    for record in point_records {
        let Some(value) = record.value.as_ref() else {
            continue;
        };
        let (file_id, referenced_bytes) = match value {
            ValueRef::BlobIndex(index) => (index.file_id, index.encoded_len),
            ValueRef::Blob { file_id, len, .. } => (*file_id, *len),
            ValueRef::Inline(_) => continue,
        };

        references
            .entry(file_id)
            .and_modify(|reference| {
                reference.referenced_bytes =
                    reference.referenced_bytes.saturating_add(referenced_bytes);
                reference.referenced_record_count =
                    reference.referenced_record_count.saturating_add(1);
                if record.internal_key < reference.smallest_internal_key {
                    reference.smallest_internal_key = record.internal_key.clone();
                }
                if record.internal_key > reference.largest_internal_key {
                    reference.largest_internal_key = record.internal_key.clone();
                }
            })
            .or_insert_with(|| TableBlobReference {
                file_id,
                referenced_bytes,
                referenced_record_count: 1,
                smallest_internal_key: record.internal_key.clone(),
                largest_internal_key: record.internal_key.clone(),
            });
    }

    references.into_values().collect()
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
            &restart_indices,
            build_point_key_filter(options, records),
            build_prefix_filter(options, records),
        )?);
        block_start = block_end;
    }

    Ok(data_blocks)
}

fn index_partitions_for_loaded_blocks(data_blocks: &[TableDataBlock]) -> Vec<IndexPartitionEntry> {
    data_blocks
        .chunks(INDEX_PARTITION_TARGET_ENTRIES)
        .enumerate()
        .filter_map(|(partition_index, blocks)| {
            let first = blocks.first()?;
            let last = blocks.last()?;
            Some(IndexPartitionEntry {
                smallest_internal_key: first.smallest_internal_key.clone(),
                largest_internal_key: last.largest_internal_key.clone(),
                block: BlockHandle { offset: 0, len: 0 },
                first_data_block_index: partition_index * INDEX_PARTITION_TARGET_ENTRIES,
                data_block_count: blocks.len(),
            })
        })
        .collect()
}

fn encode_table(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let codec = table.properties.codec;
    let point_records = table
        .point_records
        .as_deref()
        .ok_or_else(|| invalid_table("cannot encode table without point records"))?;
    let data_block_metadata = table
        .data_blocks
        .as_deref()
        .ok_or_else(|| invalid_table("cannot encode table without data block metadata"))?;
    let (data_blocks, index_entries) =
        append_data_blocks(&mut bytes, codec, point_records, data_block_metadata)?;
    let range_tombstones =
        append_single_block_section(&mut bytes, codec, &encode_range_tombstone_block(table)?)?;
    let filters = append_single_block_section(&mut bytes, codec, &encode_filter_block(table)?)?;
    let indexes = append_index_section(&mut bytes, codec, &index_entries)?;
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
    let backend = crate::storage::MemoryStorageBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    let object = StorageObjectId::memory(StorageObjectKind::Table, "test-table");
    backend.insert_read_object(object.clone(), bytes.to_vec())?;
    let table_object = backend.open_read_blocking(object)?;
    decode_table_from_storage_object(&table_object)
}

#[cfg(test)]
#[allow(clippy::too_many_lines)]
fn decode_table_from_storage_object(
    table_object: &impl BlockingStorageReadObject,
) -> Result<Table> {
    let source = StorageReadSource::new(table_object);
    let file_len = table_object.len_blocking()?;
    if file_len < HEADER_LEN as u64 {
        return Err(invalid_table("short header"));
    }

    let mut header = [0_u8; HEADER_LEN];
    source.read_exact_at(0, &mut header)?;
    let magic = read_u32_at(&header, 0)?;
    let version = read_u16_at(&header, 4)?;
    let payload_len = read_u32_at(&header, 6)? as usize;
    let payload_checksum = read_u32_at(&header, 10)?;
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

    let mut payload = vec![0_u8; payload_len];
    source.read_exact_at(HEADER_LEN, &mut payload)?;
    if checksum(&payload) != payload_checksum {
        return Err(Error::Corruption {
            message: "table checksum mismatch".to_owned(),
        });
    }

    let footer = read_footer_from_source(&source, payload_len)?;
    validate_footer_sections_by_len(payload_len, &footer)?;

    let (properties_codec, properties_payload) =
        read_single_block_section_from_source(&source, payload_len, footer.properties)?;
    let properties = decode_properties_block(&properties_payload)?;
    validate_block_codec(properties_codec, properties.codec, TableSection::Properties)?;

    let (top_level_block, index_codec, index_payload) =
        read_first_block_in_section_from_source(&source, payload_len, footer.indexes)?;
    validate_index_top_level_codec(index_codec, properties.codec)?;
    let index_partitions = decode_index_top_level(&index_payload)?;
    let data_block_count =
        validate_index_top_level(top_level_block, &index_partitions, footer.indexes)?;

    let (point_records, data_blocks) = decode_test_point_records_and_blocks_from_source(
        &source,
        payload_len,
        &properties,
        &index_partitions,
        footer.data_blocks,
    )?;

    let (tombstone_codec, tombstone_payload) =
        read_single_block_section_from_source(&source, payload_len, footer.range_tombstones)?;
    validate_block_codec(
        tombstone_codec,
        properties.codec,
        TableSection::RangeTombstones,
    )?;
    let range_tombstones = decode_range_tombstone_block(&tombstone_payload)?;
    let (filter_codec, filter_payload) =
        read_single_block_section_from_source(&source, payload_len, footer.filters)?;
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

    let may_have_range_tombstones = !range_tombstones.is_empty();

    Ok(Table {
        path: None,
        file: None,
        payload_len,
        footer,
        properties,
        point_records: Some(point_records),
        data_block_count,
        index_partitions: index_partitions_for_loaded_blocks(&data_blocks),
        index_partition_cache: Arc::new(RwLock::new(BTreeMap::new())),
        data_blocks: Some(data_blocks),
        range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
            range_tombstones,
        ))))),
        may_have_range_tombstones,
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
        read_path_stats: Arc::new(TableReadPathStats::default()),
    })
}

#[cfg(test)]
fn decode_test_point_records_and_blocks_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    properties: &TableProperties,
    index_partitions: &[IndexPartitionEntry],
    data_blocks_section: SectionHandle,
) -> Result<(Vec<TablePointRecord>, Vec<TableDataBlock>)> {
    let mut point_records = Vec::new();
    let mut data_blocks = Vec::new();
    for partition in index_partitions {
        let (index_codec, index_payload) =
            read_checked_block_from_source(source, payload_len, partition.block)?;
        validate_block_codec(index_codec, properties.codec, TableSection::Indexes)?;
        let index_entries = decode_index_block(&index_payload)?;
        validate_index_partition(partition, &index_entries, data_blocks_section)?;
        for entry in index_entries {
            let (block_codec, block_payload) =
                read_checked_block_from_source(source, payload_len, entry.block)?;
            validate_block_codec(block_codec, properties.codec, TableSection::DataBlocks)?;
            let decoded_block = decode_data_block_for_verify(block_payload)?;
            validate_decoded_data_block_entry(&entry, &decoded_block)?;
            validate_decoded_data_block_filters(&entry, &decoded_block)?;
            let block_records = decoded_block.records_owned()?;
            let record_start = point_records.len();
            point_records.extend(block_records);
            let record_end = point_records.len();
            data_blocks.push(TableDataBlock::from_record_range_and_block(
                &point_records,
                record_start..record_end,
                &decoded_block.restart_indices_with_base(record_start),
                entry.block,
                entry.point_key_filter.clone(),
                entry.prefix_filter.clone(),
            )?);
        }
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
        let block = BlockManager::append_checked(bytes, codec, &block_payload)?;
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

fn append_index_section(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    index_entries: &[DataBlockIndexEntry],
) -> Result<SectionHandle> {
    let section_start = bytes.len();
    let partition_payloads = index_entries
        .chunks(INDEX_PARTITION_TARGET_ENTRIES)
        .enumerate()
        .map(|(partition_index, entries)| {
            let payload = encode_index_block(entries)?;
            let block = BlockManager::encode_checked(codec, &payload)?;
            let first_data_block_index = partition_index * INDEX_PARTITION_TARGET_ENTRIES;
            let first = entries
                .first()
                .ok_or_else(|| invalid_table("empty index partition"))?;
            let last = entries
                .last()
                .ok_or_else(|| invalid_table("empty index partition"))?;
            Ok((
                IndexPartitionEntry {
                    smallest_internal_key: first.smallest_internal_key.clone(),
                    largest_internal_key: last.largest_internal_key.clone(),
                    block: BlockHandle { offset: 0, len: 0 },
                    first_data_block_index,
                    data_block_count: entries.len(),
                },
                block,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut partitions = partition_payloads
        .iter()
        .map(|(partition, block)| {
            let mut partition = partition.clone();
            partition.block.len = usize_to_u64(block.len(), "index partition block length")?;
            Ok(partition)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut top_level =
        BlockManager::encode_checked(CodecId::None, &encode_index_top_level(&partitions)?)?;
    let mut next_offset = section_start
        .checked_add(top_level.len())
        .ok_or_else(|| invalid_table("index section offset overflow"))?;
    for (partition, (_, block)) in partitions.iter_mut().zip(&partition_payloads) {
        partition.block.offset = usize_to_u64(next_offset, "index partition block offset")?;
        next_offset = next_offset
            .checked_add(block.len())
            .ok_or_else(|| invalid_table("index section offset overflow"))?;
    }
    // The top-level partition index stores absolute offsets for the following
    // partition blocks. Keep it uncompressed so its byte length is stable after
    // those offsets are filled in; partition blocks still use the table codec.
    top_level = BlockManager::encode_checked(CodecId::None, &encode_index_top_level(&partitions)?)?;
    bytes.extend_from_slice(&top_level);
    for (_, block) in partition_payloads {
        bytes.extend_from_slice(&block);
    }

    SectionHandle::from_span(section_start, bytes.len())
}

fn append_single_block_section(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    block_payload: &[u8],
) -> Result<SectionHandle> {
    let section_start = bytes.len();
    BlockManager::append_checked(bytes, codec, block_payload)?;
    SectionHandle::from_span(section_start, bytes.len())
}

fn encode_data_block(records: &[TablePointRecord]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut restart_offsets = Vec::new();
    let point_lookup_index = DataBlockPointLookupIndex::from_records(records)?;
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
    put_data_block_point_lookup_index(&mut bytes, &point_lookup_index)?;

    Ok(bytes)
}

fn put_data_block_point_lookup_index(
    bytes: &mut Vec<u8>,
    index: &DataBlockPointLookupIndex,
) -> Result<()> {
    put_u32(
        bytes,
        usize_to_u32(index.entries.len(), "data block hash index entry count")?,
    );
    for entry in &index.entries {
        put_u64(bytes, entry.key_hash);
        put_u32(bytes, entry.start_record);
        put_u32(bytes, entry.end_record);
    }
    Ok(())
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

fn encode_index_top_level(partitions: &[IndexPartitionEntry]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(partitions.len(), "index partition count")?,
    );
    for partition in partitions {
        put_internal_key(&mut bytes, &partition.smallest_internal_key)?;
        put_internal_key(&mut bytes, &partition.largest_internal_key)?;
        put_u64(&mut bytes, partition.block.offset);
        put_u64(&mut bytes, partition.block.len);
        put_u32(
            &mut bytes,
            usize_to_u32(
                partition.first_data_block_index,
                "index partition first data block",
            )?,
        );
        put_u32(
            &mut bytes,
            usize_to_u32(
                partition.data_block_count,
                "index partition data block count",
            )?,
        );
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

fn read_footer_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
) -> Result<TableFooter> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let footer_start = HEADER_LEN
        .checked_add(payload_len - FOOTER_LEN)
        .ok_or_else(|| invalid_table("footer offset overflow"))?;
    let mut footer = [0_u8; FOOTER_LEN];
    source.read_exact_at(footer_start, &mut footer)?;
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
fn read_first_block_in_section(
    payload: &[u8],
    section: SectionHandle,
) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
    let (section_start, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty block section"));
    }
    let (block, codec, decoded) = read_checked_block_at_payload_offset(payload, section_start)?;
    let (_, block_end) = block_bounds(block)?;
    if block_end > section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    Ok((block, codec, decoded))
}

fn read_single_block_section_from_file(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    section: SectionHandle,
) -> Result<(CodecId, Vec<u8>)> {
    let source = table_read_source(path, file);
    read_single_block_section_from_source(&source, payload_len, section)
}

fn read_single_block_section_from_source(
    source: &impl BlockReadSource,
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
    read_checked_block_from_source(source, payload_len, block)
}

fn read_first_block_in_section_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    section: SectionHandle,
) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let (section_start, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty block section"));
    }
    if section_end > payload_len - FOOTER_LEN {
        return Err(Error::Corruption {
            message: "table section layout is inconsistent".to_owned(),
        });
    }
    let (block, codec, decoded) =
        read_checked_block_at_source_offset(source, payload_len, section_start)?;
    let (_, block_end) = block_bounds(block)?;
    if block_end > section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    Ok((block, codec, decoded))
}

#[cfg(test)]
fn read_checked_block(payload: &[u8], block: BlockHandle) -> Result<(CodecId, Vec<u8>)> {
    BlockManager::read_checked(payload, block)
}

#[cfg(test)]
fn read_checked_block_at_payload_offset(
    payload: &[u8],
    offset: usize,
) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
    BlockManager::read_checked_at_payload_offset(payload, offset)
}

#[cfg(test)]
fn read_checked_block_from_file(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    block: BlockHandle,
) -> Result<(CodecId, Vec<u8>)> {
    let source = table_read_source(path, file);
    read_checked_block_from_source(&source, payload_len, block)
}

fn read_checked_block_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    block: BlockHandle,
) -> Result<(CodecId, Vec<u8>)> {
    BlockManager::read_checked_from_source(payload_len, HEADER_LEN, block, source)
}

fn read_checked_block_at_source_offset(
    source: &impl BlockReadSource,
    payload_len: usize,
    offset: usize,
) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
    BlockManager::read_checked_at_source_offset(payload_len, HEADER_LEN, offset, source)
}

fn read_data_block_from_file(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    expected_codec: CodecId,
    entry: &DataBlockIndexEntry,
) -> Result<DecodedDataBlock> {
    let source = table_read_source(path, file);
    read_data_block_from_source(&source, payload_len, expected_codec, entry)
}

fn read_data_block_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    expected_codec: CodecId,
    entry: &DataBlockIndexEntry,
) -> Result<DecodedDataBlock> {
    let (actual_codec, payload) = read_checked_block_from_source(source, payload_len, entry.block)?;
    validate_block_codec(actual_codec, expected_codec, TableSection::DataBlocks)?;
    let decoded = decode_data_block(payload)?;
    validate_decoded_data_block_entry(entry, &decoded)?;
    validate_decoded_data_block_filters(entry, &decoded)?;
    Ok(decoded)
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

fn validate_index_top_level_codec(actual: CodecId, expected: CodecId) -> Result<()> {
    if matches!(actual, CodecId::None) || actual == expected {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "table index top-level block codec {} does not match table codec {} or none",
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

fn decode_index_top_level(bytes: &[u8]) -> Result<Vec<IndexPartitionEntry>> {
    let mut cursor = Cursor::new(bytes);
    let partition_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        partition_count,
        cursor.remaining_len(),
        MIN_INDEX_PARTITION_ENTRY_BYTES,
        "index partition count exceeds block bytes",
    )?;
    let mut partitions = Vec::with_capacity(partition_count);
    for _ in 0..partition_count {
        partitions.push(IndexPartitionEntry {
            smallest_internal_key: cursor.read_internal_key()?,
            largest_internal_key: cursor.read_internal_key()?,
            block: BlockHandle {
                offset: cursor.read_u64()?,
                len: cursor.read_u64()?,
            },
            first_data_block_index: cursor.read_u32()? as usize,
            data_block_count: cursor.read_u32()? as usize,
        });
    }
    if !cursor.is_finished() {
        return Err(invalid_table("trailing index top-level block bytes"));
    }
    Ok(partitions)
}

fn data_block_record_view_at<'block>(
    bytes: &'block [u8],
    record_headers: &[DataBlockRecordHeader],
    index: usize,
) -> Result<DataBlockRecordView<'block>> {
    record_headers
        .get(index)
        .ok_or_else(|| invalid_table("record index outside data block"))?
        .view(bytes)
}

fn read_data_block_record_header(cursor: &mut Cursor<'_>) -> Result<DataBlockRecordHeader> {
    let record_offset = usize_to_u32(cursor.offset, "data block record offset")?;
    let user_key = cursor.read_bytes_range()?;
    let sequence = Sequence::new(cursor.read_u64()?);
    let kind = cursor.read_value_kind()?;
    let batch_index = cursor.read_u32()?;
    let value = read_value_ref_header(cursor)?;
    let record_end = usize_to_u32(cursor.offset, "data block record end")?;
    Ok(DataBlockRecordHeader {
        record_offset,
        record_end,
        user_key_offset: user_key.start,
        user_key_len: user_key.end.saturating_sub(user_key.start),
        sequence,
        kind,
        batch_index,
        value,
    })
}

fn read_value_ref_header(cursor: &mut Cursor<'_>) -> Result<Option<ValueRefHeader>> {
    match cursor.read_u8()? {
        VALUE_NONE => Ok(None),
        VALUE_INLINE => {
            let bytes = cursor.read_bytes_range()?;
            Ok(Some(ValueRefHeader::Inline {
                offset: bytes.start,
                len: bytes.end.saturating_sub(bytes.start),
            }))
        }
        VALUE_BLOB => Ok(Some(ValueRefHeader::Blob {
            file_id: cursor.read_u64()?,
            offset: cursor.read_u64()?,
            len: cursor.read_u64()?,
            checksum: cursor.read_u32()?,
        })),
        VALUE_BLOB_INDEX => Ok(Some(ValueRefHeader::BlobIndex(BlobIndex {
            file_id: cursor.read_u64()?,
            offset: cursor.read_u64()?,
            encoded_len: cursor.read_u64()?,
            value_len: cursor.read_u64()?,
            value_checksum: cursor.read_u32()?,
            record_checksum: cursor.read_u32()?,
            compression: cursor.read_codec()?,
        }))),
        tag => Err(Error::InvalidFormat {
            message: format!("unknown table value reference {tag}"),
        }),
    }
}

fn decode_data_block(bytes: Vec<u8>) -> Result<DecodedDataBlock> {
    decode_data_block_with_hash_validation(bytes, false)
}

#[cfg(test)]
fn decode_data_block_for_verify(bytes: Vec<u8>) -> Result<DecodedDataBlock> {
    decode_data_block_with_hash_validation(bytes, true)
}

fn decode_data_block_with_hash_validation(
    bytes: Vec<u8>,
    validate_full_hash_index: bool,
) -> Result<DecodedDataBlock> {
    let bytes = Arc::<[u8]>::from(bytes);
    let mut cursor = Cursor::new(&bytes);
    let record_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        record_count,
        cursor.remaining_len(),
        MIN_DATA_RECORD_BYTES,
        "data record count exceeds block bytes",
    )?;
    let mut record_headers = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        record_headers.push(read_data_block_record_header(&mut cursor)?);
    }
    let restart_indices = decode_restart_points(&mut cursor, &record_headers)?;
    let point_lookup_index = decode_data_block_point_lookup_index(
        &mut cursor,
        &bytes,
        &record_headers,
        validate_full_hash_index,
    )?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing data block bytes"));
    }
    Ok(DecodedDataBlock {
        bytes,
        record_headers: record_headers.into_boxed_slice(),
        restart_indices,
        point_lookup_index,
    })
}

fn decode_data_block_point_lookup_index(
    cursor: &mut Cursor<'_>,
    bytes: &[u8],
    record_headers: &[DataBlockRecordHeader],
    validate_full_hash_index: bool,
) -> Result<DataBlockPointLookupIndex> {
    let entry_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        entry_count,
        cursor.remaining_len(),
        MIN_DATA_BLOCK_HASH_ENTRY_BYTES,
        "data block hash index entry count exceeds block bytes",
    )?;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let entry = BlockHashEntry {
            key_hash: cursor.read_u64()?,
            start_record: cursor.read_u32()?,
            end_record: cursor.read_u32()?,
        };
        validate_data_block_hash_entry(entry, bytes, record_headers)?;
        entries.push(entry);
    }

    let decoded = DataBlockPointLookupIndex::from_entries(entries);
    if validate_full_hash_index {
        let expected = data_block_point_lookup_index_from_block(bytes, record_headers)?;
        if decoded != expected {
            return Err(invalid_table(
                "data block hash index does not match records",
            ));
        }
    }
    Ok(decoded)
}

fn validate_data_block_hash_entry(
    entry: BlockHashEntry,
    bytes: &[u8],
    record_headers: &[DataBlockRecordHeader],
) -> Result<()> {
    if entry.start_record >= entry.end_record
        || u32_to_usize(entry.end_record) > record_headers.len()
    {
        return Err(invalid_table("data block hash index range is invalid"));
    }
    let first_record =
        data_block_record_view_at(bytes, record_headers, u32_to_usize(entry.start_record))?;
    if user_key_hash(first_record.user_key) != entry.key_hash {
        return Err(invalid_table("data block hash index hash mismatch"));
    }
    for record_index in u32_to_usize(entry.start_record)..u32_to_usize(entry.end_record) {
        let record = data_block_record_view_at(bytes, record_headers, record_index)?;
        if record.user_key != first_record.user_key {
            return Err(invalid_table("data block hash index range crosses keys"));
        }
    }
    Ok(())
}

fn data_block_point_lookup_index_from_block(
    bytes: &[u8],
    record_headers: &[DataBlockRecordHeader],
) -> Result<DataBlockPointLookupIndex> {
    let mut entries = Vec::new();
    let mut start = 0;
    while start < record_headers.len() {
        let first_record = data_block_record_view_at(bytes, record_headers, start)?;
        let key = first_record.user_key;
        let mut end = start + 1;
        while end < record_headers.len()
            && data_block_record_view_at(bytes, record_headers, end)?.user_key == key
        {
            end += 1;
        }
        entries.push(BlockHashEntry {
            key_hash: user_key_hash(key),
            start_record: usize_to_u32(start, "data block hash range start")?,
            end_record: usize_to_u32(end, "data block hash range end")?,
        });
        start = end;
    }
    Ok(DataBlockPointLookupIndex::from_entries(entries))
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

fn decode_restart_points(
    cursor: &mut Cursor<'_>,
    record_headers: &[DataBlockRecordHeader],
) -> Result<Box<[u32]>> {
    // The on-disk restart list stores byte offsets. Convert them to record
    // indexes once during table open, and reject offsets that do not land
    // exactly on a decoded record boundary.
    let records_end = cursor.offset;
    let restart_count = cursor.read_u32()? as usize;
    if record_headers.is_empty() {
        if restart_count == 0 {
            return Ok(Box::default());
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
        let restart = cursor.read_u32()?;
        if u32_to_usize(restart) >= records_end {
            return Err(invalid_table("data block restart outside record area"));
        }
        if previous_restart.is_some_and(|previous| restart <= previous) {
            return Err(invalid_table("data block restart points are not sorted"));
        }
        let record_index = record_headers
            .binary_search_by_key(&restart, |record| record.record_offset)
            .map_err(|_| invalid_table("data block restart is not a record start"))?;
        restart_indices.push(usize_to_u32(
            record_index,
            "data block restart record index",
        )?);
        previous_restart = Some(restart);
    }
    if restart_indices.first().copied() != Some(0) {
        return Err(invalid_table(
            "data block first restart is not first record",
        ));
    }

    Ok(restart_indices.into_boxed_slice())
}

fn validate_index_top_level(
    top_level_block: BlockHandle,
    partitions: &[IndexPartitionEntry],
    indexes: SectionHandle,
) -> Result<usize> {
    let (section_start, section_end) = section_bounds(indexes)?;
    let (top_start, top_end) = block_bounds(top_level_block)?;
    if top_start != section_start || top_end > section_end {
        return Err(Error::Corruption {
            message: "index top-level block is outside index section".to_owned(),
        });
    }
    let mut expected_block_index = 0_usize;
    let mut expected_offset = top_end;
    let mut previous_largest = None;
    for partition in partitions {
        if partition.data_block_count == 0 {
            return Err(invalid_table("empty index partition"));
        }
        if partition.first_data_block_index != expected_block_index {
            return Err(invalid_table("index partitions are not contiguous"));
        }
        let (partition_start, partition_end) = block_bounds(partition.block)?;
        if partition_start != expected_offset || partition_end > section_end {
            return Err(Error::Corruption {
                message: "index partition layout is inconsistent".to_owned(),
            });
        }
        if partition.smallest_internal_key > partition.largest_internal_key {
            return Err(invalid_table("index partition key bounds are inverted"));
        }
        if previous_largest
            .as_ref()
            .is_some_and(|previous| previous >= &partition.smallest_internal_key)
        {
            return Err(invalid_table("index partitions are not sorted"));
        }
        expected_block_index = expected_block_index
            .checked_add(partition.data_block_count)
            .ok_or_else(|| invalid_table("index partition data block count overflow"))?;
        expected_offset = partition_end;
        previous_largest = Some(partition.largest_internal_key.clone());
    }
    if expected_offset != section_end {
        return Err(Error::Corruption {
            message: "index partitions do not cover index section".to_owned(),
        });
    }
    Ok(expected_block_index)
}

fn validate_index_partition(
    partition: &IndexPartitionEntry,
    entries: &[DataBlockIndexEntry],
    data_blocks: SectionHandle,
) -> Result<()> {
    if entries.len() != partition.data_block_count {
        return Err(invalid_table("index partition entry count mismatch"));
    }
    let first = entries
        .first()
        .ok_or_else(|| invalid_table("empty index partition"))?;
    let last = entries
        .last()
        .ok_or_else(|| invalid_table("empty index partition"))?;
    if first.smallest_internal_key != partition.smallest_internal_key
        || last.largest_internal_key != partition.largest_internal_key
    {
        return Err(invalid_table("index partition key bounds mismatch"));
    }

    let (data_start, data_end) = section_bounds(data_blocks)?;
    let mut previous_end = None;
    let mut previous_largest = None;
    for entry in entries {
        let (block_start, block_end) = block_bounds(entry.block)?;
        if block_start < data_start || block_end > data_end {
            return Err(Error::Corruption {
                message: "data block index points outside data section".to_owned(),
            });
        }
        if previous_end.is_some_and(|end| end != block_start) {
            return Err(Error::Corruption {
                message: "data block index partition has a gap".to_owned(),
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
        previous_end = Some(block_end);
        previous_largest = Some(entry.largest_internal_key.clone());
    }
    Ok(())
}

fn validate_decoded_data_block_entry(
    entry: &DataBlockIndexEntry,
    block: &DecodedDataBlock,
) -> Result<()> {
    if block.record_count() == 0 {
        return Err(Error::Corruption {
            message: "data block index points to an empty block".to_owned(),
        });
    }
    let first = block.record_view(0)?;
    let last = block.record_view(block.record_count() - 1)?;
    if !record_view_matches_internal_key(first, &entry.smallest_internal_key)
        || !record_view_matches_internal_key(last, &entry.largest_internal_key)
    {
        return Err(Error::Corruption {
            message: "data block index key bounds do not match block records".to_owned(),
        });
    }

    validate_sorted_decoded_data_block(block)
}

#[cfg(test)]
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

fn validate_decoded_data_block_filters(
    entry: &DataBlockIndexEntry,
    block: &DecodedDataBlock,
) -> Result<()> {
    for record_index in 0..block.record_count() {
        let record = block.record_view(record_index)?;
        if entry
            .point_key_filter
            .as_ref()
            .is_some_and(|filter| !filter.may_contain_key(record.user_key))
        {
            return Err(Error::Corruption {
                message: "data block point-key filter misses a block key".to_owned(),
            });
        }

        if let Some(filter) = &entry.prefix_filter {
            if filter
                .extractor()
                .extract(record.user_key)
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

fn validate_sorted_decoded_data_block(block: &DecodedDataBlock) -> Result<()> {
    for record_index in 1..block.record_count() {
        let previous = block.record_view(record_index - 1)?;
        let current = block.record_view(record_index)?;
        if data_block_record_view_cmp(previous, current) != std::cmp::Ordering::Less {
            return Err(Error::Corruption {
                message: "table point records are not sorted by internal key".to_owned(),
            });
        }
    }

    Ok(())
}

fn data_block_record_view_cmp(
    left: DataBlockRecordView<'_>,
    right: DataBlockRecordView<'_>,
) -> std::cmp::Ordering {
    left.user_key
        .cmp(right.user_key)
        .then_with(|| right.sequence.cmp(&left.sequence))
        .then_with(|| right.batch_index.cmp(&left.batch_index))
        .then_with(|| left.kind.cmp(&right.kind))
}

fn record_view_matches_internal_key(record: DataBlockRecordView<'_>, key: &InternalKey) -> bool {
    record.user_key == key.user_key()
        && record.sequence == key.sequence()
        && record.kind == key.kind()
        && record.batch_index == key.batch_index()
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
    put_u32(
        bytes,
        usize_to_u32(
            properties.blob_references.len(),
            "properties blob reference count",
        )?,
    );
    for reference in &properties.blob_references {
        put_u64(bytes, reference.file_id);
        put_u64(bytes, reference.referenced_bytes);
        put_u64(bytes, reference.referenced_record_count);
        put_internal_key(bytes, &reference.smallest_internal_key)?;
        put_internal_key(bytes, &reference.largest_internal_key)?;
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
        Some(ValueRef::BlobIndex(index)) => {
            put_u8(bytes, VALUE_BLOB_INDEX);
            put_blob_index(bytes, *index);
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

fn put_blob_index(bytes: &mut Vec<u8>, index: BlobIndex) {
    put_u64(bytes, index.file_id);
    put_u64(bytes, index.offset);
    put_u64(bytes, index.encoded_len);
    put_u64(bytes, index.value_len);
    put_u32(bytes, index.value_checksum);
    put_u32(bytes, index.record_checksum);
    put_codec(bytes, index.compression);
}

fn put_codec(bytes: &mut Vec<u8>, codec: CodecId) {
    put_u8(bytes, codec.tag());
}

fn codec_from_tag(tag: u8) -> Result<CodecId> {
    CodecId::from_tag(tag)
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
        Some(ValueRef::BlobIndex(_)) => 1 + 8 + 8 + 8 + 8 + 4 + 4 + 1,
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

fn u32_to_usize(value: u32) -> usize {
    value as usize
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

fn user_key_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
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

    fn read_bytes_range(&mut self) -> Result<Range<u32>> {
        let len = self.read_u32()? as usize;
        let start = self.offset;
        let end = start
            .checked_add(len)
            .ok_or_else(|| invalid_table("byte field length overflows"))?;
        if self.payload.get(start..end).is_none() {
            return Err(invalid_table("short bytes"));
        }
        self.offset = end;
        Ok(usize_to_u32(start, "byte field start")?..usize_to_u32(end, "byte field end")?)
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
            blob_references: self.read_blob_references()?,
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

    fn read_blob_references(&mut self) -> Result<Vec<TableBlobReference>> {
        let reference_count = self.read_u32()? as usize;
        ensure_count_fits_remaining(
            reference_count,
            self.remaining_len(),
            8 + 8 + 8 + MIN_INTERNAL_KEY_BYTES * 2,
            "properties blob reference count exceeds block bytes",
        )?;
        let mut references = Vec::with_capacity(reference_count);
        let mut previous = None;
        for _ in 0..reference_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_table("properties blob references are not sorted"));
            }
            let referenced_bytes = self.read_u64()?;
            let referenced_record_count = self.read_u64()?;
            let smallest_internal_key = self.read_internal_key()?;
            let largest_internal_key = self.read_internal_key()?;
            if smallest_internal_key > largest_internal_key {
                return Err(invalid_table(
                    "properties blob reference key bounds are invalid",
                ));
            }
            references.push(TableBlobReference {
                file_id,
                referenced_bytes,
                referenced_record_count,
                smallest_internal_key,
                largest_internal_key,
            });
            previous = Some(file_id);
        }
        Ok(references)
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
    use crate::options::BucketOptions;

    #[test]
    fn list_table_file_ids_reads_backend_object_listing() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-list-table-ids-{}-{}",
            std::process::id(),
            table_time_suffix()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        std::fs::write(table_path(&root, TableId(2)), b"table").expect("first table file writes");
        std::fs::write(table_path(&root, TableId(4)), b"table").expect("second table file writes");
        std::fs::write(root.join("MANIFEST"), b"manifest").expect("manifest writes");
        std::fs::write(root.join("table-00000000000000000005.tmp"), b"tmp")
            .expect("tmp file writes");
        std::fs::create_dir(table_path(&root, TableId(6))).expect("table-shaped dir creates");

        let ids = list_table_file_ids(&root).expect("table ids list");
        assert_eq!(
            ids.iter().copied().collect::<Vec<_>>(),
            vec![TableId(2), TableId(4)]
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn write_table_creates_final_object_without_leftover_tmp() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-write-table-object-{}-{}",
            std::process::id(),
            table_time_suffix()
        ));
        let table_id = TableId(9);
        let path = table_path(&root, table_id);
        let records = vec![(
            InternalKey::new(b"key".to_vec(), Sequence::new(1), ValueKind::Put, 0),
            Some(ValueRef::Inline(b"value".to_vec())),
        )];

        let table = write_table(
            &path,
            table_id,
            TableLevel::ZERO,
            &test_table_options(CodecId::None, false),
            &records,
            &[],
        )
        .expect("table writes");

        assert_eq!(table.properties().id, table_id);
        assert!(path.exists(), "final table object should exist");
        assert!(
            !path.with_extension("tmp").exists(),
            "successful table write should leave no temporary file"
        );

        std::fs::remove_dir_all(root).expect("test dir removes");
    }

    #[test]
    fn checked_block_index_round_trips_multiple_data_blocks() {
        let table = table_with_records(160, CodecId::None);
        let payload = encode_table(&table).expect("table encodes");
        let footer = read_footer(&payload).expect("footer reads");
        let index_entries = decode_test_index_entries(&payload, &footer, table.properties.codec)
            .expect("index entries decode");
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
            loaded_data_blocks(&table).len() > 1,
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
        let records = vec![
            test_point_record(b"a", 10, b"a1"),
            test_point_record(b"target", 9, b"newer"),
            test_point_record(b"target", 7, b"older"),
            test_point_record(b"z", 1, b"z1"),
        ];
        let encoded = encode_data_block(&records).expect("data block encodes");
        let block = decode_data_block(encoded).expect("data block decodes");
        assert_eq!(
            data_block_point_record_range_for_key(&block, b"target", IndexSearchPolicy::Binary)
                .expect("hash lookup succeeds"),
            1..3
        );

        let records =
            data_block_point_records_for_key(&block, b"target", IndexSearchPolicy::Binary)
                .expect("target records decode");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].internal_key.sequence(), Sequence::new(9));
        assert_eq!(records[1].internal_key.sequence(), Sequence::new(7));

        let visible = data_block_newest_visible_point_record_for_key(
            &block,
            b"target",
            Sequence::new(8),
            IndexSearchPolicy::Binary,
        )
        .expect("visible lookup decodes")
        .1
        .expect("older target version is visible");
        assert_eq!(visible.internal_key.sequence(), Sequence::new(7));

        assert!(
            data_block_point_records_for_key(&block, b"missing", IndexSearchPolicy::Binary)
                .expect("missing lookup decodes")
                .is_empty()
        );
    }

    #[test]
    fn data_block_keeps_compact_bytes_and_record_views() {
        let records = vec![
            test_point_record(b"a", 3, b"alpha"),
            test_point_record(b"b", 2, b"bravo"),
        ];
        let encoded = encode_data_block(&records).expect("data block encodes");
        let block = decode_data_block(encoded).expect("data block decodes");

        assert_eq!(block.record_count(), 2);
        assert_eq!(block.record_headers.len(), 2);
        assert!(!block.bytes.is_empty());

        let record = block.record_view(1).expect("record view decodes");
        assert_eq!(record.user_key, b"b");
        assert_eq!(record.sequence, Sequence::new(2));
        match record.value {
            Some(ValueRefView::Inline(bytes)) => assert_eq!(bytes, b"bravo"),
            other => panic!("expected inline value view, got {other:?}"),
        }
    }

    #[test]
    fn full_hash_index_validation_is_explicit() {
        let records = vec![
            test_point_record(b"a", 2, b"alpha"),
            test_point_record(b"b", 1, b"bravo"),
        ];
        let mut encoded = encode_data_block(&records).expect("data block encodes");
        let hash_section = encoded
            .len()
            .checked_sub(4 + 2 * MIN_DATA_BLOCK_HASH_ENTRY_BYTES)
            .expect("test block has two hash entries");
        encoded[hash_section..hash_section + 4].copy_from_slice(&1_u32.to_le_bytes());
        encoded.truncate(encoded.len() - MIN_DATA_BLOCK_HASH_ENTRY_BYTES);

        let block = decode_data_block(encoded.clone()).expect("normal read decode is lightweight");
        assert!(
            data_block_point_records_for_key(&block, b"b", IndexSearchPolicy::Binary)
                .expect("hash lookup runs")
                .is_empty(),
            "lightweight decode trusts the encoded lookup index after structural checks"
        );
        let error = decode_data_block_for_verify(encoded)
            .expect_err("full verification should rebuild and compare the index");
        assert_invalid_table_message(&error, "hash index does not match records");
    }

    #[test]
    fn deeper_table_uses_block_cache_for_lazy_index_partitions() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-lazy-index-partitions-{}-{}.trinet",
            std::process::id(),
            table_time_suffix()
        ));
        let mut options = test_table_options(CodecId::None, true);
        options.block_bytes = 1;
        let point_records = (0..260)
            .map(|index| {
                (
                    InternalKey::new(
                        format!("key-{index:03}").into_bytes(),
                        Sequence::new(u64::try_from(index + 1).expect("test sequence fits u64")),
                        ValueKind::Put,
                        0,
                    ),
                    Some(ValueRef::Inline(format!("value-{index:03}").into_bytes())),
                )
            })
            .collect::<Vec<_>>();

        let table = write_table(
            &path,
            TableId(999),
            TableLevel(2),
            &options,
            &point_records,
            &[],
        )
        .expect("table writes and reopens");
        assert!(table.data_blocks.is_none());
        assert!(table.index_partitions.len() > 1);
        assert!(
            table
                .index_partition_cache
                .read()
                .expect("cache lock reads")
                .is_empty()
        );
        let block_cache = BlockCache::new(1024 * 1024);

        let records = table
            .point_records_for_key_with_cache(
                b"key-250",
                IndexSearchPolicy::Binary,
                Some(&block_cache),
            )
            .expect("point lookup loads one index partition");
        assert_eq!(records.len(), 1);
        assert!(
            table
                .index_partition_cache
                .read()
                .expect("cache lock reads")
                .is_empty()
        );
        let stats_after_first = block_cache.stats();

        let records = table
            .point_records_for_key_with_cache(
                b"key-250",
                IndexSearchPolicy::Binary,
                Some(&block_cache),
            )
            .expect("point lookup reuses cached metadata and data block");
        assert_eq!(records.len(), 1);
        let stats_after_second = block_cache.stats();
        assert!(
            stats_after_second.hits > stats_after_first.hits,
            "global block cache should serve the lazy index partition and data block"
        );
        assert!(
            table
                .index_partition_cache
                .read()
                .expect("cache lock reads")
                .is_empty(),
            "deeper levels keep table-local partition metadata lazy"
        );

        std::fs::remove_file(path).expect("test table file removes");
    }

    #[test]
    fn point_lookup_enters_partition_at_binary_located_block() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-partition-binary-seek-{}-{}.trinet",
            std::process::id(),
            table_time_suffix()
        ));
        let mut options = test_table_options(CodecId::None, true);
        options.block_bytes = 1;
        let point_records = (0..260)
            .map(|index| {
                (
                    InternalKey::new(
                        format!("key-{index:03}").into_bytes(),
                        Sequence::new(u64::try_from(index + 1).expect("test sequence fits u64")),
                        ValueKind::Put,
                        0,
                    ),
                    Some(ValueRef::Inline(format!("value-{index:03}").into_bytes())),
                )
            })
            .collect::<Vec<_>>();

        let table = write_table(
            &path,
            TableId(1_001),
            TableLevel::ZERO,
            &options,
            &point_records,
            &[],
        )
        .expect("table writes and reopens");

        let record = table
            .newest_visible_point_record_for_key_with_cache(
                b"key-250",
                Sequence::new(u64::MAX),
                IndexSearchPolicy::Binary,
                None,
            )
            .expect("point lookup succeeds")
            .expect("record exists");
        assert_eq!(record.internal_key.user_key(), b"key-250");
        let stats = table.read_path_stats();
        assert_eq!(
            stats.point_block_metadata_probes, 1,
            "point lookup should inspect only the binary-located block metadata"
        );
        assert_eq!(stats.point_data_block_reads, 1);

        std::fs::remove_file(path).expect("test table file removes");
    }

    #[test]
    fn l0_l1_tables_pin_filters_and_index_partitions() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-hot-table-metadata-{}-{}.trinet",
            std::process::id(),
            table_time_suffix()
        ));
        let mut options = test_table_options(CodecId::None, true);
        options.block_bytes = 1;
        let point_records = (0..32)
            .map(|index| {
                (
                    InternalKey::new(
                        format!("key-{index:03}").into_bytes(),
                        Sequence::new(u64::try_from(index + 1).expect("test sequence fits u64")),
                        ValueKind::Put,
                        0,
                    ),
                    Some(ValueRef::Inline(format!("value-{index:03}").into_bytes())),
                )
            })
            .collect::<Vec<_>>();

        let table = write_table(
            &path,
            TableId(1_002),
            TableLevel(1),
            &options,
            &point_records,
            &[],
        )
        .expect("table writes and reopens");
        let filter = table
            .point_key_filter
            .as_ref()
            .expect("L1 filter is pinned");
        assert_eq!(
            table
                .index_partition_cache
                .read()
                .expect("cache lock reads")
                .len(),
            table.index_partitions.len(),
            "L1 index partitions are pinned"
        );
        let missing = bounded_point_filter_miss(filter, b"key-000", b"key-031");

        assert!(!table.may_contain_key(&missing));
        let stats = table.read_path_stats();
        assert_eq!(stats.point_filter_misses, 1);
        assert_eq!(
            stats.point_data_block_reads, 0,
            "table-level filter miss should not read a data block"
        );

        std::fs::remove_file(path).expect("test table file removes");
    }

    #[test]
    fn block_read_uses_cached_file_handle() {
        let mut payload = Vec::new();
        let block = BlockManager::append_checked(&mut payload, CodecId::None, b"cached block")
            .expect("checked block appends");
        let path = std::env::temp_dir().join(format!(
            "trine-kv-cached-table-handle-{}-{}.trinet",
            std::process::id(),
            table_time_suffix()
        ));
        let mut file_bytes = vec![0_u8; HEADER_LEN];
        file_bytes.extend_from_slice(&payload);
        std::fs::write(&path, file_bytes).expect("test table file writes");
        let table_file = table_storage_backend()
            .open_read_blocking(table_storage_object(&path))
            .expect("test table file opens");
        let missing_path = path.with_extension("missing");

        let (codec, decoded) =
            read_checked_block_from_file(&missing_path, Some(&table_file), payload.len(), block)
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
            loaded_data_blocks(&small_table).len() > loaded_data_blocks(&large_table).len(),
            "smaller configured blocks should split records into more data blocks"
        );
    }

    #[test]
    fn partitioned_filters_round_trip_through_index_entries() {
        let table = table_with_filters(160, CodecId::None);
        let payload = encode_table(&table).expect("table encodes");
        let footer = read_footer(&payload).expect("footer reads");
        let index_entries = decode_test_index_entries(&payload, &footer, table.properties.codec)
            .expect("index entries decode");
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
        let block = loaded_data_blocks(&table)
            .first()
            .expect("test table has a block");
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
        let block = loaded_data_blocks(&table)
            .first()
            .expect("test table has a block");
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
        let error = decode_data_block(count_block(u32::MAX))
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

        let error = decode_data_block(bytes).expect_err("impossible restart count should fail");
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
        let data_block_count = data_blocks.len();
        let index_partitions = index_partitions_for_loaded_blocks(&data_blocks);
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
            read_path_stats: Arc::new(TableReadPathStats::default()),
            point_records: Some(point_records),
            data_blocks: Some(data_blocks),
            data_block_count,
            index_partitions,
            index_partition_cache: Arc::new(RwLock::new(BTreeMap::new())),
            range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
                Vec::new(),
            ))))),
            may_have_range_tombstones: false,
        }
    }

    fn loaded_data_blocks(table: &Table) -> &[TableDataBlock] {
        table
            .data_blocks
            .as_deref()
            .expect("test table keeps loaded block metadata")
    }

    fn decode_test_index_entries(
        payload: &[u8],
        footer: &TableFooter,
        codec: CodecId,
    ) -> Result<Vec<DataBlockIndexEntry>> {
        let (top_level_block, top_codec, top_payload) =
            read_first_block_in_section(payload, footer.indexes)?;
        validate_index_top_level_codec(top_codec, codec)?;
        let partitions = decode_index_top_level(&top_payload)?;
        validate_index_top_level(top_level_block, &partitions, footer.indexes)?;
        let mut entries = Vec::new();
        for partition in partitions {
            let (partition_codec, partition_payload) =
                read_checked_block(payload, partition.block)?;
            validate_block_codec(partition_codec, codec, TableSection::Indexes)?;
            let partition_entries = decode_index_block(&partition_payload)?;
            validate_index_partition(&partition, &partition_entries, footer.data_blocks)?;
            entries.extend(partition_entries);
        }
        Ok(entries)
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
            blob_threshold_bytes: BucketOptions::DEFAULT_BLOB_THRESHOLD_BYTES,
            rewrite_blob_indexes: false,
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

    fn bounded_point_filter_miss(
        filter: &PointKeyFilter,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<u8> {
        for index in 0..10_000 {
            let key = format!("key-{:03}!{index:04}", index % 31).into_bytes();
            if smallest <= key.as_slice()
                && key.as_slice() <= largest
                && !filter.may_contain_key(&key)
            {
                return key;
            }
        }
        panic!("test filter should have at least one bounded definite miss");
    }
}
