use bytes::Bytes;
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
    block::{BlockHandle, BlockManager, BlockReadSource, DecodedBlock, block_bounds, checksum},
    cache::{BlockCache, BlockCacheKey, CacheKind},
    codec::CodecId,
    error::{Error, Result},
    filter::{PointKeyFilter, PrefixFilter},
    internal_key::{InternalKey, ValueKind},
    iterator::{
        Direction, ForwardKeyState, RecordGroup, ReverseKeyState, ScanRecord, ScanSelector,
        prefix_successor, record_group_from_first_and_rest,
    },
    limits,
    options::{
        DurabilityMode, FilterDepthCurve, FilterPolicy, IndexSearchPolicy, PrefixFilterPolicy,
    },
    point_value::PointValueSource,
    prefix::PrefixExtractor,
    range_tombstone::{self, RangeTombstoneIndex, RangeTombstoneLike},
    search,
    stats::{FilterStats, ReadPathStats},
    storage::{
        BlockingStorageObjectListBackend, BlockingStorageObjectWriteBackend,
        BlockingStorageReadBackend, BlockingStorageReadObject, MemoryStorageBackend,
        NativeFileBackend, NativeFileObject, NativeFileReadSource, StorageCapability,
        StorageObjectId, StorageObjectKind, StorageObjectListBackend, StorageObjectListRequest,
        StorageObjectWriteBackend, StorageReadBackend, StorageReadBuffer, StorageReadObject,
        StorageReadSource,
    },
    types::{KeyRange, Sequence},
};

pub const TABLE_FILE_EXTENSION: &str = "trinet";
const TABLE_MAGIC: u32 = 0x5452_5442;
// Version 7 removes raw blob references; every external value is bound to its
// blob header, full record metadata and internal key through `BlobIndex`.
const TABLE_VERSION: u16 = 7;
const HEADER_LEN: usize = 14;
const FOOTER_MAGIC: u32 = 0x5452_5446;
const FOOTER_LEN: usize = 90;
const DATA_BLOCK_RESTART_INTERVAL: usize = 16;
const INDEX_PARTITION_TARGET_ENTRIES: usize = 128;
const PINNED_READ_METADATA_MAX_LEVEL: u32 = 1;
const WHOLE_TABLE_SYNC_OPEN_MAX_BYTES: u64 = 256 * 1024;

const VALUE_KIND_PUT: u8 = 1;
const VALUE_KIND_POINT_DELETE: u8 = 2;
const VALUE_KIND_RANGE_DELETE: u8 = 3;

const VALUE_NONE: u8 = 0;
const VALUE_INLINE: u8 = 1;
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
const INLINE_VALUE_HEADER_BYTES: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

impl TableId {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
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
    pub(crate) filter_depth_curve: FilterDepthCurve,
    pub(crate) blob_threshold_bytes: usize,
    pub(crate) rewrite_blob_indexes: bool,
}

fn sort_point_records_if_needed(point_records: &mut [(InternalKey, Option<ValueRef>)]) {
    if point_records.windows(2).all(|pair| pair[0].0 <= pair[1].0) {
        return;
    }
    point_records.sort_by(|left, right| left.0.cmp(&right.0));
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

struct EncodedTable {
    payload: Vec<u8>,
    payload_len: usize,
    footer: TableFooter,
    data_block_count: usize,
    index_partitions: Vec<IndexPartitionEntry>,
    pinned_index_partitions: BTreeMap<usize, Arc<Vec<TableDataBlock>>>,
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
    bytes: Bytes,
    payload_range: Range<usize>,
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
        let payload = self.payload_bytes();
        self.record_headers
            .get(index)
            .ok_or_else(|| invalid_table("record index outside data block"))?
            .view(payload)
    }

    fn record_owned(&self, index: usize) -> Result<TablePointRecord> {
        self.record_view(index).map(DataBlockRecordView::to_owned)
    }

    fn point_value_record(&self, index: usize) -> Result<TablePointValueRecord> {
        let header = self
            .record_headers
            .get(index)
            .ok_or_else(|| invalid_table("record index outside data block"))?;
        let payload = self.payload_bytes();
        let record = header.view(payload)?;
        let value = match header.value {
            Some(ValueRefHeader::Inline { offset, len }) => {
                let range = inline_value_range(offset, len, payload.len())?;
                Some(PointValueSource::from_shared(
                    self.bytes.clone(),
                    self.payload_absolute_range(range)?,
                )?)
            }
            Some(value) => Some(PointValueSource::from_value_ref(
                value.view(payload)?.to_owned(),
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

    fn restart_indices_with_base(&self, base: usize) -> Vec<usize> {
        self.restart_indices
            .iter()
            .map(|index| base.saturating_add(u32_to_usize(*index)))
            .collect()
    }

    fn payload_bytes(&self) -> &[u8] {
        &self.bytes[self.payload_range.clone()]
    }

    fn payload_absolute_range(&self, range: Range<usize>) -> Result<Range<usize>> {
        let start = self
            .payload_range
            .start
            .checked_add(range.start)
            .ok_or_else(|| invalid_table("data block payload range overflow"))?;
        let end = self
            .payload_range
            .start
            .checked_add(range.end)
            .ok_or_else(|| invalid_table("data block payload range overflow"))?;
        if end > self.payload_range.end {
            return Err(invalid_table(
                "data block payload range outside shared bytes",
            ));
        }
        Ok(start..end)
    }

    #[cfg(test)]
    pub(crate) fn empty_for_cache_test() -> Self {
        Self {
            bytes: Bytes::new(),
            payload_range: 0..0,
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
    Inline { offset: u32, len: u32 },
    BlobIndex(BlobIndex),
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
}

impl ValueRefView<'_> {
    fn to_owned(self) -> ValueRef {
        match self {
            Self::Inline(bytes) => ValueRef::Inline(bytes.to_vec()),
            Self::BlobIndex(index) => ValueRef::BlobIndex(index),
        }
    }
}

struct BufferedBlockReadSource<'src> {
    bytes: &'src [u8],
}

impl BlockReadSource for BufferedBlockReadSource<'_> {
    fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| invalid_table("read offset overflow"))?;
        let source = self
            .bytes
            .get(offset..end)
            .ok_or_else(|| invalid_table("read past end"))?;
        bytes.copy_from_slice(source);
        Ok(())
    }

    fn read_exact_at_owned(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| invalid_table("read offset overflow"))?;
        let bytes = self
            .bytes
            .get(offset..end)
            .ok_or_else(|| invalid_table("read past end"))?
            .to_vec();
        Ok(StorageReadBuffer::from_vec(offset, bytes))
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
    point_table_probes: AtomicU64,
    point_l0_table_probes: AtomicU64,
    point_non_l0_table_probes: AtomicU64,
    point_index_partition_probes: AtomicU64,
    point_block_metadata_probes: AtomicU64,
    point_data_block_reads: AtomicU64,
    point_filter_misses: AtomicU64,
    range_table_probes: AtomicU64,
    range_l0_table_probes: AtomicU64,
    range_non_l0_table_probes: AtomicU64,
    range_tombstone_table_probes: AtomicU64,
    prefix_table_probes: AtomicU64,
    prefix_tombstone_table_probes: AtomicU64,
    prefix_block_metadata_probes: AtomicU64,
    prefix_data_block_reads: AtomicU64,
    prefix_filter_misses: AtomicU64,
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
                .saturating_add(shard.point_table_probes.load(Ordering::Acquire));
            stats.point_l0_table_probes = stats
                .point_l0_table_probes
                .saturating_add(shard.point_l0_table_probes.load(Ordering::Acquire));
            stats.point_non_l0_table_probes = stats
                .point_non_l0_table_probes
                .saturating_add(shard.point_non_l0_table_probes.load(Ordering::Acquire));
            stats.point_index_partition_probes = stats
                .point_index_partition_probes
                .saturating_add(shard.point_index_partition_probes.load(Ordering::Acquire));
            stats.point_block_metadata_probes = stats
                .point_block_metadata_probes
                .saturating_add(shard.point_block_metadata_probes.load(Ordering::Acquire));
            stats.point_data_block_reads = stats
                .point_data_block_reads
                .saturating_add(shard.point_data_block_reads.load(Ordering::Acquire));
            stats.point_filter_misses = stats
                .point_filter_misses
                .saturating_add(shard.point_filter_misses.load(Ordering::Acquire));
            stats.range_table_probes = stats
                .range_table_probes
                .saturating_add(shard.range_table_probes.load(Ordering::Acquire));
            stats.range_l0_table_probes = stats
                .range_l0_table_probes
                .saturating_add(shard.range_l0_table_probes.load(Ordering::Acquire));
            stats.range_non_l0_table_probes = stats
                .range_non_l0_table_probes
                .saturating_add(shard.range_non_l0_table_probes.load(Ordering::Acquire));
            stats.range_tombstone_table_probes = stats
                .range_tombstone_table_probes
                .saturating_add(shard.range_tombstone_table_probes.load(Ordering::Acquire));
            stats.prefix_table_probes = stats
                .prefix_table_probes
                .saturating_add(shard.prefix_table_probes.load(Ordering::Acquire));
            stats.prefix_tombstone_table_probes = stats
                .prefix_tombstone_table_probes
                .saturating_add(shard.prefix_tombstone_table_probes.load(Ordering::Acquire));
            stats.prefix_block_metadata_probes = stats
                .prefix_block_metadata_probes
                .saturating_add(shard.prefix_block_metadata_probes.load(Ordering::Acquire));
            stats.prefix_data_block_reads = stats
                .prefix_data_block_reads
                .saturating_add(shard.prefix_data_block_reads.load(Ordering::Acquire));
            stats.prefix_filter_misses = stats
                .prefix_filter_misses
                .saturating_add(shard.prefix_filter_misses.load(Ordering::Acquire));
        }
        stats
    }

    fn record_point_table_probe(&self, level: TableLevel) {
        let shard = self.shard();
        shard.point_table_probes.fetch_add(1, Ordering::Relaxed);
        if level == TableLevel::ZERO {
            shard.point_l0_table_probes.fetch_add(1, Ordering::Relaxed);
        } else {
            shard
                .point_non_l0_table_probes
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_point_index_partition_probe(&self) {
        self.shard()
            .point_index_partition_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_point_block_metadata_probe(&self) {
        self.shard()
            .point_block_metadata_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_point_data_block_read(&self) {
        self.shard()
            .point_data_block_reads
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_point_filter_miss(&self) {
        self.shard()
            .point_filter_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_range_table_probe(&self, level: TableLevel) {
        let shard = self.shard();
        shard.range_table_probes.fetch_add(1, Ordering::Relaxed);
        if level == TableLevel::ZERO {
            shard.range_l0_table_probes.fetch_add(1, Ordering::Relaxed);
        } else {
            shard
                .range_non_l0_table_probes
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_range_tombstone_table_probe(&self) {
        self.shard()
            .range_tombstone_table_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_prefix_table_probe(&self) {
        self.shard()
            .prefix_table_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_prefix_tombstone_table_probe(&self) {
        self.shard()
            .prefix_tombstone_table_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_prefix_block_metadata_probe(&self) {
        self.shard()
            .prefix_block_metadata_probes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_prefix_data_block_read(&self) {
        self.shard()
            .prefix_data_block_reads
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_prefix_filter_miss(&self) {
        self.shard()
            .prefix_filter_misses
            .fetch_add(1, Ordering::Relaxed);
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

mod block_access;
mod cursor;
mod format;
mod metadata;
mod read;

pub(crate) use cursor::*;
use format::{
    decode_data_block, decode_filter_block, decode_index_block, decode_index_top_level,
    decode_properties_block, decode_range_tombstone_block, decode_table_bytes, empty_footer,
    encode_data_block, encode_table_for_write, invalid_table, key_is_after_end,
    key_is_before_start, point_record_encoded_len, read_checked_block_from_source_shared,
    read_checked_block_from_storage_object_shared_async, read_data_block_from_file,
    read_data_block_from_file_async, read_data_block_from_source,
    read_first_block_in_section_from_source_shared, read_footer_from_source,
    read_single_block_section_from_file_shared, read_single_block_section_from_file_shared_async,
    read_single_block_section_from_source_shared, read_u16_at, read_u32_at, u32_to_usize,
    user_key_hash, usize_to_u32, usize_to_u64, usize_to_u64_saturating, validate_block_codec,
    validate_footer_sections_by_len, validate_index_partition, validate_index_top_level,
    validate_index_top_level_codec, validate_sorted_point_records, validate_table_filters_for_key,
};
pub(crate) use metadata::*;

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_decode_table(bytes: &[u8]) {
    let _ = decode_table_bytes(bytes);
}

#[cfg(test)]
mod tests;
