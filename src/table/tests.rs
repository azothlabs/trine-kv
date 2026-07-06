use super::*;
use crate::filter::PointKeyFilter;
use crate::options::BucketOptions;
use crate::storage::StorageReadBuffer;
use crate::table::format::put_u32;
use crate::table::format::read_checked_block;
use crate::table::format::read_first_block_in_section;

struct OwnedOnlySource {
    bytes: Vec<u8>,
}

impl BlockReadSource for OwnedOnlySource {
    fn read_exact_at(&self, _offset: usize, _bytes: &mut [u8]) -> Result<()> {
        panic!("table open metadata should use owned reads")
    }

    fn read_exact_at_owned(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        let end = limits::checked_add_invalid_format(offset, len, "test read offset")?;
        let bytes = self
            .bytes
            .get(offset..end)
            .ok_or_else(|| invalid_table("owned read outside source"))?
            .to_vec();
        Ok(StorageReadBuffer::from_vec(offset, bytes))
    }
}

mod decode_hardening;
mod filters_blocks;
mod write_open;

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
    let data_blocks =
        build_data_blocks(&point_records, options, TableLevel::ZERO).expect("test blocks build");
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
        point_key_filter: build_point_key_filter(options, &point_records, TableLevel::ZERO),
        prefix_filter: build_prefix_filter(options, &point_records, TableLevel::ZERO),
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
        let (partition_codec, partition_payload) = read_checked_block(payload, partition.block)?;
        validate_block_codec(partition_codec, codec, TableSection::Indexes)?;
        let partition_entries = decode_index_block(&partition_payload)?;
        validate_index_partition(&partition, &partition_entries, footer.data_blocks)?;
        entries.extend(partition_entries);
    }
    Ok(entries)
}

fn test_point_record(key: &[u8], sequence: u64, value: &[u8]) -> TablePointRecord {
    TablePointRecord {
        internal_key: InternalKey::new(key.to_vec(), Sequence::new(sequence), ValueKind::Put, 0),
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
        filter_depth_curve: FilterDepthCurve::Auto,
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

fn poll_ready<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(result) => result,
        std::task::Poll::Pending => panic!("table storage future unexpectedly pending"),
    }
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

fn bounded_point_filter_miss(filter: &PointKeyFilter, smallest: &[u8], largest: &[u8]) -> Vec<u8> {
    for index in 0..10_000 {
        let key = format!("key-{:03}!{index:04}", index % 31).into_bytes();
        if smallest <= key.as_slice() && key.as_slice() <= largest && !filter.may_contain_key(&key)
        {
            return key;
        }
    }
    panic!("test filter should have at least one bounded definite miss");
}
