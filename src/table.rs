use std::{
    fs::{self, File},
    io::{Read, Write},
    ops::Bound,
    path::{Path, PathBuf},
};

use crate::{
    blob::ValueRef,
    codec::{self, CodecId},
    error::{Error, Result},
    filter::PrefixFilter,
    internal_key::{InternalKey, ValueKind},
    options::PrefixFilterPolicy,
    prefix::PrefixExtractor,
    types::{KeyRange, Sequence},
};

pub const TABLE_FILE_EXTENSION: &str = "trinet";
const TABLE_MAGIC: u32 = 0x5452_5442;
const TABLE_VERSION: u16 = 1;
const HEADER_LEN: usize = 14;
const FOOTER_MAGIC: u32 = 0x5452_5446;
const FOOTER_LEN: usize = 90;
const BLOCK_HEADER_LEN: usize = 13;
const DATA_BLOCK_TARGET_BYTES: usize = 1024;
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

const PREFIX_EXTRACTOR_DISABLED: u8 = 0;
const PREFIX_EXTRACTOR_FIXED_LEN: u8 = 1;
const PREFIX_EXTRACTOR_SEPARATOR: u8 = 2;
const PREFIX_EXTRACTOR_CUSTOM: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub smallest_user_key: Vec<u8>,
    pub largest_user_key: Vec<u8>,
    pub smallest_sequence: Sequence,
    pub largest_sequence: Sequence,
    pub codec: CodecId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableWriteOptions {
    pub(crate) codec: CodecId,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRangeTombstone {
    pub(crate) range: KeyRange,
    pub(crate) sequence: Sequence,
    pub(crate) batch_index: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct Table {
    properties: TableProperties,
    point_records: Vec<TablePointRecord>,
    range_tombstones: Vec<TableRangeTombstone>,
    prefix_filter: Option<PrefixFilter>,
}

impl Table {
    #[must_use]
    pub(crate) const fn properties(&self) -> &TableProperties {
        &self.properties
    }

    #[must_use]
    pub(crate) fn point_records(&self) -> &[TablePointRecord] {
        &self.point_records
    }

    #[must_use]
    pub(crate) fn range_tombstones(&self) -> &[TableRangeTombstone] {
        &self.range_tombstones
    }

    #[must_use]
    pub(crate) fn may_contain_prefix(&self, prefix: &[u8], extractor: &PrefixExtractor) -> bool {
        self.prefix_filter
            .as_ref()
            .is_none_or(|filter| filter.may_contain_query_prefix(prefix, extractor))
    }
}

#[must_use]
pub fn table_path(db_path: &Path, table_id: TableId) -> PathBuf {
    db_path.join(format!(
        "table-{id:020}.{TABLE_FILE_EXTENSION}",
        id = table_id.get()
    ))
}

pub(crate) fn write_table(
    path: &Path,
    table_id: TableId,
    options: &TableWriteOptions,
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[TableRangeTombstone],
) -> Result<Table> {
    if point_records.is_empty() && range_tombstones.is_empty() {
        return Err(Error::invalid_options("cannot write an empty table"));
    }

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

    let table = Table {
        properties: table_properties(table_id, options.codec, &point_records, range_tombstones),
        prefix_filter: build_prefix_filter(options, &point_records),
        point_records,
        range_tombstones: range_tombstones.to_vec(),
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

    Ok(table)
}

pub(crate) fn read_table(path: &Path) -> Result<Table> {
    let mut bytes = Vec::new();
    let mut file = File::open(path).map_err(|error| Error::Corruption {
        message: format!(
            "referenced table {} cannot be opened: {error}",
            path.display()
        ),
    })?;
    file.read_to_end(&mut bytes)
        .map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} cannot be read: {error}",
                path.display()
            ),
        })?;
    decode_table(&bytes)
}

fn table_properties(
    table_id: TableId,
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

    TableProperties {
        id: table_id,
        smallest_user_key: point_records
            .first()
            .map_or_else(Vec::new, |record| record.internal_key.user_key().to_vec()),
        largest_user_key: point_records
            .last()
            .map_or_else(Vec::new, |record| record.internal_key.user_key().to_vec()),
        smallest_sequence: smallest_sequence.unwrap_or(Sequence::ZERO),
        largest_sequence: largest_sequence.unwrap_or(Sequence::ZERO),
        codec,
    }
}

fn build_prefix_filter(
    options: &TableWriteOptions,
    point_records: &[TablePointRecord],
) -> Option<PrefixFilter> {
    if matches!(options.prefix_filter_policy, PrefixFilterPolicy::Disabled) {
        return None;
    }

    PrefixFilter::from_keys(
        options.prefix_extractor.clone(),
        point_records
            .iter()
            .map(|record| record.internal_key.user_key()),
    )
}

fn encode_table(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let codec = table.properties.codec;
    let (data_blocks, index_entries) = append_data_blocks(&mut bytes, codec, &table.point_records)?;
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

    let mut point_records = Vec::new();
    for entry in &index_entries {
        let (block_codec, block_payload) = read_checked_block(payload, entry.block)?;
        validate_block_codec(block_codec, properties.codec, TableSection::DataBlocks)?;
        let block_records = decode_data_block(&block_payload)?;
        validate_data_block_entry(entry, &block_records)?;
        point_records.extend(block_records);
    }
    validate_sorted_point_records(&point_records)?;

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
    let prefix_filter = decode_filter_block(&filter_payload)?;
    if properties
        != table_properties(
            properties.id,
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
        properties,
        point_records,
        range_tombstones,
        prefix_filter,
    })
}

fn append_data_blocks(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    point_records: &[TablePointRecord],
) -> Result<(SectionHandle, Vec<DataBlockIndexEntry>)> {
    let section_start = bytes.len();
    let mut index_entries = Vec::new();
    let mut block_start = 0;

    while block_start < point_records.len() {
        let mut block_end = block_start;
        let mut estimated_len = 0_usize;
        while block_end < point_records.len() {
            let next_len = point_record_encoded_len(&point_records[block_end]);
            if block_end > block_start && estimated_len + next_len > DATA_BLOCK_TARGET_BYTES {
                break;
            }
            estimated_len += next_len;
            block_end += 1;
        }

        let records = &point_records[block_start..block_end];
        let block_payload = encode_data_block(records)?;
        let block = append_checked_block(bytes, codec, &block_payload)?;
        index_entries.push(DataBlockIndexEntry {
            smallest_internal_key: records
                .first()
                .expect("data block has at least one record")
                .internal_key
                .clone(),
            largest_internal_key: records
                .last()
                .expect("data block has at least one record")
                .internal_key
                .clone(),
            block,
        });
        block_start = block_end;
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
    let mut bytes = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(
            table.range_tombstones.len(),
            "range tombstone block record count",
        )?,
    );
    for tombstone in &table.range_tombstones {
        put_bound(&mut bytes, &tombstone.range.start)?;
        put_bound(&mut bytes, &tombstone.range.end)?;
        put_u64(&mut bytes, tombstone.sequence.get());
        put_u32(&mut bytes, tombstone.batch_index);
    }
    Ok(bytes)
}

fn encode_filter_block(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match &table.prefix_filter {
        None => put_u8(&mut bytes, PREFIX_FILTER_ABSENT),
        Some(filter) => {
            put_u8(&mut bytes, PREFIX_FILTER_PRESENT);
            put_prefix_extractor(&mut bytes, filter.extractor())?;
            put_u32(
                &mut bytes,
                usize_to_u32(filter.prefixes().len(), "prefix filter entry count")?,
            );
            for prefix in filter.prefixes() {
                put_bytes(&mut bytes, prefix)?;
            }
        }
    }
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
    }
    Ok(bytes)
}

fn encode_properties_block(properties: &TableProperties) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_properties(&mut bytes, properties)?;
    Ok(bytes)
}

fn read_footer(payload: &[u8]) -> Result<TableFooter> {
    if payload.len() < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let footer_start = payload.len() - FOOTER_LEN;
    let footer = &payload[footer_start..];
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

fn validate_footer_sections(payload: &[u8], footer: &TableFooter) -> Result<()> {
    let footer_start = payload.len() - FOOTER_LEN;
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

fn read_checked_block(payload: &[u8], block: BlockHandle) -> Result<(CodecId, Vec<u8>)> {
    let (start, end) = block_bounds(block)?;
    let block_bytes = payload
        .get(start..end)
        .ok_or_else(|| invalid_table("block outside table payload"))?;
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
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(DataBlockIndexEntry {
            smallest_internal_key: cursor.read_internal_key()?,
            largest_internal_key: cursor.read_internal_key()?,
            block: BlockHandle {
                offset: cursor.read_u64()?,
                len: cursor.read_u64()?,
            },
        });
    }
    if !cursor.is_finished() {
        return Err(invalid_table("trailing index block bytes"));
    }
    Ok(entries)
}

fn decode_data_block(bytes: &[u8]) -> Result<Vec<TablePointRecord>> {
    let mut cursor = Cursor::new(bytes);
    let record_count = cursor.read_u32()? as usize;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(TablePointRecord {
            internal_key: cursor.read_internal_key()?,
            value: cursor.read_value_ref()?,
        });
    }
    validate_restart_points(bytes, &mut cursor, record_count)?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing data block bytes"));
    }
    Ok(records)
}

fn decode_range_tombstone_block(bytes: &[u8]) -> Result<Vec<TableRangeTombstone>> {
    let mut cursor = Cursor::new(bytes);
    let tombstone_count = cursor.read_u32()? as usize;
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
    Ok(range_tombstones)
}

fn decode_filter_block(bytes: &[u8]) -> Result<Option<PrefixFilter>> {
    let mut cursor = Cursor::new(bytes);
    let prefix_filter = match cursor.read_u8()? {
        PREFIX_FILTER_ABSENT => None,
        PREFIX_FILTER_PRESENT => {
            let extractor = cursor.read_prefix_extractor()?;
            let prefix_count = cursor.read_u32()? as usize;
            let prefixes = (0..prefix_count)
                .map(|_| cursor.read_bytes().map(<[u8]>::to_vec))
                .collect::<Result<Vec<_>>>()?;
            Some(PrefixFilter::from_sorted_prefixes(extractor, prefixes)?)
        }
        tag => {
            return Err(Error::InvalidFormat {
                message: format!("unknown table prefix filter tag {tag}"),
            });
        }
    };
    if !cursor.is_finished() {
        return Err(invalid_table("trailing filter block bytes"));
    }
    Ok(prefix_filter)
}

fn validate_restart_points(
    block_payload: &[u8],
    cursor: &mut Cursor<'_>,
    record_count: usize,
) -> Result<()> {
    let restart_count = cursor.read_u32()? as usize;
    if record_count == 0 {
        if restart_count == 0 {
            return Ok(());
        }
        return Err(invalid_table("empty data block has restart points"));
    }
    if restart_count == 0 {
        return Err(invalid_table("data block is missing restart points"));
    }

    let mut previous_restart = None;
    for _ in 0..restart_count {
        let restart = cursor.read_u32()? as usize;
        if restart >= block_payload.len() {
            return Err(invalid_table("data block restart outside block"));
        }
        if previous_restart.is_some_and(|previous| restart <= previous) {
            return Err(invalid_table("data block restart points are not sorted"));
        }
        previous_restart = Some(restart);
    }

    Ok(())
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
    put_bytes(bytes, &properties.smallest_user_key)?;
    put_bytes(bytes, &properties.largest_user_key)?;
    put_u64(bytes, properties.smallest_sequence.get());
    put_u64(bytes, properties.largest_sequence.get());
    put_codec(bytes, properties.codec);
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
            smallest_user_key: self.read_bytes()?.to_vec(),
            largest_user_key: self.read_bytes()?.to_vec(),
            smallest_sequence: Sequence::new(self.read_u64()?),
            largest_sequence: Sequence::new(self.read_u64()?),
            codec: self.read_codec()?,
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(decoded.point_records(), table.point_records());
    }

    #[test]
    fn fast_lz4_block_index_round_trips() {
        let table = table_with_records(160, CodecId::FastLz4Block);
        let payload = encode_table(&table).expect("table encodes");
        let decoded = decode_table(&table_file_bytes(&payload)).expect("table decodes");
        assert_eq!(decoded.properties(), table.properties());
        assert_eq!(decoded.point_records(), table.point_records());
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

    fn table_with_records(count: usize, codec: CodecId) -> Table {
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
        Table {
            properties: table_properties(TableId(7), codec, &point_records, &[]),
            point_records,
            range_tombstones: Vec::new(),
            prefix_filter: None,
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
}
