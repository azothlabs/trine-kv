use std::{
    fs::{self, File},
    io::{Read, Write},
    ops::Bound,
    path::{Path, PathBuf},
};

use crate::{
    blob::ValueRef,
    codec::CodecId,
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    types::{KeyRange, Sequence},
};

pub const TABLE_FILE_EXTENSION: &str = "trinet";
const TABLE_MAGIC: u32 = 0x5452_5442;
const TABLE_VERSION: u16 = 1;
const HEADER_LEN: usize = 14;

const VALUE_KIND_PUT: u8 = 1;
const VALUE_KIND_POINT_DELETE: u8 = 2;
const VALUE_KIND_RANGE_DELETE: u8 = 3;

const VALUE_NONE: u8 = 0;
const VALUE_INLINE: u8 = 1;

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

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
pub(crate) struct TablePointRecord {
    pub(crate) internal_key: InternalKey,
    pub(crate) value: Option<ValueRef>,
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
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[TableRangeTombstone],
) -> Result<Table> {
    if point_records.is_empty() && range_tombstones.is_empty() {
        return Err(Error::invalid_options("cannot write an empty table"));
    }

    let mut point_records = point_records
        .iter()
        .map(|(internal_key, value)| TablePointRecord {
            internal_key: internal_key.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    point_records.sort_by(|left, right| left.internal_key.cmp(&right.internal_key));

    let table = Table {
        properties: table_properties(table_id, &point_records, range_tombstones),
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
        codec: CodecId::None,
    }
}

fn encode_table(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_properties(&mut bytes, &table.properties)?;
    put_u32(
        &mut bytes,
        u32::try_from(table.point_records.len())
            .map_err(|_| Error::invalid_options("too many point records for table"))?,
    );
    for record in &table.point_records {
        put_internal_key(&mut bytes, &record.internal_key)?;
        put_value_ref(&mut bytes, record.value.as_ref())?;
    }

    put_u32(
        &mut bytes,
        u32::try_from(table.range_tombstones.len())
            .map_err(|_| Error::invalid_options("too many range tombstones for table"))?,
    );
    for tombstone in &table.range_tombstones {
        put_bound(&mut bytes, &tombstone.range.start)?;
        put_bound(&mut bytes, &tombstone.range.end)?;
        put_u64(&mut bytes, tombstone.sequence.get());
        put_u32(&mut bytes, tombstone.batch_index);
    }

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

    let mut cursor = Cursor::new(payload);
    let properties = cursor.read_properties()?;
    let point_count = cursor.read_u32()? as usize;
    let mut point_records = Vec::with_capacity(point_count);
    for _ in 0..point_count {
        point_records.push(TablePointRecord {
            internal_key: cursor.read_internal_key()?,
            value: cursor.read_value_ref()?,
        });
    }

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
        return Err(invalid_table("trailing payload bytes"));
    }

    Ok(Table {
        properties,
        point_records,
        range_tombstones,
    })
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
        Some(ValueRef::Blob { .. }) => {
            return Err(Error::unsupported(
                "blob table values are not implemented yet",
            ));
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
            CodecId::CompactZlib => 2,
        },
    );
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

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
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
            tag => Err(Error::InvalidFormat {
                message: format!("unknown table value reference {tag}"),
            }),
        }
    }

    fn read_codec(&mut self) -> Result<CodecId> {
        match self.read_u8()? {
            0 => Ok(CodecId::None),
            1 => Ok(CodecId::FastLz4Block),
            2 => Ok(CodecId::CompactZlib),
            tag => Err(Error::UnsupportedFormat {
                message: format!("unknown table codec {tag}"),
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
