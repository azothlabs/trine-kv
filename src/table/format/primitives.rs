use super::{
    BOUND_EXCLUDED, BOUND_INCLUDED, BOUND_UNBOUNDED, BlobIndex, Bound, CodecId, Error, InternalKey,
    MIN_INTERNAL_KEY_BYTES, PREFIX_EXTRACTOR_CUSTOM, PREFIX_EXTRACTOR_DISABLED,
    PREFIX_EXTRACTOR_FIXED_LEN, PREFIX_EXTRACTOR_SEPARATOR, PrefixExtractor, Range, Result,
    SectionHandle, Sequence, TableBlobReference, TableId, TableLevel, TablePointRecord,
    TableProperties, VALUE_BLOB, VALUE_BLOB_INDEX, VALUE_INLINE, VALUE_KIND_POINT_DELETE,
    VALUE_KIND_PUT, VALUE_KIND_RANGE_DELETE, VALUE_NONE, ValueKind, ValueRef, limits,
};

pub(in crate::table) fn validate_sorted_point_records(
    point_records: &[TablePointRecord],
) -> Result<()> {
    for pair in point_records.windows(2) {
        if pair[0].internal_key >= pair[1].internal_key {
            return Err(Error::Corruption {
                message: "table point records are not sorted by internal key".to_owned(),
            });
        }
    }

    Ok(())
}

pub(in crate::table) fn put_properties(
    bytes: &mut Vec<u8>,
    properties: &TableProperties,
) -> Result<()> {
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

pub(in crate::table) fn put_internal_key(
    bytes: &mut Vec<u8>,
    internal_key: &InternalKey,
) -> Result<()> {
    put_bytes(bytes, internal_key.user_key())?;
    put_u64(bytes, internal_key.sequence().get());
    put_value_kind(bytes, internal_key.kind());
    put_u32(bytes, internal_key.batch_index());
    Ok(())
}

pub(in crate::table) fn put_value_kind(bytes: &mut Vec<u8>, value_kind: ValueKind) {
    put_u8(
        bytes,
        match value_kind {
            ValueKind::Put => VALUE_KIND_PUT,
            ValueKind::PointDelete => VALUE_KIND_POINT_DELETE,
            ValueKind::RangeDelete => VALUE_KIND_RANGE_DELETE,
        },
    );
}

pub(in crate::table) fn put_value_ref(bytes: &mut Vec<u8>, value: Option<&ValueRef>) -> Result<()> {
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

pub(in crate::table) fn put_blob_index(bytes: &mut Vec<u8>, index: BlobIndex) {
    put_u64(bytes, index.file_id);
    put_u64(bytes, index.offset);
    put_u64(bytes, index.encoded_len);
    put_u64(bytes, index.value_len);
    put_u32(bytes, index.value_checksum);
    put_u32(bytes, index.record_checksum);
    put_codec(bytes, index.compression);
}

pub(in crate::table) fn put_codec(bytes: &mut Vec<u8>, codec: CodecId) {
    put_u8(bytes, codec.tag());
}

pub(in crate::table) fn codec_from_tag(tag: u8) -> Result<CodecId> {
    CodecId::from_tag(tag)
}

pub(in crate::table) fn put_bound(bytes: &mut Vec<u8>, bound: &Bound<Vec<u8>>) -> Result<()> {
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

pub(in crate::table) fn put_prefix_extractor(
    bytes: &mut Vec<u8>,
    extractor: &PrefixExtractor,
) -> Result<()> {
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

pub(in crate::table) fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

pub(in crate::table) fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(in crate::table) fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(in crate::table) fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(in crate::table) fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("table byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

pub(in crate::table) fn put_section_handle(bytes: &mut Vec<u8>, handle: SectionHandle) {
    put_u64(bytes, handle.offset);
    put_u64(bytes, handle.len);
}

pub(in crate::table) fn point_record_encoded_len(record: &TablePointRecord) -> usize {
    internal_key_encoded_len(&record.internal_key) + value_ref_encoded_len(record.value.as_ref())
}

pub(in crate::table) fn internal_key_encoded_len(internal_key: &InternalKey) -> usize {
    4 + internal_key.user_key().len() + 8 + 1 + 4
}

pub(in crate::table) fn value_ref_encoded_len(value: Option<&ValueRef>) -> usize {
    match value {
        None => 1,
        Some(ValueRef::Inline(bytes)) => 1 + 4 + bytes.len(),
        Some(ValueRef::BlobIndex(_)) => 1 + 8 + 8 + 8 + 8 + 4 + 4 + 1,
        Some(ValueRef::Blob { .. }) => 1 + 8 + 8 + 8 + 4,
    }
}

pub(in crate::table) fn key_is_before_start(key: &[u8], start: &Bound<Vec<u8>>) -> bool {
    match start {
        Bound::Included(start) => key < start.as_slice(),
        Bound::Excluded(start) => key <= start.as_slice(),
        Bound::Unbounded => false,
    }
}

pub(in crate::table) fn key_is_after_end(key: &[u8], end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Included(end) => key > end.as_slice(),
        Bound::Excluded(end) => key >= end.as_slice(),
        Bound::Unbounded => false,
    }
}

pub(in crate::table) fn section_bounds(handle: SectionHandle) -> Result<(usize, usize)> {
    bounds(handle.offset, handle.len)
}

pub(in crate::table) fn bounds(offset: u64, len: u64) -> Result<(usize, usize)> {
    let start = usize::try_from(offset).map_err(|_| invalid_table("offset exceeds usize"))?;
    let len = usize::try_from(len).map_err(|_| invalid_table("length exceeds usize"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| invalid_table("offset plus length overflows usize"))?;
    Ok((start, end))
}

pub(in crate::table) fn usize_to_u32(value: usize, field: &'static str) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u32::MAX")))
}

pub(in crate::table) fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

pub(in crate::table) fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

pub(in crate::table) fn u32_to_usize(value: u32) -> usize {
    value as usize
}

pub(in crate::table) fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = limits::checked_add_invalid_format(offset, 2, "u16 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_table("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

pub(in crate::table) fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = limits::checked_add_invalid_format(offset, 4, "u32 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_table("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

pub(in crate::table) fn user_key_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

pub(in crate::table) fn invalid_table(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid table: {message}"),
    }
}

pub(in crate::table) fn ensure_count_fits_remaining(
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

pub(in crate::table) struct Cursor<'payload> {
    payload: &'payload [u8],
    pub(in crate::table) offset: usize,
}

impl<'payload> Cursor<'payload> {
    pub(in crate::table) const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    pub(in crate::table) fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_table("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    pub(in crate::table) fn read_u16(&mut self) -> Result<u16> {
        let value = read_u16_at(self.payload, self.offset)?;
        self.offset += 2;
        Ok(value)
    }

    pub(in crate::table) fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32_at(self.payload, self.offset)?;
        self.offset += 4;
        Ok(value)
    }

    pub(in crate::table) fn read_u64(&mut self) -> Result<u64> {
        let end = limits::checked_add_invalid_format(self.offset, 8, "u64 offset")?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_table("short u64"))?;
        self.offset = end;
        Ok(u64::from_le_bytes([
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ]))
    }

    pub(in crate::table) fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let end = limits::checked_add_invalid_format(self.offset, len, "byte field length")?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_table("short bytes"))?;
        self.offset = end;
        Ok(value)
    }

    pub(in crate::table) fn read_bytes_range(&mut self) -> Result<Range<u32>> {
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

    pub(in crate::table) fn read_properties(&mut self) -> Result<TableProperties> {
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

    pub(in crate::table) fn read_internal_key(&mut self) -> Result<InternalKey> {
        let user_key = self.read_bytes()?.to_vec();
        let sequence = Sequence::new(self.read_u64()?);
        let kind = self.read_value_kind()?;
        let batch_index = self.read_u32()?;
        Ok(InternalKey::new(user_key, sequence, kind, batch_index))
    }

    pub(in crate::table) fn read_value_kind(&mut self) -> Result<ValueKind> {
        match self.read_u8()? {
            VALUE_KIND_PUT => Ok(ValueKind::Put),
            VALUE_KIND_POINT_DELETE => Ok(ValueKind::PointDelete),
            VALUE_KIND_RANGE_DELETE => Ok(ValueKind::RangeDelete),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown table value kind {tag}"),
            }),
        }
    }

    pub(in crate::table) fn read_codec(&mut self) -> Result<CodecId> {
        codec_from_tag(self.read_u8()?)
    }

    pub(in crate::table) fn read_blob_file_ids(&mut self) -> Result<Vec<u64>> {
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

    pub(in crate::table) fn read_blob_references(&mut self) -> Result<Vec<TableBlobReference>> {
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

    pub(in crate::table) fn read_section_handle(&mut self) -> Result<SectionHandle> {
        Ok(SectionHandle {
            offset: self.read_u64()?,
            len: self.read_u64()?,
        })
    }

    pub(in crate::table) fn read_prefix_extractor(&mut self) -> Result<PrefixExtractor> {
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

    pub(in crate::table) fn read_bound(&mut self) -> Result<Bound<Vec<u8>>> {
        match self.read_u8()? {
            BOUND_UNBOUNDED => Ok(Bound::Unbounded),
            BOUND_INCLUDED => Ok(Bound::Included(self.read_bytes()?.to_vec())),
            BOUND_EXCLUDED => Ok(Bound::Excluded(self.read_bytes()?.to_vec())),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown table range bound tag {tag}"),
            }),
        }
    }

    pub(in crate::table) const fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }

    pub(in crate::table) const fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}
