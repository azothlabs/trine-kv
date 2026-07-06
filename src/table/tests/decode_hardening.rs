use super::*;
use crate::table::format::decode_table;
use crate::table::format::encode_table;
use crate::table::format::put_bytes;
use crate::table::format::put_internal_key;
use crate::table::format::put_u8;
use crate::table::format::put_u32;
use crate::table::format::put_u64;
use crate::table::format::put_value_ref;

#[test]
fn unknown_data_block_codec_fails_closed() {
    let table = table_with_records(4, CodecId::None);
    let mut payload = encode_table(&table).expect("table encodes");
    payload[0] = u8::MAX;

    let error = decode_table(&table_file_bytes(&payload)).expect_err("unknown block codec fails");
    assert!(matches!(error, Error::UnsupportedFormat { .. }));
}

#[test]
fn table_decode_rejects_payload_len_before_large_allocation() {
    let payload_len = u32::try_from(limits::MAX_WHOLE_TABLE_DECODE_BYTES + 1)
        .expect("test payload length fits u32");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TABLE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&TABLE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());

    let error = decode_table(&bytes).expect_err("oversized payload length should fail");

    assert_invalid_table_message(&error, "table payload length");
}

#[test]
fn table_decode_rejects_index_entry_count_before_large_allocation() {
    let error =
        decode_index_block(&count_block(u32::MAX)).expect_err("impossible index count should fail");
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
    let error = decode_filter_block(&point_bytes).expect_err("short point-key bitset should fail");
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
