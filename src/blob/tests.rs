use std::collections::BTreeSet;

use crate::{
    blob::{
        BlobFileHeader, BlobRecord, ValueRef, decode_blob_file, decode_records_with_budget,
        encode_blob_file, inline_blob_values, read_blob_file_with_backend_async,
        read_indexed_value, read_record_for_index, read_value_for_internal_key_with_backend_async,
        write_large_values,
    },
    codec::CodecId,
    internal_key::{InternalKey, ValueKind},
    storage::{MemoryStorageBackend, NativeFileBackend, StorageObjectId, StorageObjectKind},
    types::Sequence,
};

#[test]
fn blob_file_round_trips_ordered_records() {
    let header = BlobFileHeader::new(7, Sequence::new(42), 64 * 1024, CodecId::None);
    let records = vec![
        blob_record("user:1", 3, 0, b"Ada".to_vec(), CodecId::None),
        blob_record(
            "user:2",
            2,
            1,
            b"Lin Lin Lin Lin".to_vec(),
            CodecId::FastLz4Block,
        ),
    ];

    let (bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
    let decoded = decode_blob_file(&bytes).expect("blob decodes");

    assert_eq!(decoded.header, header);
    assert_eq!(indexes.len(), 2);
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].index, indexes[0]);
    assert_eq!(decoded.records[0].record, records[0]);
    assert_eq!(decoded.records[1].index, indexes[1]);
    assert_eq!(decoded.records[1].record, records[1]);
    assert_eq!(decoded.properties.record_count, 2);
    assert_eq!(
        decoded.properties.value_bytes,
        (b"Ada".len() + b"Lin Lin Lin Lin".len()) as u64
    );
}

#[test]
fn aggregate_decoded_blob_budget_is_enforced_before_next_value_allocation() {
    let header = BlobFileHeader::new(7, Sequence::new(42), 1, CodecId::FastLz4Block);
    let records = vec![
        blob_record("a", 2, 0, vec![0; 32], CodecId::FastLz4Block),
        blob_record("b", 1, 0, vec![0; 32], CodecId::FastLz4Block),
    ];
    let (bytes, _) = encode_blob_file(header, &records).expect("blob encodes");
    let footer = &bytes[bytes.len() - super::BLOB_FOOTER_LEN..];
    let properties_offset = usize::try_from(u64::from_le_bytes(
        footer[..8].try_into().expect("properties offset bytes"),
    ))
    .expect("properties offset fits usize");
    let record_bytes = &bytes[super::BLOB_HEADER_LEN..properties_offset];

    let error = decode_records_with_budget(header.file_id, record_bytes, 48)
        .expect_err("second value exceeds the aggregate decode budget");
    assert!(matches!(error, crate::Error::InvalidFormat { .. }));
}

#[test]
fn async_blob_read_decodes_from_storage_backend() {
    let file_id = 44;
    let header = BlobFileHeader::new(file_id, Sequence::new(7), 16, CodecId::None);
    let records = vec![
        blob_record("user:1", 7, 0, b"value-one".to_vec(), CodecId::None),
        blob_record("user:2", 8, 0, b"value-two".to_vec(), CodecId::None),
    ];
    let (bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
    let backend = MemoryStorageBackend::new();
    let db_path = std::path::Path::new("async-blob-db");
    backend
        .insert_read_object(
            StorageObjectId::native_file(
                StorageObjectKind::Blob,
                super::blob_path(db_path, file_id),
            ),
            bytes,
        )
        .expect("memory blob object inserts");

    let blob_file = poll_ready(read_blob_file_with_backend_async(
        &backend, db_path, file_id,
    ))
    .expect("async blob file reads");
    assert_eq!(blob_file.header, header);
    assert_eq!(blob_file.records.len(), 2);

    let value = poll_ready(read_value_for_internal_key_with_backend_async(
        &backend,
        db_path,
        &ValueRef::BlobIndex(indexes[0]),
        Some(&records[0].internal_key),
    ))
    .expect("async indexed blob reads");
    assert_eq!(value, b"value-one");
}

#[test]
fn inline_blob_values_reuses_open_blob_file() {
    let temp = temp_blob_dir("inline-cache");
    let backend = NativeFileBackend::new();
    let header = BlobFileHeader::new(51, Sequence::new(3), 16, CodecId::None);
    let records = vec![
        blob_record("user:1", 3, 0, b"value-one".to_vec(), CodecId::None),
        blob_record("user:2", 2, 0, b"value-two".to_vec(), CodecId::None),
    ];
    let indexes = super::write_blob_file_with_backend(&backend, &temp, 51, header, &records)
        .expect("blob file writes");
    let table_records = vec![
        (
            records[0].internal_key.clone(),
            Some(ValueRef::BlobIndex(indexes[0])),
        ),
        (
            records[1].internal_key.clone(),
            Some(ValueRef::BlobIndex(indexes[1])),
        ),
    ];
    let before = backend.stats().operations.open_read.requests;

    let rewritten = super::inline_blob_values_with_backend(&backend, &temp, &table_records)
        .expect("blob values inline");
    let after = backend.stats().operations.open_read.requests;

    assert_eq!(after.saturating_sub(before), 1);
    assert_eq!(
        rewritten,
        vec![
            (
                records[0].internal_key.clone(),
                Some(ValueRef::Inline(b"value-one".to_vec()))
            ),
            (
                records[1].internal_key.clone(),
                Some(ValueRef::Inline(b"value-two".to_vec()))
            ),
        ]
    );
    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn async_inline_blob_values_reuses_open_blob_file() {
    let temp = temp_blob_dir("async-inline-cache");
    let backend = NativeFileBackend::new();
    let header = BlobFileHeader::new(52, Sequence::new(3), 16, CodecId::None);
    let records = vec![
        blob_record("user:1", 3, 0, b"value-one".to_vec(), CodecId::None),
        blob_record("user:2", 2, 0, b"value-two".to_vec(), CodecId::None),
    ];
    let indexes = super::write_blob_file_with_backend(&backend, &temp, 52, header, &records)
        .expect("blob file writes");
    let table_records = vec![
        (
            records[0].internal_key.clone(),
            Some(ValueRef::BlobIndex(indexes[0])),
        ),
        (
            records[1].internal_key.clone(),
            Some(ValueRef::BlobIndex(indexes[1])),
        ),
    ];
    let before = backend.stats().operations.open_read.requests;

    let rewritten = poll_ready(super::inline_blob_values_with_backend_async(
        &backend,
        &temp,
        &table_records,
    ))
    .expect("blob values inline");
    let after = backend.stats().operations.open_read.requests;

    assert_eq!(after.saturating_sub(before), 1);
    assert_eq!(
        rewritten,
        vec![
            (
                records[0].internal_key.clone(),
                Some(ValueRef::Inline(b"value-one".to_vec()))
            ),
            (
                records[1].internal_key.clone(),
                Some(ValueRef::Inline(b"value-two".to_vec()))
            ),
        ]
    );
    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn blob_file_rejects_corrupt_footer() {
    let (mut bytes, _) = encode_blob_file(
        BlobFileHeader::new(9, Sequence::new(1), 8, CodecId::None),
        &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
    )
    .expect("blob encodes");

    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;

    let error = decode_blob_file(&bytes).expect_err("corrupt footer fails");
    assert!(error.to_string().contains("footer magic mismatch"));
}

#[test]
fn blob_decode_rejects_properties_len_before_large_allocation() {
    let header = BlobFileHeader::new(7, Sequence::new(42), 64 * 1024, CodecId::None);
    let mut bytes = Vec::new();
    super::put_header(&mut bytes, header);
    super::put_footer(
        &mut bytes,
        super::BLOB_HEADER_LEN as u64,
        (super::limits::MAX_BLOB_PROPERTIES_BYTES as u64) + 1,
    );

    let error = decode_blob_file(&bytes)
        .expect_err("oversized properties length should fail before allocation");

    assert!(error.to_string().contains("blob properties length"));
}

#[test]
fn indexed_blob_read_rejects_record_body_len_before_large_allocation() {
    let temp = temp_blob_dir("oversized-record-body");
    let path = super::blob_path(&temp, 22);
    let mut bytes = vec![0_u8; super::BLOB_HEADER_LEN];
    bytes
        .extend_from_slice(&((super::limits::MAX_BLOB_RECORD_BODY_BYTES as u64) + 1).to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    std::fs::write(&path, bytes).expect("blob bytes write");

    let backend = NativeFileBackend::new();
    let object =
        super::open_blob_read_object_with_backend(&backend, &temp, 22).expect("blob object opens");
    let index = super::BlobIndex {
        file_id: 22,
        offset: super::BLOB_HEADER_LEN as u64,
        encoded_len: 0,
        value_len: 0,
        value_checksum: 0,
        record_checksum: 0,
        compression: CodecId::None,
    };

    let error =
        super::read_indexed_blob_record(&object, super::BLOB_HEADER_LEN as u64 + 12, &index)
            .expect_err("oversized record body should fail before allocation");

    assert!(error.to_string().contains("blob record length"));
    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn blob_file_rejects_header_checksum_mismatch() {
    let (mut bytes, _) = encode_blob_file(
        BlobFileHeader::new(9, Sequence::new(1), 8, CodecId::None),
        &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
    )
    .expect("blob encodes");

    bytes[4] ^= 0xff;

    let error = decode_blob_file(&bytes).expect_err("corrupt header fails");
    assert!(error.to_string().contains("blob header checksum mismatch"));
}

#[test]
fn blob_file_rejects_properties_checksum_mismatch() {
    let (mut bytes, _) = encode_blob_file(
        BlobFileHeader::new(9, Sequence::new(1), 8, CodecId::None),
        &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
    )
    .expect("blob encodes");
    let footer_start = bytes.len() - super::BLOB_FOOTER_LEN;
    let properties_offset = usize::try_from(
        super::read_u64_at(&bytes[footer_start..], 0).expect("footer offset reads"),
    )
    .expect("footer offset fits usize");

    bytes[properties_offset] ^= 0xff;

    let error = decode_blob_file(&bytes).expect_err("corrupt properties fail");
    assert!(
        error
            .to_string()
            .contains("blob properties checksum mismatch")
    );
}

#[test]
fn blob_file_rejects_record_checksum_mismatch() {
    let (mut bytes, _) = encode_blob_file(
        BlobFileHeader::new(10, Sequence::new(1), 8, CodecId::None),
        &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
    )
    .expect("blob encodes");

    bytes[super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES] ^= 0xff;

    let error = decode_blob_file(&bytes).expect_err("corrupt record fails");
    assert!(error.to_string().contains("blob record checksum mismatch"));
}

#[test]
fn blob_file_rejects_value_checksum_mismatch() {
    let (mut bytes, _) = encode_blob_file(
        BlobFileHeader::new(10, Sequence::new(1), 8, CodecId::None),
        &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
    )
    .expect("blob encodes");

    let body_start = super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES;
    let value_checksum_offset = body_start + internal_key_len("key") + 8 + 8 + 1;
    bytes[value_checksum_offset] ^= 0xff;
    rewrite_record_checksum(&mut bytes);

    let error = decode_blob_file(&bytes).expect_err("corrupt value checksum fails");
    assert!(error.to_string().contains("blob value checksum mismatch"));
}

#[test]
fn blob_file_rejects_unknown_record_compression() {
    let (mut bytes, _) = encode_blob_file(
        BlobFileHeader::new(10, Sequence::new(1), 8, CodecId::None),
        &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
    )
    .expect("blob encodes");

    let body_start = super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES;
    let compression_offset = body_start + internal_key_len("key") + 8 + 8;
    bytes[compression_offset] = 9;
    rewrite_record_checksum(&mut bytes);

    let error = decode_blob_file(&bytes).expect_err("unknown codec fails");
    assert!(error.to_string().contains("unknown blob codec 9"));
}

#[test]
fn blob_file_rejects_unordered_records() {
    let header = BlobFileHeader::new(11, Sequence::new(1), 8, CodecId::None);
    let records = vec![
        blob_record("z", 1, 0, b"value".to_vec(), CodecId::None),
        blob_record("a", 1, 0, b"value".to_vec(), CodecId::None),
    ];

    let error = encode_blob_file(header, &records).expect_err("unordered records fail");
    assert!(error.to_string().contains("sorted by internal key"));
}

#[test]
fn indexed_read_validates_exact_blob_index() {
    let temp = temp_blob_dir("indexed-read-validates");

    let header = BlobFileHeader::new(12, Sequence::new(1), 8, CodecId::None);
    let record = blob_record("key", 1, 0, b"value".to_vec(), CodecId::None);
    let (bytes, indexes) = encode_blob_file(header, &[record]).expect("blob encodes");
    std::fs::write(super::blob_path(&temp, 12), bytes).expect("blob writes");

    let value = read_indexed_value(&temp, &indexes[0], None).expect("indexed read works");
    assert_eq!(value, b"value");

    let mut bad_index = indexes[0];
    bad_index.value_len += 1;
    let error = read_indexed_value(&temp, &bad_index, None).expect_err("bad index fails");
    assert!(error.to_string().contains("metadata mismatch"));

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn indexed_read_uses_only_target_record() {
    let temp = temp_blob_dir("indexed-read-target-record");

    let header = BlobFileHeader::new(13, Sequence::new(1), 8, CodecId::None);
    let records = vec![
        blob_record("key-a", 1, 0, b"value-a".to_vec(), CodecId::None),
        blob_record("key-b", 1, 0, b"value-b".to_vec(), CodecId::None),
    ];
    let (mut bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
    let corrupt_second_body = usize::try_from(indexes[1].offset)
        .expect("offset fits usize")
        .saturating_add(super::MIN_BLOB_RECORD_FRAME_BYTES);
    bytes[corrupt_second_body] ^= 0xff;
    assert!(
        decode_blob_file(&bytes).is_err(),
        "full blob decode should notice the unrelated corrupt record"
    );
    std::fs::write(super::blob_path(&temp, 13), bytes).expect("blob writes");

    let value = read_indexed_value(&temp, &indexes[0], None)
        .expect("targeted indexed read skips unrelated corrupt record");
    assert_eq!(value, b"value-a");

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn async_indexed_read_uses_only_target_record() {
    let file_id = 45;
    let db_path = std::path::Path::new("async-targeted-blob-db");
    let header = BlobFileHeader::new(file_id, Sequence::new(1), 8, CodecId::None);
    let records = vec![
        blob_record("key-a", 1, 0, b"value-a".to_vec(), CodecId::None),
        blob_record("key-b", 1, 0, b"value-b".to_vec(), CodecId::None),
    ];
    let (mut bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
    let corrupt_second_body = usize::try_from(indexes[1].offset)
        .expect("offset fits usize")
        .saturating_add(super::MIN_BLOB_RECORD_FRAME_BYTES);
    bytes[corrupt_second_body] ^= 0xff;
    let backend = MemoryStorageBackend::new();
    backend
        .insert_read_object(
            StorageObjectId::native_file(
                StorageObjectKind::Blob,
                super::blob_path(db_path, file_id),
            ),
            bytes,
        )
        .expect("memory blob object inserts");

    let value = poll_ready(read_value_for_internal_key_with_backend_async(
        &backend,
        db_path,
        &ValueRef::BlobIndex(indexes[0]),
        None,
    ))
    .expect("async targeted read skips unrelated corrupt record");
    assert_eq!(value, b"value-a");
}

#[test]
fn standalone_large_value_wrappers_round_trip() {
    let temp = temp_blob_dir("standalone-large-value-wrappers");
    let internal_key = InternalKey::new(b"key".to_vec(), Sequence::new(3), ValueKind::Put, 0);
    let records = vec![(
        internal_key.clone(),
        Some(ValueRef::Inline(
            b"value-through-standalone-wrapper".to_vec(),
        )),
    )];

    let rewritten =
        write_large_values(&temp, 21, 4, CodecId::None, &records).expect("large value writes");
    let Some(ValueRef::BlobIndex(index)) = rewritten[0].1.as_ref() else {
        panic!("large value should be written as a blob index");
    };
    let record = read_record_for_index(&temp, index, Some(&internal_key))
        .expect("standalone record read works");
    assert_eq!(record.record.value, b"value-through-standalone-wrapper");

    let inlined = inline_blob_values(&temp, &rewritten).expect("blob value inlines");
    assert_eq!(inlined, records);

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn properties_read_skips_record_payload_decode() {
    let temp = temp_blob_dir("properties-read");

    let header = BlobFileHeader::new(14, Sequence::new(1), 8, CodecId::None);
    let records = vec![
        blob_record("key-a", 1, 0, b"value-a".to_vec(), CodecId::None),
        blob_record("key-b", 1, 0, b"value-b".to_vec(), CodecId::None),
    ];
    let (mut bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
    let expected = decode_blob_file(&bytes)
        .expect("blob decodes before corruption")
        .properties;
    let corrupt_second_body = usize::try_from(indexes[1].offset)
        .expect("offset fits usize")
        .saturating_add(super::MIN_BLOB_RECORD_FRAME_BYTES);
    bytes[corrupt_second_body] ^= 0xff;
    std::fs::write(super::blob_path(&temp, 14), bytes).expect("blob writes");

    assert_eq!(
        super::read_blob_file_properties(&temp, 14).expect("properties read succeeds"),
        expected
    );
    assert!(
        super::read_blob_file(&temp, 14).is_err(),
        "full validation should still decode and verify blob records"
    );

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn write_blob_file_creates_final_object_without_leftover_tmp() {
    let temp = temp_blob_dir("write-object");
    let header = BlobFileHeader::new(15, Sequence::new(1), 8, CodecId::None);
    let record = blob_record("key", 1, 0, b"value".to_vec(), CodecId::None);

    let indexes = super::write_blob_file(&temp, 15, header, &[record]).expect("blob file writes");

    let path = super::blob_path(&temp, 15);
    assert_eq!(indexes.len(), 1);
    assert!(path.exists(), "final blob object should exist");
    assert!(
        !path.with_extension("tmp").exists(),
        "successful blob write should leave no temporary file"
    );
    assert_eq!(
        super::read_blob_file(&temp, 15)
            .expect("written blob file reads")
            .header,
        header
    );

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn backend_written_blob_reads_full_properties_and_indexed_values() {
    let temp = temp_blob_dir("backend-read-object");
    let header = BlobFileHeader::new(16, Sequence::new(2), 8, CodecId::None);
    let records = vec![
        blob_record("key-a", 2, 0, b"value-a".to_vec(), CodecId::None),
        blob_record("key-b", 2, 1, b"value-b".to_vec(), CodecId::FastLz4Block),
    ];

    let indexes = super::write_blob_file(&temp, 16, header, &records).expect("blob file writes");
    let blob_file = super::read_blob_file(&temp, 16).expect("full blob file reads");
    let properties = super::read_blob_file_properties(&temp, 16).expect("blob properties read");
    let indexed_value =
        read_indexed_value(&temp, &indexes[1], None).expect("indexed blob value reads");

    assert_eq!(blob_file.header, header);
    assert_eq!(blob_file.records[0].record, records[0]);
    assert_eq!(blob_file.records[1].record, records[1]);
    assert_eq!(properties, blob_file.properties);
    assert_eq!(indexed_value, b"value-b");

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn list_blob_file_ids_reads_backend_object_listing() {
    let temp = temp_blob_dir("list-object");
    std::fs::write(temp.join("blob-00000000000000000017.trineb"), b"blob-a")
        .expect("blob file writes");
    std::fs::write(temp.join("blob-00000000000000000018.TRINEB"), b"blob-b")
        .expect("uppercase blob file writes");
    std::fs::write(
        temp.join("blob-00000000000000000019.trinet"),
        b"wrong extension",
    )
    .expect("non-blob file writes");
    std::fs::write(temp.join("notes.trineb"), b"wrong prefix").expect("non-blob prefix writes");
    std::fs::create_dir(temp.join("blob-00000000000000000020.trineb"))
        .expect("blob-shaped directory creates");

    let ids = super::list_blob_file_ids(&temp).expect("blob file ids list");

    assert_eq!(ids, BTreeSet::from([17, 18]));

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

#[test]
fn list_blob_file_ids_rejects_malformed_blob_names() {
    let temp = temp_blob_dir("list-malformed");
    std::fs::write(temp.join("blob-not-a-number.trineb"), b"bad blob")
        .expect("malformed blob file writes");

    let error = super::list_blob_file_ids(&temp).expect_err("malformed blob file name fails");

    assert!(error.to_string().contains("invalid blob file name"));

    std::fs::remove_dir_all(temp).expect("cleanup temp dir");
}

fn blob_record(
    key: &str,
    sequence: u64,
    batch_index: u32,
    value: Vec<u8>,
    compression: CodecId,
) -> BlobRecord {
    BlobRecord {
        internal_key: InternalKey::new(key, Sequence::new(sequence), ValueKind::Put, batch_index),
        value,
        compression,
    }
}

fn internal_key_len(key: &str) -> usize {
    4 + key.len() + 8 + 1 + 4
}

fn rewrite_record_checksum(bytes: &mut [u8]) {
    let checksum_offset = super::BLOB_HEADER_LEN + 8;
    let body_start = super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES;
    let body_len = usize::try_from(
        super::read_u64_at(bytes, super::BLOB_HEADER_LEN).expect("record length reads"),
    )
    .expect("record length fits usize");
    let checksum = super::checksum(&bytes[body_start..body_start + body_len]);
    bytes[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());
}

fn temp_blob_dir(name: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "trine-kv-blob-{name}-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir creates");
    dir
}

fn poll_ready<T>(future: impl std::future::Future<Output = crate::Result<T>>) -> crate::Result<T> {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(result) => result,
        std::task::Poll::Pending => panic!("blob storage future unexpectedly pending"),
    }
}
