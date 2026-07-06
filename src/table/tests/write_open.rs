use super::*;

#[test]
fn table_write_sort_skips_sorted_point_records() {
    let mut records = vec![
        table_write_sort_record(b"a", 2),
        table_write_sort_record(b"a", 1),
        table_write_sort_record(b"b", 1),
    ];

    sort_point_records_if_needed(&mut records);

    assert_eq!(records[0].0.user_key(), b"a");
    assert_eq!(records[0].0.sequence(), Sequence::new(2));
    assert_eq!(records[1].0.user_key(), b"a");
    assert_eq!(records[1].0.sequence(), Sequence::new(1));
    assert_eq!(records[2].0.user_key(), b"b");
}

#[test]
fn table_write_sort_repairs_unsorted_point_records() {
    let mut records = vec![
        table_write_sort_record(b"b", 1),
        table_write_sort_record(b"a", 1),
        table_write_sort_record(b"a", 2),
    ];

    sort_point_records_if_needed(&mut records);

    assert_eq!(records[0].0.user_key(), b"a");
    assert_eq!(records[0].0.sequence(), Sequence::new(2));
    assert_eq!(records[1].0.user_key(), b"a");
    assert_eq!(records[1].0.sequence(), Sequence::new(1));
    assert_eq!(records[2].0.user_key(), b"b");
}

fn table_write_sort_record(user_key: &[u8], sequence: u64) -> (InternalKey, Option<ValueRef>) {
    (
        InternalKey::new(
            user_key.to_vec(),
            Sequence::new(sequence),
            ValueKind::Put,
            0,
        ),
        Some(ValueRef::Inline(Vec::new())),
    )
}

#[test]
fn async_table_write_returns_loaded_table_without_readback() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-async-table-write-{}",
        table_time_suffix()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");
    let backend = crate::storage::StorageBackend::Native(NativeFileBackend::new());
    let path = root.join("table-1.trinet");
    let options = test_table_options(CodecId::None, false);
    let records = vec![
        table_write_sort_record(b"a", 2),
        table_write_sort_record(b"b", 1),
    ];

    let table = futures::executor::block_on(write_table_with_backend_async(
        &backend,
        &path,
        TableId(1),
        TableLevel::ZERO,
        &options,
        &records,
        &[],
        DurabilityMode::Flush,
    ))
    .expect("async table writes");

    assert_eq!(table.point_records().expect("records stay loaded").len(), 2);
    assert!(table.estimated_file_bytes() > HEADER_LEN as u64);
    assert_eq!(
        futures::executor::block_on(read_table_with_backend_async(&backend, &path))
            .expect("written table reads")
            .point_records()
            .expect("read records load")
            .len(),
        2
    );
    std::fs::remove_dir_all(root).expect("test dir removes");
}
#[test]
fn table_open_metadata_reads_use_owned_source() {
    let header = [7_u8; HEADER_LEN];
    let mut bytes = header.to_vec();
    let footer = empty_footer();
    put_footer(&mut bytes, &footer);
    let source = OwnedOnlySource { bytes };

    assert_eq!(
        read_table_header(Path::new("owned-read.table"), &source).expect("header reads"),
        header
    );
    assert_eq!(
        read_footer_from_source(&source, FOOTER_LEN).expect("footer reads"),
        footer
    );
}

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
    std::fs::write(root.join("table-00000000000000000005.tmp"), b"tmp").expect("tmp file writes");
    std::fs::create_dir(table_path(&root, TableId(6))).expect("table-shaped dir creates");

    let ids = list_table_file_ids(&root).expect("table ids list");
    assert_eq!(
        ids.iter().copied().collect::<Vec<_>>(),
        vec![TableId(2), TableId(4)]
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn async_table_read_decodes_from_storage_backend() {
    let table = table_with_records(24, CodecId::None);
    let payload = encode_table(&table).expect("table encodes");
    let bytes = table_file_bytes(&payload);
    let backend = MemoryStorageBackend::new();
    let path = Path::new("async-table.trinet");
    backend
        .insert_read_object(
            StorageObjectId::native_file(StorageObjectKind::Table, path),
            bytes,
        )
        .expect("memory table object inserts");

    let decoded =
        poll_ready(read_table_with_backend_async(&backend, path)).expect("async table reads");

    assert_eq!(decoded.properties(), table.properties());
    assert_eq!(
        decoded.point_records().expect("decoded records load"),
        table.point_records().expect("source records load")
    );
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
    let reopened = read_table(&path).expect("standalone table read works");
    assert_eq!(reopened.properties().id, table_id);

    std::fs::remove_dir_all(root).expect("test dir removes");
}
