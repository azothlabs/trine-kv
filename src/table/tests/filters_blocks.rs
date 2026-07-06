use super::*;
use crate::table::format::decode_table;
use crate::table::format::encode_table;
use crate::table::format::read_checked_block_from_file;
use crate::table::format::read_footer;
use crate::table::format::validate_data_block_filters;

#[test]
fn level_adjusted_filter_bits_decreases_with_depth() {
    use crate::options::FilterDepthCurve;
    // Auto: pinned shallow levels keep the base; deeper levels get
    // progressively fewer bits, clamped to the floor and never exceeding the
    // base. The same curve drives both the point and prefix filters.
    let auto = FilterDepthCurve::Auto;
    assert_eq!(level_adjusted_filter_bits(auto, 10, TableLevel::ZERO), 10);
    assert_eq!(level_adjusted_filter_bits(auto, 10, TableLevel(1)), 10);
    assert_eq!(level_adjusted_filter_bits(auto, 10, TableLevel(2)), 8);
    assert_eq!(level_adjusted_filter_bits(auto, 10, TableLevel(3)), 6);
    assert_eq!(level_adjusted_filter_bits(auto, 10, TableLevel(4)), 4);
    assert_eq!(level_adjusted_filter_bits(auto, 10, TableLevel(9)), 4);
    // A small base is never raised above itself (no memory regression).
    assert_eq!(level_adjusted_filter_bits(auto, 3, TableLevel(9)), 3);

    // Uniform: every level keeps the base.
    let uniform = FilterDepthCurve::Uniform;
    assert_eq!(level_adjusted_filter_bits(uniform, 10, TableLevel(9)), 10);

    // Custom: tune step and floor; floor never exceeds the base.
    let custom = FilterDepthCurve::Custom { step: 3, floor: 2 };
    assert_eq!(level_adjusted_filter_bits(custom, 10, TableLevel(1)), 10);
    assert_eq!(level_adjusted_filter_bits(custom, 10, TableLevel(2)), 7);
    assert_eq!(level_adjusted_filter_bits(custom, 10, TableLevel(3)), 4);
    assert_eq!(level_adjusted_filter_bits(custom, 10, TableLevel(9)), 2);

    // CostWeighted (ascending, for remote backends): pinned shallow levels
    // keep the base; deeper levels gain bits up to the ceiling.
    let remote = FilterDepthCurve::CostWeighted { step: 3, ceil: 20 };
    assert_eq!(level_adjusted_filter_bits(remote, 10, TableLevel::ZERO), 10);
    assert_eq!(level_adjusted_filter_bits(remote, 10, TableLevel(1)), 10);
    assert_eq!(level_adjusted_filter_bits(remote, 10, TableLevel(2)), 13);
    assert_eq!(level_adjusted_filter_bits(remote, 10, TableLevel(3)), 16);
    assert_eq!(level_adjusted_filter_bits(remote, 10, TableLevel(4)), 19);
    assert_eq!(level_adjusted_filter_bits(remote, 10, TableLevel(9)), 20);
    // A ceiling below the base cannot lower bits: the curve only ever raises.
    let clamped = FilterDepthCurve::CostWeighted { step: 3, ceil: 5 };
    assert_eq!(level_adjusted_filter_bits(clamped, 10, TableLevel(9)), 10);
}

#[test]
fn deeper_levels_write_smaller_block_filters() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-layered-filter-{}-{}",
        std::process::id(),
        table_time_suffix()
    ));
    let records = (0_u64..600)
        .map(|index| {
            (
                InternalKey::new(
                    format!("key-{index:08}").into_bytes(),
                    Sequence::new(index + 1),
                    ValueKind::Put,
                    0,
                ),
                Some(ValueRef::Inline(format!("value-{index:08}").into_bytes())),
            )
        })
        .collect::<Vec<_>>();
    let options = test_table_options(CodecId::None, true);

    // Both levels are unpinned (no table-level filter), so the size gap is
    // purely the block-level bits/key curve: L4 uses fewer bits than L2.
    let shallow = write_table(
        &table_path(&root, TableId(2)),
        TableId(2),
        TableLevel(2),
        &options,
        &records,
        &[],
    )
    .expect("shallow table writes");
    let deep = write_table(
        &table_path(&root, TableId(4)),
        TableId(4),
        TableLevel(4),
        &options,
        &records,
        &[],
    )
    .expect("deep table writes");

    assert!(
        deep.estimated_file_bytes() < shallow.estimated_file_bytes(),
        "deep level should write smaller filters: deep={} shallow={}",
        deep.estimated_file_bytes(),
        shallow.estimated_file_bytes()
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn deeper_levels_write_smaller_prefix_filters() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-layered-prefix-{}-{}",
        std::process::id(),
        table_time_suffix()
    ));
    // Distinct 6-byte prefixes so the prefix filter holds many elements and
    // its per-prefix bit budget is visible in the encoded table size.
    let records = (0_u64..600)
        .map(|index| {
            (
                InternalKey::new(
                    format!("{index:06}---key").into_bytes(),
                    Sequence::new(index + 1),
                    ValueKind::Put,
                    0,
                ),
                Some(ValueRef::Inline(format!("value-{index:08}").into_bytes())),
            )
        })
        .collect::<Vec<_>>();
    // Prefix filter only (point filter disabled) isolates the prefix curve.
    let options = TableWriteOptions {
        codec: CodecId::None,
        block_bytes: 1024,
        filter_policy: FilterPolicy::Disabled,
        prefix_extractor: PrefixExtractor::FixedLen(6),
        prefix_filter_policy: PrefixFilterPolicy::Bloom {
            bits_per_prefix: 10,
        },
        filter_depth_curve: FilterDepthCurve::Auto,
        blob_threshold_bytes: BucketOptions::DEFAULT_BLOB_THRESHOLD_BYTES,
        rewrite_blob_indexes: false,
    };

    let shallow = write_table(
        &table_path(&root, TableId(2)),
        TableId(2),
        TableLevel(2),
        &options,
        &records,
        &[],
    )
    .expect("shallow table writes");
    let deep = write_table(
        &table_path(&root, TableId(4)),
        TableId(4),
        TableLevel(4),
        &options,
        &records,
        &[],
    )
    .expect("deep table writes");

    assert!(
        deep.estimated_file_bytes() < shallow.estimated_file_bytes(),
        "deep level should write smaller prefix filters: deep={} shallow={}",
        deep.estimated_file_bytes(),
        shallow.estimated_file_bytes()
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

    let records = data_block_point_records_for_key(&block, b"target", IndexSearchPolicy::Binary)
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
fn data_block_hash_index_mismatch_fails_closed() {
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

    let error = decode_data_block(encoded)
        .expect_err("data block decode should rebuild and compare the index");
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
        .point_records_for_key_with_cache(b"key-250", IndexSearchPolicy::Binary, Some(&block_cache))
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
        .point_records_for_key_with_cache(b"key-250", IndexSearchPolicy::Binary, Some(&block_cache))
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
fn deeper_table_metadata_survives_data_block_churn_in_small_cache() {
    let path = std::env::temp_dir().join(format!(
        "trine-kv-hot-cold-index-partition-{}-{}.trinet",
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
        TableId(1000),
        TableLevel(2),
        &options,
        &point_records,
        &[],
    )
    .expect("table writes and reopens");
    let block_cache = BlockCache::new(64 * 1024);
    table
        .point_records_for_key_with_cache(b"key-250", IndexSearchPolicy::Binary, Some(&block_cache))
        .expect("hot point lookup warms metadata");

    for index in 128..256 {
        if index == 250 {
            continue;
        }
        let key = format!("key-{index:03}");
        table
            .point_records_for_key_with_cache(
                key.as_bytes(),
                IndexSearchPolicy::Binary,
                Some(&block_cache),
            )
            .expect("same-partition point lookup churns data blocks");
    }

    let before_hot = block_cache.stats();
    table
        .point_records_for_key_with_cache(b"key-250", IndexSearchPolicy::Binary, Some(&block_cache))
        .expect("hot point lookup reuses metadata after data churn");
    let after_hot = block_cache.stats();

    assert!(
        after_hot.hits > before_hot.hits,
        "hot metadata should survive low-priority data block churn"
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
fn blob_threshold_is_capped_to_keep_inline_values_decodable() {
    let capped = effective_blob_threshold_bytes(usize::MAX);

    assert_eq!(capped, max_inline_value_bytes());
    assert!(capped < limits::MAX_DECODED_BLOCK_BYTES);
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

    let error =
        validate_data_block_filters(&entry, records).expect_err("missing block key should fail");
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

    let error =
        validate_data_block_filters(&entry, records).expect_err("missing block prefix should fail");
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
fn table_point_filter_false_negative_fails_closed() {
    let mut table = table_with_records(8, CodecId::None);
    table.point_key_filter =
        Some(PointKeyFilter::from_parts(1, 1, vec![0]).expect("test filter decodes"));
    let payload = encode_table(&table).expect("table encodes");

    let error =
        decode_table(&table_file_bytes(&payload)).expect_err("table filter miss fails closed");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(error.to_string().contains("table point-key filter"));
}

#[test]
fn table_prefix_filter_false_negative_fails_closed() {
    let mut table = table_with_records(8, CodecId::None);
    table.prefix_filter = Some(
        PrefixFilter::from_parts(PrefixExtractor::FixedLen(6), 1, 1, vec![0])
            .expect("test filter decodes"),
    );
    let payload = encode_table(&table).expect("table encodes");

    let error =
        decode_table(&table_file_bytes(&payload)).expect_err("prefix filter miss fails closed");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(error.to_string().contains("table prefix filter"));
}
