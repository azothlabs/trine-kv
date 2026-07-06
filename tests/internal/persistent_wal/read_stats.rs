use super::*;


pub(super) fn point_read_table_probes(db: &Db, bucket: &trine_kv::Bucket) -> u64 {
    let before = db.stats();
    for chunk in 0..3 {
        let key = format!("key-{chunk:03}").into_bytes();
        let _ = bucket.get_sync(&key).expect("probe read");
    }
    let after = db.stats();
    after
        .read_path
        .point_table_probes
        .saturating_sub(before.read_path.point_table_probes)
}

#[test]
fn scan_waste_and_snapshot_lag_metrics_report_gc_health() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    // Version bloat: three versions of four keys.
    for round in 0..3_u32 {
        for k in 0..4_u32 {
            bucket
                .put_sync(
                    format!("k{k}").into_bytes(),
                    format!("v{round}-{k}").into_bytes(),
                )
                .expect("put version");
        }
    }
    // Two keys hidden by a point delete.
    bucket.delete_sync(b"k0".to_vec()).expect("delete k0");
    bucket.delete_sync(b"k1".to_vec()).expect("delete k1");

    // Drain a full range scan.
    let mut returned = 0_u64;
    let mut iter = bucket.range_sync(&KeyRange::all()).expect("range scan opens");
    while let Some(row) = iter.next_sync() {
        row.expect("scan row");
        returned += 1;
    }
    drop(iter);

    let stats = db.stats();
    assert_eq!(returned, 2, "only k2 and k3 remain live");
    assert_eq!(stats.scan_user_keys, returned);
    assert!(
        stats.scan_internal_records > stats.scan_user_keys,
        "version bloat: internal {} should exceed user {}",
        stats.scan_internal_records,
        stats.scan_user_keys
    );
    assert!(
        stats.scan_tombstone_hidden_keys >= 2,
        "k0 and k1 are delete-hidden: {}",
        stats.scan_tombstone_hidden_keys
    );

    // Snapshot version-debt: holding a snapshot pins old versions.
    assert_eq!(db.stats().oldest_snapshot_lag, 0);
    let snapshot = db.snapshot();
    for k in 0..4_u32 {
        bucket
            .put_sync(format!("k{k}").into_bytes(), b"newer".to_vec())
            .expect("put after snapshot");
    }
    let during = db.stats();
    assert_eq!(during.active_snapshots, 1);
    assert!(
        during.oldest_snapshot_lag > 0,
        "held snapshot should hold version debt: {}",
        during.oldest_snapshot_lag
    );
    drop(snapshot);
    assert_eq!(
        db.stats().oldest_snapshot_lag,
        0,
        "dropping the snapshot clears version debt"
    );
}

#[test]
fn scan_skips_fully_covered_table_on_read_path() {
    let path = temp_db_path("scan-skip-covered");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    // Table 1: 50 "a*" keys. Table 2: 50 "z*" keys. Separate flushes.
    for i in 0..50_u32 {
        bucket
            .put_sync(format!("a{i:02}").into_bytes(), b"v".to_vec())
            .expect("put a");
    }
    db.flush_sync().expect("flush table a");
    for i in 0..50_u32 {
        bucket
            .put_sync(format!("z{i:02}").into_bytes(), b"v".to_vec())
            .expect("put z");
    }
    db.flush_sync().expect("flush table z");

    // Delete the whole "a*" table with one range tombstone (fresh, not compacted,
    // so the "a*" table is still physically present but fully hidden on read).
    db.delete_range_sync(KeyRange::half_open(b"a".to_vec(), b"b".to_vec()))
        .expect("range delete a*");
    db.flush_sync().expect("flush tombstone");

    let before = db.stats();
    let mut iter = bucket.range_sync(&KeyRange::all()).expect("range scan");
    let mut returned = 0_u64;
    while let Some(row) = iter.next_sync() {
        row.expect("scan row");
        returned += 1;
    }
    drop(iter);
    let after = db.stats();

    // Correctness: only the live "z*" keys come back.
    assert_eq!(returned, 50, "only z* keys are live");

    let internal = after
        .scan_internal_records
        .saturating_sub(before.scan_internal_records);
    let user = after.scan_user_keys.saturating_sub(before.scan_user_keys);
    let hidden = after
        .scan_tombstone_hidden_keys
        .saturating_sub(before.scan_tombstone_hidden_keys);
    assert_eq!(user, 50);
    // The covered "a*" table was skipped without reading: its 50 records are
    // neither merged (internal stays ~= user) nor counted as delete-hidden.
    assert!(
        internal <= user + 5,
        "covered table should be skipped, not merged: internal {internal} user {user}"
    );
    assert!(
        hidden <= 5,
        "skipped table keys must not be read as delete-hidden: {hidden}"
    );

    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn bulk_range_delete_drops_covered_tables_by_file() {
    let path = temp_db_path("drop-covered-by-file");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    for index in 0..200_u32 {
        bucket
            .put_sync(format!("k{index:04}").into_bytes(), vec![b'x'; 64])
            .expect("put");
    }
    db.flush_sync().expect("flush data");
    db.compact_range_sync(KeyRange::all()).expect("settle into a level");
    let before = db.stats();
    assert!(before.total_tables >= 1, "data tables exist before the drop");

    // Bulk-delete everything, then compact. No active snapshots, so the
    // bucket-wide range tombstone is retention-safe and the fully covered data
    // tables are dropped by file instead of rewritten.
    db.delete_range_sync(KeyRange::all()).expect("range delete");
    db.flush_sync().expect("flush range tombstone");
    db.compact_range_sync(KeyRange::all())
        .expect("compaction drops covered tables");

    for index in 0..200_u32 {
        assert_eq!(
            bucket
                .get_sync(format!("k{index:04}").as_bytes())
                .expect("get after drop"),
            None,
            "covered key must be gone"
        );
    }
    let after = db.stats();
    assert!(
        after.table_bytes <= before.table_bytes,
        "covered data must not be rewritten: before {} after {}",
        before.table_bytes,
        after.table_bytes
    );

    // Reopen proves the drop is durable.
    drop(bucket);
    drop(db);
    let db = Db::open_sync(DbOptions::persistent(&path)).expect("reopen");
    let bucket = db.default_bucket_sync().expect("bucket reopens");
    assert_eq!(
        bucket.get_sync(b"k0000").expect("get after reopen"),
        None
    );
    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_level_filter_stats_aggregate_by_level() {
    let path = temp_db_path("level-filter-stats");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 64;
    // Deterministic level layout: no background maintenance moves tables.
    options.background_worker_count = 0;

    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    let write_chunk = |chunk: usize| {
        for index in 0..64 {
            let row = chunk * 64 + index;
            let key = format!("key-{row:05}").into_bytes();
            bucket.put_sync(key, b"v").expect("write key");
        }
    };

    // First two chunks flush to L0 then compact down to L1.
    for chunk in 0..2 {
        write_chunk(chunk);
        db.flush_sync().expect("flush L0 table");
    }
    db.compact_range_sync(KeyRange::all())
        .expect("compact L0 into L1");
    // Next two chunks flush to L0 and stay there (no compaction).
    for chunk in 2..4 {
        write_chunk(chunk);
        db.flush_sync().expect("flush L0 table");
    }
    assert!(
        db.stats().l0_tables >= 1,
        "workload must leave tables on L0"
    );

    // In-range missing-key lookups exercise the table point filter (the keys sit
    // inside existing tables' key bounds but were never written).
    for row in 0..256 {
        let key = format!("key-{row:05}-absent").into_bytes();
        assert_eq!(bucket.get_sync(&key).expect("missing read"), None);
    }

    let stats = db.stats();
    assert!(
        !stats.level_filters.is_empty(),
        "per-level filter stats must be reported"
    );
    assert!(
        stats.level_filters.len() >= 2,
        "more than one level should hold tables: {:?}",
        stats.level_filters
    );

    // Per-level rollup must reconcile with the global totals and table counts.
    let summed_hits: u64 = stats
        .level_filters
        .iter()
        .map(|row| row.filters.table_point_hits)
        .sum();
    let summed_fp: u64 = stats
        .level_filters
        .iter()
        .map(|row| row.filters.table_point_false_positives)
        .sum();
    let summed_misses: u64 = stats
        .level_filters
        .iter()
        .map(|row| row.filters.table_point_misses)
        .sum();
    assert_eq!(summed_hits, stats.filters.table_point_hits);
    assert_eq!(summed_fp, stats.filters.table_point_false_positives);
    assert_eq!(summed_misses, stats.filters.table_point_misses);

    let level_filter_tables: usize = stats.level_filters.iter().map(|row| row.tables).sum();
    let level_tables: usize = stats.level_tables.iter().map(|row| row.tables).sum();
    assert_eq!(level_filter_tables, level_tables);

    // The missing-key sweep must have exercised the point filter somewhere.
    let absent_probes = stats
        .filters
        .table_point_false_positives
        .saturating_add(stats.filters.table_point_misses);
    assert!(
        absent_probes > 0,
        "missing-key lookups should reach the table point filter"
    );

    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_stats_report_tables_blobs_and_compactions() {
    let path = temp_db_path("live-stats");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 1;
    let bucket_options = BucketOptions {
        blob_threshold_bytes: 4,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        assert_eq!(db.stats().live_buckets, 1);

        let large_a = b"large-a".to_vec();
        bucket
            .put_sync(b"a", large_a.clone())
            .expect("write large a");
        assert!(
            db.stats().memtable_bytes > 0,
            "unflushed writes should contribute to memtable stats"
        );
        db.flush_sync().expect("first flush stays L0");
        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.l0_tables, 1);
        assert_eq!(level_table_count(&stats, 0), 1);
        assert!(stats.table_bytes > 0);
        assert_eq!(level_table_bytes(&stats, 0), stats.table_bytes);
        assert_eq!(stats.live_blob_files, 1);
        assert_eq!(stats.live_blob_bytes, large_a.len() as u64);

        let large_b = b"large-b".to_vec();
        bucket
            .put_sync(b"b", large_b.clone())
            .expect("write large b");
        db.flush_sync().expect("second flush triggers compaction");
        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.l0_tables, 0);
        assert_eq!(level_table_count(&stats, 0), 0);
        assert_eq!(level_table_count(&stats, 1), 1);
        assert!(level_table_bytes(&stats, 1) > 0);
        assert_eq!(stats.live_blob_files, 1);
        assert_eq!(
            stats.live_blob_bytes,
            (large_a.len() + large_b.len()) as u64
        );
        assert_eq!(stats.compaction_runs, 1);
        assert_eq!(stats.compaction_input_tables, 2);
        assert_eq!(stats.compaction_output_tables, 1);
        assert!(stats.compaction_input_bytes > 0);
        assert!(stats.compaction_output_bytes > 0);
        assert_eq!(compaction_trigger_runs(&stats, CompactionTrigger::L0Overlap), 1);

        let obsolete_blob_path = blob::blob_path(&path, 999);
        write_file(&obsolete_blob_path, b"obsolete");
        let stats = db.stats();
        assert_eq!(stats.obsolete_blob_files, 1);
        assert_eq!(stats.obsolete_blob_bytes, b"obsolete".len() as u64);
        assert_eq!(stats.stale_blob_files, 1);
        assert_eq!(stats.stale_blob_bytes, b"obsolete".len() as u64);
        fs::remove_file(obsolete_blob_path).expect("remove test obsolete blob");
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_block_cache_records_hits_and_misses() {
    let path = temp_db_path("block-cache-stats");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..64 {
            bucket
                .put_sync(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush_sync().expect("flush table");

        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(stats.block_cache_misses, 0);

        assert_eq!(
            bucket.get_sync(b"key-032").expect("first cached read"),
            Some(b"value-032".to_vec())
        );
        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert!(
            stats.block_cache_misses > 0,
            "first table block read should miss cache"
        );
        let misses = stats.block_cache_misses;

        assert_eq!(
            bucket.get_sync(b"key-032").expect("second cached read"),
            Some(b"value-032".to_vec())
        );
        let stats = db.stats();
        assert!(stats.block_cache_hits > 0);
        assert_eq!(stats.block_cache_misses, misses);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_range_iterator_defers_table_block_reads_until_next() {
    let path = temp_db_path("range-iterator-lazy-block-read");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..64 {
            bucket
                .put_sync(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush_sync().expect("flush table");

        let mut iter = bucket
            .range_sync(&KeyRange::all())
            .expect("range cursor is created");
        let stats = db.stats();
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(
            stats.block_cache_misses, 0,
            "constructing a range cursor should not touch table blocks"
        );

        let first = iter
            .next_sync()
            .expect("first row exists")
            .expect("first row reads");
        assert_eq!(first.key, b"key-000".to_vec());
        assert_eq!(first.value, b"value-000".to_vec());

        let stats = db.stats();
        assert!(
            stats.block_cache_misses > 0,
            "first iterator advance should touch the table block"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_range_iterator_keeps_active_memtable_after_flush() {
    let path = temp_db_path("range-iterator-memtable-handle");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"key-010", b"before-a").expect("write row");
        bucket.put_sync(b"key-020", b"before-b").expect("write row");

        let iter = bucket
            .range_sync(&KeyRange::all())
            .expect("range cursor is created");
        db.flush_sync().expect("flush active memtable");
        bucket
            .put_sync(b"key-000", b"after")
            .expect("write later row");

        assert_eq!(
            collect_rows(iter),
            vec![
                (b"key-010".to_vec(), b"before-a".to_vec()),
                (b"key-020".to_vec(), b"before-b".to_vec()),
            ]
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_transaction_read_range_consumes_scan_before_tracking() {
    let path = temp_db_path("transaction-read-range-consumes-scan");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..64 {
            bucket
                .put_sync(
                    format!("key-{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .expect("write row");
        }
        db.flush_sync().expect("flush table");
        assert_eq!(db.stats().block_cache_misses, 0);

        let mut txn = db.transaction(TransactionOptions::default());
        txn.read_range_sync(KeyRange::all())
            .expect("transaction range read succeeds");

        assert!(
            db.stats().block_cache_misses > 0,
            "transaction range read should advance the table cursor"
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_transaction_range_bucket_returns_data_and_tracks_conflict() {
    let path = temp_db_path("transaction-range-bucket-returns-data");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"k1", b"v1").expect("write k1");
        bucket.put_sync(b"k2", b"v2").expect("write k2");

        // Unlike `read_range`, a transaction range read returns the data ...
        let mut txn = db.transaction(TransactionOptions::default());
        let rows: Vec<(Vec<u8>, Vec<u8>)> = txn
            .range_sync(KeyRange::all())
            .expect("range read returns a cursor")
            .map(|item| {
                let kv = item.expect("row decodes");
                (kv.key, kv.value)
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ]
        );

        // ... and the range is recorded in the read set, so a later committed
        // write inside it makes this transaction's commit conflict.
        bucket
            .put_sync(b"k3", b"v3")
            .expect("write inside the read range");
        txn.put(b"k4", b"v4");
        let error = txn
            .commit_sync()
            .expect_err("a committed write inside the read range conflicts");
        assert!(matches!(error, Error::Conflict { .. }));
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_preserves_snapshot_versions() {
    let path = temp_db_path("flush-snapshot");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"v1").expect("write v1");
        let snapshot = db.snapshot();
        bucket.put_sync(b"a", b"v2").expect("write v2");

        db.flush_sync().expect("flush table");

        assert_eq!(
            snapshot
                .get_sync(&bucket, b"a")
                .expect("snapshot reads table"),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"a").expect("current reads table"),
            Some(b"v2".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_table_block_index_reads_points_and_ranges() {
    let path = temp_db_path("table-block-index");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..160 {
            bucket
                .put_sync(
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
                .expect("write indexed row");
        }
        db.flush_sync().expect("flush indexed table");

        assert_eq!(
            bucket
                .get_sync(b"key-042")
                .expect("point reads indexed table"),
            Some(b"value-042".to_vec())
        );
        let rows = bucket
            .range_sync(&KeyRange::half_open(b"key-020", b"key-030"))
            .expect("range reads indexed table")
            .map(|item| {
                let item = item.expect("range item reads");
                (item.key, item.value)
            })
            .collect::<Vec<_>>();
        let expected = (20..30)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, expected);

        let prefix_rows = collect_rows(bucket.prefix_sync(b"key-12").expect("prefix reads table"));
        let expected_prefix = (120..130)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(prefix_rows, expected_prefix);
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after block-index flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens from indexed table");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            bucket
                .get_sync(b"key-127")
                .expect("point reads after reopen"),
            Some(b"value-127".to_vec())
        );
        let rows = bucket
            .range_sync(&KeyRange::half_open(b"key-150", b"key-160"))
            .expect("range reads after reopen")
            .map(|item| {
                let item = item.expect("range item reads after reopen");
                (item.key, item.value)
            })
            .collect::<Vec<_>>();
        let expected = (150..160)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, expected);

        let prefix_rows = collect_rows(
            bucket
                .prefix_sync(b"key-12")
                .expect("prefix reads after reopen"),
        );
        let expected_prefix = (120..130)
            .map(|index| {
                (
                    format!("key-{index:03}").into_bytes(),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(prefix_rows, expected_prefix);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_index_search_policies_preserve_table_reads() {
    let path = temp_db_path("table-search-policies");
    let options = DbOptions::persistent(&path);
    let policies = [
        ("linear", IndexSearchPolicy::Linear),
        ("binary", IndexSearchPolicy::Binary),
        ("auto", IndexSearchPolicy::Auto),
    ];

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        for (name, policy) in policies {
            let bucket_options = BucketOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..BucketOptions::default()
            };
            let bucket = db
                .bucket_with_options_sync(name, bucket_options)
                .expect("policy bucket opens");
            for index in 0..80 {
                bucket
                    .put_sync(
                        format!("key-{index:03}").into_bytes(),
                        format!("value-{index:03}").into_bytes(),
                    )
                    .expect("write policy row");
            }
        }
        db.flush_sync().expect("flush policy tables");

        for (name, policy) in policies {
            let bucket_options = BucketOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..BucketOptions::default()
            };
            let bucket = db
                .bucket_with_options_sync(name, bucket_options)
                .expect("policy bucket reuses options");
            assert_eq!(
                bucket.get_sync(b"key-042").expect("policy point reads"),
                Some(b"value-042".to_vec())
            );
            assert_eq!(
                collect_rows(
                    bucket
                        .range_sync(&KeyRange::half_open(b"key-020", b"key-023"))
                        .expect("policy range reads")
                ),
                vec![
                    (b"key-020".to_vec(), b"value-020".to_vec()),
                    (b"key-021".to_vec(), b"value-021".to_vec()),
                    (b"key-022".to_vec(), b"value-022".to_vec()),
                ],
                "policy {policy:?} range changed"
            );
            assert_eq!(
                collect_rows(bucket.prefix_sync(b"key-04").expect("policy prefix reads")),
                (40..50)
                    .map(|index| {
                        (
                            format!("key-{index:03}").into_bytes(),
                            format!("value-{index:03}").into_bytes(),
                        )
                    })
                    .collect::<Vec<_>>(),
                "policy {policy:?} prefix changed"
            );
        }
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after search policy flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        for (name, policy) in policies {
            let bucket_options = BucketOptions {
                index_search_policy: policy,
                prefix_extractor: PrefixExtractor::FixedLen(6),
                ..BucketOptions::default()
            };
            let bucket = db
                .bucket_with_options_sync(name, bucket_options)
                .expect("policy bucket reopens");
            assert_eq!(
                bucket.get_sync(b"key-042").expect("policy point reopens"),
                Some(b"value-042".to_vec())
            );
        }
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_table_compression_profiles_round_trip() {
    let path = temp_db_path("table-compression");
    let options = DbOptions::persistent(&path);
    let fast_options = BucketOptions::default();
    let plain_options = BucketOptions {
        compression: CompressionProfile::None,
        ..BucketOptions::default()
    };

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let fast = db
            .bucket_with_options_sync("fast", fast_options.clone())
            .expect("fast bucket opens");
        let plain = db
            .bucket_with_options_sync("plain", plain_options.clone())
            .expect("plain bucket opens");

        for index in 0..64 {
            let value = format!("value-{index:03}-aaaaaaaaaaaaaaaaaaaaaaaa").into_bytes();
            fast.put_sync(format!("key-{index:03}").into_bytes(), value.clone())
                .expect("write fast row");
            plain
                .put_sync(format!("key-{index:03}").into_bytes(), value)
                .expect("write plain row");
        }
        db.flush_sync().expect("flush compressed tables");

        let manifest_state =
            manifest::read_manifest(&manifest::manifest_path(&path)).expect("manifest reads");
        assert_eq!(
            manifest_state
                .tables()
                .get("fast")
                .and_then(|tables| tables.first())
                .expect("fast table metadata")
                .codec,
            CodecId::FastLz4Block
        );
        assert_eq!(
            manifest_state
                .tables()
                .get("plain")
                .and_then(|tables| tables.first())
                .expect("plain table metadata")
                .codec,
            CodecId::None
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after compressed flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens from compressed tables");
        let fast = db
            .bucket_with_options_sync("fast", fast_options)
            .expect("fast bucket reopens");
        let plain = db
            .bucket_with_options_sync("plain", plain_options)
            .expect("plain bucket reopens");

        assert_eq!(
            fast.get_sync(b"key-042")
                .expect("fast row reads after reopen"),
            Some(b"value-042-aaaaaaaaaaaaaaaaaaaaaaaa".to_vec())
        );
        assert_eq!(
            plain
                .get_sync(b"key-042")
                .expect("plain row reads after reopen"),
            Some(b"value-042-aaaaaaaaaaaaaaaaaaaaaaaa".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_prefix_filter_keeps_range_tombstones_authoritative() {
    let path = temp_db_path("prefix-filter-tombstones");
    let mut options = DbOptions::persistent(&path);
    let bucket_options = BucketOptions {
        prefix_extractor: PrefixExtractor::Separator(b':'),
        prefix_filter_policy: PrefixFilterPolicy::Bloom {
            bits_per_prefix: 32,
        },
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"user:1", b"old").expect("write old user");
        bucket
            .put_sync(b"user:2", b"live")
            .expect("write live user");
        db.flush_sync().expect("flush user table");

        bucket.put_sync(b"post:1", b"post").expect("write post");
        bucket
            .delete_range_sync(KeyRange::half_open(b"user:1", b"user:2"))
            .expect("range delete one user");
        db.flush_sync()
            .expect("flush post table with user tombstone");

        assert_eq!(
            collect_rows(bucket.prefix_sync(b"user:").expect("prefix reads users")),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
        assert_eq!(
            collect_rows(bucket.prefix_sync(b"us").expect("short prefix falls back")),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after prefix-filter flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            collect_rows(
                bucket
                    .prefix_sync(b"user:")
                    .expect("prefix reads after reopen")
            ),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
        assert_eq!(
            collect_rows(
                bucket
                    .prefix_sync(b"us")
                    .expect("short prefix after reopen")
            ),
            vec![(b"user:2".to_vec(), b"live".to_vec())]
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_point_filter_keeps_range_tombstones_authoritative() {
    let path = temp_db_path("point-filter-tombstones");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"user:1", b"old").expect("write old user");
        db.flush_sync().expect("flush user table");

        bucket.put_sync(b"post:1", b"post").expect("write post");
        bucket
            .delete_range_sync(KeyRange::half_open(b"user:1", b"user:2"))
            .expect("range delete user");
        db.flush_sync()
            .expect("flush post table with user tombstone");

        assert_eq!(bucket.get_sync(b"user:1").expect("user is hidden"), None);
        assert_eq!(
            bucket.get_sync(b"post:1").expect("post survives"),
            Some(b"post".to_vec())
        );
    }

    fs::remove_file(wal::wal_path(&path)).expect("remove WAL after point-filter flush");

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");

        assert_eq!(
            bucket.get_sync(b"user:1").expect("user remains hidden"),
            None
        );
        assert_eq!(
            bucket.get_sync(b"post:1").expect("post survives reopen"),
            Some(b"post".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}
