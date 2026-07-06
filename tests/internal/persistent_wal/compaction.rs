use super::*;
use super::read_stats::point_read_table_probes;

#[test]
fn persistent_compaction_levels_preserve_newer_l0_reads() {
    let path = temp_db_path("compaction-levels");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"old-a").expect("write old a");
        db.flush_sync().expect("flush first L0 table");
        bucket.put_sync(b"b", b"old-b").expect("write b");
        db.flush_sync().expect("flush second L0 table");
        assert_eq!(default_table_levels(&path), vec![0, 0]);

        db.compact_range_sync(KeyRange::all())
            .expect("compact L0 tables");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("compacted a reads"),
            Some(b"old-a".to_vec())
        );

        bucket.put_sync(b"a", b"new-a").expect("write newer L0 a");
        db.flush_sync().expect("flush newer L0 table");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer L0 a reads"),
            Some(b"new-a".to_vec())
        );

        db.compact_range_sync(KeyRange::all())
            .expect("compact L0 into L1");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("newer a survives second compaction"),
            Some(b"new-a".to_vec())
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer L0 a reopens"),
            Some(b"new-a".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("compacted b reopens"),
            Some(b"old-b".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_single_l0_compaction_moves_table_without_rewrite() {
    let path = temp_db_path("single-l0-trivial-move");
    let options = DbOptions::persistent(&path);
    let before_table_ids;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        db.flush_sync().expect("flush L0 table");
        before_table_ids = default_table_ids(&path);
        assert_eq!(default_table_levels(&path), vec![0]);
        assert_eq!(table_file_paths(&path).len(), 1);

        db.compact_range_sync(KeyRange::all())
            .expect("single L0 table moves down");
        assert_eq!(default_table_ids(&path), before_table_ids);
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(table_file_paths(&path).len(), 1);
        assert_eq!(
            bucket.get_sync(b"a").expect("moved table reads"),
            Some(b"a1".to_vec())
        );
        let stats = db.stats();
        assert_eq!(stats.compaction_runs, 1);
        assert_eq!(stats.compaction_input_tables, 1);
        assert_eq!(stats.compaction_output_tables, 1);
        assert!(stats.compaction_input_bytes > 0);
        assert!(stats.compaction_output_bytes > 0);
        assert_eq!(compaction_trigger_runs(&stats, CompactionTrigger::L0Overlap), 1);
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_ids(&path), before_table_ids);
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("moved table reopens"),
            Some(b"a1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_auto_compacts_when_l0_pressure_exceeds_limit() {
    let path = temp_db_path("auto-compact-l0");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 1;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        db.flush_sync().expect("first flush stays L0");
        assert_eq!(default_table_levels(&path), vec![0]);

        bucket.put_sync(b"b", b"b1").expect("write b");
        db.flush_sync().expect("second flush triggers compaction");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("a reads after auto compaction"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket
                .get_sync(b"b")
                .expect("b reads after auto compaction"),
            Some(b"b1".to_vec())
        );

        bucket.put_sync(b"a", b"a2").expect("write newer a");
        db.flush_sync().expect("new L0 below pressure limit");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer a reads over L1"),
            Some(b"a2".to_vec())
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_levels(&path), vec![0, 1]);
        assert_eq!(
            bucket.get_sync(b"a").expect("newer a reopens"),
            Some(b"a2".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b reopens"),
            Some(b"b1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_flush_auto_compacts_overlapping_l0_below_file_limit() {
    let path = temp_db_path("auto-compact-overlapping-l0");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a1");
        db.flush_sync().expect("first flush stays L0");
        assert_eq!(default_table_levels(&path), vec![0]);

        bucket.put_sync(b"a", b"a2").expect("write a2");
        db.flush_sync()
            .expect("second overlapping flush triggers compaction");
        assert_eq!(default_table_levels(&path), vec![1]);
        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("newer a reads after compaction"),
            Some(b"a2".to_vec())
        );
        assert!(db.stats().compaction_runs > 0);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_tombstone_only_table_hides_range_and_prefix_scans() {
    let path = temp_db_path("tombstone-only-scan-guard");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 64;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"user:1", b"old").expect("write user 1");
        bucket.put_sync(b"user:2", b"old").expect("write user 2");
        bucket.put_sync(b"order:1", b"keep").expect("write order");
        db.flush_sync().expect("flush point table");
        let snapshot = db.snapshot();

        bucket
            .delete_range_sync(KeyRange::all())
            .expect("delete all range");
        db.flush_sync().expect("flush tombstone-only table");

        assert_eq!(bucket.get_sync(b"user:1").expect("point hidden"), None);

        let before_range = db.stats();
        assert_eq!(
            collect_rows(bucket.range_sync(&KeyRange::all()).expect("range hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        let after_range = db.stats();
        assert!(
            after_range
                .read_path
                .range_tombstone_table_probes
                .saturating_sub(before_range.read_path.range_tombstone_table_probes)
                > 0,
            "range scans must inspect candidate tombstone tables"
        );

        assert_eq!(
            collect_rows(
                bucket
                    .range_reverse_sync(&KeyRange::all())
                    .expect("reverse range hidden")
            ),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );

        let before_prefix = db.stats();
        assert_eq!(
            collect_rows(bucket.prefix_sync(b"user:").expect("prefix hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        let after_prefix = db.stats();
        assert!(
            after_prefix
                .read_path
                .prefix_tombstone_table_probes
                .saturating_sub(before_prefix.read_path.prefix_tombstone_table_probes)
                > 0,
            "prefix scans must inspect candidate tombstone tables"
        );
        assert_eq!(
            collect_rows(
                bucket
                    .prefix_reverse_sync(b"user:")
                    .expect("reverse prefix hidden")
            ),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );

        assert_eq!(
            collect_rows(
                snapshot
                    .range_sync(&bucket, &KeyRange::all())
                    .expect("snapshot still sees old rows")
            ),
            vec![
                (b"order:1".to_vec(), b"keep".to_vec()),
                (b"user:1".to_vec(), b"old".to_vec()),
                (b"user:2".to_vec(), b"old".to_vec()),
            ]
        );
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(
            collect_rows(bucket.range_sync(&KeyRange::all()).expect("range hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        assert_eq!(
            collect_rows(bucket.prefix_sync(b"user:").expect("prefix hidden")),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_budgeted_compaction_reports_exhaustion_and_resumes() {
    let path = temp_db_path("budgeted-compaction-resumes");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let default_bucket = db.default_bucket_sync().expect("default bucket opens");
        let other_bucket = db.bucket_sync("other").expect("other bucket opens");

        default_bucket
            .put_sync(b"a", b"a1")
            .expect("write default key");
        other_bucket.put_sync(b"b", b"b1").expect("write other key");
        db.flush_sync().expect("flush both buckets");
        assert_eq!(db.stats().l0_tables, 2);

        let first = db
            .compact_range_with_budget_sync(KeyRange::all(), MaintenanceBudget::default())
            .expect("first budgeted compaction runs");
        assert_eq!(first.compactions, 1);
        assert!(first.budget_exhausted());
        assert!(!first.busy());
        assert_eq!(db.stats().l0_tables, 1);
        assert_eq!(db.stats().maintenance_budget_exhaustions, 1);

        let second = db
            .compact_range_with_budget_sync(KeyRange::all(), MaintenanceBudget::default())
            .expect("second budgeted compaction resumes");
        assert_eq!(second.compactions, 1);
        assert!(!second.budget_exhausted());
        assert_eq!(db.stats().l0_tables, 0);
        assert_eq!(level_table_count(&db.stats(), 1), 2);
        assert_eq!(
            default_bucket.get_sync(b"a").expect("default key reads"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            other_bucket.get_sync(b"b").expect("other key reads"),
            Some(b"b1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_budgeted_maintenance_flushes_one_input_per_pass() {
    let path = temp_db_path("budgeted-maintenance-flushes");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let default_bucket = db.default_bucket_sync().expect("default bucket opens");
        let other_bucket = db.bucket_sync("other").expect("other bucket opens");

        default_bucket
            .put_sync(b"a", b"a1")
            .expect("write default key");
        other_bucket.put_sync(b"b", b"b1").expect("write other key");
        assert_eq!(db.stats().immutable_memtables, 2);

        let first = db
            .run_maintenance_with_budget_sync(MaintenanceBudget::default())
            .expect("first budgeted maintenance runs");
        assert_eq!(first.flushes, 1);
        assert!(first.budget_exhausted());
        assert_eq!(db.stats().immutable_memtables, 1);
        assert_eq!(db.stats().l0_tables, 1);

        let second = db
            .run_maintenance_with_budget_sync(MaintenanceBudget::default())
            .expect("second budgeted maintenance resumes");
        assert_eq!(second.flushes, 1);
        assert!(!second.budget_exhausted());
        assert_eq!(db.stats().immutable_memtables, 0);
        assert_eq!(db.stats().l0_tables, 2);
        assert_eq!(
            default_bucket.get_sync(b"a").expect("default key reads"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            other_bucket.get_sync(b"b").expect("other key reads"),
            Some(b"b1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_bucket_reader_keeps_memtable_source_after_flush() {
    let path = temp_db_path("bucket-reader-keeps-memtable-source");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        bucket.put_sync(b"a", b"a1").expect("write a1");

        let snapshot = db.snapshot();
        let reader = bucket.reader(&snapshot).expect("reader opens");
        db.flush_sync().expect("flush after reader creation");

        let value = reader
            .get_sync(b"a")
            .expect("reader value lookup")
            .expect("value is visible");
        assert_eq!(value.as_bytes(), b"a1");
        assert_eq!(
            reader
                .get_owned_sync(b"a")
                .expect("reader sees pre-flush memtable"),
            Some(b"a1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_background_workers_flush_and_compact_pressure() {
    let path = temp_db_path("background-maintenance");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.max_l0_files = 2;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        bucket.put_sync(b"a", b"a1").expect("write a");
        bucket.put_sync(b"b", b"b1").expect("write b");
        bucket.put_sync(b"c", b"c1").expect("write c");
        wait_until("background batched flush of immutable memtables", || {
            let stats = db.stats();
            stats.immutable_memtables == 0 && (stats.l0_tables >= 3 || level_table_count(&stats, 1) >= 1)
        });

        wait_until("background compaction after L0 pressure", || {
            let stats = db.stats();
            stats.l0_tables <= 2 && level_table_count(&stats, 1) >= 1
        });

        assert_eq!(
            bucket
                .get_sync(b"a")
                .expect("a reads after background work"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket
                .get_sync(b"b")
                .expect("b reads after background work"),
            Some(b"b1".to_vec())
        );
        assert_eq!(
            bucket
                .get_sync(b"c")
                .expect("c reads after background work"),
            Some(b"c1".to_vec())
        );
        db.close_sync();
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let stats = db.stats();
        assert!(stats.l0_tables <= 2);
        assert!(level_table_count(&stats, 1) >= 1);
        assert_eq!(
            bucket.get_sync(b"a").expect("a reopens"),
            Some(b"a1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"b").expect("b reopens"),
            Some(b"b1".to_vec())
        );
        assert_eq!(
            bucket.get_sync(b"c").expect("c reopens"),
            Some(b"c1".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_background_maintenance_error_surfaces_to_later_write() {
    let path = temp_db_path("background-maintenance-error");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 4;
    options.background_worker_count = 1;

    {
        let db = Db::open_sync(options).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        let manifest_tmp_dir = manifest::manifest_path(&path).with_extension("tmp");
        fs::create_dir(&manifest_tmp_dir).expect("block manifest tmp path");
        bucket.put_sync(b"a", b"a1").expect("write schedules flush");

        let mut surfaced = false;
        for index in 0..100 {
            thread::sleep(std::time::Duration::from_millis(20));
            let key = format!("probe-{index:03}").into_bytes();
            match bucket.put_sync(key, b"value") {
                Err(Error::Io(error)) if error.to_string().contains("Is a directory") => {
                    surfaced = true;
                    break;
                }
                Ok(()) => {}
                Err(error) => panic!("unexpected write error: {error}"),
            }
        }
        assert!(
            surfaced,
            "background maintenance failure should reach a later write"
        );

        fs::remove_dir(&manifest_tmp_dir).expect("remove manifest tmp blocker");
        db.close_sync();
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_compaction_splits_outputs_and_moves_overfull_l1_down() {
    let path = temp_db_path("compaction-split-output");
    let mut options = DbOptions::persistent(&path);
    options.target_table_bytes = 240;
    options.level_size_multiplier = 2;
    let bucket_options = BucketOptions {
        compression: CompressionProfile::None,
        block_bytes: 256,
        ..BucketOptions::default()
    };
    options.default_bucket_options = bucket_options;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for index in 0..30 {
            let key = format!("key-{index:03}").into_bytes();
            let value = format!("value-{index:03}-{}", "x".repeat(48)).into_bytes();
            bucket.put_sync(key, value).expect("write first batch");
        }
        db.flush_sync().expect("flush first L0 table");
        for index in 30..60 {
            let key = format!("key-{index:03}").into_bytes();
            let value = format!("value-{index:03}-{}", "y".repeat(48)).into_bytes();
            bucket.put_sync(key, value).expect("write second batch");
        }
        db.flush_sync().expect("flush second L0 table");

        db.compact_range_sync(KeyRange::all())
            .expect("manual compaction splits L1 output");
        let levels = default_table_levels(&path);
        assert!(levels.len() > 1, "small target should split output tables");
        assert!(levels.iter().all(|level| *level == 1));

        db.compact_range_sync(KeyRange::all())
            .expect("overfull L1 compacts a narrow input into L2");
        let levels = default_table_levels(&path);
        assert!(
            levels.contains(&1),
            "narrow L1 compaction should leave unrelated L1 tables"
        );
        assert!(levels.contains(&2), "selected L1 input should move to L2");

        for index in [0, 17, 30, 59] {
            let key = format!("key-{index:03}").into_bytes();
            let expected_prefix = format!("value-{index:03}-").into_bytes();
            let value = bucket
                .get_sync(&key)
                .expect("value reads")
                .expect("key exists");
            assert!(value.starts_with(&expected_prefix));
        }
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let levels = default_table_levels(&path);
        assert!(levels.contains(&1));
        assert!(levels.contains(&2));
        assert_eq!(
            bucket.get_sync(b"key-059").expect("latest key reopens"),
            Some(format!("value-059-{}", "y".repeat(48)).into_bytes())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_multi_table_compaction_moves_one_guard_local_input() {
    let path = temp_db_path("multi-table-local-compaction");
    let mut options = DbOptions::persistent(&path);
    options.target_table_bytes = usize::MAX / 4;
    options.max_l0_files = 64;

    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");

        for chunk in 0..3 {
            let key = format!("key-{chunk:03}").into_bytes();
            let value = format!("value-{chunk:03}").into_bytes();
            bucket.put_sync(key, value).expect("write chunk key");
            db.flush_sync().expect("flush L0 table");
            db.compact_range_sync(KeyRange::all())
                .expect("move flushed table to L1");
        }
        assert_eq!(default_table_levels(&path), vec![1, 1, 1]);

        let before = db.stats();
        db.compact_range_sync(KeyRange::all())
            .expect("multi-table fallback moves one guard-local input");
        let after = db.stats();

        assert_eq!(default_table_levels(&path), vec![1, 1, 2]);
        assert_eq!(
            after
                .compaction_input_tables
                .saturating_sub(before.compaction_input_tables),
            1,
            "guard-local fallback should avoid rewriting every same-level table"
        );
        assert_eq!(
            after
                .compaction_output_tables
                .saturating_sub(before.compaction_output_tables),
            1
        );
        assert_eq!(
            compaction_trigger_runs(&after, CompactionTrigger::MultiTableLevel).saturating_sub(
                compaction_trigger_runs(&before, CompactionTrigger::MultiTableLevel)
            ),
            1
        );

        for chunk in 0..3 {
            let key = format!("key-{chunk:03}").into_bytes();
            let value = bucket
                .get_sync(&key)
                .expect("guard-local compaction reads")
                .expect("key exists");
            assert_eq!(value, format!("value-{chunk:03}").into_bytes());
        }
    }

    {
        let db = Db::open_sync(options).expect("persistent db reopens");
        let bucket = db.default_bucket_sync().expect("bucket reopens");
        assert_eq!(default_table_levels(&path), vec![1, 1, 2]);
        assert_eq!(
            bucket.get_sync(b"key-002").expect("key reopens"),
            Some(b"value-002".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_deep_level_stays_lazy_and_reports_skip() {
    let path = temp_db_path("deep-level-lazy");
    let mut options = DbOptions::persistent(&path);
    // A huge target keeps every level under its byte target so no LevelSize
    // trigger fires and only the non-uniform no-pressure policy is exercised.
    options.target_table_bytes = usize::MAX / 4;
    options.max_l0_files = 64;

    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");

    // Build three disjoint single-key L1 tables, matching the guard-local setup.
    for chunk in 0..3 {
        let key = format!("key-{chunk:03}").into_bytes();
        let value = format!("value-{chunk:03}").into_bytes();
        bucket.put_sync(key, value).expect("write chunk key");
        db.flush_sync().expect("flush L0 table");
        db.compact_range_sync(KeyRange::all())
            .expect("move flushed table to L1");
    }
    assert_eq!(default_table_levels(&path), vec![1, 1, 1]);

    // Drive the picker until it stabilizes. Shallow L1 keeps merging one table
    // down to L2 (tight budget of 2); once L1 is under budget and L2 holds two
    // non-overlapping tables (deep budget of 3), the policy leaves L2 lazy and
    // records the skip instead of spawning an L3.
    let table_probes_before = point_read_table_probes(&db, &bucket);
    let mut guard = 0;
    loop {
        let before = db.stats();
        db.compact_range_sync(KeyRange::all())
            .expect("compaction step succeeds");
        let after = db.stats();
        let made_progress = after.compaction_output_tables > before.compaction_output_tables;
        guard += 1;
        if !made_progress || guard > 16 {
            break;
        }
    }

    let stats = db.stats();
    let lazy_skips = stats
        .compaction_skips
        .iter()
        .find(|row| row.skip == CompactionSkip::LowerLevelLazy)
        .map_or(0, |row| row.occurrences);
    assert!(
        lazy_skips >= 1,
        "the deep level should be left lazy at least once: {:?}",
        stats.compaction_skips
    );

    // Read amplification does not regress: the deep level stays non-overlapping,
    // so point reads still probe at most one table per level.
    let table_probes_after = point_read_table_probes(&db, &bucket);
    assert!(
        table_probes_after <= table_probes_before,
        "lazy deep level must not increase point table probes ({table_probes_before} -> {table_probes_after})"
    );

    // Data is still correct after the lazy policy stabilizes.
    for chunk in 0..3 {
        let key = format!("key-{chunk:03}").into_bytes();
        assert_eq!(
            bucket.get_sync(&key).expect("deep-level read"),
            Some(format!("value-{chunk:03}").into_bytes())
        );
    }

    drop(bucket);
    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}
