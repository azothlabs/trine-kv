use super::*;
#[cfg(target_os = "wasi")]
use crate::{FailOnCorruptionPolicy, WriteOptions, recovery, table};

#[test]
fn maintenance_success_does_not_clear_unreported_error() {
    let coordinator = MaintenanceCoordinator::new();
    coordinator.record_error(&Error::Corruption {
        message: "publish failed".to_string(),
    });

    record_maintenance_success(&coordinator);

    let error = coordinator
        .take_error()
        .expect("unreported background error remains visible");
    assert!(matches!(error, Error::Corruption { message } if message == "publish failed"));
    assert!(coordinator.take_error().is_none());
}

#[test]
fn maintenance_error_preserves_runtime_busy_category() {
    let coordinator = MaintenanceCoordinator::new();
    coordinator.record_error(&Error::runtime_busy("flush already active"));

    let error = coordinator
        .take_error()
        .expect("unreported background error remains visible");
    assert!(matches!(error, Error::RuntimeBusy { message } if message == "flush already active"));
}

#[test]
fn maintenance_error_preserves_structured_fencing_fields() {
    let coordinator = MaintenanceCoordinator::new();
    coordinator.record_error(&Error::Fenced {
        held_epoch: 7,
        current_epoch: 8,
    });

    let error = coordinator
        .take_error()
        .expect("unreported background error remains visible");
    assert!(matches!(
        error,
        Error::Fenced {
            held_epoch: 7,
            current_epoch: 8
        }
    ));
}

#[test]
fn background_shutdown_cancels_runtime_token() {
    let maintenance = Arc::new(MaintenanceCoordinator::new());
    let runtime_shutdown = CancellationToken::new();
    let workers = Mutex::new(Vec::new());

    shutdown_background_workers(&maintenance, &runtime_shutdown, &workers);

    assert!(runtime_shutdown.is_cancelled());
}

#[test]
fn background_maintenance_budget_tracks_pressure_thresholds() {
    let mut options = DbOptions::memory();
    options.max_immutable_memtables = 6;
    options.max_l0_files = 3;
    let db = Db::open_sync(options).expect("memory db opens");

    let budget = db.background_maintenance_budget();

    assert_eq!(budget.max_flush_inputs(), 6);
    assert_eq!(budget.max_compaction_inputs(), 4);
    assert_eq!(db.background_flush_request_threshold(), 5);
    assert_eq!(
        Db::background_maintenance_progress_wait(),
        BACKGROUND_MAINTENANCE_PROGRESS_WAIT
    );
}

#[test]
fn background_flush_request_threshold_keeps_tiny_pressure_foreground() {
    let mut options = DbOptions::memory();
    options.max_immutable_memtables = 2;
    let db = Db::open_sync(options).expect("memory db opens");

    assert_eq!(db.background_flush_request_threshold(), 3);
}

#[test]
fn write_pressure_maintenance_reports_foreground_progress() {
    let path = temp_db_path("write-pressure-foreground-maintenance");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    options.write_buffer_bytes = 1;
    options.max_immutable_memtables = 2;
    options.max_l0_files = 64;
    let db = Db::open_sync(options).expect("open db");

    db.put_sync(b"a", b"one").expect("write first immutable");
    db.put_sync(b"b", b"two").expect("write second immutable");
    let pressure = db.write_pressure().expect("inspect write pressure");
    assert!(pressure.flush);

    let outcome = db
        .run_maintenance_for_pressure(&path, pressure)
        .expect("foreground pressure maintenance");

    assert!(outcome.made_progress());
    assert_eq!(outcome.flushes, 2);
    assert!(!outcome.busy());
    assert_eq!(db.stats().immutable_memtables, 0);

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_open_attaches_runtime_enabled_native_storage_backend() {
    let path = temp_db_path("persistent-runtime-native-storage");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    options.default_bucket_options = options.default_bucket_options.with_blob_threshold_bytes(4);
    let db = Db::open_sync(options).expect("persistent db opens");

    let capabilities = db.inner.native_storage.capabilities();
    assert!(capabilities.supports(StorageCapability::AsyncTasks));
    assert!(capabilities.supports(StorageCapability::BlockingAdapter));
    assert!(capabilities.supports(StorageCapability::BackgroundThreads));
    assert!(!capabilities.supports(StorageCapability::PlatformAsyncIo));

    let value = b"value-stored-through-blob".to_vec();
    db.put_sync(b"key", value.clone()).expect("write");
    db.flush_sync()
        .expect("flush through db-owned native storage");
    assert_eq!(
        db.get_sync(b"key").expect("read after flush"),
        Some(value.clone())
    );
    let stats = db.stats();
    assert_eq!(stats.live_blob_files, 1);
    assert!(stats.live_blob_bytes >= value.len() as u64);
    assert!(stats.storage_uses_sync_adapter);
    assert!(!stats.storage_uses_platform_io_driver);
    assert!(!stats.storage_uses_platform_async_io);
    assert_eq!(stats.storage_sync_adapter_queue_capacity, 1024);
    assert!(stats.storage_sync_adapter_submitted_tasks >= stats.storage_sync_adapter_tasks);
    assert!(stats.storage_operations.open_append.requests > 0);
    assert!(stats.storage_operations.write_object.requests > 0);
    assert_eq!(stats.storage_inline_tasks, 0);

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn native_read_only_open_skips_writer_lease_and_rejects_writes() {
    let path = temp_db_path("native-read-only-open");
    let mut writable_options = DbOptions::persistent(&path);
    writable_options.background_worker_count = 0;
    {
        let db = Db::open_sync(writable_options.clone()).expect("persistent db opens");
        db.put_sync(b"key", b"value").expect("write succeeds");
        db.flush_sync().expect("flush succeeds");
    }

    let db = Db::open_sync(writable_options.read_only()).expect("read-only db opens");

    assert!(db.options().read_only);
    assert!(!db.inner.substrate.wal_is_present());
    assert_eq!(
        db.get_sync(b"key").expect("read-only read succeeds"),
        Some(b"value".to_vec())
    );
    assert!(matches!(
        db.put_sync(b"other", b"value"),
        Err(Error::ReadOnly)
    ));
    assert_eq!(db.stats().storage_operations.read_object_bytes.requests, 0);
    assert_eq!(
        db.stats().storage_operations.acquire_writer_lease.requests,
        0
    );

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn native_read_only_open_replays_non_empty_wal() {
    let path = temp_db_path("native-read-only-open-wal-replay");
    let mut writable_options = DbOptions::persistent(&path);
    writable_options.background_worker_count = 0;
    {
        let db = Db::open_sync(writable_options.clone()).expect("persistent db opens");
        db.put_sync(b"key", b"value").expect("write succeeds");
    }

    let db = Db::open_sync(writable_options.read_only()).expect("read-only db opens");

    assert_eq!(
        db.get_sync(b"key").expect("read-only WAL read succeeds"),
        Some(b"value".to_vec())
    );
    assert!(
        db.stats().storage_operations.read_object_bytes.requests > 0,
        "read-only open must read non-empty WAL shards"
    );

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn wasi_persistent_backend_requires_wasi_target() {
    let path = temp_db_path("wasi-persistent-host-unsupported");
    let options = DbOptions::wasi_persistent(&path);
    assert_eq!(options.runtime.mode, crate::runtime::RuntimeMode::Inline);
    assert_eq!(options.background_worker_count, 0);

    let wasi_error = Db::open_sync(options).expect_err("WASI backend requires WASI target");
    assert!(matches!(wasi_error, Error::UnsupportedBackend { .. }));
    assert!(wasi_error.to_string().contains("WASI persistent"));
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn wasi_persistent_open_async_requires_wasi_target() {
    let path = temp_db_path("wasi-persistent-async-host-unsupported");
    let error = block_on_test_future(Db::open(DbOptions::wasi_persistent(&path)))
        .expect_err("WASI async open requires WASI target");

    assert!(matches!(error, Error::UnsupportedBackend { .. }));
    assert!(error.to_string().contains("WASI persistent"));
}

#[cfg(target_os = "wasi")]
#[test]
fn wasi_persistent_backend_uses_host_filesystem() {
    let path = temp_db_path("wasi-persistent-host");
    let db = Db::open_sync(DbOptions::wasi_persistent(&path)).expect("WASI db opens");
    db.put_sync(b"key", b"value").expect("WASI write succeeds");
    db.flush_sync().expect("WASI flush succeeds");
    drop(db);

    let db = Db::open_sync(DbOptions::wasi_persistent_read_only(&path))
        .expect("WASI read-only db reopens");
    assert_eq!(
        db.get_sync(b"key").expect("WASI read succeeds"),
        Some(b"value".to_vec())
    );
    drop(db);

    cleanup_wasi_temp_db_path(path);
}

#[cfg(target_os = "wasi")]
#[test]
fn wasi_persistent_writable_reopen_releases_lock_file() {
    let path = temp_db_path("wasi-persistent-writable-reopen");
    let db = Db::open_sync(DbOptions::wasi_persistent(&path)).expect("WASI db opens");
    db.put_sync(b"key", b"value").expect("WASI write succeeds");
    drop(db);

    let db = Db::open_sync(DbOptions::wasi_persistent(&path))
        .expect("WASI writable reopen should not see a stale lock");
    assert_eq!(
        db.get_sync(b"key").expect("WASI read succeeds"),
        Some(b"value".to_vec())
    );
    drop(db);

    cleanup_wasi_temp_db_path(path);
}

#[cfg(target_os = "wasi")]
#[test]
fn wasi_persistent_write_rejects_strict_sync_durability() {
    let path = temp_db_path("wasi-persistent-strict-write");
    let db = Db::open_sync(DbOptions::wasi_persistent(&path)).expect("WASI db opens");

    let error = db
        .put_with_options_sync(b"key", b"value", WriteOptions::sync_all_strict())
        .expect_err("WASI write rejects strict sync durability");
    assert!(matches!(
        error,
        Error::UnsupportedDurability {
            requested: DurabilityMode::SyncAllStrict
        }
    ));
    drop(db);

    cleanup_wasi_temp_db_path(path);
}

#[cfg(target_os = "wasi")]
#[test]
fn wasi_persistent_safe_temp_repair_does_not_remove_leftover_lock() {
    let path = temp_db_path("wasi-persistent-stale-lock");
    let db = Db::open_sync(DbOptions::wasi_persistent(&path)).expect("WASI db opens");
    db.put_sync(b"key", b"value").expect("WASI write succeeds");
    drop(db);
    fs::write(
        path.join(recovery::PROCESS_LOCK_FILE_NAME),
        b"pid=wasi\nnonce=stale\n",
    )
    .expect("stale lock marker writes");

    let error = Db::open_sync(DbOptions::wasi_persistent(&path))
        .expect_err("stale WASI lock requires explicit repair");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(error.to_string().contains("LOCK"));

    let mut options = DbOptions::wasi_persistent(&path);
    options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    let error = Db::open_sync(options)
        .expect_err("safe temporary repair must not delete a WASI lock marker");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(error.to_string().contains("LOCK"));
    assert!(path.join(recovery::PROCESS_LOCK_FILE_NAME).exists());

    cleanup_wasi_temp_db_path(path);
}

#[cfg(target_os = "wasi")]
#[test]
fn wasi_persistent_repair_policy_does_not_remove_active_lock() {
    let path = temp_db_path("wasi-persistent-active-lock");
    let db = Db::open_sync(DbOptions::wasi_persistent(&path)).expect("WASI db opens");

    let mut options = DbOptions::wasi_persistent(&path);
    options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    let error = Db::open_sync(options).expect_err("active WASI writer keeps its lock");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(error.to_string().contains("LOCK"));
    assert!(path.join(recovery::PROCESS_LOCK_FILE_NAME).exists());

    db.put_sync(b"key", b"value")
        .expect("original WASI writer remains usable");
    drop(db);

    cleanup_wasi_temp_db_path(path);
}

#[cfg(target_os = "wasi")]
#[test]
fn wasi_persistent_open_rejects_unreferenced_table_file() {
    let path = temp_db_path("wasi-persistent-unreferenced-table");
    let db = Db::open_sync(DbOptions::wasi_persistent(&path)).expect("WASI db opens");
    drop(db);
    let table_name = format!("table-{:020}.{}", 999_u64, table::TABLE_FILE_EXTENSION);
    fs::write(path.join(&table_name), b"orphan table").expect("orphan table writes");

    let error = Db::open_sync(DbOptions::wasi_persistent(&path))
        .expect_err("WASI open rejects unreferenced table files");
    assert!(matches!(error, Error::Corruption { .. }));
    assert!(error.to_string().contains(&table_name));

    cleanup_wasi_temp_db_path(path);
}

#[cfg(target_os = "wasi")]
#[test]
fn wasi_persistent_open_async_uses_host_filesystem() {
    let path = temp_db_path("wasi-persistent-async-host");
    let db = block_on_test_future(Db::open(DbOptions::wasi_persistent(&path)))
        .expect("WASI async db opens");
    db.put_sync(b"key", b"value").expect("WASI write succeeds");
    db.flush_sync().expect("WASI flush succeeds");
    drop(db);

    let db = block_on_test_future(Db::open(DbOptions::wasi_persistent_read_only(&path)))
        .expect("WASI async read-only db reopens");
    assert_eq!(
        db.get_sync(b"key").expect("WASI read succeeds"),
        Some(b"value".to_vec())
    );
    drop(db);

    cleanup_wasi_temp_db_path(path);
}

#[test]
fn browser_persistent_backend_is_explicitly_unsupported() {
    let options = DbOptions::browser_persistent();
    assert_eq!(options.runtime.mode, crate::runtime::RuntimeMode::Inline);
    assert_eq!(options.background_worker_count, 0);

    let browser_error =
        Db::open_sync(options).expect_err("browser backend is not wired for sync open");
    assert!(matches!(browser_error, Error::UnsupportedBackend { .. }));
    assert!(browser_error.to_string().contains("browser persistent"));
}

#[test]
fn browser_persistent_read_only_options_disable_creation() {
    let options = DbOptions::browser_persistent_read_only();
    assert!(options.read_only);
    assert!(!options.create_if_missing);
    assert_eq!(options.runtime.mode, crate::runtime::RuntimeMode::Inline);
    assert_eq!(options.background_worker_count, 0);
}

#[test]
fn get_many_sync_preserves_order_missing_deletes_and_duplicates() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    db.put_sync(b"a", b"one").expect("first write");
    db.put_sync(b"b", b"two").expect("second write");
    db.delete_sync(b"deleted").expect("delete writes");

    let keys = [
        b"b".as_slice(),
        b"missing".as_slice(),
        b"a".as_slice(),
        b"b".as_slice(),
        b"deleted".as_slice(),
    ];
    let values = db.get_many_sync(&keys).expect("batch reads");

    assert_eq!(
        values,
        vec![
            Some(b"two".to_vec()),
            None,
            Some(b"one".to_vec()),
            Some(b"two".to_vec()),
            None,
        ]
    );
}

#[test]
fn bucket_get_many_sync_reads_named_bucket_only() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    db.put_sync(b"same", b"default").expect("default write");
    let users = db.bucket_sync("users").expect("named bucket opens");
    users.put_sync(b"same", b"named").expect("named write");

    let keys = [b"same".as_slice(), b"missing".as_slice()];
    let values = users.get_many_sync(&keys).expect("named batch reads");

    assert_eq!(values, vec![Some(b"named".to_vec()), None]);
    assert_eq!(
        db.get_many_sync(&keys).expect("default batch reads"),
        vec![Some(b"default".to_vec()), None]
    );
}

#[test]
fn bucket_reader_get_many_sync_keeps_snapshot_view() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let bucket = db.default_bucket_sync().expect("default bucket opens");
    bucket.put_sync(b"a", b"one").expect("first write");
    bucket.put_sync(b"b", b"two").expect("second write");
    let snapshot = db.snapshot();
    let reader = bucket.reader(&snapshot).expect("reader opens");

    bucket.put_sync(b"a", b"new").expect("new write");
    bucket.put_sync(b"c", b"three").expect("third write");

    let keys = [b"a".as_slice(), b"c".as_slice(), b"b".as_slice()];
    let values = reader
        .get_many_owned_sync(&keys)
        .expect("snapshot batch reads");

    assert_eq!(
        values,
        vec![Some(b"one".to_vec()), None, Some(b"two".to_vec())]
    );
    assert_eq!(
        bucket.get_many_sync(&keys).expect("current batch reads"),
        vec![
            Some(b"new".to_vec()),
            Some(b"three".to_vec()),
            Some(b"two".to_vec()),
        ]
    );
}

#[test]
fn get_many_async_preserves_order() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    db.put_sync(b"a", b"one").expect("first write");
    db.put_sync(b"b", b"two").expect("second write");

    let keys = [b"b".as_slice(), b"missing".as_slice(), b"a".as_slice()];
    let values = block_on_test_future(db.get_many(&keys)).expect("async batch reads");

    assert_eq!(
        values,
        vec![Some(b"two".to_vec()), None, Some(b"one".to_vec())]
    );
}

#[test]
fn get_many_sync_groups_persistent_keys_by_data_block() {
    let path = temp_db_path("get-many-block-grouping");
    let options = DbOptions::persistent(&path).with_default_bucket_options(BucketOptions {
        block_bytes: 4096,
        ..BucketOptions::default()
    });
    let db = Db::open_sync(options).expect("persistent db opens");
    for index in 0..8 {
        let key = format!("key-{index:02}");
        let value = format!("value-{index:02}");
        db.put_sync(key.as_bytes(), value.as_bytes())
            .expect("write key");
    }
    db.flush_sync().expect("flush table");

    let before = db.stats();
    let keys = [
        b"key-01".as_slice(),
        b"key-02".as_slice(),
        b"key-03".as_slice(),
        b"key-01".as_slice(),
    ];
    let values = db.get_many_sync(&keys).expect("batch reads");
    let after = db.stats();

    assert_eq!(
        values,
        vec![
            Some(b"value-01".to_vec()),
            Some(b"value-02".to_vec()),
            Some(b"value-03".to_vec()),
            Some(b"value-01".to_vec()),
        ]
    );
    assert_eq!(
        after
            .read_path
            .point_data_block_reads
            .saturating_sub(before.read_path.point_data_block_reads),
        1,
        "batch keys in one data block should share the block read"
    );

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn point_read_stats_split_l0_and_non_l0_probes() {
    let path = temp_db_path("point-read-l0-probe-stats");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    options.max_l0_files = 16;
    let db = Db::open_sync(options).expect("persistent db opens");
    for table_index in 0..2 {
        for row_index in 0..4 {
            let key = format!("key-{table_index}-{row_index}");
            let value = format!("value-{table_index}-{row_index}");
            db.put_sync(key.as_bytes(), value.as_bytes())
                .expect("write key");
        }
        db.flush_sync().expect("flush table");
    }

    let before = db.stats();
    assert_eq!(before.l0_tables, 2, "test needs two L0 tables");
    let value = db
        .get_sync(b"key-0-2")
        .expect("point read succeeds")
        .expect("value exists");
    let after = db.stats();

    assert_eq!(value, b"value-0-2".to_vec());
    let table_probes = after
        .read_path
        .point_table_probes
        .saturating_sub(before.read_path.point_table_probes);
    let l0_probes = after
        .read_path
        .point_l0_table_probes
        .saturating_sub(before.read_path.point_l0_table_probes);
    let non_l0_probes = after
        .read_path
        .point_non_l0_table_probes
        .saturating_sub(before.read_path.point_non_l0_table_probes);
    assert_eq!(table_probes, l0_probes.saturating_add(non_l0_probes));
    assert_eq!(l0_probes, 1);
    assert_eq!(non_l0_probes, 0);
    assert_eq!(
        after
            .read_path
            .point_l0_lookup_keys
            .saturating_sub(before.read_path.point_l0_lookup_keys),
        1
    );
    assert_eq!(
        after
            .read_path
            .point_l0_overlap_extra_table_probes
            .saturating_sub(before.read_path.point_l0_overlap_extra_table_probes),
        0
    );

    let before = db.stats();
    let keys = [b"key-0-2".as_slice(); 8];
    let values = db.get_many_sync(&keys).expect("batch read succeeds");
    let after = db.stats();
    assert_eq!(values, vec![Some(b"value-0-2".to_vec()); 8]);
    assert_eq!(
        after
            .read_path
            .batch_point_input_keys
            .saturating_sub(before.read_path.batch_point_input_keys),
        8
    );
    assert_eq!(
        after
            .read_path
            .batch_point_unique_keys
            .saturating_sub(before.read_path.batch_point_unique_keys),
        1
    );
    assert_eq!(
        after
            .read_path
            .batch_point_table_groups
            .saturating_sub(before.read_path.batch_point_table_groups),
        1
    );
    assert_eq!(
        after
            .read_path
            .batch_point_l0_lookup_keys
            .saturating_sub(before.read_path.batch_point_l0_lookup_keys),
        1
    );
    assert_eq!(
        after
            .read_path
            .batch_point_l0_overlap_extra_table_probes
            .saturating_sub(before.read_path.batch_point_l0_overlap_extra_table_probes),
        0
    );

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn browser_persistent_open_async_requires_browser_target() {
    let error = block_on_test_future(Db::open(DbOptions::browser_persistent_read_only()))
        .expect_err("browser async open requires browser target");
    assert!(matches!(error, Error::UnsupportedBackend { .. }));
    assert!(error.to_string().contains("browser persistent"));
}

#[test]
fn compaction_reservation_conflicts_are_bucket_and_range_scoped() {
    let base = reservation("default", KeyRange::half_open(b"a", b"c"));

    assert!(compaction_reservations_conflict(
        &base,
        &reservation("default", KeyRange::half_open(b"b", b"d"))
    ));
    assert!(!compaction_reservations_conflict(
        &base,
        &reservation("default", KeyRange::half_open(b"c", b"e"))
    ));
    assert!(!compaction_reservations_conflict(
        &base,
        &reservation("other", KeyRange::half_open(b"b", b"d"))
    ));
}

#[test]
fn maintenance_coordinator_allows_non_overlapping_compactions() {
    let coordinator = Arc::new(MaintenanceCoordinator::new());
    let first = coordinator
        .reserve_compactions(vec![reservation(
            "default",
            KeyRange::half_open(b"a", b"c"),
        )])
        .expect("first compaction reserves");
    let second = coordinator
        .reserve_compactions(vec![
            reservation("default", KeyRange::half_open(b"b", b"d")),
            reservation("default", KeyRange::half_open(b"c", b"e")),
            reservation("other", KeyRange::half_open(b"b", b"d")),
        ])
        .expect("non-overlapping compactions reserve");

    assert!(!second.contains("default", &KeyRange::half_open(b"b", b"d")));
    assert!(second.contains("default", &KeyRange::half_open(b"c", b"e")));
    assert!(second.contains("other", &KeyRange::half_open(b"b", b"d")));

    drop(first);
    drop(second);
    let third = coordinator
        .reserve_compactions(vec![reservation(
            "default",
            KeyRange::half_open(b"b", b"d"),
        )])
        .expect("released range can reserve again");
    assert!(third.contains("default", &KeyRange::half_open(b"b", b"d")));
}

#[test]
fn native_async_close_waits_for_active_publish_before_releasing_lease() {
    let path = temp_db_path("native-close-waits-for-publish");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("open db");
    db.put_sync(b"key", b"value").expect("write");

    let activity = db
        .inner
        .publish_barrier
        .begin_activity()
        .expect("test holds active publish");
    let thread_db = db.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        started_tx.send(()).expect("report close thread start");
        let result = block_on_test_future(thread_db.close());
        done_tx.send(result).expect("send close result");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("close thread starts");
    assert!(
        done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "native async close must wait for active publish before releasing the writer lease"
    );

    drop(activity);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("close finishes after active publish exits")
        .expect("close succeeds");
    handle.join().expect("close thread joins");

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn flush_waits_for_existing_flush_guard() {
    let path = temp_db_path("flush-waits-for-existing-guard");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("open db");
    db.put_sync(b"key", b"value").expect("write");

    let flush_guard = db
        .inner
        .maintenance
        .try_start_flush()
        .expect("test holds flush guard");
    let thread_db = db.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        started_tx.send(()).expect("report flush thread start");
        done_tx
            .send(thread_db.flush_sync())
            .expect("send flush result");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("flush thread starts");
    assert!(
        done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "public flush must wait while another flush guard is active"
    );

    drop(flush_guard);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("flush finishes after guard release")
        .expect("flush succeeds");
    handle.join().expect("flush thread joins");

    let stats = db.stats();
    assert_eq!(stats.memtable_bytes, 0);
    assert_eq!(stats.immutable_memtables, 0);
    assert!(stats.total_tables > 0);

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn flush_returns_after_default_background_flush_publishes_tables() {
    let path = temp_db_path("flush-default-background-publishes");
    let mut options = DbOptions::persistent(&path);
    options.write_buffer_bytes = 128;
    let db = Db::open_sync(options).expect("open db");

    for index in 0..128_u32 {
        let key = format!("key-{index:04}");
        db.put_sync(key.as_bytes(), [b'x'; 96]).expect("write");
    }

    db.flush_sync().expect("public flush");
    let stats = db.stats();
    assert_eq!(stats.memtable_bytes, 0);
    assert_eq!(stats.immutable_memtables, 0);
    assert!(stats.total_tables > 0);

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn compact_range_is_not_silent_best_effort() {
    let path = temp_db_path("compact-range-waits-for-guard");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("open db");
    db.put_sync(b"a1", b"one").expect("write first");
    db.flush_sync().expect("flush first table");
    db.put_sync(b"a2", b"two").expect("write second");
    db.flush_sync().expect("flush second table");

    let compaction_guard = db
        .inner
        .maintenance
        .reserve_compactions(vec![reservation(DEFAULT_BUCKET_NAME, KeyRange::all())])
        .expect("test holds compaction reservation");
    let thread_db = db.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        started_tx.send(()).expect("report compaction thread start");
        done_tx
            .send(thread_db.compact_range_sync(KeyRange::all()))
            .expect("send compaction result");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("compaction thread starts");
    assert!(
        done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "public compact_range must wait while its range is reserved"
    );

    drop(compaction_guard);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("compaction finishes after guard release")
        .expect("compaction succeeds");
    handle.join().expect("compaction thread joins");
    assert!(db.stats().compaction_runs > 0);
    assert!(db.stats().maintenance_cooperative_yields > 0);

    drop(db);
    fs::remove_dir_all(path).expect("cleanup test db");
}

fn reservation(bucket: &str, range: KeyRange) -> CompactionReservation {
    CompactionReservation {
        bucket: bucket.to_owned(),
        range,
    }
}

fn temp_db_path(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after UNIX epoch")
        .as_nanos();
    #[cfg(target_os = "wasi")]
    let process_id = "wasi".to_owned();
    #[cfg(not(target_os = "wasi"))]
    let process_id = std::process::id().to_string();
    let db_name = format!("trine-kv-{name}-{process_id}-{nonce}");

    #[cfg(target_os = "wasi")]
    {
        std::path::PathBuf::from("target/wasi-test-data").join(db_name)
    }

    #[cfg(not(target_os = "wasi"))]
    {
        std::env::temp_dir().join(db_name)
    }
}

#[cfg(target_os = "wasi")]
fn cleanup_wasi_temp_db_path(path: std::path::PathBuf) {
    let _ = fs::remove_dir_all(path);
}
