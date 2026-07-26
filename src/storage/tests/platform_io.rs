#![allow(unused_imports)]

use super::*;

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    target_os = "linux"
))]
#[test]
fn platform_io_native_file_read_and_append_use_platform_driver() {
    let root = temp_storage_root("trine-kv-platform-io-storage");
    std::fs::create_dir_all(&root).expect("test dir creates");
    let table = StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));
    std::fs::write(table.path(), b"abcdef").expect("table file writes");

    let runtime = Runtime::new(RuntimeOptions::platform_io());
    let backend = NativeFileBackend::with_runtime(runtime).expect("platform I/O backend starts");
    let capabilities = backend.capabilities();
    assert!(capabilities.supports(StorageCapability::PlatformAsyncIo));

    let object = backend
        .open_read_blocking(table)
        .expect("platform I/O read object opens");
    let object_len = block_on_test_future(object.len()).expect("platform I/O len completes");
    assert_eq!(object_len, 6);
    let buffer = block_on_test_future(object.read_exact_at_owned(2, 3))
        .expect("platform I/O read completes");
    assert_eq!(buffer.offset(), 2);
    assert_eq!(&*buffer.into_bytes(), b"cde");

    let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
    let mut append = block_on_test_future(backend.open_append(wal.clone()))
        .expect("platform I/O append object opens");
    block_on_test_future(StorageAppendObject::append(
        &mut append,
        b"first",
        DurabilityMode::Buffered,
    ))
    .expect("platform I/O append completes");
    block_on_test_future(StorageAppendObject::persist(
        &mut append,
        DurabilityMode::Buffered,
    ))
    .expect("platform I/O persist completes");
    assert_eq!(
        std::fs::read(wal.path()).expect("WAL object reads"),
        b"first"
    );

    let stats = backend.stats();
    assert_platform_task_accounting(&stats, 5, 0);
    if stats.platform_async_io_tasks > 0 {
        assert!(
            stats
                .platform_io_operations
                .length_lookup
                .true_platform_async
                > 0,
            "io_uring length lookup should report true platform async"
        );
        assert!(
            stats.platform_io_operations.random_read.true_platform_async > 0,
            "io_uring random reads should report true platform async"
        );
        assert!(
            stats.platform_io_operations.append_open.true_platform_async > 0,
            "io_uring append open should report true platform async"
        );
        assert!(
            stats.platform_io_operations.append.true_platform_async > 0,
            "io_uring append should report true platform async"
        );
        assert!(
            stats.platform_io_operations.persist.true_platform_async > 0,
            "io_uring persist should report true platform async"
        );
    } else {
        assert!(
            stats
                .platform_io_operations
                .total()
                .thread_pool_managed_async
                >= 5,
            "Linux without io_uring must report the selected managed executor"
        );
    }

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    target_os = "linux"
))]
fn assert_platform_io_listing_managed_async(
    backend: &NativeFileBackend,
    directory: StorageDirectoryId,
    table: &StorageObjectId,
) {
    let list_request =
        StorageObjectListRequest::native_file(StorageObjectKind::Table, directory.path())
            .with_file_extension("trinet");
    let listed_objects = block_on_test_future(backend.list_objects(list_request.clone()))
        .expect("platform I/O object listing completes");
    assert_eq!(listed_objects, vec![table.clone()]);

    let listed_files = block_on_test_future(backend.list_directory_files(directory.clone()))
        .expect("platform I/O directory listing completes");
    assert!(
        listed_files.iter().any(|file| file.path() == table.path()),
        "directory listing should include the table"
    );
    assert_eq!(
        backend
            .list_objects_blocking(list_request)
            .expect("blocking object listing fallback completes"),
        vec![table.clone()]
    );
    assert!(
        backend
            .list_directory_files_blocking(directory)
            .expect("blocking directory listing fallback completes")
            .iter()
            .any(|file| file.path() == table.path()),
        "blocking directory listing should include the table"
    );
}

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    target_os = "linux"
))]
fn assert_platform_task_accounting(
    stats: &NativeFileStorageStats,
    min_driver_tasks: u64,
    min_blocking_fallback_tasks: u64,
) {
    assert!(stats.uses_platform_io_driver);
    assert!(stats.uses_platform_async_io);
    assert_eq!(stats.blocking_adapter_tasks, 0);

    let driver_tasks = stats
        .platform_async_io_tasks
        .saturating_add(stats.platform_thread_pool_managed_async_tasks);
    assert!(
        driver_tasks >= min_driver_tasks,
        "platform driver task count {driver_tasks} should be at least {min_driver_tasks}"
    );
    assert!(
        stats.platform_blocking_fallback_tasks >= min_blocking_fallback_tasks,
        "platform blocking fallback count {} should be at least {}",
        stats.platform_blocking_fallback_tasks,
        min_blocking_fallback_tasks
    );

    assert!(
        stats.platform_async_io_tasks > 0 || stats.platform_thread_pool_managed_async_tasks > 0,
        "Linux platform backend should report its actual selected executor"
    );
}

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    target_os = "linux"
))]
#[test]
fn platform_io_native_file_management_ops_use_platform_driver() {
    let root = temp_storage_root("trine-kv-platform-io-management");
    let runtime = Runtime::new(RuntimeOptions::platform_io());
    let backend = NativeFileBackend::with_runtime(runtime).expect("platform I/O backend starts");
    let directory = StorageDirectoryId::native_file(&root);

    block_on_test_future(backend.create_directory_all(directory.clone()))
        .expect("platform I/O directory create completes");
    assert!(root.is_dir(), "directory create should create root");

    let table = StorageObjectId::native_file(
        StorageObjectKind::Table,
        root.join("table-00000000000000000033.trinet"),
    );
    block_on_test_future(backend.write_object(
        table.clone(),
        Arc::from(&b"table bytes"[..]),
        DurabilityMode::Buffered,
    ))
    .expect("platform I/O object write completes");
    let table_bytes = block_on_test_future(backend.read_object_bytes(table.clone()))
        .expect("platform I/O object read completes")
        .expect("table object exists");
    assert_eq!(&*table_bytes, b"table bytes");

    let manifest = StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
    block_on_test_future(backend.publish_manifest(
        manifest.clone(),
        Arc::from(&b"manifest bytes"[..]),
        DurabilityMode::SyncAll,
    ))
    .expect("platform I/O manifest publish completes");
    let manifest_bytes = block_on_test_future(backend.read_current_manifest(manifest.clone()))
        .expect("platform I/O manifest read completes")
        .expect("manifest exists");
    assert_eq!(&*manifest_bytes, b"manifest bytes");

    let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
    let wal_tmp = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal.tmp"));
    std::fs::write(wal.path(), b"old wal").expect("old WAL writes");
    block_on_test_future(backend.rewrite_wal(
        wal.clone(),
        wal_tmp.clone(),
        Arc::from(&b"new wal"[..]),
        DurabilityMode::SyncAll,
    ))
    .expect("platform I/O WAL rewrite completes");
    assert_eq!(
        std::fs::read(wal.path()).expect("WAL object reads"),
        b"new wal"
    );
    assert!(
        !wal_tmp.path().exists(),
        "WAL rewrite should remove the temporary object"
    );

    let lease_object =
        StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
    let lease = block_on_test_future(backend.acquire_writer_lease(lease_object.clone()))
        .expect("platform I/O writer lease acquires");
    assert!(
        lease_object.path().exists(),
        "writer lease marker should exist"
    );
    drop(lease);
    assert!(
        lease_object.path().exists(),
        "dropping writer lease should keep the lock file inode"
    );
    assert!(
        std::fs::read(lease_object.path())
            .expect("platform writer lease marker reads")
            .is_empty(),
        "dropping writer lease should clear owner text"
    );

    assert_platform_io_listing_managed_async(&backend, directory.clone(), &table);

    block_on_test_future(backend.sync_directory_after_renames(directory))
        .expect("platform I/O directory sync completes");
    block_on_test_future(backend.delete_object(table.clone()))
        .expect("platform I/O object delete completes");
    assert!(!table.path().exists(), "table object should be deleted");

    let stats = backend.stats();
    assert_platform_task_accounting(&stats, 11, 0);
    assert_linux_platform_management_counters(&stats);

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    target_os = "linux"
))]
fn assert_linux_platform_management_counters(stats: &NativeFileStorageStats) {
    if stats.platform_async_io_tasks == 0 {
        assert!(
            stats
                .platform_io_operations
                .total()
                .thread_pool_managed_async
                >= 11,
            "Linux without io_uring must report managed execution for storage operations"
        );
        return;
    }
    assert!(
        stats
            .platform_io_operations
            .temp_write_rename_publish
            .true_platform_async
            > 0,
        "Linux platform publish writes should report true platform async"
    );
    assert!(
        stats.platform_io_operations.wal_rewrite.true_platform_async > 0,
        "Linux platform WAL rewrite should report true platform async"
    );
    assert!(
        stats
            .platform_io_operations
            .whole_object_read
            .true_platform_async
            > 0,
        "Linux platform whole-object reads should report true platform async"
    );
    assert!(
        stats.platform_io_operations.delete.true_platform_async > 0,
        "Linux platform deletes should report true platform async"
    );
    assert!(
        stats
            .platform_io_operations
            .directory_create
            .true_platform_async
            > 0,
        "Linux platform directory create should report true platform async"
    );
    assert!(
        stats
            .platform_io_operations
            .writer_lease
            .thread_pool_managed_async
            > 0,
        "Linux platform writer lease should report thread-pool managed async"
    );
    assert!(
        stats
            .platform_io_operations
            .total()
            .uses_true_platform_async(),
        "Linux platform management diagnostics should aggregate true platform async work"
    );
    assert!(
        stats
            .platform_io_operations
            .directory_listing
            .thread_pool_managed_async
            > 0,
        "directory listing should report thread-pool managed async"
    );
    assert!(
        stats
            .platform_io_operations
            .directory_sync
            .true_platform_async
            > 0,
        "Linux platform directory sync should report true platform async"
    );
}

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    any(
        windows,
        target_os = "macos",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "solaris"
    )
))]
#[test]
fn platform_io_partial_native_storage_ops_use_platform_driver() {
    let root = temp_storage_root("trine-kv-platform-io-partial-native-storage");
    std::fs::create_dir_all(&root).expect("test dir creates");
    let table = StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));
    std::fs::write(table.path(), b"abcdef").expect("table file writes");

    let runtime = Runtime::new(RuntimeOptions::platform_io());
    let backend = NativeFileBackend::with_runtime(runtime).expect("platform I/O backend starts");
    let capabilities = backend.capabilities();
    assert!(capabilities.supports(StorageCapability::PlatformAsyncIo));
    assert!(capabilities.supports(StorageCapability::BlockingAdapter));

    let object = backend
        .open_read_blocking(table)
        .expect("read object opens with platform driver fallback");
    let buffer = block_on_test_future(object.read_exact_at_owned(2, 3))
        .expect("read completes through platform driver fallback");
    assert_eq!(buffer.offset(), 2);
    assert_eq!(&*buffer.into_bytes(), b"cde");

    let stats = backend.stats();
    assert!(!stats.uses_blocking_adapter);
    assert!(stats.uses_platform_io_driver);
    assert!(stats.uses_platform_async_io);
    assert!(
        stats.platform_async_io_tasks > 0,
        "partial native platform work should count as platform async I/O"
    );
    assert_eq!(
        stats.platform_thread_pool_managed_async_tasks, 0,
        "single random read should not be counted as thread-pool managed async"
    );
    assert_eq!(stats.platform_blocking_fallback_tasks, 0);
    assert_eq!(stats.blocking_adapter_tasks, 0);
    assert!(
        stats
            .platform_io_operations
            .random_read
            .platform_native_async_but_partial
            > 0,
        "platform random read should report partial native async"
    );
    let platform_total = stats.platform_io_operations.total();
    assert!(
        platform_total.uses_non_true_platform_async(),
        "non-Linux platform diagnostics should aggregate non-true-platform work"
    );
    assert!(
        !platform_total.uses_true_platform_async(),
        "partial native diagnostics should not report whole-operation true async work"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[cfg(all(
    feature = "platform-io",
    not(feature = "platform-io-native"),
    any(unix, windows)
))]
#[test]
fn platform_io_threadpool_storage_ops_use_platform_driver() {
    let root = temp_storage_root("trine-kv-platform-io-threadpool-storage");
    std::fs::create_dir_all(&root).expect("test dir creates");
    let table = StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));
    std::fs::write(table.path(), b"abcdef").expect("table file writes");

    let runtime = Runtime::new(RuntimeOptions::platform_io());
    let backend = NativeFileBackend::with_runtime(runtime).expect("platform I/O backend starts");
    let capabilities = backend.capabilities();
    assert!(capabilities.supports(StorageCapability::PlatformAsyncIo));
    assert!(capabilities.supports(StorageCapability::BlockingAdapter));

    let object = backend
        .open_read_blocking(table)
        .expect("read object opens with platform driver fallback");
    let buffer = block_on_test_future(object.read_exact_at_owned(2, 3))
        .expect("read completes through platform driver fallback");
    assert_eq!(buffer.offset(), 2);
    assert_eq!(&*buffer.into_bytes(), b"cde");

    let stats = backend.stats();
    assert!(!stats.uses_blocking_adapter);
    assert!(stats.uses_platform_io_driver);
    assert!(stats.uses_platform_async_io);
    assert_eq!(stats.platform_async_io_tasks, 0);
    assert!(
        stats.platform_thread_pool_managed_async_tasks > 0,
        "platform driver should account thread-pool managed async tasks"
    );
    assert_eq!(stats.platform_blocking_fallback_tasks, 0);
    assert_eq!(stats.blocking_adapter_tasks, 0);
    assert!(
        stats
            .platform_io_operations
            .random_read
            .thread_pool_managed_async
            > 0,
        "platform random read should report thread-pool managed async"
    );
    let platform_total = stats.platform_io_operations.total();
    assert!(
        platform_total.uses_non_true_platform_async(),
        "thread-pool diagnostics should aggregate non-true-platform work"
    );
    assert!(
        !platform_total.uses_true_platform_async(),
        "thread-pool diagnostics should not report true platform async work"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}
