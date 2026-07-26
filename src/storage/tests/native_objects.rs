use super::*;

#[test]
fn inline_runtime_native_file_mutations_remain_ready() {
    let root = temp_storage_root("trine-kv-inline-runtime-mutations");
    let runtime = Runtime::new(RuntimeOptions::inline());
    let backend = NativeFileBackend::with_runtime(runtime).expect("runtime backend starts");
    let table = StorageObjectId::native_file(
        StorageObjectKind::Table,
        root.join("table-00000000000000000031.trinet"),
    );
    poll_ready_storage_future(backend.write_object(
        table.clone(),
        Arc::from(&b"inline table"[..]),
        DurabilityMode::Buffered,
    ))
    .expect("inline runtime object write is ready");
    assert_eq!(
        std::fs::read(table.path()).expect("table object reads"),
        b"inline table"
    );

    let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
    let mut append =
        poll_ready_storage_future(backend.open_append(wal.clone())).expect("WAL opens");
    poll_ready_storage_future(StorageAppendObject::append(
        &mut append,
        b"inline wal",
        DurabilityMode::Buffered,
    ))
    .expect("inline runtime append is ready");
    poll_ready_storage_future(StorageAppendObject::persist(
        &mut append,
        DurabilityMode::Buffered,
    ))
    .expect("inline runtime append persist is ready");
    assert_eq!(
        std::fs::read(wal.path()).expect("WAL object reads"),
        b"inline wal"
    );

    poll_ready_storage_future(backend.delete_object(table))
        .expect("inline runtime object delete is ready");

    std::fs::remove_dir_all(root).expect("test dir removes");
}
#[test]
fn runtime_enabled_native_file_owned_read_uses_blocking_adapter() {
    let path = std::env::temp_dir().join(format!(
        "trine-kv-runtime-storage-read-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, b"abcdef").expect("test file writes");

    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 2);
    let release = hold_runtime_blocking_worker(&runtime);
    let backend = NativeFileBackend::with_runtime(runtime).expect("runtime backend starts");
    let capabilities = backend.capabilities();
    assert!(capabilities.supports(StorageCapability::AsyncTasks));
    assert!(capabilities.supports(StorageCapability::BlockingAdapter));
    assert!(capabilities.supports(StorageCapability::BackgroundThreads));
    assert!(!capabilities.supports(StorageCapability::PlatformAsyncIo));

    let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
    let object = poll_ready_storage_future(backend.open_read(object_id))
        .expect("runtime-backed object opens");
    let mut read = StorageReadObject::read_exact_at_owned(&object, 1, 4);
    let waker = test_waker();
    let mut context = Context::from_waker(&waker);
    assert!(
        matches!(read.as_mut().poll(&mut context), Poll::Pending),
        "owned read should wait behind the occupied blocking worker"
    );

    release.send(()).expect("release blocking worker");
    let buffer = block_on_test_future(read).expect("runtime owned read completes");
    assert_eq!(buffer.offset(), 1);
    assert_eq!(&*buffer.into_bytes(), b"bcde");
    let stats = backend.stats();
    assert!(stats.uses_blocking_adapter);
    assert!(!stats.uses_platform_async_io);
    assert_eq!(stats.blocking_adapter_tasks, 1);
    assert_eq!(stats.inline_tasks, 0);

    std::fs::remove_file(path).expect("test file removes");
}

#[test]
fn runtime_enabled_borrowed_read_never_performs_file_io_in_poll() {
    let path = temp_storage_root("trine-kv-runtime-borrowed-read").join("table.trinet");
    std::fs::create_dir_all(path.parent().expect("test parent")).expect("test parent creates");
    std::fs::write(&path, b"abcdef").expect("test file writes");

    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 2);
    let release = hold_runtime_blocking_worker(&runtime);
    let backend = NativeFileBackend::with_runtime(runtime).expect("runtime backend starts");
    let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
    let object = poll_ready_storage_future(backend.open_read(object_id))
        .expect("runtime-backed object opens");
    let mut bytes = [0_u8; 4];
    let mut read = StorageReadObject::read_exact_at(&object, 1, &mut bytes);
    let waker = test_waker();
    let mut context = Context::from_waker(&waker);
    assert!(
        matches!(read.as_mut().poll(&mut context), Poll::Pending),
        "borrowed read must hand file IO to the occupied blocking adapter"
    );

    release.send(()).expect("release blocking worker");
    block_on_test_future(read).expect("borrowed read completes");
    assert_eq!(&bytes, b"bcde");
    std::fs::remove_dir_all(path.parent().expect("test parent")).expect("test root removes");
}

#[test]
fn runtime_enabled_native_file_object_read_uses_blocking_adapter() {
    let path = std::env::temp_dir().join(format!(
        "trine-kv-runtime-object-read-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, b"whole object").expect("test file writes");

    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 2);
    let release = hold_runtime_blocking_worker(&runtime);
    let backend = NativeFileBackend::with_runtime(runtime).expect("runtime backend starts");
    let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);

    let mut read = backend.read_object_bytes(object_id);
    let waker = test_waker();
    let mut context = Context::from_waker(&waker);
    assert!(
        matches!(read.as_mut().poll(&mut context), Poll::Pending),
        "whole-object read should wait behind the occupied blocking worker"
    );

    release.send(()).expect("release blocking worker");
    let bytes = block_on_test_future(read)
        .expect("runtime object read completes")
        .expect("object exists");
    assert_eq!(&*bytes, b"whole object");
    let stats = backend.stats();
    assert!(stats.uses_blocking_adapter);
    assert_eq!(stats.blocking_adapter_tasks, 1);
    assert_eq!(stats.blocking_adapter_queue_capacity, 2);
    assert!(
        (1..=2).contains(&stats.blocking_adapter_submitted_tasks),
        "submitted blocking tasks should include the read and may include the worker holder"
    );
    assert!(
        (1..=2).contains(&stats.blocking_adapter_completed_tasks),
        "completed blocking tasks should include the read and may include the worker holder"
    );
    assert_eq!(stats.operations.read_object_bytes.requests, 1);
    assert!(stats.operations.read_object_bytes.total_latency_micros > 0);
    assert_eq!(stats.inline_tasks, 0);

    std::fs::remove_file(path).expect("test file removes");
}

#[test]
fn inline_runtime_native_file_owned_read_remains_ready() {
    let path = std::env::temp_dir().join(format!(
        "trine-kv-inline-runtime-storage-read-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, b"abcdef").expect("test file writes");

    let runtime = Runtime::new(RuntimeOptions::inline());
    let backend = NativeFileBackend::with_runtime(runtime).expect("runtime backend starts");
    let capabilities = backend.capabilities();
    assert!(!capabilities.supports(StorageCapability::AsyncTasks));
    assert!(!capabilities.supports(StorageCapability::BlockingAdapter));
    assert!(!capabilities.supports(StorageCapability::PlatformAsyncIo));

    let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
    let object = poll_ready_storage_future(backend.open_read(object_id))
        .expect("inline runtime object opens");
    let buffer = poll_ready_storage_future(StorageReadObject::read_exact_at_owned(&object, 2, 3))
        .expect("inline runtime owned read is ready");

    assert_eq!(buffer.offset(), 2);
    assert_eq!(&*buffer.into_bytes(), b"cde");
    let stats = backend.stats();
    assert!(!stats.uses_blocking_adapter);
    assert!(!stats.uses_platform_async_io);
    assert_eq!(stats.blocking_adapter_tasks, 0);
    assert_eq!(stats.inline_tasks, 1);

    std::fs::remove_file(path).expect("test file removes");
}

#[test]
fn native_file_backend_reads_optional_object_bytes() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-object-read-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");
    let path = root.join("trine.wal");
    std::fs::write(&path, b"wal bytes").expect("WAL writes");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::ObjectRead)
        .expect("native-file backend supports whole-object reads");
    let object = StorageObjectId::native_file(StorageObjectKind::Wal, &path);
    let bytes = backend
        .read_object_bytes_blocking(object)
        .expect("object read succeeds")
        .expect("object exists");
    assert_eq!(&*bytes, b"wal bytes");

    let missing = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("missing.wal"));
    assert!(
        backend
            .read_object_bytes_blocking(missing)
            .expect("missing object read succeeds")
            .is_none()
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_publishes_manifest_with_capabilities() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-manifest-publish-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");

    let backend = NativeFileBackend::new();
    let capabilities = backend.capabilities();
    capabilities
        .require(StorageCapability::AtomicManifestPublish)
        .expect("native-file backend supports manifest publish");
    capabilities
        .require_durability(DurabilityMode::SyncAll)
        .expect("native-file backend supports strict publish sync");

    let object = StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
    backend
        .publish_manifest_blocking(
            object.clone(),
            Arc::from(&b"first"[..]),
            DurabilityMode::SyncAll,
        )
        .expect("manifest publishes");
    assert_eq!(
        std::fs::read(object.path()).expect("manifest reads"),
        b"first"
    );

    backend
        .publish_manifest_blocking(
            object.clone(),
            Arc::from(&b"second"[..]),
            DurabilityMode::SyncAll,
        )
        .expect("manifest republishes");
    assert_eq!(
        std::fs::read(object.path()).expect("manifest reads"),
        b"second"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_reads_current_manifest() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-manifest-read-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");

    let backend = NativeFileBackend::new();
    let object = StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
    assert!(
        backend
            .read_current_manifest_blocking(object.clone())
            .expect("missing manifest read succeeds")
            .is_none()
    );

    backend
        .publish_manifest_blocking(
            object.clone(),
            Arc::from(&b"manifest bytes"[..]),
            DurabilityMode::SyncAll,
        )
        .expect("manifest publishes");
    let bytes = backend
        .read_current_manifest_blocking(object)
        .expect("manifest reads")
        .expect("manifest exists");
    assert_eq!(&*bytes, b"manifest bytes");

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_manifest_read_rejects_oversized_file_before_allocating_it() {
    let root = temp_storage_root("trine-kv-storage-manifest-oversized");
    std::fs::create_dir_all(&root).expect("manifest root creates");
    let object = StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
    let file = std::fs::File::create(object.path()).expect("sparse manifest creates");
    file.set_len((crate::limits::MAX_MANIFEST_PAYLOAD_BYTES as u64) + 4096)
        .expect("sparse manifest length sets");

    let error = NativeFileBackend::new()
        .read_current_manifest_blocking(object)
        .expect_err("oversized manifest is rejected from metadata");
    assert!(matches!(error, Error::Corruption { .. }));
    std::fs::remove_dir_all(root).expect("manifest root removes");
}
