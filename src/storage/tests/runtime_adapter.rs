use super::*;

#[test]
fn storage_read_buffer_from_vec_reuses_vec_allocation() {
    let bytes = b"owned read bytes".to_vec();
    let expected_ptr = bytes.as_ptr();

    let buffer = StorageReadBuffer::from_vec(7, bytes);

    assert_eq!(buffer.offset(), 7);
    assert_eq!(buffer.as_slice(), b"owned read bytes");
    assert_eq!(buffer.as_slice().as_ptr(), expected_ptr);
}

#[test]
fn native_file_backend_exposes_async_read_shape() {
    let path = std::env::temp_dir().join(format!(
        "trine-kv-async-storage-read-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, b"abcdef").expect("test file writes");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)
        .expect("native-file backend supports random reads");
    let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
    let object = poll_ready_storage_future(backend.open_read(object_id)).expect("object opens");
    assert_eq!(
        StorageReadObject::object(&object).kind(),
        StorageObjectKind::Table
    );
    assert_eq!(
        poll_ready_storage_future(StorageReadObject::len(&object)).expect("length reads"),
        6
    );

    let mut bytes = [0_u8; 3];
    poll_ready_storage_future(StorageReadObject::read_exact_at(&object, 2, &mut bytes))
        .expect("range reads");
    assert_eq!(&bytes, b"cde");

    let owned = poll_ready_storage_future(StorageReadObject::read_exact_at_owned(&object, 1, 4))
        .expect("owned range reads");
    assert_eq!(owned.offset(), 1);
    assert_eq!(owned.len(), 4);
    assert!(!owned.is_empty());
    assert_eq!(&*owned.into_bytes(), b"bcde");

    let owned_blocking = object
        .read_exact_at_owned_blocking(3, 2)
        .expect("blocking owned range reads");
    assert_eq!(owned_blocking.offset(), 3);
    assert_eq!(&*owned_blocking.into_bytes(), b"de");

    std::fs::remove_file(path).expect("test file removes");
}

#[test]
fn native_file_read_io_completion_returns_owned_buffer() {
    let path = std::env::temp_dir().join(format!(
        "trine-kv-completion-storage-read-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, b"abcdef").expect("test file writes");

    let backend = NativeFileBackend::new();
    let object_id = StorageObjectId::native_file(StorageObjectKind::Table, &path);
    let object = backend
        .open_read_blocking(object_id)
        .expect("completion object opens");
    let completion = object
        .read_exact_at_owned_io(2, 3)
        .expect("completion read submits");
    assert!(completion.is_finished().expect("completion state reads"));
    let buffer = poll_ready_storage_future(completion).expect("completion read finishes");

    assert_eq!(buffer.offset(), 2);
    assert_eq!(&*buffer.into_bytes(), b"cde");

    std::fs::remove_file(path).expect("test file removes");
}

#[test]
fn native_file_append_io_completion_writes_and_persists() {
    let root = temp_storage_root("trine-kv-completion-append-storage");
    let object = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
    let backend = NativeFileBackend::new();
    let append = backend
        .open_append_blocking(object.clone())
        .expect("completion append object opens");

    let append_completion = append
        .append_io(Arc::from(&b"wal bytes"[..]), DurabilityMode::Buffered)
        .expect("completion append submits");
    assert!(
        append_completion
            .is_finished()
            .expect("append completion state reads")
    );
    poll_ready_storage_future(append_completion).expect("completion append finishes");

    let persist_completion = append
        .persist_io(DurabilityMode::Buffered)
        .expect("completion persist submits");
    poll_ready_storage_future(persist_completion).expect("completion persist finishes");

    assert_eq!(
        std::fs::read(object.path()).expect("WAL object reads"),
        b"wal bytes"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn runtime_enabled_native_file_object_mutations_use_blocking_adapter() {
    let root = temp_storage_root("trine-kv-runtime-object-mutations");
    let path = root.join("table-00000000000000000011.trinet");
    let object = StorageObjectId::native_file(StorageObjectKind::Table, &path);
    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
    let backend = NativeFileBackend::with_runtime(runtime.clone());

    complete_after_blocking_worker_release(
        &runtime,
        backend.write_object(
            object.clone(),
            Arc::from(&b"table bytes"[..]),
            DurabilityMode::Buffered,
        ),
        "object write should wait behind the occupied blocking worker",
    )
    .expect("runtime object write completes");
    assert_eq!(
        std::fs::read(object.path()).expect("table object reads"),
        b"table bytes"
    );

    complete_after_blocking_worker_release(
        &runtime,
        backend.delete_object(object.clone()),
        "object delete should wait behind the occupied blocking worker",
    )
    .expect("runtime object delete completes");
    assert!(!object.path().exists(), "table object should be deleted");

    let blocking_path = root.join("table-00000000000000000012.trinet");
    let blocking_object = StorageObjectId::native_file(StorageObjectKind::Table, &blocking_path);
    let release = hold_runtime_blocking_worker(&runtime);
    backend
        .write_object_blocking(
            blocking_object.clone(),
            Arc::from(&b"blocking table"[..]),
            DurabilityMode::Buffered,
        )
        .expect("blocking object write stays direct");
    backend
        .delete_object_blocking(blocking_object)
        .expect("blocking object delete stays direct");
    release.send(()).expect("release blocking worker");

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn runtime_enabled_native_file_directory_and_lease_ops_use_blocking_adapter() {
    let root = temp_storage_root("trine-kv-runtime-directory-storage");
    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
    let backend = NativeFileBackend::with_runtime(runtime.clone());
    let directory = StorageDirectoryId::native_file(&root);

    complete_after_blocking_worker_release(
        &runtime,
        backend.create_directory_all(directory.clone()),
        "directory create should wait behind the occupied blocking worker",
    )
    .expect("runtime directory create completes");
    assert!(root.is_dir(), "runtime directory create should create root");

    let lease_object =
        StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
    let lease = complete_after_blocking_worker_release(
        &runtime,
        backend.acquire_writer_lease(lease_object.clone()),
        "writer lease acquire should wait behind the occupied blocking worker",
    )
    .expect("runtime writer lease acquire completes");
    assert!(
        lease_object.path().exists(),
        "runtime writer lease should create marker"
    );
    drop(lease);
    assert!(
        lease_object.path().exists(),
        "dropping runtime writer lease should keep the lock file inode"
    );
    assert!(
        std::fs::read(lease_object.path())
            .expect("runtime writer lease marker reads")
            .is_empty(),
        "dropping runtime writer lease should clear owner text"
    );

    let listed_path = root.join("directory-file.tmp");
    std::fs::write(&listed_path, b"listed").expect("directory file writes");
    let files = complete_after_blocking_worker_release(
        &runtime,
        backend.list_directory_files(directory.clone()),
        "directory listing should wait behind the occupied blocking worker",
    )
    .expect("runtime directory listing completes");
    assert!(
        files.iter().any(|file| file.path() == listed_path),
        "directory listing should include the written file"
    );

    let sync_tmp = root.join("sync.tmp");
    let sync_published = root.join("sync.trinet");
    std::fs::write(&sync_tmp, b"sync").expect("sync temp file writes");
    std::fs::rename(&sync_tmp, &sync_published).expect("sync temp file renames");
    complete_after_blocking_worker_release(
        &runtime,
        backend.sync_directory_after_renames(directory),
        "directory sync should wait behind the occupied blocking worker",
    )
    .expect("runtime directory sync completes");

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn runtime_enabled_native_file_manifest_wal_and_listing_use_blocking_adapter() {
    let root = temp_storage_root("trine-kv-runtime-metadata-storage");
    std::fs::create_dir_all(&root).expect("test dir creates");
    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
    let backend = NativeFileBackend::with_runtime(runtime.clone());

    let manifest = StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));
    complete_after_blocking_worker_release(
        &runtime,
        backend.publish_manifest(
            manifest.clone(),
            Arc::from(&b"manifest"[..]),
            DurabilityMode::Buffered,
        ),
        "manifest publish should wait behind the occupied blocking worker",
    )
    .expect("runtime manifest publish completes");
    let manifest_bytes = complete_after_blocking_worker_release(
        &runtime,
        backend.read_current_manifest(manifest.clone()),
        "manifest read should wait behind the occupied blocking worker",
    )
    .expect("runtime manifest read completes")
    .expect("manifest exists");
    assert_eq!(&*manifest_bytes, b"manifest");

    let wal = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
    let wal_tmp = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal.tmp"));
    std::fs::write(wal.path(), b"old wal").expect("old WAL writes");
    complete_after_blocking_worker_release(
        &runtime,
        backend.rewrite_wal(
            wal.clone(),
            wal_tmp.clone(),
            Arc::from(&b"new wal"[..]),
            DurabilityMode::Buffered,
        ),
        "WAL rewrite should wait behind the occupied blocking worker",
    )
    .expect("runtime WAL rewrite completes");
    assert_eq!(std::fs::read(wal.path()).expect("WAL reads"), b"new wal");
    assert!(
        !wal_tmp.path().exists(),
        "runtime WAL rewrite should remove the temporary object"
    );

    let table_path = root.join("table-00000000000000000021.trinet");
    std::fs::write(&table_path, b"table").expect("table file writes");
    let objects = complete_after_blocking_worker_release(
        &runtime,
        backend.list_objects(
            StorageObjectListRequest::native_file(StorageObjectKind::Table, &root)
                .with_file_extension("trinet"),
        ),
        "object listing should wait behind the occupied blocking worker",
    )
    .expect("runtime object listing completes");
    assert_eq!(
        objects,
        vec![StorageObjectId::native_file(
            StorageObjectKind::Table,
            &table_path
        )]
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn runtime_enabled_native_file_append_operations_use_blocking_adapter() {
    let root = temp_storage_root("trine-kv-runtime-append-storage");
    let object = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));
    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 8);
    let backend = NativeFileBackend::with_runtime(runtime.clone());

    let mut append = complete_after_blocking_worker_release(
        &runtime,
        backend.open_append(object.clone()),
        "append open should wait behind the occupied blocking worker",
    )
    .expect("runtime append object opens");
    complete_after_blocking_worker_release(
        &runtime,
        StorageAppendObject::append(&mut append, b"first", DurabilityMode::Buffered),
        "append write should wait behind the occupied blocking worker",
    )
    .expect("runtime append write completes");
    complete_after_blocking_worker_release(
        &runtime,
        StorageAppendObject::persist(&mut append, DurabilityMode::Flush),
        "append persist should wait behind the occupied blocking worker",
    )
    .expect("runtime append persist completes");
    assert_eq!(
        std::fs::read(object.path()).expect("WAL object reads"),
        b"first"
    );

    let release = hold_runtime_blocking_worker(&runtime);
    append
        .append_blocking(b"second", DurabilityMode::Buffered)
        .expect("blocking append stays direct");
    append
        .persist_blocking(DurabilityMode::Buffered)
        .expect("blocking append persist stays direct");
    release.send(()).expect("release blocking worker");
    assert_eq!(
        std::fs::read(object.path()).expect("WAL object reads"),
        b"firstsecond"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}
