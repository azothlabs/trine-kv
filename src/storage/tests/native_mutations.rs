use super::*;

#[test]
fn native_file_backend_lists_matching_file_objects() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-list-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");
    std::fs::write(root.join("table-00000000000000000001.trinet"), b"table")
        .expect("table file writes");
    std::fs::write(root.join("table-00000000000000000002.TRINET"), b"table")
        .expect("uppercase table file writes");
    std::fs::write(root.join("MANIFEST"), b"manifest").expect("manifest file writes");
    std::fs::create_dir(root.join("table-00000000000000000003.trinet"))
        .expect("table-shaped dir creates");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::ObjectListing)
        .expect("native-file backend supports object listing");
    let request = StorageObjectListRequest::native_file(StorageObjectKind::Table, &root)
        .with_file_extension("trinet");
    let objects = backend
        .list_objects_blocking(request)
        .expect("objects list");
    assert!(
        objects
            .iter()
            .all(|object| object.kind() == StorageObjectKind::Table)
    );
    let mut names = objects
        .iter()
        .map(|object| {
            object
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .expect("listed path has utf-8 file name")
                .to_owned()
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(
        names,
        vec![
            "table-00000000000000000001.trinet".to_owned(),
            "table-00000000000000000002.TRINET".to_owned(),
        ]
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}
#[test]
fn native_file_backend_writes_table_object() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-write-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let path = root.join("table-00000000000000000007.trinet");
    let object = StorageObjectId::native_file(StorageObjectKind::Table, &path);

    let backend = NativeFileBackend::new();
    let capabilities = backend.capabilities();
    capabilities
        .require(StorageCapability::ObjectWrite)
        .expect("native-file backend supports object writes");
    capabilities
        .require_durability(DurabilityMode::SyncAll)
        .expect("native-file backend supports strict object sync");
    backend
        .write_object_blocking(
            object.clone(),
            Arc::from(&b"table bytes"[..]),
            DurabilityMode::SyncAll,
        )
        .expect("table object writes");

    assert_eq!(
        std::fs::read(object.path()).expect("table object reads"),
        b"table bytes"
    );
    assert!(
        !path.with_extension("tmp").exists(),
        "successful table write should leave only the final object"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_writes_blob_object() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-write-blob-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let path = root.join("blob-00000000000000000007.trineb");
    let object = StorageObjectId::native_file(StorageObjectKind::Blob, &path);

    let backend = NativeFileBackend::new();
    let capabilities = backend.capabilities();
    capabilities
        .require(StorageCapability::ObjectWrite)
        .expect("native-file backend supports object writes");
    backend
        .write_object_blocking(
            object.clone(),
            Arc::from(&b"blob bytes"[..]),
            DurabilityMode::SyncAll,
        )
        .expect("blob object writes");

    assert_eq!(
        std::fs::read(object.path()).expect("blob object reads"),
        b"blob bytes"
    );
    assert!(
        !path.with_extension("tmp").exists(),
        "successful blob write should leave only the final object"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_writes_recovery_report_object() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-write-recovery-report-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let path = root.join("RECOVERY_REPORT");
    let tmp_path = path.with_extension("tmp");
    let object = StorageObjectId::native_file(StorageObjectKind::RecoveryReport, &path);

    NativeFileBackend::new()
        .write_object_blocking(
            object.clone(),
            Arc::from(&b"recovery report"[..]),
            DurabilityMode::SyncAll,
        )
        .expect("recovery report object writes");

    assert_eq!(
        std::fs::read(object.path()).expect("recovery report object reads"),
        b"recovery report"
    );
    assert!(
        !tmp_path.exists(),
        "successful recovery report write should leave only the final object"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_object_write_rejects_manifest_objects() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-write-manifest-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let object = StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));

    let backend = NativeFileBackend::new();
    let error = backend
        .write_object_blocking(object, Arc::from(&b"manifest"[..]), DurabilityMode::SyncAll)
        .expect_err("manifest objects use manifest publish");
    assert!(matches!(error, Error::InvalidOptions { .. }));
}

#[test]
fn native_file_backend_deletes_table_and_blob_objects() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-delete-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");
    let table_path = root.join("table-00000000000000000007.trinet");
    let blob_path = root.join("blob-00000000000000000007.trineb");
    std::fs::write(&table_path, b"table").expect("table object writes");
    std::fs::write(&blob_path, b"blob").expect("blob object writes");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)
        .expect("native-file backend supports object deletes");
    backend
        .delete_object_blocking(StorageObjectId::native_file(
            StorageObjectKind::Table,
            &table_path,
        ))
        .expect("table object deletes");
    backend
        .delete_object_blocking(StorageObjectId::native_file(
            StorageObjectKind::Blob,
            &blob_path,
        ))
        .expect("blob object deletes");
    backend
        .delete_object_blocking(StorageObjectId::native_file(
            StorageObjectKind::Blob,
            &blob_path,
        ))
        .expect("missing object delete is idempotent");

    assert!(!table_path.exists(), "table object should be deleted");
    assert!(!blob_path.exists(), "blob object should be deleted");

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_object_delete_rejects_manifest_objects() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-delete-manifest-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let object = StorageObjectId::native_file(StorageObjectKind::Manifest, root.join("MANIFEST"));

    let backend = NativeFileBackend::new();
    let error = backend
        .delete_object_blocking(object)
        .expect_err("manifest objects use manifest publish");
    assert!(matches!(error, Error::InvalidOptions { .. }));
}

#[test]
fn native_file_backend_appends_wal_object_with_capabilities() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-append-wal-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let object = StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal"));

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::Append)
        .expect("native-file backend supports append");
    let mut append = backend
        .open_append_blocking(object.clone())
        .expect("WAL append object opens");

    append
        .append_blocking(b"first", DurabilityMode::Buffered)
        .expect("first WAL bytes append");
    append
        .append_blocking(b"second", DurabilityMode::Flush)
        .expect("second WAL bytes append");
    append
        .persist_blocking(DurabilityMode::SyncData)
        .expect("WAL append object persists");

    assert_eq!(
        std::fs::read(object.path()).expect("WAL object reads"),
        b"firstsecond"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_append_rejects_non_wal_objects() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-append-table-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let object = StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet"));

    let error = NativeFileBackend::new()
        .open_append_blocking(object)
        .expect_err("only WAL objects use append");
    assert!(matches!(error, Error::InvalidOptions { .. }));
}

#[test]
fn native_file_backend_rewrites_wal_with_explicit_temporary_object() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-wal-rewrite-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");
    let wal_path = root.join("trine.wal");
    let tmp_path = root.join("trine.wal.tmp");
    std::fs::write(&wal_path, b"old wal").expect("old WAL writes");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::AtomicWalRewrite)
        .expect("native-file backend supports WAL rewrite");
    backend
        .rewrite_wal_blocking(
            StorageObjectId::native_file(StorageObjectKind::Wal, &wal_path),
            StorageObjectId::native_file(StorageObjectKind::Wal, &tmp_path),
            Arc::from(&b"new wal"[..]),
            DurabilityMode::SyncAll,
        )
        .expect("WAL rewrites");

    assert_eq!(std::fs::read(&wal_path).expect("WAL reads"), b"new wal");
    assert!(
        !tmp_path.exists(),
        "successful WAL rewrite should remove the explicit temporary object"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_wal_rewrite_rejects_non_wal_objects() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-wal-rewrite-kind-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let backend = NativeFileBackend::new();
    let error = backend
        .rewrite_wal_blocking(
            StorageObjectId::native_file(StorageObjectKind::Table, root.join("table.trinet")),
            StorageObjectId::native_file(StorageObjectKind::Wal, root.join("trine.wal.tmp")),
            Arc::from(&b"bytes"[..]),
            DurabilityMode::SyncAll,
        )
        .expect_err("WAL rewrite only accepts WAL objects");
    assert!(matches!(error, Error::InvalidOptions { .. }));
}

#[test]
fn native_file_backend_acquires_and_releases_writer_lease() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-writer-lease-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let object = StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::WriterLease)
        .expect("native-file backend supports writer leases");
    let lease = backend
        .acquire_writer_lease_blocking(object.clone())
        .expect("writer lease acquires");
    assert!(object.path().exists(), "writer lease marker should exist");

    let error = backend
        .acquire_writer_lease_blocking(object.clone())
        .expect_err("existing writer lease fails closed");
    assert!(error.to_string().contains("database lock is already held"));

    drop(lease);
    assert!(
        object.path().exists(),
        "dropping owned writer lease should keep the lock file inode"
    );
    assert!(
        std::fs::read(object.path())
            .expect("writer lease marker reads")
            .is_empty(),
        "dropping owned writer lease should clear owner text"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_recovers_stale_writer_lease_marker() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-writer-lease-stale-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");
    let object = StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
    std::fs::write(object.path(), b"pid=stale\nnonce=stale\n")
        .expect("stale writer lease marker writes");

    let lease = NativeFileBackend::new()
        .acquire_writer_lease_blocking(object.clone())
        .expect("stale writer lease marker does not block OS lock acquire");
    let marker = std::fs::read_to_string(object.path()).expect("lease marker reads");
    assert_ne!(
        marker, "pid=stale\nnonce=stale\n",
        "acquiring over a stale marker should publish the new owner"
    );

    let error = NativeFileBackend::new()
        .acquire_writer_lease_blocking(object.clone())
        .expect_err("live OS writer lease still blocks a second writer");
    assert!(error.to_string().contains("database lock is already held"));

    drop(lease);
    assert!(
        object.path().exists(),
        "dropping recovered writer lease should keep the lock file inode"
    );
    assert!(
        std::fs::read(object.path())
            .expect("recovered writer lease marker reads")
            .is_empty(),
        "dropping recovered writer lease should clear owner text"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_writer_lease_does_not_remove_changed_marker() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-writer-lease-changed-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let object = StorageObjectId::native_file(StorageObjectKind::WriterLease, root.join("LOCK"));
    let mut lease = NativeFileBackend::new()
        .acquire_writer_lease_blocking(object.clone())
        .expect("writer lease acquires");
    let file = lease
        .file
        .as_mut()
        .expect("native writer lease owns a file");
    file.set_len(0).expect("lease marker truncates");
    file.seek(SeekFrom::Start(0)).expect("lease marker seeks");
    file.write_all(b"pid=other\nnonce=other\n")
        .expect("lease marker changes");
    file.flush().expect("lease marker flushes");

    drop(lease);

    assert_eq!(
        std::fs::read(object.path()).expect("changed lease marker remains"),
        b"pid=other\nnonce=other\n"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_creates_directory_tree() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-directory-create-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    let nested = root.join("db").join("nested");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::DirectoryCreate)
        .expect("native-file backend supports directory create");
    backend
        .create_directory_all_blocking(StorageDirectoryId::native_file(&nested))
        .expect("directory tree creates");

    assert!(nested.is_dir(), "nested directory should exist");

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_lists_directory_files() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-directory-list-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");
    std::fs::write(root.join("MANIFEST.tmp"), b"manifest").expect("manifest tmp writes");
    std::fs::write(root.join("trine.wal.tmp"), b"wal").expect("wal tmp writes");
    std::fs::create_dir(root.join("nested")).expect("nested dir creates");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)
        .expect("native-file backend supports directory listing");
    let files = backend
        .list_directory_files_blocking(StorageDirectoryId::native_file(&root))
        .expect("directory files list");
    let names = files
        .iter()
        .map(|file| {
            file.path()
                .file_name()
                .and_then(|name| name.to_str())
                .expect("file name is UTF-8")
        })
        .collect::<Vec<_>>();

    assert_eq!(names, vec!["MANIFEST.tmp", "trine.wal.tmp"]);
    let lengths = files
        .iter()
        .map(|file| file.byte_len().expect("native listing records byte length"))
        .collect::<Vec<_>>();
    assert_eq!(lengths, vec![8, 3]);

    std::fs::remove_dir_all(root).expect("test dir removes");
}

#[test]
fn native_file_backend_syncs_directory_after_renames() {
    let root = std::env::temp_dir().join(format!(
        "trine-kv-storage-directory-sync-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("test dir creates");

    let tmp_path = root.join("value.tmp");
    let published_path = root.join("value.trinet");
    std::fs::write(&tmp_path, b"published").expect("temp file writes");
    std::fs::rename(&tmp_path, &published_path).expect("file renames");

    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::DirectorySync)
        .expect("native-file backend supports directory sync");
    backend
        .sync_directory_after_renames_blocking(StorageDirectoryId::native_file(&root))
        .expect("directory sync succeeds");

    let parent = StorageDirectoryId::native_file_parent_of(&published_path)
        .expect("published path has parent directory");
    assert_eq!(parent.path(), root.as_path());
    assert_eq!(
        std::fs::read(&published_path).expect("published file reads"),
        b"published"
    );

    std::fs::remove_dir_all(root).expect("test dir removes");
}
