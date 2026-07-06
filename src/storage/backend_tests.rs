use super::*;

#[test]
fn native_variant_delegates_backend_traits() {
    let backend = StorageBackend::Native(NativeFileBackend::new());
    let direct = NativeFileBackend::new();
    // Capability reporting must match the wrapped backend exactly: the enum
    // is a transparent dispatcher, not a policy layer.
    assert_eq!(
        backend
            .capabilities()
            .supports(StorageCapability::Persistent),
        direct
            .capabilities()
            .supports(StorageCapability::Persistent)
    );
    assert_eq!(
        backend
            .capabilities()
            .supports(StorageCapability::ObjectWrite),
        direct
            .capabilities()
            .supports(StorageCapability::ObjectWrite)
    );
}

#[test]
fn object_store_variant_dispatches_byte_ops_and_rejects_the_rest() {
    use crate::object_store::InMemoryObjectStore;

    let backend = StorageBackend::ObjectStore(ObjectStoreBackend::new(Arc::new(
        InMemoryObjectStore::new(),
    )));
    let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/0001.trinet");

    // Byte ops dispatch to the object-store backend.
    poll_ready_storage_future(backend.write_object(
        id.clone(),
        Arc::from(b"hi".as_slice()),
        DurabilityMode::Flush,
    ))
    .expect("write");
    assert_eq!(
        poll_ready_storage_future(backend.read_object_bytes(id.clone()))
            .expect("read")
            .as_deref(),
        Some(b"hi".as_slice())
    );
    assert!(
        backend
            .capabilities()
            .supports(StorageCapability::ObjectWrite)
    );
    assert!(!backend.capabilities().supports(StorageCapability::Append));

    // Non-byte ops are unsupported here: object-store DBs are async-only and
    // drive WAL/manifest ownership outside this byte backend.
    assert!(
        poll_ready_storage_future(
            backend.create_directory_all(StorageDirectoryId::native_file("/db"))
        )
        .is_err()
    );
    assert!(
        backend.open_read_blocking(id).is_err(),
        "object store is async-only"
    );
}
