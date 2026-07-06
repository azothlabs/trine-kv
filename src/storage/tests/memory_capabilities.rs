use super::*;

#[test]
fn memory_storage_backend_exposes_async_read_shape() {
    let backend = MemoryStorageBackend::new();
    let capabilities = backend.capabilities();
    assert!(capabilities.supports(StorageCapability::Volatile));
    assert!(capabilities.supports(StorageCapability::RandomRead));
    assert!(capabilities.supports(StorageCapability::ObjectRead));
    assert!(!capabilities.supports(StorageCapability::Persistent));
    assert!(matches!(
        capabilities.require(StorageCapability::Persistent),
        Err(Error::UnsupportedBackend {
            feature: "persistent storage"
        })
    ));

    let object_id = StorageObjectId::memory(StorageObjectKind::Table, "table-7");
    backend
        .insert_read_object(object_id.clone(), Vec::from(&b"abcdef"[..]))
        .expect("memory object inserts");

    let object =
        poll_ready_storage_future(backend.open_read(object_id)).expect("memory object opens");
    assert_eq!(
        StorageReadObject::object(&object).kind(),
        StorageObjectKind::Table
    );
    assert_eq!(
        poll_ready_storage_future(StorageReadObject::len(&object)).expect("length reads"),
        6
    );

    let mut bytes = [0_u8; 3];
    poll_ready_storage_future(StorageReadObject::read_exact_at(&object, 1, &mut bytes))
        .expect("range reads");
    assert_eq!(&bytes, b"bcd");

    let owned = poll_ready_storage_future(StorageReadObject::read_exact_at_owned(&object, 2, 3))
        .expect("owned range reads");
    assert_eq!(owned.offset(), 2);
    assert_eq!(owned.len(), 3);
    assert!(!owned.is_empty());
    assert_eq!(&*owned.into_bytes(), b"cde");

    let owned_blocking = object
        .read_exact_at_owned_blocking(0, 0)
        .expect("empty owned range reads");
    assert_eq!(owned_blocking.offset(), 0);
    assert_eq!(owned_blocking.len(), 0);
    assert!(owned_blocking.is_empty());
    assert_eq!(&*owned_blocking.into_bytes(), b"");

    let full = backend
        .read_object_bytes_blocking(StorageObjectId::memory(StorageObjectKind::Table, "table-7"))
        .expect("memory object read succeeds")
        .expect("memory object exists");
    assert_eq!(&*full, b"abcdef");
    assert!(
        backend
            .read_object_bytes_blocking(StorageObjectId::memory(
                StorageObjectKind::Table,
                "missing-table",
            ))
            .expect("missing memory object read succeeds")
            .is_none()
    );
}

#[test]
fn storage_capabilities_report_unsupported_backend_and_durability() {
    let read_only = StorageCapabilities::native_file_read();
    assert!(read_only.supports(StorageCapability::Persistent));
    assert!(read_only.supports(StorageCapability::RandomRead));
    assert!(read_only.supports(StorageCapability::ObjectRead));
    assert!(read_only.supports(StorageCapability::ObjectListing));
    assert!(read_only.supports(StorageCapability::DirectoryListing));
    assert!(!read_only.supports(StorageCapability::ObjectWrite));
    assert!(!read_only.supports(StorageCapability::ObjectDelete));
    assert!(!read_only.supports(StorageCapability::Append));
    assert!(!read_only.supports(StorageCapability::AtomicWalRewrite));
    assert!(!read_only.supports(StorageCapability::DirectoryCreate));
    assert!(!read_only.supports(StorageCapability::DirectorySync));
    assert!(!read_only.supports(StorageCapability::WriterLease));
    assert!(matches!(
        read_only.require(StorageCapability::Append),
        Err(Error::UnsupportedBackend { feature: "append" })
    ));
    assert!(read_only.supports_durability(DurabilityMode::Buffered));
    assert!(matches!(
        read_only.require_durability(DurabilityMode::SyncAll),
        Err(Error::UnsupportedDurability {
            requested: DurabilityMode::SyncAll
        })
    ));

    let strict = StorageCapabilities::empty()
        .with(StorageCapability::Flush)
        .with(StorageCapability::StrictDataSync)
        .with(StorageCapability::StrictMetadataSync);
    assert!(strict.supports_durability(DurabilityMode::SyncAll));
}
