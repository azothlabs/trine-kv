use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::ThreadPoolManagedAsync;

    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::UnixFallback,
        length_lookup: ThreadPoolManagedAsync,
        owned_random_read: ThreadPoolManagedAsync,
        optional_whole_object_read: ThreadPoolManagedAsync,
        temp_write_rename_publish: ThreadPoolManagedAsync,
        append_object_open: ThreadPoolManagedAsync,
        append: ThreadPoolManagedAsync,
        persist: ThreadPoolManagedAsync,
        wal_rewrite: ThreadPoolManagedAsync,
        object_delete: ThreadPoolManagedAsync,
        directory_create: ThreadPoolManagedAsync,
        directory_sync: ThreadPoolManagedAsync,
        directory_listing: ThreadPoolManagedAsync,
        writer_lease_acquire: ThreadPoolManagedAsync,
    }
}
