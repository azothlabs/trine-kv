use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{BlockingFallback, PlatformManagedFallback};

    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::UnixFallback,
        length_lookup: PlatformManagedFallback,
        owned_random_read: PlatformManagedFallback,
        optional_whole_object_read: PlatformManagedFallback,
        temp_write_rename_publish: PlatformManagedFallback,
        append_object_open: PlatformManagedFallback,
        append: PlatformManagedFallback,
        persist: PlatformManagedFallback,
        wal_rewrite: PlatformManagedFallback,
        object_delete: PlatformManagedFallback,
        directory_create: PlatformManagedFallback,
        directory_sync: PlatformManagedFallback,
        directory_listing: BlockingFallback,
        writer_lease_acquire: PlatformManagedFallback,
    }
}
