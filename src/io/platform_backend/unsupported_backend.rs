use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::Unsupported;

    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::UnsupportedFallback,
        length_lookup: Unsupported,
        owned_random_read: Unsupported,
        optional_whole_object_read: Unsupported,
        temp_write_rename_publish: Unsupported,
        append_object_open: Unsupported,
        append: Unsupported,
        persist: Unsupported,
        wal_rewrite: Unsupported,
        object_delete: Unsupported,
        directory_create: Unsupported,
        directory_sync: Unsupported,
        directory_listing: Unsupported,
        writer_lease_acquire: Unsupported,
    }
}
