use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{BackendFallback, BlockingFallback};

    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::UnsupportedFallback,
        length_lookup: BackendFallback,
        owned_random_read: BackendFallback,
        optional_whole_object_read: BackendFallback,
        temp_write_rename_publish: BackendFallback,
        append_object_open: BackendFallback,
        append: BackendFallback,
        persist: BackendFallback,
        object_delete: BackendFallback,
        directory_create: BackendFallback,
        directory_sync: BackendFallback,
        directory_listing: BlockingFallback,
        writer_lease_acquire: BackendFallback,
    }
}
