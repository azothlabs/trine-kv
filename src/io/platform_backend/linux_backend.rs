use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{BlockingFallback, TrueAsync};

    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::LinuxNative,
        length_lookup: TrueAsync,
        owned_random_read: TrueAsync,
        optional_whole_object_read: TrueAsync,
        temp_write_rename_publish: TrueAsync,
        append_object_open: TrueAsync,
        append: TrueAsync,
        persist: TrueAsync,
        object_delete: TrueAsync,
        directory_create: TrueAsync,
        directory_sync: TrueAsync,
        directory_listing: BlockingFallback,
        writer_lease_acquire: TrueAsync,
    }
}
