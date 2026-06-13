use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{BlockingFallback, PlatformNativeAsyncButPartial};

    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::WindowsNative,
        length_lookup: PlatformNativeAsyncButPartial,
        owned_random_read: PlatformNativeAsyncButPartial,
        optional_whole_object_read: PlatformNativeAsyncButPartial,
        temp_write_rename_publish: PlatformNativeAsyncButPartial,
        append_object_open: PlatformNativeAsyncButPartial,
        append: PlatformNativeAsyncButPartial,
        persist: PlatformNativeAsyncButPartial,
        wal_rewrite: PlatformNativeAsyncButPartial,
        object_delete: PlatformNativeAsyncButPartial,
        directory_create: PlatformNativeAsyncButPartial,
        directory_sync: PlatformNativeAsyncButPartial,
        directory_listing: BlockingFallback,
        writer_lease_acquire: PlatformNativeAsyncButPartial,
    }
}
