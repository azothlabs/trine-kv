use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{BlockingFallback, PlatformNativeAsyncButPartial};

    // The selected compio Windows backend opens files with FILE_FLAG_OVERLAPPED
    // and submits positioned ReadFile/WriteFile operations through IOCP. Trine
    // operations stay partial until their open, metadata, sync, rename, delete,
    // directory, and lease steps also have audited Windows-native async paths.
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
