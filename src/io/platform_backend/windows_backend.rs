use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{
        BlockingFallback, PlatformManagedFallback, PlatformNativeAsyncButPartial,
    };

    // The selected compio Windows backend opens files with FILE_FLAG_OVERLAPPED
    // and submits positioned ReadFile/WriteFile operations through IOCP. Trine
    // operations that include those reads/writes stay partial until their open,
    // metadata, sync, rename, directory, and lease steps also have audited
    // Windows-native async paths. Operations without an IOCP read/write substep
    // are platform-managed or explicit blocking fallbacks.
    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::WindowsNative,
        length_lookup: PlatformManagedFallback,
        owned_random_read: PlatformNativeAsyncButPartial,
        optional_whole_object_read: PlatformNativeAsyncButPartial,
        temp_write_rename_publish: PlatformNativeAsyncButPartial,
        append_object_open: PlatformManagedFallback,
        append: PlatformNativeAsyncButPartial,
        persist: PlatformManagedFallback,
        wal_rewrite: PlatformNativeAsyncButPartial,
        object_delete: PlatformManagedFallback,
        directory_create: PlatformManagedFallback,
        directory_sync: PlatformManagedFallback,
        directory_listing: BlockingFallback,
        writer_lease_acquire: PlatformNativeAsyncButPartial,
    }
}
