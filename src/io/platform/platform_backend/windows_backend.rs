use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{PlatformNativeAsyncButPartial, ThreadPoolManagedAsync};

    // The selected compio Windows backend opens files with FILE_FLAG_OVERLAPPED
    // and submits positioned ReadFile/WriteFile operations through IOCP. Trine
    // operations that include those reads/writes stay partial until their open,
    // metadata, sync, rename, directory, and lease steps also have audited
    // Windows-native async paths. Operations without an IOCP read/write substep
    // are completed through platform-io's managed blocking lane.
    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::WindowsNative,
        length_lookup: ThreadPoolManagedAsync,
        owned_random_read: PlatformNativeAsyncButPartial,
        optional_whole_object_read: PlatformNativeAsyncButPartial,
        temp_write_rename_publish: PlatformNativeAsyncButPartial,
        append_object_open: ThreadPoolManagedAsync,
        append: PlatformNativeAsyncButPartial,
        persist: ThreadPoolManagedAsync,
        wal_rewrite: PlatformNativeAsyncButPartial,
        object_delete: ThreadPoolManagedAsync,
        directory_create: ThreadPoolManagedAsync,
        directory_sync: ThreadPoolManagedAsync,
        directory_listing: ThreadPoolManagedAsync,
        writer_lease_acquire: ThreadPoolManagedAsync,
    }
}
