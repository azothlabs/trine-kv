use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{PlatformNativeAsyncButPartial, ThreadPoolManagedAsync};

    // macOS data-path reads and writes use DispatchIO through the Apple
    // platform module. Complete Trine operations stay partial when they also
    // include metadata, rename, delete, directory, or durability steps without
    // an Apple file-completion primitive.
    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::MacOsNative,
        length_lookup: ThreadPoolManagedAsync,
        owned_random_read: PlatformNativeAsyncButPartial,
        optional_whole_object_read: PlatformNativeAsyncButPartial,
        temp_write_rename_publish: PlatformNativeAsyncButPartial,
        append_object_open: PlatformNativeAsyncButPartial,
        append: PlatformNativeAsyncButPartial,
        persist: PlatformNativeAsyncButPartial,
        wal_rewrite: PlatformNativeAsyncButPartial,
        object_delete: ThreadPoolManagedAsync,
        directory_create: ThreadPoolManagedAsync,
        directory_sync: PlatformNativeAsyncButPartial,
        directory_listing: ThreadPoolManagedAsync,
        writer_lease_acquire: PlatformNativeAsyncButPartial,
    }
}
