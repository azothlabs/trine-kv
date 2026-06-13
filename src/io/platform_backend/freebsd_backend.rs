use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{PlatformNativeAsyncButPartial, ThreadPoolManagedAsync};

    // compio-driver 0.7.1 enables libc AIO for FreeBSD read/write/sync
    // primitives. Complete Trine operations stay partial when they also need
    // blocking open, metadata, rename, delete, directory, or lease steps.
    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::FreeBsdNative,
        length_lookup: ThreadPoolManagedAsync,
        owned_random_read: PlatformNativeAsyncButPartial,
        optional_whole_object_read: PlatformNativeAsyncButPartial,
        temp_write_rename_publish: PlatformNativeAsyncButPartial,
        append_object_open: ThreadPoolManagedAsync,
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
