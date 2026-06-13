use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{
        BlockingFallback, PlatformManagedFallback, PlatformNativeAsyncButPartial,
    };

    // compio-driver 0.7.1 enables libc AIO for FreeBSD read/write/sync
    // primitives. Complete Trine operations stay partial when they also need
    // blocking open, metadata, rename, delete, directory, or lease steps.
    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::FreeBsdNative,
        length_lookup: PlatformManagedFallback,
        owned_random_read: PlatformNativeAsyncButPartial,
        optional_whole_object_read: PlatformNativeAsyncButPartial,
        temp_write_rename_publish: PlatformNativeAsyncButPartial,
        append_object_open: PlatformManagedFallback,
        append: PlatformNativeAsyncButPartial,
        persist: PlatformNativeAsyncButPartial,
        wal_rewrite: PlatformNativeAsyncButPartial,
        object_delete: PlatformManagedFallback,
        directory_create: PlatformManagedFallback,
        directory_sync: PlatformNativeAsyncButPartial,
        directory_listing: BlockingFallback,
        writer_lease_acquire: PlatformNativeAsyncButPartial,
    }
}
