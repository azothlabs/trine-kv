use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{BlockingFallback, PlatformManagedFallback};

    // The selected compio macOS path uses the Unix polling driver for regular
    // files. In compio-driver 0.7.1, macOS regular-file open/stat/read/write,
    // sync, rename, delete, and directory operations are submitted as blocking
    // decisions or direct syscalls rather than audited platform async file
    // completions. Keep macOS explicit while preserving honest fallback classes.
    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::MacOsNative,
        length_lookup: PlatformManagedFallback,
        owned_random_read: PlatformManagedFallback,
        optional_whole_object_read: PlatformManagedFallback,
        temp_write_rename_publish: PlatformManagedFallback,
        append_object_open: PlatformManagedFallback,
        append: PlatformManagedFallback,
        persist: PlatformManagedFallback,
        wal_rewrite: PlatformManagedFallback,
        object_delete: PlatformManagedFallback,
        directory_create: PlatformManagedFallback,
        directory_sync: PlatformManagedFallback,
        directory_listing: BlockingFallback,
        writer_lease_acquire: PlatformManagedFallback,
    }
}
