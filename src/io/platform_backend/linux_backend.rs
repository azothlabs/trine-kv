use crate::io::{PlatformIoBackendKind, PlatformIoBackendMatrix, PlatformIoTaskClass};

pub(super) const fn matrix() -> PlatformIoBackendMatrix {
    use PlatformIoTaskClass::{ThreadPoolManagedAsync, TruePlatformAsync};

    PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::LinuxNative,
        length_lookup: TruePlatformAsync,
        owned_random_read: TruePlatformAsync,
        optional_whole_object_read: TruePlatformAsync,
        temp_write_rename_publish: TruePlatformAsync,
        append_object_open: TruePlatformAsync,
        append: TruePlatformAsync,
        persist: TruePlatformAsync,
        wal_rewrite: TruePlatformAsync,
        object_delete: TruePlatformAsync,
        directory_create: TruePlatformAsync,
        directory_sync: TruePlatformAsync,
        // Linux io_uring does not expose a directory enumeration opcode in the
        // selected UAPI/crate stack, so platform-io completes listing through
        // its managed blocking lane rather than a native completion.
        directory_listing: ThreadPoolManagedAsync,
        writer_lease_acquire: TruePlatformAsync,
    }
}
