use super::{
    Db, DbOptions, DurabilityMode, Error, HostStorageBackend, Result, StorageMode, runtime,
    validate_common_options,
};

impl Db {
    #[cfg_attr(
        not(all(target_arch = "wasm32", target_os = "unknown")),
        allow(dead_code)
    )]
    pub(in crate::db) fn validate_browser_persistent_options(options: &DbOptions) -> Result<()> {
        if !matches!(
            options.storage_mode,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. }
            }
        ) {
            return Err(Error::invalid_options(
                "browser persistent open requires browser backend",
            ));
        }
        if options.read_only && options.create_if_missing {
            return Err(Error::invalid_options(
                "browser read-only open cannot create missing storage",
            ));
        }
        if matches!(
            options.durability,
            DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
        ) {
            return Err(Error::unsupported_durability(options.durability));
        }
        if options.runtime.mode != runtime::RuntimeMode::Inline {
            return Err(Error::invalid_options(
                "browser persistent backend requires inline runtime",
            ));
        }
        if options.background_worker_count != 0 {
            return Err(Error::invalid_options(
                "browser persistent backend does not support background workers yet",
            ));
        }
        validate_common_options(options)
    }
}
