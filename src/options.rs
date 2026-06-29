use std::path::{Path, PathBuf};

use crate::{codec::CodecId, limits, prefix::PrefixExtractor, runtime::RuntimeOptions};

/// Storage location and host backend selected for a database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMode {
    /// Keep all data in memory and discard it when the handle closes.
    InMemory,
    /// Store data in a native filesystem directory.
    Persistent {
        /// Native filesystem database directory.
        path: PathBuf,
    },
    /// Store data through an explicit host-provided backend.
    HostPersistent {
        /// Host backend used for persistent storage.
        backend: HostStorageBackend,
    },
}

/// Host storage backend selected for non-native persistent modes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostStorageBackend {
    /// Use a WASI preopened filesystem path.
    Wasi {
        /// WASI preopened filesystem path.
        path: PathBuf,
    },
    /// Use browser storage.
    Browser,
    /// Use object storage (S3 and compatible). The object-store client is
    /// supplied at open time, not encoded here. Async-only.
    ObjectStore,
}

impl HostStorageBackend {
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Wasi { .. } => "WASI persistent storage backend",
            Self::Browser => "browser persistent storage backend",
            Self::ObjectStore => "object-store persistent storage backend",
        }
    }
}

impl StorageMode {
    pub(crate) fn persistent_path(&self) -> Option<&Path> {
        match self {
            Self::Persistent { path }
            | Self::HostPersistent {
                backend: HostStorageBackend::Wasi { path },
            } => Some(path.as_path()),
            Self::InMemory
            | Self::HostPersistent {
                backend: HostStorageBackend::Browser | HostStorageBackend::ObjectStore,
            } => None,
        }
    }

    pub(crate) const fn is_wasi_persistent(&self) -> bool {
        matches!(
            self,
            Self::HostPersistent {
                backend: HostStorageBackend::Wasi { .. }
            }
        )
    }

    pub(crate) const fn is_browser_persistent(&self) -> bool {
        matches!(
            self,
            Self::HostPersistent {
                backend: HostStorageBackend::Browser
            }
        )
    }

    pub(crate) const fn is_object_store_persistent(&self) -> bool {
        matches!(
            self,
            Self::HostPersistent {
                backend: HostStorageBackend::ObjectStore
            }
        )
    }
}

impl Default for StorageMode {
    fn default() -> Self {
        Self::InMemory
    }
}

/// Durability requested for committed writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurabilityMode {
    /// Accept writes after buffering them in the storage backend.
    #[default]
    Buffered,
    /// Flush buffered bytes to the backend.
    Flush,
    /// Sync file data without requiring metadata sync where the backend allows it.
    ///
    /// Non-strict: on macOS this is a plain `fsync`, which flushes to the drive
    /// but may leave the data in the drive's volatile cache, so it is not
    /// guaranteed to survive sudden power loss (it does survive a process crash
    /// or kernel panic). See [`SyncAllStrict`](Self::SyncAllStrict).
    SyncData,
    /// Sync file data and metadata where the backend allows it.
    ///
    /// Non-strict, same power-loss caveat as [`SyncData`](Self::SyncData) on
    /// macOS. This is the default for persistent databases: it trades guaranteed
    /// power-loss durability for much higher commit throughput.
    SyncAll,
    /// Strict full sync: flush file data and metadata all the way through the
    /// drive's volatile cache to permanent storage.
    ///
    /// On macOS this issues `fcntl(F_FULLFSYNC)`, the only call that survives
    /// sudden power loss, at a large throughput cost (it is the per-commit fsync
    /// floor). On Linux/Windows the ordinary full sync already flushes durably,
    /// so this behaves like [`SyncAll`](Self::SyncAll) there. Choose this when a
    /// commit must survive power loss; otherwise prefer the faster non-strict
    /// modes.
    SyncAllStrict,
}

impl DurabilityMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Flush => "flush",
            Self::SyncData => "sync-data",
            Self::SyncAll => "sync-all",
            Self::SyncAllStrict => "sync-all-strict",
        }
    }
}

/// Compression codec profile used for table blocks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompressionProfile {
    /// Store table blocks without compression.
    None,
    /// Use the v1 fast LZ4 block codec.
    #[default]
    Fast,
}

impl CompressionProfile {
    #[must_use]
    pub(crate) const fn codec_id(self) -> CodecId {
        match self {
            Self::None => CodecId::None,
            Self::Fast => CodecId::FastLz4Block,
        }
    }
}

/// Point-read filter policy for table keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterPolicy {
    /// Do not write table key filters.
    Disabled,
    /// Write Bloom filters with the requested bit budget per key.
    Bloom {
        /// Bloom-filter bit budget per key.
        bits_per_key: u8,
    },
}

impl Default for FilterPolicy {
    fn default() -> Self {
        Self::Bloom { bits_per_key: 10 }
    }
}

/// Prefix-read filter policy for extracted key prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixFilterPolicy {
    /// Do not write prefix filters.
    Disabled,
    /// Write Bloom filters with the requested bit budget per prefix.
    Bloom {
        /// Bloom-filter bit budget per extracted prefix.
        bits_per_prefix: u8,
    },
}

impl Default for PrefixFilterPolicy {
    fn default() -> Self {
        Self::Bloom {
            bits_per_prefix: 10,
        }
    }
}

/// Search strategy used inside table indexes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexSearchPolicy {
    /// Scan index entries linearly.
    Linear,
    /// Search index entries with binary search.
    Binary,
    /// Let Trine choose based on index size.
    #[default]
    Auto,
}

/// Startup policy when repairable temporary files are found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FailOnCorruptionPolicy {
    /// Report the files as corruption and leave them untouched.
    #[default]
    FailClosed,
    /// Delete safe temporary files and write a recovery report.
    RepairSafeTemporaryFiles,
}

/// Options used when opening a database.
///
/// `DbOptions` controls where data is stored, whether missing storage is
/// created, the default write durability, bucket defaults, runtime behavior,
/// maintenance thresholds, and startup recovery policy. Path-like calls to
/// [`crate::Db::open`] and [`crate::Db::open_sync`] are converted with
/// [`DbOptions::new`], which selects persistent native filesystem storage.
///
/// # Examples
///
/// ```rust
/// use trine_kv::{Db, DbOptions, DurabilityMode};
///
/// # fn main() -> trine_kv::Result<()> {
/// let persistent = Db::open_sync(
///     DbOptions::new("target/doc-example-options")
///         .with_durability(DurabilityMode::SyncAll),
/// )?;
///
/// let memory = Db::open_sync(DbOptions::memory())?;
/// assert_eq!(persistent.options().read_only, false);
/// assert_eq!(memory.options().read_only, false);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbOptions {
    /// Storage location and host backend.
    pub storage_mode: StorageMode,
    /// Create the database directory and metadata when missing.
    pub create_if_missing: bool,
    /// Open without allowing writes or maintenance that mutates storage.
    pub read_only: bool,
    /// Options used when the built-in default bucket is first created.
    pub default_bucket_options: BucketOptions,
    /// Default durability used by write helpers that do not pass `WriteOptions`.
    pub durability: DurabilityMode,
    /// Active memtable target size before flush.
    pub write_buffer_bytes: usize,
    /// Maximum queued immutable memtables before writes apply backpressure.
    pub max_immutable_memtables: usize,
    /// Target size for newly written table files.
    pub target_table_bytes: usize,
    /// Per-level size multiplier used by compaction.
    pub level_size_multiplier: usize,
    /// Maximum L0 table count before compaction is requested.
    pub max_l0_files: usize,
    /// How object-store opens treat the supplied [`crate::ObjectClient`].
    ///
    /// The default is [`ObjectClientTrustMode::Trusted`]: `Db::open_object_store*`
    /// assumes the client already satisfies Trine's conditional-write and
    /// same-key visibility contract. Prefer that default for production opens
    /// after validating the adapter in CI, at process startup, or in a deploy
    /// health check with [`crate::verify_object_client_contract`].
    ///
    /// [`ObjectClientTrustMode::VerifyOnOpen`] is available as a fail-closed
    /// diagnostic mode. It adds object-store requests and latency to each
    /// writable object-store open, so it is not the default.
    pub object_client_trust: ObjectClientTrustMode,
    /// Maximum user key length accepted by write APIs, in bytes.
    ///
    /// The same limit applies to delete keys and range-delete bounds. The value
    /// must be non-zero and may not exceed [`DbOptions::MAX_WRITE_FIELD_BYTES`].
    /// Lower this limit for multi-tenant or untrusted workloads to cap WAL,
    /// memtable, table-index, and comparison work caused by oversized keys.
    pub max_key_bytes: usize,
    /// Maximum user value length accepted by write APIs, in bytes.
    ///
    /// The value must be non-zero and may not exceed
    /// [`DbOptions::MAX_WRITE_FIELD_BYTES`]. This limit is checked before WAL
    /// acceptance or memtable publication; larger payloads should be stored
    /// outside the ordinary value path and referenced from Trine records.
    pub max_value_bytes: usize,
    /// Approximate bytes reserved for cached table blocks.
    pub block_cache_bytes: usize,
    /// Number of background workers used by maintenance-capable runtimes.
    pub background_worker_count: usize,
    /// Policy for the number of WAL shards (independent append lanes).
    ///
    /// Defaults to [`WalShardPolicy::Auto`], which picks one lane for the
    /// per-commit-fsync durable-write regime (so group commit engages) and a
    /// small parallel set otherwise. See [`WalShardPolicy`] for the trade-offs
    /// and use [`DbOptions::with_wal_shards`] / [`DbOptions::with_wal_shard_count`]
    /// to override.
    pub wal_shards: WalShardPolicy,
    /// Runtime used for async, blocking, and background work.
    pub runtime: RuntimeOptions,
    /// Startup policy for safe temporary files left by interrupted writes.
    pub fail_on_corruption: FailOnCorruptionPolicy,
    /// Number of most recent read versions retained even when no snapshot or
    /// checkpoint pins them.
    pub keep_last_read_versions: u64,
    /// Enable garbage collection of obsolete blob bytes.
    pub blob_gc_enabled: bool,
    /// Minimum discardable-byte ratio required before a blob file is collected.
    pub blob_gc_discardable_ratio: BlobGcRatio,
    /// Minimum blob file size considered for garbage collection.
    pub blob_gc_min_file_bytes: u64,
}

impl DbOptions {
    /// Default active memtable target size.
    pub const DEFAULT_WRITE_BUFFER_BYTES: usize = 64 * 1024 * 1024;
    /// Default target size for table files.
    pub const DEFAULT_TARGET_TABLE_BYTES: usize = 64 * 1024 * 1024;
    /// Default maximum user key length accepted by write APIs.
    pub const DEFAULT_MAX_KEY_BYTES: usize = limits::MAX_DECODED_BLOCK_BYTES;
    /// Default maximum user value length accepted by write APIs.
    pub const DEFAULT_MAX_VALUE_BYTES: usize = limits::MAX_DECODED_BLOCK_BYTES;
    /// Maximum configurable limit for a single write byte field.
    ///
    /// This is tied to Trine's internal decoded-block safety bound. Keeping
    /// public write limits at or below this value prevents accepted records from
    /// later creating table, WAL, or blob decode paths that exceed the engine's
    /// allocation guardrails.
    pub const MAX_WRITE_FIELD_BYTES: usize = limits::MAX_DECODED_BLOCK_BYTES;
    /// Default block-cache byte budget.
    pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 256 * 1024 * 1024;
    /// Default minimum blob file size for garbage collection.
    pub const DEFAULT_BLOB_GC_MIN_FILE_BYTES: u64 = 64 * 1024 * 1024;
    /// Default number of recent read versions retained without explicit pins.
    pub const DEFAULT_KEEP_LAST_READ_VERSIONS: u64 = 1;

    /// Creates persistent database options for `path`.
    ///
    /// This is the path-first constructor used by `Db::open(path)` and
    /// `Db::open_sync(path)`. It selects [`StorageMode::Persistent`], sets
    /// safety-first default durability for confirmed writes, and leaves
    /// `create_if_missing` enabled.
    ///
    /// # Parameters
    ///
    /// - `path`: native filesystem database directory.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::persistent(path)
    }

    /// Creates in-memory database options.
    ///
    /// In-memory databases keep memtables, tables, metadata, and WAL state only
    /// in process memory. Closing the handle drops all data. This is useful for
    /// tests, temporary indexes, and caches that can be rebuilt.
    #[must_use]
    pub fn memory() -> Self {
        Self::default()
    }

    /// Creates persistent native-filesystem options for `path`.
    ///
    /// This is equivalent to [`DbOptions::new`].
    #[must_use]
    pub fn persistent(path: impl Into<PathBuf>) -> Self {
        Self {
            storage_mode: StorageMode::Persistent { path: path.into() },
            durability: DurabilityMode::SyncAll,
            ..Self::default()
        }
    }

    /// Creates persistent WASI host-filesystem options for `path`.
    #[must_use]
    pub fn wasi_persistent(path: impl Into<PathBuf>) -> Self {
        Self {
            storage_mode: StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { path: path.into() },
            },
            background_worker_count: 0,
            durability: DurabilityMode::Flush,
            runtime: RuntimeOptions::inline(),
            ..Self::default()
        }
    }

    /// Creates read-only WASI host-filesystem options for `path`.
    #[must_use]
    pub fn wasi_persistent_read_only(path: impl Into<PathBuf>) -> Self {
        Self::wasi_persistent(path).read_only()
    }

    /// Creates browser persistent options.
    #[must_use]
    pub fn browser_persistent() -> Self {
        Self {
            storage_mode: StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser,
            },
            background_worker_count: 0,
            durability: DurabilityMode::Flush,
            runtime: RuntimeOptions::inline(),
            ..Self::default()
        }
    }

    /// Creates object-storage persistent options (async-only). The object-store
    /// client is supplied to the async open, not encoded here.
    #[doc(hidden)]
    #[must_use]
    pub fn object_store() -> Self {
        Self {
            storage_mode: StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            },
            background_worker_count: 0,
            durability: DurabilityMode::Flush,
            runtime: RuntimeOptions::inline(),
            ..Self::default()
        }
    }

    /// Creates read-only browser persistent options.
    #[must_use]
    pub fn browser_persistent_read_only() -> Self {
        Self::browser_persistent().read_only()
    }

    /// Creates read-only native-filesystem options for `path`.
    #[must_use]
    pub fn persistent_read_only(path: impl Into<PathBuf>) -> Self {
        Self::persistent(path).read_only()
    }

    /// Sets the default durability for writes that do not pass `WriteOptions`.
    ///
    /// This affects helpers such as [`crate::Db::put_sync`] and
    /// [`crate::Db::put`]. Calls that pass [`WriteOptions`] use their explicit
    /// durability instead.
    #[must_use]
    pub const fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = durability;
        self
    }

    /// Sets the options used by the built-in default bucket.
    ///
    /// Named buckets still use the options passed to `Db::bucket_with_options`.
    #[must_use]
    pub fn with_default_bucket_options(mut self, options: BucketOptions) -> Self {
        self.default_bucket_options = options;
        self
    }

    /// Marks these options read-only and disables creation of missing storage.
    ///
    /// Read-only handles can read existing persistent data, but reject writes,
    /// flushes, compactions, and repairs that would mutate storage.
    #[must_use]
    pub const fn read_only(mut self) -> Self {
        self.read_only = true;
        self.create_if_missing = false;
        self
    }

    /// Sets how many recent read versions remain available without an active
    /// [`crate::Snapshot`] or named checkpoint pin.
    ///
    /// A value of `1` retains only the latest read version by configuration.
    /// Higher values allow applications to use recently returned
    /// [`crate::ReadVersion`] cursors with [`crate::Db::snapshot_at`] even after
    /// newer writes commit, as long as the requested version is still within
    /// the window. Active snapshots and checkpoints can retain older versions
    /// independently of this setting. `0` is invalid and rejected when the
    /// database opens.
    ///
    /// # Parameters
    ///
    /// - `count`: number of recent read versions to retain. The unit is read
    ///   versions, not bytes or time.
    #[must_use]
    pub const fn with_keep_last_read_versions(mut self, count: u64) -> Self {
        self.keep_last_read_versions = count;
        self
    }

    /// Sets the WAL shard policy (see [`WalShardPolicy`]).
    #[must_use]
    pub const fn with_wal_shards(mut self, policy: WalShardPolicy) -> Self {
        self.wal_shards = policy;
        self
    }

    /// Sets a fixed WAL shard (lane) count. Convenience for
    /// `with_wal_shards(WalShardPolicy::Fixed(count))`: use 1 to force group
    /// commit on single-device storage, or a higher count to parallelize WAL
    /// fsyncs on hardware that flushes files in parallel.
    #[must_use]
    pub const fn with_wal_shard_count(mut self, count: usize) -> Self {
        self.wal_shards = WalShardPolicy::Fixed(count);
        self
    }

    /// Sets how object-store opens treat the supplied object client.
    ///
    /// The default is [`ObjectClientTrustMode::Trusted`], which does not run a
    /// contract probe during open. Use [`ObjectClientTrustMode::VerifyOnOpen`]
    /// only when the extra object-store requests are acceptable, such as adapter
    /// development or a temporary diagnostic rollout. For normal production
    /// deployments, prefer running [`crate::verify_object_client_contract`] from
    /// CI, startup, or a deployment health check.
    #[must_use]
    pub const fn with_object_client_trust(mut self, trust: ObjectClientTrustMode) -> Self {
        self.object_client_trust = trust;
        self
    }

    /// Sets the maximum user key length accepted by write APIs.
    ///
    /// This applies to put keys, delete keys, and both bounds of range deletes.
    /// The unit is bytes after the caller has encoded the key. `0` is invalid,
    /// and values above [`DbOptions::MAX_WRITE_FIELD_BYTES`] are rejected when
    /// the database opens.
    #[must_use]
    pub const fn with_max_key_bytes(mut self, max_key_bytes: usize) -> Self {
        self.max_key_bytes = max_key_bytes;
        self
    }

    /// Sets the maximum user value length accepted by write APIs.
    ///
    /// The unit is bytes after the caller has encoded the value. `0` is invalid,
    /// and values above [`DbOptions::MAX_WRITE_FIELD_BYTES`] are rejected when
    /// the database opens. Writes whose value exceeds this limit fail before
    /// they enter WAL acceptance or memtable publication.
    #[must_use]
    pub const fn with_max_value_bytes(mut self, max_value_bytes: usize) -> Self {
        self.max_value_bytes = max_value_bytes;
        self
    }
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            storage_mode: StorageMode::InMemory,
            create_if_missing: true,
            read_only: false,
            default_bucket_options: BucketOptions::default(),
            durability: DurabilityMode::Buffered,
            write_buffer_bytes: Self::DEFAULT_WRITE_BUFFER_BYTES,
            max_immutable_memtables: 4,
            target_table_bytes: Self::DEFAULT_TARGET_TABLE_BYTES,
            level_size_multiplier: 10,
            max_l0_files: 8,
            object_client_trust: ObjectClientTrustMode::default(),
            max_key_bytes: Self::DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: Self::DEFAULT_MAX_VALUE_BYTES,
            block_cache_bytes: Self::DEFAULT_BLOCK_CACHE_BYTES,
            background_worker_count: 1,
            wal_shards: WalShardPolicy::Auto,
            runtime: RuntimeOptions::default(),
            fail_on_corruption: FailOnCorruptionPolicy::FailClosed,
            keep_last_read_versions: Self::DEFAULT_KEEP_LAST_READ_VERSIONS,
            blob_gc_enabled: true,
            blob_gc_discardable_ratio: BlobGcRatio::HALF,
            blob_gc_min_file_bytes: Self::DEFAULT_BLOB_GC_MIN_FILE_BYTES,
        }
    }
}

/// Policy for trusting or probing an object-store client during open.
///
/// This only affects object-store database opens. It does not change the
/// database file format, WAL format, or recovery rules.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ObjectClientTrustMode {
    /// Trust that the supplied [`crate::ObjectClient`] satisfies Trine's object
    /// contract.
    ///
    /// This is the default because normal opens should be cheap and predictable
    /// after an adapter has already been qualified. It is not a safety proof and
    /// it does not probe the backend during open. Use
    /// [`crate::verify_object_client_contract`] in CI, process startup, or a
    /// deployment health check before relying on a custom adapter in this mode.
    ///
    /// If the adapter violates conditional-write or same-key read-after-write
    /// semantics, `Trusted` open will not detect that at open time; later WAL,
    /// lease, manifest, and recovery checks still fail closed when they can
    /// observe corruption or fencing conflicts.
    #[default]
    Trusted,
    /// Run the object-client contract probe during each writable object-store
    /// open.
    ///
    /// This mode is useful while developing a new adapter or diagnosing a
    /// provider. It writes and deletes a small temporary probe object before
    /// writer ownership is taken. That adds latency, request cost, and a
    /// permission requirement to open, and proves only the probed key at that
    /// moment. Read-only opens never run this probe.
    VerifyOnOpen,
}

/// Policy for choosing the number of WAL shards (independent append lanes).
///
/// Each shard is one append-only WAL file served by its own worker thread, and
/// each worker batches the commits queued on its lane into a single fsync (group
/// commit). The number of lanes trades two things against each other:
///
/// - Fewer lanes concentrate concurrent commits on one worker, so group commit
///   coalesces many commits under one fsync. On single-device storage, where
///   fsyncs to different files serialize at the device, this is the only way to
///   raise durable-write throughput, and one lane is the physical optimum (a
///   single `fsync` covers exactly one file, so commits cannot be merged across
///   lane files).
/// - More lanes spread commits across files, which only helps when the hardware
///   can flush multiple files in parallel (multiple devices, or an `NVMe` with
///   parallel flush). On a serial-fsync device, more lanes give each lane too
///   few concurrent commits to batch, so throughput stays at the fsync floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalShardPolicy {
    /// Choose the lane count from the scenario, recomputed at open:
    ///
    /// - Persistent storage with per-commit fsync durability
    ///   ([`DurabilityMode::SyncData`] / [`DurabilityMode::SyncAll`]) uses one
    ///   lane, so group commit coalesces concurrent commits under one fsync.
    ///   This is the durable-write bottleneck and the single-device optimum.
    /// - Persistent storage without per-commit fsync ([`DurabilityMode::Buffered`]
    ///   / [`DurabilityMode::Flush`]) uses a small parallel set of lanes, since
    ///   there is no per-commit fsync to coalesce and parallel append lanes help.
    /// - In-memory and host backends use one lane; there is no device fsync to
    ///   parallelize.
    ///
    /// Override with [`WalShardPolicy::Fixed`] on hardware that flushes files in
    /// parallel.
    Auto,
    /// Use exactly this many lanes (clamped to at least one). Set `Fixed(1)` to
    /// force group commit on single-device storage, or a higher count to
    /// parallelize WAL fsyncs where the device supports parallel flush.
    Fixed(usize),
}

impl WalShardPolicy {
    /// Lane count used when [`WalShardPolicy::Auto`] does not pick per-commit
    /// fsync mode (no fsync per write, so parallel append lanes are harmless and
    /// can help).
    const AUTO_PARALLEL_LANES: usize = crate::wal::DEFAULT_WAL_SHARD_COUNT;

    /// Resolve the policy to a concrete lane count (always at least one) for the
    /// given storage mode and default durability.
    pub(crate) fn resolve(self, storage_mode: &StorageMode, durability: DurabilityMode) -> usize {
        match self {
            Self::Fixed(count) => count.max(1),
            Self::Auto => {
                let per_commit_fsync = matches!(storage_mode, StorageMode::Persistent { .. })
                    && matches!(
                        durability,
                        DurabilityMode::SyncData
                            | DurabilityMode::SyncAll
                            | DurabilityMode::SyncAllStrict
                    );
                if per_commit_fsync {
                    // Group-commit regime: one lane lets the worker coalesce
                    // concurrent commits under a single fsync.
                    1
                } else {
                    Self::AUTO_PARALLEL_LANES
                }
            }
        }
    }
}

/// Ratio threshold used by blob garbage collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobGcRatio {
    millionths: u32,
}

impl BlobGcRatio {
    /// A 50% discardable-byte threshold.
    pub const HALF: Self = Self {
        millionths: 500_000,
    };
    /// A 100% discardable-byte threshold.
    pub const FULL: Self = Self {
        millionths: 1_000_000,
    };

    /// Creates a ratio from millionths, where `1_000_000` means 100%.
    #[must_use]
    pub const fn from_millionths(millionths: u32) -> Self {
        Self { millionths }
    }

    /// Returns this ratio in millionths.
    #[must_use]
    pub const fn millionths(self) -> u32 {
        self.millionths
    }

    pub(crate) fn should_collect(self, discardable_bytes: u64, total_bytes: u64) -> bool {
        if total_bytes == 0 {
            return false;
        }
        u128::from(discardable_bytes).saturating_mul(1_000_000)
            >= u128::from(total_bytes).saturating_mul(u128::from(self.millionths))
    }
}

impl Default for BlobGcRatio {
    fn default() -> Self {
        Self::HALF
    }
}

/// Policy for merging blob values back into table levels during compaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BlobLevelMergePolicy {
    /// Keep blob values in blob files during compaction.
    Disabled,
    /// Let Trine decide based on level and value size.
    #[default]
    Auto,
    /// Always merge blob values back into table files when compacting.
    Always,
}

/// Layered (Monkey-style) filter bits-per-element curve across LSM levels.
///
/// Both the point filter (`bits_per_key`) and the prefix filter
/// (`bits_per_prefix`) use this curve at write time. Pinned shallow levels keep
/// the configured base bits so hot, recent data stays accurately filtered;
/// deeper levels, which hold most keys and dominate filter memory, get fewer
/// bits under the descending variants (`Auto`/`Custom`), so total filter memory
/// cannot regress versus a flat allocation. The ascending `CostWeighted` variant
/// is the opposite shape for remote backends and may exceed the base. Filters are
/// self-describing, so this only affects how future tables are written, not how
/// existing tables are read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FilterDepthCurve {
    /// Built-in depth curve: deeper levels lose 2 bits each down to a floor of 4,
    /// shallow (pinned) levels keep the base. The recommended default.
    #[default]
    Auto,
    /// No curve: every level uses the base bits. Highest filter accuracy and
    /// highest filter memory; choose when memory is not constrained.
    Uniform,
    /// Custom depth curve: deeper levels lose `step` bits each, clamped to
    /// `floor` (and never above the base). The shallow boundary stays tied to the
    /// pinned-metadata levels. Raise `floor` on slow storage to keep deep-level
    /// false positives (and data-block reads) low at the cost of memory; lower it
    /// to save more memory.
    Custom {
        /// Bits removed per level below the shallow (pinned) boundary.
        step: u8,
        /// Minimum bits per element for the deepest levels.
        floor: u8,
    },
    /// Cost-weighted (ascending) curve for remote/cold backends: deeper levels
    /// *gain* `step` bits each, clamped up to `ceil`, while shallow (pinned)
    /// levels keep the base. This inverts classic Monkey: on the `s3` feature a
    /// deep-level filter miss costs a network round-trip, not a cheap local read,
    /// so deep levels deserve *more* filter budget, not less. Opt-in only; it can
    /// raise total filter memory above a flat allocation, so it is never a default
    /// and is unhelpful on local SSD. Measure before adopting on a real remote
    /// workload.
    CostWeighted {
        /// Bits added per level below the shallow (pinned) boundary.
        step: u8,
        /// Maximum bits per element for the deepest levels.
        ceil: u8,
    },
}

/// Options fixed for a bucket when the bucket is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketOptions {
    /// Allow empty user keys.
    pub allow_empty_keys: bool,
    /// Compression profile used for table blocks.
    pub compression: CompressionProfile,
    /// Target uncompressed data-block size.
    pub block_bytes: usize,
    /// Point-read filter policy for table keys.
    pub filter_policy: FilterPolicy,
    /// Prefix extractor used by prefix filters.
    pub prefix_extractor: PrefixExtractor,
    /// Prefix-read filter policy.
    pub prefix_filter_policy: PrefixFilterPolicy,
    /// Search strategy used inside table indexes.
    pub index_search_policy: IndexSearchPolicy,
    /// Values at or above this size are stored in blob files.
    pub blob_threshold_bytes: usize,
    /// Policy for merging blob values during compaction.
    pub blob_level_merge_policy: BlobLevelMergePolicy,
    /// Layered (Monkey-style) filter bits-per-element curve across levels,
    /// applied to both the point and prefix filters. Defaults to
    /// [`FilterDepthCurve::Auto`].
    pub filter_depth_curve: FilterDepthCurve,
}

impl BucketOptions {
    /// Default target data-block size.
    pub const DEFAULT_BLOCK_BYTES: usize = 16 * 1024;
    /// Default threshold for storing values in blob files.
    pub const DEFAULT_BLOB_THRESHOLD_BYTES: usize = 1024 * 1024;

    /// Sets the prefix extractor for this bucket.
    #[must_use]
    pub fn with_prefix_extractor(mut self, prefix_extractor: PrefixExtractor) -> Self {
        self.prefix_extractor = prefix_extractor;
        self
    }

    /// Sets the value-size threshold for blob storage.
    #[must_use]
    pub const fn with_blob_threshold_bytes(mut self, blob_threshold_bytes: usize) -> Self {
        self.blob_threshold_bytes = blob_threshold_bytes;
        self
    }

    /// Sets the blob-level merge policy.
    #[must_use]
    pub const fn with_blob_level_merge_policy(mut self, policy: BlobLevelMergePolicy) -> Self {
        self.blob_level_merge_policy = policy;
        self
    }

    /// Enables or disables blob-level merge with a boolean convenience flag.
    #[must_use]
    pub const fn with_blob_level_merge_enabled(mut self, enabled: bool) -> Self {
        self.blob_level_merge_policy = if enabled {
            BlobLevelMergePolicy::Always
        } else {
            BlobLevelMergePolicy::Disabled
        };
        self
    }

    /// Sets the layered filter bits-per-element curve (see [`FilterDepthCurve`]).
    #[must_use]
    pub const fn with_filter_depth_curve(mut self, curve: FilterDepthCurve) -> Self {
        self.filter_depth_curve = curve;
        self
    }
}

impl Default for BucketOptions {
    fn default() -> Self {
        Self {
            allow_empty_keys: true,
            compression: CompressionProfile::Fast,
            block_bytes: Self::DEFAULT_BLOCK_BYTES,
            filter_policy: FilterPolicy::default(),
            prefix_extractor: PrefixExtractor::default(),
            prefix_filter_policy: PrefixFilterPolicy::default(),
            index_search_policy: IndexSearchPolicy::Auto,
            blob_threshold_bytes: Self::DEFAULT_BLOB_THRESHOLD_BYTES,
            blob_level_merge_policy: BlobLevelMergePolicy::Auto,
            filter_depth_curve: FilterDepthCurve::Auto,
        }
    }
}

/// Per-write options passed to commit operations.
///
/// `WriteOptions` lets a single write or batch request a durability level
/// different from the database default. The options are evaluated when the
/// write is accepted; changing a `WriteOptions` value later has no effect on an
/// already committed write.
///
/// # Examples
///
/// ```rust
/// use trine_kv::{Db, WriteOptions};
///
/// # fn main() -> trine_kv::Result<()> {
/// let db = Db::open_sync(trine_kv::DbOptions::memory())?;
/// let commit = db.put_with_options_sync(b"k", b"v", WriteOptions::sync_all())?;
/// assert!(commit.read_version().as_u64() > 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteOptions {
    /// Durability requested for this write.
    pub durability: DurabilityMode,
}

impl WriteOptions {
    /// Creates write options with an explicit durability mode.
    ///
    /// # Parameters
    ///
    /// - `durability`: durability requested for the write or batch.
    #[must_use]
    pub const fn new(durability: DurabilityMode) -> Self {
        Self { durability }
    }

    /// Creates buffered write options.
    ///
    /// The write may return after bytes are accepted by the storage backend but
    /// before a flush or sync is requested. This is the lowest-latency and
    /// weakest durability mode.
    #[must_use]
    pub const fn buffered() -> Self {
        Self::new(DurabilityMode::Buffered)
    }

    /// Creates flush write options.
    ///
    /// The write requests a backend flush for accepted WAL bytes. Exact meaning
    /// depends on the selected backend capabilities.
    #[must_use]
    pub const fn flush() -> Self {
        Self::new(DurabilityMode::Flush)
    }

    /// Creates data-sync write options.
    ///
    /// The write requests durable file data without requiring metadata sync
    /// where the backend can distinguish those operations.
    #[must_use]
    pub const fn sync_data() -> Self {
        Self::new(DurabilityMode::SyncData)
    }

    /// Creates full-sync write options (non-strict).
    ///
    /// Syncs WAL data and required metadata. On macOS this is a plain `fsync`,
    /// which is not guaranteed to survive sudden power loss; use
    /// [`sync_all_strict`](Self::sync_all_strict) for that.
    #[must_use]
    pub const fn sync_all() -> Self {
        Self::new(DurabilityMode::SyncAll)
    }

    /// Creates strict full-sync write options.
    ///
    /// Requests the strongest durability: on macOS `fcntl(F_FULLFSYNC)`, which
    /// survives sudden power loss at a large throughput cost. On Linux/Windows it
    /// behaves like [`sync_all`](Self::sync_all). Use when a commit must be
    /// power-loss durable.
    #[must_use]
    pub const fn sync_all_strict() -> Self {
        Self::new(DurabilityMode::SyncAllStrict)
    }
}

#[cfg(test)]
mod tests {
    use super::{DurabilityMode, StorageMode, WalShardPolicy};
    use std::path::PathBuf;

    fn persistent() -> StorageMode {
        StorageMode::Persistent {
            path: PathBuf::from("/tmp/trine-wal-policy-test"),
        }
    }

    #[test]
    fn auto_uses_one_lane_for_per_commit_fsync_durability() {
        // Persistent + sync durability is the group-commit regime: one lane.
        assert_eq!(
            WalShardPolicy::Auto.resolve(&persistent(), DurabilityMode::SyncAll),
            1
        );
        assert_eq!(
            WalShardPolicy::Auto.resolve(&persistent(), DurabilityMode::SyncData),
            1
        );
    }

    #[test]
    fn auto_uses_parallel_lanes_without_per_commit_fsync() {
        // No per-commit fsync (Buffered/Flush) or in-memory: parallel lanes.
        assert_eq!(
            WalShardPolicy::Auto.resolve(&persistent(), DurabilityMode::Buffered),
            WalShardPolicy::AUTO_PARALLEL_LANES
        );
        assert_eq!(
            WalShardPolicy::Auto.resolve(&StorageMode::InMemory, DurabilityMode::SyncAll),
            WalShardPolicy::AUTO_PARALLEL_LANES
        );
    }

    #[test]
    fn fixed_clamps_to_at_least_one_lane() {
        assert_eq!(
            WalShardPolicy::Fixed(0).resolve(&persistent(), DurabilityMode::SyncAll),
            1
        );
        assert_eq!(
            WalShardPolicy::Fixed(8).resolve(&persistent(), DurabilityMode::SyncAll),
            8
        );
    }
}
