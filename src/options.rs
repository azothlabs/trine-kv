use std::path::PathBuf;

use crate::{codec::CodecId, prefix::PrefixExtractor};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMode {
    InMemory,
    Persistent { path: PathBuf },
}

impl Default for StorageMode {
    fn default() -> Self {
        Self::InMemory
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurabilityMode {
    #[default]
    Buffered,
    Flush,
    SyncData,
    SyncAll,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompressionProfile {
    None,
    #[default]
    Fast,
}

impl CompressionProfile {
    #[must_use]
    pub const fn codec_id(self) -> CodecId {
        match self {
            Self::None => CodecId::None,
            Self::Fast => CodecId::FastLz4Block,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterPolicy {
    Disabled,
    Bloom { bits_per_key: u8 },
}

impl Default for FilterPolicy {
    fn default() -> Self {
        Self::Bloom { bits_per_key: 10 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixFilterPolicy {
    Disabled,
    Bloom { bits_per_prefix: u8 },
}

impl Default for PrefixFilterPolicy {
    fn default() -> Self {
        Self::Bloom {
            bits_per_prefix: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexSearchPolicy {
    Linear,
    Binary,
    Eytzinger,
    GallopingWithHint,
    #[default]
    Auto,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FailOnCorruptionPolicy {
    #[default]
    FailClosed,
    RepairSafeTemporaryFiles,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbOptions {
    pub storage_mode: StorageMode,
    pub create_if_missing: bool,
    pub read_only: bool,
    /// Options used when the built-in default bucket is first created.
    pub default_bucket_options: BucketOptions,
    pub durability: DurabilityMode,
    pub write_buffer_bytes: usize,
    pub max_immutable_memtables: usize,
    pub target_table_bytes: usize,
    pub level_size_multiplier: usize,
    pub max_l0_files: usize,
    pub block_cache_bytes: usize,
    pub background_worker_count: usize,
    pub fail_on_corruption: FailOnCorruptionPolicy,
}

impl DbOptions {
    pub const DEFAULT_WRITE_BUFFER_BYTES: usize = 64 * 1024 * 1024;
    pub const DEFAULT_TARGET_TABLE_BYTES: usize = 64 * 1024 * 1024;
    pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 256 * 1024 * 1024;

    #[must_use]
    pub fn memory() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn persistent(path: impl Into<PathBuf>) -> Self {
        Self {
            storage_mode: StorageMode::Persistent { path: path.into() },
            ..Self::default()
        }
    }

    #[must_use]
    pub fn persistent_read_only(path: impl Into<PathBuf>) -> Self {
        Self::persistent(path).read_only()
    }

    #[must_use]
    pub const fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = durability;
        self
    }

    #[must_use]
    pub const fn read_only(mut self) -> Self {
        self.read_only = true;
        self.create_if_missing = false;
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
            block_cache_bytes: Self::DEFAULT_BLOCK_CACHE_BYTES,
            background_worker_count: 0,
            fail_on_corruption: FailOnCorruptionPolicy::FailClosed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketOptions {
    pub allow_empty_keys: bool,
    pub compression: CompressionProfile,
    pub block_bytes: usize,
    pub filter_policy: FilterPolicy,
    pub prefix_extractor: PrefixExtractor,
    pub prefix_filter_policy: PrefixFilterPolicy,
    pub index_search_policy: IndexSearchPolicy,
    pub blob_threshold_bytes: usize,
}

impl BucketOptions {
    pub const DEFAULT_BLOCK_BYTES: usize = 16 * 1024;
    pub const DEFAULT_BLOB_THRESHOLD_BYTES: usize = 1024 * 1024;

    #[must_use]
    pub fn with_prefix_extractor(mut self, prefix_extractor: PrefixExtractor) -> Self {
        self.prefix_extractor = prefix_extractor;
        self
    }

    #[must_use]
    pub const fn with_blob_threshold_bytes(mut self, blob_threshold_bytes: usize) -> Self {
        self.blob_threshold_bytes = blob_threshold_bytes;
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
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub durability: DurabilityMode,
}

impl WriteOptions {
    #[must_use]
    pub const fn new(durability: DurabilityMode) -> Self {
        Self { durability }
    }

    #[must_use]
    pub const fn buffered() -> Self {
        Self::new(DurabilityMode::Buffered)
    }

    #[must_use]
    pub const fn flush() -> Self {
        Self::new(DurabilityMode::Flush)
    }

    #[must_use]
    pub const fn sync_data() -> Self {
        Self::new(DurabilityMode::SyncData)
    }

    #[must_use]
    pub const fn sync_all() -> Self {
        Self::new(DurabilityMode::SyncAll)
    }
}
