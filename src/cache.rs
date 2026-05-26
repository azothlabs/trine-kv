use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    Result,
    table::{DecodedDataBlock, TableId},
};

const BLOCK_CACHE_SHARD_COUNT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheKind {
    Block,
    TableMetadata,
    Filter,
    BlobRead,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct BlockCacheKey {
    table_id: TableId,
    block_index: usize,
}

impl BlockCacheKey {
    pub(crate) const fn new(table_id: TableId, block_index: usize) -> Self {
        Self {
            table_id,
            block_index,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockCache {
    capacity_bytes: u64,
    shard_capacity_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    shards: Vec<RwLock<BlockCacheState>>,
}

impl BlockCache {
    pub(crate) fn new(capacity_bytes: usize) -> Self {
        let capacity_bytes = match u64::try_from(capacity_bytes) {
            Ok(value) => value,
            Err(_) => u64::MAX,
        };
        let shard_capacity_bytes = shard_capacity_bytes(capacity_bytes, BLOCK_CACHE_SHARD_COUNT);
        let shards = (0..BLOCK_CACHE_SHARD_COUNT)
            .map(|_| RwLock::new(BlockCacheState::default()))
            .collect();

        Self {
            capacity_bytes,
            shard_capacity_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            shards,
        }
    }

    pub(crate) fn get_or_insert_with(
        &self,
        key: BlockCacheKey,
        load: impl FnOnce() -> Result<DecodedDataBlock>,
    ) -> Result<Arc<DecodedDataBlock>> {
        if self.capacity_bytes == 0 {
            self.misses.fetch_add(1, Ordering::AcqRel);
            return load().map(Arc::new);
        }

        // Hits are the hot path, so split cache metadata across shards and let
        // concurrent readers share each shard. Misses load the block outside
        // the shard write lock; another reader may race and insert the same
        // block first, which is harmless and keeps file I/O out of the lock.
        let shard = &self.shards[block_cache_shard_index(key)];
        if let Ok(state) = shard.read() {
            if let Some(entry) = state.entries.get(&key) {
                self.hits.fetch_add(1, Ordering::AcqRel);
                return Ok(Arc::clone(&entry.bytes));
            }
        }

        let loaded = Arc::new(load()?);
        let loaded_bytes = loaded.estimated_bytes().max(1);
        let Ok(mut state) = shard.write() else {
            self.misses.fetch_add(1, Ordering::AcqRel);
            return Ok(loaded);
        };
        if let Some(entry) = state.entries.get(&key) {
            self.hits.fetch_add(1, Ordering::AcqRel);
            return Ok(Arc::clone(&entry.bytes));
        }

        self.misses.fetch_add(1, Ordering::AcqRel);
        if loaded_bytes <= self.capacity_bytes {
            state.insert(key, loaded_bytes, Arc::clone(&loaded));
            state.evict_to(self.shard_capacity_bytes);
        }

        Ok(loaded)
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Acquire),
            misses: self.misses.load(Ordering::Acquire),
        }
    }
}

fn shard_capacity_bytes(capacity_bytes: u64, shard_count: usize) -> u64 {
    let shard_count = u64::try_from(shard_count).unwrap_or(u64::MAX).max(1);
    capacity_bytes.saturating_add(shard_count.saturating_sub(1)) / shard_count
}

fn block_cache_shard_index(key: BlockCacheKey) -> usize {
    let mixed = key.table_id.get().wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ usize_to_u64_saturating(key.block_index);
    usize::try_from(mixed % usize_to_u64_saturating(BLOCK_CACHE_SHARD_COUNT)).unwrap_or(0)
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[derive(Debug, Default)]
struct BlockCacheState {
    entries: BTreeMap<BlockCacheKey, BlockCacheEntry>,
    order: VecDeque<BlockCacheKey>,
    bytes: u64,
}

#[derive(Debug, Clone)]
struct BlockCacheEntry {
    bytes: Arc<DecodedDataBlock>,
    size: u64,
}

impl BlockCacheState {
    fn insert(&mut self, key: BlockCacheKey, size: u64, bytes: Arc<DecodedDataBlock>) {
        if self
            .entries
            .insert(key, BlockCacheEntry { bytes, size })
            .is_none()
        {
            self.order.push_back(key);
            self.bytes = self.bytes.saturating_add(size);
        }
    }

    fn evict_to(&mut self, capacity_bytes: u64) {
        while self.bytes > capacity_bytes {
            let Some(key) = self.order.pop_front() else {
                self.entries.clear();
                self.bytes = 0;
                return;
            };
            if let Some(entry) = self.entries.remove(&key) {
                self.bytes = self.bytes.saturating_sub(entry.size);
            }
        }
    }
}
