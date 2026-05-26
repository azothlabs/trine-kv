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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CacheKind {
    DataBlock,
    IndexBlock,
    FilterBlock,
    RangeTombstoneBlock,
    BlobBlock,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct BlockCacheKey {
    kind: CacheKind,
    table_id: TableId,
    block_index: usize,
}

impl BlockCacheKey {
    pub(crate) const fn new(table_id: TableId, block_index: usize) -> Self {
        Self::with_kind(CacheKind::DataBlock, table_id, block_index)
    }

    pub(crate) const fn with_kind(kind: CacheKind, table_id: TableId, block_index: usize) -> Self {
        Self {
            kind,
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
                let bytes = Arc::clone(&entry.bytes);
                drop(state);
                if let Ok(mut state) = shard.write() {
                    state.promote(key);
                }
                self.hits.fetch_add(1, Ordering::AcqRel);
                return Ok(bytes);
            }
        }

        let loaded = Arc::new(load()?);
        let loaded_bytes = loaded.estimated_bytes().max(1);
        let Ok(mut state) = shard.write() else {
            self.misses.fetch_add(1, Ordering::AcqRel);
            return Ok(loaded);
        };
        if let Some(entry) = state.entries.get(&key) {
            let bytes = Arc::clone(&entry.bytes);
            state.promote(key);
            self.hits.fetch_add(1, Ordering::AcqRel);
            return Ok(bytes);
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
    let kind = key.kind as u64;
    let mixed = key.table_id.get().wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ usize_to_u64_saturating(key.block_index)
        ^ kind.wrapping_mul(0x517C_C1B7_2722_0A95);
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

    fn promote(&mut self, key: BlockCacheKey) {
        let Some(position) = self.order.iter().position(|candidate| *candidate == key) else {
            return;
        };
        self.order.remove(position);
        self.order.push_back(key);
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        BLOCK_CACHE_SHARD_COUNT, BlockCache, BlockCacheKey, CacheKind, block_cache_shard_index,
    };
    use crate::table::{DecodedDataBlock, TableId};

    #[test]
    fn cache_keys_include_block_kind() {
        let data = BlockCacheKey::with_kind(CacheKind::DataBlock, TableId(7), 3);
        let filter = BlockCacheKey::with_kind(CacheKind::FilterBlock, TableId(7), 3);
        let range_tombstone =
            BlockCacheKey::with_kind(CacheKind::RangeTombstoneBlock, TableId(7), 3);
        let blob = BlockCacheKey::with_kind(CacheKind::BlobBlock, TableId(7), 3);

        assert_ne!(data, filter);
        assert_ne!(data, range_tombstone);
        assert_ne!(data, blob);
    }

    #[test]
    fn cache_hit_promotes_entry_before_eviction() {
        let keys = keys_in_same_shard(3);
        let cache = BlockCache::new(BLOCK_CACHE_SHARD_COUNT * 2);
        let loads = AtomicUsize::new(0);

        cache
            .get_or_insert_with(keys[0], || Ok(load_counted_block(&loads)))
            .expect("first block loads");
        cache
            .get_or_insert_with(keys[1], || Ok(load_counted_block(&loads)))
            .expect("second block loads");
        cache
            .get_or_insert_with(keys[0], || Ok(load_counted_block(&loads)))
            .expect("first block hits and promotes");
        cache
            .get_or_insert_with(keys[2], || Ok(load_counted_block(&loads)))
            .expect("third block loads and evicts one entry");
        let loads_after_eviction = loads.load(Ordering::Acquire);

        cache
            .get_or_insert_with(keys[0], || Ok(load_counted_block(&loads)))
            .expect("promoted first block stays cached");
        assert_eq!(loads.load(Ordering::Acquire), loads_after_eviction);

        cache
            .get_or_insert_with(keys[1], || Ok(load_counted_block(&loads)))
            .expect("least recently used second block reloads");
        assert_eq!(loads.load(Ordering::Acquire), loads_after_eviction + 1);
    }

    fn load_counted_block(loads: &AtomicUsize) -> DecodedDataBlock {
        loads.fetch_add(1, Ordering::AcqRel);
        DecodedDataBlock::empty_for_cache_test()
    }

    fn keys_in_same_shard(count: usize) -> Vec<BlockCacheKey> {
        let mut keys = Vec::new();
        let mut table_id = 1_u64;
        let mut shard = None;
        while keys.len() < count {
            let key = BlockCacheKey::new(TableId(table_id), 0);
            let key_shard = block_cache_shard_index(key);
            if shard.is_none_or(|shard| shard == key_shard) {
                shard = Some(key_shard);
                keys.push(key);
            }
            table_id += 1;
        }
        keys
    }
}
