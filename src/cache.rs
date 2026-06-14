use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::{
    Error, Result,
    table::{DecodedDataBlock, TableDataBlock, TableId},
};

// Calibrated against the threaded point-read workload: 64 keeps high-thread
// reads stable but hurts 4-thread reads, while 256/512 regress high threads.
const BLOCK_CACHE_SHARD_COUNT: usize = 128;
const CACHE_COUNTER_SHARD_COUNT: usize = 128;

static NEXT_CACHE_COUNTER_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static CACHE_COUNTER_SHARD_INDEX: usize =
        NEXT_CACHE_COUNTER_SHARD.fetch_add(1, Ordering::Relaxed) % CACHE_COUNTER_SHARD_COUNT;
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CacheKind {
    DataBlock,
    IndexBlock,
    FilterBlock,
    RangeTombstoneBlock,
    BlobBlock,
}

impl CacheKind {
    const fn priority(self) -> CachePriority {
        match self {
            Self::IndexBlock | Self::FilterBlock | Self::RangeTombstoneBlock => CachePriority::High,
            Self::DataBlock | Self::BlobBlock => CachePriority::Low,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CachePriority {
    High,
    Low,
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
    hits: CacheCounter,
    misses: CacheCounter,
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
            hits: CacheCounter::new(),
            misses: CacheCounter::new(),
            shards,
        }
    }

    pub(crate) fn get_or_insert_data_block_with(
        &self,
        key: BlockCacheKey,
        load: impl FnOnce() -> Result<DecodedDataBlock>,
    ) -> Result<Arc<DecodedDataBlock>> {
        let value = self.get_or_insert_value_with(key, || {
            let block = Arc::new(load()?);
            Ok((
                CacheValue::DataBlock(Arc::clone(&block)),
                block.estimated_bytes(),
            ))
        })?;
        match value {
            CacheValue::DataBlock(block) => Ok(block),
            CacheValue::IndexPartition(_) => Err(cache_value_kind_mismatch(key)),
        }
    }

    pub(crate) async fn get_or_insert_data_block_with_async<F, Fut>(
        &self,
        key: BlockCacheKey,
        load: F,
    ) -> Result<Arc<DecodedDataBlock>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<DecodedDataBlock>>,
    {
        let value = self
            .get_or_insert_value_with_async(key, || async {
                let block = Arc::new(load().await?);
                Ok((
                    CacheValue::DataBlock(Arc::clone(&block)),
                    block.estimated_bytes(),
                ))
            })
            .await?;
        match value {
            CacheValue::DataBlock(block) => Ok(block),
            CacheValue::IndexPartition(_) => Err(cache_value_kind_mismatch(key)),
        }
    }

    pub(crate) fn get_or_insert_index_partition_with(
        &self,
        key: BlockCacheKey,
        load: impl FnOnce() -> Result<Vec<TableDataBlock>>,
    ) -> Result<Arc<Vec<TableDataBlock>>> {
        let value = self.get_or_insert_value_with(key, || {
            let partition = Arc::new(load()?);
            Ok((
                CacheValue::IndexPartition(Arc::clone(&partition)),
                estimate_index_partition_bytes(&partition),
            ))
        })?;
        match value {
            CacheValue::IndexPartition(partition) => Ok(partition),
            CacheValue::DataBlock(_) => Err(cache_value_kind_mismatch(key)),
        }
    }

    pub(crate) async fn get_or_insert_index_partition_with_async<F, Fut>(
        &self,
        key: BlockCacheKey,
        load: F,
    ) -> Result<Arc<Vec<TableDataBlock>>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<TableDataBlock>>>,
    {
        let value = self
            .get_or_insert_value_with_async(key, || async {
                let partition = Arc::new(load().await?);
                Ok((
                    CacheValue::IndexPartition(Arc::clone(&partition)),
                    estimate_index_partition_bytes(&partition),
                ))
            })
            .await?;
        match value {
            CacheValue::IndexPartition(partition) => Ok(partition),
            CacheValue::DataBlock(_) => Err(cache_value_kind_mismatch(key)),
        }
    }

    fn get_or_insert_value_with(
        &self,
        key: BlockCacheKey,
        load: impl FnOnce() -> Result<(CacheValue, u64)>,
    ) -> Result<CacheValue> {
        if self.capacity_bytes == 0 {
            self.misses.increment();
            return load().map(|(value, _)| value);
        }

        // Hits are the hot path, so split cache metadata across shards and let
        // concurrent readers share each shard. Hit recency is best effort:
        // readers only update queue order when the shard write lock is
        // immediately available. Misses load the block outside the shard write
        // lock; another reader may race and insert the same block first, which
        // is harmless and keeps file I/O out of the lock.
        let shard = &self.shards[block_cache_shard_index(key)];
        if let Ok(state) = shard.read() {
            if let Some(entry) = state.entries.get(&key) {
                let value = entry.value.clone();
                drop(state);
                if let Ok(mut state) = shard.try_write() {
                    state.promote(key);
                }
                self.hits.increment();
                return Ok(value);
            }
        }

        let (loaded, loaded_bytes) = load()?;
        let loaded_bytes = loaded_bytes.max(1);
        let Ok(mut state) = shard.write() else {
            self.misses.increment();
            return Ok(loaded);
        };
        if let Some(entry) = state.entries.get(&key) {
            let value = entry.value.clone();
            state.promote(key);
            self.hits.increment();
            return Ok(value);
        }

        self.misses.increment();
        if loaded_bytes <= self.capacity_bytes {
            state.insert(key, loaded_bytes, loaded.clone());
            state.evict_to(self.shard_capacity_bytes);
        }

        Ok(loaded)
    }

    async fn get_or_insert_value_with_async<F, Fut>(
        &self,
        key: BlockCacheKey,
        load: F,
    ) -> Result<CacheValue>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(CacheValue, u64)>>,
    {
        if self.capacity_bytes == 0 {
            self.misses.increment();
            return load().await.map(|(value, _)| value);
        }

        let shard = &self.shards[block_cache_shard_index(key)];
        if let Ok(state) = shard.read() {
            if let Some(entry) = state.entries.get(&key) {
                let value = entry.value.clone();
                drop(state);
                if let Ok(mut state) = shard.try_write() {
                    state.promote(key);
                }
                self.hits.increment();
                return Ok(value);
            }
        }

        let (loaded, loaded_bytes) = load().await?;
        let loaded_bytes = loaded_bytes.max(1);
        let Ok(mut state) = shard.write() else {
            self.misses.increment();
            return Ok(loaded);
        };
        if let Some(entry) = state.entries.get(&key) {
            let value = entry.value.clone();
            state.promote(key);
            self.hits.increment();
            return Ok(value);
        }

        self.misses.increment();
        if loaded_bytes <= self.capacity_bytes {
            state.insert(key, loaded_bytes, loaded.clone());
            state.evict_to(self.shard_capacity_bytes);
        }

        Ok(loaded)
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(),
            misses: self.misses.load(),
        }
    }
}

#[derive(Debug)]
struct CacheCounter {
    shards: Vec<CacheCounterShard>,
}

#[derive(Debug)]
#[repr(align(64))]
struct CacheCounterShard {
    value: AtomicU64,
}

impl CacheCounter {
    fn new() -> Self {
        let shards = (0..CACHE_COUNTER_SHARD_COUNT)
            .map(|_| CacheCounterShard {
                value: AtomicU64::new(0),
            })
            .collect();
        Self { shards }
    }

    fn increment(&self) {
        self.shards[cache_counter_shard_index()]
            .value
            .fetch_add(1, Ordering::Relaxed);
    }

    fn load(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.value.load(Ordering::Acquire))
            .fold(0_u64, u64::saturating_add)
    }
}

fn cache_counter_shard_index() -> usize {
    CACHE_COUNTER_SHARD_INDEX.with(|index| *index)
}

fn cache_value_kind_mismatch(key: BlockCacheKey) -> Error {
    Error::Corruption {
        message: format!("block cache key {key:?} reused for a different value kind"),
    }
}

fn estimate_index_partition_bytes(partition: &[TableDataBlock]) -> u64 {
    partition
        .iter()
        .map(TableDataBlock::estimated_bytes)
        .fold(1_u64, u64::saturating_add)
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
    entries: HashMap<BlockCacheKey, BlockCacheEntry>,
    high_order: VecDeque<BlockCacheKey>,
    low_order: VecDeque<BlockCacheKey>,
    high_bytes: u64,
    low_bytes: u64,
}

#[derive(Debug, Clone)]
enum CacheValue {
    DataBlock(Arc<DecodedDataBlock>),
    IndexPartition(Arc<Vec<TableDataBlock>>),
}

#[derive(Debug)]
struct BlockCacheEntry {
    value: CacheValue,
    size: u64,
    priority: CachePriority,
}

impl BlockCacheState {
    fn insert(&mut self, key: BlockCacheKey, size: u64, value: CacheValue) {
        if self
            .entries
            .insert(
                key,
                BlockCacheEntry {
                    value,
                    size,
                    priority: key.kind.priority(),
                },
            )
            .is_none()
        {
            self.push_order(key);
            self.add_bytes(key.kind.priority(), size);
        }
    }

    fn promote(&mut self, key: BlockCacheKey) {
        match key.kind.priority() {
            CachePriority::High => promote_order(&mut self.high_order, key),
            CachePriority::Low => promote_order(&mut self.low_order, key),
        }
    }

    fn evict_to(&mut self, capacity_bytes: u64) {
        while self.total_bytes() > capacity_bytes {
            // Metadata entries are cheap and prevent extra table I/O, so data
            // churn gives up low-priority entries before touching metadata.
            let Some(key) = self
                .low_order
                .pop_front()
                .or_else(|| self.high_order.pop_front())
            else {
                self.entries.clear();
                self.high_order.clear();
                self.low_order.clear();
                self.high_bytes = 0;
                self.low_bytes = 0;
                return;
            };
            if let Some(entry) = self.entries.remove(&key) {
                self.subtract_bytes(entry.priority, entry.size);
            }
        }
    }

    fn push_order(&mut self, key: BlockCacheKey) {
        self.push_order_for(key.kind.priority(), key);
    }

    fn push_order_for(&mut self, priority: CachePriority, key: BlockCacheKey) {
        match priority {
            CachePriority::High => self.high_order.push_back(key),
            CachePriority::Low => self.low_order.push_back(key),
        }
    }

    fn add_bytes(&mut self, priority: CachePriority, size: u64) {
        match priority {
            CachePriority::High => self.high_bytes = self.high_bytes.saturating_add(size),
            CachePriority::Low => self.low_bytes = self.low_bytes.saturating_add(size),
        }
    }

    fn subtract_bytes(&mut self, priority: CachePriority, size: u64) {
        match priority {
            CachePriority::High => self.high_bytes = self.high_bytes.saturating_sub(size),
            CachePriority::Low => self.low_bytes = self.low_bytes.saturating_sub(size),
        }
    }

    const fn total_bytes(&self) -> u64 {
        self.high_bytes.saturating_add(self.low_bytes)
    }
}

fn promote_order(order: &mut VecDeque<BlockCacheKey>, key: BlockCacheKey) {
    if order.back().is_some_and(|candidate| *candidate == key) {
        return;
    }
    let Some(position) = order.iter().position(|candidate| *candidate == key) else {
        return;
    };
    order.remove(position);
    order.push_back(key);
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        BLOCK_CACHE_SHARD_COUNT, BlockCache, BlockCacheKey, CacheKind, block_cache_shard_index,
        promote_order,
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
            .get_or_insert_data_block_with(keys[0], || Ok(load_counted_block(&loads)))
            .expect("first block loads");
        cache
            .get_or_insert_data_block_with(keys[1], || Ok(load_counted_block(&loads)))
            .expect("second block loads");
        cache
            .get_or_insert_data_block_with(keys[0], || Ok(load_counted_block(&loads)))
            .expect("first block hits and promotes");
        cache
            .get_or_insert_data_block_with(keys[2], || Ok(load_counted_block(&loads)))
            .expect("third block loads and evicts one entry");
        let loads_after_eviction = loads.load(Ordering::Acquire);

        cache
            .get_or_insert_data_block_with(keys[0], || Ok(load_counted_block(&loads)))
            .expect("promoted first block stays cached");
        assert_eq!(loads.load(Ordering::Acquire), loads_after_eviction);

        cache
            .get_or_insert_data_block_with(keys[1], || Ok(load_counted_block(&loads)))
            .expect("least recently used second block reloads");
        assert_eq!(loads.load(Ordering::Acquire), loads_after_eviction + 1);
    }

    #[test]
    fn high_priority_entries_survive_low_priority_churn() {
        let target_shard = 0;
        let high_key = key_in_shard(CacheKind::IndexBlock, target_shard, 1);
        let low_a = key_in_shard(
            CacheKind::DataBlock,
            target_shard,
            high_key.table_id.get() + 1,
        );
        let low_b = key_in_shard(CacheKind::DataBlock, target_shard, low_a.table_id.get() + 1);
        let cache = BlockCache::new(BLOCK_CACHE_SHARD_COUNT * 2);
        let high_loads = AtomicUsize::new(0);
        let low_loads = AtomicUsize::new(0);

        cache
            .get_or_insert_data_block_with(high_key, || Ok(load_counted_block(&high_loads)))
            .expect("high-priority block loads");
        cache
            .get_or_insert_data_block_with(low_a, || Ok(load_counted_block(&low_loads)))
            .expect("first low-priority block loads");
        cache
            .get_or_insert_data_block_with(low_b, || Ok(load_counted_block(&low_loads)))
            .expect("second low-priority block loads and evicts low-priority first");

        cache
            .get_or_insert_data_block_with(high_key, || Ok(load_counted_block(&high_loads)))
            .expect("high-priority block hits");
        assert_eq!(high_loads.load(Ordering::Acquire), 1);

        cache
            .get_or_insert_data_block_with(low_a, || Ok(load_counted_block(&low_loads)))
            .expect("low-priority block reloads");
        assert_eq!(low_loads.load(Ordering::Acquire), 3);
    }

    #[test]
    fn promoting_newest_entry_keeps_recency_order() {
        let keys = keys_in_same_shard(3);
        let mut order = VecDeque::from([keys[0], keys[1], keys[2]]);

        promote_order(&mut order, keys[2]);

        assert_eq!(order, VecDeque::from([keys[0], keys[1], keys[2]]));
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

    fn key_in_shard(kind: CacheKind, shard: usize, start_table_id: u64) -> BlockCacheKey {
        let mut table_id = start_table_id.max(1);
        loop {
            let key = BlockCacheKey::with_kind(kind, TableId(table_id), 0);
            if block_cache_shard_index(key) == shard {
                return key;
            }
            table_id += 1;
        }
    }
}
