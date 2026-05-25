use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::table::TableId;

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
    hits: AtomicU64,
    misses: AtomicU64,
    state: Mutex<BlockCacheState>,
}

impl BlockCache {
    pub(crate) fn new(capacity_bytes: usize) -> Self {
        let capacity_bytes = match u64::try_from(capacity_bytes) {
            Ok(value) => value,
            Err(_) => u64::MAX,
        };
        Self {
            capacity_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            state: Mutex::new(BlockCacheState::default()),
        }
    }

    pub(crate) fn record_access(&self, key: BlockCacheKey, estimated_bytes: u64) {
        if self.capacity_bytes == 0 {
            return;
        }

        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.entries.contains_key(&key) {
            self.hits.fetch_add(1, Ordering::AcqRel);
            return;
        }

        self.misses.fetch_add(1, Ordering::AcqRel);
        let estimated_bytes = estimated_bytes.max(1);
        if estimated_bytes > self.capacity_bytes {
            return;
        }
        state.insert(key, estimated_bytes);
        state.evict_to(self.capacity_bytes);
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Acquire),
            misses: self.misses.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Default)]
struct BlockCacheState {
    entries: BTreeMap<BlockCacheKey, u64>,
    order: VecDeque<BlockCacheKey>,
    bytes: u64,
}

impl BlockCacheState {
    fn insert(&mut self, key: BlockCacheKey, bytes: u64) {
        if self.entries.insert(key, bytes).is_none() {
            self.order.push_back(key);
            self.bytes = self.bytes.saturating_add(bytes);
        }
    }

    fn evict_to(&mut self, capacity_bytes: u64) {
        while self.bytes > capacity_bytes {
            let Some(key) = self.order.pop_front() else {
                self.entries.clear();
                self.bytes = 0;
                return;
            };
            if let Some(bytes) = self.entries.remove(&key) {
                self.bytes = self.bytes.saturating_sub(bytes);
            }
        }
    }
}
