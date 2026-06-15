use std::{
    cmp::Ordering as CmpOrdering, collections::BinaryHeap, ops::Bound, path::PathBuf, sync::Arc,
};

use crate::{
    blob::ValueRef,
    error::{Error, Result},
    internal_key::{
        InternalKey, ValueKind, first_internal_key_for_user, last_internal_key_for_user,
    },
    memtable::Memtable,
    range_tombstone::{RangeTombstoneIndex, RangeTombstoneLike},
    snapshot::Snapshot,
    stats::{BlobReadMetrics, ScanGroupOutcome, ScanWasteMetrics},
    storage::NativeFileBackend,
    table::TablePointCursor,
    types::{KeyRange, KeyValue, Sequence, Value},
};

/// Scan direction for range and prefix iterators.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Direction {
    /// Visit keys in ascending byte order.
    #[default]
    Forward,
    /// Visit keys in descending byte order.
    Reverse,
}

/// Eager iterator that returns owned key/value rows.
#[derive(Debug, Clone)]
pub struct Iter {
    direction: Direction,
    inner: IterInner,
}

/// Iterator that can defer reading large blob values until requested.
#[derive(Debug, Clone)]
pub struct LazyIter {
    direction: Direction,
    scan: LazyScan,
}

/// Row returned by `LazyIter`.
#[derive(Debug, Clone)]
pub struct LazyKeyValue {
    /// User key bytes.
    pub key: Vec<u8>,
    /// Value handle that may defer reading blob bytes.
    pub value: LazyValue,
}

/// Value returned by `LazyIter`.
#[derive(Debug, Clone)]
pub struct LazyValue {
    inner: LazyValueInner,
}

#[derive(Debug, Clone)]
enum LazyValueInner {
    Inline(Vec<u8>),
    Blob {
        db_path: PathBuf,
        native_storage: Option<NativeFileBackend>,
        internal_key: InternalKey,
        value: ValueRef,
        blob_reads: Option<Arc<BlobReadMetrics>>,
        _read_pin: Arc<Snapshot>,
    },
}

#[derive(Debug, Clone)]
enum IterInner {
    Items(std::vec::IntoIter<KeyValue>),
    Lazy(LazyScan),
}

#[derive(Debug, Clone)]
pub(crate) struct ScanSourceInput {
    pub(crate) read_sequence: Sequence,
    pub(crate) read_pin: Snapshot,
    pub(crate) db_path: Option<PathBuf>,
    pub(crate) native_storage: Option<NativeFileBackend>,
    pub(crate) blob_reads: Option<Arc<BlobReadMetrics>>,
    pub(crate) scan_waste: Option<Arc<ScanWasteMetrics>>,
    pub(crate) range_tombstones: Vec<ScanRangeTombstone>,
    pub(crate) sources: Vec<RecordSource>,
}

impl Iter {
    /// Creates an empty iterator with the requested direction.
    #[must_use]
    pub fn empty(direction: Direction) -> Self {
        Self::from_items(Vec::new(), direction)
    }

    /// Creates an iterator from already-owned rows.
    #[must_use]
    pub fn from_items(mut items: Vec<KeyValue>, direction: Direction) -> Self {
        if direction == Direction::Reverse {
            items.reverse();
        }

        Self {
            direction,
            inner: IterInner::Items(items.into_iter()),
        }
    }

    pub(crate) fn from_sources(direction: Direction, input: ScanSourceInput) -> Self {
        Self {
            direction,
            inner: IterInner::Lazy(LazyScan {
                direction,
                read_sequence: input.read_sequence,
                read_pin: Arc::new(input.read_pin),
                db_path: input.db_path,
                native_storage: input.native_storage,
                blob_reads: input.blob_reads,
                scan_waste: input.scan_waste,
                range_tombstones: RangeTombstoneIndex::new(input.range_tombstones),
                sources: input.sources,
                source_heap: BinaryHeap::new(),
                source_heap_initialized: false,
            }),
        }
    }

    /// Returns this iterator's scan direction.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }
}

impl LazyIter {
    pub(crate) fn from_sources(direction: Direction, input: ScanSourceInput) -> Self {
        Self {
            direction,
            scan: LazyScan {
                direction,
                read_sequence: input.read_sequence,
                read_pin: Arc::new(input.read_pin),
                db_path: input.db_path,
                native_storage: input.native_storage,
                blob_reads: input.blob_reads,
                scan_waste: input.scan_waste,
                range_tombstones: RangeTombstoneIndex::new(input.range_tombstones),
                sources: input.sources,
                source_heap: BinaryHeap::new(),
                source_heap_initialized: false,
            },
        }
    }

    /// Returns this iterator's scan direction.
    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }
}

impl LazyKeyValue {
    /// Reads any deferred value bytes synchronously and returns an owned row.
    pub fn into_key_value_sync(self) -> Result<KeyValue> {
        Ok(KeyValue::new(self.key, self.value.into_value_sync()?))
    }

    /// Reads any deferred value bytes asynchronously and returns an owned row.
    pub async fn into_key_value(self) -> Result<KeyValue> {
        let value = self.value.into_value().await?;
        Ok(KeyValue::new(self.key, value))
    }
}

impl LazyValue {
    /// Returns `true` if the value bytes are already inline.
    #[must_use]
    pub fn is_inline(&self) -> bool {
        matches!(self.inner, LazyValueInner::Inline(_))
    }

    /// Reads the value bytes synchronously without consuming this handle.
    pub fn read_sync(&self) -> Result<Value> {
        match &self.inner {
            LazyValueInner::Inline(bytes) => Ok(bytes.clone()),
            LazyValueInner::Blob {
                db_path,
                native_storage: _,
                internal_key,
                value,
                blob_reads,
                _read_pin: _,
            } => {
                let bytes =
                    crate::blob::read_value_for_internal_key(db_path, value, Some(internal_key))?;
                if let Some(blob_reads) = blob_reads {
                    blob_reads.record(bytes.len() as u64);
                }
                Ok(bytes)
            }
        }
    }

    /// Reads the value bytes synchronously and consumes this handle.
    pub fn into_value_sync(self) -> Result<Value> {
        match self.inner {
            LazyValueInner::Inline(bytes) => Ok(bytes),
            LazyValueInner::Blob {
                db_path,
                native_storage: _,
                internal_key,
                value,
                blob_reads,
                _read_pin: _,
            } => {
                let bytes = crate::blob::read_value_for_internal_key(
                    &db_path,
                    &value,
                    Some(&internal_key),
                )?;
                if let Some(blob_reads) = blob_reads {
                    blob_reads.record(bytes.len() as u64);
                }
                Ok(bytes)
            }
        }
    }

    /// Reads the value bytes asynchronously without consuming this handle.
    pub async fn read(&self) -> Result<Value> {
        match &self.inner {
            LazyValueInner::Inline(bytes) => Ok(bytes.clone()),
            LazyValueInner::Blob {
                db_path,
                native_storage: Some(native_storage),
                internal_key,
                value,
                blob_reads,
                _read_pin: _,
            } => {
                let bytes = crate::blob::read_value_for_internal_key_with_backend_async(
                    native_storage,
                    db_path,
                    value,
                    Some(internal_key),
                )
                .await?;
                if let Some(blob_reads) = blob_reads {
                    blob_reads.record(bytes.len() as u64);
                }
                Ok(bytes)
            }
            LazyValueInner::Blob {
                native_storage: None,
                ..
            } => self.read_sync(),
        }
    }

    /// Reads the value bytes asynchronously and consumes this handle.
    pub async fn into_value(self) -> Result<Value> {
        match self.inner {
            LazyValueInner::Inline(bytes) => Ok(bytes),
            LazyValueInner::Blob {
                db_path,
                native_storage: Some(native_storage),
                internal_key,
                value,
                blob_reads,
                _read_pin: _,
            } => {
                let bytes = crate::blob::read_value_for_internal_key_with_backend_async(
                    &native_storage,
                    &db_path,
                    &value,
                    Some(&internal_key),
                )
                .await?;
                if let Some(blob_reads) = blob_reads {
                    blob_reads.record(bytes.len() as u64);
                }
                Ok(bytes)
            }
            LazyValueInner::Blob {
                db_path,
                native_storage: None,
                internal_key,
                value,
                blob_reads,
                _read_pin: _,
            } => {
                let bytes = crate::blob::read_value_for_internal_key(
                    &db_path,
                    &value,
                    Some(&internal_key),
                )?;
                if let Some(blob_reads) = blob_reads {
                    blob_reads.record(bytes.len() as u64);
                }
                Ok(bytes)
            }
        }
    }
}

impl Iter {
    /// Returns the next owned row, reading deferred sources asynchronously when needed.
    pub async fn next(&mut self) -> Result<Option<KeyValue>> {
        match &mut self.inner {
            IterInner::Items(items) => Ok(items.next()),
            IterInner::Lazy(scan) => scan.next_async().await,
        }
    }

    /// Returns the next owned row using the synchronous iterator path.
    pub fn next_sync(&mut self) -> Option<Result<KeyValue>> {
        Iterator::next(self)
    }
}

impl LazyIter {
    /// Returns the next lazy row, reading deferred metadata asynchronously when needed.
    pub async fn next(&mut self) -> Result<Option<LazyKeyValue>> {
        self.scan.next_lazy_async().await
    }

    /// Returns the next lazy row using the synchronous iterator path.
    pub fn next_sync(&mut self) -> Option<Result<LazyKeyValue>> {
        Iterator::next(self)
    }
}

impl Iterator for Iter {
    type Item = Result<KeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            IterInner::Items(items) => items.next().map(Ok),
            IterInner::Lazy(scan) => scan.next(),
        }
    }
}

impl Iterator for LazyIter {
    type Item = Result<LazyKeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next_lazy()
    }
}

#[derive(Debug, Clone)]
struct LazyScan {
    direction: Direction,
    read_sequence: Sequence,
    read_pin: Arc<Snapshot>,
    db_path: Option<PathBuf>,
    native_storage: Option<NativeFileBackend>,
    blob_reads: Option<Arc<BlobReadMetrics>>,
    scan_waste: Option<Arc<ScanWasteMetrics>>,
    range_tombstones: RangeTombstoneIndex<ScanRangeTombstone>,
    sources: Vec<RecordSource>,
    source_heap: BinaryHeap<SourceHeapEntry>,
    source_heap_initialized: bool,
}

impl LazyScan {
    fn next(&mut self) -> Option<Result<KeyValue>> {
        self.next_lazy()
            .map(|item| item.and_then(LazyKeyValue::into_key_value_sync))
    }

    async fn next_async(&mut self) -> Result<Option<KeyValue>> {
        let Some(item) = self.next_lazy_async().await? else {
            return Ok(None);
        };
        item.into_key_value().await.map(Some)
    }

    fn next_lazy(&mut self) -> Option<Result<LazyKeyValue>> {
        if !self.source_heap_initialized {
            if let Err(error) = self.initialize_source_heap() {
                return Some(Err(error));
            }
        }

        loop {
            let entry = self.source_heap.pop()?;
            let user_key = entry.user_key;
            let mut source_indices = vec![entry.source_index];
            while self
                .source_heap
                .peek()
                .is_some_and(|entry| entry.user_key == user_key)
            {
                let entry = self
                    .source_heap
                    .pop()
                    .expect("heap peek promised another source entry");
                source_indices.push(entry.source_index);
            }

            let mut first_record = None;
            let mut rest_records = Vec::new();

            for source_index in source_indices {
                match self.sources[source_index].take_current_group() {
                    Ok(Some(group)) => {
                        push_group_records(&mut first_record, &mut rest_records, group);
                    }
                    Ok(None) => {}
                    Err(error) => return Some(Err(error)),
                }
                if let Err(error) = self.push_source_heap_entry(source_index) {
                    return Some(Err(error));
                }
            }

            let Some(first_record) = first_record else {
                continue;
            };
            match self.visible_lazy_item_from_records(first_record, rest_records) {
                Ok(Some(item)) => return Some(Ok(item)),
                Ok(None) => {}
                Err(error) => return Some(Err(error)),
            }
        }
    }

    async fn next_lazy_async(&mut self) -> Result<Option<LazyKeyValue>> {
        if !self.source_heap_initialized {
            self.initialize_source_heap_async().await?;
        }

        loop {
            let Some(entry) = self.source_heap.pop() else {
                return Ok(None);
            };
            let user_key = entry.user_key;
            let mut source_indices = vec![entry.source_index];
            while self
                .source_heap
                .peek()
                .is_some_and(|entry| entry.user_key == user_key)
            {
                let entry = self
                    .source_heap
                    .pop()
                    .expect("heap peek promised another source entry");
                source_indices.push(entry.source_index);
            }

            let mut first_record = None;
            let mut rest_records = Vec::new();

            for source_index in source_indices {
                if let Some(group) = self.sources[source_index]
                    .take_current_group_async()
                    .await?
                {
                    push_group_records(&mut first_record, &mut rest_records, group);
                }
                self.push_source_heap_entry_async(source_index).await?;
            }

            let Some(first_record) = first_record else {
                continue;
            };
            if let Some(item) = self.visible_lazy_item_from_records(first_record, rest_records)? {
                return Ok(Some(item));
            }
        }
    }

    fn initialize_source_heap(&mut self) -> Result<()> {
        for source_index in 0..self.sources.len() {
            self.push_source_heap_entry(source_index)?;
        }
        self.source_heap_initialized = true;
        Ok(())
    }

    async fn initialize_source_heap_async(&mut self) -> Result<()> {
        for source_index in 0..self.sources.len() {
            self.push_source_heap_entry_async(source_index).await?;
        }
        self.source_heap_initialized = true;
        Ok(())
    }

    fn push_source_heap_entry(&mut self, source_index: usize) -> Result<()> {
        let Some(user_key) = self.sources[source_index]
            .current_key()?
            .map(<[u8]>::to_vec)
        else {
            return Ok(());
        };
        self.source_heap.push(SourceHeapEntry {
            user_key,
            source_index,
            direction: self.direction,
        });
        Ok(())
    }

    async fn push_source_heap_entry_async(&mut self, source_index: usize) -> Result<()> {
        let Some(user_key) = self.sources[source_index].current_user_key_async().await? else {
            return Ok(());
        };
        self.source_heap.push(SourceHeapEntry {
            user_key,
            source_index,
            direction: self.direction,
        });
        Ok(())
    }

    fn visible_lazy_item_from_records(
        &self,
        first_record: ScanRecord,
        mut rest_records: Vec<ScanRecord>,
    ) -> Result<Option<LazyKeyValue>> {
        // The merge handled this many version records for one user key; the ratio
        // of these to returned user keys is the scan read-amplification signal.
        let group_records = 1 + rest_records.len();
        let visibility = if rest_records.is_empty() {
            self.visible_lazy_item_from_sorted_records(std::iter::once(first_record))?
        } else {
            rest_records.push(first_record);
            rest_records.sort_by(|left, right| left.0.cmp(&right.0));
            self.visible_lazy_item_from_sorted_records(rest_records)?
        };

        if let Some(scan_waste) = &self.scan_waste {
            scan_waste.record_group(group_records as u64, visibility.outcome());
        }
        Ok(visibility.into_item())
    }

    fn visible_lazy_item_from_sorted_records(
        &self,
        records: impl IntoIterator<Item = ScanRecord>,
    ) -> Result<LazyVisibility> {
        for (internal_key, value) in records {
            if internal_key.sequence() > self.read_sequence {
                continue;
            }

            match internal_key.kind() {
                ValueKind::Put => {
                    if range_tombstones_cover(
                        &self.range_tombstones,
                        internal_key.user_key(),
                        internal_key.sequence(),
                        internal_key.batch_index(),
                        self.read_sequence,
                    ) {
                        return Ok(LazyVisibility::HiddenByDelete);
                    }

                    let key = internal_key.user_key().to_vec();
                    let value = lazy_value(
                        value,
                        internal_key,
                        self.db_path.as_deref(),
                        self.native_storage.clone(),
                        self.blob_reads.clone(),
                        Arc::clone(&self.read_pin),
                    )?;
                    return Ok(LazyVisibility::Visible(LazyKeyValue { key, value }));
                }
                ValueKind::PointDelete => return Ok(LazyVisibility::HiddenByDelete),
                ValueKind::RangeDelete => {}
            }
        }

        Ok(LazyVisibility::NoVisibleVersion)
    }
}

/// Outcome of resolving one user-key version group during a lazy scan.
enum LazyVisibility {
    Visible(LazyKeyValue),
    HiddenByDelete,
    NoVisibleVersion,
}

impl LazyVisibility {
    fn outcome(&self) -> ScanGroupOutcome {
        match self {
            Self::Visible(_) => ScanGroupOutcome::Visible,
            Self::HiddenByDelete => ScanGroupOutcome::HiddenByDelete,
            Self::NoVisibleVersion => ScanGroupOutcome::NoVisibleVersion,
        }
    }

    fn into_item(self) -> Option<LazyKeyValue> {
        match self {
            Self::Visible(item) => Some(item),
            Self::HiddenByDelete | Self::NoVisibleVersion => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceHeapEntry {
    user_key: Vec<u8>,
    source_index: usize,
    direction: Direction,
}

impl Ord for SourceHeapEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        debug_assert_eq!(self.direction, other.direction);
        match compare_scan_keys(&self.user_key, &other.user_key, self.direction) {
            CmpOrdering::Less => CmpOrdering::Greater,
            CmpOrdering::Equal => other.source_index.cmp(&self.source_index),
            CmpOrdering::Greater => CmpOrdering::Less,
        }
    }
}

impl PartialOrd for SourceHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn push_group_records(
    first_record: &mut Option<ScanRecord>,
    rest_records: &mut Vec<ScanRecord>,
    group: RecordGroup,
) {
    if let Some(previous_first) = first_record.take() {
        rest_records.push(previous_first);
    }
    *first_record = Some(group.first);
    rest_records.extend(group.rest);
}

fn compare_scan_keys(left: &[u8], right: &[u8], direction: Direction) -> CmpOrdering {
    match direction {
        Direction::Forward => left.cmp(right),
        Direction::Reverse => right.cmp(left),
    }
}

pub(crate) type ScanRecord = (InternalKey, Option<ValueRef>);

#[derive(Debug, Clone)]
pub(crate) struct RecordGroup {
    pub(crate) user_key: Vec<u8>,
    pub(crate) first: ScanRecord,
    pub(crate) rest: Vec<ScanRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecordSource {
    cursor: SourceCursor,
    current: Option<RecordGroup>,
}

impl RecordSource {
    pub(crate) fn memtable(
        memtable: Arc<Memtable>,
        selector: ScanSelector,
        direction: Direction,
    ) -> Self {
        Self {
            cursor: SourceCursor::Memtable(MemtableCursor::new(memtable, selector, direction)),
            current: None,
        }
    }

    pub(crate) fn table(cursor: TablePointCursor) -> Self {
        Self {
            cursor: SourceCursor::Table(cursor),
            current: None,
        }
    }

    fn current_key(&mut self) -> Result<Option<&[u8]>> {
        self.ensure_current()?;
        Ok(self.current.as_ref().map(|group| group.user_key.as_slice()))
    }

    fn take_current_group(&mut self) -> Result<Option<RecordGroup>> {
        self.ensure_current()?;
        Ok(self.current.take())
    }

    async fn current_user_key_async(&mut self) -> Result<Option<Vec<u8>>> {
        self.ensure_current_async().await?;
        Ok(self.current.as_ref().map(|group| group.user_key.clone()))
    }

    async fn take_current_group_async(&mut self) -> Result<Option<RecordGroup>> {
        self.ensure_current_async().await?;
        Ok(self.current.take())
    }

    fn ensure_current(&mut self) -> Result<()> {
        if self.current.is_none() {
            self.current = self.cursor.next_group()?;
        }
        Ok(())
    }

    async fn ensure_current_async(&mut self) -> Result<()> {
        if self.current.is_none() {
            self.current = self.cursor.next_group_async().await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum SourceCursor {
    Memtable(MemtableCursor),
    Table(TablePointCursor),
}

impl SourceCursor {
    fn next_group(&mut self) -> Result<Option<RecordGroup>> {
        match self {
            Self::Memtable(cursor) => cursor.next_group(),
            Self::Table(cursor) => cursor.next_group(),
        }
    }

    async fn next_group_async(&mut self) -> Result<Option<RecordGroup>> {
        match self {
            Self::Memtable(cursor) => cursor.next_group_async().await,
            Self::Table(cursor) => cursor.next_group_async().await,
        }
    }
}

#[derive(Debug, Clone)]
struct MemtableCursor {
    // The cursor keeps the memtable handle that was active when the scan was
    // created. A later flush can swap in a fresh active memtable without
    // changing what this iterator is allowed to see.
    memtable: Arc<Memtable>,
    selector: ScanSelector,
    direction: Direction,
    lower_bound: Bound<InternalKey>,
    upper_bound: Bound<InternalKey>,
    exhausted: bool,
}

impl MemtableCursor {
    fn new(memtable: Arc<Memtable>, selector: ScanSelector, direction: Direction) -> Self {
        let (lower_bound, upper_bound) = memtable_scan_bounds(&selector);

        Self {
            memtable,
            selector,
            direction,
            lower_bound,
            upper_bound,
            exhausted: false,
        }
    }

    fn next_group(&mut self) -> Result<Option<RecordGroup>> {
        match self.direction {
            Direction::Forward => self.next_group_forward(),
            Direction::Reverse => self.next_group_reverse(),
        }
    }

    // Memtable advancement has no I/O, but it participates in the async scan
    // chain so mixed memtable/table sources share one awaitable shape.
    #[allow(clippy::unused_async)]
    async fn next_group_async(&mut self) -> Result<Option<RecordGroup>> {
        self.next_group()
    }

    fn next_group_forward(&mut self) -> Result<Option<RecordGroup>> {
        if self.exhausted {
            return Ok(None);
        }

        let entries = self
            .memtable
            .read_entries()
            .map_err(|_| lock_poisoned("memtable entries"))?;
        let mut records = Vec::new();
        let mut group_user_key = None;

        for (internal_key, value) in
            entries.range((self.lower_bound.clone(), self.upper_bound.clone()))
        {
            match self.selector.forward_key_state(internal_key.user_key()) {
                ForwardKeyState::Before => {}
                ForwardKeyState::Match => {
                    let user_key =
                        group_user_key.get_or_insert_with(|| internal_key.user_key().to_vec());
                    if internal_key.user_key() == user_key.as_slice() {
                        records.push((internal_key.clone(), value.clone()));
                    } else {
                        break;
                    }
                }
                ForwardKeyState::After => {
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }
        drop(entries);

        let Some(user_key) = group_user_key else {
            self.exhausted = true;
            return Ok(None);
        };
        self.lower_bound = Bound::Excluded(last_internal_key_for_user(&user_key));
        Ok(Some(record_group_from_records(user_key, records)))
    }

    fn next_group_reverse(&mut self) -> Result<Option<RecordGroup>> {
        if self.exhausted {
            return Ok(None);
        }

        let entries = self
            .memtable
            .read_entries()
            .map_err(|_| lock_poisoned("memtable entries"))?;
        let mut records = Vec::new();
        let mut group_user_key = None;

        for (internal_key, value) in entries
            .range((self.lower_bound.clone(), self.upper_bound.clone()))
            .rev()
        {
            match self.selector.reverse_key_state(internal_key.user_key()) {
                ReverseKeyState::Above => {}
                ReverseKeyState::Match => {
                    let user_key =
                        group_user_key.get_or_insert_with(|| internal_key.user_key().to_vec());
                    if internal_key.user_key() == user_key.as_slice() {
                        records.push((internal_key.clone(), value.clone()));
                    } else {
                        break;
                    }
                }
                ReverseKeyState::Below => {
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }
        drop(entries);

        let Some(user_key) = group_user_key else {
            self.exhausted = true;
            return Ok(None);
        };
        self.upper_bound = Bound::Excluded(first_internal_key_for_user(&user_key));
        Ok(Some(record_group_from_records(user_key, records)))
    }
}

fn record_group_from_records(user_key: Vec<u8>, mut records: Vec<ScanRecord>) -> RecordGroup {
    let first = records
        .pop()
        .expect("memtable cursor only builds groups after finding a record");
    let (first, rest) = sort_group_records(first, records);
    RecordGroup {
        user_key,
        first,
        rest,
    }
}

pub(crate) fn sort_group_records(
    first: ScanRecord,
    mut rest: Vec<ScanRecord>,
) -> (ScanRecord, Vec<ScanRecord>) {
    if rest.is_empty() {
        return (first, rest);
    }

    rest.push(first);
    rest.sort_by(|left, right| left.0.cmp(&right.0));
    let mut records = rest.into_iter();
    let first = records
        .next()
        .expect("non-empty record group must keep a first record");
    let rest = records.collect();
    (first, rest)
}

fn memtable_scan_bounds(selector: &ScanSelector) -> (Bound<InternalKey>, Bound<InternalKey>) {
    match selector {
        ScanSelector::Range(range) => (
            memtable_start_bound(&range.start),
            memtable_end_bound(&range.end),
        ),
        ScanSelector::Prefix(prefix) => {
            let start = Bound::Included(first_internal_key_for_user(prefix));
            let end = prefix_successor(prefix).map_or(Bound::Unbounded, |end| {
                Bound::Excluded(first_internal_key_for_user(&end))
            });
            (start, end)
        }
    }
}

fn memtable_start_bound(start: &Bound<Vec<u8>>) -> Bound<InternalKey> {
    match start {
        Bound::Included(key) => Bound::Included(first_internal_key_for_user(key)),
        Bound::Excluded(key) => Bound::Excluded(last_internal_key_for_user(key)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn memtable_end_bound(end: &Bound<Vec<u8>>) -> Bound<InternalKey> {
    match end {
        Bound::Included(key) => Bound::Included(last_internal_key_for_user(key)),
        Bound::Excluded(key) => Bound::Excluded(first_internal_key_for_user(key)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanSelector {
    Range(KeyRange),
    Prefix(Vec<u8>),
}

impl ScanSelector {
    pub(crate) fn forward_key_state(&self, key: &[u8]) -> ForwardKeyState {
        match self {
            Self::Range(range) => {
                if key_is_before_start(key, &range.start) {
                    ForwardKeyState::Before
                } else if key_is_after_end(key, &range.end) {
                    ForwardKeyState::After
                } else {
                    ForwardKeyState::Match
                }
            }
            Self::Prefix(prefix) => {
                if key < prefix.as_slice() {
                    ForwardKeyState::Before
                } else if key.starts_with(prefix) {
                    ForwardKeyState::Match
                } else {
                    ForwardKeyState::After
                }
            }
        }
    }

    pub(crate) fn reverse_key_state(&self, key: &[u8]) -> ReverseKeyState {
        match self {
            Self::Range(range) => {
                if key_is_after_end(key, &range.end) {
                    ReverseKeyState::Above
                } else if key_is_before_start(key, &range.start) {
                    ReverseKeyState::Below
                } else {
                    ReverseKeyState::Match
                }
            }
            Self::Prefix(prefix) => {
                if key.starts_with(prefix) {
                    ReverseKeyState::Match
                } else if key < prefix.as_slice() {
                    ReverseKeyState::Below
                } else {
                    ReverseKeyState::Above
                }
            }
        }
    }

    pub(crate) fn prefix(&self) -> Option<&[u8]> {
        match self {
            Self::Range(_) => None,
            Self::Prefix(prefix) => Some(prefix),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForwardKeyState {
    Before,
    Match,
    After,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReverseKeyState {
    Above,
    Match,
    Below,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanRangeTombstone {
    range: KeyRange,
    sequence: Sequence,
    batch_index: u32,
}

impl ScanRangeTombstone {
    #[must_use]
    pub(crate) fn new(range: KeyRange, sequence: Sequence, batch_index: u32) -> Self {
        Self {
            range,
            sequence,
            batch_index,
        }
    }

    fn covers_visible_point(
        &self,
        key: &[u8],
        point_sequence: Sequence,
        point_batch_index: u32,
        read_sequence: Sequence,
    ) -> bool {
        if self.sequence > read_sequence || !key_is_in_range(key, &self.range) {
            return false;
        }

        self.sequence > point_sequence
            || (self.sequence == point_sequence && self.batch_index > point_batch_index)
    }
}

impl RangeTombstoneLike for ScanRangeTombstone {
    fn range(&self) -> &KeyRange {
        &self.range
    }
}

fn range_tombstones_cover(
    range_tombstones: &RangeTombstoneIndex<ScanRangeTombstone>,
    key: &[u8],
    point_sequence: Sequence,
    point_batch_index: u32,
    read_sequence: Sequence,
) -> bool {
    range_tombstones.covering_key(key).any(|tombstone| {
        tombstone.covers_visible_point(key, point_sequence, point_batch_index, read_sequence)
    })
}

fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}

fn lazy_value(
    value: Option<ValueRef>,
    internal_key: InternalKey,
    db_path: Option<&std::path::Path>,
    native_storage: Option<NativeFileBackend>,
    blob_reads: Option<Arc<BlobReadMetrics>>,
    read_pin: Arc<Snapshot>,
) -> Result<LazyValue> {
    let value = value.ok_or_else(|| Error::Corruption {
        message: "put record is missing value bytes".to_owned(),
    })?;

    match value {
        ValueRef::Inline(bytes) => Ok(LazyValue {
            inner: LazyValueInner::Inline(bytes),
        }),
        ValueRef::BlobIndex(_) | ValueRef::Blob { .. } => {
            let db_path = db_path.ok_or_else(|| Error::Corruption {
                message: "in-memory database cannot read blob value references".to_owned(),
            })?;
            Ok(LazyValue {
                inner: LazyValueInner::Blob {
                    db_path: db_path.to_path_buf(),
                    native_storage,
                    internal_key,
                    value,
                    blob_reads,
                    _read_pin: read_pin,
                },
            })
        }
    }
}

pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last == u8::MAX {
            end.pop();
        } else {
            *last += 1;
            return Some(end);
        }
    }

    None
}

fn key_is_before_start(key: &[u8], start: &Bound<Vec<u8>>) -> bool {
    match start {
        Bound::Included(start) => key < start.as_slice(),
        Bound::Excluded(start) => key <= start.as_slice(),
        Bound::Unbounded => false,
    }
}

fn key_is_after_end(key: &[u8], end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Included(end) => key > end.as_slice(),
        Bound::Excluded(end) => key >= end.as_slice(),
        Bound::Unbounded => false,
    }
}

fn key_is_in_range(key: &[u8], range: &KeyRange) -> bool {
    !key_is_before_start(key, &range.start) && !key_is_after_end(key, &range.end)
}

#[cfg(test)]
mod tests {
    use std::{collections::BinaryHeap, sync::Arc};

    use super::{Direction, Iter, RecordSource, ScanSelector, ScanSourceInput, SourceHeapEntry};
    use crate::{
        blob::ValueRef,
        internal_key::{InternalKey, ValueKind},
        memtable::Memtable,
        snapshot::Snapshot,
        types::{KeyRange, Sequence},
    };

    #[test]
    fn source_heap_orders_forward_and_reverse_keys() {
        let mut forward = BinaryHeap::new();
        forward.push(heap_entry(b"c", 0, Direction::Forward));
        forward.push(heap_entry(b"a", 1, Direction::Forward));
        forward.push(heap_entry(b"b", 2, Direction::Forward));

        assert_eq!(forward.pop().expect("entry").user_key, b"a");
        assert_eq!(forward.pop().expect("entry").user_key, b"b");
        assert_eq!(forward.pop().expect("entry").user_key, b"c");

        let mut reverse = BinaryHeap::new();
        reverse.push(heap_entry(b"c", 0, Direction::Reverse));
        reverse.push(heap_entry(b"a", 1, Direction::Reverse));
        reverse.push(heap_entry(b"b", 2, Direction::Reverse));

        assert_eq!(reverse.pop().expect("entry").user_key, b"c");
        assert_eq!(reverse.pop().expect("entry").user_key, b"b");
        assert_eq!(reverse.pop().expect("entry").user_key, b"a");
    }

    #[test]
    fn lazy_scan_heap_merge_preserves_forward_and_reverse_order() {
        let left = memtable_with(&[(b"a", b"a1"), (b"c", b"c1")]);
        let right = memtable_with(&[(b"b", b"b1"), (b"d", b"d1")]);

        let forward = Iter::from_sources(
            Direction::Forward,
            ScanSourceInput {
                read_sequence: Sequence::new(4),
                read_pin: Snapshot::new(Sequence::new(4)),
                db_path: None,
                native_storage: None,
                blob_reads: None,
                scan_waste: None,
                range_tombstones: Vec::new(),
                sources: vec![
                    RecordSource::memtable(
                        Arc::clone(&left),
                        ScanSelector::Range(KeyRange::all()),
                        Direction::Forward,
                    ),
                    RecordSource::memtable(
                        Arc::clone(&right),
                        ScanSelector::Range(KeyRange::all()),
                        Direction::Forward,
                    ),
                ],
            },
        );
        assert_eq!(
            collect_keys(forward),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );

        let reverse = Iter::from_sources(
            Direction::Reverse,
            ScanSourceInput {
                read_sequence: Sequence::new(4),
                read_pin: Snapshot::new(Sequence::new(4)),
                db_path: None,
                native_storage: None,
                blob_reads: None,
                scan_waste: None,
                range_tombstones: Vec::new(),
                sources: vec![
                    RecordSource::memtable(
                        left,
                        ScanSelector::Range(KeyRange::all()),
                        Direction::Reverse,
                    ),
                    RecordSource::memtable(
                        right,
                        ScanSelector::Range(KeyRange::all()),
                        Direction::Reverse,
                    ),
                ],
            },
        );
        assert_eq!(
            collect_keys(reverse),
            vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
        );
    }

    fn heap_entry(user_key: &[u8], source_index: usize, direction: Direction) -> SourceHeapEntry {
        SourceHeapEntry {
            user_key: user_key.to_vec(),
            source_index,
            direction,
        }
    }

    fn memtable_with(records: &[(&[u8], &[u8])]) -> Arc<Memtable> {
        let memtable = Arc::new(Memtable::default());
        {
            let mut entries = memtable.write_entries().expect("memtable lock");
            for (index, (key, value)) in records.iter().enumerate() {
                entries.insert(
                    InternalKey::new(
                        *key,
                        Sequence::new(u64::try_from(index + 1).expect("test sequence fits")),
                        ValueKind::Put,
                        0,
                    ),
                    Some(ValueRef::Inline((*value).to_vec())),
                );
            }
        }
        memtable
    }

    fn collect_keys(iter: Iter) -> Vec<Vec<u8>> {
        iter.map(|item| item.expect("iterator item").key)
            .collect::<Vec<_>>()
    }
}
