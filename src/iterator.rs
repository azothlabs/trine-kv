use std::{cmp::Ordering as CmpOrdering, ops::Bound, path::PathBuf, sync::Arc};

use crate::{
    blob::ValueRef,
    error::{Error, Result},
    internal_key::{
        InternalKey, ValueKind, first_internal_key_for_user, last_internal_key_for_user,
    },
    memtable::Memtable,
    snapshot::Snapshot,
    table::TablePointCursor,
    types::{KeyRange, KeyValue, Sequence},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Direction {
    #[default]
    Forward,
    Reverse,
}

#[derive(Debug, Clone)]
pub struct Iter {
    direction: Direction,
    inner: IterInner,
}

#[derive(Debug, Clone)]
enum IterInner {
    Items(std::vec::IntoIter<KeyValue>),
    Lazy(LazyScan),
}

impl Iter {
    #[must_use]
    pub fn empty(direction: Direction) -> Self {
        Self::from_items(Vec::new(), direction)
    }

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

    pub(crate) fn from_sources(
        direction: Direction,
        read_sequence: Sequence,
        read_pin: Snapshot,
        db_path: Option<PathBuf>,
        range_tombstones: Vec<ScanRangeTombstone>,
        sources: Vec<RecordSource>,
    ) -> Self {
        Self {
            direction,
            inner: IterInner::Lazy(LazyScan {
                direction,
                read_sequence,
                _read_pin: read_pin,
                db_path,
                range_tombstones,
                sources,
            }),
        }
    }

    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
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

#[derive(Debug, Clone)]
struct LazyScan {
    direction: Direction,
    read_sequence: Sequence,
    _read_pin: Snapshot,
    db_path: Option<PathBuf>,
    range_tombstones: Vec<ScanRangeTombstone>,
    sources: Vec<RecordSource>,
}

impl LazyScan {
    fn next(&mut self) -> Option<Result<KeyValue>> {
        loop {
            let user_key = match self.next_user_key() {
                Ok(Some(user_key)) => user_key,
                Ok(None) => return None,
                Err(error) => return Some(Err(error)),
            };
            let mut first_record = None;
            let mut rest_records = Vec::new();

            for source in &mut self.sources {
                let source_matches = match source.current_key() {
                    Ok(Some(source_key)) => source_key == user_key.as_slice(),
                    Ok(None) => false,
                    Err(error) => return Some(Err(error)),
                };
                if source_matches {
                    match source.take_current_group() {
                        Ok(Some(group)) => {
                            push_group_records(&mut first_record, &mut rest_records, group);
                        }
                        Ok(None) => {}
                        Err(error) => return Some(Err(error)),
                    }
                }
            }

            let first_record =
                first_record.expect("selected user key must have at least one source record");
            match self.visible_item_from_records(first_record, rest_records) {
                Ok(Some(item)) => return Some(Ok(item)),
                Ok(None) => {}
                Err(error) => return Some(Err(error)),
            }
        }
    }

    fn next_user_key(&mut self) -> Result<Option<Vec<u8>>> {
        let mut selected: Option<Vec<u8>> = None;

        for source in &mut self.sources {
            let Some(user_key) = source.current_key()? else {
                continue;
            };
            let replace = selected.as_ref().is_none_or(|selected| {
                compare_scan_keys(user_key, selected, self.direction) == CmpOrdering::Less
            });
            if replace {
                selected = Some(user_key.to_vec());
            }
        }

        Ok(selected)
    }

    fn visible_item_from_records(
        &self,
        first_record: ScanRecord,
        mut rest_records: Vec<ScanRecord>,
    ) -> Result<Option<KeyValue>> {
        if rest_records.is_empty() {
            return self.visible_item_from_sorted_records(std::iter::once(first_record));
        }

        rest_records.push(first_record);
        rest_records.sort_by(|left, right| left.0.cmp(&right.0));

        self.visible_item_from_sorted_records(rest_records)
    }

    fn visible_item_from_sorted_records(
        &self,
        records: impl IntoIterator<Item = ScanRecord>,
    ) -> Result<Option<KeyValue>> {
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
                        return Ok(None);
                    }

                    return Ok(Some(KeyValue::new(
                        internal_key.user_key().to_vec(),
                        value_bytes(value.as_ref(), self.db_path.as_deref())?,
                    )));
                }
                ValueKind::PointDelete => return Ok(None),
                ValueKind::RangeDelete => {}
            }
        }

        Ok(None)
    }
}

fn push_group_records(
    first_record: &mut Option<ScanRecord>,
    rest_records: &mut Vec<ScanRecord>,
    group: RecordGroup,
) {
    if first_record.is_none() && rest_records.is_empty() {
        *first_record = Some(group.first);
        rest_records.extend(group.rest);
        return;
    }

    if let Some(previous_first) = first_record.take() {
        rest_records.push(previous_first);
    }
    rest_records.push(group.first);
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

    fn ensure_current(&mut self) -> Result<()> {
        if self.current.is_none() {
            self.current = self.cursor.next_group()?;
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

fn range_tombstones_cover(
    range_tombstones: &[ScanRangeTombstone],
    key: &[u8],
    point_sequence: Sequence,
    point_batch_index: u32,
    read_sequence: Sequence,
) -> bool {
    range_tombstones.iter().any(|tombstone| {
        tombstone.covers_visible_point(key, point_sequence, point_batch_index, read_sequence)
    })
}

fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}

fn value_bytes(value: Option<&ValueRef>, db_path: Option<&std::path::Path>) -> Result<Vec<u8>> {
    let value = value.ok_or_else(|| Error::Corruption {
        message: "put record is missing value bytes".to_owned(),
    })?;

    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::Blob { .. } => {
            let db_path = db_path.ok_or_else(|| Error::Corruption {
                message: "in-memory database cannot read blob value references".to_owned(),
            })?;
            crate::blob::read_value(db_path, value)
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
