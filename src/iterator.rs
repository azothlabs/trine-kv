use std::{cmp::Ordering as CmpOrdering, ops::Bound, path::PathBuf};

use crate::{
    blob::ValueRef,
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
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
            let user_key = self.next_user_key()?;
            let mut first_record = None;
            let mut rest_records = Vec::new();

            for source in &mut self.sources {
                let source_matches = source
                    .current_key()
                    .is_some_and(|source_key| source_key == user_key.as_slice());
                if source_matches {
                    let Some(group) = source.take_current_group() else {
                        continue;
                    };
                    push_group_records(&mut first_record, &mut rest_records, group);
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

    fn next_user_key(&mut self) -> Option<Vec<u8>> {
        let mut selected: Option<Vec<u8>> = None;

        for source in &mut self.sources {
            let Some(user_key) = source.current_key() else {
                continue;
            };
            let replace = selected.as_ref().is_none_or(|selected| {
                compare_scan_keys(user_key, selected, self.direction) == CmpOrdering::Less
            });
            if replace {
                selected = Some(user_key.to_vec());
            }
        }

        selected
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
        records: Vec<ScanRecord>,
        selector: ScanSelector,
        direction: Direction,
    ) -> Self {
        Self {
            cursor: SourceCursor::Memtable(MemtableCursor::new(records, selector, direction)),
            current: None,
        }
    }

    pub(crate) fn table(cursor: TablePointCursor) -> Self {
        Self {
            cursor: SourceCursor::Table(cursor),
            current: None,
        }
    }

    fn current_key(&mut self) -> Option<&[u8]> {
        self.ensure_current();
        self.current.as_ref().map(|group| group.user_key.as_slice())
    }

    fn take_current_group(&mut self) -> Option<RecordGroup> {
        self.ensure_current();
        self.current.take()
    }

    fn ensure_current(&mut self) {
        if self.current.is_none() {
            self.current = self.cursor.next_group();
        }
    }
}

#[derive(Debug, Clone)]
enum SourceCursor {
    Memtable(MemtableCursor),
    Table(TablePointCursor),
}

impl SourceCursor {
    fn next_group(&mut self) -> Option<RecordGroup> {
        match self {
            Self::Memtable(cursor) => cursor.next_group(),
            Self::Table(cursor) => cursor.next_group(),
        }
    }
}

#[derive(Debug, Clone)]
struct MemtableCursor {
    records: Vec<ScanRecord>,
    selector: ScanSelector,
    direction: Direction,
    index: usize,
    pending: Option<ScanRecord>,
}

impl MemtableCursor {
    fn new(mut records: Vec<ScanRecord>, selector: ScanSelector, direction: Direction) -> Self {
        records.sort_by(|left, right| left.0.cmp(&right.0));
        let index = match direction {
            Direction::Forward => forward_start_index(&records, &selector),
            Direction::Reverse => reverse_start_index(&records, &selector),
        };

        Self {
            records,
            selector,
            direction,
            index,
            pending: None,
        }
    }

    fn next_group(&mut self) -> Option<RecordGroup> {
        let first = self.pending.take().or_else(|| self.next_record())?;
        let user_key = first.0.user_key().to_vec();
        let mut rest = Vec::new();

        while let Some(record) = self.next_record() {
            if record.0.user_key() == user_key.as_slice() {
                rest.push(record);
            } else {
                self.pending = Some(record);
                break;
            }
        }
        let (first, rest) = sort_group_records(first, rest);

        Some(RecordGroup {
            user_key,
            first,
            rest,
        })
    }

    fn next_record(&mut self) -> Option<ScanRecord> {
        match self.direction {
            Direction::Forward => self.next_record_forward(),
            Direction::Reverse => self.next_record_reverse(),
        }
    }

    fn next_record_forward(&mut self) -> Option<ScanRecord> {
        while self.index < self.records.len() {
            let record = self.records[self.index].clone();
            self.index += 1;
            match self.selector.forward_key_state(record.0.user_key()) {
                ForwardKeyState::Before => {}
                ForwardKeyState::Match => return Some(record),
                ForwardKeyState::After => {
                    self.index = self.records.len();
                    return None;
                }
            }
        }

        None
    }

    fn next_record_reverse(&mut self) -> Option<ScanRecord> {
        while self.index > 0 {
            self.index -= 1;
            let record = self.records[self.index].clone();
            match self.selector.reverse_key_state(record.0.user_key()) {
                ReverseKeyState::Above => {}
                ReverseKeyState::Match => return Some(record),
                ReverseKeyState::Below => {
                    self.index = 0;
                    return None;
                }
            }
        }

        None
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

fn forward_start_index(records: &[ScanRecord], selector: &ScanSelector) -> usize {
    records.partition_point(|(internal_key, _)| {
        selector.forward_key_state(internal_key.user_key()) == ForwardKeyState::Before
    })
}

fn reverse_start_index(records: &[ScanRecord], selector: &ScanSelector) -> usize {
    match selector {
        ScanSelector::Range(range) => records.partition_point(|(internal_key, _)| {
            !key_is_after_end(internal_key.user_key(), &range.end)
        }),
        ScanSelector::Prefix(prefix) => match prefix_successor(prefix) {
            Some(end) => records
                .partition_point(|(internal_key, _)| internal_key.user_key() < end.as_slice()),
            None => records.len(),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanSelector {
    Range(KeyRange),
    Prefix(Vec<u8>),
}

impl ScanSelector {
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.forward_key_state(key) == ForwardKeyState::Match
    }

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
