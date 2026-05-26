use std::{cmp::Ordering, ops::Bound};

use crate::types::KeyRange;

pub(crate) trait RangeTombstoneLike {
    fn range(&self) -> &KeyRange;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeTombstoneIndex<T> {
    tombstones: Vec<T>,
}

impl<T: RangeTombstoneLike> RangeTombstoneIndex<T> {
    pub(crate) fn new(mut tombstones: Vec<T>) -> Self {
        sort_tombstones(&mut tombstones);
        Self { tombstones }
    }

    pub(crate) fn all(&self) -> &[T] {
        &self.tombstones
    }

    pub(crate) fn covering_key<'idx, 'key>(
        &'idx self,
        key: &'key [u8],
    ) -> impl Iterator<Item = &'idx T> + 'idx
    where
        'key: 'idx,
    {
        let end = self
            .tombstones
            .partition_point(|tombstone| start_can_cover_key(&tombstone.range().start, key));
        self.tombstones[..end]
            .iter()
            .filter(move |tombstone| key_is_in_range(key, tombstone.range()))
    }

    pub(crate) fn overlapping_range<'idx, 'range>(
        &'idx self,
        range: &'range KeyRange,
    ) -> impl Iterator<Item = &'idx T> + 'idx
    where
        'range: 'idx,
    {
        let end = self
            .tombstones
            .partition_point(|tombstone| start_can_overlap_range(&tombstone.range().start, range));
        self.tombstones[..end]
            .iter()
            .filter(move |tombstone| ranges_overlap(range, tombstone.range()))
    }
}

pub(crate) fn sort_tombstones<T: RangeTombstoneLike>(tombstones: &mut [T]) {
    tombstones.sort_by(compare_tombstones);
}

pub(crate) fn insert_sorted<T: RangeTombstoneLike>(tombstones: &mut Vec<T>, tombstone: T) {
    let position = tombstones
        .binary_search_by(|probe| compare_tombstones(probe, &tombstone))
        .unwrap_or_else(|position| position);
    tombstones.insert(position, tombstone);
}

pub(crate) fn key_is_in_range(key: &[u8], range: &KeyRange) -> bool {
    !key_is_before_start(key, &range.start) && !key_is_after_end(key, &range.end)
}

pub(crate) fn ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    !range_ends_before_start(&left.end, &right.start)
        && !range_ends_before_start(&right.end, &left.start)
}

pub(crate) fn range_intersection(left: &KeyRange, right: &KeyRange) -> Option<KeyRange> {
    if !ranges_overlap(left, right) {
        return None;
    }

    Some(KeyRange {
        start: max_start_bound(&left.start, &right.start),
        end: min_end_bound(&left.end, &right.end),
    })
}

pub(crate) fn range_from_inclusive_span(smallest: &[u8], largest: &[u8]) -> KeyRange {
    KeyRange {
        start: Bound::Included(smallest.to_vec()),
        end: Bound::Included(largest.to_vec()),
    }
}

fn compare_tombstones<T: RangeTombstoneLike>(left: &T, right: &T) -> Ordering {
    compare_start_bounds(&left.range().start, &right.range().start)
        .then_with(|| compare_end_bounds(&left.range().end, &right.range().end))
}

fn start_can_cover_key(start: &Bound<Vec<u8>>, key: &[u8]) -> bool {
    match start {
        Bound::Unbounded => true,
        Bound::Included(start) => start.as_slice() <= key,
        Bound::Excluded(start) => start.as_slice() < key,
    }
}

fn start_can_overlap_range(start: &Bound<Vec<u8>>, range: &KeyRange) -> bool {
    match &range.end {
        Bound::Unbounded => true,
        Bound::Included(end) => match start {
            Bound::Unbounded => true,
            Bound::Included(start) => start <= end,
            Bound::Excluded(start) => start < end,
        },
        Bound::Excluded(end) => match start {
            Bound::Unbounded => true,
            Bound::Included(start) | Bound::Excluded(start) => start < end,
        },
    }
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

fn range_ends_before_start(end: &Bound<Vec<u8>>, start: &Bound<Vec<u8>>) -> bool {
    match (end, start) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Excluded(end), Bound::Included(start) | Bound::Excluded(start)) => {
            end.as_slice() <= start.as_slice()
        }
        (Bound::Included(end), Bound::Included(start)) => end.as_slice() < start.as_slice(),
        (Bound::Included(end), Bound::Excluded(start)) => end.as_slice() <= start.as_slice(),
    }
}

fn max_start_bound(left: &Bound<Vec<u8>>, right: &Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    if compare_start_bounds(left, right).is_lt() {
        right.clone()
    } else {
        left.clone()
    }
}

fn min_end_bound(left: &Bound<Vec<u8>>, right: &Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    if compare_end_bounds(left, right).is_gt() {
        right.clone()
    } else {
        left.clone()
    }
}

fn compare_start_bounds(left: &Bound<Vec<u8>>, right: &Bound<Vec<u8>>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(left), Bound::Included(right))
        | (Bound::Excluded(left), Bound::Excluded(right)) => left.cmp(right),
        (Bound::Included(left), Bound::Excluded(right)) => left.cmp(right).then(Ordering::Less),
        (Bound::Excluded(left), Bound::Included(right)) => left.cmp(right).then(Ordering::Greater),
    }
}

fn compare_end_bounds(left: &Bound<Vec<u8>>, right: &Bound<Vec<u8>>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(left), Bound::Included(right))
        | (Bound::Excluded(left), Bound::Excluded(right)) => left.cmp(right),
        (Bound::Excluded(left), Bound::Included(right)) => left.cmp(right).then(Ordering::Less),
        (Bound::Included(left), Bound::Excluded(right)) => left.cmp(right).then(Ordering::Greater),
    }
}

#[cfg(test)]
mod tests {
    use super::{RangeTombstoneIndex, RangeTombstoneLike};
    use crate::types::KeyRange;
    use std::ops::Bound;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestTombstone {
        name: &'static str,
        range: KeyRange,
    }

    impl RangeTombstoneLike for TestTombstone {
        fn range(&self) -> &KeyRange {
            &self.range
        }
    }

    #[test]
    fn covering_key_returns_only_possible_covering_tombstones() {
        let index = RangeTombstoneIndex::new(vec![
            tombstone("late", b"m", b"z"),
            tombstone("hit", b"b", b"d"),
            tombstone("early", b"a", b"b"),
        ]);

        let names = index
            .covering_key(b"c")
            .map(|tombstone| tombstone.name)
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["hit"]);
    }

    #[test]
    fn overlapping_range_returns_only_intersecting_tombstones() {
        let index = RangeTombstoneIndex::new(vec![
            tombstone("left", b"a", b"b"),
            tombstone("hit", b"c", b"f"),
            tombstone("right", b"z", b"zz"),
        ]);

        let names = index
            .overlapping_range(&KeyRange::half_open(b"d", b"e"))
            .map(|tombstone| tombstone.name)
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["hit"]);
    }

    fn tombstone(name: &'static str, start: &[u8], end: &[u8]) -> TestTombstone {
        TestTombstone {
            name,
            range: KeyRange::half_open(start, end),
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RandomTombstone {
        id: usize,
        range: KeyRange,
    }

    impl RangeTombstoneLike for RandomTombstone {
        fn range(&self) -> &KeyRange {
            &self.range
        }
    }

    #[test]
    fn randomized_queries_match_brute_force_reference() {
        let mut rng = TestRng::new(0x9e37_79b9_7f4a_7c15);

        for _case in 0..200 {
            let tombstones = (0..24)
                .map(|id| RandomTombstone {
                    id,
                    range: random_range(&mut rng),
                })
                .collect::<Vec<_>>();
            let index = RangeTombstoneIndex::new(tombstones.clone());

            for key_index in 0..20 {
                let key = key_bytes(key_index);
                let mut actual = index
                    .covering_key(&key)
                    .map(|tombstone| tombstone.id)
                    .collect::<Vec<_>>();
                let mut expected = tombstones
                    .iter()
                    .filter(|tombstone| super::key_is_in_range(&key, &tombstone.range))
                    .map(|tombstone| tombstone.id)
                    .collect::<Vec<_>>();
                actual.sort_unstable();
                expected.sort_unstable();
                assert_eq!(actual, expected);
            }

            for _range_index in 0..20 {
                let range = random_range(&mut rng);
                let mut actual = index
                    .overlapping_range(&range)
                    .map(|tombstone| tombstone.id)
                    .collect::<Vec<_>>();
                let mut expected = tombstones
                    .iter()
                    .filter(|tombstone| super::ranges_overlap(&range, &tombstone.range))
                    .map(|tombstone| tombstone.id)
                    .collect::<Vec<_>>();
                actual.sort_unstable();
                expected.sort_unstable();
                assert_eq!(actual, expected);
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct TestRng {
        state: u64,
    }

    impl TestRng {
        const fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        fn next(&mut self) -> u64 {
            self.state = self
                .state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            self.state
        }

        fn usize(&mut self, upper: usize) -> usize {
            let upper = u64::try_from(upper).expect("test upper bound fits u64");
            usize::try_from(self.next() % upper).expect("bounded random value fits usize")
        }
    }

    fn random_range(rng: &mut TestRng) -> KeyRange {
        let left = rng.usize(19);
        let right = left + 1 + rng.usize(20 - left);
        KeyRange {
            start: random_start_bound(rng, left),
            end: random_end_bound(rng, right),
        }
    }

    fn random_start_bound(rng: &mut TestRng, value: usize) -> Bound<Vec<u8>> {
        match rng.usize(4) {
            0 => Bound::Unbounded,
            1 | 2 => Bound::Included(key_bytes(value)),
            _ => Bound::Excluded(key_bytes(value)),
        }
    }

    fn random_end_bound(rng: &mut TestRng, value: usize) -> Bound<Vec<u8>> {
        match rng.usize(4) {
            0 => Bound::Unbounded,
            1 | 2 => Bound::Excluded(key_bytes(value)),
            _ => Bound::Included(key_bytes(value)),
        }
    }

    fn key_bytes(value: usize) -> Vec<u8> {
        format!("k{value:02}").into_bytes()
    }
}
