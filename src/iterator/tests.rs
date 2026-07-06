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
