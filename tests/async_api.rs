use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use trine_kv::{
    Db, DbOptions, DurabilityMode, Iter, KeyRange, KeyValue, LazyIter, WriteBatch, WriteOptions,
};

fn block_on_ready<T>(future: impl Future<Output = T>) -> T {
    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("async compatibility future unexpectedly pending"),
    }
}

fn collect_async(mut iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    while let Some(KeyValue { key, value }) =
        block_on_ready(iter.next_async()).expect("async iterator item is readable")
    {
        rows.push((key, value));
    }
    rows
}

fn collect_lazy_async(mut iter: LazyIter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    while let Some(item) =
        block_on_ready(iter.next_async()).expect("async lazy iterator item is readable")
    {
        let value = block_on_ready(item.value.read_async()).expect("lazy value reads");
        rows.push((item.key, value));
    }
    rows
}

#[test]
fn memory_async_compatibility_surface_smoke() {
    let db = block_on_ready(Db::open_async(DbOptions::memory())).expect("memory db opens");

    block_on_ready(db.put_async(b"a".to_vec(), b"one".to_vec())).expect("put through async API");
    assert_eq!(
        block_on_ready(db.get_async(b"a")).expect("get through async API"),
        Some(b"one".to_vec())
    );

    let mut batch = WriteBatch::new();
    batch.put(b"b".to_vec(), b"two".to_vec());
    let commit =
        block_on_ready(db.write_async(batch, WriteOptions::default())).expect("batch writes");
    assert_eq!(commit.sequence(), db.last_committed_sequence());

    let default_rows =
        collect_async(block_on_ready(db.prefix_async(b"b".to_vec())).expect("prefix opens"));
    assert_eq!(default_rows, vec![(b"b".to_vec(), b"two".to_vec())]);
    assert_eq!(
        collect_lazy_async(
            block_on_ready(db.prefix_lazy_async(b"b".to_vec())).expect("prefix opens")
        ),
        vec![(b"b".to_vec(), b"two".to_vec())]
    );

    let events = block_on_ready(db.bucket_async("events")).expect("bucket opens");
    block_on_ready(events.put_async(b"e1".to_vec(), b"event".to_vec()))
        .expect("bucket put through async API");
    assert_eq!(
        block_on_ready(events.get_async(b"e1")).expect("bucket get through async API"),
        Some(b"event".to_vec())
    );
    assert_eq!(
        collect_async(block_on_ready(events.range_async(&KeyRange::all())).expect("range opens")),
        vec![(b"e1".to_vec(), b"event".to_vec())]
    );
    let mut lazy_events =
        block_on_ready(events.range_lazy_async(&KeyRange::all())).expect("lazy range opens");
    let lazy_event = block_on_ready(lazy_events.next_async())
        .expect("lazy event advances")
        .expect("lazy event exists");
    let lazy_event = block_on_ready(lazy_event.into_key_value_async())
        .expect("lazy event converts into owned key/value");
    assert_eq!(lazy_event.key, b"e1".to_vec());
    assert_eq!(lazy_event.value, b"event".to_vec());
    assert!(
        block_on_ready(lazy_events.next_async())
            .expect("lazy event iterator finishes")
            .is_none()
    );

    block_on_ready(db.delete_async(b"a".to_vec())).expect("delete through async API");
    assert_eq!(
        block_on_ready(db.get_async(b"a")).expect("deleted key reads"),
        None
    );

    block_on_ready(db.persist_async(DurabilityMode::Buffered)).expect("memory persist is accepted");
    block_on_ready(db.flush_async()).expect("memory flush is accepted");
    block_on_ready(db.compact_range_async(KeyRange::all())).expect("memory compact is accepted");
    block_on_ready(db.close_async());
}
