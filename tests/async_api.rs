use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use trine_kv::{Db, DbOptions, DurabilityMode, Iter, KeyRange, KeyValue, WriteBatch, WriteOptions};

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

fn collect(iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|item| {
        let KeyValue { key, value } = item.expect("iterator item is readable");
        (key, value)
    })
    .collect()
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
        collect(block_on_ready(db.prefix_async(b"b".to_vec())).expect("prefix opens"));
    assert_eq!(default_rows, vec![(b"b".to_vec(), b"two".to_vec())]);

    let events = block_on_ready(db.bucket_async("events")).expect("bucket opens");
    block_on_ready(events.put_async(b"e1".to_vec(), b"event".to_vec()))
        .expect("bucket put through async API");
    assert_eq!(
        block_on_ready(events.get_async(b"e1")).expect("bucket get through async API"),
        Some(b"event".to_vec())
    );
    assert_eq!(
        collect(block_on_ready(events.range_async(&KeyRange::all())).expect("range opens")),
        vec![(b"e1".to_vec(), b"event".to_vec())]
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
