use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use trine_kv::{Bucket, BucketOptions, Db, DbOptions, Iter, KeyRange, Snapshot};

struct ModelSnapshot {
    snapshot: Snapshot,
    model: BTreeMap<Vec<u8>, Vec<u8>>,
    step: usize,
}

#[test]
fn persistent_snapshot_range_ignores_newer_point_delete() {
    let path = temp_db_path("snapshot-range-newer-delete");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options).expect("persistent db opens");
        let bucket = db
            .open_bucket_with_options("default", BucketOptions::default())
            .expect("bucket opens");
        bucket.put(b"key-06", b"value-06").expect("put");
        db.flush().expect("flush table");

        let snapshot = db.snapshot();
        bucket.delete(b"key-06").expect("newer delete");

        assert_eq!(
            snapshot.get(&bucket, b"key-06").expect("snapshot get"),
            Some(b"value-06".to_vec())
        );
        assert_eq!(
            collect_rows(
                snapshot
                    .range(&bucket, &KeyRange::all())
                    .expect("snapshot range")
            ),
            vec![(b"key-06".to_vec(), b"value-06".to_vec())]
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn randomized_operations_match_mvcc_reference_across_reopen() {
    let path = temp_db_path("model-reference");
    let mut options = DbOptions::persistent(&path);
    options.max_l0_files = 2;
    options.target_table_bytes = 512;
    let bucket_options = BucketOptions {
        block_bytes: 128,
        ..BucketOptions::default()
    };
    let keys = model_keys();
    let mut rng = TestRng::new(0x6eed_5eed_d15e_a5e5);
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let bucket = db
            .open_bucket_with_options("default", bucket_options.clone())
            .expect("bucket opens");
        let mut run = ModelRun::new(&db, &bucket, &keys, &mut rng, &mut model);
        run.run();
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let bucket = db
            .open_bucket_with_options("default", bucket_options)
            .expect("bucket reopens");
        assert_range(&bucket, &model);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

struct ModelRun<'run> {
    db: &'run Db,
    bucket: &'run Bucket,
    keys: &'run [Vec<u8>],
    rng: &'run mut TestRng,
    model: &'run mut BTreeMap<Vec<u8>, Vec<u8>>,
    snapshots: Vec<ModelSnapshot>,
    history: Vec<String>,
}

impl<'run> ModelRun<'run> {
    fn new(
        db: &'run Db,
        bucket: &'run Bucket,
        keys: &'run [Vec<u8>],
        rng: &'run mut TestRng,
        model: &'run mut BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Self {
        Self {
            db,
            bucket,
            keys,
            rng,
            model,
            snapshots: Vec::new(),
            history: Vec::new(),
        }
    }

    fn run(&mut self) {
        for step in 0..320 {
            self.apply_random_operation(step);
            self.assert_periodic_checks(step);
        }
        self.assert_after_final_flush_and_compaction();
    }

    fn apply_random_operation(&mut self, step: usize) {
        match self.rng.usize(10) {
            0..=2 => self.put_random_key(step),
            3 => self.delete_random_key(step),
            4 => self.delete_random_range(step),
            5 => self.assert_random_get(step),
            6 => self.assert_full_range(step),
            7 => self.capture_snapshot(step),
            8 => self.assert_random_snapshot(step),
            _ => self.flush_and_maybe_compact(step),
        }
    }

    fn put_random_key(&mut self, step: usize) {
        let key = self.keys[self.rng.usize(self.keys.len())].clone();
        let value = format!("value-{step:03}-{}", String::from_utf8_lossy(&key)).into_bytes();
        self.bucket.put(key.clone(), value.clone()).expect("put");
        self.history.push(format!(
            "{step}: put {} -> {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        ));
        self.model.insert(key, value);
    }

    fn delete_random_key(&mut self, step: usize) {
        let key = self.keys[self.rng.usize(self.keys.len())].clone();
        self.bucket.delete(key.clone()).expect("point delete");
        self.history
            .push(format!("{step}: delete {}", String::from_utf8_lossy(&key)));
        self.model.remove(&key);
    }

    fn delete_random_range(&mut self, step: usize) {
        let (start, end) = random_key_span(self.rng, self.keys);
        self.bucket
            .delete_range(KeyRange::half_open(
                self.keys[start].clone(),
                self.keys[end].clone(),
            ))
            .expect("range delete");
        self.history.push(format!(
            "{step}: delete_range {}..{}",
            String::from_utf8_lossy(&self.keys[start]),
            String::from_utf8_lossy(&self.keys[end])
        ));
        remove_model_range(self.model, &self.keys[start], &self.keys[end]);
    }

    fn assert_random_get(&mut self, step: usize) {
        self.history.push(format!("{step}: get"));
        assert_random_get(
            self.bucket,
            self.model,
            self.keys,
            self.rng,
            step,
            &self.history,
        );
    }

    fn assert_full_range(&mut self, step: usize) {
        self.history.push(format!("{step}: range"));
        assert_range(self.bucket, self.model);
    }

    fn capture_snapshot(&mut self, step: usize) {
        self.history.push(format!("{step}: snapshot"));
        self.snapshots.push(ModelSnapshot {
            snapshot: self.db.snapshot(),
            model: (*self.model).clone(),
            step,
        });
        if self.snapshots.len() > 6 {
            self.snapshots.remove(0);
        }
    }

    fn assert_random_snapshot(&mut self, step: usize) {
        if self.snapshots.is_empty() {
            return;
        }
        let index = self.rng.usize(self.snapshots.len());
        self.history.push(format!("{step}: check snapshot {index}"));
        assert_snapshot(&self.snapshots[index], self.bucket, self.keys, step);
    }

    fn flush_and_maybe_compact(&mut self, step: usize) {
        self.history.push(format!("{step}: flush"));
        self.db.flush().expect("flush succeeds");
        if self.rng.usize(2) == 0 {
            self.history.push(format!("{step}: compact_all"));
            self.db
                .compact_range(KeyRange::all())
                .expect("compaction succeeds");
        }
    }

    fn assert_periodic_checks(&mut self, step: usize) {
        if step % 17 == 0 {
            assert_random_get(
                self.bucket,
                self.model,
                self.keys,
                self.rng,
                step,
                &self.history,
            );
        }
        if step % 41 == 0 {
            assert_range(self.bucket, self.model);
        }
    }

    fn assert_after_final_flush_and_compaction(&mut self) {
        assert_range(self.bucket, self.model);
        self.snapshots.clear();
        self.db.flush().expect("final flush succeeds");
        self.db
            .compact_range(KeyRange::all())
            .expect("final compaction succeeds");
        assert_range(self.bucket, self.model);
    }
}

fn assert_random_get(
    bucket: &Bucket,
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    keys: &[Vec<u8>],
    rng: &mut TestRng,
    step: usize,
    history: &[String],
) {
    let key = &keys[rng.usize(keys.len())];
    assert_eq!(
        bucket.get(key).expect("point read"),
        model.get(key).cloned(),
        "point mismatch at step {step} for key {:?}\n{}",
        String::from_utf8_lossy(key),
        history_tail(history)
    );
}

fn assert_range(bucket: &Bucket, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    assert_eq!(
        collect_rows(bucket.range(&KeyRange::all()).expect("range read")),
        model_rows(model)
    );
}

fn assert_snapshot(snapshot: &ModelSnapshot, bucket: &Bucket, keys: &[Vec<u8>], step: usize) {
    for key in keys {
        assert_eq!(
            snapshot
                .snapshot
                .get(bucket, key)
                .expect("snapshot point read"),
            snapshot.model.get(key).cloned(),
            "snapshot point mismatch at step {step} for snapshot from step {} and key {:?}",
            snapshot.step,
            String::from_utf8_lossy(key)
        );
    }
    assert_eq!(
        collect_rows(
            snapshot
                .snapshot
                .range(bucket, &KeyRange::all())
                .expect("snapshot range read")
        ),
        model_rows(&snapshot.model),
        "snapshot range mismatch at step {step} for snapshot from step {}",
        snapshot.step
    );
}

fn collect_rows(iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|item| {
        let item = item.expect("iterator item reads");
        (item.key, item.value)
    })
    .collect()
}

fn model_rows(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn remove_model_range(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, start: &[u8], end: &[u8]) {
    let removed = model
        .range(start.to_vec()..end.to_vec())
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in removed {
        model.remove(&key);
    }
}

fn history_tail(history: &[String]) -> String {
    history
        .iter()
        .skip(history.len().saturating_sub(32))
        .cloned()
        .collect::<Vec<_>>()
        .join("\n")
}

fn random_key_span(rng: &mut TestRng, keys: &[Vec<u8>]) -> (usize, usize) {
    let start = rng.usize(keys.len() - 1);
    let end = start + 1 + rng.usize(keys.len() - start - 1);
    (start, end)
}

fn model_keys() -> Vec<Vec<u8>> {
    (0..12)
        .map(|index| format!("key-{index:02}").into_bytes())
        .collect()
}

fn temp_db_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("trine-kv-{name}-{}-{nonce}", std::process::id()))
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
