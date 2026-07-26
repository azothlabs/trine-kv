use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use proptest::{collection::vec, prelude::*, test_runner::Config};
use trine_kv::{Db, DbOptions, Iter, KeyRange, Snapshot};

static PROPERTY_DB_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
struct Action {
    kind: u8,
    first: u8,
    second: u8,
    value: Vec<u8>,
}

#[derive(Debug)]
struct PropertyDbPath(PathBuf);

impl PropertyDbPath {
    fn new() -> Self {
        let id = PROPERTY_DB_ID.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!("trine-property-{}-{id}", std::process::id())))
    }

    fn as_path(&self) -> &Path {
        &self.0
    }
}

impl Drop for PropertyDbPath {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn action_strategy() -> impl Strategy<Value = Action> {
    (0_u8..8, any::<u8>(), any::<u8>(), vec(any::<u8>(), 0..48)).prop_map(
        |(kind, first, second, value)| Action {
            kind,
            first,
            second,
            value,
        },
    )
}

proptest! {
    #![proptest_config(Config {
        cases: 32,
        max_shrink_iters: 4_096,
        ..Config::default()
    })]

    #[test]
    fn generated_persistent_histories_match_the_reference_model(
        actions in vec(action_strategy(), 1..72),
    ) {
        let path = PropertyDbPath::new();
        let options = DbOptions::persistent(path.as_path());
        let mut db = Db::open_sync(options.clone()).expect("property database opens");
        let mut bucket = db.default_bucket_sync().expect("default bucket opens");
        let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut snapshots = Vec::<(Snapshot, BTreeMap<Vec<u8>, Vec<u8>>)>::new();

        for action in actions {
            let first = model_key(action.first);
            let second = model_key(action.second);
            match action.kind {
                0 | 1 => {
                    bucket
                        .put_sync(first.clone(), action.value.clone())
                        .expect("generated put commits");
                    model.insert(first.clone(), action.value);
                }
                2 => {
                    bucket
                        .delete_sync(first.clone())
                        .expect("generated point delete commits");
                    model.remove(&first);
                }
                3 => {
                    let (start, end) = ordered_range(first.clone(), second);
                    bucket
                        .delete_range_sync(KeyRange::half_open(start.clone(), end.clone()))
                        .expect("generated range delete commits");
                    model.retain(|key, _| key < &start || key >= &end);
                }
                4 => {
                    snapshots.push((db.snapshot(), model.clone()));
                    if snapshots.len() > 4 {
                        snapshots.remove(0);
                    }
                }
                5 => db.flush_sync().expect("generated flush succeeds"),
                6 => {
                    snapshots.clear();
                    db.close_sync();
                    db = Db::open_sync(options.clone()).expect("generated reopen succeeds");
                    bucket = db.default_bucket_sync().expect("reopened bucket opens");
                }
                7 => db
                    .compact_range_sync(KeyRange::all())
                    .expect("generated compaction succeeds"),
                _ => unreachable!("strategy constrains action kinds"),
            }

            prop_assert_eq!(bucket.get_sync(&first).expect("generated point read"), model.get(&first).cloned());
            prop_assert_eq!(collect_rows(bucket.range_sync(&KeyRange::all()).expect("generated range read")), model_rows(&model));
            for (snapshot, expected) in &snapshots {
                prop_assert_eq!(
                    collect_rows(snapshot.range_sync(&bucket, &KeyRange::all()).expect("snapshot range read")),
                    model_rows(expected),
                );
            }
        }

        snapshots.clear();
        db.close_sync();
        let reopened = Db::open_sync(options).expect("final property reopen succeeds");
        let reopened_bucket = reopened.default_bucket_sync().expect("final bucket opens");
        prop_assert_eq!(
            collect_rows(reopened_bucket.range_sync(&KeyRange::all()).expect("final range read")),
            model_rows(&model),
        );
        reopened.close_sync();
    }
}

fn model_key(value: u8) -> Vec<u8> {
    format!("key-{:02}", value % 16).into_bytes()
}

fn ordered_range(first: Vec<u8>, second: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    if first <= second {
        (first, second)
    } else {
        (second, first)
    }
}

fn model_rows(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn collect_rows(iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|row| {
        let row = row.expect("iterator advances");
        (row.key, row.value)
    })
    .collect()
}
