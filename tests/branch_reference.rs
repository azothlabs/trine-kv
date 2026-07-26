use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use proptest::prelude::*;
use trine_kv::{Db, DbOptions, KeyRange};

static CASE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
enum Mutation {
    Put(u8, u8),
    Delete(u8),
}

impl Mutation {
    fn key(&self) -> Vec<u8> {
        let index = match self {
            Self::Put(index, _) | Self::Delete(index) => index,
        };
        format!("key-{index:02}").into_bytes()
    }

    fn apply_to_model(&self, model: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
        match self {
            Self::Put(_, value) => {
                model.insert(self.key(), format!("value-{value:02}").into_bytes());
            }
            Self::Delete(_) => {
                model.remove(&self.key());
            }
        }
    }
}

fn mutations() -> impl Strategy<Value = Vec<Mutation>> {
    prop::collection::vec(
        prop_oneof![
            (0_u8..16, 0_u8..32).prop_map(|(key, value)| Mutation::Put(key, value)),
            (0_u8..16).prop_map(Mutation::Delete),
        ],
        0..48,
    )
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let id = CASE_ID.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "trine-branch-reference-{}-{id}",
            std::process::id()
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if self.0.exists() {
            fs::remove_dir_all(&self.0).expect("branch reference directory removes");
        }
    }
}

fn apply_root(db: &Db, mutations: &[Mutation], model: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
    let bucket = db.bucket_sync("data").expect("root bucket opens");
    for mutation in mutations {
        match mutation {
            Mutation::Put(_, value) => bucket
                .put_sync(mutation.key(), format!("value-{value:02}").into_bytes())
                .expect("root put commits"),
            Mutation::Delete(_) => bucket
                .delete_sync(mutation.key())
                .expect("root delete commits"),
        }
        mutation.apply_to_model(model);
    }
}

fn apply_branch(db: &Db, mutations: &[Mutation], model: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut branch = db.open_branch("candidate").expect("branch opens");
    for mutation in mutations {
        match mutation {
            Mutation::Put(_, value) => branch
                .put(
                    "data",
                    mutation.key(),
                    format!("value-{value:02}").into_bytes(),
                )
                .expect("branch put commits"),
            Mutation::Delete(_) => branch
                .delete("data", mutation.key())
                .expect("branch delete commits"),
        }
        mutation.apply_to_model(model);
    }
}

fn read_branch(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.open_branch("candidate")
        .expect("branch opens")
        .range("data", &KeyRange::all())
        .expect("branch range opens")
        .map(|row| {
            let row = row.expect("branch row reads");
            (row.key, row.value)
        })
        .collect()
}

fn read_root(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.bucket_sync("data")
        .expect("root bucket opens")
        .range_sync(&KeyRange::all())
        .expect("root range opens")
        .map(|row| {
            let row = row.expect("root row reads");
            (row.key, row.value)
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 4_096,
        ..ProptestConfig::default()
    })]

    #[test]
    fn durable_branch_matches_frozen_parent_plus_divergence_across_reopen(
        seed in mutations(),
        root_after_fork in mutations(),
        branch_after_fork in mutations(),
    ) {
        let directory = TestDirectory::new();
        let mut root_model = BTreeMap::new();
        let mut branch_model;

        {
            let db = Db::open_sync(
                DbOptions::persistent(directory.path()).with_keep_last_read_versions(1),
            )
            .expect("persistent database opens");
            apply_root(&db, &seed, &mut root_model);
            branch_model = root_model.clone();
            db.create_branch("candidate", db.latest_read_version())
                .expect("durable branch creates");

            apply_root(&db, &root_after_fork, &mut root_model);
            apply_branch(&db, &branch_after_fork, &mut branch_model);
            db.flush_sync().expect("database flushes");

            prop_assert_eq!(
                read_root(&db),
                root_model.clone().into_iter().collect::<Vec<_>>()
            );
            prop_assert_eq!(
                read_branch(&db),
                branch_model.clone().into_iter().collect::<Vec<_>>()
            );
            db.close_sync();
        }

        let reopened = Db::open_sync(
            DbOptions::persistent(directory.path()).with_keep_last_read_versions(1),
        )
        .expect("persistent database reopens");
        prop_assert_eq!(
            read_root(&reopened),
            root_model.into_iter().collect::<Vec<_>>()
        );
        prop_assert_eq!(
            read_branch(&reopened),
            branch_model.into_iter().collect::<Vec<_>>()
        );
        reopened.close_sync();
    }
}
