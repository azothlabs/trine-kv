use super::{Arc, BatchOperation, BucketOptions, Db, Error, LsmTree, Path, Result, lock_poisoned};

impl Db {
    pub(crate) fn reads_follow_bucket_registry(&self) -> bool {
        self.inner.options.read_only && self.inner.options.storage_mode.is_object_store_persistent()
    }

    pub(crate) fn bucket_state(&self, bucket: &str) -> Result<Arc<LsmTree>> {
        self.bucket_state_if_exists(bucket)?
            .ok_or_else(|| Error::BucketMissing {
                name: bucket.to_owned(),
            })
    }

    pub(crate) fn bucket_state_if_exists(&self, bucket: &str) -> Result<Option<Arc<LsmTree>>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;

        Ok(buckets.get(bucket).cloned())
    }

    pub(in crate::db) fn persistent_path(&self) -> Option<&Path> {
        self.inner.options.storage_mode.persistent_path()
    }

    pub(in crate::db) fn persist_bucket_creation(
        &self,
        name: &str,
        options: &BucketOptions,
    ) -> Result<()> {
        if let Some(manifest) = &self.inner.manifest {
            // Manifest I/O happens outside the bucket registry lock. Two
            // racing creators are serialized by the manifest lock, and the
            // second identical request becomes a no-op.
            let result = manifest
                .lock()
                .map_err(|_| lock_poisoned("manifest store"))?
                .create_bucket(name, options.clone());
            result.map_err(|error| {
                self.close_after_manifest_durability_failure("bucket creation", error)
            })?;
        }

        Ok(())
    }

    pub(in crate::db) fn resolve_batch_buckets(
        &self,
        operations: &[BatchOperation],
        expected_bucket: Option<&crate::db::commit::ExpectedBucketState>,
    ) -> Result<Vec<Arc<LsmTree>>> {
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let mut states = Vec::with_capacity(operations.len());

        if let Some(expected) = expected_bucket {
            let current = buckets.get(&expected.name);
            let same_generation = current.is_some_and(|current| {
                if expected.state.generation == 0 {
                    Arc::ptr_eq(current, &expected.state)
                } else {
                    current.generation == expected.state.generation
                }
            });
            if !same_generation {
                return Err(Error::BucketStale {
                    name: expected.name.clone(),
                });
            }
        }

        for operation in operations {
            let state =
                buckets
                    .get(operation.bucket())
                    .cloned()
                    .ok_or_else(|| Error::BucketMissing {
                        name: operation.bucket().to_owned(),
                    })?;
            states.push(state);
        }

        Ok(states)
    }
}
