use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    error::{Error, Result},
    table::{Table, TableId, TableLevel},
    types::KeyRange,
};

#[derive(Debug, Clone)]
pub(crate) struct LsmVersion {
    levels: Vec<LevelState>,
}

impl LsmVersion {
    pub(crate) fn new(tables: Vec<Arc<Table>>) -> Result<Self> {
        let mut by_level = BTreeMap::<TableLevel, Vec<Arc<Table>>>::new();
        for table in tables {
            by_level
                .entry(table.properties().level)
                .or_default()
                .push(table);
        }

        let mut levels = Vec::new();
        for (level, tables) in by_level {
            levels.push(LevelState::new(level, tables)?);
        }

        Ok(Self { levels })
    }

    #[must_use]
    pub(crate) fn table_handles(&self) -> Vec<Arc<Table>> {
        self.levels
            .iter()
            .flat_map(|level| level.tables.iter().cloned())
            .collect()
    }

    #[must_use]
    pub(crate) fn level_table_handles(&self) -> Vec<(TableLevel, Vec<Arc<Table>>)> {
        self.levels
            .iter()
            .map(|level| (level.level, level.tables.clone()))
            .collect()
    }

    #[must_use]
    pub(crate) fn point_lookup_tables(&self, key: &[u8]) -> Vec<Arc<Table>> {
        let mut tables = Vec::new();
        for level in &self.levels {
            if level.level == TableLevel::ZERO {
                tables.extend(
                    level
                        .tables
                        .iter()
                        .filter(|table| table.may_contain_key(key))
                        .cloned(),
                );
            } else if let Some(table) = level.table_for_key(key) {
                if table.may_contain_key(key) {
                    tables.push(Arc::clone(table));
                }
            }
        }
        tables
    }

    #[must_use]
    pub(crate) fn range_tombstone_tables_for_key(&self, key: &[u8]) -> Vec<Arc<Table>> {
        let mut tables = Vec::new();
        for level in &self.levels {
            if level.level == TableLevel::ZERO {
                tables.extend(
                    level
                        .tables
                        .iter()
                        .filter(|table| table.key_bounds_may_contain_key(key))
                        .cloned(),
                );
            } else if let Some(table) = level.table_for_key(key) {
                if table.key_bounds_may_contain_key(key) {
                    tables.push(Arc::clone(table));
                }
            }
        }
        tables
    }

    #[must_use]
    pub(crate) fn range_scan_tables(&self, range: &KeyRange) -> Vec<Arc<Table>> {
        self.levels
            .iter()
            .flat_map(|level| level.tables_overlapping_range(range))
            .collect()
    }

    #[must_use]
    pub(crate) fn l0_table_count(&self) -> usize {
        self.levels
            .iter()
            .find(|level| level.level == TableLevel::ZERO)
            .map_or(0, LevelState::table_count)
    }

    pub(crate) fn with_added_l0_table(&self, table: Arc<Table>) -> Result<Self> {
        if table.properties().level != TableLevel::ZERO {
            return Err(Error::Corruption {
                message: format!(
                    "flush table {} was not written to L0",
                    table.properties().id.get()
                ),
            });
        }

        let mut tables = self.table_handles();
        tables.push(table);
        Self::new(tables)
    }

    pub(crate) fn with_replaced_tables(
        &self,
        input_table_ids: &[TableId],
        output_tables: Vec<Arc<Table>>,
    ) -> Result<Self> {
        let input_table_ids = input_table_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut removed = 0_usize;
        let mut tables = Vec::new();

        for table in self.table_handles() {
            if input_table_ids.contains(&table.properties().id) {
                removed += 1;
            } else {
                tables.push(table);
            }
        }

        if removed != input_table_ids.len() {
            return Err(Error::Corruption {
                message: "compaction tried to replace a table outside current version".to_owned(),
            });
        }

        tables.extend(output_tables);
        Self::new(tables)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LevelState {
    level: TableLevel,
    tables: Vec<Arc<Table>>,
}

impl LevelState {
    fn new(level: TableLevel, mut tables: Vec<Arc<Table>>) -> Result<Self> {
        if level == TableLevel::ZERO {
            tables.sort_by(compare_l0_tables_for_reads);
        } else {
            tables.sort_by(compare_non_overlapping_tables);
            validate_non_overlapping_level(level, &tables)?;
        }

        Ok(Self { level, tables })
    }

    #[must_use]
    fn table_count(&self) -> usize {
        self.tables.len()
    }

    fn table_for_key(&self, key: &[u8]) -> Option<&Arc<Table>> {
        let index = self.tables.partition_point(|table| {
            let properties = table.properties();
            table_has_key_bounds(table) && properties.largest_user_key.as_slice() < key
        });
        let table = self.tables.get(index)?;
        table_has_key_bounds(table).then_some(table)
    }

    fn tables_overlapping_range(&self, range: &KeyRange) -> Vec<Arc<Table>> {
        self.tables
            .iter()
            .filter(|table| table.key_bounds_overlap_range(range))
            .cloned()
            .collect()
    }
}

fn compare_l0_tables_for_reads(left: &Arc<Table>, right: &Arc<Table>) -> Ordering {
    let left = left.properties();
    let right = right.properties();
    left.level
        .cmp(&right.level)
        .then_with(|| right.largest_sequence.cmp(&left.largest_sequence))
        .then_with(|| right.id.cmp(&left.id))
}

fn compare_non_overlapping_tables(left: &Arc<Table>, right: &Arc<Table>) -> Ordering {
    let left_props = left.properties();
    let right_props = right.properties();
    table_has_key_bounds(left)
        .cmp(&table_has_key_bounds(right))
        .reverse()
        .then_with(|| {
            left_props
                .smallest_user_key
                .cmp(&right_props.smallest_user_key)
        })
        .then_with(|| {
            left_props
                .largest_user_key
                .cmp(&right_props.largest_user_key)
        })
        .then_with(|| left_props.id.cmp(&right_props.id))
}

fn validate_non_overlapping_level(level: TableLevel, tables: &[Arc<Table>]) -> Result<()> {
    let mut previous: Option<&Arc<Table>> = None;

    for table in tables {
        validate_table_key_bounds(table)?;
        if !table_has_key_bounds(table) {
            if tables.len() > 1 {
                return Err(Error::Corruption {
                    message: format!(
                        "level {} has an unbounded table mixed with other tables",
                        level.get()
                    ),
                });
            }
            continue;
        }

        if let Some(previous) = previous {
            let previous_properties = previous.properties();
            let properties = table.properties();
            if previous_properties.largest_user_key >= properties.smallest_user_key {
                return Err(Error::Corruption {
                    message: format!(
                        "level {} has overlapping tables {} and {}",
                        level.get(),
                        previous_properties.id.get(),
                        properties.id.get()
                    ),
                });
            }
        }
        previous = Some(table);
    }

    Ok(())
}

fn validate_table_key_bounds(table: &Arc<Table>) -> Result<()> {
    let properties = table.properties();
    if table_has_key_bounds(table) && properties.smallest_user_key > properties.largest_user_key {
        return Err(Error::Corruption {
            message: format!("table {} has invalid key bounds", properties.id.get()),
        });
    }
    Ok(())
}

fn table_has_key_bounds(table: &Arc<Table>) -> bool {
    table.has_key_bounds()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering as AtomicOrdering},
        },
    };

    use crate::{
        blob::ValueRef,
        codec::CodecId,
        internal_key::{InternalKey, ValueKind},
        options::{FilterPolicy, PrefixFilterPolicy},
        prefix::PrefixExtractor,
        table::{self, TableId, TableLevel},
        types::{KeyRange, Sequence},
    };

    use super::LsmVersion;

    static NEXT_TEST_FILE_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn l0_allows_overlap_and_orders_newest_first() {
        let older = Arc::new(test_table(1, TableLevel::ZERO, b"k", 10));
        let newer = Arc::new(test_table(2, TableLevel::ZERO, b"k", 20));

        let version = LsmVersion::new(vec![older, Arc::clone(&newer)]).expect("valid version");

        let level = version.levels.first().expect("L0 exists");
        assert_eq!(level.level, TableLevel::ZERO);
        assert_eq!(level.tables[0].properties().id, newer.properties().id);
        assert_eq!(level.table_count(), 2);
    }

    #[test]
    fn deeper_levels_reject_overlap() {
        let left = Arc::new(test_table(10, TableLevel(1), b"k", 10));
        let right = Arc::new(test_table(11, TableLevel(1), b"k", 5));

        let error = LsmVersion::new(vec![left, right]).expect_err("overlap is invalid");

        assert!(error.to_string().contains("overlapping tables"));
    }

    #[test]
    fn deeper_levels_sort_by_key_range() {
        let high = Arc::new(test_table(20, TableLevel(1), b"z", 10));
        let low = Arc::new(test_table(21, TableLevel(1), b"a", 10));

        let version = LsmVersion::new(vec![high, Arc::clone(&low)]).expect("valid version");

        let level = version.levels.first().expect("L1 exists");
        assert_eq!(level.tables[0].properties().id, low.properties().id);
    }

    #[test]
    fn flush_adds_table_to_l0() {
        let base = Arc::new(test_table(30, TableLevel(1), b"a", 10));
        let flushed = Arc::new(test_table(31, TableLevel::ZERO, b"a", 20));
        let version = LsmVersion::new(vec![base]).expect("valid version");

        let next = version
            .with_added_l0_table(Arc::clone(&flushed))
            .expect("flush installs");

        assert_eq!(next.l0_table_count(), 1);
        assert_eq!(
            next.point_lookup_tables(b"a")[0].properties().id,
            flushed.properties().id
        );
    }

    #[test]
    fn point_lookup_uses_l0_overlaps_and_one_deeper_table_per_level() {
        let l0_old = Arc::new(test_table(32, TableLevel::ZERO, b"k", 10));
        let l0_new = Arc::new(test_table(33, TableLevel::ZERO, b"k", 20));
        let l1_hit = Arc::new(test_table(34, TableLevel(1), b"k", 5));
        let l1_miss = Arc::new(test_table(35, TableLevel(1), b"z", 5));
        let l2_hit = Arc::new(test_table(36, TableLevel(2), b"k", 4));
        let version = LsmVersion::new(vec![
            Arc::clone(&l0_old),
            Arc::clone(&l0_new),
            Arc::clone(&l1_hit),
            Arc::clone(&l1_miss),
            Arc::clone(&l2_hit),
        ])
        .expect("valid version");

        let ids = table_ids(version.point_lookup_tables(b"k"));

        assert_eq!(
            ids,
            vec![
                l0_new.properties().id,
                l0_old.properties().id,
                l1_hit.properties().id,
                l2_hit.properties().id,
            ]
        );
    }

    #[test]
    fn range_scan_tables_skip_unrelated_non_overlapping_tables() {
        let l0_hit = Arc::new(test_table(37, TableLevel::ZERO, b"c", 10));
        let l0_miss = Arc::new(test_table(38, TableLevel::ZERO, b"z", 10));
        let l1_left = Arc::new(test_table(39, TableLevel(1), b"a", 5));
        let l1_hit = Arc::new(test_table(40, TableLevel(1), b"c", 5));
        let l1_right = Arc::new(test_table(41, TableLevel(1), b"z", 5));
        let version = LsmVersion::new(vec![
            Arc::clone(&l0_hit),
            Arc::clone(&l0_miss),
            Arc::clone(&l1_left),
            Arc::clone(&l1_hit),
            Arc::clone(&l1_right),
        ])
        .expect("valid version");

        let ids = table_ids(version.range_scan_tables(&KeyRange::half_open(b"b", b"d")));

        assert_eq!(ids, vec![l0_hit.properties().id, l1_hit.properties().id]);
    }

    #[test]
    fn range_tombstone_lookup_uses_key_bounds_without_table_filter() {
        let tombstone_table = Arc::new(test_table_with_tombstone(
            43,
            TableLevel(1),
            b"a",
            KeyRange::half_open(b"a", b"z"),
        ));
        let version = LsmVersion::new(vec![Arc::clone(&tombstone_table)]).expect("valid version");

        assert_eq!(
            table_ids(version.point_lookup_tables(b"m")),
            vec![tombstone_table.properties().id],
            "key bounds keep the table visible when full-table filters are absent"
        );
        assert_eq!(
            table_ids(version.range_tombstone_tables_for_key(b"m")),
            vec![tombstone_table.properties().id]
        );
    }

    #[test]
    fn replace_tables_installs_outputs_and_removes_inputs() {
        let old_l0 = Arc::new(test_table(40, TableLevel::ZERO, b"a", 10));
        let old_l1 = Arc::new(test_table(41, TableLevel(1), b"z", 10));
        let output = Arc::new(test_table(42, TableLevel(1), b"a", 20));
        let version =
            LsmVersion::new(vec![Arc::clone(&old_l0), Arc::clone(&old_l1)]).expect("valid version");

        let next = version
            .with_replaced_tables(&[old_l0.properties().id], vec![Arc::clone(&output)])
            .expect("compaction installs");

        let ids = next
            .table_handles()
            .into_iter()
            .map(|table| table.properties().id)
            .collect::<Vec<_>>();
        assert!(!ids.contains(&old_l0.properties().id));
        assert!(ids.contains(&old_l1.properties().id));
        assert!(ids.contains(&output.properties().id));
    }

    #[test]
    fn old_version_handle_keeps_previous_tables_after_replacement() {
        let old = Arc::new(test_table(50, TableLevel::ZERO, b"a", 10));
        let output = Arc::new(test_table(51, TableLevel(1), b"a", 20));
        let version = Arc::new(LsmVersion::new(vec![Arc::clone(&old)]).expect("valid version"));

        let held_version = Arc::clone(&version);
        let next = version
            .with_replaced_tables(&[old.properties().id], vec![Arc::clone(&output)])
            .expect("compaction installs");

        let held_ids = held_version
            .table_handles()
            .into_iter()
            .map(|table| table.properties().id)
            .collect::<Vec<_>>();
        let next_ids = next
            .table_handles()
            .into_iter()
            .map(|table| table.properties().id)
            .collect::<Vec<_>>();

        assert_eq!(held_ids, vec![old.properties().id]);
        assert_eq!(next_ids, vec![output.properties().id]);
    }

    fn table_ids(tables: Vec<Arc<table::Table>>) -> Vec<TableId> {
        tables
            .into_iter()
            .map(|table| table.properties().id)
            .collect()
    }

    fn test_table(id: u64, level: TableLevel, key: &[u8], sequence: u64) -> table::Table {
        let path = test_table_path(id);
        let _ = fs::remove_file(&path);
        table::write_table(
            &path,
            TableId(id),
            level,
            &test_table_options(),
            &[(
                InternalKey::new(key.to_vec(), Sequence::new(sequence), ValueKind::Put, 0),
                Some(ValueRef::Inline(vec![b'v'])),
            )],
            &[],
        )
        .expect("test table writes")
    }

    fn test_table_with_tombstone(
        id: u64,
        level: TableLevel,
        point_key: &[u8],
        range: KeyRange,
    ) -> table::Table {
        let path = test_table_path(id);
        let _ = fs::remove_file(&path);
        table::write_table(
            &path,
            TableId(id),
            level,
            &test_table_options_with_filter(),
            &[(
                InternalKey::new(point_key.to_vec(), Sequence::new(1), ValueKind::Put, 0),
                Some(ValueRef::Inline(vec![b'v'])),
            )],
            &[table::TableRangeTombstone {
                range,
                sequence: Sequence::new(2),
                batch_index: 0,
            }],
        )
        .expect("test table writes")
    }

    fn test_table_path(id: u64) -> PathBuf {
        let unique = NEXT_TEST_FILE_ID.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!(
            "trine-kv-lsm-version-{}-{id}-{unique}.{}",
            std::process::id(),
            table::TABLE_FILE_EXTENSION
        ))
    }

    fn test_table_options() -> table::TableWriteOptions {
        table::TableWriteOptions {
            codec: CodecId::None,
            block_bytes: 4096,
            filter_policy: FilterPolicy::Disabled,
            prefix_extractor: PrefixExtractor::Disabled,
            prefix_filter_policy: PrefixFilterPolicy::Disabled,
            blob_threshold_bytes: usize::MAX,
            rewrite_blob_indexes: false,
        }
    }

    fn test_table_options_with_filter() -> table::TableWriteOptions {
        table::TableWriteOptions {
            codec: CodecId::None,
            block_bytes: 4096,
            filter_policy: FilterPolicy::Bloom { bits_per_key: 64 },
            prefix_extractor: PrefixExtractor::Disabled,
            prefix_filter_policy: PrefixFilterPolicy::Disabled,
            blob_threshold_bytes: usize::MAX,
            rewrite_blob_indexes: false,
        }
    }
}
