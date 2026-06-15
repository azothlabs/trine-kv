use std::{cmp::Ordering, ops::Bound};

use crate::{
    error::{Error, Result},
    stats::{CompactionSkip, CompactionTrigger},
    table::{TableId, TableLevel, TableProperties},
    types::{KeyRange, Sequence},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub bucket: String,
    pub input_tables: Vec<TableId>,
    pub output_level: TableLevel,
    pub oldest_active_snapshot: Sequence,
    pub key_range: KeyRange,
    pub trigger: CompactionTrigger,
}

/// Outcome of one picker evaluation.
///
/// `plan` carries the compaction to run, if any. `skip` records a deliberate
/// non-uniform per-level policy decision (for example, leaving a deep level
/// lazy) so the caller can surface it through stats even when no compaction
/// runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionDecision {
    pub(crate) plan: Option<CompactionPlan>,
    pub(crate) skip: Option<CompactionSkip>,
}

impl CompactionDecision {
    fn planned(plan: CompactionPlan) -> Self {
        Self {
            plan: Some(plan),
            skip: None,
        }
    }

    fn idle(skip: Option<CompactionSkip>) -> Self {
        Self { plan: None, skip }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactionOptions {
    pub(crate) target_table_bytes: u64,
    pub(crate) level_size_multiplier: u64,
    pub(crate) max_l0_files: usize,
    pub(crate) local_l0_compaction: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionTable {
    pub(crate) id: TableId,
    pub(crate) level: TableLevel,
    pub(crate) bytes: u64,
    has_range_tombstones: bool,
    smallest_user_key: Vec<u8>,
    largest_user_key: Vec<u8>,
}

impl CompactionTable {
    pub(crate) fn from_properties_with_bytes(
        properties: &TableProperties,
        bytes: u64,
        has_range_tombstones: bool,
    ) -> Self {
        Self {
            id: properties.id,
            level: properties.level,
            bytes,
            has_range_tombstones,
            smallest_user_key: properties.smallest_user_key.clone(),
            largest_user_key: properties.largest_user_key.clone(),
        }
    }

    fn has_key_bounds(&self) -> bool {
        !(self.smallest_user_key.is_empty() && self.largest_user_key.is_empty())
    }

    fn overlaps_key_span(&self, span: &KeySpan) -> bool {
        if !self.has_key_bounds() {
            return true;
        }
        self.smallest_user_key.as_slice() <= span.largest.as_slice()
            && self.largest_user_key.as_slice() >= span.smallest.as_slice()
    }

    fn overlaps_range(&self, range: &KeyRange) -> bool {
        if is_all_range(range) || !self.has_key_bounds() {
            return true;
        }
        !key_is_after_end(&self.smallest_user_key, &range.end)
            && !key_is_before_start(&self.largest_user_key, &range.start)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeySpan {
    smallest: Vec<u8>,
    largest: Vec<u8>,
}

pub(crate) fn plan_compaction(
    bucket: &str,
    tables: &[CompactionTable],
    range: &KeyRange,
    oldest_active_snapshot: Sequence,
    options: CompactionOptions,
) -> Result<CompactionDecision> {
    if let Some(input_tables) = l0_compaction_inputs(tables, range, options) {
        let key_range = key_range_for_inputs(tables, &input_tables);
        return Ok(CompactionDecision::planned(CompactionPlan {
            bucket: bucket.to_owned(),
            input_tables,
            output_level: TableLevel(1),
            oldest_active_snapshot,
            key_range,
            trigger: CompactionTrigger::L0Overlap,
        }));
    }

    if let Some(level) = highest_scored_level(tables, range, options) {
        let output_level = level.next().ok_or_else(level_overflow)?;
        let input_tables = narrow_leveled_inputs(tables, range, level, output_level);
        if !input_tables.is_empty() {
            let key_range = key_range_for_inputs(tables, &input_tables);
            return Ok(CompactionDecision::planned(CompactionPlan {
                bucket: bucket.to_owned(),
                input_tables,
                output_level,
                oldest_active_snapshot,
                key_range,
                trigger: CompactionTrigger::LevelSize,
            }));
        }
    }

    // Tombstone-debt cleanup. A non-bottom table carrying range tombstones with
    // overlapping lower-level data is pushed down so the tombstone meets and
    // drops the data it covers instead of lingering on the read path. This runs
    // ahead of the no-pressure spread below because read-path pollution is worth
    // more than spreading. It is bounded and terminating: it only fires when
    // there is lower-level overlap to act on (a pure move would just relocate the
    // tombstone), and the deepest populated level is excluded, so the tombstone
    // migrates down at most to where its covered data already lives.
    if let Some(plan) = tombstone_debt_plan(bucket, tables, range, oldest_active_snapshot)? {
        return Ok(CompactionDecision::planned(plan));
    }

    // No-pressure fallback. The non-uniform per-level policy only merges a level
    // downward when its in-range table count reaches the level's depth-scaled
    // budget. Shallow levels stay tight so data keeps spreading toward larger
    // levels; deep levels stay lazy because their extra non-overlapping tables
    // add no point-read candidate depth and rewriting them only spawns an even
    // deeper level. `LevelSize` above still compacts any over-target level.
    let Some(level) = shallowest_multi_table_level(tables, range) else {
        return Ok(CompactionDecision::idle(lower_level_lazy_skip(
            tables, range,
        )));
    };
    let output_level = level.next().ok_or_else(level_overflow)?;
    let input_tables = narrow_leveled_inputs(tables, range, level, output_level);
    if input_tables.is_empty() {
        return Ok(CompactionDecision::idle(lower_level_lazy_skip(
            tables, range,
        )));
    }

    Ok(CompactionDecision::planned(CompactionPlan {
        bucket: bucket.to_owned(),
        key_range: key_range_for_inputs(tables, &input_tables),
        input_tables,
        output_level,
        oldest_active_snapshot,
        trigger: CompactionTrigger::MultiTableLevel,
    }))
}

/// Number of in-range tables a non-level-0 level must hold before the
/// no-pressure fallback merges one of them downward.
///
/// The budget grows with depth (`1 + level`: L1 -> 2, L2 -> 3, L3 -> 4, ...) so
/// shallow levels are compacted aggressively to keep read candidates low while
/// deep levels are left lazy until size pressure justifies a rewrite.
fn multi_table_fallback_threshold(level: TableLevel) -> usize {
    1usize.saturating_add(level.get() as usize)
}

/// Report a `LowerLevelLazy` skip when at least one non-level-0 level holds two
/// or more in-range tables (which the old uniform `>= 2` rule would have merged)
/// but stays under its depth-scaled budget, so the picker leaves it lazy.
fn lower_level_lazy_skip(tables: &[CompactionTable], range: &KeyRange) -> Option<CompactionSkip> {
    let suppressed = level_set(tables, range).into_iter().any(|level| {
        let count = overlapping_table_count(tables, range, level);
        count >= 2 && count < multi_table_fallback_threshold(level)
    });
    suppressed.then_some(CompactionSkip::LowerLevelLazy)
}

fn level_set(
    tables: &[CompactionTable],
    range: &KeyRange,
) -> std::collections::BTreeSet<TableLevel> {
    tables
        .iter()
        .filter(|table| table.level != TableLevel::ZERO && table.overlaps_range(range))
        .map(|table| table.level)
        .collect()
}

fn overlapping_table_count(
    tables: &[CompactionTable],
    range: &KeyRange,
    level: TableLevel,
) -> usize {
    tables
        .iter()
        .filter(|table| table.level == level && table.overlaps_range(range))
        .count()
}

fn deepest_nonzero_level(tables: &[CompactionTable]) -> Option<TableLevel> {
    tables
        .iter()
        .filter(|table| table.level != TableLevel::ZERO)
        .map(|table| table.level)
        .max()
}

/// Plan a tombstone-debt compaction: the shallowest non-level-0, non-deepest
/// level holding an in-range range-tombstone table, compacted down with its
/// overlapping lower-level data so the tombstone can drop what it covers.
///
/// Returns `None` (no compaction) unless there is lower-level overlap to merge
/// with, which both guarantees forward progress and prevents a re-trigger storm:
/// a tombstone with no lower data to act on is left alone.
fn tombstone_debt_plan(
    bucket: &str,
    tables: &[CompactionTable],
    range: &KeyRange,
    oldest_active_snapshot: Sequence,
) -> Result<Option<CompactionPlan>> {
    let deepest = deepest_nonzero_level(tables);
    let Some(level) = level_set(tables, range).into_iter().find(|level| {
        Some(*level) != deepest && level_has_range_tombstone_table(tables, range, *level)
    }) else {
        return Ok(None);
    };
    let output_level = level.next().ok_or_else(level_overflow)?;
    let input_tables = narrow_tombstone_inputs(tables, range, level, output_level);
    // A single input is a pure move that only relocates the tombstone; require
    // lower-level overlap so the compaction actually drops covered data.
    if input_tables.len() < 2 {
        return Ok(None);
    }

    Ok(Some(CompactionPlan {
        bucket: bucket.to_owned(),
        key_range: key_range_for_inputs(tables, &input_tables),
        input_tables,
        output_level,
        oldest_active_snapshot,
        trigger: CompactionTrigger::TombstoneDebt,
    }))
}

fn level_has_range_tombstone_table(
    tables: &[CompactionTable],
    range: &KeyRange,
    level: TableLevel,
) -> bool {
    tables.iter().any(|table| {
        table.level == level && table.has_range_tombstones && table.overlaps_range(range)
    })
}

fn narrow_tombstone_inputs(
    tables: &[CompactionTable],
    range: &KeyRange,
    input_level: TableLevel,
    output_level: TableLevel,
) -> Vec<TableId> {
    let Some(table) = tables.iter().find(|table| {
        table.level == input_level && table.has_range_tombstones && table.overlaps_range(range)
    }) else {
        return Vec::new();
    };
    let mut input_tables = vec![table.id];
    let span = key_span_for_inputs(tables, &input_tables);
    include_overlapping_level(tables, &mut input_tables, output_level, span.as_ref());
    input_tables
}

fn l0_compaction_inputs(
    tables: &[CompactionTable],
    range: &KeyRange,
    options: CompactionOptions,
) -> Option<Vec<TableId>> {
    let mut input_tables = l0_inputs_with_overlap(tables, range, options);
    if input_tables.is_empty() {
        return None;
    }

    let span = key_span_for_inputs(tables, &input_tables);
    include_overlapping_level(tables, &mut input_tables, TableLevel(1), span.as_ref());

    Some(input_tables)
}

fn l0_inputs_with_overlap(
    tables: &[CompactionTable],
    range: &KeyRange,
    options: CompactionOptions,
) -> Vec<TableId> {
    let broad_inputs = broad_l0_inputs(tables, range);
    let mut inputs = if options.local_l0_compaction {
        let local_inputs = local_l0_inputs(tables, range);
        if local_l0_inputs_save_rewrite(tables, &local_inputs, &broad_inputs) {
            local_inputs
        } else {
            broad_inputs
        }
    } else {
        broad_inputs
    };
    if inputs.is_empty() {
        return inputs;
    }
    close_overlapping_l0_inputs(tables, &mut inputs);
    inputs
}

fn close_overlapping_l0_inputs(tables: &[CompactionTable], inputs: &mut Vec<TableId>) {
    // L0 tables may overlap each other. Start from one local seed and then
    // close only the L0 span that touches it; unrelated L0 files remain for a
    // later pass instead of being rewritten just because the request range was
    // broad.
    loop {
        let Some(span) = key_span_for_inputs(tables, inputs) else {
            return;
        };
        let before = inputs.len();
        for table in tables {
            if table.level == TableLevel::ZERO
                && table.overlaps_key_span(&span)
                && !inputs.contains(&table.id)
            {
                inputs.push(table.id);
            }
        }
        if inputs.len() == before {
            return;
        }
    }
}

fn broad_l0_inputs(tables: &[CompactionTable], range: &KeyRange) -> Vec<TableId> {
    tables
        .iter()
        .filter(|table| table.level == TableLevel::ZERO && table.overlaps_range(range))
        .map(|table| table.id)
        .collect()
}

fn local_l0_inputs(tables: &[CompactionTable], range: &KeyRange) -> Vec<TableId> {
    let mut inputs = pick_l0_seed_table(tables, range).map_or_else(Vec::new, |seed| vec![seed.id]);
    close_overlapping_l0_inputs(tables, &mut inputs);
    inputs
}

fn local_l0_inputs_save_rewrite(
    tables: &[CompactionTable],
    local_inputs: &[TableId],
    broad_inputs: &[TableId],
) -> bool {
    if local_inputs.is_empty() || local_inputs.len() >= broad_inputs.len() {
        return false;
    }
    let local_bytes = compaction_input_bytes(tables, local_inputs);
    let broad_bytes = compaction_input_bytes(tables, broad_inputs);
    if broad_bytes == 0 {
        return local_inputs.len().saturating_mul(2) < broad_inputs.len();
    }
    local_bytes.saturating_mul(2) < broad_bytes
}

fn compaction_input_bytes(tables: &[CompactionTable], input_tables: &[TableId]) -> u64 {
    tables
        .iter()
        .filter(|table| input_tables.contains(&table.id))
        .map(|table| table.bytes)
        .sum()
}

fn pick_l0_seed_table<'table>(
    tables: &'table [CompactionTable],
    range: &KeyRange,
) -> Option<&'table CompactionTable> {
    tables
        .iter()
        .filter(|table| table.level == TableLevel::ZERO && table.overlaps_range(range))
        .max_by(|left, right| compare_l0_seed_candidates(tables, left, right))
}

fn compare_l0_seed_candidates(
    tables: &[CompactionTable],
    left: &CompactionTable,
    right: &CompactionTable,
) -> Ordering {
    let left_overlap = overlapping_level_bytes(tables, left, TableLevel(1));
    let right_overlap = overlapping_level_bytes(tables, right, TableLevel(1));

    // Prefer the seed that rewrites fewer lower-level bytes. If that is tied,
    // take the larger L0 file to reduce file-count pressure, then use table id
    // for deterministic plans.
    right_overlap
        .cmp(&left_overlap)
        .then_with(|| left.bytes.cmp(&right.bytes))
        .then_with(|| right.id.cmp(&left.id))
}

fn overlapping_level_bytes(
    tables: &[CompactionTable],
    candidate: &CompactionTable,
    level: TableLevel,
) -> u64 {
    let Some(span) = key_span_for_inputs(tables, &[candidate.id]) else {
        return tables
            .iter()
            .filter(|table| table.level == level)
            .map(|table| table.bytes)
            .sum();
    };
    tables
        .iter()
        .filter(|table| table.level == level && table.overlaps_key_span(&span))
        .map(|table| table.bytes)
        .sum()
}

fn shallowest_multi_table_level(
    tables: &[CompactionTable],
    range: &KeyRange,
) -> Option<TableLevel> {
    level_set(tables, range).into_iter().find(|level| {
        overlapping_table_count(tables, range, *level) >= multi_table_fallback_threshold(*level)
    })
}

fn highest_scored_level(
    tables: &[CompactionTable],
    range: &KeyRange,
    options: CompactionOptions,
) -> Option<TableLevel> {
    tables
        .iter()
        .filter(|table| table.level != TableLevel::ZERO && table.overlaps_range(range))
        .map(|table| table.level)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|level| level_is_over_target(tables, *level, options))
        .max_by(|left, right| compare_level_scores(tables, *left, *right, options))
}

fn level_is_over_target(
    tables: &[CompactionTable],
    level: TableLevel,
    options: CompactionOptions,
) -> bool {
    level_bytes(tables, level) > level_target_bytes(level, options)
}

fn compare_level_scores(
    tables: &[CompactionTable],
    left: TableLevel,
    right: TableLevel,
    options: CompactionOptions,
) -> std::cmp::Ordering {
    let left_bytes = u128::from(level_bytes(tables, left));
    let right_bytes = u128::from(level_bytes(tables, right));
    let left_target = u128::from(level_target_bytes(left, options));
    let right_target = u128::from(level_target_bytes(right, options));

    left_bytes
        .saturating_mul(right_target)
        .cmp(&right_bytes.saturating_mul(left_target))
}

fn level_bytes(tables: &[CompactionTable], level: TableLevel) -> u64 {
    tables
        .iter()
        .filter(|table| table.level == level)
        .map(|table| table.bytes)
        .sum()
}

fn level_target_bytes(level: TableLevel, options: CompactionOptions) -> u64 {
    let exponent = level.get().saturating_sub(1);
    let mut target = options.target_table_bytes.max(1);
    for _ in 0..exponent {
        target = target.saturating_mul(options.level_size_multiplier.max(2));
    }
    target
}

fn narrow_leveled_inputs(
    tables: &[CompactionTable],
    range: &KeyRange,
    input_level: TableLevel,
    output_level: TableLevel,
) -> Vec<TableId> {
    let Some(table) = pick_leveled_input_table(tables, range, input_level) else {
        return Vec::new();
    };
    let mut input_tables = vec![table.id];
    let span = key_span_for_inputs(tables, &input_tables);
    include_overlapping_level(tables, &mut input_tables, output_level, span.as_ref());
    input_tables
}

fn pick_leveled_input_table<'table>(
    tables: &'table [CompactionTable],
    range: &KeyRange,
    level: TableLevel,
) -> Option<&'table CompactionTable> {
    tables
        .iter()
        .filter(|table| table.level == level && table.overlaps_range(range))
        .max_by(|left, right| compare_input_candidates(left, right))
}

fn compare_input_candidates(left: &CompactionTable, right: &CompactionTable) -> Ordering {
    left.bytes
        .cmp(&right.bytes)
        .then_with(|| right.id.cmp(&left.id))
}

fn include_overlapping_level(
    tables: &[CompactionTable],
    input_tables: &mut Vec<TableId>,
    level: TableLevel,
    span: Option<&KeySpan>,
) {
    let mut span = span.cloned();
    loop {
        let before = input_tables.len();
        for table in tables {
            let overlaps = span.as_ref().map_or_else(
                || table.overlaps_range(&KeyRange::all()),
                |span| table.overlaps_key_span(span),
            );
            if table.level == level && overlaps && !input_tables.contains(&table.id) {
                input_tables.push(table.id);
            }
        }
        if input_tables.len() == before {
            return;
        }
        span = key_span_for_inputs(tables, input_tables);
    }
}

fn key_span_for_inputs(tables: &[CompactionTable], input_tables: &[TableId]) -> Option<KeySpan> {
    let mut span: Option<KeySpan> = None;
    for table in tables
        .iter()
        .filter(|table| input_tables.contains(&table.id) && table.has_key_bounds())
    {
        span = Some(match span {
            Some(current) => KeySpan {
                smallest: std::cmp::min(current.smallest, table.smallest_user_key.clone()),
                largest: std::cmp::max(current.largest, table.largest_user_key.clone()),
            },
            None => KeySpan {
                smallest: table.smallest_user_key.clone(),
                largest: table.largest_user_key.clone(),
            },
        });
    }
    span
}

fn key_range_for_inputs(tables: &[CompactionTable], input_tables: &[TableId]) -> KeyRange {
    key_span_for_inputs(tables, input_tables).map_or_else(KeyRange::all, |span| KeyRange {
        start: Bound::Included(span.smallest),
        end: Bound::Included(span.largest),
    })
}

fn level_overflow() -> Error {
    Error::Corruption {
        message: "table level counter overflow".to_owned(),
    }
}

fn is_all_range(range: &KeyRange) -> bool {
    matches!(
        (&range.start, &range.end),
        (Bound::Unbounded, Bound::Unbounded)
    )
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

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::{CompactionOptions, CompactionTable, plan_compaction};
    use crate::{
        stats::CompactionTrigger,
        table::{TableId, TableLevel},
        types::{KeyRange, Sequence},
    };

    #[test]
    fn l0_plan_expands_overlapping_l0_group_and_lower_level_tables() {
        let tables = vec![
            table(1, 0, b"a", b"d"),
            table(2, 0, b"c", b"f"),
            table(3, 1, b"b", b"e"),
            table(4, 1, b"x", b"z"),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::half_open(b"b", b"c"),
            Sequence::new(7),
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(2), TableId(3)]);
        assert_eq!(plan.output_level, TableLevel(1));
        assert_eq!(plan.oldest_active_snapshot, Sequence::new(7));
        assert_eq!(plan.trigger, CompactionTrigger::L0Overlap);
    }

    #[test]
    fn single_l0_with_lower_overlap_is_planned() {
        let tables = vec![table(1, 0, b"a", b"c"), table(2, 1, b"b", b"d")];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(2)]);
        assert_eq!(plan.output_level, TableLevel(1));
    }

    #[test]
    fn single_l0_without_lower_overlap_is_planned_for_move() {
        let tables = vec![table(1, 0, b"a", b"c"), table(2, 1, b"x", b"z")];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::half_open(b"a", b"b"),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1)]);
        assert_eq!(plan.output_level, TableLevel(1));
    }

    #[test]
    fn l0_plan_uses_broad_inputs_when_local_rewrite_saving_is_small() {
        let tables = vec![
            table_with_bytes(1, 0, b"a", b"b", 10),
            table_with_bytes(2, 0, b"m", b"n", 20),
            table_with_bytes(3, 1, b"x", b"z", 1),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(2)]);
        assert_eq!(plan.key_range.start, Bound::Included(b"a".to_vec()));
        assert_eq!(plan.key_range.end, Bound::Included(b"n".to_vec()));
        assert_eq!(plan.output_level, TableLevel(1));
    }

    #[test]
    fn l0_plan_uses_local_seed_when_rewrite_saving_is_large() {
        let tables = vec![
            table_with_bytes(1, 0, b"a", b"b", 10),
            table_with_bytes(2, 0, b"m", b"n", 10),
            table_with_bytes(3, 0, b"q", b"r", 10),
            table_with_bytes(4, 0, b"x", b"z", 10),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1)]);
        assert_eq!(plan.key_range.start, Bound::Included(b"a".to_vec()));
        assert_eq!(plan.key_range.end, Bound::Included(b"b".to_vec()));
        assert_eq!(plan.output_level, TableLevel(1));
    }

    #[test]
    fn multi_table_fallback_uses_guard_local_input() {
        let tables = vec![
            table(1, 1, b"a", b"b"),
            table(2, 1, b"c", b"d"),
            table(3, 2, b"a", b"d"),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(3)]);
        assert_eq!(plan.output_level, TableLevel(2));
        assert_eq!(plan.trigger, CompactionTrigger::MultiTableLevel);
    }

    #[test]
    fn multi_table_fallback_closes_lower_level_overlaps() {
        let tables = vec![
            table(1, 1, b"a", b"b"),
            table(2, 1, b"m", b"n"),
            table(3, 2, b"b", b"c"),
            table(4, 2, b"c", b"d"),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(3), TableId(4)]);
        assert_eq!(plan.output_level, TableLevel(2));
        assert_eq!(plan.key_range.start, Bound::Included(b"a".to_vec()));
        assert_eq!(plan.key_range.end, Bound::Included(b"d".to_vec()));
        assert_eq!(plan.trigger, CompactionTrigger::MultiTableLevel);
    }

    #[test]
    fn overfull_level_score_picks_largest_pressure_ratio() {
        let tables = vec![
            table_with_bytes(1, 1, b"a", b"b", 90),
            table_with_bytes(2, 2, b"a", b"b", 1_500),
            table_with_bytes(3, 3, b"a", b"b", 2_000),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(2), TableId(3)]);
        assert_eq!(plan.output_level, TableLevel(3));
        assert_eq!(plan.trigger, CompactionTrigger::LevelSize);
    }

    #[test]
    fn overfull_level_uses_narrow_input_and_lower_overlap() {
        let tables = vec![
            table_with_bytes(1, 1, b"a", b"b", 60),
            table_with_bytes(2, 1, b"c", b"d", 90),
            table_with_bytes(3, 2, b"c", b"e", 1),
            table_with_bytes(4, 2, b"x", b"z", 1),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(2), TableId(3)]);
        assert_eq!(plan.output_level, TableLevel(2));
    }

    #[test]
    fn overfull_level_without_lower_overlap_selects_single_move_input() {
        let tables = vec![
            table_with_bytes(1, 1, b"a", b"b", 60),
            table_with_bytes(2, 1, b"c", b"d", 90),
            table_with_bytes(3, 2, b"x", b"z", 1),
        ];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds")
        .plan
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(2)]);
        assert_eq!(plan.output_level, TableLevel(2));
    }

    #[test]
    fn shallow_multi_table_level_still_merges_at_two_tables() {
        // L1 stays tight: two non-overlapping tables and no L0 trigger one
        // guard-local downward merge, preserving the upper/middle policy.
        let tables = vec![
            table(1, 1, b"a", b"b"),
            table(2, 1, b"c", b"d"),
            table(3, 2, b"a", b"d"),
        ];

        let decision = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds");

        let plan = decision.plan.expect("plan exists");
        assert_eq!(plan.trigger, CompactionTrigger::MultiTableLevel);
        assert_eq!(decision.skip, None);
    }

    #[test]
    fn deep_level_stays_lazy_below_depth_budget() {
        // L2 holds two non-overlapping tables but its depth-scaled budget is 3,
        // so the no-pressure fallback leaves it lazy and reports the skip.
        let tables = vec![
            table(1, 1, b"a", b"z"),
            table(2, 2, b"a", b"b"),
            table(3, 2, b"c", b"d"),
        ];

        let decision = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds");

        assert_eq!(decision.plan, None);
        assert_eq!(
            decision.skip,
            Some(crate::stats::CompactionSkip::LowerLevelLazy)
        );
    }

    #[test]
    fn deep_level_merges_once_budget_is_reached() {
        // Three non-overlapping L2 tables reach the L2 budget of 3, so the
        // fallback merges one guard-local input downward.
        let tables = vec![
            table(1, 1, b"a", b"z"),
            table(2, 2, b"a", b"b"),
            table(3, 2, b"d", b"e"),
            table(4, 2, b"h", b"i"),
        ];

        let decision = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds");

        let plan = decision.plan.expect("plan exists");
        assert_eq!(plan.trigger, CompactionTrigger::MultiTableLevel);
        assert_eq!(plan.output_level, TableLevel(3));
        assert_eq!(plan.input_tables.len(), 1);
        assert_eq!(decision.skip, None);
    }

    #[test]
    fn deep_level_size_pressure_still_compacts_when_over_target() {
        // Even under the lazy budget, a level above its byte target compacts via
        // the LevelSize trigger: lower levels are lazy only without a trigger.
        let tables = vec![
            table_with_bytes(1, 1, b"a", b"z", 1),
            table_with_bytes(2, 2, b"a", b"b", 5_000),
            table_with_bytes(3, 2, b"c", b"d", 5_000),
        ];

        let decision = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds");

        let plan = decision.plan.expect("plan exists");
        assert_eq!(plan.trigger, CompactionTrigger::LevelSize);
        assert_eq!(decision.skip, None);
    }

    #[test]
    fn range_tombstone_table_with_lower_overlap_triggers_tombstone_debt() {
        // L1 carries a range tombstone with overlapping L2 data, and L2 is the
        // deepest level. The tombstone is pushed down to meet and drop the data.
        let tables = vec![tombstone_table(1, 1, b"a", b"m"), table(2, 2, b"b", b"c")];

        let decision = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds");

        let plan = decision.plan.expect("plan exists");
        assert_eq!(plan.trigger, CompactionTrigger::TombstoneDebt);
        assert_eq!(plan.output_level, TableLevel(2));
        assert_eq!(plan.input_tables, vec![TableId(1), TableId(2)]);
    }

    #[test]
    fn range_tombstone_table_without_lower_overlap_is_left_alone() {
        // L1 range tombstone but no overlapping L2 data: a pure move would only
        // relocate the tombstone, so the picker does not fire tombstone-debt.
        let tables = vec![tombstone_table(1, 1, b"a", b"c"), table(2, 2, b"x", b"z")];

        let decision = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds");

        assert_eq!(decision.plan, None);
    }

    #[test]
    fn range_tombstone_at_deepest_level_does_not_trigger_tombstone_debt() {
        // The deepest populated level is excluded: a tombstone there is already
        // where its covered data lives, so it is left for size/full compaction.
        let tables = vec![table(1, 1, b"a", b"z"), tombstone_table(2, 2, b"a", b"m")];

        let decision = plan_compaction(
            "default",
            &tables,
            &KeyRange::all(),
            Sequence::ZERO,
            options(),
        )
        .expect("planning succeeds");

        assert_eq!(decision.plan, None);
    }

    fn table(id: u64, level: u32, smallest: &[u8], largest: &[u8]) -> CompactionTable {
        table_with_bytes(id, level, smallest, largest, 1)
    }

    fn table_with_bytes(
        id: u64,
        level: u32,
        smallest: &[u8],
        largest: &[u8],
        bytes: u64,
    ) -> CompactionTable {
        CompactionTable {
            id: TableId(id),
            level: TableLevel(level),
            bytes,
            has_range_tombstones: false,
            smallest_user_key: smallest.to_vec(),
            largest_user_key: largest.to_vec(),
        }
    }

    fn tombstone_table(id: u64, level: u32, smallest: &[u8], largest: &[u8]) -> CompactionTable {
        CompactionTable {
            has_range_tombstones: true,
            ..table_with_bytes(id, level, smallest, largest, 1)
        }
    }

    const fn options() -> CompactionOptions {
        CompactionOptions {
            target_table_bytes: 100,
            level_size_multiplier: 10,
            max_l0_files: 8,
            local_l0_compaction: true,
        }
    }
}
