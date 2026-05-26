use std::ops::Bound;

use crate::{
    error::{Error, Result},
    table::{TableId, TableLevel, TableProperties},
    types::{KeyRange, Sequence},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub keyspace: String,
    pub input_tables: Vec<TableId>,
    pub output_level: TableLevel,
    pub oldest_active_snapshot: Sequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionTable {
    pub(crate) id: TableId,
    pub(crate) level: TableLevel,
    smallest_user_key: Vec<u8>,
    largest_user_key: Vec<u8>,
}

impl CompactionTable {
    pub(crate) fn from_properties(properties: &TableProperties) -> Self {
        Self {
            id: properties.id,
            level: properties.level,
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
    keyspace: &str,
    tables: &[CompactionTable],
    range: &KeyRange,
    oldest_active_snapshot: Sequence,
) -> Result<Option<CompactionPlan>> {
    let mut input_tables = l0_inputs_with_overlap(tables, range);
    if input_tables.is_empty() {
        input_tables = same_level_inputs(tables, range);
    } else {
        let span = key_span_for_inputs(tables, &input_tables);
        for table in tables {
            if table.level != TableLevel::ZERO
                && span.as_ref().map_or_else(
                    || table.overlaps_range(range),
                    |span| table.overlaps_key_span(span),
                )
                && !input_tables.contains(&table.id)
            {
                input_tables.push(table.id);
            }
        }
    }

    if input_tables.len() < 2 {
        return Ok(None);
    }

    Ok(Some(CompactionPlan {
        keyspace: keyspace.to_owned(),
        output_level: compaction_output_level(tables, &input_tables)?,
        input_tables,
        oldest_active_snapshot,
    }))
}

fn l0_inputs_with_overlap(tables: &[CompactionTable], range: &KeyRange) -> Vec<TableId> {
    let mut inputs = tables
        .iter()
        .filter(|table| table.level == TableLevel::ZERO && table.overlaps_range(range))
        .map(|table| table.id)
        .collect::<Vec<_>>();
    if inputs.is_empty() {
        return inputs;
    }

    // L0 tables may overlap each other. Once one L0 table is selected, include
    // every other L0 table whose key bounds touch the selected L0 span so the
    // replacement can move down without leaving overlapping L0 fragments behind.
    loop {
        let Some(span) = key_span_for_inputs(tables, &inputs) else {
            return inputs;
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
            return inputs;
        }
    }
}

fn same_level_inputs(tables: &[CompactionTable], range: &KeyRange) -> Vec<TableId> {
    let Some(level) = tables
        .iter()
        .filter(|table| table.overlaps_range(range))
        .map(|table| table.level)
        .min()
    else {
        return Vec::new();
    };

    tables
        .iter()
        .filter(|table| table.level == level && table.overlaps_range(range))
        .map(|table| table.id)
        .collect()
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

fn compaction_output_level(
    tables: &[CompactionTable],
    input_tables: &[TableId],
) -> Result<TableLevel> {
    let highest_level = tables
        .iter()
        .filter(|table| input_tables.contains(&table.id))
        .map(|table| table.level)
        .max()
        .unwrap_or(TableLevel::ZERO);
    if highest_level == TableLevel::ZERO {
        highest_level.next().ok_or_else(|| Error::Corruption {
            message: "table level counter overflow".to_owned(),
        })
    } else {
        Ok(highest_level)
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
    use super::{CompactionTable, plan_compaction};
    use crate::{
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
        )
        .expect("planning succeeds")
        .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(2), TableId(3)]);
        assert_eq!(plan.output_level, TableLevel(1));
        assert_eq!(plan.oldest_active_snapshot, Sequence::new(7));
    }

    #[test]
    fn single_l0_with_lower_overlap_is_planned() {
        let tables = vec![table(1, 0, b"a", b"c"), table(2, 1, b"b", b"d")];

        let plan = plan_compaction("default", &tables, &KeyRange::all(), Sequence::ZERO)
            .expect("planning succeeds")
            .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(2)]);
        assert_eq!(plan.output_level, TableLevel(1));
    }

    #[test]
    fn single_l0_without_lower_overlap_is_skipped() {
        let tables = vec![table(1, 0, b"a", b"c"), table(2, 1, b"x", b"z")];

        let plan = plan_compaction(
            "default",
            &tables,
            &KeyRange::half_open(b"a", b"b"),
            Sequence::ZERO,
        )
        .expect("planning succeeds");

        assert!(plan.is_none());
    }

    #[test]
    fn no_l0_plan_uses_shallowest_overlapping_level() {
        let tables = vec![
            table(1, 1, b"a", b"b"),
            table(2, 1, b"c", b"d"),
            table(3, 2, b"a", b"d"),
        ];

        let plan = plan_compaction("default", &tables, &KeyRange::all(), Sequence::ZERO)
            .expect("planning succeeds")
            .expect("plan exists");

        assert_eq!(plan.input_tables, vec![TableId(1), TableId(2)]);
        assert_eq!(plan.output_level, TableLevel(1));
    }

    fn table(id: u64, level: u32, smallest: &[u8], largest: &[u8]) -> CompactionTable {
        CompactionTable {
            id: TableId(id),
            level: TableLevel(level),
            smallest_user_key: smallest.to_vec(),
            largest_user_key: largest.to_vec(),
        }
    }
}
