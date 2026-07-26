use super::{
    BTreeMap, CompactionTrigger, CompactionTriggerStats, NamedCompactionInput,
    NamedCompactionOutput, usize_to_u64_saturating,
};

pub(in crate::db) fn compaction_trigger_stat_deltas(
    inputs: &[NamedCompactionInput],
    outputs: &[NamedCompactionOutput],
) -> Vec<CompactionTriggerStats> {
    let mut triggers = BTreeMap::<CompactionTrigger, CompactionTriggerStats>::new();
    for input in inputs {
        let entry = triggers
            .entry(input.input.trigger)
            .or_insert_with(|| empty_compaction_trigger_stats(input.input.trigger));
        entry.runs = entry.runs.saturating_add(1);
        entry.input_tables = entry
            .input_tables
            .saturating_add(usize_to_u64_saturating(input.input.input_tables.len()));
        entry.input_bytes = entry.input_bytes.saturating_add(
            input
                .input
                .input_tables
                .iter()
                .map(|table| table.estimated_file_bytes())
                .sum::<u64>(),
        );
    }
    for output in outputs {
        let Some(trigger) = output.trigger else {
            continue;
        };
        let entry = triggers
            .entry(trigger)
            .or_insert_with(|| empty_compaction_trigger_stats(trigger));
        entry.output_tables = entry
            .output_tables
            .saturating_add(usize_to_u64_saturating(output.output.tables.len()));
        entry.output_bytes = entry.output_bytes.saturating_add(
            output
                .output
                .tables
                .iter()
                .map(|table| table.estimated_file_bytes())
                .sum::<u64>(),
        );
    }
    triggers.into_values().collect()
}

pub(in crate::db) const fn empty_compaction_trigger_stats(
    trigger: CompactionTrigger,
) -> CompactionTriggerStats {
    CompactionTriggerStats {
        trigger,
        runs: 0,
        input_tables: 0,
        output_tables: 0,
        input_bytes: 0,
        output_bytes: 0,
    }
}
