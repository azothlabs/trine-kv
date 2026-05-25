use crate::{table::TableId, types::Sequence};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub keyspace: String,
    pub input_tables: Vec<TableId>,
    pub oldest_active_snapshot: Sequence,
}
