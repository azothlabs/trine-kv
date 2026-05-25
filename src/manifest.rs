use crate::{options::KeyspaceOptions, table::TableId, types::Sequence};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestEdit {
    CreateKeyspace {
        name: String,
        options: KeyspaceOptions,
    },
    UpdateKeyspaceOptions {
        name: String,
        options: KeyspaceOptions,
    },
    AddTable {
        keyspace: String,
        table_id: TableId,
    },
    RemoveTable {
        keyspace: String,
        table_id: TableId,
    },
    UpdateWalReplayFloor {
        sequence: Sequence,
    },
}
