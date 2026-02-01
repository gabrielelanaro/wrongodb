mod core;
mod engine;
mod index;
mod server;
mod storage;
mod txn;

pub mod commands {
    pub use crate::server::commands::*;
}

pub use crate::core::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::engine::{Collection, CollectionTxn, DbStats, IndexInfo, UpdateResult, WrongoDB, WrongoDBConfig};
pub use crate::index::{InMemoryIndex, ScalarKey, SecondaryIndexManager};
pub use crate::server::commands::CommandRegistry;
pub use crate::server::start_server;
pub use crate::storage::block::file::{BlockFile, CheckpointSlot, FileHeader, NONE_BLOCK_ID};
pub use crate::storage::btree::page::{InternalPage, InternalPageError, LeafPage, LeafPageError};
pub use crate::storage::btree::BTree;
pub use crate::txn::{GlobalTxnState, IsolationLevel, NonTransactional, ReadContext, Transaction, TxnId, TxnState};

pub type Document = serde_json::Map<String, serde_json::Value>;
