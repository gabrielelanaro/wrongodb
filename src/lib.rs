mod connection;
mod core;
mod cursor;
mod datahandle_cache;
mod engine;
mod index;
mod server;
mod session;
mod storage;
mod txn;

pub mod commands {
    pub use crate::server::commands::*;
}

pub use crate::connection::{Connection, ConnectionConfig};
pub use crate::core::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::cursor::{Cursor, CursorKind};
pub use crate::engine::{Collection, DbStats, IndexInfo, UpdateResult, WrongoDB, WrongoDBConfig};
pub use crate::index::{
    decode_index_id, encode_index_key, encode_range_bounds, encode_scalar_prefix, IndexCatalog,
    IndexDefinition, IndexOpRecord, IndexOpType,
};
pub use crate::server::commands::CommandRegistry;
pub use crate::server::start_server;
pub use crate::session::Session;
pub use crate::storage::block::file::{BlockFile, CheckpointSlot, FileHeader, NONE_BLOCK_ID};
pub use crate::storage::btree::page::{InternalPage, InternalPageError, LeafPage, LeafPageError};
pub use crate::storage::btree::BTree;
pub use crate::txn::{
    GlobalTxnState, IsolationLevel, NonTransactional, ReadContext, Transaction, TxnId, TxnState,
};

pub type Document = serde_json::Map<String, serde_json::Value>;
