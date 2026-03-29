mod api;
mod catalog;
mod collection_write_path;
mod core;
mod document_query;
mod index;
mod replication;
mod server;
mod storage;
mod txn;
mod write_ops;

pub use crate::core::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::server::start_server;
pub use crate::storage::api::{
    Connection, ConnectionConfig, CursorEntry, LogSyncMethod, LoggingConfig, Session, TableCursor,
    TransactionSyncConfig,
};

pub(crate) type Document = serde_json::Map<String, serde_json::Value>;
