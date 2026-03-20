mod api;
mod collection_write_path;
mod core;
mod database_context;
mod document_query;
mod durability;
mod index;
mod raft;
mod recovery;
mod replication;
mod schema;
mod server;
mod storage;
mod store_write_path;
mod txn;

pub use crate::api::{
    Connection, ConnectionConfig, Cursor, CursorEntry, RaftMode, RaftPeerConfig, Session,
    WriteUnitOfWork,
};
pub use crate::core::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::server::start_server;

pub(crate) type Document = serde_json::Map<String, serde_json::Value>;
