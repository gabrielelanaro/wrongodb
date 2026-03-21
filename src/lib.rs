mod api;
mod collection_write_path;
mod core;
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

pub use crate::core::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::replication::{RaftMode, RaftPeerConfig};
pub use crate::server::start_server;
pub use crate::storage::api::{
    Connection, ConnectionConfig, Cursor, CursorEntry, Session, WriteUnitOfWork,
};

pub(crate) type Document = serde_json::Map<String, serde_json::Value>;
