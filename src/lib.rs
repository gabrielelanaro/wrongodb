mod api;
mod collection_write_path;
mod core;
mod document_query;
mod index;
mod schema;
mod server;
mod storage;
mod txn;

pub use crate::core::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::server::start_server;
pub use crate::storage::api::{Connection, ConnectionConfig, CursorEntry, Session, TableCursor};

pub(crate) type Document = serde_json::Map<String, serde_json::Value>;
