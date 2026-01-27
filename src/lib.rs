mod blockfile;
mod btree;
pub mod commands;
mod document;
mod engine;
mod errors;
mod index;
mod index_key;
mod internal_page;
mod leaf_page;
mod server;
mod storage;

pub use crate::blockfile::{BlockFile, CheckpointSlot, FileHeader, NONE_BLOCK_ID};
pub use crate::btree::BTree;
pub use crate::commands::CommandRegistry;
pub use crate::engine::{DbStats, IndexInfo, UpdateResult, WrongoDB};
pub use crate::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::index::{InMemoryIndex, ScalarKey, SecondaryIndexManager};
pub use crate::internal_page::{InternalPage, InternalPageError};
pub use crate::leaf_page::{LeafPage, LeafPageError};
pub use crate::server::start_server;
pub use crate::storage::AppendOnlyStorage;

pub type Document = serde_json::Map<String, serde_json::Value>;
