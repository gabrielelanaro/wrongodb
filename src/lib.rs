mod btree;
mod blockfile;
mod document;
mod engine;
mod errors;
mod index;
mod internal_page;
mod leaf_page;
mod storage;

pub use crate::btree::BTree;
pub use crate::blockfile::{BlockFile, FileHeader, NONE_BLOCK_ID};
pub use crate::engine::WrongoDB;
pub use crate::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::index::{InMemoryIndex, ScalarKey};
pub use crate::internal_page::{InternalPage, InternalPageError};
pub use crate::leaf_page::{LeafPage, LeafPageError};
pub use crate::storage::AppendOnlyStorage;

pub type Document = serde_json::Map<String, serde_json::Value>;
