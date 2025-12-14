mod blockfile;
mod document;
mod engine;
mod errors;
mod index;
mod leaf_page;
mod storage;

pub use crate::blockfile::{BlockFile, FileHeader};
pub use crate::engine::{MiniMongo, WrongoDB};
pub use crate::errors::{DocumentValidationError, StorageError, WrongoDBError};
pub use crate::index::{InMemoryIndex, ScalarKey};
pub use crate::leaf_page::{LeafPage, LeafPageError};
pub use crate::storage::AppendOnlyStorage;

pub type Document = serde_json::Map<String, serde_json::Value>;
