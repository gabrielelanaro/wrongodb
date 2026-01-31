use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::WrongoDBError;

use super::collection::Collection;

/// Database statistics
#[derive(Debug, Clone)]
pub struct DbStats {
    pub collection_count: usize,
    pub document_count: usize,
    pub index_count: usize,
}

#[derive(Debug)]
pub struct WrongoDB {
    base_path: PathBuf,
    collections: HashMap<String, Collection>,
    default_index_fields: HashSet<String>,
}

impl WrongoDB {
    pub fn open<P, I, S>(path: P, index_fields: I) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let base_path = path.as_ref().to_path_buf();
        let default_index_fields: HashSet<String> =
            index_fields.into_iter().map(|s| s.into()).collect();

        let db = Self {
            base_path,
            collections: HashMap::new(),
            default_index_fields,
        };

        Ok(db)
    }

    pub fn collection(&mut self, name: &str) -> Result<&mut Collection, WrongoDBError> {
        self.get_or_create_collection(name)
    }

    fn get_or_create_collection(&mut self, name: &str) -> Result<&mut Collection, WrongoDBError> {
        if !self.collections.contains_key(name) {
            let coll_path = PathBuf::from(format!("{}.{}", self.base_path.display(), name));
            let coll = Collection::new(&coll_path, &self.default_index_fields)?;
            self.collections.insert(name.to_string(), coll);
        }
        Ok(self.collections.get_mut(name).unwrap())
    }

    // ========================================================================
    // Metadata operations
    // ========================================================================

    pub fn list_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    /// Checkpoint all id indexes to durable storage.
    ///
    /// This flushes all dirty pages to disk and atomically swaps the root.
    /// After this returns, all previous mutations are durable.
    ///
    /// # Durability semantics
    /// - `checkpoint()` = durability boundary
    /// - Unflushed pages may be lost on crash
    /// - Call after important writes to ensure they survive crashes
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        for coll in self.collections.values_mut() {
            coll.checkpoint()?;
        }
        Ok(())
    }

    /// Request automatic checkpointing after N updates on the id index.
    ///
    /// Once the threshold is reached, `put()` operations will automatically
    /// call `checkpoint()` after the operation completes.
    pub fn request_checkpoint_after_updates(&mut self, count: usize) {
        for coll in self.collections.values_mut() {
            coll.request_checkpoint_after_updates(count);
        }
    }

    pub fn stats(&self) -> DbStats {
        let doc_count: usize = self.collections.values().map(|c| c.doc_count()).sum();
        let index_count: usize = self.collections.values().map(|c| c.index_count()).sum();

        DbStats {
            collection_count: self.collections.len(),
            document_count: doc_count,
            index_count,
        }
    }
}
