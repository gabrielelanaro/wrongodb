use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::WrongoDBError;

use super::collection::Collection;

/// Configuration for opening a WrongoDB database.
///
/// # Example
/// ```
/// use wrongodb::WrongoDBConfig;
///
/// let config = WrongoDBConfig::new()
///     .wal_enabled(true);
/// ```
#[derive(Debug, Clone)]
pub struct WrongoDBConfig {
    /// Enable WAL for durability (default: true)
    pub wal_enabled: bool,
}

impl Default for WrongoDBConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
        }
    }
}

impl WrongoDBConfig {
    /// Create a new config with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable WAL (default: true).
    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }
}

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
    wal_enabled: bool,
}

impl WrongoDB {
    /// Open a database with the given configuration.
    ///
    /// # Example
    /// ```
    /// use wrongodb::{WrongoDB, WrongoDBConfig};
    ///
    /// let config = WrongoDBConfig::new()
    ///     .wal_enabled(true);
    ///
    /// let db = WrongoDB::open_with_config("data.db", config).unwrap();
    /// ```
    pub fn open_with_config<P>(path: P, config: WrongoDBConfig) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let base_path = path.as_ref().to_path_buf();

        let db = Self {
            base_path,
            collections: HashMap::new(),
            wal_enabled: config.wal_enabled,
        };

        Ok(db)
    }

    /// Open a database with default settings.
    ///
    /// WAL is enabled by default.
    pub fn open<P>(path: P) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        Self::open_with_config(path, WrongoDBConfig::default())
    }

    pub fn collection(&mut self, name: &str) -> Result<&mut Collection, WrongoDBError> {
        if !self.collections.contains_key(name) {
            let coll_path = PathBuf::from(format!("{}.{}", self.base_path.display(), name));
            let coll = Collection::new(&coll_path, self.wal_enabled)?;
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
