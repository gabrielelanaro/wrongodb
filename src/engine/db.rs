use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

use super::collection::Collection;

/// Configuration for opening a WrongoDB database.
///
/// # Example
/// ```
/// use wrongodb::WrongoDBConfig;
///
/// let config = WrongoDBConfig::new()
///     .wal_enabled(true)
///     .checkpoint_after_updates(100);
/// ```
#[derive(Debug, Clone)]
pub struct WrongoDBConfig {
    /// Enable WAL for durability (default: true)
    pub wal_enabled: bool,
    /// Automatically checkpoint after N document updates (default: None)
    pub checkpoint_after_updates: Option<usize>,
}

impl Default for WrongoDBConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            checkpoint_after_updates: None,
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

    /// Automatically checkpoint after N document updates.
    ///
    /// When set, the collection will automatically flush all
    /// changes to disk after every N insert/update/delete operations.
    /// This provides durability without manual checkpoint calls.
    pub fn checkpoint_after_updates(mut self, count: usize) -> Self {
        self.checkpoint_after_updates = Some(count);
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
    checkpoint_after_updates: Option<usize>,
    global_txn: Arc<GlobalTxnState>,
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
        let global_txn = Arc::new(GlobalTxnState::new());

        let db = Self {
            base_path,
            collections: HashMap::new(),
            wal_enabled: config.wal_enabled,
            checkpoint_after_updates: config.checkpoint_after_updates,
            global_txn,
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
            let coll = Collection::new(
                &coll_path,
                self.wal_enabled,
                self.checkpoint_after_updates,
                self.global_txn.clone(),
            )?;
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
