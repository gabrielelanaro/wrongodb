use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

use super::collection::Collection;
use super::transaction::MultiCollectionTxn;

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

    /// Open a database with an existing GlobalTxnState for sharing across databases.
    ///
    /// This is primarily useful for tests where multiple collections need to
    /// share the same transaction state.
    pub fn open_with_global_txn<P>(path: P, global_txn: Arc<GlobalTxnState>) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let base_path = path.as_ref().to_path_buf();

        let db = Self {
            base_path,
            collections: HashMap::new(),
            wal_enabled: true,
            checkpoint_after_updates: None,
            global_txn,
        };

        Ok(db)
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

    // ========================================================================
    // Multi-collection transaction API
    // ========================================================================

    /// Begin a multi-collection transaction.
    ///
    /// Returns a `MultiCollectionTxn` that supports atomic operations across
    /// multiple collections with snapshot isolation.
    ///
    /// # Example
    ///
    /// ```
    /// # use wrongodb::WrongoDB;
    /// # use serde_json::json;
    /// # let tmp = tempfile::tempdir().unwrap();
    /// # let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
    /// let mut txn = db.begin_txn().unwrap();
    ///
    /// let doc = txn.insert_one("collection_a", json!({"name": "alice"})).unwrap();
    ///
    /// txn.commit().unwrap();
    /// ```
    pub fn begin_txn(&mut self) -> Result<MultiCollectionTxn<'_>, WrongoDBError> {
        MultiCollectionTxn::new(self)
    }

    /// Run a function in a multi-collection transaction.
    ///
    /// If the function returns `Ok`, the transaction is committed.
    /// If the function returns `Err`, the transaction is aborted.
    ///
    /// # Example
    ///
    /// ```
    /// # use wrongodb::WrongoDB;
    /// # use serde_json::json;
    /// # let tmp = tempfile::tempdir().unwrap();
    /// # let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
    /// let result: Result<_, _> = db.with_txn(|txn| {
    ///     let doc = txn.insert_one("test", json!({"name": "alice"}))?;
    ///     Ok(doc)
    /// });
    /// ```
    pub fn with_txn<F, R>(&mut self, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&mut MultiCollectionTxn<'_>) -> Result<R, WrongoDBError>,
    {
        let mut txn = self.begin_txn()?;
        match f(&mut txn) {
            Ok(result) => {
                txn.commit()?;
                Ok(result)
            }
            Err(e) => {
                let _ = txn.abort();
                Err(e)
            }
        }
    }

    /// Checkpoint all collections.
    ///
    /// This flushes all changes to disk and runs garbage collection
    /// on MVCC update chains for all collections.
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        for coll in self.collections.values_mut() {
            coll.checkpoint()?;
        }
        Ok(())
    }

    // ========================================================================
    // Internal methods for MultiCollectionTxn
    // ========================================================================

    /// Get a reference to the global transaction state.
    pub(crate) fn global_txn(&self) -> &GlobalTxnState {
        &self.global_txn
    }

    /// Check if a collection exists.
    pub(crate) fn has_collection(&self, name: &str) -> bool {
        self.collections.contains_key(name)
    }

    /// Create a new collection without returning it.
    pub(crate) fn create_collection(&mut self, name: &str) -> Result<(), WrongoDBError> {
        let _ = self.collection(name)?;
        Ok(())
    }

    /// Get a mutable reference to a collection by name.
    pub(crate) fn get_collection_mut(
        &mut self,
        name: &str,
    ) -> Result<&mut Collection, WrongoDBError> {
        self.collection(name)
    }
}
