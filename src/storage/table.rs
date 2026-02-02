use std::path::Path;
use std::sync::Arc;

use crate::storage::btree::BTree;
use crate::txn::{GlobalTxnState, ReadContext, Transaction};
use crate::WrongoDBError;

/// A low-level storage table, wrapping a BTree.
///
/// This provides a byte-oriented interface for storage operations.
/// It does not know about BSON or Documents.
#[derive(Debug)]
pub struct Table {
    btree: BTree,
}

impl Table {
    /// Open or create a table by name in given base path.
    #[allow(dead_code)]
    pub fn new(
        base_path: &std::path::Path,
        name: &str,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        let table_path = std::path::PathBuf::from(format!("{}.{}.main.wt", base_path.display(), name));
        Self::open_or_create(table_path, wal_enabled, global_txn)
    }

    pub fn open_or_create<P: AsRef<Path>>(
        path: P,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        let btree = if path.as_ref().exists() {
            BTree::open(path, wal_enabled, global_txn)?
        } else {
            BTree::create(path, 4096, wal_enabled, global_txn)?
        };
        Ok(Self { btree })
    }

    /// Insert a key/value pair.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        if !self.btree.insert_unique(key, value)? {
            // We use DocumentValidationError for duplicate keys to maintain compatibility
            // for now, but we might want a more generic error later.
            return Err(crate::core::errors::DocumentValidationError("duplicate key error".into()).into());
        }
        Ok(())
    }

    /// Update a key/value pair.
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> Result<bool, WrongoDBError> {
        if self.btree.get(key)?.is_none() {
            return Ok(false);
        }
        self.btree.put(key, value)?;
        Ok(true)
    }

    /// Get a value by key within a context.
    pub fn get(&mut self, key: &[u8], ctx: &dyn ReadContext) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.btree.get_ctx(key, ctx)
    }

    /// Delete a key.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, WrongoDBError> {
        self.btree.delete(key)
    }

    /// Scan all key/value pairs visible to the given context.
    pub fn scan(&mut self, ctx: &dyn ReadContext) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        let mut out = Vec::new();

        let entries = self
            .btree
            .range(None, None)
            .map_err(|e| crate::core::errors::StorageError(format!("table scan failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()?;

        for (key, _value) in entries {
            if let Some(bytes) = self.btree.get_ctx(&key, ctx)? {
                out.push((key.to_vec(), bytes));
            }
        }

        Ok(out)
    }

    /// Iterate over key/value pairs in range [start, end).
    #[allow(dead_code)]
    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<crate::storage::btree::BTreeRangeIter<'_>, WrongoDBError> {
        self.btree.range(start, end)
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.btree.checkpoint()
    }

    // ==========================================================================
    // MVCC operations
    // ==========================================================================

    pub fn begin_txn(&self) -> Transaction {
        self.btree.begin_txn(crate::txn::IsolationLevel::Snapshot)
    }

    pub fn commit_txn(&mut self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        self.btree.commit_txn(txn)
    }

    pub fn abort_txn(&mut self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        self.btree.abort_txn(txn)
    }

    pub fn insert_mvcc(&mut self, key: &[u8], value: &[u8], txn: &mut Transaction) -> Result<(), WrongoDBError> {
        if self.btree.get_mvcc(key, txn)?.is_some() {
            return Err(crate::core::errors::DocumentValidationError("duplicate key error".into()).into());
        }
        self.btree.put_mvcc(key, value, txn)?;
        Ok(())
    }

    pub fn update_mvcc(&mut self, key: &[u8], value: &[u8], txn: &mut Transaction) -> Result<bool, WrongoDBError> {
        if self.btree.get_mvcc(key, txn)?.is_none() {
            return Ok(false);
        }
        self.btree.put_mvcc(key, value, txn)?;
        Ok(true)
    }

    pub fn delete_mvcc(&mut self, key: &[u8], txn: &mut Transaction) -> Result<bool, WrongoDBError> {
        if self.btree.get_mvcc(key, txn)?.is_none() {
            return Ok(false);
        }
        self.btree.delete_mvcc(key, txn)?;
        Ok(true)
    }

    pub fn get_mvcc(&mut self, key: &[u8], txn: &Transaction) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.btree.get_mvcc(key, txn)
    }

    /// Scan all key/value pairs visible to the given transaction.
    pub fn scan_txn(&mut self, txn: &Transaction) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        let mut out = Vec::new();

        let entries = self
            .btree
            .range(None, None)
            .map_err(|e| crate::core::errors::StorageError(format!("table scan failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()?;

        for (key, _value) in entries {
            if let Some(bytes) = self.btree.get_mvcc(&key, txn)? {
                out.push((key.to_vec(), bytes));
            }
        }

        Ok(out)
    }

    pub fn run_gc(&mut self) -> (usize, usize, usize) {
        self.btree.run_gc()
    }

    pub fn sync_wal(&mut self) -> Result<(), WrongoDBError> {
        self.btree.sync_wal()
    }
}
