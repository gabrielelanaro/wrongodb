use std::path::Path;
use std::sync::Arc;

use crate::storage::btree::BTree;
use crate::txn::{GlobalTxnState, TxnId};
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

    pub fn get(&mut self, key: &[u8], txn_id: TxnId) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.btree.get_version(key, txn_id)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, WrongoDBError> {
        self.btree.delete(key)
    }

    pub fn scan(&mut self, txn_id: TxnId) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        let mut out = Vec::new();

        let entries = self
            .btree
            .range(None, None)
            .map_err(|e| crate::core::errors::StorageError(format!("table scan failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()?;

        for (key, _value) in entries {
            if let Some(bytes) = self.btree.get_version(&key, txn_id)? {
                out.push((key.to_vec(), bytes));
            }
        }

        Ok(out)
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.btree.checkpoint()
    }

    // ==========================================================================
    // MVCC operations
    // ==========================================================================

    pub fn mark_updates_committed(&mut self, txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        self.btree.mark_updates_committed(txn_id)
    }

    pub fn mark_updates_aborted(&mut self, txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        self.btree.mark_updates_aborted(txn_id)
    }

    pub fn insert_mvcc(&mut self, key: &[u8], value: &[u8], txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        self.btree.put_version(key, value, txn_id)?;
        Ok(())
    }

    pub fn update_mvcc(&mut self, key: &[u8], value: &[u8], txn_id: crate::txn::TxnId) -> Result<bool, WrongoDBError> {
        self.btree.put_version(key, value, txn_id)?;
        Ok(true)
    }

    pub fn delete_mvcc(&mut self, key: &[u8], txn_id: crate::txn::TxnId) -> Result<bool, WrongoDBError> {
        self.btree.delete_version(key, txn_id)?;
        Ok(true)
    }

    pub fn get_version(&mut self, key: &[u8], txn_id: TxnId) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.btree.get_version(key, txn_id)
    }

    pub fn scan_from(&mut self, start_key: Option<&[u8]>, txn_id: TxnId) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        let mut out = Vec::new();

        let entries = self
            .btree
            .range(start_key, None)
            .map_err(|e| crate::core::errors::StorageError(format!("table scan failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()?;

        for (key, _value) in entries {
            if let Some(start) = start_key {
                if key.as_slice() == start {
                    continue;
                }
            }

            if let Some(bytes) = self.btree.get_version(&key, txn_id)? {
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
