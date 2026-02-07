use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::index::IndexCatalog;
use crate::storage::btree::BTree;
use crate::storage::wal::GlobalWal;
use crate::txn::{GlobalTxnState, TxnId};
use crate::WrongoDBError;

type TableEntry = (Vec<u8>, Vec<u8>);
type ScanEntries = Vec<TableEntry>;

/// A low-level storage table, wrapping a BTree.
///
/// This provides a byte-oriented interface for storage operations.
/// It does not know about BSON or Documents.
#[derive(Debug)]
pub struct Table {
    btree: BTree,
    index_catalog: Option<IndexCatalog>,
}

impl Table {
    pub fn open_or_create_primary<P: AsRef<Path>>(
        collection: &str,
        db_dir: P,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
        global_wal: Option<Arc<Mutex<GlobalWal>>>,
    ) -> Result<Self, WrongoDBError> {
        let db_dir = db_dir.as_ref();
        let path = db_dir.join(format!("{}.main.wt", collection));
        let btree = if path.exists() {
            BTree::open_with_global_wal(path, wal_enabled, global_txn.clone(), global_wal.clone())?
        } else {
            BTree::create_with_global_wal(
                path,
                4096,
                wal_enabled,
                global_txn.clone(),
                global_wal.clone(),
            )?
        };
        let index_catalog =
            IndexCatalog::load_or_init(collection, db_dir, wal_enabled, global_txn, global_wal)?;
        Ok(Self {
            btree,
            index_catalog: Some(index_catalog),
        })
    }

    pub fn open_or_create_index<P: AsRef<Path>>(
        path: P,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
        global_wal: Option<Arc<Mutex<GlobalWal>>>,
    ) -> Result<Self, WrongoDBError> {
        let btree = if path.as_ref().exists() {
            BTree::open_with_global_wal(path, wal_enabled, global_txn, global_wal)?
        } else {
            BTree::create_with_global_wal(path, 4096, wal_enabled, global_txn, global_wal)?
        };
        Ok(Self {
            btree,
            index_catalog: None,
        })
    }

    pub fn index_catalog(&self) -> Option<&IndexCatalog> {
        self.index_catalog.as_ref()
    }

    pub fn index_catalog_mut(&mut self) -> Option<&mut IndexCatalog> {
        self.index_catalog.as_mut()
    }

    pub fn scan_range(
        &mut self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        txn_id: TxnId,
    ) -> Result<ScanEntries, WrongoDBError> {
        let entries = self
            .btree
            .range(start_key, end_key)
            .map_err(|e| crate::core::errors::StorageError(format!("table scan failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()?;

        let mut keys: Vec<Vec<u8>> = entries.into_iter().map(|(key, _)| key.to_vec()).collect();
        keys.extend(self.btree.mvcc_keys_in_range(start_key, end_key));
        keys.sort();
        keys.dedup();

        let mut out = Vec::new();
        for key in keys {
            if let Some(bytes) = self.btree.get_version(&key, txn_id)? {
                out.push((key, bytes));
            }
        }

        Ok(out)
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.btree.checkpoint()?;
        if let Some(catalog) = self.index_catalog.as_mut() {
            catalog.checkpoint()?;
        }
        Ok(())
    }

    /// Insert a key/value pair without MVCC (used by index tables).
    pub fn insert_raw(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.insert_raw_with_txn(key, value, crate::txn::TXN_NONE)
    }

    /// Delete a key without MVCC (used by index tables).
    pub fn delete_raw(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        let _ = self.delete_raw_with_txn(key, crate::txn::TXN_NONE)?;
        Ok(())
    }

    pub fn insert_raw_with_txn(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.btree.put_with_txn(key, value, txn_id, true)
    }

    pub fn delete_raw_with_txn(
        &mut self,
        key: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        let deleted = self.btree.delete_with_txn(key, txn_id, true)?;
        Ok(deleted)
    }

    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.btree.sync_all()
    }

    pub fn put_recovery(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.btree
            .put_with_txn(key, value, crate::txn::TXN_NONE, false)?;
        Ok(())
    }

    pub fn delete_recovery(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        let _ = self
            .btree
            .delete_with_txn(key, crate::txn::TXN_NONE, false)?;
        Ok(())
    }

    // ==========================================================================
    // MVCC operations
    // ==========================================================================

    pub fn mark_updates_committed(
        &mut self,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.btree.mark_updates_committed(txn_id)
    }

    pub fn mark_updates_aborted(&mut self, txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        self.btree.mark_updates_aborted(txn_id)
    }

    pub fn insert_mvcc(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.btree.put_version(key, value, txn_id)?;
        Ok(())
    }

    pub fn update_mvcc(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.btree.put_version(key, value, txn_id)?;
        Ok(true)
    }

    pub fn delete_mvcc(
        &mut self,
        key: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.btree.delete_version(key, txn_id)?;
        Ok(true)
    }

    pub fn get_version(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.btree.get_version(key, txn_id)
    }

    #[allow(dead_code)]
    pub fn run_gc(&mut self) -> (usize, usize, usize) {
        let (chains, updates, dropped) = self.btree.run_gc();
        if let Some(catalog) = self.index_catalog.as_mut() {
            let (idx_chains, idx_updates, idx_dropped) = catalog.run_gc();
            return (
                chains + idx_chains,
                updates + idx_updates,
                dropped + idx_dropped,
            );
        }
        (chains, updates, dropped)
    }
}
