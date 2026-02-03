use std::path::Path;
use std::sync::Arc;

use crate::index::IndexCatalog;
use crate::storage::btree::BTree;
use crate::storage::wal::{GlobalWal, WalHandle};
use crate::txn::snapshot::Snapshot;
use crate::txn::{GlobalTxnState, TxnId};
use crate::WrongoDBError;

/// A low-level storage table, wrapping a BTree.
///
/// This provides a byte-oriented interface for storage operations.
/// It does not know about BSON or Documents.
#[derive(Debug)]
pub struct Table {
    btree: BTree,
    index_catalog: Option<IndexCatalog>,
    wal: Option<WalHandle>,
}

impl Table {
    pub fn open_or_create_primary<P: AsRef<Path>>(
        collection: &str,
        db_dir: P,
        wal: Option<Arc<GlobalWal>>,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        let db_dir = db_dir.as_ref();
        let path = db_dir.join(format!("{}.main.wt", collection));
        let uri = format!("table:{}", collection);
        let wal_handle = wal.as_ref().map(|wal| wal.handle(&uri));
        let btree = if path.exists() {
            BTree::open(path, global_txn.clone())?
        } else {
            BTree::create(path, 4096, global_txn.clone())?
        };
        let index_catalog = IndexCatalog::load_or_init(collection, db_dir, wal, global_txn)?;
        Ok(Self {
            btree,
            index_catalog: Some(index_catalog),
            wal: wal_handle,
        })
    }

    pub fn open_or_create_index<P: AsRef<Path>>(
        uri: &str,
        path: P,
        wal: Option<Arc<GlobalWal>>,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        let wal_handle = wal.as_ref().map(|wal| wal.handle(uri));
        let btree = if path.as_ref().exists() {
            BTree::open(path, global_txn)?
        } else {
            BTree::create(path, 4096, global_txn)?
        };
        Ok(Self {
            btree,
            index_catalog: None,
            wal: wal_handle,
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
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
        let snapshot = self.btree.checkpoint_snapshot();
        self.checkpoint_with_snapshot(&snapshot)
    }

    pub fn checkpoint_with_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), WrongoDBError> {
        self.btree.checkpoint_with_snapshot(snapshot)?;
        if let Some(catalog) = self.index_catalog.as_mut() {
            catalog.checkpoint_with_snapshot(snapshot)?;
        }
        Ok(())
    }

    pub(crate) fn apply_recovery_put(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.btree.put(key, value)
    }

    pub(crate) fn apply_recovery_delete(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        let _ = self.btree.delete(key)?;
        Ok(())
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
        self.log_put(key, value, txn_id)?;
        self.btree.put_version(key, value, txn_id)?;
        Ok(())
    }

    pub fn update_mvcc(&mut self, key: &[u8], value: &[u8], txn_id: crate::txn::TxnId) -> Result<bool, WrongoDBError> {
        self.log_put(key, value, txn_id)?;
        self.btree.put_version(key, value, txn_id)?;
        Ok(true)
    }

    pub fn delete_mvcc(&mut self, key: &[u8], txn_id: crate::txn::TxnId) -> Result<bool, WrongoDBError> {
        self.log_delete(key, txn_id)?;
        self.btree.delete_version(key, txn_id)?;
        Ok(true)
    }

    pub fn get_version(&mut self, key: &[u8], txn_id: TxnId) -> Result<Option<Vec<u8>>, WrongoDBError> {
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

    fn log_put(&mut self, key: &[u8], value: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        if let Some(wal) = self.wal.as_mut() {
            wal.log_put(key, value, txn_id)?;
        }
        Ok(())
    }

    fn log_delete(&mut self, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        if let Some(wal) = self.wal.as_mut() {
            wal.log_delete(key, txn_id)?;
        }
        Ok(())
    }

}
