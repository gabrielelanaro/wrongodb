use std::path::Path;
use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::index::IndexCatalog;
use crate::storage::btree::BTree;
use crate::txn::{TxnId, TxnManager};
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
    store_name: String,
    txn_manager: Arc<TxnManager>,
    index_catalog: Option<IndexCatalog>,
}

impl Table {
    pub fn open_or_create_primary<P: AsRef<Path>>(
        collection: &str,
        db_dir: P,
        txn_manager: Arc<TxnManager>,
    ) -> Result<Self, WrongoDBError> {
        let db_dir = db_dir.as_ref();
        let path = db_dir.join(format!("{}.main.wt", collection));
        let btree = Self::open_or_create_btree(&path)?;
        let store_name = store_name_from_path(&path)?;
        let index_catalog = IndexCatalog::load_or_init(collection, db_dir, txn_manager.clone())?;
        Ok(Self {
            btree,
            store_name,
            txn_manager,
            index_catalog: Some(index_catalog),
        })
    }

    pub fn open_or_create_index<P: AsRef<Path>>(
        path: P,
        txn_manager: Arc<TxnManager>,
    ) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let btree = Self::open_or_create_btree(path)?;
        let store_name = store_name_from_path(path)?;
        Ok(Self {
            btree,
            store_name,
            txn_manager,
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
        keys.extend(
            self.txn_manager
                .mvcc_keys_in_range(&self.store_name, start_key, end_key),
        );
        keys.sort();
        keys.dedup();

        let mut out = Vec::new();
        for key in keys {
            if let Some(bytes) =
                self.txn_manager
                    .get(&self.store_name, &mut self.btree, &key, txn_id)?
            {
                out.push((key, bytes));
            }
        }

        Ok(out)
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.txn_manager
            .materialize_committed_updates(&self.store_name, &mut self.btree)?;
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
        self.txn_manager
            .put(&self.store_name, &mut self.btree, key, value, txn_id)
    }

    pub fn delete_raw_with_txn(
        &mut self,
        key: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.txn_manager
            .delete(&self.store_name, &mut self.btree, key, txn_id)
    }

    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.btree.sync_all()
    }

    pub fn put_recovery(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.txn_manager.put_recovery(&mut self.btree, key, value)
    }

    pub fn delete_recovery(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        self.txn_manager.delete_recovery(&mut self.btree, key)
    }

    // ==========================================================================
    // MVCC operations
    // ==========================================================================

    pub fn mark_updates_committed(
        &mut self,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.txn_manager
            .mark_updates_committed(&self.store_name, txn_id)
    }

    pub fn mark_updates_aborted(&mut self, txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        self.txn_manager
            .mark_updates_aborted(&self.store_name, txn_id)
    }

    pub fn insert_mvcc(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.txn_manager
            .put(&self.store_name, &mut self.btree, key, value, txn_id)
    }

    pub fn update_mvcc(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.txn_manager
            .put(&self.store_name, &mut self.btree, key, value, txn_id)?;
        Ok(true)
    }

    pub fn delete_mvcc(
        &mut self,
        key: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.txn_manager
            .delete(&self.store_name, &mut self.btree, key, txn_id)?;
        Ok(true)
    }

    pub fn get_version(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.txn_manager
            .get(&self.store_name, &mut self.btree, key, txn_id)
    }

    #[allow(dead_code)]
    pub fn run_gc(&mut self) -> (usize, usize, usize) {
        let (chains, updates, dropped) = self.txn_manager.run_gc_for_store(&self.store_name);
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

    fn open_or_create_btree(path: &Path) -> Result<BTree, WrongoDBError> {
        if path.exists() {
            BTree::open(path)
        } else {
            BTree::create(path, 4096)
        }
    }
}

fn store_name_from_path(path: &Path) -> Result<String, WrongoDBError> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(StorageError(format!(
            "unable to determine store name for path: {}",
            path.display()
        ))
        .into());
    };
    Ok(name.to_string())
}
