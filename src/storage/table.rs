use std::path::Path;
use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::storage::block::file::NONE_BLOCK_ID;
use crate::storage::btree::page::LeafPage;
use crate::storage::btree::BTree;
use crate::storage::page_store::{PageStore, PageStoreTrait};
use crate::txn::{TransactionManager, TxnId};
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
    transaction_manager: Arc<TransactionManager>,
}

impl Table {
    pub fn open_or_create_store<P: AsRef<Path>>(
        path: P,
        transaction_manager: Arc<TransactionManager>,
    ) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let btree = Self::open_or_create_btree(path)?;
        let store_name = store_name_from_path(path)?;
        Ok(Self {
            btree,
            store_name,
            transaction_manager,
        })
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
        keys.extend(self.transaction_manager.mvcc_keys_in_range(
            &self.store_name,
            start_key,
            end_key,
        ));
        keys.sort();
        keys.dedup();

        let mut out = Vec::new();
        for key in keys {
            if let Some(bytes) =
                self.transaction_manager
                    .get(&self.store_name, &mut self.btree, &key, txn_id)?
            {
                out.push((key, bytes));
            }
        }

        Ok(out)
    }

    pub fn checkpoint_store(&mut self) -> Result<(), WrongoDBError> {
        let _ = self
            .transaction_manager
            .reconcile_store_for_checkpoint(&self.store_name, &mut self.btree)?;
        self.btree.checkpoint()
    }

    pub fn local_apply_put_with_txn(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.transaction_manager
            .put(&self.store_name, &mut self.btree, key, value, txn_id)
    }

    pub fn local_apply_delete_with_txn(
        &mut self,
        key: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.transaction_manager
            .delete(&self.store_name, &mut self.btree, key, txn_id)
    }

    pub fn local_mark_updates_committed(
        &mut self,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.transaction_manager
            .mark_updates_committed(&self.store_name, txn_id)
    }

    pub fn local_mark_updates_aborted(
        &mut self,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.transaction_manager
            .mark_updates_aborted(&self.store_name, txn_id)
    }

    pub fn get_version(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.transaction_manager
            .get(&self.store_name, &mut self.btree, key, txn_id)
    }

    pub fn contains_key(&mut self, key: &[u8], txn_id: TxnId) -> Result<bool, WrongoDBError> {
        Ok(self.get_version(key, txn_id)?.is_some())
    }

    fn open_or_create_btree(path: &Path) -> Result<BTree, WrongoDBError> {
        if path.exists() {
            let mut page_store = PageStore::open(path)?;
            init_root_if_missing(&mut page_store)?;
            Ok(BTree::new(Box::new(page_store)))
        } else {
            let mut page_store = PageStore::create(path, 4096)?;
            init_root_if_missing(&mut page_store)?;
            page_store.checkpoint()?;
            Ok(BTree::new(Box::new(page_store)))
        }
    }
}

fn init_root_if_missing(page_store: &mut dyn PageStoreTrait) -> Result<(), WrongoDBError> {
    if page_store.root_page_id() != NONE_BLOCK_ID {
        return Ok(());
    }

    let payload_len = page_store.page_payload_len();
    let mut leaf_bytes = vec![0u8; payload_len];
    LeafPage::init(&mut leaf_bytes)
        .map_err(|e| StorageError(format!("init root leaf failed: {e}")))?;
    let leaf_id = page_store.write_new_page(&leaf_bytes)?;
    page_store.set_root_page_id(leaf_id)?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::storage::mvcc::ReconcileStats;
    use crate::txn::{GlobalTxnState, TXN_NONE};

    fn open_table(store_name: &str) -> (tempfile::TempDir, Arc<TransactionManager>, Table) {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join(store_name);
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table = Table::open_or_create_store(&path, transaction_manager.clone()).unwrap();
        (tmp, transaction_manager, table)
    }

    #[test]
    fn local_apply_writes_do_not_depend_on_hooks() {
        let (_tmp, _transaction_manager, mut table) = open_table("table.idx.wt");

        table
            .local_apply_put_with_txn(b"k1", b"v1", TXN_NONE)
            .unwrap();
        assert_eq!(
            table.get_version(b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );
        let deleted = table.local_apply_delete_with_txn(b"k1", TXN_NONE).unwrap();
        assert!(deleted);
        assert_eq!(table.get_version(b"k1", TXN_NONE).unwrap(), None);
    }

    #[test]
    fn reconcile_materializes_and_drops_current_committed_chain_without_active_transactions() {
        let (_tmp, transaction_manager, mut table) = open_table("table.idx.wt");

        let mut txn = transaction_manager.begin_snapshot_txn();
        let txn_id = txn.id();
        table
            .local_apply_put_with_txn(b"k1", b"v1", txn_id)
            .unwrap();
        table.local_mark_updates_committed(txn_id).unwrap();
        transaction_manager.commit_txn_state(&mut txn).unwrap();

        assert_eq!(table.get_version(b"k1", TXN_NONE).unwrap(), None);

        let stats = transaction_manager
            .reconcile_store_for_checkpoint(&table.store_name, &mut table.btree)
            .unwrap();
        assert_eq!(
            stats,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 0,
                chains_dropped: 1,
            }
        );
        assert_eq!(
            table.get_version(b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );

        let second_pass = transaction_manager
            .reconcile_store_for_checkpoint(&table.store_name, &mut table.btree)
            .unwrap();
        assert_eq!(second_pass, ReconcileStats::default());
    }

    #[test]
    fn reconcile_keeps_old_versions_needed_by_active_transactions() {
        let (_tmp, transaction_manager, mut table) = open_table("table.idx.wt");

        let mut first_writer = transaction_manager.begin_snapshot_txn();
        let first_writer_id = first_writer.id();
        table
            .local_apply_put_with_txn(b"k1", b"v1", first_writer_id)
            .unwrap();
        table.local_mark_updates_committed(first_writer_id).unwrap();
        transaction_manager
            .commit_txn_state(&mut first_writer)
            .unwrap();

        let mut reader = transaction_manager.begin_snapshot_txn();
        let reader_id = reader.id();

        let mut second_writer = transaction_manager.begin_snapshot_txn();
        let second_writer_id = second_writer.id();
        table
            .local_apply_put_with_txn(b"k1", b"v2", second_writer_id)
            .unwrap();
        table
            .local_mark_updates_committed(second_writer_id)
            .unwrap();
        transaction_manager
            .commit_txn_state(&mut second_writer)
            .unwrap();

        let stats = transaction_manager
            .reconcile_store_for_checkpoint(&table.store_name, &mut table.btree)
            .unwrap();
        assert_eq!(
            stats,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 0,
                chains_dropped: 0,
            }
        );
        assert_eq!(
            table.get_version(b"k1", reader_id).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            table.get_version(b"k1", TXN_NONE).unwrap(),
            Some(b"v2".to_vec())
        );

        transaction_manager.commit_txn_state(&mut reader).unwrap();

        let second_pass = transaction_manager
            .reconcile_store_for_checkpoint(&table.store_name, &mut table.btree)
            .unwrap();
        assert_eq!(
            second_pass,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 1,
                chains_dropped: 1,
            }
        );
        assert_eq!(
            table.get_version(b"k1", TXN_NONE).unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[test]
    fn reconcile_materializes_deletes_and_drops_tombstone_chains() {
        let (_tmp, transaction_manager, mut table) = open_table("table.idx.wt");

        table
            .local_apply_put_with_txn(b"k1", b"v1", TXN_NONE)
            .unwrap();

        let mut txn = transaction_manager.begin_snapshot_txn();
        let txn_id = txn.id();
        let deleted = table.local_apply_delete_with_txn(b"k1", txn_id).unwrap();
        assert!(deleted);
        table.local_mark_updates_committed(txn_id).unwrap();
        transaction_manager.commit_txn_state(&mut txn).unwrap();

        let stats = transaction_manager
            .reconcile_store_for_checkpoint(&table.store_name, &mut table.btree)
            .unwrap();
        assert_eq!(
            stats,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 0,
                chains_dropped: 1,
            }
        );
        assert_eq!(table.get_version(b"k1", TXN_NONE).unwrap(), None);
    }
}
