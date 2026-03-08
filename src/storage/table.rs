use std::path::Path;
use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::storage::block::file::NONE_BLOCK_ID;
use crate::storage::btree::BTreeCursor;
use crate::storage::mvcc::ReconcileStats;
use crate::storage::page_store::{BlockFilePageStore, Page, PageStore};
use crate::txn::{ReadVisibility, Transaction, TransactionManager, TxnId, TxnOp, WriteContext};
use crate::WrongoDBError;

type TableEntry = (Vec<u8>, Vec<u8>);
type ScanEntries = Vec<TableEntry>;

/// A low-level storage table, wrapping a BTreeCursor.
///
/// This provides a byte-oriented interface for storage operations.
/// It does not know about BSON or Documents.
#[derive(Debug)]
pub struct Table {
    btree: BTreeCursor,
    transaction_manager: Arc<TransactionManager>,
}

impl Table {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn open_or_create_store<P: AsRef<Path>>(
        path: P,
        transaction_manager: Arc<TransactionManager>,
    ) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let btree = Self::open_or_create_btree(path)?;
        Ok(Self {
            btree,
            transaction_manager,
        })
    }

    // ------------------------------------------------------------------------
    // Read Operations
    // ------------------------------------------------------------------------

    pub fn get_version(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let visibility = ReadVisibility::from_txn_id(txn_id);
        self.btree.get(key, &visibility)
    }

    pub fn contains_key(&mut self, key: &[u8], txn_id: TxnId) -> Result<bool, WrongoDBError> {
        Ok(self.get_version(key, txn_id)?.is_some())
    }

    pub fn scan_range(
        &mut self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        txn_id: TxnId,
    ) -> Result<ScanEntries, WrongoDBError> {
        let visibility = ReadVisibility::from_txn_id(txn_id);
        self.btree
            .range(start_key, end_key, &visibility)
            .map_err(|e| StorageError(format!("table scan failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()
    }

    // ------------------------------------------------------------------------
    // Write Operations
    // ------------------------------------------------------------------------

    pub fn local_apply_put_with_txn(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.btree
            .put(key, value, &WriteContext::from_txn_id(txn_id))
    }

    pub fn local_apply_delete_with_txn(
        &mut self,
        key: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.btree.delete(key, &WriteContext::from_txn_id(txn_id))
    }

    pub fn local_apply_put_in_txn(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        let write_context = WriteContext::from_txn_id(txn.id());
        let update_ref = self.btree.put_with_update_ref(key, value, &write_context)?;
        txn.record_op(TxnOp::PageUpdate(update_ref));
        Ok(())
    }

    pub fn local_apply_delete_in_txn(
        &mut self,
        key: &[u8],
        txn: &mut Transaction,
    ) -> Result<bool, WrongoDBError> {
        let write_context = WriteContext::from_txn_id(txn.id());
        let update_ref = self.btree.delete_with_update_ref(key, &write_context)?;
        txn.record_op(TxnOp::PageUpdate(update_ref));
        Ok(true)
    }

    // ------------------------------------------------------------------------
    // Transaction Lifecycle
    // ------------------------------------------------------------------------

    pub fn local_mark_updates_committed(
        &mut self,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.btree.mark_updates_committed(txn_id)
    }

    pub fn local_mark_updates_aborted(
        &mut self,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        self.btree.mark_updates_aborted(txn_id)
    }

    // ------------------------------------------------------------------------
    // Checkpoint
    // ------------------------------------------------------------------------

    pub fn checkpoint_store(&mut self) -> Result<(), WrongoDBError> {
        let _ = self.reconcile_for_checkpoint()?;
        self.btree.checkpoint()
    }

    pub(crate) fn reconcile_for_checkpoint(&mut self) -> Result<ReconcileStats, WrongoDBError> {
        self.btree.reconcile_page_local_updates(
            self.transaction_manager.oldest_active_txn_id(),
            !self.transaction_manager.has_active_transactions(),
        )
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn open_or_create_btree(path: &Path) -> Result<BTreeCursor, WrongoDBError> {
        if path.exists() {
            let mut page_store = BlockFilePageStore::open(path)?;
            init_root_if_missing(&mut page_store)?;
            Ok(BTreeCursor::new(Box::new(page_store)))
        } else {
            let mut page_store = BlockFilePageStore::create(path, 4096)?;
            init_root_if_missing(&mut page_store)?;
            page_store.checkpoint()?;
            Ok(BTreeCursor::new(Box::new(page_store)))
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn init_root_if_missing(page_store: &mut dyn PageStore) -> Result<(), WrongoDBError> {
    if page_store.root_page_id() != NONE_BLOCK_ID {
        return Ok(());
    }

    let payload_len = page_store.page_payload_len();
    let leaf = Page::new_leaf(payload_len)
        .map_err(|e| StorageError(format!("init root leaf failed: {e}")))?;
    let leaf_id = page_store.write_new_page(leaf)?;
    page_store.set_root_page_id(leaf_id)?;
    Ok(())
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

        let stats = table.reconcile_for_checkpoint().unwrap();
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

        let second_pass = table.reconcile_for_checkpoint().unwrap();
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

        let stats = table.reconcile_for_checkpoint().unwrap();
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

        let second_pass = table.reconcile_for_checkpoint().unwrap();
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

        let stats = table.reconcile_for_checkpoint().unwrap();
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

    #[test]
    fn scan_range_merges_page_local_inserts_and_slot_updates() {
        let (_tmp, transaction_manager, mut table) = open_table("table.idx.wt");

        table
            .local_apply_put_with_txn(b"k1", b"v1", TXN_NONE)
            .unwrap();
        table
            .local_apply_put_with_txn(b"k3", b"v3", TXN_NONE)
            .unwrap();

        let mut txn = transaction_manager.begin_snapshot_txn();
        table
            .local_apply_put_in_txn(b"k2", b"v2", &mut txn)
            .unwrap();
        table
            .local_apply_put_in_txn(b"k3", b"v3x", &mut txn)
            .unwrap();

        let entries = table.scan_range(None, None, txn.id()).unwrap();
        assert_eq!(
            entries,
            vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
                (b"k3".to_vec(), b"v3x".to_vec()),
            ]
        );
    }

    #[test]
    fn scan_range_skips_page_local_tombstones() {
        let (_tmp, transaction_manager, mut table) = open_table("table.idx.wt");

        table
            .local_apply_put_with_txn(b"k1", b"v1", TXN_NONE)
            .unwrap();
        table
            .local_apply_put_with_txn(b"k2", b"v2", TXN_NONE)
            .unwrap();

        let mut txn = transaction_manager.begin_snapshot_txn();
        let deleted = table.local_apply_delete_in_txn(b"k1", &mut txn).unwrap();
        assert!(deleted);

        let entries = table.scan_range(None, None, txn.id()).unwrap();
        assert_eq!(entries, vec![(b"k2".to_vec(), b"v2".to_vec())]);
    }
}
