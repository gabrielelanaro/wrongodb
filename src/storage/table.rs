use std::path::Path;

use crate::core::errors::StorageError;
use crate::storage::block::file::NONE_BLOCK_ID;
use crate::storage::btree::BTreeCursor;
use crate::storage::mvcc::ReconcileStats;
use crate::storage::page_store::{BlockFilePageStore, Page, PageStore};
use crate::txn::{ReadVisibility, TransactionManager, TxnId};
use crate::WrongoDBError;

type StoreEntry = (Vec<u8>, Vec<u8>);
type ScanEntries = Vec<StoreEntry>;

/// Logical metadata for one secondary index attached to a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    name: String,
    uri: String,
    columns: Vec<String>,
}

impl IndexMetadata {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(
        name: impl Into<String>,
        uri: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            uri: uri.into(),
            columns,
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

/// Logical metadata for one storage table URI.
///
/// `TableMetadata` is intentionally cheap. It identifies a logical storage
/// table such as `table:users`, along with the logical secondary indexes that
/// should be maintained when rows are written through a table cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    uri: String,
    indexes: Vec<IndexMetadata>,
}

impl TableMetadata {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    #[cfg(test)]
    pub(crate) fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            indexes: Vec::new(),
        }
    }

    pub(crate) fn with_indexes(uri: impl Into<String>, indexes: Vec<IndexMetadata>) -> Self {
        Self {
            uri: uri.into(),
            indexes,
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn indexes(&self) -> &[IndexMetadata] {
        &self.indexes
    }
}

// ============================================================================
// Store Lifecycle
// ============================================================================

pub(crate) fn open_or_create_btree<P: AsRef<Path>>(path: P) -> Result<BTreeCursor, WrongoDBError> {
    let path = path.as_ref();
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

// ============================================================================
// Read Operations
// ============================================================================

pub(crate) fn get_version(
    btree: &mut BTreeCursor,
    key: &[u8],
    txn_id: TxnId,
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let visibility = ReadVisibility::from_txn_id(txn_id);
    btree.get(key, &visibility)
}

pub(crate) fn contains_key(
    btree: &mut BTreeCursor,
    key: &[u8],
    txn_id: TxnId,
) -> Result<bool, WrongoDBError> {
    Ok(get_version(btree, key, txn_id)?.is_some())
}

pub(crate) fn scan_range(
    btree: &mut BTreeCursor,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    txn_id: TxnId,
) -> Result<ScanEntries, WrongoDBError> {
    let visibility = ReadVisibility::from_txn_id(txn_id);
    btree
        .range(start_key, end_key, &visibility)
        .map_err(|e| StorageError(format!("table scan failed: {e}")))?
        .collect::<Result<Vec<_>, _>>()
}

// ============================================================================
// Write Operations
// ============================================================================

#[cfg(test)]
pub(crate) fn apply_put_autocommit(
    uri: &str,
    btree: &mut BTreeCursor,
    transaction_manager: &TransactionManager,
    key: &[u8],
    value: &[u8],
) -> Result<(), WrongoDBError> {
    let mut txn = transaction_manager.begin_snapshot_txn();
    if let Err(err) = btree.put(uri, key, value, &mut txn) {
        let _ = transaction_manager.abort_txn_state(&mut txn);
        return Err(err);
    }
    transaction_manager.commit_txn_state(&mut txn)?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn apply_delete_autocommit(
    uri: &str,
    btree: &mut BTreeCursor,
    transaction_manager: &TransactionManager,
    key: &[u8],
) -> Result<bool, WrongoDBError> {
    let mut txn = transaction_manager.begin_snapshot_txn();
    let deleted = match btree.delete(uri, key, &mut txn) {
        Ok(deleted) => deleted,
        Err(err) => {
            let _ = transaction_manager.abort_txn_state(&mut txn);
            return Err(err);
        }
    };
    transaction_manager.commit_txn_state(&mut txn)?;
    Ok(deleted)
}

// ============================================================================
// Checkpoint
// ============================================================================

pub(crate) fn checkpoint_store(
    btree: &mut BTreeCursor,
    transaction_manager: &TransactionManager,
) -> Result<(), WrongoDBError> {
    let _ = reconcile_for_checkpoint(btree, transaction_manager)?;
    btree.checkpoint()
}

pub(crate) fn reconcile_for_checkpoint(
    btree: &mut BTreeCursor,
    transaction_manager: &TransactionManager,
) -> Result<ReconcileStats, WrongoDBError> {
    btree.reconcile_page_local_updates(
        transaction_manager.oldest_active_txn_id(),
        !transaction_manager.has_active_transactions(),
    )
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
    use crate::txn::{GlobalTxnState, TxnLogOp, TXN_NONE};

    const TEST_URI: &str = "table:test";

    fn open_store(store_name: &str) -> (tempfile::TempDir, Arc<TransactionManager>, BTreeCursor) {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join(store_name);
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let btree = open_or_create_btree(&path).unwrap();
        (tmp, transaction_manager, btree)
    }

    #[test]
    fn local_apply_writes_do_not_depend_on_hooks() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");

        apply_put_autocommit(
            TEST_URI,
            &mut btree,
            transaction_manager.as_ref(),
            b"k1",
            b"v1",
        )
        .unwrap();
        assert_eq!(
            get_version(&mut btree, b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );
        let deleted =
            apply_delete_autocommit(TEST_URI, &mut btree, transaction_manager.as_ref(), b"k1")
                .unwrap();
        assert!(deleted);
        assert_eq!(get_version(&mut btree, b"k1", TXN_NONE).unwrap(), None);
    }

    #[test]
    fn reconcile_materializes_and_drops_current_committed_chain_without_active_transactions() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");

        let mut txn = transaction_manager.begin_snapshot_txn();
        btree.put(TEST_URI, b"k1", b"v1", &mut txn).unwrap();
        transaction_manager.commit_txn_state(&mut txn).unwrap();

        assert_eq!(
            get_version(&mut btree, b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );

        let stats = reconcile_for_checkpoint(&mut btree, transaction_manager.as_ref()).unwrap();
        assert_eq!(
            stats,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 0,
                chains_dropped: 1,
            }
        );
        assert_eq!(
            get_version(&mut btree, b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );

        let second_pass =
            reconcile_for_checkpoint(&mut btree, transaction_manager.as_ref()).unwrap();
        assert_eq!(second_pass, ReconcileStats::default());
    }

    #[test]
    fn reconcile_keeps_old_versions_needed_by_active_transactions() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");

        let mut first_writer = transaction_manager.begin_snapshot_txn();
        btree
            .put(TEST_URI, b"k1", b"v1", &mut first_writer)
            .unwrap();
        transaction_manager
            .commit_txn_state(&mut first_writer)
            .unwrap();

        let mut reader = transaction_manager.begin_snapshot_txn();
        let reader_id = reader.id();

        let mut second_writer = transaction_manager.begin_snapshot_txn();
        btree
            .put(TEST_URI, b"k1", b"v2", &mut second_writer)
            .unwrap();
        transaction_manager
            .commit_txn_state(&mut second_writer)
            .unwrap();

        let stats = reconcile_for_checkpoint(&mut btree, transaction_manager.as_ref()).unwrap();
        assert_eq!(
            stats,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 0,
                chains_dropped: 0,
            }
        );
        assert_eq!(
            get_version(&mut btree, b"k1", reader_id).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            get_version(&mut btree, b"k1", TXN_NONE).unwrap(),
            Some(b"v2".to_vec())
        );

        transaction_manager.commit_txn_state(&mut reader).unwrap();

        let second_pass =
            reconcile_for_checkpoint(&mut btree, transaction_manager.as_ref()).unwrap();
        assert_eq!(
            second_pass,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 1,
                chains_dropped: 1,
            }
        );
        assert_eq!(
            get_version(&mut btree, b"k1", TXN_NONE).unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[test]
    fn reconcile_materializes_deletes_and_drops_tombstone_chains() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");

        apply_put_autocommit(
            TEST_URI,
            &mut btree,
            transaction_manager.as_ref(),
            b"k1",
            b"v1",
        )
        .unwrap();

        let mut txn = transaction_manager.begin_snapshot_txn();
        let deleted = btree.delete(TEST_URI, b"k1", &mut txn).unwrap();
        assert!(deleted);
        transaction_manager.commit_txn_state(&mut txn).unwrap();

        let stats = reconcile_for_checkpoint(&mut btree, transaction_manager.as_ref()).unwrap();
        assert_eq!(
            stats,
            ReconcileStats {
                materialized_entries: 1,
                obsolete_updates_removed: 1,
                chains_dropped: 1,
            }
        );
        assert_eq!(get_version(&mut btree, b"k1", TXN_NONE).unwrap(), None);
    }

    #[test]
    fn scan_range_merges_page_local_inserts_and_slot_updates() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");

        apply_put_autocommit(
            TEST_URI,
            &mut btree,
            transaction_manager.as_ref(),
            b"k1",
            b"v1",
        )
        .unwrap();
        apply_put_autocommit(
            TEST_URI,
            &mut btree,
            transaction_manager.as_ref(),
            b"k3",
            b"v3",
        )
        .unwrap();

        let mut txn = transaction_manager.begin_snapshot_txn();
        btree.put(TEST_URI, b"k2", b"v2", &mut txn).unwrap();
        btree.put(TEST_URI, b"k3", b"v3x", &mut txn).unwrap();

        let entries = scan_range(&mut btree, None, None, txn.id()).unwrap();
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
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");

        apply_put_autocommit(
            TEST_URI,
            &mut btree,
            transaction_manager.as_ref(),
            b"k1",
            b"v1",
        )
        .unwrap();
        apply_put_autocommit(
            TEST_URI,
            &mut btree,
            transaction_manager.as_ref(),
            b"k2",
            b"v2",
        )
        .unwrap();

        let mut txn = transaction_manager.begin_snapshot_txn();
        let deleted = btree.delete(TEST_URI, b"k1", &mut txn).unwrap();
        assert!(deleted);

        let entries = scan_range(&mut btree, None, None, txn.id()).unwrap();
        assert_eq!(entries, vec![(b"k2".to_vec(), b"v2".to_vec())]);
    }

    #[test]
    fn snapshot_write_records_put_log_op() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");
        let mut txn = transaction_manager.begin_snapshot_txn();

        btree.put(TEST_URI, b"k1", b"v1", &mut txn).unwrap();

        assert_eq!(
            txn.log_ops(),
            &[TxnLogOp::Put {
                uri: TEST_URI.to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }]
        );
    }

    #[test]
    fn snapshot_write_records_delete_log_op() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");
        apply_put_autocommit(
            TEST_URI,
            &mut btree,
            transaction_manager.as_ref(),
            b"k1",
            b"v1",
        )
        .unwrap();
        let mut txn = transaction_manager.begin_snapshot_txn();

        let deleted = btree.delete(TEST_URI, b"k1", &mut txn).unwrap();

        assert!(deleted);
        assert_eq!(
            txn.log_ops(),
            &[TxnLogOp::Delete {
                uri: TEST_URI.to_string(),
                key: b"k1".to_vec(),
            }]
        );
    }

    #[test]
    fn replay_write_skips_log_capture() {
        let (_tmp, transaction_manager, mut btree) = open_store("table.idx.wt");
        let mut txn = transaction_manager.begin_replay_txn(42);

        btree.put(TEST_URI, b"k1", b"v1", &mut txn).unwrap();

        assert!(txn.log_ops().is_empty());
    }
}
