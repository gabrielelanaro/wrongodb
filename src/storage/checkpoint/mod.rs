use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::btree::BTreeCursor;
use crate::storage::history::HistoryStore;
use crate::storage::log_manager::LogManager;
use crate::storage::reserved_store::{StoreId, METADATA_STORE_ID};
use crate::storage::table::checkpoint_store;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

/// Run a checkpoint across the given stores.
///
/// This is the main checkpoint driver that:
/// 1. Pins the GC threshold so reconciliation sees a consistent snapshot
/// 2. Reconciles MVCC updates and flushes each store
/// 3. Releases the pinned GC threshold
/// 4. Logs the checkpoint in the WAL and truncates (if no active transactions)
pub(in crate::storage) fn run_checkpoint(
    stores: &[(StoreId, Arc<RwLock<BTreeCursor>>)],
    history_store: &mut HistoryStore,
    global_txn: &GlobalTxnState,
    log_manager: &LogManager,
) -> Result<(), WrongoDBError> {
    // Pin the GC threshold for the reconciliation phase
    global_txn.begin_checkpoint();

    // Reconcile and flush each store
    let reconcile_result = checkpoint_user_stores_then_history(stores, history_store, global_txn);

    // Always release the pin, even if reconciliation failed
    global_txn.end_checkpoint();

    reconcile_result?;

    // WAL operations (don't need the GC pin)
    if !global_txn.has_active_transactions() && log_manager.is_enabled() {
        log_manager.log_checkpoint()?;
        log_manager.truncate_to_checkpoint()?;
    }

    Ok(())
}

/// Reconcile user stores first, then flush the history store that those
/// reconciliations just populated.
fn checkpoint_user_stores_then_history(
    stores: &[(StoreId, Arc<RwLock<BTreeCursor>>)],
    history_store: &mut HistoryStore,
    global_txn: &GlobalTxnState,
) -> Result<(), WrongoDBError> {
    for (store_id, store) in stores {
        // Metadata reconciliation must not write into the history store because
        // metadata rows are not versioned user data.
        let history_store = if *store_id == METADATA_STORE_ID {
            None
        } else {
            Some(&mut *history_store)
        };
        checkpoint_store(&mut store.write(), global_txn, *store_id, history_store)?;
    }

    history_store.checkpoint(global_txn)
}
