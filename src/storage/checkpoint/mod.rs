//! Checkpoint coordination module.
//!
//! Provides the checkpoint driver that coordinates multi-store checkpoints
//! with MVCC reconciliation.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::btree::BTreeCursor;
use crate::storage::log_manager::LogManager;
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
pub fn run_checkpoint(
    stores: &[Arc<RwLock<BTreeCursor>>],
    global_txn: &GlobalTxnState,
    log_manager: &LogManager,
) -> Result<(), WrongoDBError> {
    // Pin the GC threshold for the reconciliation phase
    global_txn.begin_checkpoint();

    // Reconcile and flush each store
    let reconcile_result: Result<(), WrongoDBError> = stores
        .iter()
        .try_for_each(|store| checkpoint_store(&mut store.write(), global_txn));

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
