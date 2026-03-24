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
/// 1. Reconciles MVCC updates and flushes each store
/// 2. Logs the checkpoint in the WAL (if enabled)
/// 3. Truncates the WAL to the checkpoint (if no active transactions)
pub fn run_checkpoint(
    stores: &[Arc<RwLock<BTreeCursor>>],
    global_txn: &GlobalTxnState,
    log_manager: &LogManager,
) -> Result<(), WrongoDBError> {
    for store in stores {
        checkpoint_store(&mut store.write(), global_txn)?;
    }

    if global_txn.has_active_transactions() || !log_manager.is_enabled() {
        return Ok(());
    }

    log_manager.log_checkpoint()?;
    log_manager.truncate_to_checkpoint()
}
