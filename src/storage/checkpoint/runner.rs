use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::btree::BTreeCursor;
use crate::storage::history::HistoryStore;
use crate::storage::log_manager::LogManager;
use crate::storage::reserved_store::{StoreId, METADATA_STORE_ID};
use crate::storage::table::checkpoint_store;
use crate::storage::wal::Lsn;
use crate::txn::{GlobalTxnState, TxnId};
use crate::WrongoDBError;

/// Run a checkpoint across the given stores.
///
/// One checkpoint run uses a single captured WAL boundary for every store.
/// User stores are checkpointed first, `history.wt` second, and `metadata.wt`
/// last so that restart uses `metadata.wt` as the authoritative replay start.
///
/// WAL truncation is optional cleanup only. It is allowed only when:
/// - there were no active transactions when the checkpoint started
/// - no transaction started before the checkpoint finished
/// - the WAL tail still matches the captured checkpoint boundary
pub(in crate::storage) fn run_checkpoint(
    stores: &[(StoreId, Arc<RwLock<BTreeCursor>>)],
    history_store: &mut HistoryStore,
    global_txn: &GlobalTxnState,
    log_manager: &LogManager,
) -> Result<(), WrongoDBError> {
    let checkpoint_window = CheckpointWindow::capture(log_manager, global_txn)?;

    global_txn.begin_checkpoint();
    let checkpoint_result =
        checkpoint_stores_in_order(stores, history_store, global_txn, checkpoint_window.lsn);
    global_txn.end_checkpoint();

    checkpoint_result?;

    if checkpoint_window.allows_wal_truncation(global_txn) {
        let _ = log_manager.truncate_if_tail_matches(checkpoint_window.lsn)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckpointWindow {
    lsn: Lsn,
    had_active_transactions: bool,
    start_txn_id: TxnId,
}

impl CheckpointWindow {
    fn capture(
        log_manager: &LogManager,
        global_txn: &GlobalTxnState,
    ) -> Result<Self, WrongoDBError> {
        Ok(Self {
            lsn: log_manager.capture_checkpoint_boundary()?,
            had_active_transactions: global_txn.has_active_transactions(),
            start_txn_id: global_txn.current_txn_id(),
        })
    }

    fn allows_wal_truncation(&self, global_txn: &GlobalTxnState) -> bool {
        self.lsn.is_valid()
            && !self.had_active_transactions
            && !global_txn.has_active_transactions()
            && global_txn.current_txn_id() == self.start_txn_id
    }
}

fn checkpoint_stores_in_order(
    stores: &[(StoreId, Arc<RwLock<BTreeCursor>>)],
    history_store: &mut HistoryStore,
    global_txn: &GlobalTxnState,
    checkpoint_lsn: Lsn,
) -> Result<(), WrongoDBError> {
    let metadata_store = stores
        .iter()
        .find(|(store_id, _)| *store_id == METADATA_STORE_ID)
        .map(|(_, store)| Arc::clone(store));

    for (store_id, store) in stores {
        if *store_id == METADATA_STORE_ID {
            continue;
        }

        checkpoint_store(
            &mut store.write(),
            global_txn,
            *store_id,
            Some(&mut *history_store),
            checkpoint_lsn,
        )?;
    }

    history_store.checkpoint(global_txn, checkpoint_lsn)?;

    if let Some(metadata_store) = metadata_store {
        // `metadata.wt` is the authoritative replay anchor, so it must publish
        // the checkpoint boundary after every other store has done so.
        checkpoint_store(
            &mut metadata_store.write(),
            global_txn,
            METADATA_STORE_ID,
            None,
            checkpoint_lsn,
        )?;
    }

    Ok(())
}
