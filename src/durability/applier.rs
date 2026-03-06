use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::durability::{CommittedDurableOp, DurableOp};
use crate::raft::command::CommittedCommand;
use crate::raft::service::CommittedCommandExecutor;
use crate::storage::table_cache::TableCache;
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

#[derive(Debug)]
pub(crate) struct StoreCommandApplier {
    table_cache: Arc<TableCache>,
    touched_stores: Mutex<HashMap<TxnId, HashSet<String>>>,
}

impl StoreCommandApplier {
    pub(crate) fn new(table_cache: Arc<TableCache>) -> Self {
        Self {
            table_cache,
            touched_stores: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn apply(&self, cmd: CommittedDurableOp) -> Result<(), WrongoDBError> {
        self.apply_op(cmd.op)
    }

    pub(crate) fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError> {
        for table in self.table_cache.all_handles() {
            table.write().checkpoint()?;
        }
        Ok(())
    }

    fn apply_op(&self, op: DurableOp) -> Result<(), WrongoDBError> {
        match op {
            DurableOp::Put {
                store_name,
                key,
                value,
                txn_id,
            } => {
                let table = self.table_cache.get_or_open_store(&store_name)?;
                table
                    .write()
                    .local_apply_put_with_txn(&key, &value, txn_id)?;
                if txn_id != TXN_NONE {
                    let mut touched = self.touched_stores.lock();
                    touched.entry(txn_id).or_default().insert(store_name);
                }
            }
            DurableOp::Delete {
                store_name,
                key,
                txn_id,
            } => {
                let table = self.table_cache.get_or_open_store(&store_name)?;
                let _ = table.write().local_apply_delete_with_txn(&key, txn_id)?;
                if txn_id != TXN_NONE {
                    let mut touched = self.touched_stores.lock();
                    touched.entry(txn_id).or_default().insert(store_name);
                }
            }
            DurableOp::TxnCommit { txn_id, .. } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                let touched = self
                    .touched_stores
                    .lock()
                    .remove(&txn_id)
                    .unwrap_or_default();
                for store_name in touched {
                    let table = self.table_cache.get_or_open_store(&store_name)?;
                    table.write().local_mark_updates_committed(txn_id)?;
                }
            }
            DurableOp::TxnAbort { txn_id } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                let touched = self
                    .touched_stores
                    .lock()
                    .remove(&txn_id)
                    .unwrap_or_default();
                for store_name in touched {
                    let table = self.table_cache.get_or_open_store(&store_name)?;
                    table.write().local_mark_updates_aborted(txn_id)?;
                }
            }
            DurableOp::Checkpoint => {}
        }
        Ok(())
    }
}

impl CommittedCommandExecutor for StoreCommandApplier {
    fn execute(&self, cmd: CommittedCommand) -> Result<(), WrongoDBError> {
        self.apply(cmd.into())
    }
}
