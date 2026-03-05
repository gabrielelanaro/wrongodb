use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::raft::command::{CommittedCommand, RaftCommand};
use crate::raft::service::CommittedCommandExecutor;
use crate::storage::store_registry::StoreRegistry;
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

#[derive(Debug)]
pub(crate) struct StoreCommandApplier {
    store_registry: Arc<StoreRegistry>,
    touched_stores: Mutex<HashMap<TxnId, HashSet<String>>>,
}

impl StoreCommandApplier {
    pub(crate) fn new(store_registry: Arc<StoreRegistry>) -> Self {
        Self {
            store_registry,
            touched_stores: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn apply(&self, cmd: CommittedCommand) -> Result<(), WrongoDBError> {
        self.execute(cmd)
    }

    pub(crate) fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError> {
        for table in self.store_registry.all_handles() {
            table.write().checkpoint()?;
        }
        Ok(())
    }
}

impl CommittedCommandExecutor for StoreCommandApplier {
    fn execute(&self, cmd: CommittedCommand) -> Result<(), WrongoDBError> {
        match cmd.command {
            RaftCommand::Put {
                store_name,
                key,
                value,
                txn_id,
            } => {
                let table = self.store_registry.resolve_or_open_store(&store_name)?;
                table
                    .write()
                    .local_apply_put_with_txn(&key, &value, txn_id)?;
                if txn_id != TXN_NONE {
                    let mut touched = self.touched_stores.lock();
                    touched.entry(txn_id).or_default().insert(store_name);
                }
            }
            RaftCommand::Delete {
                store_name,
                key,
                txn_id,
            } => {
                let table = self.store_registry.resolve_or_open_store(&store_name)?;
                let _ = table.write().local_apply_delete_with_txn(&key, txn_id)?;
                if txn_id != TXN_NONE {
                    let mut touched = self.touched_stores.lock();
                    touched.entry(txn_id).or_default().insert(store_name);
                }
            }
            RaftCommand::TxnCommit { txn_id, .. } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                let touched = self
                    .touched_stores
                    .lock()
                    .remove(&txn_id)
                    .unwrap_or_default();
                for store_name in touched {
                    let table = self.store_registry.resolve_or_open_store(&store_name)?;
                    table.write().local_mark_updates_committed(txn_id)?;
                }
            }
            RaftCommand::TxnAbort { txn_id } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                let touched = self
                    .touched_stores
                    .lock()
                    .remove(&txn_id)
                    .unwrap_or_default();
                for store_name in touched {
                    let table = self.store_registry.resolve_or_open_store(&store_name)?;
                    table.write().local_mark_updates_aborted(txn_id)?;
                }
            }
            RaftCommand::Checkpoint => {}
        }
        Ok(())
    }
}
