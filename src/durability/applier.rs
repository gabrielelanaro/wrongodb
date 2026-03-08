use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::durability::{CommittedDurableOp, DurableOp};
use crate::raft::command::CommittedCommand;
use crate::raft::service::CommittedCommandExecutor;
use crate::storage::table_cache::TableCache;
use crate::txn::{Transaction, TransactionManager, TxnId, TXN_NONE};
use crate::WrongoDBError;

#[derive(Debug)]
pub(crate) struct StoreCommandApplier {
    table_cache: Arc<TableCache>,
    transaction_manager: Arc<TransactionManager>,
    in_flight_txns: Mutex<HashMap<TxnId, Transaction>>,
}

impl StoreCommandApplier {
    pub(crate) fn new(
        table_cache: Arc<TableCache>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Self {
        Self {
            table_cache,
            transaction_manager,
            in_flight_txns: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn apply(&self, cmd: CommittedDurableOp) -> Result<(), WrongoDBError> {
        self.apply_op(cmd.op)
    }

    pub(crate) fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError> {
        for table in self.table_cache.all_handles() {
            table.write().checkpoint_store()?;
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
                if txn_id == TXN_NONE {
                    table.write().local_apply_put_autocommit(&key, &value)?;
                } else {
                    let mut txns = self.in_flight_txns.lock();
                    let txn = txns
                        .entry(txn_id)
                        .or_insert_with(|| self.transaction_manager.begin_replay_txn(txn_id));
                    table.write().local_apply_put_in_txn(&key, &value, txn)?;
                }
            }
            DurableOp::Delete {
                store_name,
                key,
                txn_id,
            } => {
                let table = self.table_cache.get_or_open_store(&store_name)?;
                if txn_id == TXN_NONE {
                    let _ = table.write().local_apply_delete_autocommit(&key)?;
                } else {
                    let mut txns = self.in_flight_txns.lock();
                    let txn = txns
                        .entry(txn_id)
                        .or_insert_with(|| self.transaction_manager.begin_replay_txn(txn_id));
                    let _ = table.write().local_apply_delete_in_txn(&key, txn)?;
                }
            }
            DurableOp::TxnCommit { txn_id, .. } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                if let Some(mut txn) = self.in_flight_txns.lock().remove(&txn_id) {
                    self.transaction_manager.commit_txn_state(&mut txn)?;
                }
            }
            DurableOp::TxnAbort { txn_id } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                if let Some(mut txn) = self.in_flight_txns.lock().remove(&txn_id) {
                    self.transaction_manager.abort_txn_state(&mut txn)?;
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
