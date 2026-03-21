use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::durability::{CommittedDurableOp, DurableOp};
use crate::raft::command::CommittedCommand;
use crate::raft::service::CommittedCommandExecutor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::table::Table;
use crate::txn::{Transaction, TransactionManager, TxnId, TXN_NONE};
use crate::WrongoDBError;

/// Applies committed durable commands and finalizes recovery state.
///
/// Recovery depends on this narrow interface so replay logic can be tested
/// independently from the concrete store implementation.
pub(crate) trait CommandApplier: Send + Sync {
    fn apply(&self, cmd: CommittedDurableOp) -> Result<(), WrongoDBError>;
    fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError>;
}

#[derive(Debug)]
pub(crate) struct StoreCommandApplier {
    base_path: PathBuf,
    table_handles: Arc<HandleCache<String, RwLock<Table>>>,
    transaction_manager: Arc<TransactionManager>,
    in_flight_txns: Mutex<HashMap<TxnId, Transaction>>,
}

impl StoreCommandApplier {
    pub(crate) fn new(
        base_path: PathBuf,
        table_handles: Arc<HandleCache<String, RwLock<Table>>>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Self {
        Self {
            base_path,
            table_handles,
            transaction_manager,
            in_flight_txns: Mutex::new(HashMap::new()),
        }
    }

    fn apply_op(&self, op: DurableOp) -> Result<(), WrongoDBError> {
        match op {
            DurableOp::Put {
                store_name,
                key,
                value,
                txn_id,
            } => {
                let table =
                    self.table_handles
                        .get_or_try_insert_with(store_name, |store_name| {
                            let path = self.base_path.join(store_name);
                            Ok(RwLock::new(Table::open_or_create_store(
                                path,
                                self.transaction_manager.clone(),
                            )?))
                        })?;
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
                let table =
                    self.table_handles
                        .get_or_try_insert_with(store_name, |store_name| {
                            let path = self.base_path.join(store_name);
                            Ok(RwLock::new(Table::open_or_create_store(
                                path,
                                self.transaction_manager.clone(),
                            )?))
                        })?;
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

impl CommandApplier for StoreCommandApplier {
    fn apply(&self, cmd: CommittedDurableOp) -> Result<(), WrongoDBError> {
        self.apply_op(cmd.op)
    }

    fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError> {
        for table in self.table_handles.all_handles() {
            table.write().checkpoint_store()?;
        }
        Ok(())
    }
}

impl CommittedCommandExecutor for StoreCommandApplier {
    fn execute(&self, cmd: CommittedCommand) -> Result<(), WrongoDBError> {
        self.apply(cmd.into())
    }
}
