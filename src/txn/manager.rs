use std::sync::Arc;

use crate::core::errors::WrongoDBError;
use crate::txn::{GlobalTxnState, Timestamp, Transaction, TxnId};

#[derive(Debug)]
pub struct TransactionManager {
    global_txn: Arc<GlobalTxnState>,
}

impl TransactionManager {
    pub fn new(global_txn: Arc<GlobalTxnState>) -> Self {
        Self { global_txn }
    }

    pub fn has_active_transactions(&self) -> bool {
        self.global_txn.has_active_transactions()
    }

    pub fn oldest_active_txn_id(&self) -> TxnId {
        self.global_txn.oldest_active_txn_id()
    }

    pub fn begin_snapshot_txn(&self) -> Transaction {
        self.global_txn.begin_snapshot_txn()
    }

    pub(crate) fn begin_replay_txn(&self, txn_id: TxnId) -> Transaction {
        Transaction::replay(txn_id)
    }

    pub fn commit_txn_state(&self, txn: &mut Transaction) -> Result<Timestamp, WrongoDBError> {
        txn.commit(&self.global_txn)
    }

    pub fn abort_txn_state(&self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        txn.abort(&self.global_txn)
    }
}
