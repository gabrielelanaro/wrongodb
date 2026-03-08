use crate::core::errors::WrongoDBError;
use crate::txn::global_txn::GlobalTxnState;
use crate::txn::snapshot::Snapshot;
use crate::txn::update::Update;
use crate::txn::{Timestamp, TxnId, TXN_NONE};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed { commit_ts: Timestamp },
    Aborted,
}

#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    #[cfg_attr(not(test), allow(dead_code))]
    snapshot: Option<Snapshot>,
    #[cfg_attr(not(test), allow(dead_code))]
    read_ts: Option<Timestamp>,
    state: TxnState,
}

impl Transaction {
    pub(crate) fn new(id: TxnId, snapshot: Snapshot) -> Self {
        Self {
            id,
            snapshot: Some(snapshot),
            read_ts: None,
            state: TxnState::Active,
        }
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    /// Commit the transaction.
    ///
    /// Returns the commit timestamp if successful.
    pub fn commit(&mut self, global: &GlobalTxnState) -> Result<Timestamp, WrongoDBError> {
        match self.state {
            TxnState::Active => {
                // For Phase 2, we use a simple timestamp based on the current txn_id
                // In a full implementation, this would use a global timestamp oracle
                let commit_ts = self.id;

                self.state = TxnState::Committed { commit_ts };

                // Unregister from active transactions
                if self.id != TXN_NONE {
                    global.unregister_active(self.id);
                }

                Ok(commit_ts)
            }
            TxnState::Committed { .. } => Err(WrongoDBError::InvalidTransactionState(
                "transaction already committed".to_string(),
            )),
            TxnState::Aborted => Err(WrongoDBError::InvalidTransactionState(
                "cannot commit aborted transaction".to_string(),
            )),
        }
    }

    /// Abort the transaction and discard all modifications.
    pub fn abort(&mut self, global: &GlobalTxnState) -> Result<(), WrongoDBError> {
        match self.state {
            TxnState::Active => {
                self.state = TxnState::Aborted;

                // Mark as aborted and unregister from active transactions
                if self.id != TXN_NONE {
                    global.mark_aborted(self.id);
                    global.unregister_active(self.id);
                }

                Ok(())
            }
            TxnState::Committed { .. } => Err(WrongoDBError::InvalidTransactionState(
                "cannot abort committed transaction".to_string(),
            )),
            TxnState::Aborted => Err(WrongoDBError::InvalidTransactionState(
                "transaction already aborted".to_string(),
            )),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn can_see(&self, update: &Update) -> bool {
        let visible_txn = match &self.snapshot {
            Some(snapshot) => snapshot.is_visible(update.txn_id),
            None => true,
        };
        if !visible_txn {
            return false;
        }

        let Some(read_ts) = self.read_ts else {
            return true;
        };
        update.time_window.start_ts <= read_ts && read_ts < update.time_window.stop_ts
    }
}
