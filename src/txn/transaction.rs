use std::collections::HashSet;

use crate::core::errors::WrongoDBError;
use crate::txn::global_txn::GlobalTxnState;
use crate::txn::snapshot::Snapshot;
use crate::txn::update::Update;
use crate::txn::{Timestamp, TxnId, TS_NONE, TXN_NONE};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IsolationLevel {
    Snapshot,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed { commit_ts: Timestamp },
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct WriteRef {
    #[allow(dead_code)]
    pub key: Vec<u8>,
    #[allow(dead_code)]
    pub op: WriteOp,
}

#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    #[allow(dead_code)]
    isolation: IsolationLevel,
    snapshot: Option<Snapshot>,
    read_ts: Option<Timestamp>,
    state: TxnState,
    pub(crate) modifications: Vec<WriteRef>,
    /// Tables (URIs) that have been touched in this transaction
    pub(crate) touched_tables: HashSet<String>,
}

impl Transaction {
    pub(crate) fn new(id: TxnId, isolation: IsolationLevel, snapshot: Snapshot) -> Self {
        Self {
            id,
            isolation,
            snapshot: Some(snapshot),
            read_ts: None,
            state: TxnState::Active,
            modifications: Vec::new(),
            touched_tables: HashSet::new(),
        }
    }

    /// Mark a table as touched by this transaction.
    pub fn mark_table_touched(&mut self, uri: &str) {
        self.touched_tables.insert(uri.to_string());
    }

    /// Get the set of tables touched by this transaction.
    pub fn touched_tables(&self) -> &HashSet<String> {
        &self.touched_tables
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub fn state(&self) -> TxnState {
        self.state
    }

    /// Track a write operation for this transaction.
    pub fn track_write(&mut self, key: &[u8], op: WriteOp) {
        self.modifications.push(WriteRef {
            key: key.to_vec(),
            op,
        });
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

                // Clear modifications
                self.modifications.clear();

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

    /// Check if this transaction is committed.
    pub fn is_committed(&self) -> bool {
        matches!(self.state, TxnState::Committed { .. })
    }

    /// Check if this transaction is aborted.
    pub fn is_aborted(&self) -> bool {
        matches!(self.state, TxnState::Aborted)
    }

    pub fn set_read_ts(&mut self, ts: Timestamp) {
        if ts == TS_NONE {
            self.read_ts = None;
        } else {
            self.read_ts = Some(ts);
        }
    }

    pub fn end(&mut self, global: &GlobalTxnState) {
        if self.id != TXN_NONE {
            global.unregister_active(self.id);
        }
    }

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
