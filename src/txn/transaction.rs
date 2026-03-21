use crate::core::errors::WrongoDBError;
use crate::txn::global_txn::GlobalTxnState;
use crate::txn::snapshot::Snapshot;
use crate::txn::update::{Update, UpdateRef};
use crate::txn::{Timestamp, TxnId, TXN_NONE};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum TransactionKind {
    Snapshot,
    Replay,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum TransactionLogMode {
    Capture,
    Skip,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed { commit_ts: Timestamp },
    Aborted,
}

#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub enum TxnPageOp {
    PageUpdate(UpdateRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnLogOp {
    Put {
        uri: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        uri: String,
        key: Vec<u8>,
    },
}

#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    #[cfg_attr(not(test), allow(dead_code))]
    snapshot: Option<Snapshot>,
    #[cfg_attr(not(test), allow(dead_code))]
    read_ts: Option<Timestamp>,
    kind: TransactionKind,
    log_mode: TransactionLogMode,
    state: TxnState,
    page_ops: Vec<TxnPageOp>,
    log_ops: Vec<TxnLogOp>,
}

impl Transaction {
    pub(crate) fn new(id: TxnId, snapshot: Snapshot) -> Self {
        Self {
            id,
            snapshot: Some(snapshot),
            read_ts: None,
            kind: TransactionKind::Snapshot,
            log_mode: TransactionLogMode::Capture,
            state: TxnState::Active,
            page_ops: Vec::new(),
            log_ops: Vec::new(),
        }
    }

    pub(crate) fn replay(id: TxnId) -> Self {
        Self {
            id,
            snapshot: None,
            read_ts: None,
            kind: TransactionKind::Replay,
            log_mode: TransactionLogMode::Skip,
            state: TxnState::Active,
            page_ops: Vec::new(),
            log_ops: Vec::new(),
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
                let commit_ts = self.id;

                self.state = TxnState::Committed { commit_ts };

                if self.kind == TransactionKind::Snapshot && self.id != TXN_NONE {
                    global.unregister_active(self.id);
                }

                self.mark_ops_committed(commit_ts);

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

                if self.kind == TransactionKind::Snapshot && self.id != TXN_NONE {
                    global.mark_aborted(self.id);
                    global.unregister_active(self.id);
                }

                self.mark_ops_aborted();

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
    pub fn record_page_op(&mut self, op: TxnPageOp) {
        self.page_ops.push(op);
    }

    pub(crate) fn record_put_log(&mut self, uri: &str, key: &[u8], value: &[u8]) {
        self.record_log_op(TxnLogOp::Put {
            uri: uri.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    pub(crate) fn record_delete_log(&mut self, uri: &str, key: &[u8]) {
        self.record_log_op(TxnLogOp::Delete {
            uri: uri.to_string(),
            key: key.to_vec(),
        });
    }

    pub fn log_ops(&self) -> &[TxnLogOp] {
        &self.log_ops
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

    fn mark_ops_committed(&mut self, commit_ts: Timestamp) {
        for op in self.page_ops.drain(..) {
            match op {
                TxnPageOp::PageUpdate(update_ref) => update_ref.write().mark_committed(commit_ts),
            }
        }
        self.log_ops.clear();
    }

    fn mark_ops_aborted(&mut self) {
        for op in self.page_ops.drain(..).rev() {
            match op {
                TxnPageOp::PageUpdate(update_ref) => abort_update_ref(&update_ref),
            }
        }
        self.log_ops.clear();
    }

    fn record_log_op(&mut self, op: TxnLogOp) {
        if self.log_mode == TransactionLogMode::Capture {
            self.log_ops.push(op);
        }
    }
}

fn abort_update_ref(update_ref: &UpdateRef) {
    let (txn_id, next) = {
        let mut update = update_ref.write();
        update.mark_aborted();
        (update.txn_id, update.next.clone())
    };

    let Some(next_ref) = next else {
        return;
    };

    let mut next = next_ref.write();
    if next.time_window.stop_txn == txn_id && next.time_window.stop_ts == txn_id {
        next.restore_current();
    }
}
