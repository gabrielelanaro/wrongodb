//! Transaction table support for WAL replay.

use std::collections::HashSet;

use crate::storage::wal::WalRecord;
use crate::txn::{TxnId, TXN_NONE};

/// Transaction table built during WAL recovery.
///
/// Tracks which transactions were committed, aborted, or left pending (incomplete).
/// During recovery, only operations from committed transactions are applied.
#[derive(Debug, Clone)]
pub struct RecoveryTxnTable {
    committed: HashSet<TxnId>,
    aborted: HashSet<TxnId>,
    pending: HashSet<TxnId>,
}

impl RecoveryTxnTable {
    pub fn new() -> Self {
        Self {
            committed: HashSet::new(),
            aborted: HashSet::new(),
            pending: HashSet::new(),
        }
    }

    pub fn process_record(&mut self, record: &WalRecord) {
        match record {
            WalRecord::TxnCommit { txn_id, .. } => {
                self.pending.remove(txn_id);
                self.committed.insert(*txn_id);
            }
            WalRecord::TxnAbort { txn_id } => {
                self.pending.remove(txn_id);
                self.aborted.insert(*txn_id);
            }
            WalRecord::Put { txn_id, .. } | WalRecord::Delete { txn_id, .. } => {
                if *txn_id != TXN_NONE && !self.is_finalized(*txn_id) {
                    self.pending.insert(*txn_id);
                }
            }
            WalRecord::Checkpoint => {}
        }
    }

    fn is_finalized(&self, txn_id: TxnId) -> bool {
        self.committed.contains(&txn_id) || self.aborted.contains(&txn_id)
    }

    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.committed.contains(&txn_id)
    }

    pub fn should_apply(&self, record: &WalRecord) -> bool {
        let txn_id = match record {
            WalRecord::Put { txn_id, .. } => *txn_id,
            WalRecord::Delete { txn_id, .. } => *txn_id,
            WalRecord::TxnCommit { .. } | WalRecord::TxnAbort { .. } | WalRecord::Checkpoint => {
                return false;
            }
        };

        if txn_id == TXN_NONE {
            return true;
        }

        self.is_committed(txn_id)
    }

    pub fn finalize_pending(&mut self) {
        for txn_id in self.pending.drain() {
            self.aborted.insert(txn_id);
        }
    }
}

impl Default for RecoveryTxnTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_record(txn_id: TxnId, key: &[u8], value: &[u8]) -> WalRecord {
        WalRecord::Put {
            store_name: "test.main.wt".to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
            txn_id,
        }
    }

    #[test]
    fn commit_record_marks_txn_committed() {
        let mut table = RecoveryTxnTable::new();
        table.process_record(&put_record(42, b"key", b"value"));
        table.process_record(&WalRecord::TxnCommit {
            txn_id: 42,
            commit_ts: 100,
        });

        assert!(table.is_committed(42));
        assert!(table.should_apply(&put_record(42, b"key", b"value")));
    }

    #[test]
    fn finalize_pending_marks_txn_aborted() {
        let mut table = RecoveryTxnTable::new();
        table.process_record(&put_record(42, b"key", b"value"));
        table.finalize_pending();

        assert!(!table.should_apply(&put_record(42, b"key", b"value")));
    }

    #[test]
    fn aborted_transactions_are_not_applied() {
        let mut table = RecoveryTxnTable::new();
        table.process_record(&put_record(99, b"key", b"value"));
        table.process_record(&WalRecord::TxnAbort { txn_id: 99 });

        assert!(!table.should_apply(&put_record(99, b"key", b"value")));
    }

    #[test]
    fn non_transactional_records_are_applied() {
        let table = RecoveryTxnTable::new();
        assert!(table.should_apply(&put_record(TXN_NONE, b"key", b"value")));
    }
}
