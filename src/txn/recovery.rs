//! Transaction recovery support.
//!
//! Provides the `RecoveryTxnTable` for tracking transaction states during WAL replay.
//! This is used during crash recovery to determine which operations should be applied.

use std::collections::HashSet;

use crate::storage::btree::wal::WalRecord;
use crate::txn::{TxnId, TXN_NONE};

/// Transaction table built during WAL recovery.
///
/// Tracks which transactions were committed, aborted, or left pending (incomplete).
/// During recovery, only operations from committed transactions are applied.
///
/// This follows the Write-Ahead Logging protocol:
/// - TxnCommit record: transaction is committed
/// - TxnAbort record: transaction is aborted
/// - No commit/abort record: transaction is treated as aborted (presumed abort)
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RecoveryTxnTable {
    committed: HashSet<TxnId>,
    aborted: HashSet<TxnId>,
    /// Transactions that have started (seen operations) but not yet committed/aborted.
    /// These are considered pending until the end of recovery.
    pending: HashSet<TxnId>,
}

#[allow(dead_code)]
impl RecoveryTxnTable {
    /// Create a new empty transaction table.
    pub fn new() -> Self {
        Self {
            committed: HashSet::new(),
            aborted: HashSet::new(),
            pending: HashSet::new(),
        }
    }

    /// Process a WAL record and update the transaction table.
    ///
    /// This should be called for every record in the WAL during the first pass
    /// of recovery to build the complete transaction state.
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
                // Track transactions that have operations but no commit/abort yet
                if *txn_id != TXN_NONE && !self.is_finalized(*txn_id) {
                    self.pending.insert(*txn_id);
                }
            }
            WalRecord::Checkpoint { .. } => {
                // Checkpoints don't affect transaction state
            }
        }
    }

    /// Check if a transaction ID is in a finalized state (committed or aborted).
    fn is_finalized(&self, txn_id: TxnId) -> bool {
        self.committed.contains(&txn_id) || self.aborted.contains(&txn_id)
    }

    /// Check if a transaction was committed.
    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.committed.contains(&txn_id)
    }

    /// Check if a transaction was aborted.
    pub fn is_aborted(&self, txn_id: TxnId) -> bool {
        self.aborted.contains(&txn_id)
    }

    /// Check if a transaction is pending (has operations but no commit/abort record).
    pub fn is_pending(&self, txn_id: TxnId) -> bool {
        self.pending.contains(&txn_id)
    }

    /// Returns true if the transaction should be treated as aborted.
    ///
    /// A transaction is considered aborted if:
    /// - It has an explicit abort record
    /// - It is pending at the end of recovery (no commit seen)
    /// - It is unknown (never seen during recovery) - treated as aborted for safety
    pub fn should_treat_as_aborted(&self, txn_id: TxnId) -> bool {
        if txn_id == TXN_NONE {
            return false; // Non-transactional operations are never aborted
        }
        // A transaction is treated as aborted if:
        // 1. It has an explicit abort record, OR
        // 2. It is pending (no commit/abort seen), OR
        // 3. It is unknown (not committed, not in our tracking at all)
        !self.is_committed(txn_id)
    }

    /// Determine if a record should be applied during recovery.
    ///
    /// Returns `true` if the operation should be replayed:
    /// - Non-transactional operations (txn_id = TXN_NONE): always apply
    /// - Committed transactions: apply
    /// - Aborted or pending transactions: do not apply
    pub fn should_apply(&self, record: &WalRecord) -> bool {
        let txn_id = match record {
            WalRecord::Put { txn_id, .. } => *txn_id,
            WalRecord::Delete { txn_id, .. } => *txn_id,
            // Transaction marker records are not "applied" to the btree
            WalRecord::TxnCommit { .. } => return false,
            WalRecord::TxnAbort { .. } => return false,
            WalRecord::Checkpoint { .. } => return false,
        };

        // Non-transactional operations are always applied
        if txn_id == TXN_NONE {
            return true;
        }

        // Only apply committed transactions
        self.is_committed(txn_id)
    }

    /// Mark all pending transactions as aborted.
    ///
    /// This should be called at the end of recovery to finalize the state
    /// of any transactions that didn't have a commit/abort record.
    pub fn finalize_pending(&mut self) {
        for txn_id in self.pending.drain() {
            self.aborted.insert(txn_id);
        }
    }

    /// Get the number of committed transactions.
    pub fn committed_count(&self) -> usize {
        self.committed.len()
    }

    /// Get the number of explicitly aborted transactions.
    /// Note: pending transactions are not included until finalized.
    pub fn aborted_count(&self) -> usize {
        self.aborted.len()
    }

    /// Get the number of pending transactions.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
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

    #[test]
    fn test_empty_table() {
        let table = RecoveryTxnTable::new();
        assert!(!table.is_committed(1));
        assert!(!table.is_aborted(1));
        assert!(!table.is_pending(1));
    }

    #[test]
    fn test_commit_record() {
        let mut table = RecoveryTxnTable::new();

        // First, add a pending operation
        table.process_record(&WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            txn_id: 42,
        });
        assert!(table.is_pending(42));

        // Then commit
        table.process_record(&WalRecord::TxnCommit {
            txn_id: 42,
            commit_ts: 100,
        });

        assert!(table.is_committed(42));
        assert!(!table.is_pending(42));
        assert!(table.should_apply(&WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            txn_id: 42,
        }));
    }

    #[test]
    fn test_abort_record() {
        let mut table = RecoveryTxnTable::new();

        table.process_record(&WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            txn_id: 42,
        });

        table.process_record(&WalRecord::TxnAbort { txn_id: 42 });

        assert!(table.is_aborted(42));
        assert!(!table.is_pending(42));
        assert!(!table.should_apply(&WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            txn_id: 42,
        }));
    }

    #[test]
    fn test_non_transactional_always_applied() {
        let table = RecoveryTxnTable::new();

        assert!(table.should_apply(&WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            txn_id: TXN_NONE,
        }));
    }

    #[test]
    fn test_finalize_pending() {
        let mut table = RecoveryTxnTable::new();

        // Add operations without commit/abort
        table.process_record(&WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            txn_id: 42,
        });
        assert!(table.is_pending(42));

        // Finalize - pending become aborted
        table.finalize_pending();

        assert!(!table.is_pending(42));
        assert!(table.is_aborted(42));
        assert!(table.should_treat_as_aborted(42));
    }

    #[test]
    fn test_should_treat_as_aborted() {
        let mut table = RecoveryTxnTable::new();

        // Non-transactional is never aborted
        assert!(!table.should_treat_as_aborted(TXN_NONE));

        // Unknown transaction is considered aborted (was pending)
        assert!(table.should_treat_as_aborted(999));

        // Explicitly aborted
        table.process_record(&WalRecord::TxnAbort { txn_id: 1 });
        assert!(table.should_treat_as_aborted(1));

        // Committed is not aborted
        table.process_record(&WalRecord::TxnCommit {
            txn_id: 2,
            commit_ts: 100,
        });
        assert!(!table.should_treat_as_aborted(2));
    }

    #[test]
    fn test_transaction_markers_not_applied() {
        let table = RecoveryTxnTable::new();

        // Transaction markers themselves are not btree operations
        assert!(!table.should_apply(&WalRecord::TxnCommit {
            txn_id: 1,
            commit_ts: 100,
        }));
        assert!(!table.should_apply(&WalRecord::TxnAbort { txn_id: 1 }));
        assert!(!table.should_apply(&WalRecord::Checkpoint {
            root_block_id: 1,
            generation: 1,
        }));
    }

    #[test]
    fn test_multiple_transactions() {
        let mut table = RecoveryTxnTable::new();

        // Transaction 1: committed
        table.process_record(&WalRecord::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            txn_id: 1,
        });
        table.process_record(&WalRecord::TxnCommit {
            txn_id: 1,
            commit_ts: 100,
        });

        // Transaction 2: aborted
        table.process_record(&WalRecord::Put {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
            txn_id: 2,
        });
        table.process_record(&WalRecord::TxnAbort { txn_id: 2 });

        // Transaction 3: pending (incomplete)
        table.process_record(&WalRecord::Put {
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
            txn_id: 3,
        });

        assert_eq!(table.committed_count(), 1);
        assert_eq!(table.aborted_count(), 1);
        assert_eq!(table.pending_count(), 1);

        // Only tx 1 should apply
        assert!(table.should_apply(&WalRecord::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            txn_id: 1,
        }));
        assert!(!table.should_apply(&WalRecord::Put {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
            txn_id: 2,
        }));
        assert!(!table.should_apply(&WalRecord::Put {
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
            txn_id: 3,
        }));
    }
}
