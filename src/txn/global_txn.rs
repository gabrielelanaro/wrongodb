use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use super::snapshot::Snapshot;
use super::transaction::{IsolationLevel, Transaction};
use super::{TxnId, TXN_NONE};

#[derive(Debug)]
pub struct GlobalTxnState {
    current_txn_id: AtomicU64,
    active_txns: RwLock<Vec<TxnId>>,
    aborted_txns: RwLock<HashSet<TxnId>>,
    // Cached oldest active transaction ID for GC threshold
    oldest_active_txn_id: AtomicU64,
}

impl GlobalTxnState {
    pub fn new() -> Self {
        Self {
            current_txn_id: AtomicU64::new(TXN_NONE),
            active_txns: RwLock::new(Vec::new()),
            aborted_txns: RwLock::new(HashSet::new()),
            oldest_active_txn_id: AtomicU64::new(TXN_NONE),
        }
    }

    /// Get the oldest active transaction ID.
    /// This is the threshold for GC - updates with start_txn < oldest_active
    /// may be eligible for removal if they have a stop_ts.
    pub fn oldest_active_txn_id(&self) -> TxnId {
        let cached = self.oldest_active_txn_id.load(Ordering::Acquire);
        if cached == TXN_NONE {
            // No active transactions, use current + 1 as threshold
            self.current_txn_id.load(Ordering::Acquire) + 1
        } else {
            cached
        }
    }

    /// Recalculate the oldest active transaction ID.
    /// Should be called when a transaction unregisters or aborts.
    fn recalculate_oldest(&self) {
        let active_guard = self
            .active_txns
            .read()
            .expect("active_txns lock poisoned");
        let oldest = active_guard.iter().copied().min();
        drop(active_guard);

        if let Some(oldest_id) = oldest {
            self.oldest_active_txn_id.store(oldest_id, Ordering::Release);
        } else {
            // No active transactions - reset to TXN_NONE
            self.oldest_active_txn_id.store(TXN_NONE, Ordering::Release);
        }
    }

    pub fn allocate_txn_id(&self) -> TxnId {
        self.current_txn_id.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn register_active(&self, txn_id: TxnId) {
        let mut guard = self
            .active_txns
            .write()
            .expect("active_txns lock poisoned");

        // If this is the first active transaction, it becomes the oldest
        let was_empty = guard.is_empty();
        guard.push(txn_id);
        drop(guard);

        if was_empty {
            self.oldest_active_txn_id.store(txn_id, Ordering::Release);
        }
    }

    pub fn unregister_active(&self, txn_id: TxnId) {
        let mut guard = self
            .active_txns
            .write()
            .expect("active_txns lock poisoned");

        // Check if we're removing the oldest active transaction
        let was_oldest = guard.iter().min().copied() == Some(txn_id);

        guard.retain(|id| *id != txn_id);
        drop(guard);

        // Recalculate oldest if we removed the oldest transaction
        if was_oldest {
            self.recalculate_oldest();
        }
    }

    /// Mark a transaction as aborted
    pub fn mark_aborted(&self, txn_id: TxnId) {
        let mut guard = self
            .aborted_txns
            .write()
            .expect("aborted_txns lock poisoned");
        guard.insert(txn_id);
    }

    /// Check if a transaction is aborted
    pub fn is_aborted(&self, txn_id: TxnId) -> bool {
        let guard = self
            .aborted_txns
            .read()
            .expect("aborted_txns lock poisoned");
        guard.contains(&txn_id)
    }

    pub fn take_snapshot(&self, my_txn_id: TxnId) -> Snapshot {
        let current = self.current_txn_id.load(Ordering::Acquire);
        let snap_max = current.saturating_add(1);
        let active_guard = self
            .active_txns
            .read()
            .expect("active_txns lock poisoned");
        let aborted_guard = self
            .aborted_txns
            .read()
            .expect("aborted_txns lock poisoned");
        let mut active: Vec<TxnId> = active_guard
            .iter()
            .copied()
            .filter(|id| *id != my_txn_id && *id != TXN_NONE)
            .collect();
        let snap_min = active.iter().copied().min().unwrap_or(snap_max);
        let aborted: HashSet<TxnId> = aborted_guard.clone();
        Snapshot {
            snap_max,
            snap_min,
            active: {
                active.sort_unstable();
                active
            },
            aborted,
            my_txn_id,
        }
    }

    pub fn checkpoint_snapshot(&self) -> Snapshot {
        self.take_snapshot(TXN_NONE)
    }

    pub fn begin_snapshot_txn(&self) -> Transaction {
        let txn_id = self.allocate_txn_id();
        self.register_active(txn_id);
        let snapshot = self.take_snapshot(txn_id);
        Transaction::new(txn_id, IsolationLevel::Snapshot, snapshot)
    }
}
