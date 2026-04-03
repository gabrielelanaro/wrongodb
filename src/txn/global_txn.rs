use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use super::snapshot::Snapshot;
use super::transaction::Transaction;
use super::{TxnId, TXN_NONE};

#[derive(Debug)]
pub struct GlobalTxnState {
    current_txn_id: AtomicU64,
    active_txns: RwLock<Vec<TxnId>>,
    aborted_txns: RwLock<HashSet<TxnId>>,
    // Cached oldest active transaction ID, used internally by gc_threshold()
    oldest_active_txn_id: AtomicU64,
    // Checkpoint's pinned transaction ID.
    // Stored separately from the active transaction table so the checkpoint
    // doesn't block regular GC for transactions that complete during it.
    checkpoint_pinned_id: AtomicU64,
}

impl GlobalTxnState {
    pub fn new() -> Self {
        Self {
            current_txn_id: AtomicU64::new(TXN_NONE),
            active_txns: RwLock::new(Vec::new()),
            aborted_txns: RwLock::new(HashSet::new()),
            oldest_active_txn_id: AtomicU64::new(TXN_NONE),
            checkpoint_pinned_id: AtomicU64::new(TXN_NONE),
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
        let active_guard = self.active_txns.read().expect("active_txns lock poisoned");
        let oldest = active_guard.iter().copied().min();
        drop(active_guard);

        if let Some(oldest_id) = oldest {
            self.oldest_active_txn_id
                .store(oldest_id, Ordering::Release);
        } else {
            // No active transactions - reset to TXN_NONE
            self.oldest_active_txn_id.store(TXN_NONE, Ordering::Release);
        }
    }

    pub fn allocate_txn_id(&self) -> TxnId {
        self.current_txn_id.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn register_active(&self, txn_id: TxnId) {
        let mut guard = self.active_txns.write().expect("active_txns lock poisoned");

        // If this is the first active transaction, it becomes the oldest
        let was_empty = guard.is_empty();
        guard.push(txn_id);
        drop(guard);

        if was_empty {
            self.oldest_active_txn_id.store(txn_id, Ordering::Release);
        }
    }

    pub fn unregister_active(&self, txn_id: TxnId) {
        let mut guard = self.active_txns.write().expect("active_txns lock poisoned");

        // Check if we're removing the oldest active transaction
        let was_oldest = guard.iter().min().copied() == Some(txn_id);

        guard.retain(|id| *id != txn_id);
        drop(guard);

        // Recalculate oldest if we removed the oldest transaction
        if was_oldest {
            self.recalculate_oldest();
        }
    }

    pub fn has_active_transactions(&self) -> bool {
        let guard = self.active_txns.read().expect("active_txns lock poisoned");
        !guard.is_empty()
    }

    /// Return the most recently allocated transaction ID.
    ///
    /// Checkpoint code uses this to detect whether any transaction started
    /// during the checkpoint window.
    pub fn current_txn_id(&self) -> TxnId {
        self.current_txn_id.load(Ordering::Acquire)
    }

    /// Mark a transaction as aborted
    pub fn mark_aborted(&self, txn_id: TxnId) {
        let mut guard = self
            .aborted_txns
            .write()
            .expect("aborted_txns lock poisoned");
        guard.insert(txn_id);
    }

    pub fn take_snapshot(&self, my_txn_id: TxnId) -> Snapshot {
        let current = self.current_txn_id.load(Ordering::Acquire);
        let active_guard = self.active_txns.read().expect("active_txns lock poisoned");
        let aborted_guard = self
            .aborted_txns
            .read()
            .expect("aborted_txns lock poisoned");
        let mut active: Vec<TxnId> = active_guard
            .iter()
            .copied()
            .filter(|id| *id != my_txn_id && *id != TXN_NONE)
            .collect();
        let snap_min = active.iter().copied().min().unwrap_or(current);
        let aborted: HashSet<TxnId> = aborted_guard.clone();
        Snapshot {
            snap_max: current,
            snap_min,
            active: {
                active.sort_unstable();
                active
            },
            aborted,
            my_txn_id,
        }
    }

    pub fn begin_snapshot_txn(&self) -> Transaction {
        let txn_id = self.allocate_txn_id();
        self.register_active(txn_id);
        let snapshot = self.take_snapshot(txn_id);
        Transaction::new(txn_id, snapshot)
    }

    // ------------------------------------------------------------------------
    // Checkpoint Transaction Lifecycle
    // ------------------------------------------------------------------------

    /// Pin the GC threshold for the duration of a checkpoint.
    ///
    /// Captures the current `oldest_active_txn_id` as the checkpoint's pinned ID,
    /// preventing GC from removing versions that checkpoint reconciliation might
    /// need to observe.
    ///
    /// The checkpoint is NOT registered in the active transaction table, so
    /// regular GC can still reclaim versions from transactions that complete
    /// during the checkpoint.
    pub fn begin_checkpoint(&self) {
        let pinned = self.oldest_active_txn_id();
        self.checkpoint_pinned_id.store(pinned, Ordering::Release);
    }

    /// Release the checkpoint's pinned ID, allowing GC to advance.
    ///
    /// Must be called after checkpoint reconciliation completes, regardless
    /// of success or failure.
    pub fn end_checkpoint(&self) {
        self.checkpoint_pinned_id.store(TXN_NONE, Ordering::Release);
    }

    /// GC threshold for reconciliation.
    ///
    /// Returns `oldest_active_txn_id`, clamped down to the checkpoint's pinned
    /// ID if a checkpoint is active. This prevents GC from discarding versions
    /// that the checkpoint might need to observe.
    pub fn gc_threshold(&self) -> TxnId {
        let oldest = self.oldest_active_txn_id();
        let checkpoint_pinned = self.checkpoint_pinned_id.load(Ordering::Acquire);

        if checkpoint_pinned == TXN_NONE || oldest < checkpoint_pinned {
            oldest
        } else {
            checkpoint_pinned
        }
    }
}

impl Default for GlobalTxnState {
    fn default() -> Self {
        Self::new()
    }
}
