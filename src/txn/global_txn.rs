use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use super::snapshot::Snapshot;
use super::transaction::{IsolationLevel, Transaction};
use super::{TxnId, TXN_NONE};

const WAL_OP_SHARD_COUNT: usize = 64;

#[derive(Debug, Clone)]
pub enum PendingWalOp {
    Put {
        store_name: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        store_name: String,
        key: Vec<u8>,
    },
}

#[derive(Debug)]
pub struct GlobalTxnState {
    current_txn_id: AtomicU64,
    active_txns: RwLock<Vec<TxnId>>,
    aborted_txns: RwLock<HashSet<TxnId>>,
    pending_wal_ops: Vec<Mutex<HashMap<TxnId, Vec<PendingWalOp>>>>,
    // Cached oldest active transaction ID for GC threshold
    oldest_active_txn_id: AtomicU64,
}

impl GlobalTxnState {
    pub fn new() -> Self {
        Self {
            current_txn_id: AtomicU64::new(TXN_NONE),
            active_txns: RwLock::new(Vec::new()),
            aborted_txns: RwLock::new(HashSet::new()),
            pending_wal_ops: (0..WAL_OP_SHARD_COUNT)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            oldest_active_txn_id: AtomicU64::new(TXN_NONE),
        }
    }

    fn wal_shard(&self, txn_id: TxnId) -> &Mutex<HashMap<TxnId, Vec<PendingWalOp>>> {
        let idx = (txn_id as usize) % self.pending_wal_ops.len();
        &self.pending_wal_ops[idx]
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

        self.clear_pending_wal_ops(txn_id);
    }

    pub fn has_active_transactions(&self) -> bool {
        let guard = self.active_txns.read().expect("active_txns lock poisoned");
        !guard.is_empty()
    }

    /// Check if a transaction is currently active.
    pub fn is_active(&self, txn_id: TxnId) -> bool {
        if txn_id == TXN_NONE {
            return false;
        }
        let guard = self.active_txns.read().expect("active_txns lock poisoned");
        guard.contains(&txn_id)
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
        Transaction::new(txn_id, IsolationLevel::Snapshot, snapshot)
    }

    pub fn enqueue_wal_put(&self, txn_id: TxnId, store_name: &str, key: &[u8], value: &[u8]) {
        if txn_id == TXN_NONE {
            return;
        }
        let shard = self.wal_shard(txn_id);
        let mut guard = shard.lock().expect("pending_wal_ops lock poisoned");
        guard.entry(txn_id).or_default().push(PendingWalOp::Put {
            store_name: store_name.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    pub fn enqueue_wal_delete(&self, txn_id: TxnId, store_name: &str, key: &[u8]) {
        if txn_id == TXN_NONE {
            return;
        }
        let shard = self.wal_shard(txn_id);
        let mut guard = shard.lock().expect("pending_wal_ops lock poisoned");
        guard.entry(txn_id).or_default().push(PendingWalOp::Delete {
            store_name: store_name.to_string(),
            key: key.to_vec(),
        });
    }

    pub fn take_pending_wal_ops(&self, txn_id: TxnId) -> Vec<PendingWalOp> {
        if txn_id == TXN_NONE {
            return Vec::new();
        }
        let shard = self.wal_shard(txn_id);
        let mut guard = shard.lock().expect("pending_wal_ops lock poisoned");
        guard.remove(&txn_id).unwrap_or_default()
    }

    pub fn restore_pending_wal_ops(&self, txn_id: TxnId, mut ops: Vec<PendingWalOp>) {
        if txn_id == TXN_NONE || ops.is_empty() {
            return;
        }
        let shard = self.wal_shard(txn_id);
        let mut guard = shard.lock().expect("pending_wal_ops lock poisoned");
        guard.entry(txn_id).or_default().append(&mut ops);
    }

    pub fn clear_pending_wal_ops(&self, txn_id: TxnId) {
        if txn_id == TXN_NONE {
            return;
        }
        let shard = self.wal_shard(txn_id);
        let mut guard = shard.lock().expect("pending_wal_ops lock poisoned");
        guard.remove(&txn_id);
    }
}

impl Default for GlobalTxnState {
    fn default() -> Self {
        Self::new()
    }
}
