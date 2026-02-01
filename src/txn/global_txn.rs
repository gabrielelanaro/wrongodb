use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use super::snapshot::Snapshot;
use super::transaction::{IsolationLevel, Transaction};
use super::{TxnId, TXN_NONE};

#[derive(Debug)]
pub struct GlobalTxnState {
    current_txn_id: AtomicU64,
    active_txns: RwLock<Vec<TxnId>>,
}

impl GlobalTxnState {
    pub fn new() -> Self {
        Self {
            current_txn_id: AtomicU64::new(TXN_NONE),
            active_txns: RwLock::new(Vec::new()),
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
        guard.push(txn_id);
    }

    pub fn unregister_active(&self, txn_id: TxnId) {
        let mut guard = self
            .active_txns
            .write()
            .expect("active_txns lock poisoned");
        guard.retain(|id| *id != txn_id);
    }

    pub fn take_snapshot(&self, my_txn_id: TxnId) -> Snapshot {
        let current = self.current_txn_id.load(Ordering::Acquire);
        let guard = self
            .active_txns
            .read()
            .expect("active_txns lock poisoned");
        let mut active: Vec<TxnId> = guard
            .iter()
            .copied()
            .filter(|id| *id != my_txn_id && *id != TXN_NONE)
            .collect();
        let snap_min = active.iter().copied().min().unwrap_or(current);
        Snapshot {
            snap_max: current,
            snap_min,
            active: {
                active.sort_unstable();
                active
            },
            my_txn_id,
        }
    }

    pub fn begin_snapshot_txn(&self) -> Transaction {
        let txn_id = self.allocate_txn_id();
        self.register_active(txn_id);
        let snapshot = self.take_snapshot(txn_id);
        Transaction::new(txn_id, IsolationLevel::Snapshot, snapshot)
    }
}
