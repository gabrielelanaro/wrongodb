use std::sync::Arc;

use parking_lot::RwLock;

use super::{Timestamp, TxnId, TS_MAX, TS_NONE, TXN_ABORTED};

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UpdateType {
    Standard,
    Tombstone,
    Reserve,
}

#[derive(Debug, Clone)]
pub struct TimeWindow {
    pub start_ts: Timestamp,
    pub start_txn: TxnId,
    pub stop_ts: Timestamp,
    pub stop_txn: TxnId,
}

impl TimeWindow {
    pub fn new(start_txn: TxnId) -> Self {
        Self {
            start_ts: TS_NONE,
            start_txn,
            stop_ts: TS_MAX,
            stop_txn: TXN_ABORTED,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Update {
    pub txn_id: TxnId,
    pub time_window: TimeWindow,
    pub next: Option<UpdateRef>,
    pub type_: UpdateType,
    pub data: Vec<u8>,
}

pub type UpdateRef = Arc<RwLock<Update>>;

impl Update {
    pub fn new(txn_id: TxnId, update_type: UpdateType, data: Vec<u8>) -> Self {
        Self {
            txn_id,
            time_window: TimeWindow::new(txn_id),
            next: None,
            type_: update_type,
            data,
        }
    }

    pub fn is_aborted(&self) -> bool {
        self.time_window.stop_txn == TXN_ABORTED && self.time_window.stop_ts == TS_NONE
    }

    pub fn is_committed(&self) -> bool {
        !self.is_aborted() && self.time_window.start_ts != TS_NONE
    }

    pub fn is_current(&self) -> bool {
        self.time_window.stop_ts == TS_MAX
    }

    pub fn mark_committed(&mut self, commit_ts: Timestamp) {
        self.time_window.start_ts = commit_ts;
        self.time_window.stop_ts = TS_MAX;
    }

    pub fn mark_aborted(&mut self) {
        self.time_window.stop_ts = TS_NONE;
        self.time_window.stop_txn = TXN_ABORTED;
    }

    /// Check if this update is obsolete - no active transaction can see it.
    /// An aborted update is obsolete once every active transaction started after it.
    /// An overwritten update is obsolete once every active transaction can also see
    /// the update that stopped it.
    pub fn is_obsolete(&self, oldest_active_txn_id: TxnId) -> bool {
        if self.is_aborted() {
            return self.time_window.start_txn < oldest_active_txn_id;
        }

        if self.is_current() {
            return false;
        }

        self.time_window.stop_txn <= oldest_active_txn_id
    }

    /// Mark this update as stopped (overwritten by a newer update).
    pub fn mark_stopped(&mut self, stop_txn: TxnId) {
        self.time_window.stop_ts = stop_txn;
        self.time_window.stop_txn = stop_txn;
    }
}

#[derive(Debug, Default, Clone)]
pub struct UpdateChain {
    head: Option<UpdateRef>,
}

impl UpdateChain {
    pub fn prepend(&mut self, mut update: Update) -> UpdateRef {
        update.next = self.head.take();
        let update_ref = Arc::new(RwLock::new(update));
        self.head = Some(update_ref.clone());
        update_ref
    }

    pub fn head(&self) -> Option<UpdateRef> {
        self.head.clone()
    }

    pub fn mark_aborted(&mut self, txn_id: TxnId) {
        let mut current = self.head.clone();
        while let Some(update_ref) = current {
            let mut update = update_ref.write();
            if update.txn_id == txn_id {
                update.mark_aborted();
            }
            current = update.next.clone();
        }
    }

    /// Remove obsolete updates from the chain, keeping only visible ones.
    /// Returns the number of updates removed.
    pub fn truncate_obsolete(&mut self, oldest_active_txn_id: TxnId) -> usize {
        let mut removed = 0;

        while let Some(head_ref) = self.head.clone() {
            let (obsolete, next) = {
                let head = head_ref.read();
                (head.is_obsolete(oldest_active_txn_id), head.next.clone())
            };
            if obsolete {
                self.head = next;
                removed += 1;
            } else {
                break;
            }
        }

        let mut current = self.head.clone();
        while let Some(current_ref) = current.clone() {
            let next_ref = {
                let current_guard = current_ref.read();
                current_guard.next.clone()
            };

            let Some(next_ref) = next_ref else {
                break;
            };

            let (obsolete, next_next) = {
                let next_guard = next_ref.read();
                (
                    next_guard.is_obsolete(oldest_active_txn_id),
                    next_guard.next.clone(),
                )
            };

            if obsolete {
                current_ref.write().next = next_next;
                removed += 1;
            } else {
                current = Some(next_ref);
            }
        }

        removed
    }

    /// Check if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    pub fn latest_committed_entry(&self) -> Option<(UpdateType, Vec<u8>)> {
        for update_ref in self.iter() {
            let update = update_ref.read();
            if !update.is_committed() {
                continue;
            }
            return match update.type_ {
                UpdateType::Standard => Some((UpdateType::Standard, update.data.clone())),
                UpdateType::Tombstone => Some((UpdateType::Tombstone, Vec::new())),
                UpdateType::Reserve => continue,
            };
        }
        None
    }

    pub fn clear_if_materialized_current(&mut self, no_active_txns: bool) -> bool {
        if !no_active_txns {
            return false;
        }

        let Some(head_ref) = self.head.as_ref() else {
            return false;
        };
        let head = head_ref.read();
        if !head.is_committed() || !head.is_current() {
            return false;
        }

        drop(head);
        self.head = None;
        true
    }

    /// Iterate over all updates in the chain (from head to tail).
    pub fn iter(&self) -> UpdateChainIter {
        UpdateChainIter {
            current: self.head.clone(),
        }
    }
}

/// Iterator over updates in an UpdateChain.
pub struct UpdateChainIter {
    current: Option<UpdateRef>,
}

impl Iterator for UpdateChainIter {
    type Item = UpdateRef;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current.clone()?;
        self.current = current.read().next.clone();
        Some(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::GlobalTxnState;
    use std::sync::Arc;

    #[test]
    fn test_obsolete_detection() {
        let mut update = Update::new(1, UpdateType::Standard, vec![1, 2, 3]);

        // Current version (no stop_ts) is not obsolete
        assert!(!update.is_obsolete(5));

        // Mark as stopped (overwritten by txn 2)
        update.mark_stopped(2);

        // If oldest active is 3, txn 1's update is obsolete
        assert!(update.is_obsolete(3));

        // If oldest active is 1, txn 1's update is still visible to some txn
        assert!(!update.is_obsolete(1));

        // If oldest active is 2, txn 1's update is obsolete
        assert!(update.is_obsolete(2));
    }

    #[test]
    fn test_obsolete_detection_uses_stop_txn_for_overwritten_updates() {
        let mut update = Update::new(9, UpdateType::Standard, vec![1, 2, 3]);
        update.mark_stopped(15);

        assert!(!update.is_obsolete(10));
        assert!(update.is_obsolete(15));
    }

    #[test]
    fn test_aborted_update_obsolete_detection() {
        let global = Arc::new(GlobalTxnState::new());

        // Register txn 3 as active
        let _txn1 = global.allocate_txn_id();
        let _txn2 = global.allocate_txn_id();
        let txn3 = global.allocate_txn_id();
        global.register_active(txn3);

        let mut chain = UpdateChain::default();

        // Create an aborted update from txn 1
        let mut u1 = Update::new(1, UpdateType::Standard, vec![1]);
        // mark_aborted sets stop_ts=TS_NONE, stop_txn=TXN_ABORTED
        u1.time_window.stop_ts = TS_NONE;
        u1.time_window.stop_txn = TXN_ABORTED;

        // Create a current update from txn 3
        let u3 = Update::new(3, UpdateType::Standard, vec![3]);

        chain.prepend(u1);
        chain.prepend(u3);

        // With oldest_active = 3, txn 1's aborted update should be obsolete
        let threshold = global.oldest_active_txn_id();
        assert_eq!(threshold, 3);

        let removed = chain.truncate_obsolete(threshold);
        assert_eq!(removed, 1);
        assert!(!chain.is_empty());

        // Cleanup
        global.unregister_active(txn3);
    }

    #[test]
    fn test_chain_truncate_removes_obsolete_updates() {
        let global = Arc::new(GlobalTxnState::new());

        // Register txn 3 as "active" so threshold is 3
        // This means updates with start_txn < 3 are candidates for GC
        let _txn1 = global.allocate_txn_id();
        let _txn2 = global.allocate_txn_id();
        let txn3 = global.allocate_txn_id();
        global.register_active(txn3);

        let mut chain = UpdateChain::default();

        // Build chain from oldest to newest using prepend
        // u1 (txn=1) -> u2 (txn=2) -> u3 (txn=3, current)
        let mut u1 = Update::new(1, UpdateType::Standard, vec![1]);
        let mut u2 = Update::new(2, UpdateType::Standard, vec![2]);
        let u3 = Update::new(3, UpdateType::Standard, vec![3]);

        // Mark u1 and u2 as stopped (overwritten)
        u1.mark_stopped(2);
        u2.mark_stopped(3);

        chain.prepend(u1);
        chain.prepend(u2);
        chain.prepend(u3);

        // With oldest_active = 3, both u1 (start_txn=1) and u2 (start_txn=2) are obsolete
        // Only u3 (start_txn=3, current version) remains
        let threshold = global.oldest_active_txn_id();
        let removed = chain.truncate_obsolete(threshold);
        assert_eq!(removed, 2);
        assert!(!chain.is_empty());

        // Cleanup
        global.unregister_active(txn3);
    }

    #[test]
    fn test_chain_truncate_keeps_all_current_versions() {
        let global = Arc::new(GlobalTxnState::new());

        // No active transactions, threshold will be high
        let mut chain = UpdateChain::default();

        // All updates are "current" (no stop_ts)
        let u1 = Update::new(1, UpdateType::Standard, vec![1]);
        let u2 = Update::new(2, UpdateType::Standard, vec![2]);
        let u3 = Update::new(3, UpdateType::Standard, vec![3]);

        chain.prepend(u1);
        chain.prepend(u2);
        chain.prepend(u3);

        // Nothing should be removed - all are current versions
        let threshold = global.oldest_active_txn_id();
        let removed = chain.truncate_obsolete(threshold);
        assert_eq!(removed, 0);
        assert!(!chain.is_empty());
    }
}
