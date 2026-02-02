use super::{Timestamp, TxnId, TS_MAX, TS_NONE, TXN_ABORTED};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UpdateType {
    Standard,
    Tombstone,
    Reserve,
}

#[derive(Debug, Clone)]
pub struct TimeWindow {
    pub start_ts: Timestamp,
    pub durable_start_ts: Timestamp,
    pub start_txn: TxnId,
    pub stop_ts: Timestamp,
    pub durable_stop_ts: Timestamp,
    pub stop_txn: TxnId,
    pub prepared: bool,
}

impl TimeWindow {
    pub fn new(start_txn: TxnId) -> Self {
        Self {
            start_ts: TS_NONE,
            durable_start_ts: TS_NONE,
            start_txn,
            stop_ts: TS_MAX,
            durable_stop_ts: TS_MAX,
            stop_txn: TXN_ABORTED,
            prepared: false,
        }
    }
}

#[derive(Debug)]
pub struct Update {
    pub txn_id: TxnId,
    pub time_window: TimeWindow,
    pub next: Option<Box<Update>>,
    pub type_: UpdateType,
    pub data: Vec<u8>,
}

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

    /// Check if this update is obsolete - no active transaction can see it.
    /// An update is obsolete if:
    /// 1. It was aborted (stop_txn == TXN_ABORTED AND stop_ts == TS_NONE), OR
    /// 2. It has a stop_ts (was overwritten), AND
    /// 3. All active transactions have a snap_max > start_txn (started after this update)
    pub fn is_obsolete(&self, oldest_active_txn_id: TxnId) -> bool {
        // Aborted updates are obsolete if no active transaction can see them.
        // Aborted updates have stop_txn == TXN_ABORTED AND stop_ts == TS_NONE.
        if self.time_window.stop_txn == TXN_ABORTED && self.time_window.stop_ts == TS_NONE {
            return self.time_window.start_txn < oldest_active_txn_id;
        }

        // If stop_ts is MAX, the update is still the current version
        if self.time_window.stop_ts == TS_MAX {
            return false;
        }

        // If the oldest active transaction started after this update's start_txn,
        // then no active transaction can see this update
        self.time_window.start_txn < oldest_active_txn_id
    }

    /// Mark this update as stopped (overwritten by a newer update).
    pub fn mark_stopped(&mut self, stop_txn: TxnId) {
        self.time_window.stop_ts = stop_txn;
        self.time_window.stop_txn = stop_txn;
    }
}

#[derive(Debug, Default)]
pub struct UpdateChain {
    head: Option<Box<Update>>,
}

impl UpdateChain {
    pub fn prepend(&mut self, mut update: Update) {
        update.next = self.head.take();
        self.head = Some(Box::new(update));
    }

    pub fn head_mut(&mut self) -> Option<&mut Update> {
        self.head.as_deref_mut()
    }

    pub fn mark_aborted(&mut self, txn_id: TxnId) {
        let mut current = self.head.as_deref_mut();
        while let Some(update) = current {
            if update.txn_id == txn_id {
                update.time_window.stop_ts = TS_NONE;
                update.time_window.stop_txn = TXN_ABORTED;
            }
            current = update.next.as_deref_mut();
        }
    }

    /// Remove obsolete updates from the chain, keeping only visible ones.
    /// Returns the number of updates removed.
    pub fn truncate_obsolete(&mut self, oldest_active_txn_id: TxnId) -> usize {
        let mut removed = 0;

        // Remove obsolete updates from the head
        while let Some(head) = self.head.as_ref() {
            if head.is_obsolete(oldest_active_txn_id) {
                self.head = self.head.take().unwrap().next;
                removed += 1;
            } else {
                break;
            }
        }

        // Remove obsolete updates from the middle/end using raw pointers
        // This is safe because we maintain the invariants of the linked list
        if let Some(mut current_ptr) = self.head.as_mut() {
            while let Some(next_box) = current_ptr.next.as_mut() {
                if next_box.is_obsolete(oldest_active_txn_id) {
                    // Remove the obsolete next node
                    current_ptr.next = next_box.next.take();
                    removed += 1;
                } else {
                    // Move to the next node
                    current_ptr = current_ptr
                        .next
                        .as_mut()
                        .expect("we just checked it's Some");
                }
            }
        }

        removed
    }

    /// Check if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    /// Iterate over all updates in the chain (from head to tail).
    pub fn iter(&self) -> UpdateChainIter<'_> {
        UpdateChainIter {
            current: self.head.as_deref(),
        }
    }
}

/// Iterator over updates in an UpdateChain.
pub struct UpdateChainIter<'a> {
    current: Option<&'a Update>,
}

impl<'a> Iterator for UpdateChainIter<'a> {
    type Item = &'a Update;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current?;
        self.current = current.next.as_deref();
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
