use std::collections::HashSet;

use super::TxnId;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) snap_max: TxnId,
    pub(crate) snap_min: TxnId,
    pub(crate) active: Vec<TxnId>,
    pub(crate) aborted: HashSet<TxnId>,
    pub(crate) my_txn_id: TxnId,
}

impl Snapshot {
    pub fn is_visible(&self, txn_id: TxnId) -> bool {
        if txn_id == self.my_txn_id {
            return true;
        }
        // Aborted transactions are never visible
        if self.aborted.contains(&txn_id) {
            return false;
        }
        if txn_id >= self.snap_max {
            return false;
        }
        if txn_id < self.snap_min {
            return true;
        }
        !self.active.contains(&txn_id)
    }
}
