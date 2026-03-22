use crate::storage::mvcc::Update;
use crate::txn::{TxnId, TXN_ABORTED};

/// Read-time visibility context consumed by storage cursors.
///
/// The B+tree layer should not depend on global transaction state directly. It
/// only needs enough context to decide whether a version on an update chain is
/// visible to the current reader.
///
/// This type intentionally starts small. Today it carries only the bound
/// transaction id and reproduces the existing visibility rule used by the
/// transaction manager. It can later grow to include snapshot/read-timestamp
/// state without changing the B+tree/read API shape again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadVisibility {
    txn_id: TxnId,
}

impl ReadVisibility {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn from_txn_id(txn_id: TxnId) -> Self {
        Self { txn_id }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn can_see(&self, update: &Update) -> bool {
        if is_aborted(update) {
            return false;
        }

        if self.txn_id == crate::txn::TXN_NONE {
            return update.is_committed();
        }

        update.txn_id <= self.txn_id
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn is_aborted(update: &Update) -> bool {
    update.time_window.stop_txn == TXN_ABORTED && update.time_window.stop_ts == crate::txn::TS_NONE
}
