use crate::txn::TxnId;

/// Write-time transaction context consumed by storage cursors.
///
/// The B+tree layer should not depend on the transaction manager directly. It
/// only needs enough context to stamp newly created updates and attach them to
/// the target page-local update chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteContext {
    txn_id: TxnId,
}

impl WriteContext {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn from_txn_id(txn_id: TxnId) -> Self {
        Self { txn_id }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }
}
