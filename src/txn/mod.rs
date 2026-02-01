pub mod global_txn;
pub mod recovery;
pub mod snapshot;
pub mod transaction;
pub mod update;

pub use global_txn::GlobalTxnState;
#[allow(unused_imports)]
pub use recovery::RecoveryTxnTable;
#[allow(unused_imports)]
pub use transaction::{IsolationLevel, Transaction, TxnState, WriteOp, WriteRef};
pub use update::{Update, UpdateChain, UpdateType};

pub type TxnId = u64;
pub const TXN_NONE: TxnId = 0;
pub const TXN_ABORTED: TxnId = u64::MAX;

pub type Timestamp = u64;
pub const TS_NONE: Timestamp = 0;
pub const TS_MAX: Timestamp = u64::MAX;

/// Context for read operations - can be a transaction or non-transactional context
pub trait ReadContext {
    fn txn(&self) -> Option<&Transaction>;
}

impl ReadContext for Transaction {
    fn txn(&self) -> Option<&Transaction> {
        Some(self)
    }
}

impl ReadContext for &Transaction {
    fn txn(&self) -> Option<&Transaction> {
        Some(self)
    }
}

/// Non-transactional context uses TXN_NONE
pub struct NonTransactional;

impl ReadContext for NonTransactional {
    fn txn(&self) -> Option<&Transaction> {
        None
    }
}

#[cfg(test)]
mod tests;
