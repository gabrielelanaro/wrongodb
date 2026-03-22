pub(crate) mod global_txn;
pub(crate) mod read_visibility;
mod snapshot;
pub(crate) mod transaction;

pub(crate) use global_txn::GlobalTxnState;
pub(crate) use read_visibility::ReadVisibility;
pub(crate) use transaction::{Transaction, TxnLogOp};

pub(crate) type TxnId = u64;
pub(crate) const TXN_NONE: TxnId = 0;
pub(crate) const TXN_ABORTED: TxnId = u64::MAX;

pub(crate) type Timestamp = u64;
pub(crate) const TS_NONE: Timestamp = 0;
pub(crate) const TS_MAX: Timestamp = u64::MAX;

#[cfg(test)]
mod tests;
