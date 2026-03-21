pub mod global_txn;
pub(crate) mod manager;
pub mod read_visibility;
pub mod snapshot;
pub mod transaction;
pub mod update;

pub use global_txn::GlobalTxnState;
#[allow(unused_imports)]
pub(crate) use manager::TransactionManager;
pub use read_visibility::ReadVisibility;
#[allow(unused_imports)]
pub use transaction::{Transaction, TxnLogOp, TxnState};
pub use update::{Update, UpdateChain, UpdateHandle, UpdateType};

pub type TxnId = u64;
pub const TXN_NONE: TxnId = 0;
pub const TXN_ABORTED: TxnId = u64::MAX;

pub type Timestamp = u64;
pub const TS_NONE: Timestamp = 0;
pub const TS_MAX: Timestamp = u64::MAX;

#[cfg(test)]
mod tests;
