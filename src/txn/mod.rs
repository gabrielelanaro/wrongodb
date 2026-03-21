pub(crate) mod active_write_unit;
pub mod global_txn;
pub(crate) mod manager;
pub mod read_visibility;
pub mod snapshot;
pub mod transaction;
pub mod update;
pub mod write_context;

pub(crate) use active_write_unit::ActiveWriteUnit;
pub use global_txn::GlobalTxnState;
#[allow(unused_imports)]
pub(crate) use manager::TransactionManager;
pub use read_visibility::ReadVisibility;
#[allow(unused_imports)]
pub use transaction::{Transaction, TxnOp, TxnState};
pub use update::{Update, UpdateChain, UpdateRef, UpdateType};
pub use write_context::WriteContext;

pub type TxnId = u64;
pub const TXN_NONE: TxnId = 0;
pub const TXN_ABORTED: TxnId = u64::MAX;

pub type Timestamp = u64;
pub const TS_NONE: Timestamp = 0;
pub const TS_MAX: Timestamp = u64::MAX;

#[cfg(test)]
mod tests;
