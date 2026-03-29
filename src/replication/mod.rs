mod coordinator;
mod observer;
mod oplog;

#[cfg(test)]
mod tests;

pub(crate) use coordinator::{ReplicationConfig, ReplicationCoordinator};
pub(crate) use observer::ReplicationObserver;
pub(crate) use oplog::{
    OpTime, OplogEntry, OplogMode, OplogOperation, OplogStore, OPLOG_COLLECTION,
};
