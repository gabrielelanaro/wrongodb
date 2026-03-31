mod applier;
mod await_service;
mod bootstrap;
mod coordinator;
mod observer;
mod oplog;
mod secondary;
mod state_store;

use crate::core::Namespace;

pub(crate) use applier::OplogApplier;
pub(crate) use await_service::OplogAwaitService;
pub(crate) use bootstrap::{bootstrap_oplog, bootstrap_replication_state};
#[allow(unused_imports)]
pub(crate) use coordinator::{FollowerProgress, ReplicationCoordinator};
#[allow(unused_imports)]
pub use coordinator::{ReplicationConfig, ReplicationRole};
pub(crate) use observer::ReplicationObserver;
#[allow(unused_imports)]
pub(crate) use oplog::{
    oplog_entry_from_bson_document, oplog_namespace, OpTime, OplogEntry, OplogMode, OplogOperation,
    OplogStore,
};
pub(crate) use secondary::SecondaryReplicator;
#[allow(unused_imports)]
pub(crate) use state_store::{replication_state_namespace, LocalReplicationStateStore};

/// Return whether a namespace is reserved for replication-owned state.
pub(crate) fn is_reserved_namespace(namespace: &Namespace) -> bool {
    if !namespace.db_name().is_local() {
        return false;
    }

    *namespace == oplog_namespace() || *namespace == replication_state_namespace()
}
