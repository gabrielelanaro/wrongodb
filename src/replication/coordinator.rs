use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::WrongoDBError;

/// Server-layer replication policy owned above the storage connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationConfig {
    /// Whether this node currently accepts writes.
    pub(crate) is_writable_primary: bool,
    /// Optional leader hint returned in `hello` responses.
    pub(crate) primary_hint: Option<String>,
    /// Current replication term used for newly reserved oplog positions.
    pub(crate) term: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            is_writable_primary: true,
            primary_hint: None,
            term: 1,
        }
    }
}

#[derive(Debug)]
struct ReplicationState {
    config: RwLock<ReplicationConfig>,
    next_op_index: AtomicU64,
}

/// Lightweight coordinator for replication-facing server state.
///
/// The coordinator intentionally lives above [`crate::Connection`]. It owns
/// server-facing replication policy and oplog position allocation without
/// turning the storage layer into a replication subsystem.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationCoordinator {
    state: Arc<ReplicationState>,
}

impl ReplicationCoordinator {
    /// Build a coordinator from the current replication policy.
    pub(crate) fn new(config: ReplicationConfig) -> Self {
        Self {
            state: Arc::new(ReplicationState {
                config: RwLock::new(config),
                next_op_index: AtomicU64::new(1),
            }),
        }
    }

    /// Return the writable-primary state exposed by `hello`.
    pub(crate) fn hello_state(&self) -> (bool, Option<String>) {
        let config = self.state.config.read();
        (config.is_writable_primary, config.primary_hint.clone())
    }

    /// Reject writes when this node is not currently writable.
    pub(crate) fn require_writable_primary(&self) -> Result<(), WrongoDBError> {
        let config = self.state.config.read();
        if config.is_writable_primary {
            return Ok(());
        }

        Err(WrongoDBError::NotLeader {
            leader_hint: config.primary_hint.clone(),
        })
    }

    /// Return the current term used for new oplog positions.
    pub(crate) fn current_term(&self) -> u64 {
        self.state.config.read().term
    }

    /// Reserve the next oplog index.
    pub(crate) fn reserve_next_op_index(&self) -> u64 {
        self.state.next_op_index.fetch_add(1, Ordering::SeqCst)
    }

    /// Seed the next oplog index from the durable oplog tail.
    pub(crate) fn seed_next_op_index(&self, next_op_index: u64) {
        self.state
            .next_op_index
            .store(next_op_index.max(1), Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplicationConfig, ReplicationCoordinator};
    use crate::WrongoDBError;

    // EARS: When the coordinator uses default replication state, it shall report
    // the node as writable primary and start at term `1`.
    #[test]
    fn default_replication_state_is_writable() {
        let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());

        assert_eq!(coordinator.hello_state(), (true, None));
        assert_eq!(coordinator.current_term(), 1);
    }

    // EARS: When the coordinator marks the node as non-primary, it shall reject
    // writes and surface the configured leader hint.
    #[test]
    fn non_primary_rejects_writes_with_leader_hint() {
        let coordinator = ReplicationCoordinator::new(ReplicationConfig {
            is_writable_primary: false,
            primary_hint: Some("node-2".to_string()),
            term: 9,
        });

        let err = coordinator.require_writable_primary().unwrap_err();
        assert!(matches!(
            err,
            WrongoDBError::NotLeader {
                leader_hint: Some(ref leader_hint)
            } if leader_hint == "node-2"
        ));
    }
}
