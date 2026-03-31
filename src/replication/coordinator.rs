use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use super::oplog::OpTime;
use crate::WrongoDBError;

/// Replication role configured for one server process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// Writable node that accepts user writes and emits oplog entries.
    Primary,
    /// Non-writable node that follows one configured sync source.
    Secondary,
}

impl ReplicationRole {
    fn is_writable_primary(self) -> bool {
        matches!(self, Self::Primary)
    }
}

/// Primary-side progress state tracked for one follower.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FollowerProgress {
    /// Latest oplog position the follower says it has persisted locally.
    pub(crate) last_written: Option<OpTime>,
    /// Latest oplog position the follower says it has finished applying.
    pub(crate) last_applied: Option<OpTime>,
}

impl FollowerProgress {
    /// Creates empty progress for a follower that has not reported yet.
    pub(crate) fn new(last_written: Option<OpTime>, last_applied: Option<OpTime>) -> Self {
        Self {
            last_written,
            last_applied,
        }
    }
}

/// Server-layer replication policy owned above the storage connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// Whether this node accepts user writes or follows another node.
    pub role: ReplicationRole,
    /// Stable node name used in follower-progress reports.
    pub node_name: String,
    /// Optional leader hint returned in `hello` responses.
    pub primary_hint: Option<String>,
    /// Configured sync source for secondaries in the first replication version.
    pub sync_source_uri: Option<String>,
    /// Current replication term used for newly reserved oplog positions.
    pub term: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: ReplicationRole::Primary,
            node_name: "node-1".to_string(),
            primary_hint: None,
            sync_source_uri: None,
            term: 1,
        }
    }
}

#[derive(Debug)]
struct ReplicationState {
    config: RwLock<ReplicationConfig>,
    follower_progress: RwLock<BTreeMap<String, FollowerProgress>>,
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
                follower_progress: RwLock::new(BTreeMap::new()),
                next_op_index: AtomicU64::new(1),
            }),
        }
    }

    /// Return the writable-primary state exposed by `hello`.
    pub(crate) fn hello_state(&self) -> (bool, Option<String>) {
        let config = self.state.config.read();
        (
            config.role.is_writable_primary(),
            config.primary_hint.clone(),
        )
    }

    /// Return the configured replication role.
    pub(crate) fn role(&self) -> ReplicationRole {
        self.state.config.read().role
    }

    /// Return the configured node name.
    pub(crate) fn node_name(&self) -> String {
        self.state.config.read().node_name.clone()
    }

    /// Return the configured sync source URI for secondaries.
    pub(crate) fn sync_source_uri(&self) -> Option<String> {
        self.state.config.read().sync_source_uri.clone()
    }

    /// Reject writes when this node is not currently writable.
    pub(crate) fn require_writable_primary(&self) -> Result<(), WrongoDBError> {
        let config = self.state.config.read();
        if config.role.is_writable_primary() {
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

    /// Record one follower's last-written and last-applied progress.
    pub(crate) fn update_follower_progress(
        &self,
        node_name: impl Into<String>,
        last_written: Option<OpTime>,
        last_applied: Option<OpTime>,
    ) {
        self.state.follower_progress.write().insert(
            node_name.into(),
            FollowerProgress::new(last_written, last_applied),
        );
    }

    /// Return the currently known progress for one follower.
    #[allow(dead_code)]
    pub(crate) fn follower_progress(&self, node_name: &str) -> Option<FollowerProgress> {
        self.state.follower_progress.read().get(node_name).cloned()
    }
}

impl Default for ReplicationCoordinator {
    fn default() -> Self {
        Self::new(ReplicationConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::{FollowerProgress, ReplicationConfig, ReplicationCoordinator, ReplicationRole};
    use crate::replication::OpTime;
    use crate::WrongoDBError;

    // EARS: When the coordinator uses default replication state, it shall report
    // the node as writable primary and start at term `1`.
    #[test]
    fn default_replication_state_is_writable() {
        let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());

        assert_eq!(coordinator.hello_state(), (true, None));
        assert_eq!(coordinator.current_term(), 1);
        assert_eq!(coordinator.role(), ReplicationRole::Primary);
    }

    // EARS: When the coordinator marks the node as non-primary, it shall reject
    // writes and surface the configured leader hint.
    #[test]
    fn non_primary_rejects_writes_with_leader_hint() {
        let coordinator = ReplicationCoordinator::new(ReplicationConfig {
            role: ReplicationRole::Secondary,
            node_name: "node-2".to_string(),
            primary_hint: Some("node-1".to_string()),
            sync_source_uri: Some("mongodb://node-1".to_string()),
            term: 9,
        });

        let err = coordinator.require_writable_primary().unwrap_err();
        assert!(matches!(
            err,
            WrongoDBError::NotLeader {
                leader_hint: Some(ref leader_hint)
            } if leader_hint == "node-1"
        ));
    }

    // EARS: When a follower reports its replicated positions, the coordinator
    // shall store the follower's last-written and last-applied progress.
    #[test]
    fn follower_progress_is_tracked_by_node_name() {
        let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());
        let progress = FollowerProgress::new(
            Some(OpTime { term: 1, index: 7 }),
            Some(OpTime { term: 1, index: 6 }),
        );

        coordinator.update_follower_progress(
            "secondary-a",
            progress.last_written,
            progress.last_applied,
        );

        assert_eq!(coordinator.follower_progress("secondary-a"), Some(progress));
    }
}
