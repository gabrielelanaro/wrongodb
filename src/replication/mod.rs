// ============================================================================
// Configuration and runtime state
// ============================================================================

/// Server-layer replication policy owned above the storage connection.
///
/// This keeps replication-facing topology state out of the storage WAL and
/// lets the server/bootstrap layer decide whether the database presents itself
/// as writable or read-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationConfig {
    /// Whether this node currently accepts writes.
    pub(crate) is_writable_primary: bool,
    /// Optional leader hint returned in `hello` responses.
    pub(crate) primary_hint: Option<String>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            is_writable_primary: true,
            primary_hint: None,
        }
    }
}

/// Lightweight coordinator for replication-facing server state.
///
/// The coordinator intentionally lives above [`crate::Connection`]. It owns
/// server-facing replication policy without turning the storage layer into a
/// replication subsystem.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationCoordinator {
    config: ReplicationConfig,
}

impl ReplicationCoordinator {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Build a coordinator from the current replication policy.
    pub(crate) fn new(config: ReplicationConfig) -> Self {
        Self { config }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    /// Return the writable-primary state exposed by `hello`.
    pub(crate) fn hello_state(&self) -> (bool, Option<String>) {
        (
            self.config.is_writable_primary,
            self.config.primary_hint.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_replication_state_is_writable() {
        let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());

        assert_eq!(coordinator.hello_state(), (true, None));
    }
}
