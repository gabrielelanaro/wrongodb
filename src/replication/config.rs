/// Network identity for a remote RAFT peer.
///
/// This is part of the public startup configuration because clustered mode
/// needs an explicit peer set. WrongoDB does not do peer discovery dynamically,
/// so callers provide the cluster membership directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftPeerConfig {
    /// Stable node identifier used in RAFT protocol messages.
    pub node_id: String,
    /// Socket address used for RAFT transport.
    pub raft_addr: String,
}

/// Durability/replication mode chosen at startup.
///
/// WrongoDB currently supports two write paths:
/// - standalone local durability
/// - deferred apply through clustered RAFT replication
///
/// Callers choose the mode once at startup because it affects storage/recovery
/// wiring and how writes are coordinated.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RaftMode {
    /// Single-node mode with local durability only.
    #[default]
    Standalone,
    /// Multi-node RAFT replication mode.
    Cluster {
        /// Stable identifier for the local node.
        local_node_id: String,
        /// Socket address the local RAFT service listens on.
        local_raft_addr: String,
        /// Static peer list for the cluster.
        peers: Vec<RaftPeerConfig>,
    },
}
