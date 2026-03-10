use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::api::session::Session;
use crate::collection_write_path::CollectionWritePath;
use crate::document_query::DocumentQuery;
use crate::durability::{DurabilityBackend, StoreCommandApplier};
use crate::recovery::recover_from_wal;
use crate::schema::SchemaCatalog;
use crate::storage::table_cache::TableCache;
use crate::storage::wal::{GlobalWal, WalReader};
use crate::store_write_path::StoreWritePath;
use crate::txn::{GlobalTxnState, TransactionManager};
use crate::WrongoDBError;

/// Network identity for a remote RAFT peer.
///
/// This is part of the public configuration surface because clustered mode
/// needs an explicit startup-time description of the peer set. WrongoDB does
/// not do peer discovery dynamically, so callers provide this directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftPeerConfig {
    /// Stable node identifier used in RAFT protocol messages.
    pub node_id: String,
    /// Socket address used for RAFT transport.
    pub raft_addr: String,
}

/// Durability/replication mode for a connection.
///
/// This exists to keep the public API honest about the two supported write
/// paths:
/// - local durability on a single node
/// - deferred apply through clustered RAFT replication
///
/// Callers choose this once at connection startup because it changes the
/// meaning of write execution, recovery, and which low-level APIs remain
/// writable.
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

/// Configuration used when opening a [`Connection`].
///
/// The public connection constructor stays intentionally small. This struct is
/// where callers choose the coarse durability mode without having to know
/// about the internal storage, recovery, or replication wiring.
pub struct ConnectionConfig {
    /// Enables local durability and crash recovery.
    ///
    /// This is separate from `raft_mode` because standalone durability and
    /// clustered replication are distinct concerns.
    pub wal_enabled: bool,
    /// Selects standalone or clustered write replication behavior.
    pub raft_mode: RaftMode,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            raft_mode: RaftMode::Standalone,
        }
    }
}

impl ConnectionConfig {
    /// Create a config with the default durable standalone settings.
    ///
    /// This exists so callers can start from the common case and override only
    /// the coarse durability/replication knobs they care about.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override whether local durability is enabled.
    ///
    /// This stays on the config builder so durability mode is chosen before the
    /// engine is opened, not mutated later at runtime.
    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    /// Override the replication mode.
    ///
    /// This exists because replication mode affects startup wiring, not just
    /// request-time behavior.
    pub fn raft_mode(mut self, mode: RaftMode) -> Self {
        self.raft_mode = mode;
        self
    }
}

/// Top-level database handle that owns shared engine state.
///
/// `Connection` exists so sessions can stay cheap and request-scoped. Long-lived
/// components such as schema metadata, open store handles, MVCC state, and
/// durability machinery live here once, and every [`Session`](crate::Session)
/// borrows that shared infrastructure instead of rebuilding it.
///
/// In other words: `Connection` owns the engine, [`Session`](crate::Session)
/// owns request-local state.
pub struct Connection {
    base_path: PathBuf,
    wal_enabled: bool,
    schema_catalog: Arc<SchemaCatalog>,
    table_cache: Arc<TableCache>,
    transaction_manager: Arc<TransactionManager>,
    durability_backend: Arc<DurabilityBackend>,
    pub(crate) document_query: DocumentQuery,
    pub(crate) collection_write_path: CollectionWritePath,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("base_path", &self.base_path)
            .field("wal_enabled", &self.wal_enabled)
            .finish()
    }
}

impl Connection {
    /// Open or create a database rooted at `path`.
    ///
    /// This is the only supported way to construct the database because it
    /// wires recovery, local durability, replication mode, schema state, and
    /// shared caches together consistently before any session is opened.
    ///
    /// The constructor does this eagerly so callers never have to reason about
    /// partially initialized engine state.
    pub fn open<P>(path: P, config: ConnectionConfig) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;

        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let schema_catalog = Arc::new(SchemaCatalog::new(base_path.clone()));
        let table_cache = Arc::new(TableCache::new(
            base_path.clone(),
            transaction_manager.clone(),
        ));
        let applier = Arc::new(StoreCommandApplier::new(
            table_cache.clone(),
            transaction_manager.clone(),
        ));

        if config.wal_enabled {
            if let Some(recovery_reader) = open_recovery_reader(&base_path) {
                recover_from_wal(applier.clone(), recovery_reader)?;
            }
        }

        let durability_backend = Arc::new(DurabilityBackend::open(
            &base_path,
            config.wal_enabled,
            applier,
            config.raft_mode,
        )?);
        let document_query = DocumentQuery::new(schema_catalog.clone());
        let store_write_path = StoreWritePath::new(table_cache.clone(), durability_backend.clone());
        let collection_write_path = CollectionWritePath::new(
            schema_catalog.clone(),
            document_query.clone(),
            store_write_path,
        );

        Ok(Self {
            base_path,
            schema_catalog,
            table_cache,
            wal_enabled: config.wal_enabled,
            transaction_manager,
            durability_backend,
            document_query,
            collection_write_path,
        })
    }

    /// Open a fresh request-scoped [`Session`](crate::Session).
    ///
    /// Sessions are intentionally lightweight. They hold mutable per-request
    /// state such as the active transaction and bound cursors, while
    /// `Connection` retains the shared engine state.
    ///
    /// This separation is the reason session creation is cheap and why multiple
    /// sessions can share one opened database safely.
    pub fn open_session(&self) -> Session {
        Session::new(
            self.table_cache.clone(),
            self.schema_catalog.clone(),
            self.transaction_manager.clone(),
            self.durability_backend.clone(),
        )
    }

    pub(crate) fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub(crate) fn raft_hello_state(&self) -> (bool, Option<String>) {
        self.durability_backend
            .status()
            .map(|status| (status.is_writable_primary, status.leader_hint))
            .unwrap_or((true, None))
    }
}

fn open_recovery_reader(base_path: &Path) -> Option<WalReader> {
    let wal_path = GlobalWal::path_for_db(base_path);
    if !wal_path.exists() {
        return None;
    }

    match WalReader::open(&wal_path) {
        Ok(reader) => Some(reader),
        Err(err) => {
            eprintln!("Skipping global WAL recovery (failed to open WAL): {err}");
            None
        }
    }
}
