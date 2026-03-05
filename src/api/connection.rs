use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::api::data_handle_cache::DataHandleCache;
use crate::api::session::Session;
use crate::durability::{DurabilityManager, StoreCommandApplier};
use crate::recovery::RecoveryManager;
use crate::storage::store_registry::StoreRegistry;
use crate::txn::transaction_manager::TransactionManager;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftPeerConfig {
    pub node_id: String,
    pub raft_addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RaftMode {
    #[default]
    Standalone,
    Cluster {
        local_node_id: String,
        local_raft_addr: String,
        peers: Vec<RaftPeerConfig>,
    },
}

pub struct ConnectionConfig {
    pub wal_enabled: bool,
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    pub fn raft_mode(mut self, mode: RaftMode) -> Self {
        self.raft_mode = mode;
        self
    }
}

pub struct Connection {
    base_path: PathBuf,
    dhandle_cache: Arc<DataHandleCache>,
    wal_enabled: bool,
    transaction_manager: Arc<TransactionManager>,
    durability_manager: Arc<DurabilityManager>,
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
    pub fn open<P>(path: P, config: ConnectionConfig) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;

        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let store_registry = Arc::new(StoreRegistry::new(
            base_path.clone(),
            transaction_manager.clone(),
        ));
        let applier = Arc::new(StoreCommandApplier::new(store_registry.clone()));

        if config.wal_enabled {
            RecoveryManager::new(applier.clone()).recover(&base_path)?;
        }

        let durability_manager = Arc::new(DurabilityManager::initialize(
            &base_path,
            config.wal_enabled,
            applier,
            config.raft_mode,
        )?);

        if config.wal_enabled {
            store_registry.set_wal_sink(Some(durability_manager.wal_sink()));
        }

        Ok(Self {
            base_path,
            dhandle_cache: Arc::new(DataHandleCache::new(store_registry)),
            wal_enabled: config.wal_enabled,
            transaction_manager,
            durability_manager,
        })
    }

    pub fn open_session(&self) -> Session {
        Session::new(
            self.dhandle_cache.clone(),
            self.transaction_manager.clone(),
            self.durability_manager.clone(),
        )
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub(crate) fn raft_hello_state(&self) -> (bool, Option<String>) {
        self.durability_manager
            .raft_status()
            .map(|status| (status.is_writable_primary, status.leader_hint))
            .unwrap_or((true, None))
    }
}
