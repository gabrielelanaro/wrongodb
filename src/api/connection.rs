use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use crate::api::data_handle_cache::DataHandleCache;
use crate::api::session::Session;
use crate::core::errors::StorageError;
use crate::hooks::{MutationHooks, NoopMutationHooks};
use crate::recovery::RecoveryManager;
use crate::storage::store_registry::StoreRegistry;
use crate::txn::GlobalTxnState;
use crate::txn::TransactionManager;
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

#[derive(Debug)]
struct RecoveryMutationHooks {
    manager: Weak<RecoveryManager>,
}

impl MutationHooks for RecoveryMutationHooks {
    fn before_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| StorageError("recovery manager is not available".into()))?;
        manager.log_put(store_name, key, value, txn_id)
    }

    fn before_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| StorageError("recovery manager is not available".into()))?;
        manager.log_delete(store_name, key, txn_id)
    }

    fn before_commit(
        &self,
        txn_id: crate::txn::TxnId,
        commit_ts: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| StorageError("recovery manager is not available".into()))?;
        manager.log_txn_commit_sync(txn_id, commit_ts)
    }

    fn before_abort(&self, txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| StorageError("recovery manager is not available".into()))?;
        manager.log_txn_abort(txn_id)
    }

    fn should_apply_locally(&self) -> bool {
        false
    }
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
    recovery_manager: Arc<RecoveryManager>,
    mutation_hooks: Arc<dyn MutationHooks>,
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
        let recovery_manager = Arc::new(RecoveryManager::initialize(
            &base_path,
            config.wal_enabled,
            transaction_manager.clone(),
            store_registry.clone(),
            config.raft_mode,
        )?);

        let mutation_hooks: Arc<dyn MutationHooks> = if config.wal_enabled {
            Arc::new(RecoveryMutationHooks {
                manager: Arc::downgrade(&recovery_manager),
            })
        } else {
            Arc::new(NoopMutationHooks::default())
        };
        store_registry.set_mutation_hooks(mutation_hooks.clone());

        Ok(Self {
            base_path,
            dhandle_cache: Arc::new(DataHandleCache::new(store_registry)),
            wal_enabled: config.wal_enabled,
            transaction_manager,
            recovery_manager,
            mutation_hooks,
        })
    }

    pub fn open_session(&self) -> Session {
        Session::new(
            self.dhandle_cache.clone(),
            self.transaction_manager.clone(),
            self.recovery_manager.clone(),
            self.mutation_hooks.clone(),
        )
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub(crate) fn raft_hello_state(&self) -> (bool, Option<String>) {
        self.recovery_manager
            .raft_status()
            .map(|status| (status.is_writable_primary, status.leader_hint))
            .unwrap_or((true, None))
    }
}
