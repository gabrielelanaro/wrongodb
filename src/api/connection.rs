use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::api::session::Session;
use crate::collection_write_path::CollectionWritePath;
use crate::document_query::DocumentQuery;
use crate::durability::{DurabilityBackend, StoreCommandApplier};
use crate::recovery::RecoveryManager;
use crate::schema::SchemaCatalog;
use crate::storage::table_cache::TableCache;
use crate::store_write_path::StoreWritePath;
use crate::txn::{GlobalTxnState, TransactionManager};
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
        let applier = Arc::new(StoreCommandApplier::new(table_cache.clone()));

        if config.wal_enabled {
            RecoveryManager::new(applier.clone()).recover(&base_path)?;
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
