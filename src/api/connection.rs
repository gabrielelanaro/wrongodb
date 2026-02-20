use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::api::data_handle_cache::DataHandleCache;
use crate::api::session::Session;
use crate::recovery::RecoveryManager;
use crate::storage::wal::WalSink;
use crate::txn::transaction_manager::TransactionManager;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

pub struct ConnectionConfig {
    pub wal_enabled: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self { wal_enabled: true }
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
}

pub struct Connection {
    base_path: PathBuf,
    dhandle_cache: Arc<DataHandleCache>,
    wal_enabled: bool,
    transaction_manager: Arc<TransactionManager>,
    recovery_manager: Arc<RecoveryManager>,
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
        let recovery_manager = Arc::new(RecoveryManager::initialize(
            &base_path,
            config.wal_enabled,
            transaction_manager.clone(),
        )?);

        let wal_sink = if config.wal_enabled {
            Some(recovery_manager.clone() as Arc<dyn WalSink>)
        } else {
            None
        };

        Ok(Self {
            base_path,
            dhandle_cache: Arc::new(DataHandleCache::new(transaction_manager.clone(), wal_sink)),
            wal_enabled: config.wal_enabled,
            transaction_manager,
            recovery_manager,
        })
    }

    pub fn open_session(&self) -> Session {
        Session::new(
            self.dhandle_cache.clone(),
            self.base_path.clone(),
            self.transaction_manager.clone(),
            self.recovery_manager.clone(),
        )
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}
