use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::datahandle_cache::DataHandleCache;
use crate::session::Session;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

pub struct ConnectionConfig {
    pub wal_enabled: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
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
}

pub struct Connection {
    base_path: PathBuf,
    dhandle_cache: Arc<DataHandleCache>,
    wal_enabled: bool,
    global_txn: Arc<GlobalTxnState>,
}

impl Connection {
    pub fn open<P>(path: P, config: ConnectionConfig) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let base_path = path.as_ref().to_path_buf();
        let global_txn = Arc::new(GlobalTxnState::new());

        Ok(Self {
            base_path,
            dhandle_cache: Arc::new(DataHandleCache::new()),
            wal_enabled: config.wal_enabled,
            global_txn,
        })
    }

    pub fn open_session(&self) -> Session {
        Session::new(
            self.dhandle_cache.clone(),
            self.base_path.clone(),
            self.wal_enabled,
            self.global_txn.clone(),
        )
    }
}
