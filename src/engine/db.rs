use std::fs;
use std::path::Path;

use crate::{Connection, ConnectionConfig, Session, WrongoDBError};

use super::collection::Collection;

/// Configuration for opening a WrongoDB database.
#[derive(Debug, Clone)]
pub struct WrongoDBConfig {
    /// Enable WAL for durability (default: true)
    pub wal_enabled: bool,
    /// Auto-checkpoint interval in seconds (default: 60s)
    pub checkpoint_wait_secs: Option<u64>,
    /// Auto-checkpoint WAL size threshold in bytes (default: 64MB)
    pub checkpoint_log_size_bytes: Option<u64>,
}

impl Default for WrongoDBConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            checkpoint_wait_secs: Some(60),
            checkpoint_log_size_bytes: Some(64 * 1024 * 1024),
        }
    }
}

impl WrongoDBConfig {
    /// Create a new config with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable WAL (default: true).
    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    pub fn checkpoint_wait_secs(mut self, wait_secs: Option<u64>) -> Self {
        self.checkpoint_wait_secs = wait_secs;
        self
    }

    pub fn checkpoint_log_size_bytes(mut self, bytes: Option<u64>) -> Self {
        self.checkpoint_log_size_bytes = bytes;
        self
    }

    pub fn disable_auto_checkpoint(mut self) -> Self {
        self.checkpoint_wait_secs = None;
        self.checkpoint_log_size_bytes = None;
        self
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DbStats {
    pub collection_count: usize,
    pub document_count: usize,
    pub index_count: usize,
}

#[derive(Debug)]
pub struct WrongoDB {
    connection: Connection,
}

impl WrongoDB {
    /// Open a database with the given configuration.
    pub fn open_with_config<P>(path: P, config: WrongoDBConfig) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let base_path = path.as_ref();
        fs::create_dir_all(base_path)?;
        let conn = Connection::open(
            base_path,
            ConnectionConfig {
                wal_enabled: config.wal_enabled,
                checkpoint_wait_secs: config.checkpoint_wait_secs,
                checkpoint_log_size_bytes: config.checkpoint_log_size_bytes,
            },
        )?;
        Ok(Self { connection: conn })
    }

    /// Open a database with default settings.
    ///
    /// WAL is enabled by default.
    pub fn open<P>(path: P) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        Self::open_with_config(path, WrongoDBConfig::default())
    }

    pub fn open_session(&self) -> Session {
        self.connection.open_session()
    }

    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(name)
    }

    pub fn list_collections(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut names = Vec::new();
        let base = self.connection.base_path();
        for entry in fs::read_dir(base)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name = match file_name.to_str() {
                Some(s) => s,
                None => continue,
            };
            if let Some(name) = file_name.strip_suffix(".main.wt") {
                names.push(name.to_string());
            }
        }
        names.sort();
        Ok(names)
    }

    pub fn stats(&self) -> Result<DbStats, WrongoDBError> {
        let collections = self.list_collections()?;
        let mut document_count = 0usize;
        let mut index_count = 0usize;

        for name in &collections {
            let coll = self.collection(name);
            let mut session = self.open_session();
            document_count += coll.count(&mut session, None)?;
            let indexes = coll.list_indexes(&mut session)?;
            index_count += indexes.len();
        }

        Ok(DbStats {
            collection_count: collections.len(),
            document_count,
            index_count,
        })
    }

    pub fn base_path(&self) -> &Path {
        self.connection.base_path()
    }
}
