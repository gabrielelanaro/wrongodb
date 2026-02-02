use crate::cursor::Cursor;
use crate::core::errors::StorageError;
use crate::datahandle_cache::DataHandleCache;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Session {
    cache: Arc<DataHandleCache>,
    base_path: PathBuf,
    wal_enabled: bool,
    checkpoint_after_updates: Option<usize>,
    global_txn: Arc<GlobalTxnState>,
}

impl Session {
    pub(crate) fn new(
        cache: Arc<DataHandleCache>,
        base_path: PathBuf,
        wal_enabled: bool,
        checkpoint_after_updates: Option<usize>,
        global_txn: Arc<GlobalTxnState>,
    ) -> Self {
        Self {
            cache,
            base_path,
            wal_enabled,
            checkpoint_after_updates,
            global_txn,
        }
    }

    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if uri.starts_with("table:") {
            let _table = self.cache.get_or_open_table(
                uri,
                &self.base_path,
                self.wal_enabled,
                self.checkpoint_after_updates,
                self.global_txn.clone(),
            )?;
            Ok(())
        } else {
            Err(WrongoDBError::Storage(StorageError(
                format!("unsupported URI: {}", uri),
            )))
        }
    }

    pub fn open_cursor(&mut self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let table = self.cache.get_or_open_table(
            uri,
            &self.base_path,
            self.wal_enabled,
            self.checkpoint_after_updates,
            self.global_txn.clone(),
        )?;
        Ok(Cursor::new_table(table))
    }
}
