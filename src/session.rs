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
    global_txn: Arc<GlobalTxnState>,
}

impl Session {
    pub(crate) fn new(
        cache: Arc<DataHandleCache>,
        base_path: PathBuf,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Self {
        Self {
            cache,
            base_path,
            wal_enabled,
            global_txn,
        }
    }

    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if uri.starts_with("table:") {
            let table_path = self.base_path.join(&uri[6..]);
            let _table = self.cache.get_or_open_table(
                uri,
                &table_path,
                self.wal_enabled,
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
        let table_path = self.base_path.join(&uri[6..]);
        let table = self.cache.get_or_open_table(
            uri,
            &table_path,
            self.wal_enabled,
            self.global_txn.clone(),
        )?;
        Ok(Cursor::new(table))
    }
}
