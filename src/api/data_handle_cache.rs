use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::table::Table;
use crate::storage::wal::WalSink;
use crate::txn::transaction_manager::TransactionManager;
use crate::WrongoDBError;

pub struct DataHandleCache {
    handles: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
    transaction_manager: Arc<TransactionManager>,
    wal_sink: Option<Arc<dyn WalSink>>,
}

impl DataHandleCache {
    pub fn new(
        transaction_manager: Arc<TransactionManager>,
        wal_sink: Option<Arc<dyn WalSink>>,
    ) -> Self {
        Self {
            handles: RwLock::new(HashMap::new()),
            transaction_manager,
            wal_sink,
        }
    }

    pub fn get_or_open_primary(
        &self,
        uri: &str,
        collection: &str,
        db_dir: &Path,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        if let Some(cached) = self.handles.read().get(uri) {
            return Ok(cached.clone());
        }

        let mut handles = self.handles.write();
        if let Some(cached) = handles.get(uri) {
            return Ok(cached.clone());
        }

        let table = Arc::new(RwLock::new(Table::open_or_create_primary(
            collection,
            db_dir,
            self.transaction_manager.clone(),
            self.wal_sink.clone(),
        )?));
        handles.insert(uri.to_string(), table.clone());
        Ok(table)
    }

    pub fn all_handles(&self) -> Vec<Arc<RwLock<Table>>> {
        self.handles.read().values().cloned().collect()
    }
}
