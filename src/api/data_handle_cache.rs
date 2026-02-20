use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::table::Table;
use crate::txn::TxnManager;
use crate::WrongoDBError;

pub struct DataHandleCache {
    handles: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
    txn_manager: Arc<TxnManager>,
}

impl DataHandleCache {
    pub fn new(txn_manager: Arc<TxnManager>) -> Self {
        Self {
            handles: RwLock::new(HashMap::new()),
            txn_manager,
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
            self.txn_manager.clone(),
        )?));
        handles.insert(uri.to_string(), table.clone());
        Ok(table)
    }

    pub fn all_handles(&self) -> Vec<Arc<RwLock<Table>>> {
        self.handles.read().values().cloned().collect()
    }
}
