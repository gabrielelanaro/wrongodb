use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::table::Table;
use crate::txn::TransactionManager;
use crate::WrongoDBError;

#[derive(Debug)]
pub struct TableCache {
    base_path: PathBuf,
    transaction_manager: Arc<TransactionManager>,
    handles_by_store: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
}

impl TableCache {
    pub fn new(base_path: PathBuf, transaction_manager: Arc<TransactionManager>) -> Self {
        Self {
            base_path,
            transaction_manager,
            handles_by_store: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_or_open_store(&self, store_name: &str) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        if let Some(handle) = self.handles_by_store.read().get(store_name) {
            return Ok(handle.clone());
        }

        let mut handles = self.handles_by_store.write();
        if let Some(handle) = handles.get(store_name) {
            return Ok(handle.clone());
        }

        let path = self.base_path.join(store_name);
        let table = Table::open_or_create_store(path, self.transaction_manager.clone())?;
        let table = Arc::new(RwLock::new(table));
        handles.insert(store_name.to_string(), table.clone());
        Ok(table)
    }

    pub fn all_handles(&self) -> Vec<Arc<RwLock<Table>>> {
        self.handles_by_store
            .read()
            .values()
            .map(Arc::clone)
            .collect()
    }
}
