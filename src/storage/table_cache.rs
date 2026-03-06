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

    pub fn get_or_open_primary(
        &self,
        collection: &str,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        let store_name = format!("{collection}.main.wt");
        self.get_or_open_store(&store_name)
    }

    pub fn get_or_open_store(&self, store_name: &str) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        if let Some(handle) = self.handles_by_store.read().get(store_name) {
            return Ok(handle.clone());
        }

        let mut handles = self.handles_by_store.write();
        if let Some(handle) = handles.get(store_name) {
            return Ok(handle.clone());
        }

        let table = if let Some(collection) = store_name.strip_suffix(".main.wt") {
            Table::open_or_create_primary(
                collection,
                &self.base_path,
                self.transaction_manager.clone(),
            )?
        } else {
            let path = self.base_path.join(store_name);
            Table::open_or_create_index(path, self.transaction_manager.clone())?
        };
        let table = Arc::new(RwLock::new(table));
        handles.insert(store_name.to_string(), table.clone());
        let is_primary = store_name.ends_with(".main.wt");
        drop(handles);
        if is_primary {
            self.register_index_handles(&table);
        }
        Ok(table)
    }

    pub fn all_handles(&self) -> Vec<Arc<RwLock<Table>>> {
        self.handles_by_store
            .read()
            .values()
            .map(Arc::clone)
            .collect()
    }

    fn register_index_handles(&self, primary: &Arc<RwLock<Table>>) {
        let mut handles = self.handles_by_store.write();
        let table = primary.read();
        let Some(catalog) = table.index_catalog() else {
            return;
        };
        for def in catalog.index_defs() {
            if let Some(index_handle) = catalog.index_handle(&def.name) {
                handles.entry(def.source).or_insert(index_handle);
            }
        }
    }
}
