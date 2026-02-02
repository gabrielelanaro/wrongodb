use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::storage::table::Table;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;
use parking_lot::RwLock;

pub struct DataHandleCache {
    handles: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
}

impl DataHandleCache {
    pub fn new() -> Self {
        Self {
            handles: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_or_open_table(
        &self,
        uri: &str,
        table_path: &Path,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        if let Some(cached) = self.handles.read().get(uri) {
            return Ok(cached.clone());
        }

        let mut handles = self.handles.write();
        if let Some(cached) = handles.get(uri) {
            return Ok(cached.clone());
        }

        let table = Arc::new(RwLock::new(Table::open_or_create(
            table_path,
            wal_enabled,
            global_txn,
        )?));
        handles.insert(uri.to_string(), table.clone());
        Ok(table)
    }
}
