use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::storage::table::Table;
use crate::storage::wal::GlobalWal;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

pub struct DataHandleCache {
    handles: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
    global_wal: Option<Arc<Mutex<GlobalWal>>>,
}

impl DataHandleCache {
    pub fn new(global_wal: Option<Arc<Mutex<GlobalWal>>>) -> Self {
        Self {
            handles: RwLock::new(HashMap::new()),
            global_wal,
        }
    }

    pub fn get_or_open_primary(
        &self,
        uri: &str,
        collection: &str,
        db_dir: &Path,
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

        let table = Arc::new(RwLock::new(Table::open_or_create_primary(
            collection,
            db_dir,
            wal_enabled,
            global_txn,
            self.global_wal.clone(),
        )?));
        handles.insert(uri.to_string(), table.clone());
        Ok(table)
    }

    pub fn all_handles(&self) -> Vec<Arc<RwLock<Table>>> {
        self.handles.read().values().cloned().collect()
    }
}
