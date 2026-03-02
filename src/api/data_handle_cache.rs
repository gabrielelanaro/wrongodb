use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::store_registry::StoreRegistry;
use crate::storage::table::Table;
use crate::WrongoDBError;

pub struct DataHandleCache {
    registry: Arc<StoreRegistry>,
}

impl DataHandleCache {
    pub fn new(registry: Arc<StoreRegistry>) -> Self {
        Self { registry }
    }

    pub fn get_or_open_primary(
        &self,
        uri: &str,
        collection: &str,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        self.registry.get_or_open_primary(uri, collection)
    }

    pub fn all_handles(&self) -> Vec<Arc<RwLock<Table>>> {
        self.registry.all_handles()
    }
}
