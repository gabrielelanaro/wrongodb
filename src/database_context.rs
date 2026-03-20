use std::sync::Arc;

use crate::collection_write_path::CollectionWritePath;
use crate::document_query::DocumentQuery;
use crate::store_write_path::StoreWritePath;
use crate::{Connection, WrongoDBError};

/// Internal database-layer container above the WT-like storage connection.
///
/// `DatabaseContext` groups the non-storage services used by the MongoDB wire
/// protocol handlers. It intentionally sits above [`Connection`]: the storage
/// API stays WT-like while upper layers share query and write orchestration
/// through this crate-private container.
pub(crate) struct DatabaseContext {
    connection: Arc<Connection>,
    document_query: DocumentQuery,
    collection_write_path: CollectionWritePath,
}

impl DatabaseContext {
    pub(crate) fn new(connection: Arc<Connection>) -> Self {
        let schema_catalog = connection.schema_catalog();
        let table_cache = connection.table_cache();
        let durability_backend = connection.durability_backend();
        let replication_coordinator = connection.replication_coordinator();
        let document_query = DocumentQuery::new(schema_catalog.clone());
        let store_write_path =
            StoreWritePath::new(table_cache, durability_backend, replication_coordinator);
        let collection_write_path =
            CollectionWritePath::new(schema_catalog, document_query.clone(), store_write_path);

        Self {
            connection,
            document_query,
            collection_write_path,
        }
    }

    pub(crate) fn connection(&self) -> &Connection {
        self.connection.as_ref()
    }

    pub(crate) fn document_query(&self) -> &DocumentQuery {
        &self.document_query
    }

    pub(crate) fn collection_write_path(&self) -> &CollectionWritePath {
        &self.collection_write_path
    }

    pub(crate) fn hello_state(&self) -> (bool, Option<String>) {
        let state = self.connection.replication_coordinator().hello_state();
        (state.is_writable_primary, state.leader_hint)
    }

    pub(crate) fn list_collections(&self) -> Result<Vec<String>, WrongoDBError> {
        self.connection.schema_catalog().list_collections()
    }
}
