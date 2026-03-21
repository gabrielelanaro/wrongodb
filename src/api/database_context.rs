use std::sync::Arc;

use crate::collection_write_path::CollectionWritePath;
use crate::document_query::DocumentQuery;
use crate::replication::ReplicationCoordinator;
use crate::store_write_path::StoreWritePath;
use crate::{Connection, WrongoDBError};

/// Internal database-layer container above the WT-like storage connection.
///
/// `DatabaseContext` groups the non-storage services used by the MongoDB wire
/// protocol handlers. It intentionally sits above [`Connection`]: the storage
/// API stays WT-like while upper layers own replication coordination, query,
/// and write orchestration through this crate-private container.
pub(crate) struct DatabaseContext {
    connection: Arc<Connection>,
    document_query: DocumentQuery,
    collection_write_path: CollectionWritePath,
    replication_coordinator: Arc<ReplicationCoordinator>,
}

impl DatabaseContext {
    pub(crate) fn new(
        connection: Arc<Connection>,
        replication_coordinator: Arc<ReplicationCoordinator>,
    ) -> Self {
        let metadata_catalog = connection.metadata_catalog();
        let schema_catalog = connection.schema_catalog();
        let table_handles = connection.table_handles();
        let durability_backend = connection.durability_backend();
        let document_query = DocumentQuery::new(schema_catalog.clone());
        let store_write_path = StoreWritePath::new(
            connection.base_path().to_path_buf(),
            table_handles,
            connection.transaction_manager(),
            durability_backend,
            replication_coordinator.clone(),
        );
        let collection_write_path = CollectionWritePath::new(
            metadata_catalog,
            schema_catalog,
            document_query.clone(),
            store_write_path,
        );

        Self {
            connection,
            document_query,
            collection_write_path,
            replication_coordinator,
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
        let state = self.replication_coordinator.hello_state();
        (state.is_writable_primary, state.leader_hint)
    }

    pub(crate) fn list_collections(&self) -> Result<Vec<String>, WrongoDBError> {
        self.connection.schema_catalog().list_collections()
    }
}
