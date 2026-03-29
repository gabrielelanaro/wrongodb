use std::sync::Arc;

use crate::catalog::{CatalogStore, CollectionCatalog, IndexDefinition};
use crate::collection_write_path::CollectionWritePath;
use crate::document_query::DocumentQuery;
use crate::replication::ReplicationCoordinator;
use crate::{Connection, WrongoDBError};

/// Internal database-layer container above the WT-like storage connection.
///
/// `DatabaseContext` groups the non-storage services used by the MongoDB wire
/// protocol handlers. It intentionally sits above [`Connection`]: the storage
/// API stays WT-like while upper layers own query and document-level write
/// orchestration through this crate-private container.
pub(crate) struct DatabaseContext {
    connection: Arc<Connection>,
    catalog: Arc<CollectionCatalog>,
    document_query: DocumentQuery,
    collection_write_path: CollectionWritePath,
    replication: ReplicationCoordinator,
}

impl DatabaseContext {
    /// Builds the server-side services layered above one storage connection.
    pub(crate) fn new(
        connection: Arc<Connection>,
        replication: ReplicationCoordinator,
    ) -> Result<Self, WrongoDBError> {
        let metadata_store = connection.metadata_store();
        let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
        let mut session = connection.open_session();
        catalog.ensure_store_exists(&mut session)?;
        catalog.load_cache(&session)?;

        let document_query = DocumentQuery::new(catalog.clone());
        let collection_write_path =
            CollectionWritePath::new(metadata_store, catalog.clone(), document_query.clone());

        Ok(Self {
            connection,
            catalog,
            document_query,
            collection_write_path,
            replication,
        })
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
        self.replication.hello_state()
    }

    pub(crate) fn list_collections(&self) -> Result<Vec<String>, WrongoDBError> {
        Ok(self.catalog.list_collection_names())
    }

    /// Returns the committed collection definition if the collection exists.
    pub(crate) fn collection_definition(
        &self,
        collection: &str,
    ) -> Result<Option<crate::catalog::CollectionDefinition>, WrongoDBError> {
        Ok(self.catalog.lookup_collection(collection))
    }

    /// Returns the committed secondary index definitions for `collection`.
    pub(crate) fn list_indexes(
        &self,
        collection: &str,
    ) -> Result<Vec<IndexDefinition>, WrongoDBError> {
        Ok(self.catalog.list_index_definitions(collection))
    }
}
