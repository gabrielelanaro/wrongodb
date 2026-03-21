use std::sync::Arc;

use crate::collection_write_path::CollectionWritePath;
use crate::document_query::DocumentQuery;
use crate::schema::SchemaCatalog;
use crate::{Connection, WrongoDBError};

/// Internal database-layer container above the WT-like storage connection.
///
/// `DatabaseContext` groups the non-storage services used by the MongoDB wire
/// protocol handlers. It intentionally sits above [`Connection`]: the storage
/// API stays WT-like while upper layers own query and document-level write
/// orchestration through this crate-private container.
pub(crate) struct DatabaseContext {
    connection: Arc<Connection>,
    schema_catalog: Arc<SchemaCatalog>,
    document_query: DocumentQuery,
    collection_write_path: CollectionWritePath,
}

impl DatabaseContext {
    pub(crate) fn new(connection: Arc<Connection>) -> Self {
        let metadata_catalog = connection.metadata_catalog();
        let schema_catalog = Arc::new(SchemaCatalog::new(
            connection.base_path().to_path_buf(),
            metadata_catalog.clone(),
        ));
        let document_query = DocumentQuery::new(schema_catalog.clone());
        let collection_write_path = CollectionWritePath::new(
            metadata_catalog,
            schema_catalog.clone(),
            document_query.clone(),
        );

        Self {
            connection,
            schema_catalog,
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
        (true, None)
    }

    pub(crate) fn list_collections(&self) -> Result<Vec<String>, WrongoDBError> {
        self.schema_catalog.list_collections()
    }
}
