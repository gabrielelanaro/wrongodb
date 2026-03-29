use std::sync::Arc;

use crate::api::DdlPath;
use crate::catalog::{CatalogStore, CollectionCatalog, IndexDefinition};
use crate::collection_write_path::CollectionWritePath;
use crate::core::{DatabaseName, Namespace};
use crate::document_query::DocumentQuery;
use crate::replication::{bootstrap_oplog, ReplicationCoordinator, ReplicationObserver};
use crate::write_ops::WriteOps;
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
    ddl_path: DdlPath,
    write_ops: WriteOps,
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
        drop(session);

        let oplog_store = bootstrap_oplog(connection.as_ref(), catalog.as_ref(), &replication)?;

        let session = connection.open_session();
        catalog.load_cache(&session)?;

        let document_query = DocumentQuery::new(catalog.clone());
        let replication_observer = ReplicationObserver::new(replication.clone(), oplog_store);
        let collection_write_path = CollectionWritePath::new(
            metadata_store.clone(),
            catalog.clone(),
            document_query.clone(),
            replication_observer,
        );
        let ddl_path = DdlPath::new(
            connection.clone(),
            metadata_store,
            catalog.clone(),
            replication.clone(),
        );
        let write_ops = WriteOps::new(
            connection.clone(),
            collection_write_path.clone(),
            replication.clone(),
        );

        Ok(Self {
            connection,
            catalog,
            document_query,
            ddl_path,
            write_ops,
            replication,
        })
    }

    pub(crate) fn connection(&self) -> &Connection {
        self.connection.as_ref()
    }

    pub(crate) fn document_query(&self) -> &DocumentQuery {
        &self.document_query
    }

    pub(crate) fn ddl_path(&self) -> &DdlPath {
        &self.ddl_path
    }

    pub(crate) fn write_ops(&self) -> &WriteOps {
        &self.write_ops
    }

    pub(crate) fn hello_state(&self) -> (bool, Option<String>) {
        self.replication.hello_state()
    }

    pub(crate) fn list_databases(&self) -> Vec<DatabaseName> {
        self.catalog.list_database_names()
    }

    pub(crate) fn list_collections(
        &self,
        db_name: &DatabaseName,
    ) -> Result<Vec<String>, WrongoDBError> {
        Ok(self.catalog.list_collection_names(db_name))
    }

    /// Returns the committed collection definition if the collection exists.
    pub(crate) fn collection_definition(
        &self,
        namespace: &Namespace,
    ) -> Result<Option<crate::catalog::CollectionDefinition>, WrongoDBError> {
        Ok(self.catalog.lookup_collection(namespace))
    }

    /// Returns the committed secondary index definitions for `collection`.
    pub(crate) fn list_indexes(
        &self,
        namespace: &Namespace,
    ) -> Result<Vec<IndexDefinition>, WrongoDBError> {
        Ok(self.catalog.list_index_definitions(namespace))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;

    use super::DatabaseContext;
    use crate::core::{DatabaseName, Namespace};
    use crate::replication::{
        oplog_namespace, OplogStore, ReplicationConfig, ReplicationCoordinator,
    };
    use crate::storage::api::{Connection, ConnectionConfig};

    const FIXTURE_DATABASE: &str = "app";

    fn fixture_namespace(collection: &str) -> Namespace {
        Namespace::new(DatabaseName::new(FIXTURE_DATABASE).unwrap(), collection).unwrap()
    }

    // EARS: When the database context restarts after a committed oplog entry,
    // startup shall reseed the next oplog index from the durable oplog tail.
    #[test]
    fn restart_reseeds_next_oplog_index_from_durable_tail() {
        let dir = tempdir().unwrap();

        {
            let connection =
                Arc::new(Connection::open(dir.path(), ConnectionConfig::default()).unwrap());
            let db = DatabaseContext::new(
                connection,
                ReplicationCoordinator::new(ReplicationConfig::default()),
            )
            .unwrap();
            db.ddl_path()
                .create_collection(&fixture_namespace("users"), vec!["name".to_string()])
                .unwrap();
            db.write_ops()
                .insert_one(
                    &fixture_namespace("users"),
                    json!({"_id": 1, "name": "alice"}),
                )
                .unwrap();
        }

        let reopened = Arc::new(Connection::open(dir.path(), ConnectionConfig::default()).unwrap());
        let db = DatabaseContext::new(
            reopened.clone(),
            ReplicationCoordinator::new(ReplicationConfig::default()),
        )
        .unwrap();
        db.write_ops()
            .insert_one(
                &fixture_namespace("users"),
                json!({"_id": 2, "name": "bob"}),
            )
            .unwrap();

        let oplog_table_uri = db
            .collection_definition(&oplog_namespace())
            .unwrap()
            .unwrap()
            .table_uri()
            .to_string();
        let oplog_store = OplogStore::new(reopened.metadata_store(), oplog_table_uri);
        let mut session = reopened.open_session();
        let entries = oplog_store.list_entries(&mut session).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op_time.index, 1);
        assert_eq!(entries[1].op_time.index, 2);
    }

    // EARS: When two databases define collections with the same collection
    // name, the database context shall keep their catalog entries and storage
    // idents separate.
    #[test]
    fn namespace_scoped_catalog_keeps_same_named_collections_separate() {
        let dir = tempdir().unwrap();
        let connection =
            Arc::new(Connection::open(dir.path(), ConnectionConfig::default()).unwrap());
        let db = DatabaseContext::new(
            connection,
            ReplicationCoordinator::new(ReplicationConfig::default()),
        )
        .unwrap();

        let foo_users = Namespace::new(DatabaseName::new("foo").unwrap(), "users").unwrap();
        let bar_users = Namespace::new(DatabaseName::new("bar").unwrap(), "users").unwrap();

        db.ddl_path()
            .create_collection(&foo_users, vec!["name".to_string()])
            .unwrap();
        db.ddl_path()
            .create_collection(&bar_users, vec!["name".to_string()])
            .unwrap();

        let database_names = db
            .list_databases()
            .into_iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        assert_eq!(database_names, vec!["bar", "foo", "local"]);

        let foo_definition = db.collection_definition(&foo_users).unwrap().unwrap();
        let bar_definition = db.collection_definition(&bar_users).unwrap().unwrap();
        assert_ne!(foo_definition.table_uri(), bar_definition.table_uri());
    }
}
