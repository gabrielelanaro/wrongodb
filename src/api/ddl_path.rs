use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::{CollectionCatalog, CreateIndexRequest, IndexDefinition};
use crate::core::errors::StorageError;
use crate::core::Namespace;
use crate::replication::{is_reserved_namespace, ReplicationCoordinator};
use crate::storage::metadata_store::{index_uri, MetadataEntry, MetadataStore};
use crate::storage::row::validate_storage_columns;
use crate::{Connection, WrongoDBError};

#[derive(Debug, Clone)]
struct IndexBuildPlan {
    entry: MetadataEntry,
    needs_build: bool,
}

/// Top-level DDL service above the WT-like storage API and collection catalog.
#[derive(Clone)]
pub(crate) struct DdlPath {
    connection: Arc<Connection>,
    metadata_store: Arc<MetadataStore>,
    catalog: Arc<CollectionCatalog>,
    replication: ReplicationCoordinator,
}

impl DdlPath {
    /// Creates the DDL service.
    pub(crate) fn new(
        connection: Arc<Connection>,
        metadata_store: Arc<MetadataStore>,
        catalog: Arc<CollectionCatalog>,
        replication: ReplicationCoordinator,
    ) -> Self {
        Self {
            connection,
            metadata_store,
            catalog,
            replication,
        }
    }

    /// Creates one collection with an explicit storage schema.
    pub(crate) fn create_collection(
        &self,
        namespace: &Namespace,
        storage_columns: Vec<String>,
    ) -> Result<(), WrongoDBError> {
        self.replication.require_writable_primary()?;
        validate_user_namespace(namespace)?;
        validate_storage_columns(&storage_columns)?;

        let mut session = self.connection.open_session();
        let (_, created) =
            self.catalog
                .create_collection(&mut session, namespace, &storage_columns)?;

        if !created {
            return Err(StorageError(format!(
                "collection already exists: {}",
                namespace.full_name()
            ))
            .into());
        }

        Ok(())
    }

    /// Creates one secondary index from the normalized server-side request.
    pub(crate) fn create_index(
        &self,
        namespace: &Namespace,
        request: CreateIndexRequest,
    ) -> Result<(), WrongoDBError> {
        self.replication.require_writable_primary()?;
        validate_user_namespace(namespace)?;

        let mut session = self.connection.open_session();
        let definition = self.load_collection_definition(&mut session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let indexed_field = request.indexed_field()?;
        if !definition
            .storage_columns()
            .iter()
            .any(|column| column == &indexed_field)
        {
            return Err(StorageError(format!(
                "index field {indexed_field} is not declared in storageColumns for {}",
                namespace.full_name()
            ))
            .into());
        }

        // `createIndexes` is orchestration, not a pure catalog write. The DDL
        // layer must decide whether this request is creating a new index,
        // resuming an unfinished one, or repairing missing storage metadata
        // for an already-ready catalog entry. That planning belongs above the
        // catalog. The catalog should stay focused on durable metadata
        // transitions (`ready=false`, `ready=true`, lookup), while the DDL
        // layer coordinates physical index creation through `Session`.
        let build_plan = self.plan_index_build(
            &mut session,
            namespace,
            &definition,
            &request,
            &indexed_field,
        )?;
        if build_plan.needs_build {
            self.catalog.build_and_mark_index_ready(
                &mut session,
                namespace,
                &table_uri,
                request.name(),
                &build_plan.entry,
            )?;
        }

        Ok(())
    }

    /// Decide whether this index needs a new build or a repair pass.
    fn plan_index_build(
        &self,
        session: &mut crate::storage::api::Session,
        namespace: &Namespace,
        definition: &crate::catalog::CollectionDefinition,
        request: &CreateIndexRequest,
        indexed_field: &str,
    ) -> Result<IndexBuildPlan, WrongoDBError> {
        if let Some(index) = definition.indexes().get(request.name()) {
            let stored_entry = self.metadata_store.get(index.uri())?;
            let metadata_entry = stored_entry.clone().unwrap_or_else(|| {
                MetadataEntry::index(
                    table_ident_from_uri(definition.table_uri()),
                    index_ident_from_uri(index.uri()),
                    vec![indexed_field.to_string()],
                )
            });
            return Ok(IndexBuildPlan {
                entry: metadata_entry,
                needs_build: !index.ready() || stored_entry.is_none(),
            });
        }

        let index_uri = index_uri(
            table_ident_from_uri(definition.table_uri()),
            &new_index_ident(),
        );
        let _ = self.catalog.create_index(
            session,
            namespace,
            IndexDefinition::from_request_with_ready(request, index_uri.clone(), false),
        )?;
        Ok(IndexBuildPlan {
            entry: MetadataEntry::index(
                table_ident_from_uri(definition.table_uri()),
                index_uri
                    .rsplit(':')
                    .next()
                    .expect("new index URI always has an ident suffix"),
                vec![indexed_field.to_string()],
            ),
            needs_build: true,
        })
    }

    fn load_collection_definition(
        &self,
        session: &mut crate::storage::api::Session,
        namespace: &Namespace,
    ) -> Result<crate::catalog::CollectionDefinition, WrongoDBError> {
        self.catalog
            .get_collection(session, namespace)?
            .ok_or_else(|| {
                StorageError(format!("unknown collection: {}", namespace.full_name())).into()
            })
    }
}

fn validate_user_namespace(namespace: &Namespace) -> Result<(), WrongoDBError> {
    if is_reserved_namespace(namespace) {
        return Err(StorageError(format!(
            "collection name {} is reserved for internal replication state",
            namespace.full_name()
        ))
        .into());
    }

    Ok(())
}

fn new_index_ident() -> String {
    format!("i_{}", Uuid::new_v4().simple())
}

fn table_ident_from_uri(table_uri: &str) -> &str {
    table_uri
        .strip_prefix("table:")
        .expect("catalog table URI always has the table: prefix")
}

fn index_ident_from_uri(index_uri: &str) -> &str {
    index_uri
        .rsplit(':')
        .next()
        .expect("catalog index URI always has an ident suffix")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;

    use super::DdlPath;
    use crate::catalog::{CatalogStore, CollectionCatalog, CreateIndexRequest};
    use crate::collection_write_path::CollectionWritePath;
    use crate::core::errors::StorageError;
    use crate::core::{DatabaseName, Namespace};
    use crate::document_query::DocumentQuery;
    use crate::replication::{
        oplog_namespace, OplogMode, OplogStore, ReplicationConfig, ReplicationCoordinator,
        ReplicationObserver,
    };
    use crate::storage::api::{Connection, ConnectionConfig, Session};
    use crate::WrongoDBError;

    const TEST_DB: &str = "test";
    const TEST_OPLOG_TABLE_URI: &str = "table:test_oplog";

    struct TestServices {
        _dir: TempDir,
        connection: Arc<Connection>,
        ddl_path: DdlPath,
        write_path: CollectionWritePath,
        query: DocumentQuery,
        catalog: Arc<CollectionCatalog>,
    }

    impl TestServices {
        fn new(replication_config: ReplicationConfig) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let connection = Arc::new(
                Connection::open(dir.path(), ConnectionConfig::new().logging_enabled(false))
                    .unwrap(),
            );
            let metadata_store = connection.metadata_store();
            let oplog_store = OplogStore::new(metadata_store.clone(), TEST_OPLOG_TABLE_URI);
            let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
            let replication = ReplicationCoordinator::new(replication_config);
            let mut session = connection.open_session();
            oplog_store.ensure_table_exists(&mut session).unwrap();
            let next_op_index = oplog_store
                .load_last_op_time(&mut session)
                .unwrap()
                .map(|op_time| op_time.index + 1)
                .unwrap_or(1);
            replication.seed_next_op_index(next_op_index);
            catalog.ensure_store_exists(&mut session).unwrap();
            catalog.load_cache(&session).unwrap();

            let query = DocumentQuery::new(catalog.clone());
            let write_path = CollectionWritePath::new(
                metadata_store.clone(),
                catalog.clone(),
                query.clone(),
                ReplicationObserver::new(replication.clone(), oplog_store),
            );
            let ddl_path = DdlPath::new(
                connection.clone(),
                metadata_store,
                catalog.clone(),
                replication,
            );

            Self {
                _dir: dir,
                connection,
                ddl_path,
                write_path,
                query,
                catalog,
            }
        }
    }

    fn namespace(collection: &str) -> Namespace {
        Namespace::new(DatabaseName::new(TEST_DB).unwrap(), collection).unwrap()
    }

    fn insert_one(
        write_path: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        doc: serde_json::Value,
    ) {
        session
            .with_transaction(|session| {
                write_path.insert_one_in_transaction(
                    session,
                    &namespace(collection),
                    doc,
                    OplogMode::GenerateOplog,
                )?;
                Ok(())
            })
            .unwrap();
    }

    // EARS: When user DDL targets the reserved oplog collection name, the DDL
    // path shall reject the request.
    #[test]
    fn create_collection_rejects_reserved_oplog_name() {
        let services = TestServices::new(ReplicationConfig::default());

        let err = services
            .ddl_path
            .create_collection(&oplog_namespace(), vec!["term".to_string()])
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("reserved for internal replication state"));
    }

    // EARS: When the node is not writable primary, the DDL path shall reject
    // collection creation and surface the leader hint.
    #[test]
    fn non_primary_rejects_create_collection() {
        let services = TestServices::new(ReplicationConfig {
            is_writable_primary: false,
            primary_hint: Some("node-2".to_string()),
            term: 7,
        });

        let err = services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap_err();

        assert!(matches!(
            err,
            WrongoDBError::NotLeader {
                leader_hint: Some(ref leader_hint)
            } if leader_hint == "node-2"
        ));
    }

    // EARS: When index creation references a field outside `storageColumns`,
    // the DDL path shall reject the request.
    #[test]
    fn create_index_rejects_undeclared_field() {
        let services = TestServices::new(ReplicationConfig::default());
        services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();

        let err = services
            .ddl_path
            .create_index(
                &namespace("users"),
                CreateIndexRequest::single_field_ascending("age"),
            )
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("index field age is not declared in storageColumns"));
    }

    // EARS: When an index is created after documents already exist, the DDL
    // path shall backfill index storage and make indexed reads succeed.
    #[test]
    fn create_index_backfills_existing_documents_and_marks_ready() {
        let services = TestServices::new(ReplicationConfig::default());
        services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();

        let mut session = services.connection.open_session();
        insert_one(
            &services.write_path,
            &mut session,
            "users",
            json!({"_id": 1, "name": "alice"}),
        );

        services
            .ddl_path
            .create_index(
                &namespace("users"),
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();

        let users = services
            .catalog
            .lookup_collection(&namespace("users"))
            .unwrap();
        let name_index = users.indexes().get("name_1").unwrap();
        assert!(name_index.ready());

        session
            .with_transaction(|session| {
                let mut cursor = session.open_index_cursor(name_index.uri())?;
                assert!(cursor.next()?.is_some());
                Ok(())
            })
            .unwrap();

        let docs = services
            .query
            .find(
                &mut session,
                &namespace("users"),
                Some(json!({"name": "alice"})),
            )
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    // EARS: When a ready durable index is missing storage metadata, rerunning
    // createIndex shall repair the physical index and preserve indexed reads.
    #[test]
    fn create_index_repairs_missing_storage_metadata_for_ready_index() {
        let services = TestServices::new(ReplicationConfig::default());
        services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();

        let mut session = services.connection.open_session();
        insert_one(
            &services.write_path,
            &mut session,
            "users",
            json!({"_id": 1, "name": "alice"}),
        );

        services
            .ddl_path
            .create_index(
                &namespace("users"),
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();

        let initial_index_uri = services
            .catalog
            .lookup_collection(&namespace("users"))
            .unwrap()
            .indexes()
            .get("name_1")
            .unwrap()
            .uri()
            .to_string();
        assert!(services
            .ddl_path
            .metadata_store
            .remove(&initial_index_uri)
            .unwrap());

        services
            .ddl_path
            .create_index(
                &namespace("users"),
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();

        assert!(services
            .ddl_path
            .metadata_store
            .get(&initial_index_uri)
            .unwrap()
            .is_some());
        let docs = services
            .query
            .find(
                &mut session,
                &namespace("users"),
                Some(json!({"name": "alice"})),
            )
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    // EARS: When createCollection is called for an existing collection, the
    // DDL path shall reject the duplicate request.
    #[test]
    fn create_collection_reports_duplicate_names() {
        let services = TestServices::new(ReplicationConfig::default());
        services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();

        let err = services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap_err();

        assert!(matches!(err, WrongoDBError::Storage(StorageError(_))));
        assert!(err.to_string().contains("collection already exists"));
    }
}
