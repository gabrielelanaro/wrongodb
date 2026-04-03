use std::sync::Arc;

use bson::{spec::BinarySubtype, Binary};
use uuid::Uuid;

use crate::catalog::{
    CollectionCatalog, CollectionDefinition, CreateIndexRequest, IndexDefinition,
};
use crate::core::errors::StorageError;
use crate::core::Namespace;
use crate::replication::{
    is_reserved_namespace, OplogMode, ReplicationCoordinator, ReplicationObserver,
};
use crate::storage::metadata_store::{index_uri, MetadataEntry, MetadataStore};
use crate::storage::row::validate_storage_columns;
use crate::{Connection, WrongoDBError};

#[derive(Debug, Clone)]
struct IndexBuildPlan {
    entry: MetadataEntry,
    needs_build: bool,
    should_emit_oplog: bool,
    definition_before_build: Option<CollectionDefinition>,
    definition_after_build: CollectionDefinition,
}

/// Top-level DDL service above the WT-like storage API and collection catalog.
#[derive(Clone)]
pub(crate) struct DdlPath {
    connection: Arc<Connection>,
    metadata_store: Arc<MetadataStore>,
    catalog: Arc<CollectionCatalog>,
    replication: ReplicationCoordinator,
    replication_observer: ReplicationObserver,
}

impl DdlPath {
    /// Creates the DDL service.
    pub(crate) fn new(
        connection: Arc<Connection>,
        metadata_store: Arc<MetadataStore>,
        catalog: Arc<CollectionCatalog>,
        replication: ReplicationCoordinator,
        replication_observer: ReplicationObserver,
    ) -> Self {
        Self {
            connection,
            metadata_store,
            catalog,
            replication,
            replication_observer,
        }
    }

    /// Creates one collection with an explicit storage schema.
    pub(crate) fn create_collection(
        &self,
        namespace: &Namespace,
        storage_columns: Vec<String>,
    ) -> Result<(), WrongoDBError> {
        self.replication.require_writable_primary()?;
        self.create_collection_internal(
            namespace,
            storage_columns,
            None,
            OplogMode::GenerateOplog,
            false,
        )
    }

    /// Apply a replicated create-collection oplog entry locally.
    pub(crate) fn apply_create_collection(
        &self,
        namespace: &Namespace,
        storage_columns: Vec<String>,
        collection_uuid: String,
    ) -> Result<(), WrongoDBError> {
        self.create_collection_internal(
            namespace,
            storage_columns,
            Some(collection_uuid),
            OplogMode::SuppressOplog,
            true,
        )
    }

    /// Creates one secondary index from the normalized server-side request.
    pub(crate) fn create_index(
        &self,
        namespace: &Namespace,
        request: CreateIndexRequest,
    ) -> Result<(), WrongoDBError> {
        self.replication.require_writable_primary()?;
        self.create_index_internal(namespace, request, OplogMode::GenerateOplog)
    }

    /// Apply a replicated create-index oplog entry locally.
    pub(crate) fn apply_create_index(
        &self,
        namespace: &Namespace,
        name: &str,
        indexed_field: &str,
    ) -> Result<(), WrongoDBError> {
        self.create_index_internal(
            namespace,
            CreateIndexRequest::single_field_ascending_named(indexed_field, name),
            OplogMode::SuppressOplog,
        )
    }

    fn create_collection_internal(
        &self,
        namespace: &Namespace,
        storage_columns: Vec<String>,
        collection_uuid: Option<String>,
        oplog_mode: OplogMode,
        allow_existing_match: bool,
    ) -> Result<(), WrongoDBError> {
        validate_user_namespace(namespace)?;
        validate_storage_columns(&storage_columns)?;

        let mut session = self.connection.open_session();
        if let Some(existing) = self.catalog.get_collection(&session, namespace)? {
            return self.handle_existing_collection(
                namespace,
                &storage_columns,
                collection_uuid.as_deref(),
                &existing,
                allow_existing_match,
            );
        }

        let definition = if let Some(collection_uuid) = collection_uuid.clone() {
            CollectionDefinition::new_generated_with_uuid(
                namespace.clone(),
                storage_columns.clone(),
                uuid_binary_from_string(&collection_uuid)?,
            )
        } else {
            CollectionDefinition::new_generated(namespace.clone(), storage_columns.clone())
        };
        let collection_uuid = collection_uuid_string(definition.uuid())?;
        session.create_table(definition.table_uri(), storage_columns.clone())?;

        session.with_transaction(|session| {
            let created = self.catalog.create_collection(session, &definition)?;
            if !created {
                let existing = self.load_collection_definition(session, namespace)?;
                return self.handle_existing_collection(
                    namespace,
                    &storage_columns,
                    Some(collection_uuid.as_str()),
                    &existing,
                    allow_existing_match,
                );
            }

            let _ = self.replication_observer.on_create_collection(
                session,
                namespace,
                storage_columns.clone(),
                collection_uuid.clone(),
                oplog_mode,
            )?;
            Ok(())
        })
    }

    fn create_index_internal(
        &self,
        namespace: &Namespace,
        request: CreateIndexRequest,
        oplog_mode: OplogMode,
    ) -> Result<(), WrongoDBError> {
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

        let build_plan = self.plan_index_build(&definition, &request, &indexed_field)?;
        if !build_plan.needs_build {
            return Ok(());
        }

        if let Some(definition_to_persist) = &build_plan.definition_before_build {
            session.with_transaction(|session| {
                self.catalog.put_collection(session, definition_to_persist)
            })?;
        }

        session.create_index(&table_uri, &build_plan.entry)?;
        session.with_transaction(|session| {
            let mut ready_definition = build_plan.definition_after_build.clone();
            ready_definition.mark_index_ready(request.name(), true)?;
            self.catalog.put_collection(session, &ready_definition)?;
            if build_plan.should_emit_oplog {
                let _ = self.replication_observer.on_create_index(
                    session,
                    namespace,
                    request.name().to_string(),
                    indexed_field.clone(),
                    oplog_mode,
                )?;
            }
            Ok(())
        })
    }

    /// Decide whether this index needs a new build or a repair pass.
    fn plan_index_build(
        &self,
        definition: &CollectionDefinition,
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
                should_emit_oplog: !index.ready(),
                definition_before_build: None,
                definition_after_build: definition.clone(),
            });
        }

        let index_uri = index_uri(
            table_ident_from_uri(definition.table_uri()),
            &new_index_ident(),
        );
        let mut definition_before_build = definition.clone();
        let added = definition_before_build.add_index(IndexDefinition::from_request_with_ready(
            request,
            index_uri.clone(),
            false,
        ));
        debug_assert!(
            added,
            "new index build should not duplicate an existing name"
        );
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
            should_emit_oplog: true,
            definition_before_build: Some(definition_before_build.clone()),
            definition_after_build: definition_before_build,
        })
    }

    fn handle_existing_collection(
        &self,
        namespace: &Namespace,
        storage_columns: &[String],
        collection_uuid: Option<&str>,
        existing: &CollectionDefinition,
        allow_existing_match: bool,
    ) -> Result<(), WrongoDBError> {
        if allow_existing_match
            && existing.storage_columns() == storage_columns
            && collection_uuid
                .map(|expected| {
                    matches!(
                        collection_uuid_string(existing.uuid()),
                        Ok(actual) if actual == expected
                    )
                })
                .unwrap_or(true)
        {
            return Ok(());
        }

        Err(StorageError(format!(
            "collection already exists: {}",
            namespace.full_name()
        ))
        .into())
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

fn collection_uuid_string(uuid: &Binary) -> Result<String, WrongoDBError> {
    let parsed = Uuid::from_slice(&uuid.bytes)
        .map_err(|err| StorageError(format!("invalid collection UUID bytes: {err}")))?;
    Ok(parsed.to_string())
}

fn uuid_binary_from_string(raw: &str) -> Result<Binary, WrongoDBError> {
    let uuid = Uuid::parse_str(raw)
        .map_err(|err| StorageError(format!("invalid collection UUID: {err}")))?;
    Ok(Binary {
        subtype: BinarySubtype::Uuid,
        bytes: uuid.as_bytes().to_vec(),
    })
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
        oplog_namespace, OplogOperation, OplogStore, ReplicationConfig, ReplicationCoordinator,
        ReplicationObserver, ReplicationRole,
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
        oplog_store: OplogStore,
    }

    impl TestServices {
        fn new(ddl_replication_config: ReplicationConfig) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let connection = Arc::new(
                Connection::open(dir.path(), ConnectionConfig::new().logging_enabled(false))
                    .unwrap(),
            );
            let metadata_store = connection.metadata_store();
            let oplog_store = OplogStore::new(metadata_store.clone(), TEST_OPLOG_TABLE_URI);
            let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
            let ddl_replication = ReplicationCoordinator::new(ddl_replication_config);
            let mut session = connection.open_session();
            oplog_store.ensure_table_exists(&mut session).unwrap();
            let next_op_index = oplog_store
                .load_last_op_time(&mut session)
                .unwrap()
                .map(|op_time| op_time.index + 1)
                .unwrap_or(1);
            ddl_replication.seed_next_op_index(next_op_index);
            catalog.ensure_store_exists(&mut session).unwrap();
            catalog.load_cache(&session).unwrap();

            let query = DocumentQuery::new(catalog.clone());
            let write_path =
                CollectionWritePath::new(metadata_store.clone(), catalog.clone(), query.clone());
            let ddl_path = DdlPath::new(
                connection.clone(),
                metadata_store,
                catalog.clone(),
                ddl_replication.clone(),
                ReplicationObserver::new(ddl_replication, oplog_store.clone()),
            );

            Self {
                _dir: dir,
                connection,
                ddl_path,
                write_path,
                query,
                catalog,
                oplog_store,
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
                let _ = write_path.insert_one(session, &namespace(collection), doc)?;
                Ok(())
            })
            .unwrap();
    }

    // EARS: When user DDL targets a reserved replication namespace, the DDL
    // path shall reject the request.
    #[test]
    fn create_collection_rejects_reserved_replication_name() {
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
            role: ReplicationRole::Secondary,
            node_name: "node-2".to_string(),
            primary_hint: Some("node-1".to_string()),
            sync_source_uri: Some("mongodb://node-1".to_string()),
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
            } if leader_hint == "node-1"
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

    // EARS: When a primary creates a collection or index, the DDL path shall
    // append matching logical DDL entries to the oplog.
    #[test]
    fn ddl_operations_append_logical_oplog_entries() {
        let services = TestServices::new(ReplicationConfig::default());
        services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();
        services
            .ddl_path
            .create_index(
                &namespace("users"),
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();

        let mut session = services.connection.open_session();
        let entries = services.oplog_store.list_entries(&mut session).unwrap();

        assert!(matches!(
            entries[0].operation,
            OplogOperation::CreateCollection { .. }
        ));
        assert!(matches!(
            entries[1].operation,
            OplogOperation::CreateIndex { .. }
        ));
    }
}
