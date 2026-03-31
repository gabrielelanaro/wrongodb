use std::sync::Arc;

use serde_json::Value;

use crate::catalog::{CollectionCatalog, CollectionDefinition};
use crate::core::bson::encode_id_value;
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::core::Namespace;
use crate::document_query::DocumentQuery;
use crate::storage::api::Session;
use crate::storage::metadata_store::MetadataStore;
use crate::storage::row::encode_row_value;
use crate::{Document, WrongoDBError};

/// Summary of an update command result.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateResult {
    pub(crate) matched: usize,
    pub(crate) modified: usize,
}

/// Result of updating at most one document.
#[derive(Debug, Clone)]
pub(crate) struct UpdateOneOutcome {
    pub(crate) result: UpdateResult,
    pub(crate) updated_document: Option<Document>,
}

/// Result of updating multiple documents.
#[derive(Debug, Clone)]
pub(crate) struct UpdateManyOutcome {
    pub(crate) result: UpdateResult,
    pub(crate) updated_documents: Vec<Document>,
}

/// Result of deleting one or more documents.
#[derive(Debug, Clone)]
pub(crate) struct DeleteOutcome {
    pub(crate) deleted: usize,
    pub(crate) deleted_ids: Vec<Value>,
}

/// Low-level in-transaction collection mutator.
///
/// This module mirrors MongoDB's `collection_write_path` role more closely than
/// the old WrongoDB service shape: it owns document validation, row encoding,
/// and collection-local mutations, but it does not own top-level transaction
/// scope, oplog generation, replication replay semantics, or DDL orchestration.
#[derive(Clone)]
pub(crate) struct CollectionWritePath {
    metadata_store: Arc<MetadataStore>,
    catalog: Arc<CollectionCatalog>,
    document_query: DocumentQuery,
}

impl CollectionWritePath {
    /// Creates the collection write service.
    pub(crate) fn new(
        metadata_store: Arc<MetadataStore>,
        catalog: Arc<CollectionCatalog>,
        document_query: DocumentQuery,
    ) -> Self {
        Self {
            metadata_store,
            catalog,
            document_query,
        }
    }

    /// Inserts one document using the caller's active transaction.
    pub(crate) fn insert_one(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let mut obj = doc.as_object().expect("validated object").clone();
        normalize_document_in_place(&mut obj)?;

        let definition = self.registered_collection_definition(session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let id = obj
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = self.encode_row_for_table(&table_uri, &obj)?;
        self.insert_record(session, &table_uri, &key, &value)?;
        Ok(obj)
    }

    /// Updates the first matching document using the caller's active transaction.
    pub(crate) fn update_one(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateOneOutcome, WrongoDBError> {
        let definition = self.registered_collection_definition(session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, namespace, filter)?;
        if docs.is_empty() {
            return Ok(UpdateOneOutcome {
                result: UpdateResult {
                    matched: 0,
                    modified: 0,
                },
                updated_document: None,
            });
        }

        let doc = &docs[0];
        let updated_doc = apply_update(doc, &update)?;
        let id = doc
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = self.encode_row_for_table(&table_uri, &updated_doc)?;
        self.update_record(session, &table_uri, &key, &value)?;
        Ok(UpdateOneOutcome {
            result: UpdateResult {
                matched: 1,
                modified: 1,
            },
            updated_document: Some(updated_doc),
        })
    }

    /// Updates every matching document using the caller's active transaction.
    pub(crate) fn update_many(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateManyOutcome, WrongoDBError> {
        let definition = self.registered_collection_definition(session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, namespace, filter)?;
        if docs.is_empty() {
            return Ok(UpdateManyOutcome {
                result: UpdateResult {
                    matched: 0,
                    modified: 0,
                },
                updated_documents: Vec::new(),
            });
        }

        let mut modified = 0;
        let mut updated_documents = Vec::new();
        for doc in docs {
            let updated_doc = apply_update(&doc, &update)?;
            let id = doc
                .get("_id")
                .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
            let key = encode_id_value(id)?;
            let value = self.encode_row_for_table(&table_uri, &updated_doc)?;
            self.update_record(session, &table_uri, &key, &value)?;
            modified += 1;
            updated_documents.push(updated_doc);
        }

        Ok(UpdateManyOutcome {
            result: UpdateResult {
                matched: modified,
                modified,
            },
            updated_documents,
        })
    }

    /// Deletes the first matching document using the caller's active transaction.
    pub(crate) fn delete_one(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<DeleteOutcome, WrongoDBError> {
        let definition = self.registered_collection_definition(session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, namespace, filter)?;
        if docs.is_empty() {
            return Ok(DeleteOutcome {
                deleted: 0,
                deleted_ids: Vec::new(),
            });
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(DeleteOutcome {
                deleted: 0,
                deleted_ids: Vec::new(),
            });
        };
        let key = encode_id_value(id)?;
        self.delete_record(session, &table_uri, &key)?;
        Ok(DeleteOutcome {
            deleted: 1,
            deleted_ids: vec![id.clone()],
        })
    }

    /// Deletes every matching document using the caller's active transaction.
    pub(crate) fn delete_many(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<DeleteOutcome, WrongoDBError> {
        let definition = self.registered_collection_definition(session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, namespace, filter)?;
        if docs.is_empty() {
            return Ok(DeleteOutcome {
                deleted: 0,
                deleted_ids: Vec::new(),
            });
        }

        let mut deleted = 0;
        let mut deleted_ids = Vec::new();
        for doc in docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            let key = encode_id_value(id)?;
            self.delete_record(session, &table_uri, &key)?;
            deleted += 1;
            deleted_ids.push(id.clone());
        }

        Ok(DeleteOutcome {
            deleted,
            deleted_ids,
        })
    }

    /// Replaces one document by `_id` using the caller's active transaction.
    pub(crate) fn replace_one(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        document: &Document,
    ) -> Result<(), WrongoDBError> {
        let definition = self.registered_collection_definition(session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let key = encode_id_value(document_id(document)?)?;
        let value = self.encode_row_for_table(&table_uri, document)?;
        self.update_record(session, &table_uri, &key, &value)
    }

    /// Deletes one document by `_id` using the caller's active transaction.
    pub(crate) fn delete_by_id(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        id: &Value,
    ) -> Result<(), WrongoDBError> {
        let definition = self.registered_collection_definition(session, namespace)?;
        let table_uri = definition.table_uri().to_string();
        let key = encode_id_value(id)?;
        self.delete_record(session, &table_uri, &key)
    }

    fn insert_record(
        &self,
        session: &mut Session,
        uri: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let mut cursor = session.open_table_cursor(uri)?;
        cursor.insert(key, value)
    }

    fn update_record(
        &self,
        session: &mut Session,
        uri: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let mut cursor = session.open_table_cursor(uri)?;
        cursor.update(key, value)
    }

    fn delete_record(
        &self,
        session: &mut Session,
        uri: &str,
        key: &[u8],
    ) -> Result<(), WrongoDBError> {
        let mut cursor = session.open_table_cursor(uri)?;
        cursor.delete(key)
    }

    fn registered_collection_definition(
        &self,
        session: &mut Session,
        namespace: &Namespace,
    ) -> Result<CollectionDefinition, WrongoDBError> {
        self.catalog
            .get_collection(session, namespace)?
            .ok_or_else(|| {
                StorageError(format!("unknown collection: {}", namespace.full_name())).into()
            })
    }

    fn encode_row_for_table(
        &self,
        table_uri: &str,
        doc: &Document,
    ) -> Result<Vec<u8>, WrongoDBError> {
        let table_entry = self
            .metadata_store
            .get(table_uri)?
            .ok_or_else(|| StorageError(format!("unknown table URI: {table_uri}")))?;
        let row_format = table_entry.row_format().ok_or_else(|| {
            StorageError(format!(
                "table metadata row is missing row_format: {table_uri}"
            ))
        })?;
        encode_row_value(row_format, table_entry.value_columns(), doc)
    }
}

fn apply_update(doc: &Document, update: &Value) -> Result<Document, WrongoDBError> {
    let update_obj = match update.as_object() {
        Some(obj) => obj,
        None => return Ok(doc.clone()),
    };

    let is_update_operators = update_obj.keys().any(|k| k.starts_with('$'));

    if !is_update_operators {
        let mut new_doc = update_obj.clone();
        if let Some(id) = doc.get("_id") {
            new_doc.insert("_id".to_string(), id.clone());
        }
        return Ok(new_doc);
    }

    let mut new_doc = doc.clone();

    if let Some(Value::Object(set_fields)) = update_obj.get("$set") {
        for (k, v) in set_fields {
            new_doc.insert(k.clone(), v.clone());
        }
    }

    if let Some(Value::Object(unset_fields)) = update_obj.get("$unset") {
        for k in unset_fields.keys() {
            new_doc.remove(k);
        }
    }

    if let Some(Value::Object(inc_fields)) = update_obj.get("$inc") {
        for (k, v) in inc_fields {
            if let Some(inc_val) = v.as_f64() {
                let current = new_doc
                    .get(k)
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0);
                new_doc.insert(
                    k.clone(),
                    Value::Number(
                        serde_json::Number::from_f64(current + inc_val)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
            }
        }
    }

    if let Some(Value::Object(push_fields)) = update_obj.get("$push") {
        for (k, v) in push_fields {
            let arr = new_doc
                .entry(k.clone())
                .or_insert_with(|| Value::Array(vec![]));
            if let Value::Array(arr_vec) = arr {
                arr_vec.push(v.clone());
            }
        }
    }

    if let Some(Value::Object(pull_fields)) = update_obj.get("$pull") {
        for (k, v) in pull_fields {
            if let Some(Value::Array(arr)) = new_doc.get_mut(k) {
                arr.retain(|item| item != v);
            }
        }
    }

    Ok(new_doc)
}

fn document_id(document: &Document) -> Result<&Value, WrongoDBError> {
    document
        .get("_id")
        .ok_or_else(|| DocumentValidationError("missing _id".into()).into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;

    use super::apply_update;
    use crate::api::DdlPath;
    use crate::catalog::{CatalogStore, CollectionCatalog};
    use crate::collection_write_path::CollectionWritePath;
    use crate::core::{DatabaseName, Namespace};
    use crate::document_query::DocumentQuery;
    use crate::replication::{
        OplogStore, ReplicationConfig, ReplicationCoordinator, ReplicationObserver,
    };
    use crate::storage::api::{Connection, ConnectionConfig, Session};
    use crate::WrongoDBError;

    const TEST_DB: &str = "test";
    const TEST_OPLOG_TABLE_URI: &str = "table:test_oplog";

    struct TestServices {
        connection: Arc<Connection>,
        ddl_path: DdlPath,
        write_path: CollectionWritePath,
        query: DocumentQuery,
        catalog: Arc<CollectionCatalog>,
        oplog_store: OplogStore,
    }

    fn test_services(base_path: std::path::PathBuf, config: ConnectionConfig) -> TestServices {
        let connection = Arc::new(Connection::open(&base_path, config).unwrap());
        let metadata_store = connection.metadata_store();
        let oplog_store = OplogStore::new(metadata_store.clone(), TEST_OPLOG_TABLE_URI);
        let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
        let replication = ReplicationCoordinator::new(ReplicationConfig::default());
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
        let document_query = DocumentQuery::new(catalog.clone());
        let service = CollectionWritePath::new(
            metadata_store.clone(),
            catalog.clone(),
            document_query.clone(),
        );
        let ddl_path = DdlPath::new(
            connection.clone(),
            metadata_store,
            catalog.clone(),
            replication.clone(),
            ReplicationObserver::new(replication, oplog_store.clone()),
        );

        TestServices {
            connection,
            ddl_path,
            write_path: service,
            query: document_query,
            catalog,
            oplog_store,
        }
    }

    fn namespace(collection: &str) -> Namespace {
        Namespace::new(DatabaseName::new(TEST_DB).unwrap(), collection).unwrap()
    }

    fn disabled_config() -> ConnectionConfig {
        ConnectionConfig::new().logging_enabled(false)
    }

    fn create_collection(ddl_path: &DdlPath, name: &str, storage_columns: &[&str]) {
        ddl_path
            .create_collection(
                &namespace(name),
                storage_columns
                    .iter()
                    .map(|column| (*column).to_string())
                    .collect(),
            )
            .unwrap();
    }

    fn insert_one(
        service: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        doc: serde_json::Value,
    ) -> Result<crate::Document, WrongoDBError> {
        session.with_transaction(|session| service.insert_one(session, &namespace(collection), doc))
    }

    fn update_one(
        service: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        filter: Option<serde_json::Value>,
        update: serde_json::Value,
    ) -> Result<super::UpdateResult, WrongoDBError> {
        session.with_transaction(|session| {
            service
                .update_one(session, &namespace(collection), filter, update)
                .map(|outcome| outcome.result)
        })
    }

    fn delete_one(
        service: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        filter: Option<serde_json::Value>,
    ) -> Result<usize, WrongoDBError> {
        session.with_transaction(|session| {
            service
                .delete_one(session, &namespace(collection), filter)
                .map(|outcome| outcome.deleted)
        })
    }

    // EARS: When a `$set` update is applied, the updater shall replace the
    // targeted field and preserve unrelated fields.
    #[test]
    fn test_apply_update_set() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$set": {"age": 31}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("age").unwrap().as_i64().unwrap(), 31);
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "alice");
    }

    // EARS: When a `$unset` update is applied, the updater shall remove the
    // targeted field and preserve unrelated fields.
    #[test]
    fn test_apply_update_unset() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$unset": {"age": ""}});
        let result = apply_update(&doc, &update).unwrap();
        assert!(!result.contains_key("age"));
        assert!(result.contains_key("name"));
    }

    // EARS: When a `$inc` update is applied to a numeric field, the updater
    // shall store the incremented numeric value.
    #[test]
    fn test_apply_update_inc() {
        let doc = serde_json::from_value(json!({"_id": 1, "counter": 10})).unwrap();
        let update = json!({"$inc": {"counter": 5}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("counter").unwrap().as_f64().unwrap(), 15.0);
    }

    // EARS: When a `$push` update targets an array field, the updater shall
    // append the new value to the end of the array.
    #[test]
    fn test_apply_update_push() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b"]})).unwrap();
        let update = json!({"$push": {"tags": "c"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[2].as_str().unwrap(), "c");
    }

    // EARS: When a `$pull` update targets an array field, the updater shall
    // remove matching values and preserve non-matching ones.
    #[test]
    fn test_apply_update_pull() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b", "c"]})).unwrap();
        let update = json!({"$pull": {"tags": "b"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(!tags.iter().any(|t| t.as_str() == Some("b")));
    }

    // EARS: When a replacement update is applied, the updater shall replace
    // user fields while preserving the existing `_id`.
    #[test]
    fn test_apply_update_replacement() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"name": "bob", "status": "active"});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "bob");
        assert_eq!(result.get("status").unwrap().as_str().unwrap(), "active");
        assert!(!result.contains_key("age"));
        assert!(result.contains_key("_id"));
    }

    // EARS: When a write targets an unknown collection, the collection write
    // path shall reject the operation.
    #[test]
    fn writes_to_unknown_collection_fail() {
        let tmp = tempdir().unwrap();
        let services = test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = services.connection.open_session();

        let err = insert_one(
            &services.write_path,
            &mut session,
            "users",
            json!({"_id": 1, "name": "alice"}),
        )
        .unwrap_err();
        assert!(err.to_string().contains(&format!(
            "unknown collection: {}",
            namespace("users").full_name()
        )));
    }

    // EARS: When an inserted document contains fields outside the declared
    // storage schema, the collection write path shall reject the document.
    #[test]
    fn insert_rejects_undeclared_fields() {
        let tmp = tempdir().unwrap();
        let services = test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = services.connection.open_session();
        create_collection(&services.ddl_path, "users", &["name"]);

        let err = insert_one(
            &services.write_path,
            &mut session,
            "users",
            json!({"_id": 1, "name": "alice", "age": 30}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("field age is not declared"));
    }

    // EARS: When a document is inserted, updated, and deleted through the low-
    // level write path, each operation shall become visible through the read path.
    #[test]
    fn crud_roundtrip() {
        let tmp = tempdir().unwrap();
        let services = test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = services.connection.open_session();
        create_collection(&services.ddl_path, "users", &["name", "age"]);

        let inserted = insert_one(
            &services.write_path,
            &mut session,
            "users",
            json!({"name": "alice", "age": 30}),
        )
        .unwrap();
        let id = inserted.get("_id").unwrap().clone();

        let fetched = services
            .query
            .find(
                &mut session,
                &namespace("users"),
                Some(json!({"_id": id.clone()})),
            )
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(fetched.get("name"), Some(&json!("alice")));

        let updated = update_one(
            &services.write_path,
            &mut session,
            "users",
            Some(json!({"_id": id.clone()})),
            json!({"$set": {"age": 31}}),
        )
        .unwrap();
        assert_eq!(updated.matched, 1);
        assert_eq!(updated.modified, 1);

        let fetched = services
            .query
            .find(
                &mut session,
                &namespace("users"),
                Some(json!({"_id": id.clone()})),
            )
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(fetched.get("age"), Some(&json!(31)));

        let deleted = delete_one(
            &services.write_path,
            &mut session,
            "users",
            Some(json!({"_id": id})),
        )
        .unwrap();
        assert_eq!(deleted, 1);
        assert!(services
            .query
            .find(
                &mut session,
                &namespace("users"),
                Some(json!({"name": "alice"}))
            )
            .unwrap()
            .is_empty());
    }

    // EARS: When a transaction aborts after staging a user write, the write
    // path shall leave neither user data nor oplog entries behind.
    #[test]
    fn failing_transaction_aborts_changes() {
        let tmp = tempdir().unwrap();
        let services = test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = services.connection.open_session();
        create_collection(&services.ddl_path, "items", &["value"]);

        let err = session
            .with_transaction(|session| {
                let _ = services.write_path.insert_one(
                    session,
                    &namespace("items"),
                    json!({"_id": "abort-me", "value": "v1"}),
                )?;
                Err::<(), WrongoDBError>(WrongoDBError::Storage(crate::StorageError(
                    "force abort".into(),
                )))
            })
            .unwrap_err();
        assert!(err.to_string().contains("force abort"));
        assert!(services
            .query
            .find(
                &mut session,
                &namespace("items"),
                Some(json!({"_id": "abort-me"}))
            )
            .unwrap()
            .is_empty());
        assert_eq!(
            services
                .oplog_store
                .list_entries(&mut session)
                .unwrap()
                .len(),
            1
        );
        assert!(services
            .catalog
            .lookup_collection(&namespace("items"))
            .is_some());
    }

    // EARS: When a collection mutation goes through the collection write path,
    // the write path shall mutate user data without appending oplog entries on
    // its own.
    #[test]
    fn collection_write_path_mutates_data_without_appending_oplog_entries() {
        let tmp = tempdir().unwrap();
        let services = test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = services.connection.open_session();
        create_collection(&services.ddl_path, "items", &["value"]);

        session
            .with_transaction(|session| {
                let _ = services.write_path.insert_one(
                    session,
                    &namespace("items"),
                    json!({"_id": "no-oplog", "value": "v1"}),
                )?;
                Ok::<(), WrongoDBError>(())
            })
            .unwrap();

        let docs = services
            .query
            .find(
                &mut session,
                &namespace("items"),
                Some(json!({"_id": "no-oplog"})),
            )
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("value"), Some(&json!("v1")));
        assert_eq!(
            services
                .oplog_store
                .list_entries(&mut session)
                .unwrap()
                .len(),
            1
        );
    }
}
