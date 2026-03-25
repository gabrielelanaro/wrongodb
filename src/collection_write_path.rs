use std::sync::Arc;

use serde_json::Value;

use crate::catalog::{
    CollectionCatalog, CollectionDefinition, CreateIndexRequest, IndexDefinition,
};
use crate::core::bson::encode_id_value;
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::document_query::DocumentQuery;
use crate::storage::api::Session;
use crate::storage::metadata_store::{index_uri, MetadataEntry, MetadataStore};
use crate::storage::row::{encode_row_value, validate_storage_columns};
use crate::{Document, WrongoDBError};

/// Summary of an update command result.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateResult {
    pub(crate) matched: usize,
    pub(crate) modified: usize,
}

#[derive(Debug, Clone)]
struct IndexRegistration {
    entry: MetadataEntry,
    needs_build: bool,
}

/// Write path for collection CRUD and index registration.
///
/// This service coordinates the storage metadata in [`MetadataStore`], the
/// [`CollectionCatalog`] (the unified catalog), and the [`DocumentQuery`]
/// implementation used for actual record and secondary-index updates.
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

    /// Creates one collection with an explicit storage schema.
    pub(crate) fn create_collection(
        &self,
        session: &mut Session,
        collection: &str,
        storage_columns: Vec<String>,
    ) -> Result<(), WrongoDBError> {
        validate_storage_columns(&storage_columns)?;

        let (_, created) = self.catalog.create_collection(session, collection, &storage_columns)?;

        if !created {
            return Err(StorageError(format!("collection already exists: {collection}")).into());
        }

        Ok(())
    }

    /// Inserts one document into an existing collection.
    pub(crate) fn insert_one(
        &self,
        session: &mut Session,
        collection: &str,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        self.run_in_transaction(session, |this, session| {
            this.insert_one_in_transaction(session, collection, doc)
        })
    }

    /// Updates the first document matching `filter`.
    pub(crate) fn update_one(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.run_in_transaction(session, |this, session| {
            this.update_one_in_transaction(session, collection, filter, update)
        })
    }

    /// Updates every document matching `filter`.
    pub(crate) fn update_many(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.run_in_transaction(session, |this, session| {
            this.update_many_in_transaction(session, collection, filter, update)
        })
    }

    /// Deletes the first document matching `filter`.
    pub(crate) fn delete_one(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.run_in_transaction(session, |this, session| {
            this.delete_one_in_transaction(session, collection, filter)
        })
    }

    /// Deletes every document matching `filter`.
    pub(crate) fn delete_many(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.run_in_transaction(session, |this, session| {
            this.delete_many_in_transaction(session, collection, filter)
        })
    }

    /// Creates one secondary index from the normalized server-side request.
    ///
    /// If the durable catalog already knows about the index but it /// it is still
    /// marked not-ready, this resumes the storage backfill and flips the    /// ready bit once the existing rows are copied into the secondary store.
    pub(crate) fn create_index(
        &self,
        session: &mut Session,
        collection: &str,
        request: CreateIndexRequest,
    ) -> Result<(), WrongoDBError> {
        let definition = self.registered_collection_definition_committed(session, collection)?;
        let table_uri = definition.table_uri().to_string();
        let indexed_field = request.indexed_field()?;
        if !definition
            .storage_columns()
            .iter()
            .any(|column| column == &indexed_field)
        {
            return Err(StorageError(format!(
                "index field {indexed_field} is not declared in storageColumns for {collection}"
            ))
            .into());
        }

        let registration =
            self.ensure_index_registered_committed(session, collection, &request, &indexed_field)?;
        if registration.needs_build {
            self.catalog.build_and_mark_index_ready(
                session,
                collection,
                &table_uri,
                &request.name(),
                &registration.entry,
            )?;
        }
        Ok(())
    }

    /// Inserts one document inside the caller's active transaction.
    pub(crate) fn insert_one_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let mut obj = doc.as_object().expect("validated object").clone();
        normalize_document_in_place(&mut obj)?;

        let definition = self.registered_collection_definition(session, collection)?;
        let table_uri = definition.table_uri().to_string();
        let id = obj
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = self.encode_row_for_table(&table_uri, &obj)?;
        self.insert_record(session, &table_uri, &key, &value)?;
        Ok(obj)
    }

    /// Updates the first matching document inside the caller's active transaction.
    pub(crate) fn update_one_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let definition = self.registered_collection_definition(session, collection)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, collection, filter)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
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

        Ok(UpdateResult {
            matched: 1,
            modified: 1,
        })
    }

    /// Updates every matching document inside the caller's active transaction.
    pub(crate) fn update_many_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let definition = self.registered_collection_definition(session, collection)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, collection, filter)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
            });
        }

        let mut modified = 0;
        for doc in docs {
            let updated_doc = apply_update(&doc, &update)?;
            let id = doc
                .get("_id")
                .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
            let key = encode_id_value(id)?;
            let value = self.encode_row_for_table(&table_uri, &updated_doc)?;
            self.update_record(session, &table_uri, &key, &value)?;
            modified += 1;
        }

        Ok(UpdateResult {
            matched: modified,
            modified,
        })
    }

    /// Deletes the first matching document inside the caller's active transaction.
    pub(crate) fn delete_one_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let definition = self.registered_collection_definition(session, collection)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, collection, filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(0);
        };
        let key = encode_id_value(id)?;
        self.delete_record(session, &table_uri, &key)?;
        Ok(1)
    }

    /// Deletes every matching document inside the caller's active transaction.
    pub(crate) fn delete_many_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let definition = self.registered_collection_definition(session, collection)?;
        let table_uri = definition.table_uri().to_string();
        let docs = self
            .document_query
            .find_in_transaction(session, collection, filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let mut deleted = 0;
        for doc in docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            let key = encode_id_value(id)?;
            self.delete_record(session, &table_uri, &key)?;
            deleted += 1;
        }

        Ok(deleted)
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

    fn ensure_index_registered_committed(
        &self,
        session: &mut Session,
        collection: &str,
        request: &CreateIndexRequest,
        indexed_field: &str,
    ) -> Result<IndexRegistration, WrongoDBError> {
        let index_uri = index_uri(collection, request.name());
        let stored_entry = self.metadata_store.get(&index_uri)?;
        let metadata_entry = stored_entry.clone().unwrap_or_else(|| {
            MetadataEntry::index(collection, request.name(), vec![indexed_field.to_string()])
        });

        if let Some(ready) = self
            .catalog
            .get_collection(session, collection)?
            .and_then(|definition| {
                definition
                    .indexes()
                    .get(request.name())
                    .map(|index| index.ready())
            })
        {
            return Ok(IndexRegistration {
                entry: metadata_entry,
                needs_build: !ready || stored_entry.is_none(),
            });
        }

        let _ = self.catalog.create_index(
            session,
            collection,
            IndexDefinition::from_request_with_ready(request, index_uri.clone(), false),
        )?;
        Ok(IndexRegistration {
            entry: metadata_entry,
            needs_build: true,
        })
    }

    fn registered_collection_definition_committed(
        &self,
        session: &mut Session,
        collection: &str,
    ) -> Result<CollectionDefinition, WrongoDBError> {
        self.catalog
            .get_collection(session, collection)?
            .ok_or_else(|| StorageError(format!("unknown collection: {collection}")).into())
    }

    fn registered_collection_definition(
        &self,
        session: &mut Session,
        collection: &str,
    ) -> Result<CollectionDefinition, WrongoDBError> {
        self.catalog
            .get_collection(session, collection)?
            .ok_or_else(|| StorageError(format!("unknown collection: {collection}")).into())
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

    fn run_in_transaction<R, F>(&self, session: &mut Session, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&Self, &mut Session) -> Result<R, WrongoDBError>,
    {
        session.with_transaction(|session| f(self, session))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;

    use super::apply_update;
    use crate::catalog::{CatalogStore, CollectionCatalog, CreateIndexRequest};
    use crate::collection_write_path::CollectionWritePath;
    use crate::document_query::DocumentQuery;
    use crate::storage::api::{Connection, ConnectionConfig, Session};
    use crate::WrongoDBError;

    fn test_services(
        base_path: std::path::PathBuf,
        config: ConnectionConfig,
    ) -> (
        Connection,
        CollectionWritePath,
        DocumentQuery,
        Arc<CollectionCatalog>,
    ) {
        let connection = Connection::open(&base_path, config).unwrap();
        let metadata_store = connection.metadata_store();
        let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
        let mut session = connection.open_session();
        catalog.ensure_store_exists(&mut session).unwrap();
        catalog.load_cache(&session).unwrap();
        let document_query = DocumentQuery::new(catalog.clone());
        let service = CollectionWritePath::new(
            metadata_store,
            catalog.clone(),
            document_query.clone(),
        );
        (connection, service, document_query, catalog)
    }

    fn disabled_config() -> ConnectionConfig {
        ConnectionConfig::new().logging_enabled(false)
    }

    fn create_collection(
        service: &CollectionWritePath,
        session: &mut Session,
        name: &str,
        storage_columns: &[&str],
    ) {
        service
            .create_collection(
                session,
                name,
                storage_columns
                    .iter()
                    .map(|column| (*column).to_string())
                    .collect(),
            )
            .unwrap();
    }

    #[test]
    fn test_apply_update_set() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$set": {"age": 31}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("age").unwrap().as_i64().unwrap(), 31);
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "alice");
    }

    #[test]
    fn test_apply_update_unset() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$unset": {"age": ""}});
        let result = apply_update(&doc, &update).unwrap();
        assert!(!result.contains_key("age"));
        assert!(result.contains_key("name"));
    }

    #[test]
    fn test_apply_update_inc() {
        let doc = serde_json::from_value(json!({"_id": 1, "counter": 10})).unwrap();
        let update = json!({"$inc": {"counter": 5}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("counter").unwrap().as_f64().unwrap(), 15.0);
    }

    #[test]
    fn test_apply_update_push() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b"]})).unwrap();
        let update = json!({"$push": {"tags": "c"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[2].as_str().unwrap(), "c");
    }

    #[test]
    fn test_apply_update_pull() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b", "c"]})).unwrap();
        let update = json!({"$pull": {"tags": "b"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(!tags.iter().any(|t| t.as_str() == Some("b")));
    }

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

    #[test]
    fn writes_to_unknown_collection_fail() {
        let tmp = tempdir().unwrap();
        let (connection, service, _query, _catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();

        let err = service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap_err();
        assert!(err.to_string().contains("unknown collection: users"));
    }

    #[test]
    fn insert_rejects_undeclared_fields() {
        let tmp = tempdir().unwrap();
        let (connection, service, _query, _catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "users", &["name"]);

        let err = service
            .insert_one(
                &mut session,
                "users",
                json!({"_id": 1, "name": "alice", "age": 30}),
            )
            .unwrap_err();
        assert!(err.to_string().contains("field age is not declared"));
    }

    #[test]
    fn create_index_rejects_undeclared_field() {
        let tmp = tempdir().unwrap();
        let (connection, service, _query, _catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "users", &["name"]);

        let err = service
            .create_index(
                &mut session,
                "users",
                CreateIndexRequest::single_field_ascending("age"),
            )
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("index field age is not declared in storageColumns"));
    }

    #[test]
    fn ready_true_index_repairs_missing_storage_metadata() {
        let tmp = tempdir().unwrap();
        let (connection, service, query, catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "users", &["name"]);

        service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap();
        service
            .create_index(
                &mut session,
                "users",
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();

        assert!(catalog
            .list_index_definitions("users")
            .iter()
            .any(|index| index.name() == "name_1" && index.ready()));
        assert!(service.metadata_store.remove("index:users:name_1").unwrap());

        service
            .create_index(
                &mut session,
                "users",
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();

        assert!(service
            .metadata_store
            .get("index:users:name_1")
            .unwrap()
            .is_some());
        let docs = query
            .find(&mut session, "users", Some(json!({"name": "alice"})))
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn create_index_backfills_existing_documents() {
        let tmp = tempdir().unwrap();
        let (connection, service, _query, _catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "users", &["name"]);

        service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap();
        service
            .create_index(
                &mut session,
                "users",
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();

        session
            .with_transaction(|session| {
                let mut cursor = session.open_index_cursor("index:users:name_1")?;
                let has_entries = cursor.next()?.is_some();
                assert!(has_entries);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn ready_false_indexes_are_ignored_until_backfill_completes() {
        let tmp = tempdir().unwrap();
        let (connection, service, query, catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "users", &["name"]);

        service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap();

        let request = CreateIndexRequest::single_field_ascending("name");
        let registration = service
            .ensure_index_registered_committed(&mut session, "users", &request, "name")
            .unwrap();
        assert!(registration.needs_build);
        assert!(service
            .metadata_store
            .get(registration.entry.uri())
            .unwrap()
            .is_none());

        service
            .create_index(&mut session, "users", request)
            .unwrap();

        let committed_indexes = catalog.list_index_definitions("users");
        assert!(committed_indexes
            .iter()
            .any(|index| index.name() == "name_1" && index.ready()));

        let docs = query
            .find(&mut session, "users", Some(json!({"name": "alice"})))
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn crud_roundtrip() {
        let tmp = tempdir().unwrap();
        let (connection, service, query, _catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "users", &["name", "age"]);

        let inserted = service
            .insert_one(&mut session, "users", json!({"name": "alice", "age": 30}))
            .unwrap();
        let id = inserted.get("_id").unwrap().clone();

        let fetched = query
            .find(&mut session, "users", Some(json!({"_id": id.clone()})))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(fetched.get("name"), Some(&json!("alice")));

        let updated = service
            .update_one(
                &mut session,
                "users",
                Some(json!({"_id": id.clone()})),
                json!({"$set": {"age": 31}}),
            )
            .unwrap();
        assert_eq!(updated.matched, 1);
        assert_eq!(updated.modified, 1);

        let fetched = query
            .find(&mut session, "users", Some(json!({"_id": id.clone()})))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(fetched.get("age"), Some(&json!(31)));

        let deleted = service
            .delete_one(&mut session, "users", Some(json!({"_id": id})))
            .unwrap();
        assert_eq!(deleted, 1);
        assert!(query
            .find(&mut session, "users", Some(json!({"name": "alice"})))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn create_index_registers_index_name() {
        let tmp = tempdir().unwrap();
        let (connection, service, _query, catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "users", &["name"]);

        service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap();
        service
            .create_index(
                &mut session,
                "users",
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();
        assert!(catalog
            .list_index_definitions("users")
            .iter()
            .any(|index| index.name() == "name_1"));
    }

    #[test]
    fn failing_transaction_aborts_changes() {
        let tmp = tempdir().unwrap();
        let (connection, service, query, catalog) =
            test_services(tmp.path().to_path_buf(), disabled_config());
        let mut session = connection.open_session();
        create_collection(&service, &mut session, "items", &["value"]);

        let err = service
            .run_in_transaction(&mut session, |this, session| {
                let _ = this.insert_one_in_transaction(
                    session,
                    "items",
                    json!({"_id": "abort-me", "value": "v1"}),
                )?;
                Err::<(), WrongoDBError>(WrongoDBError::Storage(crate::StorageError(
                    "force abort".into(),
                )))
            })
            .unwrap_err();
        assert!(err.to_string().contains("force abort"));
        assert!(query
            .find(&mut session, "items", Some(json!({"_id": "abort-me"})))
            .unwrap()
            .is_empty());
        assert!(catalog.lookup_collection("items").is_some());
    }
}
