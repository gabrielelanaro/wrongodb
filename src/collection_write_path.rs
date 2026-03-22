use std::sync::Arc;

use serde_json::Value;

use crate::catalog::{CollectionCatalog, CreateIndexRequest, DurableCatalog, IndexDefinition};
use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::document_query::DocumentQuery;
use crate::index::encode_index_key;
use crate::storage::api::Session;
use crate::storage::metadata_store::{index_uri, table_uri, MetadataEntry, MetadataStore};
use crate::{Document, WrongoDBError};

/// Summary of an update command result.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateResult {
    pub(crate) matched: usize,
    pub(crate) modified: usize,
}

/// Write path for collection CRUD and index registration.
///
/// This service coordinates the storage metadata in [`MetadataStore`], the
/// durable server-side collection catalog, and the WT-like `TableCursor`
/// implementation used for actual record and secondary-index updates.
#[derive(Clone)]
pub(crate) struct CollectionWritePath {
    metadata_store: Arc<MetadataStore>,
    durable_catalog: Arc<DurableCatalog>,
    collection_catalog: Arc<CollectionCatalog>,
    document_query: DocumentQuery,
}

impl CollectionWritePath {
    /// Creates the collection write service.
    pub(crate) fn new(
        metadata_store: Arc<MetadataStore>,
        durable_catalog: Arc<DurableCatalog>,
        collection_catalog: Arc<CollectionCatalog>,
        document_query: DocumentQuery,
    ) -> Self {
        Self {
            metadata_store,
            durable_catalog,
            collection_catalog,
            document_query,
        }
    }

    /// Inserts one document, auto-creating the collection metadata when needed.
    pub(crate) fn insert_one(
        &self,
        session: &mut Session,
        collection: &str,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        let _ = self.ensure_collection_registered_committed(session, collection)?;
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
        let _ = self.ensure_collection_registered_committed(session, collection)?;
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
        let _ = self.ensure_collection_registered_committed(session, collection)?;
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
        let _ = self.ensure_collection_registered_committed(session, collection)?;
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
        let _ = self.ensure_collection_registered_committed(session, collection)?;
        self.run_in_transaction(session, |this, session| {
            this.delete_many_in_transaction(session, collection, filter)
        })
    }

    /// Creates one secondary index from the normalized server-side request.
    ///
    /// If the durable catalog already knows about the index but it is still
    /// marked not-ready, this resumes the storage backfill and flips the ready
    /// bit once the existing rows are copied into the secondary store.
    pub(crate) fn create_index(
        &self,
        session: &mut Session,
        collection: &str,
        request: CreateIndexRequest,
    ) -> Result<(), WrongoDBError> {
        let (table_uri, _) = self.ensure_collection_registered_committed(session, collection)?;
        let indexed_field = request.indexed_field()?;
        let (index_uri, ready) =
            self.ensure_index_registered_committed(session, collection, &request, &indexed_field)?;
        if !ready {
            self.backfill_index_committed(session, &table_uri, &index_uri, &indexed_field)?;
            self.durable_catalog.set_index_ready_committed(
                session,
                collection,
                request.name(),
                true,
            )?;
            self.refresh_collection_cache(session, collection)?;
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

        let id = obj
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&obj)?;
        let table_uri = self.registered_table_uri(session, collection)?;
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
        let value = encode_document(&updated_doc)?;
        let table_uri = self.registered_table_uri(session, collection)?;
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
        let docs = self
            .document_query
            .find_in_transaction(session, collection, filter)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
            });
        }

        let table_uri = self.registered_table_uri(session, collection)?;
        let mut modified = 0;
        for doc in docs {
            let updated_doc = apply_update(&doc, &update)?;
            let id = doc
                .get("_id")
                .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
            let key = encode_id_value(id)?;
            let value = encode_document(&updated_doc)?;
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
        let table_uri = self.registered_table_uri(session, collection)?;
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
        let docs = self
            .document_query
            .find_in_transaction(session, collection, filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let table_uri = self.registered_table_uri(session, collection)?;
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

    fn ensure_collection_registered_committed(
        &self,
        session: &mut Session,
        collection: &str,
    ) -> Result<(String, bool), WrongoDBError> {
        if let Some(definition) = self.durable_catalog.collection(session, collection)? {
            return Ok((definition.table_uri().to_string(), false));
        }

        let table_uri = table_uri(collection);
        if self.metadata_store.get(&table_uri)?.is_none() {
            let entry = MetadataEntry::table(collection, self.metadata_store.allocate_store_id());
            session.ensure_named_store(entry.source())?;
            self.metadata_store.insert(&entry)?;
        }

        let (_, created_collection) = self
            .durable_catalog
            .insert_collection_if_missing_committed(session, collection, &table_uri)?;
        if created_collection {
            self.refresh_collection_cache(session, collection)?;
        }
        Ok((table_uri, created_collection))
    }

    fn ensure_index_registered_committed(
        &self,
        session: &mut Session,
        collection: &str,
        request: &CreateIndexRequest,
        indexed_field: &str,
    ) -> Result<(String, bool), WrongoDBError> {
        let index_uri = index_uri(collection, request.name());
        if let Some(ready) = self
            .durable_catalog
            .collection(session, collection)?
            .and_then(|definition| {
                definition
                    .indexes()
                    .get(request.name())
                    .map(|index| index.ready())
            })
        {
            if self.metadata_store.get(&index_uri)?.is_none() {
                let entry = MetadataEntry::index(
                    collection,
                    request.name(),
                    vec![indexed_field.to_string()],
                    self.metadata_store.allocate_store_id(),
                );
                session.create_index(&entry)?;
            }

            return Ok((index_uri, ready));
        }

        if self.metadata_store.get(&index_uri)?.is_none() {
            let entry = MetadataEntry::index(
                collection,
                request.name(),
                vec![indexed_field.to_string()],
                self.metadata_store.allocate_store_id(),
            );
            session.create_index(&entry)?;
        }

        let _ = self.durable_catalog.ensure_index_committed(
            session,
            collection,
            IndexDefinition::from_request_with_ready(request, index_uri.clone(), false),
        )?;
        Ok((index_uri, false))
    }

    fn backfill_index_committed(
        &self,
        session: &mut Session,
        table_uri: &str,
        index_uri: &str,
        indexed_field: &str,
    ) -> Result<(), WrongoDBError> {
        let index_entry = self
            .metadata_store
            .get(index_uri)?
            .ok_or_else(|| StorageError(format!("unknown index URI: {index_uri}")))?;
        let store_name = index_entry.source().to_string();
        let store_id = index_entry.store_id();

        session.with_transaction(|session| {
            let mut primary_cursor = session.open_table_cursor(table_uri)?;
            // Use the raw named-store write path so index backfill can be retried
            // without tripping the duplicate-key guard used by normal inserts.
            while let Some((_, bytes)) = primary_cursor.next()? {
                let doc = decode_document(&bytes)?;
                let Some(id) = doc.get("_id") else {
                    continue;
                };
                let Some(value) = doc.get(indexed_field) else {
                    continue;
                };
                let Some(key) = encode_index_key(value, id)? else {
                    continue;
                };
                session.put_into_named_store(&store_name, store_id, &key, &[])?;
            }
            Ok(())
        })
    }

    fn registered_table_uri(
        &self,
        session: &mut Session,
        collection: &str,
    ) -> Result<String, WrongoDBError> {
        self.durable_catalog
            .collection_for_txn(session, collection, session.current_txn_id())?
            .map(|definition| definition.table_uri().to_string())
            .ok_or_else(|| StorageError(format!("unknown collection: {collection}")).into())
    }

    fn refresh_collection_cache(
        &self,
        session: &Session,
        collection: &str,
    ) -> Result<(), WrongoDBError> {
        self.collection_catalog.refresh_collection(
            session,
            self.durable_catalog.as_ref(),
            collection,
        )
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
    use crate::catalog::{CatalogStore, CollectionCatalog, CreateIndexRequest, DurableCatalog};
    use crate::collection_write_path::CollectionWritePath;
    use crate::document_query::DocumentQuery;
    use crate::storage::api::Session;
    use crate::storage::btree::BTreeCursor;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::log_manager::LogManager;
    use crate::storage::metadata_store::MetadataStore;
    use crate::txn::GlobalTxnState;
    use crate::WrongoDBError;

    fn test_services(
        base_path: std::path::PathBuf,
        log_manager: LogManager,
    ) -> (
        CollectionWritePath,
        DocumentQuery,
        Arc<CollectionCatalog>,
        Session,
    ) {
        let global_txn = Arc::new(GlobalTxnState::new());
        let store_handles =
            Arc::new(HandleCache::<String, parking_lot::RwLock<BTreeCursor>>::new());
        let log_manager = Arc::new(log_manager);
        let metadata_store = Arc::new(
            MetadataStore::new(
                base_path.clone(),
                store_handles.clone(),
                global_txn.clone(),
                log_manager.clone(),
            )
            .unwrap(),
        );
        let durable_catalog = Arc::new(DurableCatalog::new(CatalogStore::new()));
        let collection_catalog = Arc::new(CollectionCatalog::new());
        let session = Session::new(
            base_path,
            store_handles,
            metadata_store.clone(),
            global_txn,
            log_manager,
        );
        durable_catalog.ensure_store_exists(&session).unwrap();
        collection_catalog
            .load_from_durable(&session, durable_catalog.as_ref())
            .unwrap();
        let document_query = DocumentQuery::new(durable_catalog.clone());
        let service = CollectionWritePath::new(
            metadata_store,
            durable_catalog,
            collection_catalog.clone(),
            document_query.clone(),
        );
        (service, document_query, collection_catalog, session)
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
    fn create_index_backfills_existing_documents() {
        let tmp = tempdir().unwrap();
        let (service, _query, _collection_catalog, mut session) =
            test_services(tmp.path().to_path_buf(), LogManager::disabled());

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
                let rows = session.scan_store_range("index:users:name_1", None, None)?;
                assert!(!rows.is_empty());
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn ready_false_indexes_are_ignored_until_backfill_completes() {
        let tmp = tempdir().unwrap();
        let (service, query, _collection_catalog, mut session) =
            test_services(tmp.path().to_path_buf(), LogManager::disabled());

        service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap();

        let request = CreateIndexRequest::single_field_ascending("name");
        let (index_uri, ready) = service
            .ensure_index_registered_committed(&mut session, "users", &request, "name")
            .unwrap();
        assert!(!ready);

        session
            .with_transaction(|session| {
                let rows = session.scan_store_range(&index_uri, None, None)?;
                assert!(rows.is_empty());
                Ok(())
            })
            .unwrap();

        service
            .create_index(&mut session, "users", request)
            .unwrap();

        let committed_indexes = _collection_catalog.list_index_definitions("users");
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
        let (service, query, _collection_catalog, mut session) =
            test_services(tmp.path().to_path_buf(), LogManager::disabled());

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
        let (service, _query, collection_catalog, mut session) =
            test_services(tmp.path().to_path_buf(), LogManager::disabled());

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

        let indexes = collection_catalog
            .list_index_definitions("users")
            .into_iter()
            .map(|index| index.name().to_string())
            .collect::<Vec<_>>();
        assert!(indexes.iter().any(|index| index == "name_1"));
        assert!(!tmp.path().join("users.meta.json").exists());
    }

    #[test]
    fn failing_transaction_aborts_changes() {
        let tmp = tempdir().unwrap();
        let (service, query, collection_catalog, mut session) =
            test_services(tmp.path().to_path_buf(), LogManager::disabled());
        let (_table_uri, created_collection) = service
            .ensure_collection_registered_committed(&mut session, "items")
            .unwrap();
        assert!(created_collection);

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
        assert!(collection_catalog.lookup_collection("items").is_some());
    }
}
