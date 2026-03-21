use std::sync::Arc;

use serde_json::Value;

use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::core::errors::DocumentValidationError;
use crate::document_query::DocumentQuery;
use crate::index::encode_index_key;
use crate::replication::WritePathMode;
use crate::schema::SchemaCatalog;
use crate::storage::api::{Session, WriteUnitOfWork};
use crate::storage::metadata_catalog::{table_uri, MetadataCatalog};
use crate::store_write_path::StoreWritePath;
use crate::{Document, WrongoDBError};

#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateResult {
    pub(crate) matched: usize,
    pub(crate) modified: usize,
}

#[derive(Clone)]
pub(crate) struct CollectionWritePath {
    metadata_catalog: Arc<MetadataCatalog>,
    schema_catalog: Arc<SchemaCatalog>,
    document_query: DocumentQuery,
    store_write_path: StoreWritePath,
}

impl CollectionWritePath {
    pub(crate) fn new(
        metadata_catalog: Arc<MetadataCatalog>,
        schema_catalog: Arc<SchemaCatalog>,
        document_query: DocumentQuery,
        store_write_path: StoreWritePath,
    ) -> Self {
        Self {
            metadata_catalog,
            schema_catalog,
            document_query,
            store_write_path,
        }
    }

    pub(crate) fn insert_one(
        &self,
        session: &mut Session,
        collection: &str,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        self.run_in_write_unit(session, |this, write_unit| {
            this.insert_one_in_write_unit(write_unit, collection, doc)
        })
    }

    pub(crate) fn update_one(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.run_in_write_unit(session, |this, write_unit| {
            this.update_one_in_write_unit(write_unit, collection, filter, update)
        })
    }

    pub(crate) fn update_many(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.run_in_write_unit(session, |this, write_unit| {
            this.update_many_in_write_unit(write_unit, collection, filter, update)
        })
    }

    pub(crate) fn delete_one(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.run_in_write_unit(session, |this, write_unit| {
            this.delete_one_in_write_unit(write_unit, collection, filter)
        })
    }

    pub(crate) fn delete_many(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.run_in_write_unit(session, |this, write_unit| {
            this.delete_many_in_write_unit(write_unit, collection, filter)
        })
    }

    pub(crate) fn create_index(
        &self,
        session: &mut Session,
        collection: &str,
        field: &str,
    ) -> Result<(), WrongoDBError> {
        self.run_in_write_unit(session, |this, write_unit| {
            this.create_index_in_write_unit(write_unit, collection, field)
        })?;
        let _ = self
            .schema_catalog
            .add_index(collection, field, vec![field.to_string()])?;
        Ok(())
    }

    pub(crate) fn insert_one_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
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
        let primary_uri = self.ensure_table_registered_in_write_unit(write_unit, collection)?;
        self.store_write_path
            .insert(write_unit, &primary_uri, &key, &value)?;
        self.apply_index_add(write_unit, collection, &obj)?;
        Ok(obj)
    }

    pub(crate) fn update_one_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let docs = self
            .document_query
            .find_in_write_unit(write_unit, collection, filter)?;
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
        let primary_uri = self.ensure_table_registered_in_write_unit(write_unit, collection)?;
        self.store_write_path
            .update(write_unit, &primary_uri, &key, &value)?;

        self.apply_index_remove(write_unit, collection, doc)?;
        self.apply_index_add(write_unit, collection, &updated_doc)?;

        Ok(UpdateResult {
            matched: 1,
            modified: 1,
        })
    }

    pub(crate) fn update_many_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let docs = self
            .document_query
            .find_in_write_unit(write_unit, collection, filter)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
            });
        }

        let primary_uri = self.ensure_table_registered_in_write_unit(write_unit, collection)?;
        let mut modified = 0;
        for doc in docs {
            let updated_doc = apply_update(&doc, &update)?;
            let id = doc
                .get("_id")
                .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
            let key = encode_id_value(id)?;
            let value = encode_document(&updated_doc)?;
            self.store_write_path
                .update(write_unit, &primary_uri, &key, &value)?;

            self.apply_index_remove(write_unit, collection, &doc)?;
            self.apply_index_add(write_unit, collection, &updated_doc)?;
            modified += 1;
        }

        Ok(UpdateResult {
            matched: modified,
            modified,
        })
    }

    pub(crate) fn delete_one_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let docs = self
            .document_query
            .find_in_write_unit(write_unit, collection, filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(0);
        };
        let key = encode_id_value(id)?;
        let primary_uri = self.ensure_table_registered_in_write_unit(write_unit, collection)?;
        self.store_write_path
            .delete(write_unit, &primary_uri, &key)?;
        self.apply_index_remove(write_unit, collection, doc)?;
        Ok(1)
    }

    pub(crate) fn delete_many_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let docs = self
            .document_query
            .find_in_write_unit(write_unit, collection, filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let primary_uri = self.ensure_table_registered_in_write_unit(write_unit, collection)?;
        let mut deleted = 0;
        for doc in docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            let key = encode_id_value(id)?;
            self.store_write_path
                .delete(write_unit, &primary_uri, &key)?;
            self.apply_index_remove(write_unit, collection, &doc)?;
            deleted += 1;
        }

        Ok(deleted)
    }

    pub(crate) fn create_index_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        field: &str,
    ) -> Result<(), WrongoDBError> {
        self.ensure_table_registered_in_write_unit(write_unit, collection)?;

        let (index_uri, inserted) = self
            .metadata_catalog
            .ensure_index_uri_in_write_unit(write_unit, collection, field)?;
        if !inserted {
            return Ok(());
        }

        let mut primary_cursor = write_unit.open_cursor(&table_uri(collection))?;

        while let Some((_, bytes)) = primary_cursor.next()? {
            let doc = decode_document(&bytes)?;
            let Some(id) = doc.get("_id") else {
                continue;
            };
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = encode_index_key(value, id)? else {
                continue;
            };
            self.store_write_path
                .insert(write_unit, &index_uri, &key, &[])?;
        }

        Ok(())
    }

    fn apply_index_add(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        doc: &Document,
    ) -> Result<(), WrongoDBError> {
        self.apply_index_doc(write_unit, collection, doc, true)
    }

    fn apply_index_remove(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        doc: &Document,
    ) -> Result<(), WrongoDBError> {
        self.apply_index_doc(write_unit, collection, doc, false)
    }

    fn apply_index_doc(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        doc: &Document,
        is_add: bool,
    ) -> Result<(), WrongoDBError> {
        let Some(id) = doc.get("_id") else {
            return Ok(());
        };
        for def in self.schema_catalog.index_definitions(collection)? {
            let Some(field) = def.columns.first() else {
                continue;
            };
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = encode_index_key(value, id)? else {
                continue;
            };
            if is_add {
                self.store_write_path
                    .insert(write_unit, &def.uri, &key, &[])?;
            } else {
                self.store_write_path.delete(write_unit, &def.uri, &key)?;
            }
        }
        Ok(())
    }

    fn ensure_table_registered_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
    ) -> Result<String, WrongoDBError> {
        self.metadata_catalog
            .ensure_table_uri_in_write_unit(write_unit, collection)
    }

    fn run_in_write_unit<R, F>(&self, session: &mut Session, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&Self, &mut WriteUnitOfWork<'_>) -> Result<R, WrongoDBError>,
    {
        let write_path_mode = self.store_write_path.write_path_mode();
        let mut write_unit = session.transaction()?;
        let txn_id = write_unit.txn_id();
        let result = f(self, &mut write_unit);
        match result {
            Ok(value) => {
                if write_path_mode == WritePathMode::DeferredReplication {
                    if let Err(err) = self.store_write_path.record_commit(txn_id) {
                        let _ = self.store_write_path.record_abort(txn_id);
                        let _ = write_unit.abort();
                        return Err(err);
                    }
                }
                write_unit.commit()?;
                Ok(value)
            }
            Err(err) => {
                if write_path_mode == WritePathMode::DeferredReplication {
                    let _ = self.store_write_path.record_abort(txn_id);
                }
                let _ = write_unit.abort();
                Err(err)
            }
        }
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
    use std::net::TcpListener;
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;

    use super::apply_update;
    use crate::api::DatabaseContext;
    use crate::collection_write_path::CollectionWritePath;
    use crate::core::bson::{encode_document, encode_id_value};
    use crate::document_query::DocumentQuery;
    use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp};
    use crate::replication::{RaftMode, RaftPeerConfig, ReplicationCoordinator, WritePathMode};
    use crate::schema::SchemaCatalog;
    use crate::storage::api::{Connection, ConnectionConfig, Session};
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::metadata_catalog::MetadataCatalog;
    use crate::storage::table::Table;
    use crate::store_write_path::StoreWritePath;
    use crate::txn::{GlobalTxnState, TransactionManager};
    use crate::WrongoDBError;

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr.to_string()
    }

    fn test_services(
        base_path: std::path::PathBuf,
        backend: DurabilityBackend,
    ) -> (CollectionWritePath, DocumentQuery, Session) {
        test_services_with_replication(base_path, backend, ReplicationCoordinator::standalone())
    }

    fn test_services_with_replication(
        base_path: std::path::PathBuf,
        backend: DurabilityBackend,
        replication: ReplicationCoordinator,
    ) -> (CollectionWritePath, DocumentQuery, Session) {
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_handles = Arc::new(HandleCache::<String, parking_lot::RwLock<Table>>::new());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.clone(),
            table_handles.clone(),
            transaction_manager.clone(),
        ));
        let schema_catalog = Arc::new(SchemaCatalog::new(
            base_path.clone(),
            metadata_catalog.clone(),
        ));
        let backend = Arc::new(backend);
        let replication = Arc::new(replication);
        let document_query = DocumentQuery::new(schema_catalog.clone());
        let store_write_path = StoreWritePath::new(backend.clone(), replication.clone());
        let service = CollectionWritePath::new(
            metadata_catalog.clone(),
            schema_catalog.clone(),
            document_query.clone(),
            store_write_path,
        );
        let session = Session::new(
            base_path,
            table_handles,
            metadata_catalog,
            transaction_manager,
            backend,
        );
        (service, document_query, session)
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
    fn clustered_collection_write_path_rejects_writes_when_not_leader() {
        let dir = tempdir().unwrap();
        let raft_mode = RaftMode::Cluster {
            local_node_id: "n1".to_string(),
            local_raft_addr: free_local_addr(),
            peers: vec![RaftPeerConfig {
                node_id: "n2".to_string(),
                raft_addr: free_local_addr(),
            }],
        };
        let conn = Connection::open(dir.path(), ConnectionConfig::new(true, raft_mode.clone()));
        let conn = Arc::new(conn.unwrap());
        let replication_coordinator = Arc::new(
            ReplicationCoordinator::open(
                dir.path(),
                true,
                conn.new_store_command_applier(),
                raft_mode,
            )
            .unwrap(),
        );
        let db = DatabaseContext::new(conn, replication_coordinator);
        let service = db.collection_write_path();
        let mut session = db.connection().open_session();

        let err = service
            .insert_one(&mut session, "items", json!({"_id": "k1", "value": "v1"}))
            .unwrap_err();
        match err {
            WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn create_index_backfills_existing_documents() {
        let tmp = tempdir().unwrap();
        let (service, _query, mut session) =
            test_services(tmp.path().to_path_buf(), DurabilityBackend::Disabled);

        service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap();
        service.create_index(&mut session, "users", "name").unwrap();

        let mut wuow = session.transaction().unwrap();
        let mut cursor = wuow.open_cursor("index:users:name").unwrap();
        assert!(cursor.next().unwrap().is_some());
        wuow.commit().unwrap();
    }

    #[test]
    fn crud_roundtrip() {
        let tmp = tempdir().unwrap();
        let (service, query, mut session) =
            test_services(tmp.path().to_path_buf(), DurabilityBackend::Disabled);

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
        let (service, query, mut session) =
            test_services(tmp.path().to_path_buf(), DurabilityBackend::Disabled);

        service
            .insert_one(&mut session, "users", json!({"_id": 1, "name": "alice"}))
            .unwrap();
        service.create_index(&mut session, "users", "name").unwrap();

        let indexes = query.list_indexes("users").unwrap();
        assert!(indexes.iter().any(|index| index == "name"));
    }

    #[test]
    fn deferred_mode_records_put_and_commit_markers() {
        let (replication, recorded_ops) =
            ReplicationCoordinator::test_backend(WritePathMode::DeferredReplication);
        let tmp = tempdir().unwrap();
        let (service, _query, mut session) = test_services_with_replication(
            tmp.path().to_path_buf(),
            DurabilityBackend::Disabled,
            replication,
        );
        let doc = json!({"_id": "k1", "value": "v1"});
        let encoded_key = encode_id_value(&json!("k1")).unwrap();
        let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();

        service.insert_one(&mut session, "items", doc).unwrap();

        assert_eq!(
            *recorded_ops.lock(),
            vec![
                (
                    DurableOp::Put {
                        uri: "table:items".to_string(),
                        key: encoded_key,
                        value: encoded_doc,
                        txn_id: 1,
                    },
                    DurabilityGuarantee::Buffered,
                ),
                (
                    DurableOp::TxnCommit {
                        txn_id: 1,
                        commit_ts: 1,
                    },
                    DurabilityGuarantee::Sync,
                ),
            ]
        );
    }

    #[test]
    fn deferred_mode_records_put_and_abort_markers_for_explicit_and_drop() {
        let (replication, recorded_ops) =
            ReplicationCoordinator::test_backend(WritePathMode::DeferredReplication);
        let tmp = tempdir().unwrap();
        let (service, _query, mut session) = test_services_with_replication(
            tmp.path().to_path_buf(),
            DurabilityBackend::Disabled,
            replication,
        );

        {
            let doc = json!({"_id": "abort-me", "value": "v1"});
            let encoded_key = encode_id_value(&json!("abort-me")).unwrap();
            let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();
            let err = service
                .run_in_write_unit(&mut session, |this, write_unit| {
                    this.insert_one_in_write_unit(write_unit, "items", doc)?;
                    Err::<(), WrongoDBError>(WrongoDBError::Storage(crate::StorageError(
                        "force abort".into(),
                    )))
                })
                .unwrap_err();
            assert!(err.to_string().contains("force abort"));

            assert_eq!(
                *recorded_ops.lock(),
                vec![
                    (
                        DurableOp::Put {
                            uri: "table:items".to_string(),
                            key: encoded_key,
                            value: encoded_doc,
                            txn_id: 1,
                        },
                        DurabilityGuarantee::Buffered,
                    ),
                    (
                        DurableOp::TxnAbort { txn_id: 1 },
                        DurabilityGuarantee::Buffered
                    ),
                ]
            );
        }

        recorded_ops.lock().clear();

        {
            let doc = json!({"_id": "drop-me", "value": "v2"});
            let encoded_key = encode_id_value(&json!("drop-me")).unwrap();
            let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();
            let err = service
                .run_in_write_unit(&mut session, |this, write_unit| {
                    this.insert_one_in_write_unit(write_unit, "items", doc)?;
                    Err::<(), WrongoDBError>(WrongoDBError::Storage(crate::StorageError(
                        "force drop".into(),
                    )))
                })
                .unwrap_err();
            assert!(err.to_string().contains("force drop"));

            assert_eq!(
                *recorded_ops.lock(),
                vec![
                    (
                        DurableOp::Put {
                            uri: "table:items".to_string(),
                            key: encoded_key,
                            value: encoded_doc,
                            txn_id: 2,
                        },
                        DurabilityGuarantee::Buffered,
                    ),
                    (
                        DurableOp::TxnAbort { txn_id: 2 },
                        DurabilityGuarantee::Buffered
                    ),
                ]
            );
        }
    }
}
