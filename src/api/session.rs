use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::api::cursor::{Cursor, CursorWriteAccess, StoreWriteTracker};
use crate::core::errors::StorageError;
use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp, WritePathMode};
use crate::schema::{CollectionSchema, SchemaCatalog};
use crate::storage::table_cache::TableCache;
use crate::txn::{Transaction, TransactionManager, TxnId};
use crate::WrongoDBError;

#[derive(Debug)]
struct ActiveTxn {
    txn: Transaction,
    touched_stores: Arc<Mutex<HashSet<String>>>,
}

/// A request-scoped execution context over shared connection infrastructure.
///
/// `Connection` owns long-lived shared components (storage handles, schema
/// metadata, global transaction state, and durability machinery). `Session`
/// exists to own mutable per-request state that must not be global, especially
/// the active transaction and document-level orchestration across primary and
/// index stores.
pub struct Session {
    table_cache: Arc<TableCache>,
    schema_catalog: Arc<SchemaCatalog>,
    transaction_manager: Arc<TransactionManager>,
    durability_backend: Arc<DurabilityBackend>,
    write_tracker: StoreWriteTracker,
    active_txn: Option<ActiveTxn>,
}

impl Session {
    pub(crate) fn new(
        table_cache: Arc<TableCache>,
        schema_catalog: Arc<SchemaCatalog>,
        transaction_manager: Arc<TransactionManager>,
        durability_backend: Arc<DurabilityBackend>,
    ) -> Self {
        Self {
            table_cache,
            schema_catalog,
            transaction_manager,
            durability_backend,
            write_tracker: StoreWriteTracker::new(),
            active_txn: None,
        }
    }

    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if let Some(collection) = uri.strip_prefix("table:") {
            let store_name = self.schema_catalog.primary_store_name(collection);
            let _ = self.table_cache.get_or_open_store(&store_name)?;
            return Ok(());
        }

        if let Some(rest) = uri.strip_prefix("index:") {
            let mut parts = rest.splitn(2, ':');
            let collection = parts.next().unwrap_or("");
            let field = parts.next().unwrap_or("");
            if collection.is_empty() || field.is_empty() {
                return Err(WrongoDBError::Storage(StorageError(format!(
                    "invalid index URI: {uri}"
                ))));
            }
            return self.create_index_uri(collection, field);
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI: {uri}"
        ))))
    }

    pub fn open_cursor(&self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let store_name = self.schema_catalog.resolve_uri(uri)?;
        let write_access = match self.write_path_mode() {
            WritePathMode::LocalApply => CursorWriteAccess::ReadWrite,
            WritePathMode::DeferredReplication => CursorWriteAccess::ReadOnly,
        };
        self.open_store_cursor_with_access(&store_name, write_access)
    }

    pub fn transaction(&mut self) -> Result<SessionTxn<'_>, WrongoDBError> {
        if self.active_txn.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }

        let txn = self.transaction_manager.begin_snapshot_txn();
        let touched_stores = Arc::new(Mutex::new(HashSet::new()));
        self.write_tracker.begin(txn.id(), touched_stores.clone());
        self.active_txn = Some(ActiveTxn {
            txn,
            touched_stores,
        });
        Ok(SessionTxn::new(self))
    }

    pub fn current_txn(&self) -> Option<&Transaction> {
        self.active_txn.as_ref().map(|active| &active.txn)
    }

    pub fn current_txn_mut(&mut self) -> Option<&mut Transaction> {
        self.active_txn.as_mut().map(|active| &mut active.txn)
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        for store_name in self.schema_catalog.all_store_names()? {
            let table = self.table_cache.get_or_open_store(&store_name)?;
            table.write().checkpoint_store()?;
        }

        if self.transaction_manager.has_active_transactions()
            || !self.durability_backend.is_enabled()
        {
            return Ok(());
        }

        self.durability_backend
            .record(DurableOp::Checkpoint, DurabilityGuarantee::Sync)?;
        self.durability_backend.truncate_to_checkpoint()
    }

    fn create_index_uri(&mut self, collection: &str, field: &str) -> Result<(), WrongoDBError> {
        let primary_store = self.schema_catalog.primary_store_name(collection);
        let _ = self.table_cache.get_or_open_store(&primary_store)?;

        self.with_txn(|session| {
            let txn_id = session.require_txn_id()?;
            let mut primary_cursor = session.open_cursor(&format!("table:{collection}"))?;
            let Some(store_name) =
                session
                    .schema_catalog
                    .add_index(collection, field, vec![field.to_string()])?
            else {
                return Ok(());
            };

            let _ = session.table_cache.get_or_open_store(&store_name)?;
            while let Some((_, bytes)) = primary_cursor.next(txn_id)? {
                let doc = crate::core::bson::decode_document(&bytes)?;
                let Some(id) = doc.get("_id") else {
                    continue;
                };
                let Some(value) = doc.get(field) else {
                    continue;
                };
                let Some(key) = crate::index::encode_index_key(value, id)? else {
                    continue;
                };
                session.insert_store_value(&store_name, &key, &[], txn_id)?;
            }

            Ok(())
        })
    }

    pub(crate) fn schema_catalog(&self) -> &SchemaCatalog {
        self.schema_catalog.as_ref()
    }

    pub(crate) fn collection_schema(
        &self,
        collection: &str,
    ) -> Result<CollectionSchema, WrongoDBError> {
        self.schema_catalog.collection_schema(collection)
    }

    pub(crate) fn write_path_mode(&self) -> WritePathMode {
        self.durability_backend.write_path_mode()
    }

    fn record_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.durability_backend.record(
            DurableOp::Put {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }

    fn record_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.durability_backend.record(
            DurableOp::Delete {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }

    fn record_commit(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.durability_backend.record(
            DurableOp::TxnCommit {
                txn_id,
                commit_ts: txn_id,
            },
            DurabilityGuarantee::Sync,
        )
    }

    fn record_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.durability_backend.record(
            DurableOp::TxnAbort { txn_id },
            DurabilityGuarantee::Buffered,
        )
    }

    pub(crate) fn store_contains_key(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<bool, WrongoDBError> {
        let table = self.table_cache.get_or_open_store(store_name)?;
        let mut table = table.write();
        table.contains_key(key, txn_id)
    }

    pub(crate) fn insert_store_value(
        &mut self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        match self.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = self.open_local_store_cursor(store_name)?;
                cursor.insert(key, value, txn_id)
            }
            WritePathMode::DeferredReplication => {
                if self.store_contains_key(store_name, key, txn_id)? {
                    return Err(crate::core::errors::DocumentValidationError(
                        "duplicate key error".into(),
                    )
                    .into());
                }
                self.record_put(store_name, key, value, txn_id)
            }
        }
    }

    pub(crate) fn update_store_value(
        &mut self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        match self.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = self.open_local_store_cursor(store_name)?;
                cursor.update(key, value, txn_id)
            }
            WritePathMode::DeferredReplication => {
                if !self.store_contains_key(store_name, key, txn_id)? {
                    return Err(WrongoDBError::Storage(StorageError(
                        "key not found for update".to_string(),
                    )));
                }
                self.record_put(store_name, key, value, txn_id)
            }
        }
    }

    pub(crate) fn delete_store_value(
        &mut self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        match self.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = self.open_local_store_cursor(store_name)?;
                cursor.delete(key, txn_id)
            }
            WritePathMode::DeferredReplication => {
                if !self.store_contains_key(store_name, key, txn_id)? {
                    return Err(WrongoDBError::Storage(StorageError(
                        "key not found for delete".to_string(),
                    )));
                }
                self.record_delete(store_name, key, txn_id)
            }
        }
    }

    pub(crate) fn with_txn<R, F>(&mut self, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&mut Session) -> Result<R, WrongoDBError>,
    {
        if self.current_txn().is_some() {
            return f(self);
        }

        let mut txn = self.transaction()?;
        let result = f(txn.session_mut());
        match result {
            Ok(value) => {
                txn.commit()?;
                Ok(value)
            }
            Err(err) => {
                let _ = txn.abort();
                Err(err)
            }
        }
    }

    fn open_local_store_cursor(&self, store_name: &str) -> Result<Cursor, WrongoDBError> {
        self.open_store_cursor_with_access(store_name, CursorWriteAccess::ReadWrite)
    }

    fn open_store_cursor_with_access(
        &self,
        store_name: &str,
        write_access: CursorWriteAccess,
    ) -> Result<Cursor, WrongoDBError> {
        let table = self.table_cache.get_or_open_store(store_name)?;
        Ok(Cursor::new(
            table,
            store_name.to_string(),
            self.write_tracker.clone(),
            write_access,
        ))
    }

    fn finalize_touched_stores_locally(
        &self,
        touched_stores: &[String],
        txn_id: TxnId,
        committed: bool,
    ) -> Result<(), WrongoDBError> {
        for store_name in touched_stores {
            let table = self.table_cache.get_or_open_store(store_name)?;
            if committed {
                table.write().local_mark_updates_committed(txn_id)?;
            } else {
                table.write().local_mark_updates_aborted(txn_id)?;
            }
        }
        Ok(())
    }

    pub(crate) fn require_txn_id(&self) -> Result<TxnId, WrongoDBError> {
        self.current_txn()
            .map(Transaction::id)
            .ok_or(WrongoDBError::NoActiveTransaction)
    }
}

/// RAII transaction handle for Session.
pub struct SessionTxn<'a> {
    session: &'a mut Session,
    committed: bool,
}

impl<'a> SessionTxn<'a> {
    pub fn commit(mut self) -> Result<(), WrongoDBError> {
        let Some(active) = self.session.active_txn.as_ref() else {
            self.session.write_tracker.clear();
            self.committed = true;
            return Ok(());
        };
        let touched_stores: Vec<String> = active.touched_stores.lock().iter().cloned().collect();
        let txn_id = active.txn.id();

        if self.session.durability_backend.is_enabled() {
            self.session.record_commit(txn_id)?;
        }

        let active =
            self.session.active_txn.as_mut().ok_or_else(|| {
                StorageError("transaction context disappeared during commit".into())
            })?;
        self.session
            .transaction_manager
            .commit_txn_state(&mut active.txn)?;

        self.session.active_txn = None;
        self.session.write_tracker.clear();
        self.committed = true;

        if self.session.write_path_mode() == WritePathMode::LocalApply {
            self.session
                .finalize_touched_stores_locally(&touched_stores, txn_id, true)?;
        }
        Ok(())
    }

    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        let Some(active) = self.session.active_txn.as_ref() else {
            self.session.write_tracker.clear();
            self.committed = true;
            return Ok(());
        };
        let touched_stores: Vec<String> = active.touched_stores.lock().iter().cloned().collect();
        let txn_id = active.txn.id();

        if self.session.durability_backend.is_enabled() {
            self.session.record_abort(txn_id)?;
        }

        let active =
            self.session.active_txn.as_mut().ok_or_else(|| {
                StorageError("transaction context disappeared during abort".into())
            })?;
        self.session
            .transaction_manager
            .abort_txn_state(&mut active.txn)?;

        self.session.active_txn = None;
        self.session.write_tracker.clear();
        self.committed = true;

        if self.session.write_path_mode() == WritePathMode::LocalApply {
            self.session
                .finalize_touched_stores_locally(&touched_stores, txn_id, false)?;
        }
        Ok(())
    }

    pub fn as_mut(&mut self) -> &mut Transaction {
        &mut self
            .session
            .active_txn
            .as_mut()
            .expect("transaction should exist")
            .txn
    }

    pub fn as_ref(&self) -> &Transaction {
        &self
            .session
            .active_txn
            .as_ref()
            .expect("transaction should exist")
            .txn
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.session
    }

    fn new(session: &'a mut Session) -> Self {
        Self {
            session,
            committed: false,
        }
    }
}

impl<'a> Drop for SessionTxn<'a> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        if let Some(mut active) = self.session.active_txn.take() {
            let touched_stores: Vec<String> =
                active.touched_stores.lock().iter().cloned().collect();
            let txn_id = active.txn.id();
            if self.session.durability_backend.is_enabled() {
                let _ = self.session.record_abort(txn_id);
            }
            let _ = self
                .session
                .transaction_manager
                .abort_txn_state(&mut active.txn);
            self.session.write_tracker.clear();
            if self.session.write_path_mode() == WritePathMode::LocalApply {
                let _ =
                    self.session
                        .finalize_touched_stores_locally(&touched_stores, txn_id, false);
            }
        } else {
            self.session.write_tracker.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::tempdir;

    use super::*;
    use crate::api::connection::{Connection, ConnectionConfig};
    use crate::collection_write_path;
    use crate::collection_write_path::UpdateResult;
    use crate::core::bson::{encode_document, encode_id_value};
    use crate::document_query;
    use crate::durability::DurabilityBackend;
    use crate::storage::wal::{WalReader, WalRecord};
    use crate::txn::GlobalTxnState;

    fn new_session_with_backend(backend: DurabilityBackend) -> Session {
        let dir = tempdir().unwrap();
        let base_path = dir.path().to_path_buf();
        std::mem::forget(dir);
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_cache = Arc::new(TableCache::new(
            base_path.clone(),
            transaction_manager.clone(),
        ));
        let schema_catalog = Arc::new(SchemaCatalog::new(base_path));
        Session::new(
            table_cache,
            schema_catalog,
            transaction_manager,
            Arc::new(backend),
        )
    }

    fn read_wal_records(db_dir: &std::path::Path) -> Vec<WalRecord> {
        let wal_path = db_dir.join("global.wal");
        let mut reader = WalReader::open(&wal_path).unwrap();
        let mut records = Vec::new();
        while let Some((_header, record)) = reader.read_record().unwrap() {
            records.push(record);
        }
        records
    }

    fn insert_one(
        session: &mut Session,
        collection: &str,
        doc: serde_json::Value,
    ) -> crate::Document {
        collection_write_path::insert_one(session, collection, doc).unwrap()
    }

    fn find_one(
        session: &mut Session,
        collection: &str,
        filter: serde_json::Value,
    ) -> Option<crate::Document> {
        document_query::find(session, collection, Some(filter))
            .unwrap()
            .into_iter()
            .next()
    }

    fn create_index(session: &mut Session, collection: &str, field: &str) {
        collection_write_path::create_index(session, collection, field).unwrap();
    }

    fn list_indexes(session: &Session, collection: &str) -> Vec<String> {
        document_query::list_indexes(session, collection).unwrap()
    }

    fn update_one(
        session: &mut Session,
        collection: &str,
        filter: serde_json::Value,
        update: serde_json::Value,
    ) -> UpdateResult {
        collection_write_path::update_one(session, collection, Some(filter), update).unwrap()
    }

    fn delete_one(session: &mut Session, collection: &str, filter: serde_json::Value) -> usize {
        collection_write_path::delete_one(session, collection, Some(filter)).unwrap()
    }

    #[test]
    fn checkpoint_skips_truncate_when_transaction_active() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        session.create("table:items").unwrap();

        {
            let mut txn = session.transaction().unwrap();
            insert_one(
                txn.session_mut(),
                "items",
                json!({"_id": "k1", "value": "v1"}),
            );
        }

        let wal_path = dir.path().join("global.wal");
        let before = std::fs::metadata(&wal_path).unwrap().len();
        session.checkpoint().unwrap();
        let after = std::fs::metadata(&wal_path).unwrap().len();

        assert!(after >= before);
        let records = read_wal_records(dir.path());
        assert!(records
            .iter()
            .all(|record| !matches!(record, WalRecord::Checkpoint)));
    }

    #[test]
    fn checkpoint_truncates_when_no_active_transactions() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        session.create("table:items").unwrap();

        {
            let mut txn = session.transaction().unwrap();
            insert_one(
                txn.session_mut(),
                "items",
                json!({"_id": "k1", "value": "v1"}),
            );
            txn.commit().unwrap();
        }

        let wal_path = dir.path().join("global.wal");
        session.checkpoint().unwrap();
        let records = read_wal_records(dir.path());
        assert!(records.is_empty());

        let after = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after <= 512);
    }

    #[test]
    fn local_mode_commit_finalizes_touched_stores() {
        let mut session = new_session_with_backend(DurabilityBackend::Disabled);
        let mut txn = session.transaction().unwrap();
        insert_one(
            txn.session_mut(),
            "items",
            json!({"_id": "k1", "value": "v1"}),
        );

        txn.commit().unwrap();

        let doc = find_one(&mut session, "items", json!({"_id": "k1"})).unwrap();
        assert_eq!(doc.get("value"), Some(&json!("v1")));
    }

    #[test]
    fn local_mode_abort_discards_touched_stores() {
        let mut session = new_session_with_backend(DurabilityBackend::Disabled);
        let mut txn = session.transaction().unwrap();
        insert_one(
            txn.session_mut(),
            "items",
            json!({"_id": "k1", "value": "v1"}),
        );

        txn.abort().unwrap();

        assert!(find_one(&mut session, "items", json!({"_id": "k1"})).is_none());
    }

    #[test]
    fn deferred_mode_records_commit_after_write_ops() {
        let (backend, recorded_ops) =
            DurabilityBackend::test_backend(WritePathMode::DeferredReplication);
        let mut session = new_session_with_backend(backend);
        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        let doc = json!({"_id": "k1", "value": "v1"});
        let encoded_key = encode_id_value(&json!("k1")).unwrap();
        let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();

        insert_one(txn.session_mut(), "items", doc);
        txn.commit().unwrap();

        assert_eq!(
            *recorded_ops.lock(),
            vec![
                (
                    DurableOp::Put {
                        store_name: "items.main.wt".to_string(),
                        key: encoded_key,
                        value: encoded_doc,
                        txn_id,
                    },
                    DurabilityGuarantee::Buffered,
                ),
                (
                    DurableOp::TxnCommit {
                        txn_id,
                        commit_ts: txn_id,
                    },
                    DurabilityGuarantee::Sync,
                ),
            ]
        );
    }

    #[test]
    fn deferred_mode_records_abort_for_explicit_and_drop_abort() {
        let (backend, recorded_ops) =
            DurabilityBackend::test_backend(WritePathMode::DeferredReplication);
        let mut session = new_session_with_backend(backend);

        {
            let mut txn = session.transaction().unwrap();
            let txn_id = txn.as_ref().id();
            let doc = json!({"_id": "abort-me", "value": "v1"});
            let encoded_key = encode_id_value(&json!("abort-me")).unwrap();
            let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();
            insert_one(txn.session_mut(), "items", doc);
            txn.abort().unwrap();

            assert_eq!(
                *recorded_ops.lock(),
                vec![
                    (
                        DurableOp::Put {
                            store_name: "items.main.wt".to_string(),
                            key: encoded_key,
                            value: encoded_doc,
                            txn_id,
                        },
                        DurabilityGuarantee::Buffered,
                    ),
                    (
                        DurableOp::TxnAbort { txn_id },
                        DurabilityGuarantee::Buffered
                    ),
                ]
            );
        }

        recorded_ops.lock().clear();

        {
            let mut dropped = session.transaction().unwrap();
            let txn_id = dropped.as_ref().id();
            let doc = json!({"_id": "drop-me", "value": "v2"});
            let encoded_key = encode_id_value(&json!("drop-me")).unwrap();
            let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();
            insert_one(dropped.session_mut(), "items", doc);
            drop(dropped);

            assert_eq!(
                *recorded_ops.lock(),
                vec![
                    (
                        DurableOp::Put {
                            store_name: "items.main.wt".to_string(),
                            key: encoded_key,
                            value: encoded_doc,
                            txn_id,
                        },
                        DurabilityGuarantee::Buffered,
                    ),
                    (
                        DurableOp::TxnAbort { txn_id },
                        DurabilityGuarantee::Buffered
                    ),
                ]
            );
        }
    }

    #[test]
    fn session_crud_roundtrip() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        let inserted = insert_one(&mut session, "test", json!({"name": "alice", "age": 30}));
        let id = inserted.get("_id").unwrap().clone();

        let fetched = find_one(&mut session, "test", json!({"_id": id.clone()})).unwrap();
        assert_eq!(fetched.get("name").unwrap().as_str().unwrap(), "alice");

        let updated = update_one(
            &mut session,
            "test",
            json!({"_id": id.clone()}),
            json!({"$set": {"age": 31}}),
        );
        assert_eq!(updated.matched, 1);
        assert_eq!(updated.modified, 1);

        let fetched = find_one(&mut session, "test", json!({"_id": id.clone()})).unwrap();
        assert_eq!(fetched.get("age").unwrap().as_i64().unwrap(), 31);

        let deleted = delete_one(&mut session, "test", json!({"_id": id}));
        assert_eq!(deleted, 1);
        assert!(find_one(&mut session, "test", json!({"name": "alice"})).is_none());
    }

    #[test]
    fn session_create_and_list_indexes() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        insert_one(&mut session, "test", json!({"name": "alice"}));
        create_index(&mut session, "test", "name");

        let indexes = list_indexes(&session, "test");
        assert!(indexes.iter().any(|idx| idx == "name"));
    }

    #[test]
    fn checkpoint_preserves_indexed_lookup_after_reconciliation() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        create_index(&mut session, "test", "name");
        insert_one(&mut session, "test", json!({"_id": 1, "name": "alice"}));

        session.checkpoint().unwrap();

        let doc = find_one(&mut session, "test", json!({"name": "alice"})).unwrap();
        assert_eq!(doc.get("name"), Some(&json!("alice")));
    }
}
