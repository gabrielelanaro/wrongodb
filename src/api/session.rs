use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;

use crate::api::cursor::{Cursor, StoreWriteTracker};
use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::core::errors::StorageError;
use crate::document_ops::update::apply_update;
use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp};
use crate::hooks::MutationHooks;
use crate::index::{decode_index_id, encode_index_key, encode_range_bounds};
use crate::schema::SchemaCatalog;
use crate::storage::table_cache::TableCache;
use crate::txn::{Transaction, TransactionManager, TxnId};
use crate::{Document, WrongoDBError};

#[derive(Debug)]
struct ActiveTxn {
    txn: Transaction,
    touched_stores: Arc<Mutex<HashSet<String>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct UpdateResult {
    pub matched: usize,
    pub modified: usize,
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
    mutation_hooks: Arc<dyn MutationHooks>,
    write_tracker: StoreWriteTracker,
    active_txn: Option<ActiveTxn>,
}

impl Session {
    pub(crate) fn new(
        table_cache: Arc<TableCache>,
        schema_catalog: Arc<SchemaCatalog>,
        transaction_manager: Arc<TransactionManager>,
        durability_backend: Arc<DurabilityBackend>,
        mutation_hooks: Arc<dyn MutationHooks>,
    ) -> Self {
        Self {
            table_cache,
            schema_catalog,
            transaction_manager,
            durability_backend,
            mutation_hooks,
            write_tracker: StoreWriteTracker::new(),
            active_txn: None,
        }
    }

    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if !uri.starts_with("table:") {
            return Err(WrongoDBError::Storage(StorageError(format!(
                "unsupported URI: {uri}"
            ))));
        }

        let store_name = self.schema_catalog.resolve_uri(uri)?;
        let _ = self.table_cache.get_or_open_store(&store_name)?;
        Ok(())
    }

    pub fn open_cursor(&mut self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let store_name = self.schema_catalog.resolve_uri(uri)?;
        self.open_store_cursor(&store_name)
    }

    pub fn insert_one(&mut self, collection: &str, doc: Value) -> Result<Document, WrongoDBError> {
        self.with_txn(|session| session.insert_one_in_txn(collection, doc))
    }

    pub fn find(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        self.with_txn(|session| {
            let txn_id = session.require_txn_id()?;
            session.find_with_txn(collection, filter, txn_id)
        })
    }

    pub fn find_one(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Option<Document>, WrongoDBError> {
        Ok(self.find(collection, filter)?.into_iter().next())
    }

    pub fn count(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        Ok(self.find(collection, filter)?.len())
    }

    pub fn distinct(
        &mut self,
        collection: &str,
        key: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, WrongoDBError> {
        let docs = self.find(collection, filter)?;
        let mut seen = HashSet::new();
        let mut values = Vec::new();

        for doc in docs {
            if let Some(value) = doc.get(key) {
                let encoded = serde_json::to_string(value).unwrap_or_default();
                if seen.insert(encoded) {
                    values.push(value.clone());
                }
            }
        }

        Ok(values)
    }

    pub fn update_one(
        &mut self,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.with_txn(|session| {
            let txn_id = session.require_txn_id()?;
            let docs = session.find_with_txn(collection, filter, txn_id)?;
            if docs.is_empty() {
                return Ok(UpdateResult {
                    matched: 0,
                    modified: 0,
                });
            }

            let doc = &docs[0];
            let updated_doc = apply_update(doc, &update)?;
            let id = doc.get("_id").ok_or_else(|| {
                crate::core::errors::DocumentValidationError("missing _id".into())
            })?;
            let key = encode_id_value(id)?;
            let value = encode_document(&updated_doc)?;

            let mut cursor = session.open_cursor(&format!("table:{collection}"))?;
            cursor.update(&key, &value, txn_id)?;

            session.apply_index_remove(collection, doc, txn_id)?;
            session.apply_index_add(collection, &updated_doc, txn_id)?;

            Ok(UpdateResult {
                matched: 1,
                modified: 1,
            })
        })
    }

    pub fn update_many(
        &mut self,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.with_txn(|session| {
            let txn_id = session.require_txn_id()?;
            let docs = session.find_with_txn(collection, filter, txn_id)?;
            if docs.is_empty() {
                return Ok(UpdateResult {
                    matched: 0,
                    modified: 0,
                });
            }

            let mut modified = 0;
            for doc in docs {
                let updated_doc = apply_update(&doc, &update)?;
                let id = doc.get("_id").ok_or_else(|| {
                    crate::core::errors::DocumentValidationError("missing _id".into())
                })?;
                let key = encode_id_value(id)?;
                let value = encode_document(&updated_doc)?;

                let mut cursor = session.open_cursor(&format!("table:{collection}"))?;
                cursor.update(&key, &value, txn_id)?;

                session.apply_index_remove(collection, &doc, txn_id)?;
                session.apply_index_add(collection, &updated_doc, txn_id)?;
                modified += 1;
            }

            Ok(UpdateResult {
                matched: modified,
                modified,
            })
        })
    }

    pub fn delete_one(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.with_txn(|session| {
            let txn_id = session.require_txn_id()?;
            let docs = session.find_with_txn(collection, filter, txn_id)?;
            if docs.is_empty() {
                return Ok(0);
            }

            let doc = &docs[0];
            let Some(id) = doc.get("_id") else {
                return Ok(0);
            };
            let key = encode_id_value(id)?;

            let mut cursor = session.open_cursor(&format!("table:{collection}"))?;
            cursor.delete(&key, txn_id)?;
            session.apply_index_remove(collection, doc, txn_id)?;
            Ok(1)
        })
    }

    pub fn delete_many(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.with_txn(|session| {
            let txn_id = session.require_txn_id()?;
            let docs = session.find_with_txn(collection, filter, txn_id)?;
            if docs.is_empty() {
                return Ok(0);
            }

            let mut deleted = 0;
            for doc in docs {
                let Some(id) = doc.get("_id") else {
                    continue;
                };
                let key = encode_id_value(id)?;

                let mut cursor = session.open_cursor(&format!("table:{collection}"))?;
                cursor.delete(&key, txn_id)?;
                session.apply_index_remove(collection, &doc, txn_id)?;
                deleted += 1;
            }

            Ok(deleted)
        })
    }

    pub fn list_indexes(&self, collection: &str) -> Result<Vec<String>, WrongoDBError> {
        self.schema_catalog.list_indexes(collection)
    }

    pub fn create_index(&mut self, collection: &str, field: &str) -> Result<(), WrongoDBError> {
        self.with_txn(|session| {
            let txn_id = session.require_txn_id()?;
            let docs = session.find_with_txn(collection, None, txn_id)?;
            let Some(store_name) =
                session
                    .schema_catalog
                    .add_index(collection, field, vec![field.to_string()])?
            else {
                return Ok(());
            };

            let mut cursor = session.open_store_cursor(&store_name)?;
            for doc in docs {
                let Some(id) = doc.get("_id") else {
                    continue;
                };
                let Some(value) = doc.get(field) else {
                    continue;
                };
                let Some(key) = encode_index_key(value, id)? else {
                    continue;
                };
                cursor.insert(&key, &[], txn_id)?;
            }

            Ok(())
        })
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

    fn with_txn<R, F>(&mut self, f: F) -> Result<R, WrongoDBError>
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

    fn insert_one_in_txn(
        &mut self,
        collection: &str,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let mut obj = doc.as_object().expect("validated object").clone();
        normalize_document_in_place(&mut obj)?;

        let id = obj
            .get("_id")
            .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&obj)?;
        let txn_id = self.require_txn_id()?;

        let mut cursor = self.open_cursor(&format!("table:{collection}"))?;
        cursor.insert(&key, &value, txn_id)?;
        self.apply_index_add(collection, &obj, txn_id)?;
        Ok(obj)
    }

    fn find_with_txn(
        &mut self,
        collection: &str,
        filter: Option<Value>,
        txn_id: TxnId,
    ) -> Result<Vec<Document>, WrongoDBError> {
        let filter_doc = match filter {
            None => Document::new(),
            Some(value) => {
                validate_is_object(&value)?;
                value.as_object().expect("validated object").clone()
            }
        };

        let mut table_cursor = self.open_cursor(&format!("table:{collection}"))?;

        if filter_doc.is_empty() {
            return self.scan_with_cursor(&mut table_cursor, txn_id, |doc| {
                let _ = doc;
                true
            });
        }

        let matches_filter = |doc: &Document| {
            filter_doc.iter().all(|(key, value)| {
                if key == "_id" {
                    serde_json::to_string(doc.get(key).unwrap()).unwrap()
                        == serde_json::to_string(value).unwrap()
                } else {
                    doc.get(key) == Some(value)
                }
            })
        };

        if let Some(id_value) = filter_doc.get("_id") {
            let key = encode_id_value(id_value)?;
            let doc_bytes = table_cursor.get(&key, txn_id)?;
            return Ok(match doc_bytes {
                Some(bytes) => {
                    let doc = decode_document(&bytes)?;
                    if matches_filter(&doc) {
                        vec![doc]
                    } else {
                        Vec::new()
                    }
                }
                None => Vec::new(),
            });
        }

        let schema = self.schema_catalog.collection_schema(collection)?;
        let indexed_field = filter_doc.keys().find(|key| schema.has_index(key)).cloned();
        if let Some(field) = indexed_field {
            let value = filter_doc.get(&field).expect("field selected from filter");
            let Some((start_key, end_key)) = encode_range_bounds(value) else {
                return Ok(Vec::new());
            };
            let mut index_cursor = self.open_cursor(&format!("index:{collection}:{field}"))?;
            index_cursor.set_range(Some(start_key), Some(end_key));

            let mut results = Vec::new();
            while let Some((key, _)) = index_cursor.next(txn_id)? {
                let Some(id) = decode_index_id(&key)? else {
                    continue;
                };
                let primary_key = encode_id_value(&id)?;
                if let Some(bytes) = table_cursor.get(&primary_key, txn_id)? {
                    let doc = decode_document(&bytes)?;
                    if matches_filter(&doc) {
                        results.push(doc);
                    }
                }
            }
            return Ok(results);
        }

        self.scan_with_cursor(&mut table_cursor, txn_id, matches_filter)
    }

    fn scan_with_cursor<F>(
        &mut self,
        cursor: &mut Cursor,
        txn_id: TxnId,
        matches_filter: F,
    ) -> Result<Vec<Document>, WrongoDBError>
    where
        F: Fn(&Document) -> bool,
    {
        let mut results = Vec::new();
        while let Some((_, bytes)) = cursor.next(txn_id)? {
            let doc = decode_document(&bytes)?;
            if matches_filter(&doc) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    fn apply_index_add(
        &mut self,
        collection: &str,
        doc: &Document,
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.apply_index_doc(collection, doc, txn_id, true)
    }

    fn apply_index_remove(
        &mut self,
        collection: &str,
        doc: &Document,
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.apply_index_doc(collection, doc, txn_id, false)
    }

    fn apply_index_doc(
        &mut self,
        collection: &str,
        doc: &Document,
        txn_id: TxnId,
        is_add: bool,
    ) -> Result<(), WrongoDBError> {
        let Some(id) = doc.get("_id") else {
            return Ok(());
        };
        let schema = self.schema_catalog.collection_schema(collection)?;
        for def in schema.index_definitions() {
            let Some(field) = def.columns.first() else {
                continue;
            };
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = encode_index_key(value, id)? else {
                continue;
            };
            let mut cursor = self.open_store_cursor(&def.source)?;
            if is_add {
                cursor.insert(&key, &[], txn_id)?;
            } else {
                cursor.delete(&key, txn_id)?;
            }
        }
        Ok(())
    }

    fn open_store_cursor(&self, store_name: &str) -> Result<Cursor, WrongoDBError> {
        let table = self.table_cache.get_or_open_store(store_name)?;
        Ok(Cursor::new(
            table,
            store_name.to_string(),
            self.mutation_hooks.clone(),
            self.write_tracker.clone(),
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

    fn require_txn_id(&self) -> Result<TxnId, WrongoDBError> {
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

        self.session.mutation_hooks.before_commit(txn_id, txn_id)?;

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

        if self.session.mutation_hooks.should_apply_locally() {
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

        self.session.mutation_hooks.before_abort(txn_id)?;

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

        if self.session.mutation_hooks.should_apply_locally() {
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
            let _ = self.session.mutation_hooks.before_abort(txn_id);
            let _ = self
                .session
                .transaction_manager
                .abort_txn_state(&mut active.txn);
            self.session.write_tracker.clear();
            if self.session.mutation_hooks.should_apply_locally() {
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
    use std::sync::Arc;

    use parking_lot::Mutex;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::api::connection::{Connection, ConnectionConfig};
    use crate::durability::DurabilityBackend;
    use crate::hooks::MutationHooks;
    use crate::storage::wal::{WalReader, WalRecord};
    use crate::txn::GlobalTxnState;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum HookCall {
        Commit { txn_id: TxnId, commit_ts: TxnId },
        Abort { txn_id: TxnId },
    }

    #[derive(Debug, Default)]
    struct MockHooks {
        calls: Mutex<Vec<HookCall>>,
    }

    impl MutationHooks for MockHooks {
        fn before_commit(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError> {
            self.calls
                .lock()
                .push(HookCall::Commit { txn_id, commit_ts });
            Ok(())
        }

        fn before_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
            self.calls.lock().push(HookCall::Abort { txn_id });
            Ok(())
        }
    }

    fn new_session_with_hooks(hooks: Arc<dyn MutationHooks>) -> Session {
        let dir = tempdir().unwrap();
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_cache = Arc::new(TableCache::new(
            dir.path().to_path_buf(),
            transaction_manager.clone(),
        ));
        let schema_catalog = Arc::new(SchemaCatalog::new(dir.path().to_path_buf()));
        Session::new(
            table_cache,
            schema_catalog,
            transaction_manager,
            Arc::new(DurabilityBackend::Disabled),
            hooks,
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

    #[test]
    fn checkpoint_skips_truncate_when_transaction_active() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        session.create("table:items").unwrap();

        {
            let mut txn = session.transaction().unwrap();
            let txn_id = txn.as_ref().id();
            let mut cursor = txn.session_mut().open_cursor("table:items").unwrap();
            cursor.insert(b"k1", b"v1", txn_id).unwrap();
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
            let txn_id = txn.as_ref().id();
            let mut cursor = txn.session_mut().open_cursor("table:items").unwrap();
            cursor.insert(b"k1", b"v1", txn_id).unwrap();
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
    fn commit_calls_before_commit_hook() {
        let hooks = Arc::new(MockHooks::default());
        let mut session = new_session_with_hooks(hooks.clone());
        let txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();

        txn.commit().unwrap();

        let calls = hooks.calls.lock();
        assert_eq!(
            *calls,
            vec![HookCall::Commit {
                txn_id,
                commit_ts: txn_id
            }]
        );
    }

    #[test]
    fn abort_and_drop_call_before_abort_hook() {
        let hooks = Arc::new(MockHooks::default());
        let mut session = new_session_with_hooks(hooks.clone());
        let txn = session.transaction().unwrap();
        let abort_txn_id = txn.as_ref().id();
        txn.abort().unwrap();

        {
            let dropped = session.transaction().unwrap();
            let drop_txn_id = dropped.as_ref().id();
            drop(dropped);

            let calls = hooks.calls.lock();
            assert_eq!(
                *calls,
                vec![
                    HookCall::Abort {
                        txn_id: abort_txn_id
                    },
                    HookCall::Abort {
                        txn_id: drop_txn_id
                    }
                ]
            );
        }
    }

    #[test]
    fn session_crud_roundtrip() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        let inserted = session
            .insert_one("test", json!({"name": "alice", "age": 30}))
            .unwrap();
        let id = inserted.get("_id").unwrap().clone();

        let fetched = session
            .find_one("test", Some(json!({"_id": id.clone()})))
            .unwrap()
            .unwrap();
        assert_eq!(fetched.get("name").unwrap().as_str().unwrap(), "alice");

        let updated = session
            .update_one(
                "test",
                Some(json!({"_id": id.clone()})),
                json!({"$set": {"age": 31}}),
            )
            .unwrap();
        assert_eq!(updated.matched, 1);
        assert_eq!(updated.modified, 1);

        let fetched = session
            .find_one("test", Some(json!({"_id": id.clone()})))
            .unwrap()
            .unwrap();
        assert_eq!(fetched.get("age").unwrap().as_i64().unwrap(), 31);

        let deleted = session
            .delete_one("test", Some(json!({"_id": id})))
            .unwrap();
        assert_eq!(deleted, 1);
        assert!(session
            .find_one("test", Some(json!({"name": "alice"})))
            .unwrap()
            .is_none());
    }

    #[test]
    fn session_create_and_list_indexes() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        session
            .insert_one("test", json!({"name": "alice"}))
            .unwrap();
        session.create_index("test", "name").unwrap();

        let indexes = session.list_indexes("test").unwrap();
        assert!(indexes.iter().any(|idx| idx == "name"));
    }

    #[test]
    fn checkpoint_preserves_indexed_lookup_after_reconciliation() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        session.create_index("test", "name").unwrap();
        session
            .insert_one("test", json!({"_id": 1, "name": "alice"}))
            .unwrap();

        session.checkpoint().unwrap();

        let doc = session
            .find_one("test", Some(json!({"name": "alice"})))
            .unwrap()
            .unwrap();
        assert_eq!(doc.get("name"), Some(&json!("alice")));
    }
}
