use std::sync::{Arc, Weak};

use crate::api::cursor::{Cursor, CursorWriteAccess};
use crate::core::errors::StorageError;
use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp, WritePathMode};
use crate::schema::SchemaCatalog;
use crate::storage::table_cache::TableCache;
use crate::txn::{ActiveWriteUnit, RecoveryUnit, TransactionManager, TxnId, TXN_NONE};
use crate::WrongoDBError;

/// A request-scoped execution context over shared connection infrastructure.
///
/// `Connection` owns long-lived shared components (storage handles, schema
/// metadata, global transaction state, and durability machinery). `Session`
/// exists to own mutable per-request state that must not be global.
///
/// `Session` is where callers:
/// - open cursors
/// - start transactions
/// - trigger checkpoint
///
/// It intentionally does not own document write orchestration or the replicated
/// write path. Those live in higher internal layers.
///
/// In practice this means `Session` answers "what can this request do right
/// now?" while `Connection` answers "what engine are we attached to?".
pub struct Session {
    table_cache: Arc<TableCache>,
    schema_catalog: Arc<SchemaCatalog>,
    transaction_manager: Arc<TransactionManager>,
    durability_backend: Arc<DurabilityBackend>,
    recovery_unit: Arc<dyn RecoveryUnit>,
    active_txn: Option<ActiveWriteUnit>,
}

impl Session {
    pub(crate) fn new(
        table_cache: Arc<TableCache>,
        schema_catalog: Arc<SchemaCatalog>,
        transaction_manager: Arc<TransactionManager>,
        durability_backend: Arc<DurabilityBackend>,
    ) -> Self {
        let recovery_unit = durability_backend.new_recovery_unit();
        Self {
            table_cache,
            schema_catalog,
            transaction_manager,
            recovery_unit,
            durability_backend,
            active_txn: None,
        }
    }

    /// Ensure the primary store for `table:<collection>` exists.
    ///
    /// This method exists so callers can bootstrap a table through the same
    /// session object they use to open cursors, instead of forcing schema
    /// creation through the higher-level document write path.
    ///
    /// Only `table:...` URIs are supported publicly. Index creation stays
    /// internal because it is not a single-store operation: it has to update
    /// schema metadata and backfill index entries from existing collection
    /// data.
    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if let Some(collection) = uri.strip_prefix("table:") {
            let store_name = self.schema_catalog.primary_store_name(collection);
            let _ = self.table_cache.get_or_open_store(&store_name)?;
            return Ok(());
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI for Session::create: {uri}; only table:... is supported"
        ))))
    }

    /// Open a non-transactional cursor bound to `TXN_NONE`.
    ///
    /// This method intentionally has the same name as
    /// [`WriteUnitOfWork::open_cursor`]. The duplication is deliberate:
    /// transaction scope is expressed by the receiver instead of by exposing
    /// raw transaction ids in the public cursor API.
    ///
    /// `Session::open_cursor` means "open a cursor outside any transaction".
    /// [`WriteUnitOfWork::open_cursor`] means "open the same kind of cursor,
    /// but bound to the active transaction".
    ///
    /// The method lives on `Session` because non-transactional access is still
    /// a first-class storage API and should not require creating a transaction
    /// boundary object just to read or perform local writes.
    pub fn open_cursor(&self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let store_name = self.schema_catalog.resolve_uri(uri)?;
        self.open_store_cursor_with_access(&store_name, TXN_NONE, self.cursor_write_access())
    }

    /// Start a transactional write unit of work.
    ///
    /// This exists as a separate step so commit/abort ordering, local WAL
    /// markers, and transaction-bound cursor opening all flow through a single
    /// RAII boundary instead of being spread across the public API.
    ///
    /// The public API deliberately makes transaction scope explicit here rather
    /// than letting cursor methods take raw transaction ids.
    pub fn transaction(&mut self) -> Result<WriteUnitOfWork<'_>, WrongoDBError> {
        if self.active_txn.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }

        let txn = self.transaction_manager.begin_snapshot_txn();
        self.recovery_unit.begin_unit_of_work()?;
        self.active_txn = Some(ActiveWriteUnit::new(txn));
        Ok(WriteUnitOfWork::new(self))
    }

    /// Reconcile and checkpoint all known stores.
    ///
    /// Checkpoint is session-level because it coordinates many stores and, when
    /// durability is enabled, emits the matching durability marker only after
    /// the storage-level reconciliation pass has finished.
    ///
    /// It lives on `Session` instead of `Cursor` or `Table` because checkpoint
    /// is not a one-store concern.
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

    fn write_path_mode(&self) -> WritePathMode {
        self.durability_backend.write_path_mode()
    }

    fn cursor_write_access(&self) -> CursorWriteAccess {
        match self.write_path_mode() {
            WritePathMode::LocalApply => CursorWriteAccess::ReadWrite,
            WritePathMode::DeferredReplication => CursorWriteAccess::ReadOnly,
        }
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

    fn open_store_cursor_with_access(
        &self,
        store_name: &str,
        bound_txn_id: TxnId,
        write_access: CursorWriteAccess,
    ) -> Result<Cursor, WrongoDBError> {
        let table = self.table_cache.get_or_open_store(store_name)?;
        let active_txn = self.bound_transaction_handle(bound_txn_id)?;
        Ok(Cursor::new(
            table,
            store_name.to_string(),
            bound_txn_id,
            active_txn,
            self.recovery_unit.clone(),
            write_access,
        ))
    }

    fn bound_transaction_handle(
        &self,
        bound_txn_id: TxnId,
    ) -> Result<Option<Weak<parking_lot::Mutex<crate::txn::Transaction>>>, WrongoDBError> {
        if bound_txn_id == TXN_NONE {
            return Ok(None);
        }

        let active = self.active_txn.as_ref().ok_or_else(|| {
            StorageError(format!(
                "transaction cursor requested for txn {bound_txn_id}, but no transaction is active"
            ))
        })?;
        Ok(Some(Arc::downgrade(&active.txn_handle())))
    }
}

/// Public handle for one active transaction started from [`Session::transaction`].
///
/// This type exists so the public API has an explicit transaction scope:
/// while a write unit of work is alive, callers open cursors from it and those
/// cursors are automatically bound to the active transaction.
///
/// That is why this type is separate from [`Session`]. It keeps commit/abort
/// ownership and transaction-bound cursor opening behind a single object,
/// instead of exposing raw transaction ids or letting callers keep reaching
/// back into `Session` during a transaction.
///
/// The type is intentionally narrow: it is the transaction boundary, not a
/// second general-purpose session object.
pub struct WriteUnitOfWork<'a> {
    session: &'a mut Session,
    committed: bool,
    write_path_mode: WritePathMode,
}

impl<'a> WriteUnitOfWork<'a> {
    /// Open a cursor bound to this transaction.
    ///
    /// This duplicates [`Session::open_cursor`] by design. The reason is API
    /// clarity: a caller should choose transaction scope by opening the cursor
    /// from the transaction boundary, not by reaching back into `Session` and
    /// not by threading transaction ids through every cursor method.
    ///
    /// The duplication is about binding, not behavior: both methods open the
    /// same kind of cursor, but they bind it to different transaction scopes.
    pub fn open_cursor(&mut self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let store_name = self.session.schema_catalog.resolve_uri(uri)?;
        self.session.open_store_cursor_with_access(
            &store_name,
            self.txn_id(),
            self.session.cursor_write_access(),
        )
    }

    /// Commit the transaction and consume this write unit of work.
    ///
    /// Consuming `self` makes the transaction boundary single-use and avoids
    /// partially committed public states. This is also where durability ordering
    /// is enforced before local transaction state is advanced.
    ///
    /// The method lives here, not on `Session`, because commit is part of the
    /// transaction boundary's responsibility.
    pub fn commit(mut self) -> Result<(), WrongoDBError> {
        let Some(active) = self.session.active_txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let txn_id = active.txn_id();
        let txn_handle = active.txn_handle();

        match self.write_path_mode {
            WritePathMode::LocalApply => self
                .session
                .recovery_unit
                .commit_unit_of_work(txn_id, txn_id)?,
            WritePathMode::DeferredReplication => self.session.record_commit(txn_id)?,
        }

        {
            let mut txn = txn_handle.lock();
            self.session
                .transaction_manager
                .commit_txn_state(&mut txn)?;
        }

        self.session.active_txn = None;
        self.committed = true;
        Ok(())
    }

    /// Abort the transaction and consume this write unit of work.
    ///
    /// Like [`commit`](Self::commit), this consumes the boundary so rollback is
    /// a terminal operation. The method exists separately from `Drop` so callers
    /// can observe an explicit abort result instead of relying on best-effort
    /// cleanup.
    ///
    /// It exists for the same reason as [`commit`](Self::commit): abort belongs
    /// to the transaction boundary, not to the general session object.
    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        let Some(active) = self.session.active_txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let txn_id = active.txn_id();
        let txn_handle = active.txn_handle();

        match self.write_path_mode {
            WritePathMode::LocalApply => self.session.recovery_unit.abort_unit_of_work(txn_id)?,
            WritePathMode::DeferredReplication => self.session.record_abort(txn_id)?,
        }

        {
            let mut txn = txn_handle.lock();
            self.session.transaction_manager.abort_txn_state(&mut txn)?;
        }

        self.session.active_txn = None;
        self.committed = true;
        Ok(())
    }

    fn new(session: &'a mut Session) -> Self {
        let write_path_mode = session.write_path_mode();
        Self {
            session,
            committed: false,
            write_path_mode,
        }
    }

    pub(crate) fn txn_id(&self) -> TxnId {
        self.session
            .active_txn
            .as_ref()
            .expect("transaction should exist")
            .txn_id()
    }

    pub(crate) fn open_store_cursor_by_name(
        &mut self,
        store_name: &str,
    ) -> Result<Cursor, WrongoDBError> {
        self.session.open_store_cursor_with_access(
            store_name,
            self.txn_id(),
            self.session.cursor_write_access(),
        )
    }
}

impl<'a> Drop for WriteUnitOfWork<'a> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        if let Some(active) = self.session.active_txn.take() {
            let txn_id = active.txn_id();
            let txn_handle = active.txn_handle();
            match self.write_path_mode {
                WritePathMode::LocalApply => {
                    let _ = self.session.recovery_unit.abort_unit_of_work(txn_id);
                }
                WritePathMode::DeferredReplication => {
                    let _ = self.session.record_abort(txn_id);
                }
            }
            let mut txn = txn_handle.lock();
            let _ = self.session.transaction_manager.abort_txn_state(&mut txn);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::api::connection::{Connection, ConnectionConfig};
    use crate::collection_write_path::{CollectionWritePath, UpdateResult};
    use crate::core::bson::{encode_document, encode_id_value};
    use crate::document_query::DocumentQuery;
    use crate::durability::DurabilityBackend;
    use crate::schema::SchemaCatalog;
    use crate::storage::table_cache::TableCache;
    use crate::storage::wal::{WalReader, WalRecord};
    use crate::store_write_path::StoreWritePath;
    use crate::txn::GlobalTxnState;

    fn new_session_with_backend(
        backend: DurabilityBackend,
    ) -> (Session, CollectionWritePath, DocumentQuery) {
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
        let backend = Arc::new(backend);
        let query = DocumentQuery::new(schema_catalog.clone());
        let write_path = CollectionWritePath::new(
            schema_catalog.clone(),
            query.clone(),
            StoreWritePath::new(table_cache.clone(), backend.clone()),
        );
        let session = Session::new(table_cache, schema_catalog, transaction_manager, backend);
        (session, write_path, query)
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
        write_path: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        doc: serde_json::Value,
    ) -> crate::Document {
        write_path.insert_one(session, collection, doc).unwrap()
    }

    fn find_one(
        query: &DocumentQuery,
        session: &mut Session,
        collection: &str,
        filter: serde_json::Value,
    ) -> Option<crate::Document> {
        query
            .find(session, collection, Some(filter))
            .unwrap()
            .into_iter()
            .next()
    }

    fn create_index(
        write_path: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        field: &str,
    ) {
        write_path.create_index(session, collection, field).unwrap();
    }

    fn list_indexes(query: &DocumentQuery, collection: &str) -> Vec<String> {
        query.list_indexes(collection).unwrap()
    }

    fn update_one(
        write_path: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        filter: serde_json::Value,
        update: serde_json::Value,
    ) -> UpdateResult {
        write_path
            .update_one(session, collection, Some(filter), update)
            .unwrap()
    }

    fn delete_one(
        write_path: &CollectionWritePath,
        session: &mut Session,
        collection: &str,
        filter: serde_json::Value,
    ) -> usize {
        write_path
            .delete_one(session, collection, Some(filter))
            .unwrap()
    }

    #[test]
    fn checkpoint_skips_truncate_when_transaction_active() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        session.create("table:items").unwrap();

        {
            let mut txn = session.transaction().unwrap();
            conn.collection_write_path
                .insert_one_in_write_unit(&mut txn, "items", json!({"_id": "k1", "value": "v1"}))
                .unwrap();
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
            conn.collection_write_path
                .insert_one_in_write_unit(&mut txn, "items", json!({"_id": "k1", "value": "v1"}))
                .unwrap();
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
    fn local_mode_commit_applies_page_local_updates() {
        let (mut session, write_path, query) =
            new_session_with_backend(DurabilityBackend::Disabled);
        let mut txn = session.transaction().unwrap();
        write_path
            .insert_one_in_write_unit(&mut txn, "items", json!({"_id": "k1", "value": "v1"}))
            .unwrap();

        txn.commit().unwrap();

        let doc = find_one(&query, &mut session, "items", json!({"_id": "k1"})).unwrap();
        assert_eq!(doc.get("value"), Some(&json!("v1")));
    }

    #[test]
    fn local_mode_abort_discards_page_local_updates() {
        let (mut session, write_path, query) =
            new_session_with_backend(DurabilityBackend::Disabled);
        let mut txn = session.transaction().unwrap();
        write_path
            .insert_one_in_write_unit(&mut txn, "items", json!({"_id": "k1", "value": "v1"}))
            .unwrap();

        txn.abort().unwrap();

        assert!(find_one(&query, &mut session, "items", json!({"_id": "k1"})).is_none());
    }

    #[test]
    fn local_wal_mode_records_write_and_commit_markers() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        session.create("table:items").unwrap();
        let expected_key = encode_id_value(&json!("k1")).unwrap();
        let expected_value =
            encode_document(json!({"_id": "k1", "value": "v1"}).as_object().unwrap()).unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.txn_id();
        conn.collection_write_path
            .insert_one_in_write_unit(&mut txn, "items", json!({"_id": "k1", "value": "v1"}))
            .unwrap();
        txn.commit().unwrap();

        let records = read_wal_records(dir.path());
        assert!(records.iter().any(|record| matches!(
            record,
            WalRecord::Put {
                store_name,
                key,
                value,
                txn_id: record_txn_id,
            } if store_name == "items.main.wt"
                && key == &expected_key
                && value == &expected_value
                && *record_txn_id == txn_id
        )));
        assert!(records.iter().any(|record| matches!(
            record,
            WalRecord::TxnCommit {
                txn_id: record_txn_id,
                commit_ts,
            } if *record_txn_id == txn_id && *commit_ts == txn_id
        )));
    }

    #[test]
    fn local_wal_mode_does_not_recover_aborted_write() {
        let dir = tempdir().unwrap();
        {
            let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
            let mut session = conn.open_session();
            session.create("table:items").unwrap();

            let mut txn = session.transaction().unwrap();
            conn.collection_write_path
                .insert_one_in_write_unit(
                    &mut txn,
                    "items",
                    json!({"_id": "abort-me", "value": "v1"}),
                )
                .unwrap();
            txn.abort().unwrap();
        }

        let reopened = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let mut session = reopened.open_session();
        assert!(find_one(
            &reopened.document_query,
            &mut session,
            "items",
            json!({"_id": "abort-me"})
        )
        .is_none());
    }

    #[test]
    fn deferred_mode_records_commit_after_write_ops() {
        let (backend, recorded_ops) =
            DurabilityBackend::test_backend(WritePathMode::DeferredReplication);
        let (mut session, write_path, _query) = new_session_with_backend(backend);
        let mut txn = session.transaction().unwrap();
        let txn_id = txn.txn_id();
        let doc = json!({"_id": "k1", "value": "v1"});
        let encoded_key = encode_id_value(&json!("k1")).unwrap();
        let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();

        write_path
            .insert_one_in_write_unit(&mut txn, "items", doc)
            .unwrap();
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
        let (mut session, write_path, _query) = new_session_with_backend(backend);

        {
            let mut txn = session.transaction().unwrap();
            let txn_id = txn.txn_id();
            let doc = json!({"_id": "abort-me", "value": "v1"});
            let encoded_key = encode_id_value(&json!("abort-me")).unwrap();
            let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();
            write_path
                .insert_one_in_write_unit(&mut txn, "items", doc)
                .unwrap();
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
            let txn_id = dropped.txn_id();
            let doc = json!({"_id": "drop-me", "value": "v2"});
            let encoded_key = encode_id_value(&json!("drop-me")).unwrap();
            let encoded_doc = encode_document(doc.as_object().unwrap()).unwrap();
            write_path
                .insert_one_in_write_unit(&mut dropped, "items", doc)
                .unwrap();
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

        let inserted = insert_one(
            &conn.collection_write_path,
            &mut session,
            "test",
            json!({"name": "alice", "age": 30}),
        );
        let id = inserted.get("_id").unwrap().clone();

        let fetched = find_one(
            &conn.document_query,
            &mut session,
            "test",
            json!({"_id": id.clone()}),
        )
        .unwrap();
        assert_eq!(fetched.get("name").unwrap().as_str().unwrap(), "alice");

        let updated = update_one(
            &conn.collection_write_path,
            &mut session,
            "test",
            json!({"_id": id.clone()}),
            json!({"$set": {"age": 31}}),
        );
        assert_eq!(updated.matched, 1);
        assert_eq!(updated.modified, 1);

        let fetched = find_one(
            &conn.document_query,
            &mut session,
            "test",
            json!({"_id": id.clone()}),
        )
        .unwrap();
        assert_eq!(fetched.get("age").unwrap().as_i64().unwrap(), 31);

        let deleted = delete_one(
            &conn.collection_write_path,
            &mut session,
            "test",
            json!({"_id": id}),
        );
        assert_eq!(deleted, 1);
        assert!(find_one(
            &conn.document_query,
            &mut session,
            "test",
            json!({"name": "alice"})
        )
        .is_none());
    }

    #[test]
    fn session_create_and_list_indexes() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        insert_one(
            &conn.collection_write_path,
            &mut session,
            "test",
            json!({"name": "alice"}),
        );
        create_index(&conn.collection_write_path, &mut session, "test", "name");

        let indexes = list_indexes(&conn.document_query, "test");
        assert!(indexes.iter().any(|idx| idx == "name"));
    }

    #[test]
    fn session_create_rejects_index_uris() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        let err = session.create("index:test:name").unwrap_err();
        assert!(err.to_string().contains("only table:... is supported"));
    }

    #[test]
    fn checkpoint_preserves_indexed_lookup_after_reconciliation() {
        let tmp = tempdir().unwrap();
        let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        create_index(&conn.collection_write_path, &mut session, "test", "name");
        insert_one(
            &conn.collection_write_path,
            &mut session,
            "test",
            json!({"_id": 1, "name": "alice"}),
        );

        session.checkpoint().unwrap();

        let doc = find_one(
            &conn.document_query,
            &mut session,
            "test",
            json!({"name": "alice"}),
        )
        .unwrap();
        assert_eq!(doc.get("name"), Some(&json!("alice")));
    }
}
