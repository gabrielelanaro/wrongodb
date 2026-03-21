use std::path::PathBuf;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::storage::api::cursor::{TableCursor, TableCursorState, TableCursorWriteAccess};
use crate::storage::btree::BTreeCursor;
use crate::storage::durability::{DurabilityBackend, StorageSyncPolicy};
use crate::storage::handle_cache::HandleCache;
use crate::storage::metadata_catalog::{MetadataCatalog, METADATA_STORE_NAME, METADATA_URI};
use crate::storage::table::{
    apply_put_in_txn, checkpoint_store, contains_key, open_or_create_btree, scan_range,
    TableMetadata,
};
use crate::txn::{Transaction, TransactionManager, TxnId, TXN_NONE};
use crate::WrongoDBError;

pub(crate) type ActiveTxnHandle = Arc<Mutex<Transaction>>;

type RawScanEntries = Vec<(Vec<u8>, Vec<u8>)>;

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
/// It intentionally does not own document write orchestration. That lives in
/// higher internal layers.
///
/// In practice this means `Session` answers "what can this request do right
/// now?" while `Connection` answers "what engine are we attached to?".
pub struct Session {
    base_path: PathBuf,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    metadata_catalog: Arc<MetadataCatalog>,
    transaction_manager: Arc<TransactionManager>,
    durability_backend: Arc<DurabilityBackend>,
    active_txn: Mutex<Option<ActiveTxnHandle>>,
    open_cursors: Mutex<Vec<Weak<Mutex<TableCursorState>>>>,
}

impl Session {
    pub(crate) fn new(
        base_path: PathBuf,
        store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
        metadata_catalog: Arc<MetadataCatalog>,
        transaction_manager: Arc<TransactionManager>,
        durability_backend: Arc<DurabilityBackend>,
    ) -> Self {
        Self {
            base_path,
            store_handles,
            metadata_catalog,
            durability_backend,
            transaction_manager,
            active_txn: Mutex::new(None),
            open_cursors: Mutex::new(Vec::new()),
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
            let metadata_catalog = self.metadata_catalog.clone();
            return self.with_transaction(|session| {
                let _ = metadata_catalog.ensure_table_uri_in_transaction(session, collection)?;
                Ok(())
            });
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI for Session::create: {uri}; only table:... is supported"
        ))))
    }

    /// Open a table cursor using the session's current transaction state.
    ///
    /// Cursors consult live session transaction state at call time, so the same
    /// cursor type works both outside a transaction and inside
    /// [`Session::with_transaction`].
    pub fn open_cursor(&self, uri: &str) -> Result<TableCursor<'_>, WrongoDBError> {
        let txn_id = self.txn_id();
        let table = self.resolve_table_metadata(uri, txn_id)?;
        self.open_table_cursor_with_access(table, txn_id, TableCursorWriteAccess::ReadWrite)
    }

    /// Run `f` inside one session-owned transaction.
    ///
    /// This keeps the public Rust API narrow while preserving WT's session
    /// ownership of transaction state internally.
    pub fn with_transaction<R>(
        &mut self,
        f: impl FnOnce(&mut Session) -> Result<R, WrongoDBError>,
    ) -> Result<R, WrongoDBError> {
        self.begin_transaction_internal()?;
        let guard = SessionTransactionGuard::new(self);
        guard.run(f)
    }

    /// Reconcile and checkpoint all known stores.
    ///
    /// Checkpoint is session-level because it coordinates many stores and, when
    /// durability is enabled, emits the matching durability marker only after
    /// the storage-level reconciliation pass has finished.
    ///
    /// It lives on `Session` instead of `TableCursor` or a per-store helper
    /// because checkpoint is not a one-store concern.
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let mut store_names = self.metadata_catalog.all_sources()?;
        store_names.push(METADATA_STORE_NAME.to_string());
        store_names.sort();
        store_names.dedup();

        for store_name in store_names {
            let store = self
                .store_handles
                .get_or_try_insert_with(store_name, |store_name| {
                    let path = self.base_path.join(store_name);
                    Ok(RwLock::new(open_or_create_btree(path)?))
                })?;
            checkpoint_store(&mut store.write(), self.transaction_manager.as_ref())?;
        }

        if self.transaction_manager.has_active_transactions()
            || !self.durability_backend.is_enabled()
        {
            return Ok(());
        }

        self.durability_backend
            .record_checkpoint(StorageSyncPolicy::Sync)?;
        self.durability_backend.truncate_to_checkpoint()
    }

    fn open_table_cursor_with_access(
        &self,
        table: TableMetadata,
        txn_id: TxnId,
        write_access: TableCursorWriteAccess,
    ) -> Result<TableCursor<'_>, WrongoDBError> {
        let primary = self.open_raw_store_for_txn(table.uri(), txn_id)?;
        let indexes = table
            .indexes()
            .iter()
            .map(|index| self.open_raw_store_for_txn(index.uri(), txn_id))
            .collect::<Result<Vec<_>, _>>()?;

        TableCursor::new(self, table, primary, indexes, write_access)
    }

    fn resolve_table_metadata(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<TableMetadata, WrongoDBError> {
        if !uri.starts_with("table:") {
            return Err(StorageError(format!(
                "unsupported URI for public cursor open: {uri}; only table:... is supported"
            ))
            .into());
        }

        self.metadata_catalog
            .table_metadata_for_txn(uri, txn_id)?
            .ok_or_else(|| StorageError(format!("unknown URI: {uri}")).into())
    }

    fn resolve_raw_uri_for_txn(&self, uri: &str, txn_id: TxnId) -> Result<String, WrongoDBError> {
        if uri != METADATA_URI && !uri.starts_with("table:") && !uri.starts_with("index:") {
            return Err(StorageError(format!("unsupported URI: {uri}")).into());
        }

        self.metadata_catalog
            .lookup_source_for_txn(uri, txn_id)?
            .ok_or_else(|| StorageError(format!("unknown URI: {uri}")).into())
    }

    fn open_raw_store(&self, store_name: &str) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        self.store_handles
            .get_or_try_insert_with(store_name.to_string(), |store_name| {
                let path = self.base_path.join(store_name);
                Ok(RwLock::new(open_or_create_btree(path)?))
            })
    }

    fn open_raw_store_for_txn(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        let store_name = self.resolve_raw_uri_for_txn(uri, txn_id)?;
        self.open_raw_store(&store_name)
    }

    pub(crate) fn txn_id(&self) -> TxnId {
        self.active_txn_handle()
            .map(|txn| txn.lock().id())
            .unwrap_or(TXN_NONE)
    }

    pub(crate) fn begin_transaction_internal(&mut self) -> Result<(), WrongoDBError> {
        let txn = self.transaction_manager.begin_snapshot_txn();
        let mut active_txn = self.active_txn.lock();
        if active_txn.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }
        *active_txn = Some(Arc::new(Mutex::new(txn)));
        Ok(())
    }

    pub(crate) fn commit_transaction_internal(&mut self) -> Result<(), WrongoDBError> {
        let Some(txn_handle) = self.take_active_txn() else {
            return Ok(());
        };

        let result = {
            let mut txn = txn_handle.lock();
            self.commit_txn_with_durability(&mut txn)
        };

        if result.is_err() {
            self.reset_open_cursors();
            let mut txn = txn_handle.lock();
            let _ = self.transaction_manager.abort_txn_state(&mut txn);
        }

        result
    }

    pub(crate) fn rollback_transaction_internal(&mut self) -> Result<(), WrongoDBError> {
        let Some(txn_handle) = self.take_active_txn() else {
            return Ok(());
        };

        self.reset_open_cursors();
        let mut txn = txn_handle.lock();
        self.transaction_manager.abort_txn_state(&mut txn)
    }

    pub(crate) fn run_cursor_write_operation<R, F>(&self, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(TxnId, &mut Transaction) -> Result<R, WrongoDBError>,
    {
        if let Some(txn_handle) = self.active_txn_handle() {
            let mut txn = txn_handle.lock();
            return f(txn.id(), &mut txn);
        }

        let mut txn = self.transaction_manager.begin_snapshot_txn();
        let txn_id = txn.id();

        let result = f(txn_id, &mut txn);
        match result {
            Ok(value) => {
                if let Err(err) = self.commit_txn_with_durability(&mut txn) {
                    self.reset_open_cursors();
                    let _ = self.transaction_manager.abort_txn_state(&mut txn);
                    return Err(err);
                }
                Ok(value)
            }
            Err(err) => {
                self.reset_open_cursors();
                let _ = self.transaction_manager.abort_txn_state(&mut txn);
                Err(err)
            }
        }
    }

    pub(crate) fn raw_insert(
        &self,
        uri: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = self.txn_id();
        if txn_id == TXN_NONE {
            return Err(StorageError("raw_insert requires an active transaction".into()).into());
        }

        let store = self.open_raw_store_for_txn(uri, txn_id)?;
        let txn_handle = self
            .active_txn_handle()
            .ok_or_else(|| StorageError("raw_insert requires an active transaction".into()))?;
        let mut btree = store.write();
        if contains_key(&mut btree, key, txn_id)? {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }

        let mut txn = txn_handle.lock();
        apply_put_in_txn(uri, &mut btree, key, value, &mut txn)?;
        Ok(())
    }

    pub(crate) fn raw_scan_range(
        &self,
        uri: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<RawScanEntries, WrongoDBError> {
        let txn_id = self.txn_id();
        let store = self.open_raw_store_for_txn(uri, txn_id)?;
        let mut btree = store.write();
        scan_range(&mut btree, start, end, txn_id)
    }

    pub(crate) fn register_open_cursor(&self, state: &Arc<Mutex<TableCursorState>>) {
        self.open_cursors.lock().push(Arc::downgrade(state));
    }

    pub(crate) fn reset_open_cursors(&self) {
        let mut open_cursors = self.open_cursors.lock();
        open_cursors.retain(|weak_state| {
            let Some(state) = weak_state.upgrade() else {
                return false;
            };
            state.lock().reset_runtime();
            true
        });
    }

    pub(crate) fn active_txn_handle(&self) -> Option<ActiveTxnHandle> {
        self.active_txn.lock().clone()
    }

    fn take_active_txn(&self) -> Option<ActiveTxnHandle> {
        self.active_txn.lock().take()
    }

    fn commit_txn_with_durability(&self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let commit_ts = txn.id();
        self.durability_backend.log_transaction_commit(
            txn.id(),
            commit_ts,
            txn.log_ops(),
            StorageSyncPolicy::Sync,
        )?;
        self.transaction_manager.commit_txn_state(txn)?;
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.rollback_transaction_internal();
    }
}

struct SessionTransactionGuard<'a> {
    session: &'a mut Session,
    finished: bool,
}

impl<'a> SessionTransactionGuard<'a> {
    fn new(session: &'a mut Session) -> Self {
        Self {
            session,
            finished: false,
        }
    }

    fn run<R>(
        mut self,
        f: impl FnOnce(&mut Session) -> Result<R, WrongoDBError>,
    ) -> Result<R, WrongoDBError> {
        let result = f(self.session);
        match result {
            Ok(value) => {
                self.session.commit_transaction_internal()?;
                self.finished = true;
                Ok(value)
            }
            Err(err) => Err(err),
        }
    }
}

impl Drop for SessionTransactionGuard<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        let _ = self.session.rollback_transaction_internal();
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tempfile::tempdir;

    use super::*;
    use crate::storage::btree::BTreeCursor;
    use crate::storage::durability::DurabilityBackend;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::metadata_catalog::MetadataCatalog;
    use crate::storage::recovery::{recover_from_wal, RecoveryExecutor};
    use crate::storage::wal::{GlobalWal, WalFileReader, WalReader, WalRecord};
    use crate::txn::{GlobalTxnState, TransactionManager};

    const TEST_URI: &str = "table:items";
    const TEST_KEY: &[u8] = b"k1";
    const TEST_VALUE: &[u8] = b"v1";

    struct SessionTestFixture {
        session: Session,
    }

    impl SessionTestFixture {
        fn with_backend(backend: DurabilityBackend) -> Self {
            let dir = tempdir().unwrap();
            let base_path = dir.path().to_path_buf();
            std::mem::forget(dir);
            Self::build(base_path, Arc::new(backend))
        }

        fn open_local_wal<P: AsRef<Path>>(path: P) -> Self {
            let base_path = path.as_ref().to_path_buf();
            std::fs::create_dir_all(&base_path).unwrap();
            let transaction_manager =
                Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let metadata_catalog = Arc::new(MetadataCatalog::new(
                base_path.clone(),
                store_handles.clone(),
            ));
            let applier = Arc::new(RecoveryExecutor::new(
                base_path.clone(),
                metadata_catalog,
                store_handles.clone(),
                transaction_manager,
            ));
            recover_existing_wal_if_present(&base_path, applier.clone());
            let backend = Arc::new(DurabilityBackend::open(&base_path, true).unwrap());
            Self::build(base_path, backend)
        }

        fn into_session(self) -> Session {
            self.session
        }

        fn build(base_path: PathBuf, backend: Arc<DurabilityBackend>) -> Self {
            let transaction_manager =
                Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let metadata_catalog = Arc::new(MetadataCatalog::new(
                base_path.clone(),
                store_handles.clone(),
            ));
            let session = Session::new(
                base_path,
                store_handles,
                metadata_catalog,
                transaction_manager,
                backend,
            );

            Self { session }
        }
    }

    fn recover_existing_wal_if_present(base_path: &Path, applier: Arc<RecoveryExecutor>) {
        let wal_path = GlobalWal::path_for_db(base_path);
        if !wal_path.exists() {
            return;
        }

        let reader = WalFileReader::open(&wal_path).unwrap();
        recover_from_wal(applier, reader).unwrap();
    }

    fn read_wal_records(db_dir: &std::path::Path) -> Vec<WalRecord> {
        let wal_path = db_dir.join("global.wal");
        let mut reader = WalFileReader::open(&wal_path).unwrap();
        let mut records = Vec::new();
        while let Some((_header, record)) = reader.read_record().unwrap() {
            records.push(record);
        }
        records
    }

    fn insert_in_transaction(
        session: &mut Session,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let mut cursor = session.open_cursor(TEST_URI)?;
        cursor.insert(key, value)
    }

    #[test]
    fn create_table_allows_non_transactional_cursor_access() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        cursor.insert(TEST_KEY, TEST_VALUE).unwrap();

        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn create_rejects_index_uris() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();

        let err = session.create("index:test:name").unwrap_err();
        assert!(err.to_string().contains("only table:... is supported"));
    }

    #[test]
    fn public_open_cursor_rejects_index_and_metadata_uris() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let index_err = session.open_cursor("index:items:name").unwrap_err();
        assert!(index_err
            .to_string()
            .contains("only table:... is supported"));

        let metadata_err = session.open_cursor(METADATA_URI).unwrap_err();
        assert!(metadata_err
            .to_string()
            .contains("only table:... is supported"));
    }

    #[test]
    fn transaction_open_cursor_rejects_index_and_metadata_uris() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        session
            .with_transaction(|session| {
                let index_err = session.open_cursor("index:items:name").unwrap_err();
                assert!(index_err
                    .to_string()
                    .contains("only table:... is supported"));

                let metadata_err = session.open_cursor(METADATA_URI).unwrap_err();
                assert!(metadata_err
                    .to_string()
                    .contains("only table:... is supported"));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn transaction_cursor_reads_its_uncommitted_write() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        session
            .with_transaction(|session| {
                insert_in_transaction(session, TEST_KEY, TEST_VALUE)?;
                let mut cursor = session.open_cursor(TEST_URI)?;
                assert_eq!(cursor.get(TEST_KEY)?, Some(TEST_VALUE.to_vec()));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn local_mode_commit_makes_transactional_cursor_write_visible() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        session
            .with_transaction(|session| insert_in_transaction(session, TEST_KEY, TEST_VALUE))
            .unwrap();

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn local_mode_abort_discards_transactional_cursor_write() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let _ = session.with_transaction(|session| {
            insert_in_transaction(session, TEST_KEY, TEST_VALUE)?;
            Err::<(), WrongoDBError>(WrongoDBError::Storage(StorageError(
                "force rollback".into(),
            )))
        });

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), None);
    }

    #[test]
    fn dropped_with_transaction_guard_discards_transactional_cursor_write() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let _ = session.with_transaction(|session| {
            insert_in_transaction(session, TEST_KEY, TEST_VALUE)?;
            Err::<(), WrongoDBError>(WrongoDBError::Storage(StorageError("drop rollback".into())))
        });

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), None);
    }

    #[test]
    fn implicit_write_failure_resets_other_open_cursors() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        {
            let mut cursor = session.open_cursor(TEST_URI).unwrap();
            cursor.insert(b"k1", b"v1").unwrap();
            cursor.insert(b"k2", b"v2").unwrap();
        }

        let mut cursor_a = session.open_cursor(TEST_URI).unwrap();
        let mut cursor_b = session.open_cursor(TEST_URI).unwrap();

        let first = cursor_a.next().unwrap().unwrap();
        assert_eq!(first.0, b"k1".to_vec());

        let err = cursor_b.insert(b"k1", b"duplicate").unwrap_err();
        assert!(err.to_string().contains("duplicate key error"));

        let restarted = cursor_a.next().unwrap().unwrap();
        assert_eq!(restarted.0, b"k1".to_vec());
    }

    #[test]
    fn checkpoint_skips_truncate_when_transaction_active() {
        let dir = tempdir().unwrap();
        let base_path = dir.path().to_path_buf();
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
        let backend = Arc::new(DurabilityBackend::open(&base_path, true).unwrap());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.clone(),
            store_handles.clone(),
        ));
        let mut session = Session::new(
            base_path.clone(),
            store_handles.clone(),
            metadata_catalog.clone(),
            transaction_manager.clone(),
            backend.clone(),
        );
        let mut checkpoint_session = Session::new(
            base_path,
            store_handles,
            metadata_catalog,
            transaction_manager,
            backend,
        );
        session.create(TEST_URI).unwrap();

        session.begin_transaction_internal().unwrap();
        insert_in_transaction(&mut session, TEST_KEY, TEST_VALUE).unwrap();

        let wal_path = dir.path().join("global.wal");
        let before = std::fs::metadata(&wal_path).unwrap().len();
        checkpoint_session.checkpoint().unwrap();
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
        let mut session = SessionTestFixture::open_local_wal(dir.path()).into_session();
        session.create(TEST_URI).unwrap();

        session
            .with_transaction(|session| insert_in_transaction(session, TEST_KEY, TEST_VALUE))
            .unwrap();

        let wal_path = dir.path().join("global.wal");
        session.checkpoint().unwrap();
        let records = read_wal_records(dir.path());
        assert!(records.is_empty());

        let after = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after <= 512);
    }

    #[test]
    fn local_wal_mode_records_transactional_cursor_write_and_commit_markers() {
        let dir = tempdir().unwrap();
        let mut session = SessionTestFixture::open_local_wal(dir.path()).into_session();
        session.create(TEST_URI).unwrap();

        let txn_id = {
            let mut txn_id = 0;
            session
                .with_transaction(|session| {
                    txn_id = session.txn_id();
                    insert_in_transaction(session, TEST_KEY, TEST_VALUE)
                })
                .unwrap();
            txn_id
        };

        let records = read_wal_records(dir.path());
        assert!(records.iter().any(|record| matches!(
            record,
            WalRecord::TxnCommit {
                txn_id: record_txn_id,
                commit_ts,
                ops,
            } if *record_txn_id == txn_id
                && *commit_ts == txn_id
                && ops == &vec![crate::txn::TxnLogOp::Put {
                    uri: TEST_URI.to_string(),
                    key: TEST_KEY.to_vec(),
                    value: TEST_VALUE.to_vec(),
                }]
        )));
    }
}
