use std::path::PathBuf;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::storage::api::cursor::{TableCursor, TableCursorState, TableCursorWriteAccess};
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::log_manager::LogManager;
use crate::storage::metadata_catalog::{MetadataCatalog, METADATA_STORE_NAME, METADATA_URI};
use crate::storage::table::{
    checkpoint_store, contains_key, open_or_create_btree, scan_range, TableMetadata,
};
use crate::txn::{GlobalTxnState, Transaction, TxnId, TXN_NONE};
use crate::WrongoDBError;

pub(crate) type ActiveTransactionHandle = Arc<Mutex<Transaction>>;

type StoreEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// A short-lived handle for one storage request.
///
/// A session tracks the current transaction, opens table cursors, and
/// coordinates checkpoints over shared connection state.
pub struct Session {
    base_path: PathBuf,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    metadata_catalog: Arc<MetadataCatalog>,
    global_txn: Arc<GlobalTxnState>,
    log_manager: Arc<LogManager>,
    active_transaction: Mutex<Option<ActiveTransactionHandle>>,
    open_cursor_states: Mutex<Vec<Weak<Mutex<TableCursorState>>>>,
}

impl Session {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(
        base_path: PathBuf,
        store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
        metadata_catalog: Arc<MetadataCatalog>,
        global_txn: Arc<GlobalTxnState>,
        log_manager: Arc<LogManager>,
    ) -> Self {
        Self {
            base_path,
            store_handles,
            metadata_catalog,
            log_manager,
            global_txn,
            active_transaction: Mutex::new(None),
            open_cursor_states: Mutex::new(Vec::new()),
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    /// Ensure that `table:<name>` exists.
    pub fn create_table(&mut self, table_uri: &str) -> Result<(), WrongoDBError> {
        if let Some(collection) = table_uri.strip_prefix("table:") {
            let metadata_catalog = self.metadata_catalog.clone();
            return self.with_transaction(|session| {
                let _ = metadata_catalog.ensure_table_uri_in_transaction(session, collection)?;
                Ok(())
            });
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI for Session::create_table: {table_uri}; only table:... is supported"
        ))))
    }

    /// Open a cursor for `table:<name>` in the current transaction context.
    pub fn open_table_cursor(&self, table_uri: &str) -> Result<TableCursor<'_>, WrongoDBError> {
        let txn_id = self.current_txn_id();
        let table = self.load_table_metadata(table_uri, txn_id)?;
        let primary = self.open_store_for_uri(table.uri(), txn_id)?;
        let indexes = table
            .indexes()
            .iter()
            .map(|index| self.open_store_for_uri(index.uri(), txn_id))
            .collect::<Result<Vec<_>, _>>()?;

        TableCursor::new(
            self,
            table,
            primary,
            indexes,
            TableCursorWriteAccess::ReadWrite,
        )
    }

    /// Run `f` inside a session transaction.
    pub fn with_transaction<R>(
        &mut self,
        f: impl FnOnce(&mut Session) -> Result<R, WrongoDBError>,
    ) -> Result<R, WrongoDBError> {
        self.begin_transaction()?;
        let guard = SessionTransactionGuard::new(self);
        guard.run(f)
    }

    /// Flush all known stores to a stable checkpoint.
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let mut store_names = self.metadata_catalog.all_store_names()?;
        store_names.push(METADATA_STORE_NAME.to_string());
        store_names.sort();
        store_names.dedup();

        for store_name in store_names {
            let store = self.open_store_by_name(&store_name)?;
            checkpoint_store(&mut store.write(), self.global_txn.as_ref())?;
        }

        if self.global_txn.has_active_transactions() || !self.log_manager.is_enabled() {
            return Ok(());
        }

        self.log_manager.log_checkpoint()?;
        self.log_manager.truncate_to_checkpoint()
    }

    // ------------------------------------------------------------------------
    // Transaction lifecycle
    // ------------------------------------------------------------------------

    pub(crate) fn current_txn_id(&self) -> TxnId {
        self.current_transaction()
            .map(|txn| txn.lock().id())
            .unwrap_or(TXN_NONE)
    }

    pub(crate) fn begin_transaction(&mut self) -> Result<(), WrongoDBError> {
        let mut active_transaction = self.active_transaction.lock();
        if active_transaction.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }
        let txn = self.global_txn.begin_snapshot_txn();
        *active_transaction = Some(Arc::new(Mutex::new(txn)));
        Ok(())
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<(), WrongoDBError> {
        let Some(txn_handle) = self.active_transaction.lock().take() else {
            return Ok(());
        };

        let result = {
            let mut txn = txn_handle.lock();
            self.durably_commit_transaction(&mut txn)
        };

        if result.is_err() {
            self.reset_open_cursor_states();
            let mut txn = txn_handle.lock();
            let _ = txn.abort(self.global_txn.as_ref());
        }

        result
    }

    pub(crate) fn rollback_transaction(&mut self) -> Result<(), WrongoDBError> {
        let Some(txn_handle) = self.active_transaction.lock().take() else {
            return Ok(());
        };

        self.reset_open_cursor_states();
        let mut txn = txn_handle.lock();
        txn.abort(self.global_txn.as_ref())
    }

    pub(crate) fn with_write_transaction<R, F>(&self, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(TxnId, &mut Transaction) -> Result<R, WrongoDBError>,
    {
        if let Some(txn_handle) = self.current_transaction() {
            let mut txn = txn_handle.lock();
            return f(txn.id(), &mut txn);
        }

        let mut txn = self.global_txn.begin_snapshot_txn();
        let txn_id = txn.id();

        let result = f(txn_id, &mut txn);
        match result {
            Ok(value) => {
                if let Err(err) = self.durably_commit_transaction(&mut txn) {
                    self.reset_open_cursor_states();
                    let _ = txn.abort(self.global_txn.as_ref());
                    return Err(err);
                }
                Ok(value)
            }
            Err(err) => {
                self.reset_open_cursor_states();
                let _ = txn.abort(self.global_txn.as_ref());
                Err(err)
            }
        }
    }

    // ------------------------------------------------------------------------
    // Store operations
    // ------------------------------------------------------------------------

    pub(crate) fn insert_into_store(
        &self,
        store_uri: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = self.current_txn_id();
        if txn_id == TXN_NONE {
            return Err(
                StorageError("insert_into_store requires an active transaction".into()).into(),
            );
        }

        let store = self.open_store_for_uri(store_uri, txn_id)?;
        let txn_handle = self.current_transaction().ok_or_else(|| {
            StorageError("insert_into_store requires an active transaction".into())
        })?;
        let mut btree = store.write();
        if contains_key(&mut btree, key, txn_id)? {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }

        let mut txn = txn_handle.lock();
        btree.put(store_uri, key, value, &mut txn)?;
        Ok(())
    }

    pub(crate) fn scan_store_range(
        &self,
        store_uri: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<StoreEntries, WrongoDBError> {
        let txn_id = self.current_txn_id();
        let store = self.open_store_for_uri(store_uri, txn_id)?;
        let mut btree = store.write();
        scan_range(&mut btree, start, end, txn_id)
    }

    // ------------------------------------------------------------------------
    // Cursor lifecycle
    // ------------------------------------------------------------------------

    pub(crate) fn track_open_cursor(&self, state: &Arc<Mutex<TableCursorState>>) {
        self.open_cursor_states.lock().push(Arc::downgrade(state));
    }

    pub(crate) fn reset_open_cursor_states(&self) {
        let mut open_cursor_states = self.open_cursor_states.lock();
        open_cursor_states.retain(|weak_state| {
            let Some(state) = weak_state.upgrade() else {
                return false;
            };
            state.lock().reset_runtime();
            true
        });
    }

    // ------------------------------------------------------------------------
    // Lookup and durability helpers
    // ------------------------------------------------------------------------

    fn load_table_metadata(
        &self,
        table_uri: &str,
        txn_id: TxnId,
    ) -> Result<TableMetadata, WrongoDBError> {
        if !table_uri.starts_with("table:") {
            return Err(StorageError(format!(
                "unsupported URI for public cursor open: {table_uri}; only table:... is supported"
            ))
            .into());
        }

        self.metadata_catalog
            .table_metadata_for_txn(table_uri, txn_id)?
            .ok_or_else(|| StorageError(format!("unknown URI: {table_uri}")).into())
    }

    fn open_store_for_uri(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        if uri != METADATA_URI && !uri.starts_with("table:") && !uri.starts_with("index:") {
            return Err(StorageError(format!("unsupported URI: {uri}")).into());
        }

        let store_name = self
            .metadata_catalog
            .lookup_store_name_for_txn(uri, txn_id)?
            .ok_or_else(|| StorageError(format!("unknown URI: {uri}")))?;
        self.open_store_by_name(&store_name)
    }

    fn open_store_by_name(
        &self,
        store_name: &str,
    ) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        self.store_handles
            .get_or_try_insert_with(store_name.to_string(), |store_name| {
                let path = self.base_path.join(store_name);
                Ok(RwLock::new(open_or_create_btree(path)?))
            })
    }

    fn current_transaction(&self) -> Option<ActiveTransactionHandle> {
        self.active_transaction.lock().clone()
    }

    fn durably_commit_transaction(&self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let commit_ts = txn.id();
        // The commit record must reach the log before MVCC state becomes
        // globally visible.
        self.log_manager
            .log_transaction_commit(txn.id(), commit_ts, txn.log_ops())?;
        txn.commit(self.global_txn.as_ref())?;
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.rollback_transaction();
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
                self.session.commit_transaction()?;
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

        let _ = self.session.rollback_transaction();
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
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::log_manager::{open_recovery_reader_if_present, LogManager, LoggingConfig};
    use crate::storage::metadata_catalog::MetadataCatalog;
    use crate::storage::recovery::recover_from_wal;
    use crate::storage::wal::{WalFileReader, WalReader, WalRecord};
    use crate::txn::GlobalTxnState;

    const TEST_URI: &str = "table:items";
    const TEST_KEY: &[u8] = b"k1";
    const TEST_VALUE: &[u8] = b"v1";

    struct SessionTestFixture {
        session: Session,
    }

    impl SessionTestFixture {
        fn with_log_manager(log_manager: LogManager) -> Self {
            let dir = tempdir().unwrap();
            let base_path = dir.path().to_path_buf();
            std::mem::forget(dir);
            Self::build(base_path, Arc::new(log_manager))
        }

        fn open_local_wal<P: AsRef<Path>>(path: P) -> Self {
            let base_path = path.as_ref().to_path_buf();
            std::fs::create_dir_all(&base_path).unwrap();
            let global_txn = Arc::new(GlobalTxnState::new());
            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let metadata_catalog = Arc::new(MetadataCatalog::new(
                base_path.clone(),
                store_handles.clone(),
            ));
            recover_existing_wal_if_present(
                &base_path,
                metadata_catalog.as_ref(),
                store_handles.as_ref(),
                global_txn.as_ref(),
            );
            let log_manager =
                Arc::new(LogManager::open(&base_path, &LoggingConfig::default()).unwrap());
            Self::build(base_path, log_manager)
        }

        fn into_session(self) -> Session {
            self.session
        }

        fn build(base_path: PathBuf, log_manager: Arc<LogManager>) -> Self {
            let global_txn = Arc::new(GlobalTxnState::new());
            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let metadata_catalog = Arc::new(MetadataCatalog::new(
                base_path.clone(),
                store_handles.clone(),
            ));
            let session = Session::new(
                base_path,
                store_handles,
                metadata_catalog,
                global_txn,
                log_manager,
            );

            Self { session }
        }
    }

    fn recover_existing_wal_if_present(
        base_path: &Path,
        metadata_catalog: &MetadataCatalog,
        store_handles: &HandleCache<String, RwLock<BTreeCursor>>,
        global_txn: &GlobalTxnState,
    ) {
        let Some(reader) = open_recovery_reader_if_present(base_path) else {
            return;
        };
        recover_from_wal(
            base_path,
            metadata_catalog,
            store_handles,
            global_txn,
            reader,
        )
        .unwrap();
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
        let mut cursor = session.open_table_cursor(TEST_URI)?;
        cursor.insert(key, value)
    }

    #[test]
    fn create_table_allows_non_transactional_cursor_access() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        let mut cursor = session.open_table_cursor(TEST_URI).unwrap();
        cursor.insert(TEST_KEY, TEST_VALUE).unwrap();

        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn create_rejects_index_uris() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();

        let err = session.create_table("index:test:name").unwrap_err();
        assert!(err.to_string().contains("only table:... is supported"));
    }

    #[test]
    fn public_open_cursor_rejects_index_and_metadata_uris() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        let index_err = session.open_table_cursor("index:items:name").unwrap_err();
        assert!(index_err
            .to_string()
            .contains("only table:... is supported"));

        let metadata_err = session.open_table_cursor(METADATA_URI).unwrap_err();
        assert!(metadata_err
            .to_string()
            .contains("only table:... is supported"));
    }

    #[test]
    fn transaction_open_cursor_rejects_index_and_metadata_uris() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        session
            .with_transaction(|session| {
                let index_err = session.open_table_cursor("index:items:name").unwrap_err();
                assert!(index_err
                    .to_string()
                    .contains("only table:... is supported"));

                let metadata_err = session.open_table_cursor(METADATA_URI).unwrap_err();
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
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        session
            .with_transaction(|session| {
                insert_in_transaction(session, TEST_KEY, TEST_VALUE)?;
                let mut cursor = session.open_table_cursor(TEST_URI)?;
                assert_eq!(cursor.get(TEST_KEY)?, Some(TEST_VALUE.to_vec()));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn local_mode_commit_makes_transactional_cursor_write_visible() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        session
            .with_transaction(|session| insert_in_transaction(session, TEST_KEY, TEST_VALUE))
            .unwrap();

        let mut cursor = session.open_table_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn local_mode_abort_discards_transactional_cursor_write() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        let _ = session.with_transaction(|session| {
            insert_in_transaction(session, TEST_KEY, TEST_VALUE)?;
            Err::<(), WrongoDBError>(WrongoDBError::Storage(StorageError(
                "force rollback".into(),
            )))
        });

        let mut cursor = session.open_table_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), None);
    }

    #[test]
    fn dropped_with_transaction_guard_discards_transactional_cursor_write() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        let _ = session.with_transaction(|session| {
            insert_in_transaction(session, TEST_KEY, TEST_VALUE)?;
            Err::<(), WrongoDBError>(WrongoDBError::Storage(StorageError("drop rollback".into())))
        });

        let mut cursor = session.open_table_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), None);
    }

    #[test]
    fn implicit_write_failure_resets_other_open_cursors() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI).unwrap();

        {
            let mut cursor = session.open_table_cursor(TEST_URI).unwrap();
            cursor.insert(b"k1", b"v1").unwrap();
            cursor.insert(b"k2", b"v2").unwrap();
        }

        let mut cursor_a = session.open_table_cursor(TEST_URI).unwrap();
        let mut cursor_b = session.open_table_cursor(TEST_URI).unwrap();

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
        let global_txn = Arc::new(GlobalTxnState::new());
        let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
        let log_manager =
            Arc::new(LogManager::open(&base_path, &LoggingConfig::default()).unwrap());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.clone(),
            store_handles.clone(),
        ));
        let mut session = Session::new(
            base_path.clone(),
            store_handles.clone(),
            metadata_catalog.clone(),
            global_txn.clone(),
            log_manager.clone(),
        );
        let mut checkpoint_session = Session::new(
            base_path,
            store_handles,
            metadata_catalog,
            global_txn,
            log_manager,
        );
        session.create_table(TEST_URI).unwrap();

        session.begin_transaction().unwrap();
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
        session.create_table(TEST_URI).unwrap();

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
        session.create_table(TEST_URI).unwrap();

        let txn_id = {
            let mut txn_id = 0;
            session
                .with_transaction(|session| {
                    txn_id = session.current_txn_id();
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
