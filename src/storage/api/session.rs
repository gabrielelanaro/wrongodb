use std::path::PathBuf;
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::core::errors::StorageError;
use crate::storage::api::cursor::{
    FileCursor, TableCursor, TableCursorState, TableCursorWriteAccess,
};
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::history::HistoryStore;
use crate::storage::log_manager::LogManager;
use crate::storage::metadata_store::{
    file_name_from_uri, table_file_uri, MetadataEntry, MetadataStore, FILE_URI_PREFIX,
    INDEX_URI_PREFIX, TABLE_URI_PREFIX,
};
use crate::storage::reserved_store::{
    reserved_store_identity_for_uri, reserved_store_names, StoreId, HS_STORE_NAME, HS_URI,
    METADATA_URI,
};
use crate::storage::row::{index_key_from_row, validate_storage_columns};
use crate::storage::table::{open_or_create_btree, TableMetadata};
use crate::txn::{GlobalTxnState, Transaction, TxnId, TXN_NONE};
use crate::WrongoDBError;

pub(crate) type ActiveTransactionHandle = Arc<Mutex<Transaction>>;

struct ResolvedStore {
    store_id: StoreId,
    handle: Arc<RwLock<BTreeCursor>>,
}

struct LoadedTable {
    table: TableMetadata,
    primary: Arc<RwLock<BTreeCursor>>,
    indexes: Vec<Arc<RwLock<BTreeCursor>>>,
}

/// A short-lived handle for one storage request.
///
/// A session tracks the current transaction, opens table cursors, and
/// coordinates checkpoints over shared connection state.
pub struct Session {
    base_path: PathBuf,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    metadata_store: Arc<MetadataStore>,
    history_store: Arc<RwLock<HistoryStore>>,
    global_txn: Arc<GlobalTxnState>,
    log_manager: Arc<LogManager>,
    active_transaction: Mutex<Option<ActiveTransactionHandle>>,
    open_cursor_states: Mutex<Vec<Weak<Mutex<TableCursorState>>>>,
}

impl Session {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(in crate::storage) fn new(
        base_path: PathBuf,
        store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
        metadata_store: Arc<MetadataStore>,
        history_store: Arc<RwLock<HistoryStore>>,
        global_txn: Arc<GlobalTxnState>,
        log_manager: Arc<LogManager>,
    ) -> Self {
        Self {
            base_path,
            store_handles,
            metadata_store,
            history_store,
            log_manager,
            global_txn,
            active_transaction: Mutex::new(None),
            open_cursor_states: Mutex::new(Vec::new()),
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    /// Ensure that the managed `file:<name>` object exists.
    pub fn create_file(&mut self, file_uri: &str) -> Result<(), WrongoDBError> {
        if self.current_transaction().is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }

        let file_name = file_name_from_uri(file_uri)?;
        if reserved_store_names().contains(&file_name) {
            return Err(StorageError(format!(
                "reserved store file is not managed through file URIs: {file_uri}"
            ))
            .into());
        }
        if let Some(existing) = self.metadata_store.get(file_uri)? {
            if !existing.is_file() {
                return Err(WrongoDBError::Storage(StorageError(format!(
                    "metadata row for {file_uri} is not a file"
                ))));
            }
            let _ = self.open_store_by_name(file_name)?;
            return Ok(());
        }

        let _ = self.open_store_by_name(file_name)?;
        self.metadata_store.insert(&MetadataEntry::file(
            file_name,
            self.metadata_store.allocate_store_id(),
        ))
    }

    /// Ensure that `table:<name>` exists with the requested storage columns.
    pub fn create_table(
        &mut self,
        table_uri: &str,
        value_columns: Vec<String>,
    ) -> Result<(), WrongoDBError> {
        if let Some(collection) = table_uri.strip_prefix(TABLE_URI_PREFIX) {
            if self.current_transaction().is_some() {
                return Err(WrongoDBError::TransactionAlreadyActive);
            }

            validate_storage_columns(&value_columns)?;

            if let Some(existing) = self.metadata_store.get(table_uri)? {
                if existing.is_table()
                    && existing.value_columns() == value_columns.as_slice()
                    && existing.key_columns().len() == 1
                    && existing.key_columns()[0] == "_id"
                    && existing.source_uri()? == table_file_uri(collection)
                {
                    return Ok(());
                }

                return Err(WrongoDBError::Storage(StorageError(format!(
                    "table metadata for {table_uri} does not match the requested storage schema"
                ))));
            }

            self.create_file(&table_file_uri(collection))?;
            let entry = MetadataEntry::table(collection, value_columns);
            self.metadata_store.insert(&entry)?;
            return Ok(());
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI for Session::create_table: {table_uri}; only table:... is supported"
        ))))
    }

    /// Ensure that a storage-layer `index:` row and backing store exist.
    pub(crate) fn create_index(
        &mut self,
        table_uri: &str,
        index_entry: &MetadataEntry,
    ) -> Result<(), WrongoDBError> {
        if self.current_transaction().is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }

        if !index_entry.is_index() {
            return Err(WrongoDBError::Storage(StorageError(format!(
                "unsupported URI for Session::create_index: {}; only index:... is supported",
                index_entry.uri()
            ))));
        }

        if table_uri != Self::table_uri_for_index(index_entry.uri())?.as_str() {
            return Err(WrongoDBError::Storage(StorageError(format!(
                "index {} does not belong to table {table_uri}",
                index_entry.uri()
            ))));
        }

        let loaded = self.load_table_runtime(table_uri)?;
        let stored_index = self.ensure_index_store_exists(&loaded.table, index_entry)?;
        let index_runtime = self.load_file_runtime(stored_index.source_uri()?)?;
        let index_metadata = stored_index
            .clone()
            .into_index_metadata(index_runtime.store_id)?;
        let index_file_uri = stored_index.source_uri()?.to_string();
        let table = loaded.table.clone();

        self.with_transaction(|session| {
            let mut primary_cursor = session.open_table_cursor(table_uri)?;
            let mut index_cursor = session.open_file_cursor(&index_file_uri)?;
            while let Some((primary_key, primary_value)) = primary_cursor.next()? {
                let Some(index_key) =
                    index_key_from_row(&table, &index_metadata, &primary_key, &primary_value)?
                else {
                    continue;
                };
                if index_cursor.get(&index_key)?.is_some() {
                    index_cursor.update(&index_key, &[])?;
                } else {
                    index_cursor.insert(&index_key, &[])?;
                }
            }
            Ok(())
        })
    }

    /// Open a cursor for `file:<name>` in the current transaction context.
    pub fn open_file_cursor(&self, file_uri: &str) -> Result<FileCursor<'_>, WrongoDBError> {
        let loaded = self.load_file_runtime(file_uri)?;
        let state = self.create_cursor_state();

        Ok(FileCursor::new(
            self,
            file_uri,
            loaded.store_id,
            loaded.handle,
            TableCursorWriteAccess::ReadWrite,
            state,
        ))
    }

    /// Open a cursor for `table:<name>` in the current transaction context.
    pub fn open_table_cursor(&self, table_uri: &str) -> Result<TableCursor<'_>, WrongoDBError> {
        let loaded = self.load_table_runtime(table_uri)?;

        if loaded.table.indexes().len() != loaded.indexes.len() {
            return Err(StorageError(format!(
                "table cursor index handle mismatch: metadata has {}, runtime has {}",
                loaded.table.indexes().len(),
                loaded.indexes.len()
            ))
            .into());
        }

        let state = self.create_cursor_state();

        Ok(TableCursor::new(
            self,
            loaded.table,
            loaded.primary,
            loaded.indexes,
            TableCursorWriteAccess::ReadWrite,
            state,
        ))
    }

    /// Open a cursor for `index:<collection>:<name>` in the current transaction context.
    ///
    /// Returns a [`FileCursor`] for raw key/value access to the index store.
    /// Index cursors return `(index_key, primary_key)` tuples that can be used
    /// to fetch full documents from the main table.
    pub fn open_index_cursor(&self, index_uri: &str) -> Result<FileCursor<'_>, WrongoDBError> {
        let resolved = self.resolve_store(index_uri)?;
        let state = self.create_cursor_state();

        Ok(FileCursor::new(
            self,
            index_uri,
            resolved.store_id,
            resolved.handle,
            TableCursorWriteAccess::ReadWrite,
            state,
        ))
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
        let mut store_entries = [METADATA_URI, HS_URI]
            .into_iter()
            .filter_map(reserved_store_identity_for_uri)
            .filter(|(_, store_name)| *store_name != HS_STORE_NAME)
            .map(|(store_id, store_name)| (store_id, store_name.to_string()))
            .collect::<Vec<_>>();
        store_entries.extend(self.metadata_store.all_stores_with_id()?);
        store_entries.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        store_entries.dedup();

        let stores = store_entries
            .iter()
            .map(|(store_id, store_name)| {
                Ok::<_, WrongoDBError>((*store_id, self.open_store_by_name(store_name)?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut history_store = self.history_store.write();

        crate::storage::checkpoint::run_checkpoint(
            &stores,
            &mut history_store,
            self.global_txn.as_ref(),
            &self.log_manager,
        )
    }

    // ------------------------------------------------------------------------
    // Transaction lifecycle
    // ------------------------------------------------------------------------

    pub(crate) fn current_txn_id(&self) -> TxnId {
        self.current_transaction()
            .map(|txn| txn.lock().id())
            .unwrap_or(TXN_NONE)
    }

    fn begin_transaction(&mut self) -> Result<(), WrongoDBError> {
        let mut active_transaction = self.active_transaction.lock();
        if active_transaction.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }
        let txn = self.global_txn.begin_snapshot_txn();
        *active_transaction = Some(Arc::new(Mutex::new(txn)));
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), WrongoDBError> {
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

    fn rollback_transaction(&mut self) -> Result<(), WrongoDBError> {
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
    // Cursor lifecycle
    // ------------------------------------------------------------------------

    fn create_cursor_state(&self) -> Arc<Mutex<TableCursorState>> {
        let state = Arc::new(Mutex::new(TableCursorState::default()));
        self.open_cursor_states.lock().push(Arc::downgrade(&state));
        state
    }

    fn reset_open_cursor_states(&self) {
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

    fn load_table_runtime(&self, table_uri: &str) -> Result<LoadedTable, WrongoDBError> {
        if !table_uri.starts_with(TABLE_URI_PREFIX) {
            return Err(StorageError(format!(
                "unsupported URI for public cursor open: {table_uri}; only table:... is supported"
            ))
            .into());
        }

        let Some(table_entry) = self.metadata_store.get(table_uri)? else {
            return Err(StorageError(format!("unknown URI: {table_uri}")).into());
        };
        if !table_entry.is_table() {
            return Err(StorageError(format!("URI is not a table: {table_uri}")).into());
        }

        let collection = table_uri.strip_prefix(TABLE_URI_PREFIX).ok_or_else(|| {
            StorageError(format!(
                "unsupported URI for public cursor open: {table_uri}; only table:... is supported"
            ))
        })?;
        let index_prefix = format!("{INDEX_URI_PREFIX}{collection}:");
        let mut index_entries = self.metadata_store.scan_prefix(&index_prefix)?;
        index_entries.sort_by(|left, right| left.uri().cmp(right.uri()));

        let primary = self.load_file_runtime(table_entry.source_uri()?)?;
        let mut indexes = Vec::new();
        let mut index_metadata = Vec::new();
        for entry in index_entries {
            let resolved = self.load_file_runtime(entry.source_uri()?)?;
            indexes.push(resolved.handle.clone());
            index_metadata.push(entry.into_index_metadata(resolved.store_id)?);
        }
        let table = table_entry.into_table_metadata(primary.store_id, index_metadata)?;

        Ok(LoadedTable {
            table,
            primary: primary.handle,
            indexes,
        })
    }

    fn load_file_runtime(&self, file_uri: &str) -> Result<ResolvedStore, WrongoDBError> {
        if let Some((store_id, store_name)) = reserved_store_identity_for_uri(file_uri) {
            return Ok(ResolvedStore {
                store_id,
                handle: self.open_store_by_name(store_name)?,
            });
        }

        if !file_uri.starts_with(FILE_URI_PREFIX) {
            return Err(StorageError(format!(
                "unsupported URI for public file cursor open: {file_uri}; only file:... is supported"
            ))
            .into());
        }
        if reserved_store_names().contains(&file_name_from_uri(file_uri)?) {
            return Err(StorageError(format!(
                "reserved store file is not exposed as a public file cursor: {file_uri}"
            ))
            .into());
        }

        let Some(file_entry) = self.metadata_store.get(file_uri)? else {
            return Err(StorageError(format!("unknown URI: {file_uri}")).into());
        };
        if !file_entry.is_file() {
            return Err(StorageError(format!("URI is not a file: {file_uri}")).into());
        }

        Ok(ResolvedStore {
            store_id: file_entry.store_id()?,
            handle: self.open_store_by_name(file_entry.file_name()?)?,
        })
    }

    fn resolve_store(&self, uri: &str) -> Result<ResolvedStore, WrongoDBError> {
        if let Some((store_id, store_name)) = reserved_store_identity_for_uri(uri) {
            return Ok(ResolvedStore {
                store_id,
                handle: self.open_store_by_name(store_name)?,
            });
        }

        let Some(entry) = self.metadata_store.get(uri)? else {
            return Err(StorageError(format!("unknown URI: {uri}")).into());
        };

        if entry.is_file() {
            return self.load_file_runtime(entry.uri());
        }

        self.load_file_runtime(entry.source_uri()?)
    }

    fn table_uri_for_index(index_uri: &str) -> Result<String, WrongoDBError> {
        let Some(rest) = index_uri.strip_prefix(INDEX_URI_PREFIX) else {
            return Err(StorageError(format!(
                "unsupported URI for Session::create_index: {index_uri}; only index:... is supported"
            ))
            .into());
        };
        let Some((collection, _)) = rest.split_once(':') else {
            return Err(StorageError(format!("invalid index URI: {index_uri}")).into());
        };
        Ok(format!("{TABLE_URI_PREFIX}{collection}"))
    }

    fn ensure_index_store_exists(
        &mut self,
        table: &TableMetadata,
        index_entry: &MetadataEntry,
    ) -> Result<MetadataEntry, WrongoDBError> {
        if index_entry.columns().len() != 1 {
            return Err(StorageError(format!(
                "index {} must declare exactly one indexed column",
                index_entry.uri()
            ))
            .into());
        }

        let indexed_column = &index_entry.columns()[0];
        if table.value_column_position(indexed_column).is_none() {
            return Err(StorageError(format!(
                "table {} does not declare storage column {} required by {}",
                table.uri(),
                indexed_column,
                index_entry.uri()
            ))
            .into());
        }

        let stored_entry = if let Some(existing) = self.metadata_store.get(index_entry.uri())? {
            if existing.columns() != index_entry.columns()
                || existing.source_uri()? != index_entry.source_uri()?
            {
                return Err(StorageError(format!(
                    "index metadata for {} does not match the requested definition",
                    index_entry.uri()
                ))
                .into());
            }
            existing
        } else {
            self.create_file(index_entry.source_uri()?)?;
            self.metadata_store.insert(index_entry)?;
            index_entry.clone()
        };

        self.create_file(stored_entry.source_uri()?)?;
        Ok(stored_entry)
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
    use crate::storage::history::HistoryStore;
    use crate::storage::log_manager::{open_recovery_reader_if_present, LogManager, LoggingConfig};
    use crate::storage::metadata_store::MetadataStore;
    use crate::storage::recovery::recover_from_wal;
    use crate::storage::reserved_store::HS_STORE_NAME;
    use crate::storage::wal::{WalFileReader, WalReader, WalRecord};
    use crate::txn::GlobalTxnState;

    const TEST_FILE_URI: &str = "file:items.main.wt";
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
            let log_manager =
                Arc::new(LogManager::open(&base_path, &LoggingConfig::default()).unwrap());
            let history_store = Arc::new(RwLock::new(
                HistoryStore::open_or_create(base_path.join(HS_STORE_NAME)).unwrap(),
            ));
            let metadata_store = Arc::new(
                MetadataStore::new(
                    base_path.clone(),
                    store_handles.clone(),
                    global_txn.clone(),
                    log_manager.clone(),
                )
                .unwrap(),
            );
            recover_existing_wal_if_present(
                &base_path,
                metadata_store.as_ref(),
                store_handles.as_ref(),
                &mut history_store.write(),
                global_txn.as_ref(),
            );
            Self::build(base_path, log_manager)
        }

        fn into_session(self) -> Session {
            self.session
        }

        fn build(base_path: PathBuf, log_manager: Arc<LogManager>) -> Self {
            let global_txn = Arc::new(GlobalTxnState::new());
            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let history_store = Arc::new(RwLock::new(
                HistoryStore::open_or_create(base_path.join(HS_STORE_NAME)).unwrap(),
            ));
            let metadata_store = Arc::new(
                MetadataStore::new(
                    base_path.clone(),
                    store_handles.clone(),
                    global_txn.clone(),
                    log_manager.clone(),
                )
                .unwrap(),
            );
            let session = Session::new(
                base_path,
                store_handles,
                metadata_store,
                history_store,
                global_txn,
                log_manager,
            );

            Self { session }
        }
    }

    fn recover_existing_wal_if_present(
        base_path: &Path,
        metadata_store: &MetadataStore,
        store_handles: &HandleCache<String, RwLock<BTreeCursor>>,
        history_store: &mut HistoryStore,
        global_txn: &GlobalTxnState,
    ) {
        let Some(reader) = open_recovery_reader_if_present(base_path) else {
            return;
        };
        recover_from_wal(
            base_path,
            metadata_store,
            store_handles,
            history_store,
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
        session.create_table(TEST_URI, Vec::new()).unwrap();

        let mut cursor = session.open_table_cursor(TEST_URI).unwrap();
        cursor.insert(TEST_KEY, TEST_VALUE).unwrap();

        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn create_file_allows_non_transactional_cursor_access() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_file(TEST_FILE_URI).unwrap();

        let stored = session.metadata_store.get(TEST_FILE_URI).unwrap().unwrap();
        assert!(stored.is_file());
        assert!(session.base_path.join("items.main.wt").exists());

        let mut cursor = session.open_file_cursor(TEST_FILE_URI).unwrap();
        cursor.insert(TEST_KEY, TEST_VALUE).unwrap();

        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn public_open_file_cursor_rejects_non_file_and_unknown_uris() {
        let session = SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();

        let table_err = session.open_file_cursor(TEST_URI).unwrap_err();
        assert!(table_err.to_string().contains("only file:... is supported"));

        let missing_err = session.open_file_cursor(TEST_FILE_URI).unwrap_err();
        assert!(missing_err
            .to_string()
            .contains("unknown URI: file:items.main.wt"));

        let reserved_err = session.open_file_cursor("file:metadata.wt").unwrap_err();
        assert!(reserved_err
            .to_string()
            .contains("reserved store file is not exposed as a public file cursor"));
    }

    #[test]
    fn create_file_rejects_reserved_metadata_store_name() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();

        let err = session.create_file("file:metadata.wt").unwrap_err();
        assert!(err
            .to_string()
            .contains("reserved store file is not managed through file URIs"));
    }

    #[test]
    fn create_index_creates_backing_store_and_metadata() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session
            .create_table(TEST_URI, vec!["name".to_string()])
            .unwrap();

        let entry = MetadataEntry::index("items", "name_1", vec!["name".to_string()]);
        let source = entry.source_uri().unwrap().to_string();

        session.create_index(TEST_URI, &entry).unwrap();
        session.create_index(TEST_URI, &entry).unwrap();

        let stored = session.metadata_store.get(entry.uri()).unwrap().unwrap();
        assert_eq!(stored.source_uri().unwrap(), source);
        assert!(session
            .base_path
            .join(file_name_from_uri(&source).unwrap())
            .exists());
    }

    #[test]
    fn create_rejects_index_uris() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();

        let err = session
            .create_table("index:test:name", Vec::new())
            .unwrap_err();
        assert!(err.to_string().contains("only table:... is supported"));
    }

    #[test]
    fn create_index_rejects_missing_table() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();

        let entry = MetadataEntry::index("items", "name_1", vec!["name".to_string()]);

        let err = session.create_index(TEST_URI, &entry).unwrap_err();
        assert!(err.to_string().contains("unknown URI: table:items"));
    }

    #[test]
    fn public_open_cursor_rejects_index_and_metadata_uris() {
        let mut session =
            SessionTestFixture::with_log_manager(LogManager::disabled()).into_session();
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        let metadata_store = Arc::new(
            MetadataStore::new(
                base_path.clone(),
                store_handles.clone(),
                global_txn.clone(),
                log_manager.clone(),
            )
            .unwrap(),
        );
        let history_store = Arc::new(RwLock::new(
            HistoryStore::open_or_create(base_path.join(HS_STORE_NAME)).unwrap(),
        ));
        let mut session = Session::new(
            base_path.clone(),
            store_handles.clone(),
            metadata_store.clone(),
            history_store.clone(),
            global_txn.clone(),
            log_manager.clone(),
        );
        let mut checkpoint_session = Session::new(
            base_path,
            store_handles,
            metadata_store,
            history_store,
            global_txn,
            log_manager,
        );
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();

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
        session.create_table(TEST_URI, Vec::new()).unwrap();
        let store_id = session
            .metadata_store
            .get(TEST_URI)
            .unwrap()
            .unwrap()
            .source_uri()
            .map(|source_uri| {
                session
                    .metadata_store
                    .get(source_uri)
                    .unwrap()
                    .unwrap()
                    .store_id()
                    .unwrap()
            })
            .unwrap();

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
                    store_id,
                    key: TEST_KEY.to_vec(),
                    value: TEST_VALUE.to_vec(),
                }]
        )));
    }
}
